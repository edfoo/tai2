"""Phase 5 of the wick_rejection plan: the dedicated ``vol`` screener universe.

Covers ``compute_speed`` (§3.5), the vol pool in ``score_universe`` (gates,
score, cap, dual and legacy mode, appended to ``selected``), routing in
``universe_for_strategy`` and live ``_strategy_allowed_on_symbol``, and the
backtest universe (vol list per interval, closed-candle visibility).
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from app.services.backtest.models import Candle
from app.services.backtest.universe import UniverseSchedule, UniverseInterval, build_universe_schedule
from app.services.market_service import MarketService
from app.services.screener import compute_speed, score_universe, universe_for_strategy
from app.services.strategies.defaults import screener_defaults

HOUR = 3_600_000
NOW = 1_700_000_000.0


def _cand(symbol: str, *, rng: float | None = None, hours: float | None = 4.0,
          vol_ratio: float | None = 1.0, decay: float | None = 1.0,
          mom: float = 1.0, hl: float = 30.0) -> dict[str, Any]:
    """A build_candidates-shaped candidate plus its activity features."""
    return {
        "symbol": symbol, "vol_ccy_24h": 50_000_000.0, "vol_spike_ratio": None,
        "hl_range_pct": hl, "momentum_pct": mom, "spread_pct": None,
        "_act": None if rng is None else {
            "recent_vol": 1.0, "recent_vol_ratio": vol_ratio, "recent_range_pct": rng,
            "recent_mom_pct": mom, "decay_ratio": decay, "hours_covered": hours,
            "candle_count": 4,
        },
    }


def _hist(move_pct: float) -> deque:
    """Price samples over the last hour ending at NOW with the given move."""
    return deque([(NOW - 3600, 100.0), (NOW - 1800, 100.0 + move_pct / 2),
                  (NOW, 100.0 + move_pct)])


def _score(cands: list[dict[str, Any]], *, history: dict[str, deque] | None = None,
           **cfg_over: Any) -> dict[str, Any]:
    cfg = dict(screener_defaults(), enabled=True, vol_enabled=True,
               activity_lookback_hours=4, sc_max_symbols=1, mr_max_symbols=1)
    cfg.update(cfg_over)
    activity = {c["symbol"]: c["_act"] for c in cands if c["_act"]}
    base = [{k: v for k, v in c.items() if k != "_act"} for c in cands]
    return score_universe([], cfg, candidates=base, activity=activity,
                          price_history=history or {}, now_ts=NOW)


# ── Speed ─────────────────────────────────────────────────────────────────────


class TestComputeSpeed:
    def test_closed_speed_uses_hours_covered(self) -> None:
        out = compute_speed({"recent_range_pct": 8.0, "recent_hours_covered": 2.0},
                            None, now_ts=NOW, window_minutes=60)
        assert out == {"closed_speed": 4.0, "fast_speed": None, "speed": 4.0}

    def test_fast_speed_from_history(self) -> None:
        out = compute_speed({}, _hist(-3.0), now_ts=NOW, window_minutes=60)
        assert out["closed_speed"] is None
        assert out["fast_speed"] == pytest.approx(3.0)
        assert out["speed"] == pytest.approx(3.0)

    def test_speed_is_max(self) -> None:
        out = compute_speed({"recent_range_pct": 4.0, "recent_hours_covered": 4.0},
                            _hist(5.0), now_ts=NOW, window_minutes=60)
        assert out["speed"] == pytest.approx(5.0)

    def test_never_falls_back_to_24h_range(self) -> None:
        out = compute_speed({"hl_range_pct": 60.0, "recent_range_pct": None},
                            None, now_ts=NOW, window_minutes=60)
        assert out["speed"] is None


# ── Vol pool ──────────────────────────────────────────────────────────────────


class TestVolPool:
    def test_gates(self) -> None:
        res = _score([
            _cand("FAST-USDT-SWAP", rng=8.0, vol_ratio=1.0),       # 2%/h
            _cand("SLOW-USDT-SWAP", rng=2.0, vol_ratio=1.0),       # 0.5%/h → speed fail
            _cand("THIN-USDT-SWAP", rng=8.0, vol_ratio=0.3),       # vol ratio fail
            _cand("NOACT-USDT-SWAP"),                              # no activity
        ], history={"NOACT-USDT-SWAP": _hist(10.0)})
        assert res["vol"] == ["FAST-USDT-SWAP"]
        assert res["vol_pool_size"] == 1

    def test_activity_min_ratio_not_applied(self) -> None:
        res = _score([_cand("A-USDT-SWAP", rng=8.0, vol_ratio=0.6)], activity_min_ratio=1.0)
        assert res["vol"] == ["A-USDT-SWAP"]

    def test_score_order_and_cap(self) -> None:
        cands = [_cand(f"S{i}-USDT-SWAP", rng=4.0 * (i + 1), vol_ratio=1.0) for i in range(5)]
        res = _score(cands, vol_max_symbols=3)
        assert res["vol"] == ["S4-USDT-SWAP", "S3-USDT-SWAP", "S2-USDT-SWAP"]

    def test_fast_speed_reaches_list(self) -> None:
        # Quiet closed hours but moving fast this hour.
        res = _score([_cand("SPIKE-USDT-SWAP", rng=1.0, vol_ratio=1.0)],
                     history={"SPIKE-USDT-SWAP": _hist(6.0)})
        assert res["vol"] == ["SPIKE-USDT-SWAP"]
        cand = res["candidates"][0]
        assert cand["fast_speed"] == pytest.approx(6.0)
        assert cand["closed_speed"] == pytest.approx(0.25)

    def test_appended_after_sc_and_mr(self) -> None:
        cands = [
            _cand("SC-USDT-SWAP", rng=8.0, mom=10.0),
            _cand("MR-USDT-SWAP", rng=8.0, mom=0.5),
            _cand("VOLONLY-USDT-SWAP", rng=20.0, mom=8.0),
        ]
        res = _score(cands, mr_min_hl_range_pct=2.0, mr_max_momentum_pct=1.0)
        assert set(res["vol"]) >= {"VOLONLY-USDT-SWAP"}
        n = len(res["sc"]) + len([s for s in res["mr"] if s not in res["sc"]])
        assert res["selected"][:n] == list(dict.fromkeys(res["sc"] + res["mr"]))
        assert set(res["selected"]) == set(res["sc"]) | set(res["mr"]) | set(res["vol"])
        assert any("Screener VOL selected" in d for d in res["diagnostics"])

    def test_legacy_mode(self) -> None:
        res = _score([_cand("A-USDT-SWAP", rng=8.0), _cand("B-USDT-SWAP", rng=12.0)],
                     dual_universe=False, max_symbols=1)
        # Vol is built in legacy mode too (own gates, own cap) and fetched.
        assert res["vol"] == ["B-USDT-SWAP", "A-USDT-SWAP"]
        assert set(res["vol"]) <= set(res["selected"])
        assert res["sc"] == res["mr"]

    def test_disabled(self) -> None:
        res = _score([_cand("A-USDT-SWAP", rng=8.0)], vol_enabled=False)
        assert res["vol"] == [] and res["vol_pool_size"] == 0
        assert not any("VOL" in d for d in res["diagnostics"])


# ── Routing ───────────────────────────────────────────────────────────────────


class TestRouting:
    KW = dict(sc_symbols=["SC"], mr_symbols=["MR", "BOTH"],
              selected_symbols=["SC", "MR", "BOTH", "VOLONLY"],
              fallback_symbols=["FB"], vol_symbols=["VOLONLY", "BOTH"])

    def test_wr_gets_vol(self) -> None:
        assert universe_for_strategy("wick_rejection", **self.KW) == ["VOLONLY", "BOTH"]

    def test_wr_falls_back_to_selected(self) -> None:
        kw = dict(self.KW, vol_symbols=[])
        assert universe_for_strategy("wick_rejection", **kw) == ["SC", "MR", "BOTH", "VOLONLY"]

    def test_others_never_get_vol_only(self) -> None:
        assert universe_for_strategy("unknown", **self.KW) == ["SC", "MR", "BOTH"]
        legacy = universe_for_strategy("mean_reversion", dual_universe=False, **self.KW)
        assert "VOLONLY" not in legacy and "BOTH" in legacy
        assert universe_for_strategy("mean_reversion", **self.KW) == ["MR", "BOTH"]


def _service(dual: bool) -> MarketService:
    service = MarketService(
        state_service=None, enable_websocket=False,  # type: ignore[arg-type]
        trade_api=object(), account_api=object(), market_api=object(),
        public_api=object(), trading_api=object(), funding_api=object(),
    )
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(service, attr, None)
    service._screener_config = {"enabled": True, "dual_universe": dual}
    service._screener_sc_symbols = ["SC-USDT-SWAP"]
    service._screener_mr_symbols = ["MR-USDT-SWAP", "BOTH-USDT-SWAP"]
    service._screener_vol_symbols = ["VOLONLY-USDT-SWAP", "BOTH-USDT-SWAP"]
    service._screener_selected_symbols = [
        "SC-USDT-SWAP", "MR-USDT-SWAP", "BOTH-USDT-SWAP", "VOLONLY-USDT-SWAP",
    ]
    return service


class TestLiveRouting:
    @pytest.mark.parametrize("dual", [True, False])
    def test_allowed(self, dual: bool) -> None:
        s = _service(dual)
        allowed = s._strategy_allowed_on_symbol
        assert allowed("wick_rejection", "VOLONLY-USDT-SWAP")
        assert allowed("wick_rejection", "BOTH-USDT-SWAP")
        assert not allowed("wick_rejection", "SC-USDT-SWAP")
        assert not allowed("mean_reversion", "VOLONLY-USDT-SWAP")
        assert not allowed("spike_continuation", "VOLONLY-USDT-SWAP")
        assert allowed("mean_reversion", "BOTH-USDT-SWAP")
        assert s.get_screener_universe("wick_rejection") == [
            "VOLONLY-USDT-SWAP", "BOTH-USDT-SWAP",
        ]

    def test_wr_empty_vol_falls_back(self) -> None:
        s = _service(True)
        s._screener_vol_symbols = []
        assert s._strategy_allowed_on_symbol("wick_rejection", "SC-USDT-SWAP")


# ── Backtest universe ─────────────────────────────────────────────────────────


class _FakeFetcher:
    def __init__(self, candles: dict[str, list[Candle]]) -> None:
        self._candles = candles
        self.last_universe_provenance = {"source": "test"}

    async def fetch_swap_universe(self, *, force: bool = False) -> list[str]:
        return list(self._candles)

    async def fetch_candles(self, symbol: str, timeframe: str, start_ts: int, end_ts: int,
                            *, warmup_candles: int = 0, progress_cb: Any = None) -> list[Candle]:
        return [c for c in self._candles.get(symbol, []) if start_ts <= c.ts <= end_ts]


def _flat(start: int, count: int, spike_at: int | None = None) -> list[Candle]:
    out = []
    for i in range(count):
        hi, lo = (100.2, 99.8)
        close = 100.0
        if i == spike_at:
            hi, close = 115.0, 114.0
        out.append(Candle(ts=start + i * HOUR, open=100.0, high=hi, low=lo, close=close,
                          volume=10.0, quote_volume=2_000_000.0))
    return out


class TestBacktestUniverse:
    def test_vol_list_sees_only_closed_candles(self) -> None:
        start = 1_700_000_000_000 - (1_700_000_000_000 % HOUR)
        spike = 30
        fetcher = _FakeFetcher({
            "SPIKE-USDT-SWAP": _flat(start, 40, spike_at=spike),
            "FLAT-USDT-SWAP": _flat(start, 40),
        })
        cfg = dict(screener_defaults(), enabled=True, vol_enabled=True,
                   activity_lookback_hours=4, min_volume_usd=1.0)
        schedule = asyncio.run(build_universe_schedule(
            fetcher=fetcher,  # type: ignore[arg-type]
            start_ts=start + 25 * HOUR, end_ts=start + 36 * HOUR, screener_config=cfg,
        ))
        by_ts = {i.ts: i for i in schedule.intervals}
        spike_open = start + spike * HOUR
        # At the spike candle's open its high/close are still in the future.
        assert "SPIKE-USDT-SWAP" not in by_ts[spike_open].vol
        # One hour later the candle has closed.
        assert "SPIKE-USDT-SWAP" in by_ts[spike_open + HOUR].vol
        assert "FLAT-USDT-SWAP" not in by_ts[spike_open + HOUR].vol
        assert schedule.universe_at(spike_open + HOUR + 1, "wick_rejection") == by_ts[
            spike_open + HOUR
        ].vol
        d = schedule.to_dict()
        assert "vol" in d["intervals"][0]
        assert isinstance(d["provenance"]["vol_universe"], dict)
        assert "SPIKE-USDT-SWAP" in schedule.all_symbols

    def test_universe_at_routes_wr(self) -> None:
        sched = UniverseSchedule(intervals=[UniverseInterval(
            ts=0, sc=["A"], mr=["B"], selected=["A", "B", "V"], vol=["V"],
        )])
        assert sched.universe_at(1, "wick_rejection") == ["V"]
        assert "V" not in (sched.universe_at(1, "") or [])


def test_schedule_symbols_for_routes_like_the_step_gate() -> None:
    """The engine fetches only symbols an enabled strategy can be routed to."""
    from app.services.backtest.universe import UniverseInterval, UniverseSchedule

    sched = UniverseSchedule(intervals=[
        UniverseInterval(ts=1, sc=["S1"], mr=["M1"], vol=["V1", "S1"], selected=["S1", "M1", "V1"]),
        UniverseInterval(ts=2, sc=["S2"], mr=["M1"], vol=[], selected=["S2", "M1"]),
    ])
    assert sched.symbols_for(["wick_rejection"]) == ["V1", "S1", "S2", "M1"]  # no vol at ts=2 → selected
    assert sched.symbols_for(["mean_reversion"]) == ["M1"]
    assert sched.symbols_for(["spike_continuation", "vwap_reversion"]) == ["S1", "M1", "S2"]
    assert sched.symbols_for([]) == sched.all_symbols
