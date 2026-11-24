"""wick_rejection vol list: refresh before the entry and the thin-coin floor.

Covers the shared screener pieces (``compute_recent_activity(include_forming)``,
``vol_volume_floor`` / ``vol_only`` candidates, ``activity_fetch_symbols``),
the refresh timing shared by trigger and backtest, the backtest's partial-hour
rescoring (no data after the cut), the trigger order (refresh, then entry) and
``MarketService.refresh_wr_vol_list`` (stubbed APIs only).
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.backtest.models import Candle
from app.services.backtest.universe import _partial_hours, build_universe_schedule
from app.services.market_service import MarketService
from app.services.screener import (
    activity_fetch_symbols,
    build_candidates,
    compute_recent_activity,
    merge_candidates,
    score_universe,
    vol_volume_floor,
)
from app.services.strategies.defaults import screener_defaults, strategy_defaults
from app.services.strategies.wick_rejection import (
    backtest_vol_refresh_cut_seconds,
    vol_refresh_lead_seconds,
)
from app.services.wick_rejection_trigger import WickRejectionTrigger

HOUR = 3_600_000
MIN = 60_000
WR = strategy_defaults("wick_rejection")


def _cfg(**over: Any) -> dict[str, Any]:
    cfg = dict(screener_defaults(), enabled=True, vol_enabled=True, activity_lookback_hours=4)
    cfg.update(over)
    return cfg


def _ticker(sym: str, quote_vol: float, last: float = 100.0) -> dict[str, Any]:
    # Backtest-style ticker (volCcy24h already quote volume).
    return {"instId": sym, "last": last, "open24h": 100.0, "high24h": 101.0,
            "low24h": 99.0, "volCcy24h": quote_vol}


# ── Shared screener pieces ────────────────────────────────────────────────────


class TestActivityIncludeForming:
    ROWS = [
        [0, 100, 100.2, 99.8, 100, 1, 0, 1_000, "1"],
        [HOUR, 100, 100.2, 99.8, 100, 1, 0, 1_000, "1"],
        [2 * HOUR, 100, 114, 100, 113, 1, 0, 3_000, "0"],  # forming
    ]

    def test_forming_dropped_by_default(self) -> None:
        act = compute_recent_activity(self.ROWS, lookback_hours=4)
        assert act["candle_count"] == 2
        assert act["recent_range_pct"] == pytest.approx(0.4)

    def test_forming_kept_when_asked(self) -> None:
        act = compute_recent_activity(self.ROWS, lookback_hours=4, include_forming=True)
        assert act["candle_count"] == 3
        assert act["recent_range_pct"] == pytest.approx((114 - 99.8) / 113 * 100)
        assert act["recent_vol"] == 5_000


class TestVolFloor:
    def test_floor(self) -> None:
        assert vol_volume_floor(_cfg(min_volume_usd=10e6)) == 10e6
        assert vol_volume_floor(_cfg(min_volume_usd=10e6, vol_min_volume_usd=5e5)) == 5e5
        # Never raises the main floor; ignored with the vol list off.
        assert vol_volume_floor(_cfg(min_volume_usd=10e6, vol_min_volume_usd=20e6)) == 10e6
        assert vol_volume_floor(_cfg(min_volume_usd=10e6, vol_min_volume_usd=5e5,
                                     vol_enabled=False)) == 10e6

    def test_thin_coins_are_vol_only(self) -> None:
        cfg = _cfg(min_volume_usd=10e6, vol_min_volume_usd=5e5)
        cands = build_candidates([_ticker("BIG-USDT-SWAP", 20e6), _ticker("THIN-USDT-SWAP", 1e6), _ticker("DUST-USDT-SWAP", 1e5)],
                                 cfg, vol_ccy_is_quote=True)
        by = {c["symbol"].split("-")[0]: c for c in cands}
        assert set(by) == {"BIG", "THIN"}
        assert by["BIG"]["vol_only"] is False and by["THIN"]["vol_only"] is True
        assert [c["symbol"] for c in build_candidates(
            [_ticker("BIG-USDT-SWAP", 20e6), _ticker("THIN-USDT-SWAP", 1e6)], _cfg(min_volume_usd=10e6),
            vol_ccy_is_quote=True)] == ["BIG-USDT-SWAP"]

    def test_vol_only_ranks_for_vol_list_but_never_sc_mr(self) -> None:
        cfg = _cfg(min_volume_usd=10e6, vol_min_volume_usd=5e5, sc_min_momentum_pct=0.0,
                   sc_min_hl_range_pct=0.0, mr_min_hl_range_pct=0.0, mr_max_momentum_pct=100.0,
                   activity_min_ratio=0.0)
        act = {"recent_vol": 1.0, "recent_vol_ratio": 2.0, "recent_range_pct": 12.0,
               "recent_mom_pct": 10.0, "decay_ratio": 1.0, "hours_covered": 4.0, "candle_count": 4}
        tickers = [_ticker("BIG-USDT-SWAP", 20e6), _ticker("THIN-USDT-SWAP", 1e6)]
        res = score_universe(tickers, cfg, activity={"BIG-USDT-SWAP": dict(act, recent_range_pct=8.0),
                                                     "THIN-USDT-SWAP": act},
                             vol_ccy_is_quote=True, now_ts=0.0)
        assert "THIN-USDT-SWAP" in res["vol"]
        assert "THIN-USDT-SWAP" not in res["sc"] + res["mr"]
        assert "BIG-USDT-SWAP" in res["sc"]
        assert res["base_candidate_count"] == 1 and res["vol_only_count"] == 1

    def test_fast_lane_admission_clears_vol_only(self) -> None:
        merged = merge_candidates([{"symbol": "THIN", "vol_only": True}],
                                  [{"symbol": "THIN", "fast_move_pct": 9.0, "_fast_lane": True}])
        assert merged[0]["vol_only"] is False and merged[0]["_fast_lane"] is True


class TestActivityFetchSymbols:
    NOW = 1_700_000_000.0

    def _hist(self, move: float) -> deque:
        n = self.NOW
        return deque([(n - 3600, 100.0), (n - 1800, 100 + move / 2), (n, 100 + move)])

    def test_selection(self) -> None:
        cands = [
            {"symbol": "BIG1", "vol_ccy_24h": 50e6}, {"symbol": "BIG2", "vol_ccy_24h": 40e6},
            {"symbol": "BIG3", "vol_ccy_24h": 30e6},
            {"symbol": "LANE", "vol_ccy_24h": 1e6, "_fast_lane": True},
            {"symbol": "T_FAST", "vol_ccy_24h": 1e6, "vol_only": True},
            {"symbol": "T_SLOW", "vol_ccy_24h": 2e6, "vol_only": True},
            {"symbol": "T_NOHIST", "vol_ccy_24h": 3e6, "vol_only": True},
        ]
        hist = {"T_FAST": self._hist(8.0), "T_SLOW": self._hist(1.0)}
        cfg = _cfg(activity_prefilter_top_n=2, vol_activity_top_n=1)
        out = activity_fetch_symbols(cands, cfg, price_history=hist, now_ts=self.NOW)
        # Top-2 by volume (vol_only excluded), the lane, the fastest thin coin.
        assert out == ["BIG1", "BIG2", "LANE", "T_FAST"]
        cfg["vol_activity_top_n"] = 5
        assert activity_fetch_symbols(cands, cfg, price_history=hist, now_ts=self.NOW)[-2:] == [
            "T_FAST", "T_SLOW"]
        cfg["vol_enabled"] = False
        assert "T_FAST" not in activity_fetch_symbols(cands, cfg, price_history=hist, now_ts=self.NOW)


# ── Timing ────────────────────────────────────────────────────────────────────


class TestRefreshTiming:
    def test_defaults(self) -> None:
        assert vol_refresh_lead_seconds(WR) == 90
        assert backtest_vol_refresh_cut_seconds(WR) == 120  # H:58:00 for a H:58:30 refresh

    def test_at_least_20s_before_wake(self) -> None:
        assert vol_refresh_lead_seconds({**WR, "vol_refresh_seconds_before_close": 30}) == 60
        mom = {**WR, "entry_mode": "momentum"}
        assert vol_refresh_lead_seconds(mom) == 320
        assert backtest_vol_refresh_cut_seconds(mom) == 360

    def test_off(self) -> None:
        off = {**WR, "vol_refresh_seconds_before_close": 0}
        assert vol_refresh_lead_seconds(off) is None
        assert backtest_vol_refresh_cut_seconds(off) is None

    def test_cut_never_after_live_refresh(self) -> None:
        for s in (61, 90, 119, 120, 150):
            cfg = {**WR, "vol_refresh_seconds_before_close": s}
            assert backtest_vol_refresh_cut_seconds(cfg) >= vol_refresh_lead_seconds(cfg)


# ── Backtest universe ─────────────────────────────────────────────────────────


class _TfFetcher:
    def __init__(self, data: dict[tuple[str, str], list[Candle]]) -> None:
        self._data = data
        self.last_universe_provenance = {"source": "test"}
        self.calls: list[tuple[str, str, int]] = []

    async def fetch_swap_universe(self, *, force: bool = False) -> list[str]:
        return sorted({s for s, _ in self._data})

    async def fetch_candles(self, symbol: str, timeframe: str, start_ts: int, end_ts: int,
                            *, warmup_candles: int = 0, progress_cb: Any = None) -> list[Candle]:
        self.calls.append((symbol, timeframe, warmup_candles))
        return [c for c in self._data.get((symbol, timeframe), []) if start_ts <= c.ts <= end_ts]


def _minutes(start: int, hours: int, pump_hour: int | None, pump_from: int, pump_to: int) -> list[Candle]:
    """1m candles at 100 (±0.2); during ``pump_hour`` minutes [pump_from, pump_to)
    trade at 114."""
    out = []
    for i in range(hours * 60):
        ts = start + i * MIN
        h, m = divmod(i, 60)
        px = 114.0 if h == pump_hour and pump_from <= m < pump_to else 100.0
        out.append(Candle(ts=ts, open=px, high=px + 0.2, low=px - 0.2, close=px,
                          volume=1.0, quote_volume=2_000_000.0 / 60))
    return out


def _hours(minutes: list[Candle]) -> list[Candle]:
    by: dict[int, list[Candle]] = {}
    for c in minutes:
        by.setdefault(c.ts - c.ts % HOUR, []).append(c)
    return [Candle(ts=h, open=cs[0].open, high=max(c.high for c in cs), low=min(c.low for c in cs),
                   close=cs[-1].close, volume=sum(c.volume for c in cs),
                   quote_volume=sum(c.quote_volume for c in cs)) for h, cs in sorted(by.items())]


class TestBacktestRefresh:
    START = 1_700_000_000_000 - (1_700_000_000_000 % HOUR)
    SPIKE = 30

    def _run(self, cut: int | None) -> Any:
        s = self.START
        early = _minutes(s, 40, self.SPIKE, 10, 60)   # pumps from minute 10
        late = _minutes(s, 40, self.SPIKE, 59, 60)    # only the last minute
        flat = _minutes(s, 40, None, 0, 0)
        data: dict[tuple[str, str], list[Candle]] = {}
        for sym, m in (("EARLY-USDT-SWAP", early), ("LATE-USDT-SWAP", late), ("FLAT-USDT-SWAP", flat)):
            data[(sym, "1m")] = m
            data[(sym, "1H")] = _hours(m)
        fetcher = _TfFetcher(data)
        sched = asyncio.run(build_universe_schedule(
            fetcher=fetcher,  # type: ignore[arg-type]
            start_ts=s + 25 * HOUR, end_ts=s + 36 * HOUR,
            screener_config=_cfg(min_volume_usd=1.0),
            vol_refresh_seconds_before_close=cut, refresh_warmup_candles=200,
        ))
        return sched, fetcher

    def test_pump_hour_listed_before_its_close(self) -> None:
        spike_open = self.START + self.SPIKE * HOUR
        closed, _ = self._run(None)
        refreshed, fetcher = self._run(120)
        c = {i.ts: i.vol for i in closed.intervals}
        r = {i.ts: i.vol for i in refreshed.intervals}
        e, la, f = "EARLY-USDT-SWAP", "LATE-USDT-SWAP", "FLAT-USDT-SWAP"
        assert e not in c[spike_open]
        assert e in r[spike_open]
        # The last-minute pump is after the cut (H:58:00): not visible.
        assert la not in r[spike_open]
        # Next hour both pumps are closed history.
        assert {e, la} <= set(r[spike_open + HOUR])
        assert f not in r[spike_open]
        # 1m fetched with the engine's warmup so the cache is shared.
        assert (e, "1m", 200) in fetcher.calls
        assert refreshed.provenance["vol_universe"]["refresh_seconds_before_close"] == 120
        assert any("Universe vol refresh" in d for d in refreshed.diagnostics)

    def test_partial_hours_cut(self) -> None:
        m = _minutes(self.START, 1, 0, 58, 60)
        p = _partial_hours(m, cut_offset_ms=58 * MIN, step_ms=MIN)[self.START]
        assert p.close == 100.0 and p.high == pytest.approx(100.2)  # minutes 58-59 excluded
        assert p.quote_volume == pytest.approx(58 * 2_000_000.0 / 60)


# ── Trigger: refresh, then entry ─────────────────────────────────────────────


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _MS:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.events: list[tuple[str, float]] = []
        self._launcher_in_position: dict[str, Any] = {}
        self._last_full_snapshot = {"account": {"total_eq_usd": 100.0}}

    async def refresh_wr_vol_list(self) -> dict[str, Any]:
        self.events.append(("refresh", self.clock.now % 3600))
        self.clock.now += 5.0  # the refresh takes 5 s
        return {"symbols": ["A-USDT-SWAP"], "added": ["A-USDT-SWAP"], "removed": [], "seconds": 5.0}

    def get_screener_universe(self, name: str) -> list[str]:
        self.events.append(("entry", self.clock.now % 3600))
        return []

    def _emit_debug(self, msg: str) -> None:
        pass


def _loop_once(cfg: dict[str, Any], start: float) -> _MS:
    clock = _Clock(start)
    ms = _MS(clock)
    rc = {"launcher": {"mode": "launcher_only", "strategies": {"wick_rejection": cfg}}}
    app = SimpleNamespace(state=SimpleNamespace(runtime_config=rc, market_service=ms))

    async def sleep(d: float) -> None:
        if any(e == "entry" for e, _ in ms.events):
            raise asyncio.CancelledError
        clock.now += d

    trig = WickRejectionTrigger(app, asyncio.Lock(), clock=clock, sleep=sleep)

    async def go() -> None:
        try:
            await trig._run()
        except asyncio.CancelledError:
            pass

    asyncio.run(go())
    ms.trigger = trig  # type: ignore[attr-defined]
    return ms


class TestTriggerOrder:
    H0 = 1_700_000_000 - (1_700_000_000 % 3600)

    def test_refresh_then_entry(self) -> None:
        ms = _loop_once({**WR, "enabled": True}, self.H0 + 100)
        assert ms.events == [("refresh", 3600 - 90), ("entry", 3600 - 40)]
        assert ms.trigger.last_vol_refresh["symbols"] == ["A-USDT-SWAP"]  # type: ignore[attr-defined]

    def test_no_refresh_when_off(self) -> None:
        ms = _loop_once({**WR, "enabled": True, "vol_refresh_seconds_before_close": 0}, self.H0 + 100)
        assert ms.events == [("entry", 3600 - 40)]

    def test_started_after_refresh_moment_skips_it(self) -> None:
        ms = _loop_once({**WR, "enabled": True}, self.H0 + 3600 - 60)
        assert ms.events == [("entry", 3600 - 40)]


# ── Live refresh (stubbed APIs) ──────────────────────────────────────────────


def _service() -> MarketService:
    service = MarketService(
        state_service=None, enable_websocket=False,  # type: ignore[arg-type]
        trade_api=object(), account_api=object(), market_api=object(),
        public_api=object(), trading_api=object(), funding_api=object(),
    )
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(service, attr, None)
    return service


class TestLiveRefresh:
    def _setup(self) -> tuple[MarketService, list[list[str]]]:
        s = _service()
        s._screener_config = _cfg(min_volume_usd=1e6, vol_min_volume_usd=5e5,
                                  activity_cache_seconds=300)
        s._screener_sc_symbols = ["SC-USDT-SWAP"]
        s._screener_mr_symbols = []
        s._screener_vol_symbols = []
        s._screener_selected_symbols = ["SC-USDT-SWAP"]
        s.symbols = ["SC-USDT-SWAP"]
        now = time.time()
        hour = int(now // 3600) * 3600 * 1000
        # Live tickers: volCcy24h in base units (× last = quote).
        tickers = [
            {"instId": "PUMP-USDT-SWAP", "last": "113", "open24h": "100", "high24h": "114",
             "low24h": "99.8", "volCcy24h": str(5e6 / 113)},
            {"instId": "THIN-USDT-SWAP", "last": "113", "open24h": "100", "high24h": "114",
             "low24h": "99.8", "volCcy24h": str(7e5 / 113)},
            {"instId": "FLAT-USDT-SWAP", "last": "100", "open24h": "100", "high24h": "100.2",
             "low24h": "99.8", "volCcy24h": str(5e6 / 100)},
        ]

        def rows(sym: str) -> list[list[Any]]:
            q = (5e6 if sym != "THIN-USDT-SWAP" else 7e5) / 24
            closed = [[hour - k * HOUR, 100, 100.2, 99.8, 100, 1, 0, q, "1"] for k in (4, 3, 2, 1)]
            if sym == "FLAT-USDT-SWAP":
                forming = [hour, 100, 100.2, 99.8, 100, 1, 0, q, "0"]
            else:
                forming = [hour, 100, 114, 100, 113, 1, 0, q * 3, "0"]
            return [forming, *reversed(closed)]

        async def fetch_tickers() -> list[dict[str, Any]]:
            return tickers

        async def fetch_bar(symbol: str, bar: str, cache: dict, limit: int = 100, **_: Any):
            return rows(symbol)

        updates: list[list[str]] = []

        async def update_symbols(symbols: list[str]) -> None:
            updates.append(list(symbols))
            s.symbols = list(symbols)

        s._fetch_all_swap_tickers = fetch_tickers  # type: ignore[method-assign]
        s._fetch_ohlcv_bar = fetch_bar  # type: ignore[method-assign]
        s.update_symbols = update_symbols  # type: ignore[method-assign]
        # Price samples so the thin coin ranks for activity (fast speed).
        t = time.time()
        s._screener_price_history["THIN-USDT-SWAP"] = deque(
            [(t - 3600, 100.0), (t - 1800, 105.0), (t - 1, 113.0)], maxlen=64)
        return s, updates

    def test_refresh_lists_pump_in_progress_and_keeps_it_for_the_hour(self) -> None:
        s, updates = self._setup()
        out = asyncio.run(s.refresh_wr_vol_list())
        assert out is not None
        assert set(out["symbols"]) == {"PUMP-USDT-SWAP", "THIN-USDT-SWAP"}
        assert s._screener_vol_symbols == out["symbols"]
        assert s._strategy_allowed_on_symbol("wick_rejection", "PUMP-USDT-SWAP")
        # New coins get a snapshot; SC/MR untouched; thin coin never in SC/MR.
        assert updates and set(updates[-1]) == {"SC-USDT-SWAP", *out["symbols"]}
        assert s._screener_sc_symbols == ["SC-USDT-SWAP"]
        # The hourly screener (closed candles only) does not replace it this hour.
        asyncio.run(s.run_screener_if_due(force=True))
        assert set(s._screener_vol_symbols) == set(out["symbols"])
        assert "THIN-USDT-SWAP" not in s._screener_sc_symbols + s._screener_mr_symbols
        # Next hour it does.
        s._wr_vol_refresh["hour_ts"] -= 3600  # type: ignore[index]
        asyncio.run(s.run_screener_if_due(force=True))
        assert "PUMP-USDT-SWAP" not in s._screener_vol_symbols

    def test_off_without_vol_list(self) -> None:
        s, _ = self._setup()
        s._screener_config["vol_enabled"] = False
        assert asyncio.run(s.refresh_wr_vol_list()) is None
