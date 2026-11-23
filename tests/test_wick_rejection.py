"""Tests for the hourly Wick Rejection short (``wick_rejection``).

Covers the §3.1 funnel stage by stage, the ``body_ratio`` / exit helpers, the
registries, and an end-to-end backtest snapshot (finer_ltf, 1m eval) to pin
that the strategy reads the forming 1H candle at the right moment.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.services.backtest.engine import available_strategy_names
from app.services.backtest.models import Candle
from app.services.backtest.snapshot_builder import SnapshotBuilder
from app.services.backtest.sweep_catalog import STRATEGY_SWEEP_PARAMS
from app.services.market_service import MarketService
from app.services.strategies import StrategyHelpers
from app.services.strategies.defaults import DEFAULT_STRATEGY_CONFIG, strategy_defaults
from app.services.strategies.wick_rejection import (
    WickRejectionStrategy,
    body_ratio,
    exit_price_pcts,
)

SYMBOL = "TEST-USDT-SWAP"
M = 60_000
H = 3_600_000
T0 = (1_700_000_000_000 // (24 * H)) * 24 * H


def _helpers(log: list[str] | None = None) -> StrategyHelpers:
    return StrategyHelpers(
        extract_float=MarketService._extract_float,
        emit_debug=(log.append if log is not None else (lambda msg: None)),
        get_last_price=lambda symbol: None,
    )


def _closed_hours(n: int = 12, *, big_at: int | None = None) -> list[dict[str, Any]]:
    """``n`` closed 1H candles, alternating small bodies of 0.2; optional big one."""
    out: list[dict[str, Any]] = []
    price = 100.0
    for i in range(n):
        o = price
        c = o + (0.2 if i % 2 == 0 else -0.2)
        if big_at is not None and i == big_at:
            c = o + 1.0  # 5× the 0.2 average body
        out.append({"ts": T0 + i * H, "open": o, "high": max(o, c) + 0.05,
                    "low": min(o, c) - 0.05, "close": c, "volume": 1.0})
        price = c
    return out


def _snapshot(
    *,
    closed: list[dict[str, Any]],
    forming: dict[str, Any] | None,
    seconds_to_close: float = 40.0,
    adx_htf: float | None = None,
    chop_htf: float | None = None,
) -> dict[str, Any]:
    f_ts = T0 + len(closed) * H
    block = {"ohlcv": closed, "forming_candle": forming,
             "adx_htf": adx_htf, "choppiness_htf": chop_htf}
    return {
        "as_of_ts": int(f_ts + H - seconds_to_close * 1000),
        "market_data": {SYMBOL: {"indicators": {}, "timeframes": {"1H": block}}},
    }


def _forming(closed: list[dict[str, Any]], *, open_: float | None = None,
             high: float = 103.0, low: float | None = None, price: float = 102.8) -> dict[str, Any]:
    o = closed[-1]["close"] if open_ is None else open_
    return {"ts": T0 + len(closed) * H, "open": o, "high": high,
            "low": o - 0.05 if low is None else low, "close": price, "volume": 1.0}


def _cfg(**over: Any) -> dict[str, Any]:
    cfg = {"enabled": True}
    cfg.update(over)
    return cfg


def _eval(snapshot: dict[str, Any], log: list[str] | None = None, **over: Any):
    return WickRejectionStrategy().evaluate(SYMBOL, snapshot, _cfg(**over), _helpers(log))


# ── Helpers ───────────────────────────────────────────────────────────────────


class TestBodyRatio:
    def test_matches_definition(self) -> None:
        candles = _closed_hours(9, big_at=8)
        # Prior 8 bodies all 0.2 → ratio 1.0 / 0.2.
        assert body_ratio(candles, 8, 8) == pytest.approx(5.0)

    def test_red_candle_is_zero(self) -> None:
        candles = _closed_hours(10)
        assert body_ratio(candles, 9, 8) == 0.0  # i=9 is a red candle

    def test_insufficient_history_is_none(self) -> None:
        assert body_ratio(_closed_hours(5), 4, 8) is None

    def test_zero_average_body_is_none(self) -> None:
        flat = [{"open": 1.0, "close": 1.0} for _ in range(8)] + [{"open": 1.0, "close": 2.0}]
        assert body_ratio(flat, 8, 8) is None


class TestExitPricePcts:
    def test_defaults_price_tp_margin_stop(self) -> None:
        tp, sl = exit_price_pcts(strategy_defaults("wick_rejection"))
        assert tp == pytest.approx(0.5)
        assert sl == pytest.approx(7.0)  # 35% margin at 5×

    def test_margin_tp_and_price_stop(self) -> None:
        cfg = dict(strategy_defaults("wick_rejection"), tp_unit="margin", tp_pct=2.5,
                   emergency_stop_unit="price", emergency_stop_pct=4.0)
        assert exit_price_pcts(cfg) == (pytest.approx(0.5), pytest.approx(4.0))

    def test_stop_off(self) -> None:
        cfg = dict(strategy_defaults("wick_rejection"), emergency_stop_mode="off")
        assert exit_price_pcts(cfg)[1] is None


# ── Funnel ────────────────────────────────────────────────────────────────────


class TestFunnel:
    def test_signal_fires_on_valid_setup(self) -> None:
        closed = _closed_hours(12, big_at=10)
        sig = _eval(_snapshot(closed=closed, forming=_forming(closed)), setup_include_forming=False)
        assert sig is not None
        assert sig.direction == "sell"
        assert sig.strategy_name == "wick_rejection"
        assert sig.tp_pct == pytest.approx(0.5)
        assert sig.sl_pct == pytest.approx(7.0)
        assert sig.rank_score is not None and sig.rank_score > 0
        assert "2h ago" in sig.rationale  # big candle i=10 of 12 closed

    def test_disabled(self) -> None:
        closed = _closed_hours(12, big_at=10)
        snap = _snapshot(closed=closed, forming=_forming(closed))
        assert WickRejectionStrategy().evaluate(SYMBOL, snap, {"enabled": False}, _helpers()) is None

    def test_no_forming_candle(self) -> None:
        closed = _closed_hours(12, big_at=10)
        assert _eval(_snapshot(closed=closed, forming=None)) is None

    def test_missing_as_of_ts(self) -> None:
        closed = _closed_hours(12, big_at=10)
        snap = _snapshot(closed=closed, forming=_forming(closed))
        del snap["as_of_ts"]
        assert _eval(snap) is None

    @pytest.mark.parametrize("secs,fires", [(5, False), (10, True), (90, True), (91, False), (1800, False)])
    def test_entry_window(self, secs: float, fires: bool) -> None:
        closed = _closed_hours(12, big_at=10)
        sig = _eval(_snapshot(closed=closed, forming=_forming(closed), seconds_to_close=secs))
        assert (sig is not None) is fires

    def test_forming_red_vetoes(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        forming = _forming(closed, open_=o, high=o + 0.1, low=o - 3.0, price=o - 0.05)
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=forming), log) is None
        assert any("not green" in m for m in log)

    def test_forming_red_allowed_when_opted_out(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        # Red candle (price < open) but price near the high of a wide range.
        forming = _forming(closed, open_=o, high=o + 0.1, low=o - 3.0, price=o - 0.05)
        assert _eval(_snapshot(closed=closed, forming=forming), require_forming_green=False) is not None

    def test_small_range_vetoes(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        forming = _forming(closed, high=o + 0.5, price=o + 0.45)  # ~0.5% range
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=forming), log) is None
        assert any("veto range" in m for m in log)

    def test_zero_range_vetoes(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        forming = _forming(closed, open_=o, high=o, low=o, price=o)
        log: list[str] = []
        snap = _snapshot(closed=closed, forming=forming)
        assert _eval(snap, log, require_forming_green=False, min_candle_range_pct=0.0) is None
        assert any("zero range" in m for m in log)

    def test_high_offset_vetoes(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        # Range o-0.05 .. o+3; price at the middle → offset ≈ 50%.
        forming = _forming(closed, high=o + 3.0, price=o + 1.5)
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=forming), log) is None
        assert any("high offset" in m for m in log)

    def test_price_above_recorded_high_counts_as_new_high(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        forming = _forming(closed, high=o + 2.0, price=o + 2.5)
        assert _eval(_snapshot(closed=closed, forming=forming)) is not None

    def test_no_big_body_vetoes(self) -> None:
        closed = _closed_hours(12)
        # A forming candle with a modest body (0.4 = 2×) — below 3×.
        o = closed[-1]["close"]
        forming = _forming(closed, low=o - 2.0, high=o + 0.42, price=o + 0.4)
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=forming), log) is None
        assert any("veto setup" in m for m in log)

    def test_big_body_outside_lookback_vetoes(self) -> None:
        closed = _closed_hours(16, big_at=10)  # 6h ago, lookback is 4
        o = closed[-1]["close"]
        forming = _forming(closed, low=o - 2.0, high=o + 0.42, price=o + 0.4)
        assert _eval(_snapshot(closed=closed, forming=forming)) is None
        assert _eval(_snapshot(closed=closed, forming=forming), setup_lookback_candles=6) is not None

    def test_forming_big_body_counts(self) -> None:
        closed = _closed_hours(12)
        sig = _eval(_snapshot(closed=closed, forming=_forming(closed)))  # body ≈ 2.8 → 14×
        assert sig is not None
        assert "(forming)" in sig.rationale
        assert _eval(_snapshot(closed=closed, forming=_forming(closed)), setup_include_forming=False) is None

    def test_htf_regime_opt_in(self) -> None:
        closed = _closed_hours(12, big_at=10)
        snap = _snapshot(closed=closed, forming=_forming(closed), adx_htf=40.0, chop_htf=30.0)
        assert _eval(snap) is not None  # "off" by default
        assert _eval(snap, htf_regime_preference="chop") is None

    def test_rank_score_prefers_bigger_body_and_closer_to_high(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        at_high = _eval(_snapshot(closed=closed, forming=_forming(closed, high=o + 3.0, price=o + 3.0)))
        off_high = _eval(_snapshot(closed=closed, forming=_forming(closed, high=o + 3.0, price=o + 2.6)))
        assert at_high is not None and off_high is not None
        assert at_high.rank_score > off_high.rank_score


# ── Registries ────────────────────────────────────────────────────────────────


class TestRegistration:
    def test_defaults_registered(self) -> None:
        d = DEFAULT_STRATEGY_CONFIG["wick_rejection"]
        assert d["enabled"] is False
        assert d["analysis_timeframe"] == "1H"
        assert d["use_global_trade_management"] is False

    def test_backtest_registry(self) -> None:
        assert "wick_rejection" in available_strategy_names()

    def test_sweep_keys_exist_in_defaults(self) -> None:
        d = DEFAULT_STRATEGY_CONFIG["wick_rejection"]
        assert set(STRATEGY_SWEEP_PARAMS["wick_rejection"]) <= set(d)

    def test_live_registry(self) -> None:
        import inspect

        src = inspect.getsource(MarketService.__init__)
        assert "WickRejectionStrategy()" in src


# ── Backtest end-to-end (finer_ltf snapshot) ──────────────────────────────────


def _minute_series(hours: int) -> list[Candle]:
    """Quiet hours (±0.2 bodies), a big green hour at ``hours-2``, and a last
    hour that rises to 105 by minute 58 and dumps at minute 59."""
    path: list[float] = []
    price = 100.0
    for h in range(hours):
        if h == hours - 2:
            target = price + 1.5
        elif h == hours - 1:
            target = price + 2.0
        else:
            target = price + (0.2 if h % 2 == 0 else -0.2)
        start = price
        for m in range(60):
            if h == hours - 1 and m == 59:
                path.append(start - 1.0)  # the future dump
            else:
                path.append(start + (target - start) * (m + 1) / (59 if h == hours - 1 else 60))
        price = path[-1] if h != hours - 1 else target
    candles: list[Candle] = []
    prev = 100.0
    for i, c in enumerate(path):
        candles.append(Candle(ts=T0 + i * M, open=prev, high=max(prev, c) + 0.01,
                              low=min(prev, c) - 0.01, close=c, volume=1.0))
        prev = c
    return candles


def _aggregate(candles: list[Candle], bar_ms: int) -> list[Candle]:
    buckets: dict[int, list[Candle]] = {}
    for c in candles:
        buckets.setdefault(c.ts - (c.ts - T0) % bar_ms, []).append(c)
    return [
        Candle(ts=ts, open=g[0].open, high=max(c.high for c in g), low=min(c.low for c in g),
               close=g[-1].close, volume=sum(c.volume for c in g))
        for ts, g in sorted(buckets.items())
    ]


class TestBacktestSnapshot:
    HOURS = 14

    @pytest.fixture(scope="class")
    def setup(self) -> tuple[SnapshotBuilder, list[Candle], list[Candle]]:
        m1 = _minute_series(self.HOURS)
        m15 = _aggregate(m1, 15 * M)
        h1 = _aggregate(m1, H)
        builder = SnapshotBuilder(
            SYMBOL, m15, _aggregate(m1, 4 * H), "15m",
            tf_candles={"1H": h1}, eval_candles=m1, eval_timeframe="1m",
        )
        return builder, m1, m15

    def _snap(self, setup, current_ts: int) -> dict[str, Any]:
        builder, m1, m15 = setup
        bucket = current_ts - (current_ts - T0) % (15 * M)
        part = [c for c in m1 if bucket <= c.ts <= current_ts]
        incomplete = Candle(ts=bucket, open=part[0].open, high=max(c.high for c in part),
                            low=min(c.low for c in part), close=part[-1].close,
                            volume=sum(c.volume for c in part))
        return builder.build_with_incomplete_ltf([c for c in m15 if c.ts < bucket], incomplete, current_ts)

    def test_fires_at_minute_58_without_seeing_the_dump(self, setup) -> None:
        last_hour = T0 + (self.HOURS - 1) * H
        snap = self._snap(setup, last_hour + 58 * M)
        forming = snap["market_data"][SYMBOL]["timeframes"]["1H"]["forming_candle"]
        assert forming["close"] == pytest.approx(forming["high"] - 0.01, abs=0.02)
        sig = WickRejectionStrategy().evaluate(SYMBOL, snap, _cfg(), _helpers())
        assert sig is not None
        assert "60s to close" in sig.rationale

    def test_silent_mid_hour(self, setup) -> None:
        last_hour = T0 + (self.HOURS - 1) * H
        snap = self._snap(setup, last_hour + 30 * M)
        assert WickRejectionStrategy().evaluate(SYMBOL, snap, _cfg(), _helpers()) is None


# ── Ranking and entry caps (shared by the live trigger and the backtest) ──────


def test_select_ranked_entries_ranks_and_caps() -> None:
    from app.services.strategies.wick_rejection import select_ranked_entries

    cands = [("a", 1.0, 20.0), ("b", 3.0, 20.0), ("c", 2.0, 20.0), ("d", 2.0, 20.0)]
    keep, drop = select_ranked_entries(
        cands, open_positions=1, open_margin=20.0, equity=1000.0,
        max_concurrent_positions=3, max_total_margin_pct=100.0,
    )
    assert keep == ["b", "c"]  # ties keep input order
    assert set(drop) == {"d", "a"} and "max_concurrent_positions" in drop["d"]

    keep, drop = select_ranked_entries(
        cands, open_positions=0, open_margin=10.0, equity=100.0,
        max_concurrent_positions=0, max_total_margin_pct=50.0,
    )
    assert keep == ["b", "c"] and "max_total_margin_pct" in drop["d"]

    keep, _ = select_ranked_entries(
        cands, open_positions=5, open_margin=0.0, equity=None,
        max_concurrent_positions=None, max_total_margin_pct=50.0,
    )
    assert keep == ["b", "c", "d", "a"]  # no caps: everything, ranked
