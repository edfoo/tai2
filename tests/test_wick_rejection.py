"""Tests for the hourly Wick Rejection short (``wick_rejection``).

Covers the §3.1 funnel stage by stage, the ``body_ratio`` / exit helpers, the
registries, and an end-to-end backtest snapshot (finer_ltf, 1m eval) to pin
that the strategy reads the forming 1H candle at the right moment.
"""

from __future__ import annotations

import statistics
from typing import Any

import pytest

from app.services.backtest.engine import available_strategy_names
from app.services.backtest.models import Candle
from app.services.backtest.snapshot_builder import SnapshotBuilder
from app.services.backtest.sweep_catalog import STRATEGY_SWEEP_PARAMS
from app.services.market_service import MarketService
from app.services.strategies import StrategyHelpers
from app.services.strategies.defaults import DEFAULT_STRATEGY_CONFIG, WR_WAKE_PRESET, strategy_defaults
from app.services.strategies.wick_rejection import (
    WakeProfile,
    WickRejectionStrategy,
    attach_wr_inputs,
    avg_1m_range_pct,
    body_ratio,
    candles_1m_to_dicts,
    exit_price_pcts,
    prior_rise_pct,
    wake_profile,
    wake_profile_fails,
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


class TestPriorRisePct:
    def test_definition(self) -> None:
        closed = [{"close": c} for c in (100.0, 50.0, 60.0, 55.0)]
        assert prior_rise_pct(closed, 2) == pytest.approx(10.0)  # 55 / 50
        assert prior_rise_pct(closed, 3) == pytest.approx(-45.0)

    def test_insufficient_history(self) -> None:
        assert prior_rise_pct([{"close": 1.0}] * 3, 3) is None
        assert prior_rise_pct([{"close": 1.0}] * 3, 0) is None


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

    def test_prior_rise_off_by_default(self) -> None:
        closed = _closed_hours(30, big_at=28)
        closed[-25]["close"] = 50.0  # +100% over the prior 24h
        assert _eval(_snapshot(closed=closed, forming=_forming(closed))) is not None

    def test_prior_rise_gate(self) -> None:
        closed = _closed_hours(30, big_at=28)
        snap = _snapshot(closed=closed, forming=_forming(closed))
        assert _eval(snap, max_prior_rise_pct=8.0) is not None  # ~1% rise
        ref = closed[-1]["close"] / 1.10  # last closed close is +10% on the close 24h before
        closed[-25]["close"] = ref
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=_forming(closed)), log, max_prior_rise_pct=8.0) is None
        assert any("prior 24h rise 10.0% > 8%" in m for m in log)
        assert _eval(_snapshot(closed=closed, forming=_forming(closed)), max_prior_rise_pct=12.0) is not None
        # A shorter window no longer reaches back to the low reference close.
        assert _eval(_snapshot(closed=closed, forming=_forming(closed)),
                     max_prior_rise_pct=8.0, prior_rise_hours=12) is not None

    def test_prior_rise_unknown_history_vetoes(self) -> None:
        closed = _closed_hours(20, big_at=18)
        log: list[str] = []
        assert _eval(_snapshot(closed=closed, forming=_forming(closed)), log, max_prior_rise_pct=8.0) is None
        assert any("prior rise unknown" in m for m in log)

    def test_rank_score_prefers_bigger_body_and_closer_to_high(self) -> None:
        closed = _closed_hours(12, big_at=10)
        o = closed[-1]["close"]
        at_high = _eval(_snapshot(closed=closed, forming=_forming(closed, high=o + 3.0, price=o + 3.0)))
        off_high = _eval(_snapshot(closed=closed, forming=_forming(closed, high=o + 3.0, price=o + 2.6)))
        assert at_high is not None and off_high is not None
        assert at_high.rank_score > off_high.rank_score


# ── Registries ────────────────────────────────────────────────────────────────


# ── Quiet-coin wake profile ───────────────────────────────────────────────────

CT = 10.0  # contract size: candle volume is in contracts


def _wake_hours(n: int = 60, *, base_vol: float = 100.0, recent_vol: float = 500.0) -> list[dict[str, Any]]:
    """Quiet hours (0.5% range, ``base_vol`` contracts ≈ 100k USDT/h at CT 10),
    then 3 busier closed hours of ``recent_vol``."""
    out: list[dict[str, Any]] = []
    for i in range(n):
        o = 100.0
        c = o + (0.2 if i % 2 == 0 else -0.2)
        out.append({"ts": T0 + i * H, "open": o, "high": max(o, c) + 0.05,
                    "low": min(o, c) - 0.05, "close": c,
                    "volume": recent_vol if i >= n - 3 else base_vol})
    return out


def _wake_forming(closed: list[dict[str, Any]], *, open_: float = 100.0, high: float = 104.0,
                  price: float = 103.0, volume: float = 1000.0) -> dict[str, Any]:
    return {"ts": T0 + len(closed) * H, "open": open_, "high": high, "low": open_ - 0.05,
            "close": price, "volume": volume}


def _minutes_1m(f_ts: int, *, rng: float = 0.006, upto: int = 59) -> list[dict[str, Any]]:
    """1m candles H:40 … H:``upto`` of the forming hour, each ``rng`` wide."""
    return [{"ts": f_ts + k * M, "open": 103.0, "high": 103.0 * (1 + rng), "low": 103.0,
             "close": 103.0} for k in range(40, upto + 1)]


def _wake_snap(closed, forming, *, ct: float | None = CT, candles=None, seconds_to_close: float = 40.0):
    snap = _snapshot(closed=closed, forming=forming, seconds_to_close=seconds_to_close)
    if ct is None and candles is None:
        return snap
    c1m = _minutes_1m(forming["ts"]) if candles is None else candles
    return attach_wr_inputs(snap, SYMBOL, ct_val=ct, candles_1m=c1m)


_PRESET = {"wake_profile_enabled": True, "min_big_bodies": 0, "max_high_offset_pct": 100.0}


class TestWakeHelpers:
    def test_avg_1m_range_reads_closed_minutes_before_the_current_one(self) -> None:
        f_ts = T0 + 60 * H
        candles = _minutes_1m(f_ts, rng=0.01)
        candles[-1]["high"] = 200.0  # H:59, still forming: never read
        candles[4]["high"] = 200.0   # H:44, the first one read
        live = avg_1m_range_pct(candles, f_ts + 59 * M + 20_000)  # trigger at H:59:20
        bt = avg_1m_range_pct(candles, f_ts + 59 * M)             # backtest step at H:59:00
        assert live == bt
        want = ((200.0 / 103.0 - 1) * 100 + 14 * 1.0) / 15
        assert live == pytest.approx(want)

    def test_avg_1m_range_needs_half_the_minutes(self) -> None:
        f_ts = T0 + 60 * H
        assert avg_1m_range_pct(_minutes_1m(f_ts)[-7:], f_ts + 59 * M, 15) is None  # 6 closed
        assert avg_1m_range_pct(_minutes_1m(f_ts)[-8:], f_ts + 59 * M, 15) is not None  # 7

    def test_candles_1m_to_dicts_accepts_okx_rows_and_objects(self) -> None:
        rows = [["2000", "1", "3", "0.5", "2", "9"], ["1000", "1", "2", "1", "1.5", "9"]]
        assert candles_1m_to_dicts(rows) == [
            {"ts": 1000.0, "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5},
            {"ts": 2000.0, "open": 1.0, "high": 3.0, "low": 0.5, "close": 2.0},
        ]
        obj = Candle(ts=5, open=1.0, high=2.0, low=0.5, close=1.0, volume=3.0)
        assert candles_1m_to_dicts([obj]) == [{"ts": 5.0, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.0}]

    def test_wake_profile_measures(self) -> None:
        closed = _wake_hours()
        forming = _wake_forming(closed)
        as_of = forming["ts"] + 59 * M
        p = wake_profile(closed, forming, as_of, CT, _minutes_1m(forming["ts"]))
        assert p is not None
        base_value = statistics.median(c["volume"] * CT * c["close"] for c in closed[-51:-3])
        assert p.base_volume_usd == pytest.approx(base_value)
        assert p.recent_volume_x == pytest.approx(
            sum(c["volume"] * CT * c["close"] for c in closed[-3:]) / 3 / base_value)
        assert p.hour_volume_x == pytest.approx(1000.0 * CT * 103.0 * 60 / 59 / base_value)
        base_range = statistics.median((c["high"] / c["low"] - 1) * 100 for c in closed[-51:-3])
        assert p.hour_range_x == pytest.approx((104.0 / 99.95 - 1) * 100 / base_range)
        assert p.avg_1m_range_pct == pytest.approx(0.6)
        assert p.below_high_pct == pytest.approx((104.0 / 103.0 - 1) * 100)
        assert p.candle_pct == pytest.approx(3.0)

    def test_contract_size_scales_value_not_ratios(self) -> None:
        closed = _wake_hours()
        forming = _wake_forming(closed)
        as_of = forming["ts"] + 59 * M
        a = wake_profile(closed, forming, as_of, CT, [])
        b = wake_profile(closed, forming, as_of, 2 * CT, [])
        assert a is not None and b is not None
        assert b.base_volume_usd == pytest.approx(2 * a.base_volume_usd)
        assert (b.recent_volume_x, b.hour_volume_x) == pytest.approx((a.recent_volume_x, a.hour_volume_x))

    def test_wake_profile_needs_history(self) -> None:
        closed = _wake_hours(50)
        assert wake_profile(closed, _wake_forming(closed), 0, CT, []) is None
        assert wake_profile(closed, _wake_forming(closed), 0, CT, [], base_hours=47) is not None

    def test_fails_each_threshold_and_blank_is_off(self) -> None:
        d = strategy_defaults("wick_rejection")
        good = WakeProfile(base_volume_usd=100_000, recent_volume_x=4, hour_volume_x=6,
                           hour_range_x=4, avg_1m_range_pct=0.5, below_high_pct=1.0, candle_pct=3.0)
        assert wake_profile_fails(good, d) == []
        cases = {
            "base_volume_usd": (600_000, "base volume 600k > 500k USDT/h"),
            "recent_volume_x": (2.0, "recent volume 2.00x < 3x"),
            "hour_volume_x": (4.0, "hour volume 4.00x < 5x"),
            "hour_range_x": (2.0, "hour range 2.00x < 3x"),
            "avg_1m_range_pct": (0.3, "avg 1m range 0.30% < 0.4%"),
            "candle_pct": (4.0, "candle +4.00% >= 4%"),
        }
        for field, (value, msg) in cases.items():
            bad = WakeProfile(**{**good.__dict__, field: value})
            assert wake_profile_fails(bad, d) == [msg]
        assert wake_profile_fails(WakeProfile(**{**good.__dict__, "below_high_pct": 0.2}), d) == [
            "below high 0.20% < 0.3%"]
        assert wake_profile_fails(WakeProfile(**{**good.__dict__, "below_high_pct": 1.6}), d) == [
            "below high 1.60% > 1.5%"]
        assert wake_profile_fails(WakeProfile(**{**good.__dict__, "avg_1m_range_pct": None}), d) == [
            "avg 1m range unknown"]
        off = {**d, "wake_max_candle_pct": None, "wake_max_base_volume_usd": None}
        assert wake_profile_fails(WakeProfile(**{**good.__dict__, "candle_pct": 9.0,
                                                 "base_volume_usd": 9e9}), off) == []


class TestWakeFunnel:
    def test_off_by_default_needs_no_inputs(self) -> None:
        closed = _wake_hours()
        snap = _wake_snap(closed, _wake_forming(closed), ct=None)
        assert _eval(snap, min_big_bodies=0, max_high_offset_pct=100.0) is not None

    def test_missing_inputs_veto(self) -> None:
        closed = _wake_hours()
        log: list[str] = []
        assert _eval(_wake_snap(closed, _wake_forming(closed), ct=None), log, **_PRESET) is None
        assert any("wake profile inputs missing" in m for m in log)

    def test_signal_carries_the_profile(self) -> None:
        closed = _wake_hours()
        sig = _eval(_wake_snap(closed, _wake_forming(closed)), **_PRESET)
        assert sig is not None
        assert "wake: base 100k/h" in sig.rationale and "candle +3.00%" in sig.rationale

    def test_candle_cap(self) -> None:
        closed = _wake_hours()
        forming = _wake_forming(closed, open_=99.0)  # +4.04%
        log: list[str] = []
        assert _eval(_wake_snap(closed, forming), log, **_PRESET) is None
        assert any("candle +4.04% >= 4%" in m for m in log)
        assert _eval(_wake_snap(closed, forming), **_PRESET, wake_max_candle_pct=None) is not None

    def test_busy_baseline_veto(self) -> None:
        closed = _wake_hours()
        log: list[str] = []
        assert _eval(_wake_snap(closed, _wake_forming(closed), ct=100.0), log, **_PRESET) is None
        assert any("base volume 1000k > 500k" in m for m in log)

    def test_no_wake_veto(self) -> None:
        closed = _wake_hours(recent_vol=100.0)  # recent hours as quiet as the baseline
        log: list[str] = []
        assert _eval(_wake_snap(closed, _wake_forming(closed)), log, **_PRESET) is None
        assert any("recent volume 1.00x < 3x" in m for m in log)

    def test_still_moving_veto(self) -> None:
        closed = _wake_hours()
        forming = _wake_forming(closed)
        log: list[str] = []
        snap = _wake_snap(closed, forming, candles=_minutes_1m(forming["ts"], rng=0.002))
        assert _eval(snap, log, **_PRESET) is None
        assert any("avg 1m range 0.20% < 0.4%" in m for m in log)

    def test_short_history_veto(self) -> None:
        closed = _wake_hours(50)
        log: list[str] = []
        assert _eval(_wake_snap(closed, _wake_forming(closed)), log, **_PRESET) is None
        assert any("wake profile history < 51 closed candles" in m for m in log)


class TestRegistration:
    def test_defaults_registered(self) -> None:
        d = DEFAULT_STRATEGY_CONFIG["wick_rejection"]
        assert d["enabled"] is False
        assert d["analysis_timeframe"] == "1H"
        assert d["use_global_trade_management"] is False

    def test_backtest_registry(self) -> None:
        assert "wick_rejection" in available_strategy_names()

    def test_wake_profile_off_and_preset_keys_exist(self) -> None:
        d = DEFAULT_STRATEGY_CONFIG["wick_rejection"]
        assert d["wake_profile_enabled"] is False
        assert set(WR_WAKE_PRESET) <= set(d)
        assert WR_WAKE_PRESET["wake_profile_enabled"] is True

    def test_strategy_page_saves_every_wake_key(self) -> None:
        from pathlib import Path

        src = (Path(__file__).resolve().parents[1] / "app" / "ui" / "pages.py").read_text()
        for key in (k for k in DEFAULT_STRATEGY_CONFIG["wick_rejection"] if k.startswith("wake_")):
            assert f'"{key}": ' in src, key

    def test_risk_readout_reads_equity_from_live_snapshot_shape(self) -> None:
        from app.ui.pages import snapshot_equity

        # Live: equity at the top, "account" is the per-currency details list.
        assert snapshot_equity({"account": [{"ccy": "USDT"}], "account_equity": 250.0}) == 250.0
        assert snapshot_equity({"account": [], "account_equity": 0.0, "total_eq_usd": 90.0}) == 90.0
        assert snapshot_equity({"account": {"total_equity": 40.0}}) == 40.0  # older dict shape
        assert snapshot_equity({"account": []}) is None
        assert snapshot_equity(None) is None

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
