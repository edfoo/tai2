"""Tests for the §3 liquidity-aware entry gates.

Each strategy gate is opt-in (default OFF) so legacy behaviour is unchanged.
These tests exercise the gate logic end-to-end through the strategies:

  - Mean Reversion: price-inside-VA, funding veto, balanced-order-book.
  - Spike Continuation: open-interest momentum (oi_zscore) confirmation.
  - Liquidity Sweep: close-back-inside-VA and macro-SL.
  - VWAP Reversion: funding-bias veto.
  - Trend Pullback: POC / value-area proximity.
"""

from __future__ import annotations

from typing import Any

from app.services.market_service import MarketService
from app.services.strategies import StrategyHelpers
from app.services.strategies.liquidity_helpers import (
    funding_is_blocked,
    oi_confirms_momentum,
    order_book_imbalance,
)
from app.services.strategies.liquidity_sweep import LiquiditySweepStrategy
from app.services.strategies.mean_reversion import MeanReversionStrategy
from app.services.strategies.spike_continuation import SpikeContinuationStrategy
from app.services.strategies.trend_pullback import TrendPullbackStrategy
from app.services.strategies.vwap_reversion import VWAPReversionStrategy


def _make_helpers(last_price: float = 100.0) -> StrategyHelpers:
    return StrategyHelpers(
        extract_float=MarketService._extract_float,
        emit_debug=lambda msg: None,
        get_last_price=lambda symbol: last_price,
        compute_footprint=lambda symbol: {},
    )


def _snap(
    *,
    indicators: dict[str, Any] | None = None,
    funding: dict[str, Any] | None = None,
    open_interest: dict[str, Any] | None = None,
    order_book: dict[str, Any] | None = None,
    symbol: str = "BTC-USDT-SWAP",
) -> dict[str, Any]:
    """Build a snapshot with the given per-symbol liquidity fields."""
    base_indicators: dict[str, Any] = {
        "rsi": 50.0,
        "cmf_14": {"value": 0.0},
        "adx": {"value": 20.0},
        "bollinger_bands": {"lower": 95.0, "upper": 105.0, "middle": 100.0},
        "htf_indicators": {
            "moving_averages": {"ema_50": 100.0, "ema_200": 99.0},
            "cmf": {"value": None},
        },
    }
    if indicators:
        base_indicators.update(indicators)
    entry: dict[str, Any] = {"indicators": base_indicators, "custom_metrics": {}}
    if funding is not None:
        entry["funding_rate"] = funding
    if open_interest is not None:
        entry["open_interest"] = open_interest
    if order_book is not None:
        entry["order_book"] = order_book
    return {"market_data": {symbol: entry}, "positions": []}


def _mr_bare(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "enabled": True,
        "rsi_oversold": 30.0,
        "rsi_overbought": 70.0,
        "require_cmf": False,
        "require_htf_trend": False,
        "require_htf_cmf": False,
        "require_cmf_cross": False,
        "require_bb_position": False,
        "require_candle_rejection": False,
        "require_vwap_reversion": False,
        "require_volume_cooling": False,
        "require_regime": False,
        "use_atr_sizing": False,
        "max_adx": 0.0,
        "min_adx": 0.0,
        "min_atr_pct": 0.0,
        "min_bb_bandwidth": 0.0,
        "max_bb_bandwidth": 0.0,
        "bb_proximity_pct": 0.0,
    }
    cfg.update(overrides)
    return cfg


def _sc_bare(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "enabled": True,
        "require_regime": False,
        "use_atr_sizing": False,
        "min_atr_pct": 0.0,
        "max_adx": 0.0,
        "max_adx_for_entry": 0.0,
        "require_bb_breakout": False,
        "require_candle_strength": False,
        "require_momentum_acceleration": False,
        "require_rsi_rising": False,
        "require_volume_rsi_rising": False,
        "max_spike_extension_pct": 0.0,
        "min_bb_bandwidth": 0.0,
        "volume_rsi_min": 0.0,
        "rsi_min": 0.0,
        "rsi_max": 100.0,
    }
    cfg.update(overrides)
    return cfg


# ── liquidity_helpers unit tests ────────────────────────────────────────────


class TestLiquidityHelpers:
    def test_order_book_imbalance(self) -> None:
        book = {
            "bids": [[100.0, 50.0], [99.5, 50.0]],
            "asks": [[100.5, 100.0], [101.0, 100.0]],
        }
        # bid_qty=100, ask_qty=200 → 0.5
        assert order_book_imbalance(book) == 0.5

    def test_order_book_imbalance_empty(self) -> None:
        assert order_book_imbalance({}) is None
        assert order_book_imbalance({"bids": [], "asks": []}) is None

    def test_funding_is_blocked_long(self) -> None:
        blocked, info = funding_is_blocked({"fundingRate": "0.003"}, direction="long", max_abs_rate=0.001)
        assert blocked is True
        assert info["rate"] == 0.003

    def test_funding_not_blocked_baseline(self) -> None:
        blocked, info = funding_is_blocked({"fundingRate": "0.00003"}, direction="long", max_abs_rate=0.001)
        assert blocked is False
        assert info["available"] is True

    def test_funding_unavailable_is_neutral(self) -> None:
        blocked, info = funding_is_blocked({}, direction="long", max_abs_rate=0.001)
        assert blocked is False
        assert info["available"] is False

    def test_oi_confirms_with_rising_delta(self) -> None:
        ok, info = oi_confirms_momentum(
            {"oi": "1000", "oi_prev": "900"}, direction="long", min_zscore=1.0
        )
        assert ok is True
        assert info["delta_ratio"] is not None

    def test_oi_confirms_no_history_passes(self) -> None:
        # Single snapshot with rising OI (no history) → weak confirm = pass.
        ok, _ = oi_confirms_momentum({"oi": "1000", "oi_prev": "990"}, direction="long")
        assert ok is True

    def test_oi_unavailable_is_neutral(self) -> None:
        ok, info = oi_confirms_momentum({}, direction="long")
        assert ok is True
        assert info["available"] is False

    # ── Phase 0d: z-score path tests ──────────────────────────────────

    def test_funding_blocked_by_high_z_long(self) -> None:
        # funding_z = 2.0 (top 2 %) → blocked for long entries.
        blocked, info = funding_is_blocked({}, direction="long", funding_z=2.0, max_funding_z=1.28)
        assert blocked is True
        assert info["method"] == "zscore"

    def test_funding_not_blocked_neutral_z_long(self) -> None:
        # funding_z = 0.5 → not blocked.
        blocked, info = funding_is_blocked({}, direction="long", funding_z=0.5, max_funding_z=1.28)
        assert blocked is False
        assert info["method"] == "zscore"

    def test_funding_blocked_by_low_z_short(self) -> None:
        # funding_z = -2.0 → blocked for short entries.
        blocked, info = funding_is_blocked({}, direction="short", funding_z=-2.0, max_funding_z=1.28)
        assert blocked is True

    def test_funding_zscore_overrides_abs_rate(self) -> None:
        # Rate is extreme (0.005) but z-score is low → z-score wins, not blocked.
        blocked, info = funding_is_blocked(
            {"fundingRate": "0.005"}, direction="long",
            max_abs_rate=0.001, funding_z=0.3, max_funding_z=1.28,
        )
        assert blocked is False
        assert info["method"] == "zscore"

    def test_oi_confirmed_by_positive_zscore(self) -> None:
        # oi_zscore = 1.5 > min 1.0 → ok for long.
        ok, info = oi_confirms_momentum({}, direction="long", oi_zscore=1.5, min_zscore=1.0)
        assert ok is True
        assert info["method"] == "zscore"

    def test_oi_blocked_by_low_zscore(self) -> None:
        # oi_zscore = 0.5 < 1.0 → blocked for long.
        ok, info = oi_confirms_momentum({}, direction="long", oi_zscore=0.5, min_zscore=1.0)
        assert ok is False

    def test_oi_zscore_overrides_prev_delta(self) -> None:
        # Flat OI (oi==oi_prev) but z-score says rising → z-score wins.
        ok, info = oi_confirms_momentum(
            {"oi": "1000", "oi_prev": "1000"}, direction="long",
            oi_zscore=2.0, min_zscore=1.0,
        )
        assert ok is True
        assert info["method"] == "zscore"


# ── Mean Reversion gates ────────────────────────────────────────────────────


class TestMeanReversionLiquidity:
    def test_requires_price_inside_va_blocks_outside(self) -> None:
        mr = MeanReversionStrategy()
        # price = last_price 100, VA is [95, 98] → price OUTSIDE VA.
        snapshot = _snap(
            indicators={"value_area_high": 98.0, "value_area_low": 95.0},
        )
        config = _mr_bare(rsi_oversold=30.0, require_price_in_va=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None  # blocked — price outside VA

    def test_requires_price_inside_va_allows_inside(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _snap(
            indicators={"rsi": 20.0, "value_area_high": 101.0, "value_area_low": 97.0},
        )
        config = _mr_bare(rsi_oversold=30.0, require_price_in_va=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is not None
        assert result.direction == "buy"

    def test_funding_veto_blocks_crowded_long(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _snap(funding={"fundingRate": "0.003"})
        config = _mr_bare(
            rsi_oversold=30.0,
            require_no_extreme_funding=True,
            funding_max_abs_rate=0.001,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None  # funding crowded long → no buy

    def test_funding_veto_allows_neutral_funding(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _snap(indicators={"rsi": 20.0}, funding={"fundingRate": "0.00003"})
        config = _mr_bare(
            rsi_oversold=30.0,
            require_no_extreme_funding=True,
            funding_max_abs_rate=0.001,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None

    def test_balanced_book_blocks_extreme_imbalance(self) -> None:
        mr = MeanReversionStrategy()
        # bid_qty=100, ask_qty=1000 → imbalance 0.1 (below min 0.6).
        snapshot = _snap(
            order_book={"bids": [[100.0, 50.0], [99.5, 50.0]], "asks": [[100.5, 500.0], [101.0, 500.0]]}
        )
        config = _mr_bare(
            rsi_oversold=30.0,
            require_balanced_book=True,
            imbalance_min=0.6,
            imbalance_max=1.4,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_balanced_book_allows_balanced(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _snap(
            indicators={"rsi": 20.0},
            order_book={"bids": [[100.0, 100.0]], "asks": [[100.5, 100.0]]},
        )
        config = _mr_bare(
            rsi_oversold=30.0,
            require_balanced_book=True,
            imbalance_min=0.6,
            imbalance_max=1.4,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None

    def test_gates_off_by_default_preserve_signal(self) -> None:
        mr = MeanReversionStrategy()
        # No VA/funding/order-book fields at all — gates default OFF → pass.
        snapshot = _snap(indicators={"rsi": 20.0})
        config = _mr_bare(rsi_oversold=30.0)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None

    def test_funding_z_blocks_crowded_long(self) -> None:
        """Phase 0d: pre-computed funding_z in snapshot blocks buy signal."""
        mr = MeanReversionStrategy()
        snapshot = _snap(indicators={"rsi": 20.0})
        snapshot["market_data"]["BTC-USDT-SWAP"]["funding_z"] = 2.0  # heavily long-crowded
        config = _mr_bare(
            rsi_oversold=30.0,
            require_no_extreme_funding=True,
            funding_max_abs_rate=0.001,  # rate proxy would pass but z-score wins
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None  # blocked by funding z-score

    def test_funding_z_neutral_allows_buy(self) -> None:
        """Phase 0d: neutral funding_z lets an oversold buy through."""
        mr = MeanReversionStrategy()
        snapshot = _snap(indicators={"rsi": 20.0})
        snapshot["market_data"]["BTC-USDT-SWAP"]["funding_z"] = 0.4
        config = _mr_bare(
            rsi_oversold=30.0,
            require_no_extreme_funding=True,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None


# ── Spike Continuation OI gate ──────────────────────────────────────────────


class TestSpikeContinuationLiquidity:
    def _sc_rising_ohlcv(self) -> dict[str, Any]:
        return {
            "ohlcv": [
                {"open": 96.0, "high": 97.0, "low": 95.0, "close": 96.5, "volume": 100.0},
                {"open": 96.5, "high": 100.0, "low": 96.0, "close": 99.5, "volume": 10.0},
                {"open": 99.5, "high": 105.0, "low": 99.0, "close": 104.5, "volume": 10.0},
                {"open": 104.5, "high": 110.0, "low": 104.0, "close": 109.5, "volume": 10.0},
                {"open": 109.5, "high": 113.0, "low": 109.0, "close": 112.0, "volume": 10.0},
            ]
        }

    def test_oi_confirmation_allows_rising_oi(self) -> None:
        sc = SpikeContinuationStrategy()
        snapshot = _snap(
            indicators={"rsi": 65.0, "volume_rsi_series": [80.0, 85.0], **self._sc_rising_ohlcv()},
            open_interest={"oi": "1200", "oi_prev": "900"},
        )
        config = _sc_bare(require_oi_confirmation=True, oi_min_zscore=1.0, require_volume_rsi_rising=False)
        result = sc.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=112.0))
        assert result is not None

    def test_oi_confirmation_blocks_without_rise(self) -> None:
        sc = SpikeContinuationStrategy()
        # OI flat/lower — no fresh leverage → block momentum entry.
        snapshot = _snap(
            indicators={"rsi": 65.0, "volume_rsi_series": [80.0, 85.0], **self._sc_rising_ohlcv()},
            open_interest={"oi": "900", "oi_prev": "900"},
        )
        config = _sc_bare(
            require_oi_confirmation=True,
            oi_min_zscore=1.0,
            require_volume_rsi_rising=False,
            max_spike_extension_pct=0,
        )
        result = sc.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=112.0))
        assert result is None  # blocked — OI not confirming momentum

    def test_oi_gate_off_when_disabled(self) -> None:
        sc = SpikeContinuationStrategy()
        snapshot = _snap(
            indicators={"rsi": 65.0, "volume_rsi_series": [80.0, 85.0], **self._sc_rising_ohlcv()},
            open_interest={"oi": "900", "oi_prev": "900"},
        )
        config = _sc_bare(require_oi_confirmation=False, require_volume_rsi_rising=False)
        result = sc.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=112.0))
        assert result is not None

    def test_oi_zscore_from_snapshot_blocks(self) -> None:
        """Phase 0d: pre-computed oi_zscore in snapshot blocks when below threshold."""
        sc = SpikeContinuationStrategy()
        snapshot = _snap(
            indicators={"rsi": 65.0, "volume_rsi_series": [80.0, 85.0], **self._sc_rising_ohlcv()},
        )
        snapshot["market_data"]["BTC-USDT-SWAP"]["oi_zscore"] = 0.3  # below min 1.0
        config = _sc_bare(require_oi_confirmation=True, oi_min_zscore=1.0, require_volume_rsi_rising=False)
        result = sc.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=112.0))
        assert result is None  # blocked by oi_zscore

    def test_oi_zscore_from_snapshot_allows(self) -> None:
        """Phase 0d: high oi_zscore in snapshot allows the momentum entry."""
        sc = SpikeContinuationStrategy()
        snapshot = _snap(
            indicators={"rsi": 65.0, "volume_rsi_series": [80.0, 85.0], **self._sc_rising_ohlcv()},
        )
        snapshot["market_data"]["BTC-USDT-SWAP"]["oi_zscore"] = 2.5  # above min 1.0
        config = _sc_bare(require_oi_confirmation=True, oi_min_zscore=1.0, require_volume_rsi_rising=False)
        result = sc.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=112.0))
        assert result is not None


# ── Liquidity Sweep gates ──────────────────────────────────────────────────


class TestLiquiditySweepLiquidity:
    def _sweep_ohlcv(self, curr_close: float = 100.0) -> dict[str, Any]:
        # Prior candles establish a swing_low ~ 99; current candle wicks below
        # then reclaims. Close configurable to test the close-in-VA gate.
        ohlcv: list[dict] = []
        base = 100.0
        for i in range(25):
            ohlcv.append({"open": base, "high": base + 1.0, "low": base - 1.0, "close": base, "volume": 50.0})
        ohlcv[-1] = {
            "open": 100.0,
            "high": 101.5,
            "low": 97.0,   # wicks below swing_low (~99)
            "close": curr_close,
            "volume": 120.0,
        }
        return {"ohlcv": ohlcv, "adx": {"value": 15.0}}

    def test_close_in_va_allows_close_inside(self) -> None:
        ls = LiquiditySweepStrategy()
        snapshot = _snap(
            indicators={**self._sweep_ohlcv(curr_close=100.0), "value_area_high": 101.0, "value_area_low": 99.0},
        )
        config = {
            "enabled": True,
            "require_close_in_va": True,
            "require_htf_trend": False,
            "require_volume_spike": False,
            "require_regime": False,
            "max_adx": 30.0,
            "use_structural_sizing": False,
            "use_atr_sizing": False,
            "min_atr_pct": 0.0,
        }
        result = ls.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.5))
        assert result is not None

    def test_close_in_va_blocks_close_outside(self) -> None:
        ls = LiquiditySweepStrategy()
        # Current candle closes at 104 — OUTSIDE VA [99, 101] → treated as breakout.
        snapshot = _snap(
            indicators={**self._sweep_ohlcv(curr_close=104.0), "value_area_high": 101.0, "value_area_low": 99.0},
        )
        config = {
            "enabled": True,
            "require_close_in_va": True,
            "require_htf_trend": False,
            "require_volume_spike": False,
            "require_regime": False,
            "max_adx": 30.0,
            "use_structural_sizing": False,
            "use_atr_sizing": False,
            "min_atr_pct": 0.0,
        }
        result = ls.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=104.0))
        assert result is None  # blocked — close outside VA


# ── VWAP Reversion gates ────────────────────────────────────────────────────


class TestVWAPReversionLiquidity:
    def _vwap_snapshot(self, *, distance_above: bool = True, closeback: bool = True) -> dict[str, Any]:
        indicators: dict[str, Any] = {
            "vwap": 100.0,
            "atr_pct": 1.0,
            "rsi": 50.0,
            "bollinger_bands": {"lower": 95.0, "upper": 105.0, "middle": 100.0},
            "htf_indicators": {"moving_averages": {"ema_50": 100.0, "ema_200": 99.0}},
        }
        # prev close / curr close: for short, price above VWAP, curr closer to VWAP (down).
        indicators["ohlcv"] = [
            {"open": 103.5, "high": 104.0, "low": 102.0, "close": 103.5, "volume": 10.0},
            {"open": 103.0, "high": 103.5, "low": 102.5, "close": 103.0, "volume": 10.0},
        ]
        return _snap(indicators=indicators)

    def test_funding_bias_blocks_crowded_short(self) -> None:
        vr = VWAPReversionStrategy()
        snapshot = self._vwap_snapshot(distance_above=True)
        # Short entry (price above VWAP) blocked when funding strongly negative.
        snapshot["market_data"]["BTC-USDT-SWAP"]["funding_rate"] = {"fundingRate": "-0.003"}
        config = {
            "enabled": True,
            "vwap_min_distance_atr": 2.0,
            "vwap_max_distance_atr": 3.0,
            "max_adx": 25.0,
            "require_closeback": True,
            "require_htf_trend": False,
            "require_regime": False,
            "use_atr_sizing": False,
            "use_structural_sizing": False,
            "min_atr_pct": 0.0,
            "require_no_funding_bias": True,
            "funding_max_abs_rate": 0.0007,
        }
        result = vr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=103.0))
        assert result is None  # blocked — funding heavily short-crowded

    def test_funding_bias_off_allows_short(self) -> None:
        vr = VWAPReversionStrategy()
        snapshot = self._vwap_snapshot(distance_above=True)
        snapshot["market_data"]["BTC-USDT-SWAP"]["funding_rate"] = {"fundingRate": "-0.003"}
        config = {
            "enabled": True,
            "vwap_min_distance_atr": 2.0,
            "vwap_max_distance_atr": 3.0,
            "max_adx": 25.0,
            "require_closeback": True,
            "require_htf_trend": False,
            "require_regime": False,
            "use_atr_sizing": False,
            "use_structural_sizing": False,
            "min_atr_pct": 0.0,
            "require_no_funding_bias": False,
        }
        result = vr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=103.0))
        assert result is not None


# ── Trend Pullback POC proximity gate ───────────────────────────────────────


class TestTrendPullbackLiquidity:
    def _tp_snapshot(self, *, near_poc: bool = True) -> dict[str, Any]:
        indicators: dict[str, Any] = {
            "rsi": 55.0,
            "adx": {"value": 25.0},
            "atr_pct": 1.0,
            "moving_averages": {"ema_21": 100.0},
            "vwap": 101.0,
            "htf_indicators": {"moving_averages": {"ema_50": 100.0, "ema_200": 99.0}},
        }
        # Current candle: pullback to EMA21 (100) with bullish rejection.
        indicators["ohlcv"] = [
            {"open": 100.5, "high": 102.0, "low": 99.8, "close": 101.0, "volume": 10.0},
            {"open": 100.6, "high": 101.2, "low": 99.9, "close": 100.8, "volume": 10.0},
        ]
        # POC node at 100.1; VA width ~1.0. If near_poc, price (100.8 → EMA21 100)
        # is within 0.2 × 1.0 of a VA node. Place the VA-high at 100.9 so price
        # 100.8 is within 0.2 of a node.
        if near_poc:
            indicators["vpoc"] = 100.1
            indicators["value_area_high"] = 100.9
            indicators["value_area_low"] = 99.9
            indicators["value_area_width"] = 1.0
        return _snap(indicators=indicators)

    def test_poc_proximity_allows_near_node(self) -> None:
        tp = TrendPullbackStrategy()
        snapshot = self._tp_snapshot(near_poc=True)
        config = {
            "enabled": True,
            "pullback_ema": 21,
            "pullback_proximity_pct": 1.0,
            "use_vwap_as_level": False,
            "require_htf_trend": False,
            "require_bullish_candle": False,
            "min_adx": 0.0,
            "max_adx_for_entry": 40.0,
            "use_structural_sizing": False,
            "use_atr_sizing": False,
            "min_atr_pct": 0.0,
            "require_poc_proximity": True,
            "poc_proximity_va_width": 0.2,
        }
        result = tp.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.8))
        assert result is not None

    def test_poc_proximity_blocks_away_from_node(self) -> None:
        tp = TrendPullbackStrategy()
        # No VA data → neutral (pass). To test a *block*, put POC far from price.
        snapshot = _snap(
            indicators={
                "rsi": 55.0,
                "adx": {"value": 25.0},
                "atr_pct": 1.0,
                "moving_averages": {"ema_21": 100.0},
                "vwap": 101.0,
                "htf_indicators": {"moving_averages": {"ema_50": 100.0, "ema_200": 99.0}},
                "ohlcv": [
                    {"open": 100.5, "high": 102.0, "low": 99.8, "close": 101.0, "volume": 10.0},
                    {"open": 100.6, "high": 101.2, "low": 99.9, "close": 100.8, "volume": 10.0},
                ],
                "vpoc": 95.0,  # POC far from price
                "value_area_high": 96.0,
                "value_area_low": 94.0,
                "value_area_width": 2.0,
            }
        )
        config = {
            "enabled": True,
            "pullback_ema": 21,
            "pullback_proximity_pct": 1.0,
            "use_vwap_as_level": False,
            "require_htf_trend": False,
            "require_bullish_candle": False,
            "min_adx": 0.0,
            "max_adx_for_entry": 40.0,
            "use_structural_sizing": False,
            "use_atr_sizing": False,
            "min_atr_pct": 0.0,
            "require_poc_proximity": True,
            "poc_proximity_va_width": 0.2,
        }
        result = tp.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.8))
        assert result is None  # blocked — price far from any POC/VA node

    def test_poc_proximity_off_allows_away(self) -> None:
        tp = TrendPullbackStrategy()
        snapshot = self._tp_snapshot(near_poc=False)  # no VA data
        config = {
            "enabled": True,
            "pullback_ema": 21,
            "pullback_proximity_pct": 1.0,
            "use_vwap_as_level": False,
            "require_htf_trend": False,
            "require_bullish_candle": False,
            "min_adx": 0.0,
            "max_adx_for_entry": 40.0,
            "use_structural_sizing": False,
            "use_atr_sizing": False,
            "min_atr_pct": 0.0,
            "require_poc_proximity": False,
        }
        result = tp.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.8))
        assert result is not None