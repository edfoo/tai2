"""Tests for the pluggable strategy system.

Covers:
  - Strategy protocol compliance
  - StrategyHelpers delegation
  - MeanReversionStrategy signal evaluation
  - Config migration from legacy flat format
  - MarketService strategy registry integration
  - build_launcher_decision() reading from strategy config
  - llm_with_filter path reading from strategy config
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.market_service import MarketService
from app.services.strategies import Strategy, StrategyHelpers, StrategySignal
from app.services.strategies.mean_reversion import MeanReversionStrategy
from app.services.strategies.spike_continuation import SpikeContinuationStrategy
from app.services.strategies.liquidity_sweep import LiquiditySweepStrategy
from app.services.strategies.vwap_reversion import VWAPReversionStrategy
from app.services.strategies.trend_pullback import TrendPullbackStrategy


# ── Helpers ──────────────────────────────────────────────────────────────────


class _DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _make_helpers(last_price: float = 100.0) -> StrategyHelpers:
    """Create a StrategyHelpers with mock functions."""
    return StrategyHelpers(
        extract_float=MarketService._extract_float,
        emit_debug=lambda msg: None,
        get_last_price=lambda symbol: last_price,
        compute_footprint=lambda symbol: {},
    )


def _mr_bare(**overrides: Any) -> dict[str, Any]:
    """MR config with recommended default-on filters explicitly disabled.

    Production defaults are intentionally strict. Unit tests that isolate a
    single filter must opt out of the rest so missing snapshot fields do not
    block the signal under test.
    """
    cfg: dict[str, Any] = {
        "enabled": True,
        "require_cmf": False,
        "require_cmf_cross": False,
        "require_htf_trend": False,
        "require_htf_cmf": False,
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
    """SC config with recommended default-on filters explicitly disabled."""
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


def _make_snapshot(
    *,
    rsi: float | None = 50.0,
    cmf_value: float | None = 0.0,
    adx_value: float | None = 20.0,
    bb_lower: float | None = 95.0,
    bb_upper: float | None = 105.0,
    bb_middle: float | None = 100.0,
    htf_ema50: float | None = 100.0,
    htf_ema200: float | None = 99.0,
    htf_cmf: float | None = None,
    cmf_series: list[float] | None = None,
    symbol: str = "BTC-USDT-SWAP",
) -> dict[str, Any]:
    """Build a minimal snapshot with configurable indicator values."""
    cmf_block: dict[str, Any] = {"value": cmf_value}
    if cmf_series is not None:
        cmf_block["series"] = cmf_series

    indicators: dict[str, Any] = {
        "rsi": rsi,
        "cmf_14": cmf_block,
        "adx": {"value": adx_value},
        "bollinger_bands": {
            "lower": bb_lower,
            "upper": bb_upper,
            "middle": bb_middle,
        },
        "htf_indicators": {
            "moving_averages": {
                "ema_50": htf_ema50,
                "ema_200": htf_ema200,
            },
            "cmf": {"value": htf_cmf},
        },
    }

    return {
        "market_data": {
            symbol: {
                "indicators": indicators,
                "custom_metrics": {},
            }
        },
        "positions": [],
    }


def _make_service() -> MarketService:
    """Create a MarketService with minimal mocking for strategy tests."""
    service = MarketService(
        state_service=_DummyStateService(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    return service


# ── Strategy Protocol ────────────────────────────────────────────────────────


class TestStrategyProtocol:
    def test_mean_reversion_satisfies_protocol(self) -> None:
        mr = MeanReversionStrategy()
        assert isinstance(mr, Strategy)

    def test_custom_strategy_satisfies_protocol(self) -> None:
        class MyStrategy:
            name = "custom"

            def evaluate(self, symbol, snapshot, config, helpers):
                return None

        assert isinstance(MyStrategy(), Strategy)

    def test_incomplete_strategy_fails_protocol(self) -> None:
        class Incomplete:
            name = "incomplete"
            # Missing evaluate()

        assert not isinstance(Incomplete(), Strategy)


# ── StrategyHelpers ──────────────────────────────────────────────────────────


class TestStrategyHelpers:
    def test_extract_float_delegates(self) -> None:
        helpers = _make_helpers()
        assert helpers.extract_float("3.14") == 3.14
        assert helpers.extract_float(None) is None

    def test_get_last_price_delegates(self) -> None:
        helpers = _make_helpers()
        assert helpers.get_last_price("BTC-USDT-SWAP") == 100.0

    def test_compute_footprint_delegates(self) -> None:
        helpers = _make_helpers()
        assert helpers.compute_footprint("BTC-USDT-SWAP") == {}

    def test_compute_footprint_returns_empty_when_not_configured(self) -> None:
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=lambda msg: None,
            get_last_price=lambda symbol: 100.0,
            # compute_footprint not provided
        )
        assert helpers.compute_footprint("BTC-USDT-SWAP") == {}

    def test_emit_debug_delegates(self) -> None:
        messages: list[str] = []
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=messages.append,
            get_last_price=lambda symbol: 100.0,
        )
        helpers.emit_debug("test message")
        assert messages == ["test message"]


# ── MeanReversionStrategy ────────────────────────────────────────────────────


class TestMeanReversionStrategy:
    def test_returns_none_when_disabled(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, {"enabled": False}, _make_helpers())
        assert result is None

    def test_returns_none_when_no_enabled_key(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, {}, _make_helpers())
        assert result is None

    def test_buy_signal_when_rsi_oversold(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0, cmf_value=0.1, htf_ema50=101.0, htf_ema200=99.0)
        config = _mr_bare(
            rsi_oversold=30.0,
            rsi_overbought=70.0,
            require_cmf=True,
            require_htf_trend=True,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_sell_signal_when_rsi_overbought(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=80.0, cmf_value=-0.1, htf_ema50=99.0, htf_ema200=101.0)
        config = _mr_bare(
            rsi_oversold=30.0,
            rsi_overbought=70.0,
            require_cmf=True,
            require_htf_trend=True,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "sell"

    def test_no_signal_when_rsi_neutral(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=50.0)
        config = _mr_bare()
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_no_signal_when_cmf_disagrees(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold but CMF negative → no buy
        snapshot = _make_snapshot(rsi=20.0, cmf_value=-0.1)
        config = {"enabled": True, "require_cmf": True}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_no_signal_when_htf_trend_disagrees(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold but HTF bearish (EMA50 < EMA200) → no buy
        snapshot = _make_snapshot(rsi=20.0, htf_ema50=99.0, htf_ema200=101.0)
        config = {"enabled": True, "require_htf_trend": True}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_signal_without_optional_filters(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold, no CMF/HTF requirements → buy
        snapshot = _make_snapshot(rsi=20.0, cmf_value=-0.5, htf_ema50=99.0, htf_ema200=101.0)
        config = _mr_bare(
            rsi_oversold=30.0,
            require_cmf=False,
            require_htf_trend=False,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_adx_min_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0, adx_value=10.0)
        config = {"enabled": True, "min_adx": 15.0}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_adx_max_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0, adx_value=50.0)
        config = {"enabled": True, "max_adx": 30.0}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_bb_position_filter_allows_long_at_lower_band(self) -> None:
        mr = MeanReversionStrategy()
        # Price at 94.5, lower band at 95.0, proximity 1% → 94.5 < 95 * 1.01 = 95.95 → OK
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=lambda msg: None,
            get_last_price=lambda symbol: 94.5,
            compute_footprint=lambda symbol: {},
        )
        snapshot = _make_snapshot(rsi=20.0, bb_lower=95.0, bb_upper=105.0, bb_middle=100.0)
        config = _mr_bare(require_bb_position=True, bb_proximity_pct=1.0)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert result is not None
        assert result.direction == "buy"

    def test_bb_position_filter_blocks_long_above_band(self) -> None:
        mr = MeanReversionStrategy()
        # Price at 97.0, lower band at 95.0, proximity 0% → 97 > 95 → blocked
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=lambda msg: None,
            get_last_price=lambda symbol: 97.0,
            compute_footprint=lambda symbol: {},
        )
        snapshot = _make_snapshot(rsi=20.0, bb_lower=95.0, bb_upper=105.0, bb_middle=100.0)
        config = {"enabled": True, "require_bb_position": True, "bb_proximity_pct": 0.0}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert result is None

    def test_min_bb_bandwidth_blocks(self) -> None:
        mr = MeanReversionStrategy()
        # BB bandwidth = (105-95)/100*100 = 10%, min = 15% → blocked
        snapshot = _make_snapshot(rsi=20.0, bb_lower=95.0, bb_upper=105.0, bb_middle=100.0)
        config = {"enabled": True, "min_bb_bandwidth": 15.0}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_max_bb_bandwidth_blocks(self) -> None:
        mr = MeanReversionStrategy()
        # BB bandwidth = (115-85)/100*100 = 30%, max = 20% → blocked
        snapshot = _make_snapshot(rsi=20.0, bb_lower=85.0, bb_upper=115.0, bb_middle=100.0)
        config = {"enabled": True, "max_bb_bandwidth": 20.0}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_no_signal_when_rsi_unavailable(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=None)
        config = _mr_bare()
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_htf_cmf_filter(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold, HTF CMF positive → buy
        snapshot = _make_snapshot(rsi=20.0, htf_cmf=0.1)
        config = _mr_bare(require_htf_cmf=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_htf_cmf_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold, HTF CMF negative → no buy
        snapshot = _make_snapshot(rsi=20.0, htf_cmf=-0.1)
        config = _mr_bare(require_htf_cmf=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_cmf_cross_filter(self) -> None:
        mr = MeanReversionStrategy()
        # CMF crossed up: prev=-0.1, current=0.1
        snapshot = _make_snapshot(rsi=20.0, cmf_value=0.1, cmf_series=[-0.3, -0.2, -0.1, -0.1, 0.1])
        config = _mr_bare(require_cmf_cross=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_cmf_cross_filter_blocks_when_no_cross(self) -> None:
        mr = MeanReversionStrategy()
        # CMF stayed positive: prev=0.05, current=0.1 → no cross
        snapshot = _make_snapshot(rsi=20.0, cmf_value=0.1, cmf_series=[0.01, 0.02, 0.03, 0.05, 0.1])
        config = _mr_bare(require_cmf_cross=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_debug_emitted_on_no_signal(self) -> None:
        mr = MeanReversionStrategy()
        messages: list[str] = []
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=messages.append,
            get_last_price=lambda symbol: 100.0,
            compute_footprint=lambda symbol: {},
        )
        snapshot = _make_snapshot(rsi=50.0)
        config = {"enabled": True}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert result is None
        assert any("no entry signal" in m for m in messages)

    def test_uses_default_thresholds_when_config_missing(self) -> None:
        mr = MeanReversionStrategy()
        # RSI=27 → below default oversold of 28 → buy (with other filters bare-off)
        snapshot = _make_snapshot(rsi=27.0)
        config = _mr_bare()
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_footprint_delta_filter(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        snapshot["market_data"]["BTC-USDT-SWAP"]["custom_metrics"]["footprint"] = {"net_delta": 50.0}
        config = _mr_bare(require_footprint_delta=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_footprint_delta_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        snapshot["market_data"]["BTC-USDT-SWAP"]["custom_metrics"]["footprint"] = {"net_delta": -50.0}
        config = _mr_bare(require_footprint_delta=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_footprint_fallback_to_compute(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        # No footprint in custom_metrics, but compute_footprint returns data
        helpers = StrategyHelpers(
            extract_float=MarketService._extract_float,
            emit_debug=lambda msg: None,
            get_last_price=lambda symbol: 100.0,
            compute_footprint=lambda symbol: {"net_delta": 30.0},
        )
        config = _mr_bare(require_footprint_delta=True)
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert result is not None
        assert result.direction == "buy"


# ── MarketService Strategy Registry ──────────────────────────────────────────


class TestMarketServiceStrategyRegistry:
    def test_strategies_list_contains_mean_reversion(self) -> None:
        service = _make_service()
        strategy_names = [s.name for s in service._strategies]
        assert "mean_reversion" in strategy_names

    def test_strategy_helpers_has_required_methods(self) -> None:
        service = _make_service()
        h = service._strategy_helpers
        assert callable(h.extract_float)
        assert callable(h.emit_debug)
        assert callable(h.get_last_price)
        assert callable(h.compute_footprint)

    def test_launcher_evaluate_signal_delegates_to_strategy(self) -> None:
        service = _make_service()
        # Configure mean reversion as enabled with oversold threshold
        service.set_launcher_config({
            "mode": "launcher_only",
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        # Set up a snapshot with RSI oversold
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        signals = service._launcher_evaluate_signals("BTC-USDT-SWAP")
        assert len(signals) == 1
        assert signals[0].direction == "buy"
        assert signals[0].strategy_name == "mean_reversion"

    def test_launcher_evaluate_signal_returns_empty_when_no_strategy_fires(self) -> None:
        service = _make_service()
        service.set_launcher_config({
            "mode": "launcher_only",
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 10.0,  # very low, RSI=50 won't trigger,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=50.0)

        signals = service._launcher_evaluate_signals("BTC-USDT-SWAP")
        assert signals == []

    def test_launcher_evaluate_signal_returns_empty_when_all_disabled(self) -> None:
        service = _make_service()
        service.set_launcher_config({
            "mode": "launcher_only",
            "strategies": {
                "mean_reversion": {"enabled": False},
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=5.0)  # extremely oversold

        signals = service._launcher_evaluate_signals("BTC-USDT-SWAP")
        assert signals == []

    def test_launcher_evaluate_signal_returns_empty_when_no_snapshot(self) -> None:
        service = _make_service()
        service._last_full_snapshot = None
        signals = service._launcher_evaluate_signals("BTC-USDT-SWAP")
        assert signals == []

    def test_multiple_strategies_fire_concurrently(self) -> None:
        """When multiple strategies fire, all signals are returned."""
        service = _make_service()

        class AlwaysBuy:
            name = "always_buy"
            def evaluate(self, symbol, snapshot, config, helpers):
                return StrategySignal(direction="buy", strategy_name="always_buy") if config.get("enabled") else None

        class AlwaysSell:
            name = "always_sell"
            def evaluate(self, symbol, snapshot, config, helpers):
                return StrategySignal(direction="sell", strategy_name="always_sell") if config.get("enabled") else None

        service._strategies = [AlwaysBuy(), AlwaysSell(), MeanReversionStrategy()]
        service.set_launcher_config({
            "strategies": {
                "always_buy": {"enabled": True},
                "always_sell": {"enabled": True},
                "mean_reversion": {
                    "enabled": True,
                    "rsi_overbought": 70.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                },
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=80.0)  # would trigger MR sell

        signals = service._launcher_evaluate_signals("BTC-USDT-SWAP")
        # All three strategies fire: AlwaysBuy, AlwaysSell, MeanReversion(sell)
        assert len(signals) == 3
        directions = {s.direction for s in signals}
        assert directions == {"buy", "sell"}


# ── build_launcher_decision with strategy config ─────────────────────────────


class TestBuildLauncherDecision:
    @staticmethod
    def _setup_service_with_price(service: MarketService, price: float = 100.0) -> None:
        """Set up the service's ticker cache so get_last_price() works."""
        symbol = "BTC-USDT-SWAP"
        service._latest_ticker[symbol] = {"last": str(price), "px": str(price)}

    def test_reads_tp_sl_from_strategy_config(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "tp_pct": 3.0,
                    "sl_pct": 10.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["action"] == "BUY"
        # TP should be 3% above last price (100 * 1.03 = 103)
        assert abs(decision["take_profit"] - 103.0) < 0.01
        # SL should be 10% below last price (100 * 0.90 = 90)
        assert abs(decision["stop_loss"] - 90.0) < 0.01

    def test_falls_back_to_launcher_level_tp_sl(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "tp_pct": 5.0,
            "sl_pct": 15.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    # No tp_pct/sl_pct in strategy config → fallback to launcher level,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # TP should be 5% above (launcher-level fallback)
        assert abs(decision["take_profit"] - 105.0) < 0.01
        # SL should be 15% below (launcher-level fallback)
        assert abs(decision["stop_loss"] - 85.0) < 0.01

    def test_returns_empty_when_strategy_disabled(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": {"enabled": False},
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=5.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert decisions == []

    def test_reads_dynamic_tp_from_strategy_config(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "tp_pct": 10.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "dynamic_tp": True,
                    "dynamic_tp_fraction": 0.7,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        # BB bandwidth = (105-95)/100*100 = 10%
        # Dynamic TP = (10/2) * 0.7 * 1.0 = 3.5%
        # effective_tp = min(10%, 3.5%) = 3.5%
        service._last_full_snapshot = _make_snapshot(rsi=20.0, bb_lower=95.0, bb_upper=105.0, bb_middle=100.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # TP should be 3.5% above 100 = 103.5
        assert abs(decision["take_profit"] - 103.5) < 0.01

    def test_reads_flip_direction_from_strategy_config(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "flip_launcher_direction": "both",
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # RSI oversold → buy signal, but flip "both" → SELL
        assert decision["action"] == "SELL"

    def test_decision_origin_is_launcher(self) -> None:
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "require_cmf": False,
                    "require_htf_trend": False,
                    "require_cmf_cross": False,
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "max_adx": 0.0,
                    "min_atr_pct": 0.0,
                    "min_bb_bandwidth": 0.0,

                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["_decision_origin"] == "launcher"

    def test_vwap_reversion_flip_direction_and_tp_sl_reflection(self) -> None:
        """VWAP Reversion flip mirrors side, TP and SL around last_price."""
        service = _make_service()
        self._setup_service_with_price(service, 95.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "vwap_reversion": {
                    "enabled": True,
                    "tp_pct": 3.0,
                    "sl_pct": 5.0,
                    "vwap_min_distance_atr": 2.0,
                    "require_closeback": False,
                    "require_htf_trend": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "both",
                },
            },
        })
        # VWAP=100, ATR%=2% → ATR_price=2.0. Price=95 → 2.5 ATR below → buy signal.
        service._last_full_snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # VWAP reversion buy signal, flipped "both" → SELL
        assert decision["action"] == "SELL"
        # Original BUY: TP=95*1.03=97.85 (above), SL=95*0.95=90.25 (below).
        # Flipped to SELL: mirror around last_price=95 →
        #   TP=2*95-97.85=92.15 (below, correct for short)
        #   SL=2*95-90.25=99.75 (above, correct for short)
        assert abs(decision["take_profit"] - 92.15) < 0.01
        assert abs(decision["stop_loss"] - 99.75) < 0.01

    def test_vwap_reversion_flip_from_long_only_keeps_short(self) -> None:
        """from_long flip should leave a SELL signal unchanged."""
        service = _make_service()
        self._setup_service_with_price(service, 105.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "vwap_reversion": {
                    "enabled": True,
                    "tp_pct": 3.0,
                    "sl_pct": 5.0,
                    "vwap_min_distance_atr": 2.0,
                    "require_closeback": False,
                    "require_htf_trend": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "from_long",
                },
            },
        })
        # Price 105 → 2.5 ATR above VWAP → sell signal; from_long should NOT flip it.
        service._last_full_snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=105.0,
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["action"] == "SELL"
        # Unflipped SELL: TP=105*0.97=101.85, SL=105*1.05=110.25
        assert abs(decision["take_profit"] - 101.85) < 0.01
        assert abs(decision["stop_loss"] - 110.25) < 0.01

    def test_liquidity_sweep_flip_direction_and_tp_sl_reflection(self) -> None:
        """Liquidity Sweep flip mirrors side, TP and SL around last_price."""
        service = _make_service()
        self._setup_service_with_price(service, 100.5)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "liquidity_sweep": {
                    "enabled": True,
                    "tp_pct": 3.0,
                    "sl_pct": 5.0,
                    "lookback": 10,
                    "sweep_buffer_pct": 0.1,
                    "reclaim_ratio": 0.5,
                    "require_htf_trend": False,
                    "require_volume_spike": False,
                    "max_adx": 0.0,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "use_structural_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "both",
                },
            },
        })
        # Low sweep: wick below swing low, close reclaims above → buy signal.
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        service._last_full_snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=100.5)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # Sweep buy signal, flipped "both" → SELL
        assert decision["action"] == "SELL"
        # Original BUY: TP=100.5*1.03=103.515 (above), SL=100.5*0.95=95.475 (below).
        # Flipped to SELL: mirror around last_price=100.5 →
        #   TP=2*100.5-103.515=97.485 (below, correct for short)
        #   SL=2*100.5-95.475=105.525 (above, correct for short)
        assert abs(decision["take_profit"] - 97.485) < 0.01
        assert abs(decision["stop_loss"] - 105.525) < 0.01

    def test_liquidity_sweep_flip_from_short_only_keeps_long(self) -> None:
        """from_short flip should leave a BUY signal unchanged."""
        service = _make_service()
        self._setup_service_with_price(service, 100.5)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "liquidity_sweep": {
                    "enabled": True,
                    "tp_pct": 3.0,
                    "sl_pct": 5.0,
                    "lookback": 10,
                    "sweep_buffer_pct": 0.1,
                    "reclaim_ratio": 0.5,
                    "require_htf_trend": False,
                    "require_volume_spike": False,
                    "max_adx": 0.0,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "use_structural_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "from_short",
                },
            },
        })
        # Low sweep → buy signal; from_short should NOT flip it.
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        service._last_full_snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=100.5)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["action"] == "BUY"
        # Unflipped BUY: TP=100.5*1.03=103.515, SL=100.5*0.95=95.475
        assert abs(decision["take_profit"] - 103.515) < 0.01
        assert abs(decision["stop_loss"] - 95.475) < 0.01

    def test_trend_pullback_flip_direction_and_tp_sl_reflection(self) -> None:
        """Trend Pullback flip mirrors side, TP and SL around last_price."""
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "trend_pullback": {
                    "enabled": True,
                    "tp_pct": 4.0,
                    "sl_pct": 3.0,
                    "pullback_ema": 21,
                    "use_vwap_as_level": False,
                    "pullback_proximity_pct": 0.5,
                    "require_htf_trend": True,
                    "require_bullish_candle": True,
                    "candle_rejection_pct": 25.0,
                    "min_adx": 0.0,
                    "max_adx_for_entry": 0.0,
                    "use_atr_sizing": False,
                    "use_structural_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "both",
                },
            },
        })
        # EMA21=100, price=100 (touching), HTF bullish, bullish candle → buy signal.
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        service._last_full_snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # Pullback buy signal, flipped "both" → SELL
        assert decision["action"] == "SELL"
        # Original BUY: TP=100*1.04=104 (above), SL=100*0.97=97 (below).
        # Flipped to SELL: mirror around last_price=100 →
        #   TP=2*100-104=96 (below, correct for short)
        #   SL=2*100-97=103 (above, correct for short)
        assert abs(decision["take_profit"] - 96.0) < 0.01
        assert abs(decision["stop_loss"] - 103.0) < 0.01

    def test_trend_pullback_flip_from_long_only_keeps_short(self) -> None:
        """from_long flip should leave a SELL signal unchanged."""
        service = _make_service()
        self._setup_service_with_price(service, 100.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "trend_pullback": {
                    "enabled": True,
                    "tp_pct": 4.0,
                    "sl_pct": 3.0,
                    "pullback_ema": 21,
                    "use_vwap_as_level": False,
                    "pullback_proximity_pct": 0.5,
                    "require_htf_trend": True,
                    "require_bullish_candle": True,
                    "candle_rejection_pct": 25.0,
                    "min_adx": 0.0,
                    "max_adx_for_entry": 0.0,
                    "use_atr_sizing": False,
                    "use_structural_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "from_long",
                },
            },
        })
        # HTF bearish, bearish candle off EMA21 → sell signal; from_long should NOT flip it.
        ohlcv = _make_pullback_ohlcv(prev_close=100.5, last_close=100.0, last_high=100.4)
        service._last_full_snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=99.0, htf_ema200=101.0,
            ohlcv=ohlcv, last_price=100.0,
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["action"] == "SELL"
        # Unflipped SELL: TP=100*0.96=96, SL=100*1.03=103
        assert abs(decision["take_profit"] - 96.0) < 0.01
        assert abs(decision["stop_loss"] - 103.0) < 0.01

    def test_spike_continuation_flip_direction_and_tp_sl_reflection(self) -> None:
        """Spike Continuation flip mirrors side, TP and SL around last_price."""
        service = _make_service()
        self._setup_service_with_price(service, 106.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "spike_continuation": {
                    "enabled": True,
                    "tp_pct": 4.0,
                    "sl_pct": 3.0,
                    "volume_rsi_min": 75.0,
                    "rsi_min": 55.0,
                    "rsi_max": 75.0,
                    "require_bb_breakout": True,
                    "require_candle_strength": True,
                    "candle_strength_pct": 70.0,
                    "min_bb_bandwidth": 3.0,
                    "max_adx": 40.0,
                    "require_momentum_acceleration": True,
                    "acceleration_lookback": 3,
                    "acceleration_min_ratio": 1.5,
                    "require_rsi_rising": True,
                    "require_volume_rsi_rising": True,
                    "max_spike_extension_pct": 5.0,
                    "spike_lookback": 5,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "both",
                },
            },
        })
        # Strong accelerating bullish spike → buy signal.
        service._last_full_snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[70.0, 82.0],
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        # Spike buy signal, flipped "both" → SELL
        assert decision["action"] == "SELL"
        # Original BUY: TP=106*1.04=110.24 (above), SL=106*0.97=102.82 (below).
        # Flipped to SELL: mirror around last_price=106 →
        #   TP=2*106-110.24=101.76 (below, correct for short)
        #   SL=2*106-102.82=109.18 (above, correct for short)
        assert abs(decision["take_profit"] - 101.76) < 0.01
        assert abs(decision["stop_loss"] - 109.18) < 0.01

    def test_spike_continuation_flip_from_short_only_keeps_long(self) -> None:
        """from_short flip should leave a BUY signal unchanged."""
        service = _make_service()
        self._setup_service_with_price(service, 106.0)
        service.set_launcher_config({
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "spike_continuation": {
                    "enabled": True,
                    "tp_pct": 4.0,
                    "sl_pct": 3.0,
                    "volume_rsi_min": 75.0,
                    "rsi_min": 55.0,
                    "rsi_max": 75.0,
                    "require_bb_breakout": True,
                    "require_candle_strength": True,
                    "candle_strength_pct": 70.0,
                    "min_bb_bandwidth": 3.0,
                    "max_adx": 40.0,
                    "require_momentum_acceleration": True,
                    "acceleration_lookback": 3,
                    "acceleration_min_ratio": 1.5,
                    "require_rsi_rising": True,
                    "require_volume_rsi_rising": True,
                    "max_spike_extension_pct": 5.0,
                    "spike_lookback": 5,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "min_atr_pct": 0.0,
                    "flip_launcher_direction": "from_short",
                },
            },
        })
        # Bullish spike → buy signal; from_short should NOT flip it.
        service._last_full_snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[70.0, 82.0],
        )

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["action"] == "BUY"
        # Unflipped BUY: TP=106*1.04=110.24, SL=106*0.97=102.82
        assert abs(decision["take_profit"] - 110.24) < 0.01
        assert abs(decision["stop_loss"] - 102.82) < 0.01


# ── Spike Continuation Strategy ──────────────────────────────────────────────


def _make_spike_snapshot(
    *,
    rsi: float | None = 65.0,
    adx_value: float | None = 25.0,
    bb_lower: float | None = 95.0,
    bb_upper: float | None = 105.0,
    bb_middle: float | None = 100.0,
    last_price: float = 106.0,
    volume_rsi_series: list[float] | None = None,
    rsi_series: list[float] | None = None,
    ohlcv: list[dict[str, Any]] | None = None,
    symbol: str = "BTC-USDT-SWAP",
) -> dict[str, Any]:
    """Build a snapshot for Spike Continuation tests."""
    if volume_rsi_series is None:
        volume_rsi_series = [70.0, 82.0]  # rising by default
    if rsi_series is None:
        rsi_series = [60.0, 65.0]  # rising by default (matches rsi=65.0)
    if ohlcv is None:
        # Default: current candle is a strong bullish candle with large body.
        # Spike origin (lowest low) is at 103.0, current close at 106.5 → ~3.4% extension.
        # Keep extension under 5% so default max_spike_extension_pct=3.0 can be overridden in tests.
        ohlcv = [
            {"open": 103.0, "high": 103.5, "low": 102.8, "close": 103.2, "volume": 100.0},
            {"open": 103.2, "high": 104.0, "low": 103.0, "close": 103.8, "volume": 120.0},
            {"open": 103.8, "high": 104.5, "low": 103.5, "close": 104.3, "volume": 150.0},
            {"open": 104.3, "high": 105.0, "low": 104.0, "close": 104.8, "volume": 180.0},
            # Current candle: large body, close near high
            {"open": 104.8, "high": 107.0, "low": 104.5, "close": 106.5, "volume": 250.0},
        ]

    indicators: dict[str, Any] = {
        "rsi": rsi,
        "rsi_series": rsi_series,
        "adx": {"value": adx_value},
        "bollinger_bands": {
            "lower": bb_lower,
            "upper": bb_upper,
            "middle": bb_middle,
        },
        "volume_rsi_series": volume_rsi_series,
        "ohlcv": ohlcv,
    }

    return {
        "market_data": {
            symbol: {
                "indicators": indicators,
                "custom_metrics": {},
            }
        },
        "positions": [],
        "last_price": last_price,
    }


class TestSpikeContinuationStrategy:
    """Tests for the SpikeContinuationStrategy."""

    def test_satisfies_protocol(self) -> None:
        strategy = SpikeContinuationStrategy()
        assert isinstance(strategy, Strategy)

    def test_returns_none_when_disabled(self) -> None:
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers()
        snapshot = _make_spike_snapshot()
        config = {"enabled": False}
        assert strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers) is None

    def test_buy_signal_when_all_filters_pass(self) -> None:
        """A strong accelerating spike with rising volume RSI should fire a buy."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[70.0, 82.0],
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_bb_breakout=True,
            require_candle_strength=True,
            candle_strength_pct=70.0,
            min_bb_bandwidth=3.0,
            max_adx=40.0,
            require_momentum_acceleration=True,
            acceleration_lookback=3,
            acceleration_min_ratio=1.5,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=5.0,  # allow up to 5% extension,
            spike_lookback=5,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is not None
        assert signal.direction == "buy"
        assert signal.strategy_name == "spike_continuation"

    def test_blocks_when_momentum_decelerating(self) -> None:
        """If the current candle body is smaller than recent average, don't enter."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=111.5)
        # Current candle has a tiny body compared to recent large candles
        ohlcv = [
            {"open": 100.0, "high": 105.0, "low": 99.0, "close": 104.0, "volume": 200.0},
            {"open": 104.0, "high": 108.0, "low": 103.0, "close": 107.0, "volume": 250.0},
            {"open": 107.0, "high": 111.0, "low": 106.0, "close": 110.0, "volume": 300.0},
            {"open": 110.0, "high": 112.0, "low": 109.0, "close": 111.0, "volume": 280.0},
            # Current candle: tiny body (close near high but body is small)
            {"open": 111.0, "high": 112.0, "low": 110.5, "close": 111.5, "volume": 100.0},
        ]
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=111.5,
            volume_rsi_series=[80.0, 82.0],
            ohlcv=ohlcv,
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_momentum_acceleration=True,
            acceleration_lookback=3,
            acceleration_min_ratio=1.5,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=0,  # disable for this test,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None  # blocked — momentum decelerating

    def test_blocks_when_volume_rsi_falling(self) -> None:
        """If volume RSI is falling, don't enter — volume momentum is fading."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[88.0, 82.0],  # falling
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=0,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None

    def test_blocks_when_spike_already_extended(self) -> None:
        """If price has moved more than max_spike_extension_pct from origin, don't enter."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=112.0)
        # Spike origin (lowest low) is at 95.0, current close is at 112.0 → 17.9% extension
        ohlcv = [
            {"open": 96.0, "high": 97.0, "low": 95.0, "close": 96.5, "volume": 100.0},
            {"open": 96.5, "high": 100.0, "low": 96.0, "close": 99.5, "volume": 150.0},
            {"open": 99.5, "high": 105.0, "low": 99.0, "close": 104.5, "volume": 200.0},
            {"open": 104.5, "high": 110.0, "low": 104.0, "close": 109.5, "volume": 250.0},
            {"open": 109.5, "high": 113.0, "low": 109.0, "close": 112.0, "volume": 300.0},
        ]
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=112.0,
            volume_rsi_series=[80.0, 85.0],
            ohlcv=ohlcv,
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            max_spike_extension_pct=3.0,  # only allow 3% extension,
            spike_lookback=5,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None  # blocked — spike already extended 17.9%

    def test_blocks_when_rsi_too_extreme(self) -> None:
        """If RSI is above rsi_max, don't enter — that's Mean Reversion territory."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=85.0,  # too extreme
            last_price=106.0,
            volume_rsi_series=[80.0, 85.0],
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            max_spike_extension_pct=0,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None

    def test_blocks_when_volume_rsi_too_low(self) -> None:
        """If volume RSI is below the minimum, don't enter."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[60.0, 65.0],  # below 75
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            max_spike_extension_pct=0,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None

    def test_sell_signal_when_all_filters_pass(self) -> None:
        """A strong accelerating bearish spike should fire a sell."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=94.0)
        # Bearish spike: price below BB lower, RSI in sell zone (25-45)
        # Keep extension small: origin high at 107.0, current close at 103.5 → ~3.3%
        ohlcv = [
            {"open": 107.0, "high": 107.2, "low": 106.8, "close": 106.8, "volume": 100.0},
            {"open": 106.8, "high": 107.0, "low": 106.0, "close": 106.2, "volume": 120.0},
            {"open": 106.2, "high": 106.5, "low": 105.5, "close": 105.7, "volume": 150.0},
            {"open": 105.7, "high": 106.0, "low": 105.0, "close": 105.2, "volume": 180.0},
            # Current candle: large bearish body, close near low
            {"open": 105.2, "high": 105.5, "low": 103.0, "close": 103.5, "volume": 250.0},
        ]
        snapshot = _make_spike_snapshot(
            rsi=35.0,  # in sell zone (100-75=25 to 100-55=45)
            bb_lower=95.0,
            bb_upper=105.0,
            bb_middle=100.0,
            last_price=94.0,  # below BB lower
            volume_rsi_series=[70.0, 82.0],
            ohlcv=ohlcv,
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_bb_breakout=True,
            require_candle_strength=True,
            candle_strength_pct=70.0,
            min_bb_bandwidth=3.0,
            max_adx=40.0,
            require_momentum_acceleration=True,
            acceleration_lookback=3,
            acceleration_min_ratio=1.5,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=5.0,  # allow up to 5% extension,
            spike_lookback=5,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is not None
        assert signal.direction == "sell"

    def test_acceleration_can_be_disabled(self) -> None:
        """When require_momentum_acceleration is False, small bodies are OK."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=105.3)
        # Use candles with small extension so spike extension filter doesn't block
        ohlcv = [
            {"open": 103.0, "high": 103.5, "low": 102.8, "close": 103.2, "volume": 200.0},
            {"open": 103.2, "high": 104.0, "low": 103.0, "close": 103.8, "volume": 250.0},
            {"open": 103.8, "high": 104.5, "low": 103.5, "close": 104.3, "volume": 300.0},
            {"open": 104.3, "high": 105.0, "low": 104.0, "close": 104.8, "volume": 280.0},
            # Current candle: tiny body but close near high
            {"open": 104.8, "high": 105.5, "low": 104.5, "close": 105.3, "volume": 100.0},
        ]
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=105.3,
            volume_rsi_series=[80.0, 82.0],
            ohlcv=ohlcv,
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_momentum_acceleration=False,  # disabled,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=0,  # disabled,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is not None
        assert signal.direction == "buy"

    def test_blocks_when_rsi_falling_via_series(self) -> None:
        """If RSI is falling (via rsi_series), don't enter — momentum fading."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[70.0, 82.0],
            rsi_series=[70.0, 65.0],  # falling: prev=70, current=65
        )
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=0,  # disabled,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        assert signal is None  # blocked — RSI falling via series

    def test_rsi_rising_falls_back_to_candle_direction(self) -> None:
        """When rsi_series is unavailable, RSI rising falls back to candle direction."""
        strategy = SpikeContinuationStrategy()
        helpers = _make_helpers(last_price=106.0)
        snapshot = _make_spike_snapshot(
            rsi=65.0,
            last_price=106.0,
            volume_rsi_series=[70.0, 82.0],
            rsi_series=None,  # no series — will be removed from indicators
        )
        # Remove rsi_series to simulate unavailable data
        del snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]["rsi_series"]
        config = _sc_bare(
            volume_rsi_min=75.0,
            rsi_min=55.0,
            rsi_max=75.0,
            require_rsi_rising=True,
            require_volume_rsi_rising=True,
            max_spike_extension_pct=5.0,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, helpers)
        # Current candle is bullish (close > open) → fallback says RSI rising → buy
        assert signal is not None
        assert signal.direction == "buy"


# ── Mean Reversion HTF-absent guard ──────────────────────────────────────────


class TestMeanReversionHtfAbsent:
    """Tests for the HTF-absent auto-disable guard in Mean Reversion."""

    def test_htf_absent_auto_disables_htf_trend_filter(self) -> None:
        """require_htf_trend should auto-disable when no HTF data (e.g. 1D LTF)."""
        mr = MeanReversionStrategy()
        # Snapshot with no htf_indicators (simulates 1D LTF)
        snapshot = _make_snapshot(rsi=20.0, htf_ema50=None, htf_ema200=None)
        # Remove htf_indicators entirely to simulate 1D
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        if "htf_indicators" in indicators:
            del indicators["htf_indicators"]
        config = _mr_bare(
            require_htf_trend=True,  # would normally block without HTF,
            require_cmf=False,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_htf_absent_auto_disables_htf_cmf_filter(self) -> None:
        """require_htf_cmf should auto-disable when no HTF data."""
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0, htf_cmf=None)
        # Remove htf_indicators entirely
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        if "htf_indicators" in indicators:
            del indicators["htf_indicators"]
        config = _mr_bare(
            require_htf_cmf=True,  # would normally block without HTF,
            require_cmf=False,
            require_htf_trend=False,
        )
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"


# ── Liquidity Sweep ──────────────────────────────────────────────────────────


def _make_ls_snapshot(
    *,
    ohlcv: list[dict[str, Any]] | None = None,
    htf_ema50: float | None = 101.0,
    htf_ema200: float | None = 99.0,
    adx_value: float | None = 20.0,
    bb_lower: float | None = 95.0,
    bb_upper: float | None = 105.0,
    bb_middle: float | None = 100.0,
    atr_pct: float | None = 2.0,
    symbol: str = "BTC-USDT-SWAP",
    last_price: float = 100.0,
) -> dict[str, Any]:
    """Build a snapshot for Liquidity Sweep tests."""
    indicators: dict[str, Any] = {
        "adx": {"value": adx_value},
        "bollinger_bands": {
            "lower": bb_lower,
            "upper": bb_upper,
            "middle": bb_middle,
        },
        "atr_pct": atr_pct,
        "ohlcv": ohlcv or [],
    }
    if htf_ema50 is not None or htf_ema200 is not None:
        indicators["htf_indicators"] = {
            "moving_averages": {
                "ema_50": htf_ema50,
                "ema_200": htf_ema200,
            }
        }
    return {
        "market_data": {
            symbol: {
                "indicators": indicators,
                "custom_metrics": {},
            }
        },
        "positions": [],
        "last_price": last_price,
    }


def _ls_bare(**overrides: Any) -> dict[str, Any]:
    """LS config with default-on filters explicitly disabled for unit tests."""
    cfg: dict[str, Any] = {
        "enabled": True,
        "lookback": 10,
        "sweep_buffer_pct": 0.1,
        "reclaim_ratio": 0.5,
        "require_htf_trend": False,
        "require_volume_spike": False,
        "volume_spike_ratio": 1.5,
        "max_adx": 0.0,
        "require_regime": False,
        "max_bb_bandwidth_percentile": 60.0,
        "use_atr_sizing": False,
        "atr_tp_multiplier": 1.5,
        "atr_sl_multiplier": 1.2,
        "min_atr_pct": 0.0,
    }
    cfg.update(overrides)
    return cfg


def _make_range_ohlcv(
    n: int = 12,
    base: float = 100.0,
    range_pct: float = 2.0,
    volume: float = 100.0,
) -> list[dict[str, Any]]:
    """Generate N candles in a tight range for sweep lookback."""
    high = base * (1 + range_pct / 200)
    low = base * (1 - range_pct / 200)
    candles = []
    for i in range(n):
        candles.append({
            "open": base,
            "high": high + i * 0.001,
            "low": low - i * 0.001,
            "close": base,
            "volume": volume,
        })
    return candles


class TestLiquiditySweepStrategy:
    """Tests for the LiquiditySweepStrategy."""

    def test_satisfies_protocol(self) -> None:
        strategy = LiquiditySweepStrategy()
        assert isinstance(strategy, Strategy)

    def test_returns_none_when_disabled(self) -> None:
        strategy = LiquiditySweepStrategy()
        snapshot = _make_ls_snapshot(ohlcv=_make_range_ohlcv())
        config = {"enabled": False}
        assert strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers()) is None

    def test_returns_none_with_insufficient_candles(self) -> None:
        strategy = LiquiditySweepStrategy()
        snapshot = _make_ls_snapshot(ohlcv=_make_range_ohlcv(n=5))
        config = _ls_bare(lookback=20)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_buy_signal_on_low_sweep_with_reclaim(self) -> None:
        """Wick below swing low, close reclaims above → buy signal."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        # swing_low ≈ 99.0; sweep candle: low=98.5 (below), close=100.5 (reclaim)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=100.5)
        config = _ls_bare()
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        assert signal.direction == "buy"
        assert signal.strategy_name == "liquidity_sweep"

    def test_sell_signal_on_high_sweep_with_reclaim(self) -> None:
        """Wick above swing high, close reclaims below → sell signal."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        # swing_high ≈ 101.0; sweep candle: high=101.8 (above), close=99.5 (reclaim)
        sweep_candle = {"open": 100.5, "high": 101.8, "low": 99.2, "close": 99.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=99.5)
        config = _ls_bare()
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        assert signal.direction == "sell"

    def test_no_signal_when_no_sweep(self) -> None:
        """Candle stays inside range → no signal."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        normal_candle = {"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.2, "volume": 100.0}
        ohlcv = prior + [normal_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv)
        config = _ls_bare()
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_no_signal_when_sweep_but_no_reclaim(self) -> None:
        """Wick below swing low but close stays low → no reclaim → no signal."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 99.8, "low": 98.5, "close": 98.8, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=98.8)
        config = _ls_bare(reclaim_ratio=0.5)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_htf_trend_blocks_counter_trend_sweep(self) -> None:
        """HTF bearish should block a long sweep (counter-trend)."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, htf_ema50=99.0, htf_ema200=101.0, last_price=100.5)
        config = _ls_bare(require_htf_trend=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_htf_trend_allows_trend_aligned_sweep(self) -> None:
        """HTF bullish should allow a long sweep."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, htf_ema50=101.0, htf_ema200=99.0, last_price=100.5)
        config = _ls_bare(require_htf_trend=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        assert signal.direction == "buy"

    def test_max_adx_blocks_strong_trend(self) -> None:
        """High ADX (strong trend) should block — sweep likely real breakout."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, adx_value=45.0, last_price=100.5)
        config = _ls_bare(max_adx=35.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_volume_spike_blocks_low_volume_sweep(self) -> None:
        """Sweep with low volume should be blocked when require_volume_spike is on."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0, volume=200.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=100.5)
        config = _ls_bare(require_volume_spike=True, volume_spike_ratio=1.5)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_volume_spike_allows_high_volume_sweep(self) -> None:
        """Sweep with high volume should pass the volume spike filter."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0, volume=100.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 300.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, last_price=100.5)
        config = _ls_bare(require_volume_spike=True, volume_spike_ratio=1.5)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        assert signal.direction == "buy"

    def test_atr_sizing_produces_tp_sl(self) -> None:
        """When use_atr_sizing is on, TP/SL should be ATR-scaled."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, atr_pct=2.0, last_price=100.5)
        config = _ls_bare(use_atr_sizing=True, use_structural_sizing=False, atr_tp_multiplier=1.5, atr_sl_multiplier=1.2)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        # TP = 1.5 × 2.0% = 3.0%, SL = 1.2 × 2.0% = 2.4%
        assert signal.tp_pct is not None
        assert abs(signal.tp_pct - 3.0) < 0.01
        assert signal.sl_pct is not None
        assert abs(signal.sl_pct - 2.4) < 0.01

    def test_min_atr_pct_blocks_quiet_coins(self) -> None:
        """ATR% below min_atr_pct should block entry."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, atr_pct=0.5, last_price=100.5)
        config = _ls_bare(min_atr_pct=1.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_htf_absent_auto_disables_htf_trend(self) -> None:
        """require_htf_trend should auto-disable when no HTF data."""
        strategy = LiquiditySweepStrategy()
        prior = _make_range_ohlcv(n=11, base=100.0, range_pct=2.0)
        sweep_candle = {"open": 99.5, "high": 100.8, "low": 98.5, "close": 100.5, "volume": 200.0}
        ohlcv = prior + [sweep_candle]
        snapshot = _make_ls_snapshot(ohlcv=ohlcv, htf_ema50=None, htf_ema200=None, last_price=100.5)
        # Remove htf_indicators entirely
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        if "htf_indicators" in indicators:
            del indicators["htf_indicators"]
        config = _ls_bare(require_htf_trend=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert signal is not None
        assert signal.direction == "buy"


# ── VWAPReversionStrategy ────────────────────────────────────────────────────


def _make_vr_snapshot(
    *,
    vwap: float | None = 100.0,
    atr_pct: float | None = 2.0,
    htf_ema50: float | None = 101.0,
    htf_ema200: float | None = 99.0,
    bb_lower: float | None = 95.0,
    bb_upper: float | None = 105.0,
    bb_middle: float | None = 100.0,
    ohlcv: list[dict[str, Any]] | None = None,
    symbol: str = "BTC-USDT-SWAP",
    last_price: float = 100.0,
) -> dict[str, Any]:
    """Build a snapshot for VWAP Reversion tests."""
    indicators: dict[str, Any] = {
        "vwap": vwap,
        "atr_pct": atr_pct,
        "bollinger_bands": {
            "lower": bb_lower,
            "upper": bb_upper,
            "middle": bb_middle,
        },
        "ohlcv": ohlcv or [],
    }
    if htf_ema50 is not None or htf_ema200 is not None:
        indicators["htf_indicators"] = {
            "moving_averages": {
                "ema_50": htf_ema50,
                "ema_200": htf_ema200,
            }
        }
    return {
        "market_data": {
            symbol: {
                "indicators": indicators,
                "custom_metrics": {},
            }
        },
        "positions": [],
        "last_price": last_price,
    }


def _vr_bare(**overrides: Any) -> dict[str, Any]:
    """VWAP Reversion config with default-on filters explicitly disabled for unit tests."""
    cfg: dict[str, Any] = {
        "enabled": True,
        "vwap_min_distance_atr": 2.0,
        "require_closeback": False,
        "require_htf_trend": False,
        "require_regime": False,
        "max_bb_bandwidth_percentile": 55.0,
        "use_atr_sizing": False,
        "atr_tp_multiplier": 1.5,
        "atr_sl_multiplier": 2.5,
        "min_atr_pct": 0.0,
    }
    cfg.update(overrides)
    return cfg


def _make_trend_ohlcv(n: int = 30, base: float = 100.0) -> list[dict[str, Any]]:
    """Generate N candles with closes rising slowly (for BB bandwidth percentile)."""
    candles = []
    for i in range(n):
        c = base + i * 0.01
        candles.append({
            "open": c - 0.05,
            "high": c + 0.1,
            "low": c - 0.1,
            "close": c,
            "volume": 100.0,
        })
    return candles


class TestVWAPReversionStrategy:
    """Tests for the VWAPReversionStrategy."""

    def test_satisfies_protocol(self) -> None:
        strategy = VWAPReversionStrategy()
        assert isinstance(strategy, Strategy)

    def test_returns_none_when_disabled(self) -> None:
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(ohlcv=_make_trend_ohlcv(), last_price=94.0)
        config = {"enabled": False}
        assert strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers()) is None

    def test_returns_none_when_no_enabled_key(self) -> None:
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(ohlcv=_make_trend_ohlcv(), last_price=94.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, {}, _make_helpers())
        assert result is None

    def test_returns_none_when_vwap_unavailable(self) -> None:
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(vwap=None, ohlcv=_make_trend_ohlcv(), last_price=94.0)
        config = _vr_bare()
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_returns_none_when_atr_unavailable(self) -> None:
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(atr_pct=None, ohlcv=_make_trend_ohlcv(), last_price=94.0)
        config = _vr_bare()
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_buy_signal_when_extended_below_vwap(self) -> None:
        """Price 2.5 ATR below VWAP → buy signal (no closeback required)."""
        strategy = VWAPReversionStrategy()
        # VWAP=100, ATR%=2% → ATR_price=2.0. Price=95 → distance=5 → 2.5 ATR.
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(vwap_min_distance_atr=2.0, require_closeback=False)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        assert signal.direction == "buy"
        assert signal.strategy_name == "vwap_reversion"

    def test_sell_signal_when_extended_above_vwap(self) -> None:
        """Price 2.5 ATR above VWAP → sell signal (no closeback required)."""
        strategy = VWAPReversionStrategy()
        # VWAP=100, ATR%=2% → ATR_price=2.0. Price=105 → distance=5 → 2.5 ATR.
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=105.0,
        )
        config = _vr_bare(vwap_min_distance_atr=2.0, require_closeback=False)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=105.0))
        assert signal is not None
        assert signal.direction == "sell"

    def test_no_signal_when_not_extended(self) -> None:
        """Price within min_distance ATR of VWAP → no signal."""
        strategy = VWAPReversionStrategy()
        # VWAP=100, ATR%=2% → ATR_price=2.0. Price=99 → distance=1 → 0.5 ATR.
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=99.0,
        )
        config = _vr_bare(vwap_min_distance_atr=2.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=99.0))
        assert result is None

    def test_closeback_blocks_when_closing_away_from_vwap(self) -> None:
        """Price below VWAP but closing down (away from VWAP) → no buy."""
        strategy = VWAPReversionStrategy()
        ohlcv = _make_trend_ohlcv()
        # Override last two candles: prev close=95.5, curr close=95.0 (closing down).
        ohlcv[-2] = {"open": 95.6, "high": 95.8, "low": 95.2, "close": 95.5, "volume": 100.0}
        ohlcv[-1] = {"open": 95.4, "high": 95.6, "low": 94.8, "close": 95.0, "volume": 100.0}
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=ohlcv, last_price=95.0,
        )
        config = _vr_bare(vwap_min_distance_atr=2.0, require_closeback=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert result is None

    def test_closeback_allows_when_closing_toward_vwap(self) -> None:
        """Price below VWAP and closing up (toward VWAP) → buy."""
        strategy = VWAPReversionStrategy()
        ohlcv = _make_trend_ohlcv()
        # prev close=94.5, curr close=95.0 (closing up toward VWAP=100).
        ohlcv[-2] = {"open": 94.6, "high": 94.8, "low": 94.2, "close": 94.5, "volume": 100.0}
        ohlcv[-1] = {"open": 94.6, "high": 95.2, "low": 94.4, "close": 95.0, "volume": 100.0}
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=ohlcv, last_price=95.0,
        )
        config = _vr_bare(vwap_min_distance_atr=2.0, require_closeback=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        assert signal.direction == "buy"

    def test_htf_trend_blocks_counter_trend_long(self) -> None:
        """HTF bearish should block a long (counter-trend)."""
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, htf_ema50=99.0, htf_ema200=101.0,
            ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(require_htf_trend=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert result is None

    def test_htf_trend_allows_trend_aligned_long(self) -> None:
        """HTF bullish should allow a long."""
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(require_htf_trend=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        assert signal.direction == "buy"

    def test_htf_absent_auto_disables_htf_trend(self) -> None:
        """require_htf_trend should auto-disable when no HTF data."""
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, htf_ema50=None, htf_ema200=None,
            ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        if "htf_indicators" in indicators:
            del indicators["htf_indicators"]
        config = _vr_bare(require_htf_trend=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        assert signal.direction == "buy"

    def test_min_atr_pct_blocks_quiet_coins(self) -> None:
        """ATR% below min_atr_pct should block entry."""
        strategy = VWAPReversionStrategy()
        # VWAP=100, ATR%=0.5% → ATR_price=0.5. Price=95 → distance=5 → 10 ATR (extended).
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=0.5, ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(min_atr_pct=1.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert result is None

    def test_atr_sizing_produces_tp_sl(self) -> None:
        """When use_atr_sizing is on, TP/SL should be ATR-scaled."""
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(use_atr_sizing=True, atr_tp_multiplier=1.5, atr_sl_multiplier=2.5)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        # TP = 1.5 × 2.0% = 3.0%, SL = 2.5 × 2.0% = 5.0%
        assert signal.tp_pct is not None
        assert abs(signal.tp_pct - 3.0) < 0.01
        assert signal.sl_pct is not None
        assert abs(signal.sl_pct - 5.0) < 0.01

    def test_static_tp_sl_when_atr_sizing_off(self) -> None:
        """When use_atr_sizing is off, static tp_pct/sl_pct should be used."""
        strategy = VWAPReversionStrategy()
        snapshot = _make_vr_snapshot(
            vwap=100.0, atr_pct=2.0, ohlcv=_make_trend_ohlcv(), last_price=95.0,
        )
        config = _vr_bare(use_atr_sizing=False, tp_pct=2.0, sl_pct=3.0)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert signal is not None
        assert signal.tp_pct == 2.0
        assert signal.sl_pct == 3.0


# ── TrendPullbackStrategy ────────────────────────────────────────────────────


def _make_tp_snapshot(
    *,
    ema_21: float | None = 100.0,
    vwap: float | None = 100.0,
    atr_pct: float | None = 2.0,
    adx_value: float | None = 25.0,
    htf_ema50: float | None = 101.0,
    htf_ema200: float | None = 99.0,
    ohlcv: list[dict[str, Any]] | None = None,
    symbol: str = "BTC-USDT-SWAP",
    last_price: float = 100.0,
) -> dict[str, Any]:
    """Build a snapshot for Trend Pullback tests."""
    indicators: dict[str, Any] = {
        "atr_pct": atr_pct,
        "adx": {"value": adx_value},
        "vwap": vwap,
        "moving_averages": {
            "ema_21": ema_21,
        },
        "ohlcv": ohlcv or [],
    }
    if htf_ema50 is not None or htf_ema200 is not None:
        indicators["htf_indicators"] = {
            "moving_averages": {
                "ema_50": htf_ema50,
                "ema_200": htf_ema200,
            }
        }
    return {
        "market_data": {
            symbol: {
                "indicators": indicators,
                "custom_metrics": {},
            }
        },
        "positions": [],
        "last_price": last_price,
    }


def _tp_bare(**overrides: Any) -> dict[str, Any]:
    """Trend Pullback config with default-on filters explicitly disabled for unit tests."""
    cfg: dict[str, Any] = {
        "enabled": True,
        "pullback_ema": 21,
        "use_vwap_as_level": False,
        "pullback_proximity_pct": 0.5,
        "require_htf_trend": False,
        "require_bullish_candle": False,
        "candle_rejection_pct": 25.0,
        "min_adx": 0.0,
        "max_adx_for_entry": 0.0,
        "use_atr_sizing": False,
        "atr_tp_multiplier": 2.0,
        "atr_sl_multiplier": 1.5,
        "min_atr_pct": 0.0,
    }
    cfg.update(overrides)
    return cfg


def _make_pullback_ohlcv(
    n: int = 30,
    base: float = 100.0,
    last_close: float | None = None,
    last_open: float | None = None,
    last_high: float | None = None,
    last_low: float | None = None,
    prev_close: float | None = None,
) -> list[dict[str, Any]]:
    """Generate N candles; the last two are controllable for candle confirmation."""
    candles = []
    for i in range(n - 2):
        c = base + i * 0.01
        candles.append({
            "open": c - 0.05, "high": c + 0.1, "low": c - 0.1,
            "close": c, "volume": 100.0,
        })
    # Second-to-last candle (prev).
    pc = prev_close if prev_close is not None else base + (n - 2) * 0.01
    candles.append({
        "open": pc - 0.05, "high": pc + 0.1, "low": pc - 0.1,
        "close": pc, "volume": 100.0,
    })
    # Last candle (curr) — defaults to a bullish candle off the level.
    _open = last_open if last_open is not None else 99.7
    _high = last_high if last_high is not None else 100.2
    _low = last_low if last_low is not None else 99.6
    _close = last_close if last_close is not None else 100.0
    candles.append({
        "open": _open, "high": _high, "low": _low,
        "close": _close, "volume": 100.0,
    })
    return candles


class TestTrendPullbackStrategy:
    """Tests for the TrendPullbackStrategy."""

    def test_satisfies_protocol(self) -> None:
        strategy = TrendPullbackStrategy()
        assert isinstance(strategy, Strategy)

    def test_returns_none_when_disabled(self) -> None:
        strategy = TrendPullbackStrategy()
        snapshot = _make_tp_snapshot(ohlcv=_make_pullback_ohlcv(), last_price=100.0)
        config = {"enabled": False}
        assert strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0)) is None

    def test_returns_none_when_no_enabled_key(self) -> None:
        strategy = TrendPullbackStrategy()
        snapshot = _make_tp_snapshot(ohlcv=_make_pullback_ohlcv(), last_price=100.0)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, {}, _make_helpers(last_price=100.0))
        assert result is None

    def test_buy_signal_on_pullback_to_ema_in_uptrend(self) -> None:
        """Price pulls back to EMA21 in HTF uptrend → buy signal."""
        strategy = TrendPullbackStrategy()
        # EMA21=100, price=100 (touching), HTF bullish, bullish candle.
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        assert signal.direction == "buy"
        assert signal.strategy_name == "trend_pullback"

    def test_sell_signal_on_pullback_to_ema_in_downtrend(self) -> None:
        """Price pulls back to EMA21 in HTF downtrend → sell signal."""
        strategy = TrendPullbackStrategy()
        # Bearish candle: close < prev close, upper wick.
        ohlcv = _make_pullback_ohlcv(
            prev_close=100.5, last_open=100.3, last_high=100.4,
            last_low=99.8, last_close=100.0,
        )
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=99.0, htf_ema200=101.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        assert signal.direction == "sell"

    def test_no_signal_when_not_at_pullback_level(self) -> None:
        """Price far from EMA21/VWAP → no pullback → no signal."""
        strategy = TrendPullbackStrategy()
        snapshot = _make_tp_snapshot(
            ema_21=100.0, vwap=100.0, ohlcv=_make_pullback_ohlcv(), last_price=95.0,
        )
        config = _tp_bare(pullback_proximity_pct=0.5)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=95.0))
        assert result is None

    def test_vwap_also_qualifies_as_level(self) -> None:
        """When use_vwap_as_level is True, touching VWAP qualifies."""
        strategy = TrendPullbackStrategy()
        # EMA21 far away (105), VWAP=100, price=100 → VWAP touched.
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=105.0, vwap=100.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(use_vwap_as_level=True, require_htf_trend=True, require_bullish_candle=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        assert signal.direction == "buy"

    def test_htf_trend_blocks_counter_trend(self) -> None:
        """HTF bearish should block a long pullback (counter-trend)."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=99.0, htf_ema200=101.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None

    def test_htf_flat_blocks_when_required(self) -> None:
        """HTF EMA50 == EMA200 (flat) should block when require_htf_trend is on."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=100.0, htf_ema200=100.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None

    def test_htf_absent_auto_disables_htf_trend(self) -> None:
        """require_htf_trend should auto-disable when no HTF data."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=None, htf_ema200=None,
            ohlcv=ohlcv, last_price=100.0,
        )
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        if "htf_indicators" in indicators:
            del indicators["htf_indicators"]
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        assert signal.direction == "buy"

    def test_bullish_candle_blocks_bearish_close(self) -> None:
        """A bearish close (close < prev close) should block a long entry."""
        strategy = TrendPullbackStrategy()
        # close=99.5 < prev_close=100.0 → bearish, no lower wick confirmation.
        ohlcv = _make_pullback_ohlcv(
            prev_close=100.0, last_open=99.8, last_high=100.0,
            last_low=99.4, last_close=99.5,
        )
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=99.5,
        )
        config = _tp_bare(require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=99.5))
        assert result is None

    def test_min_adx_blocks_chop(self) -> None:
        """ADX below min_adx should block — not a real trend."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, adx_value=10.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(min_adx=18.0, require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None

    def test_max_adx_blocks_extended_trend(self) -> None:
        """ADX above max_adx_for_entry should block — trend extended."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, adx_value=50.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(max_adx_for_entry=40.0, require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None

    def test_min_atr_pct_blocks_quiet_coins(self) -> None:
        """ATR% below min_atr_pct should block entry."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, atr_pct=0.5, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(min_atr_pct=1.0, require_htf_trend=True, require_bullish_candle=True)
        result = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert result is None

    def test_atr_sizing_produces_tp_sl(self) -> None:
        """When use_atr_sizing is on, TP/SL should be ATR-scaled."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, atr_pct=2.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(
            use_atr_sizing=True, use_structural_sizing=False,
            atr_tp_multiplier=2.0, atr_sl_multiplier=1.5,
            require_htf_trend=True, require_bullish_candle=True,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        # TP = 2.0 × 2.0% = 4.0%, SL = 1.5 × 2.0% = 3.0%
        assert signal.tp_pct is not None
        assert abs(signal.tp_pct - 4.0) < 0.01
        assert signal.sl_pct is not None
        assert abs(signal.sl_pct - 3.0) < 0.01

    def test_static_tp_sl_when_atr_sizing_off(self) -> None:
        """When use_atr_sizing is off, static tp_pct/sl_pct should be used."""
        strategy = TrendPullbackStrategy()
        ohlcv = _make_pullback_ohlcv(prev_close=99.5, last_close=100.0, last_low=99.6)
        snapshot = _make_tp_snapshot(
            ema_21=100.0, htf_ema50=101.0, htf_ema200=99.0,
            ohlcv=ohlcv, last_price=100.0,
        )
        config = _tp_bare(
            use_atr_sizing=False, use_structural_sizing=False,
            tp_pct=4.0, sl_pct=3.0,
            require_htf_trend=True, require_bullish_candle=True,
        )
        signal = strategy.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers(last_price=100.0))
        assert signal is not None
        assert signal.tp_pct == 4.0
        assert signal.sl_pct == 3.0
