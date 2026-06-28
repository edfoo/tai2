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


# ── Helpers ──────────────────────────────────────────────────────────────────


class _DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _make_helpers() -> StrategyHelpers:
    """Create a StrategyHelpers with mock functions."""
    return StrategyHelpers(
        extract_float=MarketService._extract_float,
        emit_debug=lambda msg: None,
        get_last_price=lambda symbol: 100.0,
        compute_footprint=lambda symbol: {},
    )


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
        config = {
            "enabled": True,
            "rsi_oversold": 30.0,
            "rsi_overbought": 70.0,
            "require_cmf": True,
            "require_htf_trend": True,
        }
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_sell_signal_when_rsi_overbought(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=80.0, cmf_value=-0.1, htf_ema50=99.0, htf_ema200=101.0)
        config = {
            "enabled": True,
            "rsi_oversold": 30.0,
            "rsi_overbought": 70.0,
            "require_cmf": True,
            "require_htf_trend": True,
        }
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "sell"

    def test_no_signal_when_rsi_neutral(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=50.0)
        config = {"enabled": True}
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
        config = {
            "enabled": True,
            "rsi_oversold": 30.0,
            "require_cmf": False,
            "require_htf_trend": False,
        }
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
        config = {"enabled": True, "require_bb_position": True, "bb_proximity_pct": 1.0,
                  "require_cmf": False, "require_htf_trend": False}
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
        config = {"enabled": True}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_htf_cmf_filter(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold, HTF CMF positive → buy
        snapshot = _make_snapshot(rsi=20.0, htf_cmf=0.1)
        config = {"enabled": True, "require_htf_cmf": True, "require_cmf": False, "require_htf_trend": False}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_htf_cmf_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        # RSI oversold, HTF CMF negative → no buy
        snapshot = _make_snapshot(rsi=20.0, htf_cmf=-0.1)
        config = {"enabled": True, "require_htf_cmf": True, "require_cmf": False, "require_htf_trend": False}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is None

    def test_cmf_cross_filter(self) -> None:
        mr = MeanReversionStrategy()
        # CMF crossed up: prev=-0.1, current=0.1
        snapshot = _make_snapshot(rsi=20.0, cmf_value=0.1, cmf_series=[-0.3, -0.2, -0.1, -0.1, 0.1])
        config = {"enabled": True, "require_cmf_cross": True, "require_cmf": False, "require_htf_trend": False}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_cmf_cross_filter_blocks_when_no_cross(self) -> None:
        mr = MeanReversionStrategy()
        # CMF stayed positive: prev=0.05, current=0.1 → no cross
        snapshot = _make_snapshot(rsi=20.0, cmf_value=0.1, cmf_series=[0.01, 0.02, 0.03, 0.05, 0.1])
        config = {"enabled": True, "require_cmf_cross": True, "require_cmf": False, "require_htf_trend": False}
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
        # RSI=34 → below default oversold of 35 → buy (with no other filters)
        snapshot = _make_snapshot(rsi=34.0)
        config = {"enabled": True, "require_cmf": False, "require_htf_trend": False}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_footprint_delta_filter(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        snapshot["market_data"]["BTC-USDT-SWAP"]["custom_metrics"]["footprint"] = {"net_delta": 50.0}
        config = {"enabled": True, "require_footprint_delta": True, "require_cmf": False, "require_htf_trend": False}
        result = mr.evaluate("BTC-USDT-SWAP", snapshot, config, _make_helpers())
        assert result is not None
        assert result.direction == "buy"

    def test_footprint_delta_filter_blocks(self) -> None:
        mr = MeanReversionStrategy()
        snapshot = _make_snapshot(rsi=20.0)
        snapshot["market_data"]["BTC-USDT-SWAP"]["custom_metrics"]["footprint"] = {"net_delta": -50.0}
        config = {"enabled": True, "require_footprint_delta": True, "require_cmf": False, "require_htf_trend": False}
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
        config = {"enabled": True, "require_footprint_delta": True, "require_cmf": False, "require_htf_trend": False}
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
                    "rsi_oversold": 10.0,  # very low, RSI=50 won't trigger
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
                    # No tp_pct/sl_pct in strategy config → fallback to launcher level
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
                }
            },
        })
        service._last_full_snapshot = _make_snapshot(rsi=20.0)

        decisions = service.build_launcher_decisions("BTC-USDT-SWAP")
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision["_decision_origin"] == "launcher"
