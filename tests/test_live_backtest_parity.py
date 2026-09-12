"""Cross-path parity regression tests (Phase 6).

Feeds an identical synthetic signal + snapshot through the *live* launcher
decision path (``MarketService.build_launcher_decisions``) and the *backtest*
path (``BacktestEngine._compute_tp_sl``) and asserts identical TP/SL/direction
output.  This pins the two callers to the shared ``resolve_launcher_tp_sl`` so
a future edit to one side cannot silently diverge.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.services.backtest.engine import BacktestEngine
from app.services.backtest.models import BacktestConfig
from app.services.market_service import MarketService
from app.services.strategies import StrategySignal


class _DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


SYMBOL = "BTC-USDT-SWAP"


def _make_service() -> MarketService:
    return MarketService(
        state_service=_DummyStateService(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )


def _make_snapshot(
    *,
    rsi: float = 20.0,
    bb_lower: float = 95.0,
    bb_upper: float = 105.0,
    bb_middle: float = 100.0,
) -> dict[str, Any]:
    """Minimal snapshot with fields the MR strategy + dynamic-TP read."""
    return {
        "market_data": {
            SYMBOL: {
                "indicators": {
                    "rsi": rsi,
                    "cmf_14": {"value": 0.0},
                    "adx": {"value": 20.0},
                    "bollinger_bands": {
                        "lower": bb_lower,
                        "upper": bb_upper,
                        "middle": bb_middle,
                    },
                },
                "custom_metrics": {},
            },
        },
        "positions": [],
    }


def _mr_bare_config(**overrides: Any) -> dict[str, Any]:
    """Mean-reversion config with filters disabled so a bare snapshot fires."""
    cfg: dict[str, Any] = {
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
        "use_structural_sizing": False,
        "max_adx": 0.0,
        "min_atr_pct": 0.0,
        "min_bb_bandwidth": 0.0,
    }
    cfg.update(overrides)
    return cfg


def _live_decisions(
    service: MarketService,
    *,
    snapshot: dict[str, Any],
    launcher_config: dict[str, Any],
    guardrails_config: dict[str, Any] | None = None,
    last_price: float = 100.0,
) -> tuple[StrategySignal, dict[str, Any]]:
    service.set_launcher_config(launcher_config)
    if guardrails_config is not None:
        service.set_guardrails(guardrails_config)
    service._last_full_snapshot = snapshot
    service._latest_ticker[SYMBOL] = {"last": str(last_price), "px": str(last_price)}
    signals = service._launcher_evaluate_signals(SYMBOL)
    assert len(signals) == 1, f"expected one signal, got {signals}"
    decisions = service.build_launcher_decisions(SYMBOL)
    assert len(decisions) == 1, f"expected one decision, got {decisions}"
    return signals[0], decisions[0]


def _backtest_tp_sl(
    *,
    signal: StrategySignal,
    snapshot: dict[str, Any],
    launcher_config: dict[str, Any],
    last_price: float = 100.0,
) -> tuple[str, float | None, float | None]:
    engine = BacktestEngine(
        BacktestConfig(
            symbols=[SYMBOL],
            timeframe="15m",
            start_ts=0,
            end_ts=1000,
            strategy_names=["mean_reversion"],
            launcher_config=launcher_config,
        )
    )
    resolved = engine._compute_tp_sl(signal, last_price, launcher_config, snapshot=snapshot)
    return resolved.action, resolved.tp_price, resolved.sl_price


def _assert_parity(
    *,
    launcher_config: dict[str, Any],
    snapshot: dict[str, Any],
    guardrails_config: dict[str, Any] | None = None,
    last_price: float = 100.0,
) -> None:
    service = _make_service()
    signal, live = _live_decisions(
        service,
        snapshot=snapshot,
        launcher_config=launcher_config,
        guardrails_config=guardrails_config,
        last_price=last_price,
    )

    # Feed the exact signal the live path produced to the backtest resolver.
    action, tp, sl = _backtest_tp_sl(
        signal=signal,
        snapshot=snapshot,
        launcher_config=launcher_config,
        last_price=last_price,
    )

    assert action == live["action"], f"action drift: {action} != {live['action']}"
    assert (tp is None and live["take_profit"] is None) or abs(tp - live["take_profit"]) < 1e-6, \
        f"tp drift: {tp} != {live['take_profit']}"
    assert (sl is None and live["stop_loss"] is None) or abs(sl - live["stop_loss"]) < 1e-6, \
        f"sl drift: {sl} != {live['stop_loss']}"


class TestLiveBacktestParity:
    def test_static_tp_sl_parity(self) -> None:
        launcher_config = {
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": _mr_bare_config(tp_pct=3.0, sl_pct=5.0),
            },
        }
        service = _make_service()
        _signal, live = _live_decisions(
            service,
            snapshot=_make_snapshot(),
            launcher_config=launcher_config,
        )
        # Pin a concrete (non-None) TP/SL so a "both sides return None" drift
        # cannot trivially pass the parity check.
        assert live["take_profit"] is not None
        assert live["stop_loss"] is not None
        assert abs(live["take_profit"] - 103.0) < 1e-6
        assert abs(live["stop_loss"] - 95.0) < 1e-6
        _assert_parity(launcher_config=launcher_config, snapshot=_make_snapshot())

    def test_dynamic_tp_parity(self) -> None:
        launcher_config = {
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": _mr_bare_config(
                    tp_pct=10.0,
                    dynamic_tp=True,
                    dynamic_tp_fraction=0.7,
                ),
            },
        }
        # BB bandwidth = 10% → dyn TP = 3.5% → effective = min(10, 3.5) = 3.5%.
        _assert_parity(launcher_config=launcher_config, snapshot=_make_snapshot())

    def test_flip_direction_parity(self) -> None:
        launcher_config = {
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "strategies": {
                "mean_reversion": _mr_bare_config(
                    tp_pct=3.0,
                    sl_pct=5.0,
                    flip_launcher_direction="both",
                ),
            },
        }
        _assert_parity(launcher_config=launcher_config, snapshot=_make_snapshot())

    def test_pnl_pct_mode_parity(self) -> None:
        launcher_config = {
            "mode": "launcher_only",
            "notional_usd": 50.0,
            "tp_sl_in_pnl_pct": True,
            "strategies": {
                "mean_reversion": _mr_bare_config(tp_pct=30.0, sl_pct=50.0),
            },
            "guardrails": {"max_leverage": 10.0},
        }
        # PnL% mode: tp_pct/sl_pct divided by leverage (10x) → 3% / 5%.
        guardrails = {"max_leverage": 10.0}
        _assert_parity(
            launcher_config=launcher_config,
            snapshot=_make_snapshot(),
            guardrails_config=guardrails,
        )