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

# ── wick_rejection (plan Phase 8) ─────────────────────────────────────────────
#
# The real strategy on one hourly fixture, through the live decision path and
# the backtest resolver: same signal, same TP/SL prices, leverage, margin,
# notional and strategy-owned exit params.  Exit timing parity: the simulator
# closes on the first candle where the shared ``compute_strategy_exit`` says
# so, and its held time equals the time since the fill.

from app.services.backtest.models import Candle  # noqa: E402
from app.services.backtest.simulator import Simulator  # noqa: E402
from app.services.strategies.defaults import strategy_defaults  # noqa: E402
from app.services.strategies.wick_rejection import WickRejectionStrategy  # noqa: E402
from app.services.trade_management_rules import (  # noqa: E402
    StrategyExitParams,
    compute_strategy_exit,
)

_H = 3_600_000
_T0 = (1_700_000_000_000 // (24 * _H)) * 24 * _H
_WR_PRICE = 102.8


def _wr_snapshot(equity: float = 1000.0) -> dict[str, Any]:
    closed: list[dict[str, Any]] = []
    price = 100.0
    for i in range(12):
        o = price
        c = o + (1.0 if i == 10 else (0.2 if i % 2 == 0 else -0.2))
        closed.append({"ts": _T0 + i * _H, "open": o, "high": max(o, c) + 0.05,
                       "low": min(o, c) - 0.05, "close": c, "volume": 1.0})
        price = c
    f_ts = _T0 + 12 * _H
    forming = {"ts": f_ts, "open": price, "high": 103.0, "low": price - 0.05,
               "close": _WR_PRICE, "volume": 1.0}
    return {
        "as_of_ts": f_ts + _H - 40_000,
        "market_data": {SYMBOL: {"indicators": {}, "timeframes": {
            "1H": {"ohlcv": closed, "forming_candle": forming}}}},
        "positions": [],
        "account": {"total_eq_usd": equity},
    }


_WR_GUARDS = {"min_leverage": 1.0, "max_leverage": 10.0}


@pytest.mark.parametrize(
    "over,guards",
    [
        ({}, _WR_GUARDS),
        ({"emergency_stop_mode": "software"}, _WR_GUARDS),
        ({"emergency_stop_mode": "off"}, _WR_GUARDS),
        ({"tp_unit": "margin", "tp_pct": 2.5, "emergency_stop_unit": "price",
          "emergency_stop_pct": 6.0}, _WR_GUARDS),
        ({}, {"min_leverage": 1.0, "max_leverage": 3.0}),          # leverage clamp
        ({"margin_mode": "pct_equity", "margin_pct": 5.0}, _WR_GUARDS),
    ],
)
def test_wick_rejection_decision_parity(
    monkeypatch: pytest.MonkeyPatch, over: dict[str, Any], guards: dict[str, Any]
) -> None:
    cfg = {**strategy_defaults("wick_rejection"), "enabled": True, **over}
    launcher = {"mode": "launcher_only", "strategies": {"wick_rejection": cfg}}
    snapshot = _wr_snapshot()

    service = _make_service()
    service._strategies = [WickRejectionStrategy()]
    service._last_full_snapshot = snapshot
    monkeypatch.setattr(service, "_build_closed_candle_snapshot", lambda snap, sym, **k: snap)
    monkeypatch.setattr(service, "_strategy_allowed_on_symbol", lambda name, sym: True)
    monkeypatch.setattr(service, "get_last_price", lambda sym: _WR_PRICE)
    service.set_launcher_config(launcher)
    service.set_guardrails(dict(guards))

    signals = service._launcher_evaluate_signals(SYMBOL)
    assert [s.strategy_name for s in signals] == ["wick_rejection"]
    decisions = service.build_launcher_decisions(SYMBOL)
    assert len(decisions) == 1
    live = decisions[0]

    # Backtest: the strategy on the same snapshot, then the engine resolver.
    bt_signal = WickRejectionStrategy().evaluate(
        SYMBOL, snapshot, cfg, _bt_helpers(),
    )
    assert bt_signal is not None
    assert (bt_signal.direction, bt_signal.tp_pct, bt_signal.sl_pct, bt_signal.rank_score) == (
        signals[0].direction, signals[0].tp_pct, signals[0].sl_pct, signals[0].rank_score
    )
    engine = BacktestEngine(BacktestConfig(
        symbols=[SYMBOL], timeframe="1H", start_ts=0, end_ts=1000,
        strategy_names=["wick_rejection"], launcher_config=launcher,
        guardrails_config=dict(guards), initial_capital=1000.0,
    ))
    bt = engine._compute_tp_sl(bt_signal, _WR_PRICE, launcher, snapshot=snapshot)

    assert bt.action == live["action"] == "SELL"
    for got, want in ((bt.tp_price, live["take_profit"]), (bt.sl_price, live["stop_loss"])):
        assert (got is None and want is None) or got == pytest.approx(want)
    assert bt.leverage_override == live["_leverage_override"]
    assert bt.margin_usd == pytest.approx(live["_margin_usd"])
    assert bt.notional_override == pytest.approx(live["notional_usd"])
    assert (bt.strategy_exit.to_dict() if bt.strategy_exit else None) == live["_strategy_exit"]


def _bt_helpers():
    from app.services.strategies import StrategyHelpers

    return StrategyHelpers(extract_float=MarketService._extract_float,
                           emit_debug=lambda m: None, get_last_price=lambda s: None)


@pytest.mark.parametrize("stop_pct", [None, 7.0])
def test_wick_rejection_exit_timing_parity(stop_pct: float | None) -> None:
    """Simulator exits on the first 1m close where the shared rule says close."""
    params = StrategyExitParams(max_hold_seconds=3600.0, software_stop_pct=stop_pct)
    sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0,
                    strategy_config={"guardrails": {"max_leverage": 10.0}},
                    timeframe_seconds=60.0)
    entry_ts = 0
    pos = sim.open_position(
        symbol=SYMBOL, direction="short", entry_price=100.0, entry_ts=entry_ts,
        tp_price=90.0, sl_price=None, strategy_name="wick_rejection",
        notional=100.0, leverage=5.0, margin=20.0, strategy_exit=params,
    )
    assert pos is not None
    # Drift up 0.15 per minute: with a stop the emergency stop hits first.
    expected = None
    for i in range(1, 90):
        o = 100.0 + 0.15 * (i - 1)
        c = o + 0.15
        ts = entry_ts + i * 60_000
        rule = compute_strategy_exit(params, is_long=False, entry_price=100.0,
                                     held_seconds=(ts - entry_ts) / 1000.0, high=c, low=o)
        if expected is None and rule.close:
            expected = (ts, rule.reason)
        sim.update_multi({SYMBOL: Candle(ts=ts, open=o, high=c, low=o, close=c, volume=1.0)})
        if not pos.is_open:
            break
    assert expected is not None
    assert (pos.close_ts, pos.close_reason) == expected


# ── wick_rejection wake profile: live vs backtest inputs ──────────────────────
#
# Live attaches the contract size from ``_instrument_specs`` and the 1m candles
# the trigger fetched (raw OKX rows, newest first); the backtest attaches the
# spec from its config and its recent 1m eval candles.  Same snapshot, same
# candles → same decision and the same measured profile.

from collections import deque  # noqa: E402


def _wake_snapshot() -> dict[str, Any]:
    closed: list[dict[str, Any]] = []
    for i in range(60):
        o = 100.0
        c = o + (0.2 if i % 2 == 0 else -0.2)
        closed.append({"ts": _T0 + i * _H, "open": o, "high": max(o, c) + 0.05,
                       "low": min(o, c) - 0.05, "close": c,
                       "volume": 500.0 if i >= 57 else 100.0})
    f_ts = _T0 + 60 * _H
    forming = {"ts": f_ts, "open": 100.0, "high": 104.0, "low": 99.95,
               "close": 103.0, "volume": 1000.0}
    return {
        "as_of_ts": f_ts + _H - 40_000,
        "market_data": {SYMBOL: {"indicators": {}, "timeframes": {
            "1H": {"ohlcv": closed, "forming_candle": forming}}}},
        "positions": [],
        "account": {"total_eq_usd": 1000.0},
    }


@pytest.mark.parametrize("ct_val,fires", [(10.0, True), (100.0, False)])  # 100k vs 1M USDT/h baseline
def test_wick_rejection_wake_profile_parity(
    monkeypatch: pytest.MonkeyPatch, ct_val: float, fires: bool
) -> None:
    cfg = {**strategy_defaults("wick_rejection"), "enabled": True, "wake_profile_enabled": True,
           "min_big_bodies": 0, "max_high_offset_pct": 100.0}
    launcher = {"mode": "launcher_only", "strategies": {"wick_rejection": cfg}}
    snapshot = _wake_snapshot()
    f_ts = _T0 + 60 * _H
    minutes = [Candle(ts=f_ts + k * 60_000, open=103.0, high=103.0 * 1.006, low=103.0,
                      close=103.0, volume=5.0) for k in range(43, 59)]
    okx_rows = [[str(c.ts), str(c.open), str(c.high), str(c.low), str(c.close), "5", "0", "0", "1"]
                for c in reversed(minutes)]

    service = _make_service()
    service._strategies = [WickRejectionStrategy()]
    service._last_full_snapshot = snapshot
    service._instrument_specs[SYMBOL] = {"ct_val": ct_val}
    monkeypatch.setattr(service, "_build_closed_candle_snapshot", lambda snap, sym, **k: snap)
    monkeypatch.setattr(service, "_strategy_allowed_on_symbol", lambda name, sym: True)
    service.set_launcher_config(launcher)
    live = service._launcher_evaluate_signals(SYMBOL, candles_1m=okx_rows)

    engine = BacktestEngine(BacktestConfig(
        symbols=[SYMBOL], timeframe="1H", start_ts=0, end_ts=1000,
        strategy_names=["wick_rejection"], launcher_config=launcher,
        guardrails_config=dict(_WR_GUARDS), initial_capital=1000.0,
        evaluation_mode="finer_ltf", evaluation_timeframe="1m",
        instrument_specs={SYMBOL: {"ct_val": ct_val}},
    ))
    bt = engine._evaluate_wick_rejection(
        WickRejectionStrategy(), SYMBOL, snapshot, cfg, deque(minutes, maxlen=16),
        snapshot["as_of_ts"],
    )

    assert bool(live) is fires and (bt is not None) is fires
    if fires:
        assert "wake: base 100k/h" in bt.rationale
        assert live[0].rationale == bt.rationale
        assert live[0].rank_score == bt.rank_score
