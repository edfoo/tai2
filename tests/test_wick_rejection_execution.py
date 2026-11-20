"""Phase 3 of the wick_rejection plan: TP/SL, sizing and guardrail plumbing.

Covers ``launcher_tp_sl`` (strategy-owned exits, protection modes, per-strategy
leverage + margin sizing), the backtest simulator/engine side of the same
numbers, ``build_launcher_decisions`` forwarding and the ``strategies=`` filter,
and ``handle_llm_decision`` applying the leverage override, margin sizing,
TP-only protection and the ``max_position_pct`` cap.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from app.services.backtest.engine import _passes_protection_guard
from app.services.backtest.simulator import Simulator
from app.services.launcher_tp_sl import (
    LauncherTpSl,
    clamp_leverage_to_guardrails,
    resolve_launcher_tp_sl,
    resolve_strategy_sizing,
)
from app.services.market_service import MarketService
from app.services.strategies import StrategySignal
from app.services.strategies.defaults import strategy_defaults

SYMBOL = "TEST-USDT-SWAP"
WIDE_GUARDS = {"min_leverage": 0.5, "max_leverage": 10.0}


def _wr(**over: Any) -> dict[str, Any]:
    cfg = dict(strategy_defaults("wick_rejection"), enabled=True)
    cfg.update(over)
    return cfg


def _resolve(
    strategy: str = "wick_rejection",
    *,
    cfg: dict[str, Any] | None = None,
    guards: dict[str, Any] | None = None,
    equity: float | None = 1000.0,
    launcher: dict[str, Any] | None = None,
    signal_tp: float | None = 0.5,
    signal_sl: float | None = 7.0,
) -> LauncherTpSl:
    gov = dict(launcher or {})
    gov["strategies"] = {strategy: cfg if cfg is not None else _wr()}
    return resolve_launcher_tp_sl(
        strategy_name=strategy,
        direction="sell",
        signal_tp_pct=signal_tp,
        signal_sl_pct=signal_sl,
        last_price=100.0,
        launcher_config=gov,
        guardrails_config=guards if guards is not None else WIDE_GUARDS,
        sym_data={},
        equity=equity,
    )


# ── Resolver ──────────────────────────────────────────────────────────────────


class TestClampLeverage:
    @pytest.mark.parametrize(
        "lev,guards,expected",
        [
            (5, {"min_leverage": 1, "max_leverage": 10}, 5),
            (5, {"min_leverage": 1, "max_leverage": 3}, 3),
            (2, {"min_leverage": 3, "max_leverage": 10}, 3),
            (5, {}, 1),  # no max → max(min, 1)
            (5, {"min_leverage": 10, "max_leverage": 4}, 5),  # inverted pair swapped
        ],
    )
    def test_clamp(self, lev: float, guards: dict[str, Any], expected: float) -> None:
        assert clamp_leverage_to_guardrails(lev, guards) == expected


class TestStrategySizing:
    def test_fixed_usdt(self) -> None:
        lev, margin, notional, _ = resolve_strategy_sizing(_wr(), WIDE_GUARDS, None)
        assert (lev, margin, notional) == (5, 20.0, 100.0)

    def test_pct_equity(self) -> None:
        cfg = _wr(margin_mode="pct_equity", margin_pct=50.0)
        assert resolve_strategy_sizing(cfg, WIDE_GUARDS, 400.0)[:3] == (5, 200.0, 1000.0)

    def test_pct_equity_without_equity_fails(self) -> None:
        cfg = _wr(margin_mode="pct_equity")
        lev, margin, notional, lines = resolve_strategy_sizing(cfg, WIDE_GUARDS, None)
        assert lev == 5 and margin is None and notional is None
        assert lines

    def test_clamp_keeps_margin(self) -> None:
        lev, margin, notional, lines = resolve_strategy_sizing(
            _wr(), {"min_leverage": 1, "max_leverage": 3}, None
        )
        assert (lev, margin, notional) == (3, 20.0, 60.0)
        assert any("clamped" in line for line in lines)

    def test_no_leverage_means_no_override(self) -> None:
        assert resolve_strategy_sizing({}, WIDE_GUARDS, 100.0)[:3] == (None, None, None)


class TestResolveWickRejection:
    def test_defaults(self) -> None:
        r = _resolve()
        assert r.action == "SELL"
        assert r.tp_price == pytest.approx(99.5)
        assert r.sl_price == pytest.approx(107.0)  # 35% margin / 5x
        assert r.protection == "full"
        assert r.skip_rr_guard is True
        assert (r.leverage_override, r.margin_usd, r.notional_override) == (5, 20.0, 100.0)
        assert r.sizing_failed is False

    def test_margin_stop_converts_at_clamped_leverage(self) -> None:
        r = _resolve(guards={"min_leverage": 1, "max_leverage": 3})
        assert r.leverage_override == 3
        assert r.sl_price == pytest.approx(100.0 * (1 + 35.0 / 3 / 100))
        assert r.notional_override == pytest.approx(60.0)

    @pytest.mark.parametrize("mode", ["software", "off"])
    def test_tp_only_modes(self, mode: str) -> None:
        r = _resolve(cfg=_wr(emergency_stop_mode=mode))
        assert r.protection == "tp_only"
        assert r.sl_price is None
        assert r.tp_price == pytest.approx(99.5)

    def test_disable_protection(self) -> None:
        r = _resolve(cfg=_wr(disable_protection=True))
        assert r.protection == "none"
        assert r.disable_protection is True
        assert r.tp_price is None and r.sl_price is None

    def test_pnl_pct_mode_does_not_rescale(self) -> None:
        r = _resolve(launcher={"tp_sl_in_pnl_pct": True})
        assert r.tp_price == pytest.approx(99.5)

    def test_sizing_failed_flag(self) -> None:
        r = _resolve(cfg=_wr(margin_mode="pct_equity"), equity=None)
        assert r.sizing_failed is True

    def test_skip_rr_guard_opt_out(self) -> None:
        assert _resolve(cfg=_wr(skip_rr_guard=False)).skip_rr_guard is False

    def test_other_strategies_unchanged(self) -> None:
        r = _resolve("vwap_reversion", cfg={"enabled": True}, signal_tp=2.0, signal_sl=3.0)
        assert r.protection == "full"
        assert r.skip_rr_guard is False
        assert r.leverage_override is None and r.notional_override is None
        assert r.tp_price == pytest.approx(98.0)
        assert r.sl_price == pytest.approx(103.0)


# ── Backtest side ─────────────────────────────────────────────────────────────


class TestBacktestSizing:
    def _sim(self, guards: dict[str, Any] | None = None) -> Simulator:
        return Simulator(
            initial_capital=1000.0,
            notional_per_trade=10.0,
            strategy_config={"guardrails": guards or {"max_leverage": 10.0}},
        )

    def test_strategy_notional_and_leverage(self) -> None:
        sim = self._sim()
        pos = sim.open_position(
            symbol=SYMBOL, direction="short", entry_price=100.0, entry_ts=0,
            tp_price=99.5, sl_price=107.0, strategy_name="wick_rejection",
            notional=100.0, leverage=5.0, margin=20.0,
        )
        assert pos is not None
        assert pos.size * pos.entry_price == pytest.approx(100.0, rel=0.01)
        assert pos.leverage == 5.0
        assert pos.initial_margin == pytest.approx(20.0, rel=0.01)

    def test_instrument_leverage_cap_keeps_margin(self) -> None:
        sim = self._sim()
        sim.set_instrument_specs({SYMBOL: {"position_tiers": [{"max_leverage": 2.0}]}})
        pos = sim.open_position(
            symbol=SYMBOL, direction="short", entry_price=100.0, entry_ts=0,
            tp_price=99.5, sl_price=107.0, strategy_name="wick_rejection",
            notional=100.0, leverage=5.0, margin=20.0,
        )
        assert pos is not None
        assert pos.leverage == 2.0
        assert pos.size * pos.entry_price == pytest.approx(40.0, rel=0.01)

    def test_default_sizing_unchanged(self) -> None:
        pos = self._sim().open_position(
            symbol=SYMBOL, direction="long", entry_price=100.0, entry_ts=0,
            tp_price=101.0, sl_price=99.0,
        )
        assert pos is not None
        assert pos.size * pos.entry_price == pytest.approx(10.0, rel=0.01)

    def test_protection_guard_tp_only(self) -> None:
        guards = {"require_protection": True}
        tp_only = LauncherTpSl(action="SELL", tp_price=99.5, sl_price=None, protection="tp_only")
        full_no_sl = LauncherTpSl(action="SELL", tp_price=99.5, sl_price=None)
        no_tp = LauncherTpSl(action="SELL", tp_price=None, sl_price=None, protection="tp_only")
        assert _passes_protection_guard(resolved=tp_only, guardrails_config=guards)
        assert not _passes_protection_guard(resolved=full_no_sl, guardrails_config=guards)
        assert not _passes_protection_guard(resolved=no_tp, guardrails_config=guards)


# ── Live: build_launcher_decisions ────────────────────────────────────────────


class _DummyState:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


class _FakeStrategy:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    def evaluate(self, symbol, snapshot, config, helpers):
        self.calls += 1
        return StrategySignal(direction="sell", rationale="fake", rank_score=2.5,
                              tp_pct=config.get("tp_pct"), sl_pct=config.get("sl_pct"))


def _service() -> MarketService:
    return MarketService(
        state_service=_DummyState(), enable_websocket=False,
        account_api=object(), market_api=object(), public_api=object(), trade_api=object(),
    )


class TestBuildLauncherDecisions:
    def _setup(self, monkeypatch: pytest.MonkeyPatch, *, launcher_notional: float | None = 50.0):
        service = _service()
        wr, other = _FakeStrategy("wick_rejection"), _FakeStrategy("vwap_reversion")
        service._strategies = [wr, other]
        service._last_full_snapshot = {
            "market_data": {SYMBOL: {}}, "positions": [],
            "account": {"total_eq_usd": 1000.0},
        }
        monkeypatch.setattr(service, "_build_closed_candle_snapshot", lambda snap, sym, **k: snap)
        monkeypatch.setattr(service, "_strategy_allowed_on_symbol", lambda name, sym: True)
        monkeypatch.setattr(service, "get_last_price", lambda sym: 100.0)
        gov: dict[str, Any] = {
            "mode": "launcher_only",
            "strategies": {
                "wick_rejection": _wr(),
                "vwap_reversion": {"enabled": True, "tp_pct": 2.0, "sl_pct": 3.0},
            },
        }
        if launcher_notional is not None:
            gov["notional_usd"] = launcher_notional
        service.set_launcher_config(gov)
        service.set_guardrails(dict(WIDE_GUARDS))
        return service, wr, other

    def test_strategies_filter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service, wr, other = self._setup(monkeypatch)
        decisions = service.build_launcher_decisions(SYMBOL, strategies=["wick_rejection"])
        assert [d["_strategy_name"] for d in decisions] == ["wick_rejection"]
        assert wr.calls == 1 and other.calls == 0

    def test_wr_decision_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service, _, _ = self._setup(monkeypatch)
        by_name = {d["_strategy_name"]: d for d in service.build_launcher_decisions(SYMBOL)}
        wr = by_name["wick_rejection"]
        assert wr["notional_usd"] == pytest.approx(100.0)
        assert wr["_leverage_override"] == 5
        assert wr["_margin_usd"] == pytest.approx(20.0)
        assert wr["_protection"] == "full"
        assert wr["_skip_rr_guard"] is True
        assert wr["_rank_score"] == 2.5
        assert wr["take_profit"] == pytest.approx(99.5)
        assert wr["stop_loss"] == pytest.approx(107.0)
        assert wr["_strategy_exit"] == {
            "max_hold_seconds": 3600.0,
            "software_stop_pct": None,
            "reentry_cooldown_seconds": 3300.0,
            "use_global_trade_management": False,
        }
        other = by_name["vwap_reversion"]
        assert other["notional_usd"] == 50.0
        assert other["_leverage_override"] is None
        assert other["_strategy_exit"] is None

    def test_wr_needs_no_launcher_notional(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service, _, _ = self._setup(monkeypatch, launcher_notional=None)
        decisions = service.build_launcher_decisions(SYMBOL)
        assert [d["_strategy_name"] for d in decisions] == ["wick_rejection"]

    def test_pct_equity_uses_snapshot_equity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service, _, _ = self._setup(monkeypatch)
        service._launcher_config["strategies"]["wick_rejection"] = _wr(
            margin_mode="pct_equity", margin_pct=10.0
        )
        wr = service.build_launcher_decisions(SYMBOL, strategies=["wick_rejection"])[0]
        assert wr["_margin_usd"] == pytest.approx(100.0)
        assert wr["notional_usd"] == pytest.approx(500.0)


# ── Live: handle_llm_decision ─────────────────────────────────────────────────


def _execute(
    monkeypatch: pytest.MonkeyPatch,
    decision_over: dict[str, Any],
    *,
    guards: dict[str, Any] | None = None,
    notional_mode: str | None = None,
    trade_mode: str = "cross",
    balance: float = 150.0,
) -> tuple[bool, dict[str, Any], list[str]]:
    async def scenario() -> tuple[bool, dict[str, Any], list[str]]:
        service = MarketService(
            state_service=_DummyState(), enable_websocket=False,
            trade_api=object(), account_api=None, market_api=None, public_api=None,
        )
        service._account_api = None
        service._market_api = None
        service._public_api = None
        service._funding_api = None
        service._instrument_specs[SYMBOL] = {"lot_size": 0.01, "min_size": 0.01, "tick_size": 0.01}
        submitted: dict[str, Any] = {}
        log: list[str] = []

        async def fake_submit_order(**payload: Any):
            submitted.update(payload)
            return {"ordId": "1"}, False

        async def fake_noop(*args: Any, **kwargs: Any):
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", fake_noop)
        monkeypatch.setattr(service, "_cancel_position_protection", fake_noop)
        monkeypatch.setattr(service, "_emit_debug", lambda msg, *a, **k: log.append(str(msg)))

        g = {"min_leverage": 0.5, "max_leverage": 10.0, "max_position_pct": 1.0,
             "require_protection": True}
        g.update(guards or {})
        if notional_mode:
            g["llm_notional_mode"] = notional_mode
        context = {
            "symbol": SYMBOL,
            "guardrails": g,
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": balance,
                "available_eq_usd": balance,
                "available_balances": (
                    {"USDT": {"available": balance, "cash": balance}}
                    if trade_mode == "isolated" else {}
                ),
            },
            "execution": {"enabled": True, "trade_mode": trade_mode, "order_type": "market", "min_size": 0.01},
            "positions": [],
        }
        decision = {
            "action": "SELL",
            "symbol": SYMBOL,
            "notional_usd": 100.0,
            "confidence": 1.0,
            "risk_score": 0.5,
            "take_profit": 99.5,
            "stop_loss": 107.0,
            "_decision_origin": "launcher",
            "_strategy_name": "wick_rejection",
            "_skip_rr_guard": True,
            "_protection": "full",
            "_leverage_override": 5.0,
            "_margin_usd": 20.0,
        }
        decision.update(decision_over)
        ok = await service.handle_llm_decision(decision, context)
        return ok, submitted, log

    return asyncio.run(scenario())


def _algo_kinds(payload: dict[str, Any]) -> set[str]:
    kinds: set[str] = set()
    for algo in payload.get("attach_algo_orders") or []:
        if algo.get("tpTriggerPx"):
            kinds.add("tp")
        if algo.get("slTriggerPx"):
            kinds.add("sl")
    return kinds


class TestExecution:
    @pytest.mark.parametrize("mode", [None, "pre_leverage", "post_leverage"])
    def test_margin_times_leverage_regardless_of_notional_mode(
        self, monkeypatch: pytest.MonkeyPatch, mode: str | None
    ) -> None:
        ok, payload, log = _execute(monkeypatch, {}, notional_mode=mode)
        assert ok is True
        assert payload["size"] == pytest.approx(1.0)  # 20 × 5 / 100
        assert payload["leverage"] == pytest.approx(5.0)
        assert _algo_kinds(payload) == {"tp", "sl"}
        assert any("sizing from strategy margin" in m for m in log)

    def test_guardrail_clamp_keeps_margin(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, _ = _execute(monkeypatch, {}, guards={"max_leverage": 3.0})
        assert ok is True
        assert payload["leverage"] == pytest.approx(3.0)
        assert payload["size"] == pytest.approx(0.6)  # 20 × 3 / 100

    def test_max_position_pct_caps_and_logs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, log = _execute(monkeypatch, {}, guards={"max_position_pct": 0.4})
        assert ok is True
        assert payload["size"] == pytest.approx(0.6)  # 150 × 0.4 / 100
        assert any("capped" in m and "max_position_pct" in m for m in log)

    def test_tp_only_places_tp_without_sl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, _ = _execute(monkeypatch, {"_protection": "tp_only", "stop_loss": None})
        assert ok is True
        assert _algo_kinds(payload) == {"tp"}

    def test_tp_only_strips_a_supplied_sl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, _ = _execute(monkeypatch, {"_protection": "tp_only"})
        assert ok is True
        assert _algo_kinds(payload) == {"tp"}

    def test_full_protection_still_requires_sl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, _ = _execute(monkeypatch, {"stop_loss": None})
        assert ok is False
        assert payload == {}

    def test_tp_only_still_requires_tp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ok, payload, _ = _execute(
            monkeypatch, {"_protection": "tp_only", "stop_loss": None, "take_profit": None}
        )
        assert ok is False
        assert payload == {}


class TestIsolatedExecution:
    """Isolated bootstrap limits are margin budgets for strategy-sized entries."""

    def test_full_size_when_margin_fits_bootstrap_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # 25% bootstrap of 117 USDT = 29.25 margin ≥ 20 → notional 100 (not 29).
        ok, payload, _ = _execute(
            monkeypatch, {}, trade_mode="isolated", balance=117.0,
            guards={"min_leverage": 3.0, "max_leverage": 5.0},
        )
        assert ok is True
        assert payload["size"] == pytest.approx(1.0)
        assert payload["leverage"] == pytest.approx(5.0)

    def test_bootstrap_budget_caps_margin(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # 25% of 40 USDT = 10 margin → 10 × 5 = 50 notional.  max_position_pct
        # raised so its notional cap (equity × pct) does not bind first.
        ok, payload, _ = _execute(
            monkeypatch, {}, trade_mode="isolated", balance=40.0,
            guards={"min_leverage": 3.0, "max_leverage": 5.0, "max_position_pct": 5.0},
        )
        assert ok is True
        assert payload["size"] == pytest.approx(0.5)
        assert payload["leverage"] == pytest.approx(5.0)
