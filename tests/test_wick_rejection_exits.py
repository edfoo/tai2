"""Phase 4 of the wick_rejection plan: strategy-owned exits.

Covers the shared rule (``trade_management_rules.compute_strategy_exit``), the
exit parameters resolved at entry by ``resolve_launcher_tp_sl``, the simulator
split (software stop before TP, time stop, global rules skipped, own re-entry
cooldown) and the live ``_check_trade_management`` split.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from app.services.backtest.models import Candle
from app.services.backtest.simulator import Simulator
from app.services.launcher_tp_sl import resolve_launcher_tp_sl
from app.services.market_service import MarketService
from app.services.strategies.defaults import strategy_defaults
from app.services.trade_management_rules import (
    StrategyExitParams,
    compute_strategy_exit,
    software_stop_price,
)

SYMBOL = "TEST-USDT-SWAP"
MIN = 60_000


def _wr(**over: Any) -> dict[str, Any]:
    cfg = dict(strategy_defaults("wick_rejection"), enabled=True)
    cfg.update(over)
    return cfg


# ── Shared rule ───────────────────────────────────────────────────────────────


class TestComputeStrategyExit:
    P = StrategyExitParams(max_hold_seconds=3600.0, software_stop_pct=7.0)

    def _run(self, params: StrategyExitParams = P, *, is_long: bool = False,
             held: float = 0.0, high: float = 100.0, low: float = 100.0):
        return compute_strategy_exit(
            params, is_long=is_long, entry_price=100.0, held_seconds=held, high=high, low=low
        )

    def test_short_stop_above_entry(self) -> None:
        assert software_stop_price(self.P, is_long=False, entry_price=100.0) == pytest.approx(107.0)
        assert not self._run(high=106.9).close
        d = self._run(high=107.0)
        assert (d.close, d.reason) == (True, "emergency_stop")
        assert d.stop_price == pytest.approx(107.0)

    def test_long_stop_below_entry(self) -> None:
        d = self._run(is_long=True, low=93.0)
        assert d.reason == "emergency_stop"
        assert d.stop_price == pytest.approx(93.0)
        assert not self._run(is_long=True, high=110.0, low=93.1).close

    def test_time_stop_win_or_lose(self) -> None:
        assert not self._run(held=3599).close
        assert self._run(held=3600).reason == "time_stop"
        # No R condition: a winning position is closed too.
        assert self._run(held=3600, low=95.0).reason == "time_stop"

    def test_stop_wins_over_time_stop(self) -> None:
        assert self._run(held=4000, high=110.0).reason == "emergency_stop"

    def test_off(self) -> None:
        off = StrategyExitParams()
        assert not self._run(off, held=1e9, high=1e9).close

    def test_dict_round_trip(self) -> None:
        p = StrategyExitParams(1800.0, 5.0, 3300.0, True)
        assert StrategyExitParams.from_dict(p.to_dict()) == p
        assert StrategyExitParams.from_dict(None) is None


# ── Resolver ──────────────────────────────────────────────────────────────────


def _resolve(cfg: dict[str, Any], strategy: str = "wick_rejection",
             guards: dict[str, Any] | None = None):
    return resolve_launcher_tp_sl(
        strategy_name=strategy,
        direction="sell",
        signal_tp_pct=1.0,
        signal_sl_pct=2.0,
        last_price=100.0,
        launcher_config={"strategies": {strategy: cfg}},
        guardrails_config=guards or {"min_leverage": 1, "max_leverage": 10},
        sym_data={},
        equity=1000.0,
    )


class TestResolverExitParams:
    def test_exchange_mode_defaults(self) -> None:
        ex = _resolve(_wr()).strategy_exit
        assert ex == StrategyExitParams(3600.0, None, 3300.0, False)

    def test_software_mode_stop_at_leverage(self) -> None:
        r = _resolve(_wr(emergency_stop_mode="software"))
        assert r.protection == "tp_only" and r.sl_price is None
        assert r.strategy_exit is not None
        assert r.strategy_exit.software_stop_pct == pytest.approx(7.0)  # 35% margin / 5x

    def test_software_stop_uses_clamped_leverage(self) -> None:
        r = _resolve(_wr(emergency_stop_mode="software"), guards={"max_leverage": 3})
        assert r.strategy_exit.software_stop_pct == pytest.approx(35.0 / 3)

    def test_off_mode_has_no_stop(self) -> None:
        r = _resolve(_wr(emergency_stop_mode="off", max_hold_minutes=0))
        assert r.strategy_exit == StrategyExitParams(0.0, None, 3300.0, False)

    def test_other_strategies_have_none(self) -> None:
        r = _resolve({"enabled": True, "tp_pct": 1.0, "sl_pct": 2.0}, strategy="vwap_reversion")
        assert r.strategy_exit is None


# ── Simulator ─────────────────────────────────────────────────────────────────


def _candle(ts: int, o: float, h: float, low: float, c: float) -> Candle:
    return Candle(ts=ts, open=o, high=h, low=low, close=c, volume=1.0)


def _sim(tm: dict[str, Any] | None = None) -> Simulator:
    cfg: dict[str, Any] = {"guardrails": {"max_leverage": 10.0}}
    if tm is not None:
        cfg["trade_management"] = tm
    return Simulator(initial_capital=1000.0, notional_per_trade=100.0,
                     strategy_config=cfg, timeframe_seconds=60.0)


def _open(sim: Simulator, exit_params: StrategyExitParams | None, *,
          tp: float | None = 99.5, sl: float | None = None):
    pos = sim.open_position(
        symbol=SYMBOL, direction="short", entry_price=100.0, entry_ts=0,
        tp_price=tp, sl_price=sl, strategy_name="wick_rejection",
        notional=100.0, leverage=5.0, margin=20.0, strategy_exit=exit_params,
    )
    assert pos is not None
    return pos


SOFT = StrategyExitParams(max_hold_seconds=3600.0, software_stop_pct=7.0,
                          reentry_cooldown_seconds=3300.0)


class TestSimulatorExits:
    def test_software_stop_fills_before_tp_in_same_candle(self) -> None:
        sim = _sim()
        pos = _open(sim, SOFT)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 108.0, 99.0, 100.0)})
        assert pos.close_reason == "emergency_stop"
        assert pos.close_price is not None
        assert pos.close_price >= 107.0  # stop level (plus any slippage)

    def test_software_stop_gap_fills_at_open(self) -> None:
        sim = _sim()
        pos = _open(sim, SOFT)
        sim.update_multi({SYMBOL: _candle(MIN, 110.0, 111.0, 109.0, 110.0)})
        assert pos.close_reason == "emergency_stop"
        assert pos.close_price >= 110.0

    def test_exchange_stop_and_tp_same_candle_stop_first(self) -> None:
        sim = _sim()
        pos = _open(sim, StrategyExitParams(max_hold_seconds=3600.0), sl=107.0)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 108.0, 99.0, 100.0)})
        assert pos.close_reason == "sl"

    def test_tp(self) -> None:
        sim = _sim()
        pos = _open(sim, SOFT)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 100.5, 99.4, 99.6)})
        assert pos.close_reason == "tp"

    def test_time_stop_after_max_hold(self) -> None:
        sim = _sim()
        pos = _open(sim, SOFT)
        for i in range(1, 60):
            sim.update_multi({SYMBOL: _candle(i * MIN, 100.0, 100.2, 99.8, 100.1)})
        assert pos.is_open
        sim.update_multi({SYMBOL: _candle(60 * MIN, 100.0, 100.2, 99.8, 100.1)})
        assert pos.close_reason == "time_stop"
        assert pos.close_ts == 60 * MIN
        assert pos.close_price == pytest.approx(100.1, rel=0.01)

    def test_global_rules_skipped(self) -> None:
        tm = {"enabled": True, "breakeven_at_r": 0.01, "partial_tp_enabled": False,
              "trailing_enabled": False, "time_stop_enabled": False}
        sim = _sim(tm)
        owned = _open(sim, StrategyExitParams(max_hold_seconds=3600.0), tp=90.0, sl=107.0)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 100.0, 99.0, 99.2)})
        assert owned.is_open and not owned.breakeven_done
        assert owned.sl_price == pytest.approx(107.0)

        # Control: the same position without strategy exits gets breakeven.
        sim2 = _sim(tm)
        generic = _open(sim2, None, tp=90.0, sl=107.0)
        sim2.update_multi({SYMBOL: _candle(MIN, 100.0, 100.0, 99.0, 99.2)})
        assert generic.breakeven_done

    def test_use_global_trade_management_opt_in(self) -> None:
        tm = {"enabled": True, "breakeven_at_r": 0.01, "partial_tp_enabled": False,
              "trailing_enabled": False, "time_stop_enabled": False}
        sim = _sim(tm)
        pos = _open(sim, StrategyExitParams(3600.0, None, 0.0, True), tp=90.0, sl=107.0)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 100.0, 99.0, 99.2)})
        assert pos.breakeven_done

    def test_own_reentry_cooldown(self) -> None:
        sim = _sim({"enabled": False, "reentry_cooldown_seconds": 600})
        _open(sim, SOFT)
        sim.update_multi({SYMBOL: _candle(MIN, 100.0, 100.5, 99.4, 99.6)})  # TP at 1 min
        assert not sim.can_enter(SYMBOL, MIN + 3299_000)
        assert sim.can_enter(SYMBOL, MIN + 3300_000)

        # A generic close on the symbol restores the global cooldown.
        sim.open_position(symbol=SYMBOL, direction="short", entry_price=100.0,
                          entry_ts=10 * 3600_000, tp_price=99.5, sl_price=107.0)
        sim.update_multi({SYMBOL: _candle(10 * 3600_000 + MIN, 100.0, 100.5, 99.4, 99.6)})
        t = 10 * 3600_000 + MIN
        assert not sim.can_enter(SYMBOL, t + 599_000)
        assert sim.can_enter(SYMBOL, t + 600_000)


# ── Live ──────────────────────────────────────────────────────────────────────


class _DummyState:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _service(tm: dict[str, Any], price: float) -> tuple[MarketService, list[dict[str, Any]], list[str]]:
    # Stub every API: passing None makes the constructor build real OKX
    # clients from .env.  Null them afterwards so nothing can reach OKX.
    service = MarketService(
        state_service=_DummyState(), enable_websocket=False,
        trade_api=object(), account_api=object(), market_api=object(),
        public_api=object(), trading_api=object(), funding_api=object(),
    )
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(service, attr, None)
    service._strategy_config = {"trade_management": tm}
    closes: list[dict[str, Any]] = []
    log: list[str] = []

    async def fake_close(**kwargs: Any) -> None:
        closes.append(kwargs)

    async def fake_move_sl(**kwargs: Any) -> None:
        closes.append({"move_sl": kwargs})

    async def fake_partial(**kwargs: Any) -> None:
        closes.append({"partial": kwargs})

    async def no_submit(**kwargs: Any) -> None:
        raise AssertionError(f"unexpected order submission: {kwargs}")

    service._trade_mgmt_full_close = fake_close  # type: ignore[method-assign]
    service._trade_mgmt_move_sl = fake_move_sl  # type: ignore[method-assign]
    service._trade_mgmt_partial_close = fake_partial  # type: ignore[method-assign]
    service._submit_order = no_submit  # type: ignore[method-assign]
    service._emit_debug = lambda msg, *a, **k: log.append(str(msg))  # type: ignore[method-assign]
    service.get_last_price = lambda sym: price  # type: ignore[method-assign]
    service._last_full_snapshot = {
        "positions": [{"instId": SYMBOL, "pos": "-1", "avgPx": "100", "posSide": "short",
                       "mgnMode": "isolated"}],
        "market_data": {},
    }
    return service, closes, log


def _seed(service: MarketService, exit_params: StrategyExitParams | None, *,
          held: float = 0.0, sl: float | None = 107.0) -> None:
    service._seed_trade_mgmt_state(
        symbol=SYMBOL, side="short", strategy_name="wick_rejection",
        entry_price=100.0, tp_price=99.5, sl_price=sl,
        strategy_exit=exit_params.to_dict() if exit_params else None,
    )
    if SYMBOL in service._trade_mgmt_state:
        service._trade_mgmt_state[SYMBOL]["entry_ts"] = time.time() - held


async def _check(service: MarketService) -> None:
    await service._check_trade_management()
    await asyncio.sleep(0)


TM_OFF = {"enabled": False, "reentry_cooldown_seconds": 600}
TM_AGGRESSIVE = {"enabled": True, "breakeven_at_r": 0.01, "partial_tp_at_r": 0.01,
                 "trailing_activate_r": 0.01, "time_stop_seconds": 1,
                 "time_stop_underwater_only": False, "time_stop_min_r": 100,
                 "reentry_cooldown_seconds": 600}


class TestLiveExits:
    def test_seeded_even_with_global_tm_off(self) -> None:
        service, _, _ = _service(TM_OFF, 100.0)
        _seed(service, SOFT)
        assert service._trade_mgmt_state[SYMBOL]["strategy_exit"] == SOFT.to_dict()
        service2, _, _ = _service(TM_OFF, 100.0)
        _seed(service2, None)
        assert SYMBOL not in service2._trade_mgmt_state

    def test_time_stop_with_global_tm_off(self) -> None:
        async def run():
            service, closes, log = _service(TM_OFF, 99.8)
            _seed(service, SOFT, held=3601)
            await _check(service)
            return closes, log

        closes, log = asyncio.run(run())
        assert len(closes) == 1 and closes[0]["reason"] == "time_stop"
        assert closes[0]["close_side"] == "buy"
        assert any("TradeMgmt time_stop" in m and "[wick_rejection]" in m for m in log)

    def test_software_stop(self) -> None:
        async def run():
            service, closes, _ = _service(TM_OFF, 107.5)
            _seed(service, SOFT, held=60, sl=None)
            await _check(service)
            return closes

        closes = asyncio.run(run())
        assert [c["reason"] for c in closes] == ["emergency_stop"]

    def test_no_exit_and_global_rules_skipped(self) -> None:
        async def run():
            # Price in profit, aggressive global rules that would all fire.
            service, closes, _ = _service(TM_AGGRESSIVE, 99.0)
            _seed(service, SOFT, held=600)
            await _check(service)
            return closes, service._trade_mgmt_state[SYMBOL]

        closes, state = asyncio.run(run())
        assert closes == []
        assert not state["breakeven_done"] and not state["partial_done"]

    def test_generic_position_still_gets_global_rules(self) -> None:
        async def run():
            service, closes, _ = _service(TM_AGGRESSIVE, 99.0)
            _seed(service, None, held=600)
            await _check(service)
            return closes, service._trade_mgmt_state[SYMBOL]

        closes, state = asyncio.run(run())
        assert state["breakeven_done"] and state["partial_done"]
        assert any("partial" in c for c in closes)

    def test_own_reentry_cooldown_on_clear(self) -> None:
        service, _, _ = _service(TM_OFF, 100.0)
        _seed(service, SOFT)
        service._clear_trade_mgmt_state(SYMBOL, reason="tp")
        remaining = service._reentry_cooldown_until[SYMBOL] - time.time()
        assert 3290 < remaining <= 3300
