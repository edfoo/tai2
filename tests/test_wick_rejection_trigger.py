"""Phase 6 of the wick_rejection plan: the clock-aligned live entry trigger.

Covers the trigger timing maths, one trigger cycle against a fake market
service (ranking, ``max_concurrent_positions``, ``max_total_margin_pct``, late
data, missing prices, a busy execute lock), the forming-candle price patch in
the real ``MarketService`` snapshot path, the scheduler excluding WR from its
tick, and the backtest entry-timing diagnostics.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.backtest.engine import BacktestEngine
from app.services.backtest.models import BacktestConfig
from app.services.market_service import MarketService
from app.services.prompt_scheduler import PromptScheduler
from app.services.strategies.defaults import strategy_defaults
from app.services.strategies.wick_rejection import backtest_entry_timing
from app.services.wick_rejection_trigger import (
    WickRejectionTrigger,
    launcher_execution_context,
    next_trigger_ts,
    seconds_to_hour_close,
)

HOUR = 3600.0
H0 = 1_700_002_800.0  # an exact hour boundary (epoch seconds)
assert H0 % HOUR == 0


def _wr(**over: Any) -> dict[str, Any]:
    cfg = dict(strategy_defaults("wick_rejection"), enabled=True)
    cfg.update(over)
    return cfg


# ── Timing ────────────────────────────────────────────────────────────────────


class TestTiming:
    def test_next_trigger(self) -> None:
        assert next_trigger_ts(H0 + 600, 40) == H0 + HOUR - 40
        # Already past this hour's trigger second → next hour.
        assert next_trigger_ts(H0 + HOUR - 30, 40) == H0 + 2 * HOUR - 40
        assert next_trigger_ts(H0 + HOUR - 40, 40) == H0 + 2 * HOUR - 40

    def test_seconds_to_close(self) -> None:
        assert seconds_to_hour_close(H0 + HOUR - 40) == pytest.approx(40)
        assert seconds_to_hour_close(H0) == pytest.approx(HOUR)

    def test_execution_context(self) -> None:
        ctx = launcher_execution_context(
            {"execution_enabled": True, "execution_trade_mode": "cross",
             "guardrails": {"x": 1}, "risk_locks": {"daily_loss": {}}}, "A",
        )
        assert ctx["symbol"] == "A"
        assert ctx["execution"]["enabled"] is True
        assert ctx["execution"]["trade_mode"] == "cross"
        assert ctx["guardrails"] == {"x": 1}


# ── One trigger cycle (fake market service) ───────────────────────────────────


class _FakeMS:
    def __init__(self, universe: list[str], ranks: dict[str, float],
                 prices: dict[str, float | None]) -> None:
        self.universe = universe
        self.ranks = ranks
        self.prices = prices
        self.refreshed: list[tuple[str, str]] = []
        self.built: list[tuple[str, Any, float | None]] = []
        self.executed: list[str] = []
        self.log: list[str] = []
        self._launcher_in_position: dict[str, dict[str, Any]] = {}
        self._last_full_snapshot = {"account": {"total_eq_usd": 100.0}}

    def get_screener_universe(self, name: str) -> list[str]:
        assert name == "wick_rejection"
        return list(self.universe)

    async def refresh_timeframe_candles(self, sym: str, bar: str) -> int:
        self.refreshed.append((sym, bar))
        return 100

    async def fetch_trigger_price(self, sym: str, *, max_age_seconds: float):
        px = self.prices.get(sym)
        return (px, "ws", 0.4) if px else (None, "rest_failed", None)

    def build_launcher_decisions(self, sym: str, strategies=None, *, price=None):
        self.built.append((sym, strategies, price))
        if sym not in self.ranks:
            return []
        return [{"action": "SELL", "symbol": sym, "_rank_score": self.ranks[sym],
                 "_margin_usd": 20.0, "_strategy_name": "wick_rejection"}]

    async def handle_llm_decision(self, decision: dict[str, Any], ctx: dict[str, Any]) -> bool:
        self.executed.append(decision["symbol"])
        return True

    def _emit_debug(self, msg: str) -> None:
        self.log.append(msg)


def _trigger(ms: _FakeMS, *, now: float = H0 + HOUR - 40, cfg: dict[str, Any] | None = None,
             mode: str = "launcher_only", lock: asyncio.Lock | None = None,
             enabled: bool = True) -> WickRejectionTrigger:
    rc = {"launcher": {"mode": mode, "strategies": {"wick_rejection": cfg or _wr()}},
          "execution_enabled": True}
    app = SimpleNamespace(state=SimpleNamespace(runtime_config=rc, market_service=ms))
    return WickRejectionTrigger(app, lock or asyncio.Lock(), clock=lambda: now,
                                is_enabled=lambda: enabled)


def _run(trigger: WickRejectionTrigger):
    return asyncio.run(trigger.run_once())


SYMS = ["A-USDT-SWAP", "B-USDT-SWAP", "C-USDT-SWAP"]


class TestRunOnce:
    def test_skipped_when_disabled(self) -> None:
        ms = _FakeMS(SYMS, {}, {})
        assert _run(_trigger(ms, mode="disabled")).status == "skipped"
        assert _run(_trigger(ms, cfg=_wr(enabled=False))).status == "skipped"
        assert _run(_trigger(ms, enabled=False)).status == "skipped"
        assert ms.refreshed == []

    def test_ranks_and_executes(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": 3.0, "C-USDT-SWAP": 2.0},
                     {s: 1.0 for s in SYMS})
        res = _run(_trigger(ms))
        assert res.status == "ok"
        assert ms.executed == ["B-USDT-SWAP", "C-USDT-SWAP", "A-USDT-SWAP"]
        assert sorted(ms.refreshed) == [(s, "1H") for s in SYMS]
        assert all(b[1] == ["wick_rejection"] and b[2] == 1.0 for b in ms.built)
        assert "entries 3" in res.summary()

    def test_max_concurrent_counts_open_wr(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": 3.0, "C-USDT-SWAP": 2.0},
                     {s: 1.0 for s in SYMS})
        ms._launcher_in_position = {
            "wick_rejection:X-USDT-SWAP": {"strategy": "wick_rejection", "margin_usd": 20.0},
            "mean_reversion:Y-USDT-SWAP": {"strategy": "mean_reversion"},
        }
        res = _run(_trigger(ms, cfg=_wr(max_concurrent_positions=2)))
        assert ms.executed == ["B-USDT-SWAP"]
        assert "max_concurrent_positions" in res.dropped["C-USDT-SWAP"]

    def test_total_margin_cap(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": 3.0, "C-USDT-SWAP": 2.0},
                     {s: 1.0 for s in SYMS})
        # Equity 100 → 50% = 50 USDT of WR margin: two entries of 20.
        res = _run(_trigger(ms, cfg=_wr(max_total_margin_pct=50.0)))
        assert ms.executed == ["B-USDT-SWAP", "C-USDT-SWAP"]
        assert "max_total_margin_pct" in res.dropped["A-USDT-SWAP"]

    def test_missing_price_dropped(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": 3.0},
                     {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": None})
        res = _run(_trigger(ms))
        assert ms.executed == ["A-USDT-SWAP"]
        assert res.dropped["B-USDT-SWAP"] == "no price (rest_failed)"

    def test_late_data_enters_nothing(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0}, {s: 1.0 for s in SYMS})
        res = _run(_trigger(ms, now=H0 + HOUR - 5))  # 5 s < min 10 s
        assert res.status == "late" and ms.executed == [] and ms.built == []

    def test_busy_lock_drops_entry(self) -> None:
        ms = _FakeMS(SYMS, {"A-USDT-SWAP": 1.0}, {s: 1.0 for s in SYMS})

        async def scenario():
            lock = asyncio.Lock()
            await lock.acquire()  # the scheduler tick is mid-execution
            trig = _trigger(ms, now=H0 + HOUR - 10.05, lock=lock)
            return await trig.run_once()

        res = asyncio.run(scenario())
        assert ms.executed == []
        assert "lock busy" in res.dropped["A-USDT-SWAP"]


# ── Forming-candle patch in the real snapshot path ────────────────────────────


SYMBOL = "WR-USDT-SWAP"


def _service() -> MarketService:
    service = MarketService(
        state_service=None, enable_websocket=False,  # type: ignore[arg-type]
        trade_api=object(), account_api=object(), market_api=object(),
        public_api=object(), trading_api=object(), funding_api=object(),
    )
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(service, attr, None)

    async def no_submit(**kwargs: Any) -> None:
        raise AssertionError("no orders in this test")

    service._submit_order = no_submit  # type: ignore[method-assign]
    return service


def _hour_rows(now: float) -> list[list[str]]:
    """OKX-style 1H rows (newest first): 30 closed small candles + a forming one."""
    hour_start = int(now // HOUR * HOUR) * 1000
    rows = []
    for i in range(30, 0, -1):
        ts = hour_start - i * 3_600_000
        o, c = (100.0, 100.5) if i % 2 else (100.5, 100.0)
        rows.append([str(ts), str(o), "100.8", "99.8", str(c), "1", "1", "100", "1"])
    rows.append([str(hour_start), "100", "101", "99.5", "100.5", "1", "1", "100", "0"])
    return list(reversed(rows))


class TestFormingPricePatch:
    def _setup(self) -> MarketService:
        service = _service()
        now = time.time()
        service._latest_ohlcv_tf[(SYMBOL, "1H")] = _hour_rows(now)
        service._last_full_snapshot = {
            "market_data": {SYMBOL: {"indicators": {"atr_pct": 1.0, "ohlcv": []}}},
            "positions": [], "account": {"total_eq_usd": 1000.0},
        }
        service.set_launcher_config({
            "mode": "launcher_only",
            # The real clock cannot be pinned to H:59 here, so open the window.
            "strategies": {"wick_rejection": _wr(min_seconds_to_close=0,
                                                 max_seconds_to_close=3600)},
        })
        service.set_guardrails({"min_leverage": 1, "max_leverage": 10})
        service.get_last_price = lambda sym: 100.5  # type: ignore[method-assign]
        return service

    def test_patch_widens_forming_candle(self) -> None:
        service = self._setup()
        snap = service._build_closed_candle_snapshot(
            service._last_full_snapshot, SYMBOL, forming_price=104.0,
            ensure_timeframes=["1H"],
        )
        forming = snap["market_data"][SYMBOL]["timeframes"]["1H"]["forming_candle"]
        assert forming["close"] == 104.0 and forming["high"] == 104.0
        assert forming["low"] == pytest.approx(99.5) and forming["open"] == pytest.approx(100.0)
        # Without ensure_timeframes the missing 1H block is not invented.
        plain = service._build_closed_candle_snapshot(service._last_full_snapshot, SYMBOL)
        assert "timeframes" not in plain["market_data"][SYMBOL]

    def test_decision_uses_trigger_price(self) -> None:
        service = self._setup()
        # Cached forming close 100.5 is no big body → no signal.
        assert service.build_launcher_decisions(SYMBOL, strategies=["wick_rejection"]) == []
        # At the trigger price 104 the forming candle is a big green body at its high.
        decisions = service.build_launcher_decisions(
            SYMBOL, strategies=["wick_rejection"], price=104.0
        )
        assert len(decisions) == 1
        d = decisions[0]
        assert d["action"] == "SELL" and d["_strategy_name"] == "wick_rejection"
        assert d["take_profit"] == pytest.approx(104.0 * 0.995)
        assert d["_rank_score"] > 0


class TestFetchTriggerPrice:
    def test_fresh_ws_price(self) -> None:
        service = _service()
        service._latest_ticker[SYMBOL] = {"last": "101.5", "ts": str(int(time.time() * 1000))}
        px, src, age = asyncio.run(service.fetch_trigger_price(SYMBOL, max_age_seconds=5))
        assert (px, src) == (101.5, "ws") and age is not None and age < 5

    def test_stale_ws_without_rest(self) -> None:
        service = _service()
        service._latest_ticker[SYMBOL] = {"last": "101.5",
                                          "ts": str(int((time.time() - 60) * 1000))}
        px, src, _ = asyncio.run(service.fetch_trigger_price(SYMBOL, max_age_seconds=5))
        assert px is None and src == "no_market_api"

    def test_stale_ws_falls_back_to_rest(self) -> None:
        service = _service()
        service._latest_ticker[SYMBOL] = {"last": "101.5",
                                          "ts": str(int((time.time() - 60) * 1000))}
        calls: list[str] = []

        class _Market:
            def get_ticker(self, sym: str):
                calls.append(sym)
                return {"code": "0", "data": [{"instId": sym, "last": "102.0",
                                               "ts": str(int(time.time() * 1000))}]}

        service._market_api = _Market()
        px, src, _ = asyncio.run(service.fetch_trigger_price(SYMBOL, max_age_seconds=5))
        assert (px, src) == (102.0, "rest") and calls == [SYMBOL]
        assert service._latest_ticker[SYMBOL]["last"] == "102.0"


# ── Scheduler ─────────────────────────────────────────────────────────────────


class TestScheduler:
    def test_tick_excludes_wick_rejection(self) -> None:
        calls: list[Any] = []

        class _MS:
            _strategies = [SimpleNamespace(name="mean_reversion"),
                           SimpleNamespace(name="wick_rejection")]

            def build_launcher_decisions(self, symbol, strategies=None):
                calls.append(strategies)
                return [{"symbol": symbol}]

        app = SimpleNamespace(state=SimpleNamespace(runtime_config={}))
        sched = PromptScheduler(app)  # type: ignore[arg-type]
        out = sched._collect_launcher_decisions(["A"], _MS())
        assert calls == [["mean_reversion"]] and len(out) == 1

    def test_start_stop_runs_trigger(self) -> None:
        async def scenario():
            app = SimpleNamespace(state=SimpleNamespace(
                runtime_config={}, market_service=None, state_service=None,
            ))
            sched = PromptScheduler(app)  # type: ignore[arg-type]
            await sched.start()
            running = sched._wr_trigger._task is not None and not sched._wr_trigger._task.done()
            await sched.stop()
            return running, sched._wr_trigger._task

        running, task = asyncio.run(scenario())
        assert running and task is None


# ── Backtest timing diagnostics ───────────────────────────────────────────────


class TestBacktestTiming:
    def test_closed_mode_unsupported(self) -> None:
        t = backtest_entry_timing(_wr(), evaluation_mode="closed", evaluation_timeframe=None)
        assert not t["supported"] and "finer_ltf" in t["reason"]

    def test_one_minute_supported_with_gap(self) -> None:
        t = backtest_entry_timing(_wr(), evaluation_mode="finer_ltf", evaluation_timeframe="1m")
        assert t["supported"] and t["backtest_seconds_before_close"] == 60
        assert t["live_seconds_before_close"] == 40 and "parity gap" in t["reason"]

    @pytest.mark.parametrize("eval_tf,over", [("5m", {}), ("1m", {"max_seconds_to_close": 30})])
    def test_no_step_in_window(self, eval_tf: str, over: dict[str, Any]) -> None:
        t = backtest_entry_timing(_wr(**over), evaluation_mode="finer_ltf",
                                  evaluation_timeframe=eval_tf)
        assert not t["supported"] and "no " in t["reason"]

    def test_engine_records_timing(self) -> None:
        cfg = BacktestConfig(
            symbols=["A-USDT-SWAP"], timeframe="15m", start_ts=0, end_ts=1,
            evaluation_mode="closed",
            launcher_config={"strategies": {"wick_rejection": _wr()}},
        )
        engine = BacktestEngine(cfg)
        assert engine._wr_timing is not None and not engine._wr_timing["supported"]
        assert BacktestEngine(BacktestConfig(symbols=["A"], timeframe="15m", start_ts=0,
                                             end_ts=1))._wr_timing is None


# ── Entry caps: live trigger ↔ backtest parity ────────────────────────────────


_CAP_RANKS = {"A-USDT-SWAP": 1.0, "B-USDT-SWAP": 3.0, "C-USDT-SWAP": 2.0,
              "D-USDT-SWAP": 2.5, "E-USDT-SWAP": 0.5}


def _backtest_cap_pick(cfg: dict[str, Any], open_wr: int, equity: float) -> list[str]:
    """Symbols the backtest opens for one step's WR candidates."""
    eng = BacktestEngine(BacktestConfig(
        symbols=[], timeframe="1H", start_ts=0, end_ts=10, initial_capital=equity,
        strategy_names=["wick_rejection"],
        launcher_config={"strategies": {"wick_rejection": cfg}},
    ))
    for i in range(open_wr):
        eng._simulator.open_position(
            symbol=f"X{i}-USDT-SWAP", direction="short", entry_price=1.0, entry_ts=0,
            tp_price=None, sl_price=None, strategy_name="wick_rejection",
            notional=100.0, leverage=5.0, margin=20.0,
        )
        eng._current_prices[f"X{i}-USDT-SWAP"] = 1.0
    pending = [
        (dict(symbol=s, direction="short", entry_price=1.0, entry_ts=0, tp_price=None,
              sl_price=None, strategy_name="wick_rejection", notional=100.0,
              leverage=5.0, margin=20.0, strategy_exit=None), r, cfg)
        for s, r in _CAP_RANKS.items()
    ]
    before = {p.symbol for p in eng._simulator.open_positions}
    eng._open_ranked_wick_rejection(pending)
    return [p.symbol for p in eng._simulator.open_positions if p.symbol not in before]


@pytest.mark.parametrize(
    "over, open_wr",
    [
        ({}, 0),                                            # default cap 3
        ({"max_concurrent_positions": 3}, 2),               # one slot left
        ({"max_concurrent_positions": 0}, 0),               # no cap
        ({"max_concurrent_positions": 0, "max_total_margin_pct": 50.0}, 1),  # margin budget
        ({"max_concurrent_positions": 3}, 3),               # full
    ],
)
def test_entry_caps_match_backtest(over: dict[str, Any], open_wr: int) -> None:
    equity = 100.0
    cfg = _wr(**over)
    ms = _FakeMS(list(_CAP_RANKS), _CAP_RANKS, {s: 1.0 for s in _CAP_RANKS})
    ms._last_full_snapshot = {"account": {"total_eq_usd": equity}}
    ms._launcher_in_position = {
        f"wick_rejection:X{i}-USDT-SWAP": {"strategy": "wick_rejection", "margin_usd": 20.0}
        for i in range(open_wr)
    }
    _run(_trigger(ms, cfg=cfg))
    assert ms.executed == _backtest_cap_pick(cfg, open_wr, equity)
