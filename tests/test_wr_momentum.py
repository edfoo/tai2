"""Phase 9 of the wick_rejection plan: momentum entry mode (§3.8).

Covers the pure measures (``compute_momentum``) on synthetic tapes, the entry
conditions (``momentum_entry_ok``), the 1m-candle tape of the backtest, the
strategy's momentum gate and window, the live tape recorder on
``MarketService`` (recording, pruning, gap detection), the trigger's watch
loop (entry window, ``momentum`` vs ``momentum_or_clock``, tape gaps), the
backtest's 1m approximation and live/backtest parity of the measures.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.backtest.engine import BacktestEngine
from app.services.backtest.models import BacktestConfig, Candle
from app.services.market_service import MarketService
from app.services.strategies import StrategyHelpers, StrategySignal
from app.services.strategies.defaults import strategy_defaults
from app.services.strategies.wick_rejection import (
    WickRejectionStrategy,
    backtest_entry_timing,
)
from app.services.strategies.wr_momentum import (
    MOMENTUM_MISSING,
    MomentumMeasures,
    attach_momentum,
    compute_momentum,
    momentum_entry_ok,
    tape_from_candles,
)
from app.services.wick_rejection_trigger import WickRejectionTrigger, wake_lead_seconds

SYMBOL = "TEST-USDT-SWAP"
S = 1000
H = 3_600_000
T0 = (1_700_000_000_000 // (24 * H)) * 24 * H


def _tape(points: list[tuple[float, float]], *, side: str = "buy", sz: float = 1.0) -> list[dict[str, Any]]:
    """``(seconds, price)`` points relative to T0."""
    return [{"ts": int(T0 + t * S), "px": p, "sz": sz, "side": side} for t, p in points]


# ── compute_momentum ──────────────────────────────────────────────────────────


class TestComputeMomentum:
    def test_stall_after_high(self) -> None:
        # Rise to 103 at t=60, then drift just below for 2 minutes.
        pts = [(t, 100 + 3 * t / 60) for t in range(0, 61, 5)] + [
            (t, 102.8) for t in range(65, 181, 5)
        ]
        m = compute_momentum(_tape(pts), T0 + 180 * S, 300)
        assert m is not None
        assert m.high_px == pytest.approx(103.0)
        assert m.secs_since_high == pytest.approx(120.0)
        assert m.below_high_pct == pytest.approx((103 - 102.8) / 103 * 100)
        assert m.ret_60s == pytest.approx(0.0)
        assert m.ret_120s == pytest.approx((102.8 / 103.0 - 1) * 100)

    def test_push(self) -> None:
        pts = [(t, 100 + t / 60) for t in range(0, 181, 5)]
        m = compute_momentum(_tape(pts), T0 + 180 * S, 300)
        assert m is not None
        assert m.secs_since_high == 0.0 and m.below_high_pct == 0.0
        assert m.ret_60s == pytest.approx((103 / 102 - 1) * 100)
        assert m.accel_60s == pytest.approx((103 / 102 - 1) * 100 - (102 / 101 - 1) * 100)

    def test_drop(self) -> None:
        pts = [(t, 103.0) for t in range(0, 121, 5)] + [(t, 103 - (t - 120) / 30) for t in range(125, 181, 5)]
        m = compute_momentum(_tape(pts), T0 + 180 * S, 300)
        assert m is not None
        assert m.ret_60s == pytest.approx((101 / 103 - 1) * 100)
        assert m.ret_60s < -1.0

    def test_short_tape_returns_none_for_long_lookbacks(self) -> None:
        m = compute_momentum(_tape([(150, 100.0), (170, 101.0)]), T0 + 180 * S, 300)
        assert m is not None
        assert m.ret_30s == pytest.approx(1.0)  # 150 s print is 30 s back
        assert m.ret_60s is None and m.ret_120s is None
        assert m.rate_ratio_60s is None  # less than a minute of tape

    def test_first_touch_of_high_counts(self) -> None:
        m = compute_momentum(_tape([(0, 100.0), (30, 103.0), (90, 102.0), (150, 103.0), (170, 102.9)]),
                             T0 + 180 * S, 300)
        assert m is not None and m.secs_since_high == pytest.approx(150.0)

    def test_ignores_trades_after_now_and_before_window(self) -> None:
        tape = _tape([(0, 200.0), (100, 100.0), (170, 101.0), (200, 300.0)])
        m = compute_momentum(tape, T0 + 180 * S, 120)
        assert m is not None
        assert m.high_px == 101.0 and m.last_px == 101.0 and m.trades == 2

    def test_buy_share_and_rate(self) -> None:
        tape = _tape([(t, 100.0) for t in range(0, 120, 10)], side="sell")
        tape += _tape([(t, 100.0) for t in range(120, 180, 2)], side="buy", sz=3.0)
        tape += _tape([(170, 100.0)], side="sell", sz=10.0)
        tape.sort(key=lambda r: r["ts"])
        m = compute_momentum(tape, T0 + 180 * S, 180)
        assert m is not None
        # Window bounds are exclusive at the start: the 120 s buy and the 0 s
        # sell fall outside the last 60 s / the 180 s window.
        assert m.buy_share_60s == pytest.approx(87 / 97)
        # 30 trades in the last 60 s vs 42 over 3 minutes.
        assert m.rate_ratio_60s == pytest.approx(30 / (42 / 3))

    def test_empty(self) -> None:
        assert compute_momentum([], T0, 300) is None
        assert compute_momentum(_tape([(500, 1.0)]), T0 + 10 * S, 300) is None

    def test_dict_round_trip(self) -> None:
        m = compute_momentum(_tape([(0, 100.0), (60, 101.0)]), T0 + 60 * S, 300)
        assert m is not None
        assert MomentumMeasures.from_dict(json.loads(json.dumps(m.to_dict()))) == m


# ── momentum_entry_ok ─────────────────────────────────────────────────────────


def _m(**over: Any) -> MomentumMeasures:
    base = dict(now_ms=T0, last_px=102.8, ret_30s=0.0, ret_60s=0.0, ret_120s=0.0,
                accel_60s=0.0, high_px=103.0, secs_since_high=120.0, below_high_pct=0.2,
                buy_share_60s=0.5, rate_ratio_60s=1.0, trades=50, tape_start_ms=T0)
    base.update(over)
    return MomentumMeasures(**base)


DEFAULTS = strategy_defaults("wick_rejection")


class TestEntryOk:
    def test_defaults_pass(self) -> None:
        assert momentum_entry_ok(_m(), DEFAULTS) == (True, [])

    @pytest.mark.parametrize("over,key", [
        ({"secs_since_high": 30.0}, "stall_min_seconds"),
        ({"below_high_pct": 2.0}, "max_below_high_pct"),
        ({"ret_60s": -2.3}, "min_ret_60s"),
    ])
    def test_each_default_condition(self, over: dict[str, Any], key: str) -> None:
        ok, fails = momentum_entry_ok(_m(**over), DEFAULTS)
        assert not ok and fails[0].startswith(key)
        assert momentum_entry_ok(_m(**over), {**DEFAULTS, key: None})[0]

    def test_off_by_default_vetoes(self) -> None:
        pushing = _m(ret_60s=1.5, buy_share_60s=0.9)
        assert momentum_entry_ok(pushing, DEFAULTS)[0]
        assert not momentum_entry_ok(pushing, {**DEFAULTS, "max_ret_60s": 1.0})[0]
        assert not momentum_entry_ok(pushing, {**DEFAULTS, "max_buy_share_60s": 0.6})[0]

    def test_missing_measure_fails_only_when_enabled(self) -> None:
        m = _m(ret_60s=None, buy_share_60s=None)
        ok, fails = momentum_entry_ok(m, DEFAULTS)
        assert not ok and fails == ["min_ret_60s: no data"]
        assert momentum_entry_ok(m, {**DEFAULTS, "min_ret_60s": None})[0]
        assert momentum_entry_ok(None, DEFAULTS) == (False, ["no tape"])


# ── 1m tape (backtest approximation) ─────────────────────────────────────────


class TestTapeFromCandles:
    def test_paths(self) -> None:
        green = Candle(ts=T0, open=100, high=103, low=99, close=102, volume=1)
        red = {"ts": T0 + 60 * S, "open": 102, "high": 104, "low": 98, "close": 99}
        tape = tape_from_candles([green, red])
        assert [r["px"] for r in tape] == [100, 99, 103, 102, 102, 104, 98, 99]
        assert [r["ts"] - T0 for r in tape] == [0, 20000, 40000, 59999, 60000, 80000, 100000, 119999]
        assert all(r["side"] == "" for r in tape)

    def test_ret_60s_is_last_candle_change_at_its_close(self) -> None:
        candles = [Candle(ts=T0 + i * 60 * S, open=100 + i, high=101.5 + i, low=99.5 + i,
                          close=101 + i, volume=1) for i in range(5)]
        m = compute_momentum(tape_from_candles(candles), T0 + 5 * 60 * S, 300)
        assert m is not None
        assert m.ret_60s == pytest.approx((105 / 104 - 1) * 100)
        assert m.buy_share_60s is None


# ── Strategy gate ─────────────────────────────────────────────────────────────


def _closed_hours() -> list[dict[str, Any]]:
    out, price = [], 100.0
    for i in range(12):
        o = price
        c = o + (1.0 if i == 10 else (0.2 if i % 2 == 0 else -0.2))
        out.append({"ts": T0 + i * H, "open": o, "high": max(o, c) + 0.05,
                    "low": min(o, c) - 0.05, "close": c, "volume": 1.0})
        price = c
    return out


def _snap(seconds_to_close: float) -> dict[str, Any]:
    closed = _closed_hours()
    f_ts = T0 + 12 * H
    forming = {"ts": f_ts, "open": closed[-1]["close"], "high": 103.0,
               "low": closed[-1]["close"] - 0.05, "close": 102.8, "volume": 1.0}
    return {
        "as_of_ts": int(f_ts + H - seconds_to_close * 1000),
        "market_data": {SYMBOL: {"indicators": {}, "timeframes": {
            "1H": {"ohlcv": closed, "forming_candle": forming}}}},
    }


def _helpers(log: list[str] | None = None) -> StrategyHelpers:
    return StrategyHelpers(extract_float=MarketService._extract_float,
                           emit_debug=(log.append if log is not None else (lambda m: None)),
                           get_last_price=lambda s: None)


def _eval(snap: dict[str, Any], log: list[str] | None = None, **over: Any) -> StrategySignal | None:
    return WickRejectionStrategy().evaluate(SYMBOL, snap, {"enabled": True, **over}, _helpers(log))


class TestStrategyGate:
    def test_momentum_widens_window(self) -> None:
        snap = attach_momentum(_snap(110), SYMBOL, _m())
        sig = _eval(snap, entry_mode="momentum")
        assert sig is not None and "momentum:" in sig.rationale
        # Beyond momentum_entry_max_seconds_to_close (120).
        assert _eval(attach_momentum(_snap(130), SYMBOL, _m()), entry_mode="momentum") is None

    def test_failing_momentum_vetoes(self) -> None:
        log: list[str] = []
        snap = attach_momentum(_snap(60), SYMBOL, _m(secs_since_high=10.0))
        assert _eval(snap, log, entry_mode="momentum_or_clock") is None
        assert any("veto momentum (stall_min_seconds" in m for m in log)

    def test_momentum_mode_needs_measures(self) -> None:
        assert _eval(_snap(40), entry_mode="momentum") is None

    def test_momentum_or_clock_without_measures_is_clock(self) -> None:
        sig = _eval(_snap(40), entry_mode="momentum_or_clock")
        assert sig is not None and sig.rationale.endswith("clock entry")
        assert _eval(_snap(110), entry_mode="momentum_or_clock") is None  # clock window max 90

    def test_missing_tape_vetoes(self) -> None:
        snap = attach_momentum(_snap(60), SYMBOL, None)
        assert _eval(snap, entry_mode="momentum_or_clock") is None

    def test_clock_mode_ignores_measures(self) -> None:
        snap = attach_momentum(_snap(40), SYMBOL, _m(secs_since_high=0.0))
        assert _eval(snap) is not None
        assert _eval(attach_momentum(_snap(110), SYMBOL, _m())) is None

    def test_attach_does_not_mutate(self) -> None:
        snap = _snap(60)
        out = attach_momentum(snap, SYMBOL, _m())
        assert "wr_momentum" not in snap["market_data"][SYMBOL]
        assert out["market_data"][SYMBOL]["wr_momentum"]["secs_since_high"] == 120.0


# ── Live tape recorder (MarketService) ───────────────────────────────────────


class _DummyState:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _service() -> MarketService:
    svc = MarketService(
        state_service=_DummyState(), enable_websocket=False, symbols=[SYMBOL, "OTHER-USDT-SWAP"],
        account_api=object(), market_api=object(), public_api=object(), trade_api=object(),
    )
    # Never let a test reach OKX (see memory: MarketService builds live clients).
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(svc, attr, None)
    return svc


def _trade_msg(sym: str, ts_ms: int, px: float, side: str = "buy") -> dict[str, Any]:
    return {"arg": {"channel": "trades", "instId": sym},
            "data": [{"instId": sym, "px": str(px), "sz": "2", "side": side, "ts": str(ts_ms)}]}


class TestLiveTape:
    def test_records_registered_symbols_and_prunes(self) -> None:
        svc = _service()
        svc._subscribed_symbols = {SYMBOL, "OTHER-USDT-SWAP"}
        svc.start_wr_tape([SYMBOL], keep_seconds=60)
        for t in (0, 30, 61, 90):
            svc._handle_ws_message(_trade_msg(SYMBOL, T0 + t * S, 100 + t))
        svc._handle_ws_message(_trade_msg("OTHER-USDT-SWAP", T0, 5.0))
        tape, gap = svc.wr_tape(SYMBOL)
        assert gap is None
        assert [r["ts"] - T0 for r in tape] == [30_000, 61_000, 90_000]
        assert tape[-1] == {"ts": T0 + 90 * S, "px": 190.0, "sz": 2.0, "side": "buy"}
        assert svc.wr_tape("OTHER-USDT-SWAP") == ([], "not recorded")
        svc.stop_wr_tape()
        svc._handle_ws_message(_trade_msg(SYMBOL, T0 + 100 * S, 1.0))
        assert svc.wr_tape(SYMBOL) == ([], "not recorded")

    def test_gap_detection(self) -> None:
        svc = _service()
        svc._subscribed_symbols = {SYMBOL}
        svc.start_wr_tape([SYMBOL, "OTHER-USDT-SWAP"], keep_seconds=300)
        assert svc.wr_tape("OTHER-USDT-SWAP")[1] == "not subscribed at watch start"
        assert svc.wr_tape(SYMBOL)[1] is None
        svc._ws_epoch += 1  # reconnect
        assert svc.wr_tape(SYMBOL)[1] == "websocket reconnected during the watch"


# ── Trigger watch loop ────────────────────────────────────────────────────────

HOUR = 3600.0
H0_S = T0 / 1000.0 + 12 * HOUR  # hour whose close the trigger targets is H0_S + HOUR


class _Clock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


class _WatchMS:
    """Fake market service: tape from a price function of time."""

    def __init__(self, clock: _Clock, prices: dict[str, Any], gaps: dict[str, str] | None = None,
                 clock_signal: bool = True) -> None:
        self.clock = clock
        self.prices = prices
        self.gaps = gaps or {}
        self.clock_signal = clock_signal
        self.executed: list[tuple[str, float]] = []
        self.built: list[tuple[str, Any]] = []
        self.log: list[str] = []
        self.tape_started: list[str] = []
        self.tape_stopped = False
        self._launcher_in_position: dict[str, Any] = {}
        self._last_full_snapshot = {"account": {"total_eq_usd": 1000.0}}
        self.wr_cfg = dict(DEFAULTS, enabled=True)

    def get_screener_universe(self, name: str) -> list[str]:
        return list(self.prices)

    def start_wr_tape(self, symbols, keep_seconds) -> None:
        self.tape_started = list(symbols)
        self.start = self.clock.now

    def stop_wr_tape(self) -> None:
        self.tape_stopped = True

    def wr_tape(self, sym: str):
        if sym in self.gaps:
            return [], self.gaps[sym]
        fn = self.prices[sym]
        t = self.start
        out = []
        while t <= self.clock.now + 1e-9:
            out.append({"ts": int(t * 1000), "px": fn(t - self.start), "sz": 1.0, "side": "buy"})
            t += 1.0
        return out, None

    async def refresh_timeframe_candles(self, sym: str, bar: str) -> int:
        return 100

    async def fetch_trigger_price(self, sym: str, *, max_age_seconds: float):
        return self.prices[sym](self.clock.now - self.start), "ws", 0.1

    def build_launcher_decisions(self, sym: str, strategies=None, *, price=None, momentum=None):
        self.built.append((sym, momentum))
        if momentum is None and not self.clock_signal:
            return []
        # Mirror the strategy: the momentum gate and window live there.
        snap = attach_momentum(_snap(HOUR - (self.clock.now % HOUR)), SYMBOL, momentum) \
            if momentum is not None else _snap(HOUR - (self.clock.now % HOUR))
        cfg = dict(self.wr_cfg)
        sig = WickRejectionStrategy().evaluate(SYMBOL, snap, cfg, _helpers())
        if sig is None:
            return []
        return [{"action": "SELL", "symbol": sym, "_rank_score": 1.0, "_margin_usd": 20.0,
                 "_strategy_name": "wick_rejection", "_momentum": momentum is not None}]

    async def handle_llm_decision(self, decision: dict[str, Any], ctx: dict[str, Any]) -> bool:
        self.executed.append((decision["symbol"], HOUR - (self.clock.now % HOUR)))
        self._launcher_in_position[f"wick_rejection:{decision['symbol']}"] = {
            "strategy": "wick_rejection", "margin_usd": 20.0}
        return True

    def _emit_debug(self, msg: str) -> None:
        self.log.append(msg)


def _stall_at_high(t: float) -> float:
    """Up to 103 over the first 60 s of the watch, then 102.8 flat."""
    return 100 + 3 * t / 60 if t <= 60 else 102.8


def _still_pushing(t: float) -> float:
    return 100 + t / 100


def _watch(ms: _WatchMS, clock: _Clock, mode: str) -> Any:
    ms.wr_cfg["entry_mode"] = mode
    rc = {"launcher": {"mode": "launcher_only", "strategies": {"wick_rejection": ms.wr_cfg}},
          "execution_enabled": True}
    app = SimpleNamespace(state=SimpleNamespace(runtime_config=rc, market_service=ms))
    trig = WickRejectionTrigger(app, asyncio.Lock(), clock=clock, sleep=clock.sleep)
    return trig, asyncio.run(trig.run_watch())


class TestWatch:
    def test_wake_lead(self) -> None:
        assert wake_lead_seconds(DEFAULTS) == 40
        assert wake_lead_seconds({**DEFAULTS, "entry_mode": "momentum"}) == 300

    def test_enters_inside_entry_window_only(self) -> None:
        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"A": _stall_at_high})
        trig, res = _watch(ms, clock, "momentum")
        assert res.executed == ["A"]
        # Momentum passed long before (stall ≥ 60 s from t=120), entered at the
        # first evaluation inside 120 s.
        assert ms.executed[0][1] == pytest.approx(120.0, abs=2.0)
        assert any("before the 120s entry window" in m for m in ms.log)
        assert ms.tape_started == ["A"] and ms.tape_stopped
        assert trig.watch["active"] is False and trig.watch["symbols"]["A"]["status"] == "entered"

    def test_momentum_mode_never_enters_without_trigger(self) -> None:
        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"A": _still_pushing})
        _, res = _watch(ms, clock, "momentum")
        assert res.executed == []
        assert all(m is not None for _, m in ms.built)  # no clock evaluation
        assert any("no entry this hour" in m for m in ms.log)
        assert seconds_left(clock) < 10.0 + 2.0

    def test_momentum_or_clock_falls_back(self) -> None:
        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"A": _still_pushing})
        _, res = _watch(ms, clock, "momentum_or_clock")
        assert res.executed == ["A"]
        assert ms.executed[0][1] <= 40.0 and ms.executed[0][1] >= 38.0
        assert ms.built[-1] == ("A", None)
        assert any("clock fallback" in m for m in ms.log)

    def test_tape_gap(self) -> None:
        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"A": _stall_at_high, "B": _stall_at_high},
                      gaps={"B": "websocket reconnected during the watch"})
        _, res = _watch(ms, clock, "momentum")
        assert res.executed == ["A"]
        assert res.dropped["B"].startswith("tape gap")
        assert all(sym != "B" for sym, _ in ms.built)

        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"B": _stall_at_high}, gaps={"B": "not subscribed at watch start"})
        _, res = _watch(ms, clock, "momentum_or_clock")
        assert res.executed == ["B"]  # clock entry still allowed
        assert ms.built == [("B", None)]

    def test_respects_max_concurrent(self) -> None:
        clock = _Clock(H0_S + HOUR - 300)
        ms = _WatchMS(clock, {"A": _stall_at_high, "B": _stall_at_high})
        ms.wr_cfg["max_concurrent_positions"] = 1
        _, res = _watch(ms, clock, "momentum")
        assert len(res.executed) == 1


def seconds_left(clock: _Clock) -> float:
    return HOUR - (clock.now % HOUR)


# ── Backtest 1m approximation + parity ────────────────────────────────────────


class _SpyStrategy:
    name = "wick_rejection"

    def __init__(self, fire_on_clock: bool = False) -> None:
        self.seen: list[Any] = []
        self.fire_on_clock = fire_on_clock

    def evaluate(self, symbol, snapshot, config, helpers):
        mom = snapshot["market_data"][symbol].get("wr_momentum")
        self.seen.append(mom)
        if mom is None and self.fire_on_clock:
            return StrategySignal(direction="sell", rationale="clock")
        return None


def _engine(cfg: dict[str, Any]) -> BacktestEngine:
    return BacktestEngine(BacktestConfig(
        symbols=[SYMBOL], timeframe="1H", start_ts=0, end_ts=1000,
        strategy_names=["wick_rejection"],
        launcher_config={"strategies": {"wick_rejection": cfg}},
    ))


class TestBacktest:
    candles = [Candle(ts=T0 + i * 60 * S, open=100 + 0.2 * i, high=100.5 + 0.2 * i,
                      low=99.9 + 0.2 * i, close=100.2 + 0.2 * i, volume=1) for i in range(8)]

    def test_measures_parity_with_live(self) -> None:
        """Backtest attaches exactly what compute_momentum gives on the 1m tape,
        and live attaches the same dict for the same measures."""
        cfg = {**DEFAULTS, "enabled": True, "entry_mode": "momentum"}
        spy = _SpyStrategy()
        now = self.candles[-1].ts + 60 * S
        _engine(cfg)._evaluate_wick_rejection(spy, SYMBOL, _snap(60), cfg, self.candles, now)
        expected = compute_momentum(tape_from_candles(self.candles), now, 300)
        assert expected is not None
        assert spy.seen == [expected.to_dict()]

        svc = _service()
        svc._strategies = [spy]
        svc._last_full_snapshot = _snap(60)
        svc._build_closed_candle_snapshot = lambda snap, sym, **k: snap  # type: ignore[method-assign]
        svc._strategy_allowed_on_symbol = lambda name, sym: True  # type: ignore[method-assign]
        svc.set_launcher_config({"mode": "launcher_only", "strategies": {"wick_rejection": cfg}})
        svc._launcher_evaluate_signals(SYMBOL, ["wick_rejection"], momentum=expected)
        svc._launcher_evaluate_signals(SYMBOL, ["wick_rejection"], momentum=MOMENTUM_MISSING)
        assert spy.seen[1:] == [expected.to_dict(), {"missing": True}]

    def test_momentum_or_clock_falls_back_to_clock(self) -> None:
        cfg = {**DEFAULTS, "enabled": True, "entry_mode": "momentum_or_clock"}
        spy = _SpyStrategy(fire_on_clock=True)
        sig = _engine(cfg)._evaluate_wick_rejection(
            spy, SYMBOL, _snap(60), cfg, self.candles, self.candles[-1].ts + 60 * S)
        assert sig is not None and len(spy.seen) == 2 and spy.seen[1] is None

    def test_clock_and_momentum_modes_evaluate_once(self) -> None:
        for mode, first in (("clock", None), ("momentum", dict)):
            cfg = {**DEFAULTS, "enabled": True, "entry_mode": mode}
            spy = _SpyStrategy(fire_on_clock=True)
            _engine(cfg)._evaluate_wick_rejection(
                spy, SYMBOL, _snap(60), cfg, self.candles, self.candles[-1].ts + 60 * S)
            assert len(spy.seen) == 1
            assert (spy.seen[0] is None) if first is None else isinstance(spy.seen[0], dict)

    def test_entry_timing_provenance(self) -> None:
        t = backtest_entry_timing({"entry_mode": "momentum_or_clock"},
                                  evaluation_mode="finer_ltf", evaluation_timeframe="1m")
        assert t["entry_mode"] == "momentum_or_clock"
        assert t["momentum_seconds_before_close"] == [60.0, 120.0]
        assert "1m approximation" in t["momentum"]
        assert "momentum" not in backtest_entry_timing(
            {}, evaluation_mode="finer_ltf", evaluation_timeframe="1m")
