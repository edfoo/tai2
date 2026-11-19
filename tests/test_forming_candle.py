"""Closed-candle alignment and ``forming_candle`` exposure (live + backtest).

A snapshot describes one moment.  Every ``timeframes[<tf>]`` block and every
HTF layer must contain only candles closed by that moment, and the bar in
progress is exposed separately as ``forming_candle``.  OKX candle timestamps
are the bar's *open* time, so selecting by ``ts <= now`` leaks the in-progress
bar's final high/low/close — these tests pin that it never happens, and that
live and the backtest agree on what is visible at the same moment.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest

from app.services.backtest.models import Candle
from app.services.backtest.snapshot_builder import SnapshotBuilder
from app.services.market_service import MarketService

SYMBOL = "BTC-USDT-SWAP"
M = 60_000
H = 3_600_000
T0 = (1_700_000_000_000 // (4 * H)) * 4 * H  # aligned to a 4H boundary
SPIKE = 50.0


def _minutes(hours: int) -> list[Candle]:
    """1m candles with a spike in the last minute of every hour.

    A snapshot taken mid-hour that can see the spike is reading the future.
    """
    candles: list[Candle] = []
    price = 100.0
    for i in range(hours * 60):
        price = max(1.0, price + ((i * 37) % 11 - 5) * 0.02)
        high = price + 0.1 + (SPIKE if i % 60 == 59 else 0.0)
        candles.append(Candle(ts=T0 + i * M, open=price, high=high, low=price - 0.1, close=price, volume=1.0 + i % 5))
    return candles


def _aggregate(candles: list[Candle], bar_ms: int) -> list[Candle]:
    buckets: dict[int, list[Candle]] = {}
    for c in candles:
        buckets.setdefault(c.ts - (c.ts - T0) % bar_ms, []).append(c)
    return [
        Candle(
            ts=ts,
            open=group[0].open,
            high=max(c.high for c in group),
            low=min(c.low for c in group),
            close=group[-1].close,
            volume=sum(c.volume for c in group),
        )
        for ts, group in sorted(buckets.items())
    ]


def _expected_forming(minutes: list[Candle], bucket_ts: int, current_ts: int) -> dict[str, Any]:
    part = [c for c in minutes if bucket_ts <= c.ts <= current_ts]
    return {
        "ts": bucket_ts,
        "open": part[0].open,
        "high": max(c.high for c in part),
        "low": min(c.low for c in part),
        "close": part[-1].close,
        "volume": sum(c.volume for c in part),
    }


def _row_ts(row: Any) -> int:
    return int(row["ts"] if isinstance(row, dict) else float(row[0]))


def _okx_rows(candles: list[Candle]) -> list[list[str]]:
    """Raw OKX candle rows: strings, newest first."""
    return [
        [str(c.ts), str(c.open), str(c.high), str(c.low), str(c.close), str(c.volume), "0", "0", "0"]
        for c in reversed(candles)
    ]


def _finer_ltf_snapshot(builder: SnapshotBuilder, m15: list[Candle], m1: list[Candle], current_ts: int) -> dict[str, Any]:
    """Mirror the engine's finer_ltf step: closed 15m window + in-progress 15m bar."""
    bucket = current_ts - (current_ts - T0) % (15 * M)
    closed = [c for c in m15 if c.ts < bucket]
    part = [c for c in m1 if bucket <= c.ts <= current_ts]
    incomplete = Candle(
        ts=bucket,
        open=part[0].open,
        high=max(c.high for c in part),
        low=min(c.low for c in part),
        close=part[-1].close,
        volume=sum(c.volume for c in part),
    )
    return builder.build_with_incomplete_ltf(closed, incomplete, current_ts)


@pytest.fixture(scope="module")
def series() -> dict[str, list[Candle]]:
    m1 = _minutes(60)
    return {"1m": m1, "15m": _aggregate(m1, 15 * M), "1H": _aggregate(m1, H), "4H": _aggregate(m1, 4 * H)}


@pytest.fixture(scope="module")
def builder(series: dict[str, list[Candle]]) -> SnapshotBuilder:
    return SnapshotBuilder(
        SYMBOL,
        series["15m"],
        series["1H"],
        "15m",
        tf_candles={"1H": series["1H"], "4H": series["4H"]},
        eval_candles=series["1m"],
        eval_timeframe="1m",
    )


# ── Backtest ──────────────────────────────────────────────────────────────────


class TestBacktestNoLookahead:
    def test_finer_ltf_blocks_hold_only_closed_candles(self, builder, series) -> None:
        m1 = series["1m"]
        for current_ts in [c.ts for c in m1[50 * 60: 53 * 60]]:
            moment = current_ts + M
            snap = _finer_ltf_snapshot(builder, series["15m"], m1, current_ts)
            sym = snap["market_data"][SYMBOL]
            for tf, tf_ms in (("1H", H), ("4H", 4 * H)):
                block = sym["timeframes"][tf]
                assert all(_row_ts(r) + tf_ms <= moment for r in block["ohlcv"]), (tf, current_ts)
            # HTF layers: base (1H) and the 1H block's own (4H).
            assert all(_row_ts(r) + H <= moment for r in sym["indicators"]["ohlcv_htf"])
            assert all(_row_ts(r) + 4 * H <= moment for r in sym["timeframes"]["1H"]["ohlcv_htf"])

    def test_finer_ltf_forming_candle_is_built_from_minutes_so_far(self, builder, series) -> None:
        m1 = series["1m"]
        for current_ts in [c.ts for c in m1[50 * 60: 52 * 60]]:
            moment = current_ts + M
            hour = current_ts - (current_ts - T0) % H
            forming = _finer_ltf_snapshot(builder, series["15m"], m1, current_ts)["market_data"][SYMBOL][
                "timeframes"
            ]["1H"]["forming_candle"]
            if hour + H <= moment:
                # Last minute of the hour: the bar has closed and is in the block.
                assert forming is None
            else:
                assert forming == pytest.approx(_expected_forming(m1, hour, current_ts))
                assert forming["high"] < forming["close"] + SPIKE  # the spike is still in the future

    def test_last_minute_of_hour_moves_bar_into_closed_block(self, builder, series) -> None:
        current_ts = T0 + 50 * H + 59 * M  # closes exactly at the hour
        block = _finer_ltf_snapshot(builder, series["15m"], series["1m"], current_ts)["market_data"][SYMBOL][
            "timeframes"
        ]["1H"]
        assert block["forming_candle"] is None
        assert _row_ts(block["ohlcv"][-1]) == T0 + 50 * H

    def test_base_forming_candle_is_the_incomplete_ltf_bar(self, builder, series) -> None:
        current_ts = T0 + 50 * H + 7 * M
        indicators = _finer_ltf_snapshot(builder, series["15m"], series["1m"], current_ts)["market_data"][SYMBOL][
            "indicators"
        ]
        assert indicators["forming_candle"] == pytest.approx(
            _expected_forming(series["1m"], T0 + 50 * H, current_ts)
        )

    def test_closed_mode_blocks_hold_only_closed_candles(self, builder, series) -> None:
        m15 = series["15m"]
        for idx in range(200, 216):
            moment = m15[idx].ts + 15 * M
            sym = builder.build(idx)["market_data"][SYMBOL]
            assert sym["indicators"]["forming_candle"] is None
            assert all(_row_ts(r) + H <= moment for r in sym["indicators"]["ohlcv_htf"])
            for tf, tf_ms in (("1H", H), ("4H", 4 * H)):
                block = sym["timeframes"][tf]
                assert block["forming_candle"] is None
                assert all(_row_ts(r) + tf_ms <= moment for r in block["ohlcv"]), (tf, idx)


# ── Live ──────────────────────────────────────────────────────────────────────


class _DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _service() -> MarketService:
    service = MarketService(
        state_service=_DummyStateService(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    service._ohlc_bar = "15m"
    return service


def _live_snapshot(service: MarketService, m1: list[Candle], now_ms: int) -> dict[str, Any]:
    """Live-shaped snapshot as OKX would return it at ``now_ms`` (forming bars included)."""
    seen = [c for c in m1 if c.ts + M <= now_ms]
    rows15 = _okx_rows(_aggregate(seen, 15 * M))
    service._latest_ohlcv_tf[(SYMBOL, "1H")] = _okx_rows(_aggregate(seen, H))
    service._latest_ohlcv_tf[(SYMBOL, "4H")] = _okx_rows(_aggregate(seen, 4 * H))
    indicators = service._compute_indicators(rows15)
    indicators["ohlcv_htf"] = service._latest_ohlcv_tf[(SYMBOL, "1H")]
    indicators["htf_indicators"] = service._compute_indicators(indicators["ohlcv_htf"])
    return {
        "market_data": {
            SYMBOL: {
                "indicators": indicators,
                "timeframes": {"1H": service._build_timeframes_block(SYMBOL, "1H")},
            }
        }
    }


class TestLiveClosedSnapshot:
    def test_split_forming_candle_handles_raw_newest_first_rows(self) -> None:
        rows = [
            [str(T0 + 2 * H), "3", "4", "2", "3.5", "10"],
            [str(T0 + H), "2", "3", "1", "2.5", "10"],
            [str(T0), "1", "2", "0.5", "1.5", "10"],
        ]
        closed, forming = MarketService._split_forming_candle(rows, H, T0 + 2 * H + 10 * M)
        assert [r[0] for r in closed] == [T0, T0 + H]
        assert forming == {"ts": T0 + 2 * H, "open": 3.0, "high": 4.0, "low": 2.0, "close": 3.5, "volume": 10.0}
        closed, forming = MarketService._split_forming_candle(rows, H, T0 + 3 * H)
        assert forming is None and len(closed) == 3

    def test_trims_forming_bars_and_exposes_them(self, series) -> None:
        service = _service()
        # 14:30 into a 15m bar — inside its last 10%, which the old heuristic
        # wrongly treated as closed.
        now = T0 + 50 * H + 14 * M + 30_000
        snapshot = _live_snapshot(service, series["1m"], now)
        out = service._build_closed_candle_snapshot(snapshot, SYMBOL, now_ms=now)
        ind = out["market_data"][SYMBOL]["indicators"]
        assert ind["forming_candle"]["ts"] == T0 + 50 * H
        assert _row_ts(ind["ohlcv"][-1]) == T0 + 50 * H - 15 * M
        assert _row_ts(ind["ohlcv_htf"][-1]) == T0 + 49 * H
        block = out["market_data"][SYMBOL]["timeframes"]["1H"]
        assert block["forming_candle"]["ts"] == T0 + 50 * H
        assert _row_ts(block["ohlcv"][-1]) == T0 + 49 * H
        # The 1H block's HTF (4H): the bar opened at 48H is still forming.
        assert _row_ts(block["ohlcv_htf"][-1]) == T0 + 44 * H
        # The input snapshot is not mutated.
        assert "forming_candle" not in snapshot["market_data"][SYMBOL]["indicators"]
        assert out["as_of_ts"] == now and "as_of_ts" not in snapshot

    def test_closed_bar_is_kept(self, series) -> None:
        service = _service()
        now = T0 + 51 * H  # exactly on the hour: the 50H bar has just closed
        snapshot = _live_snapshot(service, series["1m"], now)
        out = service._build_closed_candle_snapshot(snapshot, SYMBOL, now_ms=now)
        block = out["market_data"][SYMBOL]["timeframes"]["1H"]
        assert block["forming_candle"] is None
        assert _row_ts(block["ohlcv"][-1]) == T0 + 50 * H

    def test_timeframe_cache_is_keyed_by_symbol_and_bar(self) -> None:
        """Regression: per-tf candles were stored under ``symbol`` and read via
        ``(symbol, bar)``, so live ``timeframes`` blocks were never built and
        every bar overwrote the previous one."""

        class _Api:
            def get_candlesticks(self, instId: str, bar: str, limit: int) -> dict[str, Any]:
                return {"code": "0", "data": [[bar]]}

        async def _slot() -> tuple[Any, asyncio.Semaphore]:
            sem = asyncio.Semaphore(1)
            await sem.acquire()
            return _Api(), sem

        fake = types.SimpleNamespace(
            _ohlcv_fetch_limit=5, _acquire_candle_slot=_slot, _safe_data=MarketService._safe_data
        )
        cache: dict[Any, Any] = {}

        async def _run() -> None:
            for bar in ("15m", "1H"):
                await MarketService._fetch_ohlcv_bar(fake, SYMBOL, bar, cache, cache_key=(SYMBOL, bar))

        asyncio.run(_run())
        assert cache == {(SYMBOL, "15m"): [["15m"]], (SYMBOL, "1H"): [["1H"]]}


# ── Live ↔ backtest parity ────────────────────────────────────────────────────


class TestFormingParity:
    @pytest.mark.parametrize("minute", [5, 30, 58])
    def test_live_and_backtest_see_the_same_1h_candles(self, builder, series, minute: int) -> None:
        """At the same moment, live and the backtest expose identical closed 1H
        candles, identical 1H indicators and the same forming 1H candle."""
        now = T0 + 50 * H + minute * M
        service = _service()
        live_snap = service._build_closed_candle_snapshot(
            _live_snapshot(service, series["1m"], now), SYMBOL, now_ms=now
        )
        # The backtest step whose close is ``now``.
        back_snap = _finer_ltf_snapshot(builder, series["15m"], series["1m"], now - M)
        assert live_snap["as_of_ts"] == back_snap["as_of_ts"] == now
        live = live_snap["market_data"][SYMBOL]["timeframes"]["1H"]
        back = back_snap["market_data"][SYMBOL]["timeframes"]["1H"]
        assert [_row_ts(r) for r in live["ohlcv"]] == [_row_ts(r) for r in back["ohlcv"]]
        assert live["forming_candle"] == pytest.approx(back["forming_candle"])
        assert live["rsi"] == pytest.approx(back["rsi"], nan_ok=True)
        assert live["atr"] == pytest.approx(back["atr"], nan_ok=True)
        assert [_row_ts(r) for r in live["ohlcv_htf"]] == [_row_ts(r) for r in back["ohlcv_htf"]]
