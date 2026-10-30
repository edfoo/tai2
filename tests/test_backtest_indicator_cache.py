"""Parity tests for the backtest indicator cache.

The cache precomputes each pandas-ta series once over the full candle array
and serves per-step blocks by slicing.  This is only valid because every
indicator is causal.  These tests pin that invariant: a cached block must be
byte-for-byte identical to a full recompute over the same prefix window.
"""

from __future__ import annotations

import math
from typing import Any

import pytest

from app.services.backtest.indicator_cache import IndicatorCache
from app.services.backtest.models import Candle
from app.services.backtest.snapshot_builder import SnapshotBuilder
from app.services.market_service import MarketService


def _rows(n: int, *, start_ts: int = 1_700_000_000_000, step_ms: int = 900_000) -> list[list[Any]]:
    """Build a deterministic, mildly trending candle series."""
    rows: list[list[Any]] = []
    price = 100.0
    for i in range(n):
        # Deterministic pseudo-random walk (no RNG dependency).
        drift = ((i * 37) % 11 - 5) * 0.05
        price = max(1.0, price + drift)
        high = price + 0.4
        low = price - 0.4
        rows.append([start_ts + i * step_ms, price, high, low, price, 1000.0 + (i % 7) * 50.0])
    return rows


def _assert_equal(a: Any, b: Any, path: str = "") -> None:
    """Recursively assert two indicator payloads are numerically identical."""
    if isinstance(a, dict):
        assert isinstance(b, dict), f"{path}: type mismatch"
        assert a.keys() == b.keys(), f"{path}: key mismatch {a.keys() ^ b.keys()}"
        for key in a:
            _assert_equal(a[key], b[key], f"{path}.{key}")
        return
    if isinstance(a, list):
        assert isinstance(b, list), f"{path}: type mismatch"
        assert len(a) == len(b), f"{path}: length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            _assert_equal(x, y, f"{path}[{i}]")
        return
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return
        assert a == pytest.approx(b, rel=1e-12, abs=1e-12), f"{path}: {a} != {b}"
        return
    assert a == b, f"{path}: {a!r} != {b!r}"


class TestIndicatorCacheParity:
    def test_block_matches_recompute_at_multiple_windows(self) -> None:
        rows = _rows(400)
        cache = IndicatorCache(rows)
        for end_idx in (60, 150, 300, 399):
            expected = MarketService._compute_indicators(rows[: end_idx + 1])
            _assert_equal(cache.block_at(end_idx), expected, f"idx={end_idx}")

    def test_block_for_ts_matches_recompute(self) -> None:
        rows = _rows(300)
        cache = IndicatorCache(rows)
        ts = rows[200][0]
        expected = MarketService._compute_indicators(rows[:201])
        _assert_equal(cache.block_for_ts(ts), expected)

    def test_block_for_ts_before_first_candle_is_none(self) -> None:
        cache = IndicatorCache(_rows(50))
        assert cache.block_for_ts(0) is None

    def test_empty_input_returns_empty_bundle(self) -> None:
        cache = IndicatorCache([])
        assert cache.block_at(0) == MarketService._empty_indicators()

    def test_negative_index_returns_empty_bundle(self) -> None:
        cache = IndicatorCache(_rows(50))
        assert cache.block_at(-1) == MarketService._empty_indicators()

    def test_end_idx_for_ts(self) -> None:
        rows = _rows(10)
        cache = IndicatorCache(rows)
        assert cache.end_idx_for_ts(rows[0][0]) == 0
        assert cache.end_idx_for_ts(rows[4][0]) == 4
        # Between candles → last candle at or before ts.
        assert cache.end_idx_for_ts(rows[4][0] + 1) == 4
        assert cache.end_idx_for_ts(rows[0][0] - 1) == -1

    def test_unsorted_input_falls_back_to_exact_recompute(self) -> None:
        """Unsorted input must still match the positional-prefix recompute.

        The cache cannot slice a sorted series for an unsorted prefix, so it
        must fall back to recomputing.  This pins that fallback.
        """
        rows = _rows(60)
        # Move the last candle to the middle → out of order.
        shuffled = rows[:-1]
        shuffled.insert(30, rows[-1])
        cache = IndicatorCache(shuffled)
        for end_idx in (10, 30, 45, len(shuffled) - 1):
            expected = MarketService._compute_indicators(shuffled[: end_idx + 1])
            _assert_equal(cache.block_at(end_idx), expected, f"idx={end_idx}")


class TestSnapshotBuilderCacheParity:
    def _candles(self, n: int) -> list[Candle]:
        return [
            Candle(
                ts=r[0], open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5],
                quote_volume=r[4] * r[5],
            )
            for r in _rows(n)
        ]

    def test_ltf_indicators_match_recompute(self) -> None:
        candles = self._candles(300)
        builder = SnapshotBuilder("BTC-USDT-SWAP", candles, None, "15m")
        for idx in (80, 200, 299):
            snap = builder.build(idx)
            got = snap["market_data"]["BTC-USDT-SWAP"]["indicators"]
            raw = [[c.ts, c.open, c.high, c.low, c.close, c.volume] for c in candles[: idx + 1]]
            expected = MarketService._compute_indicators(raw)
            expected["structure"] = MarketService._compute_structure(raw)
            _assert_equal(got, expected, f"idx={idx}")

    def test_timeframe_block_matches_recompute(self) -> None:
        candles = self._candles(300)
        # A coarser "1H" series derived from every 4th candle.
        hourly = candles[::4]
        builder = SnapshotBuilder(
            "BTC-USDT-SWAP", candles, None, "15m", tf_candles={"1H": hourly}
        )
        idx = 200
        snap = builder.build(idx)
        block = snap["market_data"]["BTC-USDT-SWAP"]["timeframes"]["1H"]
        current_ts = candles[idx].ts
        window = [c for c in hourly if c.ts <= current_ts]
        raw = [[c.ts, c.open, c.high, c.low, c.close, c.volume] for c in window]
        expected = MarketService._compute_indicators(raw)
        expected["structure"] = MarketService._compute_structure(raw)
        # Compare only the indicator keys (the block also carries HTF keys).
        for key in expected:
            _assert_equal(block[key], expected[key], f"1H.{key}")

    def test_htf_indicators_match_recompute(self) -> None:
        candles = self._candles(300)
        hourly = candles[::4]
        builder = SnapshotBuilder("BTC-USDT-SWAP", candles, hourly, "15m")
        idx = 200
        snap = builder.build(idx)
        ind = snap["market_data"]["BTC-USDT-SWAP"]["indicators"]
        current_ts = candles[idx].ts
        window = [c for c in hourly if c.ts <= current_ts]
        raw = [[c.ts, c.open, c.high, c.low, c.close, c.volume] for c in window]
        expected = MarketService._compute_indicators(raw)
        _assert_equal(ind["htf_indicators"], expected, "htf")
        assert ind["adx_htf"] == expected["adx"]["value"]
        assert ind["choppiness_htf"] == expected["choppiness"]
