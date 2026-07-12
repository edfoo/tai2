"""Paginated OKX historical OHLCV fetcher with file cache.

The live ``MarketService`` only fetches the latest N candles (no pagination).
For backtesting we need arbitrary historical periods, so this module walks
backward in time using OKX's ``after`` cursor (max 300 candles per request).

Fetched data is cached to a local JSON file keyed by
``symbol_timeframe_start_end`` so re-running a backtest with different
strategy parameters is instant.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.services.backtest.models import Candle

logger = logging.getLogger(__name__)

# OKX returns at most 300 candles per request.
_OKX_MAX_LIMIT = 300

# Default cache directory (relative to project root).
_DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "backtest_cache"


def _build_market_api(flag: str = "0") -> Any | None:
    """Create a read-only OKX MarketAPI client (no auth needed for candles)."""
    try:
        from okx import MarketData as OkxMarket  # type: ignore[import-untyped]
    except Exception:  # pragma: no cover - optional dependency
        logger.warning("python-okx not installed; historical fetch unavailable")
        return None
    return OkxMarket.MarketAPI(flag=flag)


def _safe_data(response: Any) -> list[list[Any]]:
    """Extract the data list from an OKX SDK response."""
    if response is None:
        return []
    if isinstance(response, dict):
        if response.get("code") != "0":
            logger.warning("OKX API error: %s", response.get("msg", response.get("code")))
            return []
        return response.get("data") or []
    if isinstance(response, list):
        return response
    return []


def _parse_candle(row: list[Any]) -> Candle | None:
    """Convert a raw OKX candle row to a :class:`Candle`."""
    if not row or len(row) < 6:
        return None
    try:
        return Candle(
            ts=int(float(row[0])),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
    except (TypeError, ValueError):
        return None


class HistoricalDataFetcher:
    """Fetches and caches historical OHLCV candles from OKX."""

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        api_flag: str = "0",
    ) -> None:
        self._cache_dir = cache_dir or _DEFAULT_CACHE_DIR
        self._api = _build_market_api(api_flag)

    # ── Public API ────────────────────────────────────────────────────

    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
        *,
        warmup_candles: int = 0,
        progress_cb: Callable[[int, int, str], None] | None = None,
    ) -> list[Candle]:
        """Fetch candles for *symbol* in ``[start_ts, end_ts]`` (ms epoch).

        If ``warmup_candles > 0``, that many extra candles are fetched *before*
        ``start_ts`` for indicator warmup.  The returned list includes warmup
        candles — the caller should slice them off.

        Candles are returned in ascending order (oldest first).
        """
        cache_key = self._cache_key(symbol, timeframe, start_ts, end_ts, warmup_candles)
        cached = self._load_cache(cache_key)
        if cached is not None:
            if progress_cb:
                progress_cb(len(cached), len(cached), "loaded from cache")
            return cached

        if self._api is None:
            logger.error("OKX MarketAPI unavailable — cannot fetch historical data")
            return []

        # Walk backward from end_ts to start_ts using the ``after`` cursor.
        raw_candles = await self._fetch_range(
            symbol=symbol,
            timeframe=timeframe,
            end_ts=end_ts,
            start_ts=start_ts,
            warmup_candles=warmup_candles,
            progress_cb=progress_cb,
        )

        # Sort ascending and deduplicate by ts.
        seen: set[int] = set()
        candles: list[Candle] = []
        for c in sorted(raw_candles, key=lambda c: c.ts):
            if c.ts in seen:
                continue
            seen.add(c.ts)
            candles.append(c)

        self._save_cache(cache_key, candles)
        return candles

    async def fetch_htf_candles(
        self,
        symbol: str,
        ltf_timeframe: str,
        htf_timeframe: str,
        start_ts: int,
        end_ts: int,
        *,
        warmup_candles: int = 0,
        progress_cb: Callable[[int, int, str], None] | None = None,
    ) -> list[Candle]:
        """Fetch higher-timeframe candles covering the same period."""
        # HTF candles are coarser, so we need fewer of them.  Fetch with the
        # same logic but the warmup is in HTF candles.
        return await self.fetch_candles(
            symbol,
            htf_timeframe,
            start_ts,
            end_ts,
            warmup_candles=warmup_candles,
            progress_cb=progress_cb,
        )

    # ── Internal: paginated fetch ─────────────────────────────────────

    async def _fetch_range(
        self,
        *,
        symbol: str,
        timeframe: str,
        end_ts: int,
        start_ts: int,
        warmup_candles: int,
        progress_cb: Callable[[int, int, str], None] | None,
    ) -> list[Candle]:
        """Walk backward from ``end_ts`` to ``start_ts`` using ``after`` cursor."""
        all_candles: list[Candle] = []
        # Start cursor = end_ts (fetch candles older than end_ts)
        after_ts = end_ts
        # Extend the start boundary to accommodate warmup candles.
        # We don't know the exact ts offset, so we fetch a bit more.
        extended_start = start_ts
        if warmup_candles > 0:
            # Estimate: warmup_candles * timeframe_ms.  We over-fetch and trim.
            tf_ms = _timeframe_to_ms(timeframe)
            extended_start = start_ts - warmup_candles * tf_ms

        request_count = 0
        max_requests = 500  # safety cap

        while after_ts > extended_start and request_count < max_requests:
            request_count += 1
            try:
                response = await asyncio.to_thread(
                    self._api.get_candlesticks,
                    instId=symbol,
                    bar=timeframe,
                    limit=_OKX_MAX_LIMIT,
                    after=str(after_ts),
                )
            except Exception as exc:
                logger.warning("Historical fetch failed for %s: %s", symbol, exc)
                break

            rows = _safe_data(response)
            if not rows:
                break

            batch: list[Candle] = []
            for row in rows:
                c = _parse_candle(row)
                if c is not None:
                    batch.append(c)

            if not batch:
                break

            all_candles.extend(batch)

            # OKX returns candles in descending order (newest first).
            # The oldest candle in this batch becomes the next ``after`` cursor.
            oldest_ts = min(c.ts for c in batch)
            if oldest_ts >= after_ts:
                # No progress — avoid infinite loop.
                break
            after_ts = oldest_ts

            if progress_cb:
                progress_cb(len(all_candles), 0, f"fetched {len(all_candles)} candles")

            # Be gentle with rate limits (~20 req/2s for public endpoints).
            await asyncio.sleep(0.05)

        return all_candles

    # ── Internal: file cache ──────────────────────────────────────────

    def _cache_key(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
        warmup_candles: int,
    ) -> str:
        return f"{symbol}_{timeframe}_{start_ts}_{end_ts}_w{warmup_candles}"

    def _load_cache(self, key: str) -> list[Candle] | None:
        path = self._cache_dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return [Candle(**row) for row in data]
        except Exception as exc:
            logger.warning("Failed to load cache %s: %s", key, exc)
            return None

    def _save_cache(self, key: str, candles: list[Candle]) -> None:
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            path = self._cache_dir / f"{key}.json"
            data = [
                {"ts": c.ts, "open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume}
                for c in candles
            ]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as exc:  # pragma: no cover - cache is best-effort
            logger.warning("Failed to save cache %s: %s", key, exc)


# ── Helpers ─────────────────────────────────────────────────────────────


def _timeframe_to_ms(timeframe: str) -> int:
    """Convert an OKX bar string to milliseconds."""
    tf = timeframe.strip().upper()
    if tf.endswith("M"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("H"):
        return int(tf[:-1]) * 3_600_000
    if tf.endswith("D"):
        return int(tf[:-1]) * 86_400_000
    if tf.endswith("W"):
        return int(tf[:-1]) * 604_800_000
    return 3_600_000  # default 1H


def timeframe_ms(timeframe: str) -> int:
    """Public alias for :func:`_timeframe_to_ms`.

    Converts an OKX bar string (e.g. ``"1m"``, ``"15m"``, ``"1H"``, ``"4H"``)
    to milliseconds.  Used by the finer-LTF engine to compute LTF bucket
    boundaries from eval-candle timestamps.
    """
    return _timeframe_to_ms(timeframe)


def ltf_bucket_ts(eval_ts: int, ltf_timeframe: str) -> int:
    """Return the start ts of the LTF bucket containing ``eval_ts``.

    OKX candles are aligned to timeframe boundaries, so the bucket start is
    simply ``eval_ts`` rounded down to the nearest LTF period.  This works
    for any aligned eval candle (1m, 5m, etc.) within a coarser LTF (15m, 1H).
    """
    ltf_ms = _timeframe_to_ms(ltf_timeframe)
    if ltf_ms <= 0:
        return eval_ts
    return eval_ts - (eval_ts % ltf_ms)


def is_finer_than(eval_timeframe: str, ltf_timeframe: str) -> bool:
    """Return True if ``eval_timeframe`` is strictly finer than ``ltf_timeframe``."""
    return _timeframe_to_ms(eval_timeframe) < _timeframe_to_ms(ltf_timeframe)


def htf_for(timeframe: str) -> str:
    """Return the higher timeframe for a given LTF, matching ``_HTF_MAP``."""
    tf = timeframe.strip().upper()
    htf_map = {
        "15M": "1H",
        "1H": "4H",
        "4H": "1D",
    }
    return htf_map.get(tf, "")
