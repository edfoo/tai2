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
import hashlib
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
    """Convert a raw OKX candle row to a :class:`Candle`.

    OKX candle rows are ``[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]``.
    ``vol`` is in contracts, ``volCcy`` in base currency, and ``volCcyQuote``
    in quote currency (USDT) — the latter is what the live screener's
    ``volCcy24h`` filter uses, so we preserve it for faithful reconstruction.
    """
    if not row or len(row) < 6:
        return None
    try:
        quote_volume = 0.0
        if len(row) >= 8 and row[7] not in (None, ""):
            quote_volume = float(row[7])
        return Candle(
            ts=int(float(row[0])),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            quote_volume=quote_volume,
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
        self._last_fetch_provenance: dict[str, Any] = {}
        self._last_funding_provenance: dict[str, Any] = {}
        self._last_instrument_provenance: dict[str, Any] = {}
        self._last_tape_provenance: dict[str, Any] = {}
        self._last_universe_provenance: dict[str, Any] = {}

    @property
    def last_fetch_provenance(self) -> dict[str, Any]:
        """Metadata and content digest for the most recent candle request."""
        return dict(self._last_fetch_provenance)

    @property
    def last_funding_provenance(self) -> dict[str, Any]:
        return dict(self._last_funding_provenance)

    @property
    def last_instrument_provenance(self) -> dict[str, Any]:
        return dict(self._last_instrument_provenance)

    @property
    def last_tape_provenance(self) -> dict[str, Any]:
        return dict(self._last_tape_provenance)

    async def fetch_funding_rates(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> list[dict[str, float | int]]:
        """Fetch and cache timestamped OKX funding settlements for a symbol."""
        key = f"{symbol}_funding_{start_ts}_{end_ts}"
        path = self._cache_dir / f"{key}.json"
        records: list[dict[str, float | int]] | None = None
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    records = [
                        {"ts": int(row["ts"]), "rate": float(row["rate"])}
                        for row in loaded if isinstance(row, dict)
                    ]
            except (OSError, ValueError, TypeError, KeyError):
                records = None
        source = "file_cache" if records is not None else "okx_public_api"
        if records is None:
            from app.services.okx_metrics import fetch_funding_history_records

            try:
                records = await fetch_funding_history_records(symbol, start_ts, end_ts)
            except Exception as exc:
                logger.warning("Historical funding unavailable for %s: %s", symbol, exc)
                records = []
                source = "unavailable"
            if source != "unavailable":
                try:
                    self._cache_dir.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(records), encoding="utf-8")
                except OSError as exc:
                    logger.warning("Failed to cache funding history %s: %s", key, exc)
        payload = json.dumps(records, separators=(",", ":"), sort_keys=True).encode("ascii")
        self._last_funding_provenance = {
            "symbol": symbol,
            "timeframe": "funding",
            "requested_start_ts": start_ts,
            "requested_end_ts": end_ts,
            "source": source,
            "cache_hit": source == "file_cache",
            "cache_key": key,
            "candle_count": len(records),
            "first_candle_ts": records[0]["ts"] if records else None,
            "last_candle_ts": records[-1]["ts"] if records else None,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
        }
        return records

    async def fetch_trade_tape(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> list[dict[str, float | int | str]]:
        """Fetch and cache the public trade tape for a symbol.

        Used to estimate the effective bid-ask spread (Roll estimator).  OKX
        only exposes ~3 months of tape, so the returned range may be shorter
        than requested; callers should treat a short/empty tape as "spread
        unavailable" and fall back to the base bps.
        """
        key = f"{symbol}_tape_{start_ts}_{end_ts}"
        path = self._cache_dir / f"{key}.json"
        records: list[dict[str, float | int | str]] | None = None
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    records = [
                        {
                            "ts": int(row["ts"]),
                            "px": float(row["px"]),
                            "sz": float(row.get("sz") or 0.0),
                            "side": str(row.get("side") or ""),
                        }
                        for row in loaded if isinstance(row, dict)
                    ]
            except (OSError, ValueError, TypeError, KeyError):
                records = None
        source = "file_cache" if records is not None else "okx_public_api"
        if records is None:
            from app.services.okx_metrics import fetch_trade_history_records

            try:
                records = await fetch_trade_history_records(symbol, start_ts, end_ts)
            except Exception as exc:
                logger.warning("Trade tape unavailable for %s: %s", symbol, exc)
                records = []
                source = "unavailable"
            if source != "unavailable":
                try:
                    self._cache_dir.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(records), encoding="utf-8")
                except OSError as exc:
                    logger.warning("Failed to cache trade tape %s: %s", key, exc)
        payload = json.dumps(records, separators=(",", ":"), sort_keys=True).encode("ascii")
        self._last_tape_provenance = {
            "symbol": symbol,
            "timeframe": "trade_tape",
            "requested_start_ts": start_ts,
            "requested_end_ts": end_ts,
            "source": source,
            "cache_hit": source == "file_cache",
            "cache_key": key,
            "trade_count": len(records),
            "first_trade_ts": records[0]["ts"] if records else None,
            "last_trade_ts": records[-1]["ts"] if records else None,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
        }
        return records

    async def fetch_swap_universe(self, *, force: bool = False) -> list[str]:
        """Fetch/cache the full list of live OKX SWAP instrument ids.

        Used by the screener-universe mode to reconstruct the same candidate
        pool live screens over.  Cached to a single file (the instrument list
        changes rarely) so repeated backtests don't re-hit the API.
        """
        path = self._cache_dir / "swap_universe.json"
        symbols: list[str] | None = None
        if path.exists() and not force:
            try:
                cached = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(cached, list):
                    symbols = [str(s).upper() for s in cached if s]
            except (OSError, ValueError):
                symbols = None
        source = "file_cache" if symbols is not None else "okx_public_api"
        if symbols is None:
            from app.services.okx_metrics import _get

            try:
                rows = await _get("/api/v5/public/instruments", {"instType": "SWAP"})
            except Exception as exc:
                logger.warning("Swap-universe fetch failed: %s", exc)
                rows = []
            symbols = sorted({
                str(row.get("instId") or "").upper()
                for row in rows
                if str(row.get("instId") or "").upper().endswith("-USDT-SWAP")
            })
            if symbols:
                try:
                    self._cache_dir.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(symbols), encoding="utf-8")
                except OSError as exc:
                    logger.warning("Failed to cache swap universe: %s", exc)
            else:
                source = "unavailable"
        payload = json.dumps(symbols, separators=(",", ":")).encode("ascii")
        self._last_universe_provenance = {
            "source": source,
            "cache_hit": source == "file_cache",
            "cache_key": "swap_universe",
            "symbol_count": len(symbols),
            "content_sha256": hashlib.sha256(payload).hexdigest(),
        }
        return symbols

    @property
    def last_universe_provenance(self) -> dict[str, Any]:
        return dict(self._last_universe_provenance)

    async def fetch_instrument_specs(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch/cache OKX contract value and size increments for requested swaps."""
        normalized_symbols = sorted(set(symbol.upper() for symbol in symbols))
        key = "swap_specs_" + "_".join(normalized_symbols)
        path = self._cache_dir / f"{key}.json"
        specs: dict[str, dict[str, Any]] | None = None
        if path.exists():
            try:
                cached = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(cached, dict):
                    specs = cached
            except (OSError, ValueError):
                specs = None
        source = "file_cache" if specs is not None else "okx_public_api"
        if specs is None:
            from app.services.okx_metrics import _get

            try:
                rows = await _get("/api/v5/public/instruments", {"instType": "SWAP"})
            except Exception as exc:
                logger.warning("Instrument-spec fetch failed: %s", exc)
                rows = []
            specs = {}
            requested = set(normalized_symbols)
            for row in rows:
                symbol = str(row.get("instId") or "").upper()
                if symbol not in requested:
                    continue
                try:
                    specs[symbol] = {
                        "ct_val": float(row.get("ctVal") or 1.0),
                        "lot_size": float(row.get("lotSz") or 0.0),
                        "min_size": float(row.get("minSz") or 0.0),
                        "tick_size": float(row.get("tickSz") or 0.0),
                        "max_market_size": float(row.get("maxMktSz") or 0.0),
                        "max_limit_size": float(row.get("maxLmtSz") or 0.0),
                        "contract_type": str(row.get("ctType") or "linear"),
                    }
                except (TypeError, ValueError):
                    continue
            for symbol in normalized_symbols:
                try:
                    parts = symbol.split("-")
                    params = {"instType": "SWAP", "tdMode": "isolated", "instId": symbol}
                    if len(parts) >= 2:
                        params["instFamily"] = "-".join(parts[:2])
                    tier_rows = await _get("/api/v5/public/position-tiers", params)
                    tiers = []
                    for tier in tier_rows:
                        try:
                            tiers.append({
                                "min_size": float(tier.get("minSz") or 0.0),
                                "max_size": float(tier.get("maxSz") or 0.0),
                                "initial_margin_ratio": float(tier.get("imr") or 0.0),
                                "maintenance_margin_ratio": float(tier.get("mmr") or 0.0),
                                "max_leverage": float(tier.get("maxLever") or 0.0),
                                "maintenance_deduction": float(tier.get("mmrDeduction") or 0.0),
                            })
                        except (TypeError, ValueError):
                            continue
                    if symbol in specs and tiers:
                        specs[symbol]["position_tiers"] = tiers
                except Exception as exc:
                    logger.warning("Position-tier fetch failed for %s: %s", symbol, exc)
            if specs:
                try:
                    self._cache_dir.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(specs, sort_keys=True), encoding="utf-8")
                except OSError as exc:
                    logger.warning("Failed to cache instrument specs %s: %s", key, exc)
            else:
                source = "unavailable"
        payload = json.dumps(specs, separators=(",", ":"), sort_keys=True).encode("ascii")
        self._last_instrument_provenance = {
            "source": source,
            "cache_hit": source == "file_cache",
            "cache_key": key,
            "symbols": normalized_symbols,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
        }
        return specs

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
        self._last_fetch_provenance = {}
        cached = self._load_cache(cache_key)
        if cached is not None:
            self._record_provenance(
                cache_key, symbol, timeframe, start_ts, end_ts, warmup_candles,
                cached, source="file_cache", cache_hit=True,
            )
            if progress_cb:
                progress_cb(len(cached), len(cached), "loaded from cache")
            return cached

        if self._api is None:
            logger.error("OKX MarketAPI unavailable — cannot fetch historical data")
            self._record_provenance(
                cache_key, symbol, timeframe, start_ts, end_ts, warmup_candles,
                [], source="unavailable", cache_hit=False,
            )
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
        self._record_provenance(
            cache_key, symbol, timeframe, start_ts, end_ts, warmup_candles,
            candles, source="okx_market_api", cache_hit=False,
        )
        return candles

    def _record_provenance(
        self,
        cache_key: str,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
        warmup_candles: int,
        candles: list[Candle],
        *,
        source: str,
        cache_hit: bool,
    ) -> None:
        rows = [
            [c.ts, c.open, c.high, c.low, c.close, c.volume, c.quote_volume]
            for c in candles
        ]
        payload = json.dumps(rows, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        self._last_fetch_provenance = {
            "symbol": symbol,
            "timeframe": timeframe,
            "requested_start_ts": start_ts,
            "requested_end_ts": end_ts,
            "warmup_candles": warmup_candles,
            "source": source,
            "cache_hit": cache_hit,
            "cache_key": cache_key,
            "candle_count": len(candles),
            "first_candle_ts": candles[0].ts if candles else None,
            "last_candle_ts": candles[-1].ts if candles else None,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
        }

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
                {"ts": c.ts, "open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume, "quote_volume": c.quote_volume}
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
