"""Asynchronous helpers for pulling OKX V5 *public* metrics that are required
by the liquidity-aware filters (funding-rate anomalies, open-interest delta,
and order-book imbalance).

The module is intentionally dependency-free beyond **aiohttp** and **pandas** –
both already present in the project.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Literal

import pandas as pd
from aiohttp import ClientSession, ClientTimeout

BASE = "https://www.okx.com"
TIMEOUT = ClientTimeout(total=10)

_log = logging.getLogger(__name__)


async def _get(path: str, params: dict[str, str] | None = None) -> list[dict]:
    """Fire a GET request and return the raw ``data`` list from OKX."""

    async with ClientSession(timeout=TIMEOUT) as session:  # new session per call keeps code simple
        url = f"{BASE}{path}"
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            payload = await resp.json()
            return payload.get("data", [])


# ---------------------------------------------------------------------------
# Funding Rate
# ---------------------------------------------------------------------------


async def fetch_funding(symbols: Iterable[str]) -> pd.DataFrame:
    """Return a tidy *long* DataFrame with funding-rate snapshots.

    The DataFrame index = (symbol, funding_time) and contains a single column
    ``funding_rate`` as *float64*.
    """

    tasks = [_get("/api/v5/public/funding-rate", {"instId": s}) for s in symbols]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    frames: list[pd.DataFrame] = []
    for s, raw in zip(symbols, data, strict=False):
        if isinstance(raw, Exception):
            _log.warning("Funding fetch failed for %s: %s", s, raw)
            continue
        df = pd.DataFrame(raw)
        if df.empty:
            continue
        df = df.assign(symbol=s)
        df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
        df = df.rename(columns={"fundingRate": "funding_rate"})[["symbol", "fundingTime", "funding_rate"]]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["funding_rate"], dtype=float).set_index(["symbol", "fundingTime"])

    out = pd.concat(frames, ignore_index=True)
    return out.set_index(["symbol", "fundingTime"]).sort_index()


# ---------------------------------------------------------------------------
# Open Interest
# ---------------------------------------------------------------------------


async def fetch_open_interest(symbols: Iterable[str]) -> pd.DataFrame:
    """Return a tidy DataFrame (symbol, ts) → open_interest as float."""

    tasks = [_get("/api/v5/public/open-interest", {"instId": s}) for s in symbols]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    frames: list[pd.DataFrame] = []
    for s, raw in zip(symbols, data, strict=False):
        if isinstance(raw, Exception):
            _log.warning("Open-interest fetch failed for %s: %s", s, raw)
            continue
        df = pd.DataFrame(raw)
        if df.empty:
            continue
        df = df.assign(symbol=s)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df = df.rename(columns={"oi": "open_interest"})[["symbol", "ts", "open_interest"]]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["open_interest"], dtype=float).set_index(["symbol", "ts"])

    out = pd.concat(frames, ignore_index=True)
    return out.set_index(["symbol", "ts"]).sort_index()


# ---------------------------------------------------------------------------
# Mark Prices (for implied funding calc / price series)
# ---------------------------------------------------------------------------


async def fetch_mark_prices(symbols: Iterable[str]) -> pd.DataFrame:
    """Return mark-price snapshots (symbol, ts) → price."""

    tasks = [_get("/api/v5/public/mark-price", {"instId": s}) for s in symbols]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    frames: list[pd.DataFrame] = []
    for s, raw in zip(symbols, data, strict=False):
        if isinstance(raw, Exception):
            _log.warning("Mark-price fetch failed for %s: %s", s, raw)
            continue
        df = pd.DataFrame(raw)
        if df.empty:
            continue
        df = df.assign(symbol=s)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df = df.rename(columns={"markPx": "mark_price"})[["symbol", "ts", "mark_price"]]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["mark_price"], dtype=float).set_index(["symbol", "ts"])

    out = pd.concat(frames, ignore_index=True)
    return out.set_index(["symbol", "ts"]).sort_index()


# ---------------------------------------------------------------------------
# Utility – Convenience Wrapper
# ---------------------------------------------------------------------------


async def fetch_all_metrics(symbols: Iterable[str]) -> dict[str, pd.DataFrame]:
    """Fire funding, OI, and mark-price requests concurrently."""

    funding, oi, mark = await asyncio.gather(
        fetch_funding(symbols), fetch_open_interest(symbols), fetch_mark_prices(symbols)
    )
    return {"funding": funding, "open_interest": oi, "mark_price": mark}


# ---------------------------------------------------------------------------
# Funding-Rate History  (for funding_z computation)
# ---------------------------------------------------------------------------


async def fetch_funding_history(
    symbol: str,
    *,
    limit: int = 100,
) -> list[float]:
    """Return up to *limit* historical funding rates for *symbol*, newest last.

    OKX ``/api/v5/public/funding-rate-history`` returns up to 100 records per
    call, newest first.  OKX perpetual contracts settle every 8 hours so 100
    records cover about 33 days — enough for a meaningful 30-day rolling
    z-score.  Rates are returned as plain floats (e.g. ``0.0001``).
    """
    try:
        raw = await _get(
            "/api/v5/public/funding-rate-history",
            {"instId": symbol, "limit": str(limit)},
        )
    except Exception as exc:
        _log.warning("Funding history fetch failed for %s: %s", symbol, exc)
        return []

    rates: list[float] = []
    for entry in reversed(raw):  # OKX returns newest first; reverse to oldest-first
        try:
            rate = float(entry.get("realizedRate") or entry.get("fundingRate") or 0.0)
            rates.append(rate)
        except (TypeError, ValueError):
            pass
    return rates


# ---------------------------------------------------------------------------
# Z-score helpers  (pure math – no I/O)
# ---------------------------------------------------------------------------


def zscore_latest(series: list[float]) -> float | None:
    """Return the z-score of the *last* element relative to the full series.

    Returns ``None`` when the series has fewer than 2 non-NaN elements or when
    the standard deviation is zero (all values identical).

    Uses **population** standard deviation (divides by N) to match the
    behaviour of ``scipy.stats.zscore`` and pandas ``pstd`` with ddof=0.
    This is the convention in the refactor guide's rolling-percentile spec.
    """
    if not series or len(series) < 2:
        return None

    clean = [x for x in series if x is not None and _isfinite(x)]
    if len(clean) < 2:
        return None

    mean = sum(clean) / len(clean)
    variance = sum((x - mean) ** 2 for x in clean) / len(clean)
    if variance == 0.0:
        return 0.0
    std = variance ** 0.5
    return (clean[-1] - mean) / std


def oi_delta_zscore(oi_series: list[float]) -> float | None:
    """Return the z-score of the *latest OI delta* (first-difference) vs history.

    ``oi_series`` should be a time-ordered list of open-interest readings
    (oldest first, newest last).  Returns ``None`` when fewer than 3 elements
    (need at least 2 deltas to compute a std-dev).
    """
    if not oi_series or len(oi_series) < 3:
        return None

    clean = [x for x in oi_series if x is not None and _isfinite(x)]
    if len(clean) < 3:
        return None

    deltas = [clean[i] - clean[i - 1] for i in range(1, len(clean))]
    return zscore_latest(deltas)


def _isfinite(value: float) -> bool:
    import math

    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False
