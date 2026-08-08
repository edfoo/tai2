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
