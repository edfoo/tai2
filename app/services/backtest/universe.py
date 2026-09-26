"""Historical screener-universe reconstruction for backtests.

Live trading does not trade a fixed symbol list — the dual-universe screener
(``MarketService.run_screener_if_due``) re-ranks the whole OKX SWAP universe
every ``interval_minutes`` and routes each strategy to its SC (trending) or MR
(chop) list.  A backtest that runs on a hand-picked symbol list therefore tests
a *different* universe than live would have traded.

This module reconstructs the screener's inputs from historical candles so the
backtest can select the same universe live would have, at each historical
interval:

  * ``last``        ← close of the most recent 1H candle at the interval
  * ``open24h``     ← close 24h earlier
  * ``high24h``     ← max high over the trailing 24h
  * ``low24h``      ← min low over the trailing 24h
  * ``volCcy24h``   ← sum of quote volume (``volCcyQuote``) over the trailing 24h
  * ``vol_spike_ratio`` ← rolling mean of the per-interval ``volCcy24h`` series
                      (mirrors live's ``_screener_vol_history`` deque)

Scoring itself is delegated to :func:`app.services.screener.score_universe` —
the exact function live uses — so the two cannot drift.

**Data limitation:** the live screener's ``max_spread_pct`` filter reads live
bid/ask, which OHLCV cannot reproduce.  When that filter is enabled the
backtest skips it and records the omission in the schedule provenance; the
reconstructed universe may therefore be slightly larger than live's.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from typing import Any

from app.services.backtest.data_fetcher import HistoricalDataFetcher
from app.services.backtest.models import Candle
from app.services.screener import score_universe, universe_for_strategy

logger = logging.getLogger(__name__)

# The screener's 24h lookback is expressed in 1H candles.
_LOOKBACK_MS = 24 * 60 * 60 * 1000
_UNIVERSE_TIMEFRAME = "1H"


@dataclass(slots=True)
class UniverseInterval:
    """The screener's selection at a single historical interval."""

    ts: int  # interval end (ms epoch)
    sc: list[str] = field(default_factory=list)
    mr: list[str] = field(default_factory=list)
    selected: list[str] = field(default_factory=list)


@dataclass(slots=True)
class UniverseSchedule:
    """Time-indexed screener universe for a backtest window.

    ``intervals`` is sorted ascending by ``ts``.  ``universe_at`` returns the
    most recent interval at or before a timestamp, so the engine can resolve
    the active universe at every backtest step.
    """

    intervals: list[UniverseInterval] = field(default_factory=list)
    dual_universe: bool = True
    interval_minutes: int = 60
    candidate_count: int = 0
    diagnostics: list[str] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def all_symbols(self) -> list[str]:
        """Union of every symbol selected in any interval (fetch list)."""
        seen: dict[str, None] = {}
        for interval in self.intervals:
            for sym in interval.selected:
                seen.setdefault(sym, None)
        return list(seen)

    def universe_at(self, ts: int, strategy_name: str = "") -> list[str] | None:
        """Return the strategy's universe at ``ts``, or None if before the first interval.

        None signals "no screener selection yet" — the caller should fall back
        to the configured symbol list rather than trade nothing.
        """
        if not self.intervals:
            return None
        idx = bisect.bisect_right([i.ts for i in self.intervals], ts) - 1
        if idx < 0:
            return None
        interval = self.intervals[idx]
        return universe_for_strategy(
            strategy_name,
            sc_symbols=interval.sc,
            mr_symbols=interval.mr,
            selected_symbols=interval.selected,
            fallback_symbols=[],
            dual_universe=self.dual_universe,
        )

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe summary (per-interval lists + provenance)."""
        return {
            "dual_universe": self.dual_universe,
            "interval_minutes": self.interval_minutes,
            "candidate_count": self.candidate_count,
            "interval_count": len(self.intervals),
            "all_symbols": self.all_symbols,
            "intervals": [
                {"ts": i.ts, "sc": i.sc, "mr": i.mr, "selected": i.selected}
                for i in self.intervals
            ],
            "diagnostics": self.diagnostics,
            "provenance": self.provenance,
        }


def _interval_boundaries(start_ts: int, end_ts: int, interval_ms: int) -> list[int]:
    """Return interval-end timestamps aligned to ``interval_ms`` within the window."""
    if interval_ms <= 0 or end_ts <= start_ts:
        return []
    first = (start_ts // interval_ms) * interval_ms
    if first < start_ts:
        first += interval_ms
    boundaries: list[int] = []
    ts = first
    while ts <= end_ts:
        boundaries.append(ts)
        ts += interval_ms
    return boundaries


def _ticker_at(
    candles: list[Candle],
    ts_list: list[int],
    ts: int,
) -> dict[str, Any] | None:
    """Reconstruct a live-style ticker dict for one symbol at interval ``ts``.

    Uses the most recent 1H candle at or before ``ts`` as ``last`` and the
    trailing 24h of candles for the 24h open/high/low/volume fields.  Returns
    None when there is no candle at or before ``ts``.
    """
    idx = bisect.bisect_right(ts_list, ts) - 1
    if idx < 0:
        return None
    last_candle = candles[idx]
    last = last_candle.close
    if last <= 0:
        return None

    # Trailing 24h window: candles with ts in (ts - 24h, ts].
    window_start = ts - _LOOKBACK_MS
    lo = bisect.bisect_right(ts_list, window_start)
    window = candles[lo:idx + 1]
    if not window:
        return None

    high24h = max(c.high for c in window)
    low24h = min(c.low for c in window)
    vol_ccy_24h = sum(c.effective_quote_volume for c in window)
    # open24h = close of the candle immediately before the window (the 24h-ago
    # reference).  Fall back to the window's first open when history is short.
    if lo - 1 >= 0:
        open24h = candles[lo - 1].close
    else:
        open24h = window[0].open
    if open24h <= 0:
        return None

    return {
        "last": last,
        "open24h": open24h,
        "high24h": high24h,
        "low24h": low24h,
        "volCcy24h": vol_ccy_24h,
    }


async def build_universe_schedule(
    *,
    fetcher: HistoricalDataFetcher,
    start_ts: int,
    end_ts: int,
    screener_config: dict[str, Any],
    candidate_symbols: list[str] | None = None,
    progress_cb: Any | None = None,
) -> UniverseSchedule:
    """Reconstruct the screener universe over ``[start_ts, end_ts]``.

    Parameters
    ----------
    fetcher:
        Historical candle fetcher (cached).
    start_ts, end_ts:
        Backtest window (ms epoch).
    screener_config:
        The live ``runtime_config["screener"]`` dict.
    candidate_symbols:
        Optional explicit candidate pool.  When omitted the full OKX SWAP
        universe is fetched (matching live).  Supplying a list trades fidelity
        for speed.
    progress_cb:
        Optional ``(done, total, message)`` callback for UI progress.
    """
    cfg = dict(screener_config or {})
    interval_minutes = max(1, int(cfg.get("interval_minutes") or 60))
    interval_ms = interval_minutes * 60_000
    dual_universe = bool(cfg.get("dual_universe", True))

    # ── Resolve the candidate pool ────────────────────────────────────
    if candidate_symbols:
        symbols = sorted({s.upper() for s in candidate_symbols if s})
        universe_source = "explicit_candidate_list"
        universe_provenance: dict[str, Any] = {"symbol_count": len(symbols)}
    else:
        symbols = await fetcher.fetch_swap_universe()
        universe_source = "okx_swap_universe"
        universe_provenance = fetcher.last_universe_provenance

    schedule = UniverseSchedule(
        dual_universe=dual_universe,
        interval_minutes=interval_minutes,
        provenance={
            "universe_source": universe_source,
            "universe": universe_provenance,
            "timeframe": _UNIVERSE_TIMEFRAME,
            "lookback_hours": 24,
            "spread_filter": (
                "skipped (live bid/ask unavailable historically)"
                if float(cfg.get("max_spread_pct") or 0.0) > 0
                else "disabled"
            ),
        },
    )
    if not symbols:
        schedule.diagnostics.append("Universe screener: no candidate symbols available")
        return schedule

    # ── Fetch 1H candles for the whole pool (with 24h warmup) ─────────
    # Warmup: one extra day so the first interval has a full 24h window.
    fetch_start = start_ts - _LOOKBACK_MS
    symbol_candles: dict[str, list[Candle]] = {}
    symbol_ts: dict[str, list[int]] = {}
    total = len(symbols)
    for i, symbol in enumerate(symbols):
        candles = await fetcher.fetch_candles(
            symbol=symbol,
            timeframe=_UNIVERSE_TIMEFRAME,
            start_ts=fetch_start,
            end_ts=end_ts,
            warmup_candles=0,
        )
        if candles:
            symbol_candles[symbol] = candles
            symbol_ts[symbol] = [c.ts for c in candles]
        if progress_cb:
            progress_cb(i + 1, total, f"universe data: {symbol}")

    boundaries = _interval_boundaries(start_ts, end_ts, interval_ms)
    if not boundaries:
        schedule.diagnostics.append("Universe screener: empty interval window")
        return schedule

    # ── Score each interval with a rolling volume history ─────────────
    vol_history: dict[str, Any] = {}
    for ts in boundaries:
        tickers: list[dict[str, Any]] = []
        for symbol, candles in symbol_candles.items():
            ticker = _ticker_at(candles, symbol_ts[symbol], ts)
            if ticker is not None:
                ticker["instId"] = symbol
                tickers.append(ticker)
        result = score_universe(
            tickers,
            cfg,
            vol_history=vol_history,
            update_vol_history=True,
        )
        schedule.intervals.append(
            UniverseInterval(
                ts=ts,
                sc=list(result["sc"]),
                mr=list(result["mr"]),
                selected=list(result["selected"]),
            )
        )
        schedule.candidate_count = max(schedule.candidate_count, result["base_candidate_count"])

    schedule.diagnostics.append(
        f"Universe screener: {len(schedule.intervals)} intervals over "
        f"{len(symbol_candles)}/{total} symbols with data "
        f"(interval={interval_minutes}min, dual={dual_universe})"
    )
    if not schedule.all_symbols:
        schedule.diagnostics.append(
            "Universe screener: no symbols selected in any interval — "
            "check volume/momentum filters against the historical window"
        )
    return schedule
