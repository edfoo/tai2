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

**Candle visibility:** at an interval boundary ``ts`` only 1H candles that
have *closed* by ``ts`` are used (open time ``<= ts − 1h``).  OKX ``ts`` is a
bar's open time, so the candle opening at the boundary would carry the coming
hour's high/low/close (look-ahead).  Price samples are stamped with the
candle's close time.

**Vol universe** (``vol_enabled``, wick_rejection plan §3.5): ranked on speed
from the same 1H candles.  The fast-speed term is a close-to-close 1H move, so
a coin spiking during hour H enters the backtest's vol list at H+1, while live
(screener every tick, poll-cadence samples) can list it before H:59.  The
backtest undercounts fresh-spike entries rather than inventing them; recorded
in the provenance.

**Data limitation:** the live screener's ``max_spread_pct`` filter reads live
bid/ask, which OHLCV cannot reproduce.  When that filter is enabled the
backtest skips it and records the omission in the schedule provenance; the
reconstructed universe may therefore be slightly larger than live's.
"""

from __future__ import annotations

import bisect
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from app.services.backtest.data_fetcher import HistoricalDataFetcher
from app.services.backtest.models import Candle
from app.services.screener import (
    compute_recent_activity,
    fast_lane_candidates,
    merge_candidates,
    score_universe,
    timeframe_to_ms,
    universe_for_strategy,
)

logger = logging.getLogger(__name__)

# The screener's 24h lookback is expressed in 1H candles.
_LOOKBACK_MS = 24 * 60 * 60 * 1000
_UNIVERSE_TIMEFRAME = "1H"
_UNIVERSE_BAR_MS = 60 * 60 * 1000


@dataclass(slots=True)
class UniverseInterval:
    """The screener's selection at a single historical interval."""

    ts: int  # interval end (ms epoch)
    sc: list[str] = field(default_factory=list)
    mr: list[str] = field(default_factory=list)
    selected: list[str] = field(default_factory=list)
    vol: list[str] = field(default_factory=list)


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
            vol_symbols=interval.vol,
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
                {"ts": i.ts, "sc": i.sc, "mr": i.mr, "vol": i.vol, "selected": i.selected}
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


def _activity_at(
    candles: list[Candle],
    ts_list: list[int],
    ts: int,
    *,
    lookback_hours: float,
    timeframe_ms: int,
    vol_ccy_24h: float | None,
    hl_range_pct: float | None,
) -> dict[str, Any] | None:
    """Reconstruct the live recent-activity features at interval ``ts``.

    Converts the already-fetched 1H candles into OKX-style rows and delegates to
    :func:`app.services.screener.compute_recent_activity` — the exact function
    live uses — so the backtest ranks on the same features with no extra fetch.
    """
    idx = bisect.bisect_right(ts_list, ts) - 1
    if idx < 0:
        return None
    # Only the bars the window can possibly need (plus one reference bar).
    bars_needed = int(round(lookback_hours * 3_600_000 / max(1, timeframe_ms))) + 2
    window = candles[max(0, idx - bars_needed + 1): idx + 1]
    rows = [
        [
            c.ts,
            c.open,
            c.high,
            c.low,
            c.close,
            c.volume,
            0.0,
            c.effective_quote_volume,
            "1",
        ]
        for c in window
    ]
    return compute_recent_activity(
        rows,
        lookback_hours=lookback_hours,
        timeframe_ms=timeframe_ms,
        vol_ccy_24h=vol_ccy_24h,
        hl_range_pct=hl_range_pct,
        as_of_ts=ts,
    )


def _price_history_at(
    candles: list[Candle],
    ts_list: list[int],
    ts: int,
    *,
    window_minutes: float,
) -> deque:
    """Reconstruct the fast lane's rolling price deque at interval ``ts``.

    Live samples ``last`` at every ticker poll (``poll_interval`` seconds); the
    backtest only has 1H candles, so samples are candle closes.  Both are fed to
    :func:`app.services.screener.fast_lane_candidates`, which measures the move
    against the newest sample at or before ``ts - window`` — so with a 60-minute
    window and 1H candles the reconstructed measurement is the same
    close-to-close 1h move live would have computed, just sampled at bar
    granularity instead of poll granularity.

    Enough history is returned to satisfy the lane's minimum-sample guard even
    for sub-hour windows.
    """
    idx = bisect.bisect_right(ts_list, ts) - 1
    if idx < 0:
        return deque()
    bars_needed = int(round(window_minutes * 60_000 / (60 * 60 * 1000))) + 1
    bars_needed = max(4, bars_needed)
    window = candles[max(0, idx - bars_needed + 1): idx + 1]
    # Stamp each sample with the candle's close time (OKX ts is the open).
    return deque(((c.ts + _UNIVERSE_BAR_MS) / 1000.0, c.close) for c in window)


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
            "activity_lookback_hours": float(cfg.get("activity_lookback_hours") or 0.0),
            "activity_timeframe": str(cfg.get("activity_timeframe") or "1H"),
            "fast_lane": (
                {
                    "threshold_pct": float(cfg.get("fast_move_threshold_pct") or 0.0),
                    "window_minutes": float(cfg.get("fast_move_window_minutes") or 60.0),
                    "min_volume_usd": float(cfg.get("fast_lane_min_volume_usd") or 0.0),
                    # The cold-start sodUtc0 fallback cannot be reproduced from
                    # 1H candles, so the backtest relies on the reconstructed
                    # price deque only.  Recorded so a live/backtest delta in
                    # the first interval of a session is explainable.
                    "sod_fallback": "disabled (reconstructed from candles)",
                }
                if bool(cfg.get("fast_lane_enabled", False))
                else "disabled"
            ),
            "candle_visibility": "closed 1H candles only (open ts <= interval ts - 1h)",
            "vol_universe": (
                {
                    "max_symbols": int(cfg.get("vol_max_symbols") or 8),
                    "min_speed_pct_per_hour": float(cfg.get("vol_min_speed_pct_per_hour") or 0.0),
                    "min_vol_ratio": float(cfg.get("vol_min_vol_ratio") or 0.0),
                    # §3.5 parity limitation.
                    "fast_speed": (
                        "1H close-to-close from candles: a coin spiking during "
                        "hour H enters the vol list at H+1 (live can list it "
                        "before H:59)"
                    ),
                }
                if bool(cfg.get("vol_enabled", False))
                else "disabled"
            ),
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
    # Recent-activity features are derived from the same 1H candles already
    # fetched above, so enabling the activity window costs the backtest nothing
    # and keeps it in lockstep with live.
    activity_lookback_hours = float(cfg.get("activity_lookback_hours") or 0.0)
    activity_tf_ms = timeframe_to_ms(str(cfg.get("activity_timeframe") or "1H"))
    fast_lane_enabled = bool(cfg.get("fast_lane_enabled", False))
    vol_enabled = bool(cfg.get("vol_enabled", False))
    fast_window_minutes = max(1.0, float(cfg.get("fast_move_window_minutes") or 60.0))
    vol_history: dict[str, Any] = {}
    for ts in boundaries:
        # Only candles closed by the boundary: the newest usable bar opened
        # one bar earlier (OKX ts = open time).
        closed_ts = ts - _UNIVERSE_BAR_MS
        tickers: list[dict[str, Any]] = []
        activity: dict[str, dict[str, Any]] = {}
        price_history: dict[str, deque] = {}
        for symbol, candles in symbol_candles.items():
            ticker = _ticker_at(candles, symbol_ts[symbol], closed_ts)
            if ticker is None:
                continue
            ticker["instId"] = symbol
            tickers.append(ticker)
            if fast_lane_enabled or vol_enabled:
                # Reconstruct the lane's price deque from the same 1H candles —
                # no extra fetch, and identical code path to live.
                price_history[symbol] = _price_history_at(
                    candles,
                    symbol_ts[symbol],
                    closed_ts,
                    window_minutes=fast_window_minutes,
                )
            if activity_lookback_hours > 0:
                act = _activity_at(
                    candles,
                    symbol_ts[symbol],
                    closed_ts,
                    lookback_hours=activity_lookback_hours,
                    timeframe_ms=activity_tf_ms,
                    vol_ccy_24h=ticker.get("volCcy24h"),
                    hl_range_pct=(
                        (ticker["high24h"] - ticker["low24h"]) / ticker["open24h"] * 100.0
                        if ticker.get("open24h")
                        else None
                    ),
                )
                if act is not None:
                    activity[symbol] = act
        fast_lane: list[dict[str, Any]] = []
        if fast_lane_enabled:
            fast_lane = fast_lane_candidates(
                tickers,
                price_history,
                cfg,
                # Interval boundaries are bar-close timestamps in ms; the lane
                # expects epoch seconds.
                now_ts=ts / 1000.0,
                vol_ccy_is_quote=True,
            )
        result = score_universe(
            tickers,
            cfg,
            vol_history=vol_history,
            update_vol_history=True,
            activity=activity or None,
            # _ticker_at() sums Candle.effective_quote_volume (OKX volCcyQuote),
            # so volCcy24h here is already quote-currency — do not re-convert.
            vol_ccy_is_quote=True,
            fast_lane=fast_lane,
            price_history=price_history if vol_enabled else None,
            now_ts=ts / 1000.0,
        )
        schedule.intervals.append(
            UniverseInterval(
                ts=ts,
                sc=list(result["sc"]),
                mr=list(result["mr"]),
                selected=list(result["selected"]),
                vol=list(result.get("vol") or []),
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
