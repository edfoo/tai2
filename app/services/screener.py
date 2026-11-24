"""Shared symbol-screener scoring core (live + backtest).

The live :class:`~app.services.market_service.MarketService` fetches OKX tickers
and calls :func:`score_universe`; the backtest reconstructs historical tickers
from candles and calls the **same** function.  Keeping the scoring in one place
means the backtest selects the same universe live would have, from the same
inputs, instead of a hand-mirrored duplicate that can silently drift.

The function is pure: it reads a ticker snapshot plus a config dict and returns
the ranked SC / MR / union lists.  The only mutable state is the optional
``vol_history`` mapping (a per-symbol rolling deque of 24h quote volume) which
the caller owns — live keeps it on the service, the backtest keeps it on the
universe schedule builder.
"""

from __future__ import annotations

import fnmatch
import math
from collections import deque
from typing import Any


def _extract_float(value: Any) -> float | None:
    """Safely coerce arbitrary types to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Recent-activity features ────────────────────────────────────────────────
#
# The 24h ticker fields (``open24h``/``high24h``/``low24h``/``volCcy24h``) are
# *aggregates*: a coin that pumped 20 hours ago and has been flat since still
# shows a large ``momentum_pct`` and a wide ``hl_range_pct`` for the rest of the
# day.  Ranking on them therefore selects "was hot, now dead" names.
#
# ``compute_recent_activity`` derives the same three dimensions over a short
# trailing window (default 3h) so the screener can rank on what is moving *now*.
# It is pure and shared by live (``MarketService``) and the backtest
# (``app/services/backtest/universe.py``) so the two cannot drift.

_HOUR_MS = 3_600_000

# OKX bar string → duration in ms.  Mirrors
# ``app.services.backtest.data_fetcher._timeframe_to_ms`` but lives here so the
# live service can size its activity window without importing the backtest.
_TIMEFRAME_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1H": 3_600_000,
    "2H": 7_200_000,
    "4H": 14_400_000,
    "6H": 21_600_000,
    "12H": 43_200_000,
    "1D": 86_400_000,
}


def timeframe_to_ms(bar: str | None) -> int:
    """Return the duration of an OKX bar string in milliseconds.

    Unknown bars fall back to 1H, matching the screener's default activity
    timeframe.
    """
    key = str(bar or "").strip()
    if key in _TIMEFRAME_MS:
        return _TIMEFRAME_MS[key]
    # Case-insensitive retry (OKX accepts "1h" and "1H").
    for candidate, ms in _TIMEFRAME_MS.items():
        if candidate.lower() == key.lower():
            return ms
    return _HOUR_MS


def _row_quote_volume(row: Any) -> float:
    """Return a candle row's quote-currency volume (OKX ``volCcyQuote``).

    OKX rows are ``[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]``.
    Legacy/short rows fall back to ``vol × close`` (an approximation).
    """
    if len(row) >= 8:
        quote = _extract_float(row[7])
        if quote is not None and quote > 0:
            return quote
    vol = _extract_float(row[5]) or 0.0
    close = _extract_float(row[4]) or 0.0
    return max(vol, 0.0) * max(close, 0.0)


def _row_is_confirmed(row: Any) -> bool:
    """Return False for the still-forming candle (OKX ``confirm == "0"``)."""
    if len(row) < 9:
        return True
    flag = row[8]
    if isinstance(flag, str):
        return flag.strip().lower() not in {"0", "false"}
    return bool(flag)


def compute_recent_activity(
    candles: list[Any],
    *,
    lookback_hours: float = 4.0,
    timeframe_ms: int = _HOUR_MS,
    vol_ccy_24h: float | None = None,
    hl_range_pct: float | None = None,
    as_of_ts: int | None = None,
    include_forming: bool = False,
) -> dict[str, Any] | None:
    """Derive short-window activity features from raw OKX candle rows.

    Parameters
    ----------
    candles:
        Raw OKX candlestick rows (any order).  Unconfirmed (forming) candles are
        dropped so live and backtest see the same closed-candle window.
    lookback_hours:
        Trailing window length in hours (default 4).
    timeframe_ms:
        Candle duration in ms — used to size the window and to express the
        expected 24h-proportional baseline.
    vol_ccy_24h:
        The ticker's 24h quote volume, used to normalise recent volume against
        the symbol's *own* baseline.  ``None`` disables ``recent_vol_ratio``.
    hl_range_pct:
        The ticker's 24h high-low range %, used to compute ``decay_ratio``.
    as_of_ts:
        Optional ms-epoch cutoff; candles after it are ignored (backtest use).
    include_forming:
        Keep the unconfirmed (forming) candle as the newest bar.  The
        wick_rejection vol refresh (shortly before the hourly close) uses it so
        the list ranks on the hour in progress; the backtest equivalent is a
        partial candle cut at the same moment (``universe.py``).

    Returns
    -------
    dict with ``recent_vol``, ``recent_vol_ratio``, ``recent_range_pct``,
    ``recent_mom_pct`` (signed), ``decay_ratio``, ``hours_covered``,
    ``candle_count`` — or ``None`` when there is no usable candle.
    """
    rows: list[tuple[int, float, float, float, float, float]] = []
    for row in candles or []:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        if not include_forming and not _row_is_confirmed(row):
            continue
        ts = _extract_float(row[0])
        o = _extract_float(row[1])
        h = _extract_float(row[2])
        lo = _extract_float(row[3])
        c = _extract_float(row[4])
        if ts is None or o is None or h is None or lo is None or c is None:
            continue
        if as_of_ts is not None and ts > as_of_ts:
            continue
        rows.append((int(ts), o, h, lo, c, _row_quote_volume(row)))
    if not rows:
        return None
    rows.sort(key=lambda r: r[0])

    tf_ms = max(1, int(timeframe_ms))
    window_bars = max(1, int(round(float(lookback_hours) * _HOUR_MS / tf_ms)))
    window = rows[-window_bars:]
    if not window:
        return None

    last = window[-1][4]
    if last <= 0:
        return None
    base_open = window[0][1]
    high = max(r[2] for r in window)
    low = min(r[3] for r in window)
    recent_vol = sum(r[5] for r in window)

    recent_range_pct = (high - low) / last * 100.0
    recent_mom_pct = (
        (last - base_open) / base_open * 100.0 if base_open > 0 else 0.0
    )

    hours_covered = len(window) * tf_ms / _HOUR_MS
    share = hours_covered / 24.0

    # Volume accumulates linearly with time, so the 24h baseline scales by
    # ``share`` directly.
    recent_vol_ratio: float | None = None
    if vol_ccy_24h and vol_ccy_24h > 0 and share > 0:
        expected_vol = vol_ccy_24h * share
        if expected_vol > 0:
            recent_vol_ratio = recent_vol / expected_vol

    # Range does NOT scale linearly: the 24h high-low is the extreme of the
    # whole path, and a sub-window captures a disproportionate slice of it.
    # For a random walk the range over a fraction ``f`` of the time is ~√f of
    # the full range, so the baseline uses ``sqrt(share)``.  Using ``share``
    # here inflates every value ~2.8x at a 3h window and makes the diagnostic
    # useless (nothing ever reads as faded).
    decay_ratio: float | None = None
    if hl_range_pct and hl_range_pct > 0 and share > 0:
        expected_range = hl_range_pct * math.sqrt(share)
        if expected_range > 0:
            decay_ratio = recent_range_pct / expected_range

    return {
        "recent_vol": recent_vol,
        "recent_vol_ratio": recent_vol_ratio,
        "recent_range_pct": recent_range_pct,
        "recent_mom_pct": recent_mom_pct,
        "decay_ratio": decay_ratio,
        "hours_covered": hours_covered,
        "candle_count": len(window),
    }


def build_candidates(
    tickers: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    vol_history: dict[str, deque] | None = None,
    update_vol_history: bool = True,
    vol_ccy_is_quote: bool = False,
) -> list[dict[str, Any]]:
    """Filter a ticker snapshot into enriched screener candidates.

    This is the shared prefilter: it applies the pattern / price / volume /
    spread gates and attaches the 24h-derived features.  Live uses it to pick
    which symbols are worth fetching recent candles for; ``score_universe``
    uses it as its first stage.

    ``vol_ccy_is_quote``
        OKX's raw ticker ``volCcy24h`` is volume in **base currency** (BTC,
        ETH, tokens), not quote currency.  Comparing it directly against
        ``min_volume_usd`` silently rejects high-priced symbols (BTC's
        ``volCcy24h`` is ~55k, which reads as "55k USDT" against a 10M floor)
        while letting low-priced tokens through on raw token counts.  Live
        therefore passes ``False`` so this function converts to quote volume
        via ``volCcy24h × last``.  The backtest already reconstructs
        ``volCcy24h`` from summed ``volCcyQuote`` (i.e. it is already quote
        volume) and passes ``True`` to skip the conversion.
    """
    cfg = cfg or {}
    universe_pattern = str(cfg.get("universe_filter") or "*-USDT-SWAP").strip().upper()
    min_volume_usd = float(cfg.get("min_volume_usd") or 0.0)
    # Vol list (wick_rejection) may admit thinner coins than SC/MR: candidates
    # between ``vol_min_volume_usd`` and ``min_volume_usd`` are kept but marked
    # ``vol_only`` — score_universe ranks them for the vol list only.
    floor = vol_volume_floor(cfg)
    max_spread_pct = float(cfg.get("max_spread_pct") or 0.0)
    vol_history_window = max(2, int(cfg.get("vol_history_window") or 8))

    candidates: list[dict[str, Any]] = []
    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue
        inst_id = str(ticker.get("instId") or "").upper()
        if not inst_id:
            continue
        if not fnmatch.fnmatch(inst_id, universe_pattern):
            continue
        last = _extract_float(ticker.get("last"))
        open24h = _extract_float(ticker.get("open24h"))
        high24h = _extract_float(ticker.get("high24h"))
        low24h = _extract_float(ticker.get("low24h"))
        vol_ccy_24h = _extract_float(ticker.get("volCcy24h"))
        if not last or last <= 0:
            continue
        # OKX's volCcy24h is base-currency volume; convert to quote (USDT) so it
        # is comparable to min_volume_usd and to candle volCcyQuote sums.
        if vol_ccy_24h is not None and not vol_ccy_is_quote:
            vol_ccy_24h *= last
        if vol_ccy_24h is None or vol_ccy_24h < floor:
            continue
        # Liquidity guard: reject wide-spread (thin book) symbols.
        # OKX tickers expose bidPx/askPx; spread = (ask-bid)/mid × 100.
        # A wide spread means market-order closes (SL triggers) will slip.
        spread_pct: float | None = None
        if max_spread_pct > 0:
            bid_px = _extract_float(ticker.get("bidPx"))
            ask_px = _extract_float(ticker.get("askPx"))
            if bid_px is not None and ask_px is not None and bid_px > 0 and ask_px > 0:
                mid = (bid_px + ask_px) / 2.0
                if mid > 0:
                    spread_pct = (ask_px - bid_px) / mid * 100.0
                    if spread_pct > max_spread_pct:
                        continue
            # If bid/ask missing, do not reject — let volume filter decide.
        momentum_pct = (
            abs((last - open24h) / open24h * 100)
            if open24h and open24h > 0
            else 0.0
        )
        hl_range_pct = (
            (high24h - low24h) / open24h * 100
            if open24h and open24h > 0 and high24h is not None and low24h is not None
            else 0.0
        )
        # Update rolling volume history, resizing deque if window changed.
        vol_spike_ratio: float | None = None
        if vol_history is not None:
            hist = vol_history.get(inst_id)
            if hist is None or hist.maxlen != vol_history_window:
                hist = deque(hist or [], maxlen=vol_history_window)
                vol_history[inst_id] = hist
            if update_vol_history:
                hist.append(vol_ccy_24h)
            # vol_spike_ratio: current vol / rolling average (needs ≥2 samples).
            if len(hist) >= 2:
                avg_vol = sum(hist) / len(hist)
                vol_spike_ratio = vol_ccy_24h / avg_vol if avg_vol > 0 else 1.0
            else:
                vol_spike_ratio = None  # fall back to raw vol until history builds
        candidates.append(
            {
                "symbol": inst_id,
                "vol_ccy_24h": vol_ccy_24h,
                "vol_spike_ratio": vol_spike_ratio,
                "hl_range_pct": hl_range_pct,
                "momentum_pct": momentum_pct,
                "spread_pct": spread_pct,
                "vol_only": vol_ccy_24h < min_volume_usd,
            }
        )
    return candidates


def vol_volume_floor(cfg: dict[str, Any] | None) -> float:
    """24h quote-volume floor of the candidate prefilter.

    ``min_volume_usd``, lowered to ``vol_min_volume_usd`` when the vol list is
    enabled and that is set (None = same as ``min_volume_usd``).
    """
    cfg = cfg or {}
    main = float(cfg.get("min_volume_usd") or 0.0)
    raw = cfg.get("vol_min_volume_usd")
    if not bool(cfg.get("vol_enabled", False)) or raw is None or raw == "":
        return main
    try:
        return min(main, max(0.0, float(raw)))
    except (TypeError, ValueError):
        return main


def activity_fetch_symbols(
    candidates: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    price_history: dict[str, deque] | None = None,
    now_ts: float | None = None,
) -> list[str]:
    """Symbols that get recent-activity features (candles) this run.

    Live fetches candles only for these; the backtest computes activity for
    every candidate and keeps only these, so both rank on the same inputs.

    * the ``activity_prefilter_top_n`` highest-24h-volume candidates that are
      not ``vol_only``, plus every fast-lane admission (``_fast_lane``);
    * with the vol list on, the ``vol_activity_top_n`` fastest ``vol_only``
      candidates by price-sample speed (``compute_speed`` without activity),
      so thin coins moving now get activity without fetching all of them.
    """
    cfg = cfg or {}
    top_n = max(1, int(cfg.get("activity_prefilter_top_n") or 120))
    main = [c for c in candidates if not c.get("vol_only")]
    ranked = sorted(main, key=lambda c: c.get("vol_ccy_24h") or 0.0, reverse=True)[:top_n]
    out = [c["symbol"] for c in ranked]
    seen = set(out)
    for c in candidates:
        if c.get("_fast_lane") and c["symbol"] not in seen:
            out.append(c["symbol"])
            seen.add(c["symbol"])
    vol_n = int(cfg.get("vol_activity_top_n") or 0)
    if bool(cfg.get("vol_enabled", False)) and vol_n > 0:
        window = float(cfg.get("fast_move_window_minutes") or 60.0)
        now = float(now_ts if now_ts is not None else _now_seconds())
        fast: list[tuple[float, str]] = []
        for c in candidates:
            if not c.get("vol_only") or c["symbol"] in seen:
                continue
            sp = compute_speed({}, (price_history or {}).get(c["symbol"]),
                               now_ts=now, window_minutes=window)["fast_speed"]
            if sp is not None:
                fast.append((sp, c["symbol"]))
        fast.sort(key=lambda t: (-t[0], t[1]))
        for _sp, sym in fast[:vol_n]:
            out.append(sym)
            seen.add(sym)
    return out


# ── Fast lane: short-window move discovery ──────────────────────────────────
#
# ``build_candidates`` gates on trailing-24h quote volume.  A 24h *sum* is a
# lagging indicator by construction: a coin that starts from a quiet baseline
# (SHELL: ~100k/day) cannot reach the ``min_volume_usd`` floor until the pump's
# own candles have printed.  SHELL went +9% at 15:00, +26% at 17:00, and only
# crossed the 10M floor at 18:00 — by which point the move was over.
#
# The fast lane therefore admits symbols on a *short-window price move* instead,
# with a much lower (tradability-only) volume floor.  It is a discovery
# widening, not a selection change: admitted symbols still go through
# ``compute_recent_activity`` and the normal SC/MR scoring, so they must win on
# activity to be traded.  It costs **no extra fetches** when the caller already
# polls all tickers — the move is computed from a rolling ``(ts, last)`` deque.

# Minimum samples before the deque-based move is trusted (guards a single
# thin print from being read as a pump).
_FAST_LANE_MIN_SAMPLES = 3


def _fast_move_from_history(
    history: Any,
    *,
    now_ts: float,
    window_seconds: float,
) -> tuple[float, float, str] | None:
    """Return ``(move_pct, span_seconds, source)`` from a rolling price deque.

    ``history`` is any iterable of ``(ts_epoch_seconds, last_price)`` samples,
    oldest-first.  The move is measured against the newest sample that is at
    least ``window_seconds`` old — or the oldest sample available when the
    history is shorter than the window.

    Returns None when there are too few samples or the span is too short to
    be meaningful.
    """
    samples = [(float(ts), float(px)) for ts, px in history if px]
    if len(samples) < _FAST_LANE_MIN_SAMPLES:
        return None
    samples.sort(key=lambda s: s[0])
    last_ts, last_px = samples[-1]
    if last_px <= 0 or last_ts <= 0:
        return None

    window_start = now_ts - window_seconds
    # Newest sample at or before the window start; fall back to the oldest.
    base = samples[0]
    for sample in samples:
        if sample[0] <= window_start:
            base = sample
        else:
            break
    span = last_ts - base[0]
    # Require at least half the window to actually be covered, otherwise the
    # "move" is just noise between adjacent polls.
    if span < window_seconds * 0.5:
        return None
    if base[1] <= 0:
        return None
    return ((last_px - base[1]) / base[1] * 100.0, span, "history")


def fast_lane_candidates(
    tickers: list[dict[str, Any]],
    price_history: dict[str, deque] | None,
    cfg: dict[str, Any],
    *,
    now_ts: float | None = None,
    vol_ccy_is_quote: bool = False,
) -> list[dict[str, Any]]:
    """Admit symbols moving sharply in the recent window, ignoring the 24h gate.

    This is the discovery complement to :func:`build_candidates`.  A symbol
    qualifies when **all** of the following hold:

      * ``|move_pct|`` over the last ``fast_move_window_minutes`` exceeds
        ``fast_move_threshold_pct`` — the discovery signal.
      * trailing-24h quote volume is at least ``fast_lane_min_volume_usd``
        (default 250k) — a *tradability* floor, far below ``min_volume_usd``.
      * the instrument matches ``universe_filter``.
      * bid/ask spread is within ``max_spread_pct`` (when bid/ask is present).

    Cold start: when the price history is too short, ``sodUtc0`` (the OKX
    session-open price, already in every ticker) is used as the reference — but
    only while the session is young enough that the move is still recent
    (``fast_lane_sod_max_age_mult`` × the window), so a pump from hours ago is
    not mistaken for a fresh one.

    Returns candidate dicts shaped like :func:`build_candidates` output, plus
    ``fast_move_pct``, ``fast_span_seconds`` and ``fast_source``.  Each carries
    ``_fast_lane = True`` so callers can exempt them from volume-based fetch
    caps and surface them in diagnostics.
    """
    cfg = cfg or {}
    if not bool(cfg.get("fast_lane_enabled", False)):
        return []

    threshold = abs(float(cfg.get("fast_move_threshold_pct") or 4.0))
    window_minutes = max(1.0, float(cfg.get("fast_move_window_minutes") or 60.0))
    window_seconds = window_minutes * 60.0
    min_volume_usd = max(0.0, float(cfg.get("fast_lane_min_volume_usd") or 250_000.0))
    max_symbols = max(0, int(cfg.get("fast_lane_max_symbols") or 40))
    sod_max_age = max(1.0, float(cfg.get("fast_lane_sod_max_age_mult") or 3.0))
    universe_pattern = str(cfg.get("universe_filter") or "*-USDT-SWAP").strip().upper()
    max_spread_pct = float(cfg.get("max_spread_pct") or 0.0)
    history = price_history or {}
    now = float(now_ts if now_ts is not None else _now_seconds())

    admitted: list[dict[str, Any]] = []
    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue
        inst_id = str(ticker.get("instId") or "").upper()
        if not inst_id or not fnmatch.fnmatch(inst_id, universe_pattern):
            continue
        last = _extract_float(ticker.get("last"))
        if not last or last <= 0:
            continue
        vol_ccy_24h = _extract_float(ticker.get("volCcy24h"))
        if vol_ccy_24h is not None and not vol_ccy_is_quote:
            vol_ccy_24h *= last
        if vol_ccy_24h is None or vol_ccy_24h < min_volume_usd:
            continue

        spread_pct: float | None = None
        if max_spread_pct > 0:
            bid_px = _extract_float(ticker.get("bidPx"))
            ask_px = _extract_float(ticker.get("askPx"))
            if bid_px and ask_px and bid_px > 0 and ask_px > 0:
                mid = (bid_px + ask_px) / 2.0
                if mid > 0:
                    spread_pct = (ask_px - bid_px) / mid * 100.0
                    if spread_pct > max_spread_pct:
                        continue

        measured = _fast_move_from_history(
            history.get(inst_id) or (),
            now_ts=now,
            window_seconds=window_seconds,
        )
        if measured is not None:
            move_pct, span, source = measured
        else:
            # Cold start: fall back to the session-open price, but only while
            # the session is young enough for "since open" to mean "recently".
            sod = _extract_float(ticker.get("sodUtc0"))
            if not sod or sod <= 0:
                continue
            session_age = _seconds_since_utc_midnight(now)
            if session_age <= 0 or session_age > window_seconds * sod_max_age:
                continue
            move_pct = (last - sod) / sod * 100.0
            span = session_age
            source = "sodUtc0"

        if abs(move_pct) < threshold:
            continue

        open24h = _extract_float(ticker.get("open24h"))
        high24h = _extract_float(ticker.get("high24h"))
        low24h = _extract_float(ticker.get("low24h"))
        hl_range_pct = (
            (high24h - low24h) / open24h * 100
            if open24h and open24h > 0 and high24h is not None and low24h is not None
            else 0.0
        )
        momentum_pct = (
            abs((last - open24h) / open24h * 100)
            if open24h and open24h > 0
            else 0.0
        )
        admitted.append(
            {
                "symbol": inst_id,
                "vol_ccy_24h": vol_ccy_24h,
                # No rolling history for lane-added symbols — score_universe
                # falls back to raw volume for normalisation.
                "vol_spike_ratio": None,
                "hl_range_pct": hl_range_pct,
                "momentum_pct": momentum_pct,
                "spread_pct": spread_pct,
                "fast_move_pct": move_pct,
                "fast_span_seconds": span,
                "fast_source": source,
                "_fast_lane": True,
            }
        )

    # Biggest absolute move first, then cap.
    admitted.sort(key=lambda c: abs(c["fast_move_pct"]), reverse=True)
    return admitted[:max_symbols] if max_symbols else admitted


def merge_candidates(
    base: list[dict[str, Any]],
    extra: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Union two candidate lists by symbol, preferring ``base``'s feature dict.

    Symbols present in both keep their ``build_candidates`` entry (so the
    rolling ``vol_spike_ratio`` is preserved) but are annotated with the extra
    list's fast-lane fields, letting diagnostics report *why* they were found
    twice without double-counting them in the pool.
    """
    by_symbol = {c["symbol"]: c for c in base}
    for candidate in extra:
        symbol = candidate["symbol"]
        existing = by_symbol.get(symbol)
        if existing is None:
            by_symbol[symbol] = candidate
            continue
        existing["fast_move_pct"] = candidate.get("fast_move_pct")
        existing["fast_span_seconds"] = candidate.get("fast_span_seconds")
        existing["fast_source"] = candidate.get("fast_source")
        if existing.get("vol_only"):
            # Admitted by the fast lane: eligible for SC/MR like any lane symbol.
            existing["vol_only"] = False
            existing["_fast_lane"] = True
    return list(by_symbol.values())


def _now_seconds() -> float:
    """Current epoch seconds (patchable in tests)."""
    import time

    return time.time()


def _seconds_since_utc_midnight(now_ts: float) -> float:
    """Seconds elapsed since 00:00 UTC for an epoch-seconds timestamp."""
    return float(now_ts) % 86_400.0


def score_universe(
    tickers: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    vol_history: dict[str, deque] | None = None,
    update_vol_history: bool = True,
    activity: dict[str, dict[str, Any]] | None = None,
    candidates: list[dict[str, Any]] | None = None,
    vol_ccy_is_quote: bool = False,
    fast_lane: list[dict[str, Any]] | None = None,
    price_history: dict[str, deque] | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Score a ticker snapshot into SC / MR / vol / union universes.

    Parameters
    ----------
    tickers:
        Raw OKX ticker dicts (``instId``, ``last``, ``open24h``, ``high24h``,
        ``low24h``, ``volCcy24h``, optional ``bidPx``/``askPx``).  The backtest
        synthesises these from candles.
    cfg:
        Screener configuration (see ``runtime_config["screener"]``).
    vol_history:
        Optional per-symbol rolling deque of 24h quote volume used to compute
        ``vol_spike_ratio``.  When provided and ``update_vol_history`` is True
        the deque is appended to in place (matching live's rolling window).
    update_vol_history:
        Set False to score against a pre-built history without mutating it.
    activity:
        Optional ``symbol → compute_recent_activity()`` mapping.  When supplied
        (and ``activity_lookback_hours`` > 0) the SC/MR gates and scores use the
        short-window features instead of the 24h aggregates, so a coin that was
        hot hours ago but has since gone quiet no longer ranks.  Symbols missing
        from the mapping fall back to their 24h features.
    candidates:
        Optional pre-built candidate list from :func:`build_candidates`.  Live
        builds it once to decide which symbols need an activity candle fetch,
        then passes it here so the prefilter does not run twice (which would
        double-append to ``vol_history``).
    vol_ccy_is_quote:
        Set True when the tickers' ``volCcy24h`` is already quote-currency
        volume (the backtest reconstructs it from summed ``volCcyQuote``).
        Live passes False so ``build_candidates`` converts OKX's base-currency
        ``volCcy24h`` to USDT via ``× last``.
    fast_lane:
        Optional output of :func:`fast_lane_candidates` — symbols admitted on a
        short-window price move rather than the trailing-24h volume gate.  They
        are merged into the candidate pool (deduped by symbol) so the normal
        SC/MR gates and scoring decide whether they are actually selected.
        Admitted symbols carry ``_fast_lane = True``.
    price_history, now_ts:
        Rolling ``(epoch_seconds, price)`` samples per symbol (live:
        ``_screener_price_history``) and the moment to measure at (epoch
        seconds).  They give the ``fast_speed`` term of the vol list, the only
        one that sees the current hour.  See :func:`compute_speed`.

    Vol universe (``vol_enabled``, wick_rejection plan §3.5)
    -------------------------------------------------------
    Ranked on speed (% per hour) rather than 24h aggregates, with its own gates
    (``vol_min_speed_pct_per_hour``, ``vol_min_vol_ratio``; the SC/MR
    ``activity_min_ratio`` gate does not apply) and cap ``vol_max_symbols``.
    Built in dual and legacy mode and appended to ``selected`` after SC and MR
    so its symbols get market data.

    Returns
    -------
    dict with keys:
        ``selected`` (union, SC-first, then MR, then vol), ``sc``, ``mr``,
        ``vol``, ``candidates``, ``base_candidate_count``, ``ticker_count``,
        ``sc_pool_size``, ``mr_pool_size``, ``vol_pool_size``, ``overlap``,
        ``dual_universe``, ``activity_used``, ``diagnostics``.
    """
    cfg = cfg or {}
    dual_universe = bool(cfg.get("dual_universe", True))
    # Shared / legacy caps.
    max_symbols = max(1, int(cfg.get("max_symbols") or 10))
    # Dual-universe caps (default to half of max_symbols each, min 3).
    sc_max = max(1, int(cfg.get("sc_max_symbols") or max(3, max_symbols // 2)))
    mr_max = max(1, int(cfg.get("mr_max_symbols") or max(3, max_symbols // 2)))
    min_volume_usd = float(cfg.get("min_volume_usd") or 0.0)
    # Liquidity guard: reject symbols whose bid/ask spread is too wide.
    # Wide spreads signal thin order books where market-order SL closes
    # slip badly (the dominant loss mechanism on low-liquidity alts).
    # 0.0 = disabled.  Default 0.5% filters out names like SATS/MMT where
    # a 10-USDT market close slipped 2-5x beyond the SL trigger.
    max_spread_pct = float(cfg.get("max_spread_pct") or 0.0)
    # SC filters: want movement.
    sc_min_momentum_pct = float(
        cfg.get("sc_min_momentum_pct", cfg.get("min_momentum_pct") or 1.5)
    )
    sc_min_hl_range_pct = float(
        cfg.get("sc_min_hl_range_pct", cfg.get("min_hl_range_pct") or 0.0)
    )
    # MR filters: want range but not strong trend.
    mr_min_hl_range_pct = float(cfg.get("mr_min_hl_range_pct") or 2.5)
    mr_max_momentum_pct = float(cfg.get("mr_max_momentum_pct") or 5.0)
    # Legacy single-list filters.
    min_momentum_pct = float(cfg.get("min_momentum_pct") or 0.0)
    min_hl_range_pct = float(cfg.get("min_hl_range_pct") or 0.0)
    # Recent-activity gate: require the symbol to be at least as active in the
    # short window as its own 24h average.  0 disables the gate.
    activity_min_ratio = float(cfg.get("activity_min_ratio") or 0.0)
    activity_lookback_hours = float(cfg.get("activity_lookback_hours") or 0.0)

    if candidates is None:
        candidates = build_candidates(
            tickers,
            cfg,
            vol_history=vol_history,
            update_vol_history=update_vol_history,
            vol_ccy_is_quote=vol_ccy_is_quote,
        )

    # ── Fast-lane discovery merge ─────────────────────────────────────
    # Symbols admitted on a short-window move join the pool before scoring, so
    # they must still win on activity to be selected.  This is the only place
    # the 24h volume gate is bypassed; nothing downstream changes.
    fast_lane_admitted = list(fast_lane or [])
    if fast_lane_admitted:
        candidates = merge_candidates(candidates, fast_lane_admitted)

    # ── Attach recent-activity features ───────────────────────────────
    # ``rec_*`` are the ranking inputs when activity data is available; the
    # 24h ``momentum_pct``/``hl_range_pct`` remain as context/diagnostics.
    activity_used = bool(activity) and activity_lookback_hours > 0
    for c in candidates:
        act = (activity or {}).get(c["symbol"])
        if act:
            c["recent_vol"] = act.get("recent_vol")
            c["recent_vol_ratio"] = act.get("recent_vol_ratio")
            c["recent_range_pct"] = act.get("recent_range_pct")
            c["recent_mom_pct"] = act.get("recent_mom_pct")
            c["decay_ratio"] = act.get("decay_ratio")
            c["recent_hours_covered"] = act.get("hours_covered")
        else:
            c["recent_hours_covered"] = None
            c["recent_vol"] = None
            c["recent_vol_ratio"] = None
            c["recent_range_pct"] = None
            c["recent_mom_pct"] = None
            c["decay_ratio"] = None
        # Ranking inputs: prefer recent features, fall back to 24h aggregates.
        if activity_used and c["recent_range_pct"] is not None:
            c["rank_mom_pct"] = abs(c["recent_mom_pct"] or 0.0)
            c["rank_range_pct"] = c["recent_range_pct"]
            c["rank_vol_ratio"] = c["recent_vol_ratio"]
        else:
            c["rank_mom_pct"] = c["momentum_pct"]
            c["rank_range_pct"] = c["hl_range_pct"]
            c["rank_vol_ratio"] = c["vol_spike_ratio"]

    # ── Speed (vol universe, §3.5) ────────────────────────────────────
    vol_enabled = bool(cfg.get("vol_enabled", False))
    speed_now = float(now_ts if now_ts is not None else _now_seconds())
    for c in candidates:
        c.update(
            compute_speed(
                c,
                (price_history or {}).get(c["symbol"]),
                now_ts=speed_now,
                window_minutes=float(cfg.get("fast_move_window_minutes") or 60.0),
            )
        )

    # Candidates below min_volume_usd (vol_min_volume_usd) compete for the vol
    # list only.
    main = [c for c in candidates if not c.get("vol_only")]
    vol_only_count = len(candidates) - len(main)

    spread_str = f", spread<={max_spread_pct:.2f}%" if max_spread_pct > 0 else ""
    activity_str = (
        f", activity={activity_lookback_hours:g}h" if activity_used else ""
    )
    vol_only_str = (
        f" + {vol_only_count} vol-only (vol>={vol_volume_floor(cfg):.0f} USD)"
        if vol_only_count else ""
    )
    diagnostics: list[str] = [
        f"Screener: {len(main)} base candidates{vol_only_str} from {len(tickers)} tickers "
        f"(vol>={min_volume_usd:.0f} USD{spread_str}{activity_str}, dual={dual_universe})"
    ]
    if fast_lane_admitted:
        # Report the lane separately so "why is this symbol here?" is answerable
        # from the log alone.
        window = float(cfg.get("fast_move_window_minutes") or 60.0)
        samples = ", ".join(
            f"{c['symbol']}({c.get('fast_move_pct') or 0:+.2f}%/"
            f"{(c.get('fast_span_seconds') or 0) / 60:.0f}m "
            f"v24h={c.get('vol_ccy_24h') or 0:,.0f} {c.get('fast_source') or '?'})"
            for c in sorted(
                fast_lane_admitted,
                key=lambda c: abs(c.get("fast_move_pct") or 0.0),
                reverse=True,
            )[:8]
        )
        diagnostics.append(
            f"Screener fast lane: {len(fast_lane_admitted)} admitted "
            f"(|move|>={abs(float(cfg.get('fast_move_threshold_pct') or 4.0)):g}% "
            f"over {window:g}m): {samples}"
        )

    selected: list[str] = []
    sc_selected: list[str] = []
    mr_selected: list[str] = []
    sc_pool_size = 0
    mr_pool_size = 0
    overlap: list[str] = []

    if not candidates:
        return {
            "selected": selected,
            "sc": sc_selected,
            "mr": mr_selected,
            "candidates": candidates,
            "base_candidate_count": 0,
            "ticker_count": len(tickers),
            "sc_pool_size": 0,
            "mr_pool_size": 0,
            "vol": [],
            "vol_pool_size": 0,
            "overlap": overlap,
            "dual_universe": dual_universe,
            "activity_used": activity_used,
            "fast_lane_count": len(fast_lane_admitted),
            "fast_lane_symbols": [c["symbol"] for c in fast_lane_admitted],
            "diagnostics": diagnostics,
        }

    # ── Normalisers ───────────────────────────────────────────────────
    # Computed over the ranking inputs so scores stay comparable whether we
    # rank on recent-window features or the 24h aggregates.
    max_vol_ratio = max(
        (c["rank_vol_ratio"] for c in main if c["rank_vol_ratio"] is not None),
        default=None,
    ) or 1.0
    max_raw_vol = max((c["vol_ccy_24h"] for c in main), default=0.0) or 1.0
    max_range = max((c["rank_range_pct"] for c in main), default=0.0) or 1.0
    max_mom = max((c["rank_mom_pct"] for c in main), default=0.0) or 1.0

    def _norm_vol(c: dict[str, Any]) -> float:
        ratio = c["rank_vol_ratio"]
        if ratio is not None:
            return float(ratio) / max_vol_ratio
        return float(c["vol_ccy_24h"]) / max_raw_vol

    def _fmt(c: dict[str, Any], score_key: str) -> str:
        """Diagnostic fragment: recent features when used, else 24h ones."""
        ratio = c["rank_vol_ratio"]
        ratio_str = f"{ratio:.2f}x" if ratio is not None else "n/a"
        decay = c.get("decay_ratio")
        decay_str = f" decay={decay:.2f}" if decay is not None else ""
        return (
            f"{c['symbol']}({score_key}={c.get(score_key, 0):.3f} vol={ratio_str} "
            f"rng={c['rank_range_pct']:.2f}% mom={c['rank_mom_pct']:.2f}%{decay_str})"
        )

    if dual_universe:
        # ── SC universe: expansion / momentum ──────────────────────
        sc_pool = [
            c for c in main
            if c["rank_mom_pct"] >= sc_min_momentum_pct
            and c["rank_range_pct"] >= sc_min_hl_range_pct
            and (
                activity_min_ratio <= 0
                or c["rank_vol_ratio"] is None
                or c["rank_vol_ratio"] >= activity_min_ratio
            )
        ]
        for c in sc_pool:
            norm_vol = _norm_vol(c)
            norm_range = c["rank_range_pct"] / max_range
            norm_mom = c["rank_mom_pct"] / max_mom
            # Reward unusual volume + range + directional momentum.
            c["sc_score"] = norm_vol * 0.45 + norm_range * 0.25 + norm_mom * 0.30
        sc_pool.sort(key=lambda x: x.get("sc_score", 0.0), reverse=True)
        sc_selected = [c["symbol"] for c in sc_pool[:sc_max]]
        sc_pool_size = len(sc_pool)

        # ── MR universe: chop / mean reversion ─────────────────────
        mr_pool = [
            c for c in main
            if c["rank_range_pct"] >= mr_min_hl_range_pct
            and c["rank_mom_pct"] <= mr_max_momentum_pct
            and (
                activity_min_ratio <= 0
                or c["rank_vol_ratio"] is None
                or c["rank_vol_ratio"] >= activity_min_ratio
            )
        ]
        for c in mr_pool:
            norm_vol = _norm_vol(c)
            norm_range = c["rank_range_pct"] / max_range
            # Inverse momentum: low directional body is better for MR.
            inv_mom = 1.0 - (c["rank_mom_pct"] / max_mom if max_mom > 0 else 0.0)
            inv_mom = max(0.0, min(1.0, inv_mom))
            # Prefer wide range + low trend + some activity.
            c["mr_score"] = norm_range * 0.45 + inv_mom * 0.35 + norm_vol * 0.20
        mr_pool.sort(key=lambda x: x.get("mr_score", 0.0), reverse=True)
        mr_selected = [c["symbol"] for c in mr_pool[:mr_max]]
        mr_pool_size = len(mr_pool)

        # Union preserves SC-first order then MR-only additions.
        selected = list(sc_selected)
        for sym in mr_selected:
            if sym not in selected:
                selected.append(sym)

        sc_parts = [_fmt(c, "sc_score") for c in sc_pool[:sc_max]]
        mr_parts = [_fmt(c, "mr_score") for c in mr_pool[:mr_max]]
        diagnostics.append(
            f"Screener SC selected ({len(sc_selected)}/{len(sc_pool)} pool): "
            f"{sc_selected} | {', '.join(sc_parts) if sc_parts else 'none'}"
        )
        diagnostics.append(
            f"Screener MR selected ({len(mr_selected)}/{len(mr_pool)} pool): "
            f"{mr_selected} | {', '.join(mr_parts) if mr_parts else 'none'}"
        )
        overlap = sorted(set(sc_selected) & set(mr_selected))
        diagnostics.append(
            f"Screener dual union={len(selected)} overlap={len(overlap)} "
            f"{overlap if overlap else '[]'}"
        )
    else:
        # ── Legacy single-list mode ────────────────────────────────
        legacy_pool = [
            c for c in main
            if c["rank_mom_pct"] >= min_momentum_pct
            and c["rank_range_pct"] >= min_hl_range_pct
            and (
                activity_min_ratio <= 0
                or c["rank_vol_ratio"] is None
                or c["rank_vol_ratio"] >= activity_min_ratio
            )
        ]
        for c in legacy_pool:
            norm_vol = _norm_vol(c)
            norm_range = c["rank_range_pct"] / max_range
            norm_mom = c["rank_mom_pct"] / max_mom
            c["score"] = norm_vol * 0.5 + norm_range * 0.3 + norm_mom * 0.2
        legacy_pool.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        selected = [c["symbol"] for c in legacy_pool[:max_symbols]]
        sc_selected = list(selected)
        mr_selected = list(selected)
        sc_pool_size = len(legacy_pool)
        mr_pool_size = len(legacy_pool)
        top_parts = [_fmt(c, "score") for c in legacy_pool[:max_symbols]]
        diagnostics.append(f"Screener selected: {selected} | {', '.join(top_parts)}")

    # ── Vol universe: speed (wick_rejection §3.5) ─────────────────────
    vol_selected: list[str] = []
    vol_pool_size = 0
    if vol_enabled:
        vol_max = max(1, int(cfg.get("vol_max_symbols") or 8))
        vol_min_speed = float(cfg.get("vol_min_speed_pct_per_hour") or 0.0)
        vol_min_ratio = float(cfg.get("vol_min_vol_ratio") or 0.0)
        vol_pool = [
            c for c in candidates
            if c.get("speed") is not None
            and c["speed"] >= vol_min_speed
            # Missing activity volume fails the gate (no 24h fallback).
            and c.get("recent_vol_ratio") is not None
            and c["recent_vol_ratio"] >= vol_min_ratio
        ]
        if vol_pool:
            max_speed = max(c["speed"] for c in vol_pool) or 1.0
            max_ratio = max(c["recent_vol_ratio"] for c in vol_pool) or 1.0
            max_decay = max((c.get("decay_ratio") or 0.0) for c in vol_pool) or 1.0
            for c in vol_pool:
                c["vol_score"] = (
                    c["speed"] / max_speed * 0.50
                    + c["recent_vol_ratio"] / max_ratio * 0.30
                    + (c.get("decay_ratio") or 0.0) / max_decay * 0.20
                )
            vol_pool.sort(key=lambda x: x["vol_score"], reverse=True)
        vol_selected = [c["symbol"] for c in vol_pool[:vol_max]]
        vol_pool_size = len(vol_pool)
        for sym in vol_selected:
            if sym not in selected:
                selected.append(sym)

        def _fmt_speed(v: float | None) -> str:
            return f"{v:.2f}" if v is not None else "n/a"

        vol_parts = [
            f"{c['symbol']}(vol_score={c['vol_score']:.3f} speed={c['speed']:.2f}%/h "
            f"closed={_fmt_speed(c.get('closed_speed'))} fast={_fmt_speed(c.get('fast_speed'))} "
            f"vol={c['recent_vol_ratio']:.2f}x decay={_fmt_speed(c.get('decay_ratio'))})"
            for c in vol_pool[:vol_max]
        ]
        diagnostics.append(
            f"Screener VOL selected ({len(vol_selected)}/{vol_pool_size} pool, "
            f"speed>={vol_min_speed:g}%/h vol>={vol_min_ratio:g}x): "
            f"{vol_selected} | {', '.join(vol_parts) if vol_parts else 'none'}"
        )

    # ── Faded-name diagnostic ─────────────────────────────────────────
    # Names that passed the liquidity gates and still look active on 24h
    # aggregates but have gone quiet in the recent window.  These are exactly
    # the "big 24h change, not moving now" picks the activity window removes.
    if activity_used:
        faded = [
            c for c in main
            if c.get("decay_ratio") is not None
            and c["decay_ratio"] < 0.25
            and c["hl_range_pct"] >= mr_min_hl_range_pct
        ]
        if faded:
            faded.sort(key=lambda c: c["decay_ratio"])
            sample = ", ".join(
                f"{c['symbol']}(decay={c['decay_ratio']:.2f} "
                f"24h_hl={c['hl_range_pct']:.1f}%)"
                for c in faded[:8]
            )
            diagnostics.append(
                f"Screener dropped {len(faded)} faded name(s) (decay<0.25): {sample}"
            )

    return {
        "selected": selected,
        "sc": sc_selected,
        "mr": mr_selected,
        "candidates": candidates,
        "base_candidate_count": len(main),
        "vol_only_count": vol_only_count,
        "ticker_count": len(tickers),
        "sc_pool_size": sc_pool_size,
        "mr_pool_size": mr_pool_size,
        "vol": vol_selected,
        "vol_pool_size": vol_pool_size,
        "overlap": overlap,
        "dual_universe": dual_universe,
        "activity_used": activity_used,
        "fast_lane_count": len(fast_lane_admitted),
        "fast_lane_symbols": [c["symbol"] for c in fast_lane_admitted],
        "diagnostics": diagnostics,
    }


def compute_speed(
    candidate: dict[str, Any],
    history: Any,
    *,
    now_ts: float,
    window_minutes: float,
) -> dict[str, float | None]:
    """Return ``closed_speed``, ``fast_speed`` and ``speed`` (% per hour).

    * ``closed_speed`` = ``recent_range_pct / recent_hours_covered`` from
      :func:`compute_recent_activity` features only — never the 24h
      ``hl_range_pct`` (that inflates speed ~6×).  None without activity data.
    * ``fast_speed`` = ``|move| / (window_minutes / 60)`` from the rolling
      price samples (:func:`_fast_move_from_history`); the only term that sees
      the current hour.  None without enough history.
    * ``speed`` = the larger defined term, or None when neither is defined
      (the symbol cannot enter the vol list).
    """
    closed_speed: float | None = None
    rng = candidate.get("recent_range_pct")
    hours = candidate.get("recent_hours_covered")
    if rng is not None and hours:
        closed_speed = float(rng) / float(hours)

    fast_speed: float | None = None
    window_minutes = max(1.0, float(window_minutes))
    if history:
        measured = _fast_move_from_history(
            history, now_ts=now_ts, window_seconds=window_minutes * 60.0
        )
        if measured is not None:
            fast_speed = abs(measured[0]) / (window_minutes / 60.0)

    defined = [v for v in (closed_speed, fast_speed) if v is not None]
    return {
        "closed_speed": closed_speed,
        "fast_speed": fast_speed,
        "speed": max(defined) if defined else None,
    }


def universe_for_strategy(
    strategy_name: str,
    *,
    sc_symbols: list[str],
    mr_symbols: list[str],
    selected_symbols: list[str],
    fallback_symbols: list[str],
    dual_universe: bool = True,
    vol_symbols: list[str] | None = None,
) -> list[str]:
    """Return the active screener universe for a strategy.

    Mirrors ``MarketService.get_screener_universe`` so the backtest routes
    strategies to the same SC/MR/vol lists live uses:

      - ``spike_continuation`` / ``trend_pullback`` → SC (trending) list
      - ``mean_reversion`` / ``liquidity_sweep`` / ``vwap_reversion`` → MR (chop) list
      - ``wick_rejection`` → vol list (dual and legacy mode)
      - other / empty → union of SC and MR

    Falls back to the combined selected list, then to ``fallback_symbols``
    (the configured symbol list) when the screener lists are empty.  Symbols
    only on the vol list never reach the other strategies through the
    fallback.
    """
    name = (strategy_name or "").strip().lower()
    vol = list(vol_symbols or [])
    if name == "wick_rejection" and vol:
        return vol
    if vol and name != "wick_rejection":
        vol_only = set(vol) - set(sc_symbols) - set(mr_symbols)
        selected_symbols = [s for s in selected_symbols if s not in vol_only]
    if dual_universe and name in ("spike_continuation", "trend_pullback") and sc_symbols:
        return list(sc_symbols)
    if dual_universe and name in (
        "mean_reversion", "liquidity_sweep", "vwap_reversion"
    ) and mr_symbols:
        return list(mr_symbols)
    if selected_symbols:
        return list(selected_symbols)
    return list(fallback_symbols)
