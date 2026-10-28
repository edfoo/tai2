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
        if not _row_is_confirmed(row):
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
) -> list[dict[str, Any]]:
    """Filter a ticker snapshot into enriched screener candidates.

    This is the shared prefilter: it applies the pattern / price / volume /
    spread gates and attaches the 24h-derived features.  Live uses it to pick
    which symbols are worth fetching recent candles for; ``score_universe``
    uses it as its first stage.
    """
    cfg = cfg or {}
    universe_pattern = str(cfg.get("universe_filter") or "*-USDT-SWAP").strip().upper()
    min_volume_usd = float(cfg.get("min_volume_usd") or 0.0)
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
        if vol_ccy_24h is None or vol_ccy_24h < min_volume_usd:
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
            }
        )
    return candidates


def score_universe(
    tickers: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    vol_history: dict[str, deque] | None = None,
    update_vol_history: bool = True,
    activity: dict[str, dict[str, Any]] | None = None,
    candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Score a ticker snapshot into SC / MR / union universes.

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

    Returns
    -------
    dict with keys:
        ``selected`` (union, SC-first), ``sc``, ``mr``, ``candidates``,
        ``base_candidate_count``, ``ticker_count``, ``sc_pool_size``,
        ``mr_pool_size``, ``overlap``, ``dual_universe``, ``activity_used``,
        ``diagnostics``.
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
        )

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
        else:
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

    spread_str = f", spread<={max_spread_pct:.2f}%" if max_spread_pct > 0 else ""
    activity_str = (
        f", activity={activity_lookback_hours:g}h" if activity_used else ""
    )
    diagnostics: list[str] = [
        f"Screener: {len(candidates)} base candidates from {len(tickers)} tickers "
        f"(vol>={min_volume_usd:.0f} USD{spread_str}{activity_str}, dual={dual_universe})"
    ]

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
            "overlap": overlap,
            "dual_universe": dual_universe,
            "activity_used": activity_used,
            "diagnostics": diagnostics,
        }

    # ── Normalisers ───────────────────────────────────────────────────
    # Computed over the ranking inputs so scores stay comparable whether we
    # rank on recent-window features or the 24h aggregates.
    max_vol_ratio = max(
        (c["rank_vol_ratio"] for c in candidates if c["rank_vol_ratio"] is not None),
        default=None,
    ) or 1.0
    max_raw_vol = max(c["vol_ccy_24h"] for c in candidates) or 1.0
    max_range = max(c["rank_range_pct"] for c in candidates) or 1.0
    max_mom = max(c["rank_mom_pct"] for c in candidates) or 1.0

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
            c for c in candidates
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
            c for c in candidates
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
            c for c in candidates
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

    # ── Faded-name diagnostic ─────────────────────────────────────────
    # Names that passed the liquidity gates and still look active on 24h
    # aggregates but have gone quiet in the recent window.  These are exactly
    # the "big 24h change, not moving now" picks the activity window removes.
    if activity_used:
        faded = [
            c for c in candidates
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
        "base_candidate_count": len(candidates),
        "ticker_count": len(tickers),
        "sc_pool_size": sc_pool_size,
        "mr_pool_size": mr_pool_size,
        "overlap": overlap,
        "dual_universe": dual_universe,
        "activity_used": activity_used,
        "diagnostics": diagnostics,
    }


def universe_for_strategy(
    strategy_name: str,
    *,
    sc_symbols: list[str],
    mr_symbols: list[str],
    selected_symbols: list[str],
    fallback_symbols: list[str],
    dual_universe: bool = True,
) -> list[str]:
    """Return the active screener universe for a strategy.

    Mirrors ``MarketService.get_screener_universe`` so the backtest routes
    strategies to the same SC/MR lists live uses:

      - ``spike_continuation`` / ``trend_pullback`` → SC (trending) list
      - ``mean_reversion`` / ``liquidity_sweep`` / ``vwap_reversion`` → MR (chop) list
      - other / empty → union of both

    Falls back to the combined selected list, then to ``fallback_symbols``
    (the configured symbol list) when the screener lists are empty.
    """
    name = (strategy_name or "").strip().lower()
    if dual_universe and name in ("spike_continuation", "trend_pullback") and sc_symbols:
        return list(sc_symbols)
    if dual_universe and name in (
        "mean_reversion", "liquidity_sweep", "vwap_reversion"
    ) and mr_symbols:
        return list(mr_symbols)
    if selected_symbols:
        return list(selected_symbols)
    return list(fallback_symbols)
