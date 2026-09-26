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


def score_universe(
    tickers: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    vol_history: dict[str, deque] | None = None,
    update_vol_history: bool = True,
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

    Returns
    -------
    dict with keys:
        ``selected`` (union, SC-first), ``sc``, ``mr``, ``candidates``,
        ``base_candidate_count``, ``ticker_count``, ``sc_pool_size``,
        ``mr_pool_size``, ``overlap``, ``dual_universe``, ``diagnostics``.
    """
    cfg = cfg or {}
    universe_pattern = str(cfg.get("universe_filter") or "*-USDT-SWAP").strip().upper()
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

    spread_str = f", spread<={max_spread_pct:.2f}%" if max_spread_pct > 0 else ""
    diagnostics: list[str] = [
        f"Screener: {len(candidates)} base candidates from {len(tickers)} tickers "
        f"(vol>={min_volume_usd:.0f} USD{spread_str}, dual={dual_universe})"
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
            "diagnostics": diagnostics,
        }

    spike_candidates = [c for c in candidates if c["vol_spike_ratio"] is not None]
    max_spike = max((c["vol_spike_ratio"] for c in spike_candidates), default=None) or 1.0
    max_raw_vol = max(c["vol_ccy_24h"] for c in candidates) or 1.0
    max_hl = max(c["hl_range_pct"] for c in candidates) or 1.0
    max_mom = max(c["momentum_pct"] for c in candidates) or 1.0

    def _norm_vol(c: dict[str, Any]) -> float:
        if c["vol_spike_ratio"] is not None and spike_candidates:
            return float(c["vol_spike_ratio"]) / max_spike
        return float(c["vol_ccy_24h"]) / max_raw_vol

    if dual_universe:
        # ── SC universe: expansion / momentum ──────────────────────
        sc_pool = [
            c for c in candidates
            if c["momentum_pct"] >= sc_min_momentum_pct
            and c["hl_range_pct"] >= sc_min_hl_range_pct
        ]
        for c in sc_pool:
            norm_vol = _norm_vol(c)
            norm_hl = c["hl_range_pct"] / max_hl
            norm_mom = c["momentum_pct"] / max_mom
            # Reward unusual volume + range + directional momentum.
            c["sc_score"] = norm_vol * 0.45 + norm_hl * 0.25 + norm_mom * 0.30
        sc_pool.sort(key=lambda x: x.get("sc_score", 0.0), reverse=True)
        sc_selected = [c["symbol"] for c in sc_pool[:sc_max]]
        sc_pool_size = len(sc_pool)

        # ── MR universe: chop / mean reversion ─────────────────────
        mr_pool = [
            c for c in candidates
            if c["hl_range_pct"] >= mr_min_hl_range_pct
            and c["momentum_pct"] <= mr_max_momentum_pct
        ]
        for c in mr_pool:
            norm_vol = _norm_vol(c)
            norm_hl = c["hl_range_pct"] / max_hl
            # Inverse momentum: low directional body is better for MR.
            inv_mom = 1.0 - (c["momentum_pct"] / max_mom if max_mom > 0 else 0.0)
            inv_mom = max(0.0, min(1.0, inv_mom))
            # Prefer wide range + low trend + some activity.
            c["mr_score"] = norm_hl * 0.45 + inv_mom * 0.35 + norm_vol * 0.20
        mr_pool.sort(key=lambda x: x.get("mr_score", 0.0), reverse=True)
        mr_selected = [c["symbol"] for c in mr_pool[:mr_max]]
        mr_pool_size = len(mr_pool)

        # Union preserves SC-first order then MR-only additions.
        selected = list(sc_selected)
        for sym in mr_selected:
            if sym not in selected:
                selected.append(sym)

        sc_parts = []
        for c in sc_pool[:sc_max]:
            spike_str = f"{c['vol_spike_ratio']:.2f}x" if c["vol_spike_ratio"] is not None else "n/a"
            sc_parts.append(
                f"{c['symbol']}(sc={c.get('sc_score', 0):.3f} spike={spike_str} "
                f"hl={c['hl_range_pct']:.2f}% mom={c['momentum_pct']:.2f}%)"
            )
        mr_parts = []
        for c in mr_pool[:mr_max]:
            spike_str = f"{c['vol_spike_ratio']:.2f}x" if c["vol_spike_ratio"] is not None else "n/a"
            mr_parts.append(
                f"{c['symbol']}(mr={c.get('mr_score', 0):.3f} spike={spike_str} "
                f"hl={c['hl_range_pct']:.2f}% mom={c['momentum_pct']:.2f}%)"
            )
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
            if c["momentum_pct"] >= min_momentum_pct
            and c["hl_range_pct"] >= min_hl_range_pct
        ]
        for c in legacy_pool:
            norm_vol = _norm_vol(c)
            norm_hl = c["hl_range_pct"] / max_hl
            norm_mom = c["momentum_pct"] / max_mom
            c["score"] = norm_vol * 0.5 + norm_hl * 0.3 + norm_mom * 0.2
        legacy_pool.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        selected = [c["symbol"] for c in legacy_pool[:max_symbols]]
        sc_selected = list(selected)
        mr_selected = list(selected)
        sc_pool_size = len(legacy_pool)
        mr_pool_size = len(legacy_pool)
        top_parts = []
        for c in legacy_pool[:max_symbols]:
            spike_str = f"{c['vol_spike_ratio']:.2f}x" if c["vol_spike_ratio"] is not None else "n/a"
            top_parts.append(
                f"{c['symbol']}(score={c.get('score', 0):.3f} spike={spike_str} "
                f"hl={c['hl_range_pct']:.2f}% mom={c['momentum_pct']:.2f}%)"
            )
        diagnostics.append(f"Screener selected: {selected} | {', '.join(top_parts)}")

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
