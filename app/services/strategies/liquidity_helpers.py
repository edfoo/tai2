"""Shared liquidity-aware entry helpers for the Launcher strategies.

These pure functions let every strategy apply the same funding-rate,
open-interest, and order-book imbalance gates (refactor guide §3) without
duplicating logic.  Each returns a boolean "ok / veto" plus a lightweight
report dict so strategies can enrich their debug lines.

Data sources (read from the per-symbol snapshot entry):
  - ``funding_rate``: OKX funding-rate metadata dict.  Field ``fundingRate``
    (decimal, e.g. ``0.0001``).  Falls back to ``fundingRate``/``fundRate``.
  - ``open_interest``: OKX OI metadata dict.  Field ``oi`` (coins) or
    ``oiCcy``.  A rolling z-score is computed from an accumulated history
    (``oi_history``) when provided; otherwise a flat delta threshold is used.
  - ``order_book``: normalized ``{"bids": [[px, sz]...], "asks": [[px, sz]...]}``.

Z-score maths are deliberately simple so a strategy (or live/backtest caller)
can feed the same data every tick without needing stored state.
"""

from __future__ import annotations

from statistics import mean, pstdev


def _to_float(value: object) -> float | None:
    """Coerce a value to float, returning None for empty/unparseable input."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if _isfinite(v) else None


def _isfinite(value: float) -> bool:
    import math

    return math.isfinite(value)


def order_book_imbalance(order_book: dict) -> float | None:
    """Return bid_qty / ask_qty across the book (None when not computable)."""
    if not order_book:
        return None
    _bids = order_book.get("bids") or []
    _asks = order_book.get("asks") or []
    if not _bids or not _asks:
        return None
    bid_qty = 0.0
    for row in _bids:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            bid_qty += _to_float(row[1]) or 0.0
    ask_qty = 0.0
    for row in _asks:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            ask_qty += _to_float(row[1]) or 0.0
    if ask_qty <= 0:
        return None
    return bid_qty / ask_qty


def funding_is_blocked(
    funding: dict,
    *,
    direction: str = "long",
    max_abs_rate: float = 0.001,  # 0.1 % — beyond this funding is "extreme"
) -> tuple[bool, dict]:
    """Return ``(blocked, info)`` for a funding-rate filter.

    Long entries are blocked when funding is *extremely* long-positive
    (crowded longs, favourable to shorts); short entries blocked when funding
    is extremely short-negative (the mirror).  This mirrors §3's
    "no funding/extreme imbalance veto" and the VWAP |funding_z| < 0.7 bias
    filter, but uses an absolute-rate threshold so it works with a single
    snapshot (no 30-day history required).  ``max_abs_rate`` defaults to
    0.1 % — a level meaningfully above OKX's typical 0.01 % baseline.
    """
    rate = _to_float(funding.get("fundingRate") or funding.get("fundRate"))
    if rate is None:
        return False, {"available": False, "rate": None}
    blocked = False
    if direction == "long":
        blocked = rate > max_abs_rate
    elif direction == "short":
        blocked = rate < -max_abs_rate
    return blocked, {"available": True, "rate": rate, "max_abs_rate": max_abs_rate}


def oi_confirms_momentum(
    open_interest: dict,
    *,
    direction: str = "long",
    oi_history: list[float] | None = None,
    min_delta_ratio: float = 0.0,
    min_zscore: float = 1.0,
    require_positive_delta: bool = True,
) -> tuple[bool, dict]:
    """Return ``(ok, info)`` for an open-interest momentum confirmation.

    Momentum entries (spike continuation) should only fire when OI is *rising*
    (fresh leverage).  Uses a rolling z-score over ``oi_history`` when
    supplied; otherwise falls back to a flat delta check.  Returns
    ``ok=True`` when OI data is unavailable (neutral) so strategies degrade
    gracefully on instruments with no OI feed.
    """
    oi_now = _to_float(open_interest.get("oi") or open_interest.get("oiCcy"))
    if oi_now is None:
        return True, {"available": False, "oi": None}
    # Need at least one prior sample to measure a delta.
    prev = _to_float(open_interest.get("oi_prev") or open_interest.get("oiCcyPrev"))
    info: dict = {"available": True, "oi": oi_now}

    hist = [x for x in (oi_history or []) if _to_float(x) is not None and _to_float(x) > 0]
    if prev is not None:
        prev = prev or 0.0
        delta_ratio = (oi_now - prev) / prev if prev > 0 else None
        info["prev"] = prev
        info["delta_ratio"] = delta_ratio
        if delta_ratio is not None and delta_ratio < min_delta_ratio:
            return False, info
        # Without a z-score history, enforce the trade-direction delta: longs
        # need OI rising, shorts need OI falling (fresh leverage in direction).
        if require_positive_delta and len(hist) < 2 and delta_ratio is not None:
            in_direction = delta_ratio > 0 if direction == "long" else delta_ratio < 0
            if not in_direction:
                info["reason"] = "oi_delta_against_direction"
                return False, info

    if len(hist) >= 2:
        _h = [_to_float(x) or 0.0 for x in hist]
        _mu = mean(_h)
        _sd = pstdev(_h) if len(_h) > 1 else 0.0
        if _sd > 0:
            z = (oi_now - _mu) / _sd
            info["zscore"] = z
            z_ok = (z > min_zscore) if direction == "long" else (z < -min_zscore)
            return (z_ok, info)
    # Fall through: no history and no prev → neutral (pass).
    if prev is None:
        return True, info
    if require_positive_delta:
        in_direction = (oi_now > prev) if direction == "long" else (oi_now < prev)
        return (in_direction, info)
    return True, info