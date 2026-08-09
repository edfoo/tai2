"""Shared liquidity-aware entry helpers for the Launcher strategies.

These pure functions let every strategy apply the same funding-rate,
open-interest, and order-book imbalance gates (refactor guide §3) without
duplicating logic.  Each returns a boolean "ok / veto" plus a lightweight
report dict so strategies can enrich their debug lines.

Data sources (read from the per-symbol snapshot entry):
  - ``funding_rate``: OKX funding-rate metadata dict.  Field ``fundingRate``
    (decimal, e.g. ``0.0001``).  Falls back to ``fundingRate``/``fundRate``.
  - ``funding_z`` (optional scalar, Phase 0d): pre-computed z-score of the
    current funding rate relative to a 30-day rolling history.  When present,
    it is used instead of the absolute-rate proxy.
  - ``open_interest``: OKX OI metadata dict.  Field ``oi`` (coins) or
    ``oiCcy``.
  - ``oi_zscore`` (optional scalar, Phase 0d): pre-computed z-score of the
    latest OI delta vs its rolling history.  When present it replaces the
    flat positive-delta fallback.
  - ``order_book``: normalized ``{"bids": [[px, sz]...], "asks": [[px, sz]...]}``.

Phase 0d upgrade: ``_build_snapshot`` now appends each poll's funding rate
and OI reading to rolling history buffers and writes ``funding_z`` /
``oi_zscore`` into the per-symbol snapshot entry.  The helpers below
**prefer** these pre-computed scalars when available, and fall back to the
absolute-rate / delta proxy when the history is still short (bot just started).
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
    max_abs_rate: float = 0.001,
    funding_z: float | None = None,
    max_funding_z: float = 1.28,
) -> tuple[bool, dict]:
    """Return ``(blocked, info)`` for a funding-rate filter.

    **Phase 0d upgrade**: when ``funding_z`` (a pre-computed 30-day rolling
    z-score) is supplied, the gate uses ``|funding_z| > max_funding_z``
    (default 1.28 ≈ top-10 % tail) instead of the absolute-rate proxy.  This
    directly implements the guide's §3 requirement for mean-reversion and VWAP.

    Falls back to the absolute ``max_abs_rate`` threshold when ``funding_z``
    is None (history not yet seeded, or backtest snapshot).
    """
    rate = _to_float(funding.get("fundingRate") or funding.get("fundRate"))
    info: dict = {"available": rate is not None}
    if rate is not None:
        info["rate"] = rate

    if funding_z is not None:
        info["funding_z"] = funding_z
        if direction == "long":
            blocked = funding_z > max_funding_z
        elif direction == "short":
            blocked = funding_z < -max_funding_z
        else:
            blocked = False
        info["method"] = "zscore"
        return blocked, info

    # Fallback: absolute-rate proxy (Phase 1 behaviour, no 30-day history).
    if rate is None:
        return False, info
    if direction == "long":
        blocked = rate > max_abs_rate
    elif direction == "short":
        blocked = rate < -max_abs_rate
    else:
        blocked = False
    info["method"] = "abs_rate"
    return blocked, info


def oi_confirms_momentum(
    open_interest: dict,
    *,
    direction: str = "long",
    oi_history: list[float] | None = None,
    oi_zscore: float | None = None,
    min_delta_ratio: float = 0.0,
    min_zscore: float = 1.0,
    require_positive_delta: bool = True,
) -> tuple[bool, dict]:
    """Return ``(ok, info)`` for an open-interest momentum confirmation.

    **Phase 0d upgrade**: when ``oi_zscore`` (the pre-computed z-score of the
    latest OI delta relative to rolling history) is supplied, the gate uses it
    directly.  For longs: ok when ``oi_zscore > min_zscore``.  For shorts: ok
    when ``oi_zscore < -min_zscore``.  Falls back to the flat-delta proxy
    (Phase 1 behaviour) when ``oi_zscore`` is None.

    Returns ``ok=True`` when OI data is unavailable (neutral) so strategies
    degrade gracefully on instruments with no OI feed.
    """
    oi_now = _to_float(open_interest.get("oi") or open_interest.get("oiCcy"))
    if oi_now is None and oi_zscore is None:
        return True, {"available": False, "oi": None}

    info: dict = {"available": True}
    if oi_now is not None:
        info["oi"] = oi_now

    # ── Phase 0d path: use pre-computed z-score ───────────────────────────
    if oi_zscore is not None:
        info["oi_zscore"] = oi_zscore
        info["method"] = "zscore"
        if direction == "long":
            ok = oi_zscore > min_zscore
        else:
            ok = oi_zscore < -min_zscore
        return ok, info

    # ── Phase 1 fallback: flat delta check ────────────────────────────────
    if oi_now is None:
        return True, {"available": False, "oi": None}

    prev = _to_float(open_interest.get("oi_prev") or open_interest.get("oiCcyPrev"))
    hist = [x for x in (oi_history or []) if _to_float(x) is not None and _to_float(x) > 0]
    if prev is not None:
        prev = prev or 0.0
        delta_ratio = (oi_now - prev) / prev if prev > 0 else None
        info["prev"] = prev
        info["delta_ratio"] = delta_ratio
        if delta_ratio is not None and delta_ratio < min_delta_ratio:
            return False, info
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