"""Light-weight volume-profile helpers."""
from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["poc", "value_area"]

_MIN_BINS = 5


def _histogram(
    price: pd.Series,
    volume: pd.Series,
    bins: int,
) -> tuple[np.ndarray, np.ndarray, float, float]:
    """Return ``(hist, edges, lo, hi)`` for a volume-weighted price histogram.

    Raises ``ValueError`` when either input series is empty.  ``bins`` is
    clamped to ``_MIN_BINS`` so degenerate windows (a single distinct price)
    still produce a usable, non-empty histogram instead of crashing.
    """
    if price.empty or volume.empty:
        raise ValueError("price and volume series must be non-empty")
    price = pd.to_numeric(price, errors="coerce").dropna()
    volume = pd.to_numeric(volume, errors="coerce").fillna(0.0)
    if price.empty:
        raise ValueError("price series has no valid numeric values")
    # Align both series to the same index before bucketing.
    df = pd.concat([price, volume], axis=1).dropna()
    if df.empty:
        raise ValueError("price/volume series share no valid rows")
    price, volume = df.iloc[:, 0], df.iloc[:, 1]
    lo, hi = float(price.min()), float(price.max())
    if not np.isfinite(hi - lo) or hi - lo == 0.0:
        # Degenerate (all prices identical): pad the range so the histogram
        # still has a flat, valid bucket containing that price.
        lo = lo - 0.5
        hi = hi + 0.5
    used_bins = max(int(bins), _MIN_BINS)
    hist, edges = np.histogram(price, bins=used_bins, weights=volume, range=(lo, hi))
    return hist, edges, lo, hi


def poc(price: pd.Series, volume: pd.Series, bins: int = 50) -> float:
    """Return Point-of-Control price for the given window.

    Uses a simple histogram (price weighted by volume). Suitable for
    small windows (≤1 000 rows) which is typical in intraday strategies.
    """
    hist, edges, _lo, _hi = _histogram(price, volume, bins)
    idx = int(hist.argmax())
    return float((edges[idx] + edges[idx + 1]) / 2.0)


def value_area(
    price: pd.Series,
    volume: pd.Series,
    bins: int = 50,
    va_pct: float = 0.70,
) -> tuple[float, float, float]:
    """Return ``(poc, value_area_high, value_area_low)`` for a window.

    Computes a volume-weighted price histogram, finds the Point of Control
    (the bucket with the most volume), then expands symmetrically around it
    until the cumulative participant volume reaches ``va_pct`` of the total.
    The resulting high/low bounds define the Value Area — the price range in
    which the majority of trading occurred.

    Parameters
    ----------
    price
        Price series (e.g. typical prices ``(high+low+close)/3``).
    volume
        Volume series aligned to ``price``.
    bins
        Number of histogram buckets.
    va_pct
        Fraction of total volume that must be enclosed by the value area
        (0.70 is the classic 70 % value area).

    Returns
    -------
    (poc, value_area_high, value_area_low) as floats.  On degenerate input
    (e.g. a single price) all three collapse to that same price.
    """
    if price.empty or volume.empty:
        raise ValueError("price and volume series must be non-empty")
    hist, edges, _lo, _hi = _histogram(price, volume, bins)
    total = float(hist.sum())
    if total <= 0:
        raise ValueError("volume series has no positive volume")

    poc_idx = int(hist.argmax())
    vah_idx = poc_idx
    val_idx = poc_idx
    enclosed = float(hist[poc_idx])
    target = total * float(va_pct)

    # Expand left/right from the POC, each step adding the larger neighbouring
    # bucket to enclose the requested volume fraction most efficiently.
    n = len(hist)
    left = poc_idx - 1
    right = poc_idx + 1
    while enclosed < target and (left >= 0 or right < n):
        cand_left = hist[left] if left >= 0 else -1.0
        cand_right = hist[right] if right < n else -1.0
        if cand_right >= cand_left:
            enclosed += cand_right
            vah_idx = right
            right += 1
        else:
            enclosed += cand_left
            val_idx = left
            left -= 1

    poc_price = float((edges[poc_idx] + edges[poc_idx + 1]) / 2.0)
    va_high = float(edges[vah_idx + 1])
    va_low = float(edges[val_idx])
    return poc_price, va_high, va_low
