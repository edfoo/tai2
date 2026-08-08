"""Light-weight volume-profile helpers."""
from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["poc"]

def poc(price: pd.Series, volume: pd.Series, bins: int = 50) -> float:
    """Return Point-of-Control price for the given window.

    Uses a simple histogram (price weighted by volume). Suitable for
    small windows (≤1 000 rows) which is typical in intraday strategies.
    """
    if price.empty or volume.empty:
        raise ValueError("price and volume series must be non-empty")
    lo, hi = float(price.min()), float(price.max())
    hist, edges = np.histogram(price, bins=bins, weights=volume, range=(lo, hi))
    idx = int(hist.argmax())
    return float((edges[idx] + edges[idx + 1]) / 2.0)
