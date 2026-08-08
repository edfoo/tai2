"""Volatility helpers: adaptive ATR that scales by regime.

This module is self-contained and pure-Python so strategies can import
`adaptive_atr` without creating heavy external dependencies.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["adaptive_atr"]


def _true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    """Vectorised True Range (TR)."""
    prev_close = close.shift()
    return np.maximum(
        high - low,
        np.maximum((high - prev_close).abs(), (low - prev_close).abs()),
    )


def adaptive_atr(df: pd.DataFrame, lookback: int = 14) -> pd.Series:
    """Return ATR distance *already* scaled by a volatility-regime multiplier.

    Parameters
    ----------
    df:
        DataFrame with at least ``high``, ``low``, ``close`` columns.
    lookback:
        Period for the classical Wilder ATR window.

    Notes
    -----
    The returned series is **ATR * k**, where *k* is chosen per-row:

    * Quiet regime   (ATR% < 1.5) → k = 1.20
    * Normal regime  (1.5 ≤ ATR% < 3.0) → k = 1.80
    * Volatile regime(ATR% ≥ 3.0) → k = 2.50

    This gives naturally wider stops in explosive conditions without
    parameter hand-tuning inside every strategy.
    """
    if not {"high", "low", "close"}.issubset(df.columns):
        raise ValueError("DataFrame must contain high, low, close columns")

    tr = _true_range(df["high"], df["low"], df["close"])
    atr = tr.rolling(lookback, min_periods=lookback).mean()

    atr_pct = atr / df["close"] * 100.0
    # regime multipliers
    regime_mult = np.select(
        [atr_pct < 1.5, atr_pct < 3.0],
        [1.20, 1.80],
        default=2.50,
    )
    return atr * regime_mult
