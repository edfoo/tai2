"""Indicator service helper functions.

Currently only provides a lightweight `is_trending` utility that allows any
strategy to quickly gate signals when the higher-timeframe market is trending
instead of ranging.

The definition of a trending regime is deliberately simple so it can run on the
same vectorised `indicators` payload that the rest of the application already
generates:

* **ADX_HTF ≥ 25**   → strong directional movement
* **Choppiness_HTF < 40** → low choppiness = trending

Either condition marks the row as trending.  Missing values default to
"not-trending" to avoid false positives on incomplete datasets.
"""

from __future__ import annotations

from typing import Optional


def is_trending(
    adx_htf: Optional[float] | float | None,
    chop_htf: Optional[float] | float | None,
    *,
    adx_threshold: float = 25.0,
    choppiness_threshold: float = 40.0,
) -> bool | None:
    """Return ``True``/``False`` when the higher-timeframe regime is trending.

    Returns ``None`` when *no* higher-timeframe regime data is available, so
    callers can treat the gate as neutral (do not block) rather than assuming
    a trend or a range.  This mirrors the existing ``require_htf_trend``
    auto-disable behaviour when HTF indicators are absent.

    Parameters
    ----------
    adx_htf
        Average Directional Index value from the *higher timeframe* (e.g. 4H
        when the strategy runs on 15-minute bars).
    chop_htf
        Choppiness Index value from the higher timeframe.
    adx_threshold
        ADX value that constitutes a trend.  Classic textbooks use 25.
    choppiness_threshold
        Choppiness value below which the market is deemed directional
        (scale 0–100).  A threshold of 40 roughly matches ADX 25.
    """

    if adx_htf is None and chop_htf is None:
        return None  # no HTF regime data → neutral

    trending = False

    if adx_htf is not None:
        try:
            trending |= float(adx_htf) >= adx_threshold
        except (TypeError, ValueError):
            pass

    if chop_htf is not None:
        try:
            trending |= float(chop_htf) < choppiness_threshold
        except (TypeError, ValueError):
            pass

    return bool(trending)
