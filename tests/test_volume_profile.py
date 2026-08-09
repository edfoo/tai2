"""Tests for the volume-profile / value-area helpers.

Covers:
  - ``poc`` returns a price within the observed range.
  - ``value_area`` returns (poc, va_high, va_low) with sane bounds.
  - Value area encloses an increasing volume fraction as ``va_pct`` rises.
  - Degenerate windows (single price / empty series) behave gracefully.
"""

from __future__ import annotations

import pandas as pd
import pytest

from app.lib.volume_profile import poc, value_area


def _series(values: list[float]) -> pd.Series:
    return pd.Series(values, dtype=float)


def test_poc_within_range() -> None:
    price = _series([100, 101, 102, 101, 100, 102, 99, 100.5])
    volume = _series([1, 1, 1, 1, 1, 1, 1, 1])
    out = poc(price, volume, bins=20)
    assert 99.0 <= out <= 102.0


def test_poc_prefers_high_volume_zone() -> None:
    # Heavy volume clustered around a narrow zone should pull the POC there.
    # A few outlier prices are included to ensure the histogram spans a range
    # without a single bin being empty at the cluster location.
    price = _series([98, 99, 101, 101.5, 102, 101.8, 100.9, 100.2, 101.1, 101.4])
    volume = _series([1, 1, 10, 12, 9, 11, 8, 7, 10, 9])
    out = poc(price, volume, bins=10)
    assert 100.0 <= out <= 103.0


def test_value_area_bounds_and_width() -> None:
    price = _series([100, 101, 102, 101, 100, 102, 99, 100.5, 101.5, 100.2])
    volume = _series([1, 2, 1, 3, 2, 1, 1, 2, 1, 2])
    poc_price, va_high, va_low = value_area(price, volume, bins=20, va_pct=0.70)
    assert va_high is not None and va_low is not None
    assert va_high >= va_low
    assert poc_price is not None
    # POC must sit within the value area.
    assert va_low <= poc_price <= va_high
    # Value area bounds stay within the observed price range.
    assert va_low >= price.min() - 1e-6
    assert va_high <= price.max() + 1e-6
    # Larger va_pct encloses a wider area.
    _, va_high_90, va_low_90 = value_area(price, volume, bins=20, va_pct=0.90)
    assert (va_high_90 - va_low_90) >= (va_high - va_low) - 1e-9


def test_value_area_degenerate_single_price() -> None:
    price = _series([100.0, 100.0, 100.0])
    volume = _series([1.0, 1.0, 1.0])
    poc_price, va_high, va_low = value_area(price, volume, bins=20)
    assert va_low <= 100.0 <= va_high


def test_value_area_empty_series_raises() -> None:
    with pytest.raises(ValueError):
        poc(_series([]), _series([1.0]))
    with pytest.raises(ValueError):
        value_area(_series([]), _series([1.0]))