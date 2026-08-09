"""Tests for okx_metrics pure math helpers (zscore_latest, oi_delta_zscore).

These functions are pure (no I/O) and must be deterministic so they are
straightforward to unit-test without network stubs.
"""

from __future__ import annotations

import math

import pytest

from app.services.okx_metrics import oi_delta_zscore, zscore_latest


class TestZscoreLatest:
    def test_single_element_returns_none(self) -> None:
        assert zscore_latest([0.001]) is None

    def test_empty_returns_none(self) -> None:
        assert zscore_latest([]) is None

    def test_all_identical_returns_zero(self) -> None:
        # All values identical → std=0 → z=0 by convention.
        assert zscore_latest([0.001, 0.001, 0.001]) == 0.0

    def test_last_value_above_mean_positive_z(self) -> None:
        # Mean = 0, last = 3.0 → z > 0
        series = [0.0, 0.0, 0.0, 3.0]
        z = zscore_latest(series)
        assert z is not None and z > 0

    def test_last_value_below_mean_negative_z(self) -> None:
        series = [3.0, 3.0, 3.0, 0.0]
        z = zscore_latest(series)
        assert z is not None and z < 0

    def test_z_score_symmetric(self) -> None:
        # z(last = mean + k*sigma) == k
        series = [0.0, 0.0, 0.0, 0.0, 3.0]
        z_high = zscore_latest(series)
        series_low = [3.0, 3.0, 3.0, 3.0, 0.0]
        z_low = zscore_latest(series_low)
        assert z_high is not None and z_low is not None
        assert math.isclose(z_high, -z_low, rel_tol=1e-9)

    def test_result_finite(self) -> None:
        series = list(range(50))
        z = zscore_latest(series)
        assert z is not None and math.isfinite(z)

    def test_known_value(self) -> None:
        # Series = [0, 0, 0, 0, 1]; mean=0.2, pstd ≈ 0.4
        series = [0.0, 0.0, 0.0, 0.0, 1.0]
        z = zscore_latest(series)
        assert z is not None
        mean_ = 0.2
        std_ = (4 * 0.04 + 0.64) ** 0.5 / 5 ** 0.5  # population std
        expected = (1.0 - mean_) / std_
        assert math.isclose(z, expected, rel_tol=1e-6)

    def test_filters_none_values(self) -> None:
        series = [None, 1.0, 2.0, 3.0, 4.0]  # type: ignore[list-item]
        z = zscore_latest(series)
        assert z is not None
        # Same as [1,2,3,4]
        expected = zscore_latest([1.0, 2.0, 3.0, 4.0])
        assert math.isclose(z, expected, rel_tol=1e-9)


class TestOIDeltaZscore:
    def test_fewer_than_three_returns_none(self) -> None:
        assert oi_delta_zscore([]) is None
        assert oi_delta_zscore([100.0]) is None
        assert oi_delta_zscore([100.0, 110.0]) is None

    def test_flat_oi_returns_zero(self) -> None:
        # All identical → all deltas 0 → std=0 → z=0.
        z = oi_delta_zscore([100.0] * 10)
        assert z == 0.0

    def test_rising_oi_spike_positive_z(self) -> None:
        # Last delta much larger than history.
        series = [100.0, 101.0, 102.0, 103.0, 104.0, 110.0]
        z = oi_delta_zscore(series)
        assert z is not None and z > 0

    def test_falling_oi_spike_negative_z(self) -> None:
        series = [110.0, 109.0, 108.0, 107.0, 106.0, 100.0]
        z = oi_delta_zscore(series)
        assert z is not None and z < 0

    def test_result_finite(self) -> None:
        import random
        random.seed(42)
        series = [1000.0 + random.gauss(0, 10) for _ in range(50)]
        z = oi_delta_zscore(series)
        assert z is not None and math.isfinite(z)

    def test_minimum_three_elements(self) -> None:
        # 3 elements → 2 deltas; can compute z from 2 values.
        z = oi_delta_zscore([100.0, 105.0, 120.0])
        assert z is not None
