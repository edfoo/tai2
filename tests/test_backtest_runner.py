"""Tests for backtest orchestration helpers (walk-forward splits)."""

from __future__ import annotations

from app.services.backtest.runner import walk_forward_splits


def test_no_split_when_single_fold() -> None:
    assert walk_forward_splits(start_ts=0, end_ts=1000, folds=1) == []


def test_invalid_range_returns_empty() -> None:
    assert walk_forward_splits(start_ts=1000, end_ts=0, folds=3) == []


def test_expanding_window_folds() -> None:
    splits = walk_forward_splits(start_ts=0, end_ts=1000, folds=4)
    # 4 folds → test span 250 each.
    assert len(splits) == 4
    # Each fold trains from the start (expanding window).
    for train_start, _train_end, _test_start, _test_end in splits:
        assert train_start == 0
    # Test windows partition the range.
    test_starts = [s[2] for s in splits]
    test_ends = [s[3] for s in splits]
    assert test_starts == [250, 500, 750, 1000]
    assert test_ends == [500, 750, 1000, 1000]


def test_train_ratio_scales_train_end() -> None:
    splits = walk_forward_splits(start_ts=0, end_ts=1000, folds=2, train_ratio=0.5)
    # fold 0: test_start=500, train_end=250
    assert splits[0] == (0, 250, 500, 1000)
    # fold 1: test_start=1000, train_end=500
    assert splits[1] == (0, 500, 1000, 1000)
