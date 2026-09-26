"""Tests for backtest orchestration helpers (walk-forward splits)."""

from __future__ import annotations

from app.services.backtest.runner import build_backtest_config, walk_forward_splits


def test_single_validation_fold_uses_the_reserved_tail() -> None:
    assert walk_forward_splits(start_ts=0, end_ts=1000, folds=1) == [
        (0, 700, 700, 1000)
    ]


def test_invalid_range_returns_empty() -> None:
    assert walk_forward_splits(start_ts=1000, end_ts=0, folds=3) == []


def test_expanding_window_folds() -> None:
    splits = walk_forward_splits(start_ts=0, end_ts=1000, folds=4)
    assert len(splits) == 4
    for train_start, train_end, test_start, test_end in splits:
        assert train_start == 0
        assert train_end == test_start
        assert test_end > test_start
    test_starts = [split[2] for split in splits]
    test_ends = [split[3] for split in splits]
    assert test_starts == [700, 775, 850, 925]
    assert test_ends == [775, 850, 925, 1000]


def test_train_ratio_scales_train_end() -> None:
    splits = walk_forward_splits(start_ts=0, end_ts=1000, folds=2, train_ratio=0.5)
    assert splits[0] == (0, 500, 500, 750)
    assert splits[1] == (0, 750, 750, 1000)


def test_validation_folds_reject_invalid_ratio_or_empty_windows() -> None:
    assert walk_forward_splits(start_ts=0, end_ts=1000, folds=2, train_ratio=1.0) == []
    assert walk_forward_splits(start_ts=0, end_ts=1000, folds=20, train_ratio=0.99) == []


def test_config_builder_preserves_p2_execution_settings() -> None:
    config = build_backtest_config(
        symbols=["BTC-USDT-SWAP"],
        timeframe="15m",
        strategy_names=["mean_reversion"],
        start_ts=0,
        end_ts=1000,
        funding_mode="constant",
        funding_rate_pct=0.01,
        slippage_mode="fixed",
        slippage_bps=4.0,
        allow_concurrent_strategies_per_symbol=True,
    )

    assert config.funding_mode == "constant"
    assert config.funding_rate_pct == 0.01
    assert config.slippage_mode == "fixed"
    assert config.slippage_bps == 4.0
    assert config.allow_concurrent_strategies_per_symbol is True
