"""Tests for the parallelised parameter-sweep grid runner."""

from __future__ import annotations

import pytest

from app.services.backtest import grid as G
from app.services.backtest.models import (
    BacktestConfig,
    BacktestResult,
    GridConfig,
    GridParamDef,
)


def _base_config() -> BacktestConfig:
    return BacktestConfig(
        symbols=["BTC-USDT-SWAP"],
        timeframe="15m",
        start_ts=1_000,
        end_ts=2_000,
        strategy_names=["mean_reversion"],
        launcher_config={"strategies": {"mean_reversion": {"enabled": True, "rsi_oversold": 30.0}}},
    )


def test_apply_params_sets_dotted_path() -> None:
    cfg = _base_config()
    G._apply_params(cfg, {"strategies.mean_reversion.rsi_oversold": 25})
    assert cfg.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"] == 25


def test_set_nested_creates_intermediate_dicts() -> None:
    d: dict = {}
    G._set_nested(d, "strategies.mean_reversion.rsi_oversold", 25)
    assert d["strategies"]["mean_reversion"]["rsi_oversold"] == 25


def test_combination_at_index_matches_cartesian_order() -> None:
    values = [["a", "b"], [1, 2, 3]]
    assert [G._combination_at_index(values, i) for i in range(6)] == list(
        __import__("itertools").product(*values)
    )


def test_random_search_obeys_budget_and_seed() -> None:
    values = [list(range(10)), list(range(8))]
    first, total = G._build_combinations(
        values, search_mode="random", budget=7, seed=42
    )
    second, second_total = G._build_combinations(
        values, search_mode="random", budget=7, seed=42
    )

    assert total == second_total == 80
    assert len(first) == 7
    assert first == second
    assert len(set(first)) == 7


def test_default_workers_positive(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_WORKERS", raising=False)
    assert G._default_workers() >= 1
    monkeypatch.setenv("BACKTEST_WORKERS", "3")
    assert G._default_workers() == 3
    monkeypatch.setenv("BACKTEST_WORKERS", "bogus")
    assert G._default_workers() >= 1


@pytest.mark.asyncio
async def test_grid_runs_combinations_in_parallel(monkeypatch) -> None:
    """The grid dispatches every combination and returns ranked results.

    The process pool is swapped for a thread pool so the test stays in-process
    and fast, and the per-combination worker is stubbed to return a synthetic
    result keyed off the swept parameter.
    """
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 3,
            "sharpe_per_candle": float(rsi),
            "profit_factor": 1.5,
        }
        result.per_strategy = {}
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)

    cfg = GridConfig(
        base_config=_base_config(),
        params=[GridParamDef(key="strategies.mean_reversion.rsi_oversold", values=[25, 30, 35])],
        rank_by="sharpe_per_candle",
        min_trades=1,
    )
    grid = G.BacktestGrid(cfg, workers=2)
    result = await grid.run()

    assert result.is_error is False
    assert len(result.runs) == 3
    # Ranked by sharpe desc → 35, 30, 25.
    rankings = [r.params["strategies.mean_reversion.rsi_oversold"] for r in result.ranked]
    assert rankings == [35, 30, 25], rankings


@pytest.mark.asyncio
async def test_grid_validation_ranks_mean_oos_score_and_preserves_folds(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 2,
            "net_profit_pct": (
                90.0 if rsi == 20 else 0.0
            ) if config.start_ts == 500 else (
                0.0 if rsi == 20 else 80.0
            ),
        }
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    config = _base_config()
    config.start_ts = 0
    config.end_ts = 1000
    sweep = GridConfig(
        base_config=config,
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [20, 30])],
        rank_by="net_profit_pct",
        min_trades=1,
        validation_folds=2,
        validation_train_ratio=0.5,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    assert result.is_error is False
    assert result.total_combinations == result.attempted_combinations == 2
    assert len(result.ranked) == 2
    assert result.ranked[0].params["strategies.mean_reversion.rsi_oversold"] == 20
    assert result.ranked[0].rank_score == pytest.approx(45.0)
    assert len(result.ranked[0].fold_metrics) == 2


@pytest.mark.asyncio
async def test_grid_keeps_failed_validation_fold_in_result(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        if config.start_ts == 750:
            raise RuntimeError("synthetic validation failure")
        result = BacktestResult(config=config)
        result.metrics = {"total_trades": 2, "net_profit_pct": 1.0}
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    config = _base_config()
    config.start_ts = 0
    config.end_ts = 1000
    sweep = GridConfig(
        base_config=config,
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [30])],
        rank_by="net_profit_pct",
        min_trades=1,
        validation_folds=2,
        validation_train_ratio=0.5,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    assert result.is_error is False
    candidate = result.runs[0]
    assert [fold["status"] for fold in candidate.fold_metrics] == ["completed", "failed"]
    assert candidate.fold_metrics[1]["error"] == "synthetic validation failure"
    assert candidate.rank_score is None
    assert candidate.below_min_trades is True


@pytest.mark.asyncio
async def test_grid_evaluates_selected_candidate_once_on_untouched_holdout(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)
    evaluated: list[tuple[float, int, int]] = []

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        evaluated.append((rsi, config.start_ts, config.end_ts))
        is_holdout = config.start_ts == 800
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 3,
            "net_profit_after_cost_pct": (
                100.0 if rsi == 20 else -10.0
            ) if is_holdout else (10.0 if rsi == 30 else 5.0),
        }
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    config = _base_config()
    config.start_ts = 0
    config.end_ts = 1000
    sweep = GridConfig(
        base_config=config,
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [20, 30])],
        rank_by="net_profit_after_cost_pct",
        min_trades=1,
        validation_folds=2,
        validation_train_ratio=0.5,
        final_holdout_fraction=0.2,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    assert result.is_error is False
    assert result.ranked[0].params["strategies.mean_reversion.rsi_oversold"] == 30
    assert result.final_holdout is not None
    assert result.final_holdout.params["strategies.mean_reversion.rsi_oversold"] == 30
    assert result.final_holdout.result is not None
    assert result.final_holdout.result.metrics["net_profit_after_cost_pct"] == -10.0
    assert result.final_holdout.fold_metrics[0]["role"] == "final_holdout"
    assert result.final_holdout.fold_metrics[0]["start_ts"] == 800
    assert sum(start == 800 for _rsi, start, _end in evaluated) == 1
    assert all(end <= 800 for _rsi, _start, end in evaluated if end != 1000)
    assert (30, 400, 599) in evaluated
    assert (30, 600, 799) in evaluated
    assert (30, 800, 1000) in evaluated


@pytest.mark.asyncio
async def test_grid_requires_validation_folds_when_holdout_is_enabled() -> None:
    config = _base_config()
    sweep = GridConfig(
        base_config=config,
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [30])],
        final_holdout_fraction=0.2,
    )

    result = await G.BacktestGrid(sweep, workers=1).run()

    assert result.is_error is True
    assert "requires at least one validation fold" in result.error


@pytest.mark.asyncio
async def test_grid_marks_holdout_skipped_when_no_candidate_meets_minimum(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        result = BacktestResult(config=config)
        result.metrics = {"total_trades": 0, "net_profit_after_cost_pct": 0.0}
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    config = _base_config()
    config.start_ts = 0
    config.end_ts = 1000
    sweep = GridConfig(
        base_config=config,
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [30])],
        min_trades=5,
        validation_folds=1,
        final_holdout_fraction=0.2,
    )

    result = await G.BacktestGrid(sweep, workers=1).run()

    assert result.final_holdout is not None
    assert result.final_holdout.fold_metrics[0]["status"] == "skipped"
    assert "No complete validation candidate" in result.final_holdout.fold_metrics[0]["error"]