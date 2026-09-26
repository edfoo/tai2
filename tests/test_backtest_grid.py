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


def test_universe_fields_survive_grid_combination_build() -> None:
    """Grid combinations deep-copy the base config, so screener-universe
    settings must propagate to every combination (and the final holdout)."""
    import copy

    base = _base_config()
    base.universe_mode = "screener"
    base.screener_config = {"enabled": True, "dual_universe": True, "max_symbols": 10}
    base.universe_candidate_symbols = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]

    combo = copy.deepcopy(base)
    G._apply_params(combo, {"strategies.mean_reversion.rsi_oversold": 25})

    assert combo.universe_mode == "screener"
    assert combo.screener_config["max_symbols"] == 10
    assert combo.universe_candidate_symbols == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    # The swept param still applied without clobbering universe fields.
    assert combo.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"] == 25


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


# ── Phase 1: ranking integrity ──────────────────────────────────────────


def test_extract_metric_scoped_reports_scope() -> None:
    """Aggregate metric → 'aggregate'; per-strategy fallback → named scope."""
    result = BacktestResult(config=_base_config())
    result.metrics = {"sharpe_per_candle": 1.5}
    result.per_strategy = {"mean_reversion": {"sharpe_per_candle": 2.5}}

    value, scope = G._extract_metric_scoped(result, "sharpe_per_candle")
    assert value == 1.5
    assert scope == "aggregate"

    # Missing aggregate → per-strategy fallback, scope recorded.
    value, scope = G._extract_metric_scoped(result, "net_profit_after_cost_pct")
    assert value is None
    assert scope == "aggregate"

    result.metrics = {}
    value, scope = G._extract_metric_scoped(result, "sharpe_per_candle")
    assert value == 2.5
    assert scope == "per_strategy:mean_reversion"


def test_below_min_t_stat_gate() -> None:
    assert G._below_min_t_stat({"net_expectancy_t_stat": 2.5}, 0.0) is False
    assert G._below_min_t_stat({"net_expectancy_t_stat": 2.5}, 2.0) is False
    assert G._below_min_t_stat({"net_expectancy_t_stat": 1.0}, 2.0) is True
    # Enabled gate with no measurable t-stat → flagged.
    assert G._below_min_t_stat({}, 2.0) is True
    assert G._below_min_t_stat(None, 2.0) is True


def test_robust_score_prefers_significant_consistent_returns() -> None:
    strong = G._robust_score(
        {
            "net_profit_after_cost_pct": 20.0,
            "net_expectancy_t_stat": 3.0,
            "max_drawdown_pct": 5.0,
        },
        [{"status": "completed", "metrics": {"net_profit_after_cost": 10.0}}] * 4,
    )
    weak = G._robust_score(
        {
            "net_profit_after_cost_pct": 20.0,
            "net_expectancy_t_stat": 0.2,
            "max_drawdown_pct": 25.0,
        },
        [{"status": "completed", "metrics": {"net_profit_after_cost": -1.0}}] * 4,
    )
    assert 0.0 <= weak < strong <= 1.0


@pytest.mark.asyncio
async def test_grid_records_rank_scope_and_t_stat_gate(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 10,
            "net_profit_after_cost_pct": float(rsi),
            "net_expectancy_t_stat": 0.5 if rsi == 25 else 3.0,
        }
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    sweep = GridConfig(
        base_config=_base_config(),
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [25, 30])],
        rank_by="net_profit_after_cost_pct",
        min_trades=1,
        min_expectancy_t_stat=2.0,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    by_rsi = {r.params["strategies.mean_reversion.rsi_oversold"]: r for r in result.runs}
    assert by_rsi[25].below_min_t_stat is True
    assert by_rsi[30].below_min_t_stat is False
    assert all(r.rank_scope == "aggregate" for r in result.runs)
    # The low-t-stat run is excluded from the ranked head.
    assert result.ranked[0].params["strategies.mean_reversion.rsi_oversold"] == 30


@pytest.mark.asyncio
async def test_grid_robust_score_ranking(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 10,
            "net_profit_after_cost_pct": 30.0 if rsi == 25 else 10.0,
            "net_expectancy_t_stat": 0.1 if rsi == 25 else 3.0,
            "max_drawdown_pct": 5.0,
        }
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    sweep = GridConfig(
        base_config=_base_config(),
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [25, 30])],
        rank_by="robust_score",
        min_trades=1,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    # rsi=25 has higher raw return but no significance → robust_score prefers 30.
    assert result.ranked[0].params["strategies.mean_reversion.rsi_oversold"] == 30
    assert all(0.0 <= r.rank_score <= 1.0 for r in result.runs if r.rank_score is not None)


# ── Phase 2: per-run evidence ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_grid_builds_evidence_and_retains_top_n_detail(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(G, "ProcessPoolExecutor", ThreadPoolExecutor)

    def _fake_combination(config: BacktestConfig) -> BacktestResult:
        rsi = config.launcher_config["strategies"]["mean_reversion"]["rsi_oversold"]
        result = BacktestResult(config=config)
        result.metrics = {
            "total_trades": 10,
            "net_profit_after_cost_pct": float(rsi),
            "net_expectancy_t_stat": 2.0,
            "buy_and_hold": {"total_return_pct": 5.0, "symbols": ["BTC-USDT-SWAP"]},
        }
        result.per_symbol = {"BTC-USDT-SWAP": {"net_profit_after_cost": float(rsi), "trades": 10}}
        result.trades = []
        result.equity_curve = []
        return result

    monkeypatch.setattr(G, "_run_combination", _fake_combination)
    sweep = GridConfig(
        base_config=_base_config(),
        params=[GridParamDef("strategies.mean_reversion.rsi_oversold", [25, 30, 35])],
        rank_by="net_profit_after_cost_pct",
        min_trades=1,
        top_n_detail=1,
    )

    result = await G.BacktestGrid(sweep, workers=2).run()

    # Every run carries an evidence block.
    assert all(run.evidence for run in result.runs)
    assert result.runs[0].evidence["summary"]["total_trades"] == 10
    assert result.runs[0].evidence["benchmark_delta"]["beat_benchmark"] is True
    # Only the top-ranked run retains full detail.
    retained = [r for r in result.runs if r.detail_retained]
    assert len(retained) == 1
    assert retained[0] is result.ranked[0]