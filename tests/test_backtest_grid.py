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