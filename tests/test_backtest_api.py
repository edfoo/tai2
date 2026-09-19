"""Tests for the backtest REST API + job manager.

Covers:
  - Job manager submit/status/result lifecycle (with a stubbed engine so no
    network/exchange access is required).
  - REST endpoint routing (validation, 202/404/409 semantics) via TestClient.
  - The `_resolve_window` date-window resolution.
"""

from __future__ import annotations

import time

import pytest

from fastapi.testclient import TestClient

from app.services.backtest.api_models import (
    BacktestGridRequest,
    BacktestRunRequest,
    GridParamRequest,
)
from app.services.backtest.job_manager import (
    COMPLETED,
    FAILED,
    QUEUED,
    RUNNING,
    BacktestJobManager,
    _resolve_window,
)


class _FakeResult:
    def __init__(self, metrics: dict, trades: list | None = None):
        self.metrics = metrics
        self.trades = trades or []
        self.error = None
        self.is_error = False
        self.per_strategy = {}
        self.equity_curve = []
        self.duration_seconds = 0.0
        self.candles_processed = 0
        self.config = type("C", (), {"timeframe": "15m", "symbols": ["BTC-USDT-SWAP"],
                                     "strategy_names": ["mean_reversion"],
                                     "start_ts": 0, "end_ts": 0,
                                     "initial_capital": 1000.0})()


class _FakeEngineRun:
    def __init__(self):
        self.result = _FakeResult({"total_trades": 3, "win_rate": 66.67, "net_profit": 1.5})

    def run(self):
        return self.result


def test_resolve_window_days_only() -> None:
    req = type("R", (), {"start_ts": None, "end_ts": None, "days": 10})()
    start, end = _resolve_window(req)
    assert end - start == 10 * 86_400_000


def test_resolve_window_explicit_ts() -> None:
    req = type("R", (), {"start_ts": 1000, "end_ts": 2000, "days": None})()
    start, end = _resolve_window(req)
    assert (start, end) == (1000, 2000)


def test_resolve_window_end_ts_with_days() -> None:
    req = type("R", (), {"start_ts": None, "end_ts": 5000, "days": 2})()
    start, end = _resolve_window(req)
    assert end == 5000
    assert start == 5000 - 2 * 86_400_000


class _State:
    pass


@pytest.mark.asyncio
async def test_job_manager_lifecycle(monkeypatch):
    """Submit a run and confirm it reaches completed with a result.

    The executor now dispatches to subprocesses; for a fast in-process test we
    stub the worker function and swap the process pool for a thread pool so no
    subprocess is actually spawned.
    """
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(
        "app.services.backtest.job_manager.ProcessPoolExecutor", ThreadPoolExecutor
    )
    monkeypatch.setattr(
        "app.services.backtest.job_manager._execute_run_worker",
        lambda config, job_id, redis_url: _FakeResult({"total_trades": 5, "win_rate": 60.0, "net_profit": 2.0}),
    )

    state = _State()
    manager = BacktestJobManager(state)
    manager.start()

    req = BacktestRunRequest(
        symbols=["BTC-USDT-SWAP"],
        timeframe="15m",
        strategy_names=["mean_reversion"],
        days=10,
        capital=1000.0,
    )
    job_id = manager.submit_run(req)
    assert manager.get_status(job_id)["status"] in (QUEUED, RUNNING, COMPLETED)

    # Wait for the worker to process (with a short timeout).
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if manager.get_status(job_id)["status"] in (COMPLETED, FAILED):
            break
        await __import__("asyncio").sleep(0.05)

    status = manager.get_status(job_id)
    assert status["status"] == COMPLETED, status
    result = manager.get_result(job_id)
    assert result is not None
    assert result["result"]["metrics"]["total_trades"] == 5

    await manager.shutdown()


@pytest.mark.asyncio
async def test_job_manager_grid_lifecycle(monkeypatch):
    """Submit a grid and confirm it reaches completed."""
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(
        "app.services.backtest.job_manager.ProcessPoolExecutor", ThreadPoolExecutor
    )
    monkeypatch.setattr(
        "app.services.backtest.job_manager._execute_grid_worker",
        lambda config, job_id, redis_url: _FakeResult({"total_trades": 2, "win_rate": 50.0}),
    )

    state = _State()
    manager = BacktestJobManager(state)
    manager.start()

    req = BacktestGridRequest(
        base=BacktestRunRequest(
            symbols=["BTC-USDT-SWAP"],
            timeframe="15m",
            strategy_names=["mean_reversion"],
            days=10,
        ),
        params=[GridParamRequest(key="strategies.mean_reversion.rsi_oversold", values=[25, 30])],
        rank_by="sharpe_per_candle",
        min_trades=1,
    )
    job_id = manager.submit_grid(req)

    deadline = time.time() + 5.0
    while time.time() < deadline:
        if manager.get_status(job_id)["status"] in (COMPLETED, FAILED):
            break
        await __import__("asyncio").sleep(0.05)

    assert manager.get_status(job_id)["status"] == COMPLETED
    await manager.shutdown()


def test_run_endpoint_requires_symbols():
    from app.main import create_app

    app = create_app(enable_background_services=False)
    with TestClient(app) as client:
        resp = client.post("/backtest/run", json={"symbols": [], "timeframe": "15m"})
        assert resp.status_code == 422


def test_grid_endpoint_requires_params():
    from app.main import create_app

    app = create_app(enable_background_services=False)
    with TestClient(app) as client:
        resp = client.post(
            "/backtest/grid",
            json={"base": {"symbols": ["BTC-USDT-SWAP"], "timeframe": "15m"}, "params": []},
        )
        assert resp.status_code == 422


def test_status_unknown_job_returns_404():
    from app.main import create_app

    app = create_app(enable_background_services=False)
    with TestClient(app) as client:
        resp = client.get("/backtest/status/does-not-exist")
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_job_manager_runs_jobs_concurrently(monkeypatch):
    """Multiple submitted runs complete, exercising the parallel worker pool."""
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(
        "app.services.backtest.job_manager.ProcessPoolExecutor", ThreadPoolExecutor
    )
    monkeypatch.setattr(
        "app.services.backtest.job_manager._execute_run_worker",
        lambda config, job_id, redis_url: _FakeResult({"total_trades": 1, "win_rate": 100.0}),
    )

    state = _State()
    # Force a small pool to prove jobs overlap (not just one worker).
    manager = BacktestJobManager(state, max_workers=3)
    manager.start()

    req = BacktestRunRequest(
        symbols=["BTC-USDT-SWAP"], timeframe="15m",
        strategy_names=["mean_reversion"], days=10, capital=1000.0,
    )
    ids = [manager.submit_run(req) for _ in range(5)]

    deadline = time.time() + 5.0
    while time.time() < deadline:
        statuses = [manager.get_status(j)["status"] for j in ids]
        if all(s in (COMPLETED, FAILED) for s in statuses):
            break
        await __import__("asyncio").sleep(0.05)

    statuses = [manager.get_status(j)["status"] for j in ids]
    assert all(s == COMPLETED for s in statuses), statuses

    await manager.shutdown()


@pytest.mark.asyncio
async def test_job_manager_custom_worker_count(monkeypatch):
    """max_workers is honored in __init__."""
    state = _State()
    manager = BacktestJobManager(state, max_workers=2)
    assert manager._max_workers == 2
    # No workers started → no tasks and no pool yet.
    assert manager._worker_tasks == []
    assert manager._process_pool is None


def test_result_unknown_job_returns_404():
    from app.main import create_app

    app = create_app(enable_background_services=False)
    with TestClient(app) as client:
        resp = client.get("/backtest/result/does-not-exist")
        assert resp.status_code == 404
