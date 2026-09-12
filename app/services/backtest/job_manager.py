"""In-process backtest job manager.

A small registry that runs backtest jobs (single engine runs and grid sweeps)
as detached asyncio tasks and exposes their status/results by ``job_id``.

The engine is CPU-bound (recomputes pandas-ta indicators per step), so each
job's run is delegated to ``asyncio.to_thread``.  Only one job executes at a
time (a simple FIFO queue) to avoid oversubscribing the single process —
grid sweeps can still be large, but we keep the concurrency model simple and
deterministic.  This mirrors how the NiceGUI BACKTEST page already runs
backtests detached from the client lifecycle.

Jobs are stored in-memory on ``app.state``; completed results are additionally
persisted to disk via :func:`save_result` so they survive restarts and appear
in the UI's Saved Runs browser (same ``backtest_cache/cli/`` tree).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from app.services.backtest.engine import BacktestEngine, available_strategy_names
from app.services.backtest.grid import BacktestGrid
from app.services.backtest.models import BacktestConfig, GridConfig, GridParamDef
from app.services.backtest.persistence import make_run_id, save_result
from app.services.backtest.runner import build_backtest_config

logger = logging.getLogger(__name__)

# Job statuses.
QUEUED = "queued"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def _resolve_window(req: Any) -> tuple[int, int]:
    """Resolve (start_ts, end_ts) in ms from a request, trailing ``days`` last."""
    import time as _time

    now_ms = int(_time.time() * 1000)
    if req.start_ts and req.end_ts:
        return int(req.start_ts), int(req.end_ts)
    if req.end_ts:
        end = int(req.end_ts)
        days = int(req.days or 30)
        return end - days * 86_400_000, end
    days = int(req.days or 30)
    return now_ms - days * 86_400_000, now_ms


class BacktestJobManager:
    """Registry + executor for backtest jobs (single-run and grid)."""

    def __init__(self, app_state: Any) -> None:
        self._state = app_state
        self._jobs: dict[str, dict[str, Any]] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background worker (idempotent)."""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker())

    async def shutdown(self) -> None:
        """Cancel the worker task (called on app shutdown)."""
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None

    def submit_run(self, req: Any) -> str:
        """Queue a single engine run.  Returns the job_id."""
        job_id = uuid.uuid4().hex
        start_ts, end_ts = _resolve_window(req)
        strategy_names = list(req.strategy_names) if req.strategy_names else available_strategy_names()
        config = build_backtest_config(
            symbols=[s.upper() for s in req.symbols],
            timeframe=req.timeframe,
            strategy_names=strategy_names,
            start_ts=start_ts,
            end_ts=end_ts,
            capital=req.capital,
            warmup=req.warmup,
            evaluation_mode=req.evaluation_mode,
            evaluation_timeframe=req.evaluation_timeframe,
            launcher_config=req.launcher_config,
            strategy_config=req.strategy_config,
            guardrails_config=req.guardrails_config,
        )
        self._jobs[job_id] = {
            "job_id": job_id,
            "kind": "run",
            "status": QUEUED,
            "config": config,
            "request": req,
            "result": None,
            "run_id": None,
            "error": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
        self._queue.put_nowait(job_id)
        return job_id

    def submit_grid(self, req: Any) -> str:
        """Queue a parameter sweep.  Returns the job_id."""
        job_id = uuid.uuid4().hex
        base_req = req.base
        start_ts, end_ts = _resolve_window(base_req)
        strategy_names = list(base_req.strategy_names) if base_req.strategy_names else available_strategy_names()
        base_config = build_backtest_config(
            symbols=[s.upper() for s in base_req.symbols],
            timeframe=base_req.timeframe,
            strategy_names=strategy_names,
            start_ts=start_ts,
            end_ts=end_ts,
            capital=base_req.capital,
            warmup=base_req.warmup,
            evaluation_mode=base_req.evaluation_mode,
            evaluation_timeframe=base_req.evaluation_timeframe,
            launcher_config=base_req.launcher_config,
            strategy_config=base_req.strategy_config,
            guardrails_config=base_req.guardrails_config,
        )
        params = [
            GridParamDef(key=p.key, values=list(p.values), label=p.label)
            for p in req.params
        ]
        grid_config = GridConfig(
            base_config=base_config,
            params=params,
            rank_by=req.rank_by,
            min_trades=req.min_trades,
        )
        self._jobs[job_id] = {
            "job_id": job_id,
            "kind": "grid",
            "status": QUEUED,
            "config": grid_config,
            "request": req,
            "result": None,
            "run_id": None,
            "error": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
        self._queue.put_nowait(job_id)
        return job_id

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        """Return a job's public status dict, or None if unknown."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        return {
            "job_id": job["job_id"],
            "kind": job["kind"],
            "status": job["status"],
            "created_at": job["created_at"],
            "finished_at": job["finished_at"],
            "run_id": job["run_id"],
            "error": job["error"],
        }

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        """Return the serialised result payload, or None if not completed."""
        job = self._jobs.get(job_id)
        if job is None:
            return None
        if job["status"] != COMPLETED:
            return None
        from app.services.backtest.persistence import result_to_dict

        result = job["result"]
        payload = result_to_dict(result) if result is not None else None
        return {
            "job_id": job_id,
            "status": job["status"],
            "run_id": job["run_id"],
            "result": payload,
        }

    # ── Worker ────────────────────────────────────────────────────────

    async def _worker(self) -> None:
        """Process jobs FIFO, one at a time."""
        while True:
            job_id = await self._queue.get()
            try:
                await self._run_job(job_id)
            except Exception as exc:  # noqa: BLE001 - defensive
                logger.exception("Backtest job %s crashed", job_id)
                job = self._jobs.get(job_id)
                if job is not None:
                    job["status"] = FAILED
                    job["error"] = str(exc)
                    job["finished_at"] = datetime.now(timezone.utc).isoformat()
            finally:
                self._queue.task_done()

    async def _run_job(self, job_id: str) -> None:
        job = self._jobs[job_id]
        job["status"] = RUNNING
        try:
            if job["kind"] == "run":
                result = await asyncio.to_thread(self._execute_run, job["config"])
            else:
                result = await asyncio.to_thread(self._execute_grid, job["config"])
            job["result"] = result
            job["status"] = COMPLETED
            if result is not None and not getattr(result, "is_error", False):
                self._persist(result, job)
        except Exception as exc:  # noqa: BLE001
            job["status"] = FAILED
            job["error"] = str(exc)
            logger.exception("Backtest job %s failed", job_id)
        finally:
            job["finished_at"] = datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _execute_run(config: BacktestConfig) -> Any:
        engine = BacktestEngine(config)
        return asyncio.run(engine.run())

    @staticmethod
    def _execute_grid(config: GridConfig) -> Any:
        grid = BacktestGrid(config)
        return asyncio.run(grid.run())

    def _persist(self, result: Any, job: dict[str, Any]) -> None:
        """Best-effort persist the result to disk (survives restarts)."""
        try:
            ltf = str(getattr(getattr(result, "config", None), "timeframe", "") or "run")
            run_id = make_run_id(ltf)
            save_result(result, run_id=run_id)
            job["run_id"] = run_id
        except Exception as exc:  # noqa: BLE001 - persistence is best-effort
            logger.warning("Failed to persist backtest job result: %s", exc)
