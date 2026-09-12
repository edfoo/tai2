"""In-process backtest job manager.

A registry that runs backtest jobs (single engine runs and grid sweeps) as
detached asyncio tasks and exposes their status/results by ``job_id``.

The engine is CPU-bound (recomputes pandas-ta indicators per step), so each
job's run is delegated to ``asyncio.to_thread``.  Jobs are executed
**concurrently** across a pool of ``max_workers`` worker tasks (a FIFO queue
feeds them); the default worker count is ``os.cpu_count()``, overridable via
the ``BACKTEST_WORKERS`` environment variable.  The engine releases the GIL
during indicator computation and its per-step ``time.sleep(0)`` yields, so
concurrent jobs overlap their fetch (I/O) and indicator phases.

Jobs are stored in-memory on ``app.state``; completed results are additionally
persisted to disk via :func:`save_result` so they survive restarts and appear
in the UI's Saved Runs browser (same ``backtest_cache/cli/`` tree).
"""

from __future__ import annotations

import asyncio
import logging
import os
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

# Default concurrent workers: one per CPU core (bounded to a sane floor).
# Override with the BACKTEST_WORKERS environment variable.
_DEFAULT_WORKERS = max(1, os.cpu_count() or 1)


def _default_workers() -> int:
    raw = os.environ.get("BACKTEST_WORKERS", "")
    if raw.strip():
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning("Invalid BACKTEST_WORKERS=%r; using %d", raw, _DEFAULT_WORKERS)
    return _DEFAULT_WORKERS


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

    def __init__(self, app_state: Any, *, max_workers: int | None = None) -> None:
        self._state = app_state
        self._jobs: dict[str, dict[str, Any]] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._max_workers = max_workers if max_workers and max_workers > 0 else _default_workers()
        self._worker_tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background worker pool (idempotent)."""
        if not self._worker_tasks:
            self._worker_tasks = [
                asyncio.create_task(self._worker()) for _ in range(self._max_workers)
            ]

    async def shutdown(self) -> None:
        """Cancel the worker tasks (called on app shutdown)."""
        tasks = self._worker_tasks
        self._worker_tasks = []
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
        """Best-effort persist the result to disk (survives restarts).

        The run_id embeds the job_id so concurrent jobs (which may finish in
        the same wall-clock second) never collide on a shared ``make_run_id``
        timestamp.  ``save_result`` is called on the event loop (no ``await``
        inside), so its comparison.csv read-modify-write is safe across the
        concurrent worker tasks.
        """
        try:
            ltf = str(getattr(getattr(result, "config", None), "timeframe", "") or "run")
            run_id = f"{make_run_id(ltf)}_{job['job_id'][:8]}"
            save_result(result, run_id=run_id)
            job["run_id"] = run_id
        except Exception as exc:  # noqa: BLE001 - persistence is best-effort
            logger.warning("Failed to persist backtest job result: %s", exc)
