"""In-process backtest job manager.

A registry that runs backtest jobs (single engine runs and grid sweeps) and
exposes their status/results by ``job_id``.

The engine is CPU-bound (recomputes pandas-ta indicators per step) AND does a
significant amount of pure-Python strategy evaluation that holds the GIL, so
thread-level parallelism measured **no speedup** and even starved the server's
event loop (``status poll failed: timed out``).  Jobs are therefore dispatched
to a :class:`concurrent.futures.ProcessPoolExecutor` — each job runs in its own
process, giving true multi-core parallelism.  Only the picklable
``BacktestConfig`` / ``GridConfig`` is sent across the process boundary; the
engine and strategy instances are constructed *inside* the worker so no
lambdas/bound methods are pickled.

Jobs are stored in-memory on ``app.state``; completed results are additionally
persisted to disk via :func:`save_result` so they survive restarts and appear
in the UI's Saved Runs browser (same ``backtest_cache/cli/`` tree).

The process pool size defaults to ``os.cpu_count()``, overridable via the
``BACKTEST_WORKERS`` environment variable.
"""

from __future__ import annotations

import asyncio
import ctypes
import logging
import os
import signal
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Any

from app.core.config import get_settings
from app.services.backtest.engine import BacktestEngine, available_strategy_names
from app.services.backtest.grid import BacktestGrid
from app.services.backtest.models import BacktestConfig, GridConfig, GridParamDef
from app.services.backtest.persistence import make_run_id, save_result
from app.services.backtest.progress import ProgressSink, make_progress_cb
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


def _worker_initializer() -> None:
    """Run in each freshly-forked worker to die with the parent process.

    ``prctl(PR_SET_PDEATHSIG, SIGTERM)`` makes the kernel deliver SIGTERM to the
    worker the moment its parent (the uvicorn server) dies — even if the parent
    is SIGKILLed.  Without this, a hard-killed server strands its forked workers
    as orphans (reparented to PID 1) that keep burning CPU on stale jobs.  The
    follow-up ``getppid()`` guard covers the (Linux-only) race where prctl is set
    right as the parent already died.
    """
    parent_pid = os.getppid()
    try:
        _libc = ctypes.CDLL("libc.so.6", use_errno=True)
        # PR_SET_PDEATHSIG = 1
        _libc.prctl(1, signal.SIGTERM)
    except (AttributeError, OSError):
        # Non-Linux or libc unavailable — degrade to the getppid check only.
        pass
    if os.getppid() != parent_pid:
        # Parent already gone between fork and this init — exit immediately.
        os._exit(0)


def _execute_run_worker(
    config: BacktestConfig,
    job_id: str,
    redis_url: str | None,
) -> Any:
    """Run a single backtest **in a subprocess** and return its result.

    Module-level (not a bound method) so it pickles by qualified name for the
    ``ProcessPoolExecutor``.  The engine is constructed here, inside the worker,
    so no asyncio event loop or bound-method state crosses the process boundary.

    Progress is published through a :class:`ProgressSink` constructed *inside*
    the worker from ``redis_url`` (never pickling a live client across the fork
    boundary) and keyed by ``job_id``.
    """
    sink = ProgressSink(redis_url=redis_url)
    progress_cb = make_progress_cb(sink, job_id)
    return asyncio.run(BacktestEngine(config).run(progress_cb=progress_cb))


def _execute_grid_worker(
    config: GridConfig,
    job_id: str,
    redis_url: str | None,
) -> Any:
    """Run a parameter sweep **in a subprocess** and return its result.

    The grid's own nested combination pool does not emit progress; the
    ``GridProgress`` "Run X/Y" markers come from the grid's ``as_completed``
    loop, which runs in *this* worker process, so a single sink keyed by
    ``job_id`` captures the whole sweep.
    """
    sink = ProgressSink(redis_url=redis_url)
    progress_cb = make_progress_cb(sink, job_id)
    return asyncio.run(BacktestGrid(config).run(progress_cb=progress_cb))


class BacktestJobManager:
    """Registry + executor for backtest jobs (single-run and grid)."""

    def __init__(self, app_state: Any, *, max_workers: int | None = None) -> None:
        self._state = app_state
        self._jobs: dict[str, dict[str, Any]] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._max_workers = max_workers if max_workers and max_workers > 0 else _default_workers()
        self._worker_tasks: list[asyncio.Task] = []
        self._process_pool: ProcessPoolExecutor | None = None
        self._lock = asyncio.Lock()
        # Progress publishing.  The manager keeps its own sink for reading /
        # clearing; workers rebuild sinks from ``_redis_url`` inside the fork.
        self._redis_url = get_settings().redis_url
        self._progress_sink = ProgressSink(redis_url=self._redis_url)

    # ── Public API ────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the process pool + queued worker coroutines (idempotent)."""
        if self._process_pool is None:
            self._process_pool = ProcessPoolExecutor(
                max_workers=self._max_workers,
                initializer=_worker_initializer,
            )
            logger.info(
                "Backtest job pool started: %d worker process(es)",
                self._max_workers,
            )
        if not self._worker_tasks:
            self._worker_tasks = [
                asyncio.create_task(self._worker()) for _ in range(self._max_workers)
            ]

    async def shutdown(self) -> None:
        """Cancel workers and shut down the process pool (called on app shutdown)."""
        tasks = self._worker_tasks
        self._worker_tasks = []
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._process_pool is not None:
            self._process_pool.shutdown(wait=False, cancel_futures=True)
            self._process_pool = None

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
            validation_folds=req.validation_folds,
            validation_train_ratio=req.validation_train_ratio,
            search_mode=req.search_mode,
            combination_budget=req.combination_budget,
            random_seed=req.random_seed,
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
            "progress": self.get_progress(job_id),
        }

    def get_progress(self, job_id: str) -> dict[str, Any] | None:
        """Return the latest progress record for ``job_id``, or None.

        Reads fresh from the sink each call (the worker publishes to Redis/file
        independently of this process), so polling always sees the newest
        heartbeat for a running job.
        """
        return self._progress_sink.read(job_id)

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

    async def run_single(
        self, config: BacktestConfig, *, job_id: str | None = None
    ) -> tuple[str, Any]:
        """Run a single backtest config in the process pool; return (job_id, result).

        Used by the UI single-run path so a long CPU-bound backtest is isolated
        in a forked worker (the same model as the REST/job-manager path) rather
        than running inside the server process and starving the event loop /
        websocket keepalive.  The worker publishes granular progress to the
        sink under ``job_id``; the caller polls it via :meth:`get_progress`.

        ``job_id`` may be supplied by the caller so it is known *before* the
        run completes (enabling the UI to poll granular progress inline); if
        omitted, one is generated.
        """
        pool = self._process_pool
        if pool is None:
            raise RuntimeError("backtest process pool not started")
        loop = asyncio.get_running_loop()
        if job_id is None:
            job_id = uuid.uuid4().hex
        result = await loop.run_in_executor(
            pool, _execute_run_worker, config, job_id, self._redis_url
        )
        return job_id, result

    async def run_single_grid(
        self, config: GridConfig, *, job_id: str | None = None
    ) -> tuple[str, Any]:
        """Run a parameter sweep in the process pool; return (job_id, result).

        The UI sweep path's counterpart to :meth:`run_single`: isolates the
        grid orchestration in a forked worker (instead of running the
        ``BacktestGrid`` object on the event loop) and publishes granular
        ``GridProgress`` to the sink under ``job_id``.
        """
        pool = self._process_pool
        if pool is None:
            raise RuntimeError("backtest process pool not started")
        loop = asyncio.get_running_loop()
        if job_id is None:
            job_id = uuid.uuid4().hex
        result = await loop.run_in_executor(
            pool, _execute_grid_worker, config, job_id, self._redis_url
        )
        return job_id, result

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
            loop = asyncio.get_running_loop()
            pool = self._process_pool
            if pool is None:
                raise RuntimeError("backtest process pool not started")
            if job["kind"] == "run":
                result = await loop.run_in_executor(
                    pool, _execute_run_worker, job["config"], job_id, self._redis_url
                )
            else:
                result = await loop.run_in_executor(
                    pool, _execute_grid_worker, job["config"], job_id, self._redis_url
                )
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
            # Drop the job's progress heartbeats (Redis key and/or file).
            self._progress_sink.clear(job_id)

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
