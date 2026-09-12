"""FastAPI router exposing the backtest engine over REST.

Endpoints
---------
POST /backtest/run        → submit a single engine run, returns {job_id}
POST /backtest/grid       → submit a parameter sweep, returns {job_id}
GET  /backtest/status/{job_id}   → status (queued/running/completed/failed)
GET  /backtest/result/{job_id}   → full serialised result (only when completed)

The router is mounted on the FastAPI app in ``app/main.py``.  The job manager
lives on ``app.state.backtest_jobs``.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.services.backtest.api_models import (
    BacktestGridRequest,
    BacktestJobAccepted,
    BacktestRunRequest,
)
from app.services.backtest.job_manager import BacktestJobManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/backtest", tags=["backtest"])


def _manager(app_state) -> BacktestJobManager | None:
    return getattr(app_state, "backtest_jobs", None)


@router.post("/run")
async def submit_run(request: Request, body: BacktestRunRequest) -> JSONResponse:
    manager = _manager(request.app.state)
    if manager is None:
        return JSONResponse({"detail": "backtest service unavailable"}, status_code=503)
    if not body.symbols:
        return JSONResponse({"detail": "at least one symbol is required"}, status_code=422)
    job_id = manager.submit_run(body)
    return JSONResponse(BacktestJobAccepted(job_id=job_id).model_dump(), status_code=202)


@router.post("/grid")
async def submit_grid(request: Request, body: BacktestGridRequest) -> JSONResponse:
    manager = _manager(request.app.state)
    if manager is None:
        return JSONResponse({"detail": "backtest service unavailable"}, status_code=503)
    if not body.base.symbols:
        return JSONResponse({"detail": "at least one symbol is required"}, status_code=422)
    if not body.params:
        return JSONResponse({"detail": "at least one parameter is required"}, status_code=422)
    job_id = manager.submit_grid(body)
    return JSONResponse(BacktestJobAccepted(job_id=job_id).model_dump(), status_code=202)


@router.get("/status/{job_id}")
async def get_status(request: Request, job_id: str) -> JSONResponse:
    manager = _manager(request.app.state)
    if manager is None:
        return JSONResponse({"detail": "backtest service unavailable"}, status_code=503)
    status = manager.get_status(job_id)
    if status is None:
        return JSONResponse({"detail": "job not found"}, status_code=404)
    return JSONResponse(status, status_code=200)


@router.get("/result/{job_id}")
async def get_result(request: Request, job_id: str) -> JSONResponse:
    manager = _manager(request.app.state)
    if manager is None:
        return JSONResponse({"detail": "backtest service unavailable"}, status_code=503)
    job = manager.get_status(job_id)
    if job is None:
        return JSONResponse({"detail": "job not found"}, status_code=404)
    if job["status"] != "completed":
        return JSONResponse(
            {"detail": f"job is {job['status']}, not completed"},
            status_code=409,
        )
    return JSONResponse(manager.get_result(job_id), status_code=200)
