"""Pydantic request models for the backtest REST API.

These mirror the fields of :class:`BacktestConfig` (and :class:`GridConfig`)
so a CLI/HTTP client can submit a backtest using the same inputs the UI
passes to :func:`build_backtest_config`.  A date window may be expressed
either as explicit ``start_ts``/``end_ts`` (millisecond epoch) or as a
trailing ``days`` window ending now.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BacktestRunRequest(BaseModel):
    """Request body for ``POST /backtest/run`` (single engine run)."""

    symbols: list[str] = Field(default_factory=list)
    timeframe: str = "15m"
    strategy_names: list[str] | None = None  # None → all available strategies
    start_ts: int | None = None  # ms epoch (inclusive)
    end_ts: int | None = None  # ms epoch (inclusive)
    days: int | None = None  # trailing window ending now (used if ts are absent)
    capital: float = 1000.0
    warmup: int = 200
    evaluation_mode: str = "finer_ltf"
    evaluation_timeframe: str = "1m"
    # Live runtime config snapshots (mirror runtime_config["launcher"] /
    # ["strategy"] / ["guardrails"]).  Omitted → engine defaults.
    launcher_config: dict[str, Any] | None = None
    strategy_config: dict[str, Any] | None = None
    guardrails_config: dict[str, Any] | None = None


class GridParamRequest(BaseModel):
    """A single parameter to sweep (mirrors :class:`GridParamDef`)."""

    key: str
    values: list[Any]
    label: str = ""


class BacktestGridRequest(BaseModel):
    """Request body for ``POST /backtest/grid`` (parameter sweep)."""

    base: BacktestRunRequest
    params: list[GridParamRequest] = Field(default_factory=list)
    rank_by: str = "sharpe_per_candle"
    min_trades: int = 5


class BacktestJobAccepted(BaseModel):
    """Response body returned when a job is queued."""

    job_id: str
    status: str = "queued"
    detail: str = ""
