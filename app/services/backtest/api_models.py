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
    taker_fee_bps: float = Field(default=5.0, ge=0.0)
    maker_fee_bps: float = Field(default=0.0, ge=0.0)
    slippage_bps: float = Field(default=0.0, ge=0.0)
    slippage_mode: str = Field(default="ohlcv_liquidity", pattern="^(fixed|ohlcv_liquidity|tape_spread)$")
    slippage_stress_multiplier: float = Field(default=1.0, ge=0.0)
    spread_estimator: str = Field(default="corwin_schultz", pattern="^(corwin_schultz|roll)$")
    spread_window: int = Field(default=20, ge=2)
    liquidity_impact_coefficient: float = Field(default=0.05, ge=0.0)
    candle_range_slippage_fraction: float = Field(default=0.1, ge=0.0)
    max_liquidity_slippage_bps: float = Field(default=500.0, ge=0.0)
    liquidation_fee_bps: float = Field(default=0.0, ge=0.0)
    funding_rate_pct: float = 0.0
    funding_mode: str = Field(default="historical", pattern="^(historical|constant|off)$")
    allow_concurrent_strategies_per_symbol: bool = False
    margin_mode: str = Field(default="isolated", pattern="^(isolated|cross)$")
    # ── Universe selection ────────────────────────────────────────────
    # "explicit" trades exactly ``symbols``; "screener" reconstructs the live
    # dual-universe screener from historical candles and trades the symbols it
    # would have selected (``symbols`` then acts as the pre-first-interval
    # fallback list and may be empty).
    universe_mode: str = Field(default="explicit", pattern="^(explicit|screener)$")
    screener_config: dict[str, Any] | None = None
    universe_candidate_symbols: list[str] | None = None


class GridParamRequest(BaseModel):
    """A single parameter to sweep (mirrors :class:`GridParamDef`)."""

    key: str
    values: list[Any]
    label: str = ""


class BacktestGridRequest(BaseModel):
    """Request body for ``POST /backtest/grid`` (parameter sweep)."""

    base: BacktestRunRequest
    params: list[GridParamRequest] = Field(default_factory=list)
    rank_by: str = "net_profit_after_cost_pct"
    min_trades: int = 5
    validation_folds: int = Field(default=0, ge=0)
    validation_train_ratio: float = Field(default=0.7, gt=0.0, lt=1.0)
    final_holdout_fraction: float = Field(default=0.0, ge=0.0, lt=0.5)
    search_mode: str = Field(default="exhaustive", pattern="^(exhaustive|random)$")
    combination_budget: int = Field(default=0, ge=0)
    random_seed: int = 0


class BacktestJobAccepted(BaseModel):
    """Response body returned when a job is queued."""

    job_id: str
    status: str = "queued"
    detail: str = ""
