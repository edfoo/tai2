"""Data models for the backtesting engine.

All models are plain dataclasses (no Pydantic) to keep the backtest module
lightweight and dependency-free.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ── Candle ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Candle:
    """Normalised OHLCV candle.

    ``ts`` is millisecond epoch (matching OKX's native format).
    """

    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def dt(self) -> datetime:
        """UTC datetime of the candle open."""
        return datetime.fromtimestamp(self.ts / 1000.0, tz=timezone.utc)


# ── Simulated position ──────────────────────────────────────────────────


@dataclass(slots=True)
class SimPosition:
    """A single simulated position opened by a strategy signal."""

    symbol: str
    direction: str  # "long" | "short"
    size: float  # base-token units
    entry_price: float
    entry_ts: int  # ms epoch
    tp_price: float | None = None
    sl_price: float | None = None
    strategy_name: str = ""
    # Populated on close
    close_price: float | None = None
    close_ts: int | None = None
    close_reason: str = ""  # "tp" | "sl" | "pm_skimming" | "end_of_data" | ...
    pnl: float = 0.0
    pnl_pct: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.close_price is None

    @property
    def is_long(self) -> bool:
        return self.direction == "long"

    def unrealised_pnl(self, price: float) -> float:
        """PnL if closed at *price* (in quote currency, e.g. USDT)."""
        if self.is_long:
            return (price - self.entry_price) * self.size
        return (self.entry_price - price) * self.size

    def unrealised_pnl_pct(self, price: float) -> float:
        """PnL as a percentage of entry notional."""
        if self.entry_price <= 0:
            return 0.0
        if self.is_long:
            return (price - self.entry_price) / self.entry_price * 100.0
        return (self.entry_price - price) / self.entry_price * 100.0


# ── Equity curve point ──────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class EquityPoint:
    """A single point on the equity curve."""

    ts: int  # ms epoch
    equity: float  # account equity in quote currency
    open_positions: int


# ── Backtest configuration ──────────────────────────────────────────────


@dataclass(slots=True)
class BacktestConfig:
    """User-facing configuration for a single backtest run."""

    symbols: list[str]
    timeframe: str  # OKX bar, e.g. "4H"
    start_ts: int  # ms epoch (inclusive)
    end_ts: int  # ms epoch (inclusive)
    initial_capital: float = 1000.0
    strategy_names: list[str] = field(default_factory=list)
    # Snapshot of runtime_config["launcher"] and runtime_config["strategy"]
    # captured at run start so mid-run config changes don't affect results.
    launcher_config: dict[str, Any] = field(default_factory=dict)
    strategy_config: dict[str, Any] = field(default_factory=dict)
    # Warmup candles to fetch before start_ts (for indicator stabilisation).
    warmup_candles: int = 200
    # Whether to disable live execution during the backtest.
    disable_live_execution: bool = True


# ── Backtest result ─────────────────────────────────────────────────────


@dataclass(slots=True)
class BacktestResult:
    """Complete output of a backtest run."""

    config: BacktestConfig
    trades: list[SimPosition] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    # Per-strategy breakdown
    per_strategy: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Aggregate metrics
    metrics: dict[str, Any] = field(default_factory=dict)
    # Execution metadata
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    candles_processed: int = 0
    error: str | None = None

    @property
    def is_error(self) -> bool:
        return self.error is not None


# ── Progress callback ───────────────────────────────────────────────────


@dataclass(slots=True)
class BacktestProgress:
    """Progress update emitted during a backtest run."""

    phase: str  # "fetch" | "warmup" | "backtest" | "metrics" | "done" | "error"
    current: int
    total: int
    message: str = ""
