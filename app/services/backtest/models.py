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
    # Number of candles the position has been held through (incremented
    # by the simulator on each update_multi/update tick).
    candles_held: int = 0
    # Trade-management state (breakeven / partial TP).
    initial_size: float | None = None
    breakeven_done: bool = False
    partial_done: bool = False

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
    # ── Finer-LTF evaluation (replicates live intra-candle behaviour) ──
    # evaluation_mode:
    #   "finer_ltf" (default) — step the backtest loop on `evaluation_timeframe`
    #                           candles (e.g. 1m) while computing indicators on
    #                           `timeframe` (e.g. 15m) with the last LTF candle
    #                           INCOMPLETE (close = current eval candle close).
    #                           Mirrors live where the scheduler polls mid-candle
    #                           and last_price = real-time ticker.
    #   "closed"               — legacy: step on closed `timeframe` candles only.
    evaluation_mode: str = "finer_ltf"
    # Fine timeframe used for loop stepping when evaluation_mode="finer_ltf".
    # Must be strictly finer than `timeframe`. If equal or coarser, the engine
    # falls back to "closed" mode automatically.
    evaluation_timeframe: str = "1m"


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


# ── Parameter sweep (grid) models ───────────────────────────────────────


@dataclass(slots=True)
class GridParamDef:
    """Definition of a single parameter to sweep.

    Parameters
    ----------
    key:
        Dotted path into the ``launcher_config`` dict, using ``strategies``
        as the top-level key.  Examples:
          - ``"strategies.mean_reversion.rsi_oversold"``
          - ``"strategies.spike_continuation.max_spike_extension_atr"``
          - ``"tp_pct"``  (launcher-level)
    values:
        List of values to try for this parameter.
    label:
        Human-readable label for the UI / results table.  Defaults to ``key``.
    """

    key: str
    values: list[Any]
    label: str = ""


@dataclass(slots=True)
class GridConfig:
    """Configuration for a parameter-sweep (grid) backtest run.

    Parameters
    ----------
    base_config:
        A :class:`BacktestConfig` whose symbols, timeframe, date range,
        capital, etc. are used as the template.  The ``launcher_config``
        is deep-copied per combination and the swept parameters are
        overridden.
    params:
        List of :class:`GridParamDef` definitions.  The grid is the
        Cartesian product of all ``param.values``.
    rank_by:
        Metric key to rank results by (descending).  Common choices:
        ``"sharpe_per_candle"``, ``"profit_factor"``, ``"net_profit"``,
        ``"win_rate"``, ``"total_trades"``.
    min_trades:
        Minimum number of trades for a result to be included in the
        ranking.  Results with fewer trades are still reported but
        flagged as ``below_min_trades``.
    """

    base_config: BacktestConfig
    params: list[GridParamDef] = field(default_factory=list)
    rank_by: str = "sharpe_per_candle"
    min_trades: int = 5


@dataclass(slots=True)
class GridRunResult:
    """Result of a single grid combination.

    Parameters
    ----------
    params:
        The parameter values used for this run, as a ``{key: value}`` dict.
    result:
        The :class:`BacktestResult` from the engine, or ``None`` if the
        run errored.
    rank_score:
        The value of the ``rank_by`` metric (or ``None`` if unavailable).
    below_min_trades:
        True if the run produced fewer than ``GridConfig.min_trades`` trades.
    """

    params: dict[str, Any]
    result: BacktestResult | None
    rank_score: float | None
    below_min_trades: bool


@dataclass(slots=True)
class GridProgress:
    """Progress update emitted during a grid run."""

    phase: str  # "grid" | "done" | "error"
    current: int
    total: int
    message: str = ""


@dataclass(slots=True)
class GridResult:
    """Complete output of a parameter-sweep run."""

    config: GridConfig
    runs: list[GridRunResult] = field(default_factory=list)
    # Runs sorted by rank_score descending (excluding below_min_trades).
    ranked: list[GridRunResult] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    error: str | None = None

    @property
    def is_error(self) -> bool:
        return self.error is not None
