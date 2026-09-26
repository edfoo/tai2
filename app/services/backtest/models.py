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
    trade_id: str = ""
    margin_mode: str = "isolated"
    leverage: float = 1.0
    initial_margin: float = 0.0
    maintenance_margin_ratio: float = 0.0
    maintenance_margin_deduction: float = 0.0
    liquidation_price: float | None = None
    tp_price: float | None = None
    sl_price: float | None = None
    strategy_name: str = ""
    # Populated on close
    close_price: float | None = None
    close_ts: int | None = None
    close_reason: str = ""  # "tp" | "sl" | "pm_skimming" | "end_of_data" | ...
    pnl: float = 0.0
    pnl_pct: float = 0.0
    # Trading costs accrued over the position's life (quote currency).
    entry_fee: float = 0.0
    exit_fee: float = 0.0
    funding: float = 0.0
    funding_settled_through_ts: int = 0
    funding_intervals_paid: int = 0
    slippage_cost: float = 0.0
    # Number of candles the position has been held through (incremented
    # by the simulator on each update_multi/update tick).
    candles_held: int = 0
    # Peak favorable / adverse excursion (unrealised PnL %, direction-signed).
    # Tracked per candle while open for MAE/MFE metrics.
    max_favorable_pct: float = 0.0
    max_adverse_pct: float = 0.0
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

    @property
    def fee_and_funding_cost(self) -> float:
        """Cash costs (entry fee, exit fee, funding) not already in ``pnl``.

        Slippage is excluded because it is baked into the fill prices and thus
        already reflected in ``pnl`` / ``unrealised_pnl``.
        """
        return self.entry_fee + self.exit_fee + self.funding

    @property
    def net_pnl(self) -> float:
        """Realised PnL after fees and funding (slippage already in ``pnl``)."""
        return self.pnl - self.fee_and_funding_cost


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
    # Snapshot of runtime_config["guardrails"] (max_position_pct, daily_loss
    # limit, atr_risk_per_trade_pct, min/max leverage, R:R/protection toggles,
    # min_hold_seconds, etc.).  Live keeps these separate from launcher/strategy;
    # the backtest must be passed them explicitly or its sizing, daily-loss
    # lockout, leverage (PnL%-mode), and R:R guards silently diverge from live.
    guardrails_config: dict[str, Any] = field(default_factory=dict)
    instrument_specs: dict[str, dict[str, Any]] = field(default_factory=dict)
    allow_concurrent_strategies_per_symbol: bool = False
    margin_mode: str = "isolated"
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
    # ── Cost model (fees / slippage / funding) ────────────────────────
    # Taker fee per fill in basis points (5 = 0.05%).  Maker fills are not
    # modeled by the launcher (market orders), so maker_fee_bps defaults to 0.
    taker_fee_bps: float = 5.0
    maker_fee_bps: float = 0.0
    # Base adverse entry/exit bps. The default OHLCV mode adds a prior-bar
    # range/turnover impact proxy; it does not use the execution candle.
    slippage_bps: float = 0.0
    slippage_mode: str = "ohlcv_liquidity"
    # Adverse-scenario multiplier applied to the estimated slippage (both
    # modes). 1.0 = unmodified estimate; >1.0 stresses execution costs.
    slippage_stress_multiplier: float = 1.0
    # Spread-estimation method for slippage_mode="tape_spread":
    # "corwin_schultz" (OHLC high-low) or "roll" (trade-tape covariance).
    spread_estimator: str = "corwin_schultz"
    spread_window: int = 20
    liquidity_impact_coefficient: float = 0.05
    candle_range_slippage_fraction: float = 0.1
    max_liquidity_slippage_bps: float = 500.0
    # Extra fee charged on a liquidation fill, in bps (OKX liquidation fee).
    liquidation_fee_bps: float = 0.0
    # Constant fallback funding rate per interval (0.01 = 0.01% of notional).
    # Historical mode uses timestamped rates when fetched, then this fallback.
    funding_rate_pct: float = 0.0
    funding_mode: str = "historical"
    # Funding cadence in milliseconds (default 8h).
    funding_interval_ms: int = 8 * 60 * 60 * 1000


# ── Backtest result ─────────────────────────────────────────────────────


@dataclass(slots=True)
class BacktestResult:
    """Complete output of a backtest run."""

    config: BacktestConfig
    trades: list[SimPosition] = field(default_factory=list)
    equity_curve: list[EquityPoint] = field(default_factory=list)
    # Per-strategy breakdown
    per_strategy: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Per-symbol (token) breakdown
    per_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Aggregate metrics
    metrics: dict[str, Any] = field(default_factory=dict)
    assumptions: dict[str, Any] = field(default_factory=dict)
    data_provenance: list[dict[str, Any]] = field(default_factory=list)
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
        ``"net_profit_after_cost_pct"``, ``"sharpe_per_candle"``,
        ``"profit_factor"``, ``"win_rate"``, ``"total_trades"``.
    validation_folds:
        Number of chronological forward-validation windows.  Zero preserves
        the legacy single-window sweep.  Positive values reserve the initial
        ``validation_train_ratio`` portion of the date range as warmup/history
        and split the remainder into non-overlapping validation windows.
    validation_train_ratio:
        Fraction of the date range preceding validation; defaults to 0.7.
    search_mode:
        ``"exhaustive"`` enumerates every combination; ``"random"`` samples
        up to ``combination_budget`` combinations reproducibly.
    min_trades:
        Minimum number of trades for a result to be included in the
        ranking.  Results with fewer trades are still reported but
        flagged as ``below_min_trades``.
    """

    base_config: BacktestConfig
    params: list[GridParamDef] = field(default_factory=list)
    rank_by: str = "net_profit_after_cost_pct"
    min_trades: int = 5
    validation_folds: int = 0
    validation_train_ratio: float = 0.7
    final_holdout_fraction: float = 0.0
    search_mode: str = "exhaustive"
    combination_budget: int = 0
    random_seed: int = 0


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
    fold_metrics: list[dict[str, Any]] = field(default_factory=list)


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
    total_combinations: int = 0
    attempted_combinations: int = 0
    search_mode: str = "exhaustive"
    random_seed: int = 0
    assumptions: dict[str, Any] = field(default_factory=dict)
    data_provenance: list[dict[str, Any]] = field(default_factory=list)
    final_holdout: GridRunResult | None = None
    error: str | None = None

    @property
    def is_error(self) -> bool:
        return self.error is not None
