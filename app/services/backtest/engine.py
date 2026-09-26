"""Backtest engine — orchestrates the full backtest run.

The engine ties together:
  1. :class:`HistoricalDataFetcher` — fetches LTF + HTF candles from OKX.
  2. :class:`SnapshotBuilder` — builds synthetic snapshots from candles.
  3. Strategy evaluation — reuses the live ``Strategy.evaluate()`` protocol.
  4. :class:`Simulator` — simulates fills, TP/SL closes, and equity.
  5. :func:`compute_metrics` — produces performance metrics.

The engine is **async** (data fetching uses the OKX SDK via ``asyncio.to_thread``)
and emits progress updates via a callback so the UI can show a progress bar.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.backtest.data_fetcher import (
    HistoricalDataFetcher,
    htf_for,
    is_finer_than,
    ltf_bucket_ts,
    timeframe_ms,
)
from app.services.backtest.costs import CostModel
from app.services.backtest.metrics import (
    compute_equal_weight_buy_and_hold,
    compute_metrics,
    compute_per_strategy_metrics,
    compute_per_symbol_metrics,
)
from app.services.backtest.models import (
    BacktestConfig,
    BacktestProgress,
    BacktestResult,
    Candle,
)
from app.services.backtest.simulator import Simulator
from app.services.backtest.snapshot_builder import SnapshotBuilder
from app.services.backtest.spread import estimate_spread_series, roll_spread_bps
from app.services.backtest.universe import UniverseSchedule, build_universe_schedule
from app.services.launcher_tp_sl import LauncherTpSl, resolve_launcher_tp_sl
from app.services.strategies import Strategy, StrategyHelpers
from app.services.strategies.liquidity_sweep import LiquiditySweepStrategy
from app.services.strategies.mean_reversion import MeanReversionStrategy
from app.services.strategies.spike_continuation import SpikeContinuationStrategy
from app.services.strategies.trend_pullback import TrendPullbackStrategy
from app.services.strategies.vwap_reversion import VWAPReversionStrategy

logger = logging.getLogger(__name__)

# Registry of available strategies (name → instance).
_AVAILABLE_STRATEGIES: dict[str, Strategy] = {
    "mean_reversion": MeanReversionStrategy(),
    "spike_continuation": SpikeContinuationStrategy(),
    "liquidity_sweep": LiquiditySweepStrategy(),
    "vwap_reversion": VWAPReversionStrategy(),
    "trend_pullback": TrendPullbackStrategy(),
}


def available_strategy_names() -> list[str]:
    """Return the names of all strategies available for backtesting."""
    return list(_AVAILABLE_STRATEGIES.keys())


def _requested_tfs(launcher_config: dict[str, Any]) -> list[str]:
    """Return the distinct analysis timeframes requested by enabled strategies.

    Reads each enabled strategy's ``analysis_timeframe`` from
    ``launcher_config["strategies"]``.  ``None``/empty means "use the global
    LTF" (skipped here).  Constants like ``"15m"`` / ``"1H"`` are passed
    through; the caller normalizes via the fetcher's timeframe handling.
    """
    strategies_cfg = launcher_config.get("strategies") or {}
    requested: list[str] = []
    for strat_cfg in strategies_cfg.values():
        if not isinstance(strat_cfg, dict):
            continue
        if not bool(strat_cfg.get("enabled", False)):
            continue
        tf = strat_cfg.get("analysis_timeframe")
        if not tf:
            continue
        tf_str = str(tf).strip().upper()
        if tf_str and tf_str not in requested:
            requested.append(tf_str)
    return requested


class BacktestEngine:
    """Orchestrates a single backtest run.

    Usage::

        engine = BacktestEngine(config)
        result = await engine.run(progress_cb=my_callback)
    """

    def __init__(self, config: BacktestConfig) -> None:
        if config.margin_mode not in ("isolated", "cross"):
            raise ValueError(
                f"Unsupported margin_mode '{config.margin_mode}'; use 'isolated' or 'cross'."
            )
        self._config = config
        self._fetcher = HistoricalDataFetcher()
        # Merge launcher + strategy config so the simulator can read both
        # max_hold_candles (launcher) and trade_management (strategy).
        _sim_cfg = dict(config.launcher_config or {})
        _strat_cfg = dict(config.strategy_config or {})
        if "trade_management" in _strat_cfg and "trade_management" not in _sim_cfg:
            _sim_cfg["trade_management"] = _strat_cfg["trade_management"]
        # Merge the live guardrails config so the simulator's sizing, daily-loss
        # lockout, and re-entry logic read the same values live uses.  Live keeps
        # these under runtime_config["guardrails"] (separate from launcher/strategy),
        # so without this the backtest's max_position_pct / atr_risk_per_trade_pct /
        # daily_loss_limit_pct / leverage would all be inert.
        self._guardrails_config = dict(config.guardrails_config or {})
        if self._guardrails_config:
            _existing_guard = _sim_cfg.get("guardrails")
            if isinstance(_existing_guard, dict):
                _existing_guard.update(self._guardrails_config)
            else:
                _sim_cfg["guardrails"] = self._guardrails_config
        # Derive the per-candle time-stop from time_stop_seconds (see Simulator).
        _timeframe_seconds: float | None = None
        try:
            _tf_ms = timeframe_ms(self._config.timeframe)
            if _tf_ms and _tf_ms > 0:
                _timeframe_seconds = _tf_ms / 1000.0
        except Exception:
            _timeframe_seconds = None
        self._cost_model = CostModel(
            taker_fee_bps=config.taker_fee_bps,
            maker_fee_bps=config.maker_fee_bps,
            slippage_bps=config.slippage_bps,
            slippage_mode=config.slippage_mode,
            slippage_stress_multiplier=config.slippage_stress_multiplier,
            liquidity_impact_coefficient=config.liquidity_impact_coefficient,
            candle_range_slippage_fraction=config.candle_range_slippage_fraction,
            max_liquidity_slippage_bps=config.max_liquidity_slippage_bps,
            liquidation_fee_bps=config.liquidation_fee_bps,
            funding_rate_pct=config.funding_rate_pct,
            funding_interval_ms=config.funding_interval_ms,
            funding_mode=config.funding_mode,
        )
        self._simulator = Simulator(
            initial_capital=config.initial_capital,
            notional_per_trade=float(
                (config.launcher_config or {}).get("notional_usd") or 10.0
            ),
            strategy_config=_sim_cfg,
            timeframe_seconds=_timeframe_seconds,
            cost_model=self._cost_model,
            margin_mode=config.margin_mode,
        )
        # Build the list of strategy instances to evaluate.
        self._strategies: list[Strategy] = [
            _AVAILABLE_STRATEGIES[name]
            for name in config.strategy_names
            if name in _AVAILABLE_STRATEGIES
        ]
        # Helpers for strategy evaluation — footprint returns empty (not
        # available in backtest), get_last_price reads from the snapshot.
        self._helpers = StrategyHelpers(
            extract_float=_extract_float,
            emit_debug=lambda msg: logger.debug("[backtest] %s", msg),
            get_last_price=self._get_last_price,
            compute_footprint=lambda symbol: {},
        )
        self._current_prices: dict[str, float] = {}
        self._last_candle_ts: int = 0
        # Effective symbol list for this run.  In "explicit" mode this equals
        # ``config.symbols``; in "screener" mode it is the union of every
        # symbol the reconstructed screener selects across the window (plus
        # the configured fallback list), resolved in ``run()``.
        self._symbols: list[str] = list(config.symbols)
        self._universe_schedule: UniverseSchedule | None = None

        # ── Finer-LTF evaluation mode ─────────────────────────────────
        # Resolve the effective evaluation mode.  If the user requested
        # "finer_ltf" but the eval TF is not strictly finer than the LTF,
        # fall back to "closed" mode (no benefit to finer stepping).
        self._eval_tf = config.evaluation_timeframe
        self._eval_mode = config.evaluation_mode
        if self._eval_mode == "finer_ltf" and not is_finer_than(self._eval_tf, config.timeframe):
            logger.info(
                "Backtest: evaluation_timeframe=%s is not finer than timeframe=%s — "
                "falling back to closed-candle mode.",
                self._eval_tf,
                config.timeframe,
            )
            self._eval_mode = "closed"

        # Warn if any enabled strategy config requires footprint data — it's
        # never available in backtest (no historical trade tape), so the filter
        # is silently skipped. This makes backtests with that flag a lower
        # bound on filtering (more trades than live).
        _strat_cfg = self._config.strategy_config or {}
        _mr = (_strat_cfg.get("strategies") or {}).get("mean_reversion") or {}
        if bool(_mr.get("require_footprint_delta", False)):
            logger.warning(
                "Backtest: mean_reversion.require_footprint_delta=True — "
                "footprint data is never available historically; the footprint "
                "filter will be skipped. Backtest may show more trades than live."
            )

    # ── Public API ────────────────────────────────────────────────────

    async def run(
        self,
        progress_cb: Callable[[BacktestProgress], None] | None = None,
    ) -> BacktestResult:
        """Execute the backtest and return the result."""
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        result = BacktestResult(config=self._config, started_at=started.isoformat())
        data_provenance: list[dict[str, Any]] = []

        def _capture_fetch_provenance() -> None:
            metadata = getattr(self._fetcher, "last_fetch_provenance", None)
            if metadata:
                data_provenance.append(dict(metadata))

        try:
            # ── Phase 0: Resolve the trading universe ─────────────────
            # In "screener" mode, reconstruct the live dual-universe screener
            # from historical candles and trade the symbols it would have
            # selected.  The configured ``symbols`` become the fallback list
            # used before the first screener interval.
            if self._config.universe_mode == "screener":
                if progress_cb:
                    progress_cb(BacktestProgress(
                        phase="fetch", current=0, total=1,
                        message="Reconstructing screener universe",
                    ))
                self._universe_schedule = await build_universe_schedule(
                    fetcher=self._fetcher,
                    start_ts=self._config.start_ts,
                    end_ts=self._config.end_ts,
                    screener_config=self._config.screener_config,
                    candidate_symbols=self._config.universe_candidate_symbols or None,
                    progress_cb=lambda done, total, msg: progress_cb(
                        BacktestProgress(phase="fetch", current=done, total=total, message=msg)
                    ) if progress_cb else None,
                )
                universe_provenance = getattr(self._fetcher, "last_universe_provenance", None)
                if universe_provenance:
                    data_provenance.append({
                        "timeframe": "screener_universe",
                        **dict(universe_provenance),
                    })
                # Union of every selected symbol + the configured fallback.
                selected_union = self._universe_schedule.all_symbols
                merged: list[str] = []
                for sym in [*self._config.symbols, *selected_union]:
                    if sym and sym not in merged:
                        merged.append(sym)
                self._symbols = merged
                if not selected_union:
                    logger.warning(
                        "Screener universe selected no symbols over the window; "
                        "falling back to the configured symbol list."
                    )
                if not self._symbols:
                    result.error = (
                        "Screener universe mode selected no symbols and no "
                        "fallback symbols were configured."
                    )
                    result.data_provenance = data_provenance
                    result.finished_at = datetime.now(timezone.utc).isoformat()
                    result.duration_seconds = time.monotonic() - t0
                    return result

            # ── Phase 1: Fetch historical data ────────────────────────
            if progress_cb:
                progress_cb(BacktestProgress(phase="fetch", current=0, total=len(self._symbols), message="Fetching historical data"))

            symbol_candles: dict[str, list[Candle]] = {}
            symbol_htf_candles: dict[str, list[Candle]] = {}
            symbol_eval_candles: dict[str, list[Candle]] = {}
            symbol_tf_candles: dict[str, dict[str, list[Candle]]] = {}
            htf_tf = htf_for(self._config.timeframe)
            use_finer_ltf = self._eval_mode == "finer_ltf"
            # Distinct per-strategy analysis timeframes (plus their HTFs) to
            # fetch for each symbol.
            analysis_tfs = _requested_tfs(self._config.launcher_config or {})
            all_tf_bars: list[str] = []
            for tf in analysis_tfs:
                if tf not in all_tf_bars:
                    all_tf_bars.append(tf)
                tf_htf = htf_for(tf)
                if tf_htf and tf_htf not in all_tf_bars:
                    all_tf_bars.append(tf_htf)

            for idx, symbol in enumerate(self._symbols):
                candles = await self._fetcher.fetch_candles(
                    symbol=symbol,
                    timeframe=self._config.timeframe,
                    start_ts=self._config.start_ts,
                    end_ts=self._config.end_ts,
                    warmup_candles=self._config.warmup_candles,
                    progress_cb=lambda done, total, msg: progress_cb(
                        BacktestProgress(phase="fetch", current=idx, total=len(self._symbols), message=f"{symbol}: {msg}")
                    ) if progress_cb else None,
                )
                _capture_fetch_provenance()
                symbol_candles[symbol] = candles

                if htf_tf:
                    htf_candles = await self._fetcher.fetch_htf_candles(
                        symbol=symbol,
                        ltf_timeframe=self._config.timeframe,
                        htf_timeframe=htf_tf,
                        start_ts=self._config.start_ts,
                        end_ts=self._config.end_ts,
                        warmup_candles=self._config.warmup_candles,
                    )
                    _capture_fetch_provenance()
                    symbol_htf_candles[symbol] = htf_candles

                # Fetch per-strategy analysis timeframe candles (and their
                # HTFs) so the snapshot exposes ``timeframes[<tf>]``.
                per_tf: dict[str, list[Candle]] = {}
                for bar in all_tf_bars:
                    tf_candles = await self._fetcher.fetch_htf_candles(
                        symbol=symbol,
                        ltf_timeframe=self._config.timeframe,
                        htf_timeframe=bar,
                        start_ts=self._config.start_ts,
                        end_ts=self._config.end_ts,
                        warmup_candles=self._config.warmup_candles,
                    )
                    _capture_fetch_provenance()
                    per_tf[bar] = tf_candles
                symbol_tf_candles[symbol] = per_tf

                # Fetch the finer evaluation timeframe (e.g. 1m) for stepping.
                # These candles drive the loop; indicators are still computed
                # on the LTF (with the last LTF candle incomplete).
                if use_finer_ltf:
                    eval_candles = await self._fetcher.fetch_candles(
                        symbol=symbol,
                        timeframe=self._eval_tf,
                        start_ts=self._config.start_ts,
                        end_ts=self._config.end_ts,
                        warmup_candles=self._config.warmup_candles,
                    )
                    _capture_fetch_provenance()
                    symbol_eval_candles[symbol] = eval_candles

                if progress_cb:
                    progress_cb(BacktestProgress(phase="fetch", current=idx + 1, total=len(self._symbols), message=f"{symbol}: {len(candles)} candles"))

            if self._config.funding_mode == "historical":
                historical_rates: dict[str, list[dict[str, float | int]]] = {}
                for symbol in self._symbols:
                    fetch_funding_rates = getattr(self._fetcher, "fetch_funding_rates", None)
                    rates = (
                        await fetch_funding_rates(
                            symbol, self._config.start_ts, self._config.end_ts
                        )
                        if fetch_funding_rates is not None else []
                    )
                    historical_rates[symbol] = rates
                    funding_provenance = getattr(self._fetcher, "last_funding_provenance", None)
                    if funding_provenance:
                        data_provenance.append(dict(funding_provenance))
                self._cost_model.historical_funding_rates = historical_rates

            if self._config.slippage_mode == "tape_spread":
                spread_series: dict[str, list[dict[str, float | int]]] = {}
                for symbol in self._symbols:
                    candles = symbol_candles.get(symbol) or []
                    if self._config.spread_estimator == "roll":
                        fetch_tape = getattr(self._fetcher, "fetch_trade_tape", None)
                        tape = (
                            await fetch_tape(symbol, self._config.start_ts, self._config.end_ts)
                            if fetch_tape is not None else []
                        )
                        tape_provenance = getattr(self._fetcher, "last_tape_provenance", None)
                        if tape_provenance:
                            data_provenance.append(dict(tape_provenance))
                        series = _spread_series_from_tape(tape, self._config.spread_window)
                    else:
                        series = estimate_spread_series(
                            candles,
                            method="corwin_schultz",
                            window=self._config.spread_window,
                        )
                    spread_series[symbol] = series
                self._cost_model.spread_series = spread_series

            fetch_instrument_specs = getattr(self._fetcher, "fetch_instrument_specs", None)
            instrument_specs = (
                await fetch_instrument_specs(self._symbols)
                if fetch_instrument_specs is not None else {}
            )
            self._config.instrument_specs = instrument_specs
            self._simulator.set_instrument_specs(instrument_specs)
            instrument_provenance = getattr(self._fetcher, "last_instrument_provenance", None)
            if instrument_provenance:
                data_provenance.append({
                    "timeframe": "instrument_specs",
                    **dict(instrument_provenance),
                })

            # ── Phase 2: Build snapshot builders ──────────────────────
            snapshot_builders: dict[str, SnapshotBuilder] = {}
            for symbol in self._symbols:
                snapshot_builders[symbol] = SnapshotBuilder(
                    symbol=symbol,
                    ltf_candles=symbol_candles[symbol],
                    htf_candles=symbol_htf_candles.get(symbol),
                    ltf_timeframe=self._config.timeframe,
                    tf_candles=symbol_tf_candles.get(symbol) or {},
                )

            # ── Phase 3: Determine backtest window ────────────────────
            # Find the index of the first candle at or after start_ts for
            # each symbol (after warmup).
            stepping_candles = symbol_eval_candles if use_finer_ltf else symbol_candles
            start_indices: dict[str, int] = {}
            for symbol in self._symbols:
                candles = stepping_candles[symbol]
                start_idx = 0
                for i, c in enumerate(candles):
                    if c.ts >= self._config.start_ts:
                        start_idx = i
                        break
                start_indices[symbol] = start_idx

            # Total candles to process = max length across symbols.
            max_len = max(
                len(stepping_candles[s]) - start_indices[s]
                for s in self._symbols
                if stepping_candles[s]
            ) if self._symbols else 0

            if max_len == 0:
                result.error = "No candles found in the specified date range."
                result.data_provenance = data_provenance
                result.finished_at = datetime.now(timezone.utc).isoformat()
                result.duration_seconds = time.monotonic() - t0
                return result

            # ── Phase 4: Backtest loop ────────────────────────────────
            # The loop is CPU-bound (each step recomputes a full set of
            # pandas-ta indicators on the growing candle window), so running
            # it directly in the event loop blocks NiceGUI's websocket
            # keepalive and causes the client to disconnect.  We delegate it
            # to a worker thread and communicate progress via the callback.
            if progress_cb:
                progress_cb(BacktestProgress(phase="backtest", current=0, total=max_len, message="Running backtest"))

            if use_finer_ltf:
                candles_processed = await asyncio.to_thread(
                    self._run_finer_ltf_loop,
                    symbol_candles=symbol_candles,
                    symbol_eval_candles=symbol_eval_candles,
                    snapshot_builders=snapshot_builders,
                    start_indices=start_indices,
                    max_len=max_len,
                    progress_cb=progress_cb,
                )
            else:
                candles_processed = await asyncio.to_thread(
                    self._run_backtest_loop,
                    symbol_candles=symbol_candles,
                    snapshot_builders=snapshot_builders,
                    start_indices=start_indices,
                    max_len=max_len,
                    progress_cb=progress_cb,
                )

            # ── Phase 5: Close remaining positions at last price ──────
            self._simulator.close_all_at_market(self._current_prices, self._last_candle_ts)

            # ── Phase 6: Compute metrics ──────────────────────────────
            if progress_cb:
                progress_cb(BacktestProgress(phase="metrics", current=0, total=1, message="Computing metrics"))

            all_trades = self._simulator.closed_positions
            result.trades = all_trades
            result.equity_curve = self._simulator.equity_curve
            # Annualise Sharpe/Sortino/Calmar based on the eval timeframe.
            cpy = _candles_per_year(self._eval_tf if use_finer_ltf else self._config.timeframe)
            result.metrics = compute_metrics(
                all_trades,
                self._simulator.equity_curve,
                self._config.initial_capital,
                candles_per_year=cpy,
            )
            result.per_strategy = compute_per_strategy_metrics(all_trades)
            result.per_symbol = compute_per_symbol_metrics(all_trades)
            result.data_provenance = data_provenance
            if self._universe_schedule is not None:
                result.universe_schedule = self._universe_schedule.to_dict()
            # Equal-weight buy-and-hold benchmark across the configured symbols.
            _benchmark = self._compute_benchmark(symbol_candles)
            if _benchmark is not None:
                result.metrics["buy_and_hold"] = _benchmark
            result.candles_processed = candles_processed

            if progress_cb:
                progress_cb(BacktestProgress(phase="done", current=1, total=1, message="Backtest complete"))

        except Exception as exc:
            logger.exception("Backtest failed")
            result.error = str(exc)
            if progress_cb:
                progress_cb(BacktestProgress(phase="error", current=0, total=0, message=str(exc)))

            result.data_provenance = data_provenance
        result.finished_at = datetime.now(timezone.utc).isoformat()
        result.duration_seconds = round(time.monotonic() - t0, 3)
        return result

    # ── Internal helpers ──────────────────────────────────────────────

    def _run_backtest_loop(
        self,
        *,
        symbol_candles: dict[str, list[Candle]],
        snapshot_builders: dict[str, SnapshotBuilder],
        start_indices: dict[str, int],
        max_len: int,
        progress_cb: Callable[[BacktestProgress], None] | None = None,
    ) -> int:
        """Run the CPU-bound backtest loop (designed to run in a worker thread).

        Returns the number of candles processed.  Progress is reported via
        *progress_cb* (if provided) — the callback must be thread-safe (the
        UI layer satisfies this by writing to a plain dict).
        """
        candles_processed = 0
        strategies_cfg = (self._config.launcher_config.get("strategies") or {})

        for step in range(max_len):
            # Build the set of candles at this time-step across symbols.
            step_candles: dict[str, Candle] = {}
            for symbol in self._symbols:
                candles = symbol_candles[symbol]
                idx = start_indices[symbol] + step
                if idx < len(candles):
                    step_candles[symbol] = candles[idx]

            if not step_candles:
                continue

            # Update current prices for equity calculation.
            for symbol, candle in step_candles.items():
                self._current_prices[symbol] = candle.close
                if candle.ts > self._last_candle_ts:
                    self._last_candle_ts = candle.ts

            # Evaluate strategies for each symbol.
            step_snapshots: dict[str, dict[str, Any]] = {}
            for symbol, candle in step_candles.items():
                builder = snapshot_builders[symbol]
                # The window includes all candles up to the current one.
                candle_idx = start_indices[symbol] + step
                snapshot = builder.build(candle_idx)
                step_snapshots[symbol] = snapshot

                # Evaluate each selected strategy.
                for strategy in self._strategies:
                    strat_cfg = strategies_cfg.get(strategy.name) or {}
                    if not strat_cfg.get("enabled", False):
                        continue
                    # Screener-universe gate: only evaluate a strategy on
                    # symbols the reconstructed screener assigned to it.
                    if not self._symbol_in_universe(symbol, strategy.name, candle.ts):
                        continue
                    # Per-strategy position guard: skip if already in position.
                    if (
                        self._has_blocking_position(symbol, strategy.name)
                        or not self._simulator.can_enter(symbol, candle.ts)
                    ):
                        continue
                    signal = strategy.evaluate(symbol, snapshot, strat_cfg, self._helpers)
                    if signal is None:
                        continue
                    # Resolve TP/SL and direction-flip from signal (shared with live).
                    resolved = self._compute_tp_sl(
                        signal, candle.close, self._config.launcher_config,
                        snapshot=snapshot, strat_cfg=strat_cfg,
                    )
                    if not _passes_protection_guard(
                        resolved=resolved,
                        guardrails_config=self._guardrails_config,
                    ):
                        continue
                    if not _passes_reward_risk_guard(
                        action=resolved.action,
                        entry_price=candle.close,
                        tp_price=resolved.tp_price,
                        sl_price=resolved.sl_price,
                        guardrails_config=self._guardrails_config,
                        strategy_config=strat_cfg,
                        skip_guard=resolved.skip_rr_guard,
                    ):
                        continue
                    direction = "long" if resolved.action == "BUY" else "short"
                    self._simulator.open_position(
                        symbol=symbol,
                        direction=direction,
                        entry_price=candle.close,
                        entry_ts=candle.ts,
                        tp_price=resolved.tp_price,
                        sl_price=resolved.sl_price,
                        strategy_name=strategy.name,
                    )

            # Update simulator (check TP/SL, record equity).
            self._simulator.update_multi(step_candles)
            # Strategy-specific exits (regime-breakdown / momentum-rollover).
            self._simulator.apply_strategy_exits(step_snapshots, step_candles)

            candles_processed += 1
            if progress_cb and step % 10 == 0:
                progress_cb(BacktestProgress(phase="backtest", current=step + 1, total=max_len, message=f"Processed {step + 1}/{max_len} candles"))

            # Release the GIL periodically so the asyncio event loop thread
            # (which runs NiceGUI's websocket keepalive) gets CPU time.
            # Without this, the worker thread hogs the GIL during the
            # pandas-ta indicator computations and the websocket disconnects.
            # time.sleep(0) in a thread releases the GIL for one scheduler tick.
            if step % 5 == 0:
                time.sleep(0)

        return candles_processed

    def _run_finer_ltf_loop(
        self,
        *,
        symbol_candles: dict[str, list[Candle]],
        symbol_eval_candles: dict[str, list[Candle]],
        snapshot_builders: dict[str, SnapshotBuilder],
        start_indices: dict[str, int],
        max_len: int,
        progress_cb: Callable[[BacktestProgress], None] | None = None,
    ) -> int:
        """Finer-LTF backtest loop — steps on eval candles, indicators on LTF.

        At each eval-candle step, the last LTF candle in the indicator window
        is INCOMPLETE: its close = current eval candle close (real-time proxy),
        and its open/high/low/volume are aggregated from eval candles seen so
        far in the current LTF bucket.  This mirrors live behaviour where the
        scheduler polls mid-candle and ``last_price`` = real-time ticker.

        When the eval candle crosses into a new LTF bucket, the previous
        accumulator is appended to ``closed_ltf_window`` as a fully-closed
        LTF candle, and a new accumulator is started.
        """
        candles_processed = 0
        strategies_cfg = (self._config.launcher_config.get("strategies") or {})
        ltf_tf = self._config.timeframe
        ltf_ms = timeframe_ms(ltf_tf)

        # Per-symbol state for the in-progress LTF bucket.
        # closed_window[symbol]  → list of fully-closed LTF candles (grows).
        # acc[symbol]             → current in-progress LTF accumulator, or None.
        closed_window: dict[str, list[Candle]] = {s: [] for s in self._symbols}
        acc: dict[str, dict[str, Any] | None] = {s: None for s in self._symbols}

        # Pre-seed closed_window with LTF candles whose ts < the first eval
        # candle's LTF bucket.  This gives indicators enough warmup history
        # on the first step (the LTF fetch already includes warmup_candles).
        for symbol in self._symbols:
            ltf_candles = symbol_candles[symbol]
            eval_candles = symbol_eval_candles[symbol]
            start_idx = start_indices[symbol]
            if start_idx >= len(eval_candles):
                continue
            first_eval_ts = eval_candles[start_idx].ts
            first_bucket = ltf_bucket_ts(first_eval_ts, ltf_tf)
            # All LTF candles strictly before the first eval bucket are closed.
            for c in ltf_candles:
                if c.ts < first_bucket:
                    closed_window[symbol].append(c)
                else:
                    break

        for step in range(max_len):
            # Build the set of eval candles at this time-step across symbols.
            step_candles: dict[str, Candle] = {}
            for symbol in self._symbols:
                eval_candles = symbol_eval_candles[symbol]
                idx = start_indices[symbol] + step
                if idx < len(eval_candles):
                    step_candles[symbol] = eval_candles[idx]

            if not step_candles:
                continue

            # Update current prices for equity calculation.
            for symbol, candle in step_candles.items():
                self._current_prices[symbol] = candle.close
                if candle.ts > self._last_candle_ts:
                    self._last_candle_ts = candle.ts

            # Update each symbol's LTF accumulator with its eval candle.
            for symbol, eval_candle in step_candles.items():
                bucket = ltf_bucket_ts(eval_candle.ts, ltf_tf)
                cur = acc[symbol]
                if cur is None:
                    # Start a new accumulator for this bucket.
                    acc[symbol] = {
                        "ts": bucket,
                        "open": eval_candle.open,
                        "high": eval_candle.high,
                        "low": eval_candle.low,
                        "close": eval_candle.close,
                        "volume": eval_candle.volume,
                    }
                elif cur["ts"] == bucket:
                    # Same bucket — update the accumulator.
                    cur["high"] = max(cur["high"], eval_candle.high)
                    cur["low"] = min(cur["low"], eval_candle.low)
                    cur["close"] = eval_candle.close  # real-time proxy
                    cur["volume"] += eval_candle.volume
                else:
                    # New bucket — close out the previous accumulator and
                    # append it to the closed window, then start fresh.
                    closed_window[symbol].append(Candle(
                        ts=cur["ts"],
                        open=cur["open"],
                        high=cur["high"],
                        low=cur["low"],
                        close=cur["close"],
                        volume=cur["volume"],
                    ))
                    acc[symbol] = {
                        "ts": bucket,
                        "open": eval_candle.open,
                        "high": eval_candle.high,
                        "low": eval_candle.low,
                        "close": eval_candle.close,
                        "volume": eval_candle.volume,
                    }

            # Evaluate strategies for each symbol using the incomplete LTF candle.
            step_snapshots: dict[str, dict[str, Any]] = {}
            for symbol, eval_candle in step_candles.items():
                cur = acc[symbol]
                if cur is None:
                    continue  # no accumulator yet (shouldn't happen)
                incomplete = Candle(
                    ts=cur["ts"],
                    open=cur["open"],
                    high=cur["high"],
                    low=cur["low"],
                    close=cur["close"],
                    volume=cur["volume"],
                )
                builder = snapshot_builders[symbol]
                snapshot = builder.build_with_incomplete_ltf(
                    closed_ltf_window=closed_window[symbol],
                    incomplete_candle=incomplete,
                    current_ts=eval_candle.ts,
                )
                step_snapshots[symbol] = snapshot

                # Evaluate each selected strategy.
                for strategy in self._strategies:
                    strat_cfg = strategies_cfg.get(strategy.name) or {}
                    if not strat_cfg.get("enabled", False):
                        continue
                    # Screener-universe gate: only evaluate a strategy on
                    # symbols the reconstructed screener assigned to it.
                    if not self._symbol_in_universe(symbol, strategy.name, eval_candle.ts):
                        continue
                    # Per-strategy position guard: skip if already in position.
                    if (
                        self._has_blocking_position(symbol, strategy.name)
                        or not self._simulator.can_enter(symbol, eval_candle.ts)
                    ):
                        continue
                    signal = strategy.evaluate(symbol, snapshot, strat_cfg, self._helpers)
                    if signal is None:
                        continue
                    # Resolve TP/SL and direction-flip from signal (shared with live).
                    resolved = self._compute_tp_sl(
                        signal, eval_candle.close, self._config.launcher_config,
                        snapshot=snapshot, strat_cfg=strat_cfg,
                    )
                    if not _passes_protection_guard(
                        resolved=resolved,
                        guardrails_config=self._guardrails_config,
                    ):
                        continue
                    if not _passes_reward_risk_guard(
                        action=resolved.action,
                        entry_price=eval_candle.close,
                        tp_price=resolved.tp_price,
                        sl_price=resolved.sl_price,
                        guardrails_config=self._guardrails_config,
                        strategy_config=strat_cfg,
                        skip_guard=resolved.skip_rr_guard,
                    ):
                        continue
                    direction = "long" if resolved.action == "BUY" else "short"
                    self._simulator.open_position(
                        symbol=symbol,
                        direction=direction,
                        entry_price=eval_candle.close,
                        entry_ts=eval_candle.ts,
                        tp_price=resolved.tp_price,
                        sl_price=resolved.sl_price,
                        strategy_name=strategy.name,
                    )

            # Update simulator (check TP/SL at eval granularity, record equity).
            self._simulator.update_multi(step_candles)
            # Strategy-specific exits (regime-breakdown / momentum-rollover).
            self._simulator.apply_strategy_exits(step_snapshots, step_candles)

            candles_processed += 1
            if progress_cb and step % 50 == 0:
                progress_cb(BacktestProgress(phase="backtest", current=step + 1, total=max_len, message=f"Processed {step + 1}/{max_len} eval candles"))

            # Release the GIL periodically so the asyncio event loop thread
            # (which runs NiceGUI's websocket keepalive) gets CPU time.
            if step % 5 == 0:
                time.sleep(0)

        return candles_processed

    def _has_blocking_position(self, symbol: str, strategy_name: str) -> bool:
        if self._simulator.has_open_position(symbol, strategy_name):
            return True
        return (
            not self._config.allow_concurrent_strategies_per_symbol
            and self._simulator.has_open_position(symbol)
        )

    def _symbol_in_universe(self, symbol: str, strategy_name: str, ts: int) -> bool:
        """Return whether *strategy_name* may evaluate *symbol* at time *ts*.

        In "explicit" mode every configured symbol is always allowed.  In
        "screener" mode the reconstructed schedule decides: a strategy may only
        evaluate symbols in its SC/MR list for the interval active at *ts*.
        Before the first interval (or when the schedule is empty) the
        configured fallback list is used, matching live's behaviour of trading
        the configured pairs until the first screener run.
        """
        if self._config.universe_mode != "screener" or self._universe_schedule is None:
            return True
        universe = self._universe_schedule.universe_at(ts, strategy_name)
        if universe is None:
            # No screener selection yet — fall back to the configured list.
            return symbol in self._config.symbols
        return symbol in universe

    def _get_last_price(self, symbol: str) -> float | None:
        """Return the current price for a symbol (from the backtest window)."""
        return self._current_prices.get(symbol)

    def _compute_benchmark(self, symbol_candles: dict[str, list[Candle]]) -> dict[str, Any] | None:
        """Return an equal-weight synchronized buy-and-hold portfolio benchmark."""
        if not symbol_candles:
            return None
        candles_by_symbol = {
            symbol: [
                candle for candle in symbol_candles[symbol]
                if self._config.start_ts <= candle.ts <= self._config.end_ts
            ]
            for symbol in self._symbols
            if symbol in symbol_candles
        }
        if not candles_by_symbol:
            return None
        return compute_equal_weight_buy_and_hold(candles_by_symbol, self._config.initial_capital)

    def _compute_tp_sl(
        self,
        signal: Any,
        last_price: float,
        launcher_config: dict[str, Any],
        snapshot: dict[str, Any] | None = None,
        strat_cfg: dict[str, Any] | None = None,
    ) -> LauncherTpSl:
        """Resolve TP/SL (and direction-flip) for a strategy signal.

        Delegates to :func:`resolve_launcher_tp_sl`, the same function used
        by ``MarketService.build_launcher_decisions`` live — see
        app/services/launcher_tp_sl.py. This guarantees the backtest applies
        identical static/ATR/structural fallback, PnL%-mode conversion,
        Mean-Reversion dynamic-TP, and per-strategy direction-flip logic that
        live uses, instead of a hand-mirrored subset.
        """
        # The snapshot is single-symbol in backtest — grab the first symbol's
        # per-symbol block (dynamic-TP resolves its own analysis timeframe
        # block internally via resolve_analysis_block).
        _md = (snapshot or {}).get("market_data") or {}
        sym_data = next(iter(_md.values()), {}) or {}
        guardrails_config = self._guardrails_config or launcher_config.get("guardrails") or {}
        return resolve_launcher_tp_sl(
            strategy_name=signal.strategy_name,
            direction=signal.direction,
            signal_tp_pct=signal.tp_pct,
            signal_sl_pct=signal.sl_pct,
            last_price=last_price,
            launcher_config=launcher_config,
            guardrails_config=guardrails_config,
            sym_data=sym_data,
        )



def _extract_float(value: Any) -> float | None:
    """Extract a float from a config value, returning None on failure.

    Mirrors ``MarketService._extract_float``.
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _spread_series_from_tape(
    tape: list[dict[str, Any]],
    window: int,
) -> list[dict[str, float | int]]:
    """Build a rolling Roll spread series from a trade tape.

    Groups trades into ``window``-sized chunks and estimates the spread from
    each chunk's prices, timestamped at the chunk's last trade.  Returns an
    empty list when the tape is too short.
    """
    if len(tape) < 3 or window < 2:
        return []
    series: list[dict[str, float | int]] = []
    for i in range(window, len(tape) + 1):
        chunk = tape[i - window:i]
        prices = [float(row["px"]) for row in chunk if row.get("px")]
        spread = roll_spread_bps(prices)
        if spread is None:
            continue
        series.append({"ts": int(chunk[-1]["ts"]), "spread_bps": round(spread, 6)})
    return series


def _candles_per_year(timeframe: str) -> int:
    """Return the number of bars per year for a timeframe string (approx)."""
    ms = timeframe_ms(timeframe)
    if ms <= 0:
        return 0
    return int(round(365.0 * 24 * 60 * 60 * 1000 / ms))


def _passes_reward_risk_guard(
    *,
    action: str,
    entry_price: float,
    tp_price: float | None,
    sl_price: float | None,
    guardrails_config: dict[str, Any],
    strategy_config: dict[str, Any],
    skip_guard: bool = False,
) -> bool:
    """Return whether a launcher entry satisfies the live R:R guardrail."""
    if skip_guard or not tp_price or not sl_price or entry_price <= 0:
        return True

    guardrails = guardrails_config or {}
    min_rr = _extract_float(guardrails.get("min_reward_risk_ratio")) or 1.0
    strategy_rr = _extract_float(strategy_config.get("min_reward_risk_ratio"))
    if strategy_rr is not None:
        min_rr = strategy_rr

    if action == "BUY":
        tp_distance = tp_price - entry_price
        sl_distance = entry_price - sl_price
    else:
        tp_distance = entry_price - tp_price
        sl_distance = sl_price - entry_price

    if sl_distance <= 0:
        return True
    if tp_distance <= 0:
        return False
    return (tp_distance / sl_distance) >= min_rr - 1e-6


def _passes_protection_guard(
    *,
    resolved: LauncherTpSl,
    guardrails_config: dict[str, Any],
) -> bool:
    """Return whether an entry has the protection required by live config."""
    guardrails = guardrails_config or {}
    if not bool(guardrails.get("require_protection", False)) or resolved.disable_protection:
        return True
    return (
        isinstance(resolved.tp_price, (int, float))
        and resolved.tp_price > 0
        and isinstance(resolved.sl_price, (int, float))
        and resolved.sl_price > 0
    )
