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
from app.services.backtest.metrics import compute_metrics, compute_per_strategy_metrics
from app.services.backtest.models import (
    BacktestConfig,
    BacktestProgress,
    BacktestResult,
    Candle,
)
from app.services.backtest.simulator import Simulator
from app.services.backtest.snapshot_builder import SnapshotBuilder
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


class BacktestEngine:
    """Orchestrates a single backtest run.

    Usage::

        engine = BacktestEngine(config)
        result = await engine.run(progress_cb=my_callback)
    """

    def __init__(self, config: BacktestConfig) -> None:
        self._config = config
        self._fetcher = HistoricalDataFetcher()
        # Merge launcher + strategy config so the simulator can read both
        # max_hold_candles (launcher) and trade_management (strategy).
        _sim_cfg = dict(config.launcher_config or {})
        _strat_cfg = dict(config.strategy_config or {})
        if "trade_management" in _strat_cfg and "trade_management" not in _sim_cfg:
            _sim_cfg["trade_management"] = _strat_cfg["trade_management"]
        self._simulator = Simulator(
            initial_capital=config.initial_capital,
            notional_per_trade=float(
                (config.launcher_config or {}).get("notional_usd") or 10.0
            ),
            strategy_config=_sim_cfg,
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

        try:
            # ── Phase 1: Fetch historical data ────────────────────────
            if progress_cb:
                progress_cb(BacktestProgress(phase="fetch", current=0, total=len(self._config.symbols), message="Fetching historical data"))

            symbol_candles: dict[str, list[Candle]] = {}
            symbol_htf_candles: dict[str, list[Candle]] = {}
            symbol_eval_candles: dict[str, list[Candle]] = {}
            htf_tf = htf_for(self._config.timeframe)
            use_finer_ltf = self._eval_mode == "finer_ltf"

            for idx, symbol in enumerate(self._config.symbols):
                candles = await self._fetcher.fetch_candles(
                    symbol=symbol,
                    timeframe=self._config.timeframe,
                    start_ts=self._config.start_ts,
                    end_ts=self._config.end_ts,
                    warmup_candles=self._config.warmup_candles,
                    progress_cb=lambda done, total, msg: progress_cb(
                        BacktestProgress(phase="fetch", current=idx, total=len(self._config.symbols), message=f"{symbol}: {msg}")
                    ) if progress_cb else None,
                )
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
                    symbol_htf_candles[symbol] = htf_candles

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
                    symbol_eval_candles[symbol] = eval_candles

                if progress_cb:
                    progress_cb(BacktestProgress(phase="fetch", current=idx + 1, total=len(self._config.symbols), message=f"{symbol}: {len(candles)} candles"))

            # ── Phase 2: Build snapshot builders ──────────────────────
            snapshot_builders: dict[str, SnapshotBuilder] = {}
            for symbol in self._config.symbols:
                snapshot_builders[symbol] = SnapshotBuilder(
                    symbol=symbol,
                    ltf_candles=symbol_candles[symbol],
                    htf_candles=symbol_htf_candles.get(symbol),
                    ltf_timeframe=self._config.timeframe,
                )

            # ── Phase 3: Determine backtest window ────────────────────
            # Find the index of the first candle at or after start_ts for
            # each symbol (after warmup).
            stepping_candles = symbol_eval_candles if use_finer_ltf else symbol_candles
            start_indices: dict[str, int] = {}
            for symbol in self._config.symbols:
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
                for s in self._config.symbols
                if stepping_candles[s]
            ) if self._config.symbols else 0

            if max_len == 0:
                result.error = "No candles found in the specified date range."
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
            result.metrics = compute_metrics(all_trades, self._simulator.equity_curve, self._config.initial_capital)
            result.per_strategy = compute_per_strategy_metrics(all_trades)
            result.candles_processed = candles_processed

            if progress_cb:
                progress_cb(BacktestProgress(phase="done", current=1, total=1, message="Backtest complete"))

        except Exception as exc:
            logger.exception("Backtest failed")
            result.error = str(exc)
            if progress_cb:
                progress_cb(BacktestProgress(phase="error", current=0, total=0, message=str(exc)))

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
            for symbol in self._config.symbols:
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
            for symbol, candle in step_candles.items():
                builder = snapshot_builders[symbol]
                # The window includes all candles up to the current one.
                candle_idx = start_indices[symbol] + step
                snapshot = builder.build(candle_idx)

                # Evaluate each selected strategy.
                for strategy in self._strategies:
                    strat_cfg = strategies_cfg.get(strategy.name) or {}
                    if not strat_cfg.get("enabled", False):
                        continue
                    # Per-strategy position guard: skip if already in position.
                    if self._simulator.has_open_position(symbol, strategy.name):
                        continue
                    signal = strategy.evaluate(symbol, snapshot, strat_cfg, self._helpers)
                    if signal is None:
                        continue
                    # Compute TP/SL prices from signal (matching live logic).
                    tp_price, sl_price = self._compute_tp_sl(
                        signal, candle.close, self._config.launcher_config,
                        snapshot=snapshot, strat_cfg=strat_cfg,
                    )
                    direction = "long" if signal.direction == "buy" else "short"
                    self._simulator.open_position(
                        symbol=symbol,
                        direction=direction,
                        entry_price=candle.close,
                        entry_ts=candle.ts,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        strategy_name=strategy.name,
                    )

            # Update simulator (check TP/SL, record equity).
            self._simulator.update_multi(step_candles)

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
        closed_window: dict[str, list[Candle]] = {s: [] for s in self._config.symbols}
        acc: dict[str, dict[str, Any] | None] = {s: None for s in self._config.symbols}

        # Pre-seed closed_window with LTF candles whose ts < the first eval
        # candle's LTF bucket.  This gives indicators enough warmup history
        # on the first step (the LTF fetch already includes warmup_candles).
        for symbol in self._config.symbols:
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
            for symbol in self._config.symbols:
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

                # Evaluate each selected strategy.
                for strategy in self._strategies:
                    strat_cfg = strategies_cfg.get(strategy.name) or {}
                    if not strat_cfg.get("enabled", False):
                        continue
                    # Per-strategy position guard: skip if already in position.
                    if self._simulator.has_open_position(symbol, strategy.name):
                        continue
                    signal = strategy.evaluate(symbol, snapshot, strat_cfg, self._helpers)
                    if signal is None:
                        continue
                    # Compute TP/SL prices from signal (matching live logic).
                    tp_price, sl_price = self._compute_tp_sl(
                        signal, eval_candle.close, self._config.launcher_config,
                        snapshot=snapshot, strat_cfg=strat_cfg,
                    )
                    direction = "long" if signal.direction == "buy" else "short"
                    self._simulator.open_position(
                        symbol=symbol,
                        direction=direction,
                        entry_price=eval_candle.close,
                        entry_ts=eval_candle.ts,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        strategy_name=strategy.name,
                    )

            # Update simulator (check TP/SL at eval granularity, record equity).
            self._simulator.update_multi(step_candles)

            candles_processed += 1
            if progress_cb and step % 50 == 0:
                progress_cb(BacktestProgress(phase="backtest", current=step + 1, total=max_len, message=f"Processed {step + 1}/{max_len} eval candles"))

            # Release the GIL periodically so the asyncio event loop thread
            # (which runs NiceGUI's websocket keepalive) gets CPU time.
            if step % 5 == 0:
                time.sleep(0)

        return candles_processed

    def _get_last_price(self, symbol: str) -> float | None:
        """Return the current price for a symbol (from the backtest window)."""
        return self._current_prices.get(symbol)

    def _compute_tp_sl(
        self,
        signal: Any,
        last_price: float,
        launcher_config: dict[str, Any],
        snapshot: dict[str, Any] | None = None,
        strat_cfg: dict[str, Any] | None = None,
    ) -> tuple[float | None, float | None]:
        """Compute TP/SL prices from a strategy signal.

        Mirrors ``build_launcher_decisions()`` in market_service.py:
            BUY:  tp = last * (1 + tp_pct/100),  sl = last * (1 - sl_pct/100)
            SELL: tp = last * (1 - tp_pct/100),  sl = last * (1 + sl_pct/100)

        Also mirrors the **Dynamic TP** logic for Mean Reversion: when
        ``strat_cfg["dynamic_tp"]`` is True, the effective TP is tightened
        using the current BB bandwidth (``min(static_tp, bandwidth/2 × fraction)``).
        This previously only existed in the live path, causing backtests to
        diverge from live trade behaviour.
        """
        _disable_protection = False
        if signal.strategy_name == "mean_reversion" and strat_cfg is not None:
            _mr_use_atr = bool(strat_cfg.get("use_atr_sizing", True))
            _mr_use_struct = bool(strat_cfg.get("use_structural_sizing", True))
            _mr_static_tp = _extract_float(strat_cfg.get("tp_pct"))
            _mr_static_sl = _extract_float(strat_cfg.get("sl_pct"))
            if not _mr_use_atr and not _mr_use_struct and _mr_static_tp is None and _mr_static_sl is None:
                _disable_protection = True

        tp_pct = signal.tp_pct
        sl_pct = signal.sl_pct
        if not _disable_protection:
            if tp_pct is None:
                tp_pct = _extract_float(launcher_config.get("tp_pct"))
            if sl_pct is None:
                sl_pct = _extract_float(launcher_config.get("sl_pct"))

        # ── Dynamic TP (Mean Reversion only) ──────────────────────────
        # Tighten TP using BB bandwidth at entry.  Mirrors the live logic in
        # ``MarketService.build_launcher_decisions`` so backtests reproduce
        # live trade behaviour when dynamic_tp is enabled.
        # Disabled when use_atr_sizing is True — ATR sizing already adapts
        # TP to volatility, so dynamic_tp would double-tighten it.
        effective_tp_pct = tp_pct
        if (
            signal.strategy_name == "mean_reversion"
            and snapshot is not None
            and strat_cfg is not None
            and bool(strat_cfg.get("dynamic_tp", False))
            and not bool(strat_cfg.get("use_atr_sizing", False))
            and tp_pct is not None
            and tp_pct > 0
        ):
            dynamic_tp_fraction = _extract_float(strat_cfg.get("dynamic_tp_fraction")) or 0.7
            # The snapshot is single-symbol in backtest — grab the first
            # symbol's indicator block.
            _md = snapshot.get("market_data") or {}
            sym_data = next(iter(_md.values()), {}) or {}
            sym_indicators = sym_data.get("indicators") or {}
            _bb = sym_indicators.get("bollinger_bands") or {}
            _bb_lower = _extract_float(_bb.get("lower"))
            _bb_upper = _extract_float(_bb.get("upper"))
            _bb_middle = _extract_float(_bb.get("middle"))
            if (
                _bb_lower is not None
                and _bb_upper is not None
                and _bb_middle is not None
                and _bb_middle > 0
            ):
                _bw = (_bb_upper - _bb_lower) / _bb_middle * 100.0
                _dyn = (_bw / 2.0) * dynamic_tp_fraction * 1.0  # leverage = 1.0
                if _dyn > 0:
                    effective_tp_pct = min(tp_pct, _dyn)
                    logger.debug(
                        "[backtest] Dynamic TP: bw=%.2f%% frac=%.2f → dyn=%.2f%% → eff=%.2f%%",
                        _bw, dynamic_tp_fraction, _dyn, effective_tp_pct,
                    )

        tp_price: float | None = None
        sl_price: float | None = None
        if effective_tp_pct and effective_tp_pct > 0:
            if signal.direction == "buy":
                tp_price = last_price * (1 + effective_tp_pct / 100.0)
            else:
                tp_price = last_price * (1 - effective_tp_pct / 100.0)
        if sl_pct and sl_pct > 0:
            if signal.direction == "buy":
                sl_price = last_price * (1 - sl_pct / 100.0)
            else:
                sl_price = last_price * (1 + sl_pct / 100.0)
        return tp_price, sl_price


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
