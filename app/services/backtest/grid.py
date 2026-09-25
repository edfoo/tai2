"""Parameter-sweep (grid) runner for backtesting.

Runs the :class:`BacktestEngine` across the Cartesian product of a set of
parameter values, collects the results, and ranks them by a configurable
metric (default: Sharpe per candle).

The grid runner reuses the same :class:`BacktestEngine` for each
combination, so every run goes through the full fetch → snapshot →
simulate → metrics pipeline.  Fetched OHLCV data is cached by the
:class:`HistoricalDataFetcher` file cache, so re-runs with different
parameters but the same symbols/timeframe/date-range are fast (no
re-fetch).

Usage::

    from app.services.backtest.grid import BacktestGrid
    from app.services.backtest.models import BacktestConfig, GridConfig, GridParamDef

    base = BacktestConfig(symbols=["BTC-USDT-SWAP"], timeframe="15m", ...)
    grid_cfg = GridConfig(
        base_config=base,
        params=[
            GridParamDef("strategies.mean_reversion.rsi_oversold", [25, 30, 35]),
            GridParamDef("strategies.mean_reversion.max_adx", [20, 25, 30]),
        ],
        rank_by="sharpe_per_candle",
        min_trades=5,
    )
    grid = BacktestGrid(grid_cfg)
    result = await grid.run(progress_cb=my_callback)
"""

from __future__ import annotations

import asyncio
import copy
import ctypes
import itertools
import logging
import math
import os
import random
import signal
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.backtest.engine import BacktestEngine
from app.services.backtest.data_fetcher import timeframe_ms
from app.services.backtest.models import (
    BacktestConfig,
    BacktestResult,
    EquityPoint,
    GridConfig,
    GridProgress,
    GridResult,
    GridRunResult,
)
from app.services.backtest.runner import walk_forward_splits

logger = logging.getLogger(__name__)


def _default_workers() -> int:
    """Number of parallel grid workers: ``BACKTEST_WORKERS`` env or CPU count."""
    raw = os.environ.get("BACKTEST_WORKERS", "")
    if raw.strip():
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning("Invalid BACKTEST_WORKERS=%r", raw)
    return max(1, os.cpu_count() or 1)


def _worker_initializer() -> None:
    """Die with the parent (server) so a hard-killed server reaps its workers.

    ``prctl(PR_SET_PDEATHSIG, SIGTERM)`` tells the kernel to SIGTERM this worker
    the instant its parent dies, even on SIGKILL.  The getppid() re-check closes
    the fork-vs-parent-death race.
    """
    parent_pid = os.getppid()
    try:
        _libc = ctypes.CDLL("libc.so.6", use_errno=True)
        _libc.prctl(1, signal.SIGTERM)
    except (AttributeError, OSError):
        pass
    if os.getppid() != parent_pid:
        os._exit(0)


def _run_combination(config: BacktestConfig) -> BacktestResult:
    """Run one grid combination **in a subprocess** (module-level → picklable).

    Each combination is an independent engine run, so it is dispatched to its
    own process (one per core) — the same model the old headless
    ``run_grid_sweep.py`` used.  Only the picklable ``BacktestConfig`` crosses
    the process boundary.
    """
    return asyncio.run(BacktestEngine(config).run())


class BacktestGrid:
    """Orchestrates a parameter-sweep backtest run.

    Parameters
    ----------
    config:
        A :class:`GridConfig` specifying the base backtest config and the
        parameters to sweep.
    workers:
        Number of parallel subprocesses (one per core by default).
    """

    def __init__(self, config: GridConfig, *, workers: int | None = None) -> None:
        self._config = config
        self._workers = workers

    async def run(
        self,
        progress_cb: Callable[[GridProgress], None] | None = None,
    ) -> GridResult:
        """Execute the grid sweep and return ranked results."""
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        result = GridResult(config=self._config, started_at=started.isoformat())

        try:
            # ── Build the Cartesian product of all parameter values ─────
            param_keys = [p.key for p in self._config.params]
            param_labels = [p.label or p.key for p in self._config.params]
            value_lists = [p.values for p in self._config.params]
            if self._config.search_mode not in {"exhaustive", "random"}:
                result.error = f"Unsupported search mode: {self._config.search_mode}"
                result.finished_at = datetime.now(timezone.utc).isoformat()
                result.duration_seconds = round(time.monotonic() - t0, 3)
                return result
            combinations, total_combinations = _build_combinations(
                value_lists,
                search_mode=self._config.search_mode,
                budget=self._config.combination_budget,
                seed=self._config.random_seed,
            )
            result.total_combinations = total_combinations
            result.search_mode = self._config.search_mode
            result.random_seed = self._config.random_seed
            total = len(combinations)
            result.attempted_combinations = total

            if total == 0:
                result.error = "No parameter combinations — add at least one GridParamDef with values."
                result.finished_at = datetime.now(timezone.utc).isoformat()
                result.duration_seconds = round(time.monotonic() - t0, 3)
                return result

            logger.info(
                "BacktestGrid: %d combinations across %d params: %s",
                total, len(param_keys), param_keys,
            )

            if progress_cb:
                progress_cb(GridProgress(phase="grid", current=0, total=total, message="Starting sweep"))

            # ── Run each combination in parallel across a process pool ──
            # Each combination is an independent engine run, so it can run on
            # its own core (one process per combination, bounded by ``workers``).
            # The batch is built up-front so it can be submitted at once.
            jobs: list[tuple[dict[str, Any], dict[str, Any], BacktestConfig]] = []
            for combo in combinations:
                param_values = dict(zip(param_keys, combo))
                label_values = dict(zip(param_labels, combo))
                bt_config = copy.deepcopy(self._config.base_config)
                _apply_params(bt_config, param_values)
                jobs.append((param_values, label_values, bt_config))

            validation_windows: list[tuple[int, int]] = []
            if self._config.validation_folds:
                splits = walk_forward_splits(
                    start_ts=self._config.base_config.start_ts,
                    end_ts=self._config.base_config.end_ts,
                    folds=self._config.validation_folds,
                    train_ratio=self._config.validation_train_ratio,
                )
                if len(splits) != self._config.validation_folds:
                    result.error = "Validation folds do not fit the requested date range."
                    result.finished_at = datetime.now(timezone.utc).isoformat()
                    result.duration_seconds = round(time.monotonic() - t0, 3)
                    return result
                validation_windows = [(split[2], split[3]) for split in splits]
            fold_count = len(validation_windows) or 1

            workers = self._workers if self._workers and self._workers > 0 else _default_workers()
            workers = min(workers, total)

            completed = 0
            ordered_results: dict[int, dict[int, tuple[BacktestResult | None, str | None]]] = {}

            with ProcessPoolExecutor(max_workers=workers, initializer=_worker_initializer) as ex:
                future_to_job: dict[Any, tuple[int, int]] = {}
                for idx, (_pv, _lv, cfg) in enumerate(jobs):
                    windows = validation_windows or [(cfg.start_ts, cfg.end_ts)]
                    for fold_idx, (test_start, test_end) in enumerate(windows):
                        fold_config = copy.deepcopy(cfg)
                        fold_config.start_ts = test_start
                        fold_config.end_ts = test_end
                        future_to_job[ex.submit(_run_combination, fold_config)] = (idx, fold_idx)

                for fut in as_completed(future_to_job):
                    idx, fold_idx = future_to_job[fut]
                    param_values, label_values, _cfg = jobs[idx]
                    try:
                        bt_result = fut.result()
                        bt_error = None
                    except Exception as exc:  # noqa: BLE001 - a bad combination shouldn't kill the sweep
                        logger.exception("BacktestGrid: combination %d failed", idx + 1)
                        bt_result = None
                        bt_error = str(exc)

                    if bt_result is not None and bt_result.is_error:
                        logger.warning(
                            "BacktestGrid: combination %d fold %d errored: %s",
                            idx + 1, fold_idx + 1, bt_result.error,
                        )
                        bt_error = bt_result.error
                        bt_result = None
                    ordered_results.setdefault(idx, {})[fold_idx] = (bt_result, bt_error)

                    completed += 1
                    if progress_cb:
                        combo_str = ", ".join(f"{k}={v}" for k, v in label_values.items())
                        fold_note = f", fold {fold_idx + 1}/{fold_count}" if fold_count > 1 else ""
                        progress_cb(GridProgress(
                            phase="grid",
                            current=completed,
                            total=total * fold_count,
                            message=f"Run {completed}/{total * fold_count}{fold_note}: {combo_str}",
                        ))

            # ── Assemble in deterministic (submission) order ────────────
            for idx in range(total):
                fold_rows = ordered_results.get(idx, {})
                successful = [
                    (fold_idx, row[0]) for fold_idx, row in sorted(fold_rows.items())
                    if row[0] is not None
                ]
                if not successful:
                    continue
                fold_results = [bt for _fold_idx, bt in successful]
                fold_metrics = [
                    {
                        "fold": fold_idx + 1,
                        "start_ts": validation_windows[fold_idx][0] if validation_windows else jobs[idx][2].start_ts,
                        "end_ts": validation_windows[fold_idx][1] if validation_windows else jobs[idx][2].end_ts,
                        "metrics": dict(bt.metrics or {}),
                    }
                    for fold_idx, bt in successful
                ]
                fold_scores = [
                    _extract_metric(bt, self._config.rank_by)
                    for bt in fold_results
                ]
                valid_scores = [score for score in fold_scores if score is not None]
                rank_score = (
                    sum(valid_scores) / len(valid_scores)
                    if valid_scores and len(successful) == fold_count and len(valid_scores) == fold_count
                    else None
                )
                aggregate = _aggregate_fold_results(fold_results)
                aggregate.config = jobs[idx][2]
                total_trades = sum(int(bt.metrics.get("total_trades", 0)) for bt in fold_results)
                below_min = total_trades < self._config.min_trades
                result.runs.append(GridRunResult(
                    params=jobs[idx][1],
                    result=aggregate,
                    rank_score=rank_score,
                    below_min_trades=below_min or len(successful) != fold_count,
                    fold_metrics=fold_metrics,
                ))

            # ── Rank results ────────────────────────────────────────────
            # Sort by rank_score descending.  Runs with None score or
            # below_min_trades go to the bottom.
            def _sort_key(r: GridRunResult) -> tuple[int, float]:
                if r.rank_score is None or r.below_min_trades:
                    return (0, 0.0)
                return (1, r.rank_score)

            result.ranked = sorted(result.runs, key=_sort_key, reverse=True)

            if progress_cb:
                progress_cb(GridProgress(
                    phase="done", current=total * fold_count, total=total * fold_count,
                    message="Sweep complete",
                ))

        except Exception as exc:
            logger.exception("BacktestGrid failed")
            result.error = str(exc)
            if progress_cb:
                progress_cb(GridProgress(phase="error", current=0, total=0, message=str(exc)))

        result.finished_at = datetime.now(timezone.utc).isoformat()
        result.duration_seconds = round(time.monotonic() - t0, 3)
        return result


# ── Helpers ─────────────────────────────────────────────────────────────


def _apply_params(config: BacktestConfig, params: dict[str, Any]) -> None:
    """Apply swept parameter values to a backtest config's launcher_config.

    Supports dotted paths where the first segment is either a top-level
    launcher_config key or ``strategies.<strategy_name>.<param>``.

    Examples::

        _apply_params(config, {"tp_pct": 2.0})
        _apply_params(config, {"strategies.mean_reversion.rsi_oversold": 25})
    """
    for key, value in params.items():
        _set_nested(config.launcher_config, key, value)


def _set_nested(d: dict[str, Any], key: str, value: Any) -> None:
    """Set a nested dict value using a dotted path.

    Creates intermediate dicts as needed.  The ``strategies`` key is
    expected to be a dict of strategy-name → dict.
    """
    parts = key.split(".")
    cur = d
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def _extract_metric(result: Any, key: str) -> float | None:
    """Extract a numeric metric from a BacktestResult.

    Falls back to per_strategy metrics if the aggregate metric is missing
    or zero (e.g. when only one strategy was enabled and the aggregate
    is dominated by that strategy's numbers).
    """
    val = result.metrics.get(key)
    if val is not None and isinstance(val, (int, float)) and val == val:  # NaN check
        return float(val)
    # Try per-strategy (take the best non-zero value).
    best: float | None = None
    for _name, sm in (result.per_strategy or {}).items():
        sv = sm.get(key)
        if sv is not None and isinstance(sv, (int, float)) and sv == sv:
            if best is None or sv > best:
                best = float(sv)
    return best


def _aggregate_fold_results(results: list[BacktestResult]) -> BacktestResult:
    """Combine chronological fold trades/equity while preserving per-fold scoring separately."""
    if len(results) == 1:
        return results[0]

    aggregate = BacktestResult(config=results[0].config)
    aggregate.trades = [trade for fold in results for trade in fold.trades]
    offset = 0.0
    for fold in results:
        for point in fold.equity_curve:
            aggregate.equity_curve.append(EquityPoint(
                ts=point.ts,
                equity=point.equity + offset,
                open_positions=point.open_positions,
            ))
        if fold.equity_curve:
            offset += fold.equity_curve[-1].equity - fold.config.initial_capital
    from app.services.backtest.metrics import compute_metrics, compute_per_strategy_metrics, compute_per_symbol_metrics

    aggregate.metrics = compute_metrics(
        aggregate.trades,
        aggregate.equity_curve,
        results[0].config.initial_capital,
        candles_per_year=_candles_per_year(results[0].config),
    )
    aggregate.per_strategy = compute_per_strategy_metrics(aggregate.trades)
    aggregate.per_symbol = compute_per_symbol_metrics(aggregate.trades)
    aggregate.candles_processed = sum(fold.candles_processed for fold in results)
    aggregate.duration_seconds = sum(fold.duration_seconds for fold in results)
    return aggregate


def _candles_per_year(config: BacktestConfig) -> int:
    timeframe = (
        config.evaluation_timeframe
        if config.evaluation_mode == "finer_ltf"
        else config.timeframe
    )
    interval_ms = timeframe_ms(timeframe)
    if interval_ms <= 0:
        return 0
    return int(round(365.0 * 24 * 60 * 60 * 1000 / interval_ms))


def _combination_at_index(value_lists: list[list[Any]], index: int) -> tuple[Any, ...]:
    """Decode a row-major Cartesian-product index without materializing the product."""
    values: list[Any] = [None] * len(value_lists)
    for position in range(len(value_lists) - 1, -1, -1):
        radix = len(value_lists[position])
        index, digit = divmod(index, radix)
        values[position] = value_lists[position][digit]
    return tuple(values)


def _build_combinations(
    value_lists: list[list[Any]],
    *,
    search_mode: str,
    budget: int,
    seed: int,
) -> tuple[list[tuple[Any, ...]], int]:
    """Build all combinations or a reproducible bounded random sample."""
    total = math.prod(len(values) for values in value_lists)
    if search_mode == "random" and budget > 0 and budget < total:
        rng = random.Random(seed)
        indices: set[int] = set()
        while len(indices) < budget:
            indices.add(rng.randrange(total))
        return [_combination_at_index(value_lists, idx) for idx in sorted(indices)], total
    return list(itertools.product(*value_lists)), total
