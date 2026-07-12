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

import copy
import itertools
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.backtest.engine import BacktestEngine
from app.services.backtest.models import (
    BacktestConfig,
    GridConfig,
    GridProgress,
    GridResult,
    GridRunResult,
)

logger = logging.getLogger(__name__)


class BacktestGrid:
    """Orchestrates a parameter-sweep backtest run.

    Parameters
    ----------
    config:
        A :class:`GridConfig` specifying the base backtest config and the
        parameters to sweep.
    """

    def __init__(self, config: GridConfig) -> None:
        self._config = config

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
            combinations = list(itertools.product(*value_lists))
            total = len(combinations)

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

            # ── Run each combination ────────────────────────────────────
            for idx, combo in enumerate(combinations):
                param_values = dict(zip(param_keys, combo))
                label_values = dict(zip(param_labels, combo))

                if progress_cb:
                    combo_str = ", ".join(f"{k}={v}" for k, v in label_values.items())
                    progress_cb(GridProgress(
                        phase="grid",
                        current=idx,
                        total=total,
                        message=f"Run {idx + 1}/{total}: {combo_str}",
                    ))

                # Deep-copy the base config and override the swept parameters.
                bt_config = copy.deepcopy(self._config.base_config)
                _apply_params(bt_config, param_values)

                # Run the engine for this combination.
                engine = BacktestEngine(bt_config)
                try:
                    bt_result = await engine.run()
                except Exception as exc:
                    logger.exception("BacktestGrid: run %d failed", idx + 1)
                    bt_result = None
                    _err = str(exc)

                # Extract the rank metric.
                rank_score: float | None = None
                below_min = False
                if bt_result is not None and not bt_result.is_error:
                    rank_score = _extract_metric(bt_result, self._config.rank_by)
                    total_trades = bt_result.metrics.get("total_trades", 0)
                    below_min = total_trades < self._config.min_trades
                elif bt_result is not None and bt_result.is_error:
                    logger.warning(
                        "BacktestGrid: run %d errored: %s", idx + 1, bt_result.error
                    )

                run_result = GridRunResult(
                    params=label_values,
                    result=bt_result,
                    rank_score=rank_score,
                    below_min_trades=below_min,
                )
                result.runs.append(run_result)

            # ── Rank results ────────────────────────────────────────────
            # Sort by rank_score descending.  Runs with None score or
            # below_min_trades go to the bottom.
            def _sort_key(r: GridRunResult) -> tuple[int, float]:
                if r.rank_score is None or r.below_min_trades:
                    return (0, 0.0)
                return (1, r.rank_score)

            result.ranked = sorted(result.runs, key=_sort_key, reverse=True)

            if progress_cb:
                progress_cb(GridProgress(phase="done", current=total, total=total, message="Sweep complete"))

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
