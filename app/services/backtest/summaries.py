"""Compact per-run evidence summaries for parameter-sweep results.

The grid runner produces a full :class:`BacktestResult` per combination, but a
sweep of hundreds of combinations cannot hand every trade list and equity curve
to a human or an expert/LLM.  This module distils each run into a small,
JSON-safe summary that carries the evidence needed to judge a candidate:

  * **headline metrics** — return, risk, significance (t-stat / CI95),
    drawdown, Sharpe/Sortino/Calmar, R-multiples, time-in-market, costs;
  * **fold summary** — dispersion, worst fold, and sign consistency across
    validation folds (a candidate that only works in one fold is fragile);
  * **symbol summary** — per-symbol net share (concentration risk);
  * **benchmark delta** — did the candidate beat equal-weight buy-and-hold?

Everything here is pure and dependency-light: it consumes plain metric dicts
(the output of :func:`app.services.backtest.metrics.compute_metrics`) and
returns plain dicts, so the grid runner, the research bundle, the UI, and the
CLI all report identically.
"""

from __future__ import annotations

import math
from typing import Any, Iterable

# Headline metric keys copied verbatim into a run summary (when present).
_SUMMARY_METRIC_KEYS: tuple[str, ...] = (
    "total_trades",
    "net_profit_after_cost",
    "net_profit_after_cost_pct",
    "net_profit_factor_after_cost",
    "net_win_rate_after_cost_pct",
    "net_expectancy_after_cost",
    "net_expectancy_t_stat",
    "net_expectancy_ci95_low_normal_approx",
    "net_expectancy_ci95_high_normal_approx",
    "net_trade_pnl_stddev",
    "net_trade_return_stddev_pct",
    "max_drawdown_pct",
    "max_drawdown_duration_bars",
    "sharpe_per_candle",
    "sharpe_annualized",
    "sortino_annualized",
    "calmar_ratio",
    "avg_r_multiple",
    "median_r_multiple",
    "time_in_market_pct",
    "average_concurrent_positions",
    "max_concurrent_positions",
    "avg_candles_held",
    "total_cost",
    "total_fees",
    "total_funding",
    "total_slippage_cost",
    "win_rate",
    "profit_factor",
)


def _num(value: Any) -> float:
    """Coerce to float, treating None/bool/NaN as 0.0 (inf preserved)."""
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed == parsed else 0.0  # NaN -> 0.0


def _finite(value: Any) -> float | None:
    """Return a finite float or ``None`` (JSON-safe)."""
    parsed = _num(value)
    if parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def run_summary(metrics: dict[str, Any] | None) -> dict[str, Any]:
    """Distil a run's metrics into a compact, JSON-safe evidence summary."""
    metrics = metrics or {}
    summary: dict[str, Any] = {}
    for key in _SUMMARY_METRIC_KEYS:
        if key in metrics:
            summary[key] = _finite(metrics.get(key))
    exit_reasons = metrics.get("exit_reasons")
    if isinstance(exit_reasons, dict):
        summary["exit_reasons"] = dict(exit_reasons)
    return summary


def fold_summary(fold_metrics: Iterable[dict[str, Any]] | None) -> dict[str, Any]:
    """Summarise validation-fold dispersion and sign consistency.

    Reports the per-fold after-cost net PnL, its mean/stddev, the worst and
    best folds, and the fraction of completed folds that were profitable.
    """
    folds = [f for f in (fold_metrics or []) if isinstance(f, dict)]
    completed = [f for f in folds if f.get("status") == "completed"]
    nets = [
        _num((f.get("metrics") or {}).get("net_profit_after_cost"))
        for f in completed
    ]
    positive = sum(1 for net in nets if net > 0)
    mean = sum(nets) / len(nets) if nets else 0.0
    if len(nets) > 1:
        variance = sum((net - mean) ** 2 for net in nets) / (len(nets) - 1)
        std = math.sqrt(variance)
    else:
        std = 0.0
    return {
        "completed_folds": len(completed),
        "failed_folds": sum(1 for f in folds if f.get("status") == "failed"),
        "skipped_folds": sum(1 for f in folds if f.get("status") == "skipped"),
        "positive_folds": positive,
        "sign_consistency": round(positive / len(completed), 4) if completed else 0.0,
        "net_mean": round(mean, 4),
        "net_std": round(std, 4),
        "net_worst": round(min(nets), 4) if nets else 0.0,
        "net_best": round(max(nets), 4) if nets else 0.0,
        "per_fold_net": [round(net, 4) for net in nets],
    }


def symbol_summary(per_symbol: dict[str, Any] | None) -> dict[str, Any]:
    """Per-symbol net contribution and share of total net profit.

    Shares are only meaningful when total net profit is positive; otherwise
    they are reported as ``None``.
    """
    per_symbol = per_symbol or {}
    total = sum(
        _num((group or {}).get("net_profit_after_cost"))
        for group in per_symbol.values()
    )
    rows: list[dict[str, Any]] = []
    for symbol, group in per_symbol.items():
        net = _num((group or {}).get("net_profit_after_cost"))
        rows.append({
            "symbol": symbol,
            "net_profit_after_cost": _finite(net),
            "trades": (group or {}).get("trades"),
            "share": round(net / total, 4) if total > 0 else None,
        })
    rows.sort(key=lambda row: _num(row["net_profit_after_cost"]), reverse=True)
    max_share = None
    if total > 0 and rows:
        max_share = rows[0]["share"]
    return {
        "total_net_profit_after_cost": _finite(total),
        "max_symbol_share": max_share,
        "symbols": rows,
    }


def benchmark_delta(metrics: dict[str, Any] | None) -> dict[str, Any] | None:
    """Compare the strategy's return against the equal-weight buy-and-hold.

    The benchmark is stored by the engine under ``metrics["buy_and_hold"]``.
    Returns ``None`` when no benchmark is present.
    """
    metrics = metrics or {}
    benchmark = metrics.get("buy_and_hold")
    if not isinstance(benchmark, dict) or benchmark.get("error"):
        return None
    strategy_return = _num(metrics.get("net_profit_after_cost_pct"))
    benchmark_return = _num(benchmark.get("total_return_pct"))
    return {
        "strategy_return_pct": _finite(strategy_return),
        "buy_and_hold_return_pct": _finite(benchmark_return),
        "delta_pct": _finite(strategy_return - benchmark_return),
        "beat_benchmark": strategy_return > benchmark_return,
        "benchmark_symbols": benchmark.get("symbols"),
        "benchmark_costs_included": benchmark.get("costs_included", False),
    }


def build_run_evidence(
    metrics: dict[str, Any] | None,
    *,
    fold_metrics: Iterable[dict[str, Any]] | None = None,
    per_symbol: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the full per-run evidence block (summary + folds + symbols)."""
    return {
        "summary": run_summary(metrics),
        "fold_summary": fold_summary(fold_metrics),
        "symbol_summary": symbol_summary(per_symbol),
        "benchmark_delta": benchmark_delta(metrics),
    }
