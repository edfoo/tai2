"""Compact, bounded sweep report for expert / LLM handoff.

A full :class:`GridResult` embeds every run's trade list and equity curve, which
is impractical to hand to an analyst or an LLM.  This module builds a
**bounded, self-describing** report from a serialised grid payload:

  * config + provenance + assumptions (reproducibility),
  * baseline and buy-and-hold benchmark,
  * **top-N runs (summary only)** — no trade lists or equity curves,
  * marginal sensitivity, interactions, plateaus, robustness,
  * the deterministic recommendation,
  * caveats.

The report is JSON-safe, schema-versioned, and stable so downstream consumers
(the UI "Copy for analysis" button, the CLI ``--emit-report``, or an LLM prompt
template) can rely on its shape.  It is a *screening aid*, not a profitability
guarantee.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.backtest.recommendations import (
    build_recommendation,
    render_recommendation_markdown,
)
from app.services.backtest.sweep_analysis import DEFAULT_RANK_BY, analyze_sweep

SWEEP_REPORT_SCHEMA_VERSION = 1


def _num(value: Any) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed == parsed else 0.0


def _finite(value: Any) -> float | None:
    parsed = _num(value)
    if parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _sweep_entries(grid: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for run in grid.get("runs") or []:
        if not isinstance(run, dict):
            continue
        result = run.get("result") or {}
        entries.append({
            "params": run.get("params") or {},
            "metrics": result.get("metrics") or {},
        })
    return entries


def _top_runs(grid: dict[str, Any], top_n: int) -> list[dict[str, Any]]:
    """Return the top-N ranked runs as compact summaries (no trades/equity)."""
    runs = grid.get("runs") or []
    ranked_indexes = grid.get("ranked_indexes") or []
    ordered: list[dict[str, Any]] = []
    if ranked_indexes:
        for idx in ranked_indexes:
            if isinstance(idx, int) and 0 <= idx < len(runs):
                ordered.append(runs[idx])
    else:
        scored = [r for r in runs if isinstance(r, dict) and r.get("rank_score") is not None]
        ordered = sorted(scored, key=lambda r: _num(r.get("rank_score")), reverse=True)

    rows: list[dict[str, Any]] = []
    for run in ordered[:top_n]:
        result = run.get("result") or {}
        metrics = result.get("metrics") or {}
        evidence = run.get("evidence") or {}
        summary = evidence.get("summary") or {}
        rows.append({
            "params": run.get("params") or {},
            "rank_score": _finite(run.get("rank_score")),
            "rank_scope": run.get("rank_scope", "aggregate"),
            "below_min_trades": bool(run.get("below_min_trades", False)),
            "below_min_t_stat": bool(run.get("below_min_t_stat", False)),
            "net_profit_after_cost_pct": _finite(metrics.get("net_profit_after_cost_pct")),
            "total_trades": metrics.get("total_trades"),
            "t_stat": _finite(metrics.get("net_expectancy_t_stat")),
            "ci95": [
                _finite(metrics.get("net_expectancy_ci95_low_normal_approx")),
                _finite(metrics.get("net_expectancy_ci95_high_normal_approx")),
            ],
            "max_drawdown_pct": _finite(metrics.get("max_drawdown_pct")),
            "fold_consistency": (evidence.get("fold_summary") or {}).get("sign_consistency"),
            "max_symbol_share": (evidence.get("symbol_summary") or {}).get("max_symbol_share"),
            "benchmark_delta": evidence.get("benchmark_delta"),
            "summary": summary,
        })
    return rows


def build_sweep_report(
    *,
    grid: dict[str, Any],
    baseline: dict[str, Any] | None = None,
    holdout: dict[str, Any] | None = None,
    stress_runs: list[dict[str, Any]] | None = None,
    workflow: dict[str, Any] | None = None,
    top_n: int = 10,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a compact, bounded sweep report from a serialised grid payload."""
    grid_config = grid.get("config") or {}
    rank_by = grid_config.get("rank_by") or DEFAULT_RANK_BY
    min_trades = int(grid_config.get("min_trades") or 0)

    entries = _sweep_entries(grid)
    analysis = analyze_sweep(entries, rank_by=rank_by, min_trades=min_trades)
    best = analysis.get("best") or {}
    best_metrics = best.get("metrics") or {}

    baseline_metrics = (baseline or {}).get("metrics") or {}
    baseline_config = (baseline or {}).get("config") or {}
    holdout_metrics = ((holdout or {}).get("result") or {}).get("metrics") or {}

    # Fold consistency + concentration for the best run (from its evidence).
    best_run = None
    ranked_indexes = grid.get("ranked_indexes") or []
    runs = grid.get("runs") or []
    if ranked_indexes and isinstance(ranked_indexes[0], int) and 0 <= ranked_indexes[0] < len(runs):
        best_run = runs[ranked_indexes[0]]
    best_evidence = (best_run or {}).get("evidence") or {}
    fold = best_evidence.get("fold_summary") or {}
    symbol = best_evidence.get("symbol_summary") or {}

    recommendation = build_recommendation(
        analysis=analysis,
        baseline_metrics=baseline_metrics,
        baseline_config=baseline_config,
        fold_consistency={"consistency": fold.get("sign_consistency", 0.0)},
        concentration={
            "max_symbol_share": symbol.get("max_symbol_share"),
            "max_trade_share": None,
        },
        stress_rows=stress_runs or [],
        holdout_metrics=holdout_metrics or None,
    )

    benchmark = None
    if baseline_metrics.get("buy_and_hold"):
        benchmark = baseline_metrics["buy_and_hold"]

    return {
        "schema_version": SWEEP_REPORT_SCHEMA_VERSION,
        "report_type": "grid_sweep",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "workflow": dict(workflow or {}),
        "config": {
            "rank_by": rank_by,
            "min_trades": min_trades,
            "min_expectancy_t_stat": grid_config.get("min_expectancy_t_stat", 0.0),
            "validation_folds": grid_config.get("validation_folds", 0),
            "final_holdout_fraction": grid_config.get("final_holdout_fraction", 0.0),
            "search_mode": grid.get("search_mode"),
            "random_seed": grid.get("random_seed"),
            "total_combinations": grid.get("total_combinations"),
            "attempted_combinations": grid.get("attempted_combinations"),
        },
        "baseline": {
            "net_profit_after_cost_pct": _finite(baseline_metrics.get("net_profit_after_cost_pct")),
            "total_trades": baseline_metrics.get("total_trades"),
            "t_stat": _finite(baseline_metrics.get("net_expectancy_t_stat")),
        },
        "benchmark": benchmark,
        "top_runs": _top_runs(grid, top_n),
        "marginal_sensitivity": analysis.get("marginal_sensitivity") or [],
        "interactions": analysis.get("interactions") or [],
        "plateaus": analysis.get("plateaus") or [],
        "robustness": analysis.get("robustness") or {},
        "robustness_score": analysis.get("robustness_score") or {},
        "multiple_comparison": analysis.get("multiple_comparison") or {},
        "fold_consistency": fold,
        "concentration": symbol,
        "holdout": (
            {
                "params": (holdout or {}).get("params") or {},
                "net_profit_after_cost_pct": _finite(holdout_metrics.get("net_profit_after_cost_pct")),
                "total_trades": holdout_metrics.get("total_trades"),
                "below_min_trades": (holdout or {}).get("below_min_trades"),
            }
            if holdout else None
        ),
        "stress": list(stress_runs or []),
        "recommendation": recommendation,
        "assumptions": grid.get("assumptions") or {},
        "data_provenance": grid.get("data_provenance") or [],
        "caveats": [
            "Screening aid only; not a profitability guarantee.",
            "In-sample hypothesis until confirmed on untouched out-of-sample data and paper trading.",
            "OHLCV slippage is a proxy, not historical order-book impact.",
        ],
    }


def render_sweep_markdown(report: dict[str, Any]) -> str:
    """Render a compact sweep report as a human-readable Markdown document."""
    lines: list[str] = ["# Grid Sweep Report", ""]
    workflow = report.get("workflow") or {}
    if workflow.get("strategy"):
        lines.append(f"- Strategy: {workflow['strategy']}")
    if workflow.get("symbols"):
        lines.append(f"- Symbols: {', '.join(workflow['symbols'])}")
    if workflow.get("timeframe"):
        lines.append(f"- Timeframe: {workflow['timeframe']}")
    config = report.get("config") or {}
    lines.append(
        f"- Combinations: {config.get('attempted_combinations')}/"
        f"{config.get('total_combinations')} ({config.get('search_mode')}, "
        f"seed {config.get('random_seed')})"
    )
    lines.append(f"- Ranked by: `{config.get('rank_by')}`")
    lines.append("")

    baseline = report.get("baseline") or {}
    lines.append("## Baseline")
    lines.append("")
    lines.append(
        f"- Net after cost %: {baseline.get('net_profit_after_cost_pct')}; "
        f"trades: {baseline.get('total_trades')}; t-stat: {baseline.get('t_stat')}"
    )
    benchmark = report.get("benchmark")
    if benchmark:
        lines.append(
            f"- Buy-and-hold: {benchmark.get('total_return_pct')}% "
            f"(costs included: {benchmark.get('costs_included', False)})"
        )
    lines.append("")

    top_runs = report.get("top_runs") or []
    if top_runs:
        lines.append("## Top runs")
        lines.append("")
        lines.append("| Params | Net % | Trades | t-stat | MaxDD % | Fold cons. |")
        lines.append("|---|---|---|---|---|---|")
        for run in top_runs:
            params = ", ".join(f"{k.split('.')[-1]}={v}" for k, v in run["params"].items())
            lines.append(
                f"| {params} | {run.get('net_profit_after_cost_pct')} | "
                f"{run.get('total_trades')} | {run.get('t_stat')} | "
                f"{run.get('max_drawdown_pct')} | {run.get('fold_consistency')} |"
            )
        lines.append("")

    plateaus = report.get("plateaus") or []
    if plateaus:
        lines.append("## Parameter plateaus")
        lines.append("")
        for plateau in plateaus:
            lines.append(
                f"- `{plateau['key'].split('.')[-1]}`: best {plateau.get('best_value')}, "
                f"plateau {plateau.get('plateau_values')}"
            )
        lines.append("")

    interactions = report.get("interactions") or []
    if interactions:
        lines.append("## Interactions")
        lines.append("")
        for interaction in interactions:
            lines.append(
                f"- {interaction['a'].split('.')[-1]} × {interaction['b'].split('.')[-1]}: "
                f"strength {interaction.get('strength')}"
            )
        lines.append("")

    recommendation = report.get("recommendation")
    if recommendation:
        lines.append(render_recommendation_markdown(recommendation))
        lines.append("")

    caveats = report.get("caveats") or []
    if caveats:
        lines.append("## Caveats")
        lines.append("")
        for caveat in caveats:
            lines.append(f"- {caveat}")
        lines.append("")

    return "\n".join(lines)
