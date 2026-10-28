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
from app.services.backtest.sweep_analysis import (
    DEFAULT_RANK_BY,
    analyze_sweep,
    strategy_of_key,
)

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
        strategy_summary = evidence.get("strategy_summary") or {}
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
            "max_strategy_share": strategy_summary.get("max_strategy_share"),
            "dominant_strategy": strategy_summary.get("dominant_strategy"),
            "strategy_breakdown": {
                "n_strategies": strategy_summary.get("n_strategies"),
                "max_strategy_share": strategy_summary.get("max_strategy_share"),
                "dominant_strategy": strategy_summary.get("dominant_strategy"),
                "strategies": strategy_summary.get("strategies") or [],
            },
            "benchmark_delta": evidence.get("benchmark_delta"),
            "summary": summary,
        })
    return rows


def _param_label(key: str, value: Any) -> str:
    """Format a swept parameter as ``strategy.param=value`` when it has an owner.

    Launcher-level keys stay as ``param=value``.  Including the owning strategy
    prevents ambiguity when several strategies share a parameter name (e.g.
    ``max_adx`` exists on multiple strategies).
    """
    owner = strategy_of_key(key)
    short = str(key).split(".")[-1]
    label = f"{owner}.{short}" if owner else short
    return f"{label}={value}"


def _strategy_scope(
    grid: dict[str, Any],
    analysis: dict[str, Any],
    best_strategy_summary: dict[str, Any],
) -> dict[str, Any]:
    """Describe which strategies were enabled, tuned, and actually traded.

    * ``enabled`` — the run's configured strategy list (from ``base_config``).
    * ``tuned`` — strategies that owned at least one swept parameter.
    * ``traded`` — strategies that produced trades in the best run.
    * ``dominant`` — the biggest net-PnL contributor in the best run.

    A tuned strategy that is not the dominant one — or an enabled strategy that
    contributed most of the net — is exactly the attribution ambiguity this
    report exists to expose.
    """
    base_config = (grid.get("config") or {}).get("base_config") or {}
    enabled = base_config.get("strategy_names") or []
    scope = analysis.get("strategy_scope") or {}
    tuned = scope.get("tuned") or []
    traded = [s.get("strategy") for s in (best_strategy_summary.get("strategies") or [])]
    return {
        "enabled": list(enabled),
        "tuned": list(tuned),
        "traded": traded,
        "params_by_strategy": scope.get("params_by_strategy") or {},
        "dominant_strategy": best_strategy_summary.get("dominant_strategy"),
        "max_strategy_share": best_strategy_summary.get("max_strategy_share"),
    }


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
    best_strategy_summary = best_evidence.get("strategy_summary") or {}

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
        strategy_scope=_strategy_scope(grid, analysis, best_strategy_summary),
        best_strategy_summary=best_strategy_summary,
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
        "strategy_scope": _strategy_scope(grid, analysis, best_strategy_summary),
        "best_run_strategy_breakdown": {
            "n_strategies": best_strategy_summary.get("n_strategies"),
            "max_strategy_share": best_strategy_summary.get("max_strategy_share"),
            "dominant_strategy": best_strategy_summary.get("dominant_strategy"),
            "strategies": best_strategy_summary.get("strategies") or [],
        },
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

    scope = report.get("strategy_scope") or {}
    if scope.get("enabled") or scope.get("tuned"):
        lines.append("## Strategy scope")
        lines.append("")
        lines.append(f"- Enabled: {', '.join(scope.get('enabled') or []) or '—'}")
        lines.append(f"- Tuned: {', '.join(scope.get('tuned') or []) or '—'}")
        if scope.get("traded"):
            lines.append(f"- Traded (best run): {', '.join(scope['traded'])}")
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
        lines.append("| Params | Net % | Trades | t-stat | MaxDD % | Fold cons. | Dominant strategy (share) |")
        lines.append("|---|---|---|---|---|---|---|")
        for run in top_runs:
            params = ", ".join(_param_label(k, v) for k, v in run["params"].items())
            dominant = run.get("dominant_strategy") or "—"
            share = run.get("max_strategy_share")
            share_txt = f" ({share:.0%})" if isinstance(share, (int, float)) else ""
            lines.append(
                f"| {params} | {run.get('net_profit_after_cost_pct')} | "
                f"{run.get('total_trades')} | {run.get('t_stat')} | "
                f"{run.get('max_drawdown_pct')} | {run.get('fold_consistency')} | "
                f"{dominant}{share_txt} |"
            )
        lines.append("")

    breakdown = report.get("best_run_strategy_breakdown") or {}
    strategies = breakdown.get("strategies") or []
    if strategies:
        lines.append("## Strategy attribution (best run)")
        lines.append("")
        lines.append("| Strategy | Trades | Net after cost | Share | Net win % | Net PF | Expectancy | Avg R |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for row in strategies:
            share = row.get("share")
            share_txt = f"{share:.0%}" if isinstance(share, (int, float)) else "—"
            lines.append(
                f"| {row.get('strategy')} | {row.get('trades')} | "
                f"{row.get('net_profit_after_cost')} | {share_txt} | "
                f"{row.get('net_win_rate_after_cost_pct')} | "
                f"{row.get('net_profit_factor_after_cost')} | "
                f"{row.get('net_expectancy_after_cost')} | "
                f"{row.get('avg_r_multiple')} |"
            )
        lines.append("")

    plateaus = report.get("plateaus") or []
    if plateaus:
        lines.append("## Parameter plateaus")
        lines.append("")
        for plateau in plateaus:
            owner = plateau.get("strategy")
            label = f"{owner}.{plateau['key'].split('.')[-1]}" if owner else plateau["key"].split('.')[-1]
            lines.append(
                f"- `{label}`: best {plateau.get('best_value')}, "
                f"plateau {plateau.get('plateau_values')}"
            )
        lines.append("")

    interactions = report.get("interactions") or []
    if interactions:
        lines.append("## Interactions")
        lines.append("")
        for interaction in interactions:
            a_owner = interaction.get("a_strategy")
            b_owner = interaction.get("b_strategy")
            a = f"{a_owner}.{interaction['a'].split('.')[-1]}" if a_owner else interaction['a'].split('.')[-1]
            b = f"{b_owner}.{interaction['b'].split('.')[-1]}" if b_owner else interaction['b'].split('.')[-1]
            lines.append(
                f"- {a} × {b}: strength {interaction.get('strength')}"
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
