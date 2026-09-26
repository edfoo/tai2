"""P3 research workflow: turn backtest artifacts into an auditable bundle.

P0–P2 made the engine trustworthy (no look-ahead, out-of-sample folds,
reproducible handoff, realistic costs/margin).  P3 is the *methodology* layer:
it takes the raw artifacts a run already produces and assembles the
**analysis bundle** an expert needs to judge whether a parameter set is a
defensible candidate or an overfit artifact.

This module is deliberately pure and dependency-light: it consumes plain
dicts (the serialised ``BacktestResult`` / ``GridResult`` payloads from
:mod:`app.services.backtest.persistence`) plus optional stress runs, and
returns a JSON-safe bundle.  It does **not** run backtests — the CLI
(``scripts/backtest_research.py``) drives the REST API and feeds the results
in here, so the same analysis is available to the UI, the CLI, and tests.

What the bundle adds beyond a raw grid result
---------------------------------------------
* **Plateau / robustness** — reuses :func:`analyze_sweep` to flag a lone spike
  versus a plateau of near-equal results.
* **Fold consistency** — the fraction of validation folds with positive net
  expectancy after costs (a candidate that only works in one fold is fragile).
* **Concentration** — whether profit is dominated by one symbol or one trade.
* **Stress survival** — whether the edge survives higher fees/slippage/funding.
* **Verdict** — a screening status (``candidate`` / ``inconclusive`` /
  ``reject``) with explicit reasons and caveats.

The verdict is a *screening aid*, not a profitability guarantee.  It encodes
the plan's principles (prefer plateaus, reject single-symbol/single-fold
concentration, stress before deploying) so a human can review the evidence
quickly — it never claims future profitability.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from app.services.backtest.sweep_analysis import DEFAULT_RANK_BY, analyze_sweep

BUNDLE_SCHEMA_VERSION = 1

# Screening thresholds.  These are deliberately conservative defaults and are
# overridable per call so an expert can tighten or loosen the screen.
DEFAULT_MAX_SYMBOL_SHARE = 0.8
DEFAULT_MAX_TRADE_SHARE = 0.5
DEFAULT_MIN_FOLD_CONSISTENCY = 0.6


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
    """Return a finite float or ``None`` (for JSON-safe deltas)."""
    parsed = _num(value)
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def fold_consistency(
    fold_metrics: Iterable[dict[str, Any]],
    *,
    rank_by: str = DEFAULT_RANK_BY,
) -> dict[str, Any]:
    """Summarise how consistently a candidate performs across validation folds.

    A candidate whose edge lives in a single fold is fragile.  This counts
    completed folds with positive net PnL after costs and reports the
    fraction, plus the per-fold net PnL so the spread is visible.
    """
    folds = [f for f in fold_metrics if isinstance(f, dict)]
    completed = [f for f in folds if f.get("status") == "completed"]
    failed = [f for f in folds if f.get("status") == "failed"]
    skipped = [f for f in folds if f.get("status") == "skipped"]

    per_fold: list[dict[str, Any]] = []
    positive = 0
    for fold in completed:
        metrics = fold.get("metrics") or {}
        net = _num(metrics.get("net_profit_after_cost"))
        if net > 0:
            positive += 1
        per_fold.append({
            "fold": fold.get("fold"),
            "start_ts": fold.get("start_ts"),
            "end_ts": fold.get("end_ts"),
            "trade_count": fold.get("trade_count"),
            "rank_score": fold.get("rank_score"),
            "net_profit_after_cost": _finite(net),
            "net_profit_after_cost_pct": _finite(metrics.get("net_profit_after_cost_pct")),
        })

    consistency = (positive / len(completed)) if completed else 0.0
    return {
        "completed_folds": len(completed),
        "failed_folds": len(failed),
        "skipped_folds": len(skipped),
        "positive_folds": positive,
        "consistency": round(consistency, 4),
        "rank_by": rank_by,
        "per_fold": per_fold,
    }


def concentration(
    *,
    per_symbol: dict[str, Any] | None,
    trades: Iterable[dict[str, Any]] | None,
    total_net_profit: float,
) -> dict[str, Any]:
    """Measure whether profit is dominated by one symbol or one trade.

    Returns the largest per-symbol share of total net profit and the largest
    single-trade share of gross profit.  Shares are only meaningful when total
    net profit is positive; otherwise they are reported as ``None``.
    """
    symbol_shares: list[dict[str, Any]] = []
    max_symbol_share: float | None = None
    if per_symbol and total_net_profit > 0:
        for symbol, group in per_symbol.items():
            net = _num((group or {}).get("net_profit_after_cost"))
            share = net / total_net_profit
            symbol_shares.append({
                "symbol": symbol,
                "net_profit_after_cost": _finite(net),
                "share": round(share, 4),
                "trades": (group or {}).get("trades"),
            })
        symbol_shares.sort(key=lambda row: row["share"], reverse=True)
        if symbol_shares:
            max_symbol_share = symbol_shares[0]["share"]

    trade_rows = [t for t in (trades or []) if isinstance(t, dict)]
    gross_profit = sum(
        _num(t.get("net_pnl")) for t in trade_rows if _num(t.get("net_pnl")) > 0
    )
    largest_win = max((_num(t.get("net_pnl")) for t in trade_rows), default=0.0)
    max_trade_share: float | None = None
    if gross_profit > 0 and largest_win > 0:
        max_trade_share = round(largest_win / gross_profit, 4)

    return {
        "max_symbol_share": max_symbol_share,
        "max_trade_share": max_trade_share,
        "symbol_shares": symbol_shares,
        "gross_profit": _finite(gross_profit),
        "largest_win": _finite(largest_win),
    }


def stress_comparison(
    baseline_metrics: dict[str, Any],
    stress_runs: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compare a baseline against adverse-scenario runs.

    Each stress run is ``{"label": str, "overrides": {...}, "metrics": {...}}``.
    Reports the net-after-cost delta and whether the edge stayed positive.
    """
    baseline_net = _num(baseline_metrics.get("net_profit_after_cost"))
    baseline_pct = _num(baseline_metrics.get("net_profit_after_cost_pct"))
    rows: list[dict[str, Any]] = []
    for run in stress_runs:
        metrics = run.get("metrics") or {}
        net = _num(metrics.get("net_profit_after_cost"))
        pct = _num(metrics.get("net_profit_after_cost_pct"))
        rows.append({
            "label": run.get("label") or "stress",
            "overrides": dict(run.get("overrides") or {}),
            "net_profit_after_cost": _finite(net),
            "net_profit_after_cost_pct": _finite(pct),
            "delta_net_profit_after_cost": _finite(net - baseline_net),
            "delta_net_profit_after_cost_pct": _finite(pct - baseline_pct),
            "survives": net > 0,
            "total_trades": metrics.get("total_trades"),
        })
    return rows


def _sweep_entries(grid: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a serialised grid result into ``analyze_sweep`` entries."""
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


def _best_run(grid: dict[str, Any]) -> dict[str, Any] | None:
    """Return the top-ranked run (by the grid's own ranking) or ``None``."""
    ranked_indexes = grid.get("ranked_indexes") or []
    runs = grid.get("runs") or []
    if ranked_indexes:
        idx = ranked_indexes[0]
        if isinstance(idx, int) and 0 <= idx < len(runs):
            return runs[idx]
    # Fall back to the highest rank_score.
    scored = [r for r in runs if isinstance(r, dict) and r.get("rank_score") is not None]
    if not scored:
        return None
    return max(scored, key=lambda r: _num(r.get("rank_score")))


def build_research_bundle(
    *,
    baseline: dict[str, Any],
    grid: dict[str, Any] | None = None,
    stress_runs: Iterable[dict[str, Any]] | None = None,
    holdout: dict[str, Any] | None = None,
    workflow: dict[str, Any] | None = None,
    max_symbol_share: float = DEFAULT_MAX_SYMBOL_SHARE,
    max_trade_share: float = DEFAULT_MAX_TRADE_SHARE,
    min_fold_consistency: float = DEFAULT_MIN_FOLD_CONSISTENCY,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Assemble a JSON-safe research bundle from serialised run artifacts.

    Parameters
    ----------
    baseline:
        Serialised ``BacktestResult`` dict for the frozen-defaults baseline.
    grid:
        Optional serialised ``GridResult`` dict for the tuned strategy.
    stress_runs:
        Optional adverse-scenario runs (see :func:`stress_comparison`).
    holdout:
        Optional serialised ``GridRunResult`` dict for the untouched holdout.
    workflow:
        Optional metadata (strategy, symbols, timeframe, date range, …).
    """
    baseline_metrics = baseline.get("metrics") or {}
    baseline_config = baseline.get("config") or {}
    rank_by = (
        (grid or {}).get("config", {}).get("rank_by")
        or DEFAULT_RANK_BY
    )

    bundle: dict[str, Any] = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "bundle_type": "backtest_research",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "workflow": dict(workflow or {}),
        "baseline": {
            "metrics": baseline_metrics,
            "config": baseline_config,
            "assumptions": baseline.get("assumptions") or {},
        },
        "sweep": None,
        "holdout": None,
        "stress": [],
        "verdict": {},
        "assumptions": baseline.get("assumptions") or {},
        "data_provenance": baseline.get("data_provenance") or [],
    }

    reasons: list[str] = []
    caveats: list[str] = []

    # ── Sweep analysis ────────────────────────────────────────────────
    if grid:
        entries = _sweep_entries(grid)
        min_trades = int((grid.get("config") or {}).get("min_trades") or 0)
        analysis = analyze_sweep(entries, rank_by=rank_by, min_trades=min_trades)
        best = _best_run(grid)
        best_result = (best or {}).get("result") or {}
        best_metrics = best_result.get("metrics") or {}

        fold = fold_consistency(
            (best or {}).get("fold_metrics") or [], rank_by=rank_by
        )
        conc = concentration(
            per_symbol=best_result.get("per_symbol"),
            trades=best_result.get("trades"),
            total_net_profit=_num(best_metrics.get("net_profit_after_cost")),
        )

        bundle["sweep"] = {
            "rank_by": rank_by,
            "min_trades": min_trades,
            "total_combinations": grid.get("total_combinations"),
            "attempted_combinations": grid.get("attempted_combinations"),
            "search_mode": grid.get("search_mode"),
            "random_seed": grid.get("random_seed"),
            "best": analysis.get("best"),
            "best_params": (best or {}).get("params") or {},
            "best_metrics": best_metrics,
            "robustness": analysis.get("robustness") or {},
            "sensitivity": analysis.get("sensitivity") or [],
            "fold_consistency": fold,
            "concentration": conc,
        }

        # ── Screening reasons ─────────────────────────────────────────
        robustness = analysis.get("robustness") or {}
        if not analysis.get("best"):
            reasons.append("No sweep combination met the minimum-trade threshold.")
        if robustness.get("single_point_optimum"):
            reasons.append(
                "Best combination is a lone spike with no near-equal neighbours "
                "(likely overfit)."
            )
        if fold["completed_folds"] and fold["consistency"] < min_fold_consistency:
            reasons.append(
                f"Only {fold['positive_folds']}/{fold['completed_folds']} validation "
                f"folds were profitable after costs."
            )
        if fold["failed_folds"]:
            reasons.append(f"{fold['failed_folds']} validation fold(s) failed to run.")
        if conc["max_symbol_share"] is not None and conc["max_symbol_share"] > max_symbol_share:
            reasons.append(
                f"Profit is concentrated in one symbol "
                f"({conc['max_symbol_share']:.0%} of net)."
            )
        if conc["max_trade_share"] is not None and conc["max_trade_share"] > max_trade_share:
            reasons.append(
                f"Profit is concentrated in one trade "
                f"({conc['max_trade_share']:.0%} of gross profit)."
            )
        if _num(best_metrics.get("net_profit_after_cost")) <= 0:
            reasons.append("Best combination is not profitable after modeled costs.")

    # ── Holdout ───────────────────────────────────────────────────────
    if holdout:
        holdout_result = holdout.get("result") or {}
        holdout_metrics = holdout_result.get("metrics") or {}
        bundle["holdout"] = {
            "params": holdout.get("params") or {},
            "rank_score": holdout.get("rank_score"),
            "metrics": holdout_metrics,
            "below_min_trades": holdout.get("below_min_trades"),
        }
        if _num(holdout_metrics.get("net_profit_after_cost")) <= 0:
            reasons.append("Untouched final holdout was not profitable after costs.")
        if holdout.get("below_min_trades"):
            reasons.append("Holdout produced fewer than the minimum required trades.")

    # ── Stress ────────────────────────────────────────────────────────
    stress_rows = stress_comparison(baseline_metrics, stress_runs or [])
    bundle["stress"] = stress_rows
    for row in stress_rows:
        if not row["survives"]:
            reasons.append(
                f"Edge does not survive stress scenario '{row['label']}' "
                f"(net after cost {row['net_profit_after_cost']})."
            )

    # ── Verdict ───────────────────────────────────────────────────────
    if not grid:
        status = "inconclusive"
        caveats.append("No sweep was supplied; only a baseline was analysed.")
    elif not reasons:
        status = "candidate"
    elif any("not profitable" in r or "does not survive" in r for r in reasons):
        status = "reject"
    else:
        status = "inconclusive"

    caveats.extend([
        "Screening only: a 'candidate' verdict is a hypothesis, not a "
        "profitability guarantee.",
        "OHLCV slippage is a proxy, not historical order-book impact; "
        "isolated liquidation is an approximation.",
        "Confirm any candidate on untouched out-of-sample data and paper "
        "trading before live deployment.",
    ])

    bundle["verdict"] = {
        "status": status,
        "reasons": reasons,
        "caveats": caveats,
        "thresholds": {
            "max_symbol_share": max_symbol_share,
            "max_trade_share": max_trade_share,
            "min_fold_consistency": min_fold_consistency,
        },
    }
    return bundle


def render_bundle_markdown(bundle: dict[str, Any]) -> str:
    """Render a research bundle as a human-readable Markdown report."""
    lines: list[str] = []
    workflow = bundle.get("workflow") or {}
    verdict = bundle.get("verdict") or {}

    lines.append("# Backtest Research Bundle")
    lines.append("")
    lines.append(f"- Generated: {bundle.get('generated_at', '')}")
    if workflow.get("strategy"):
        lines.append(f"- Strategy: {workflow['strategy']}")
    if workflow.get("symbols"):
        lines.append(f"- Symbols: {', '.join(workflow['symbols'])}")
    if workflow.get("timeframe"):
        lines.append(
            f"- Timeframe: {workflow['timeframe']} "
            f"(eval {workflow.get('evaluation_mode', '')} "
            f"{workflow.get('evaluation_timeframe', '')})"
        )
    lines.append(f"- Verdict: **{verdict.get('status', 'unknown')}**")
    lines.append("")

    if verdict.get("reasons"):
        lines.append("## Screening reasons")
        lines.append("")
        for reason in verdict["reasons"]:
            lines.append(f"- {reason}")
        lines.append("")

    baseline_metrics = (bundle.get("baseline") or {}).get("metrics") or {}
    lines.append("## Baseline")
    lines.append("")
    lines.append(
        f"- Trades: {baseline_metrics.get('total_trades')}; "
        f"net after cost: {baseline_metrics.get('net_profit_after_cost')} "
        f"({baseline_metrics.get('net_profit_after_cost_pct')}%)"
    )
    lines.append("")

    sweep = bundle.get("sweep")
    if sweep:
        lines.append("## Sweep")
        lines.append("")
        lines.append(
            f"- Ranked by `{sweep.get('rank_by')}`; "
            f"{sweep.get('attempted_combinations')}/{sweep.get('total_combinations')} "
            f"combinations ({sweep.get('search_mode')}, seed {sweep.get('random_seed')})"
        )
        robustness = sweep.get("robustness") or {}
        lines.append(
            f"- Robustness: {robustness.get('note', '')} "
            f"({robustness.get('within_5pct')} within 5% of best)"
        )
        fold = sweep.get("fold_consistency") or {}
        lines.append(
            f"- Fold consistency: {fold.get('positive_folds')}/"
            f"{fold.get('completed_folds')} folds profitable "
            f"({fold.get('consistency')})"
        )
        conc = sweep.get("concentration") or {}
        lines.append(
            f"- Concentration: max symbol share {conc.get('max_symbol_share')}, "
            f"max trade share {conc.get('max_trade_share')}"
        )
        lines.append("")
        lines.append("### Best parameters")
        lines.append("")
        for key, value in (sweep.get("best_params") or {}).items():
            lines.append(f"- `{key}` = {value}")
        lines.append("")

    holdout = bundle.get("holdout")
    if holdout:
        metrics = holdout.get("metrics") or {}
        lines.append("## Final holdout (untouched)")
        lines.append("")
        lines.append(
            f"- Trades: {metrics.get('total_trades')}; "
            f"net after cost: {metrics.get('net_profit_after_cost')} "
            f"({metrics.get('net_profit_after_cost_pct')}%)"
        )
        lines.append("")

    stress = bundle.get("stress") or []
    if stress:
        lines.append("## Stress scenarios")
        lines.append("")
        for row in stress:
            lines.append(
                f"- {row['label']}: net {row['net_profit_after_cost']} "
                f"(Δ {row['delta_net_profit_after_cost']}); "
                f"survives={row['survives']}"
            )
        lines.append("")

    if verdict.get("caveats"):
        lines.append("## Caveats")
        lines.append("")
        for caveat in verdict["caveats"]:
            lines.append(f"- {caveat}")
        lines.append("")

    return "\n".join(lines)
