"""Deterministic parameter-change recommendations from a sweep analysis.

This is the layer that turns a sweep's statistics into an actionable, auditable
recommendation an expert can review.  It is deliberately **deterministic and
data-only**: every recommendation is derived from the sweep's own numbers
(plateaus, marginal curves, fold consistency, concentration, stress), never
from free-form generation.

The output is a JSON-safe dict:

  * ``changes`` — per parameter: current (baseline) value, recommended value,
    and a rationale (plateau centre / marginal best / keep default).
  * ``expected_effect`` — baseline → recommended delta on key metrics,
    explicitly labelled **in-sample**.
  * ``confidence`` — ``low`` / ``medium`` / ``high`` from robustness score,
    fold consistency, significance, and holdout survival.
  * ``risks`` — edge optimum, single-symbol concentration, low trades,
    dominant interaction, stress failure.
  * ``next_experiments`` — narrower ranges, untested parameters, regime windows.
  * ``caveats`` — standard non-guarantee language.

The recommendation is a *hypothesis*, not a profitability guarantee.
"""

from __future__ import annotations

from typing import Any, Iterable

# Confidence thresholds (overridable per call).
DEFAULT_HIGH_ROBUSTNESS = 0.65
DEFAULT_MEDIUM_ROBUSTNESS = 0.4
DEFAULT_MIN_FOLD_CONSISTENCY = 0.6
DEFAULT_MIN_T_STAT = 1.5


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


def _short(key: str) -> str:
    return key.split(".")[-1]


def _baseline_value(baseline_config: dict[str, Any], key: str) -> Any:
    """Resolve a dotted ``strategies.<name>.<param>`` key in a config dict."""
    parts = key.split(".")
    cur: Any = baseline_config
    for part in parts:
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def build_recommendation(
    *,
    analysis: dict[str, Any],
    baseline_metrics: dict[str, Any] | None = None,
    baseline_config: dict[str, Any] | None = None,
    fold_consistency: dict[str, Any] | None = None,
    concentration: dict[str, Any] | None = None,
    stress_rows: Iterable[dict[str, Any]] | None = None,
    holdout_metrics: dict[str, Any] | None = None,
    high_robustness: float = DEFAULT_HIGH_ROBUSTNESS,
    medium_robustness: float = DEFAULT_MEDIUM_ROBUSTNESS,
    min_fold_consistency: float = DEFAULT_MIN_FOLD_CONSISTENCY,
    min_t_stat: float = DEFAULT_MIN_T_STAT,
) -> dict[str, Any]:
    """Assemble a deterministic recommendation from sweep analysis artifacts."""
    best = analysis.get("best") or {}
    best_params: dict[str, Any] = best.get("params") or {}
    best_metrics: dict[str, Any] = best.get("metrics") or {}
    baseline_metrics = baseline_metrics or {}
    baseline_config = baseline_config or {}
    fold_consistency = fold_consistency or {}
    concentration = concentration or {}
    stress_rows = list(stress_rows or [])

    plateaus = {p["key"]: p for p in (analysis.get("plateaus") or [])}
    marginal = {m["key"]: m for m in (analysis.get("marginal_sensitivity") or [])}
    interactions = analysis.get("interactions") or []
    robustness = analysis.get("robustness_score") or {}

    # ── Per-parameter changes ─────────────────────────────────────────
    changes: list[dict[str, Any]] = []
    for key, recommended in best_params.items():
        current = _baseline_value(baseline_config, key)
        plateau = plateaus.get(key) or {}
        marg = marginal.get(key) or {}
        if plateau.get("is_plateau"):
            rationale = (
                f"plateau of {plateau.get('plateau_size')} near-equal values "
                f"({plateau.get('plateau_values')})"
            )
        elif marg.get("edge_of_range"):
            rationale = "marginal best sits at the swept boundary — extend the range"
        else:
            rationale = "marginal best (single point)"
        changes.append({
            "key": key,
            "param": _short(key),
            "from": current,
            "to": recommended,
            "changed": str(current) != str(recommended),
            "rationale": rationale,
            "edge_of_range": bool(marg.get("edge_of_range")),
            "in_plateau": bool(plateau.get("is_plateau")),
        })

    # ── Expected effect (in-sample) ───────────────────────────────────
    expected_effect = {
        "basis": "in_sample_validation",
        "net_profit_after_cost_pct": {
            "baseline": _finite(baseline_metrics.get("net_profit_after_cost_pct")),
            "recommended": _finite(best_metrics.get("net_profit_after_cost_pct")),
            "delta": round(
                _num(best_metrics.get("net_profit_after_cost_pct"))
                - _num(baseline_metrics.get("net_profit_after_cost_pct")),
                6,
            ),
        },
        "total_trades": {
            "baseline": baseline_metrics.get("total_trades"),
            "recommended": best_metrics.get("total_trades"),
        },
        "max_drawdown_pct": {
            "baseline": _finite(baseline_metrics.get("max_drawdown_pct")),
            "recommended": _finite(best_metrics.get("max_drawdown_pct")),
        },
    }

    # ── Risks ─────────────────────────────────────────────────────────
    risks: list[str] = []
    edge_params = [c["param"] for c in changes if c["edge_of_range"]]
    if edge_params:
        risks.append(
            f"Edge-of-range optimum for {', '.join(edge_params)} — the true "
            "optimum may lie outside the swept range."
        )
    max_symbol_share = concentration.get("max_symbol_share")
    if max_symbol_share is not None and max_symbol_share > 0.8:
        risks.append(f"Profit concentrated in one symbol ({max_symbol_share:.0%} of net).")
    max_trade_share = concentration.get("max_trade_share")
    if max_trade_share is not None and max_trade_share > 0.5:
        risks.append(f"Profit concentrated in one trade ({max_trade_share:.0%} of gross).")
    if int(best_metrics.get("total_trades") or 0) < 30:
        risks.append(
            f"Only {best_metrics.get('total_trades')} trades — the edge is "
            "low-sample and may not repeat."
        )
    strong_interactions = [i for i in interactions if _num(i.get("strength")) > 0.3]
    if strong_interactions:
        pairs = ", ".join(f"{_short(i['a'])}×{_short(i['b'])}" for i in strong_interactions[:3])
        risks.append(f"Strong parameter interaction(s): {pairs} — tune jointly, not one at a time.")
    failed_stress = [row for row in stress_rows if not row.get("survives")]
    if failed_stress:
        labels = ", ".join(str(row.get("label")) for row in failed_stress)
        risks.append(f"Edge does not survive stress scenario(s): {labels}.")

    # ── Confidence ────────────────────────────────────────────────────
    rob_score = _num(robustness.get("score"))
    consistency = _num(fold_consistency.get("consistency"))
    t_stat = _num(best_metrics.get("net_expectancy_t_stat"))
    holdout_ok = (
        holdout_metrics is None
        or _num(holdout_metrics.get("net_profit_after_cost")) > 0
    )

    if (
        rob_score >= high_robustness
        and consistency >= min_fold_consistency
        and t_stat >= min_t_stat
        and holdout_ok
        and not failed_stress
    ):
        confidence = "high"
    elif (
        rob_score >= medium_robustness
        and t_stat >= min_t_stat
        and holdout_ok
    ):
        confidence = "medium"
    else:
        confidence = "low"

    # ── Next experiments ──────────────────────────────────────────────
    next_experiments: list[str] = []
    for key, plateau in plateaus.items():
        if plateau.get("is_plateau"):
            values = plateau.get("plateau_values") or []
            if values:
                next_experiments.append(
                    f"Refine {_short(key)} around the plateau {values} at finer resolution."
                )
    for change in changes:
        if change["edge_of_range"]:
            next_experiments.append(
                f"Extend the swept range for {change['param']} beyond the current boundary."
            )
    if strong_interactions:
        next_experiments.append(
            "Run a joint 2-D sweep of the interacting parameters instead of OAT."
        )
    if not next_experiments:
        next_experiments.append(
            "Confirm the candidate on a different market regime and untouched out-of-sample data."
        )

    caveats = [
        "Recommendation is an in-sample hypothesis, not a profitability guarantee.",
        "Confirm on untouched out-of-sample data and paper trading before live deployment.",
        "OHLCV slippage is a proxy, not historical order-book impact.",
    ]

    return {
        "changes": changes,
        "expected_effect": expected_effect,
        "confidence": confidence,
        "confidence_inputs": {
            "robustness_score": round(rob_score, 4),
            "fold_consistency": round(consistency, 4),
            "t_stat": round(t_stat, 4),
            "holdout_positive": holdout_ok,
            "stress_failures": len(failed_stress),
        },
        "risks": risks,
        "next_experiments": next_experiments,
        "caveats": caveats,
    }


def render_recommendation_markdown(recommendation: dict[str, Any]) -> str:
    """Render a recommendation as a human-readable Markdown section."""
    lines: list[str] = ["## Recommendation", ""]
    lines.append(f"- Confidence: **{recommendation.get('confidence', 'unknown')}**")
    lines.append("")

    changes = recommendation.get("changes") or []
    if changes:
        lines.append("### Parameter changes")
        lines.append("")
        lines.append("| Parameter | From | To | Rationale |")
        lines.append("|---|---|---|---|")
        for change in changes:
            lines.append(
                f"| `{change['param']}` | {change['from']} | {change['to']} | "
                f"{change['rationale']} |"
            )
        lines.append("")

    effect = recommendation.get("expected_effect") or {}
    net = effect.get("net_profit_after_cost_pct") or {}
    if net:
        lines.append("### Expected effect (in-sample)")
        lines.append("")
        lines.append(
            f"- Net after cost %: {net.get('baseline')} → {net.get('recommended')} "
            f"(Δ {net.get('delta')})"
        )
        lines.append("")

    risks = recommendation.get("risks") or []
    if risks:
        lines.append("### Risks")
        lines.append("")
        for risk in risks:
            lines.append(f"- {risk}")
        lines.append("")

    next_experiments = recommendation.get("next_experiments") or []
    if next_experiments:
        lines.append("### Next experiments")
        lines.append("")
        for item in next_experiments:
            lines.append(f"- {item}")
        lines.append("")

    caveats = recommendation.get("caveats") or []
    if caveats:
        lines.append("### Caveats")
        lines.append("")
        for caveat in caveats:
            lines.append(f"- {caveat}")
        lines.append("")

    return "\n".join(lines)
