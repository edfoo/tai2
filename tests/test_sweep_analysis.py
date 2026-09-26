from __future__ import annotations

from app.services.backtest.sweep_analysis import DEFAULT_RANK_BY, analyze_sweep


def test_default_sweep_ranking_uses_net_return_after_costs() -> None:
    entries = [
        {
            "params": {"threshold": 1},
            "metrics": {
                "total_trades": 20,
                "net_profit_pct": 20.0,
                "net_profit_after_cost_pct": 1.0,
            },
        },
        {
            "params": {"threshold": 2},
            "metrics": {
                "total_trades": 20,
                "net_profit_pct": 5.0,
                "net_profit_after_cost_pct": 4.0,
            },
        },
    ]

    result = analyze_sweep(entries, min_trades=1)

    assert DEFAULT_RANK_BY == "net_profit_after_cost_pct"
    assert result["best"]["params"] == {"threshold": 2}


def test_sensitivity_reports_average_after_cost_return() -> None:
    entries = [
        {"params": {"threshold": 1}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": 2.0}},
        {"params": {"threshold": 1}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": 4.0}},
        {"params": {"threshold": 2}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": -1.0}},
    ]

    result = analyze_sweep(entries, min_trades=1)
    values = {item["value"]: item for item in result["sensitivity"][0]["values"]}

    assert values["1"]["avg_rank"] == 3.0
    assert values["1"]["avg_net_profit_after_cost_pct"] == 3.0
    assert values["2"]["avg_rank"] == -1.0


# ── Phase 3: marginal sensitivity, interactions, plateaus ───────────────


def _grid_entries() -> list[dict]:
    """A 3×3 grid where A=2 is best and B has a plateau at 1–2."""
    entries = []
    for a in (1, 2, 3):
        for b in (1, 2, 3):
            # A=2 best; B=1,2 near-equal, B=3 worse.
            score = {1: 2.0, 2: 10.0, 3: 4.0}[a] + {1: 0.0, 2: -0.5, 3: -6.0}[b]
            entries.append({
                "params": {"a": a, "b": b},
                "metrics": {
                    "total_trades": 20,
                    "net_profit_after_cost_pct": score,
                    "net_expectancy_t_stat": 2.5,
                },
            })
    return entries


def test_marginal_sensitivity_is_oat_not_confounded() -> None:
    result = analyze_sweep(_grid_entries(), min_trades=1)
    marginal = {m["key"]: m for m in result["marginal_sensitivity"]}

    # Holding b at the best value (b=1), a's curve is 2, 10, 4.
    a_curve = {point["value"]: point["score"] for point in marginal["a"]["curve"]}
    assert a_curve == {1: 2.0, 2: 10.0, 3: 4.0}
    assert marginal["a"]["best_value"] == 2
    assert marginal["a"]["edge_of_range"] is False


def test_marginal_sensitivity_flags_edge_of_range() -> None:
    entries = [
        {"params": {"a": 1}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": 1.0}},
        {"params": {"a": 2}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": 2.0}},
        {"params": {"a": 3}, "metrics": {"total_trades": 5, "net_profit_after_cost_pct": 9.0}},
    ]
    result = analyze_sweep(entries, min_trades=1)
    marginal = result["marginal_sensitivity"][0]
    assert marginal["best_value"] == 3
    assert marginal["edge_of_range"] is True
    assert marginal["monotonic"] is True


def test_parameter_plateaus_detects_safe_range() -> None:
    result = analyze_sweep(_grid_entries(), min_trades=1)
    plateaus = {p["key"]: p for p in result["plateaus"]}
    # b=1 (0.0) and b=2 (-0.5) are within 5% of best; b=3 (-6.0) is not.
    assert plateaus["b"]["is_plateau"] is True
    assert set(plateaus["b"]["plateau_values"]) == {1, 2}
    assert plateaus["a"]["is_plateau"] is False


def test_detect_interactions_ranks_pairs() -> None:
    entries = []
    for a in (1, 2):
        for b in (1, 2):
            # A's best level flips with B → strong interaction.
            score = 10.0 if (a == 1) == (b == 1) else 0.0
            entries.append({
                "params": {"a": a, "b": b},
                "metrics": {"total_trades": 5, "net_profit_after_cost_pct": score},
            })
    result = analyze_sweep(entries, min_trades=1)
    assert result["interactions"]
    assert result["interactions"][0]["strength"] > 0


def test_robustness_score_and_multiple_comparison() -> None:
    result = analyze_sweep(_grid_entries(), min_trades=1)
    score = result["robustness_score"]
    assert 0.0 <= score["score"] <= 1.0
    assert score["plateau_fraction"] > 0
    assert result["multiple_comparison"]["combinations_tested"] == 9
    assert 0.0 < result["multiple_comparison"]["deflation_factor"] <= 1.0