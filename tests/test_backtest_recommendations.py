"""Tests for the deterministic recommendation layer (Phase 4)."""

from __future__ import annotations

from app.services.backtest.recommendations import (
    build_recommendation,
    render_recommendation_markdown,
)


def _analysis() -> dict:
    return {
        "best": {
            "params": {
                "strategies.mean_reversion.rsi_oversold": 35,
                "strategies.mean_reversion.max_adx": 25,
            },
            "metrics": {
                "total_trades": 96,
                "net_profit_after_cost_pct": 11.8,
                "net_expectancy_t_stat": 2.4,
                "max_drawdown_pct": 7.2,
            },
        },
        "plateaus": [
            {"key": "strategies.mean_reversion.rsi_oversold", "best_value": 35,
             "plateau_values": [30, 35, 40], "plateau_size": 3, "is_plateau": True},
            {"key": "strategies.mean_reversion.max_adx", "best_value": 25,
             "plateau_values": [25], "plateau_size": 1, "is_plateau": False},
        ],
        "marginal_sensitivity": [
            {"key": "strategies.mean_reversion.rsi_oversold", "best_value": 35,
             "edge_of_range": False},
            {"key": "strategies.mean_reversion.max_adx", "best_value": 25,
             "edge_of_range": True},
        ],
        "interactions": [],
        "robustness_score": {"score": 0.72},
    }


def test_recommendation_changes_and_confidence() -> None:
    rec = build_recommendation(
        analysis=_analysis(),
        baseline_metrics={"net_profit_after_cost_pct": 3.1, "total_trades": 84,
                          "max_drawdown_pct": 9.0},
        baseline_config={"strategies": {"mean_reversion": {"rsi_oversold": 30, "max_adx": 25}}},
        fold_consistency={"consistency": 0.75},
        concentration={"max_symbol_share": 0.5, "max_trade_share": 0.3},
        stress_rows=[{"label": "fees_2x", "survives": True}],
        holdout_metrics={"net_profit_after_cost": 5.0},
    )
    changes = {c["param"]: c for c in rec["changes"]}
    assert changes["rsi_oversold"]["from"] == 30
    assert changes["rsi_oversold"]["to"] == 35
    assert changes["rsi_oversold"]["changed"] is True
    assert changes["rsi_oversold"]["in_plateau"] is True
    assert changes["max_adx"]["changed"] is False
    assert changes["max_adx"]["edge_of_range"] is True
    assert rec["confidence"] == "high"
    assert rec["expected_effect"]["net_profit_after_cost_pct"]["delta"] == 8.7


def test_recommendation_flags_risks() -> None:
    rec = build_recommendation(
        analysis=_analysis(),
        baseline_metrics={"net_profit_after_cost_pct": 3.1},
        baseline_config={"strategies": {"mean_reversion": {"rsi_oversold": 30, "max_adx": 25}}},
        fold_consistency={"consistency": 0.4},
        concentration={"max_symbol_share": 0.9, "max_trade_share": 0.7},
        stress_rows=[{"label": "combined", "survives": False}],
        holdout_metrics={"net_profit_after_cost": -1.0},
    )
    risks = " ".join(rec["risks"])
    assert "Edge-of-range" in risks
    assert "one symbol" in risks
    assert "one trade" in risks
    assert "does not survive" in risks
    assert rec["confidence"] == "low"


def test_recommendation_next_experiments_and_markdown() -> None:
    rec = build_recommendation(
        analysis=_analysis(),
        baseline_metrics={"net_profit_after_cost_pct": 3.1},
        baseline_config={"strategies": {"mean_reversion": {"rsi_oversold": 30, "max_adx": 25}}},
        fold_consistency={"consistency": 0.75},
        concentration={},
        stress_rows=[],
        holdout_metrics={"net_profit_after_cost": 5.0},
    )
    experiments = " ".join(rec["next_experiments"])
    assert "plateau" in experiments
    assert "Extend the swept range" in experiments

    markdown = render_recommendation_markdown(rec)
    assert "## Recommendation" in markdown
    assert "rsi_oversold" in markdown
    assert "Confidence" in markdown


def test_recommendation_handles_no_baseline_config() -> None:
    rec = build_recommendation(
        analysis=_analysis(),
        baseline_metrics={},
        baseline_config={},
    )
    assert rec["changes"]
    assert all(change["from"] is None for change in rec["changes"])
    assert rec["confidence"] in {"low", "medium", "high"}
