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