"""Tests for the compact sweep report handoff artifact (Phase 5)."""

from __future__ import annotations

import json

from app.services.backtest.sweep_report import (
    build_sweep_report,
    render_sweep_markdown,
)


def _grid_payload() -> dict:
    runs = []
    for rsi in (25, 30, 35):
        score = {25: 4.0, 30: 9.0, 35: 11.0}[rsi]
        runs.append({
            "params": {"strategies.mean_reversion.rsi_oversold": rsi},
            "rank_score": score,
            "rank_scope": "aggregate",
            "below_min_trades": False,
            "below_min_t_stat": False,
            "evidence": {
                "summary": {"total_trades": 50, "net_profit_after_cost_pct": score},
                "fold_summary": {"sign_consistency": 0.75, "completed_folds": 4},
                "symbol_summary": {"max_symbol_share": 0.6},
                "benchmark_delta": {"beat_benchmark": True},
            },
            "result": {
                "metrics": {
                    "total_trades": 50,
                    "net_profit_after_cost_pct": score,
                    "net_expectancy_t_stat": 2.2,
                    "net_expectancy_ci95_low_normal_approx": 1.0,
                    "net_expectancy_ci95_high_normal_approx": 5.0,
                    "max_drawdown_pct": 6.0,
                },
                "trades": [{"pnl": 1.0}] * 50,
                "equity_curve": [{"ts": 1, "equity": 1000.0, "open_positions": 0}],
            },
        })
    return {
        "config": {
            "rank_by": "net_profit_after_cost_pct",
            "min_trades": 5,
            "min_expectancy_t_stat": 0.0,
            "validation_folds": 4,
            "final_holdout_fraction": 0.15,
        },
        "search_mode": "exhaustive",
        "random_seed": 42,
        "total_combinations": 3,
        "attempted_combinations": 3,
        "runs": runs,
        "ranked_indexes": [2, 1, 0],
        "assumptions": {"validation_protocol": "folds"},
        "data_provenance": [],
    }


def test_report_is_bounded_and_json_safe() -> None:
    report = build_sweep_report(
        grid=_grid_payload(),
        baseline={"metrics": {"net_profit_after_cost_pct": 3.0, "total_trades": 40}},
        workflow={"strategy": "mean_reversion", "symbols": ["BTC-USDT-SWAP"]},
    )
    # Round-trips through json.dumps (no inf/NaN).
    encoded = json.dumps(report)
    # No embedded trade lists or equity curves (bounded artifact).
    assert '"trades": [' not in encoded
    assert '"equity_curve"' not in encoded
    assert report["schema_version"] == 1
    assert report["report_type"] == "grid_sweep"
    assert len(report["top_runs"]) == 3
    # Top run is the highest-ranked (rsi=35).
    assert report["top_runs"][0]["params"]["strategies.mean_reversion.rsi_oversold"] == 35
    assert report["top_runs"][0]["t_stat"] == 2.2
    assert report["recommendation"] is not None


def test_report_top_n_limits_runs() -> None:
    report = build_sweep_report(grid=_grid_payload(), top_n=1)
    assert len(report["top_runs"]) == 1


def test_report_markdown_renders() -> None:
    report = build_sweep_report(
        grid=_grid_payload(),
        baseline={"metrics": {"net_profit_after_cost_pct": 3.0}},
        workflow={"strategy": "mean_reversion", "symbols": ["BTC-USDT-SWAP"]},
    )
    markdown = render_sweep_markdown(report)
    assert "# Grid Sweep Report" in markdown
    assert "## Top runs" in markdown
    assert "## Recommendation" in markdown
    assert "rsi_oversold" in markdown


def test_report_handles_missing_baseline() -> None:
    report = build_sweep_report(grid=_grid_payload())
    assert report["baseline"]["net_profit_after_cost_pct"] == 0.0
    assert report["recommendation"] is not None
