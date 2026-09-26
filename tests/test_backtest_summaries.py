"""Tests for compact per-run evidence summaries (Phase 2)."""

from __future__ import annotations

from app.services.backtest.summaries import (
    benchmark_delta,
    build_run_evidence,
    fold_summary,
    run_summary,
    symbol_summary,
)


def test_run_summary_copies_headline_metrics_and_sanitizes() -> None:
    summary = run_summary({
        "total_trades": 12,
        "net_profit_after_cost_pct": 8.5,
        "net_expectancy_t_stat": 2.4,
        "profit_factor": float("inf"),
        "exit_reasons": {"tp": 5, "sl": 7},
        "unrelated": 999,
    })
    assert summary["total_trades"] == 12
    assert summary["net_profit_after_cost_pct"] == 8.5
    assert summary["net_expectancy_t_stat"] == 2.4
    assert summary["profit_factor"] is None  # inf -> None
    assert summary["exit_reasons"] == {"tp": 5, "sl": 7}
    assert "unrelated" not in summary


def test_fold_summary_dispersion_and_consistency() -> None:
    folds = [
        {"status": "completed", "metrics": {"net_profit_after_cost": 10.0}},
        {"status": "completed", "metrics": {"net_profit_after_cost": -4.0}},
        {"status": "completed", "metrics": {"net_profit_after_cost": 6.0}},
        {"status": "failed", "metrics": {}},
    ]
    summary = fold_summary(folds)
    assert summary["completed_folds"] == 3
    assert summary["failed_folds"] == 1
    assert summary["positive_folds"] == 2
    assert summary["sign_consistency"] == round(2 / 3, 4)
    assert summary["net_worst"] == -4.0
    assert summary["net_best"] == 10.0
    assert summary["net_std"] > 0


def test_symbol_summary_shares() -> None:
    summary = symbol_summary({
        "BTC-USDT-SWAP": {"net_profit_after_cost": 9.0, "trades": 10},
        "ETH-USDT-SWAP": {"net_profit_after_cost": 1.0, "trades": 10},
    })
    assert summary["max_symbol_share"] == 0.9
    assert summary["symbols"][0]["symbol"] == "BTC-USDT-SWAP"


def test_symbol_summary_no_shares_when_unprofitable() -> None:
    summary = symbol_summary({"BTC-USDT-SWAP": {"net_profit_after_cost": -5.0}})
    assert summary["max_symbol_share"] is None


def test_benchmark_delta() -> None:
    delta = benchmark_delta({
        "net_profit_after_cost_pct": 12.0,
        "buy_and_hold": {"total_return_pct": 6.0, "symbols": ["BTC-USDT-SWAP"]},
    })
    assert delta is not None
    assert delta["delta_pct"] == 6.0
    assert delta["beat_benchmark"] is True

    assert benchmark_delta({"net_profit_after_cost_pct": 1.0}) is None
    assert benchmark_delta({"buy_and_hold": {"error": "no data"}}) is None


def test_build_run_evidence_shape() -> None:
    evidence = build_run_evidence(
        {"total_trades": 5, "net_profit_after_cost_pct": 3.0},
        fold_metrics=[{"status": "completed", "metrics": {"net_profit_after_cost": 3.0}}],
        per_symbol={"BTC-USDT-SWAP": {"net_profit_after_cost": 3.0, "trades": 5}},
    )
    assert set(evidence) == {"summary", "fold_summary", "symbol_summary", "benchmark_delta"}
    assert evidence["summary"]["total_trades"] == 5
    assert evidence["fold_summary"]["completed_folds"] == 1
