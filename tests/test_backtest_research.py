"""Tests for the P3 research-workflow bundle builder."""

from __future__ import annotations

from app.services.backtest.research import (
    build_research_bundle,
    concentration,
    fold_consistency,
    render_bundle_markdown,
    stress_comparison,
)


def _baseline(net: float = 5.0, trades: int = 20) -> dict:
    return {
        "metrics": {
            "total_trades": trades,
            "net_profit_after_cost": net,
            "net_profit_after_cost_pct": net / 10.0,
        },
        "config": {"symbols": ["BTC-USDT-SWAP"]},
        "assumptions": {"cost_model": {"slippage_mode": "ohlcv_liquidity"}},
        "data_provenance": [{"symbol": "BTC-USDT-SWAP", "content_sha256": "abc"}],
    }


def _fold(fold: int, net: float, status: str = "completed") -> dict:
    return {
        "fold": fold,
        "status": status,
        "start_ts": fold * 1000,
        "end_ts": (fold + 1) * 1000,
        "trade_count": 5,
        "rank_score": net,
        "metrics": {
            "net_profit_after_cost": net,
            "net_profit_after_cost_pct": net / 10.0,
        },
    }


def _run(params: dict, net: float, *, folds: list[dict] | None = None,
         per_symbol: dict | None = None, trades: list[dict] | None = None) -> dict:
    return {
        "params": params,
        "rank_score": net,
        "below_min_trades": False,
        "fold_metrics": folds or [],
        "result": {
            "metrics": {
                "total_trades": 20,
                "net_profit_after_cost": net,
                "net_profit_after_cost_pct": net / 10.0,
            },
            "per_symbol": per_symbol or {},
            "trades": trades or [],
        },
    }


def _grid(runs: list[dict], *, ranked: list[int] | None = None,
          holdout: dict | None = None) -> dict:
    return {
        "config": {"rank_by": "net_profit_after_cost_pct", "min_trades": 5},
        "runs": runs,
        "ranked_indexes": ranked if ranked is not None else list(range(len(runs))),
        "total_combinations": len(runs),
        "attempted_combinations": len(runs),
        "search_mode": "exhaustive",
        "random_seed": 0,
        "final_holdout": holdout,
    }


class TestFoldConsistency:
    def test_counts_positive_completed_folds(self) -> None:
        result = fold_consistency([
            _fold(1, 3.0),
            _fold(2, -1.0),
            _fold(3, 2.0),
            _fold(4, 1.0),
        ])
        assert result["completed_folds"] == 4
        assert result["positive_folds"] == 3
        assert result["consistency"] == 0.75

    def test_failed_and_skipped_folds_are_reported(self) -> None:
        result = fold_consistency([
            _fold(1, 3.0),
            _fold(2, 0.0, status="failed"),
            _fold(3, 0.0, status="skipped"),
        ])
        assert result["completed_folds"] == 1
        assert result["failed_folds"] == 1
        assert result["skipped_folds"] == 1
        assert result["consistency"] == 1.0


class TestConcentration:
    def test_symbol_and_trade_shares(self) -> None:
        result = concentration(
            per_symbol={
                "BTC-USDT-SWAP": {"net_profit_after_cost": 9.0, "trades": 10},
                "ETH-USDT-SWAP": {"net_profit_after_cost": 1.0, "trades": 10},
            },
            trades=[
                {"net_pnl": 8.0},
                {"net_pnl": 1.0},
                {"net_pnl": 1.0},
            ],
            total_net_profit=10.0,
        )
        assert result["max_symbol_share"] == 0.9
        assert result["max_trade_share"] == 0.8

    def test_no_shares_when_not_profitable(self) -> None:
        result = concentration(
            per_symbol={"BTC-USDT-SWAP": {"net_profit_after_cost": -5.0}},
            trades=[{"net_pnl": -5.0}],
            total_net_profit=-5.0,
        )
        assert result["max_symbol_share"] is None
        assert result["max_trade_share"] is None


class TestStressComparison:
    def test_reports_delta_and_survival(self) -> None:
        rows = stress_comparison(
            {"net_profit_after_cost": 10.0, "net_profit_after_cost_pct": 1.0},
            [
                {"label": "fees_2x", "overrides": {"taker_fee_bps": 10.0},
                 "metrics": {"net_profit_after_cost": 6.0, "net_profit_after_cost_pct": 0.6}},
                {"label": "combined", "overrides": {},
                 "metrics": {"net_profit_after_cost": -2.0, "net_profit_after_cost_pct": -0.2}},
            ],
        )
        assert rows[0]["delta_net_profit_after_cost"] == -4.0
        assert rows[0]["survives"] is True
        assert rows[1]["survives"] is False


class TestBuildResearchBundle:
    def test_robust_candidate_passes_screening(self) -> None:
        runs = [
            _run({"threshold": 1}, 4.9, folds=[_fold(i, 2.0) for i in range(1, 5)]),
            _run({"threshold": 2}, 5.0, folds=[_fold(i, 2.5) for i in range(1, 5)]),
            _run({"threshold": 3}, 4.8, folds=[_fold(i, 2.2) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(),
            grid=_grid(runs, ranked=[1, 2, 0]),
            stress_runs=[
                {"label": "fees_2x", "overrides": {}, "metrics": {"net_profit_after_cost": 3.0}},
            ],
            holdout=_run({"threshold": 2}, 4.0),
        )
        assert bundle["verdict"]["status"] == "candidate"
        assert bundle["sweep"]["best_params"] == {"threshold": 2}
        assert bundle["sweep"]["fold_consistency"]["consistency"] == 1.0

    def test_single_point_optimum_is_flagged(self) -> None:
        runs = [
            _run({"threshold": 1}, 1.0, folds=[_fold(i, 1.0) for i in range(1, 5)]),
            _run({"threshold": 2}, 50.0, folds=[_fold(i, 50.0) for i in range(1, 5)]),
            _run({"threshold": 3}, 1.0, folds=[_fold(i, 1.0) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(),
            grid=_grid(runs, ranked=[1, 0, 2]),
        )
        assert bundle["sweep"]["robustness"]["single_point_optimum"] is True
        assert bundle["verdict"]["status"] == "inconclusive"
        assert any("lone spike" in r for r in bundle["verdict"]["reasons"])

    def test_unprofitable_best_is_rejected(self) -> None:
        runs = [
            _run({"threshold": 1}, -3.0, folds=[_fold(i, -3.0) for i in range(1, 5)]),
            _run({"threshold": 2}, -1.0, folds=[_fold(i, -1.0) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(net=-2.0),
            grid=_grid(runs, ranked=[1, 0]),
        )
        assert bundle["verdict"]["status"] == "reject"
        assert any("not profitable" in r for r in bundle["verdict"]["reasons"])

    def test_failed_stress_rejects_candidate(self) -> None:
        runs = [
            _run({"threshold": 1}, 4.0, folds=[_fold(i, 2.0) for i in range(1, 5)]),
            _run({"threshold": 2}, 5.0, folds=[_fold(i, 2.5) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(),
            grid=_grid(runs, ranked=[1, 0]),
            stress_runs=[
                {"label": "combined_adverse", "overrides": {},
                 "metrics": {"net_profit_after_cost": -1.0}},
            ],
        )
        assert bundle["verdict"]["status"] == "reject"
        assert any("does not survive" in r for r in bundle["verdict"]["reasons"])

    def test_symbol_concentration_is_flagged(self) -> None:
        runs = [
            _run(
                {"threshold": 1}, 10.0,
                folds=[_fold(i, 2.0) for i in range(1, 5)],
                per_symbol={
                    "BTC-USDT-SWAP": {"net_profit_after_cost": 9.5, "trades": 10},
                    "ETH-USDT-SWAP": {"net_profit_after_cost": 0.5, "trades": 10},
                },
            ),
            _run({"threshold": 2}, 9.0, folds=[_fold(i, 2.0) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(),
            grid=_grid(runs, ranked=[0, 1]),
        )
        assert any("concentrated in one symbol" in r for r in bundle["verdict"]["reasons"])

    def test_baseline_only_is_inconclusive(self) -> None:
        bundle = build_research_bundle(baseline=_baseline())
        assert bundle["verdict"]["status"] == "inconclusive"
        assert bundle["sweep"] is None

    def test_bundle_is_json_safe_and_renders(self) -> None:
        import json

        runs = [
            _run({"threshold": 1}, 4.0, folds=[_fold(i, 2.0) for i in range(1, 5)]),
            _run({"threshold": 2}, 5.0, folds=[_fold(i, 2.5) for i in range(1, 5)]),
        ]
        bundle = build_research_bundle(
            baseline=_baseline(),
            grid=_grid(runs, ranked=[1, 0]),
            holdout=_run({"threshold": 2}, 4.0),
            stress_runs=[
                {"label": "fees_2x", "overrides": {}, "metrics": {"net_profit_after_cost": 3.0}},
            ],
        )
        # Must round-trip through JSON (no inf/nan).
        json.dumps(bundle)
        markdown = render_bundle_markdown(bundle)
        assert "# Backtest Research Bundle" in markdown
        assert "Verdict" in markdown
        assert "Final holdout" in markdown
