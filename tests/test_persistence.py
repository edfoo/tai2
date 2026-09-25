"""Tests for the backtest persistence module (app/services/backtest/persistence.py).

Covers the JSON round-trip (result -> dict -> result), CSV writing/reading,
result-file save/load/delete, and run-id generation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services.backtest.models import (
    BacktestConfig,
    BacktestResult,
    EquityPoint,
    GridConfig,
    GridParamDef,
    GridResult,
    GridRunResult,
    SimPosition,
)
from app.services.backtest import persistence as P


def _sample_result() -> BacktestResult:
    config = BacktestConfig(
        symbols=["BTC-USDT-SWAP"],
        timeframe="15m",
        start_ts=1_000_000_000,  # 2001-09-09
        end_ts=1_000_086_400,
        initial_capital=1000.0,
        strategy_names=["mean_reversion", "liquidity_sweep"],
        launcher_config={
            "notional_usd": 75.0,
            "strategies": {
                "spike_continuation": {"enabled": True, "require_oi_confirmation": True}
            },
        },
        strategy_config={"trade_management": {"enabled": True}},
        guardrails_config={"max_position_pct": 0.25},
        warmup_candles=321,
        evaluation_mode="closed",
        evaluation_timeframe="15m",
        taker_fee_bps=6.0,
        slippage_bps=2.0,
        funding_rate_pct=0.01,
    )
    trade = SimPosition(
        symbol="BTC-USDT-SWAP",
        direction="long",
        size=0.1,
        entry_price=50_000.0,
        entry_ts=1_000_000_000,
        trade_id="trade-1",
        tp_price=52_000.0,
        sl_price=48_000.0,
        strategy_name="mean_reversion",
        close_price=52_000.0,
        close_ts=1_000_086_400,
        close_reason="tp",
        pnl=200.0,
        pnl_pct=4.0,
        entry_fee=1.0,
        exit_fee=2.0,
        funding=0.5,
        slippage_cost=0.3,
        max_favorable_pct=5.0,
        max_adverse_pct=-1.0,
        candles_held=12,
    )
    return BacktestResult(
        config=config,
        trades=[trade],
        equity_curve=[EquityPoint(ts=1_000_000_000, equity=1000.0, open_positions=0)],
        per_strategy={
            "mean_reversion": {
                "total_trades": 1, "win_rate": 100.0, "net_profit": 200.0,
                "profit_factor": 2.0,
            }
        },
        metrics={
            "total_trades": 1, "win_rate": 100.0, "net_profit": 200.0,
            "profit_factor": 2.0, "max_drawdown_pct": 0.0,
            "sharpe_per_candle": 0.0,
        },
        duration_seconds=0.5,
        candles_processed=200,
        data_provenance=[{
            "symbol": "BTC-USDT-SWAP",
            "timeframe": "15m",
            "content_sha256": "a" * 64,
            "cache_hit": True,
        }],
        error=None,
    )


def _sample_grid_result() -> GridResult:
    backtest = _sample_result()
    candidate = GridRunResult(
        params={"strategies.mean_reversion.rsi_oversold": 30.0},
        result=backtest,
        rank_score=1.25,
        below_min_trades=False,
        fold_metrics=[
            {
                "fold": 1,
                "start_ts": 100,
                "end_ts": 200,
                "status": "completed",
                "trade_count": 1,
                "rank_score": 1.25,
                "metrics": {"net_profit_pct": 2.0, "max_drawdown_pct": 0.5},
            },
            {
                "fold": 2,
                "start_ts": 200,
                "end_ts": 300,
                "status": "failed",
                "error": "synthetic fold failure",
                "trade_count": 0,
                "rank_score": None,
                "metrics": {},
            },
        ],
    )
    return GridResult(
        config=GridConfig(
            base_config=backtest.config,
            params=[GridParamDef(
                "strategies.mean_reversion.rsi_oversold", [25.0, 30.0], "RSI"
            )],
            rank_by="net_profit_pct",
            min_trades=1,
            validation_folds=2,
            validation_train_ratio=0.6,
            final_holdout_fraction=0.15,
            search_mode="random",
            combination_budget=2,
            random_seed=17,
        ),
        runs=[candidate],
        ranked=[candidate],
        final_holdout=GridRunResult(
            params=dict(candidate.params),
            result=backtest,
            rank_score=-0.5,
            below_min_trades=False,
            fold_metrics=[{
                "fold": 1,
                "role": "final_holdout",
                "start_ts": 300,
                "end_ts": 400,
                "status": "completed",
                "trade_count": 1,
                "rank_metric": "net_profit_pct",
                "rank_score": -0.5,
                "metrics": {"net_profit_pct": -0.5},
            }],
        ),
        total_combinations=8,
        attempted_combinations=2,
        search_mode="random",
        random_seed=17,
        data_provenance=backtest.data_provenance,
    )


# ── JSON round-trip ──────────────────────────────────────────────────────────


def test_result_to_dict_is_json_serialisable() -> None:
    d = P.result_to_dict(_sample_result())
    # Must be serialisable without errors.
    json.dumps(d)
    assert d["config"]["timeframe"] == "15m"
    assert d["trades_count"] == 1
    assert d["trades"][0]["direction"] == "long"


def test_result_to_dict_sanitises_non_finite_floats() -> None:
    """inf/-inf/nan must become None (JSONResponse uses allow_nan=False)."""
    result = _sample_result()
    result.metrics["profit_factor"] = float("inf")
    result.per_strategy["mean_reversion"]["profit_factor"] = float("inf")
    result.metrics["some_nan"] = float("nan")

    d = P.result_to_dict(result)
    # json.dumps with allow_nan=False must not raise (this is what FastAPI does).
    json.dumps(d, allow_nan=False)

    assert d["metrics"]["profit_factor"] is None
    assert d["metrics"]["some_nan"] is None
    assert d["per_strategy"]["mean_reversion"]["profit_factor"] is None
    # A valid finite float is left untouched.
    assert d["metrics"]["net_profit"] == 200.0


def test_round_trip_preserves_result() -> None:
    d = P.result_to_dict(_sample_result())
    back = P.result_from_dict(d)
    assert back is not None
    assert back.config.timeframe == "15m"
    assert back.config.symbols == ["BTC-USDT-SWAP"]
    assert back.config.strategy_names == ["mean_reversion", "liquidity_sweep"]
    assert back.config.launcher_config["notional_usd"] == 75.0
    assert back.config.strategy_config == {"trade_management": {"enabled": True}}
    assert back.config.guardrails_config == {"max_position_pct": 0.25}
    assert back.config.warmup_candles == 321
    assert back.config.evaluation_mode == "closed"
    assert back.config.evaluation_timeframe == "15m"
    assert back.config.taker_fee_bps == 6.0
    assert back.config.slippage_bps == 2.0
    assert back.config.funding_rate_pct == 0.01
    assert back.data_provenance[0]["content_sha256"] == "a" * 64
    assert back.assumptions["cost_model"]["funding_source"].startswith("constant configured rate")
    assert back.assumptions["enabled_gates_with_unavailable_inputs"] == [{
        "strategy": "spike_continuation",
        "gate": "require_oi_confirmation",
        "unavailable_input": "historical open interest",
        "backtest_behavior": "passes neutrally when open-interest data is absent",
    }]
    assert len(back.trades) == 1
    t = back.trades[0]
    assert t.direction == "long"
    assert t.entry_price == 50_000.0
    assert t.trade_id == "trade-1"
    assert t.tp_price == 52_000.0
    assert t.close_reason == "tp"
    assert t.pnl == 200.0
    assert t.entry_fee == 1.0
    assert t.exit_fee == 2.0
    assert t.funding == 0.5
    assert t.slippage_cost == 0.3
    assert t.net_pnl == pytest.approx(196.5)
    assert t.max_favorable_pct == 5.0
    assert len(back.equity_curve) == 1
    assert back.equity_curve[0].equity == 1000.0
    assert back.metrics["total_trades"] == 1
    assert back.per_strategy["mean_reversion"]["net_profit"] == 200.0


def test_result_from_dict_rejects_non_dict() -> None:
    assert P.result_from_dict(None) is None
    assert P.result_from_dict("nope") is None
    assert P.result_from_dict({"no": "config"}) is None


def test_result_from_dict_bad_types_returns_none() -> None:
    d = P.result_to_dict(_sample_result())
    d["trades"] = "not a list"  # corrupt
    # trades is accessed with `or []` and filtered by isinstance, so this
    # should still parse safely (not raise).
    back = P.result_from_dict(d)
    assert back is not None
    assert back.trades == []


def test_legacy_result_config_loads_with_backtest_defaults() -> None:
    data = P.result_to_dict(_sample_result())
    data["config"] = {
        "timeframe": "15m",
        "symbols": ["BTC-USDT-SWAP"],
        "start_ts": 100,
        "end_ts": 200,
        "strategy_names": ["mean_reversion"],
        "initial_capital": 500.0,
    }

    result = P.result_from_dict(data)

    assert result is not None
    assert result.config.launcher_config == {}
    assert result.config.guardrails_config == {}
    assert result.config.warmup_candles == 200
    assert result.config.taker_fee_bps == 5.0


def test_grid_result_round_trip_preserves_config_ranking_and_fold_evidence() -> None:
    payload = P.result_to_dict(_sample_grid_result())
    json.dumps(payload, allow_nan=False)

    result = P.result_from_dict(payload)

    assert isinstance(result, GridResult)
    assert result.config.base_config.guardrails_config == {"max_position_pct": 0.25}
    assert result.config.params[0].values == [25.0, 30.0]
    assert result.config.validation_folds == 2
    assert result.config.final_holdout_fraction == pytest.approx(0.15)
    assert result.config.search_mode == "random"
    assert result.total_combinations == 8
    assert result.attempted_combinations == 2
    assert result.ranked[0] is result.runs[0]
    assert result.runs[0].fold_metrics[0]["metrics"]["net_profit_pct"] == 2.0
    assert result.runs[0].fold_metrics[1]["status"] == "failed"
    assert result.runs[0].result is not None
    assert result.runs[0].result.trades[0].pnl == 200.0
    assert result.assumptions["cost_model"]["funding_interval_ms"] == 28_800_000
    assert result.data_provenance[0]["cache_hit"] is True
    assert result.final_holdout is not None
    assert result.final_holdout.fold_metrics[0]["role"] == "final_holdout"


# ── Summary row / CSV ────────────────────────────────────────────────────────


def test_result_summary_row_shape() -> None:
    row = P.result_summary_row(_sample_result(), run_id="abc_15m", ltf="15m", htf="1H")
    assert row["run_id"] == "abc_15m"
    assert row["ltf"] == "15m"
    assert row["htf"] == "1H"
    assert row["m_total_trades"] == 1
    assert row["m_win_rate"] == 100.0
    assert row["symbols"] == "BTC-USDT-SWAP"


def test_write_and_read_comparison_csv(tmp_path: Path) -> None:
    row = P.result_summary_row(_sample_result(), run_id="r1", ltf="15m", htf="1H")
    path = P.write_comparison_csv([row], output_dir=tmp_path, append=False)
    assert path.exists()
    read = P.read_comparison_csv(tmp_path)
    assert len(read) == 1
    assert read[0]["run_id"] == "r1"
    assert float(read[0]["m_total_trades"]) == 1


def test_append_dedups_by_run_id(tmp_path: Path) -> None:
    row = P.result_summary_row(_sample_result(), run_id="r1", ltf="15m", htf="1H")
    P.write_comparison_csv([row], output_dir=tmp_path, append=False)
    # Same run_id appended again -> ignored.
    P.write_comparison_csv([row], output_dir=tmp_path, append=True)
    read = P.read_comparison_csv(tmp_path)
    assert len(read) == 1

    # A different run_id is appended.
    row2 = P.result_summary_row(_sample_result(), run_id="r2", ltf="1H", htf="4H")
    P.write_comparison_csv([row2], output_dir=tmp_path, append=True)
    read = P.read_comparison_csv(tmp_path)
    assert len(read) == 2


# ── Save / load / delete files ──────────────────────────────────────────────


def test_save_then_load_result(tmp_path: Path) -> None:
    run_id = "20260101_000000_15m"
    saved = P.save_result(_sample_result(), run_id=run_id, output_dir=tmp_path)
    assert saved.name == f"{run_id}_results.json"
    assert saved.exists()

    # Per-strategy breakdown wrote too.
    assert (tmp_path / f"{run_id}_per_strategy.json").exists()

    loaded = P.load_result(saved)
    assert loaded is not None
    assert loaded.config.timeframe == "15m"
    assert len(loaded.trades) == 1
    assert loaded.metrics["net_profit"] == 200.0

    # CSV row was also appended by save_result.
    assert P.read_comparison_csv(tmp_path)


def test_save_then_load_grid_result(tmp_path: Path) -> None:
    saved = P.save_result(_sample_grid_result(), run_id="sweep_15m", output_dir=tmp_path)

    loaded = P.load_result(saved)

    assert isinstance(loaded, GridResult)
    assert loaded.ranked[0].fold_metrics[1]["status"] == "failed"
    assert loaded.ranked[0].result is not None
    assert loaded.ranked[0].result.equity_curve[0].equity == 1000.0
    assert loaded.assumptions["entry_fill"].startswith("Signal evaluated at candle close")
    assert loaded.final_holdout is not None
    assert loaded.final_holdout.rank_score == pytest.approx(-0.5)


def test_load_missing_file_returns_none(tmp_path: Path) -> None:
    assert P.load_result(tmp_path / "missing.json") is None


def test_delete_result_removes_breakdown(tmp_path: Path) -> None:
    run_id = "run_x"
    P.save_result(_sample_result(), run_id=run_id, output_dir=tmp_path)
    results = (tmp_path / f"{run_id}_results.json")
    breakdown = (tmp_path / f"{run_id}_per_strategy.json")
    assert breakdown.exists()
    P.delete_result(results, output_dir=tmp_path)
    assert not results.exists()
    assert not breakdown.exists()


def test_iter_result_files_newest_first(tmp_path: Path) -> None:
    # Create two result files with different mtimes.
    r1 = P.save_result(_sample_result(), run_id="aaa", output_dir=tmp_path)
    r2 = P.save_result(_sample_result(), run_id="bbb", output_dir=tmp_path)
    # Force bbb older than aaa.
    os_import = __import__("os")
    old = os_import.stat(r2).st_mtime - 100
    os_import.utime(r2, (old, old))
    files = list(P.iter_result_files(tmp_path))
    names = [f.name for f in files]
    assert "aaa_results.json" in names and "bbb_results.json" in names
    # Newest (aaa) first.
    assert names[0] == "aaa_results.json"


# ── Run-id helper ────────────────────────────────────────────────────────────


def test_make_run_id() -> None:
    rid = P.make_run_id("15m")
    assert rid.endswith("_15m")
    assert len(rid) >= len("YYYYMMDD_HHMMSS_15m")