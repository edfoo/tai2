"""Tests for the shared backtest REST client library (``client.py``).

Covers config building (single-strategy launcher overrides) and the
summary-row / close-reason helpers operating on serialised REST envelopes.
"""

from __future__ import annotations

from app.services.backtest.client import (
    build_single_strategy_launcher,
    count_close_reasons,
    count_stop_outs,
    count_timeouts,
    count_tp,
    fmt_cell,
    submit_many_and_poll,
    summary_row,
)


def _envelope(trades: list[dict]) -> dict:
    return {
        "job_id": "abc",
        "status": "completed",
        "run_id": "run123",
        "result": {
            "metrics": {"total_trades": len(trades), "win_rate": 50.0, "net_profit": 1.0},
            "trades": trades,
            "error": None,
            "duration_seconds": 1.2,
            "candles_processed": 10,
        },
    }


def test_build_single_strategy_launcher_applies_overrides():
    launcher = build_single_strategy_launcher(
        strategy_name="mean_reversion",
        capital=500.0,
        overrides={"rsi_oversold": 25},
    )
    assert launcher["notional_usd"] == 500.0
    assert launcher["mode"] == "launcher_only"
    strat = launcher["strategies"]["mean_reversion"]
    assert strat["enabled"] is True
    assert strat["rsi_oversold"] == 25


def test_build_single_strategy_launcher_canonical_defaults():
    launcher = build_single_strategy_launcher(strategy_name="trend_pullback", capital=1000.0)
    strat = launcher["strategies"]["trend_pullback"]
    # Canonical defaults must be present (a well-known key).
    assert "min_reward_risk_ratio" in strat


def test_summary_row_flattens_metrics():
    env = _envelope([
        {"close_reason": "tp", "pnl": 1.0},
        {"close_reason": "sl", "pnl": -1.0},
    ])
    row = summary_row(env, run_id="r", ltf="15m", htf="1H", symbols="BTC", strategies="mr")
    assert row["run_id"] == "r"
    assert row["ltf"] == "15m"
    assert row["m_total_trades"] == 2
    assert row["m_win_rate"] == 50.0
    assert row["m_net_profit"] == 1.0


def test_count_close_reasons():
    env = _envelope([
        {"close_reason": "tp"},
        {"close_reason": "sl"},
        {"close_reason": "timeout"},
        {"close_reason": "partial_tp"},
    ])
    assert count_tp(env) == 2  # "tp" + "partial_tp"
    assert count_stop_outs(env) == 1  # "sl"
    assert count_timeouts(env) == 1  # "timeout"
    assert count_close_reasons(env, "end_of_data") == 0


def test_count_helpers_handle_empty_trades():
    env = _envelope([])
    assert count_tp(env) == 0
    assert count_stop_outs(env) == 0
    assert count_timeouts(env) == 0


def test_fmt_cell_handles_none_and_nonfinite():
    # None (the sanitised inf/nan) must not raise on format.
    assert fmt_cell(None, 5) == "—".rjust(5)
    assert fmt_cell(float("inf"), 5) == "inf".rjust(5)
    assert fmt_cell(float("-inf"), 5) == "-inf".rjust(5)
    assert fmt_cell(float("nan"), 5) == "nan".rjust(5)
    # Finite numbers and values right-justify normally.
    assert fmt_cell(12, 5) == "   12"
    assert fmt_cell(1.5, 5) == "  1.5"
    assert fmt_cell("x", 3) == "  x"


def test_submit_many_and_poll_empty():
    assert submit_many_and_poll(base_url="http://x", payloads=[]) == []


def test_submit_many_and_poll_collects_errors(monkeypatch):
    """Per-variant failures are collected as (None, exc) pairs, not raised."""
    def _fake_submit(base_url, payload, poll_interval=1.5):
        if payload["fail"]:
            raise RuntimeError("boom")
        return {"job_id": "j", "result": {"metrics": {"total_trades": 1}}}

    monkeypatch.setattr(
        "app.services.backtest.client.submit_and_poll", _fake_submit
    )
    results = submit_many_and_poll(
        base_url="http://x",
        payloads=[{"fail": False}, {"fail": True}, {"fail": False}],
        max_workers=3,
    )
    assert len(results) == 3
    # Success envelopes come through; the failure is captured.
    assert results[0][0]["result"]["metrics"]["total_trades"] == 1
    assert results[1][0] is None and isinstance(results[1][1], RuntimeError)
    assert results[2][0] is not None
