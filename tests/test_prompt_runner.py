import asyncio

import pytest

from app.core.config import get_settings
from app.main import create_app
from app.services.prompt_runner import (
    PromptPayloadBundle,
    _evaluate_daily_loss_guard,
    compute_daily_loss_guard_state,
    execute_llm_decision,
)


class _StubLLM:
    async def run(self, payload):
        return {
            "action": "BUY",
            "confidence": 0.5,
            "position_size": 1.0,
            "_decision_origin": "fallback",
        }


class _StubMarket:
    def __init__(self) -> None:
        self.handle_called = False
        self.refresh_called = False

    async def handle_llm_decision(self, decision, context):
        self.handle_called = True
        return True

    async def refresh_snapshot(self, reason: str):
        self.refresh_called = True


def test_execute_llm_decision_skips_fallback_when_disabled(monkeypatch):
    app = create_app(enable_background_services=False)
    app.state.llm_service = _StubLLM()
    app.state.market_service = _StubMarket()
    app.state.backend_events = []

    async def fake_persist_prompt_run(app_obj, bundle, *, decision=None):
        return "prompt-id"

    monkeypatch.setattr(
        "app.services.prompt_runner.persist_prompt_run",
        fake_persist_prompt_run,
    )

    bundle = PromptPayloadBundle(
        payload={"context": {"symbol": "BTC-USDT-SWAP"}, "prompt": {}},
        runtime_meta={"fallback_orders_enabled": False},
        metadata={},
        snapshot={},
    )

    decision, prompt_id = asyncio.run(execute_llm_decision(app, bundle))

    assert decision["_decision_origin"] == "fallback"
    assert prompt_id == "prompt-id"
    assert app.state.market_service.handle_called is False
    assert app.state.market_service.refresh_called is False


def test_compute_daily_loss_guard_state_triggers_when_limit_hit():
    history = [
        {"observed_at": "2026-01-16T12:00:00Z", "total_eq_usd": 1000.0},
        {"observed_at": "2026-01-16T18:00:00Z", "total_eq_usd": 980.0},
    ]
    state = compute_daily_loss_guard_state(
        limit_pct=0.05,
        window_hours=24.0,
        current_equity=900.0,
        history=history,
        current_timestamp="2026-01-17T12:00:00Z",
    )
    assert state["active"] is True
    assert state["reason"] == "daily loss limit reached"
    assert state["change_pct"] == pytest.approx(0.1)


def test_compute_daily_loss_guard_state_handles_missing_history():
    state = compute_daily_loss_guard_state(
        limit_pct=0.05,
        window_hours=24.0,
        current_equity=900.0,
        history=[],
        current_timestamp="2026-01-17T12:00:00Z",
    )
    assert state["active"] is False
    assert state["reason"] == "insufficient equity history"


def test_evaluate_daily_loss_guard_tracks_metadata(monkeypatch):
    app = create_app(enable_background_services=False)
    app.state.runtime_config = {
        "guardrails": {"daily_loss_limit_pct": 0.05},
        "risk_locks": {},
        "auto_prompt_enabled": True,
        "auto_prompt_interval": 300,
    }
    settings = get_settings()
    original_db = settings.database_url
    settings.database_url = "postgres://test"

    history = [
        {"observed_at": "2026-01-16T12:00:00Z", "total_eq_usd": 1000.0},
        {"observed_at": "2026-01-16T18:00:00Z", "total_eq_usd": 980.0},
    ]

    async def fake_fetch_equity_window(_: float) -> list[dict[str, float]]:
        return history

    monkeypatch.setattr(
        "app.services.prompt_runner.fetch_equity_window",
        fake_fetch_equity_window,
    )

    snapshot = {
        "total_eq_usd": 900.0,
        "account_equity": 900.0,
        "total_account_value": 900.0,
        "generated_at": "2026-01-17T12:00:00Z",
    }

    async def _exercise() -> None:
        status = await _evaluate_daily_loss_guard(app, snapshot)
        assert status["active"] is True
        assert status["locked_at"] == "2026-01-17T12:00:00Z"
        assert status["execution_alert_logged"] is False
        assert status["auto_prompt_disabled"] is False

        runtime_state = app.state.runtime_config["risk_locks"]["daily_loss"]
        runtime_state["execution_alert_logged"] = True

        status_again = await _evaluate_daily_loss_guard(app, snapshot)
        assert status_again["execution_alert_logged"] is True

        recovered_snapshot = dict(snapshot)
        recovered_snapshot["total_eq_usd"] = 1100.0
        recovered_snapshot["account_equity"] = 1100.0
        status_recovered = await _evaluate_daily_loss_guard(app, recovered_snapshot)
        assert status_recovered["active"] is False
        assert status_recovered["execution_alert_logged"] is False
        assert status_recovered["locked_at"] == "2026-01-17T12:00:00Z"

    try:
        asyncio.run(_exercise())
    finally:
        settings.database_url = original_db


def test_manual_override_suppresses_guard(monkeypatch):
    app = create_app(enable_background_services=False)
    app.state.runtime_config = {
        "guardrails": {"daily_loss_limit_pct": 0.05},
        "risk_locks": {
            "daily_loss": {
                "manual_override_active": True,
                "manual_override_since": "2026-01-17T12:30:00Z",
                "locked_at": "2026-01-17T12:00:00Z",
            }
        },
        "auto_prompt_enabled": True,
        "auto_prompt_interval": 300,
    }
    settings = get_settings()
    original_db = settings.database_url
    settings.database_url = "postgres://test"

    history = [
        {"observed_at": "2026-01-16T12:00:00Z", "total_eq_usd": 1000.0},
        {"observed_at": "2026-01-16T18:00:00Z", "total_eq_usd": 980.0},
    ]

    async def fake_fetch_equity_window(_: float) -> list[dict[str, float]]:
        return history

    monkeypatch.setattr(
        "app.services.prompt_runner.fetch_equity_window",
        fake_fetch_equity_window,
    )

    snapshot = {
        "total_eq_usd": 900.0,
        "account_equity": 900.0,
        "total_account_value": 900.0,
        "generated_at": "2026-01-17T13:00:00Z",
    }

    async def _exercise() -> None:
        status = await _evaluate_daily_loss_guard(app, snapshot)
        assert status["active"] is False
        assert status["manual_override_active"] is True
        assert status["reason"] == "manual override active"

        recovered_snapshot = dict(snapshot)
        recovered_snapshot["total_eq_usd"] = 1100.0
        recovered_snapshot["account_equity"] = 1100.0
        status_recovered = await _evaluate_daily_loss_guard(app, recovered_snapshot)
        assert status_recovered["active"] is False
        assert status_recovered["manual_override_active"] is False

    try:
        asyncio.run(_exercise())
    finally:
        settings.database_url = original_db