import asyncio

import pytest

from app.main import create_app
from app.services.prompt_runner import (
    PromptPayloadBundle,
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