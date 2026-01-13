from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, List

import pytest

from app.services.market_service import MarketService


class DummyStateService:
    def __init__(self) -> None:
        self.snapshots: list[dict[str, Any]] = []

    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.snapshots.append(snapshot)


class DummySnapshotStore(DummyStateService):
    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def test_market_service_builds_snapshot_with_mocked_fetchers(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> dict[str, Any]:
        state = DummyStateService()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=object(),
            market_api=object(),
            public_api=object(),
        )
        service._trade_buffers[service.symbol] = deque(
            [
                {"side": 1.0, "volume": 2.0},
                {"side": -1.0, "volume": 0.5},
            ],
            maxlen=500,
        )
        order_book = {
            "bids": [[100.0, 3.0], [99.5, 1.0]],
            "asks": [[100.5, 1.0], [101.0, 0.5]],
        }

        async def fake_fetch_positions() -> list[dict[str, Any]]:
            return [{"instId": service.symbol, "pos": "1"}]

        async def fake_fetch_account_balance() -> dict[str, Any]:
            return {
                "details": [{"ccy": "USDT", "eq": "1000"}],
                "total_equity": 1000.0,
                "total_account_value": 1000.0,
                "total_eq_usd": 1000.0,
            }

        async def fake_fetch_order_book(symbol: str) -> dict[str, Any]:
            return order_book

        async def fake_fetch_ticker(symbol: str) -> dict[str, Any]:
            return {"last": "42050"}

        async def fake_fetch_funding(symbol: str) -> dict[str, Any]:
            return {"fundingRate": "0.0001"}

        async def fake_fetch_open_interest(symbol: str) -> dict[str, Any]:
            return {"oi": "12345"}

        async def fake_fetch_ohlcv(symbol: str) -> list[list[Any]]:
            rows: List[list[Any]] = []
            for i in range(30):
                rows.append(
                    [
                        str(1_700_000_000_000 + i * 60_000),
                        str(42000 + i),
                        str(42100 + i),
                        str(41900 + i),
                        str(42050 + i),
                        str(5 + i / 10),
                    ]
                )
            return rows

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)
        monkeypatch.setattr(service, "_fetch_account_balance", fake_fetch_account_balance)
        monkeypatch.setattr(service, "_fetch_order_book", fake_fetch_order_book)
        monkeypatch.setattr(service, "_fetch_ticker", fake_fetch_ticker)
        monkeypatch.setattr(service, "_fetch_funding_rate", fake_fetch_funding)
        monkeypatch.setattr(service, "_fetch_open_interest", fake_fetch_open_interest)
        monkeypatch.setattr(service, "_fetch_ohlcv", fake_fetch_ohlcv)

        return await service._build_snapshot()

    snapshot = asyncio.run(scenario())
    assert snapshot["positions"][0]["pos"] == "1"
    assert snapshot["account"][0]["eq"] == "1000"
    assert snapshot["account_equity"] == pytest.approx(1000.0)
    assert snapshot["custom_metrics"]["cumulative_volume_delta"] == pytest.approx(1.5)
    assert snapshot["custom_metrics"]["order_flow_imbalance"]["net"] == pytest.approx(2.5)
    assert snapshot["indicators"]["bollinger_bands"]["upper"] is not None
    assert snapshot["indicators"]["vwap"] is not None
    assert snapshot["market_data"][snapshot["symbol"]]["ticker"]["last"] == "42050"


def test_indicator_helper_handles_empty_data() -> None:
    indicators = MarketService._compute_indicators([])
    assert indicators["vwap"] is None
    assert indicators["bollinger_bands"] == {}
    assert indicators["stoch_rsi"] == {}


def test_handle_llm_decision_blocks_without_positions(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[int, list[str]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=None,
            market_api=None,
            public_api=None,
        )

        fetch_calls = {"count": 0}

        async def fake_fetch_positions() -> list[dict[str, Any]]:
            fetch_calls["count"] += 1
            return []

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)

        captured: list[str] = []
        monkeypatch.setattr(service, "_emit_debug", lambda message: captured.append(message))

        await service.handle_llm_decision(
            {"action": "BUY", "confidence": 0.9},
            {"symbol": service.symbol},
        )

        return fetch_calls["count"], captured

    calls, messages = asyncio.run(scenario())
    assert calls == 1
    assert any("Execution disabled" in message for message in messages)


def test_handle_llm_decision_enforces_min_leverage(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=None,
            market_api=None,
            public_api=None,
            trade_api=object(),
        )

        submit_called = {"value": False}

        async def fake_submit_order(*args, **kwargs):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 5,
                "max_leverage": 5,
                "max_position_pct": 0.01,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 1000,
                "available_eq_usd": 1000,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.001,
            },
            "positions": [],
        }
        decision = {"action": "BUY", "confidence": 0.9}
        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is False
    assert submit_called is False
