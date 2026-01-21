from __future__ import annotations

import asyncio
from collections import deque
from types import MethodType
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


def test_normalize_account_balances_preserves_unknown_available_margin() -> None:
    payload = [
        {
            "details": [
                {
                    "ccy": "USDT",
                    "eq": "100",
                    "eqUsd": "100",
                }
            ]
        }
    ]
    normalized = MarketService._normalize_account_balances(payload)
    assert normalized["available_eq_usd"] is None
    assert normalized["available_equity"] is None


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
        service._account_api = None
        service._market_api = None
        service._public_api = None

        submit_called = {"value": False}

        async def fake_submit_order(*args, **kwargs):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 0.01,
        )

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 5,
                "max_leverage": 5,
                "max_position_pct": 1.0,
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


def test_handle_llm_respects_symbol_position_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, dict[str, float]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=object(),
            account_api=None,
            market_api=None,
            public_api=None,
        )
        service._account_api = None
        service._market_api = None
        service._public_api = None

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 10.0,
        )
        async def fake_tier_guard(**kwargs):
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        recorded: dict[str, float] = {}
        service._instrument_specs["BTC-USDT-SWAP"] = {
            "lot_size": 0.001,
            "min_size": 0.001,
            "tick_size": 0.1,
        }

        original_quantize = MarketService._quantize_order_size

        def fake_quantize(self: MarketService, symbol: str, size: float) -> float:
            recorded["pre_quantize_size"] = size
            return original_quantize(self, symbol, size)

        monkeypatch.setattr(MarketService, "_quantize_order_size", fake_quantize)

        async def fake_submit_order(*, size: float, **kwargs):
            recorded["size"] = size
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)

        context = {
            "symbol": "BTC-USDT-SWAP",
            "guardrails": {
                "min_leverage": 1,
                "max_leverage": 2,
                "max_position_pct": 0.5,
                "symbol_position_caps": {"BTC-USDT-SWAP": 0.1},
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
                "min_size": 0.0001,
            },
            "positions": [],
        }
        executed = await service.handle_llm_decision({"action": "BUY", "confidence": 0.9}, context)
        return executed, recorded

    executed, recorded = asyncio.run(scenario())
    assert executed is True
    assert recorded.get("pre_quantize_size") == pytest.approx(2.0)
    assert recorded.get("size", 0.0) == pytest.approx(2.0)


def test_handle_llm_blocks_isolated_when_quote_margin_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, list[dict[str, Any]]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=object(),
            account_api=None,
            market_api=None,
            public_api=None,
        )
        service._account_api = None
        service._market_api = None
        service._public_api = None

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 5.0,
        )

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 3,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 1000,
                "available_eq_usd": 1000,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.001,
            },
            "positions": [],
        }

        executed = await service.handle_llm_decision({"action": "SELL", "confidence": 0.6}, context)
        return executed, list(service._execution_feedback)

    executed, feedback = asyncio.run(scenario())
    assert executed is False
    assert feedback
    latest = feedback[-1]
    assert latest["message"] == "Isolated margin unavailable"
    assert latest["meta"]["trade_mode"] == "isolated"


def test_handle_llm_attempts_isolated_margin_top_up(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTradeApi:
        def __init__(self) -> None:
            self.payloads: list[dict[str, Any]] = []

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.payloads.append(payload)
            return {
                "code": "0",
                "data": [
                    {
                        "ordId": "123",
                        "state": "filled",
                        "fillPx": "100",
                        "fillSz": payload.get("sz", "0"),
                    }
                ],
            }

    class DummyAccountApi:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(
            self,
            instId: str,
            posSide: str,
            amt: str,
            *,
            type: str = "add",
            loanTrans: str = "",
            subAcct: str | None = None,
        ) -> dict[str, Any]:
            self.calls.append(
                {
                    "instId": instId,
                    "posSide": posSide,
                    "amt": amt,
                    "type": type,
                    "subAcct": subAcct,
                }
            )
            return {"code": "0", "data": [{"sCode": "0"}]}

    class BalanceResponder:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                return {
                    "available_balances": {},
                    "available_eq_usd": 0.0,
                    "total_eq_usd": 1000.0,
                }
            return {
                "available_balances": {
                    "USDT": {
                        "available_usd": 250.0,
                        "cash": 250.0,
                    }
                },
                "available_eq_usd": 250.0,
                "total_eq_usd": 1000.0,
            }

    async def scenario() -> tuple[bool, DummyAccountApi, DummyTradeApi]:
        state = DummySnapshotStore()
        trade_api = DummyTradeApi()
        account_api = DummyAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=account_api,
            market_api=None,
            public_api=None,
        )

        balance_responder = BalanceResponder()

        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return await balance_responder()

        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        async def fake_fetch_positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 3,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 1000,
                "available_eq_usd": 0.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.001,
            },
            "positions": [],
        }
        decision = {"action": "BUY", "confidence": 0.6, "position_size": 1.0}
        executed = await service.handle_llm_decision(decision, context)
        return executed, account_api, trade_api

    executed, account_api, trade_api = asyncio.run(scenario())
    assert executed is True
    assert account_api.calls, "expected isolated margin top-up call"
    latest_call = account_api.calls[-1]
    assert latest_call["instId"] == "BTC-USDT-SWAP"
    assert latest_call["posSide"] in {"long", "net"}
    assert trade_api.payloads, "order should be sent after margin top-up"


def test_handle_llm_top_up_when_margin_partially_funded(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTradeApi:
        def __init__(self) -> None:
            self.payloads: list[dict[str, Any]] = []

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.payloads.append(payload)
            return {
                "code": "0",
                "data": [
                    {
                        "ordId": "123",
                        "state": "filled",
                        "fillPx": "100",
                        "fillSz": payload.get("sz", "0"),
                    }
                ],
            }

    class DummyAccountApi:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(
            self,
            instId: str,
            posSide: str,
            amt: str,
            *,
            type: str = "add",
            loanTrans: str = "",
            subAcct: str | None = None,
        ) -> dict[str, Any]:
            self.calls.append(
                {
                    "instId": instId,
                    "posSide": posSide,
                    "amt": amt,
                    "type": type,
                    "subAcct": subAcct,
                }
            )
            return {"code": "0", "data": [{"sCode": "0"}]}

    class BalanceResponder:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                return {
                    "available_balances": {
                        "USDT": {
                            "available_usd": 40.0,
                            "cash": 40.0,
                        }
                    },
                    "available_eq_usd": 40.0,
                    "total_eq_usd": 1000.0,
                }
            return {
                "available_balances": {
                    "USDT": {
                        "available_usd": 250.0,
                        "cash": 250.0,
                    }
                },
                "available_eq_usd": 250.0,
                "total_eq_usd": 1000.0,
            }

    async def scenario() -> tuple[bool, DummyAccountApi, DummyTradeApi]:
        state = DummySnapshotStore()
        trade_api = DummyTradeApi()
        account_api = DummyAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=account_api,
            market_api=None,
            public_api=None,
        )

        balance_responder = BalanceResponder()

        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return await balance_responder()

        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        async def fake_fetch_positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 3,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 1000,
                "available_eq_usd": 40.0,
                "available_balances": {
                    "USDT": {
                        "available_usd": 40.0,
                        "cash": 40.0,
                    }
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.001,
            },
            "positions": [],
        }
        decision = {"action": "BUY", "confidence": 0.6, "position_size": 1.0}
        executed = await service.handle_llm_decision(decision, context)
        return executed, account_api, trade_api

    executed, account_api, trade_api = asyncio.run(scenario())
    assert executed is True
    assert account_api.calls, "expected margin adjustment despite partial funding"
    assert trade_api.payloads, "order should proceed after top-up"


def test_handle_llm_blocks_when_margin_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTradeApi:
        def place_order(self, **payload: Any) -> dict[str, Any]:  # pragma: no cover - should not run
            return {"code": "0", "data": [{"ordId": "1"}]}

    class BalanceResponder:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self) -> dict[str, Any]:
            self.calls += 1
            return {
                "available_balances": {},
                "available_eq_usd": None,
                "total_eq_usd": 1000.0,
            }

    async def scenario() -> tuple[bool, deque[dict[str, Any]], int]:
        state = DummySnapshotStore()
        trade_api = DummyTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=object(),
            trade_api=trade_api,
            market_api=None,
            public_api=None,
        )
        balance_responder = BalanceResponder()

        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return await balance_responder()

        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        async def fake_fetch_positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)
        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 5.0,
        )

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 3,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 1000,
                "available_eq_usd": None,
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
        executed = await service.handle_llm_decision(
            {"action": "BUY", "confidence": 0.9, "position_size": 1.0},
            context,
        )
        return executed, service._execution_feedback, balance_responder.calls

    executed, feedback, balance_calls = asyncio.run(scenario())
    assert executed is False
    assert balance_calls >= 2
    assert feedback, "expected feedback entry when margin is unknown"
    assert feedback[-1]["message"] == "Available margin unknown; execution paused"


def test_refresh_execution_limits_from_account_populates_snapshot() -> None:
    state = DummySnapshotStore()
    service = MarketService(
        state_service=state,
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
    )
    service._latest_ticker[service.symbol] = {"last": "27000"}
    account_payload = {
        "available_eq_usd": 500.0,
        "total_eq_usd": 1200.0,
        "available_balances": {
            "USDT": {
                "available_usd": 450.0,
                "cash": 430.0,
            }
        },
    }

    service._refresh_execution_limits_from_account(account_payload)

    limits = service._latest_execution_limits.get(service.symbol)
    assert limits is not None
    assert limits["source"] == "balance-snapshot"
    assert limits["available_margin_usd"] == pytest.approx(500.0)
    assert limits["account_equity_usd"] == pytest.approx(1200.0)
    assert limits["quote_currency"] == "USDT"
    assert limits["quote_available_usd"] == pytest.approx(450.0)
    assert limits["quote_cash_usd"] == pytest.approx(430.0)


def test_record_execution_limits_preserves_existing_caps() -> None:
    state = DummySnapshotStore()
    service = MarketService(
        state_service=state,
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
    )
    symbol = service.symbol
    service._record_execution_limits(
        symbol,
        available_margin_usd=300.0,
        account_equity_usd=1000.0,
        quote_currency="USDT",
        quote_available_usd=250.0,
        quote_cash_usd=240.0,
        max_leverage=3.0,
        max_notional_usd=900.0,
    )

    service._record_execution_limits(
        symbol,
        available_margin_usd=400.0,
        account_equity_usd=None,
        quote_currency=None,
        quote_available_usd=None,
        quote_cash_usd=None,
        max_leverage=None,
        max_notional_usd=None,
        source="balance-snapshot",
    )

    limits = service._latest_execution_limits[symbol]
    assert limits["available_margin_usd"] == pytest.approx(400.0)
    assert limits["max_leverage"] == pytest.approx(3.0)
    assert limits["max_notional_usd"] == pytest.approx(900.0)
    assert limits["quote_currency"] == "USDT"


def test_leverage_adjusted_size_scales_up_when_hint_too_small_and_confident() -> None:
    result = MarketService._compute_leverage_adjusted_size(
        size_hint=0.1,
        account_equity=1000.0,
        last_price=70.0,
        min_leverage=1.0,
        max_leverage=5.0,
        confidence=0.75,
        confidence_gate=0.5,
    )
    expected_target = (1000.0 * 3.0) / 70.0
    assert result == pytest.approx(expected_target)


def test_leverage_adjusted_size_respects_gate_when_confidence_low() -> None:
    result = MarketService._compute_leverage_adjusted_size(
        size_hint=0.1,
        account_equity=1000.0,
        last_price=70.0,
        min_leverage=1.0,
        max_leverage=5.0,
        confidence=0.25,
        confidence_gate=0.5,
    )
    assert result == pytest.approx(0.1)


def test_leverage_adjusted_size_scales_down_when_excessive() -> None:
    result = MarketService._compute_leverage_adjusted_size(
        size_hint=50.0,
        account_equity=1000.0,
        last_price=50.0,
        min_leverage=1.0,
        max_leverage=2.0,
        confidence=1.0,
        confidence_gate=0.5,
    )
    assert result == pytest.approx(40.0)
