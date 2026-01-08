from __future__ import annotations

from typing import Any

from app.services.okx_sdk_adapter import OkxAccountAdapter, OkxTradeAdapter


class _FakeAccountAPI:
    def __init__(self) -> None:
        self.balance_calls: list[dict[str, Any]] = []
        self.position_calls: list[dict[str, Any]] = []
        self.legacy_position_calls: list[dict[str, Any]] = []

    def _request_with_params(self, method: str, path: str, params: dict[str, Any]):
        if path.endswith("account/balance"):
            self.balance_calls.append({"method": method, "path": path, "params": params})
        else:
            self.position_calls.append({"method": method, "path": path, "params": params})
        return {"data": []}

    def get_positions(self, **kwargs: Any):
        self.legacy_position_calls.append(kwargs)
        return {"data": []}

    def get_account_balance(self, **kwargs: Any):
        self.balance_calls.append(kwargs)
        return {"data": []}


class _FakeTradeAPI:
    def __init__(self) -> None:
        self.placed_with_request: list[dict[str, Any]] = []
        self.legacy_orders: list[dict[str, Any]] = []

    def _request_with_params(self, method: str, path: str, params: dict[str, Any]):
        self.placed_with_request.append({"method": method, "path": path, "params": params})
        return {"data": []}

    def place_order(self, **kwargs: Any):
        self.legacy_orders.append(kwargs)
        return {"data": []}


def test_account_adapter_uses_sub_account_request_path() -> None:
    api = _FakeAccountAPI()
    adapter = OkxAccountAdapter(api)

    adapter.get_positions(instType="SWAP", subAcct="alpha")
    adapter.get_account_balance(ccy="USDT", subAcct="alpha")

    assert api.position_calls, "Expected sub-account positions to use raw request method"
    assert api.balance_calls[0]["params"]["subAcct"] == "alpha"


def test_account_adapter_falls_back_without_sub_account() -> None:
    api = _FakeAccountAPI()
    adapter = OkxAccountAdapter(api)

    adapter.get_positions(instType="SWAP")
    adapter.get_account_balance()

    assert api.legacy_position_calls, "Legacy get_positions should handle non sub-account calls"
    assert any("params" not in call for call in api.balance_calls)


def test_trade_adapter_injects_sub_account_payload() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.place_order(
        instId="BTC-USDT-SWAP",
        tdMode="cross",
        side="buy",
        ordType="market",
        sz="1",
        clOrdId="abc",
        subAcct="beta",
    )

    assert api.placed_with_request, "Sub-account orders should use raw request helper"
    params = api.placed_with_request[0]["params"]
    assert params["subAcct"] == "beta"
    assert params["instId"] == "BTC-USDT-SWAP"


def test_trade_adapter_delegates_without_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.place_order(
        instId="BTC-USDT-SWAP",
        tdMode="cross",
        side="buy",
        ordType="market",
        sz="1",
        clOrdId="abc",
    )

    assert api.legacy_orders, "Non sub-account orders should call legacy place_order"
    assert api.legacy_orders[0]["instId"] == "BTC-USDT-SWAP"
