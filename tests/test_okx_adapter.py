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
        self.algo_orders: list[dict[str, Any]] = []
        self.cancelled_algos: list[list[dict[str, Any]]] = []
        self.pending_queries: list[dict[str, Any]] = []
        self.history_queries: list[dict[str, Any]] = []

    def _request_with_params(self, method: str, path: str, params: dict[str, Any]):
        self.placed_with_request.append({"method": method, "path": path, "params": params})
        return {"data": []}

    def place_order(self, **kwargs: Any):
        self.legacy_orders.append(kwargs)
        return {"data": []}

    def place_algo_order(self, **kwargs: Any):
        self.algo_orders.append(kwargs)
        return {"data": []}

    def cancel_algo_order(self, params):
        self.cancelled_algos.append(params)
        return {"data": []}

    def order_algo_pending(self, **kwargs: Any):
        self.pending_queries.append(kwargs)
        return {"data": []}

    def order_algo_history(self, **kwargs: Any):
        self.history_queries.append(kwargs)
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


def test_trade_adapter_place_algo_order_with_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.place_algo_order(
        instId="BTC-USDT-SWAP",
        tdMode="cross",
        side="sell",
        ordType="conditional",
        closeFraction="1",
        tpTriggerPx="100",
        tpOrdPx="-1",
        subAcct="beta",
    )

    assert api.placed_with_request, "Sub-account algo orders should use raw request"
    params = api.placed_with_request[0]["params"]
    assert params["subAcct"] == "beta"
    assert params["instId"] == "BTC-USDT-SWAP"


def test_trade_adapter_place_algo_order_without_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.place_algo_order(
        instId="BTC-USDT-SWAP",
        tdMode="cross",
        side="sell",
        ordType="conditional",
        sz="1",
        slTriggerPx="95",
        slOrdPx="-1",
    )

    assert api.algo_orders, "Algo orders without sub account should call legacy method"
    assert api.algo_orders[0]["instId"] == "BTC-USDT-SWAP"


def test_trade_adapter_cancel_algo_order_with_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.cancel_algo_order(
        [
            {
                "instId": "BTC-USDT-SWAP",
                "algoId": "123",
                "subAcct": "beta",
            }
        ]
    )

    assert api.placed_with_request, "Cancel with sub account should use raw request"
    params = api.placed_with_request[0]["params"]
    assert params[0]["algoId"] == "123"


def test_trade_adapter_cancel_algo_order_without_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.cancel_algo_order([
        {
            "instId": "BTC-USDT-SWAP",
            "algoId": "123",
        }
    ])

    assert api.cancelled_algos, "Cancel without sub account should call legacy method"
    assert api.cancelled_algos[0][0]["algoId"] == "123"


def test_trade_adapter_list_algo_orders_with_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.list_algo_orders(
        state="live",
        ordType="conditional",
        instId="BTC-USDT-SWAP",
        ordId="abc123",
        subAcct="beta",
    )

    assert api.placed_with_request, "Listing with sub account should use raw request helper"
    payload = api.placed_with_request[-1]
    assert payload["path"].endswith("orders-algo-pending"), "Expected pending endpoint"
    assert payload["params"]["subAcct"] == "beta"
    assert payload["params"]["ordId"] == "abc123"


def test_trade_adapter_list_algo_orders_without_sub_account() -> None:
    api = _FakeTradeAPI()
    adapter = OkxTradeAdapter(api)

    adapter.list_algo_orders(state="history", instId="BTC-USDT-SWAP")

    assert api.history_queries, "History queries without sub account should use legacy method"
    assert api.history_queries[0]["instId"] == "BTC-USDT-SWAP"
    assert api.history_queries[0]["state"] == "triggered"
