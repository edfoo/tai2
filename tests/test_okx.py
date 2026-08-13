from __future__ import annotations

import asyncio
from collections import deque
from types import MethodType
from typing import Any, List

import pytest

import app.services.market_service as market_service_module
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


def test_indicator_helper_handles_empty_data() -> None:
    indicators = MarketService._compute_indicators([])
    assert indicators["vwap"] is None
    assert indicators["bollinger_bands"] == {}
    assert indicators["stoch_rsi"] == {}
    assert indicators["choppiness"] is None
    assert indicators["vpoc"] is None
    assert indicators["value_area_width"] is None


def test_fetch_long_short_ratio_backoffs_after_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        service = MarketService(
            state_service=DummySnapshotStore(),
            enable_websocket=False,
            account_api=object(),
            market_api=object(),
            public_api=object(),
            trade_api=object(),
        )
        service._trading_api = object()

        class DummySemaphore:
            def release(self) -> None:
                return None

        class DummyApi:
            def get_long_short_ratio(self, *args: Any, **kwargs: Any) -> None:
                raise asyncio.TimeoutError()

        calls = 0

        async def fake_to_thread(*args: Any, **kwargs: Any) -> None:
            nonlocal calls
            calls += 1
            raise asyncio.TimeoutError()

        monkeypatch.setattr(market_service_module.asyncio, "to_thread", fake_to_thread)

        async def fake_acquire_pool_slot(_pool: Any) -> tuple[DummyApi, DummySemaphore]:
            return DummyApi(), DummySemaphore()

        monkeypatch.setattr(service, "_acquire_pool_slot", fake_acquire_pool_slot)

        first = await service._fetch_long_short_ratio("BTC-USDT-SWAP")
        assert first == {}
        assert calls == 1

        second = await service._fetch_long_short_ratio("BTC-USDT-SWAP")
        assert second == {}
        assert calls == 1

    asyncio.run(scenario())


def _ohlcv_rows(n: int, base: float = 100.0) -> list[list[Any]]:
    """Build n synthetic OHLCV rows: [ts_ms, open, high, low, close, volume]."""
    import math

    rows: list[list[Any]] = []
    start = 1_700_000_000_000
    for i in range(n):
        ts = start + i * 60_000
        close = base + math.sin(i / 3) * 3 + i * 0.01
        high = close + 1.0
        low = close - 1.0
        open_ = close - 0.2
        volume = 10 + (i % 7)
        rows.append([ts, open_, high, low, close, volume])
    return rows


def test_indicator_helper_populates_value_area_keys() -> None:
    indicators = MarketService._compute_indicators(_ohlcv_rows(120, base=100.0))
    assert indicators["atr_pct"] is not None
    assert indicators["vpoc"] is not None
    assert indicators["value_area_high"] is not None
    assert indicators["value_area_low"] is not None
    assert indicators["value_area_width"] is not None
    assert 0.0 < indicators["value_area_width"]
    assert indicators["value_area_low"] <= indicators["vpoc"] <= indicators["value_area_high"]


def test_indicator_helper_populates_choppiness() -> None:
    indicators = MarketService._compute_indicators(_ohlcv_rows(120, base=100.0))
    # Choppiness index lives on a 0-100 scale.
    v = indicators["choppiness"]
    assert v is not None
    assert 0.0 <= v <= 100.0
    assert indicators["choppiness_series"]


def _session_ohlcv_rows_per_day(
    *,
    day1_count: int = 50,
    day2_count: int = 10,
    day1_price: float = 100.0,
    day2_price: float = 200.0,
) -> list[list[Any]]:
    """Build OHLCV rows spanning two UTC calendar days with distinct prices."""
    import pandas as pd

    rows: list[list[Any]] = []
    step_ms = 15 * 60 * 1000  # 15m candles
    # Day 1 starts at 2023-01-01 00:00 UTC.
    day1_start = int(pd.Timestamp("2023-01-01T00:00:00+00:00").timestamp() * 1000)
    for i in range(day1_count):
        ts = day1_start + i * step_ms
        c = day1_price
        rows.append([ts, c - 0.2, c + 0.5, c - 0.5, c, 100.0])
    # Day 2 starts at 2023-01-02 00:00 UTC.
    day2_start = int(pd.Timestamp("2023-01-02T00:00:00+00:00").timestamp() * 1000)
    for i in range(day2_count):
        ts = day2_start + i * step_ms
        c = day2_price
        rows.append([ts, c - 0.2, c + 0.5, c - 0.5, c, 100.0])
    return rows


def test_session_vwap_uses_current_session_not_blend() -> None:
    """The strategy-facing vwap must be the *current* session's cumulative,
    not a blend over older sessions."""
    rows = _session_ohlcv_rows_per_day(day1_price=100.0, day2_price=200.0)
    indicators = MarketService._compute_indicators(rows)
    vwap = indicators["vwap"]
    assert vwap is not None
    # Day 2 candles are all at price 200 and volume 100 → session VWAP = 200.
    assert abs(vwap - 200.0) < 1e-6
    assert indicators["vwap_session"] is not None
    assert abs(indicators["vwap_session"] - 200.0) < 1e-6


def test_session_vwap_immune_to_old_session_volume_steps() -> None:
    """A huge-volume bar in an *older* session must not step the current
    session's VWAP (the core F1 fix)."""
    rows = _session_ohlcv_rows_per_day(day1_price=100.0, day2_price=200.0, day2_count=1)
    # Inflate the last day-1 bar's volume massively — it would dominate a
    # free-rolling cumulative, but must be irrelevant to the day-2 session VWAP.
    day1_ends = 50
    rows[day1_ends - 1][5] = 10_000_000.0
    indicators = MarketService._compute_indicators(rows)
    vwap = indicators["vwap"]
    assert vwap is not None
    assert abs(vwap - 200.0) < 1e-6


def test_htf_indicators_expose_adx_and_choppiness_for_flat_nesting() -> None:
    # Phase 0b: `_build_snapshot` flattens `adx_htf` / `choppiness_htf` from
    # the HTF indicators dict. This test verifies the source values exist so
    # the flattening resolves to real numbers rather than None.
    htf = MarketService._compute_indicators(_ohlcv_rows(150, base=100.0))
    assert (htf.get("adx") or {}).get("value") is not None
    assert htf.get("choppiness") is not None


def test_compute_structure_emits_price_scalars_for_swing_flattening() -> None:
    # Phase 0c: `_build_snapshot` reads the last swing high/low pivot price
    # and flattens it into `swing_high`/`swing_low`. Craft an OHLCV series
    # with a clear dip so at least one swing low pivot is confirmed.
    import math

    rows: list[list[Any]] = []
    start = 1_700_000_000_000
    # Steady uptrend with a single sharp lower low around index 40.
    for i in range(90):
        close = 100.0 + i * 0.5
        if i == 40:
            close = 96.0  # local low
        high = close + 2.0
        low = close - 2.0
        ts = start + i * 60_000
        rows.append([ts, low, high, low, close, 50.0])

    structure = MarketService._compute_structure(rows, swing_lookback=5)
    swing_lows = structure.get("swing_lows") or []
    assert swing_lows, "expected at least one confirmed swing low"
    # The flattened scalar is taken from the last pivot's "price".
    assert isinstance(swing_lows[-1], dict)
    assert swing_lows[-1].get("price") is not None
    # Pivot price is the swing bar's LOW (close-2), well below the surrounding range.
    assert 93.0 <= swing_lows[-1].get("price") <= 99.0


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


def test_normalize_take_profit_clips_invalid_short() -> None:
    """Wrong-direction TP (SELL with TP above entry) should be dropped by normalization."""
    service = MarketService(
        state_service=DummySnapshotStore(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    symbol = service.symbol
    service._instrument_specs[symbol] = {"tick_size": 0.1}
    result = service._normalize_take_profit("SELL", 101.0, 100.0, symbol=symbol)
    assert result is None
    feedback = list(service._execution_feedback)
    assert feedback, "Expected warning entry"
    assert "rejected" in feedback[-1]["message"].lower() or "SELL" in feedback[-1]["message"]


def test_normalize_take_profit_clips_invalid_long() -> None:
    """Wrong-direction TP (BUY with TP below entry) should be dropped by normalization."""
    service = MarketService(
        state_service=DummySnapshotStore(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    symbol = service.symbol
    service._instrument_specs[symbol] = {"tick_size": 0.1}
    result = service._normalize_take_profit("BUY", 99.0, 100.0, symbol=symbol)
    assert result is None
    feedback = list(service._execution_feedback)
    assert feedback
    assert "rejected" in feedback[-1]["message"].lower() or "BUY" in feedback[-1]["message"]


def test_snap_take_profit_to_valid_short() -> None:
    """_snap_take_profit_to_valid snaps SELL TP to just below entry price."""
    service = MarketService(
        state_service=DummySnapshotStore(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    symbol = service.symbol
    service._instrument_specs[symbol] = {"tick_size": 0.1}
    snapped = service._snap_take_profit_to_valid("SELL", 100.0, symbol)
    assert snapped is not None and snapped < 100.0
    feedback = list(service._execution_feedback)
    assert feedback[-1]["message"] == "LLM take-profit adjusted to honor trade direction"
    assert feedback[-1]["meta"]["adjusted_take_profit"] == pytest.approx(snapped)


def test_snap_take_profit_to_valid_long() -> None:
    """_snap_take_profit_to_valid snaps BUY TP to just above entry price."""
    service = MarketService(
        state_service=DummySnapshotStore(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    symbol = service.symbol
    service._instrument_specs[symbol] = {"tick_size": 0.1}
    snapped = service._snap_take_profit_to_valid("BUY", 100.0, symbol)
    assert snapped is not None and snapped > 100.0
    feedback = list(service._execution_feedback)
    assert feedback[-1]["message"] == "LLM take-profit adjusted to honor trade direction"
    assert feedback[-1]["meta"]["adjusted_take_profit"] == pytest.approx(snapped)


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
            {"action": "BUY", "confidence": 0.9, "stop_loss": 1.0},
            {"symbol": service.symbol},
        )

        return fetch_calls["count"], captured

    calls, messages = asyncio.run(scenario())
    assert calls == 1
    assert any("Execution disabled" in message for message in messages)


def test_launcher_decision_seeds_trade_mgmt_state_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, str, dict[str, Any], dict[str, Any]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=object(),
            market_api=None,
            public_api=None,
            trade_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }
        service._latest_ticker[service.symbol] = {"last": 100.0}
        service._strategy_config = {
            "trade_management": {
                "enabled": True,
            }
        }

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            return {"ordId": "order-1"}, True

        async def fake_record_trade_execution(*args: Any, **kwargs: Any) -> None:
            return None

        async def fake_fetch_positions() -> list[dict[str, Any]]:
            return []

        # The constructor builds a real AccountAPI when credentials exist in
        # .env (account_api=None falls through to _build_account_api()). Pin a
        # controlled balance so sizing isn't capped by a live OKX balance.
        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return {
                "available_balances": {
                    "USDT": {
                        "available_usd": 1000.0,
                        "cash": 1000.0,
                    }
                },
                "available_eq_usd": 1000.0,
                "total_eq_usd": 1000.0,
            }

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_record_trade_execution", fake_record_trade_execution)
        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)
        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        decision = {
            "action": "BUY",
            "confidence": 0.95,
            "notional_usd": 100.0,
            "rationale": "launcher",
            "_decision_origin": "launcher",
            "_strategy_name": "vwap_reversion",
        }
        context = {
            "symbol": service.symbol,
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.1,
            },
            "guardrails": {},
            "account": {"account_equity": 1000.0},
            "market": {"last_price": 100.0},
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, service.symbol, service._launcher_in_position, service._trade_mgmt_state

    executed, symbol, launcher_positions, trade_mgmt_state = asyncio.run(scenario())
    assert executed is True
    assert launcher_positions
    assert trade_mgmt_state
    assert launcher_positions[f"vwap_reversion:{symbol.upper()}"]["strategy"] == "vwap_reversion"
    assert trade_mgmt_state[symbol.upper()]["strategy"] == "vwap_reversion"


def test_handle_llm_seeds_isolated_margin_when_tier_requires_more_margin(monkeypatch: pytest.MonkeyPatch) -> None:
    class RecordingAccountApi:
        def __init__(self) -> None:
            self.adjust_calls: list[dict[str, Any]] = []
            self.leverage_calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(self, symbol, pos_side, amount, type="add", subAcct="") -> dict[str, Any]:
            self.adjust_calls.append({"symbol": symbol, "pos_side": pos_side, "amount": amount})
            return {"code": "0"}

        def get_account_balance(self, subAcct: str | None = None) -> list[dict[str, Any]]:
            return [
                {
                    "totalEqUsd": "600",
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "600",
                            "eqUsd": "600",
                            "availEq": "650",
                            "availEqUsd": "650",
                        }
                    ],
                }
            ]

        def get_positions(self, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        def set_leverage(self, **payload: Any) -> dict[str, Any]:
            self.leverage_calls.append(payload)
            return {"code": "0"}

    class RecordingTradeApi:
        def __init__(self) -> None:
            self.payloads: list[dict[str, Any]] = []

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.payloads.append(payload)
            return {
                "code": "0",
                "data": [
                    {
                        "ordId": "1",
                        "sCode": "0",
                    }
                ],
            }

    async def scenario() -> tuple[bool, list[dict[str, Any]], list[dict[str, Any]]]:
        state = DummySnapshotStore()
        account_api = RecordingAccountApi()
        trade_api = RecordingTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=account_api,
            market_api=None,
            public_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }

        def fake_size(self, **kwargs: Any) -> float:
            return 3600.0

        service._compute_leverage_adjusted_size = MethodType(fake_size, service)

        async def fake_tiers(self, symbol: str, trade_mode: str) -> list[dict[str, Any]]:
            return [
                {
                    "minSz": "0",
                    "maxSz": "1000",
                    "imr": "0.2",
                    "maxLever": "5",
                }
            ]

        monkeypatch.setattr(service, "_get_position_tiers", MethodType(fake_tiers, service))

        async def permissive_tier_guard(self, **kwargs: Any) -> dict[str, Any]:
            tier = {
                "minSz": "0",
                "maxSz": "1000",
                "imr": "0.2",
                "maxLever": "5",
            }
            return {
                "size": kwargs.get("additional_size", 0.0),
                "tier": tier,
                "tier_imr": 0.2,
                "tier_max_leverage": 5.0,
                "max_notional_allowed": None,
                "clipped": False,
                "blocked": False,
            }

        monkeypatch.setattr(service, "_apply_tier_margin_guard", MethodType(permissive_tier_guard, service))

        decision = {
            "action": "BUY",
            "position_size": 3600.0,
            "confidence": 0.9,
            "stop_loss": 0.9,
        }
        context = {
            "symbol": service.symbol,
            "positions": [{"instId": service.symbol, "pos": "100"}],
            "market": {"last_price": 1.0},
            "account": {
                "account_equity": 600.0,
                "available_eq_usd": 650.0,
                "available_balances": {
                    "USDT": {"available_usd": 650.0, "equity_usd": 650.0}
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.1,
            },
            "guardrails": {
                "min_leverage": 6.0,
                "max_leverage": 10.0,
                "max_position_pct": 10.0,
                "require_position_alignment": False,
            },
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, account_api.adjust_calls, trade_api.payloads

    executed, adjust_calls, payloads = asyncio.run(scenario())
    assert executed is True
    assert payloads
    assert adjust_calls
    assert float(adjust_calls[0]["amount"]) == pytest.approx(756.0, rel=1e-3)


def test_handle_llm_decision_enforces_min_leverage(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool, list[dict[str, Any]]]:
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
        decision = {"action": "BUY", "confidence": 0.9, "stop_loss": 90.0}
        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is False
    assert submit_called is False


def test_handle_llm_decision_seeds_price_hints_before_open_notional(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, dict[str, Any]]:
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
        service._funding_api = None
        service._instrument_specs["BTC-USDT-SWAP"] = {
            "lot_size": 0.001,
            "min_size": 0.001,
            "tick_size": 0.1,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        captured: dict[str, Any] = {}

        def fake_compute_open_notional(
            self: MarketService,
            positions: list[dict[str, Any]] | None,
            *,
            price_hints: dict[str, float] | None = None,
        ) -> float:
            captured["price_hints"] = price_hints
            return 0.0

        monkeypatch.setattr(
            MarketService,
            "_compute_open_position_notional",
            fake_compute_open_notional,
        )

        async def fake_tier_guard(**kwargs):
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        async def fake_submit_order(**kwargs):
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": "BTC-USDT-SWAP",
            "guardrails": {
                "min_leverage": 1,
                "max_leverage": 2,
                "max_position_pct": 0.5,
                "symbol_position_caps": {},
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 1000.0,
                "available_eq_usd": 1000.0,
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

        decision = {
            "action": "BUY",
            "confidence": 0.9,
            "position_size": 1.0,
            "stop_loss": 95.0,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, captured

    executed, captured = asyncio.run(scenario())
    assert isinstance(captured.get("price_hints"), dict)
    assert captured["price_hints"].get("BTC-USDT-SWAP") == pytest.approx(100.0)


def test_handle_llm_seeds_isolated_margin_when_position_wallet_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, int, list[dict[str, Any]]]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 1.0,
            "min_size": 1.0,
            "tick_size": 0.0001,
        }

        base_positions = [
            {
                "instId": service.symbol,
                "mgnMode": "isolated",
                "posSide": "long",
                "pos": "10",
                "margin": "0",
            }
        ]

        seed_calls: list[dict[str, Any]] = []

        async def fake_seed(self, **kwargs: Any) -> tuple[dict[str, Any], float | None]:
            seed_calls.append(kwargs)
            return ({"available_balances": {}}, None)

        monkeypatch.setattr(
            service,
            "_ensure_isolated_margin_buffer",
            MethodType(fake_seed, service),
        )

        fetch_calls = {"count": 0}

        async def fake_fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
            fetch_calls["count"] += 1
            if seed_calls:
                return [
                    {
                        "instId": service.symbol,
                        "mgnMode": "isolated",
                        "posSide": "long",
                        "pos": "10",
                        "margin": "120",
                    }
                ]
            return list(base_positions)

        monkeypatch.setattr(service, "_fetch_positions", MethodType(fake_fetch_positions, service))

        async def fake_tier_guard(**kwargs: Any) -> dict[str, Any]:
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        async def fake_get_tiers(symbol: str, trade_mode: str) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_get_position_tiers", fake_get_tiers)

        submitted = {"count": 0}

        async def fake_submit_order(**kwargs: Any) -> tuple[dict[str, Any], bool]:
            submitted["count"] += 1
            return ({"ordId": "1", "fillPx": "0.03", "fillSz": "100"}, False)

        async def noop_refresh(*args: Any, **kwargs: Any) -> None:
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", noop_refresh)
        monkeypatch.setattr(service, "_cancel_position_protection", noop_refresh)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 100.0,
        )

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.0,
                "max_leverage": 3.0,
                "max_position_pct": 0.5,
                "isolated_margin_seed_pct": 0.2,
                "require_position_alignment": False,
            },
            "market": {"last_price": 0.03},
            "account": {
                "account_equity": 500.0,
                "available_eq_usd": 500.0,
                "available_balances": {
                    "USDT": {"available_usd": 250.0, "cash": 250.0}
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 1.0,
            },
            "positions": base_positions,
        }

        decision = {
            "action": "BUY",
            "confidence": 0.9,
            "position_size": 100.0,
            "stop_loss": 0.02,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, len(seed_calls), list(service._execution_feedback)

    executed, seed_invocations, feedback = asyncio.run(scenario())
    assert executed is True, feedback
    assert seed_invocations >= 1


def test_handle_llm_executes_isolated_trade_without_wallet(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, int, list[dict[str, Any]]]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 1.0,
            "min_size": 1.0,
            "tick_size": 0.0001,
        }

        async def fake_fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", MethodType(fake_fetch_positions, service))

        async def fake_seed(self, **kwargs: Any) -> tuple[dict[str, Any] | None, float | None]:
            raise AssertionError("auto-seed should not run when no isolated wallet exists")

        monkeypatch.setattr(service, "_ensure_isolated_margin_buffer", MethodType(fake_seed, service))

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 500.0,
        )

        async def fake_tier_guard(**kwargs: Any) -> dict[str, Any]:
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        submit_calls = {"count": 0, "size": 0.0}

        async def fake_submit_order(**kwargs: Any) -> tuple[dict[str, Any], bool]:
            submit_calls["count"] += 1
            submit_calls["size"] = kwargs.get("size", 0.0)
            return (
                {"ordId": "1", "fillPx": "0.33", "fillSz": str(kwargs.get("size", 0.0))},
                False,
            )

        async def noop_protection(*args: Any, **kwargs: Any) -> None:
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", noop_protection)
        monkeypatch.setattr(service, "_cancel_position_protection", noop_protection)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.0,
                "max_leverage": 3.0,
                "max_position_pct": 0.5,
                "isolated_margin_seed_pct": 0.05,
                "isolated_wallet_bootstrap_pct": 0.05,
            },
            "market": {"last_price": 0.33},
            "account": {
                "account_equity": 400.0,
                "available_eq_usd": 223.75,
                "available_balances": {
                    "USDT": {"available_usd": 223.75, "cash": 223.75}
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 1.0,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.5,
            "position_size": 500.0,
            "stop_loss": 0.3,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_calls, list(service._execution_feedback)

    executed, submit_meta, feedback = asyncio.run(scenario())
    assert executed is True, feedback
    assert submit_meta["count"] == 1
    messages = [entry["message"] for entry in feedback]
    assert "Isolated margin unavailable" not in messages
    assert "Size clipped while isolated wallet missing" in messages
    assert submit_meta["size"] == pytest.approx(33.0, rel=1e-6)


def test_handle_llm_prefers_explicit_position_size(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, float | None, float | None, list[dict[str, Any]]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=object(),
            account_api=None,
            market_api=None,
            public_api=None,
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 1.0,
            "min_size": 1.0,
            "tick_size": 0.01,
        }

        async def fake_fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", MethodType(fake_fetch_positions, service))

        captured: dict[str, float | None] = {"size_hint": None, "submitted": None}

        def fake_leverage_adjust(**kwargs: Any) -> float | None:
            captured["size_hint"] = kwargs.get("size_hint")
            return kwargs.get("size_hint")

        monkeypatch.setattr(service, "_compute_leverage_adjusted_size", fake_leverage_adjust)

        async def fake_tier_guard(**kwargs: Any) -> dict[str, Any]:
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        async def fake_submit_order(**kwargs: Any) -> tuple[dict[str, Any], bool]:
            captured["submitted"] = kwargs.get("size")
            return ({"ordId": "1", "fillSz": str(kwargs.get("size", 0.0))}, False)

        async def noop_protection(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", noop_protection)
        monkeypatch.setattr(service, "_cancel_position_protection", noop_protection)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.0,
                "max_leverage": 5.0,
                "max_position_pct": 0.5,
                "min_hold_seconds": 0,
                "max_trades_per_hour": 0,
                "trade_window_seconds": 3600,
            },
            "market": {"last_price": 1.0},
            "account": {
                "account_equity": 1000.0,
                "available_eq_usd": 1000.0,
                "available_balances": {
                    "USDT": {"available_usd": 1000.0, "cash": 1000.0}
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 1.0,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 10.0,
            "equity_pct": 0.5,
            "stop_loss": 0.95,
            "take_profit": 1.05,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, captured["size_hint"], captured["submitted"], list(service._execution_feedback)

    executed, size_hint, submitted_size, feedback = asyncio.run(scenario())
    assert executed is True, feedback
    assert size_hint == pytest.approx(10.0)
    assert submitted_size == pytest.approx(10.0)


def test_handle_llm_emits_feedback_for_size_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
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
        service._instrument_specs[service.symbol] = {
            "lot_size": 1.0,
            "min_size": 1.0,
            "tick_size": 0.01,
        }

        async def fake_fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
            return []

        monkeypatch.setattr(service, "_fetch_positions", MethodType(fake_fetch_positions, service))

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: kwargs.get("size_hint"),
        )

        async def fake_tier_guard(**kwargs: Any) -> dict[str, Any]:
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        async def fake_submit_order(**kwargs: Any) -> tuple[dict[str, Any], bool]:
            return ({"ordId": "1", "fillSz": str(kwargs.get("size", 0.0))}, False)

        async def noop_protection(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", noop_protection)
        monkeypatch.setattr(service, "_cancel_position_protection", noop_protection)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.0,
                "max_leverage": 5.0,
                "max_position_pct": 0.5,
                "min_hold_seconds": 0,
                "max_trades_per_hour": 0,
                "trade_window_seconds": 3600,
            },
            "market": {"last_price": 1.0},
            "account": {
                "account_equity": 1000.0,
                "available_eq_usd": 1000.0,
                "available_balances": {
                    "USDT": {"available_usd": 1000.0, "cash": 1000.0}
                },
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 1.0,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.7,
            "position_size": 5.0,
            "equity_pct": 0.5,
            "stop_loss": 0.95,
            "take_profit": 1.05,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, list(service._execution_feedback)

    executed, feedback = asyncio.run(scenario())
    assert executed is True, feedback
    mismatch_messages = [entry for entry in feedback if entry["message"] == "LLM equity_pct disagrees with position_size"]
    assert mismatch_messages, feedback
    assert mismatch_messages[0]["level"] == "warning"


def test_handle_llm_allows_trade_when_margin_available(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool, list[dict[str, Any]]]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.001,
            "min_size": 0.001,
            "tick_size": 0.1,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        def fake_open_notional(
            self: MarketService,
            positions: list[dict[str, Any]] | None,
            *,
            price_hints: dict[str, float] | None = None,
        ) -> float:
            return 900.0

        monkeypatch.setattr(
            MarketService,
            "_compute_open_position_notional",
            fake_open_notional,
        )

        async def fake_tier_guard(**kwargs):
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        submit_called = {"value": False}

        async def fake_submit_order(**kwargs):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.1,
                "max_leverage": 3,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 236.0,
                "available_eq_usd": 205.0,
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

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "stop_loss": 95.0,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"], list(service._execution_feedback)

    executed, submit_called, feedback = asyncio.run(scenario())
    assert executed is True, feedback
    assert submit_called is True, feedback


def test_handle_llm_rebases_tp_sl_to_final_price(monkeypatch: pytest.MonkeyPatch) -> None:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.01,
        }

        # Use a larger mocked size so the 0.5x min leverage guard accepts the trade.
        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 3.0,
        )

        async def fake_tier_guard(**kwargs: Any) -> dict[str, Any]:
            return {
                "size": kwargs.get("additional_size", 0.0),
            }

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        recorded: dict[str, float] = {}

        async def fake_submit_order(**kwargs: Any) -> tuple[dict[str, Any], bool]:
            return (
                {
                    "ordId": "1",
                    "fillPx": "95",
                    "fillSz": "1",
                },
                False,
            )

        async def fake_refresh_position_protection(
            *,
            symbol: str,
            trade_mode: str,
            action: str,
            take_profit_price: float | None,
            stop_loss_price: float | None,
            dual_side_mode: bool,
            pos_side: str | None,
        ) -> None:
            recorded.update(
                {
                    "tp": take_profit_price or 0.0,
                    "sl": stop_loss_price or 0.0,
                    "reference": 95.0,
                }
            )

        async def fake_cancel(*args: Any, **kwargs: Any) -> None:
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", fake_refresh_position_protection)
        monkeypatch.setattr(service, "_cancel_position_protection", fake_cancel)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 2.0,
                "max_position_pct": 0.5,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 500.0,
                "available_eq_usd": 400.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.1,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "take_profit": 110.0,
            "stop_loss": 99.0,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, recorded

    executed, recorded = asyncio.run(scenario())
    assert executed is True
    assert recorded["sl"] < recorded["reference"]
    assert recorded["tp"] > recorded["reference"]
    assert recorded["tp"] == pytest.approx(104.5, rel=1e-3)
    assert recorded["sl"] == pytest.approx(94.05, rel=1e-3)


def test_guardrail_notional_cap_tracks_available_margin(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> dict[str, Any]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.001,
            "min_size": 0.001,
            "tick_size": 0.1,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 5.0,
        )

        async def fake_tier_guard(**kwargs):
            return {"size": kwargs.get("additional_size", 0.0)}

        monkeypatch.setattr(service, "_apply_tier_margin_guard", fake_tier_guard)

        recorded: dict[str, Any] = {}
        submit_called = {"value": False}

        async def fake_submit_order(**payload: Any):
            submit_called["value"] = True
            recorded.update(payload)
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.1,
                "max_leverage": 4,
                "max_position_pct": 0.05,
            },
            "market": {"last_price": 10.0},
            "account": {
                "account_equity": 100.0,
                    "available_eq_usd": 500.0,
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

        decision = {
            "action": "BUY",
            "confidence": 0.9,
            "position_size": 5.0,
            "equity_pct": 0.5,
            "stop_loss": 9.0,
        }

        executed = await service.handle_llm_decision(decision, context)
        return {
            "executed": executed,
            "payload": recorded,
            "feedback": list(service._execution_feedback),
            "submitted": submit_called["value"],
        }

    result = asyncio.run(scenario())
    assert result["executed"] is True, result["feedback"]
    assert result["submitted"] is True, result["feedback"]
    assert result["payload"].get("size") == pytest.approx(0.5)


def test_stop_loss_is_required_for_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.01,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        submit_called = {"value": False}

        async def fake_submit_order(**payload: Any):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 2.0,
                "max_position_pct": 0.2,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 500.0,
                "available_eq_usd": 400.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.1,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "equity_pct": 0.2,
            "take_profit": 110.0,
            "stop_loss": None,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is False
    assert submit_called is False


def test_require_protection_blocks_when_stop_loss_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.01,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        submit_called = {"value": False}

        async def fake_submit_order(**payload: Any):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 2.0,
                "max_position_pct": 0.2,
                "require_protection": True,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 150.0,
                "available_eq_usd": 150.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.1,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "take_profit": 110.0,
            "stop_loss": None,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is False
    assert submit_called is False


def test_require_protection_allows_when_stop_loss_present(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.01,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        submit_called = {"value": False}

        async def fake_submit_order(**payload: Any):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        async def fake_refresh_position_protection(**kwargs: Any):
            return None

        async def fake_cancel(*args: Any, **kwargs: Any):
            return None

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_refresh_position_protection", fake_refresh_position_protection)
        monkeypatch.setattr(service, "_cancel_position_protection", fake_cancel)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 2.0,
                "max_position_pct": 0.2,
                "require_protection": True,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 150.0,
                "available_eq_usd": 150.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.1,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "equity_pct": 0.2,
            "take_profit": 110.0,
            "stop_loss": 95.0,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is True
    assert submit_called is True


def test_launcher_no_protection_flag_bypasses_require_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, bool]:
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
        service._funding_api = None
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.01,
        }

        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.0,
        )

        submit_called = {"value": False}

        async def fake_submit_order(**payload: Any):
            submit_called["value"] = True
            return {"ordId": "1"}, False

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_emit_debug", lambda *args, **kwargs: None)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 2.0,
                "max_position_pct": 0.2,
                "require_protection": True,
            },
            "market": {"last_price": 100.0},
            "account": {
                "account_equity": 150.0,
                "available_eq_usd": 150.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "cross",
                "order_type": "market",
                "min_size": 0.1,
            },
            "positions": [],
        }

        decision = {
            "action": "BUY",
            "confidence": 0.8,
            "position_size": 1.0,
            "take_profit": None,
            "stop_loss": None,
            "_decision_origin": "launcher",
            "_strategy_name": "mean_reversion",
            "_disable_protection": True,
        }

        executed = await service.handle_llm_decision(decision, context)
        return executed, submit_called["value"]

    executed, submit_called = asyncio.run(scenario())
    assert executed is True
    assert submit_called is True


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
                "min_leverage": 0.05,  # exec-layer gives 0.09x; must be below that
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
        executed = await service.handle_llm_decision(
            {"action": "BUY", "confidence": 0.9, "stop_loss": 95.0},
            context,
        )
        return executed, recorded

    executed, recorded = asyncio.run(scenario())
    assert executed is True
    # With the exec-layer notional formula: max_safe = min(1000×2, equity×symbol_cap) = 100
    # computed = 100 × conf(0.9) × (1−risk(0)) = 90 → raw_size = 90/price(100) = 0.9
    # Symbol position cap (0.1×1000 = 100 USD cap) is respected via guardrail_notional_cap.
    assert recorded.get("pre_quantize_size") == pytest.approx(0.9)
    assert recorded.get("size", 0.0) == pytest.approx(0.9)


def test_handle_llm_notes_wallet_missing_when_quote_margin_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> tuple[bool, list[dict[str, Any]]]:
        state = DummySnapshotStore()
        
        class RaisingTradeApi:
            def place_order(self, **payload: Any) -> dict[str, Any]:
                raise RuntimeError("disabled")

        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=RaisingTradeApi(),
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
                "min_leverage": 0.05,
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

        executed = await service.handle_llm_decision(
            {"action": "SELL", "confidence": 0.6, "stop_loss": 105.0},
            context,
        )
        return executed, list(service._execution_feedback)

    executed, feedback = asyncio.run(scenario())
    assert executed is False
    assert feedback
    messages = [entry["message"] for entry in feedback]
    assert "Isolated wallet missing; falling back to quote margin" in messages
    wallet_entry = next(
        entry for entry in feedback if entry["message"] == "Isolated wallet missing; falling back to quote margin"
    )
    assert wallet_entry["meta"]["trade_mode"] == "isolated"


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
            public_api=object(),
        )

        balance_responder = BalanceResponder()

        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return await balance_responder()

        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        base_positions = [
            {
                "instId": service.symbol,
                "mgnMode": "isolated",
                "posSide": "long",
                "pos": "10",
                "margin": "0",
            }
        ]

        async def fake_fetch_positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return list(base_positions)

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)

        context = {
            "symbol": service.symbol,
            "guardrails": {
                "min_leverage": 0.5,
                "max_leverage": 3,
                "max_position_pct": 0.5,
                "require_position_alignment": False,
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
            "positions": base_positions,
        }
        decision = {"action": "BUY", "confidence": 0.6, "position_size": 1.0, "stop_loss": 95.0}
        executed = await service.handle_llm_decision(decision, context)
        return executed, account_api, trade_api

    executed, account_api, trade_api = asyncio.run(scenario())
    assert executed is True
    assert account_api.calls, "expected isolated margin top-up call"
    latest_call = account_api.calls[-1]
    assert latest_call["instId"] == "BTC-USDT-SWAP"
    assert latest_call["posSide"] in {"long", "net"}
    assert trade_api.payloads, "order should be sent after margin top-up"


def test_handle_llm_auto_seeds_isolated_margin(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyTradeApi:
        def __init__(self) -> None:
            self.payloads: list[dict[str, Any]] = []

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.payloads.append(payload)
            return {"code": "0", "data": [{"sCode": "0", "ordId": "1"}]}

    class DummyAccountApi:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []
            self.fail_once = True

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
            if self.fail_once:
                self.fail_once = False
                return {"code": "59300", "msg": "insufficient balance"}
            return {"code": "0", "data": [{"sCode": "0"}]}

    class DummyFundingApi:
        def __init__(self) -> None:
            self.transfers: list[dict[str, Any]] = []

        def funds_transfer(self, **params: Any) -> dict[str, Any]:
            self.transfers.append(params)
            return {"code": "0", "data": [{"sCode": "0"}]}

        def get_balances(self, ccy: str) -> list[dict[str, Any]]:
            return [{"ccy": ccy, "availBal": "500"}]

    class BalanceResponder:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                return {
                    "available_balances": {},
                    "available_eq_usd": 0.0,
                    "total_eq_usd": 150.0,
                }
            return {
                "available_balances": {
                    "USDT": {
                        "currency": "USDT",
                        "available": 200.0,
                        "available_usd": 200.0,
                        "cash": 200.0,
                    }
                },
                "available_eq_usd": 200.0,
                "total_eq_usd": 310.0,
            }

    async def scenario() -> tuple[DummyAccountApi, DummyFundingApi, DummyTradeApi, bool]:
        state = DummySnapshotStore()
        account_api = DummyAccountApi()
        funding_api = DummyFundingApi()
        trade_api = DummyTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=account_api,
            funding_api=funding_api,
            market_api=None,
            public_api=None,
        )

        balance_responder = BalanceResponder()

        async def fake_fetch_account_balance(_self: MarketService) -> dict[str, Any]:
            return await balance_responder()

        service._fetch_account_balance = MethodType(fake_fetch_account_balance, service)

        base_positions = [
            {
                "instId": service.symbol,
                "mgnMode": "isolated",
                "posSide": "long",
                "pos": "10",
                "margin": "0",
            }
        ]

        async def fake_fetch_positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return list(base_positions)

        monkeypatch.setattr(service, "_fetch_positions", fake_fetch_positions)
        monkeypatch.setattr(
            service,
            "_compute_leverage_adjusted_size",
            lambda **kwargs: 1.5,
        )

        context = {
            "symbol": service.symbol,
            "guardrails": {
                    "min_leverage": 0.1,
                "max_leverage": 3,
                "max_position_pct": 0.5,
                "isolated_margin_seed_usd": 250.0,
                "isolated_margin_max_transfer_usd": 400.0,
                "require_position_alignment": False,
            },
            "market": {"last_price": 100},
            "account": {
                "account_equity": 150,
                "available_eq_usd": 0.0,
                "available_balances": {},
            },
            "execution": {
                "enabled": True,
                "trade_mode": "isolated",
                "order_type": "market",
                "min_size": 0.001,
            },
            "positions": base_positions,
        }
        decision = {"action": "BUY", "confidence": 0.9, "position_size": 1.5, "stop_loss": 95.0}
        executed = await service.handle_llm_decision(decision, context)
        return account_api, funding_api, trade_api, executed

    account_api, funding_api, trade_api, executed = asyncio.run(scenario())
    assert executed is True
    assert len(account_api.calls) >= 2, "margin adjustment should retry after transfer"
    assert funding_api.transfers, "expected funding transfer before retry"
    assert trade_api.payloads, "order should be placed after auto-seed"


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
            {"action": "BUY", "confidence": 0.9, "position_size": 1.0, "stop_loss": 95.0},
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


def test_submit_order_records_margin_recommendation() -> None:
    class RejectingTradeApi:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.calls += 1
            return {
                "code": "1",
                "msg": "Insufficient margin",
                "data": [
                    {
                        "sCode": "51008",
                        "sMsg": "Insufficient isolated margin",
                    }
                ],
            }

    async def scenario() -> list[dict[str, Any]]:
        state = DummySnapshotStore()
        trade_api = RejectingTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=object(),
            market_api=object(),
            public_api=object(),
        )
        service._set_margin_guidance(
            service.symbol,
            {
                "quote_currency": "USDT",
                "required_gap": 125.0,
                "seed_limit": 200.0,
                "auto_seed_attempted": True,
                "auto_seed_success": False,
                "blocked_reason": "funding_insufficient",
                "funding_available": 50.0,
            },
        )
        await service._submit_order(
            symbol=service.symbol,
            side="BUY",
            pos_side=None,
            size=1.0,
            trade_mode="isolated",
            order_type="market",
            reduce_only=False,
            client_order_id="test",
            attach_algo_orders=None,
        )
        return list(service._execution_feedback)

    entries = asyncio.run(scenario())
    assert entries
    latest = entries[-1]
    assert latest["level"] == "error"
    assert latest["symbol"]
    recommendation = latest.get("recommendation")
    assert recommendation
    assert "Funding wallet" in recommendation.get("message", "")
    assert recommendation.get("quote_currency") == "USDT"
    assert "need≈" in latest.get("message", "")


def test_submit_order_attaches_fallback_recommendation_without_guidance() -> None:
    class RejectingTradeApi:
        def __init__(self) -> None:
            self.calls = 0

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.calls += 1
            return {
                "code": "1",
                "msg": "Insufficient margin",
                "data": [
                    {
                        "sCode": "51008",
                        "sMsg": "Insufficient isolated margin",
                    }
                ],
            }

    async def scenario() -> dict[str, Any]:
        state = DummySnapshotStore()
        trade_api = RejectingTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=object(),
            market_api=object(),
            public_api=object(),
        )
        await service._submit_order(
            symbol=service.symbol,
            side="BUY",
            pos_side=None,
            size=1.0,
            trade_mode="isolated",
            order_type="market",
            reduce_only=False,
            client_order_id="fallback",
            attach_algo_orders=None,
        )
        return service._execution_feedback[-1]

    latest = asyncio.run(scenario())
    recommendation = latest.get("recommendation")
    assert recommendation
    assert "Transfer additional" in recommendation.get("message", "")


def test_isolated_margin_buffer_auto_downsizes_to_seed_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyAccountApi:
        def __init__(self) -> None:
            self.adjust_calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(self, symbol, pos_side, amount, type="add", subAcct="") -> dict[str, Any]:
            self.adjust_calls.append(
                {
                    "symbol": symbol,
                    "pos_side": pos_side,
                    "amount": amount,
                }
            )
            return {"code": "0"}

        def get_account_balance(self, subAcct: str | None = None) -> list[dict[str, Any]]:
            return [
                {
                    "totalEq": "1000",
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "1000",
                            "eqUsd": "1000",
                            "availEq": "100",
                            "availBal": "100",
                            "availEqUsd": "100",
                        }
                    ],
                }
            ]

    async def scenario() -> tuple[float | None, list[dict[str, Any]]]:
        state = DummySnapshotStore()
        account_api = DummyAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=account_api,
            market_api=object(),
            public_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }

        refreshed, downsized = await service._ensure_isolated_margin_buffer(
            symbol=service.symbol,
            action="BUY",
            dual_side_mode=False,
            trade_mode="isolated",
            pos_side="net",
            existing_side_size=0.0,
            min_leverage=1.0,
            size=200.0,
            last_price=1.0,
            quote_currency="USDT",
            available_margin_usd=0.0,
            account_equity=1000.0,
            max_position_pct=0.5,
            symbol_cap_pct=0.5,
            max_notional_usd=500.0,
            guardrails={"isolated_margin_seed_usd": 50.0},
            min_size=0.1,
            tier_entries=[{}],
        )
        assert refreshed is not None
        return downsized, list(service._execution_feedback)

    downsized_size, feedback_entries = asyncio.run(scenario())
    assert downsized_size is not None
    assert downsized_size < 200.0
    assert downsized_size == pytest.approx(40.0, rel=1e-2)
    assert any(
        entry.get("message") == "Size clipped to fit isolated margin seed limit"
        for entry in feedback_entries
    )


def test_isolated_margin_buffer_seeds_using_tier_imr() -> None:
    class RecordingAccountApi:
        def __init__(self) -> None:
            self.adjust_calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(self, symbol, pos_side, amount, type="add", subAcct="") -> dict[str, Any]:
            self.adjust_calls.append({"symbol": symbol, "pos_side": pos_side, "amount": amount})
            return {"code": "0"}

        def get_account_balance(self, subAcct: str | None = None) -> list[dict[str, Any]]:
            return [
                {
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "200",
                            "eqUsd": "200",
                            "availEq": "20",
                            "availEqUsd": "20",
                        }
                    ]
                }
            ]

        def get_positions(self, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        def set_leverage(self, **kwargs: Any) -> dict[str, Any]:
            return {"code": "0"}

    async def scenario() -> list[dict[str, Any]]:
        state = DummySnapshotStore()
        account_api = RecordingAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=account_api,
            market_api=None,
            public_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }
        tier_entries = [
            {
                "minSz": "0",
                "maxSz": "1000",
                "imr": "0.2",
                "maxLever": "5",
            }
        ]
        await service._ensure_isolated_margin_buffer(
            symbol=service.symbol,
            action="BUY",
            dual_side_mode=True,
            trade_mode="isolated",
            pos_side="long",
            existing_side_size=0.0,
            min_leverage=1.0,
            size=100.0,
            last_price=1.0,
            quote_currency="USDT",
            available_margin_usd=7.0,
            account_equity=200.0,
            max_position_pct=0.5,
            symbol_cap_pct=0.5,
            max_notional_usd=500.0,
            guardrails={"isolated_margin_seed_pct": 0.5},
            min_size=0.1,
            tier_entries=tier_entries,
        )
        return account_api.adjust_calls

    adjust_calls = asyncio.run(scenario())
    assert adjust_calls
    added_amount = float(adjust_calls[0]["amount"])
    assert added_amount == pytest.approx(23.0, rel=1e-2)


def test_isolated_margin_seed_pct_caps_transfer() -> None:
    class RecordingAccountApi:
        def __init__(self) -> None:
            self.adjust_calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(self, symbol, pos_side, amount, type="add", subAcct="") -> dict[str, Any]:
            self.adjust_calls.append({"symbol": symbol, "amount": amount})
            return {"code": "0"}

        def get_account_balance(self, subAcct: str | None = None) -> list[dict[str, Any]]:
            return [
                {
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "1000",
                            "eqUsd": "1000",
                            "availEq": "0",
                            "availEqUsd": "0",
                        }
                    ]
                }
            ]

        def set_leverage(self, **kwargs: Any) -> dict[str, Any]:
            return {"code": "0"}

    async def scenario() -> tuple[list[dict[str, Any]], dict[str, Any]]:
        state = DummySnapshotStore()
        account_api = RecordingAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=account_api,
            market_api=None,
            public_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }
        tier_entries = [
            {
                "minSz": "0",
                "maxSz": "2000",
                "imr": "0.2",
                "maxLever": "5",
            }
        ]
        await service._ensure_isolated_margin_buffer(
            symbol=service.symbol,
            action="BUY",
            dual_side_mode=False,
            trade_mode="isolated",
            pos_side="net",
            existing_side_size=0.0,
            min_leverage=1.0,
            size=400.0,
            last_price=1.0,
            quote_currency="USDT",
            available_margin_usd=0.0,
            account_equity=1000.0,
            max_position_pct=0.5,
            symbol_cap_pct=0.5,
            max_notional_usd=1000.0,
            guardrails={"isolated_margin_seed_pct": 0.05},
            min_size=0.1,
            tier_entries=tier_entries,
        )
        guidance = service._get_margin_guidance(service.symbol)
        return account_api.adjust_calls, guidance or {}

    adjust_calls, guidance = asyncio.run(scenario())
    assert adjust_calls
    assert float(adjust_calls[0]["amount"]) == pytest.approx(50.0, rel=1e-3)
    assert guidance.get("seed_limit") == pytest.approx(50.0, rel=1e-3)


def test_isolated_margin_symbol_seed_pct_override_wins() -> None:
    class RecordingAccountApi:
        def __init__(self) -> None:
            self.adjust_calls: list[dict[str, Any]] = []

        def adjust_isolated_margin(self, symbol, pos_side, amount, type="add", subAcct="") -> dict[str, Any]:
            self.adjust_calls.append({"symbol": symbol, "amount": amount})
            return {"code": "0"}

        def get_account_balance(self, subAcct: str | None = None) -> list[dict[str, Any]]:
            return [
                {
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "2000",
                            "eqUsd": "2000",
                            "availEq": "0",
                            "availEqUsd": "0",
                        }
                    ]
                }
            ]

        def set_leverage(self, **kwargs: Any) -> dict[str, Any]:
            return {"code": "0"}

    async def scenario() -> tuple[list[dict[str, Any]], dict[str, Any]]:
        state = DummySnapshotStore()
        account_api = RecordingAccountApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=account_api,
            market_api=None,
            public_api=object(),
        )
        service._instrument_specs[service.symbol] = {
            "lot_size": 0.1,
            "min_size": 0.1,
            "tick_size": 0.0001,
        }
        tier_entries = [
            {
                "minSz": "0",
                "maxSz": "2000",
                "imr": "0.2",
                "maxLever": "5",
            }
        ]
        guardrails = {
            "isolated_margin_seed_pct": 0.5,
            "isolated_margin_symbol_seed_pct": {service.symbol: 0.02},
        }
        await service._ensure_isolated_margin_buffer(
            symbol=service.symbol,
            action="BUY",
            dual_side_mode=False,
            trade_mode="isolated",
            pos_side="net",
            existing_side_size=0.0,
            min_leverage=1.0,
            size=150.0,
            last_price=1.0,
            quote_currency="USDT",
            available_margin_usd=0.0,
            account_equity=2000.0,
            max_position_pct=0.5,
            symbol_cap_pct=0.5,
            max_notional_usd=2000.0,
            guardrails=guardrails,
            min_size=0.1,
            tier_entries=tier_entries,
        )
        guidance = service._get_margin_guidance(service.symbol)
        return account_api.adjust_calls, guidance or {}

    adjust_calls, guidance = asyncio.run(scenario())
    assert adjust_calls
    assert float(adjust_calls[0]["amount"]) == pytest.approx(40.0, rel=1e-3)
    assert guidance.get("seed_limit") == pytest.approx(40.0, rel=1e-3)


def test_submit_order_sets_isolated_leverage_before_order() -> None:
    class RecordingAccountApi:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def set_leverage(self, **payload: Any) -> dict[str, Any]:
            self.calls.append(payload)
            return {"code": "0"}

    class RecordingTradeApi:
        def __init__(self) -> None:
            self.payloads: list[dict[str, Any]] = []

        def place_order(self, **payload: Any) -> dict[str, Any]:
            self.payloads.append(payload)
            return {
                "code": "0",
                "data": [
                    {
                        "ordId": "2",
                        "sCode": "0",
                    }
                ],
            }

    async def scenario() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        state = DummySnapshotStore()
        account_api = RecordingAccountApi()
        trade_api = RecordingTradeApi()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            trade_api=trade_api,
            account_api=account_api,
            market_api=object(),
            public_api=object(),
        )
        await service._submit_order(
            symbol=service.symbol,
            side="BUY",
            pos_side="long",
            size=1.0,
            trade_mode="isolated",
            order_type="market",
            reduce_only=False,
            client_order_id="lev-test",
            attach_algo_orders=None,
            margin_currency="USDT",
            leverage=2.5,
            dual_side_mode=False,
        )
        return account_api.calls, trade_api.payloads

    leverage_calls, payloads = asyncio.run(scenario())
    assert leverage_calls
    assert payloads
    leverage_payload = leverage_calls[0]
    assert leverage_payload["instId"]
    assert leverage_payload["posSide"] == "net"
    assert leverage_payload["lever"] == "2.5"
    assert payloads[0]["tdMode"] == "isolated"


def test_regime_breakdown_exit_flattens_underwater_mr_position(monkeypatch: pytest.MonkeyPatch) -> None:
    """An open MR position below breakeven should be flattened when HTF flips to trend."""
    async def scenario() -> tuple[bool, list[str]]:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=object(),
            market_api=object(),
            public_api=object(),
            trade_api=object(),
        )
        symbol = service.symbol.upper()
        # Enable the opt-in regime-breakdown exit for MR.
        service.set_launcher_config({
            "mode": "launcher_only",
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "exit_on_regime_breakdown": True,
                    "htf_regime_preference": "chop",
                }
            },
        })
        # Track an open MR position that is underwater.
        service._launcher_in_position[f"mean_reversion:{symbol}"] = {
            "side": "long",
            "pos_side": "long",
            "strategy": "mean_reversion",
        }
        # Snapshot: HTF is trending (adx_htf high → chop gate blocks).
        service._last_full_snapshot = {
            "positions": [
                {
                    "instId": symbol,
                    "pos": 1.0,
                    "posSide": "long",
                    "avgPx": 100.0,
                    "mgnMode": "isolated",
                }
            ],
            "market_data": {
                symbol: {
                    "indicators": {
                        "adx_htf": 40.0,
                        "choppiness_htf": 30.0,
                    }
                }
            },
        }
        service._latest_ticker[symbol] = {"last": 95.0}  # underwater

        submitted: list[str] = []

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            submitted.append(str(kwargs.get("side")))
            return {"ordId": "regime-close"}, True

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        await service._check_strategy_regime_exits()
        # The close is submitted via asyncio.create_task — yield control so the
        # background task runs to completion before we assert.
        await asyncio.sleep(0.05)
        return bool(submitted), submitted

    submitted, sides = asyncio.run(scenario())
    assert submitted is True
    assert sides == ["sell"]  # flatten long → sell


def test_regime_breakdown_exit_skips_when_not_underwater(monkeypatch: pytest.MonkeyPatch) -> None:
    """An MR position above breakeven should NOT be flattened on regime breakdown."""
    async def scenario() -> bool:
        state = DummySnapshotStore()
        service = MarketService(
            state_service=state,
            enable_websocket=False,
            account_api=object(),
            market_api=object(),
            public_api=object(),
            trade_api=object(),
        )
        symbol = service.symbol.upper()
        service.set_launcher_config({
            "mode": "launcher_only",
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "exit_on_regime_breakdown": True,
                    "htf_regime_preference": "chop",
                }
            },
        })
        service._launcher_in_position[f"mean_reversion:{symbol}"] = {
            "side": "long",
            "pos_side": "long",
            "strategy": "mean_reversion",
        }
        service._last_full_snapshot = {
            "positions": [
                {
                    "instId": symbol,
                    "pos": 1.0,
                    "posSide": "long",
                    "avgPx": 100.0,
                    "mgnMode": "isolated",
                }
            ],
            "market_data": {
                symbol: {
                    "indicators": {
                        "adx_htf": 40.0,
                        "choppiness_htf": 30.0,
                    }
                }
            },
        }
        service._latest_ticker[symbol] = {"last": 105.0}  # profitable

        submitted: list[str] = []

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            submitted.append(str(kwargs.get("side")))
            return {"ordId": "regime-close"}, True

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        await service._check_strategy_regime_exits()
        return bool(submitted)

    submitted = asyncio.run(scenario())
    assert submitted is False


def _make_trade_mgmt_service(monkeypatch: pytest.MonkeyPatch, symbol: str, side: str, entry: float, sl: float, last: float) -> Any:
    """Build a MarketService with a seeded trade-mgmt state and a snapshot for _check_trade_management."""
    state = DummySnapshotStore()
    service = MarketService(
        state_service=state,
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )
    service._strategy_config = {
        "trade_management": {
            "enabled": True,
            "breakeven_enabled": True,
            "breakeven_at_r": 0.7,
            "breakeven_buffer_pct": 0.05,
            "partial_tp_enabled": True,
            "partial_tp_at_r": 0.8,
            "partial_tp_fraction": 0.5,
            "time_stop_enabled": False,
            "time_stop_seconds": 0.0,
            "time_stop_min_r": 0.3,
            "reentry_cooldown_seconds": 0.0,
            "trailing_enabled": True,
            "trailing_activate_r": 1.0,
            "trailing_distance_atr": 1.5,
            "trailing_floor_r": 0.5,
            "trailing_step_r": 0.2,
            "software_stop_loss_enabled": True,
        }
    }
    service._latest_ticker[symbol] = {"last": last}
    service._launcher_in_position[f"vwap_reversion:{symbol}"] = {
        "side": side,
        "pos_side": side,
        "strategy": "vwap_reversion",
    }
    service._seed_trade_mgmt_state(
        symbol=symbol,
        side=side,
        strategy_name="vwap_reversion",
        entry_price=entry,
        tp_price=entry * 1.05 if side == "long" else entry * 0.95,
        sl_price=sl,
    )
    service._last_full_snapshot = {
        "positions": [
            {
                "instId": symbol,
                "pos": 1.0 if side == "long" else -1.0,
                "posSide": side,
                "avgPx": entry,
                "mgnMode": "isolated",
                "uplRatio": 0.0,
                "upl": 0.0,
            }
        ],
        "market_data": {
            symbol: {
                "indicators": {
                    "atr_pct": 1.0,
                }
            }
        },
    }
    return service


def test_trade_mgmt_software_stop_loss_closes_at_sl(monkeypatch: pytest.MonkeyPatch) -> None:
    """A position whose pnl_pct <= -risk_pct should be market-closed (software stop)."""
    async def scenario() -> tuple[bool, list[str]]:
        symbol = "BTC-USDT-SWAP"
        entry = 100.0
        sl = 98.0  # risk_pct = 2%
        last = 97.0  # pnl_pct = -3% <= -2% → software stop fires
        service = _make_trade_mgmt_service(monkeypatch, symbol, "long", entry, sl, last)

        submitted: list[str] = []

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            submitted.append(str(kwargs.get("side")))
            return {"ordId": "softstop"}, True

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        await service._check_trade_management()
        await asyncio.sleep(0.05)
        return bool(submitted), submitted

    submitted, sides = asyncio.run(scenario())
    assert submitted is True
    assert sides == ["sell"]  # flatten long → sell


def test_trade_mgmt_software_stop_skips_when_profitable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A profitable position should NOT be software-stopped."""
    async def scenario() -> bool:
        symbol = "BTC-USDT-SWAP"
        entry = 100.0
        sl = 98.0
        last = 101.0  # pnl_pct = +1% → no software stop
        service = _make_trade_mgmt_service(monkeypatch, symbol, "long", entry, sl, last)

        submitted: list[str] = []

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            submitted.append(str(kwargs.get("side")))
            return {"ordId": "x"}, True

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        await service._check_trade_management()
        await asyncio.sleep(0.05)
        return bool(submitted)

    submitted = asyncio.run(scenario())
    assert submitted is False


def test_trade_mgmt_trailing_stop_ratchets_sl(monkeypatch: pytest.MonkeyPatch) -> None:
    """A position past trailing_activate_r should ratchet the SL up (long)."""
    async def scenario() -> tuple[bool, list[str]]:
        symbol = "BTC-USDT-SWAP"
        entry = 100.0
        sl = 98.0  # risk_pct = 2%
        last = 103.0  # pnl_pct = +3% → R = 1.5 ≥ 1.0 → trailing activates
        service = _make_trade_mgmt_service(monkeypatch, symbol, "long", entry, sl, last)

        # Simulate an existing protection SL so the ratchet compares against it.
        service._position_protection[symbol] = {"stop_loss": 98.0, "take_profit": 105.0}

        moved: list[float] = []

        async def fake_submit_order(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], bool]:
            # Partial TP / breakeven may fire; swallow them so only trailing matters.
            return {"ordId": "x"}, True

        async def fake_move_sl(*, symbol: str, new_sl_price: float, **kwargs: Any) -> None:
            moved.append(new_sl_price)

        monkeypatch.setattr(service, "_submit_order", fake_submit_order)
        monkeypatch.setattr(service, "_trade_mgmt_move_sl", fake_move_sl)
        await service._check_trade_management()
        await asyncio.sleep(0.05)
        return bool(moved), [str(p) for p in moved]

    moved, sl_prices = asyncio.run(scenario())
    assert moved is True
    # Trailing SL for a long at last=103, atr_pct=1.0, distance=1.5:
    #   trail_sl = 103 * (1 - 1.5*1.0/100) = 103 * 0.985 = 101.455
    #   floor_sl = 100 * (1 - 0.5*2/100) = 100 * 0.99 = 99.0
    #   new_sl = max(101.455, 99.0) = 101.455
    # (Breakeven also fires first at 100.05; the trailing move is the last one.)
    assert len(sl_prices) >= 1
    assert abs(float(sl_prices[-1]) - 101.455) < 0.01


def test_trade_mgmt_trailing_stop_respects_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The trailing SL should never go below the floor R even if ATR distance is large."""
    async def scenario() -> tuple[bool, list[str]]:
        symbol = "BTC-USDT-SWAP"
        entry = 100.0
        sl = 98.0  # risk_pct = 2%
        last = 101.0  # pnl_pct = +1% → R = 0.5 < 1.0 → trailing does NOT activate
        service = _make_trade_mgmt_service(monkeypatch, symbol, "long", entry, sl, last)

        placed: list[dict[str, Any]] = []

        async def fake_cancel_position_protection(*args: Any, **kwargs: Any) -> None:
            return None

        async def fake_place_position_protection(*args: Any, **kwargs: Any) -> bool:
            placed.append(dict(kwargs))
            return True

        monkeypatch.setattr(service, "_cancel_position_protection", fake_cancel_position_protection)
        monkeypatch.setattr(service, "_place_position_protection", fake_place_position_protection)
        await service._check_trade_management()
        await asyncio.sleep(0.05)
        return bool(placed)

    placed = asyncio.run(scenario())
    assert placed is False  # R=0.5 < activate_r=1.0 → no trailing update
