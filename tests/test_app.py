from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from fastapi.testclient import TestClient

from app.core import config
from app.main import create_app
from app.services.prompt_builder import PromptBuilder


def test_settings_reads_environment_variables(monkeypatch) -> None:
    monkeypatch.setenv("WS_UPDATE_INTERVAL", "240")
    monkeypatch.setenv("OKX_API_KEY", "demo")
    config.get_settings.cache_clear()

    settings = config.get_settings()

    assert settings.okx_api_key == "demo"
    assert settings.ws_update_interval == 240


def test_fastapi_app_health_endpoint() -> None:
    app = create_app()
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_state_endpoint_returns_503_when_state_service_missing() -> None:
    app = create_app()
    with TestClient(app) as client:
        response = client.get("/state/latest")

    assert response.status_code == 503


def test_recent_trades_endpoint_handles_missing_database(monkeypatch) -> None:
    app = create_app(enable_background_services=False)

    async def fake_fetch_recent_trades(limit: int = 100):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("app.main.fetch_recent_trades", fake_fetch_recent_trades)

    with TestClient(app) as client:
        response = client.get("/trades/recent")

    assert response.status_code == 503


def _iso_timestamp(when: datetime) -> str:
    return when.isoformat().replace("+00:00", "Z")


def _sample_snapshot() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "generated_at": _iso_timestamp(now),
        "symbol": "BTC-USDT-SWAP",
        "symbols": ["BTC-USDT-SWAP"],
        "positions": [
            {
                "instId": "BTC-USDT-SWAP",
                "pos": "1",
                "avgPx": "42000",
                "posSide": "long",
                "lever": "3",
            }
        ],
        "account_equity": 100000,
        "total_account_value": 100000,
        "total_eq_usd": 100000,
        "market_data": {
            "BTC-USDT-SWAP": {
                "ticker": {
                    "last": "43000",
                    "bidPx": "42995",
                    "askPx": "43005",
                    "volCcy24h": "12345",
                    "changeRate": "0.02",
                },
                "funding_rate": {
                    "fundingRate": "0.0001",
                    "nextFundingRate": "0.0002",
                    "fundingTime": _iso_timestamp(now + timedelta(hours=4)),
                },
                "open_interest": {"oi": "1000", "oiCcy": "43000000"},
                "custom_metrics": {
                    "order_flow_imbalance": 10,
                    "cumulative_volume_delta": 25,
                        "market_long_short_ratio": {
                            "value": 1.2,
                            "series": [1.05, 1.1, 1.2],
                            "timestamps": [1, 2, 3],
                            "period": "5m",
                        },
                },
                "risk_metrics": {
                    "atr": 150,
                    "atr_pct": 0.35,
                    "suggested_stop": 225,
                    "suggested_stop_pct": 0.52,
                },
                "strategy_signal": {
                    "action": "BUY",
                    "confidence": 0.66,
                    "reason": "RSI oversold",
                },
                "order_book": {
                    "bids": [["42990", "20"], ["42980", "10"]],
                    "asks": [["43010", "15"], ["43020", "5"]],
                },
                "indicators": {
                    "rsi": 45,
                    "macd": {"value": 1.2, "signal": 0.8, "hist": 0.4, "series": [0.1, 0.2]},
                    "bollinger_bands": {"upper": 44000, "middle": 43000, "lower": 42000},
                    "stoch_rsi": {"k": 55, "d": 45},
                    "moving_averages": {"ema_50": 42800, "ema_200": 41000},
                    "vwap": 43100,
                    "atr": 150,
                    "atr_pct": 0.35,
                    "volume": {"last": 1200, "average": 950, "series": [1000, 1100]},
                    "vwap_series": [42900, 43000, 43100],
                    "volume_rsi_series": [45, 55],
                    "ohlcv": [
                        {"ts": 1, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100},
                        {"ts": 2, "open": 1.5, "high": 2.5, "low": 1.2, "close": 2.0, "volume": 120},
                    ],
                },
                "liquidations": [
                    {"px": "42500", "sz": "50", "side": "sell"},
                    {"px": "43500", "sz": "35", "side": "buy"},
                ],
            }
        },
        "open_orders": [
            {"instId": "BTC-USDT-SWAP", "side": "buy", "sz": "0.5", "px": "42500", "state": "live"}
        ],
    }


def test_prompt_builder_compiles_structured_payload() -> None:
    snapshot = _sample_snapshot()
    metadata = {"ta_timeframe": "1H", "guardrails": {"max_leverage": 4}}
    builder = PromptBuilder(snapshot, metadata=metadata, max_candles=1)

    payload = builder.build(symbol="BTC-USDT-SWAP", timeframe="1H")
    context = payload["context"]
    prompt = payload["prompt"]

    assert context["symbol"] == "BTC-USDT-SWAP"
    assert context["timeframe"] == "1H"
    assert context["market"]["spread"] == 10.0
    assert len(context["history"]["candles"]) == 1  # trimmed to max_candles
    assert context["positions"][0]["side"] == "LONG"
    assert context["strategy_signal"]["action"] == "BUY"
    assert context["trend_confirmation"]["moving_averages"]["bias"] == "bullish"
    assert context["liquidity_context"]["liquidity_bias"] == "bid-supported"
    assert context["derivatives_posture"]["long_short_ratio"]["value"] == 1.2
    assert context["derivatives_posture"]["liquidation_clusters"]
    assert context["snapshot_health"]["stale"] is False
    assert context["pending_orders"]["total"] == 1
    assert context["portfolio_exposure"]["long_notional"] == pytest.approx(43000)
    assert prompt["response_schema"]["properties"]["action"]["enum"] == ["BUY", "SELL", "HOLD"]
    assert prompt["model"] is None


def test_prompt_builder_respects_live_execution_limits() -> None:
    snapshot = _sample_snapshot()
    updated_at = _iso_timestamp(datetime.now(timezone.utc))
    snapshot["execution_limits"] = {
        "BTC-USDT-SWAP": {
            "available_margin_usd": 1234.5,
            "account_equity_usd": 5678.9,
            "max_notional_usd": 9876.5,
            "max_leverage": 4.5,
            "quote_currency": "USDT",
            "quote_available_usd": 1100.0,
            "quote_cash_usd": 1000.0,
            "updated_at": updated_at,
            "source": "execution",
        }
    }
    symbol_cap_pct = 0.05
    metadata = {
        "guardrails": {
            "max_position_pct": 0.2,
            "symbol_position_caps": {"BTC-USDT-SWAP": symbol_cap_pct},
        }
    }
    builder = PromptBuilder(snapshot, metadata=metadata, max_candles=1)

    payload = builder.build(symbol="BTC-USDT-SWAP", timeframe="1H")
    execution_block = payload["context"]["execution"]
    expected_guardrail_cap = 5678.9 * 0.2 * 4.5
    expected_symbol_cap = 5678.9 * symbol_cap_pct * 4.5

    assert execution_block["available_margin_usd"] == pytest.approx(1234.5)
    assert execution_block["account_equity_usd"] == pytest.approx(5678.9)
    assert execution_block["max_position_value_usd"] == pytest.approx(expected_guardrail_cap)
    assert execution_block["margin_max_position_value_usd"] == pytest.approx(9876.5)
    assert execution_block["symbol_max_position_pct"] == pytest.approx(symbol_cap_pct)
    assert execution_block["symbol_max_position_value_usd"] == pytest.approx(expected_symbol_cap)
    assert execution_block["effective_max_position_value_usd"] == pytest.approx(expected_symbol_cap)
    assert execution_block["max_leverage"] == pytest.approx(4.5)
    assert execution_block["live_margin_snapshot"]["updated_at"] == updated_at
    assert execution_block["live_margin_snapshot"]["quote_currency"] == "USDT"
    assert execution_block["live_margin_snapshot"]["quote_available_usd"] == pytest.approx(1100.0)
    assert execution_block["live_margin_snapshot"]["quote_cash_usd"] == pytest.approx(1000.0)
    assert execution_block["margin_health"]["limiting_factor"] == "symbol"
    assert execution_block["margin_health"]["symbol_cap_usd"] == pytest.approx(expected_symbol_cap)


def test_prompt_builder_applies_caps_per_symbol() -> None:
    snapshot = _sample_snapshot()
    snapshot["symbols"].append("ETH-USDT-SWAP")
    snapshot["market_data"]["ETH-USDT-SWAP"] = snapshot["market_data"]["BTC-USDT-SWAP"]
    metadata = {
        "guardrails": {
            "max_position_pct": 0.5,
            "max_leverage": 3,
            "symbol_position_caps": {
                "BTC-USDT-SWAP": 0.1,
                "ETH-USDT-SWAP": 0.2,
            },
        }
    }
    builder = PromptBuilder(snapshot, metadata=metadata, max_candles=1)

    btc_payload = builder.build(symbol="BTC-USDT-SWAP", timeframe="1H")
    eth_payload = builder.build(symbol="ETH-USDT-SWAP", timeframe="1H")

    btc_execution = btc_payload["context"]["execution"]
    eth_execution = eth_payload["context"]["execution"]
    equity = snapshot["account_equity"]
    leverage = metadata["guardrails"]["max_leverage"]

    assert btc_execution["symbol_max_position_pct"] == pytest.approx(0.1)
    assert eth_execution["symbol_max_position_pct"] == pytest.approx(0.2)
    assert btc_execution["symbol_max_position_value_usd"] == pytest.approx(equity * leverage * 0.1)
    assert eth_execution["symbol_max_position_value_usd"] == pytest.approx(equity * leverage * 0.2)
    assert btc_execution["effective_max_position_value_usd"] == pytest.approx(equity * leverage * 0.1)
    assert eth_execution["effective_max_position_value_usd"] == pytest.approx(equity * leverage * 0.2)


def test_prompt_builder_includes_margin_health_and_feedback_digest() -> None:
    snapshot = _sample_snapshot()
    now = datetime.now(timezone.utc)
    snapshot["execution_limits"] = {
        "BTC-USDT-SWAP": {
            "available_margin_usd": 8000,
            "account_equity_usd": 10000,
            "max_notional_usd": 15000,
            "max_leverage": 4,
            "tier_max_notional_usd": 50000,
            "updated_at": _iso_timestamp(now),
            "source": "execution",
        }
    }
    snapshot["execution_feedback"] = [
        {
            "timestamp": _iso_timestamp(now - timedelta(minutes=2)),
            "symbol": "BTC-USDT-SWAP",
            "side": "BUY",
            "size": 1.0,
            "message": "Insufficient available margin",
            "level": "warning",
            "meta": {"available_margin_usd": 500},
        },
        {
            "timestamp": _iso_timestamp(now - timedelta(minutes=1)),
            "symbol": "BTC-USDT-SWAP",
            "side": "BUY",
            "size": 0.5,
            "message": "Submitted test order",
            "level": "info",
        },
    ]
    metadata = {"guardrails": {"max_position_pct": 0.1, "max_leverage": 2}}
    builder = PromptBuilder(snapshot, metadata=metadata, max_candles=1)

    payload = builder.build(symbol="BTC-USDT-SWAP", timeframe="1H")
    context = payload["context"]
    execution_block = context["execution"]
    margin_health = execution_block["margin_health"]
    assert margin_health["limiting_factor"] == "guardrail"
    assert margin_health["effective_cap_usd"] == pytest.approx(4000.0)
    feedback_digest = execution_block["feedback_digest"]
    assert feedback_digest["counts"]["warning"] == 1
    assert feedback_digest["latest"]["message"] == "Submitted test order"
    assert context["execution_feedback"][0]["message"] == "Insufficient available margin"


def test_prompt_builder_filters_execution_feedback_per_symbol() -> None:
    snapshot = _sample_snapshot()
    other_symbol = "ETH-USDT-SWAP"
    snapshot["symbols"].append(other_symbol)
    now = datetime.now(timezone.utc)
    snapshot["execution_feedback"] = [
        {
            "timestamp": (now - timedelta(minutes=4)).isoformat(),
            "symbol": other_symbol,
            "message": "ETH warning",
            "level": "warning",
        },
        {
            "timestamp": (now - timedelta(minutes=3, seconds=30)).isoformat(),
            "symbol": snapshot["symbol"],
            "message": "BTC info",
            "level": "info",
        },
    ]
    builder = PromptBuilder(snapshot, metadata={}, max_candles=1)

    btc_payload = builder.build(symbol=snapshot["symbol"], timeframe="1H")
    eth_payload = builder.build(symbol=other_symbol, timeframe="1H")

    btc_feedback = btc_payload["context"].get("execution_feedback")
    eth_feedback = eth_payload["context"].get("execution_feedback")

    assert btc_feedback and btc_feedback[0]["message"] == "BTC info"
    assert eth_feedback and eth_feedback[0]["message"] == "ETH warning"


def test_llm_prompt_endpoint_uses_builder(monkeypatch) -> None:
    snapshot = _sample_snapshot()
    app = create_app(enable_background_services=False)

    class _DummyState:
        async def get_market_snapshot(self) -> dict:
            return snapshot

    recorded: dict[str, Any] = {}

    async def fake_persist_prompt_run(app_obj, bundle, *, decision=None):
        context = bundle.payload.get("context") or {}
        recorded["symbol"] = context.get("symbol")
        recorded["timeframe"] = context.get("timeframe")
        recorded["decision"] = decision
        return "prompt-1"

    monkeypatch.setattr("app.main.persist_prompt_run", fake_persist_prompt_run)

    with TestClient(app) as client:
        app.state.state_service = _DummyState()
        response = client.get("/llm/prompt", params={"symbol": "BTC-USDT-SWAP", "timeframe": "4H"})

    assert response.status_code == 200
    body = response.json()
    assert body["payload"]["context"]["market"]["last_price"] == 43000.0
    assert body["prompt_id"] == "prompt-1"
    assert recorded["symbol"] == "BTC-USDT-SWAP"
    assert recorded["timeframe"] == "4H"


def test_llm_prompt_endpoint_blocks_stale_snapshot() -> None:
    stale_snapshot = _sample_snapshot()
    stale_snapshot["generated_at"] = _iso_timestamp(
        datetime.now(timezone.utc) - timedelta(hours=2)
    )
    app = create_app(enable_background_services=False)

    class _DummyState:
        async def get_market_snapshot(self) -> dict:
            return stale_snapshot

    with TestClient(app) as client:
        app.state.state_service = _DummyState()
        response = client.get("/llm/prompt", params={"symbol": "BTC-USDT-SWAP"})

    assert response.status_code == 503
    assert response.json()["detail"] == "snapshot stale; awaiting refresh"


def test_llm_prompt_endpoint_respects_wait_for_tp_sl_guard() -> None:
    snapshot = _sample_snapshot()
    app = create_app(enable_background_services=False)

    class _DummyState:
        async def get_market_snapshot(self) -> dict:
            return snapshot

    with TestClient(app) as client:
        app.state.state_service = _DummyState()
        app.state.runtime_config["guardrails"]["wait_for_tp_sl"] = True
        app.state.runtime_config["wait_for_tp_sl"] = True
        response = client.get("/llm/prompt", params={"symbol": snapshot["symbol"]})

    assert response.status_code == 409
    assert "wait-for-tp-sl" in response.json()["detail"]


def test_equity_history_endpoint_returns_items(monkeypatch) -> None:
    app = create_app(enable_background_services=False)

    async def fake_fetch_equity_history(limit: int = 200):
        return [
            {
                "observed_at": "2025-12-31T00:00:00Z",
                "account_equity": 100000.0,
                "total_eq_usd": 100000.0,
                "total_account_value": 100000.0,
            }
        ]

    monkeypatch.setattr("app.main.fetch_equity_history", fake_fetch_equity_history)

    with TestClient(app) as client:
        response = client.get("/equity/history", params={"limit": 5})

    assert response.status_code == 200
    assert response.json()["items"][0]["total_eq_usd"] == 100000.0


def test_llm_execute_endpoint_returns_decision(monkeypatch) -> None:
    snapshot = _sample_snapshot()
    app = create_app(enable_background_services=False)

    class _DummyState:
        async def get_market_snapshot(self) -> dict:
            return snapshot

    async def fake_execute_llm_decision(app_obj, bundle):
        decision = {"action": "BUY", "confidence": 0.77}
        return decision, "prompt-xyz"

    monkeypatch.setattr("app.main.execute_llm_decision", fake_execute_llm_decision)

    with TestClient(app) as client:
        app.state.state_service = _DummyState()
        response = client.post("/llm/execute", params={"symbol": "BTC-USDT-SWAP", "timeframe": "1H"})

    body = response.json()
    assert response.status_code == 200
    assert body["decision"]["action"] in {"BUY", "SELL", "HOLD"}
    assert body["prompt_id"] == "prompt-xyz"
