"""Restart gap (wick_rejection plan, after Phase 6): re-hydrating a WR position.

After an app restart ``_rehydrate_open_positions`` rebuilds tracking for open
positions the bot opened (an unreconciled DB entry exists).  It now reads the
entry's ``strategy`` so a wick_rejection position keeps its strategy key, its
strategy-owned exits (time stop counted from the DB entry time, software stop
at the position's leverage) and its margin for the trigger's caps.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

import app.services.market_service as market_service_module
from app.services.market_service import MarketService
from app.services.strategies.defaults import strategy_defaults
from app.services.trade_management_rules import StrategyExitParams

SYMBOL = "WR-USDT-SWAP"


def _service(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]],
             wr_over: dict[str, Any] | None = None, tm_enabled: bool = False) -> MarketService:
    service = MarketService(
        state_service=None, enable_websocket=False,  # type: ignore[arg-type]
        trade_api=object(), account_api=object(), market_api=object(),
        public_api=object(), trading_api=object(), funding_api=object(),
    )
    for attr in ("_trade_api", "_account_api", "_market_api", "_market_api_prod",
                 "_public_api", "_trading_api", "_funding_api"):
        setattr(service, attr, None)

    async def no_submit(**kwargs: Any) -> None:
        raise AssertionError("no orders in this test")

    async def positions(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return [{"instId": SYMBOL, "pos": "-3", "posSide": "short", "avgPx": "100",
                 "lever": "3", "margin": "20", "mgnMode": "isolated"}]

    async def protection(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"tpTriggerPx": "99.5"}

    async def fetch_rows(**kwargs: Any) -> list[dict[str, Any]]:
        return rows

    service._submit_order = no_submit  # type: ignore[method-assign]
    service._fetch_positions = positions  # type: ignore[method-assign]
    service._fetch_latest_symbol_protection = protection  # type: ignore[method-assign]
    service.settings = SimpleNamespace(database_url="postgresql://test")  # type: ignore[assignment]
    monkeypatch.setattr(market_service_module, "fetch_unreconciled_trades", fetch_rows)
    service._strategy_config = {"trade_management": {"enabled": tm_enabled}}
    wr = dict(strategy_defaults("wick_rejection"), enabled=True)
    wr.update(wr_over or {})
    service.set_launcher_config({"mode": "launcher_only", "strategies": {"wick_rejection": wr}})
    return service


def _row(strategy: str | None, minutes_ago: float) -> dict[str, Any]:
    ts = datetime.fromtimestamp(time.time() - minutes_ago * 60, tz=timezone.utc)
    return {"id": "x", "timestamp": ts, "symbol": SYMBOL, "side": "sell", "strategy": strategy}


class TestRehydration:
    def test_wick_rejection_position_keeps_its_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        service = _service(monkeypatch, [_row(None, 600), _row("wick_rejection", 20)],
                           wr_over={"emergency_stop_mode": "software"})
        asyncio.run(service._rehydrate_open_positions())

        tracked = service._launcher_in_position[f"wick_rejection:{SYMBOL}"]
        assert tracked["strategy"] == "wick_rejection"
        assert tracked["margin_usd"] == 20.0
        state = service._trade_mgmt_state[SYMBOL]
        assert state["strategy"] == "wick_rejection"
        params = StrategyExitParams.from_dict(state["strategy_exit"])
        assert params is not None
        assert params.max_hold_seconds == 3600.0
        # 35% margin at the position's 3x leverage.
        assert params.software_stop_pct == pytest.approx(35.0 / 3)
        # Time stop keeps counting from the DB entry (20 min ago).
        assert time.time() - state["entry_ts"] == pytest.approx(1200, abs=5)

    def test_time_stop_fires_after_restart(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def scenario():
            service = _service(monkeypatch, [_row("wick_rejection", 61)])
            await service._rehydrate_open_positions()
            closes: list[dict[str, Any]] = []

            async def fake_close(**kwargs: Any) -> None:
                closes.append(kwargs)

            service._trade_mgmt_full_close = fake_close  # type: ignore[method-assign]
            service.get_last_price = lambda sym: 99.9  # type: ignore[method-assign]
            service._last_full_snapshot = {
                "positions": [{"instId": SYMBOL, "pos": "-3", "avgPx": "100",
                               "posSide": "short", "mgnMode": "isolated"}],
            }
            await service._check_trade_management()
            await asyncio.sleep(0)
            return closes

        closes = asyncio.run(scenario())
        assert [c["reason"] for c in closes] == ["time_stop"]

    # mean_reversion: other launcher strategies keep the legacy key so a
    # restart does not re-enable their own exits (MR regime breakdown).
    @pytest.mark.parametrize("strategy", [None, "", "Skimming", "mean_reversion"])
    def test_unknown_strategy_keeps_legacy_key(
        self, monkeypatch: pytest.MonkeyPatch, strategy: str | None
    ) -> None:
        service = _service(monkeypatch, [_row(strategy, 5)], tm_enabled=True)
        asyncio.run(service._rehydrate_open_positions())
        assert list(service._launcher_in_position) == [SYMBOL]
        assert service._launcher_in_position[SYMBOL]["strategy"] == ""
        assert service._trade_mgmt_state[SYMBOL]["strategy_exit"] is None
        assert service._trade_mgmt_state[SYMBOL]["strategy"] == ""

    def test_row_epoch(self) -> None:
        dt = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
        assert MarketService._row_epoch(dt) == dt.timestamp()
        assert MarketService._row_epoch("2026-11-20T12:00:00Z") == dt.timestamp()
        assert MarketService._row_epoch(dt.timestamp() * 1000) == dt.timestamp()
        assert MarketService._row_epoch(None) is None
