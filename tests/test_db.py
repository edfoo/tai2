import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

import asyncpg
import pytest

from app.core import config
from app.db import postgres
from app.models.trade import ExecutedTrade
from app.services.state_service import StateService


class DummyPool:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.queries.append((query, args))
        return "OK"

    async def close(self) -> None:  # pragma: no cover - helper for interface parity
        return None


class FakeRedis:
    def __init__(self) -> None:
        self.storage: dict[str, str] = {}
        self.published: list[str] = []

    async def set(self, key: str, value: str) -> None:
        self.storage[key] = value

    async def get(self, key: str) -> str | None:
        return self.storage.get(key)

    async def publish(self, channel: str, payload: str) -> None:
        self.published.append(f"{channel}:{payload}")

    async def ping(self) -> bool:  # pragma: no cover - parity helper
        return True

    async def close(self) -> None:  # pragma: no cover
        return None


@pytest.fixture(autouse=True)
def clear_settings_cache() -> None:
    config.get_settings.cache_clear()


def test_insert_executed_trade_uses_async_pool(monkeypatch) -> None:
    pool = DummyPool()

    async def fake_create_pool(*args: Any, **kwargs: Any) -> DummyPool:  # noqa: ARG001
        return pool

    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/tai2")
    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(postgres, "_POOL", None)

    trade = ExecutedTrade(
        id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        symbol="btc-usdt",
        side="buy",
        price=Decimal("42000.5"),
        amount=Decimal("0.25"),
        llm_reasoning="Momentum entry",
        pnl=Decimal("0"),
    )

    async def scenario() -> None:
        await postgres.init_postgres_pool()
        await postgres.insert_executed_trade(trade)

    asyncio.run(scenario())

    assert any("executed_trades" in query for query, _ in pool.queries)
    insert_calls = [q for q in pool.queries if "INSERT INTO executed_trades" in q[0]]
    assert insert_calls, "Trade insert query was not executed"


def test_state_service_round_trip() -> None:
    fake_redis = FakeRedis()
    service = StateService(redis_client=fake_redis)
    snapshot = {
        "account": {"balance": 1000},
        "positions": [{"symbol": "BTC-USDT", "size": 1}],
        "order_book": {"bids": [], "asks": []},
    }

    async def scenario() -> None:
        await service.set_market_snapshot(snapshot)
        loaded = await service.get_market_snapshot()
        assert loaded == snapshot

    asyncio.run(scenario())
