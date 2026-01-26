from __future__ import annotations

import json
import logging
from typing import Any, Optional

from redis import asyncio as redis_asyncio

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_REDIS_CLIENT: Optional[redis_asyncio.Redis] = None


def get_redis_client() -> redis_asyncio.Redis:
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT

    settings = get_settings()
    if not settings.redis_url:
        msg = "REDIS_URL environment variable is required for Redis"
        raise RuntimeError(msg)

    _REDIS_CLIENT = redis_asyncio.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    return _REDIS_CLIENT


async def ensure_redis_connection() -> redis_asyncio.Redis:
    client = get_redis_client()
    try:
        await client.ping()
    except Exception as exc:  # pragma: no cover - network dependent
        logger.error("Unable to ping Redis: %s", exc)
        raise
    return client


async def close_redis_client() -> None:
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        await _REDIS_CLIENT.close()
        _REDIS_CLIENT = None
        logger.info("Redis client closed")


class StateService:
    """Stores the most recent market state snapshot in Redis."""

    snapshot_key = "tai2:state:latest"
    risk_locks_key = "tai2:state:risk_locks"

    def __init__(self, redis_client: Optional[redis_asyncio.Redis] = None) -> None:
        self.redis = redis_client or get_redis_client()
        self.pubsub_channel = "tai2:state:channel"

    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        serialized = json.dumps(snapshot)
        await self.redis.set(self.snapshot_key, serialized)
        await self.redis.publish(self.pubsub_channel, serialized)

    async def get_market_snapshot(self) -> Optional[dict[str, Any]]:
        data = await self.redis.get(self.snapshot_key)
        if not data:
            return None
        return json.loads(data)

    async def set_risk_locks(self, risk_locks: dict[str, Any]) -> None:
        serialized = json.dumps(risk_locks or {})
        await self.redis.set(self.risk_locks_key, serialized)

    async def get_risk_locks(self) -> Optional[dict[str, Any]]:
        data = await self.redis.get(self.risk_locks_key)
        if not data:
            return None
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logger.warning("Failed to decode persisted risk locks; resetting store")
            await self.redis.delete(self.risk_locks_key)
            return None

    async def subscribe_snapshots(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.pubsub_channel)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                payload = message.get("data")
                if payload:
                    yield json.loads(payload)
        finally:  # pragma: no cover - network resource cleanup
            await pubsub.unsubscribe(self.pubsub_channel)
            await pubsub.close()


__all__ = [
    "StateService",
    "close_redis_client",
    "ensure_redis_connection",
    "get_redis_client",
]
