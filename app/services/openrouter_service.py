from __future__ import annotations

import logging
import time
from typing import Any

import httpx
from openrouter import OpenRouter

from app.core.config import get_settings

logger = logging.getLogger(__name__)

DEFAULT_MODEL_OPTIONS = [
    {
        "id": "openrouter/gpt-4o-mini",
        "label": "OpenAI GPT-4o mini",
        "pricing": None,
    },
    {
        "id": "openrouter/gpt-4o",
        "label": "OpenAI GPT-4o",
        "pricing": None,
    },
    {
        "id": "openrouter/claude-3.5",
        "label": "Anthropic Claude 3.5",
        "pricing": None,
    },
]

_CACHE_ATTR = "openrouter_model_cache"
_CACHE_TTL_SECONDS = 3600
_API_URL = "https://openrouter.ai/api/v1/models"
_USAGE_CACHE_ATTR = "openrouter_usage_cache"
_USAGE_TTL_SECONDS = 300
_CLIENT_ATTR = "openrouter_sdk_client"
_PER_MILLION = 1_000_000


def _safe_price(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_models(data: list[Any]) -> list[dict[str, Any]]:
    models: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        model_id = entry.get("id")
        if not model_id:
            continue
        label = (
            entry.get("name")
            or entry.get("display_name")
            or entry.get("provider")
            or model_id
        )
        pricing_block = entry.get("pricing") or {}
        unit_value = pricing_block.get("unit") or pricing_block.get("unit_name")
        if isinstance(unit_value, str):
            unit_value = unit_value.strip()
        else:
            unit_value = None

        unit = unit_value
        scale = 1.0
        if not unit:
            unit = "per 1M tokens"
            scale = float(_PER_MILLION)
        elif unit.lower() in {"token", "tokens", "per token", "per-token"}:
            unit = "per 1M tokens"
            scale = float(_PER_MILLION)

        prompt_cost = _safe_price(pricing_block.get("prompt"))
        if prompt_cost is not None:
            prompt_cost *= scale
        completion_cost = _safe_price(pricing_block.get("completion"))
        if completion_cost is not None:
            completion_cost *= scale
        currency = (pricing_block.get("currency") or "USD").upper()
        pricing = None
        if prompt_cost is not None or completion_cost is not None:
            pricing = {
                "prompt": prompt_cost,
                "completion": completion_cost,
                "currency": currency,
                "unit": unit,
            }
        models.append({"id": str(model_id), "label": str(label), "pricing": pricing})
    models.sort(key=lambda item: item["label"].lower())
    return models


def _get_openrouter_client(app) -> OpenRouter | None:
    client = getattr(app.state, _CLIENT_ATTR, None)
    if client:
        return client
    settings = get_settings()
    api_key = settings.openrouter_api_key
    if not api_key:
        return None
    client = OpenRouter(api_key=api_key)
    setattr(app.state, _CLIENT_ATTR, client)
    return client



async def list_openrouter_models(app) -> list[dict[str, Any]]:
    cache = getattr(app.state, _CACHE_ATTR, None)
    now = time.time()
    if cache and now - cache.get("timestamp", 0) < _CACHE_TTL_SECONDS:
        return cache.get("models", DEFAULT_MODEL_OPTIONS)

    settings = get_settings()
    api_key = settings.openrouter_api_key
    if not api_key:
        return DEFAULT_MODEL_OPTIONS

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Title": "tai2",
        "HTTP-Referer": "https://github.com/edfoo/tai2",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(_API_URL, headers=headers)
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:  # pragma: no cover - network dependency
        logger.warning("OpenRouter model listing failed: %s", exc)
        return DEFAULT_MODEL_OPTIONS

    data = payload.get("data")
    if not isinstance(data, list):
        return DEFAULT_MODEL_OPTIONS

    normalized = _normalize_models(data) or DEFAULT_MODEL_OPTIONS
    setattr(app.state, _CACHE_ATTR, {"timestamp": now, "models": normalized})
    return normalized


async def fetch_openrouter_credits(app, *, force_refresh: bool = False) -> dict[str, Any] | None:
    cache = getattr(app.state, _USAGE_CACHE_ATTR, None)
    now = time.time()
    if (not force_refresh) and cache and now - cache.get("timestamp", 0) < _USAGE_TTL_SECONDS:
        return cache.get("usage")

    client = _get_openrouter_client(app)
    if client is None:
        return None

    usage: dict[str, Any] | None = None
    try:
        response = await client.credits.get_credits_async()
        data = getattr(response, "data", None)
        if data is not None:
            granted = _safe_price(getattr(data, "total_credits", None)) or 0.0
            used = _safe_price(getattr(data, "total_usage", None)) or 0.0
            remaining = max(0.0, granted - used)
            usage = {
                "granted": granted,
                "used": used,
                "remaining": remaining,
                "currency": "USD",
                "unit": "USD",
                "resets_at": None,
            }
    except Exception as exc:  # pragma: no cover - SDK/network dependency
        logger.warning("OpenRouter usage lookup failed: %s", exc)
        usage = None

    setattr(app.state, _USAGE_CACHE_ATTR, {"timestamp": now, "usage": usage})
    return usage


__all__ = ["list_openrouter_models", "DEFAULT_MODEL_OPTIONS", "fetch_openrouter_credits"]
