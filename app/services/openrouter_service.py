from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

DEFAULT_MODEL_OPTIONS = [
    {"id": "openrouter/gpt-4o-mini", "label": "OpenAI GPT-4o mini"},
    {"id": "openrouter/gpt-4o", "label": "OpenAI GPT-4o"},
    {"id": "openrouter/claude-3.5", "label": "Anthropic Claude 3.5"},
]

_CACHE_ATTR = "openrouter_model_cache"
_CACHE_TTL_SECONDS = 3600
_API_URL = "https://openrouter.ai/api/v1/models"


def _normalize_models(data: list[Any]) -> list[dict[str, str]]:
    models: list[dict[str, str]] = []
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
        models.append({"id": str(model_id), "label": str(label)})
    models.sort(key=lambda item: item["label"].lower())
    return models


async def list_openrouter_models(app) -> list[dict[str, str]]:
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


__all__ = ["list_openrouter_models", "DEFAULT_MODEL_OPTIONS"]
