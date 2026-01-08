from __future__ import annotations

import logging
import time
from typing import Any

import httpx

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
_USAGE_URL = "https://openrouter.ai/api/v1/credits"
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


def _candidate_usage_blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return blocks
    for key in ("credits", "balance", "quota", "usage", "limits", "data"):
        block = payload.get(key)
        if isinstance(block, dict):
            blocks.append(block)
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if isinstance(meta, dict):
        for key in ("credits", "balance", "quota"):
            block = meta.get(key)
            if isinstance(block, dict):
                blocks.append(block)
    return blocks


def _normalize_usage_fields(block: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(block, dict):
        return None

    def _value(*keys: str) -> float | None:
        for key in keys:
            if key in block:
                val = _safe_price(block.get(key))
                if val is not None:
                    return val
        return None

    remaining = _value(
        "remaining",
        "available",
        "balance",
        "credits_remaining",
        "usd_remaining",
        "total_credits",
    )
    granted = _value("granted", "total", "limit", "quota", "credits_granted", "total_granted")
    used = _value("used", "spent", "consumed", "usd_used", "total_used", "total_usage")
    if remaining is None and granted is None and used is None:
        return None

    currency_raw = block.get("currency") or block.get("denomination")
    currency = str(currency_raw).upper() if isinstance(currency_raw, str) else "USD"
    unit = block.get("unit") or ("USD" if currency == "USD" else currency)
    resets_at = (
        block.get("resets_at")
        or block.get("reset_at")
        or block.get("renews_at")
        or block.get("period_end")
    )

    return {
        "remaining": remaining,
        "granted": granted,
        "used": used,
        "currency": currency,
        "unit": unit,
        "resets_at": resets_at,
        "raw": block,
    }


def _extract_usage_snapshot(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    for block in _candidate_usage_blocks(payload):
        normalized = _normalize_usage_fields(block)
        if normalized:
            return normalized
    return _normalize_usage_fields(payload)


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


async def fetch_openrouter_credits(app) -> dict[str, Any] | None:
    cache = getattr(app.state, _USAGE_CACHE_ATTR, None)
    now = time.time()
    if cache and now - cache.get("timestamp", 0) < _USAGE_TTL_SECONDS:
        return cache.get("usage")

    settings = get_settings()
    api_key = settings.openrouter_api_key
    if not api_key:
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "X-Title": "tai2",
        "HTTP-Referer": "https://github.com/edfoo/tai2",
    }
    usage: dict[str, Any] | None = None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(_USAGE_URL, headers=headers)
            response.raise_for_status()
            if not response.content or not response.text.strip():
                payload: dict[str, Any] | None = None
            else:
                try:
                    payload = response.json()
                except ValueError as exc:
                    logger.debug("OpenRouter usage payload not JSON: %s", exc)
                    payload = None
            usage = _extract_usage_snapshot(payload) if payload else None
    except Exception as exc:  # pragma: no cover - network dependency
        logger.warning("OpenRouter usage lookup failed: %s", exc)
        usage = None

    setattr(app.state, _USAGE_CACHE_ATTR, {"timestamp": now, "usage": usage})
    return usage


__all__ = ["list_openrouter_models", "DEFAULT_MODEL_OPTIONS", "fetch_openrouter_credits"]
