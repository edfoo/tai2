from __future__ import annotations

import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.db.postgres import fetch_okx_fees_window, get_prompt_version, insert_prompt_run
from app.services.llm_service import LLMService
from app.services.prompt_builder import PromptBuilder
from app.services.openrouter_service import fetch_openrouter_credits

logger = logging.getLogger(__name__)


@dataclass
class PromptPayloadBundle:
    payload: dict[str, Any]
    runtime_meta: dict[str, Any]
    metadata: dict[str, Any]
    snapshot: dict[str, Any]


def _response(message: str, status_code: int) -> JSONResponse:
    return JSONResponse({"detail": message}, status_code=status_code)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    except (ValueError, AttributeError):
        return None


def _snapshot_is_stale(snapshot: dict[str, Any], max_age_seconds: int) -> bool:
    timestamp = _parse_timestamp(snapshot.get("generated_at"))
    if timestamp is None:
        return True
    now = datetime.now(timezone.utc)
    delta = now - timestamp
    if delta.total_seconds() < 0:
        return False
    return delta.total_seconds() > max(0, max_age_seconds)


async def resolve_prompt_metadata(
    runtime_meta: dict[str, Any], requested_version_id: str | None
) -> Tuple[dict[str, Any], Optional[JSONResponse]]:
    settings = get_settings()
    metadata = dict(runtime_meta)
    metadata.setdefault("snapshot_max_age_seconds", runtime_meta.get("snapshot_max_age_seconds"))
    if requested_version_id:
        if not settings.database_url:
            return metadata, _response("prompt version storage unavailable", 503)
        try:
            version_record = await get_prompt_version(requested_version_id)
        except Exception as exc:  # pragma: no cover - DB optional
            logger.error("Failed to load prompt version %s: %s", requested_version_id, exc)
            return metadata, _response("prompt version lookup failed", 500)
        if not version_record:
            return metadata, _response("prompt version not found", 404)
        metadata["llm_system_prompt"] = version_record.get(
            "system_prompt", metadata.get("llm_system_prompt")
        )
        metadata["llm_decision_prompt"] = version_record.get(
            "decision_prompt", metadata.get("llm_decision_prompt")
        )
        metadata["prompt_version_id"] = version_record.get("id")
        metadata["prompt_version_name"] = version_record.get("name")
    else:
        metadata.setdefault("prompt_version_id", runtime_meta.get("prompt_version_id"))
        metadata.setdefault("prompt_version_name", runtime_meta.get("prompt_version_name"))
    return metadata, None


async def prepare_prompt_payload(
    app: FastAPI,
    *,
    symbol: str | None = None,
    timeframe: str | None = None,
    prompt_version_id: str | None = None,
) -> Tuple[Optional[PromptPayloadBundle], Optional[JSONResponse]]:
    state_service = getattr(app.state, "state_service", None)
    if not state_service:
        return None, _response("state service unavailable", 503)
    snapshot = await state_service.get_market_snapshot()
    if not snapshot:
        return None, _response("snapshot unavailable", 503)
    runtime_meta = getattr(app.state, "runtime_config", {}) or {}
    max_age = int(
        runtime_meta.get("snapshot_max_age_seconds")
        or get_settings().snapshot_max_age_seconds
    )
    if _snapshot_is_stale(snapshot, max_age):
        return None, _response("snapshot stale; awaiting refresh", 503)
    metadata, error_response = await resolve_prompt_metadata(runtime_meta, prompt_version_id)
    if error_response:
        return None, error_response
    credits_task = asyncio.create_task(fetch_openrouter_credits(app))
    fee_window_hours = float(runtime_meta.get("fee_window_hours") or 24.0)
    fee_task = None
    settings = get_settings()
    if settings.database_url:
        fee_task = asyncio.create_task(fetch_okx_fees_window(fee_window_hours))
    openrouter_usage: dict[str, Any] | None = None
    okx_fee_total: float | None = None
    try:
        openrouter_usage = await credits_task
    except Exception as exc:  # pragma: no cover - best effort
        logger.debug("OpenRouter usage enrichment failed: %s", exc)
    if fee_task:
        try:
            okx_fee_total = await fee_task
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("OKX fee enrichment failed: %s", exc)
    metadata["openrouter_usage"] = openrouter_usage
    metadata["fee_window_hours"] = fee_window_hours
    metadata["okx_fee_window_total"] = okx_fee_total
    builder = PromptBuilder(snapshot, metadata=metadata)
    payload = builder.build(symbol=symbol, timeframe=timeframe)
    return PromptPayloadBundle(payload=payload, runtime_meta=runtime_meta, metadata=metadata, snapshot=snapshot), None


async def persist_prompt_run(
    app: FastAPI,
    bundle: PromptPayloadBundle,
    *,
    decision: dict[str, Any] | None = None,
) -> str | None:
    settings = get_settings()
    if not settings.database_url:
        return None
    context_block = bundle.payload.get("context") or {}
    runtime_meta = bundle.runtime_meta
    model_id = bundle.metadata.get("llm_model_id") or runtime_meta.get("llm_model_id")
    try:
        prompt_id = await insert_prompt_run(
            symbol=context_block.get("symbol")
            or bundle.snapshot.get("symbol")
            or "BTC-USDT-SWAP",
            timeframe=context_block.get("timeframe"),
            model_id=model_id,
            guardrails=context_block.get("guardrails"),
            payload=bundle.payload,
            notes=runtime_meta.get("llm_notes"),
            decision=decision,
            prompt_version_id=bundle.metadata.get("prompt_version_id"),
        )
        return prompt_id
    except Exception as exc:  # pragma: no cover - best effort persistence
        logger.debug("Failed to persist LLM prompt: %s", exc)
        return None


async def execute_llm_decision(
    app: FastAPI,
    bundle: PromptPayloadBundle,
) -> Tuple[dict[str, Any], str | None]:
    runtime_meta = bundle.runtime_meta
    fallback_orders_enabled = bool(runtime_meta.get("fallback_orders_enabled", True))
    llm_service = getattr(app.state, "llm_service", None)
    if llm_service is None:
        llm_service = LLMService(model_id=runtime_meta.get("llm_model_id"))
        app.state.llm_service = llm_service
    decision = await llm_service.run(bundle.payload)
    prompt_id = await persist_prompt_run(app, bundle, decision=decision)
    market_service = getattr(app.state, "market_service", None)
    state_changed = False
    decision_is_fallback = bool((decision or {}).get("_decision_origin") == "fallback")
    fallback_blocked = decision_is_fallback and not fallback_orders_enabled
    if fallback_blocked:
        logger.info(
            "Fallback decision suppressed by configuration; action=%s",
            decision.get("action"),
        )
    if market_service and not fallback_blocked:
        context_block = bundle.payload.get("context") or {}
        try:
            state_changed = await market_service.handle_llm_decision(decision, context_block)
        except Exception as exc:  # pragma: no cover - execution path depends on adapters
            logger.error("Market service failed to apply decision: %s", exc)
        else:
            if state_changed:
                reason_symbol = str(context_block.get("symbol") or decision.get("symbol") or "*")
                try:
                    await market_service.refresh_snapshot(
                        reason=f"post-decision:{reason_symbol}"
                    )
                except Exception as exc:  # pragma: no cover - best-effort snapshot refresh
                    logger.debug("Snapshot refresh after execution failed: %s", exc)
    _record_llm_interaction(app, bundle, decision)
    version_label = (
        bundle.metadata.get("prompt_version_name")
        or bundle.metadata.get("prompt_version_id")
        or "default"
    )
    backend_events = getattr(app.state, "backend_events", None)
    if backend_events is not None:
        backend_events.append(
            f"LLM decision ({version_label}) action={decision.get('action')} conf={decision.get('confidence', '--')}"
        )
    return decision, prompt_id


def _record_llm_interaction(
    app: FastAPI,
    bundle: PromptPayloadBundle,
    decision: dict[str, Any],
) -> None:
    interactions = getattr(app.state, "llm_interactions", None)
    if interactions is None:
        interactions = {}
        app.state.llm_interactions = interactions
    context = bundle.payload.get("context") or {}
    symbol = str(context.get("symbol") or decision.get("symbol") or "--").upper()
    schema = bundle.payload.get("prompt", {}).get("response_schema") or {}
    overview: list[str] = []
    properties = schema.get("properties") or {}
    for name, meta in list(properties.items())[:4]:
        desc = meta.get("description") or "No description provided."
        overview.append(f"{name}: {desc}")
    extra = max(0, len(properties) - 4)
    if extra:
        overview.append(f"+{extra} additional fields …")
    interactions[symbol] = {
        "symbol": symbol,
        "timestamp": context.get("generated_at")
        or datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "response_schema": schema,
        "schema_overview": overview,
    }


__all__ = [
    "PromptPayloadBundle",
    "execute_llm_decision",
    "persist_prompt_run",
    "prepare_prompt_payload",
    "resolve_prompt_metadata",
]
