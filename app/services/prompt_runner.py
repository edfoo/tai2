from __future__ import annotations

import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.db.postgres import (
    fetch_equity_window,
    fetch_okx_fees_window,
    get_prompt_version,
    insert_prompt_run,
)
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


DAILY_LOSS_WINDOW_HOURS = 24.0


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_equity_value(entry: Any) -> Optional[float]:
    if not isinstance(entry, dict):
        return None
    for key in ("total_eq_usd", "account_equity", "total_account_value", "equity"):
        value = _to_float(entry.get(key))
        if value is not None and value > 0:
            return value
    return None


def compute_daily_loss_guard_state(
    *,
    limit_pct: float | None,
    window_hours: float,
    current_equity: float | None,
    history: list[dict[str, Any]] | None,
    current_timestamp: str | None = None,
) -> dict[str, Any]:
    threshold = _to_float(limit_pct) if limit_pct is not None else None
    status: dict[str, Any] = {
        "active": False,
        "threshold_pct": threshold,
        "window_hours": window_hours,
        "current_equity": current_equity,
        "reference_equity": None,
        "change_pct": None,
        "peak_drop_pct": None,
        "peak_equity": None,
        "observed_since": None,
        "latest_observed_at": current_timestamp,
        "history_samples": 0,
        "reason": None,
    }
    if threshold is None or threshold <= 0:
        status["reason"] = "daily loss limit disabled"
        return status
    if current_equity is None or current_equity <= 0:
        status["reason"] = "current equity unavailable"
        return status
    series: list[dict[str, Any]] = []
    for entry in history or []:
        value = _extract_equity_value(entry)
        if value is None or value <= 0:
            continue
        series.append(
            {
                "observed_at": entry.get("observed_at"),
                "equity": value,
            }
        )
    series.sort(key=lambda item: item.get("observed_at") or "")
    status["history_samples"] = len(series)
    if not series:
        status["reason"] = "insufficient equity history"
        return status
    last_entry = series[-1]
    if abs(last_entry["equity"] - current_equity) > 1e-9:
        series.append({"observed_at": current_timestamp, "equity": current_equity})
    reference_entry = series[0]
    reference_equity = reference_entry["equity"]
    if reference_equity <= 0:
        status["reason"] = "reference equity unavailable"
        return status
    status["reference_equity"] = reference_equity
    status["observed_since"] = reference_entry.get("observed_at")
    status["latest_observed_at"] = series[-1].get("observed_at") or current_timestamp
    status["history_samples"] = len(series)
    change_pct = (reference_equity - current_equity) / reference_equity
    status["change_pct"] = change_pct
    peak_equity = max(entry["equity"] for entry in series)
    status["peak_equity"] = peak_equity
    if peak_equity and peak_equity > 0:
        status["peak_drop_pct"] = (peak_equity - current_equity) / peak_equity
    if change_pct is not None and change_pct >= threshold:
        status["active"] = True
        status["reason"] = "daily loss limit reached"
    else:
        status["reason"] = "within daily loss limit"
    return status


async def _evaluate_daily_loss_guard(app: FastAPI, snapshot: dict[str, Any]) -> dict[str, Any]:
    runtime_meta = getattr(app.state, "runtime_config", {}) or {}
    guardrails = runtime_meta.get("guardrails") or {}
    limit_pct = guardrails.get("daily_loss_limit_pct")
    current_equity = _extract_equity_value(
        {
            "total_eq_usd": snapshot.get("total_eq_usd"),
            "account_equity": snapshot.get("account_equity"),
            "total_account_value": snapshot.get("total_account_value"),
        }
    )
    history_rows: list[dict[str, Any]] = []
    settings = get_settings()
    if settings.database_url:
        try:
            history_rows = await fetch_equity_window(DAILY_LOSS_WINDOW_HOURS)
        except Exception as exc:  # pragma: no cover - optional dependency
            logger.debug("Daily loss guard history fetch failed: %s", exc)
    status = compute_daily_loss_guard_state(
        limit_pct=limit_pct,
        window_hours=DAILY_LOSS_WINDOW_HOURS,
        current_equity=current_equity,
        history=history_rows,
        current_timestamp=snapshot.get("generated_at"),
    )
    runtime_meta.setdefault("risk_locks", {})["daily_loss"] = status
    return status


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


def _normalize_symbol(value: Any) -> str | None:
    if not value:
        return None
    try:
        text = str(value).strip().upper()
    except Exception:
        return None
    return text or None


def _symbol_has_open_position(snapshot: dict[str, Any], symbol: str | None) -> bool:
    if not symbol:
        return False
    positions = snapshot.get("positions") or []
    target = _normalize_symbol(symbol)
    if not target:
        return False
    for entry in positions:
        if not isinstance(entry, dict):
            continue
        entry_symbol = _normalize_symbol(entry.get("instId") or entry.get("symbol"))
        if entry_symbol != target:
            continue
        for key in ("pos", "posQty", "size", "position"):
            raw = entry.get(key)
            if raw is None:
                continue
            try:
                size_value = float(raw)
            except (TypeError, ValueError):
                continue
            if abs(size_value) > 0:
                return True
    return False


def _resolve_requested_symbol(snapshot: dict[str, Any], requested: str | None) -> str | None:
    if requested:
        return requested
    primary = snapshot.get("symbol")
    if primary:
        return primary
    symbols = snapshot.get("symbols") or []
    if symbols:
        return symbols[0]
    return None


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
    guardrails = runtime_meta.get("guardrails") or {}
    wait_for_tp_sl = guardrails.get("wait_for_tp_sl")
    if wait_for_tp_sl is None:
        wait_for_tp_sl = runtime_meta.get("wait_for_tp_sl", False)
    wait_for_tp_sl = bool(wait_for_tp_sl)
    resolved_symbol = _resolve_requested_symbol(snapshot, symbol)
    if wait_for_tp_sl and _symbol_has_open_position(snapshot, resolved_symbol):
        detail = (
            f"wait-for-tp-sl guard active; symbol {resolved_symbol} has an open position"
            if resolved_symbol
            else "wait-for-tp-sl guard active"
        )
        return None, _response(detail, 409)
    daily_loss_state = await _evaluate_daily_loss_guard(app, snapshot)
    if daily_loss_state.get("active"):
        drop_pct = daily_loss_state.get("change_pct")
        limit_pct = daily_loss_state.get("threshold_pct")
        drop_label = f"{drop_pct * 100:.2f}%" if drop_pct is not None else "limit"
        limit_label = f"{limit_pct * 100:.2f}%" if limit_pct is not None else "configured limit"
        detail = (
            f"daily loss limit triggered ({drop_label} drop in {int(DAILY_LOSS_WINDOW_HOURS)}h vs {limit_label}); "
            "execution locked"
        )
        return None, _response(detail, 423)
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
    metadata["risk_locks"] = runtime_meta.get("risk_locks")
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
    daily_loss_state = (runtime_meta.get("risk_locks") or {}).get("daily_loss") or {}
    if daily_loss_state.get("active"):
        logger.warning("Daily loss limit active; skipping execution layer dispatch")
        backend_events = getattr(app.state, "backend_events", None)
        if backend_events is not None:
            backend_events.append(
                "Daily loss limit active; new decisions blocked until equity recovers"
            )
        return decision, prompt_id
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
    "compute_daily_loss_guard_state",
    "execute_llm_decision",
    "persist_prompt_run",
    "prepare_prompt_payload",
    "resolve_prompt_metadata",
]
