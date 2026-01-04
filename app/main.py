import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from nicegui import ui

from app.core.config import get_settings
from app.db.postgres import (
    close_postgres_pool,
    fetch_equity_history,
    fetch_recent_trades,
    fetch_prompt_versions,
    fetch_trading_pairs,
    init_postgres_pool,
    insert_prompt_run,
    load_guardrails,
    get_prompt_version,
)
from app.services.llm_service import LLMService
from app.services.market_service import MarketService
from app.services.prompt_builder import DEFAULT_DECISION_PROMPT, DEFAULT_SYSTEM_PROMPT, PromptBuilder
from app.services.state_service import StateService, close_redis_client, ensure_redis_connection
from app.ui.pages import register_pages

logger = logging.getLogger(__name__)


def _create_lifespan(enable_background_services: bool):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        trading_pairs = settings.trading_pairs
        app.state.state_service = None
        app.state.market_service = None
        app.state.backend_events = deque(maxlen=200)
        app.state.frontend_events = deque(maxlen=200)
        app.state.runtime_config = {
            "ws_update_interval": settings.ws_update_interval,
            "llm_system_prompt": DEFAULT_SYSTEM_PROMPT,
            "llm_decision_prompt": DEFAULT_DECISION_PROMPT,
            "llm_model_id": "openrouter/gpt-4o-mini",
            "trading_pairs": trading_pairs,
            "ta_timeframe": MarketService.DEFAULT_TIMEFRAME,
            "llm_response_schemas": {},
            "guardrails": PromptBuilder._default_guardrails(),
            "prompt_version_id": None,
            "prompt_version_name": None,
        }
        app.state.llm_service = LLMService(model_id=app.state.runtime_config["llm_model_id"])

        if enable_background_services and settings.database_url:
            try:
                await init_postgres_pool()
            except Exception as exc:  # pragma: no cover - requires DB
                logger.error("Failed to initialize PostgreSQL pool: %s", exc)
            else:
                try:
                    stored_pairs = await fetch_trading_pairs()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to fetch stored trading pairs: %s", exc)
                else:
                    if stored_pairs:
                        trading_pairs = [row["symbol"] for row in stored_pairs if row.get("enabled", True)] or trading_pairs
                        app.state.runtime_config["trading_pairs"] = trading_pairs
                try:
                    stored_guardrails = await load_guardrails()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load guardrails: %s", exc)
                else:
                    if stored_guardrails:
                        app.state.runtime_config["guardrails"] = stored_guardrails
                try:
                    prompt_versions = await fetch_prompt_versions(limit=1)
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load prompt versions: %s", exc)
                else:
                    if prompt_versions:
                        latest_version = prompt_versions[0]
                        app.state.runtime_config["prompt_version_id"] = latest_version["id"]
                        app.state.runtime_config["llm_system_prompt"] = latest_version["system_prompt"]
                        app.state.runtime_config["llm_decision_prompt"] = latest_version["decision_prompt"]
                        app.state.runtime_config["prompt_version_name"] = latest_version["name"]
        elif not enable_background_services:
            logger.info("Background DB init disabled; skipping Postgres init")
        else:
            logger.info("DATABASE_URL not configured; skipping Postgres init")

        if enable_background_services and settings.redis_url:
            try:
                await ensure_redis_connection()
            except Exception as exc:  # pragma: no cover - requires Redis
                logger.error("Failed to connect to Redis: %s", exc)
            else:
                state_service = StateService()
                app.state.state_service = state_service
                market_service = MarketService(
                    state_service=state_service,
                    symbols=trading_pairs,
                    log_sink=lambda msg: app.state.backend_events.append(msg),
                    ohlc_bar=app.state.runtime_config.get("ta_timeframe"),
                )
                app.state.market_service = market_service
                await market_service.start()
        elif not enable_background_services:
            logger.info("Background Redis init disabled; skipping state service")
        else:
            logger.info("REDIS_URL not configured; skipping Redis init")

        try:
            yield
        finally:
            if app.state.market_service:
                await app.state.market_service.stop()
            await close_postgres_pool()
            await close_redis_client()

    return lifespan


def create_app(enable_background_services: bool | None = None) -> FastAPI:
    settings = get_settings()
    if enable_background_services is None:
        enable_background_services = os.environ.get("PYTEST_CURRENT_TEST") is None
    app = FastAPI(title="tai2", version="0.1.0", lifespan=_create_lifespan(enable_background_services))

    async def resolve_prompt_metadata(
        requested_version_id: str | None,
    ) -> tuple[dict[str, Any], JSONResponse | None]:
        runtime_meta = getattr(app.state, "runtime_config", {}) or {}
        metadata = dict(runtime_meta)
        if requested_version_id:
            if not settings.database_url:
                return metadata, JSONResponse(
                    {"detail": "prompt version storage unavailable"}, status_code=503
                )
            try:
                version_record = await get_prompt_version(requested_version_id)
            except Exception as exc:  # pragma: no cover - db optional
                logger.error("Failed to load prompt version %s: %s", requested_version_id, exc)
                return metadata, JSONResponse(
                    {"detail": "prompt version lookup failed"}, status_code=500
                )
            if not version_record:
                return metadata, JSONResponse(
                    {"detail": "prompt version not found"}, status_code=404
                )
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

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "ws_interval": str(settings.ws_update_interval)}

    @app.get("/state/latest")
    async def latest_state() -> JSONResponse:
        state_service = app.state.state_service
        if not state_service:
            return JSONResponse({"detail": "state service unavailable"}, status_code=503)
        snapshot = await state_service.get_market_snapshot()
        if not snapshot:
            return JSONResponse({"detail": "snapshot unavailable"}, status_code=503)
        return JSONResponse(snapshot, status_code=200)

    @app.get("/trades/recent")
    async def recent_trades(limit: int = 100) -> JSONResponse:
        try:
            items = await fetch_recent_trades(limit)
        except Exception as exc:
            logger.error("Failed to fetch trades: %s", exc)
            return JSONResponse({"detail": "trades unavailable"}, status_code=503)
        return JSONResponse({"items": items}, status_code=200)

    @app.get("/llm/prompt")
    async def llm_prompt(
        symbol: str | None = None,
        timeframe: str | None = None,
        prompt_version_id: str | None = None,
    ) -> JSONResponse:
        state_service: StateService | None = app.state.state_service
        if not state_service:
            return JSONResponse({"detail": "state service unavailable"}, status_code=503)
        snapshot = await state_service.get_market_snapshot()
        if not snapshot:
            return JSONResponse({"detail": "snapshot unavailable"}, status_code=503)
        runtime_meta = getattr(app.state, "runtime_config", {}) or {}
        metadata, error_response = await resolve_prompt_metadata(prompt_version_id)
        if error_response:
            return error_response
        builder = PromptBuilder(snapshot, metadata=metadata)
        payload = builder.build(symbol=symbol, timeframe=timeframe)
        prompt_id: str | None = None
        if settings.database_url:
            try:
                context_block = payload.get("context") or {}
                prompt_id = await insert_prompt_run(
                    symbol=context_block.get("symbol")
                    or symbol
                    or snapshot.get("symbol")
                    or "BTC-USDT-SWAP",
                    timeframe=context_block.get("timeframe"),
                    model_id=runtime_meta.get("llm_model_id"),
                    guardrails=context_block.get("guardrails"),
                    payload=payload,
                    notes=runtime_meta.get("llm_notes"),
                    prompt_version_id=metadata.get("prompt_version_id"),
                )
            except Exception as exc:  # pragma: no cover - best effort persistence
                logger.debug("Failed to persist LLM prompt: %s", exc)
        return JSONResponse({"payload": payload, "prompt_id": prompt_id}, status_code=200)

    @app.post("/llm/execute")
    async def llm_execute(
        symbol: str | None = None,
        timeframe: str | None = None,
        prompt_version_id: str | None = None,
    ) -> JSONResponse:
        state_service: StateService | None = app.state.state_service
        if not state_service:
            return JSONResponse({"detail": "state service unavailable"}, status_code=503)
        snapshot = await state_service.get_market_snapshot()
        if not snapshot:
            return JSONResponse({"detail": "snapshot unavailable"}, status_code=503)
        runtime_meta = getattr(app.state, "runtime_config", {}) or {}
        metadata, error_response = await resolve_prompt_metadata(prompt_version_id)
        if error_response:
            return error_response
        builder = PromptBuilder(snapshot, metadata=metadata)
        payload = builder.build(symbol=symbol, timeframe=timeframe)
        llm_service = getattr(app.state, "llm_service", None)
        if llm_service is None:
            llm_service = LLMService(model_id=runtime_meta.get("llm_model_id"))
            app.state.llm_service = llm_service
        decision = await llm_service.run(payload)
        prompt_id: str | None = None
        if settings.database_url:
            try:
                context_block = payload.get("context") or {}
                prompt_id = await insert_prompt_run(
                    symbol=context_block.get("symbol")
                    or symbol
                    or snapshot.get("symbol")
                    or "BTC-USDT-SWAP",
                    timeframe=context_block.get("timeframe"),
                    model_id=runtime_meta.get("llm_model_id"),
                    guardrails=context_block.get("guardrails"),
                    payload=payload,
                    notes=runtime_meta.get("llm_notes"),
                    decision=decision,
                    prompt_version_id=metadata.get("prompt_version_id"),
                )
            except Exception as exc:  # pragma: no cover - best effort persistence
                logger.debug("Failed to persist LLM decision: %s", exc)
        market_service: MarketService | None = app.state.market_service
        if market_service:
            await market_service.handle_llm_decision(decision, payload.get("context"))
        version_label = metadata.get("prompt_version_name") or metadata.get("prompt_version_id") or "default"
        decision_summary = f"LLM decision ({version_label}) action={decision.get('action')} conf={decision.get('confidence', '--')}"
        app.state.backend_events.append(decision_summary)
        return JSONResponse(
            {"payload": payload, "decision": decision, "prompt_id": prompt_id},
            status_code=200,
        )

    @app.get("/equity/history")
    async def equity_history(limit: int = 200) -> JSONResponse:
        try:
            items = await fetch_equity_history(limit)
        except Exception as exc:
            logger.error("Failed to fetch equity history: %s", exc)
            return JSONResponse({"detail": "equity history unavailable"}, status_code=503)
        return JSONResponse({"items": items}, status_code=200)

    @app.get("/config/trading-pairs")
    async def trading_pairs() -> JSONResponse:
        market_service: MarketService | None = app.state.market_service
        if not market_service:
            return JSONResponse({"pairs": settings.trading_pairs}, status_code=200)
        try:
            pairs = await market_service.list_available_symbols()
        except Exception as exc:  # pragma: no cover - network dependency
            logger.error("Failed to load trading pairs: %s", exc)
            fallback = app.state.runtime_config.get("trading_pairs") if hasattr(app.state, "runtime_config") else None
            pairs = fallback or settings.trading_pairs
        return JSONResponse({"pairs": pairs}, status_code=200)

    @app.websocket("/ws/state")
    async def state_stream(ws: WebSocket) -> None:
        state_service = app.state.state_service
        if not state_service:
            await ws.close(code=1013)
            return
        await ws.accept()
        initial = await state_service.get_market_snapshot()
        if initial:
            await ws.send_json(initial)
        try:
            async for snapshot in state_service.subscribe_snapshots():
                await ws.send_json(snapshot)
        except WebSocketDisconnect:
            return

    register_pages(app)
    ui.run_with(app)
    return app


app = create_app()
