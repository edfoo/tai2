import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
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
    load_guardrails,
    load_llm_model,
    load_execution_settings,
    load_okx_sub_account,
)
from app.services.llm_service import LLMService
from app.services.market_service import MarketService
from app.services.prompt_builder import DEFAULT_DECISION_PROMPT, DEFAULT_SYSTEM_PROMPT, PromptBuilder
from app.services.state_service import StateService, close_redis_client, ensure_redis_connection
from app.services.prompt_scheduler import PromptScheduler
from app.services.prompt_runner import (
    execute_llm_decision,
    persist_prompt_run,
    prepare_prompt_payload,
)
from app.ui.pages import register_pages

logger = logging.getLogger(__name__)


class BackendEventHandler(logging.Handler):
    """Mirror application logs into the Debug page backend log."""

    def __init__(self, sink):
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:  # pragma: no cover - defensive
            message = record.getMessage()
        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        timestamp = now_utc.isoformat().replace("+00:00", "Z")
        entry = {
            "timestamp": timestamp,
            "message": message,
            "level": (record.levelname or "INFO").lower(),
            "source": "backend",
        }
        try:
            self._sink(entry)
        except Exception:  # pragma: no cover - defensive
            pass


def _create_lifespan(enable_background_services: bool):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        trading_pairs = settings.trading_pairs
        app.state.state_service = None
        app.state.market_service = None
        app.state.prompt_scheduler = None
        app.state.backend_events = deque(maxlen=2000)
        app.state.frontend_events = deque(maxlen=1000)
        app.state.websocket_events = deque(maxlen=1000)
        app.state.backend_log_buffer = deque(maxlen=2000)
        app.state.frontend_log_buffer = deque(maxlen=1000)
        app.state.websocket_log_buffer = deque(maxlen=1000)
        backend_handler = BackendEventHandler(app.state.backend_events.append)
        backend_handler.setLevel(logging.INFO)
        backend_handler.setFormatter(logging.Formatter("%(message)s"))
        target_logger_names = {
            "app",
            "uvicorn",
            "uvicorn.error",
            "uvicorn.access",
            "uvicorn.asgi",
        }
        attached_loggers: list[logging.Logger] = []
        root_logger = logging.getLogger()
        if backend_handler not in root_logger.handlers:
            root_logger.addHandler(backend_handler)
            attached_loggers.append(root_logger)
        for name in target_logger_names:
            logger_ref = logging.getLogger(name)
            logger_ref.setLevel(logging.INFO)
            if backend_handler not in logger_ref.handlers:
                logger_ref.addHandler(backend_handler)
                attached_loggers.append(logger_ref)
        app.state.backend_log_handler = backend_handler
        app.state.backend_log_targets = attached_loggers
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
            "auto_prompt_enabled": False,
            "auto_prompt_interval": 300,
            "snapshot_max_age_seconds": settings.snapshot_max_age_seconds,
            "execution_enabled": False,
            "execution_trade_mode": "cross",
            "execution_order_type": "market",
            "execution_min_size": 1.0,
            "execution_min_sizes": {},
            "fee_window_hours": 24.0,
            "okx_sub_account": settings.okx_sub_account,
            "okx_sub_account_use_master": settings.okx_sub_account_use_master,
            "okx_api_flag": str(settings.okx_api_flag or "0") or "0",
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
                    stored_model = await load_llm_model(
                        app.state.runtime_config.get("llm_model_id")
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load LLM model preference: %s", exc)
                else:
                    if stored_model:
                        app.state.runtime_config["llm_model_id"] = stored_model
                        app.state.llm_service.set_model(stored_model)
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
                try:
                    stored_sub_account = await load_okx_sub_account(
                        {
                            "sub_account": app.state.runtime_config.get("okx_sub_account"),
                            "use_master": app.state.runtime_config.get(
                                "okx_sub_account_use_master",
                                False,
                            ),
                        }
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load OKX sub-account preference: %s", exc)
                else:
                    app.state.runtime_config["okx_sub_account"] = stored_sub_account.get("sub_account")
                    app.state.runtime_config["okx_sub_account_use_master"] = stored_sub_account.get(
                        "use_master",
                        app.state.runtime_config.get("okx_sub_account_use_master", False),
                    )
                    api_flag = stored_sub_account.get("api_flag")
                    if api_flag in {"0", "1"}:
                        app.state.runtime_config["okx_api_flag"] = api_flag
                try:
                    execution_settings = await load_execution_settings()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load execution settings: %s", exc)
                else:
                    if execution_settings:
                        app.state.runtime_config["execution_enabled"] = execution_settings.get(
                            "enabled",
                            app.state.runtime_config.get("execution_enabled"),
                        )
                        app.state.runtime_config["execution_trade_mode"] = execution_settings.get(
                            "trade_mode",
                            app.state.runtime_config.get("execution_trade_mode"),
                        )
                        app.state.runtime_config["execution_order_type"] = execution_settings.get(
                            "order_type",
                            app.state.runtime_config.get("execution_order_type"),
                        )
                        min_size = execution_settings.get("min_size")
                        if min_size is not None:
                            app.state.runtime_config["execution_min_size"] = float(min_size)
                        min_sizes = execution_settings.get("min_sizes")
                        if isinstance(min_sizes, dict):
                            app.state.runtime_config["execution_min_sizes"] = min_sizes
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
                    sub_account=app.state.runtime_config.get("okx_sub_account"),
                    sub_account_use_master=app.state.runtime_config.get(
                        "okx_sub_account_use_master",
                        False,
                    ),
                    okx_flag=app.state.runtime_config.get("okx_api_flag"),
                )
                app.state.market_service = market_service
                await market_service.start()
                scheduler = PromptScheduler(
                    app,
                    default_interval=app.state.runtime_config.get("auto_prompt_interval", 300),
                )
                app.state.prompt_scheduler = scheduler
                if app.state.runtime_config.get("auto_prompt_enabled"):
                    await scheduler.start()
        elif not enable_background_services:
            logger.info("Background Redis init disabled; skipping state service")
        else:
            logger.info("REDIS_URL not configured; skipping Redis init")

        try:
            yield
        finally:
            handler = getattr(app.state, "backend_log_handler", None)
            if handler:
                for logger_ref in getattr(app.state, "backend_log_targets", []):
                    try:
                        logger_ref.removeHandler(handler)
                    except (ValueError, AttributeError):
                        continue
                handler.close()
            scheduler = getattr(app.state, "prompt_scheduler", None)
            if scheduler:
                await scheduler.stop()
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
        bundle, error_response = await prepare_prompt_payload(
            app,
            symbol=symbol,
            timeframe=timeframe,
            prompt_version_id=prompt_version_id,
        )
        if error_response:
            return error_response
        assert bundle is not None  # for type-checkers
        prompt_id = await persist_prompt_run(app, bundle)
        return JSONResponse({"payload": bundle.payload, "prompt_id": prompt_id}, status_code=200)

    @app.post("/llm/execute")
    async def llm_execute(
        symbol: str | None = None,
        timeframe: str | None = None,
        prompt_version_id: str | None = None,
    ) -> JSONResponse:
        bundle, error_response = await prepare_prompt_payload(
            app,
            symbol=symbol,
            timeframe=timeframe,
            prompt_version_id=prompt_version_id,
        )
        if error_response:
            return error_response
        assert bundle is not None  # for type-checkers
        decision, prompt_id = await execute_llm_decision(app, bundle)
        return JSONResponse(
            {"payload": bundle.payload, "decision": decision, "prompt_id": prompt_id},
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

    def _record_websocket_event(message: str, snapshot: dict[str, Any] | None = None) -> None:
        events = getattr(app.state, "websocket_events", None)
        if events is None:
            return
        entry = {
            "message": message,
            "symbol": (snapshot or {}).get("symbol"),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "websocket",
        }
        events.append(entry)

    @app.websocket("/ws/state")
    async def state_stream(ws: WebSocket) -> None:
        state_service = app.state.state_service
        if not state_service:
            await ws.close(code=1013)
            return
        await ws.accept()
        _record_websocket_event("websocket client connected")
        initial = await state_service.get_market_snapshot()
        if initial:
            await ws.send_json(initial)
            _record_websocket_event("initial snapshot delivered", initial)
        else:
            _record_websocket_event("snapshot unavailable for websocket client")
        try:
            async for snapshot in state_service.subscribe_snapshots():
                await ws.send_json(snapshot)
                _record_websocket_event("snapshot broadcast", snapshot)
        except WebSocketDisconnect:
            _record_websocket_event("client disconnected from websocket")
            return
        except Exception as exc:
            _record_websocket_event(f"websocket stream error: {exc}")
            raise

    register_pages(app)
    ui.run_with(app)
    return app


app = create_app()
