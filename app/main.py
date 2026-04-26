import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
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
    load_screener_config,
    load_strategy_config,
    load_governor_config,
    load_ta_timeframe,
    load_llm_model,
    load_execution_settings,
    load_prompt_interval,
    load_poll_interval,
    load_okx_sub_account,
    load_frontend_timezone,
    load_candle_settings,
)
from app.services.llm_service import LLMService
from app.services.market_service import MarketService
from app.services.prompt_builder import (
    DEFAULT_DECISION_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    PromptBuilder,
    assemble_decision_prompt,
)
from app.services.prompt_utils import sanitize_prompt_text
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


class _UTCFormatter(logging.Formatter):
    converter = time.gmtime


class LogLinesHandler(logging.Handler):
    """Append fully-formatted log lines to an in-memory deque for the Debug page."""

    def __init__(self, lines: deque) -> None:
        super().__init__()
        self._lines = lines

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._lines.append(self.format(record))
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
        app.state.log_lines = deque(maxlen=5000)

        data_log_markers = (
            "api/v5/account/balance",
            "api/v5/market/candles",
            "api/v5/rubik/stat/contracts/long-short-account-ratio",
            "api/v5/trade/orders-algo-pending",
            "openrouter.ai/api/v1/credits",
        )

        def _route_backend_log(entry: dict[str, Any]) -> None:
            message = str(entry.get("message") or "")
            lowered = message.lower()
            if any(marker in lowered for marker in data_log_markers):
                frontend_entry = dict(entry)
                frontend_entry["source"] = "frontend"
                try:
                    app.state.frontend_events.append(frontend_entry)
                except Exception:  # pragma: no cover - defensive
                    pass
                return
            try:
                app.state.backend_events.append(entry)
            except Exception:  # pragma: no cover - defensive
                pass

        backend_handler = BackendEventHandler(_route_backend_log)
        backend_handler.setLevel(logging.DEBUG)
        backend_handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
        logger_levels = {
            "app": logging.DEBUG,
            "app.services.market_service": logging.DEBUG,
            "app.services.prompt_scheduler": logging.DEBUG,
            "uvicorn": logging.INFO,
            "uvicorn.error": logging.INFO,
            "uvicorn.access": logging.INFO,
            "uvicorn.asgi": logging.INFO,
            "httpx": logging.WARNING,
            "websockets": logging.WARNING,
            "websockets.client": logging.WARNING,
            "okx.websocket": logging.WARNING,
        }
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        utc_formatter = _UTCFormatter("%(asctime)s UTC · %(levelname)s:%(name)s:%(message)s")
        for handler in list(root_logger.handlers):
            try:
                handler.setFormatter(utc_formatter)
            except Exception:  # pragma: no cover - defensive
                continue
        attached_loggers: list[logging.Logger] = []
        if backend_handler not in root_logger.handlers:
            root_logger.addHandler(backend_handler)
            attached_loggers.append(root_logger)
        # In-memory log lines — populated at emit time, always current
        _lines_handler = LogLinesHandler(app.state.log_lines)
        _lines_handler.setLevel(logging.DEBUG)
        _lines_handler.setFormatter(utc_formatter)
        root_logger.addHandler(_lines_handler)
        app.state.log_lines_handler = _lines_handler
        # Rotating file handler — persistent logs across restarts
        try:
            _log_dir = Path("logs")
            _log_dir.mkdir(exist_ok=True)
            _file_handler = RotatingFileHandler(
                _log_dir / "app.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            _file_handler.setLevel(logging.DEBUG)
            _file_handler.setFormatter(utc_formatter)
            root_logger.addHandler(_file_handler)
            app.state.log_file_handler = _file_handler
            app.state.log_file_path = str(_log_dir / "app.log")
        except Exception as _exc:
            logger.warning("Failed to set up rotating log file: %s", _exc)
            app.state.log_file_handler = None
            app.state.log_file_path = None
        for name, level in logger_levels.items():
            logging.getLogger(name).setLevel(level)
        app.state.backend_log_handler = backend_handler
        app.state.backend_log_targets = attached_loggers
        app.state.runtime_config = {
            "poll_interval": settings.poll_interval,
            "enable_websocket": True,
            "llm_system_prompt": sanitize_prompt_text(DEFAULT_SYSTEM_PROMPT),
            "llm_decision_prompt": sanitize_prompt_text(DEFAULT_DECISION_PROMPT),
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
            "fallback_orders_enabled": settings.allow_fallback_orders,
            "fee_window_hours": 24.0,
            "okx_sub_account": settings.okx_sub_account,
            "okx_sub_account_use_master": settings.okx_sub_account_use_master,
            "okx_api_flag": str(settings.okx_api_flag or "0") or "0",
            "wait_for_tp_sl": False,
            "frontend_timezone": "UTC",
            "risk_locks": {},
            "ohlcv_fetch_limit": 200,
            "ohlcv_snapshot_candles": 96,
            "ohlcv_snapshot_htf_candles": 48,
            "governor": {},
        }
        app.state.runtime_config["wait_for_tp_sl"] = bool(
            app.state.runtime_config["guardrails"].get("wait_for_tp_sl", False)
        )
        app.state.runtime_config["fallback_orders_enabled"] = bool(
            app.state.runtime_config["guardrails"].get(
                "fallback_orders_enabled",
                app.state.runtime_config.get("fallback_orders_enabled", True),
            )
        )
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
                        app.state.runtime_config["wait_for_tp_sl"] = bool(
                            stored_guardrails.get("wait_for_tp_sl", False)
                        )
                        app.state.runtime_config["fallback_orders_enabled"] = bool(
                            stored_guardrails.get(
                                "fallback_orders_enabled",
                                app.state.runtime_config.get("fallback_orders_enabled", True),
                            )
                        )
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
                        app.state.runtime_config["llm_system_prompt"] = (
                            sanitize_prompt_text(latest_version.get("system_prompt"))
                            or sanitize_prompt_text(DEFAULT_SYSTEM_PROMPT)
                        )
                        app.state.runtime_config["llm_decision_prompt"] = (
                            sanitize_prompt_text(latest_version.get("decision_prompt"))
                            or sanitize_prompt_text(DEFAULT_DECISION_PROMPT)
                        )
                        app.state.runtime_config["prompt_version_name"] = latest_version["name"]
                        _v_meta = latest_version.get("metadata") or {}
                        _v_sections = _v_meta.get("prompt_sections")
                        if _v_sections and isinstance(_v_sections, dict):
                            app.state.runtime_config["prompt_sections"] = _v_sections
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
                try:
                    stored_timeframe = await load_ta_timeframe(
                        app.state.runtime_config.get("ta_timeframe")
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load TA timeframe: %s", exc)
                else:
                    if stored_timeframe:
                        app.state.runtime_config["ta_timeframe"] = stored_timeframe
                try:
                    stored_poll_interval = await load_poll_interval(
                        app.state.runtime_config.get("poll_interval")
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load poll interval: %s", exc)
                else:
                    if stored_poll_interval:
                        app.state.runtime_config["poll_interval"] = int(stored_poll_interval)
                try:
                    stored_prompt_interval = await load_prompt_interval(
                        app.state.runtime_config.get("auto_prompt_interval")
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load prompt interval: %s", exc)
                else:
                    if stored_prompt_interval:
                        app.state.runtime_config["auto_prompt_interval"] = int(stored_prompt_interval)
                try:
                    stored_timezone = await load_frontend_timezone(
                        app.state.runtime_config.get("frontend_timezone")
                    )
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load frontend timezone: %s", exc)
                else:
                    if stored_timezone:
                        app.state.runtime_config["frontend_timezone"] = stored_timezone
                try:
                    stored_screener = await load_screener_config()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load screener config: %s", exc)
                else:
                    if stored_screener:
                        app.state.runtime_config["screener"] = stored_screener
                try:
                    stored_strategy = await load_strategy_config()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load strategy config: %s", exc)
                else:
                    if stored_strategy:
                        app.state.runtime_config["strategy"] = stored_strategy
                try:
                    stored_governor = await load_governor_config()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load governor config: %s", exc)
                else:
                    if stored_governor:
                        app.state.runtime_config["governor"] = stored_governor
                try:
                    stored_candle_settings = await load_candle_settings()
                except Exception as exc:  # pragma: no cover - optional
                    logger.error("Failed to load candle settings: %s", exc)
                else:
                    if stored_candle_settings:
                        for _k, _default in [("ohlcv_fetch_limit", 200), ("ohlcv_snapshot_candles", 50), ("ohlcv_snapshot_htf_candles", 25)]:
                            if _k in stored_candle_settings:
                                app.state.runtime_config[_k] = stored_candle_settings[_k]
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
                try:
                    stored_risk_locks = await state_service.get_risk_locks()
                except Exception as exc:  # pragma: no cover - redis optional
                    logger.error("Failed to load persisted risk locks: %s", exc)
                else:
                    if stored_risk_locks:
                        app.state.runtime_config["risk_locks"] = stored_risk_locks
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
                    enable_websocket=app.state.runtime_config.get("enable_websocket", True),
                )
                market_service.set_wait_for_tp_sl(
                    app.state.runtime_config.get("wait_for_tp_sl", False)
                )
                market_service.set_screener_config(
                    app.state.runtime_config.get("screener") or {}
                )
                market_service.set_flip_llm_decision(
                    app.state.runtime_config.get("guardrails", {}).get("flip_llm_decision", False)
                )
                market_service.set_llm_service(app.state.llm_service)
                app.state.market_service = market_service
                await market_service.start()
                # Apply any DB-persisted poll interval (may differ from the .env default)
                stored_pi = app.state.runtime_config.get("poll_interval")
                if stored_pi and stored_pi != market_service._poll_interval:
                    market_service.set_poll_interval(int(stored_pi))
                # Apply persisted candle fetch limit
                stored_fetch_limit = app.state.runtime_config.get("ohlcv_fetch_limit", 200)
                market_service.set_ohlcv_fetch_limit(int(stored_fetch_limit))
                # Apply any persisted strategy config (e.g. skimming)
                stored_strategy = app.state.runtime_config.get("strategy") or {}
                if stored_strategy:
                    market_service.set_strategy_config(stored_strategy)
                stored_governor_cfg = app.state.runtime_config.get("governor") or {}
                if stored_governor_cfg:
                    market_service.set_governor_config(stored_governor_cfg)
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
            for _attr in ("log_lines_handler", "log_file_handler"):
                _h = getattr(app.state, _attr, None)
                if _h:
                    try:
                        logging.getLogger().removeHandler(_h)
                        _h.close()
                    except Exception:
                        pass
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
        return {"status": "ok", "poll_interval": str(settings.poll_interval)}

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

    @app.get("/api/logs")
    async def get_logs(lines: int = 500, filter: str = "") -> JSONResponse:
        all_lines: list[str] = list(getattr(app.state, "log_lines", []))
        if filter:
            fl = filter.lower()
            all_lines = [ln for ln in all_lines if fl in ln.lower()]
        tail = all_lines[-lines:]
        return JSONResponse({"lines": tail, "total": len(all_lines)})

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

    @app.get("/api/prompt/preview")
    async def prompt_preview() -> JSONResponse:
        """Return the assembled default decision prompt for the current guardrail config.

        Used by the CFG page to populate the prompt textarea so the user always
        sees the resolved text (no placeholders) matching their live settings.
        """
        runtime_meta: dict = getattr(app.state, "runtime_config", {}) or {}
        guardrails: dict = runtime_meta.get("guardrails") or {}
        require_rr = bool(guardrails.get("require_reward_risk_ratio"))
        llm_notional_mode = (guardrails.get("llm_notional_mode") or "post_leverage").lower()
        pre_leverage = llm_notional_mode == "pre_leverage"
        sections_config = runtime_meta.get("prompt_sections")
        if sections_config:
            assembled = sanitize_prompt_text(
                assemble_decision_prompt(sections_config=sections_config, pre_leverage=pre_leverage)
            )
        else:
            assembled = sanitize_prompt_text(
                assemble_decision_prompt(require_rr=require_rr, pre_leverage=pre_leverage)
            )
        return JSONResponse({
            "decision_prompt": assembled,
            "require_rr": require_rr,
            "pre_leverage": pre_leverage,
        })

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

    @app.post("/execution/feedback/clear")
    async def clear_execution_feedback(symbol: str | None = None) -> JSONResponse:
        market_service: MarketService | None = app.state.market_service
        if not market_service:
            return JSONResponse({"detail": "market service unavailable"}, status_code=503)
        removed = market_service.clear_execution_feedback(symbol=symbol)
        return JSONResponse({"removed": removed}, status_code=200)

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
