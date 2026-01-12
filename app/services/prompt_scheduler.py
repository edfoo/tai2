from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from fastapi import FastAPI

from app.services.prompt_runner import execute_llm_decision, prepare_prompt_payload

logger = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    enabled: bool = False
    interval_seconds: int = 300


class PromptScheduler:
    def __init__(self, app: FastAPI, *, default_interval: int = 300) -> None:
        self._app = app
        self._interval = max(30, default_interval)
        self._enabled = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._last_error: Optional[str] = None

    async def start(self) -> None:
        async with self._lock:
            self._enabled = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run(), name="prompt-scheduler")
                logger.info("Prompt scheduler started (interval=%ss)", self._interval)

    async def stop(self) -> None:
        async with self._lock:
            self._enabled = False
            if self._task is not None:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
                logger.info("Prompt scheduler stopped")

    async def set_enabled(self, value: bool) -> None:
        if value:
            await self.start()
        else:
            await self.stop()

    async def update_interval(self, seconds: int) -> None:
        seconds = max(30, int(seconds or 30))
        async with self._lock:
            self._interval = seconds
            if self._task and not self._task.done():
                logger.info("Prompt scheduler interval updated to %ss", seconds)

    async def _run(self) -> None:
        try:
            while self._enabled:
                await self._tick()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive logging
            self._last_error = str(exc)
            logger.exception("Prompt scheduler crashed: %s", exc)
        finally:
            logger.debug("Prompt scheduler loop exited")

    async def _tick(self) -> None:
        state_service = getattr(self._app.state, "state_service", None)
        if not state_service:
            logger.debug("Prompt scheduler: state service unavailable")
            return
        snapshot = await state_service.get_market_snapshot()
        if not snapshot:
            logger.debug("Prompt scheduler: snapshot unavailable")
            return
        symbols = self._resolve_symbols(snapshot)
        if not symbols:
            logger.debug("Prompt scheduler: no symbols to evaluate")
            return
        for symbol in symbols:
            try:
                await self._evaluate_symbol(symbol)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Prompt scheduler failed for %s: %s", symbol, exc)

    def _resolve_symbols(self, snapshot: dict[str, Any]) -> Iterable[str]:
        symbols = snapshot.get("symbols") or []
        if not symbols:
            primary = snapshot.get("symbol")
            if primary:
                return [primary]
        return symbols

    async def _refresh_snapshot(self, reason: str) -> None:
        market_service = getattr(self._app.state, "market_service", None)
        if not market_service:
            return
        try:
            await market_service.refresh_snapshot(reason=reason)
        except Exception as exc:  # pragma: no cover - upstream network risks
            logger.debug("Prompt scheduler snapshot refresh skipped (%s): %s", reason, exc)

    async def _evaluate_symbol(self, symbol: str) -> None:
        await self._refresh_snapshot(reason=f"scheduler:{symbol}")
        bundle, error_response = await prepare_prompt_payload(self._app, symbol=symbol)
        if error_response:
            logger.debug(
                "Prompt scheduler skipping %s: %s", symbol, getattr(error_response, "body", b"error")
            )
            return
        if not bundle:
            return
        decision, prompt_id = await execute_llm_decision(self._app, bundle)
        logger.info(
            "Prompt scheduler decision for %s action=%s confidence=%s prompt_id=%s",
            symbol,
            decision.get("action"),
            decision.get("confidence"),
            prompt_id,
        )


__all__ = ["PromptScheduler", "SchedulerConfig"]
