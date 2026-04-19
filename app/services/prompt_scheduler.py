from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from fastapi import FastAPI

from app.services.prompt_runner import (
    apply_llm_decision,
    fetch_llm_decision,
    prepare_prompt_payload,
)

logger = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    enabled: bool = False
    interval_seconds: int = 300


class PromptScheduler:
    # Maximum wall-clock time a single _tick() is allowed to run before it is
    # cancelled and the loop continues.  Sized to fit:
    #   _FETCH_TIMEOUT (620s, parallel LLM gather) +
    #   up to 2 sequential BUY/SELL executions × _EXEC_TIMEOUT (90s each) +
    #   overhead.  Slow reasoning models (e.g. Gemma 4 31B) can take up to
    #   10 minutes so the fetch budget is the dominant term.
    TICK_TIMEOUT_SECONDS = 800
    # Delay before automatically restarting after an unexpected crash.
    RESTART_DELAY_SECONDS = 30

    def __init__(self, app: FastAPI, *, default_interval: int = 300) -> None:
        self._app = app
        self._interval = max(30, default_interval)
        self._enabled = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._last_error: Optional[str] = None
        self._last_tick_at: float = 0.0
        self._tick_running: bool = False
        self._tick_started_at: float = 0.0

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

    @property
    def is_ticking(self) -> bool:
        """True while a _tick() coroutine is currently executing."""
        return self._tick_running

    @property
    def tick_elapsed_seconds(self) -> float | None:
        """Seconds since the current tick started, or None if no tick is running."""
        if not self._tick_running or self._tick_started_at == 0.0:
            return None
        return time.monotonic() - self._tick_started_at

    @property
    def seconds_until_next_tick(self) -> float | None:
        """Seconds until the next scheduled tick, or None if the scheduler is not running."""
        if not self._enabled or self._task is None or self._task.done():
            return None
        if self._tick_running:
            # Tick is currently in progress; return 0 to indicate active.
            return 0.0
        if self._last_tick_at == 0.0:
            return None
        remaining = self._interval - (time.monotonic() - self._last_tick_at)
        return max(0.0, remaining)

    async def _run(self) -> None:
        while self._enabled:
            try:
                self._tick_running = True
                self._tick_started_at = time.monotonic()
                try:
                    await asyncio.wait_for(
                        self._tick(),
                        timeout=self.TICK_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Prompt scheduler tick timed out after %ss; skipping cycle",
                        self.TICK_TIMEOUT_SECONDS,
                    )
                finally:
                    self._tick_running = False
                    self._last_tick_at = time.monotonic()
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                self._tick_running = False
                self._last_error = str(exc)
                logger.exception("Prompt scheduler crashed; restarting in %ss: %s",
                                  self.RESTART_DELAY_SECONDS, exc)
                try:
                    await asyncio.sleep(self.RESTART_DELAY_SECONDS)
                except asyncio.CancelledError:
                    raise
        logger.debug("Prompt scheduler loop exited")

    async def _tick(self) -> None:
        market_service = getattr(self._app.state, "market_service", None)
        if market_service:
            try:
                screener_fired = await market_service.run_screener_if_due(force=True)
            except Exception as exc:  # pragma: no cover - network variance
                logger.debug("Symbol screener tick failed: %s", exc)
                screener_fired = False
            if screener_fired:
                new_symbols: list[str] = list(getattr(market_service, "symbols", []))
                runtime_config = getattr(self._app.state, "runtime_config", None)
                if runtime_config is not None:
                    runtime_config["trading_pairs"] = new_symbols
                    runtime_config["_screener_pairs_changed"] = True
                try:
                    from app.db.postgres import set_enabled_trading_pairs as _set_pairs
                    await _set_pairs(new_symbols)
                except Exception as exc:  # pragma: no cover - DB optional
                    logger.debug("Screener: failed to persist symbols to DB: %s", exc)
        state_service = getattr(self._app.state, "state_service", None)
        if not state_service:
            logger.debug("Prompt scheduler: state service unavailable")
            return
        snapshot = await state_service.get_market_snapshot()
        if not snapshot:
            logger.debug("Prompt scheduler: snapshot unavailable")
            return
        # Prefer the live market_service.symbols list (always current) over the
        # snapshot's "symbols" field, which may have been baked when fewer symbols
        # were active (e.g. before the screener ran or before a CFG save).
        if market_service and getattr(market_service, "symbols", None):
            symbols: Iterable[str] = list(market_service.symbols)
        else:
            symbols = self._resolve_symbols(snapshot)
        if not symbols:
            logger.debug("Prompt scheduler: no symbols to evaluate")
            return
        # Refresh once — _build_snapshot covers all symbols in one pass.
        await self._refresh_snapshot(reason="scheduler")

        # Record the post-refresh equity as the Shotgun strategy baseline for
        # this cycle.  _check_shotgun() will compare live equity against this
        # anchor every 10 s until the next scheduler tick resets it.
        if market_service:
            _snap_after = await state_service.get_market_snapshot() if state_service else None
            if _snap_after:
                _baseline_eq = float(_snap_after.get("account_equity") or 0.0)
                if _baseline_eq > 0:
                    market_service.record_shotgun_baseline(_baseline_eq)

        # ── Phase 1: ask the LLM for ALL symbols concurrently ─────────────
        # LLM calls are pure I/O — running them in parallel costs the same
        # wall-clock time as a single call and lets us see every decision
        # before committing any funds.
        # Budget per symbol for LLM call + DB persist.  Must leave room for
        # the execution phase (90s/symbol) within TICK_TIMEOUT_SECONDS (800s).
        # Set to 620s to accommodate slow models like Gemma 4 31B (~600s).
        _FETCH_TIMEOUT = 620

        async def _fetch_decision(
            symbol: str,
        ) -> tuple[str, Any, dict[str, Any] | None, str | None]:
            """Prepare prompt + call LLM. Returns (symbol, bundle, decision, prompt_id)."""
            try:
                _ms = getattr(self._app.state, "market_service", None)
                if _ms:
                    _rc = getattr(self._app.state, "runtime_config", {}) or {}
                    _gr = _rc.get("guardrails") or {}
                    _blocked = _ms.is_symbol_blocked(symbol, _gr)
                    if _blocked:
                        logger.debug("Skipping LLM for %s: %s", symbol, _blocked)
                        return symbol, None, None, None
                bundle, error_response = await prepare_prompt_payload(self._app, symbol=symbol)
                if error_response:
                    status = getattr(error_response, "status_code", None)
                    body = getattr(error_response, "body", b"error")
                    if status == 503:
                        logger.warning(
                            "Prompt scheduler skipping %s: snapshot stale or unavailable (%s)",
                            symbol,
                            body,
                        )
                    else:
                        logger.debug(
                            "Prompt scheduler skipping %s: %s",
                            symbol,
                            body,
                        )
                    if status == 423:
                        self._handle_daily_loss_lock(symbol)
                    return symbol, None, None, None
                if not bundle:
                    return symbol, None, None, None
                decision, prompt_id = await fetch_llm_decision(self._app, bundle)
                return symbol, bundle, decision, prompt_id
            except asyncio.TimeoutError:
                # Raised by the wait_for wrapper below — already logged there.
                return symbol, None, None, None
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Prompt scheduler LLM fetch failed for %s: %s", symbol, exc)
                return symbol, None, None, None

        async def _fetch_decision_with_timeout(
            symbol: str,
        ) -> tuple[str, Any, dict[str, Any] | None, str | None]:
            try:
                return await asyncio.wait_for(_fetch_decision(symbol), timeout=_FETCH_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(
                    "Prompt scheduler LLM fetch timed out after %ss for %s; skipping",
                    _FETCH_TIMEOUT,
                    symbol,
                )
                return symbol, None, None, None

        raw_results: list[tuple[str, Any, dict[str, Any] | None, str | None]] = list(
            await asyncio.gather(*(_fetch_decision_with_timeout(s) for s in symbols))
        )
        valid = [
            (sym, bundle, decision, prompt_id)
            for sym, bundle, decision, prompt_id in raw_results
            if bundle is not None and decision is not None
        ]

        # ── Phase 2: sort BUY/SELL by quality ─────────────────────────────
        # Highest confidence first; lowest risk_score as tiebreaker so the
        # most compelling setups consume capital before weaker ones.
        def _is_actionable(item: tuple) -> bool:
            return (item[2].get("action") or "HOLD").upper() in {"BUY", "SELL"}

        def _decision_sort_key(item: tuple) -> tuple[float, float]:
            d = item[2]
            conf = float(d.get("confidence") or 0.0)
            risk = float(d.get("risk_score") or 1.0)
            return (-conf, risk)  # highest confidence, lowest risk first

        actionable = sorted([i for i in valid if _is_actionable(i)], key=_decision_sort_key)
        non_actionable = [i for i in valid if not _is_actionable(i)]

        # Per-symbol execution budget: keeps one stuck OKX call from consuming
        # the entire tick timeout and blocking all remaining decisions.
        _EXEC_TIMEOUT = 90

        # ── Phase 3: execute BUY/SELL sequentially ────────────────────────
        # Running sequentially means each trade sees the actual remaining
        # balance (or the _pending_notional deduction when OKX balance hasn't
        # updated yet), so later trades automatically size down to whatever
        # equity is left after earlier trades have committed funds.
        for sym, bundle, decision, prompt_id in actionable:
            try:
                await asyncio.wait_for(
                    apply_llm_decision(self._app, bundle, decision, prompt_id),
                    timeout=_EXEC_TIMEOUT,
                )
                logger.info(
                    "Prompt scheduler decision for %s action=%s confidence=%s prompt_id=%s",
                    sym,
                    decision.get("action"),
                    decision.get("confidence"),
                    prompt_id,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Prompt scheduler execution timed out after %ss for %s; continuing",
                    _EXEC_TIMEOUT,
                    sym,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Prompt scheduler execution failed for %s: %s", sym, exc)

        # ── Phase 4: record HOLDs concurrently ────────────────────────────
        # HOLDs do not place orders so they are safe to run in parallel.
        async def _apply_hold(item: tuple) -> None:
            sym, bundle, decision, prompt_id = item
            try:
                await asyncio.wait_for(
                    apply_llm_decision(self._app, bundle, decision, prompt_id),
                    timeout=_EXEC_TIMEOUT,
                )
                logger.info(
                    "Prompt scheduler decision for %s action=%s confidence=%s prompt_id=%s",
                    sym,
                    decision.get("action"),
                    decision.get("confidence"),
                    prompt_id,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Prompt scheduler HOLD timed out after %ss for %s; continuing",
                    _EXEC_TIMEOUT,
                    sym,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Prompt scheduler HOLD recording failed for %s: %s", sym, exc)

        await asyncio.gather(*(_apply_hold(i) for i in non_actionable))

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


    def _handle_daily_loss_lock(self, symbol: str | None) -> None:
        runtime_config = getattr(self._app.state, "runtime_config", {}) or {}
        risk_locks = runtime_config.setdefault("risk_locks", {})
        lock_state = risk_locks.get("daily_loss") if isinstance(risk_locks.get("daily_loss"), dict) else {}
        if not lock_state:
            return
        drop_pct = lock_state.get("change_pct")
        limit_pct = lock_state.get("threshold_pct")
        window_hours = lock_state.get("window_hours")
        locked_at = lock_state.get("locked_at")
        drop_label = f"{drop_pct * 100:.2f}%" if isinstance(drop_pct, (int, float)) else "--"
        limit_label = f"{limit_pct * 100:.2f}%" if isinstance(limit_pct, (int, float)) else "--"
        entry_symbol = (symbol or "ACCOUNT").upper()
        market_service = getattr(self._app.state, "market_service", None)
        if market_service and not lock_state.get("execution_alert_logged"):
            market_service.record_execution_feedback(
                entry_symbol,
                "Prompt scheduler paused: daily loss limit triggered",
                level="warning",
                meta={
                    "change_pct": drop_pct,
                    "threshold_pct": limit_pct,
                    "window_hours": window_hours,
                    "locked_at": locked_at,
                },
            )
            lock_state["execution_alert_logged"] = True
        if not lock_state.get("auto_prompt_disabled") and runtime_config.get("auto_prompt_enabled"):
            runtime_config["auto_prompt_enabled"] = False
            lock_state["auto_prompt_disabled"] = True
            self._enabled = False
            logger.warning("Prompt scheduler disabled (daily loss guard active)")
            backend_events = getattr(self._app.state, "backend_events", None)
            if backend_events is not None:
                backend_events.append(
                    "Prompt scheduler disabled · daily loss limit triggered"
                )
        risk_locks["daily_loss"] = lock_state
        state_service = getattr(self._app.state, "state_service", None)
        if state_service:
            asyncio.create_task(state_service.set_risk_locks(risk_locks))


__all__ = ["PromptScheduler", "SchedulerConfig"]
