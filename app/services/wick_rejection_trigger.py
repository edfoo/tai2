"""Clock-aligned live entry trigger for ``wick_rejection`` (plan §3.6, Phase 6).

The regular scheduler tick runs every ``interval_seconds`` and is not aligned
to the clock, so it cannot enter in the last seconds of an hour.  This task
wakes at ``H:00:00 − entry_seconds_before_close`` (default 40 s) every hour
and, for each symbol in the wick_rejection universe (the screener's ``vol``
list):

1. refetches the 1H candles (REST) and takes the current price (public
   websocket ticker when fresh, else one REST ticker call), concurrently;
2. builds the decisions through ``build_launcher_decisions(symbol,
   strategies=["wick_rejection"], price=P)`` — the forming 1H candle is
   patched with ``P`` and every launcher guard applies;
3. ranks the signals by ``rank_score``, keeps ``max_concurrent_positions``
   minus the open WR positions, applies ``max_total_margin_pct``;
4. executes them sequentially through ``handle_llm_decision`` under the
   execute lock shared with ``PromptScheduler``;
5. logs one summary line.

It is a watch loop with a single evaluation (``entry_mode="clock"``) so the
optional momentum mode (§3.8) can add a cadence later.  Nothing runs unless
``launcher.mode == "launcher_only"`` and wick_rejection is enabled.  A late
entry is worse than none: symbols are dropped once fewer than
``min_seconds_to_close`` seconds remain.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from app.services.strategies.defaults import merged_config
from app.services.strategies.wick_rejection import STRATEGY_NAME

logger = logging.getLogger(__name__)

_HOUR_S = 3600.0
# Per-order execution budget (one order + its attached algo orders).
_EXEC_TIMEOUT_S = 20.0


def next_trigger_ts(now: float, lead_seconds: float) -> float:
    """Return the next ``H:00:00 − lead_seconds`` (epoch seconds) after ``now``.

    Computed from wall-clock UTC each cycle, so no drift accumulates.
    """
    lead = max(0.0, min(float(lead_seconds), _HOUR_S - 1.0))
    hour_end = (now // _HOUR_S + 1) * _HOUR_S
    target = hour_end - lead
    if target <= now:
        target += _HOUR_S
    return target


def seconds_to_hour_close(now: float) -> float:
    return (now // _HOUR_S + 1) * _HOUR_S - now


def launcher_execution_context(runtime_config: dict[str, Any], symbol: str) -> dict[str, Any]:
    """The context ``handle_llm_decision`` needs for a launcher decision.

    ``runtime_config`` stores execution as flat keys (``execution_enabled``,
    ``execution_trade_mode``, …) — the same mapping PromptBuilder does.
    Shared by ``PromptScheduler`` and the trigger.
    """
    rc = runtime_config or {}
    return {
        "symbol": symbol,
        "execution": {
            "enabled": bool(rc.get("execution_enabled", False)),
            "trade_mode": rc.get("execution_trade_mode") or "isolated",
            "order_type": rc.get("execution_order_type") or "market",
            "min_size": rc.get("execution_min_size"),
            "min_sizes": rc.get("execution_min_sizes") or {},
        },
        "guardrails": rc.get("guardrails") or {},
        "risk_locks": rc.get("risk_locks") or {},
    }


@dataclass
class TriggerResult:
    """What one trigger cycle did (returned for tests and logging)."""

    status: str = "ok"  # ok | skipped | late
    reason: str = ""
    checked: list[str] = field(default_factory=list)
    prices: dict[str, tuple[float | None, str, float | None]] = field(default_factory=dict)
    signals: list[str] = field(default_factory=list)
    dropped: dict[str, str] = field(default_factory=dict)
    executed: list[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.status == "skipped":
            return f"WR trigger: skipped ({self.reason})"
        parts = [
            f"WR trigger: {self.status}",
            f"checked {len(self.checked)}",
            f"signals {len(self.signals)} {self.signals or ''}".rstrip(),
            f"entries {len(self.executed)} {self.executed or ''}".rstrip(),
        ]
        if self.dropped:
            parts.append(
                "dropped " + ", ".join(f"{s}: {why}" for s, why in self.dropped.items())
            )
        if self.reason:
            parts.append(self.reason)
        return " | ".join(parts)


class WickRejectionTrigger:
    """Hourly entry task for wick_rejection, owned by ``PromptScheduler``."""

    def __init__(
        self,
        app: Any,
        execute_lock: asyncio.Lock,
        *,
        is_enabled: Callable[[], bool] = lambda: True,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._app = app
        self._lock = execute_lock
        self._is_enabled = is_enabled
        self._clock = clock
        self._sleep = sleep
        self._task: asyncio.Task | None = None
        self.last_result: TriggerResult | None = None
        self.last_run_at: float | None = None

    # ── Lifecycle ───────────────────────────────────────────────────────

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="wick-rejection-trigger")
            logger.info("Wick rejection trigger started")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("Wick rejection trigger stopped")

    async def _run(self) -> None:
        while True:
            try:
                cfg = self._wr_config()
                lead = float(cfg.get("entry_seconds_before_close") or 40.0)
                now = self._clock()
                await self._sleep(max(0.0, next_trigger_ts(now, lead) - now))
                result = await self.run_once()
                if not (result.status == "skipped" and result.reason == "disabled"):
                    self.last_result = result
                    self.last_run_at = self._clock()
                if result.status != "skipped" or result.reason != "disabled":
                    self._log(result.summary())
                # Step past the trigger second so the next target is next hour.
                await self._sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Wick rejection trigger cycle failed: %s", exc)
                await self._sleep(5.0)

    # ── Status (LIVE page) ──────────────────────────────────────────────

    def is_armed(self) -> bool:
        """True when the task runs and the next cycle would evaluate WR."""
        if self._task is None or self._task.done() or not self._is_enabled():
            return False
        gov = self._runtime_config().get("launcher") or {}
        return (
            str(gov.get("mode") or "disabled").lower() == "launcher_only"
            and bool(((gov.get("strategies") or {}).get(STRATEGY_NAME) or {}).get("enabled"))
        )

    def seconds_until_next(self) -> float | None:
        """Seconds to the next trigger, or None when not armed."""
        if not self.is_armed():
            return None
        now = self._clock()
        lead = float(self._wr_config().get("entry_seconds_before_close") or 40.0)
        return max(0.0, next_trigger_ts(now, lead) - now)

    # ── One cycle ───────────────────────────────────────────────────────

    def _runtime_config(self) -> dict[str, Any]:
        return getattr(self._app.state, "runtime_config", None) or {}

    def _wr_config(self) -> dict[str, Any]:
        gov = self._runtime_config().get("launcher") or {}
        return merged_config((gov.get("strategies") or {}).get(STRATEGY_NAME), STRATEGY_NAME)

    def _log(self, message: str) -> None:
        ms = getattr(self._app.state, "market_service", None)
        if ms is not None and hasattr(ms, "_emit_debug"):
            ms._emit_debug(message)
        else:
            logger.info(message)

    async def run_once(self) -> TriggerResult:
        """Evaluate and execute one trigger cycle now."""
        result = TriggerResult()
        rc = self._runtime_config()
        gov = rc.get("launcher") or {}
        ms = getattr(self._app.state, "market_service", None)
        cfg = self._wr_config()
        if (
            str(gov.get("mode") or "disabled").lower() != "launcher_only"
            or not bool(((gov.get("strategies") or {}).get(STRATEGY_NAME) or {}).get("enabled"))
            or not self._is_enabled()
        ):
            result.status, result.reason = "skipped", "disabled"
            return result
        if ms is None:
            result.status, result.reason = "skipped", "no market service"
            return result

        min_s = float(cfg.get("min_seconds_to_close") or 0.0)
        max_age = float(cfg.get("ws_price_max_age_seconds") or 5.0)
        tf = str(cfg.get("analysis_timeframe") or "1H")
        symbols = [s for s in ms.get_screener_universe(STRATEGY_NAME) if s]
        result.checked = list(symbols)
        if not symbols:
            result.status, result.reason = "skipped", "empty universe"
            return result

        # ── 1. Data, concurrently: candles + price per symbol ──────────
        async def _gather(sym: str) -> tuple[str, tuple[float | None, str, float | None]]:
            try:
                await ms.refresh_timeframe_candles(sym, tf)
            except Exception as exc:  # pragma: no cover - network safety
                self._log(f"WR trigger: {sym} candle refresh failed: {exc}")
            return sym, await ms.fetch_trigger_price(sym, max_age_seconds=max_age)

        gathered = await asyncio.gather(*(_gather(s) for s in symbols), return_exceptions=True)
        for item in gathered:
            if isinstance(item, BaseException):
                self._log(f"WR trigger: data fetch error: {item}")
                continue
            sym, price_info = item
            result.prices[sym] = price_info
        price_txt = ", ".join(
            f"{s}={p[0]:g} ({p[1]}, {p[2]:.1f}s)" if p[0] and p[2] is not None
            else f"{s}={p[0]} ({p[1]})"
            for s, p in result.prices.items()
        )
        self._log(f"WR trigger: prices {price_txt}")

        left = seconds_to_hour_close(self._clock())
        if left < min_s:
            result.status = "late"
            result.reason = f"data ready {left:.1f}s before close < {min_s:g}s"
            return result

        # ── 2. Decisions (all launcher guards apply) ────────────────────
        decisions: list[tuple[str, dict[str, Any]]] = []
        for sym in symbols:
            price, source, _age = result.prices.get(sym, (None, "missing", None))
            if not price:
                result.dropped[sym] = f"no price ({source})"
                continue
            try:
                for d in ms.build_launcher_decisions(sym, strategies=[STRATEGY_NAME], price=price):
                    decisions.append((sym, d))
            except Exception as exc:  # pragma: no cover - defensive
                result.dropped[sym] = f"decision error: {exc}"
        result.signals = [s for s, _ in decisions]

        # ── 3. Rank, slots, margin budget ───────────────────────────────
        decisions.sort(key=lambda sd: float(sd[1].get("_rank_score") or 0.0), reverse=True)
        open_wr = [
            v for k, v in getattr(ms, "_launcher_in_position", {}).items()
            if str((v or {}).get("strategy") or str(k).split(":", 1)[0]) == STRATEGY_NAME
        ]
        max_conc = int(cfg.get("max_concurrent_positions") or 0)
        slots = max(0, max_conc - len(open_wr)) if max_conc > 0 else len(decisions)
        equity = _snapshot_equity(ms)
        max_margin_pct = float(cfg.get("max_total_margin_pct") or 0.0)
        margin_budget = (
            equity * max_margin_pct / 100.0 if equity and max_margin_pct > 0 else None
        )
        margin_used = sum(float((v or {}).get("margin_usd") or 0.0) for v in open_wr)
        chosen: list[tuple[str, dict[str, Any]]] = []
        for sym, d in decisions:
            if len(chosen) >= slots:
                result.dropped[sym] = (
                    f"max_concurrent_positions {max_conc} ({len(open_wr)} open)"
                )
                continue
            margin = float(d.get("_margin_usd") or 0.0)
            if margin_budget is not None and margin_used + margin > margin_budget + 1e-9:
                result.dropped[sym] = (
                    f"max_total_margin_pct {max_margin_pct:g}% "
                    f"({margin_used:.2f}+{margin:.2f} > {margin_budget:.2f} USDT)"
                )
                continue
            margin_used += margin
            chosen.append((sym, d))

        # ── 4. Execute sequentially under the shared lock ───────────────
        for sym, d in chosen:
            left = seconds_to_hour_close(self._clock())
            if left < min_s:
                result.dropped[sym] = f"late ({left:.1f}s to close)"
                continue
            try:
                await asyncio.wait_for(self._lock.acquire(), timeout=max(0.0, left - min_s))
            except asyncio.TimeoutError:
                result.dropped[sym] = "execute lock busy until the window closed"
                continue
            try:
                left = seconds_to_hour_close(self._clock())
                if left < min_s:
                    result.dropped[sym] = f"late ({left:.1f}s to close)"
                    continue
                ok = await asyncio.wait_for(
                    ms.handle_llm_decision(d, launcher_execution_context(rc, sym)),
                    timeout=_EXEC_TIMEOUT_S,
                )
                if ok:
                    result.executed.append(sym)
                else:
                    result.dropped[sym] = "not executed (see execution log)"
            except asyncio.TimeoutError:
                result.dropped[sym] = f"execution timed out after {_EXEC_TIMEOUT_S:g}s"
            except Exception as exc:  # pragma: no cover - defensive
                result.dropped[sym] = f"execution error: {exc}"
            finally:
                self._lock.release()
        return result


def _snapshot_equity(ms: Any) -> float | None:
    account = (getattr(ms, "_last_full_snapshot", None) or {}).get("account") or {}
    for key in ("account_equity", "total_eq_usd", "total_equity", "total_account_value"):
        try:
            value = float(account.get(key))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


__all__ = [
    "TriggerResult",
    "WickRejectionTrigger",
    "launcher_execution_context",
    "next_trigger_ts",
    "seconds_to_hour_close",
]
