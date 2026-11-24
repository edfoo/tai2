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

Momentum entry mode (§3.8, ``entry_mode`` = ``"momentum"`` /
``"momentum_or_clock"``, ``run_watch``): wakes at ``watch_seconds_before_close``
instead, records the public trade tape (``MarketService.start_wr_tape``),
computes ``wr_momentum.compute_momentum`` every
``momentum_eval_interval_seconds`` and enters a symbol the first time its
measures pass inside ``momentum_entry_max_seconds_to_close`` and the strategy
funnel agrees (the measures travel to the strategy, which applies the same
gate as the backtest).  Symbols with a tape gap are excluded from momentum for
the hour; ``momentum_or_clock`` gives the symbols not entered by
``entry_seconds_before_close`` the clock evaluation.

Nothing runs unless
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
from app.services.strategies.wick_rejection import (
    STRATEGY_NAME,
    entry_mode,
    select_ranked_entries,
    vol_refresh_lead_seconds,
    wake_lead_seconds,
)
from app.services.strategies.wr_momentum import (
    MomentumMeasures,
    compute_momentum,
    momentum_entry_ok,
)

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
    mode: str = "clock"
    checked: list[str] = field(default_factory=list)
    prices: dict[str, tuple[float | None, str, float | None]] = field(default_factory=dict)
    signals: list[str] = field(default_factory=list)
    dropped: dict[str, str] = field(default_factory=dict)
    executed: list[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.status == "skipped":
            return f"WR trigger: skipped ({self.reason})"
        label = "WR trigger" if self.mode == "clock" else f"WR trigger ({self.mode})"
        parts = [
            f"{label}: {self.status}",
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
        # Last vol-list refresh: {"at", "symbols", "added", "removed", "seconds"}.
        self.last_vol_refresh: dict[str, Any] | None = None
        # Momentum watch state for the LIVE page: {"active", "mode", "symbols":
        # {sym: {"status", "measures"}}}.
        self.watch: dict[str, Any] = {"active": False, "symbols": {}}

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
                lead = wake_lead_seconds(cfg)
                now = self._clock()
                wake = next_trigger_ts(now, lead)
                refresh_lead = vol_refresh_lead_seconds(cfg)
                if refresh_lead is not None and refresh_lead > lead:
                    # Rescore the vol list first (skipped this hour when the
                    # task starts after the refresh moment).
                    refresh_at = wake - (refresh_lead - lead)
                    if refresh_at > now:
                        await self._sleep(refresh_at - now)
                        await self.refresh_vol_list(timeout=max(1.0, wake - refresh_at))
                        now = self._clock()
                await self._sleep(max(0.0, wake - now))
                if entry_mode(self._wr_config()) == "clock":
                    result = await self.run_once()
                else:
                    result = await self.run_watch()
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
        return max(0.0, next_trigger_ts(now, wake_lead_seconds(self._wr_config())) - now)

    # ── Vol-list refresh ────────────────────────────────────────────────

    async def refresh_vol_list(self, *, timeout: float = 60.0) -> dict[str, Any] | None:
        """Have the market service rescore the vol list with the hour in
        progress (``MarketService.refresh_wr_vol_list``) before the entry.

        Skipped when WR would not run.  On failure or timeout the existing
        list stands; the entry runs at its normal time either way.
        """
        rc = self._runtime_config()
        gov = rc.get("launcher") or {}
        ms = getattr(self._app.state, "market_service", None)
        if (
            str(gov.get("mode") or "disabled").lower() != "launcher_only"
            or not bool(((gov.get("strategies") or {}).get(STRATEGY_NAME) or {}).get("enabled"))
            or not self._is_enabled()
            or ms is None
            or not hasattr(ms, "refresh_wr_vol_list")
        ):
            return None
        try:
            out = await asyncio.wait_for(ms.refresh_wr_vol_list(), timeout=timeout)
        except asyncio.TimeoutError:
            self._log(f"WR vol refresh: timed out after {timeout:.0f}s — keeping the hourly list")
            return None
        except Exception as exc:  # pragma: no cover - network safety
            self._log(f"WR vol refresh failed: {exc} — keeping the hourly list")
            return None
        if out is not None:
            self.last_vol_refresh = {"at": self._clock(), **out}
        return out

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

    def _preflight(
        self, result: TriggerResult
    ) -> tuple[dict[str, Any], Any, dict[str, Any], list[str]] | None:
        """Common gates; returns ``(runtime_config, ms, cfg, symbols)`` or None."""
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
            return None
        if ms is None:
            result.status, result.reason = "skipped", "no market service"
            return None
        symbols = [s for s in ms.get_screener_universe(STRATEGY_NAME) if s]
        result.checked = list(symbols)
        if not symbols:
            result.status, result.reason = "skipped", "empty universe"
            return None
        return rc, ms, cfg, symbols

    async def run_once(self) -> TriggerResult:
        """Evaluate and execute one clock trigger cycle now."""
        result = TriggerResult()
        pre = self._preflight(result)
        if pre is None:
            return result
        rc, ms, cfg, symbols = pre
        await self._clock_entry(symbols, result, rc, ms, cfg)
        return result

    async def _clock_entry(
        self, symbols: list[str], result: TriggerResult,
        rc: dict[str, Any], ms: Any, cfg: dict[str, Any],
    ) -> None:
        """§3.6 clock evaluation of ``symbols`` (no momentum measures)."""
        min_s = float(cfg.get("min_seconds_to_close") or 0.0)
        max_age = float(cfg.get("ws_price_max_age_seconds") or 5.0)
        tf = str(cfg.get("analysis_timeframe") or "1H")

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
            for s, p in result.prices.items() if s in symbols
        )
        self._log(f"WR trigger: prices {price_txt}")

        left = seconds_to_hour_close(self._clock())
        if left < min_s:
            result.status = "late"
            result.reason = f"data ready {left:.1f}s before close < {min_s:g}s"
            return

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
        result.signals.extend(s for s, _ in decisions)
        await self._execute_ranked(decisions, result, rc, ms, cfg)

    async def _execute_ranked(
        self, decisions: list[tuple[str, dict[str, Any]]], result: TriggerResult,
        rc: dict[str, Any], ms: Any, cfg: dict[str, Any],
    ) -> None:
        """Rank by ``rank_score``, apply the slot and margin caps, execute."""
        min_s = float(cfg.get("min_seconds_to_close") or 0.0)
        # ── 3. Rank, slots, margin budget (shared with the backtest) ────
        open_wr = [
            v for k, v in getattr(ms, "_launcher_in_position", {}).items()
            if str((v or {}).get("strategy") or str(k).split(":", 1)[0]) == STRATEGY_NAME
        ]
        keep, drop = select_ranked_entries(
            [(i, float(d.get("_rank_score") or 0.0), float(d.get("_margin_usd") or 0.0))
             for i, (_sym, d) in enumerate(decisions)],
            open_positions=len(open_wr),
            open_margin=sum(float((v or {}).get("margin_usd") or 0.0) for v in open_wr),
            equity=_snapshot_equity(ms),
            max_concurrent_positions=cfg.get("max_concurrent_positions"),
            max_total_margin_pct=cfg.get("max_total_margin_pct"),
        )
        for i, reason in drop.items():
            result.dropped[decisions[i][0]] = reason
        chosen = [decisions[i] for i in keep]

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
                    result.dropped.pop(sym, None)
                else:
                    result.dropped[sym] = "not executed (see execution log)"
            except asyncio.TimeoutError:
                result.dropped[sym] = f"execution timed out after {_EXEC_TIMEOUT_S:g}s"
            except Exception as exc:  # pragma: no cover - defensive
                result.dropped[sym] = f"execution error: {exc}"
            finally:
                self._lock.release()

    # ── Momentum mode (§3.8) ────────────────────────────────────────────

    async def run_watch(self) -> TriggerResult:
        """One momentum watch window: from ``watch_seconds_before_close`` to
        ``min_seconds_to_close`` before the hour.

        Records the trade tape, computes the measures every
        ``momentum_eval_interval_seconds``, and enters a symbol the first
        time its measures pass inside ``momentum_entry_max_seconds_to_close``
        (and the strategy funnel agrees).  ``momentum_or_clock``: symbols not
        entered by ``entry_seconds_before_close`` get the clock evaluation.
        """
        result = TriggerResult()
        pre = self._preflight(result)
        if pre is None:
            return result
        rc, ms, cfg, symbols = pre
        mode = entry_mode(cfg)
        result.mode = mode
        watch_s = float(cfg.get("watch_seconds_before_close") or 300.0)
        entry_max = float(cfg.get("momentum_entry_max_seconds_to_close") or 120.0)
        min_s = float(cfg.get("min_seconds_to_close") or 0.0)
        interval = max(0.5, float(cfg.get("momentum_eval_interval_seconds") or 2.0))
        clock_lead = float(cfg.get("entry_seconds_before_close") or 40.0)
        tf = str(cfg.get("analysis_timeframe") or "1H")

        ms.start_wr_tape(symbols, watch_s + 120.0)
        state: dict[str, dict[str, Any]] = {s: {"status": "watching", "measures": None} for s in symbols}
        self.watch = {"active": True, "mode": mode, "symbols": state}
        pending = list(symbols)
        gapped: dict[str, str] = {}
        early_logged: set[str] = set()
        last_refresh: dict[str, float] = {}
        last_veto: dict[str, str] = {}

        async def _refresh(sym: str) -> None:
            try:
                await ms.refresh_timeframe_candles(sym, tf)
                last_refresh[sym] = self._clock()
            except Exception as exc:  # pragma: no cover - network safety
                self._log(f"WR momentum: {sym} candle refresh failed: {exc}")

        try:
            await asyncio.gather(*(_refresh(s) for s in symbols), return_exceptions=True)
            while pending:
                now = self._clock()
                left = seconds_to_hour_close(now)
                if left < min_s or left > watch_s + 60.0:
                    break
                if mode == "momentum_or_clock" and left <= clock_lead:
                    self._log(
                        f"WR momentum: clock fallback for {len(pending)} symbol(s) "
                        f"{pending} at {left:.0f}s to close"
                    )
                    for sym in pending:
                        state[sym]["status"] = "clock fallback"
                    await self._clock_entry(list(pending), result, rc, ms, cfg)
                    pending = [s for s in pending if s not in result.executed]
                    break

                candidates: list[tuple[str, MomentumMeasures]] = []
                for sym in list(pending):
                    tape, gap = ms.wr_tape(sym)
                    if gap:
                        if sym not in gapped:
                            gapped[sym] = gap
                            self._log(f"WR momentum: {sym} excluded this hour — tape gap ({gap})")
                        state[sym]["status"] = f"tape gap: {gap}"
                        if mode == "momentum":
                            pending.remove(sym)
                            result.dropped[sym] = f"tape gap ({gap})"
                        continue
                    m = compute_momentum(tape, int(now * 1000), watch_s)
                    ok, fails = momentum_entry_ok(m, cfg)
                    state[sym]["measures"] = m.describe() if m else None
                    state[sym]["status"] = "momentum ok" if ok else "; ".join(fails)
                    if not ok or m is None:
                        continue
                    if left > entry_max:
                        if sym not in early_logged:
                            early_logged.add(sym)
                            self._log(
                                f"WR momentum: {sym} passed {left:.0f}s before close, before "
                                f"the {entry_max:g}s entry window — not traded: {m.describe()}"
                            )
                        continue
                    candidates.append((sym, m))

                decisions: list[tuple[str, dict[str, Any]]] = []
                for sym, m in candidates:
                    if self._clock() - last_refresh.get(sym, 0.0) >= 10.0:
                        await _refresh(sym)
                    try:
                        ds = ms.build_launcher_decisions(
                            sym, strategies=[STRATEGY_NAME], price=m.last_px, momentum=m
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        ds = []
                        self._log(f"WR momentum: {sym} decision error: {exc}")
                    verdict = "signal" if ds else "funnel veto (see strategy log)"
                    if ds or last_veto.get(sym) != verdict:
                        self._log(
                            f"WR momentum: {sym} {left:.0f}s to close → {verdict}: {m.describe()}"
                        )
                        last_veto[sym] = verdict
                    decisions.extend((sym, d) for d in ds)
                if decisions:
                    result.signals.extend(s for s, _ in decisions if s not in result.signals)
                    await self._execute_ranked(decisions, result, rc, ms, cfg)
                    for sym in list(pending):
                        if sym in result.executed:
                            pending.remove(sym)
                            state[sym]["status"] = "entered"
                await self._sleep(interval)

            for sym in pending:
                if sym in result.executed:
                    continue
                st = state.get(sym) or {}
                self._log(
                    f"WR momentum: {sym} no entry this hour — {st.get('status')}; "
                    f"last measures: {st.get('measures') or 'none'}"
                )
        finally:
            ms.stop_wr_tape()
            self.watch["active"] = False
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
    "entry_mode",
    "wake_lead_seconds",
    "WickRejectionTrigger",
    "launcher_execution_context",
    "next_trigger_ts",
    "seconds_to_hour_close",
]
