from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

from fastapi import FastAPI
from nicegui import ui

from app.core.config import get_settings
from app.db.postgres import (
    delete_prompt_version,
    fetch_equity_history,
    fetch_equity_window,
    fetch_okx_fees_window,
    fetch_prompt_versions,
    fetch_prompt_runs,
    fetch_recent_trades,
    load_execution_settings,
    insert_prompt_version,
    save_guardrails,
    save_execution_settings,
    save_prompt_interval,
    save_llm_model,
    save_okx_sub_account,
    save_poll_interval,
    save_screener_config,
    save_strategy_config,
    save_ta_timeframe,
    save_frontend_timezone,
    save_candle_settings,
    load_candle_settings,
    save_launcher_config,
    save_notifications_config,
    set_enabled_trading_pairs,
)
from app.services.prompt_builder import (
    DEFAULT_DECISION_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    PROMPT_SECTIONS,
    RESPONSE_SCHEMA,
    PromptBuilder,
    assemble_decision_prompt,
    default_prompt_sections,
)
from app.services.openrouter_service import (
    DEFAULT_MODEL_OPTIONS,
    fetch_openrouter_credits,
    list_openrouter_models,
)
from app.services.prompt_utils import sanitize_prompt_text
from app.ui.components import SnapshotStore, badge_stat

NAV_LINKS = [
    ("LIVE", "/live"),
    ("TA", "/ta"),
    ("STRATEGY", "/strategy"),
    ("BACKTEST", "/backtest"),
    ("HISTORY", "/history"),
    ("DEBUG", "/debug"),
    ("PROMPT", "/prompt"),
    ("CFG", "/cfg"),
]

TA_TIMEFRAME_OPTIONS = ["15m", "1H", "4H", "1D"]

DEFAULT_FRONTEND_TIMEZONE = "UTC"
try:
    TIMEZONE_OPTIONS = sorted(tz for tz in available_timezones() if tz)
except Exception:  # pragma: no cover - fallback when tzdata unavailable
    TIMEZONE_OPTIONS = [
        "UTC",
        "US/Eastern",
        "US/Central",
        "US/Mountain",
        "US/Pacific",
        "Europe/London",
        "Europe/Berlin",
        "Europe/Paris",
        "Asia/Singapore",
        "Asia/Tokyo",
        "Asia/Hong_Kong",
        "Australia/Sydney",
    ]
if DEFAULT_FRONTEND_TIMEZONE not in TIMEZONE_OPTIONS:
    TIMEZONE_OPTIONS.insert(0, DEFAULT_FRONTEND_TIMEZONE)


def render_backtest_page() -> None:
    """BACKTEST page — run historical strategy backtests.

    Lets the user select strategies, date range, and capital, then runs a
    backtest using the current strategy config values.  Results show an
    equity curve, trade table, and summary metrics.
    """
    from app.services.backtest.engine import BacktestEngine, available_strategy_names
    from app.services.backtest.models import BacktestConfig

    navigation("BACKTEST")
    wrapper = page_container()
    config = getattr(app.state, "runtime_config", {}) or {}
    trading_pairs = config.get("trading_pairs") or []
    launcher_config = config.get("launcher") or {}
    strategy_config = config.get("strategy") or {}
    strategy_names = available_strategy_names()

    # ── State ─────────────────────────────────────────────────────────
    backtest_running = {"flag": False}
    backtest_result = {"value": None}

    with wrapper:
        ui.label("Backtesting").classes("text-2xl font-bold")
        ui.label(
            "Run historical backtests of your strategies using current config values. "
            "Data is fetched from OKX and cached locally for re-runs."
        ).classes("text-sm text-slate-500 mb-2")
        ui.separator().classes("w-full my-2")

        # ── Configuration form ────────────────────────────────────────
        with ui.card().classes("w-full rounded-lg border border-slate-200 mb-2"):
            ui.label("Configuration").classes("text-lg font-semibold mb-2")

            with ui.row().classes("w-full gap-4 items-start"):
                # Symbol selection
                symbol_select = ui.select(
                    options={s: s for s in trading_pairs} if trading_pairs else {"BTC-USDT-SWAP": "BTC-USDT-SWAP"},
                    value=trading_pairs[0] if trading_pairs else "BTC-USDT-SWAP",
                    label="Symbol",
                    multiple=True,
                ).classes("w-64")

                # Timeframe
                timeframe_select = ui.select(
                    options={tf: tf for tf in TA_TIMEFRAME_OPTIONS},
                    value=config.get("ta_timeframe") or "4H",
                    label="Timeframe",
                ).classes("w-32")

                # Initial capital
                capital_input = ui.number(
                    label="Initial Capital (USDT)",
                    value=1000.0,
                    min=1.0,
                    step=100.0,
                    precision=2,
                ).classes("w-40")

            with ui.row().classes("w-full gap-4 items-start"):
                # Date range
                days_back_input = ui.number(
                    label="Lookback (days)",
                    value=30,
                    min=1,
                    max=365,
                    step=1,
                ).classes("w-32")
                ui.label(
                    "Backtest period = last N days from now. "
                    "A warmup of 200 candles is automatically added before the start."
                ).classes("text-xs text-slate-500")

            # Strategy selection
            with ui.row().classes("w-full gap-4 items-center mt-2"):
                ui.label("Strategies:").classes("text-sm font-medium")
                strategy_toggles: dict[str, ui.switch] = {}
                for name in strategy_names:
                    # Check if the strategy is enabled in current config
                    strat_cfg = (launcher_config.get("strategies") or {}).get(name) or {}
                    strategy_toggles[name] = ui.switch(
                        name,
                        value=bool(strat_cfg.get("enabled", False)),
                    ).props("dense color=primary")
                    strategy_toggles[name]

            # Run button + progress
            with ui.row().classes("w-full items-center gap-4 mt-2"):
                run_button = ui.button("Run Backtest", icon="play_arrow", color="primary")
                progress_label = ui.label("").classes("text-sm text-slate-500")

        # ── Results area ──────────────────────────────────────────────
        results_container = ui.column().classes("w-full gap-2")

    # ── Backtest runner ───────────────────────────────────────────────

    async def run_backtest() -> None:
        if backtest_running["flag"]:
            ui.notify("A backtest is already running", color="warning")
            return

        symbols = symbol_select.value or []
        if isinstance(symbols, str):
            symbols = [symbols]
        symbols = [s.upper() for s in symbols if s]
        if not symbols:
            ui.notify("Select at least one symbol", color="negative")
            return

        selected_strategies = [
            name for name, toggle in strategy_toggles.items() if toggle.value
        ]
        if not selected_strategies:
            ui.notify("Select at least one strategy", color="negative")
            return

        timeframe = timeframe_select.value or "4H"
        capital = float(capital_input.value or 1000.0)
        days_back = int(days_back_input.value or 30)

        # Compute start/end timestamps (ms epoch).
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days_back * 86_400_000

        bt_config = BacktestConfig(
            symbols=symbols,
            timeframe=timeframe,
            start_ts=start_ms,
            end_ts=now_ms,
            initial_capital=capital,
            strategy_names=selected_strategies,
            launcher_config=dict(launcher_config),
            strategy_config=dict(strategy_config),
            warmup_candles=200,
            disable_live_execution=True,
        )

        backtest_running["flag"] = True
        run_button.disable()
        progress_label.set_text("Starting...")
        results_container.clear()

        def progress_cb(progress: Any) -> None:
            """Update the progress label from the engine."""
            phase = getattr(progress, "phase", "")
            current = getattr(progress, "current", 0)
            total = getattr(progress, "total", 0)
            msg = getattr(progress, "message", "")
            if phase == "fetch":
                progress_label.set_text(f"Fetching data: {msg}")
            elif phase == "backtest":
                pct = (current / total * 100) if total > 0 else 0
                progress_label.set_text(f"Backtest: {pct:.0f}% ({current}/{total} candles)")
            elif phase == "metrics":
                progress_label.set_text("Computing metrics...")
            elif phase == "done":
                progress_label.set_text("Done")
            elif phase == "error":
                progress_label.set_text(f"Error: {msg}")

        engine = BacktestEngine(bt_config)
        result = await engine.run(progress_cb=progress_cb)

        backtest_running["flag"] = False
        run_button.enable()

        if result.is_error:
            ui.notify(f"Backtest failed: {result.error}", color="negative")
            progress_label.set_text(f"Error: {result.error}")
            return

        backtest_result["value"] = result
        ui.notify(
            f"Backtest complete: {len(result.trades)} trades, "
            f"net PnL {result.metrics.get('net_profit', 0):.2f} USDT",
            color="positive",
        )
        _render_results(result, results_container)

    def _render_results(result: Any, container: ui.column) -> None:
        """Render the backtest results into the results container."""
        with container:
            # ── Summary metrics ──────────────────────────────────────
            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-2"):
                ui.label("Summary").classes("text-lg font-semibold mb-2")
                m = result.metrics
                with ui.row().classes("w-full flex-wrap gap-4"):
                    _metric_card("Net Profit", f"{m.get('net_profit', 0):.2f} USDT", f"{m.get('net_profit_pct', 0):.1f}%")
                    _metric_card("Total Trades", str(m.get("total_trades", 0)), "")
                    _metric_card("Win Rate", f"{m.get('win_rate', 0):.1f}%", "")
                    _metric_card("Profit Factor", f"{m.get('profit_factor', 0):.2f}", "")
                    _metric_card("Max Drawdown", f"{m.get('max_drawdown', 0):.2f} USDT", f"{m.get('max_drawdown_pct', 0):.1f}%")
                    _metric_card("Final Equity", f"{m.get('final_equity', 0):.2f} USDT", "")
                    _metric_card("Sharpe/Candle", f"{m.get('sharpe_per_candle', 0):.4f}", "")
                    _metric_card("Avg Win", f"{m.get('average_win', 0):.2f}", "")
                    _metric_card("Avg Loss", f"{m.get('average_loss', 0):.2f}", "")
                    _metric_card("Expectancy", f"{m.get('expectancy', 0):.4f}", "")

            # ── Per-strategy breakdown ────────────────────────────────
            if result.per_strategy:
                with ui.card().classes("w-full rounded-lg border border-slate-200 mb-2"):
                    ui.label("Per-Strategy Breakdown").classes("text-lg font-semibold mb-2")
                    with ui.table(
                        columns=[
                            {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                            {"name": "trades", "label": "Trades", "field": "trades", "align": "right"},
                            {"name": "win_rate", "label": "Win Rate", "field": "win_rate", "align": "right"},
                            {"name": "net_profit", "label": "Net Profit", "field": "net_profit", "align": "right"},
                            {"name": "profit_factor", "label": "PF", "field": "profit_factor", "align": "right"},
                        ],
                        rows=[
                            {
                                "strategy": name,
                                "trades": sm.get("trades", 0),
                                "win_rate": f"{sm.get('win_rate', 0):.1f}%",
                                "net_profit": f"{sm.get('net_profit', 0):.2f}",
                                "profit_factor": f"{sm.get('profit_factor', 0):.2f}",
                            }
                            for name, sm in result.per_strategy.items()
                        ],
                    ).classes("w-full"):
                        pass

            # ── Equity curve ──────────────────────────────────────────
            if result.equity_curve:
                with ui.card().classes("w-full rounded-lg border border-slate-200 mb-2"):
                    ui.label("Equity Curve").classes("text-lg font-semibold mb-2")
                    eq_data = [
                        {"x": i, "y": p.equity}
                        for i, p in enumerate(result.equity_curve)
                    ]
                    ui.echart({
                        "tooltip": {"trigger": "axis"},
                        "xAxis": {"type": "category", "data": [p["x"] for p in eq_data]},
                        "yAxis": {"type": "value"},
                        "series": [{"data": [p["y"] for p in eq_data], "type": "line", "smooth": True}],
                    }).classes("w-full h-64")

            # ── Trade table ───────────────────────────────────────────
            if result.trades:
                with ui.card().classes("w-full rounded-lg border border-slate-200"):
                    ui.label(f"Trades ({len(result.trades)})").classes("text-lg font-semibold mb-2")
                    with ui.table(
                        columns=[
                            {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                            {"name": "direction", "label": "Dir", "field": "direction", "align": "left"},
                            {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                            {"name": "entry", "label": "Entry", "field": "entry", "align": "right"},
                            {"name": "close", "label": "Close", "field": "close", "align": "right"},
                            {"name": "reason", "label": "Reason", "field": "reason", "align": "left"},
                            {"name": "pnl", "label": "PnL", "field": "pnl", "align": "right"},
                            {"name": "pnl_pct", "label": "PnL %", "field": "pnl_pct", "align": "right"},
                        ],
                        rows=[
                            {
                                "symbol": t.symbol,
                                "direction": t.direction,
                                "strategy": t.strategy_name,
                                "entry": f"{t.entry_price:.4f}",
                                "close": f"{t.close_price:.4f}" if t.close_price else "—",
                                "reason": t.close_reason,
                                "pnl": f"{t.pnl:.2f}",
                                "pnl_pct": f"{t.pnl_pct:.2f}%",
                            }
                            for t in result.trades
                        ],
                    ).classes("w-full"):
                        pass

    def _metric_card(label: str, value: str, sub: str) -> None:
        """Render a small metric card."""
        with ui.column().classes("bg-slate-50 rounded-lg px-4 py-2 min-w-[120px]"):
            ui.label(label).classes("text-xs text-slate-500")
            ui.label(value).classes("text-lg font-bold")
            if sub:
                ui.label(sub).classes("text-xs text-slate-400")

    run_button.on("click", run_backtest)


def register_pages(app: FastAPI) -> None:
    settings = get_settings()
    try:
        default_zone = ZoneInfo(DEFAULT_FRONTEND_TIMEZONE)
    except ZoneInfoNotFoundError:
        default_zone = timezone.utc
    timezone_cache: dict[str, Any] = {"name": DEFAULT_FRONTEND_TIMEZONE, "zone": default_zone}

    def get_frontend_timezone_name() -> str:
        config = getattr(app.state, "runtime_config", {}) or {}
        value = str(config.get("frontend_timezone") or DEFAULT_FRONTEND_TIMEZONE).strip()
        return value or DEFAULT_FRONTEND_TIMEZONE

    def get_frontend_zone() -> ZoneInfo:
        tz_name = get_frontend_timezone_name()
        cached_name = timezone_cache.get("name")
        cached_zone = timezone_cache.get("zone")
        if cached_name != tz_name or cached_zone is None:
            try:
                cached_zone = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                cached_zone = default_zone
                tz_name = DEFAULT_FRONTEND_TIMEZONE
            timezone_cache["zone"] = cached_zone
            timezone_cache["name"] = tz_name
        return cached_zone  # type: ignore[return-value]

    def _ensure_aware(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def format_display_datetime(
        value: datetime | None,
        *,
        fmt: str = "%H:%M:%S %Z",
        fallback: str = "--",
    ) -> str:
        aware = _ensure_aware(value)
        if aware is None:
            return fallback
        try:
            zone = get_frontend_zone()
            return aware.astimezone(zone).strftime(fmt)
        except Exception:
            return fallback

    def format_iso_timestamp(
        raw: Any,
        *,
        fmt: str = "%H:%M:%S %Z",
        fallback: str = "--",
        passthrough_on_error: bool = True,
    ) -> str:
        if raw in (None, ""):
            return fallback
        if isinstance(raw, datetime):
            return format_display_datetime(raw, fmt=fmt, fallback=fallback)
        text = str(raw)
        try:
            candidate = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return text if passthrough_on_error else fallback
        return format_display_datetime(candidate, fmt=fmt, fallback=fallback)

    def format_epoch_ms(raw: Any, *, fmt: str = "%H:%M", fallback: str = "--") -> str:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return fallback
        dt = datetime.fromtimestamp(value / 1000.0, timezone.utc)
        return format_display_datetime(dt, fmt=fmt, fallback=fallback)

    def format_now(fmt: str = "%H:%M:%S %Z") -> str:
        return format_display_datetime(datetime.now(timezone.utc), fmt=fmt)

    def get_refresh_interval() -> float:
        # The UI refresh interval is how often the page polls Redis for a fresh
        # snapshot — a cheap local operation.  It is intentionally decoupled from
        # poll_interval (how often the backend does an expensive REST round-trip).
        # A short fixed interval keeps the LIVE page responsive to private-WS
        # patches (positions/equity) that arrive within ~300 ms of an OKX event.
        return 3.0

    def _parse_timestamp(raw: str | None) -> datetime | None:
        if not raw:
            return None
        value = raw.strip()
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _snapshot_age(snapshot: dict[str, Any] | None) -> tuple[bool, str]:
        config = getattr(app.state, "runtime_config", {}) or {}
        max_age = int(
            config.get("snapshot_max_age_seconds")
            or settings.snapshot_max_age_seconds
        )
        if not snapshot:
            return True, "No snapshot yet"
        timestamp = _parse_timestamp(snapshot.get("generated_at"))
        if not timestamp:
            return True, "Snapshot timestamp missing"
        now = datetime.now(timezone.utc)
        delta = max(0, int((now - timestamp).total_seconds()))
        if delta > max_age:
            return True, f"{delta}s old (limit {max_age}s)"
        return False, f"{delta}s old (limit {max_age}s)"

    def _ticker_price(ticker: dict[str, Any] | None) -> float | None:
        if not ticker:
            return None
        for key in ("last", "lastPx", "px", "close", "askPx", "bidPx"):
            value = ticker.get(key)
            if value in (None, ""):
                continue
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0:
                return price
        return None

    def make_snapshot_store() -> SnapshotStore:
        async def fetch_snapshot() -> dict[str, Any]:
            state_service = getattr(app.state, "state_service", None)
            if not state_service:
                return {}
            snapshot = await state_service.get_market_snapshot()
            return snapshot or {}

        store = SnapshotStore(fetch_snapshot, interval=get_refresh_interval())
        store.start()
        return store

    def page_container() -> ui.element:
        container = ui.card().classes(
            "w-full max-w-6xl mx-auto bg-white/95 p-6 md:p-8 gap-6 shadow-sm"
        )
        container.style("border-radius: 1.25rem")
        return container

    def navigation(active: str) -> dict[str, ui.element]:
        nav_refs: dict[str, ui.element] = {}
        with ui.header().classes("bg-slate-900 text-white shadow-md").style("height:64px"):
            with ui.row().classes("w-full items-center px-4 gap-4"):
                ui.label("TAI2").classes("font-semibold tracking-wide text-lg hidden md:block")
                with ui.row().classes(
                    "flex-1 justify-center items-center gap-2 text-xs md:text-sm"
                ):
                    for label, path in NAV_LINKS:
                        link = ui.link(label, path).classes(
                            "text-white/70 no-underline px-2 py-1 rounded-md hover:text-white"
                        )
                        if label == active:
                            link.classes("bg-white/10 text-white font-semibold")
                        nav_refs[label] = link
                ui.label(format_now("%H:%M %Z")).classes(
                    "text-xs text-white/70"
                )
        return nav_refs

    def render_live_page() -> None:
        navigation("LIVE")
        wrapper = page_container()
        wrapper.style("max-width: 100%; width: 100%; margin-left: 0; margin-right: 0;")
        store = make_snapshot_store()

        last_snapshot = {"value": None}
        equity_refresh = {"last": 0.0}
        equity_timeframe_hours = {"value": 24.0}
        refresh_label: dict[str, ui.label | None] = {"widget": None}
        status_label: dict[str, ui.label | None] = {"widget": None}
        stale_indicator: dict[str, ui.element | None] = {"widget": None}
        manual_refresh_button: dict[str, ui.button | None] = {"widget": None}
        manual_refresh_state = {"busy": False}
        clear_feedback_button: dict[str, ui.button | None] = {"widget": None}
        clear_feedback_state = {"busy": False}
        daily_lock_reset_button: dict[str, ui.button | None] = {"widget": None}
        daily_lock_reset_state = {"busy": False}
        selected_position_symbol = {"value": None}
        execution_feed_refs: dict[str, Any] = {"container": None, "empty": None}
        page_client = ui.context.client
        risk_lock_refs: dict[str, Any] = {
            "card": None,
            "state": None,
            "detail": None,
            "meta": None,
            "hint": None,
            "button": None,
        }
        resume_lock_state = {"busy": False}
        strategy_status_refs: dict[str, Any] = {
            "skimming_row": None,
            "skimming_badge": None,
            "skimming_detail": None,
            "shotgun_row": None,
            "shotgun_badge": None,
            "shotgun_detail": None,
            "protector_row": None,
            "protector_badge": None,
            "protector_detail": None,
            "commutator_row": None,
            "commutator_badge": None,
            "commutator_detail": None,
            "alternator_row": None,
            "alternator_badge": None,
            "alternator_detail": None,
        }

        def _format_pct(value: Any) -> str:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return "--"
            return f"{numeric * 100:.2f}%"

        def render_risk_lock_status() -> None:
            card = risk_lock_refs.get("card")
            if card is None:
                return
            config = getattr(app.state, "runtime_config", {}) or {}
            risk_locks = config.get("risk_locks") or {}
            lock_state = risk_locks.get("daily_loss") if isinstance(risk_locks.get("daily_loss"), dict) else {}
            active = bool(lock_state.get("active"))
            paused = bool(lock_state.get("auto_prompt_disabled"))
            auto_prompt_enabled = bool(config.get("auto_prompt_enabled", False))
            if not active and not paused:
                card.set_visibility(False)
                return
            card.set_visibility(True)
            drop_label = _format_pct(lock_state.get("change_pct"))
            limit_label = _format_pct(lock_state.get("threshold_pct"))
            window_hours = lock_state.get("window_hours")
            locked_at = lock_state.get("locked_at")
            state_label = risk_lock_refs.get("state")
            detail_label = risk_lock_refs.get("detail")
            meta_label = risk_lock_refs.get("meta")
            hint_label = risk_lock_refs.get("hint")
            resume_button = risk_lock_refs.get("button")
            if active:
                if state_label:
                    state_label.set_text("Daily loss lock active")
                if detail_label:
                    window_label = f"{int(window_hours)}h" if isinstance(window_hours, (int, float)) else "window"
                    detail_label.set_text(
                        f"Equity dropped {drop_label} over the last {window_label} (cap {limit_label})."
                    )
                if meta_label:
                    meta_label.set_text(
                        f"Scheduler · {'ON' if auto_prompt_enabled else 'OFF'}"
                    )
                if hint_label:
                    hint_label.set_text("Resume unlocks once equity recovers above the limit.")
                if resume_button:
                    resume_button.disable()
            else:
                if state_label:
                    state_label.set_text("Daily loss lock cleared")
                if detail_label:
                    detail_label.set_text("Equity recovered, but the scheduler remains paused for manual review.")
                if meta_label:
                    meta_label.set_text(
                        f"Paused since {format_iso_timestamp(locked_at, fmt='%Y-%m-%d %H:%M %Z')}"
                    )
                if hint_label:
                    hint_label.set_text("Click resume to restart auto prompts.")
                if resume_button and not resume_lock_state.get("busy"):
                    resume_button.enable()

        async def reset_daily_loss_lock(*, force: bool = False) -> bool:
            runtime_config = getattr(app.state, "runtime_config", {}) or {}
            risk_locks = runtime_config.setdefault("risk_locks", {})
            lock_state = risk_locks.get("daily_loss") if isinstance(risk_locks.get("daily_loss"), dict) else {}
            if not lock_state:
                with page_client:
                    ui.notify("No daily loss lock to reset", color="info")
                return False
            if lock_state.get("active") and not force:
                with page_client:
                    ui.notify(
                        "Equity is still below the configured limit; lock remains active.",
                        color="warning",
                    )
                return False
            runtime_config["auto_prompt_enabled"] = True
            lock_state["active"] = False
            lock_state["auto_prompt_disabled"] = False
            lock_state["execution_alert_logged"] = False
            if force:
                lock_state["manual_override_active"] = True
                lock_state["manual_override_since"] = datetime.now(timezone.utc).isoformat()
            else:
                lock_state["manual_override_active"] = False
                lock_state.pop("manual_override_since", None)
            risk_locks["daily_loss"] = lock_state
            state_service = getattr(app.state, "state_service", None)
            if state_service:
                try:
                    await state_service.set_risk_locks(risk_locks)
                except Exception as exc:  # pragma: no cover - UI feedback only
                    logger.debug("Risk lock persistence skipped: %s", exc)
            scheduler = getattr(app.state, "prompt_scheduler", None)
            if scheduler:
                await scheduler.update_interval(runtime_config.get("auto_prompt_interval", 300))
                await scheduler.set_enabled(True)
            backend_events = getattr(app.state, "backend_events", None)
            if backend_events is not None:
                backend_events.append(
                    "Daily loss lock manually cleared via LIVE page"
                    + (" (forced)" if force else "")
                )
            with page_client:
                ui.notify(
                    "Daily loss lock cleared; auto prompt scheduler resumed"
                    if force
                    else "Auto prompt scheduler resumed",
                    color="positive",
                )
                render_risk_lock_status()
            return True

        async def resume_prompt_scheduler() -> None:
            if resume_lock_state["busy"]:
                return
            resume_lock_state["busy"] = True
            button = risk_lock_refs.get("button")
            with page_client:
                if button:
                    button.disable()
            try:
                await reset_daily_loss_lock(force=False)
            except Exception as exc:  # pragma: no cover - UI feedback
                with page_client:
                    ui.notify(f"Failed to resume scheduler: {exc}", color="negative")
            finally:
                resume_lock_state["busy"] = False
                with page_client:
                    if button:
                        button.enable()
                    render_risk_lock_status()

        async def force_reset_daily_loss_lock() -> None:
            if daily_lock_reset_state["busy"]:
                return
            daily_lock_reset_state["busy"] = True
            button = daily_lock_reset_button.get("widget")
            with page_client:
                if button:
                    button.disable()
            try:
                await reset_daily_loss_lock(force=True)
            except Exception as exc:  # pragma: no cover - UI feedback
                with page_client:
                    ui.notify(f"Failed to reset daily loss lock: {exc}", color="negative")
            finally:
                daily_lock_reset_state["busy"] = False
                with page_client:
                    if button:
                        button.enable()
                    render_risk_lock_status()

        def set_ws_status(_ignored: bool = True) -> None:
            label = status_label["widget"]
            if not label:
                return
            market_service = getattr(app.state, "market_service", None)
            if market_service is None:
                label.set_text("OKX WS: --")
                label.style("color: #64748b")
                return
            enabled, pub, priv = market_service.ws_connection_status
            if not enabled:
                label.set_text("OKX WS: disabled")
                label.style("color: #64748b")
            elif pub and priv:
                label.set_text("OKX WS: pub+priv ✓")
                label.style("color: #16a34a")
            elif pub:
                label.set_text("OKX WS: pub only")
                label.style("color: #d97706")
            elif priv:
                label.set_text("OKX WS: priv only")
                label.style("color: #d97706")
            else:
                label.set_text("OKX WS: connecting…")
                label.style("color: #dc2626")

        with wrapper:
            with ui.column().classes("w-full gap-4"):
                with ui.column().classes("w-full gap-4"):
                    # ── Status bar ────────────────────────────────────────────
                    with ui.row().classes(
                        "w-full items-center gap-2 text-xs pb-2 border-b border-slate-100"
                    ):
                        status_label["widget"] = ui.label("WS: IDLE").classes(
                            "text-xs font-semibold text-slate-500"
                        )
                        ui.label("|").classes("text-slate-300 select-none")
                        refresh_label["widget"] = ui.label("Last refresh: --").classes(
                            "text-xs text-slate-500"
                        )
                        ui.label("|").classes("text-slate-300 select-none")
                        next_prompt_label = ui.label("Next prompt: --").classes(
                            "text-xs text-slate-500"
                        )
                        ui.label("|").classes("text-slate-300 select-none")
                        prompt_elapsed_label = ui.label("").classes(
                            "text-xs text-slate-500"
                        )
                        prompt_elapsed_label.set_visibility(False)
                        notice = (
                            ui.label("· Snapshot stale")
                            .classes("text-xs font-semibold text-red-600 uppercase tracking-wide")
                        )
                        notice.set_visibility(False)
                        stale_indicator["widget"] = notice

                    # ── Cards + buttons (left 25%) | Equity chart (right 75%) ─
                    with ui.row().classes("w-full gap-4 items-start"):
                        # Left panel: 2×2 badge stats + action buttons
                        with ui.row().classes("flex-[1] min-w-0 gap-3 items-start"):
                            with ui.element("div").classes("grid grid-cols-2 gap-2 flex-1 min-w-0"):
                                balance_card = badge_stat("Account Equity", "--")
                                position_card = badge_stat("Active Positions", "--", color="accent")
                                openrouter_credit_card = badge_stat(
                                    "OpenRouter Credits",
                                    "--",
                                    color="info",
                                )
                                okx_fee_card = badge_stat(
                                    "OKX Fees",
                                    "--",
                                    color="negative",
                                )
                            with ui.column().classes("gap-2 shrink-0"):
                                refresh_btn = ui.button("Refresh Snapshot", icon="refresh")
                                refresh_btn.classes(
                                    "text-xs bg-slate-900 text-white px-3 py-1 rounded-lg hover:bg-slate-800"
                                )
                                manual_refresh_button["widget"] = refresh_btn
                                clear_btn = ui.button(
                                    "Clear Execution Feedback",
                                    icon="cleaning_services",
                                )
                                clear_btn.classes(
                                    "text-xs bg-amber-600 text-white px-3 py-1 rounded-lg hover:bg-amber-500"
                                )
                                clear_feedback_button["widget"] = clear_btn
                                reset_btn = ui.button(
                                    "RESET DAILY LOSS LIMIT",
                                    icon="warning_amber",
                                )
                                reset_btn.classes(
                                    "text-xs bg-rose-600 text-white px-3 py-1 rounded-lg hover:bg-rose-500"
                                )
                                daily_lock_reset_button["widget"] = reset_btn
                                reset_btn.on(
                                    "click",
                                    lambda _: asyncio.create_task(force_reset_daily_loss_lock()),
                                )
                                ui.separator().classes("my-1")
                                ui.label("Active Strategies").classes(
                                    "text-xs font-semibold text-slate-500 uppercase tracking-wide"
                                )
                                with ui.row().classes("items-center gap-1") as _sg_skim_row:
                                    strategy_status_refs["skimming_row"] = _sg_skim_row
                                    strategy_status_refs["skimming_badge"] = ui.badge(
                                        "Skimming", color="grey"
                                    ).props("rounded")
                                    strategy_status_refs["skimming_detail"] = ui.label("").classes(
                                        "text-xs text-slate-500"
                                    )
                                with ui.row().classes("items-center gap-1") as _sg_shot_row:
                                    strategy_status_refs["shotgun_row"] = _sg_shot_row
                                    strategy_status_refs["shotgun_badge"] = ui.badge(
                                        "Shotgun", color="grey"
                                    ).props("rounded")
                                    strategy_status_refs["shotgun_detail"] = ui.label("").classes(
                                        "text-xs text-slate-500"
                                    )
                                with ui.row().classes("items-center gap-1") as _sg_prot_row:
                                    strategy_status_refs["protector_row"] = _sg_prot_row
                                    strategy_status_refs["protector_badge"] = ui.badge(
                                        "Protector", color="grey"
                                    ).props("rounded")
                                    strategy_status_refs["protector_detail"] = ui.label("").classes(
                                        "text-xs text-slate-500"
                                    )
                                with ui.row().classes("items-center gap-1") as _sg_cmtr_row:
                                    strategy_status_refs["commutator_row"] = _sg_cmtr_row
                                    strategy_status_refs["commutator_badge"] = ui.badge(
                                        "Commutator", color="grey"
                                    ).props("rounded")
                                    strategy_status_refs["commutator_detail"] = ui.label("").classes(
                                        "text-xs text-slate-500"
                                    )
                                with ui.row().classes("items-center gap-1") as _sg_altr_row:
                                    strategy_status_refs["alternator_row"] = _sg_altr_row
                                    strategy_status_refs["alternator_badge"] = ui.badge(
                                        "Alternator", color="grey"
                                    ).props("rounded")
                                    strategy_status_refs["alternator_detail"] = ui.label("").classes(
                                        "text-xs text-slate-500"
                                    )

                        # Right panel: equity chart (~75%)
                        with ui.column().classes("flex-[3] min-w-0 gap-2"):
                            with ui.row().classes("w-full justify-between items-center"):
                                ui.label("Total Equity").classes("text-sm font-medium text-slate-600")
                                equity_timeframe_toggle = ui.toggle(
                                    {6: "6h", 12: "12h", 24: "24h", 72: "3d", 168: "7d", 720: "30d"},
                                    value=24,
                                ).props("dense unelevated")
                                equity_timeframe_toggle.on_value_change(
                                    lambda e: [
                                        equity_timeframe_hours.update({"value": float(e.value)}),
                                        asyncio.create_task(refresh_equity_chart()),
                                    ]
                                )
                            equity_chart = ui.echart(
                                {
                                    "tooltip": {"trigger": "axis"},
                                    "grid": {"left": 40, "right": 20, "top": 20, "bottom": 30},
                                    "xAxis": {
                                        "type": "time",
                                        "axisLabel": {
                                            "color": "#475569",
                                            ":formatter": (
                                                "function(value) {"
                                                "const date = new Date(value);"
                                                "const hours = String(date.getHours()).padStart(2, '0');"
                                                "const minutes = String(date.getMinutes()).padStart(2, '0');"
                                                "if (hours === '00' && minutes === '00') {"
                                                "const year = date.getFullYear();"
                                                "const month = String(date.getMonth() + 1).padStart(2, '0');"
                                                "const day = String(date.getDate()).padStart(2, '0');"
                                                "return `${year}-${month}-${day} 00:00`;"
                                                "}"
                                                "return `${hours}:${minutes}`;"
                                                "}"
                                            ),
                                            "hideOverlap": True,
                                        },
                                        "splitNumber": 6,
                                    },
                                    "yAxis": {"type": "value", "axisLabel": {"color": "#475569"}},
                                    "series": [
                                        {
                                            "type": "line",
                                            "name": "Total Equity",
                                            "data": [],
                                            "smooth": True,
                                            "lineStyle": {"color": "#0ea5e9", "width": 2},
                                            "areaStyle": {"color": "rgba(14,165,233,0.15)"},
                                            "showSymbol": False,
                                        }
                                    ],
                                }
                            ).classes("w-full h-64 bg-white rounded-lg shadow")

                    # ── Risk lock card (full width, below the top row) ─────────
                    lock_card = ui.card().classes(
                        "w-full p-4 gap-2 bg-rose-50/80 border border-rose-200 rounded-2xl shadow-sm"
                    )
                    lock_card.set_visibility(False)
                    risk_lock_refs["card"] = lock_card
                    with lock_card:
                        risk_lock_refs["state"] = ui.label("Daily loss lock active").classes(
                            "text-xs font-semibold tracking-wide uppercase text-rose-600"
                        )
                        risk_lock_refs["detail"] = ui.label(
                            "Equity drop exceeded the configured daily cap."
                        ).classes("text-sm text-rose-800")
                        risk_lock_refs["meta"] = ui.label("Scheduler · OFF").classes(
                            "text-xs text-rose-700"
                        )
                        risk_lock_refs["hint"] = ui.label(
                            "Resume unlocks once equity recovers above the limit."
                        ).classes("text-[11px] text-slate-500")
                        resume_button = ui.button(
                            "Reset Lock & Resume Auto Prompt",
                            icon="restart_alt",
                        )
                        resume_button.classes(
                            "text-xs bg-slate-900 text-white px-3 py-1 rounded-lg hover:bg-slate-800"
                        )
                        resume_button.disable()
                        resume_button.on(
                            "click",
                            lambda _: asyncio.create_task(resume_prompt_scheduler()),
                        )
                        risk_lock_refs["button"] = resume_button

                    positions_table = ui.table(
                        columns=[
                            {"name": "symbol", "label": "Symbol", "field": "symbol"},
                            {"name": "side", "label": "Side", "field": "side"},
                            {"name": "mode", "label": "Mode", "field": "mode"},
                            {"name": "size", "label": "Size", "field": "size"},
                            {"name": "size_usd", "label": "Size (USDT)", "field": "size_usd"},
                            {"name": "entry", "label": "Entry", "field": "entry"},
                            {"name": "current", "label": "Current", "field": "current"},
                            {"name": "tp", "label": "TP", "field": "tp"},
                            {"name": "sl", "label": "SL", "field": "sl"},
                            {"name": "last_trade", "label": "Last Trade", "field": "last_trade"},
                            {"name": "pnl", "label": "PNL", "field": "pnl"},
                            {"name": "pnl_pct", "label": "PNL %", "field": "pnl_pct"},
                            {"name": "leverage", "label": "Leverage", "field": "leverage"},
                        ],
                        rows=[],
                        row_key="symbol",
                    ).classes("w-full font-semibold cursor-pointer")

                    positions_table.add_slot(
                        "body-cell-pnl",
                        """
                        <q-td :props="props" :class="props.row.symbol === 'TOTAL' ? 'border-t border-slate-300' : ''">
                            <span :class="props.row.pnl_cls">{{ props.value }}</span>
                        </q-td>
                        """,
                    )
                    positions_table.add_slot(
                        "body-cell-pnl_pct",
                        """
                        <q-td :props="props" :class="props.row.symbol === 'TOTAL' ? 'border-t border-slate-300' : ''">
                            <span :class="props.row.pnl_pct_cls">{{ props.value }}</span>
                        </q-td>
                        """,
                    )
                    positions_table.add_slot(
                        "body-cell-symbol",
                        """
                        <q-td :props="props" :class="props.row.symbol === 'TOTAL' ? 'border-t border-slate-300' : ''">
                            <span :class="props.row.symbol === 'TOTAL' ? 'font-bold text-slate-700 uppercase tracking-wide' : ''">{{ props.value }}</span>
                        </q-td>
                        """,
                    )
                    positions_table.add_slot(
                        "body-cell-size_usd",
                        """
                        <q-td :props="props" :class="props.row.symbol === 'TOTAL' ? 'border-t border-slate-300' : ''">
                            <span :class="props.row.symbol === 'TOTAL' ? 'font-bold' : ''">{{ props.value }}</span>
                        </q-td>
                        """,
                    )

                    chart_series: dict[str, Any] = {"symbol": None}
                    chart_container = ui.card().classes(
                        "w-full bg-white rounded-xl shadow-sm border border-slate-200"
                    )
                    chart_container.set_visibility(False)
                    with chart_container:
                        chart_label = ui.label("Select a position to view candles").classes(
                            "text-sm text-slate-500"
                        )
                        chart_widget = ui.echart(
                            {
                                "title": {"text": "Position Candles", "left": "center", "textStyle": {"color": "#0f172a", "fontSize": 14}},
                                "tooltip": {
                                    "trigger": "axis",
                                    "axisPointer": {"type": "cross", "link": [{"xAxisIndex": "all"}]},
                                    "backgroundColor": "rgba(15,23,42,0.9)",
                                    "borderColor": "rgba(15,23,42,0.4)",
                                    "textStyle": {"color": "#f8fafc"},
                                },
                                "grid": {"left": 40, "right": 20, "top": 35, "bottom": 60},
                                "xAxis": {
                                    "type": "category",
                                    "data": [],
                                    "axisLabel": {"color": "#475569"},
                                    "boundaryGap": False,
                                },
                                "yAxis": {"type": "value", "axisLabel": {"color": "#475569"}, "scale": True},
                                "dataZoom": [
                                    {
                                        "type": "inside",
                                        "xAxisIndex": [0],
                                        "filterMode": "filter",
                                        "zoomOnMouseWheel": False,
                                        "moveOnMouseMove": True,
                                        "moveOnMouseWheel": True,
                                        "minSpan": 5,
                                    },
                                    {
                                        "type": "slider",
                                        "xAxisIndex": [0],
                                        "height": 18,
                                        "bottom": 10,
                                        "backgroundColor": "rgba(15,23,42,0.05)",
                                        "dataBackground": {
                                            "areaStyle": {"color": "rgba(15,23,42,0.15)"},
                                            "lineStyle": {"color": "rgba(15,23,42,0.4)"},
                                        },
                                        "selectedDataBackground": {
                                            "areaStyle": {"color": "rgba(14,165,233,0.35)"},
                                            "lineStyle": {"color": "#0ea5e9"},
                                        },
                                    },
                                ],
                                "brush": {
                                    "xAxisIndex": "all",
                                    "toolbox": ["rect", "keep", "clear"],
                                    "brushLink": "all",
                                    "throttleType": "debounce",
                                    "throttleDelay": 300,
                                },
                                "series": [
                                    {
                                        "type": "candlestick",
                                        "name": "OHLC",
                                        "data": [],
                                        "itemStyle": {
                                            "color": "#10b981",
                                            "color0": "#f87171",
                                            "borderColor": "#059669",
                                            "borderColor0": "#dc2626",
                                        },
                                        "markLine": {
                                            "symbol": ["none", "none"],
                                            "lineStyle": {"type": "dashed", "width": 1.5, "color": "#94a3b8"},
                                            "label": {
                                                "color": "#0f172a",
                                                "backgroundColor": "rgba(255,255,255,0.85)",
                                                "padding": [2, 4],
                                                "borderRadius": 4,
                                            },
                                            "data": [],
                                        },
                                    }
                                ],
                            }
                        ).classes("w-full h-[30rem]")
                        chart_widget.set_visibility(False)
                    chart_series["widget"] = chart_widget

                    def update_position_chart(symbol: str | None) -> None:
                        selected_position_symbol["value"] = symbol
                        chart = chart_series["widget"]
                        snapshot = last_snapshot["value"]
                        if not symbol or not snapshot:
                            chart_container.set_visibility(False)
                            chart_label.set_text("Select a position to view candles")
                            chart.set_visibility(False)
                            chart_series["symbol"] = None
                            return

                        market_data = snapshot.get("market_data") or {}
                        entry = (
                            market_data.get(symbol)
                            or market_data.get(symbol.upper())
                            or market_data.get(symbol.lower())
                        )
                        indicators = (entry or {}).get("indicators") or {}
                        ohlcv = indicators.get("ohlcv") or []
                        if not ohlcv:
                            chart_container.set_visibility(True)
                            chart_label.set_text(f"Candle data unavailable for {symbol}")
                            chart.set_visibility(False)
                            chart_series["symbol"] = symbol
                            return

                        def _to_float(value: Any) -> float | None:
                            try:
                                return float(value)
                            except (TypeError, ValueError):
                                return None

                        def _first_price(*values: Any) -> float | None:
                            for candidate in values:
                                price = _to_float(candidate)
                                if price is not None and price > 0:
                                    return price
                            return None

                        def _resolve_protection_lines(position_side: str | None) -> tuple[float | None, float | None]:
                            target_keys = [symbol, symbol.upper(), symbol.lower()]
                            protection = snapshot.get("position_protection") or {}
                            tp_value: float | None = None
                            sl_value: float | None = None
                            for key in target_keys:
                                meta = protection.get(key)
                                if not isinstance(meta, dict):
                                    continue
                                if tp_value is None:
                                    tp_value = _first_price(
                                        meta.get("take_profit"),
                                        meta.get("tpTriggerPx"),
                                        meta.get("tp"),
                                    )
                                if sl_value is None:
                                    sl_value = _first_price(
                                        meta.get("stop_loss"),
                                        meta.get("slTriggerPx"),
                                        meta.get("sl"),
                                    )
                                if tp_value is not None and sl_value is not None:
                                    break

                            if tp_value is not None and sl_value is not None:
                                return tp_value, sl_value

                            positions = snapshot.get("positions") or []
                            symbol_upper = symbol.upper()
                            for pos in positions:
                                pos_symbol = str(pos.get("instId") or pos.get("symbol") or "").upper()
                                if pos_symbol != symbol_upper:
                                    continue
                                if tp_value is None:
                                    tp_value = _first_price(
                                        pos.get("tpTriggerPx"),
                                        pos.get("tpOrdPx"),
                                        pos.get("takeProfit"),
                                    )
                                if sl_value is None:
                                    sl_value = _first_price(
                                        pos.get("slTriggerPx"),
                                        pos.get("slOrdPx"),
                                        pos.get("stopLoss"),
                                    )
                                close_algo = pos.get("closeOrderAlgo")
                                if isinstance(close_algo, list):
                                    for algo in close_algo:
                                        if tp_value is None:
                                            tp_value = _first_price(
                                                algo.get("tpTriggerPx"),
                                                algo.get("tpOrdPx"),
                                            )
                                        if sl_value is None:
                                            sl_value = _first_price(
                                                algo.get("slTriggerPx"),
                                                algo.get("slOrdPx"),
                                            )
                                        if tp_value is not None and sl_value is not None:
                                            break
                                break

                            if tp_value is not None and sl_value is not None:
                                normalized_side = (position_side or "").upper()
                                if normalized_side == "LONG" and tp_value < sl_value:
                                    tp_value, sl_value = sl_value, tp_value
                                elif normalized_side == "SHORT" and tp_value > sl_value:
                                    tp_value, sl_value = sl_value, tp_value

                            return tp_value, sl_value

                        recent = ohlcv[-80:]
                        categories: list[str] = []
                        candles: list[list[float]] = []
                        for candle in recent:
                            ts_value = candle.get("ts")
                            label = "--"
                            if ts_value is not None:
                                label_candidate = format_epoch_ms(ts_value, fmt="%H:%M")
                                label = label_candidate if label_candidate != "--" else str(ts_value)
                            open_px = _to_float(candle.get("open"))
                            close_px = _to_float(candle.get("close"))
                            low_px = _to_float(candle.get("low"))
                            high_px = _to_float(candle.get("high"))
                            if None in (open_px, close_px, low_px, high_px):
                                continue
                            categories.append(label)
                            candles.append([open_px, close_px, low_px, high_px])

                        if not categories or not candles:
                            chart_container.set_visibility(True)
                            chart_label.set_text(f"Candle data unavailable for {symbol}")
                            chart.set_visibility(False)
                            chart_series["symbol"] = symbol
                            return

                        chart_container.set_visibility(True)
                        chart_label.set_text(f"{symbol} recent candles")
                        chart.options.setdefault("title", {})["text"] = f"{symbol} Candles"
                        chart.options["xAxis"]["data"] = categories
                        chart.options["series"][0]["data"] = candles
                        position_side = None
                        entry_price_value: float | None = None
                        positions_list = snapshot.get("positions") or []
                        for candidate in positions_list:
                            pos_symbol = str(candidate.get("instId") or candidate.get("symbol") or "").upper()
                            if pos_symbol == symbol.upper():
                                side_value = (candidate.get("posSide") or candidate.get("side") or "").upper()
                                if not side_value:
                                    try:
                                        size_val = float(candidate.get("pos") or candidate.get("size") or 0)
                                        if size_val > 0:
                                            side_value = "LONG"
                                        elif size_val < 0:
                                            side_value = "SHORT"
                                    except (TypeError, ValueError):
                                        side_value = ""
                                position_side = side_value
                                entry_price_value = _first_price(
                                    candidate.get("avgPx"),
                                    candidate.get("avgPrice"),
                                    candidate.get("openAvgPx"),
                                    candidate.get("openAvgPrice"),
                                    candidate.get("fillPx"),
                                )
                                break

                        tp_line, sl_line = _resolve_protection_lines(position_side)
                        series = chart.options["series"][0]
                        mark_line = series.setdefault(
                            "markLine",
                            {
                                "symbol": ["none", "none"],
                                "lineStyle": {"type": "dashed", "width": 1.5, "color": "#94a3b8"},
                                "label": {
                                    "color": "#0f172a",
                                    "backgroundColor": "rgba(255,255,255,0.85)",
                                    "padding": [2, 4],
                                    "borderRadius": 4,
                                },
                                "data": [],
                            },
                        )
                        mark_entries: list[dict[str, Any]] = []
                        if entry_price_value is not None:
                            mark_entries.append(
                                {
                                    "name": "Entry",
                                    "yAxis": entry_price_value,
                                    "lineStyle": {
                                        "color": "#1d4ed8",
                                        "type": "solid",
                                        "width": 1.5,
                                    },
                                    "label": {
                                        "formatter": f"Entry {entry_price_value:.4f}",
                                        "color": "#1d4ed8",
                                    },
                                }
                            )
                        if tp_line is not None:
                            mark_entries.append(
                                {
                                    "name": "Take Profit",
                                    "yAxis": tp_line,
                                    "lineStyle": {"color": "#047857"},
                                    "label": {"formatter": f"TP {tp_line:.4f}", "color": "#047857"},
                                }
                            )
                        if sl_line is not None:
                            mark_entries.append(
                                {
                                    "name": "Stop Loss",
                                    "yAxis": sl_line,
                                    "lineStyle": {"color": "#be123c"},
                                    "label": {"formatter": f"SL {sl_line:.4f}", "color": "#be123c"},
                                }
                            )
                        mark_line["data"] = mark_entries
                        chart_series["symbol"] = symbol
                        chart.set_visibility(True)
                        chart.update()

                    def handle_position_row_click(event: Any) -> None:
                        row_symbol: str | None = None
                        args = getattr(event, "args", None)
                        if isinstance(args, list):
                            for item in args:
                                if isinstance(item, dict) and item.get("symbol"):
                                    row_symbol = item.get("symbol")
                                    break
                                if isinstance(item, dict) and "row" in item and isinstance(item["row"], dict):
                                    row_symbol = item["row"].get("symbol")
                                    break
                        elif isinstance(args, dict):
                            payload = args.get("row") if isinstance(args.get("row"), dict) else args
                            if isinstance(payload, dict):
                                row_symbol = payload.get("symbol")

                        if row_symbol:
                            update_position_chart(row_symbol)

                    positions_table.on("rowClick", handle_position_row_click)

                with ui.card().classes(
                    "w-full p-4 gap-3 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    ui.label("LLM Insights & Execution Alerts").classes("text-xl font-semibold")
                    ui.label(
                        "Latest decisions and execution events per tracked symbol"
                    ).classes("text-sm text-slate-500")
                    llm_empty_state = ui.label("No LLM interactions yet.").classes(
                        "text-sm text-slate-400"
                    )
                    llm_card_container = ui.column().classes("w-full gap-3")

        def format_llm_timestamp(raw: str | None) -> str:
            return format_iso_timestamp(raw, fmt="%H:%M:%S %Z")

        def format_feedback_timestamp(raw: str | None) -> str:
            return format_iso_timestamp(raw, fmt="%H:%M:%S %Z")

        def format_decision_value(value: Any) -> str:
            if value is None or value == "":
                return "--"
            if isinstance(value, float):
                return f"{value:,.4f}".rstrip("0").rstrip(".")
            if isinstance(value, (int, bool)):
                return str(value)
            if isinstance(value, list):
                return ", ".join(format_decision_value(item) for item in value) or "[]"
            if isinstance(value, dict):
                return json.dumps(value, ensure_ascii=False)
            return str(value)

        _expanded_cards: set[str] = set()

        def refresh_llm_cards(snapshot: dict[str, Any] | None = None) -> None:
            llm_card_container.clear()
            interactions = getattr(app.state, "llm_interactions", {}) or {}
            items = sorted(
                interactions.values(),
                key=lambda entry: entry.get("timestamp") or "",
                reverse=True,
            )
            # Group execution feedback by normalised symbol (most recent first)
            feedback_by_symbol: dict[str, list[dict[str, Any]]] = {}
            if snapshot:
                fb_payload = snapshot.get("execution_feedback")
                if isinstance(fb_payload, list):
                    for fb_entry in reversed(fb_payload):
                        fb_sym = str(fb_entry.get("symbol") or "--").upper()
                        feedback_by_symbol.setdefault(fb_sym, []).append(fb_entry)
            if not items and not feedback_by_symbol:
                llm_empty_state.set_visibility(True)
                return
            llm_empty_state.set_visibility(False)
            _alert_level_classes = {
                "error": ("bg-rose-50 border-rose-200", "text-rose-700"),
                "warning": ("bg-amber-50 border-amber-200", "text-amber-700"),
                "info": ("bg-sky-50 border-sky-200", "text-sky-700"),
            }
            # Card background/border driven by worst alert level for the symbol
            _card_severity_classes = {
                "error": "bg-rose-50 border-rose-300",
                "warning": "bg-amber-50 border-amber-200",
            }
            _level_rank = {"error": 2, "warning": 1, "info": 0}

            def _worst_alert(alerts: list[dict[str, Any]]) -> tuple[str, str | None]:
                """Return (worst_level, message_of_worst) across all alerts."""
                worst_rank = -1
                worst_level = "none"
                worst_msg: str | None = None
                for a in alerts:
                    lvl = str(a.get("level") or "info").lower()
                    rank = _level_rank.get(lvl, 0)
                    if rank > worst_rank:
                        worst_rank = rank
                        worst_level = lvl
                        worst_msg = a.get("message") or None
                return worst_level, worst_msg

            def _render_alert_rows(alerts: list[dict[str, Any]]) -> None:
                for alert in alerts[-4:]:
                    level = str(alert.get("level") or "info").lower()
                    card_cls, text_cls = _alert_level_classes.get(
                        level, ("bg-slate-50 border-slate-200", "text-slate-600")
                    )
                    msg = alert.get("message") or "--"
                    ts = format_feedback_timestamp(alert.get("timestamp"))
                    with ui.row().classes(
                        f"w-full items-start gap-2 px-2 py-1.5 rounded-lg border {card_cls}"
                    ):
                        ui.label(level.upper()).classes(
                            f"text-[10px] font-bold shrink-0 mt-0.5 {text_cls}"
                        )
                        ui.label(msg).classes("text-xs flex-1 text-slate-700")
                        ui.label(ts).classes("text-[10px] shrink-0 text-slate-500")

            with llm_card_container:
                for entry in items:
                    symbol = entry.get("symbol") or "--"
                    sym_upper = symbol.upper()
                    decision = entry.get("decision") or {}
                    original_action = (decision.get("action") or "--").upper()
                    flipped = bool(entry.get("_flipped"))
                    effective_action = entry.get("_effective_action") or original_action
                    action_label = effective_action
                    if flipped:
                        action_label = f"{effective_action} (FLIPPED)"
                    origin = decision.get("_decision_origin") or ""
                    origin_suffix = f" ({origin})" if origin else ""
                    sym_alerts = feedback_by_symbol.pop(sym_upper, [])
                    worst_level, worst_msg = _worst_alert(sym_alerts)
                    alert_suffix = f" ({worst_msg})" if worst_msg and worst_level in ("error", "warning") else ""
                    header = f"{symbol} · {action_label} · {format_llm_timestamp(entry.get('timestamp'))}{origin_suffix}{alert_suffix}"
                    schema = entry.get("response_schema") or {}
                    confidence = decision.get("confidence")
                    confidence_label = (
                        f"{confidence:.2f}" if isinstance(confidence, (int, float)) else "--"
                    )
                    schema_props = list((schema.get("properties") or {}).keys())
                    ordered_fields: list[str] = [
                        name for name in schema_props if name in decision
                    ]
                    for key in decision.keys():
                        if key not in ordered_fields:
                            ordered_fields.append(key)
                    card_key = symbol
                    card_bg = _card_severity_classes.get(worst_level, "bg-white border-slate-200")
                    card = ui.expansion(header).classes(
                        f"w-full {card_bg} rounded-xl border shadow-sm"
                    )
                    if card_key in _expanded_cards:
                        card.open()

                    def _make_toggle(k: str):
                        def _on_change(e: Any) -> None:
                            if e.value:
                                _expanded_cards.add(k)
                            else:
                                _expanded_cards.discard(k)
                        return _on_change

                    card.on_value_change(_make_toggle(card_key))
                    with card:
                        decision_line = f"Decision: {effective_action} (conf {confidence_label})"
                        if flipped:
                            decision_line += f"  ·  LLM said {original_action}, flipped by guardrail"
                        ui.label(decision_line).classes("text-sm font-semibold text-slate-700")
                        with ui.column().classes("gap-2 text-xs text-slate-600"):
                            if not ordered_fields:
                                ui.label("No decision values returned.")
                            else:
                                schema_meta = schema.get("properties") or {}
                                for field in ordered_fields:
                                    rendered_value = format_decision_value(decision.get(field))
                                    desc = schema_meta.get(field, {}).get("description")
                                    ui.label(f"{field}: {rendered_value}").classes(
                                        "text-xs text-slate-700 font-medium"
                                    )
                                    if desc:
                                        ui.label(desc).classes("text-[11px] text-slate-400")
                        # Inline execution alerts for this symbol
                        if sym_alerts:
                            with ui.column().classes("w-full gap-1.5 mt-2 pt-2 border-t border-slate-200"):
                                ui.label("Execution Alerts").classes(
                                    "text-[11px] font-semibold uppercase tracking-wide text-slate-500"
                                )
                                _render_alert_rows(sym_alerts)
                # Orphan execution alerts (symbols with feedback but no LLM interaction)
                for sym, sym_fb_entries in sorted(feedback_by_symbol.items()):
                    orphan_worst, orphan_msg = _worst_alert(sym_fb_entries)
                    orphan_alert_suffix = f" ({orphan_msg})" if orphan_msg and orphan_worst in ("error", "warning") else ""
                    orphan_bg = _card_severity_classes.get(orphan_worst, "bg-white border-slate-200")
                    orphan_card = ui.expansion(f"{sym} · Execution Alerts{orphan_alert_suffix}").classes(
                        f"w-full {orphan_bg} rounded-xl border shadow-sm"
                    )
                    with orphan_card:
                        _render_alert_rows(sym_fb_entries)

        def render_execution_feedback(snapshot: dict[str, Any] | None) -> None:
            container = execution_feed_refs.get("container")
            empty_state = execution_feed_refs.get("empty")
            if container is None or empty_state is None:
                return
            entries: list[dict[str, Any]] = []
            if snapshot:
                payload = snapshot.get("execution_feedback")
                if isinstance(payload, list):
                    entries = payload
            container.clear()
            if not entries:
                empty_state.set_visibility(True)
                return
            empty_state.set_visibility(False)
            recent = list(entries)[-8:]
            recent.reverse()

            def _level_classes(level_value: str) -> tuple[str, str]:
                mapping = {
                    "error": ("border-rose-200 bg-rose-50", "text-rose-700"),
                    "warning": ("border-amber-200 bg-amber-50", "text-amber-700"),
                    "info": ("border-sky-200 bg-sky-50", "text-sky-700"),
                }
                return mapping.get(level_value, ("border-slate-200 bg-slate-50", "text-slate-600"))

            def _to_float(value: Any) -> float | None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            def _format_bool_flag(value: Any) -> str | None:
                if isinstance(value, bool):
                    return "yes" if value else "no"
                if isinstance(value, (int, float)):
                    return "yes" if value else "no"
                if value in (None, ""):
                    return None
                return str(value)

            def _format_currency(value: Any, currency: str | None) -> str | None:
                numeric = _to_float(value)
                if numeric is None:
                    return None
                label = f"{numeric:,.2f}"
                if currency:
                    label = f"{label} {currency}"
                return label

            def _format_size(value: Any) -> str | None:
                numeric = _to_float(value)
                if numeric is None:
                    return None
                return f"{numeric:,.4f}".rstrip("0").rstrip(".")

            def _first_float(*values: Any) -> float | None:
                for candidate in values:
                    numeric = _to_float(candidate)
                    if numeric is not None:
                        return numeric
                return None

            palette_styles = {
                "amber": {
                    "guidance_container": "w-full rounded-xl border border-amber-100 bg-white/70 px-3 py-2 gap-1",
                    "guidance_title": "text-[10px] font-semibold uppercase tracking-wide text-amber-600",
                    "guidance_row": "w-full justify-between text-[11px] text-amber-800 gap-2",
                    "guidance_label": "font-medium text-amber-700",
                    "guidance_value": "text-amber-900",
                    "sizing_container": "w-full rounded-xl border border-amber-50 bg-white px-3 py-2 gap-1",
                    "sizing_title": "text-[10px] font-semibold uppercase tracking-wide text-slate-600",
                    "sizing_row": "w-full justify-between text-[11px] text-slate-700 gap-2",
                    "sizing_label": "font-medium text-slate-700",
                    "sizing_value": "text-slate-900",
                },
                "slate": {
                    "guidance_container": "w-full rounded-xl border border-slate-100 bg-white px-3 py-2 gap-1",
                    "guidance_title": "text-[10px] font-semibold uppercase tracking-wide text-slate-500",
                    "guidance_row": "w-full justify-between text-[11px] text-slate-600 gap-2",
                    "guidance_label": "font-medium text-slate-600",
                    "guidance_value": "text-slate-900",
                    "sizing_container": "w-full rounded-xl border border-slate-100 bg-white px-3 py-2 gap-1",
                    "sizing_title": "text-[10px] font-semibold uppercase tracking-wide text-slate-500",
                    "sizing_row": "w-full justify-between text-[11px] text-slate-600 gap-2",
                    "sizing_label": "font-medium text-slate-600",
                    "sizing_value": "text-slate-900",
                },
            }

            def _derive_currency(source: dict[str, Any] | None, fallback: str | None) -> str | None:
                candidate = None
                if isinstance(source, dict):
                    candidate = source.get("quote_currency")
                candidate = candidate or fallback
                if candidate in (None, ""):
                    return None
                return str(candidate).upper()

            def _build_guidance_rows(source: dict[str, Any] | None, currency: str | None) -> list[tuple[str, str]]:
                rows: list[tuple[str, str]] = []
                if not isinstance(source, dict):
                    return rows
                required_gap = _to_float(source.get("required_gap"))
                if required_gap is not None:
                    gap_label = _format_currency(required_gap, currency)
                    if gap_label:
                        rows.append(("Required margin gap", gap_label))
                for label, key in (
                    ("Auto-seed configured", "auto_seed_configured"),
                    ("Auto-seed attempted", "auto_seed_attempted"),
                    ("Auto-seed success", "auto_seed_success"),
                ):
                    formatted = _format_bool_flag(source.get(key))
                    if formatted:
                        rows.append((label, formatted))
                seed_limit = _to_float(source.get("seed_limit"))
                if seed_limit is not None:
                    limit_label = _format_currency(seed_limit, currency)
                    if limit_label:
                        rows.append(("Seed cap", limit_label))
                funding_available = _to_float(source.get("funding_available"))
                if funding_available is not None:
                    funding_label = _format_currency(funding_available, currency)
                    if funding_label:
                        rows.append(("Funding wallet", funding_label))
                free_equity = _to_float(source.get("equity_available_for_trade"))
                if free_equity is not None:
                    equity_label = _format_currency(free_equity, currency)
                    if equity_label:
                        rows.append(("Free equity", equity_label))
                account_equity_value = _to_float(source.get("account_equity"))
                if account_equity_value is not None:
                    account_label = _format_currency(account_equity_value, currency)
                    if account_label:
                        rows.append(("Account equity", account_label))
                open_notional_value = _to_float(source.get("open_position_notional"))
                if open_notional_value is not None:
                    exposure_label = _format_currency(open_notional_value, currency)
                    if exposure_label:
                        rows.append(("Open exposure", exposure_label))
                if source.get("equity_clip_active"):
                    clip_reason = str(source.get("equity_clip_reason") or "free equity limit").replace(
                        "_",
                        " ",
                    )
                    clip_label = clip_reason.capitalize() if clip_reason else "Active"
                    rows.append(("Equity clip", clip_label))
                blocked_reason = source.get("blocked_reason")
                if blocked_reason:
                    rows.append(("Blocked reason", str(blocked_reason)))
                updated_at = source.get("updated_at")
                if updated_at:
                    rows.append(("Updated at", str(updated_at)))
                return rows

            def _build_sizing_rows(source: dict[str, Any] | None, currency: str | None) -> list[tuple[str, str]]:
                rows: list[tuple[str, str]] = []
                if not isinstance(source, dict):
                    return rows
                requested_notional_value = _first_float(
                    source.get("auto_downsize_previous_notional"),
                    source.get("initial_requested_notional"),
                    source.get("requested_notional"),
                    source.get("equity_clip_requested_notional"),
                )
                clipped_notional_value = _first_float(
                    source.get("auto_downsize_target_notional"),
                    source.get("requested_notional"),
                    source.get("equity_clip_target_notional"),
                )
                if requested_notional_value is not None:
                    label = _format_currency(requested_notional_value, currency)
                    if label:
                        rows.append(("Requested notional", label))
                if (
                    clipped_notional_value is not None
                    and (
                        requested_notional_value is None
                        or abs(clipped_notional_value - requested_notional_value) > 1e-6
                    )
                ):
                    label = _format_currency(clipped_notional_value, currency)
                    if label:
                        rows.append(("Clipped notional", label))
                if requested_notional_value is not None and clipped_notional_value is not None:
                    delta_value = (
                        source.get("auto_downsize_notional_delta")
                        or source.get("equity_clip_notional_delta")
                    )
                    delta_numeric = _to_float(delta_value)
                    if delta_numeric is None:
                        delta_numeric = requested_notional_value - clipped_notional_value
                    if delta_numeric and abs(delta_numeric) > 1e-6:
                        delta_label = _format_currency(delta_numeric, currency)
                        if delta_label:
                            rows.append(("Notional delta", delta_label))
                seed_attempt = _first_float(
                    source.get("auto_downsize_required_gap"),
                    source.get("required_gap"),
                )
                if seed_attempt is not None:
                    label = _format_currency(seed_attempt, currency)
                    if label:
                        rows.append(("Seed attempt", label))
                requested_size_value = _first_float(
                    source.get("auto_downsize_previous_size"),
                    source.get("initial_requested_size"),
                    source.get("requested_size"),
                    source.get("equity_clip_requested_size"),
                )
                clipped_size_value = _first_float(
                    source.get("auto_downsize_target_size"),
                    source.get("requested_size"),
                    source.get("equity_clip_target_size"),
                )
                if requested_size_value is not None:
                    label = _format_size(requested_size_value)
                    if label:
                        rows.append(("Requested size", label))
                if (
                    clipped_size_value is not None
                    and (
                        requested_size_value is None
                        or abs(clipped_size_value - requested_size_value) > 1e-9
                    )
                ):
                    label = _format_size(clipped_size_value)
                    if label:
                        rows.append(("Clipped size", label))
                scale_value = _to_float(source.get("auto_downsize_scale"))
                if scale_value is not None and scale_value > 0:
                    rows.append(("Downscale factor", f"{scale_value:.3f}×"))
                return rows

            def _render_margin_panels(
                source: dict[str, Any] | None,
                currency_hint: str | None,
                *,
                accent: str,
            ) -> None:
                if not isinstance(source, dict):
                    return
                currency = _derive_currency(source, currency_hint)
                guidance_rows = _build_guidance_rows(source, currency)
                sizing_rows = _build_sizing_rows(source, currency)
                if not guidance_rows and not sizing_rows:
                    return
                styles = palette_styles.get(accent, palette_styles["slate"])
                if guidance_rows:
                    with ui.column().classes(styles["guidance_container"]):
                        ui.label("Guidance snapshot").classes(styles["guidance_title"])
                        for label, value in guidance_rows:
                            with ui.row().classes(styles["guidance_row"]):
                                ui.label(label).classes(styles["guidance_label"])
                                ui.label(value).classes(styles["guidance_value"])
                if sizing_rows:
                    with ui.column().classes(styles["sizing_container"]):
                        ui.label("Sizing breakdown").classes(styles["sizing_title"])
                        for label, value in sizing_rows:
                            with ui.row().classes(styles["sizing_row"]):
                                ui.label(label).classes(styles["sizing_label"])
                                ui.label(value).classes(styles["sizing_value"])

            with container:
                for entry in recent:
                    level = str(entry.get("level") or "info").lower()
                    card_class, pill_text_class = _level_classes(level)
                    symbol = entry.get("symbol") or "--"
                    timestamp = format_feedback_timestamp(entry.get("timestamp"))
                    message = entry.get("message") or "--"
                    recommendation = entry.get("recommendation")
                    recommendation = recommendation if isinstance(recommendation, dict) else None
                    meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else None
                    margin_details = meta.get("margin_details") if meta else None
                    merged_margin_source: dict[str, Any] = {}
                    if isinstance(margin_details, dict):
                        merged_margin_source.update(margin_details)
                    with ui.column().classes(
                        f"w-full p-3 rounded-2xl border {card_class} shadow-sm gap-2"
                    ):
                        with ui.row().classes("w-full items-center justify-between gap-2"):
                            ui.label(symbol).classes("text-sm font-semibold text-slate-900")
                            with ui.row().classes("items-center gap-2"):
                                ui.label(level.upper()).classes(
                                    f"text-[11px] font-semibold tracking-wide px-2 py-1 rounded-full bg-white/70 {pill_text_class}"
                                )
                                ui.label(timestamp).classes("text-xs text-slate-500")
                        ui.label(message).classes("text-sm text-slate-700")
                        if recommendation:
                            currency = str(recommendation.get("quote_currency") or "").upper()
                            if merged_margin_source:
                                detail_source = dict(merged_margin_source)
                                detail_source.update(recommendation)
                            else:
                                detail_source = recommendation
                            needed = _to_float(recommendation.get("needed"))
                            seed_limit = _to_float(recommendation.get("seed_limit"))
                            funding_available = _to_float(recommendation.get("funding_available"))
                            with ui.column().classes(
                                "w-full rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 gap-1"
                            ):
                                ui.label("Recommendation").classes(
                                    "text-[11px] font-semibold text-amber-700 uppercase tracking-wide"
                                )
                                ui.label(recommendation.get("message") or "").classes(
                                    "text-sm text-amber-900 font-medium"
                                )
                                detail_bits: list[str] = []
                                if needed is not None:
                                    need_label = f"Need ≈{needed:,.2f}"
                                    if currency:
                                        need_label = f"{need_label} {currency}"
                                    detail_bits.append(need_label)
                                if seed_limit is not None:
                                    limit_label = f"Cap {seed_limit:,.2f}"
                                    if currency:
                                        limit_label = f"{limit_label} {currency}"
                                    detail_bits.append(limit_label)
                                if funding_available is not None:
                                    funding_label = f"Funding {funding_available:,.2f}"
                                    if currency:
                                        funding_label = f"{funding_label} {currency}"
                                    detail_bits.append(funding_label)
                                if detail_bits:
                                    ui.label(" · ".join(detail_bits)).classes("text-xs text-amber-700")
                                _render_margin_panels(detail_source, currency, accent="amber")
                        elif margin_details:
                            _render_margin_panels(margin_details, None, accent="slate")
                        chips: list[str] = []
                        if meta:
                            for key in ("code", "sCode"):
                                value = meta.get(key)
                                if value:
                                    chips.append(f"{key}: {value}")
                        if chips:
                            with ui.row().classes("flex-wrap gap-2"):
                                for chip in chips:
                                    ui.label(chip).classes(
                                        "text-[11px] px-2 py-1 rounded-full bg-white/70 text-slate-600 font-medium"
                                    )

        refresh_llm_cards()
        render_risk_lock_status()
        _t_llm_cards = ui.timer(15, lambda: refresh_llm_cards(last_snapshot["value"]))
        page_client.on_disconnect(_t_llm_cards.deactivate)
        page_client.on_delete(_t_llm_cards.deactivate)

        def _format_credit_amount(usage: dict[str, Any] | None) -> str:
            if not usage:
                return "--"
            amount = usage.get("remaining")
            granted = usage.get("granted")
            used = usage.get("used")
            if isinstance(granted, (int, float)) and isinstance(used, (int, float)):
                derived = max(0.0, granted - used)
                amount = derived
            if amount is None:
                return "--"
            currency = (usage.get("currency") or "USD").upper()
            if currency == "USD":
                return f"${amount:,.2f}"
            return f"{amount:,.2f} {currency}"

        def _format_credit_hint(usage: dict[str, Any] | None) -> str | None:
            if not usage:
                return None
            used = usage.get("used")
            granted = usage.get("granted")
            currency = (usage.get("currency") or "USD").upper()
            parts: list[str] = []
            if used is not None and granted is not None:
                if currency == "USD":
                    parts.append(f"Used ${used:,.2f} / ${granted:,.2f}")
                else:
                    parts.append(f"Used {used:,.2f} / {granted:,.2f} {currency}")
            elif used is not None:
                if currency == "USD":
                    parts.append(f"Used ${used:,.2f}")
                else:
                    parts.append(f"Used {used:,.2f} {currency}")
            resets_at = usage.get("resets_at")
            if resets_at:
                parts.append(
                    f"Renews {format_iso_timestamp(resets_at, fmt='%Y-%m-%d %H:%M %Z')}"
                )
            if not parts:
                return None
            return " · ".join(parts)

        def _update_credit_display(usage: dict[str, Any] | None) -> None:
            display_value = _format_credit_amount(usage)
            openrouter_credit_card.value_label.set_text(display_value)
            hint = _format_credit_hint(usage)
            if hint:
                openrouter_credit_card.hint_label.set_text(hint)
                openrouter_credit_card.hint_label.set_visibility(True)
            else:
                openrouter_credit_card.hint_label.set_visibility(False)

        def _get_fee_window_hours() -> float:
            config = getattr(app.state, "runtime_config", {}) or {}
            raw_value = config.get("fee_window_hours", 24.0)
            try:
                hours = float(raw_value)
            except (TypeError, ValueError):
                return 24.0
            return max(1.0, hours)

        def _update_fee_display(
            total_fee: float | None,
            *,
            window_hours: float,
            error: str | None = None,
        ) -> None:
            if error:
                okx_fee_card.value_label.set_text("--")
                okx_fee_card.hint_label.set_text(error)
                okx_fee_card.hint_label.set_visibility(True)
            elif total_fee is not None:
                okx_fee_card.value_label.set_text(f"${total_fee:,.2f}")
                okx_fee_card.hint_label.set_text(f"last {window_hours:g}h")
                okx_fee_card.hint_label.set_visibility(True)
            else:
                okx_fee_card.value_label.set_text("--")
                okx_fee_card.hint_label.set_visibility(False)

        async def refresh_openrouter_credits(force: bool = False) -> None:
            try:
                usage = await fetch_openrouter_credits(app, force_refresh=force)
            except Exception:
                usage = None
            _update_credit_display(usage)

        asyncio.create_task(refresh_openrouter_credits(True))
        _t_credits = ui.timer(300, lambda: asyncio.create_task(refresh_openrouter_credits(True)))
        page_client.on_disconnect(_t_credits.deactivate)
        page_client.on_delete(_t_credits.deactivate)

        async def refresh_okx_fees() -> None:
            window_hours = _get_fee_window_hours()
            try:
                total_fee = await fetch_okx_fees_window(window_hours)
            except Exception as exc:
                _update_fee_display(
                    None,
                    window_hours=window_hours,
                    error=f"OKX fees unavailable: {exc}",
                )
                return
            _update_fee_display(total_fee, window_hours=window_hours)

        asyncio.create_task(refresh_okx_fees())
        _t_fees = ui.timer(300, lambda: asyncio.create_task(refresh_okx_fees()))
        page_client.on_disconnect(_t_fees.deactivate)
        page_client.on_delete(_t_fees.deactivate)

        async def refresh_equity_chart() -> None:
            try:
                history = await fetch_equity_window(hours=equity_timeframe_hours["value"])
            except Exception:
                return
            if not history:
                return
            points: list[list[float | str | None]] = []
            for entry in history:
                ts = entry.get("observed_at")
                timestamp_value: str | None = None
                if ts:
                    try:
                        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        timestamp_value = format_display_datetime(
                            parsed,
                            fmt="%Y-%m-%d %H:%M:%S",
                            fallback=ts,
                        )
                    except ValueError:
                        timestamp_value = ts
                if not timestamp_value:
                    continue
                value = entry.get("total_eq_usd") or entry.get("account_equity")
                number_value = round(float(value), 2) if value is not None else None
                points.append([timestamp_value, number_value])
            option = equity_chart.options
            option["series"][0]["data"] = points
            equity_chart.update()

        async def trigger_manual_refresh() -> None:
            if manual_refresh_state["busy"]:
                return
            manual_refresh_state["busy"] = True
            button = manual_refresh_button.get("widget")
            with page_client:
                if button:
                    button.disable()
            try:
                market_service = getattr(app.state, "market_service", None)
                if not market_service:
                    with page_client:
                        ui.notify("Market service unavailable", color="warning")
                    return
                snapshot = await market_service.refresh_snapshot(reason="manual")
                await store.refresh_now()
                await refresh_equity_chart()
                await refresh_openrouter_credits(True)
                if snapshot:
                    with page_client:
                        ui.notify("Live data refreshed", color="positive")
                else:
                    with page_client:
                        ui.notify("Snapshot refresh returned no data", color="warning")
            except Exception as exc:  # pragma: no cover - UI feedback
                with page_client:
                    ui.notify(f"Refresh failed: {exc}", color="negative")
            finally:
                manual_refresh_state["busy"] = False
                with page_client:
                    if button:
                        button.enable()

        async def trigger_clear_feedback() -> None:
            if clear_feedback_state["busy"]:
                return
            clear_feedback_state["busy"] = True
            button = clear_feedback_button.get("widget")
            with page_client:
                if button:
                    button.disable()
            try:
                market_service = getattr(app.state, "market_service", None)
                if not market_service:
                    with page_client:
                        ui.notify("Market service unavailable", color="warning")
                    return
                removed = market_service.clear_execution_feedback()
                with page_client:
                    if removed and last_snapshot["value"] is not None:
                        last_snapshot["value"]["execution_feedback"] = []
                        refresh_llm_cards(last_snapshot["value"])
                    if removed:
                        ui.notify(f"Cleared {removed} feedback entries", color="positive")
                    else:
                        ui.notify("No execution feedback to clear", color="info")
            except Exception as exc:  # pragma: no cover - UI feedback
                with page_client:
                    ui.notify(f"Feedback clear failed: {exc}", color="negative")
            finally:
                clear_feedback_state["busy"] = False
                with page_client:
                    if button:
                        button.enable()

        refresh_btn_widget = manual_refresh_button.get("widget")
        if refresh_btn_widget:
            refresh_btn_widget.on(
                "click",
                lambda _: asyncio.create_task(trigger_manual_refresh()),
            )

        clear_btn_widget = clear_feedback_button.get("widget")
        if clear_btn_widget:
            clear_btn_widget.on(
                "click",
                lambda _: asyncio.create_task(trigger_clear_feedback()),
            )

        def render_strategy_status() -> None:
            config = getattr(app.state, "runtime_config", {}) or {}
            strategy = config.get("strategy") or {}

            skim = strategy.get("skimming") or {}
            skim_enabled = bool(skim.get("enabled"))
            skim_badge = strategy_status_refs.get("skimming_badge")
            skim_detail = strategy_status_refs.get("skimming_detail")
            if skim_badge:
                if skim_enabled:
                    skim_badge.props("color=positive rounded")
                    tp = skim.get("threshold_pct")
                    sl = skim.get("stop_loss_pct")
                    dyn = bool(skim.get("dynamic_tp", False))
                    parts: list[str] = []
                    if tp is not None:
                        try:
                            parts.append(f"TP {'≤' if dyn else ''}{float(tp):.1f}% {'(Dynamic BB)' if dyn else ''}")
                        except (TypeError, ValueError):
                            pass
                    if sl is not None:
                        try:
                            parts.append(f"SL {float(sl):.1f}%")
                        except (TypeError, ValueError):
                            pass
                    detail_text = " · ".join(parts) if parts else ""
                else:
                    skim_badge.props("color=grey rounded")
                    detail_text = ""
            else:
                detail_text = ""
            if skim_detail:
                skim_detail.set_text(detail_text)

            shot = strategy.get("shotgun") or {}
            shot_enabled = bool(shot.get("enabled"))
            shot_badge = strategy_status_refs.get("shotgun_badge")
            shot_detail = strategy_status_refs.get("shotgun_detail")
            if shot_badge:
                if shot_enabled:
                    shot_badge.props("color=positive rounded")
                    tp_pct = shot.get("tp_pct")
                    tp_usd = shot.get("tp_usd")
                    sl_pct = shot.get("sl_pct")
                    sl_usd = shot.get("sl_usd")
                    shot_parts: list[str] = []
                    tp_bits: list[str] = []
                    sl_bits: list[str] = []
                    try:
                        if tp_pct is not None:
                            tp_bits.append(f"{float(tp_pct):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        if tp_usd is not None:
                            _v = float(tp_usd)
                            tp_bits.append("$" + f"{_v:.2f}".rstrip("0").rstrip("."))
                    except (TypeError, ValueError):
                        pass
                    try:
                        if sl_pct is not None:
                            sl_bits.append(f"{float(sl_pct):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        if sl_usd is not None:
                            _v = float(sl_usd)
                            sl_bits.append("$" + f"{_v:.2f}".rstrip("0").rstrip("."))
                    except (TypeError, ValueError):
                        pass
                    if tp_bits:
                        shot_parts.append("TP " + "/".join(tp_bits))
                    if sl_bits:
                        shot_parts.append("SL " + "/".join(sl_bits))
                    if bool(shot.get("close_only_negative")):
                        shot_parts.append("-PnL only")
                    shot_detail_text = " · ".join(shot_parts) if shot_parts else ""
                else:
                    shot_badge.props("color=grey rounded")
                    shot_detail_text = ""
            else:
                shot_detail_text = ""
            if shot_detail:
                shot_detail.set_text(shot_detail_text)

            prot = strategy.get("protector") or {}
            prot_enabled = bool(prot.get("enabled"))
            prot_badge = strategy_status_refs.get("protector_badge")
            prot_detail = strategy_status_refs.get("protector_detail")
            if prot_badge:
                if prot_enabled:
                    prot_badge.props("color=positive rounded")
                    prot_parts: list[str] = []
                    try:
                        act = prot.get("activate_pct")
                        if act is not None:
                            prot_parts.append(f"act {float(act):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        step = prot.get("step_pct")
                        if step is not None:
                            prot_parts.append(f"step {float(step):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        lock = prot.get("lock_ratio")
                        if lock is not None:
                            prot_parts.append(f"lock {float(lock):.0%}")
                    except (TypeError, ValueError):
                        pass
                    prot_detail_text = " · ".join(prot_parts) if prot_parts else ""
                else:
                    prot_badge.props("color=grey rounded")
                    prot_detail_text = ""
            else:
                prot_detail_text = ""
            if prot_detail:
                prot_detail.set_text(prot_detail_text)

            cmtr = strategy.get("commutator") or {}
            cmtr_enabled = bool(cmtr.get("enabled"))
            cmtr_badge = strategy_status_refs.get("commutator_badge")
            cmtr_detail = strategy_status_refs.get("commutator_detail")
            if cmtr_badge:
                if cmtr_enabled:
                    cmtr_badge.props("color=positive rounded")
                    cmtr_parts: list[str] = []
                    try:
                        rlp = cmtr.get("reverse_at_loss_pct")
                        if rlp is not None:
                            cmtr_parts.append(f"loss {float(rlp):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        rlu = cmtr.get("reverse_at_loss_usd")
                        if rlu is not None:
                            cmtr_parts.append(f"{float(rlu):.0f}$ loss")
                    except (TypeError, ValueError):
                        pass
                    try:
                        mf = cmtr.get("max_flips")
                        if mf is not None:
                            cmtr_parts.append(f"×{int(mf)} flips")
                    except (TypeError, ValueError):
                        pass
                    cmtr_detail_text = " · ".join(cmtr_parts) if cmtr_parts else ""
                else:
                    cmtr_badge.props("color=grey rounded")
                    cmtr_detail_text = ""
            else:
                cmtr_detail_text = ""
            if cmtr_detail:
                cmtr_detail.set_text(cmtr_detail_text)

            altr = strategy.get("alternator") or {}
            altr_enabled = bool(altr.get("enabled"))
            altr_badge = strategy_status_refs.get("alternator_badge")
            altr_detail = strategy_status_refs.get("alternator_detail")
            if altr_badge:
                if altr_enabled:
                    altr_badge.props("color=positive rounded")
                    altr_parts: list[str] = []
                    try:
                        rpp = altr.get("reverse_at_profit_pct")
                        if rpp is not None:
                            altr_parts.append(f"flip @+{float(rpp):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        rlp = altr.get("restart_at_loss_pct")
                        if rlp is not None:
                            altr_parts.append(f"restart @-{float(rlp):.1f}%")
                    except (TypeError, ValueError):
                        pass
                    try:
                        mr = altr.get("max_reversals")
                        if mr is not None:
                            altr_parts.append(f"x{int(mr)} rev")
                    except (TypeError, ValueError):
                        pass
                    altr_detail_text = " · ".join(altr_parts) if altr_parts else ""
                else:
                    altr_badge.props("color=grey rounded")
                    altr_detail_text = ""
            else:
                altr_detail_text = ""
            if altr_detail:
                altr_detail.set_text(altr_detail_text)

        def update_snapshot_health(snapshot: dict[str, Any] | None) -> None:
            notice = stale_indicator.get("widget")
            if not notice:
                return
            stale, detail = _snapshot_age(snapshot)
            notice.set_visibility(stale)
            if stale:
                notice.set_text(f"Snapshot stale · {detail}")
            else:
                notice.set_text("")

        def _build_shotgun_baseline_label(current_equity: float) -> str:
            market_service = getattr(app.state, "market_service", None)
            baseline: float | None = getattr(market_service, "_shotgun_baseline_equity", None)
            if baseline is None:
                return "--"
            diff = current_equity - baseline
            sign = "+" if diff >= 0 else ""
            return f"${baseline:,.2f} / {sign}{diff:,.2f}"

        def update(snapshot: dict[str, Any] | None) -> None:
            last_snapshot["value"] = snapshot
            set_ws_status(snapshot is not None)
            refresh_llm_cards(snapshot)
            render_risk_lock_status()
            render_strategy_status()
            update_snapshot_health(snapshot)
            label = refresh_label["widget"]
            if label:
                if snapshot:
                    label.set_text(f"Last refresh: {format_now('%H:%M:%S %Z')}")
                else:
                    label.set_text("Last refresh: --")
            if not snapshot:
                return
            positions = snapshot.get("positions") or []
            symbols = snapshot.get("symbols") or []
            market_data = snapshot.get("market_data") or {}
            position_activity = snapshot.get("position_activity") or {}
            position_protection = snapshot.get("position_protection") or {}
            selected_symbol = snapshot.get("symbol")
            if not selected_symbol and symbols:
                selected_symbol = symbols[0]
            market_entry = market_data.get(selected_symbol, {})
            ticker = market_entry.get("ticker") or snapshot.get("ticker") or {}
            funding = market_entry.get("funding_rate") or snapshot.get("funding_rate") or {}
            equity_value = snapshot.get("total_eq_usd") or snapshot.get("account_equity")
            try:
                total_equity = float(equity_value or 0)
            except (TypeError, ValueError):
                total_equity = 0.0
            balance_card.value_label.set_text(f"${total_equity:,.2f}")
            position_card.value_label.set_text(str(len(positions)))
            position_lookup: dict[str, dict[str, Any]] = {}
            for pos in positions:
                raw_key = pos.get("instId") or pos.get("symbol")
                key = str(raw_key).strip() if raw_key else None
                if not key or key in position_lookup:
                    continue
                position_lookup[key] = pos

            def to_float(value: Any) -> float | None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            def format_activity_ts(value: Any) -> str:
                return format_iso_timestamp(value, fmt="%Y-%m-%d %H:%M:%S %Z")

            def _first_price(*values: Any) -> float | None:
                for candidate in values:
                    price = to_float(candidate)
                    if price is not None and price > 0:
                        return price
                return None

            def _resolve_dict_entry(mapping: dict[str, Any], symbol_key: str) -> Any:
                if not mapping or not symbol_key:
                    return None
                candidates = []
                normalized = str(symbol_key).strip()
                if normalized:
                    candidates.append(normalized)
                    candidates.append(normalized.upper())
                    candidates.append(normalized.lower())
                unique = []
                seen = set()
                for candidate in candidates:
                    if candidate and candidate not in seen:
                        seen.add(candidate)
                        unique.append(candidate)
                for candidate in unique:
                    value = mapping.get(candidate)
                    if value is not None:
                        return value
                return None

            def extract_tp_sl(
                position: dict[str, Any],
                cached_meta: dict[str, Any] | None,
                position_side: str | None,
            ) -> tuple[float | None, float | None]:
                tp_value = _first_price(
                    position.get("tpTriggerPx"),
                    position.get("tpOrdPx"),
                    position.get("takeProfit"),
                    position.get("tai2_take_profit"),
                )
                sl_value = _first_price(
                    position.get("slTriggerPx"),
                    position.get("slOrdPx"),
                    position.get("stopLoss"),
                    position.get("tai2_stop_loss"),
                )
                if cached_meta:
                    if tp_value is None:
                        tp_value = _first_price(
                            cached_meta.get("take_profit"),
                            cached_meta.get("tp"),
                            cached_meta.get("tpTriggerPx"),
                        )
                    if sl_value is None:
                        sl_value = _first_price(
                            cached_meta.get("stop_loss"),
                            cached_meta.get("sl"),
                            cached_meta.get("slTriggerPx"),
                        )
                close_order_algo = position.get("closeOrderAlgo")
                if isinstance(close_order_algo, list):
                    for algo in close_order_algo:
                        if tp_value is None:
                            tp_value = _first_price(
                                algo.get("tpTriggerPx"),
                                algo.get("tpOrdPx"),
                            )
                        if sl_value is None:
                            sl_value = _first_price(
                                algo.get("slTriggerPx"),
                                algo.get("slOrdPx"),
                            )
                        if tp_value is not None and sl_value is not None:
                            break
                        order_type = str(algo.get("orderType") or "").lower()
                        trigger_px = _first_price(
                            algo.get("triggerPx"),
                            algo.get("ordPx"),
                            algo.get("closePx"),
                        )
                        if trigger_px is None:
                            continue
                        if tp_value is None and order_type in {"take_profit", "tp"}:
                            tp_value = trigger_px
                        if sl_value is None and order_type in {"stop_loss", "sl"}:
                            sl_value = trigger_px
                        if tp_value is not None and sl_value is not None:
                            break
                if tp_value is not None and sl_value is not None:
                    normalized_side = (position_side or "").upper()
                    if normalized_side == "LONG" and tp_value < sl_value:
                        tp_value, sl_value = sl_value, tp_value
                    elif normalized_side == "SHORT" and tp_value > sl_value:
                        tp_value, sl_value = sl_value, tp_value
                return tp_value, sl_value

            def normalize_tp_sl_for_side(
                side_value: str | None,
                tp_value: float | None,
                sl_value: float | None,
            ) -> tuple[float | None, float | None]:
                if tp_value is None or sl_value is None:
                    return tp_value, sl_value
                normalized = (side_value or "").upper()
                if normalized == "LONG" and tp_value < sl_value:
                    return max(tp_value, sl_value), min(tp_value, sl_value)
                if normalized == "SHORT" and tp_value > sl_value:
                    return min(tp_value, sl_value), max(tp_value, sl_value)
                return tp_value, sl_value

            rows: list[dict[str, Any]] = []
            for symbol, pos in position_lookup.items():
                lookup_symbol = str(symbol).strip()
                market_entry_for_symbol = market_data.get(symbol) or {}
                ticker_info = market_entry_for_symbol.get("ticker") or {}
                if not ticker_info and symbol == snapshot.get("symbol"):
                    ticker_info = snapshot.get("ticker") or {}
                entry_price = to_float(pos.get("avgPx"))
                size_raw = to_float(pos.get("pos") or pos.get("size"))
                side = (pos.get("posSide") or pos.get("side") or "").upper()
                if not side and size_raw is not None:
                    side = "LONG" if size_raw >= 0 else "SHORT"
                if not side:
                    side = "--"
                if not size_raw and side in {"LONG", "SHORT"}:
                    size_abs = None
                else:
                    size_abs = abs(size_raw) if size_raw is not None else None
                if not size_abs or size_abs <= 0:
                    continue
                current_price = to_float(
                    ticker_info.get("last")
                    or ticker_info.get("px")
                    or pos.get("markPx")
                    or pos.get("last")
                )
                leverage_raw = pos.get("lever") or pos.get("leverage")
                leverage_display = str(leverage_raw) if leverage_raw not in (None, "") else "--"
                leverage_value = to_float(leverage_raw)

                margin_mode_raw = (
                    pos.get("mgnMode")
                    or pos.get("marginMode")
                    or pos.get("tdMode")
                    or pos.get("tradeMode")
                )
                if margin_mode_raw is None:
                    mode_display = "--"
                else:
                    normalized_mode = str(margin_mode_raw).strip().lower()
                    if normalized_mode in {"cross", "isolated"}:
                        mode_display = normalized_mode.capitalize()
                    else:
                        mode_display = normalized_mode.upper() if normalized_mode else "--"

                upl_value = to_float(pos.get("upl"))
                upl_ratio = to_float(pos.get("uplRatio"))

                fallback_pnl = None
                multiplier = -1.0 if side == "SHORT" else 1.0
                if (
                    entry_price
                    and entry_price != 0
                    and current_price is not None
                    and size_abs is not None
                ):
                    delta = current_price - entry_price
                    fallback_pnl = delta * size_abs * multiplier

                pnl = upl_value if upl_value is not None else fallback_pnl
                pnl_pct = upl_ratio * 100 if upl_ratio is not None else None
                if pnl_pct is None and pnl is not None and size_abs is not None and entry_price:
                    notional = entry_price * size_abs
                    margin_base = None
                    if leverage_value and leverage_value > 0:
                        margin_base = notional / leverage_value if leverage_value else None
                    else:
                        margin_base = notional
                    if margin_base:
                        pnl_pct = (pnl / margin_base) * 100

                if pnl is None:
                    pnl_color = "text-slate-900"
                elif pnl > 0:
                    pnl_color = "text-emerald-600 font-semibold"
                elif pnl < 0:
                    pnl_color = "text-rose-600 font-semibold"
                else:
                    pnl_color = "text-slate-900"

                if pnl_pct is None:
                    pnl_pct_color = "text-slate-900"
                elif pnl_pct > 0:
                    pnl_pct_color = "text-emerald-600 font-semibold"
                elif pnl_pct < 0:
                    pnl_pct_color = "text-rose-600 font-semibold"
                else:
                    pnl_pct_color = "text-slate-900"

                activity_meta = _resolve_dict_entry(position_activity, lookup_symbol) or {}
                last_trade_label = "--"
                position_last_trade = pos.get("tai2_last_trade")
                if position_last_trade:
                    last_trade_label = format_activity_ts(position_last_trade)
                elif isinstance(activity_meta, dict):
                    last_trade_label = format_activity_ts(activity_meta.get("last_trade"))
                elif isinstance(activity_meta, str):
                    last_trade_label = format_activity_ts(activity_meta)

                protection_meta = _resolve_dict_entry(position_protection, lookup_symbol)
                tp_value, sl_value = extract_tp_sl(
                    pos,
                    protection_meta if isinstance(protection_meta, dict) else None,
                    side,
                )
                tp_value, sl_value = normalize_tp_sl_for_side(side, tp_value, sl_value)

                size_notional_usd = to_float(
                    pos.get("notionalUsd")
                    or pos.get("notional_usd")
                )
                if size_notional_usd is None and size_abs is not None and current_price is not None:
                    size_notional_usd = size_abs * current_price

                row = {
                    "symbol": symbol,
                    "side": side if side != "" else "--",
                    "size": f"{size_abs:,.4f}" if size_abs is not None else "--",
                    "size_usd": f"{size_notional_usd:,.2f}" if size_notional_usd is not None else "--",
                    "entry": f"{entry_price:,.4f}" if entry_price is not None else "--",
                    "current": f"{current_price:,.4f}" if current_price is not None else "--",
                    "tp": f"{tp_value:,.4f}" if tp_value is not None else "--",
                    "sl": f"{sl_value:,.4f}" if sl_value is not None else "--",
                    "last_trade": last_trade_label,
                    "pnl": f"{pnl:,.2f}" if pnl is not None else "--",
                    "pnl_cls": pnl_color,
                    "pnl_pct": f"{pnl_pct:,.2f}%" if pnl_pct is not None else "--",
                    "pnl_pct_cls": pnl_pct_color,
                    "leverage": leverage_display,
                    "mode": mode_display,
                }
                rows.append(row)

            if len(rows) > 1:
                total_pnl: float | None = None
                for r in rows:
                    raw = r["pnl"]
                    try:
                        val = float(raw.replace(",", "")) if isinstance(raw, str) and raw != "--" else None
                    except (ValueError, AttributeError):
                        val = None
                    if val is not None:
                        total_pnl = (total_pnl or 0.0) + val

                total_size_usd: float | None = None
                for r in rows:
                    raw = r["size_usd"]
                    try:
                        val = float(raw.replace(",", "")) if isinstance(raw, str) and raw != "--" else None
                    except (ValueError, AttributeError):
                        val = None
                    if val is not None:
                        total_size_usd = (total_size_usd or 0.0) + val

                # PNL % as weighted average: sum(pnl) / sum(notional_usd) * 100
                total_pnl_pct: float | None = None
                if total_pnl is not None and total_size_usd and total_size_usd > 0:
                    total_pnl_pct = (total_pnl / total_size_usd) * 100

                if total_pnl is None:
                    total_pnl_color = "text-slate-900 font-bold"
                elif total_pnl > 0:
                    total_pnl_color = "text-emerald-600 font-bold"
                elif total_pnl < 0:
                    total_pnl_color = "text-rose-600 font-bold"
                else:
                    total_pnl_color = "text-slate-900 font-bold"

                if total_pnl_pct is None:
                    total_pnl_pct_color = "text-slate-900 font-bold"
                elif total_pnl_pct > 0:
                    total_pnl_pct_color = "text-emerald-600 font-bold"
                elif total_pnl_pct < 0:
                    total_pnl_pct_color = "text-rose-600 font-bold"
                else:
                    total_pnl_pct_color = "text-slate-900 font-bold"

                rows.append({
                    "symbol": "TOTAL",
                    "side": "",
                    "mode": "",
                    "size": "",
                    "size_usd": f"{total_size_usd:,.2f}" if total_size_usd is not None else "--",
                    "entry": "",
                    "current": "",
                    "tp": "",
                    "sl": "",
                    "last_trade": _build_shotgun_baseline_label(total_equity),
                    "pnl": f"{total_pnl:,.2f}" if total_pnl is not None else "--",
                    "pnl_cls": total_pnl_color,
                    "pnl_pct": f"{total_pnl_pct:,.2f}%" if total_pnl_pct is not None else "--",
                    "pnl_pct_cls": total_pnl_pct_color,
                    "leverage": "",
                })

            positions_table.rows = rows
            positions_table.update()

            target_symbol = selected_position_symbol["value"]
            normalized_symbol: str | None = None
            if target_symbol:
                for candidate in (target_symbol, target_symbol.upper(), target_symbol.lower()):
                    if candidate in position_lookup:
                        normalized_symbol = candidate
                        break
            update_position_chart(normalized_symbol)

            now = time.monotonic()
            if now - equity_refresh["last"] > 30:
                equity_refresh["last"] = now
                asyncio.create_task(refresh_equity_chart())

        unsubscribe_update = store.subscribe(update)
        cleanup_state = {"done": False}

        def _teardown_client(_: Any | None = None) -> None:
            if cleanup_state["done"]:
                return
            cleanup_state["done"] = True
            unsubscribe_update()
            store.stop()

        page_client.on_disconnect(_teardown_client)
        page_client.on_delete(_teardown_client)
        _t_health = ui.timer(5, lambda: [update_snapshot_health(last_snapshot["value"]), render_strategy_status()])
        _t_ws = ui.timer(5, set_ws_status)
        page_client.on_disconnect(_t_health.deactivate)
        page_client.on_delete(_t_health.deactivate)
        page_client.on_disconnect(_t_ws.deactivate)
        page_client.on_delete(_t_ws.deactivate)

        def _update_next_prompt_countdown() -> None:
            scheduler = getattr(app.state, "prompt_scheduler", None)
            if scheduler is None:
                next_prompt_label.set_text("Next prompt: --")
                prompt_elapsed_label.set_visibility(False)
                return
            elapsed = getattr(scheduler, "tick_elapsed_seconds", None)
            if elapsed is not None:
                e_mins, e_s = divmod(int(elapsed), 60)
                e_text = f"{e_mins}m {e_s:02d}s" if e_mins else f"{e_s}s"
                color = "text-orange-500" if elapsed >= 120 else "text-slate-500"
                prompt_elapsed_label.set_text(f"Running: {e_text}")
                prompt_elapsed_label.classes(replace=f"text-xs {color}")
                prompt_elapsed_label.set_visibility(True)
            else:
                prompt_elapsed_label.set_visibility(False)
            if getattr(scheduler, "is_ticking", False):
                next_prompt_label.set_text("Next prompt: running…")
                return
            secs = scheduler.seconds_until_next_tick
            if secs is None:
                next_prompt_label.set_text("Next prompt: off")
                return
            mins, s = divmod(int(secs), 60)
            if mins:
                next_prompt_label.set_text(f"Next prompt: {mins}m {s:02d}s")
            else:
                next_prompt_label.set_text(f"Next prompt: {s}s")

        _t_countdown = ui.timer(1, _update_next_prompt_countdown)
        page_client.on_disconnect(_t_countdown.deactivate)
        page_client.on_delete(_t_countdown.deactivate)
        asyncio.create_task(refresh_equity_chart())


    def render_ta_page() -> None:
        navigation("TA")
        wrapper = page_container()
        wrapper.style("max-width: 100%; width: 100%; margin-left: 0; margin-right: 0;")
        store = make_snapshot_store()
        config = getattr(app.state, "runtime_config", {}) or {}
        initial_timeframe = config.get("ta_timeframe") or "4H"
        if initial_timeframe not in TA_TIMEFRAME_OPTIONS:
            initial_timeframe = "4H"

        def fmt_number(value: Any, decimals: int = 2, prefix: str = "", suffix: str = "") -> str:
            try:
                if value is None:
                    return "--"
                return f"{prefix}{float(value):,.{decimals}f}{suffix}"
            except (TypeError, ValueError):
                return "--"

        with wrapper:
            ui.label("Technical Analysis").classes("text-2xl font-bold")
            with ui.row().classes("w-full flex-col xl:flex-row gap-6"):
                with ui.column().classes("flex-[7] w-full gap-4"):
                    with ui.row().classes("w-full flex-wrap gap-4"):
                        symbol_select = ui.select(options=[], label="Symbol").classes("w-full md:w-64")
                        symbol_select.disable()
                        timeframe_select = ui.select(
                            options=TA_TIMEFRAME_OPTIONS,
                            label="Timeframe",
                            value=initial_timeframe,
                        ).classes("w-full md:w-32")

                    indicator_cards: dict[str, ui.label] = {}
                    card_specs = [
                        ("rsi", "RSI (14)"),
                        ("stoch", "Stoch RSI"),
                        ("macd", "MACD"),
                        ("close", "Close"),
                        ("ls_ratio", "L/S Ratio"),
                    ]
                    with ui.row().classes("w-full flex flex-wrap gap-4"):
                        for key, label_text in card_specs:
                            with ui.card().classes(
                                "flex-1 min-w-[150px] p-4 shadow-sm border border-slate-200"
                            ):
                                ui.label(label_text).classes("text-xs uppercase text-slate-500")
                                value_label = ui.label("--").classes("text-2xl font-semibold text-slate-900")
                            indicator_cards[key] = value_label

                    bb_labels: dict[str, ui.label] = {}
                    trend_labels: dict[str, ui.label] = {}
                    ma_labels: dict[str, ui.label] = {}
                    risk_labels: dict[str, ui.label] = {}
                    with ui.card().classes("w-full p-4 shadow-sm border border-slate-200"):
                        with ui.row().classes("w-full flex-col md:flex-row gap-6"):
                            with ui.column().classes("flex-1 gap-1"):
                                ui.label("Bollinger Bands").classes("font-semibold text-slate-800")
                                for band in ("upper", "middle", "lower"):
                                    label = ui.label(f"{band.title()}: --").classes("text-sm text-slate-600")
                                    bb_labels[band] = label
                            with ui.column().classes("flex-1 gap-1"):
                                ui.label("Trend Analysis").classes("font-semibold text-slate-800")
                                for key, text in [
                                    ("vwap", "VWAP"),
                                    ("funding", "Funding Rate"),
                                    ("volume_24h", "24h Volume"),
                                    ("ofi", "Order Flow Imbalance"),
                                ]:
                                    label = ui.label(f"{text}: --").classes("text-sm text-slate-600")
                                    trend_labels[key] = label
                            with ui.column().classes("flex-1 gap-1"):
                                ui.label("Moving Averages").classes("font-semibold text-slate-800")
                                for key, text in [("ema_50", "EMA 50"), ("ema_200", "EMA 200")]:
                                    label = ui.label(f"{text}: --").classes("text-sm text-slate-600")
                                    ma_labels[key] = label
                            with ui.column().classes("flex-1 gap-1"):
                                ui.label("Risk Metrics").classes("font-semibold text-slate-800")
                                for key, text in [
                                    ("atr", "ATR"),
                                    ("atr_pct", "ATR %"),
                                    ("stop", "Suggested Stop"),
                                    ("stop_pct", "Stop %"),
                                ]:
                                    label = ui.label(f"{text}: --").classes("text-sm text-slate-600")
                                    risk_labels[key] = label

                    with ui.card().classes("w-full p-4 shadow-sm border border-emerald-100 bg-white"):
                        ui.label("Strategy Signal").classes("text-lg font-semibold text-emerald-800")
                        strategy_action_label = ui.label("--").classes("text-3xl font-bold text-slate-900")
                        strategy_confidence_label = ui.label("Confidence: --").classes("text-sm text-slate-600")
                        strategy_reason_label = ui.label("Reason: awaiting signal").classes("text-sm text-slate-500")
                        with ui.row().classes("gap-3 mt-3"):
                            simulate_button = ui.button("Simulate Trade", icon="science")
                            execute_button = ui.button("Send to Engine", icon="send")
                            execute_button.classes("bg-emerald-600 text-white")

                    with ui.card().classes("w-full p-4 shadow-sm border border-slate-200"):
                        ui.label("Trade Intent Feed").classes("text-lg font-semibold text-slate-800")
                        strategy_feed = ui.log(max_lines=100).classes("w-full h-48 bg-slate-950 text-emerald-100")

                with ui.column().classes("flex-[5] w-full gap-4"):
                    kline_chart = ui.echart(
                        {
                            "legend": {"data": ["K-Line", "VWAP"], "textStyle": {"color": "#0f172a"}},
                            "tooltip": {"trigger": "axis"},
                            "grid": {"left": 50, "right": 20, "top": 30, "bottom": 30},
                            "xAxis": [{"type": "category", "data": [], "boundaryGap": False, "axisLabel": {"color": "#475569"}}],
                            "yAxis": [{"scale": True, "axisLabel": {"color": "#475569"}}],
                            "series": [
                                {
                                    "type": "candlestick",
                                    "name": "K-Line",
                                    "data": [],
                                    "itemStyle": {"color": "#22c55e", "color0": "#ef4444"},
                                },
                                {
                                    "type": "line",
                                    "name": "VWAP",
                                    "data": [],
                                    "smooth": True,
                                    "lineStyle": {"color": "#6366f1", "width": 2},
                                    "showSymbol": False,
                                },
                            ],
                        }
                    ).classes("w-full h-96 bg-white rounded-lg shadow")

                    ui.label("Flow & Volatility Series").classes("text-base font-semibold text-slate-700 mt-2")
                    with ui.row().classes("w-full flex-wrap gap-4"):
                        vwap_chart = ui.echart(
                            {
                                "tooltip": {"trigger": "axis"},
                                "grid": {"left": 40, "right": 10, "top": 30, "bottom": 25},
                                "xAxis": {"type": "category", "data": []},
                                "yAxis": {"type": "value", "scale": True},
                                "series": [
                                    {
                                        "type": "line",
                                        "name": "VWAP",
                                        "data": [],
                                        "lineStyle": {"color": "#3b82f6", "width": 2},
                                        "areaStyle": {"color": "rgba(59,130,246,0.15)"},
                                        "showSymbol": False,
                                    }
                                ],
                            }
                        ).classes("flex-1 min-w-[280px] h-64 bg-white rounded-lg shadow")

                        volume_rsi_chart = ui.echart(
                            {
                                "tooltip": {"trigger": "axis"},
                                "grid": {"left": 40, "right": 10, "top": 30, "bottom": 25},
                                "xAxis": {"type": "category", "data": []},
                                "yAxis": {"type": "value", "scale": True},
                                "series": [
                                    {
                                        "type": "line",
                                        "name": "Volume RSI",
                                        "data": [],
                                        "lineStyle": {"color": "#ef4444", "width": 2},
                                        "areaStyle": {"color": "rgba(239,68,68,0.15)"},
                                        "showSymbol": False,
                                    }
                                ],
                            }
                        ).classes("flex-1 min-w-[280px] h-64 bg-white rounded-lg shadow")

                        cvd_chart = ui.echart(
                            {
                                "tooltip": {"trigger": "axis"},
                                "grid": {"left": 40, "right": 10, "top": 30, "bottom": 25},
                                "xAxis": {"type": "category", "data": []},
                                "yAxis": {"type": "value", "scale": True},
                                "series": [
                                    {
                                        "type": "line",
                                        "name": "CVD",
                                        "data": [],
                                        "lineStyle": {"color": "#10b981", "width": 2},
                                        "areaStyle": {"color": "rgba(16,185,129,0.15)"},
                                        "showSymbol": False,
                                    }
                                ],
                            }
                        ).classes("flex-1 min-w-[280px] h-64 bg-white rounded-lg shadow")

                    ui.label("Order Flow Strength").classes("text-base font-semibold text-slate-700 mt-4")
                    with ui.row().classes("w-full flex-wrap gap-4"):
                        obv_chart = ui.echart(
                            {
                                "tooltip": {"trigger": "axis"},
                                "grid": {"left": 40, "right": 10, "top": 30, "bottom": 25},
                                "xAxis": {"type": "category", "data": []},
                                "yAxis": {"type": "value", "scale": True},
                                "series": [
                                    {
                                        "type": "line",
                                        "name": "OBV",
                                        "data": [],
                                        "lineStyle": {"color": "#a855f7", "width": 2},
                                        "areaStyle": {"color": "rgba(168,85,247,0.15)"},
                                        "showSymbol": False,
                                    }
                                ],
                            }
                        ).classes("flex-1 min-w-[280px] h-64 bg-white rounded-lg shadow")

                        cmf_chart = ui.echart(
                            {
                                "tooltip": {"trigger": "axis"},
                                "grid": {"left": 40, "right": 10, "top": 30, "bottom": 25},
                                "xAxis": {"type": "category", "data": []},
                                "yAxis": {"type": "value", "scale": True},
                                "series": [
                                    {
                                        "type": "line",
                                        "name": "CMF",
                                        "data": [],
                                        "lineStyle": {"color": "#14b8a6", "width": 2},
                                        "areaStyle": {"color": "rgba(20,184,166,0.15)"},
                                        "showSymbol": False,
                                    }
                                ],
                            }
                        ).classes("flex-1 min-w-[280px] h-64 bg-white rounded-lg shadow")

        current_symbol = {"value": None}
        last_snapshot = {"value": None}
        current_signal = {"value": None, "symbol": None}
        last_logged_signal = {"value": None}

        def update(snapshot: dict[str, Any] | None) -> None:
            last_snapshot["value"] = snapshot
            if not snapshot:
                return
            symbols = snapshot.get("symbols") or []
            market_data = snapshot.get("market_data") or {}
            positions = snapshot.get("positions") or []
            if symbols:
                symbol_select.options = symbols
                symbol_select.enable()
                if current_symbol["value"] not in symbols:
                    current_symbol["value"] = symbols[0]
                    symbol_select.value = current_symbol["value"]
            else:
                symbol_select.options = []
                symbol_select.disable()
            selected_symbol = current_symbol["value"] or snapshot.get("symbol")
            market_entry = market_data.get(selected_symbol, {})
            indicators = market_entry.get("indicators") or snapshot.get("indicators") or {}
            custom = market_entry.get("custom_metrics") or snapshot.get("custom_metrics") or {}
            funding = market_entry.get("funding_rate") or snapshot.get("funding_rate") or {}
            ticker = market_entry.get("ticker") or snapshot.get("ticker") or {}
            open_interest = market_entry.get("open_interest") or snapshot.get("open_interest") or {}
            strategy_signal = market_entry.get("strategy_signal") or snapshot.get("strategy_signal") or {}
            risk_metrics = market_entry.get("risk_metrics") or snapshot.get("risk_metrics") or {}

            card_values = {
                "rsi": fmt_number(indicators.get("rsi")),
                "stoch": " / ".join(
                    [
                        fmt_number(indicators.get("stoch_rsi", {}).get("k")),
                        fmt_number(indicators.get("stoch_rsi", {}).get("d")),
                    ]
                ),
                "macd": fmt_number((indicators.get("macd") or {}).get("value")),
                "close": fmt_number(ticker.get("last"), 2, prefix="$"),
                "ls_ratio": "--",
            }

            market_ls_metric = (custom.get("market_long_short_ratio") or {}).get("value")
            market_ls_display = fmt_number(market_ls_metric) if market_ls_metric is not None else "--"
            if market_ls_display != "--":
                card_values["ls_ratio"] = market_ls_display
            else:
                long_size = 0.0
                short_size = 0.0
                for pos in positions:
                    if (pos.get("instId") or pos.get("symbol")) != selected_symbol:
                        continue
                    try:
                        size_val = float(pos.get("pos") or pos.get("posQty") or pos.get("size") or 0)
                    except (TypeError, ValueError):
                        continue
                    if size_val >= 0:
                        long_size += abs(size_val)
                    else:
                        short_size += abs(size_val)
                if long_size == 0 and short_size == 0:
                    card_values["ls_ratio"] = "--"
                elif short_size == 0:
                    card_values["ls_ratio"] = "∞"
                else:
                    ratio = long_size / short_size if short_size else 0
                    card_values["ls_ratio"] = fmt_number(ratio, 2)

            for key, label in indicator_cards.items():
                label.set_text(card_values.get(key, "--"))

            bb = indicators.get("bollinger_bands") or {}
            bb_labels["upper"].set_text(f"Upper: {fmt_number(bb.get('upper'))}")
            bb_labels["middle"].set_text(f"Middle: {fmt_number(bb.get('middle'))}")
            bb_labels["lower"].set_text(f"Lower: {fmt_number(bb.get('lower'))}")

            trend_labels["vwap"].set_text(f"VWAP: {fmt_number(indicators.get('vwap'), 2)}")
            trend_labels["funding"].set_text(f"Funding Rate: {funding.get('fundingRate', '--')}")
            volume_value = (
                ticker.get("volCcy24h")
                or ticker.get("volCcy")
                or ticker.get("vol24h")
                or ticker.get("vol")
                or custom.get("volume_24h")
            )
            trend_labels["volume_24h"].set_text(f"24h Volume: {fmt_number(volume_value, 0)}")
            trend_labels["ofi"].set_text(
                f"Order Flow Imbalance: {fmt_number(custom.get('order_flow_imbalance'))}"
            )

            ma = indicators.get("moving_averages") or {}
            ma_labels["ema_50"].set_text(f"EMA 50: {fmt_number(ma.get('ema_50'), 2)}")
            ma_labels["ema_200"].set_text(f"EMA 200: {fmt_number(ma.get('ema_200'), 2)}")

            risk_labels["atr"].set_text(f"ATR: {fmt_number(risk_metrics.get('atr'), 2)}")
            risk_labels["atr_pct"].set_text(f"ATR %: {fmt_number(risk_metrics.get('atr_pct'), 2, suffix='%')}")
            risk_labels["stop"].set_text(
                f"Suggested Stop: {fmt_number(risk_metrics.get('suggested_stop'), 2)}"
            )
            risk_labels["stop_pct"].set_text(
                f"Stop %: {fmt_number(risk_metrics.get('suggested_stop_pct'), 2, suffix='%')}"
            )

            current_signal["value"] = strategy_signal
            current_signal["symbol"] = selected_symbol
            action_text = strategy_signal.get("action", "--")
            strategy_action_label.set_text(action_text)
            conf_val = strategy_signal.get("confidence")
            conf_display = f"Confidence: {conf_val * 100:.0f}%" if isinstance(conf_val, (int, float)) else "Confidence: --"
            strategy_confidence_label.set_text(conf_display)
            strategy_reason_label.set_text(f"Reason: {strategy_signal.get('reason', 'Awaiting signal')}")

            summary = f"{action_text}-{conf_display}-{strategy_signal.get('reason')}"
            if summary != last_logged_signal.get("value") and action_text != "--":
                timestamp = format_now("%H:%M:%S %Z")
                strategy_feed.push(
                    f"{timestamp} | {selected_symbol} | {action_text} ({conf_display.split(': ')[1]})"
                )
                last_logged_signal["value"] = summary

            ohlcv = indicators.get("ohlcv") or []
            categories = [format_epoch_ms(entry.get("ts"), fmt="%H:%M") for entry in ohlcv]
            candle_data = [
                [entry.get("open"), entry.get("close"), entry.get("low"), entry.get("high")] for entry in ohlcv
            ]
            vwap_series = indicators.get("vwap_series") or []
            if categories and candle_data:
                kline_chart.options["xAxis"][0]["data"] = categories
                kline_chart.options["series"][0]["data"] = candle_data
                kline_chart.options["series"][1]["data"] = vwap_series[-len(categories) :]
                kline_chart.update()
                vwap_chart.options["xAxis"]["data"] = categories
                vwap_chart.options["series"][0]["data"] = vwap_series[-len(categories) :]
                vwap_chart.update()
            else:
                kline_chart.options["xAxis"][0]["data"] = []
                kline_chart.options["series"][0]["data"] = []
                kline_chart.options["series"][1]["data"] = []
                kline_chart.update()
                vwap_chart.options["xAxis"]["data"] = []
                vwap_chart.options["series"][0]["data"] = []
                vwap_chart.update()

            volume_rsi_series = indicators.get("volume_rsi_series") or []
            if volume_rsi_series:
                axis = list(range(len(volume_rsi_series)))
                volume_rsi_chart.options["xAxis"]["data"] = axis
                volume_rsi_chart.options["series"][0]["data"] = volume_rsi_series
                volume_rsi_chart.update()
            else:
                volume_rsi_chart.options["xAxis"]["data"] = []
                volume_rsi_chart.options["series"][0]["data"] = []
                volume_rsi_chart.update()

            cvd_series = custom.get("cvd_series") or []
            if cvd_series:
                axis = list(range(len(cvd_series)))
                cvd_chart.options["xAxis"]["data"] = axis
                cvd_chart.options["series"][0]["data"] = cvd_series
                cvd_chart.update()
            else:
                cvd_chart.options["xAxis"]["data"] = []
                cvd_chart.options["series"][0]["data"] = []
                cvd_chart.update()

            obv_block = indicators.get("obv") or {}
            obv_series = obv_block.get("series") or []
            if obv_series:
                axis = list(range(len(obv_series)))
                obv_chart.options["xAxis"]["data"] = axis
                obv_chart.options["series"][0]["data"] = obv_series
                obv_chart.update()
            else:
                obv_chart.options["xAxis"]["data"] = []
                obv_chart.options["series"][0]["data"] = []
                obv_chart.update()

            cmf_block = indicators.get("cmf") or {}
            cmf_series = cmf_block.get("series") or []
            if cmf_series:
                axis = list(range(len(cmf_series)))
                cmf_chart.options["xAxis"]["data"] = axis
                cmf_chart.options["series"][0]["data"] = cmf_series
                cmf_chart.update()
            else:
                cmf_chart.options["xAxis"]["data"] = []
                cmf_chart.options["series"][0]["data"] = []
                cmf_chart.update()

        unsubscribe_update = store.subscribe(update)
        client = ui.context.client
        cleanup_state = {"done": False}

        def _teardown_client(_: Any | None = None) -> None:
            if cleanup_state["done"]:
                return
            cleanup_state["done"] = True
            unsubscribe_update()
            store.stop()

        client.on_disconnect(_teardown_client)
        client.on_delete(_teardown_client)

        def log_trade_event(kind: str) -> None:
            signal = current_signal.get("value") or {}
            symbol = current_signal.get("symbol") or current_symbol.get("value")
            action = signal.get("action")
            if not action or action == "--":
                ui.notify("No strategy signal available yet", color="warning")
                return
            confidence = signal.get("confidence")
            confidence_pct = f"{confidence * 100:.0f}%" if isinstance(confidence, (int, float)) else "--"
            timestamp = format_now("%H:%M:%S %Z")
            entry = f"{timestamp} | {symbol} | {kind}: {action} ({confidence_pct})"
            strategy_feed.push(entry)
            app.state.frontend_events.append(entry)
            ui.notify(entry, color="positive" if kind == "EXECUTE" else "secondary")

        simulate_button.on("click", lambda _: log_trade_event("SIMULATE"))
        execute_button.on("click", lambda _: log_trade_event("EXECUTE"))

        async def apply_timeframe_change(value: str) -> None:
            if not value:
                return
            config["ta_timeframe"] = value
            market_service = getattr(app.state, "market_service", None)
            if market_service:
                await market_service.set_ohlc_bar(value)
            await store.refresh_now()

        def on_timeframe_change(e: Any) -> None:
            asyncio.create_task(apply_timeframe_change(e.value))

        timeframe_select.on_value_change(on_timeframe_change)

        def on_symbol_change(e: Any) -> None:
            current_symbol["value"] = e.value
            update(last_snapshot["value"])

        symbol_select.on_value_change(on_symbol_change)

    def render_strategy_page() -> None:
        navigation("STRATEGY")
        wrapper = page_container()
        config = getattr(app.state, "runtime_config", {}) or {}
        strategy = config.setdefault("strategy", {})
        skimming = strategy.setdefault("skimming", {"enabled": False, "threshold_pct": 2.0, "stop_loss_pct": None})
        # Dynamic TP notice banner (Mean Reversion Scalping)
        _mr_cfg_notice = ((config.get("launcher") or {}).get("strategies") or {}).get("mean_reversion") or {}
        if bool(_mr_cfg_notice.get("enabled")) and bool(_mr_cfg_notice.get("dynamic_tp", False)):
            with wrapper:
                with ui.row().classes("w-full items-center gap-2 bg-amber-50 border border-amber-300 rounded-lg px-4 py-2 mb-2"):
                    ui.icon("auto_graph", color="amber").classes("text-lg")
                    frac = _mr_cfg_notice.get("dynamic_tp_fraction", 0.7)
                    tp_floor = _mr_cfg_notice.get("tp_pct", 2.0)
                    ui.label(
                        f"Dynamic TP is active for Mean Reversion — effective TP = min({tp_floor}%, bandwidth÷2 × {frac}). "
                        f"Static {tp_floor}% acts as a ceiling; dynamic only tightens the target in low-bandwidth conditions."
                    ).classes("text-xs text-amber-800")
        shotgun = strategy.setdefault("shotgun", {
            "enabled": False,
            "tp_pct": None,
            "tp_usd": None,
            "sl_pct": None,
            "sl_usd": None,
            "close_only_negative": False,
        })
        protector = strategy.setdefault("protector", {
            "enabled": False,
            "activate_pct": 10.0,
            "step_pct": 10.0,
            "lock_ratio": 0.5,
        })
        commutator = strategy.setdefault("commutator", {
            "enabled": False,
            "reverse_at_loss_pct": None,
            "reverse_at_loss_usd": None,
            "max_flips": 1,
            "post_reversal_tp_pct": None,
        })
        alternator = strategy.setdefault("alternator", {
            "enabled": False,
            "reverse_at_profit_pct": None,
            "reverse_at_profit_usd": None,
            "dynamic_threshold": False,
            "dynamic_threshold_factor": 1.0,
            "dynamic_threshold_lookback": 20,
            "dynamic_loss_threshold": False,
            "dynamic_loss_factor": 1.0,
            "dynamic_loss_lookback": 20,
            "trailing_reverse": False,
            "trailing_pullback_pct": 10.0,
            "trailing_close": False,
            "trailing_close_activate_pct": None,
            "trailing_close_activate_usd": None,
            "trailing_close_pullback_pct": 10.0,
            "candle_position_filter": False,
            "candle_position_long_max": 0.75,
            "candle_position_short_min": 0.25,
            "candle_position_lookback": 20,
            "footprint_delta_filter": False,
            "footprint_delta_min_ratio": 0.0,
            "ob_wall_suppress": False,
            "ob_wall_proximity_pct": 1.0,
            "ob_wall_ratio": 3.0,
            "continuous_llm": False,
            "close_on_max_reversals": False,
            "max_reversals": None,
            "restart_at_loss_pct": None,
            "restart_at_loss_usd": None,
            "ride_at_profit_pct": None,
            "ride_at_profit_usd": None,
            "stop_at_loss_pct": None,
            "stop_at_loss_usd": None,
        })
        strategy.setdefault("ob_wall_stops", {
            "enabled": False,
            "proximity_pct": 2.0,
            "wall_ratio": 3.0,
            "min_sl_improvement_pct": 0.1,
            "sl_buffer_pct": 0.1,
        })

        with wrapper:
            with ui.row().classes("w-full justify-between items-center"):
                ui.label("Strategy").classes("text-2xl font-bold")
                save_button = ui.button("Save", icon="save", color="primary")
            ui.label(
                "Configure automated trading strategies that run independently of LLM decisions."
            ).classes("text-sm text-slate-500 mb-2")
            ui.separator().classes("w-full my-2")

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    skimming_switch = ui.switch(
                        value=bool(skimming.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Skimming").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Automatically close a position at market price as soon as its "
                            "unrealized PnL reaches the configured percentage threshold. "
                            "Set a Stop Loss % to also exit losing positions automatically."
                        ).classes("text-xs text-slate-500 mb-3")
                        with ui.row().classes("gap-4 items-start"):
                            threshold_input = ui.number(
                                label="Take Profit at (% PnL)",
                                value=float(skimming.get("threshold_pct") or 2.0),
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Floor TP: close when PnL reaches this % profit' persistent-hint"
                            )
                            _sl_raw = skimming.get("stop_loss_pct")
                            stop_loss_input = ui.number(
                                label="Stop Loss at (% PnL)",
                                value=float(_sl_raw) if _sl_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Close when PnL drops below this % loss (leave blank to disable)' persistent-hint clearable stack-label"
                            )
                    _active_badge = ui.badge("Active", color="positive").bind_visibility_from(
                        skimming_switch, "value"
                    )

            # ── Mean Reversion Scalping ─────────────────────────────────────────────
            _mr_cfg = ((config.get("launcher") or {}).get("strategies") or {}).get("mean_reversion") or {}
            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    mr_enabled_switch = ui.switch(
                        value=bool(_mr_cfg.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Mean Reversion Scalping").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Rule-based RSI mean-reversion entries run by the Launcher. "
                            "Launcher mode must also be enabled on the CFG page."
                        ).classes("text-xs text-slate-500 mb-3")
                        with ui.row().classes("w-full items-center gap-2 mb-2"):
                            ui.button(
                                "Set Recommended Defaults",
                                icon="tune",
                                on_click=lambda _: _set_mr_defaults(),
                            ).props("dense flat color=primary size=sm").tooltip(
                                "Fill all fields with the recommended Mean Reversion configuration. "
                                "You still need to click Save to persist."
                            )
                        with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                            _mr_tp_raw = _mr_cfg.get("tp_pct")
                            mr_tp_input = ui.number(
                                label="Take profit (%)",
                                value=float(_mr_tp_raw) if _mr_tp_raw is not None else None,
                                min=0.01, step=0.1, precision=2,
                            ).classes("w-40").props(
                                "hint='Close when uplRatio ≥ this % (blank = none)' persistent-hint clearable"
                            )
                            _mr_sl_raw = _mr_cfg.get("sl_pct")
                            mr_sl_input = ui.number(
                                label="Stop loss (%)",
                                value=float(_mr_sl_raw) if _mr_sl_raw is not None else None,
                                min=0.01, step=0.1, precision=2,
                            ).classes("w-40").props(
                                "hint='Close when uplRatio ≤ -X% (blank = none)' persistent-hint clearable"
                            )
                        with ui.row().classes("gap-4 items-center mt-2"):
                            mr_dynamic_tp_switch = ui.switch(
                                "Dynamic TP (BB Bandwidth)",
                                value=bool(_mr_cfg.get("dynamic_tp", False)),
                            ).props("dense color=amber")
                            ui.label(
                                "Adjusts TP target based on current BB bandwidth at entry: "
                                "effective TP = min(static TP, bandwidth÷2 × fraction). "
                                "Static TP acts as a ceiling."
                            ).classes("text-xs text-slate-500")
                        with ui.row().classes("gap-4 items-center mt-1"):
                            _mr_dtp_frac_raw = _mr_cfg.get("dynamic_tp_fraction")
                            mr_dynamic_tp_fraction_input = ui.number(
                                label="Reversion Fraction",
                                value=float(_mr_dtp_frac_raw) if _mr_dtp_frac_raw is not None else 0.7,
                                min=0.1,
                                max=1.0,
                                step=0.05,
                                format="%.2f",
                            ).classes("w-40").props("dense")
                            ui.label(
                                "Fraction of the half-bandwidth to use as TP (0.7 = 70% reversion toward midline)."
                            ).classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                            _mr_rsi_os_raw = _mr_cfg.get("rsi_oversold", 30.0)
                            mr_rsi_oversold_input = ui.number(
                                label="RSI oversold (BUY)",
                                value=float(_mr_rsi_os_raw) if _mr_rsi_os_raw is not None else 30.0,
                                min=1, max=49, step=1, precision=0,
                            ).classes("w-40").props(
                                "hint='BUY signal when RSI < this' persistent-hint"
                            )
                            _mr_rsi_ob_raw = _mr_cfg.get("rsi_overbought", 70.0)
                            mr_rsi_overbought_input = ui.number(
                                label="RSI overbought (SELL)",
                                value=float(_mr_rsi_ob_raw) if _mr_rsi_ob_raw is not None else 70.0,
                                min=51, max=99, step=1, precision=0,
                            ).classes("w-40").props(
                                "hint='SELL signal when RSI > this' persistent-hint"
                            )
                            _mr_adx_raw = _mr_cfg.get("min_adx", 0.0)
                            mr_min_adx_input = ui.number(
                                label="Min ADX",
                                value=float(_mr_adx_raw) if _mr_adx_raw is not None else 0.0,
                                min=0, max=100, step=5, precision=0,
                            ).classes("w-32").props(
                                "hint='Skip if ADX below this (0 = off)' persistent-hint"
                            )
                            _mr_max_adx_raw = _mr_cfg.get("max_adx", 0.0)
                            mr_max_adx_input = ui.number(
                                label="Max ADX",
                                value=float(_mr_max_adx_raw) if _mr_max_adx_raw is not None else 0.0,
                                min=0, max=100, step=5, precision=0,
                            ).classes("w-32").props(
                                "hint='Skip if ADX above this (0 = off)' persistent-hint"
                            )
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_htf_switch = ui.switch(
                                "Require HTF trend alignment",
                                value=bool(_mr_cfg.get("require_htf_trend", True)),
                            ).props("dense color=primary")
                            ui.label("HTF EMA50 > EMA200 for BUY / EMA50 < EMA200 for SELL.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_cmf_switch = ui.switch(
                                "Require CMF confirmation",
                                value=bool(_mr_cfg.get("require_cmf", True)),
                            ).props("dense color=primary")
                            ui.label("LTF CMF (14-period) must be positive for BUY and negative for SELL.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_htf_cmf_switch = ui.switch(
                                "Require HTF CMF confirmation",
                                value=bool(_mr_cfg.get("require_htf_cmf", False)),
                            ).props("dense color=primary")
                            ui.label("HTF CMF (20-period) governor: must be positive for BUY and negative for SELL.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_cmf_cross_switch = ui.switch(
                                "Require CMF zero-line cross",
                                value=bool(_mr_cfg.get("require_cmf_cross", False)),
                            ).props("dense color=primary")
                            ui.label("LTF CMF must have just crossed zero this bar.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_cmf_no_div_switch = ui.switch(
                                "Block on CMF divergence",
                                value=bool(_mr_cfg.get("require_cmf_no_divergence", False)),
                            ).props("dense color=primary")
                            ui.label("Block BUY on bearish CMF divergence, and vice versa for SELL.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_fp_delta_switch = ui.switch(
                                "Require footprint net delta",
                                value=bool(_mr_cfg.get("require_footprint_delta", False)),
                            ).props("dense color=primary")
                            ui.label("15-min tape net delta must be positive for BUY and negative for SELL.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_require_bb_switch = ui.switch(
                                "Require Bollinger Band position",
                                value=bool(_mr_cfg.get("require_bb_position", False)),
                            ).props("dense color=primary")
                            ui.label("BUY only at/below lower band; SELL only at/above upper band.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_bb_proximity_input = ui.number(
                                label="BB Proximity %",
                                value=float(_mr_cfg.get("bb_proximity_pct") or 0.0),
                                min=0.0, max=5.0, step=0.1, format="%.1f",
                            ).classes("w-48").props("dense")
                            ui.label("How far inside the band price may still qualify (0 = must be at/beyond band).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_min_bb_bw_input = ui.number(
                                label="Min BB Bandwidth %",
                                value=float(_mr_cfg.get("min_bb_bandwidth") or 0.0),
                                min=0.0, max=20.0, step=0.5, format="%.1f",
                            ).classes("w-48").props("dense")
                            mr_max_bb_bw_input = ui.number(
                                label="Max BB Bandwidth %",
                                value=float(_mr_cfg.get("max_bb_bandwidth") or 0.0),
                                min=0.0, max=50.0, step=0.5, format="%.1f",
                            ).classes("w-48").props("dense")
                            ui.label("Block when BB bandwidth is outside this range. 0 = off.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_candle_rejection_switch = ui.switch(
                                "Require candle rejection",
                                value=bool(_mr_cfg.get("require_candle_rejection", False)),
                            ).props("dense color=primary")
                            ui.label("Require upper wick for shorts / lower wick for longs (exhaustion confirmation).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_candle_rejection_pct_input = ui.number(
                                label="Candle rejection %",
                                value=float(_mr_cfg.get("candle_rejection_pct") or 30.0),
                                min=5.0, max=90.0, step=5.0, format="%.0f",
                            ).classes("w-48").props("dense")
                            ui.label("Minimum wick size as % of candle range (30 = wick is 30%+ of the candle).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_vwap_reversion_switch = ui.switch(
                                "Require VWAP reversion",
                                value=bool(_mr_cfg.get("require_vwap_reversion", False)),
                            ).props("dense color=primary")
                            ui.label("Require price extended from VWAP AND closing back toward it.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_vwap_min_dist_input = ui.number(
                                label="VWAP min distance %",
                                value=float(_mr_cfg.get("vwap_min_distance_pct") or 1.0),
                                min=0.1, max=20.0, step=0.1, format="%.1f",
                            ).classes("w-48").props("dense")
                            ui.label("Minimum % distance from VWAP to qualify as 'extended'.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_volume_cooling_switch = ui.switch(
                                "Require volume cooling",
                                value=bool(_mr_cfg.get("require_volume_cooling", False)),
                            ).props("dense color=primary")
                            ui.label("Block when volume RSI is still high (spike still being driven by heavy volume).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            mr_volume_rsi_max_input = ui.number(
                                label="Volume RSI max",
                                value=float(_mr_cfg.get("volume_rsi_max") or 70.0),
                                min=10.0, max=99.0, step=5.0, format="%.0f",
                            ).classes("w-48").props("dense")
                            ui.label("Maximum volume RSI to allow entry (below = volume momentum fading).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            _mr_flip_enabled = bool(_mr_cfg.get("flip_launcher_direction"))
                            mr_flip_switch = ui.switch(
                                "Flip Launcher Decision",
                                value=_mr_flip_enabled,
                            ).props("dense color=primary")
                            mr_flip_select = ui.select(
                                options={"both": "Both", "from_long": "From LONG only", "from_short": "From SHORT only"},
                                value=_mr_cfg.get("flip_launcher_direction") or "both",
                                label="Flip direction",
                            ).classes("w-40").props("dense")
                            ui.label("Invert the Launcher's trade direction before execution.").classes("text-xs text-slate-500")
                    _active_badge_mr = ui.badge("Active", color="positive").bind_visibility_from(
                        mr_enabled_switch, "value"
                    )

            def _set_mr_defaults() -> None:
                """Fill all Mean Reversion fields with the recommended configuration."""
                mr_tp_input.value = 3.0
                mr_sl_input.value = 4.0
                mr_dynamic_tp_switch.value = True
                mr_dynamic_tp_fraction_input.value = 0.7
                mr_rsi_oversold_input.value = 30.0
                mr_rsi_overbought_input.value = 70.0
                mr_min_adx_input.value = 0.0
                mr_max_adx_input.value = 25.0
                mr_require_htf_switch.value = True
                mr_require_cmf_switch.value = True
                mr_require_htf_cmf_switch.value = False
                mr_require_cmf_cross_switch.value = False
                mr_require_cmf_no_div_switch.value = False
                mr_require_fp_delta_switch.value = False
                mr_require_bb_switch.value = True
                mr_bb_proximity_input.value = 0.5
                mr_min_bb_bw_input.value = 2.0
                mr_max_bb_bw_input.value = 0.0
                mr_candle_rejection_switch.value = True
                mr_candle_rejection_pct_input.value = 30.0
                mr_vwap_reversion_switch.value = False
                mr_vwap_min_dist_input.value = 1.0
                mr_volume_cooling_switch.value = True
                mr_volume_rsi_max_input.value = 70.0
                mr_flip_switch.value = False
                mr_flip_select.value = "both"
                ui.notify("Mean Reversion fields set to recommended defaults — click Save to persist", color="info")

            # ── Spike Continuation ───────────────────────────────────────────────────
            _sc_cfg = ((config.get("launcher") or {}).get("strategies") or {}).get("spike_continuation") or {}
            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    sc_enabled_switch = ui.switch(
                        value=bool(_sc_cfg.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Spike Continuation").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Momentum scalp: rides volume-driven spikes for 3-5% before they revert. "
                            "Enters WITH the spike (not against it). Mirror image of Mean Reversion. "
                            "Launcher mode must also be enabled on the CFG page."
                        ).classes("text-xs text-slate-500 mb-3")
                        with ui.row().classes("w-full items-center gap-2 mb-2"):
                            ui.button(
                                "Set Recommended Defaults",
                                icon="tune",
                                on_click=lambda _: _set_sc_defaults(),
                            ).props("dense flat color=primary size=sm").tooltip(
                                "Fill all fields with the recommended Spike Continuation configuration. "
                                "You still need to click Save to persist."
                            )
                        with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                            _sc_tp_raw = _sc_cfg.get("tp_pct")
                            sc_tp_input = ui.number(
                                label="Take profit (%)",
                                value=float(_sc_tp_raw) if _sc_tp_raw is not None else 5.0,
                                min=0.5, step=0.5, precision=1,
                            ).classes("w-40").props(
                                "hint='Exit after this % price move' persistent-hint clearable"
                            )
                            _sc_sl_raw = _sc_cfg.get("sl_pct")
                            sc_sl_input = ui.number(
                                label="Stop loss (%)",
                                value=float(_sc_sl_raw) if _sc_sl_raw is not None else 3.0,
                                min=0.5, step=0.5, precision=1,
                            ).classes("w-40").props(
                                "hint='Exit if spike fails and reverses this %' persistent-hint clearable"
                            )
                        with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                            _sc_vrsi_raw = _sc_cfg.get("volume_rsi_min")
                            sc_volume_rsi_min_input = ui.number(
                                label="Volume RSI min",
                                value=float(_sc_vrsi_raw) if _sc_vrsi_raw is not None else 75.0,
                                min=50, max=99, step=1, precision=0,
                            ).classes("w-40").props(
                                "hint='Volume RSI must be above this to confirm spike' persistent-hint"
                            )
                            _sc_rsi_min_raw = _sc_cfg.get("rsi_min")
                            sc_rsi_min_input = ui.number(
                                label="RSI min (buy zone)",
                                value=float(_sc_rsi_min_raw) if _sc_rsi_min_raw is not None else 55.0,
                                min=40, max=70, step=1, precision=0,
                            ).classes("w-40").props(
                                "hint='RSI must be above this for buys (momentum confirmed)' persistent-hint"
                            )
                            _sc_rsi_max_raw = _sc_cfg.get("rsi_max")
                            sc_rsi_max_input = ui.number(
                                label="RSI max (buy zone)",
                                value=float(_sc_rsi_max_raw) if _sc_rsi_max_raw is not None else 75.0,
                                min=60, max=90, step=1, precision=0,
                            ).classes("w-40").props(
                                "hint='Dont enter if RSI above this (Mean Reversion territory)' persistent-hint"
                            )
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_bb_breakout_switch = ui.switch(
                                "Require BB breakout",
                                value=bool(_sc_cfg.get("require_bb_breakout", True)),
                            ).props("dense color=primary")
                            ui.label("Price must be beyond the BB band to confirm the spike.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_candle_strength_switch = ui.switch(
                                "Require candle strength",
                                value=bool(_sc_cfg.get("require_candle_strength", True)),
                            ).props("dense color=primary")
                            ui.label("Candle must close near its high (buy) or low (sell) — strong momentum, no rejection.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_candle_strength_pct_input = ui.number(
                                label="Candle strength %",
                                value=float(_sc_cfg.get("candle_strength_pct") or 70.0),
                                min=50, max=95, step=5, format="%.0f",
                            ).classes("w-48").props("dense")
                            ui.label("Close must be in this % of the candle range from the direction (70 = top 30% for buys).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_min_bb_bw_input = ui.number(
                                label="Min BB Bandwidth %",
                                value=float(_sc_cfg.get("min_bb_bandwidth") or 3.0),
                                min=0.0, max=20.0, step=0.5, format="%.1f",
                            ).classes("w-48").props("dense")
                            ui.label("Only enter when bands are wide enough to suggest real volatility expansion.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_max_adx_input = ui.number(
                                label="Max ADX",
                                value=float(_sc_cfg.get("max_adx") or 0.0),
                                min=0, max=100, step=5, precision=0,
                            ).classes("w-32").props(
                                "hint='Skip if trend too strong (0 = off; acceleration/extension filters already prevent late entry)' persistent-hint"
                            )
                        # ── Momentum acceleration filters (prevent entering at the top) ──
                        ui.separator().classes("my-2")
                        ui.label("Momentum acceleration filters").classes("text-xs font-semibold text-slate-600")
                        ui.label(
                            "These filters prevent entering at the TOP of a spike. "
                            "They verify the spike is still accelerating, not peaking."
                        ).classes("text-xs text-slate-500 mb-2")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_momentum_accel_switch = ui.switch(
                                "Require momentum acceleration",
                                value=bool(_sc_cfg.get("require_momentum_acceleration", True)),
                            ).props("dense color=primary")
                            ui.label("Current candle body must be larger than recent average — spike is accelerating, not peaking.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_accel_lookback_input = ui.number(
                                label="Acceleration lookback",
                                value=float(_sc_cfg.get("acceleration_lookback") or 3),
                                min=1, max=10, step=1, format="%.0f",
                            ).classes("w-40").props("dense")
                            ui.label("Number of prior candles to average for the acceleration comparison.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_accel_min_ratio_input = ui.number(
                                label="Acceleration min ratio",
                                value=float(_sc_cfg.get("acceleration_min_ratio") or 1.5),
                                min=1.0, max=5.0, step=0.1, format="%.1f",
                            ).classes("w-40").props("dense")
                            ui.label("Current body must be at least this multiple of recent average (1.5 = 50% larger).").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_rsi_rising_switch = ui.switch(
                                "Require RSI rising",
                                value=bool(_sc_cfg.get("require_rsi_rising", True)),
                            ).props("dense color=primary")
                            ui.label("RSI must be rising (bullish candle) — momentum still building, not fading.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_vol_rsi_rising_switch = ui.switch(
                                "Require volume RSI rising",
                                value=bool(_sc_cfg.get("require_volume_rsi_rising", True)),
                            ).props("dense color=primary")
                            ui.label("Volume RSI must be rising vs previous candle — volume momentum still building.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_max_spike_ext_input = ui.number(
                                label="Max spike extension %",
                                value=float(_sc_cfg.get("max_spike_extension_pct") or 2.0),
                                min=0.0, max=20.0, step=0.5, format="%.1f",
                            ).classes("w-48").props("dense")
                            ui.label("Block entry if price already moved more than this % from spike origin (0 = disabled). Prevents entering at the top.").classes("text-xs text-slate-500")
                        with ui.row().classes("w-full flex-wrap gap-4 items-center mt-1"):
                            sc_spike_lookback_input = ui.number(
                                label="Spike lookback",
                                value=float(_sc_cfg.get("spike_lookback") or 5),
                                min=2, max=20, step=1, format="%.0f",
                            ).classes("w-40").props("dense")
                            ui.label("Candles to look back to find the spike origin (lowest low for buys, highest high for sells).").classes("text-xs text-slate-500")
                    _active_badge_sc = ui.badge("Active", color="positive").bind_visibility_from(
                        sc_enabled_switch, "value"
                    )

            def _set_sc_defaults() -> None:
                """Fill all Spike Continuation fields with the recommended configuration."""
                sc_tp_input.value = 5.0
                sc_sl_input.value = 3.0
                sc_volume_rsi_min_input.value = 75.0
                sc_rsi_min_input.value = 55.0
                sc_rsi_max_input.value = 75.0
                sc_bb_breakout_switch.value = True
                sc_candle_strength_switch.value = True
                sc_candle_strength_pct_input.value = 70.0
                sc_min_bb_bw_input.value = 3.0
                sc_max_adx_input.value = 0.0
                sc_momentum_accel_switch.value = True
                sc_accel_lookback_input.value = 3
                sc_accel_min_ratio_input.value = 1.5
                sc_rsi_rising_switch.value = True
                sc_vol_rsi_rising_switch.value = True
                sc_max_spike_ext_input.value = 2.0
                sc_spike_lookback_input.value = 5
                ui.notify("Spike Continuation fields set to recommended defaults — click Save to persist", color="info")

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    shotgun_switch = ui.switch(
                        value=bool(shotgun.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Shotgun").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Close positions when total account equity has moved past a configured "
                            "threshold since the last prompt run. "
                            "Take Profit closes ALL open positions. "
                            "Stop Loss behaviour is controlled by the 'Close only negative trades' toggle."
                        ).classes("text-xs text-slate-500 mb-3")

                        ui.label("Take Profit").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _sg_tp_pct_raw = shotgun.get("tp_pct")
                            shotgun_tp_pct = ui.number(
                                label="TP at Total % PnL",
                                value=float(_sg_tp_pct_raw) if _sg_tp_pct_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Close ALL when equity up by this % (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                            _sg_tp_usd_raw = shotgun.get("tp_usd")
                            shotgun_tp_usd = ui.number(
                                label="TP at Total USDT Profit",
                                value=float(_sg_tp_usd_raw) if _sg_tp_usd_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Close ALL when equity up by this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )

                        ui.label("Stop Loss").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _sg_sl_pct_raw = shotgun.get("sl_pct")
                            shotgun_sl_pct = ui.number(
                                label="SL at Total % PnL",
                                value=float(_sg_sl_pct_raw) if _sg_sl_pct_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Trigger when equity down by this % (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                            _sg_sl_usd_raw = shotgun.get("sl_usd")
                            shotgun_sl_usd = ui.number(
                                label="SL at Total USDT Loss",
                                value=float(_sg_sl_usd_raw) if _sg_sl_usd_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Trigger when equity down by this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                        with ui.row().classes("items-center gap-2 mt-1"):
                            shotgun_close_negative = ui.switch(
                                "Close only negative trades",
                                value=bool(shotgun.get("close_only_negative", False)),
                            ).props("dense color=warning")
                            ui.label(
                                "When ON: SL closes only positions with negative PnL. "
                                "When OFF: SL closes all open positions."
                            ).classes("text-xs text-slate-500")
                    _active_badge_sg = ui.badge("Active", color="positive").bind_visibility_from(
                        shotgun_switch, "value"
                    )

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    protector_switch = ui.switch(
                        value=bool(protector.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Protector").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Automatically ratchet the stop-loss into profit as a position's "
                            "unrealised PnL climbs through configurable step levels. "
                            "Only the SL is moved; the take-profit remains unchanged."
                        ).classes("text-xs text-slate-500 mb-1")
                        ui.label(
                            "The lock ratio grows with each step so the SL trails price more "
                            "tightly as profits compound: "
                            "effective lock = 1 − (1 − lock_ratio) ÷ step_number."
                        ).classes("text-xs text-slate-400 mb-2")
                        with ui.row().classes("gap-4 items-start"):
                            _pt_act_raw = protector.get("activate_pct")
                            protector_activate = ui.number(
                                label="Activate at (% PnL)",
                                value=float(_pt_act_raw) if _pt_act_raw is not None else 10.0,
                                min=0.1,
                                step=1.0,
                                precision=1,
                            ).classes("w-48").props(
                                "hint='Minimum uplRatio % before strategy engages' persistent-hint"
                            )
                            _pt_step_raw = protector.get("step_pct")
                            protector_step = ui.number(
                                label="Step size (% PnL)",
                                value=float(_pt_step_raw) if _pt_step_raw is not None else 10.0,
                                min=0.1,
                                step=1.0,
                                precision=1,
                            ).classes("w-48").props(
                                "hint='PnL % increment at which SL is re-evaluated' persistent-hint"
                            )
                            _pt_lock_raw = protector.get("lock_ratio")
                            protector_lock = ui.number(
                                label="Lock ratio (0–1)",
                                value=float(_pt_lock_raw) if _pt_lock_raw is not None else 0.5,
                                min=0.01,
                                max=1.0,
                                step=0.05,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Fraction of step level locked in as SL (e.g. 0.5 = 50%)' persistent-hint"
                            )
                        def _protector_table_html(activate: float, step: float, lock: float, rows: int = 10) -> str:
                            tbody = ""
                            for n in range(1, rows + 1):
                                pnl = activate + (n - 1) * step
                                eff_lock = 1.0 - (1.0 - lock) / n
                                sl = pnl * eff_lock
                                tbody += (
                                    f"<tr>"
                                    f"<td class='pr-3'>{n}</td>"
                                    f"<td class='pr-3'>+{pnl:.1f} %</td>"
                                    f"<td class='pr-3'>{eff_lock * 100:.0f} %</td>"
                                    f"<td>+{sl:.1f} %</td>"
                                    f"</tr>"
                                )
                            return (
                                "<table class='text-xs text-slate-400 mt-2 mb-1 border-collapse'>"
                                "<thead><tr>"
                                "<th class='pr-3 text-left font-medium'>Step</th>"
                                "<th class='pr-3 text-left font-medium'>PnL reached</th>"
                                "<th class='pr-3 text-left font-medium'>Eff. lock</th>"
                                "<th class='text-left font-medium'>SL placed at</th>"
                                "</tr></thead>"
                                f"<tbody>{tbody}</tbody></table>"
                            )
                        ui.label("Live preview (updates as you type):").classes("text-[10px] text-slate-400 mt-1")
                        _pt_act_init = float(protector.get("activate_pct") or 10.0)
                        _pt_step_init = float(protector.get("step_pct") or 10.0)
                        _pt_lock_init = float(protector.get("lock_ratio") or 0.5)
                        protector_table = ui.html(
                            content=_protector_table_html(_pt_act_init, _pt_step_init, _pt_lock_init),
                            sanitize=False,
                        )
                        def _refresh_protector_table() -> None:
                            try:
                                act = float(protector_activate.value or 10.0)
                                stp = float(protector_step.value or 10.0)
                                lk = float(protector_lock.value or 0.5)
                            except (TypeError, ValueError):
                                return
                            protector_table.content = _protector_table_html(act, stp, lk)
                        protector_activate.on_value_change(lambda _: _refresh_protector_table())
                        protector_step.on_value_change(lambda _: _refresh_protector_table())
                        protector_lock.on_value_change(lambda _: _refresh_protector_table())
                    _active_badge_prot = ui.badge("Active", color="positive").bind_visibility_from(
                        protector_switch, "value"
                    )

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    commutator_switch = ui.switch(
                        value=bool(commutator.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Commutator").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Automatically reverse a losing position when its unrealised PnL "
                            "drops past a configured threshold. "
                            "The reversed position opens at market price with the same contract size. "
                            "Use 'Post-reversal TP' to set a take-profit on the new position. "
                            "When max flips are exhausted the position is closed without reopening."
                        ).classes("text-xs text-slate-500 mb-3")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _cmtr_lp_raw = commutator.get("reverse_at_loss_pct")
                            cmtr_loss_pct = ui.number(
                                label="Reverse at % Loss",
                                value=float(_cmtr_lp_raw) if _cmtr_lp_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip when position PnL ≤ -X% (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                            _cmtr_lu_raw = commutator.get("reverse_at_loss_usd")
                            cmtr_loss_usd = ui.number(
                                label="Reverse at USDT Loss",
                                value=float(_cmtr_lu_raw) if _cmtr_lu_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip when unrealised loss ≥ this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                        with ui.row().classes("gap-4 items-start"):
                            _cmtr_mf_raw = commutator.get("max_flips")
                            cmtr_max_flips = ui.number(
                                label="Number of flips",
                                value=int(_cmtr_mf_raw) if _cmtr_mf_raw is not None else 1,
                                min=0,
                                max=10,
                                step=1,
                                precision=0,
                            ).classes("w-48").props(
                                "hint='Max reversals before closing (0 = close without reversing)' "
                                "persistent-hint"
                            )
                            _cmtr_tp_raw = commutator.get("post_reversal_tp_pct")
                            cmtr_post_tp = ui.number(
                                label="Post-reversal TP (%)",
                                value=float(_cmtr_tp_raw) if _cmtr_tp_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Set TP at last_price ± X% after reversal (blank = none)' "
                                "persistent-hint clearable stack-label"
                            )
                    _active_badge_cmtr = ui.badge("Active", color="positive").bind_visibility_from(
                        commutator_switch, "value"
                    )

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    alternator_switch = ui.switch(
                        value=bool(alternator.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("Alternator").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Oscillate between long and short using configurable profit and loss thresholds. "
                            "When a profit threshold is hit the position is reversed; "
                            "when a loss threshold is hit the bot flips back. "
                            "Optionally let a winning position ride (hand it to Protector) or "
                            "apply a hard stop-loss with no reversal. "
                            "Mutually exclusive with Skimming and Commutator."
                        ).classes("text-xs text-slate-500 mb-3")
                        ui.label("Reverse at Profit").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_dynamic_switch = ui.switch(
                                "Dynamic Threshold",
                                value=bool(alternator.get("dynamic_threshold", False)),
                            ).props(
                                "hint='Compute reversal threshold from average HTF candle amplitude: (H−L)/mid × 100' persistent-hint dense color=primary"
                            )
                            _altr_df_raw = alternator.get("dynamic_threshold_factor", 1.0)
                            altr_dynamic_factor = ui.number(
                                label="Factor",
                                value=float(_altr_df_raw) if _altr_df_raw is not None else 1.0,
                                min=0.1,
                                max=10.0,
                                step=0.1,
                                precision=2,
                            ).classes("w-28").props(
                                "hint='threshold = avg_amplitude × factor' persistent-hint suffix='×'"
                            ).bind_enabled_from(altr_dynamic_switch, "value")
                            _altr_dl_raw = alternator.get("dynamic_threshold_lookback", 20)
                            altr_dynamic_lookback = ui.number(
                                label="Lookback",
                                value=int(_altr_dl_raw) if _altr_dl_raw is not None else 20,
                                min=3,
                                max=200,
                                step=1,
                                precision=0,
                            ).classes("w-28").props(
                                "hint='HTF bars used for amplitude average' persistent-hint suffix='bars'"
                            ).bind_enabled_from(altr_dynamic_switch, "value")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_rpp_raw = alternator.get("reverse_at_profit_pct")
                            altr_rev_profit_pct = ui.number(
                                label="Reverse at % Profit",
                                value=float(_altr_rpp_raw) if _altr_rpp_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip when position PnL >= +X% — ignored when Dynamic Threshold is ON' "
                                "persistent-hint clearable stack-label"
                            ).bind_enabled_from(altr_dynamic_switch, "value", backward=lambda v: not v)
                            _altr_rpu_raw = alternator.get("reverse_at_profit_usd")
                            altr_rev_profit_usd = ui.number(
                                label="Reverse at USDT Profit",
                                value=float(_altr_rpu_raw) if _altr_rpu_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip when unrealised profit >= this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_trailing_switch = ui.switch(
                                "Trailing Reverse",
                                value=bool(alternator.get("trailing_reverse", False)),
                            ).props(
                                "hint='Wait for PnL to pull back from its peak before reversing, rather than reversing immediately at threshold' persistent-hint dense color=primary"
                            )
                            _altr_tpb_raw = alternator.get("trailing_pullback_pct", 10.0)
                            altr_trailing_pullback_pct = ui.number(
                                label="Pullback %",
                                value=float(_altr_tpb_raw) if _altr_tpb_raw is not None else 10.0,
                                min=0.0,
                                max=100.0,
                                step=0.5,
                                precision=1,
                            ).classes("w-36").props(
                                "hint='Reverse when PnL drops this % below its peak (e.g. 10 = reverse at 90% of peak)' persistent-hint suffix='%'"
                            ).bind_enabled_from(altr_trailing_switch, "value")
                        ui.label("Trailing Close").classes("text-xs font-semibold text-slate-600 mt-1")
                        ui.label(
                            "Let profit run and close flat (no reversal) when price pulls back from the peak. "
                            "Activate at a minimum profit level, then close when PnL drops below the pullback % of the peak."
                        ).classes("text-xs text-slate-400 mb-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_trailing_close_switch = ui.switch(
                                "Trailing Close",
                                value=bool(alternator.get("trailing_close", False)),
                            ).props(
                                "hint='Close flat (no reversal) when profit pulls back from its peak — takes priority over Trailing Reverse on the profit side' persistent-hint dense color=primary"
                            )
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_tca_pct_raw = alternator.get("trailing_close_activate_pct")
                            altr_trailing_close_activate_pct = ui.number(
                                label="Activate at % profit",
                                value=float(_altr_tca_pct_raw) if _altr_tca_pct_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Start trailing close once PnL >= +X% (blank = use USD threshold)' persistent-hint clearable stack-label"
                            ).bind_enabled_from(altr_trailing_close_switch, "value")
                            _altr_tca_usd_raw = alternator.get("trailing_close_activate_usd")
                            altr_trailing_close_activate_usd = ui.number(
                                label="Activate at USDT profit",
                                value=float(_altr_tca_usd_raw) if _altr_tca_usd_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Start trailing close once unrealised profit >= this USDT (blank = use % threshold)' persistent-hint clearable stack-label"
                            ).bind_enabled_from(altr_trailing_close_switch, "value")
                            _altr_tcpb_raw = alternator.get("trailing_close_pullback_pct", 10.0)
                            altr_trailing_close_pullback_pct = ui.number(
                                label="Pullback %",
                                value=float(_altr_tcpb_raw) if _altr_tcpb_raw is not None else 10.0,
                                min=0.1,
                                max=100.0,
                                step=0.5,
                                precision=1,
                            ).classes("w-36").props(
                                "hint='Close when PnL drops this % below its peak (e.g. 10 = close at 90% of peak)' persistent-hint suffix='%'"
                            ).bind_enabled_from(altr_trailing_close_switch, "value")
                        ui.label("Entry Position Filter").classes("text-xs font-semibold text-slate-600 mt-1")
                        ui.label(
                            "Blocks reversals when price is near the top of the recent range (for LONGs) "
                            "or near the bottom (for SHORTs), using closed LTF candles."
                        ).classes("text-xs text-slate-400 mb-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_cpf_switch = ui.switch(
                                "Candle Position Filter",
                                value=bool(alternator.get("candle_position_filter", False)),
                            ).props(
                                "hint='Block reversal entries when price is at an unfavourable candle position' persistent-hint dense color=primary"
                            )
                            _altr_cpflb_raw = alternator.get("candle_position_lookback", 20)
                            altr_cpf_lookback = ui.number(
                                label="Lookback bars",
                                value=int(_altr_cpflb_raw) if _altr_cpflb_raw is not None else 20,
                                min=2,
                                max=200,
                                step=1,
                                precision=0,
                            ).classes("w-32").props(
                                "hint='Number of closed LTF bars to compute range' persistent-hint"
                            ).bind_enabled_from(altr_cpf_switch, "value")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            _altr_cplm_raw = alternator.get("candle_position_long_max", 0.75)
                            altr_cpf_long_max = ui.number(
                                label="Long max (0–1)",
                                value=float(_altr_cplm_raw) if _altr_cplm_raw is not None else 0.75,
                                min=0.0,
                                max=1.0,
                                step=0.05,
                                precision=2,
                            ).classes("w-40").props(
                                "hint='Block LONG reversal if range position > this (e.g. 0.75 = top 25%)' persistent-hint suffix=''"
                            ).bind_enabled_from(altr_cpf_switch, "value")
                            _altr_cpsm_raw = alternator.get("candle_position_short_min", 0.25)
                            altr_cpf_short_min = ui.number(
                                label="Short min (0–1)",
                                value=float(_altr_cpsm_raw) if _altr_cpsm_raw is not None else 0.25,
                                min=0.0,
                                max=1.0,
                                step=0.05,
                                precision=2,
                            ).classes("w-40").props(
                                "hint='Block SHORT reversal if range position < this (e.g. 0.25 = bottom 25%)' persistent-hint suffix=''"
                            ).bind_enabled_from(altr_cpf_switch, "value")
                        ui.label("Footprint Delta Filter").classes("text-xs font-semibold text-slate-600 mt-1")
                        ui.label(
                            "Blocks reversals when the 15-minute footprint net delta opposes the entry direction "
                            "(e.g. negative delta blocks a LONG). Requires the trades WS feed to be active."
                        ).classes("text-xs text-slate-400 mb-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_fpd_switch = ui.switch(
                                "Footprint Delta Filter",
                                value=bool(alternator.get("footprint_delta_filter", False)),
                            ).props(
                                "hint='Block reversal entries when footprint net delta opposes the intended direction' persistent-hint dense color=primary"
                            )
                            _altr_fpdr_raw = alternator.get("footprint_delta_min_ratio", 0.0)
                            altr_fpd_min_ratio = ui.number(
                                label="Min imbalance ratio",
                                value=float(_altr_fpdr_raw) if _altr_fpdr_raw is not None else 0.0,
                                min=0.0,
                                max=1.0,
                                step=0.01,
                                precision=2,
                            ).classes("w-44").props(
                                "hint='Only block if |net_delta|/total_vol ≥ this (0 = any imbalance)' "
                                "persistent-hint suffix=''"
                            ).bind_enabled_from(altr_fpd_switch, "value")
                        ui.label("OB Wall Suppression").classes("text-xs font-semibold text-slate-600 mt-1")
                        ui.label(
                            "Block reversals when a dominant opposing limit-order wall sits within proximity of the current price. "
                            "Uses the same wall-ratio logic as the OB Wall Guard guardrail."
                        ).classes("text-xs text-slate-400 mb-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_ob_wall_suppress = ui.switch(
                                "OB Wall Suppress",
                                value=bool(alternator.get("ob_wall_suppress", False)),
                            ).props(
                                "hint='Block reversal entries when a dominant order-book wall opposes the flip direction' persistent-hint dense color=primary"
                            )
                            altr_ob_wall_proximity = ui.number(
                                label="Proximity %",
                                value=alternator.get("ob_wall_proximity_pct", 1.0),
                                min=0.1, max=10.0, step=0.1, precision=1,
                            ).classes("w-28").props(
                                "hint='Scan opposing side within this % of price' persistent-hint suffix='%'"
                            ).bind_enabled_from(altr_ob_wall_suppress, "value")
                            altr_ob_wall_ratio = ui.number(
                                label="Wall Ratio",
                                value=alternator.get("ob_wall_ratio", 3.0),
                                min=1.0, max=20.0, step=0.5, precision=1,
                            ).classes("w-28").props(
                                "hint='Level counts as wall when size ≥ this × average' persistent-hint suffix='×'"
                            ).bind_enabled_from(altr_ob_wall_suppress, "value")
                        ui.label("Continuous LLM Supervision").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_continuous_llm_switch = ui.switch(
                                "Continuously call LLM",
                                value=bool(alternator.get("continuous_llm", False)),
                            ).props(
                                "hint='LLM supervises live Alternator positions: can pause, close, or cap reversals based on market signals' persistent-hint dense color=primary"
                            )
                        ui.label("Restart at Loss").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-center mb-2"):
                            altr_dynamic_loss_switch = ui.switch(
                                "Dynamic Loss Threshold",
                                value=bool(alternator.get("dynamic_loss_threshold", False)),
                            ).props(
                                "hint='Compute loss threshold from average HTF candle amplitude: (H−L)/mid × 100' persistent-hint dense color=primary"
                            )
                            _altr_dlf_raw = alternator.get("dynamic_loss_factor", 1.0)
                            altr_dynamic_loss_factor = ui.number(
                                label="Factor",
                                value=float(_altr_dlf_raw) if _altr_dlf_raw is not None else 1.0,
                                min=0.1,
                                max=10.0,
                                step=0.1,
                                precision=2,
                            ).classes("w-28").props(
                                "hint='threshold = avg_amplitude × factor' persistent-hint suffix='×'"
                            ).bind_enabled_from(altr_dynamic_loss_switch, "value")
                            _altr_dll_raw = alternator.get("dynamic_loss_lookback", 20)
                            altr_dynamic_loss_lookback = ui.number(
                                label="Lookback",
                                value=int(_altr_dll_raw) if _altr_dll_raw is not None else 20,
                                min=3,
                                max=200,
                                step=1,
                                precision=0,
                            ).classes("w-28").props(
                                "hint='HTF bars used for amplitude average' persistent-hint suffix='bars'"
                            ).bind_enabled_from(altr_dynamic_loss_switch, "value")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_rlp_raw = alternator.get("restart_at_loss_pct")
                            altr_restart_loss_pct = ui.number(
                                label="Restart at % Loss",
                                value=float(_altr_rlp_raw) if _altr_rlp_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip back when position PnL <= -X% — ignored when Dynamic Loss Threshold is ON (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            ).bind_enabled_from(altr_dynamic_loss_switch, "value", backward=lambda v: not v)
                            _altr_rlu_raw = alternator.get("restart_at_loss_usd")
                            altr_restart_loss_usd = ui.number(
                                label="Restart at USDT Loss",
                                value=float(_altr_rlu_raw) if _altr_rlu_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Flip back when unrealised loss >= this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                        ui.label("Limits").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_mr_raw = alternator.get("max_reversals")
                            altr_max_reversals = ui.number(
                                label="Max reversals",
                                value=int(_altr_mr_raw) if _altr_mr_raw is not None else None,
                                min=0,
                                max=20,
                                step=1,
                                precision=0,
                            ).classes("w-48").props(
                                "hint='Total flip limit (blank = unlimited)' "
                                "persistent-hint clearable stack-label"
                            )
                            altr_close_on_max_reversals = ui.switch(
                                "Close on Limit",
                                value=bool(alternator.get("close_on_max_reversals", False)),
                            ).props(
                                "hint='Close flat immediately once max reversals are reached, "
                                "instead of waiting for the next threshold' persistent-hint dense color=primary"
                            ).bind_enabled_from(altr_max_reversals, "value", backward=lambda v: v not in (None, ""))
                        ui.label("Ride (hand to Protector)").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_ride_pct_raw = alternator.get("ride_at_profit_pct")
                            altr_ride_profit_pct = ui.number(
                                label="Ride at % Profit",
                                value=float(_altr_ride_pct_raw) if _altr_ride_pct_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Stop reversing; hand to Protector when PnL >= +X% (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                            _altr_ride_usd_raw = alternator.get("ride_at_profit_usd")
                            altr_ride_profit_usd = ui.number(
                                label="Ride at USDT Profit",
                                value=float(_altr_ride_usd_raw) if _altr_ride_usd_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Stop reversing; hand to Protector when profit >= this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                        ui.label("Hard Stop (close only, no flip)").classes("text-xs font-semibold text-slate-600 mt-1")
                        with ui.row().classes("gap-4 items-start mb-2"):
                            _altr_slp_raw = alternator.get("stop_at_loss_pct")
                            altr_stop_loss_pct = ui.number(
                                label="Hard Stop at % Loss",
                                value=float(_altr_slp_raw) if _altr_slp_raw is not None else None,
                                min=0.01,
                                step=0.1,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Close without reversing when PnL <= -X% (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                            _altr_slu_raw = alternator.get("stop_at_loss_usd")
                            altr_stop_loss_usd = ui.number(
                                label="Hard Stop at USDT Loss",
                                value=float(_altr_slu_raw) if _altr_slu_raw is not None else None,
                                min=0.01,
                                step=1.0,
                                precision=2,
                            ).classes("w-48").props(
                                "hint='Close without reversing when loss >= this USDT (blank = disabled)' "
                                "persistent-hint clearable stack-label"
                            )
                    _active_badge_altr = ui.badge("Active", color="positive").bind_visibility_from(
                        alternator_switch, "value"
                    )

            with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                _ob_stops_cfg = strategy.get("ob_wall_stops") or {}
                with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                    ob_wall_stops_switch = ui.switch(
                        value=bool(_ob_stops_cfg.get("enabled", False)),
                    ).props("dense color=primary")
                    with ui.expansion("OB Wall Dynamic Stop-Loss").classes("flex-1 text-sm font-medium"):
                        ui.label(
                            "Automatically tightens stop-losses by anchoring them just behind the nearest "
                            "significant resting limit-order wall in the supporting direction. "
                            "For LONG positions the stop moves up toward the largest bid wall below price; "
                            "for SHORT positions it moves down toward the largest ask wall above price. "
                            "The stop is never loosened."
                        ).classes("text-xs text-slate-400 mb-2")
                        with ui.row().classes("gap-4 items-center flex-wrap mb-2"):
                            ob_wall_stops_proximity = ui.number(
                                label="Proximity %",
                                value=_ob_stops_cfg.get("proximity_pct", 2.0),
                                min=0.1, max=20.0, step=0.1, precision=1,
                            ).classes("w-32").props(
                                "hint='Scan for walls within this % of current price' persistent-hint suffix='%'"
                            ).bind_enabled_from(ob_wall_stops_switch, "value")
                            ob_wall_stops_ratio = ui.number(
                                label="Wall Ratio",
                                value=_ob_stops_cfg.get("wall_ratio", 3.0),
                                min=1.0, max=20.0, step=0.5, precision=1,
                            ).classes("w-32").props(
                                "hint='A level qualifies as a wall when its size ≥ N× average level size' persistent-hint suffix='×'"
                            ).bind_enabled_from(ob_wall_stops_switch, "value")
                            ob_wall_stops_min_improvement = ui.number(
                                label="Min Improvement %",
                                value=_ob_stops_cfg.get("min_sl_improvement_pct", 0.1),
                                min=0.01, max=5.0, step=0.05, precision=2,
                            ).classes("w-36").props(
                                "hint='Minimum % price improvement required to re-place the SL algo (avoids micro-churn)' persistent-hint suffix='%'"
                            ).bind_enabled_from(ob_wall_stops_switch, "value")
                            ob_wall_stops_buffer = ui.number(
                                label="Buffer Behind Wall %",
                                value=_ob_stops_cfg.get("sl_buffer_pct", 0.1),
                                min=0.01, max=2.0, step=0.05, precision=2,
                            ).classes("w-40").props(
                                "hint='Stop is placed this % below/above the wall price (breathing room)' persistent-hint suffix='%'"
                            ).bind_enabled_from(ob_wall_stops_switch, "value")
                _active_badge_ob_stops = ui.badge("Active", color="positive").bind_visibility_from(
                    ob_wall_stops_switch, "value"
                )

        # Only one of these three can be enabled at a time.  When the user turns
        # one ON the others are silently disabled and a notification is shown.
        _mutex_busy: dict[str, bool] = {"flag": False}

        def on_skimming_toggle(e: Any) -> None:
            if _mutex_busy["flag"] or not e.value:
                return
            _mutex_busy["flag"] = True
            alternator_switch.set_value(False)
            _mutex_busy["flag"] = False
            ui.notify("Alternator disabled — Skimming is now active", color="info")

        def on_commutator_toggle(e: Any) -> None:
            if _mutex_busy["flag"] or not e.value:
                return
            _mutex_busy["flag"] = True
            alternator_switch.set_value(False)
            _mutex_busy["flag"] = False
            ui.notify("Alternator disabled — Commutator is now active", color="info")

        def on_alternator_toggle(e: Any) -> None:
            if _mutex_busy["flag"] or not e.value:
                return
            _mutex_busy["flag"] = True
            skimming_switch.set_value(False)
            commutator_switch.set_value(False)
            _mutex_busy["flag"] = False
            ui.notify(
                "Commutator and Skimming disabled — Alternator is now the active position strategy",
                color="info",
            )

        skimming_switch.on_value_change(on_skimming_toggle)
        commutator_switch.on_value_change(on_commutator_toggle)
        alternator_switch.on_value_change(on_alternator_toggle)

        async def save_strategy_settings(event: Any | None = None) -> None:
            _sl_val = stop_loss_input.value
            _sg_tp_pct_val = shotgun_tp_pct.value
            _sg_tp_usd_val = shotgun_tp_usd.value
            _sg_sl_pct_val = shotgun_sl_pct.value
            _sg_sl_usd_val = shotgun_sl_usd.value
            updated_strategy = {
                "skimming": {
                    "enabled": bool(skimming_switch.value),
                    "threshold_pct": float(threshold_input.value or 2.0),
                    "stop_loss_pct": float(_sl_val) if _sl_val not in (None, "") else None,
                },
                "shotgun": {
                    "enabled": bool(shotgun_switch.value),
                    "tp_pct": float(_sg_tp_pct_val) if _sg_tp_pct_val not in (None, "") else None,
                    "tp_usd": float(_sg_tp_usd_val) if _sg_tp_usd_val not in (None, "") else None,
                    "sl_pct": float(_sg_sl_pct_val) if _sg_sl_pct_val not in (None, "") else None,
                    "sl_usd": float(_sg_sl_usd_val) if _sg_sl_usd_val not in (None, "") else None,
                    "close_only_negative": bool(shotgun_close_negative.value),
                },
                "protector": {
                    "enabled": bool(protector_switch.value),
                    "activate_pct": float(protector_activate.value or 10.0),
                    "step_pct": float(protector_step.value or 10.0),
                    "lock_ratio": float(protector_lock.value or 0.5),
                },
                "commutator": {
                    "enabled": bool(commutator_switch.value),
                    "reverse_at_loss_pct": float(cmtr_loss_pct.value) if cmtr_loss_pct.value not in (None, "") else None,
                    "reverse_at_loss_usd": float(cmtr_loss_usd.value) if cmtr_loss_usd.value not in (None, "") else None,
                    "max_flips": int(cmtr_max_flips.value or 1),
                    "post_reversal_tp_pct": float(cmtr_post_tp.value) if cmtr_post_tp.value not in (None, "") else None,
                },
                "alternator": {
                    "enabled": bool(alternator_switch.value),
                    "reverse_at_profit_pct": float(altr_rev_profit_pct.value) if altr_rev_profit_pct.value not in (None, "") else None,
                    "reverse_at_profit_usd": float(altr_rev_profit_usd.value) if altr_rev_profit_usd.value not in (None, "") else None,
                    "dynamic_threshold": bool(altr_dynamic_switch.value),
                    "dynamic_threshold_factor": float(altr_dynamic_factor.value or 1.0),
                    "dynamic_threshold_lookback": int(altr_dynamic_lookback.value or 20),
                    "dynamic_loss_threshold": bool(altr_dynamic_loss_switch.value),
                    "dynamic_loss_factor": float(altr_dynamic_loss_factor.value or 1.0),
                    "dynamic_loss_lookback": int(altr_dynamic_loss_lookback.value or 20),
                    "trailing_reverse": bool(altr_trailing_switch.value),
                    "trailing_pullback_pct": float(altr_trailing_pullback_pct.value or 10.0),
                    "trailing_close": bool(altr_trailing_close_switch.value),
                    "trailing_close_activate_pct": float(altr_trailing_close_activate_pct.value) if altr_trailing_close_activate_pct.value not in (None, "") else None,
                    "trailing_close_activate_usd": float(altr_trailing_close_activate_usd.value) if altr_trailing_close_activate_usd.value not in (None, "") else None,
                    "trailing_close_pullback_pct": float(altr_trailing_close_pullback_pct.value or 10.0),
                    "candle_position_filter": bool(altr_cpf_switch.value),
                    "candle_position_long_max": float(altr_cpf_long_max.value or 0.75),
                    "candle_position_short_min": float(altr_cpf_short_min.value or 0.25),
                    "candle_position_lookback": int(altr_cpf_lookback.value or 20),
                    "footprint_delta_filter": bool(altr_fpd_switch.value),
                    "footprint_delta_min_ratio": float(altr_fpd_min_ratio.value or 0.0),
                    "ob_wall_suppress": bool(altr_ob_wall_suppress.value),
                    "ob_wall_proximity_pct": float(altr_ob_wall_proximity.value or 1.0),
                    "ob_wall_ratio": float(altr_ob_wall_ratio.value or 3.0),
                    "continuous_llm": bool(altr_continuous_llm_switch.value),
                    "max_reversals": int(altr_max_reversals.value) if altr_max_reversals.value not in (None, "") else None,
                    "close_on_max_reversals": bool(altr_close_on_max_reversals.value),
                    "restart_at_loss_pct": float(altr_restart_loss_pct.value) if altr_restart_loss_pct.value not in (None, "") else None,
                    "restart_at_loss_usd": float(altr_restart_loss_usd.value) if altr_restart_loss_usd.value not in (None, "") else None,
                    "ride_at_profit_pct": float(altr_ride_profit_pct.value) if altr_ride_profit_pct.value not in (None, "") else None,
                    "ride_at_profit_usd": float(altr_ride_profit_usd.value) if altr_ride_profit_usd.value not in (None, "") else None,
                    "stop_at_loss_pct": float(altr_stop_loss_pct.value) if altr_stop_loss_pct.value not in (None, "") else None,
                    "stop_at_loss_usd": float(altr_stop_loss_usd.value) if altr_stop_loss_usd.value not in (None, "") else None,
                },
                "ob_wall_stops": {
                    "enabled": bool(ob_wall_stops_switch.value),
                    "proximity_pct": float(ob_wall_stops_proximity.value or 2.0),
                    "wall_ratio": float(ob_wall_stops_ratio.value or 3.0),
                    "min_sl_improvement_pct": float(ob_wall_stops_min_improvement.value or 0.1),
                    "sl_buffer_pct": float(ob_wall_stops_buffer.value or 0.1),
                },
            }
            config["strategy"] = updated_strategy
            try:
                await save_strategy_config(updated_strategy)
            except Exception as exc:  # pragma: no cover - optional DB
                ui.notify(f"Failed to persist strategy config: {exc}", color="warning")
            # Persist mean reversion signal fields into config["launcher"]["strategies"]["mean_reversion"].
            _launcher_cfg = config.get("launcher") or {}
            _strategies_cfg = dict(_launcher_cfg.get("strategies") or {})
            _strategies_cfg["mean_reversion"] = {
                "enabled": bool(mr_enabled_switch.value),
                "tp_pct": float(mr_tp_input.value) if mr_tp_input.value not in (None, "") else None,
                "sl_pct": float(mr_sl_input.value) if mr_sl_input.value not in (None, "") else None,
                "rsi_oversold": float(mr_rsi_oversold_input.value or 30.0),
                "rsi_overbought": float(mr_rsi_overbought_input.value or 70.0),
                "min_adx": float(mr_min_adx_input.value or 0.0),
                "max_adx": float(mr_max_adx_input.value or 0.0),
                "require_htf_trend": bool(mr_require_htf_switch.value),
                "require_cmf": bool(mr_require_cmf_switch.value),
                "require_htf_cmf": bool(mr_require_htf_cmf_switch.value),
                "require_cmf_cross": bool(mr_require_cmf_cross_switch.value),
                "require_cmf_no_divergence": bool(mr_require_cmf_no_div_switch.value),
                "require_footprint_delta": bool(mr_require_fp_delta_switch.value),
                "require_bb_position": bool(mr_require_bb_switch.value),
                "bb_proximity_pct": float(mr_bb_proximity_input.value or 0.0),
                "min_bb_bandwidth": float(mr_min_bb_bw_input.value or 0.0),
                "max_bb_bandwidth": float(mr_max_bb_bw_input.value or 0.0),
                "flip_launcher_direction": str(mr_flip_select.value) if mr_flip_switch.value else None,
                "dynamic_tp": bool(mr_dynamic_tp_switch.value),
                "dynamic_tp_fraction": float(mr_dynamic_tp_fraction_input.value or 0.7),
                "require_candle_rejection": bool(mr_candle_rejection_switch.value),
                "candle_rejection_pct": float(mr_candle_rejection_pct_input.value or 30.0),
                "require_vwap_reversion": bool(mr_vwap_reversion_switch.value),
                "vwap_min_distance_pct": float(mr_vwap_min_dist_input.value or 1.0),
                "require_volume_cooling": bool(mr_volume_cooling_switch.value),
                "volume_rsi_max": float(mr_volume_rsi_max_input.value or 70.0),
            }
            _strategies_cfg["spike_continuation"] = {
                "enabled": bool(sc_enabled_switch.value),
                "tp_pct": float(sc_tp_input.value) if sc_tp_input.value not in (None, "") else None,
                "sl_pct": float(sc_sl_input.value) if sc_sl_input.value not in (None, "") else None,
                "volume_rsi_min": float(sc_volume_rsi_min_input.value or 75.0),
                "rsi_min": float(sc_rsi_min_input.value or 55.0),
                "rsi_max": float(sc_rsi_max_input.value or 75.0),
                "require_bb_breakout": bool(sc_bb_breakout_switch.value),
                "require_candle_strength": bool(sc_candle_strength_switch.value),
                "candle_strength_pct": float(sc_candle_strength_pct_input.value or 70.0),
                "min_bb_bandwidth": float(sc_min_bb_bw_input.value or 3.0),
                "max_adx": float(sc_max_adx_input.value or 0.0),
                "require_momentum_acceleration": bool(sc_momentum_accel_switch.value),
                "acceleration_lookback": int(sc_accel_lookback_input.value or 3),
                "acceleration_min_ratio": float(sc_accel_min_ratio_input.value or 1.5),
                "require_rsi_rising": bool(sc_rsi_rising_switch.value),
                "require_volume_rsi_rising": bool(sc_vol_rsi_rising_switch.value),
                "max_spike_extension_pct": float(sc_max_spike_ext_input.value or 2.0),
                "spike_lookback": int(sc_spike_lookback_input.value or 5),
            }
            _launcher_cfg["strategies"] = _strategies_cfg
            config["launcher"] = _launcher_cfg
            try:
                await save_launcher_config(_launcher_cfg)
            except Exception as exc:  # pragma: no cover - optional DB
                ui.notify(f"Failed to persist launcher signal config: {exc}", color="warning")
            market_service = getattr(app.state, "market_service", None)
            if market_service:
                market_service.set_strategy_config(updated_strategy)
                market_service.set_launcher_config(_launcher_cfg)
            ui.notify("Strategy settings saved", color="positive")

        save_button.on("click", save_strategy_settings)

    def render_history_page() -> None:
        navigation("HISTORY")
        wrapper = page_container()
        wrapper.style("max-width: 100%; width: 100%; margin-left: 0; margin-right: 0;")
        client = ui.context.client
        max_history_rows = 100

        async def push_notification(message: str, *, color: str = "positive") -> None:
            with client:
                ui.notify(message, color=color)

        with wrapper:
            ui.label("History").classes("text-2xl font-bold")
            with ui.row().classes("w-full gap-6 flex-col xl:flex-row items-stretch"):
                with ui.card().classes("w-full flex-1 p-4 gap-3"):
                    with ui.row().classes("w-full items-center justify-between"):
                        ui.label("Executed Trades").classes("text-lg font-semibold")
                        reload_button = (
                            ui.button("Reload Trades", icon="refresh")
                            .props("dense outline")
                        )
                    trades_table = ui.table(
                        columns=[
                            {"name": "timestamp", "label": "Timestamp", "field": "timestamp"},
                            {"name": "symbol", "label": "Symbol", "field": "symbol"},
                            {"name": "side", "label": "Side", "field": "side"},
                            {"name": "price", "label": "Price", "field": "price"},
                            {"name": "amount", "label": "Amount", "field": "amount"},
                            {"name": "fee", "label": "Fee", "field": "fee"},
                            {"name": "pnl", "label": "Realized PnL", "field": "pnl"},
                        ],
                        rows=[],
                    ).classes("w-full")
                    trades_table.add_slot(
                        "body-cell-pnl",
                        """
                        <q-td :props="props">
                          <span
                            :style="{
                              color: props.value === '—'
                                ? '#94a3b8'
                                : props.value.startsWith('+')
                                  ? '#16a34a'
                                  : '#dc2626',
                              fontWeight: props.value === '—' ? 'normal' : '600'
                            }"
                          >{{ props.value }}</span>
                        </q-td>
                        """,
                    )
                with ui.card().classes("w-full flex-1 p-4 gap-3"):
                    with ui.row().classes("w-full items-center justify-between"):
                        ui.label("Prompt Runs").classes("text-lg font-semibold")
                        prompt_reload_button = (
                            ui.button("Reload Prompts", icon="refresh")
                            .props("dense outline")
                        )
                    prompt_table = ui.table(
                        columns=[
                            {"name": "created_at", "label": "Created", "field": "created_at"},
                            {"name": "symbol", "label": "Symbol", "field": "symbol"},
                            {"name": "timeframe", "label": "TF", "field": "timeframe"},
                            {"name": "model", "label": "Model", "field": "model"},
                            {"name": "prompt_version", "label": "Prompt", "field": "prompt_version"},
                            {"name": "action", "label": "Action", "field": "action"},
                            {"name": "confidence", "label": "Conf", "field": "confidence"},
                        ],
                        rows=[],
                    ).classes("w-full")

        async def refresh_trades() -> None:
            try:
                rows = await fetch_recent_trades(max_history_rows)
            except Exception as exc:  # pragma: no cover - db optional
                await push_notification(f"Unable to load trades: {exc}", color="warning")
                return
            rows = (rows or [])[:max_history_rows]
            formatted_rows: list[dict[str, Any]] = []
            for row in rows:
                record = dict(row)
                record["timestamp"] = format_iso_timestamp(
                    record.get("timestamp"),
                    fmt="%Y-%m-%d %H:%M:%S %Z",
                )
                raw_pnl = record.get("pnl")
                if raw_pnl is None:
                    record["pnl"] = "—"
                else:
                    try:
                        pnl_val = float(raw_pnl)
                        sign = "+" if pnl_val >= 0 else ""
                        record["pnl"] = f"{sign}{pnl_val:.2f} USDT"
                    except (TypeError, ValueError):
                        record["pnl"] = "—"
                raw_fee = record.get("fee")
                if raw_fee is None:
                    record["fee"] = "—"
                else:
                    try:
                        record["fee"] = f"{float(raw_fee):.4f} USDT"
                    except (TypeError, ValueError):
                        record["fee"] = "—"
                formatted_rows.append(record)
            trades_table.rows = formatted_rows
            trades_table.update()
            await push_notification(f"Trades refreshed ({len(rows)})")

        async def refresh_prompts() -> None:
            try:
                rows = await fetch_prompt_runs(max_history_rows)
            except Exception as exc:  # pragma: no cover - db optional
                await push_notification(f"Unable to load prompts: {exc}", color="warning")
                return
            rows = (rows or [])[:max_history_rows]
            formatted: list[dict[str, Any]] = []
            for entry in rows:
                decision = entry.get("decision") or {}
                version_label = entry.get("prompt_version_name") or entry.get("prompt_version_id")
                if version_label:
                    version_label = str(version_label)
                formatted.append(
                    {
                        "created_at": format_iso_timestamp(
                            entry.get("created_at"),
                            fmt="%Y-%m-%d %H:%M:%S %Z",
                        ),
                        "symbol": entry.get("symbol"),
                        "timeframe": entry.get("timeframe"),
                        "model": entry.get("model_id"),
                        "prompt_version": version_label or "--",
                        "action": decision.get("action", "--"),
                        "confidence": f"{decision.get('confidence', 0):.2f}" if isinstance(decision.get("confidence"), (int, float)) else "--",
                    }
                )
            prompt_table.rows = formatted
            prompt_table.update()
            await push_notification(f"Prompts refreshed ({len(formatted)})")

        reload_button.on("click", lambda _: asyncio.create_task(refresh_trades()))
        prompt_reload_button.on("click", lambda _: asyncio.create_task(refresh_prompts()))
        asyncio.create_task(refresh_trades())
        asyncio.create_task(refresh_prompts())

    def render_debug_page() -> None:
        navigation("DEBUG")
        wrapper = page_container()
        wrapper.style("max-width: 100%; width: 100%; margin-left: 0; margin-right: 0;")
        config = getattr(app.state, "runtime_config", {}) or {}
        active_version = config.get("prompt_version_name") or config.get("prompt_version_id") or "default"
        with wrapper:
            ui.label(f"Active Prompt Version: {active_version}").classes(
                "text-sm font-semibold text-slate-600"
            )
            with ui.column().classes("w-full gap-4"):
                # ── Application Logs ──────────────────────────────────────────────
                with ui.card().classes(
                    "w-full p-4 gap-2 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    with ui.row().classes("w-full items-center justify-between flex-wrap gap-2"):
                        with ui.column().classes("gap-0"):
                            ui.label("Application Logs").classes("text-lg font-semibold")
                            log_path = getattr(app.state, "log_file_path", None)
                            caption = f"→ {log_path}" if log_path else "in-memory only"
                            ui.label(caption).classes("text-xs text-slate-500 font-mono")
                        with ui.row().classes("items-center gap-2 flex-wrap"):
                            filter_input = (
                                ui.input(placeholder="Filter…")
                                .props("dense outlined clearable")
                                .classes("w-48")
                            )
                            level_select = (
                                ui.select(
                                    ["all", "debug", "info", "warning", "error"],
                                    value="all",
                                    label="Level",
                                )
                                .props("dense outlined")
                                .classes("w-28")
                            )
                    app_log = (
                        ui.log(max_lines=2000)
                        .classes("w-full font-mono text-xs bg-slate-900/90 text-white rounded-xl")
                        .style("min-height: 42rem; max-height: 42rem; overflow-y: auto;")
                    )
                # ── WebSocket Events ──────────────────────────────────────────────
                with ui.card().classes(
                    "w-full p-4 gap-2 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    ui.label("WebSocket Events").classes("text-lg font-semibold")
                    ui.label("Snapshot broadcasts to connected clients").classes(
                        "text-xs text-slate-500"
                    )
                    websocket_log = (
                        ui.log(max_lines=200)
                        .classes("w-full font-mono text-xs bg-slate-900/90 text-white rounded-xl")
                        .style("min-height: 8rem; max-height: 8rem;")
                    )

        def _passes_filter(line: str) -> bool:
            f = (filter_input.value or "").strip().lower()
            lv = (level_select.value or "all").lower()
            if f and f not in line.lower():
                return False
            if lv != "all":
                if f"· {lv.upper()}:" not in line:
                    return False
            return True

        def _render_event(entry: Any) -> str:
            now_label = format_now("%H:%M:%S %Z")
            if isinstance(entry, dict):
                message = entry.get("message") or entry.get("detail")
                if not message:
                    message = json.dumps(entry, ensure_ascii=False)
                ts_raw = entry.get("timestamp") or entry.get("ts")
                label = format_iso_timestamp(ts_raw, fmt="%H:%M:%S %Z") if ts_raw else now_label
                symbol = entry.get("symbol")
                if symbol:
                    message = f"{symbol}: {message}"
                return f"{label} · {message}"
            return f"{now_label} · {entry}"

        # Preload from the always-current in-memory log buffer
        all_lines = list(getattr(app.state, "log_lines", []))
        for line in all_lines:
            if _passes_filter(line):
                app_log.push(line)
        log_seen = {"count": len(all_lines)}

        # Preload WebSocket events
        ws_events = list(getattr(app.state, "websocket_events", []))
        for entry in ws_events:
            websocket_log.push(_render_event(entry))
        ws_seen = {"idx": len(ws_events)}

        def refresh_logs() -> None:
            lines = list(getattr(app.state, "log_lines", []))
            for line in lines[log_seen["count"]:]:
                if _passes_filter(line):
                    app_log.push(line)
            log_seen["count"] = len(lines)

        def refresh_websocket() -> None:
            events = list(getattr(app.state, "websocket_events", []))
            for entry in events[ws_seen["idx"]:]:
                websocket_log.push(_render_event(entry))
            ws_seen["idx"] = len(events)

        _debug_client = ui.context.client
        _t_logs = ui.timer(3, refresh_logs)
        _t_ws_dbg = ui.timer(3, refresh_websocket)
        _debug_client.on_disconnect(_t_logs.deactivate)
        _debug_client.on_delete(_t_logs.deactivate)
        _debug_client.on_disconnect(_t_ws_dbg.deactivate)
        _debug_client.on_delete(_t_ws_dbg.deactivate)

    def render_prompt_page() -> None:
        navigation("PROMPT")
        wrapper = page_container()
        config = getattr(app.state, "runtime_config", {}) or {}
        config.setdefault("llm_system_prompt", DEFAULT_SYSTEM_PROMPT)
        config.setdefault("prompt_version_name", None)
        response_schemas = config.setdefault("llm_response_schemas", {})
        _san_sys = sanitize_prompt_text(config.get("llm_system_prompt"))
        if _san_sys is not None:
            config["llm_system_prompt"] = _san_sys
        _guardrails_cfg = config.get("guardrails") or {}
        _require_rr_init = bool(_guardrails_cfg.get("require_reward_risk_ratio", True))
        _pre_leverage_init = (
            (_guardrails_cfg.get("llm_notional_mode") or "post_leverage").lower() == "pre_leverage"
        )
        sections_config: dict[str, dict] = config.get("prompt_sections") or default_prompt_sections(
            require_rr=_require_rr_init, pre_leverage=_pre_leverage_init
        )
        prompt_versions_cache: dict[str, dict[str, Any]] = {}
        prompt_version_options: dict[str, str] = {}
        section_widgets: dict[str, dict] = {}
        client = ui.context.client
        model_metadata: dict[str, dict[str, Any]] = {item["id"]: item for item in DEFAULT_MODEL_OPTIONS}
        current_model_id = config.get("llm_model_id") or next(iter(model_metadata), None)

        def schema_to_text(model_id: str | None) -> str:
            if not model_id:
                return ""
            schema = response_schemas.get(model_id)
            if not schema:
                return ""
            try:
                return json.dumps(schema, indent=2)
            except (TypeError, ValueError):
                return ""


        def _get_sec_default(sec: dict) -> str:
            if sec.get("alt_default") and _pre_leverage_init:
                return sec["alt_default"]
            return sec["default"]

        with wrapper:
            ui.label("Prompt Configuration").classes("text-2xl font-bold")
            ui.label(
                "Controls the exact text sent to the LLM on every call. "
                "Guardrail settings live on the CFG page — save those first, then refresh the preview here."
            ).classes("text-sm text-slate-500 mb-2")
            with ui.row().classes("w-full justify-between items-center"):
                ui.label("System Prompt").classes("text-sm font-medium text-slate-700")
                ui.button(
                    "Reset to default",
                    icon="restart_alt",
                    color="grey-7",
                ).props("flat dense size=sm").on(
                    "click",
                    lambda: [
                        setattr(prompt_input, "value", DEFAULT_SYSTEM_PROMPT) or prompt_input.update(),
                        ui.notify("System prompt reset — click Save to apply", type="info"),
                    ],
                )
            prompt_input = ui.textarea(
                label="System Prompt",
                value=config.get("llm_system_prompt", DEFAULT_SYSTEM_PROMPT),
            ).classes("w-full h-48")
            ui.separator().classes("my-2")
            with ui.row().classes("w-full justify-between items-center mb-1"):
                ui.label("Decision Prompt — Sections").classes("text-sm font-medium text-slate-700")
                ui.label("Toggle sections on/off; optionally edit text per section.").classes(
                    "text-xs text-slate-500"
                )
            for _sec in PROMPT_SECTIONS:
                _key = _sec["key"]
                _sec_state = sections_config.get(_key, {})
                _enabled = bool(_sec_state.get("enabled", True))
                _override = (_sec_state.get("override") or "").strip()
                _default_text = _get_sec_default(_sec)
                _current_text = _override if _override else _default_text
                with ui.card().classes("w-full rounded-lg border border-slate-200 mb-1"):
                    with ui.row().classes("w-full items-center gap-2 flex-nowrap"):
                        _sw = ui.switch(value=_enabled).props("dense color=primary")
                        with ui.expansion(_sec["label"]).classes("flex-1 text-sm font-medium"):
                            _ta = (
                                ui.textarea(value=_current_text)
                                .props('outlined autogrow input-style="min-height: 14rem; resize: vertical;"')
                                .classes("w-full font-mono text-xs mt-1")
                            )
                        _mod_badge = ui.badge("Modified", color="orange-9")
                        _mod_badge.set_visibility(bool(_override))
                        _rst = (
                            ui.button(icon="restart_alt")
                            .props("flat dense size=sm")
                            .tooltip("Reset to default text")
                        )
                section_widgets[_key] = {
                    "switch": _sw,
                    "textarea": _ta,
                    "modified_badge": _mod_badge,
                    "reset_btn": _rst,
                    "default_text": _default_text,
                }
            with ui.row().classes("w-full flex-wrap gap-4 items-end"):
                prompt_version_select = ui.select(
                    options=[],
                    label="Prompt Version",
                    with_input=False,
                ).classes("w-full md:w-64")
                prompt_version_select.disable()
                delete_version_button = ui.button(
                    icon="delete",
                    color="negative",
                ).props("flat dense").tooltip("Delete selected prompt version")
                with ui.dialog() as _delete_version_dialog, ui.card().classes("p-6 gap-4"):
                    _delete_confirm_label = ui.label("").classes("text-base font-semibold")
                    ui.label(
                        "This cannot be undone. Prompt runs that reference this version will keep their data."
                    ).classes("text-sm text-slate-500")
                    with ui.row().classes("gap-2 justify-end w-full"):
                        ui.button("Cancel", color="grey").on(
                            "click", lambda: _delete_version_dialog.submit(False)
                        )
                        ui.button("Delete", color="negative", icon="delete").on(
                            "click", lambda: _delete_version_dialog.submit(True)
                        )
                prompt_version_name_input = ui.input(
                    label="Save As New Version",
                    placeholder="e.g., Momentum bias v2",
                ).classes("w-full flex-1")
            with ui.row().classes("w-full flex-wrap gap-2 items-end"):
                prompt_version_param_input = (
                    ui.input(
                        label="Override query param",
                        value="prompt_version_id=<active>",
                    )
                    .props("readonly outlined dense")
                    .classes("w-full md:flex-1 font-mono text-sm")
                )
                copy_param_button = ui.button("Copy param", icon="content_copy")
            ui.label("Saving with a name will create a new immutable version for A/B tests").classes(
                "text-xs text-slate-500"
            )
            response_schema_input = ui.textarea(
                label="Response Schema Override (JSON)",
                value=schema_to_text(current_model_id),
                placeholder="Leave blank to use default schema",
            ).classes("w-full h-40 font-mono text-sm")
            with ui.column().classes(
                "w-full gap-2 mt-4 bg-slate-50/80 p-4 rounded-xl border border-slate-200"
            ):
                ui.label("Decision Prompt Preview").classes("text-lg font-semibold")
                ui.label(
                    "Read-only assembled decision prompt as it will be sent to the LLM"
                ).classes("text-xs text-slate-500")
                payload_preview = (
                    ui.textarea(label="Decision Prompt", value="")
                    .props("readonly outlined autogrow")
                    .classes("w-full font-mono text-xs bg-white h-full")
                    .style("min-height: 16rem; height: 100%;")
                )
            save_button = ui.button("Save", icon="save", color="primary")

        def _assemble_sections_preview() -> str:
            parts: list[str] = []
            for _sec in PROMPT_SECTIONS:
                _key = _sec["key"]
                _w = section_widgets.get(_key)
                if not _w or not bool(_w["switch"].value):
                    continue
                text = (_w["textarea"].value or "").strip()
                if text:
                    parts.append(text + " ")
            return "".join(parts)

        def _assemble_and_update_preview() -> None:
            payload_preview.value = _assemble_sections_preview()
            payload_preview.update()

        def _make_section_callbacks(key: str) -> tuple:
            def _on_text_change(e: Any) -> None:
                _w = section_widgets[key]
                _is_mod = (e.value or "").strip() != _w["default_text"].strip()
                _w["modified_badge"].set_visibility(_is_mod)
                _w["modified_badge"].update()
                _assemble_and_update_preview()

            def _on_reset(_: Any) -> None:
                _w = section_widgets[key]
                _w["textarea"].value = _w["default_text"]
                _w["textarea"].update()
                _w["modified_badge"].set_visibility(False)
                _w["modified_badge"].update()
                _assemble_and_update_preview()

            return _on_text_change, _on_reset

        for _key, _w in section_widgets.items():
            _on_change, _on_reset = _make_section_callbacks(_key)
            _w["textarea"].on_value_change(_on_change)
            _w["switch"].on_value_change(lambda _: _assemble_and_update_preview())
            _w["reset_btn"].on("click", _on_reset)

        for _w2 in [prompt_input, response_schema_input]:
            _w2.on_value_change(lambda _: _assemble_and_update_preview())

        _assemble_and_update_preview()

        def update_prompt_version_param(version_id: str | None) -> None:
            suffix = version_id or "<active>"
            prompt_version_param_input.value = f"prompt_version_id={suffix}"
            prompt_version_param_input.update()

        def copy_prompt_version_param() -> None:
            value = prompt_version_param_input.value or "prompt_version_id=<active>"
            ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(value)})")
            ui.notify("Query param copied", color="positive")

        copy_param_button.on("click", lambda _: copy_prompt_version_param())
        update_prompt_version_param(config.get("prompt_version_id"))

        def _set_prompt_version_value(version_id: str | None) -> None:
            if not version_id:
                prompt_version_select.value = None
                prompt_version_select.update()
                update_prompt_version_param(None)
                return
            for label, vid in prompt_version_options.items():
                if vid == version_id:
                    prompt_version_select.value = label
                    prompt_version_select.update()
                    update_prompt_version_param(version_id)
                    return
            prompt_version_select.value = None
            prompt_version_select.update()
            update_prompt_version_param(version_id)

        async def load_prompt_versions_list() -> None:
            try:
                records = await fetch_prompt_versions(limit=50)
            except Exception as exc:  # pragma: no cover - optional DB
                ui.notify(f"Failed to load prompt versions: {exc}", color="warning")
                return
            prompt_versions_cache.clear()
            prompt_version_options.clear()
            options: list[str] = []
            for row in records:
                prompt_versions_cache[row["id"]] = row
                created = row.get("created_at") or "recent"
                label = f"{row['name']} ({created[:16]})"
                prompt_version_options[label] = row["id"]
                options.append(label)
            prompt_version_select.set_options(options, value=None)
            if options:
                prompt_version_select.enable()
            else:
                prompt_version_select.disable()
            _set_prompt_version_value(config.get("prompt_version_id"))

        def apply_prompt_version(version_id: str | None) -> None:
            record = prompt_versions_cache.get(version_id or "")
            if not record:
                return
            new_system_prompt = sanitize_prompt_text(record.get("system_prompt", prompt_input.value))
            if new_system_prompt is not None:
                prompt_input.value = new_system_prompt
                prompt_input.update()
            meta = record.get("metadata") or {}
            version_sections = meta.get("prompt_sections")
            if version_sections and isinstance(version_sections, dict):
                for _key, _w in section_widgets.items():
                    sec_state = version_sections.get(_key, {})
                    _w["switch"].value = bool(sec_state.get("enabled", True))
                    _w["switch"].update()
                    _override = (sec_state.get("override") or "").strip()
                    _w["textarea"].value = _override if _override else _w["default_text"]
                    _w["textarea"].update()
                    _w["modified_badge"].set_visibility(bool(_override))
                    _w["modified_badge"].update()
            else:
                ui.notify(
                    "Legacy version — only system prompt applied; sections unchanged.", type="info"
                )
            prompt_version_name_input.value = ""
            prompt_version_name_input.update()
            config["llm_system_prompt"] = sanitize_prompt_text(prompt_input.value or "") or ""
            config["prompt_version_id"] = record.get("id")
            config["prompt_version_name"] = record.get("name")
            update_prompt_version_param(record.get("id"))
            _assemble_and_update_preview()

        def on_prompt_version_change(e: Any) -> None:
            label = e.value
            version_id = prompt_version_options.get(label)
            if not version_id:
                return
            apply_prompt_version(version_id)

        prompt_version_select.on_value_change(on_prompt_version_change)

        async def confirm_delete_prompt_version() -> None:
            selected_label = prompt_version_select.value
            version_id = prompt_version_options.get(selected_label) if selected_label else None
            if not version_id:
                ui.notify("Select a version to delete", color="warning")
                return
            record = prompt_versions_cache.get(version_id)
            version_name = record.get("name", selected_label) if record else selected_label
            _delete_confirm_label.set_text(f'Delete prompt version "{version_name}"?')
            confirmed = await _delete_version_dialog
            if not confirmed:
                return
            try:
                deleted = await delete_prompt_version(version_id)
            except Exception as exc:
                ui.notify(f"Delete failed: {exc}", color="negative")
                return
            if not deleted:
                ui.notify("Version not found — already deleted?", color="warning")
            else:
                ui.notify(f'Deleted "{version_name}"', color="positive")
            if config.get("prompt_version_id") == version_id:
                config["prompt_version_id"] = None
                config["prompt_version_name"] = None
                update_prompt_version_param(None)
            await load_prompt_versions_list()

        delete_version_button.on("click", confirm_delete_prompt_version)

        async def save_prompt_settings(event: Any | None = None) -> None:
            config["llm_system_prompt"] = sanitize_prompt_text(prompt_input.value or "") or ""
            saved_sections: dict[str, dict] = {}
            for _key, _w in section_widgets.items():
                _default = _w["default_text"]
                _current = (_w["textarea"].value or "").strip()
                _override: str | None = _current if _current != _default.strip() else None
                saved_sections[_key] = {"enabled": bool(_w["switch"].value), "override": _override}
            config["prompt_sections"] = saved_sections
            config["llm_decision_prompt"] = ""  # clear legacy custom prompt
            current_model = config.get("llm_model_id")
            schema_text = response_schema_input.value or ""
            if schema_text.strip():
                try:
                    response_schemas[current_model] = json.loads(schema_text)
                except json.JSONDecodeError as exc:
                    ui.notify(f"Response schema invalid JSON: {exc}", color="warning")
                    return
            else:
                response_schemas.pop(current_model, None)
            config["llm_response_schemas"] = response_schemas
            version_name = (prompt_version_name_input.value or "").strip()
            created_version_id: str | None = None
            selected_label = prompt_version_select.value
            selected_version_id = (
                prompt_version_options.get(selected_label)
                if selected_label
                else config.get("prompt_version_id")
            )
            if version_name:
                assembled_decision = _assemble_sections_preview()
                metadata = {
                    "guardrails": config.get("guardrails"),
                    "model_id": config.get("llm_model_id"),
                    "prompt_sections": saved_sections,
                }
                try:
                    created_version_id = await insert_prompt_version(
                        name=version_name,
                        system_prompt=config["llm_system_prompt"],
                        decision_prompt=assembled_decision,
                        metadata=metadata,
                    )
                    prompt_version_name_input.value = ""
                    prompt_version_name_input.update()
                    await load_prompt_versions_list()
                except Exception as exc:  # pragma: no cover - db optional
                    ui.notify(f"Failed to save prompt version: {exc}", color="warning")
            config["prompt_version_id"] = created_version_id or selected_version_id
            if created_version_id:
                config["prompt_version_name"] = version_name or config.get("prompt_version_name")
            elif selected_version_id:
                selected_record = prompt_versions_cache.get(selected_version_id)
                if selected_record:
                    config["prompt_version_name"] = selected_record.get("name")
            app.state.runtime_config = config
            _set_prompt_version_value(config.get("prompt_version_id"))
            ui.notify("Prompt saved", color="positive")
            app.state.frontend_events.append("PROMPT updated")

        save_button.on("click", save_prompt_settings)
        asyncio.create_task(load_prompt_versions_list())

    def render_cfg_page() -> None:
        navigation("CFG")
        wrapper = page_container()
        config = getattr(app.state, "runtime_config", {})
        config.setdefault("snapshot_max_age_seconds", settings.snapshot_max_age_seconds)
        config.setdefault("execution_enabled", False)
        config.setdefault("execution_trade_mode", "cross")
        config.setdefault("execution_order_type", "market")
        config.setdefault("execution_min_size", 1.0)
        config.setdefault("execution_min_sizes", {})
        config.setdefault("fee_window_hours", 24.0)
        config.setdefault("okx_sub_account", settings.okx_sub_account)
        config.setdefault("okx_sub_account_use_master", settings.okx_sub_account_use_master)
        config.setdefault("okx_api_flag", str(settings.okx_api_flag or "0") or "0")
        config.setdefault("enable_websocket", True)
        config.setdefault("frontend_timezone", DEFAULT_FRONTEND_TIMEZONE)
        config.setdefault("fallback_orders_enabled", settings.allow_fallback_orders)
        guardrails = config.setdefault("guardrails", PromptBuilder._default_guardrails())
        guardrails.setdefault(
            "snapshot_max_age_seconds", config.get("snapshot_max_age_seconds")
        )
        guardrails.setdefault("symbol_position_caps", {})
        guardrails.setdefault("min_leverage_confidence_gate", 0.5)
        guardrails.setdefault("llm_notional_mode", "post_leverage")
        guardrails.setdefault("isolated_margin_seed_usd", None)
        guardrails.setdefault("isolated_margin_max_transfer_usd", None)
        guardrails.setdefault("isolated_margin_symbol_seeds_usd", {})
        guardrails.setdefault("isolated_wallet_bootstrap_pct", None)
        guardrails.setdefault("atr_risk_per_trade_pct", None)
        guardrails.setdefault("min_trade_notional_usd", None)
        guardrails.setdefault("cvd_guard", {"enabled": False, "lookback": 10, "min_slope_pct": 0.0})
        guardrails.setdefault("ob_wall_guard", {"enabled": False, "proximity_pct": 1.0, "wall_ratio": 3.0})
        guardrails.setdefault("require_reward_risk_ratio", True)
        guardrails.setdefault("require_protection", True)
        guardrails.setdefault("footprint", {
            "poc_risk_delta": 0.05,
            "net_delta_confidence_delta": 0.02,
            "imbalance_zone_confidence_delta": 0.03,
            "imbalance_zone_proximity_pct": 0.3,
            "bucket_pct": 0.1,
        })
        if "wait_for_tp_sl" not in config:
            config["wait_for_tp_sl"] = bool(guardrails.get("wait_for_tp_sl", False))
        guardrails.setdefault("wait_for_tp_sl", bool(config.get("wait_for_tp_sl")))
        guardrails.setdefault(
            "fallback_orders_enabled",
            bool(config.get("fallback_orders_enabled", settings.allow_fallback_orders)),
        )
        config.setdefault("llm_timeout_seconds", 300)
        config.setdefault("llm_reasoning_effort", "low")
        config["fallback_orders_enabled"] = bool(guardrails.get("fallback_orders_enabled", True))
        client = ui.context.client
        price_cache: dict[str, tuple[float, float]] = {}

        def _safe_float(value: Any) -> float | None:
            try:
                if value in (None, ""):
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        def _safe_int(value: Any) -> int | None:
            try:
                if value in (None, ""):
                    return None
                return int(value)
            except (TypeError, ValueError):
                return None

        def _percent_to_fraction(value: Any) -> float | None:
            numeric = _safe_float(value)
            if numeric is None:
                return None
            return numeric / 100.0

        def _fraction_to_percent(value: Any) -> float | None:
            numeric = _safe_float(value)
            if numeric is None:
                return None
            return numeric * 100.0

        def _normalize_fraction(value: Any) -> float | None:
            numeric = _safe_float(value)
            if numeric is None:
                return None
            if numeric < 0:
                return 0.0
            if numeric > 1.0:
                numeric = numeric / 100.0
            return min(numeric, 1.0)

        normalized_max_pct = _normalize_fraction(guardrails.get("max_position_pct"))
        guardrails["max_position_pct"] = 0.2 if normalized_max_pct is None else normalized_max_pct
        normalized_daily_limit = _normalize_fraction(guardrails.get("daily_loss_limit_pct"))
        guardrails["daily_loss_limit_pct"] = (
            0.03 if normalized_daily_limit is None else normalized_daily_limit
        )
        normalized_symbol_caps: dict[str, float] = {}
        for symbol, value in (guardrails.get("symbol_position_caps") or {}).items():
            numeric = _normalize_fraction(value)
            if numeric is None or numeric <= 0:
                continue
            normalized_symbol_caps[str(symbol).upper()] = numeric
        guardrails["symbol_position_caps"] = normalized_symbol_caps
        bootstrap_fraction = guardrails.get("isolated_wallet_bootstrap_pct")
        if bootstrap_fraction is None:
            ms_instance = getattr(app.state, "market_service", None)
            bootstrap_fraction = getattr(ms_instance, "ISOLATED_WALLET_BOOTSTRAP_PCT", 0.25)
        bootstrap_pct_value = _fraction_to_percent(bootstrap_fraction)
        if bootstrap_pct_value is None:
            bootstrap_pct_value = 25.0

        async def lookup_symbol_price(symbol: str | None) -> float | None:
            normalized = (symbol or "").strip().upper()
            if not normalized:
                return None
            now = time.time()
            cached = price_cache.get(normalized)
            if cached and now - cached[0] < 15:
                return cached[1]
            market_service = getattr(app.state, "market_service", None)
            getter = getattr(market_service, "get_last_price", None)
            if callable(getter):
                price = getter(normalized)
                if price:
                    price_cache[normalized] = (now, price)
                    return price
            state_service = getattr(app.state, "state_service", None)
            if state_service:
                try:
                    snapshot = await state_service.get_market_snapshot()
                except Exception:  # pragma: no cover - defensive snapshot access
                    snapshot = None
                if snapshot:
                    market_entry = (snapshot.get("market_data") or {}).get(normalized) or {}
                    ticker = market_entry.get("ticker")
                    price = _ticker_price(ticker)
                    if not price:
                        primary_symbol = str(snapshot.get("symbol") or "").upper()
                        if primary_symbol == normalized:
                            price = _ticker_price(snapshot.get("ticker"))
                    if price:
                        price_cache[normalized] = (time.time(), price)
                        return price
            return None

        model_metadata: dict[str, dict[str, Any]] = {
            item["id"]: item for item in DEFAULT_MODEL_OPTIONS
        }

        def _format_price(value: float | None) -> str | None:
            if value is None:
                return None
            if value >= 1:
                return f"{value:,.2f}"
            return f"{value:.4f}".rstrip("0").rstrip(".")

        def _pricing_suffix(pricing: dict[str, Any] | None) -> str:
            if not pricing:
                return ""
            prompt = _format_price(pricing.get("prompt"))
            completion = _format_price(pricing.get("completion"))
            if not prompt and not completion:
                return ""
            currency = (pricing.get("currency") or "USD").upper()
            unit = pricing.get("unit") or "per 1M tokens"
            symbol = "$" if currency == "USD" else f"{currency} "
            prompt_text = f"{symbol}{prompt}" if prompt else None
            completion_text = f"{symbol}{completion}" if completion else None
            if prompt_text and completion_text:
                pair = f"{prompt_text}/{completion_text}"
            else:
                pair = prompt_text or completion_text or ""
            return f" · {pair} {unit}" if pair else ""

        def _option_label(entry: dict[str, Any]) -> str:
            label = entry.get("label") or entry.get("id") or "Model"
            return f"{label}{_pricing_suffix(entry.get('pricing'))}".strip()

        model_options = {key: _option_label(meta) for key, meta in model_metadata.items()}
        initial_model_value = config.get("llm_model_id") or next(iter(model_options), None)
        if initial_model_value and initial_model_value not in model_options:
            model_options[initial_model_value] = initial_model_value
            model_metadata.setdefault(
                initial_model_value,
                {"id": initial_model_value, "label": initial_model_value, "pricing": None},
            )

        timeframe_default = config.get("ta_timeframe") or "4H"
        if timeframe_default not in TA_TIMEFRAME_OPTIONS:
            timeframe_default = "4H"
        with wrapper:
            with ui.row().classes("w-full justify-between items-center"):
                ui.label("Engine Configuration").classes("text-2xl font-bold")
                with ui.row().classes("gap-2 items-center"):
                    # Hidden file upload used by the Import button (accepts JSON only).
                    import_upload = ui.upload(
                        label="import",
                        auto_upload=True,
                        multiple=False,
                        on_upload=lambda e: None,  # replaced below with real handler
                    ).props("accept=.json hidden").classes("hidden")
                    ui.button("Export", icon="download", color="secondary").on(
                        "click", lambda _e: export_config()
                    )
                    ui.button("Import", icon="upload", color="secondary").on(
                        "click", lambda _e: _trigger_upload()
                    )
                    save_button = ui.button("Save", icon="save", color="primary")
            ui.label("Notifications").classes("text-xl font-semibold")
            ui.label("Send Telegram alerts when trades are opened or closed (requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env)").classes("text-sm text-slate-500")
            _notifications_cfg = config.get("notifications") or {}
            with ui.row().classes("gap-8 items-center mt-1 mb-2"):
                notify_trade_open_switch = ui.switch(
                    "Send trade open notification",
                    value=bool(_notifications_cfg.get("trade_open", False)),
                )
                notify_trade_close_switch = ui.switch(
                    "Send trade close notification",
                    value=bool(_notifications_cfg.get("trade_close", False)),
                )
                async def _send_test_notification() -> None:
                    from app.services.alert_service import send_alert
                    try:
                        await send_alert(
                            "\U0001f9ea <b>Test Notification</b>\n"
                            "Test message from tai2."
                        )
                        ui.notify("Test notification sent", color="positive")
                    except Exception as exc:
                        ui.notify(f"Failed to send test notification: {exc}", color="negative")
                ui.button("Send test notification", icon="notifications", color="secondary").on(
                    "click", _send_test_notification
                )
            ui.separator().classes("mb-2")
            ui.label("Execution Guardrails").classes("text-xl font-semibold")
            ui.label("Limits enforced before orders are placed").classes("text-sm text-slate-500")
            max_position_pct_value = _fraction_to_percent(guardrails.get("max_position_pct"))
            if max_position_pct_value is None:
                max_position_pct_value = 20.0
            daily_loss_limit_value = _fraction_to_percent(guardrails.get("daily_loss_limit_pct"))
            if daily_loss_limit_value is None:
                daily_loss_limit_value = 3.0
            with ui.grid(columns=4).classes("w-full gap-4"):
                max_leverage_input = ui.number(
                    label="Max Leverage",
                    value=guardrails.get("max_leverage", 5),
                    min=1,
                ).classes("w-full").props(
                    "hint='Hard cap on leverage multiples for new positions' persistent-hint"
                )
                min_leverage_input = ui.number(
                    label="Min Leverage",
                    value=guardrails.get("min_leverage", 1),
                    min=0,
                    step=0.1,
                ).classes("w-full").props(
                    "hint='Confidence-scaling floor applied before execution' persistent-hint"
                )
                min_leverage_conf_gate_input = ui.number(
                    label="Conviction Floor",
                    value=guardrails.get("min_leverage_confidence_gate", 0.5),
                    min=0,
                    max=1,
                    step=0.05,
                ).classes("w-full").props(
                    "hint='Confidence threshold below which BUY/SELL is hard-blocked and converted to HOLD (all execution paths)' persistent-hint"
                )
                llm_notional_mode_select = ui.select(
                    label="LLM Notional Mode",
                    options={
                        "post_leverage": "Post-leverage (LLM sets position size)",
                        "pre_leverage": "Pre-leverage (LLM sets margin to commit)",
                    },
                    value=guardrails.get("llm_notional_mode", "post_leverage"),
                ).classes("w-full").props(
                    "hint='Post-leverage: notional_usd is the full position value. Pre-leverage: notional_usd is the margin committed; bot applies leverage.' persistent-hint"
                )
                max_position_pct_input = ui.number(
                    label="Max Position % of Equity",
                    value=max_position_pct_value,
                    step=0.1,
                    min=0.1,
                    max=100,
                ).classes("w-full").props(
                    "hint='Percent of equity allowed per symbol (e.g., 15 = 15%)' persistent-hint"
                )
                daily_loss_limit_input = ui.number(
                    label="Daily Loss Limit %",
                    value=daily_loss_limit_value,
                    step=0.1,
                    min=0.1,
                    max=100,
                ).classes("w-full").props(
                    "hint='Soft kill switch when daily drawdown breaches this percent (enter 3 for 3%)' persistent-hint"
                )
                atr_risk_per_trade_input = ui.number(
                    label="ATR Risk Per Trade %",
                    value=guardrails.get("atr_risk_per_trade_pct"),
                    step=0.1,
                    min=0.0,
                    max=10,
                    placeholder="e.g. 1",
                ).classes("w-full").props(
                    "clearable hint='Cap position size so a full stop-out loses at most this % of equity (1% risk model). Leave blank to disable.' persistent-hint"
                )
                min_trade_notional_usd_input = ui.number(
                    label="Min Trade Notional (USD)",
                    value=guardrails.get("min_trade_notional_usd"),
                    step=1.0,
                    min=0.0,
                    placeholder="e.g. 10",
                ).classes("w-full").props(
                    "clearable hint='Scale up entry orders whose final notional (after all caps) is below this USD value to meet the minimum. Leave blank to disable.' persistent-hint"
                )
            ui.separator().classes("mt-4")
            with ui.grid(columns=4).classes("w-full gap-4 items-start"):
                _cvd_cfg_ui = guardrails.get("cvd_guard") or {}
                cvd_guard_enabled_toggle = ui.switch(
                    "CVD Guard Enabled",
                    value=bool(_cvd_cfg_ui.get("enabled", False)),
                ).props(
                    "hint='Block BUY/SELL when CVD momentum contradicts the trade direction' persistent-hint"
                )
                cvd_guard_lookback_input = ui.number(
                    label="CVD Lookback Bars",
                    value=_cvd_cfg_ui.get("lookback", 10),
                    step=1,
                    min=2,
                    max=200,
                ).classes("w-full").props(
                    "hint='Recent CVD series bars used to measure slope' persistent-hint"
                )
                cvd_guard_min_slope_input = ui.number(
                    label="CVD Min Slope %",
                    value=_cvd_cfg_ui.get("min_slope_pct", 0.0),
                    step=0.5,
                    min=0.0,
                    max=100,
                ).classes("w-full").props(
                    "hint='Minimum % change in CVD to count as directional (0 = any). Neutral CVD never blocks.' persistent-hint"
                )
                ui.space()
                _ob_cfg_ui = guardrails.get("ob_wall_guard") or {}
                ob_wall_enabled_toggle = ui.switch(
                    "OB Wall Guard Enabled",
                    value=bool(_ob_cfg_ui.get("enabled", False)),
                ).props(
                    "hint='Block BUY/SELL when a dominant opposing limit-order wall sits within proximity of the current price' persistent-hint"
                )
                ob_wall_proximity_input = ui.number(
                    label="Wall Proximity %",
                    value=_ob_cfg_ui.get("proximity_pct", 1.0),
                    step=0.1,
                    min=0.1,
                    max=10.0,
                ).classes("w-full").props(
                    "hint='Scan the opposing order-book side within this % of current price for walls' persistent-hint"
                )
                ob_wall_ratio_input = ui.number(
                    label="Wall Size Ratio",
                    value=_ob_cfg_ui.get("wall_ratio", 3.0),
                    step=0.5,
                    min=1.0,
                    max=20.0,
                ).classes("w-full").props(
                    "hint='A level is a wall when its size exceeds this multiple of the average level size (e.g. 3 = 3× average)' persistent-hint"
                )
                ui.space()
            ui.separator().classes("mt-4")
            with ui.grid(columns=4).classes("w-full gap-4"):
                min_hold_seconds_input = ui.number(
                    label="Min Hold / Cooldown (sec)",
                    value=guardrails.get("min_hold_seconds", 180),
                    min=0,
                ).classes("w-full").props(
                    "hint='Minimum time to wait before allowing another trade on the same symbol' persistent-hint"
                )
                max_trades_per_hour_input = ui.number(
                    label="Max Trades Per Hour",
                    value=guardrails.get("max_trades_per_hour", 2),
                    min=0,
                ).classes("w-full").props(
                    "hint='Prevents over-trading by capping per-symbol order count in any rolling hour' persistent-hint"
                )
                max_trades_to_open_input = ui.number(
                    label="Max Trades to Open",
                    value=guardrails.get("max_trades_to_open", 0),
                    min=0,
                ).classes("w-full").props(
                    "hint='Per scheduler cycle: open only the top N ranked BUY/SELL decisions (0 = unlimited)' persistent-hint"
                )
                trade_window_seconds_input = ui.number(
                    label="Trade Window (sec)",
                    value=guardrails.get("trade_window_seconds", 3600),
                    min=60,
                    step=60,
                ).classes("w-full").props(
                    "hint='Window used for trade limit and activity metrics' persistent-hint"
                )
                snapshot_max_age_input = ui.number(
                    label="Snapshot Max Age (sec)",
                    value=config.get(
                        "snapshot_max_age_seconds",
                        settings.snapshot_max_age_seconds,
                    ),
                    min=60,
                ).classes("w-full").props(
                    "hint='Blocks LLM prompts whenever Redis snapshot is older than this' persistent-hint"
                )
                execution_feedback_ttl_input = ui.number(
                    label="Execution Feedback TTL (sec)",
                    value=guardrails.get("execution_feedback_ttl_seconds", 600),
                    min=0,
                ).classes("w-full").props(
                    "hint='How long warnings/errors stay in prompts before auto-expiring; set 0 to disable' persistent-hint"
                )
                adjust_invalid_tp_switch = ui.switch(
                    "Adjust Invalid TP",
                    value=guardrails.get("adjust_invalid_tp", False),
                ).props(
                    "hint='When the LLM supplies a TP on the wrong side of entry, replace it with the configured % offset rather than dropping it' persistent-hint"
                )
                adjust_invalid_tp_pct_input = ui.number(
                    label="Adjust TP %",
                    value=guardrails.get("adjust_invalid_tp_pct", 0.10) * 100,
                    min=0.1, max=200.0, step=0.5, format="%.1f",
                ).classes("w-full").props(
                    "hint='Fallback TP target as OKX Floating PnL % (divided by leverage to get entry-price distance)' persistent-hint suffix='%'"
                )
            with ui.grid(columns=3).classes("w-full gap-x-8 gap-y-4 mt-2"):
                require_alignment_switch = ui.switch(
                    "Require Position Alignment",
                    value=guardrails.get("require_position_alignment", True),
                ).props(
                    "hint='Blocks conflicting orders unless an opposite signal closes the position' persistent-hint"
                )
                wait_for_tp_sl_switch = ui.switch(
                    "Wait for TP/SL to Hit",
                    value=guardrails.get("wait_for_tp_sl", False),
                ).props(
                    "hint='When enabled, opposing signals are ignored until the current position\'s TP or SL executes' persistent-hint"
                )
                fallback_orders_switch = ui.switch(
                    "Allow Fallback Orders",
                    value=config.get("fallback_orders_enabled", settings.allow_fallback_orders),
                ).props(
                    "hint='Permit heuristic backup trades when LLM calls fail; disable to ignore fallback orders entirely' persistent-hint"
                )
                require_rr_switch = ui.switch(
                    "Require Min Reward-to-Risk Ratio (≥ 1)",
                    value=guardrails.get("require_reward_risk_ratio", True),
                ).props(
                    "hint='When enabled, entries where take-profit distance is less than stop-loss distance are hard-blocked' persistent-hint"
                )
                require_protection_switch = ui.switch(
                    "Require TP/SL Protection on Entry",
                    value=guardrails.get("require_protection", True),
                ).props(
                    "hint='Block any new entry order that has no stop-loss; prevents unprotected positions' persistent-hint"
                )
                flip_llm_decision_switch = ui.switch(
                    "Flip LLM Decision",
                    value=guardrails.get("flip_llm_decision", False),
                ).props(
                    "hint='Invert the LLM\'s SIDE and swap TP/SL before opening the trade; useful for contrarian testing' persistent-hint"
                )
            ui.separator().classes("w-full my-3")
            ui.label("Isolated Margin Auto-Seed").classes("text-sm font-semibold text-slate-600")
            ui.label(
                "Transfers USDT from funding into trading before retrying isolated margin top-ups when code 59300 appears."
            ).classes("text-xs text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                isolated_seed_default_input = ui.number(
                    label="Default Funding Transfer (USDT)",
                    value=guardrails.get("isolated_margin_seed_usd"),
                    min=0,
                    step=1,
                ).classes("w-full md:w-56").props(
                    "hint='Maximum USDT auto-moved per symbol before retrial; leave blank to disable' persistent-hint"
                )
                isolated_seed_max_input = ui.number(
                    label="Global Transfer Cap (USDT)",
                    value=guardrails.get("isolated_margin_max_transfer_usd"),
                    min=0,
                    step=1,
                ).classes("w-full md:w-56").props(
                    "hint='Absolute ceiling for any auto-seed attempt; blank means no extra cap' persistent-hint"
                )
                isolated_bootstrap_pct_input = ui.number(
                    label="Wallet Bootstrap % of Equity",
                    value=bootstrap_pct_value,
                    min=0,
                    max=100,
                    step=0.5,
                ).classes("w-full md:w-56").props(
                    "hint='When isolated wallets are missing, cap exposure to this percent of fallback margin (enter 25 for 25%)' persistent-hint"
                )
            ui.separator().classes("w-full my-3")
            ui.label("Footprint Chart Modifiers").classes("text-sm font-semibold text-slate-600")
            ui.label(
                "Confidence and risk-score adjustments applied when the live 15-min tape footprint is available."
            ).classes("text-xs text-slate-500")
            _fp_cfg = guardrails.setdefault("footprint", {})
            with ui.row().classes("w-full flex-wrap gap-4"):
                fp_bucket_pct_input = ui.number(
                    label="Bucket Width %",
                    value=_fp_cfg.get("bucket_pct", 0.1),
                    min=0.01, max=5.0, step=0.01, format="%.2f",
                ).classes("w-full md:w-48").props(
                    "hint='Price bucket width as a % of current price (0 = use tick_size×100 fallback)' persistent-hint suffix='%'"
                )
                fp_poc_risk_input = ui.number(
                    label="POC Risk Δ",
                    value=_fp_cfg.get("poc_risk_delta", 0.05),
                    min=0.0, max=0.5, step=0.01, format="%.2f",
                ).classes("w-full md:w-48").props(
                    "hint='Added to risk_score when the Point of Control opposes trade direction' persistent-hint"
                )
                fp_net_delta_conf_input = ui.number(
                    label="Net-Delta Conf Δ",
                    value=_fp_cfg.get("net_delta_confidence_delta", 0.02),
                    min=0.0, max=0.2, step=0.01, format="%.2f",
                ).classes("w-full md:w-48").props(
                    "hint='Confidence added/subtracted when net_delta agrees/disagrees with direction' persistent-hint"
                )
                fp_imbalance_conf_input = ui.number(
                    label="Imbalance Zone Conf Δ",
                    value=_fp_cfg.get("imbalance_zone_confidence_delta", 0.03),
                    min=0.0, max=0.2, step=0.01, format="%.2f",
                ).classes("w-full md:w-48").props(
                    "hint='Confidence boost when a matching imbalance zone is within proximity of entry' persistent-hint"
                )
                fp_proximity_input = ui.number(
                    label="Zone Proximity %",
                    value=_fp_cfg.get("imbalance_zone_proximity_pct", 0.3),
                    min=0.0, max=5.0, step=0.05, format="%.2f",
                ).classes("w-full md:w-48").props(
                    "hint='How close (as % of price) an imbalance zone must be to entry to count' persistent-hint suffix='%'"
                )
            ui.separator().classes("w-full my-4")
            ui.label("Autonomous Symbol Screener").classes("text-sm font-semibold text-slate-600")
            ui.label(
                "When enabled, replaces the manual trading pairs list by scoring all USDT-SWAP "
                "instruments on OKX using three components: volume spike ratio vs. rolling average (50%), "
                "24h high-low oscillation range (30%), and absolute price momentum (20%). "
                "This favours coins with unusual activity and wide ranges rather than simply the largest by volume. "
                "The manual list is ignored while this is on."
            ).classes("text-xs text-slate-500 mb-1")
            screener_cfg = (config.get("screener") or {})
            with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                auto_select_symbols_switch = ui.switch(
                    "Auto-select symbols",
                    value=bool(screener_cfg.get("enabled", False)),
                ).classes("w-full md:w-56").props(
                    "hint='Let the engine pick symbols by market activity instead of the manual list' persistent-hint"
                )
            with ui.row().classes("w-full flex-wrap gap-4"):
                screener_universe_input = ui.input(
                    label="Universe filter",
                    value=str(screener_cfg.get("universe_filter") or "*-USDT-SWAP"),
                ).classes("w-full md:w-56").props(
                    "hint='Glob pattern for eligible instruments, e.g. *-USDT-SWAP' persistent-hint"
                )
                screener_max_symbols_input = ui.number(
                    label="Max active symbols",
                    value=int(screener_cfg.get("max_symbols") or 5),
                    min=1,
                    max=20,
                    step=1,
                ).classes("w-full md:w-48").props(
                    "hint='How many top-scoring symbols to trade at once' persistent-hint"
                )
                screener_interval_input = ui.number(
                    label="Selection interval (min)",
                    value=int(screener_cfg.get("interval_minutes") or 60),
                    min=5,
                    step=5,
                ).classes("w-full md:w-48").props(
                    "hint='How often to re-score the universe and update the active list' persistent-hint"
                )
                screener_min_volume_input = ui.number(
                    label="Min 24h volume (M USDT)",
                    value=float((screener_cfg.get("min_volume_usd") or 500_000) / 1_000_000),
                    min=0,
                    step=0.1,
                    format="%.2f",
                ).classes("w-full md:w-56").props(
                    "hint='Exclude symbols below this 24h quote-volume — enter in millions, e.g. 0.5 = 500,000 USDT (0 = no filter)' persistent-hint"
                )
                screener_min_momentum_input = ui.number(
                    label="Min momentum (%)",
                    value=float(screener_cfg.get("min_momentum_pct") or 0.5),
                    min=0,
                    step=0.1,
                ).classes("w-full md:w-48").props(
                    "hint='Exclude symbols whose absolute 24h price change is below this %' persistent-hint"
                )
                screener_min_hl_range_input = ui.number(
                    label="Min HL range (%)",
                    value=float(screener_cfg.get("min_hl_range_pct") or 0.0),
                    min=0,
                    step=0.1,
                ).classes("w-full md:w-48").props(
                    "hint='Exclude symbols whose 24h high-low range is below this % of open price (0 = no filter)' persistent-hint"
                )
            ui.separator().classes("w-full my-4")
            ui.label("Model, cadence, and prompt controls").classes("text-sm text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                ws_interval_input = ui.number(
                    label="Poll Interval (seconds)",
                    value=config.get("poll_interval", 180),
                    min=1,
                ).classes("w-full md:w-48")
                websocket_switch = ui.switch(
                    "Live Websocket Stream",
                    value=config.get("enable_websocket", True),
                ).classes("w-full md:w-48").props(
                    "hint='Disabling falls back to REST polling every interval' persistent-hint"
                )
                auto_prompt_switch = ui.switch(
                    "Scheduler",
                    value=config.get("auto_prompt_enabled", False),
                ).classes("w-full md:w-48").props(
                    "hint='Runs the active decision mode (LLM, Launcher, or both) on the configured trigger' persistent-hint"
                )
                fee_window_input = ui.number(
                    label="Fee Window (hours)",
                    value=config.get("fee_window_hours", 24.0),
                    min=1,
                    step=1,
                ).classes("w-full md:w-48").props(
                    "hint='Rolling hours of OKX fees shown on LIVE' persistent-hint"
                )
                auto_prompt_interval_input = ui.number(
                    label="Prompt Interval (seconds)",
                    value=config.get("auto_prompt_interval", 300),
                    min=30,
                ).classes("w-full md:w-48").props(
                    "hint='LLM / llm_with_filter modes only — auto-derived from Launcher Entry interval when in launcher_only mode' persistent-hint"
                )
                auto_prompt_trigger_select = ui.select(
                    options={
                        "scheduled": "Scheduled (fixed interval)",
                        "consecutive": "Consecutive (re-run when positions close)",
                    },
                    value=config.get("auto_prompt_trigger", "scheduled"),
                    label="Scheduler trigger",
                ).classes("w-full md:flex-1").props(
                    "hint='LLM / llm_with_filter modes only — auto-set from Launcher Entry schedule when in launcher_only mode (on_close→consecutive, timer→scheduled)' persistent-hint"
                )
                ta_timeframe_select_cfg = ui.select(
                    options=TA_TIMEFRAME_OPTIONS,
                    label="Analysis Timeframe",
                    value=timeframe_default,
                ).classes("w-full md:flex-1")
                timezone_select = (
                    ui.select(
                        options=TIMEZONE_OPTIONS,
                        label="Display Timezone",
                        value=config.get("frontend_timezone", DEFAULT_FRONTEND_TIMEZONE),
                        with_input=True,
                    )
                    .classes("w-full md:flex-1")
                    .props("use-input fill-input input-debounce='0' clearable")
                )
                ui.element("div").classes("w-full")  # force model row onto its own line
                with ui.column().classes("flex-1 gap-0 min-w-0"):
                    model_select = ui.select(
                        model_options,
                        label="Model",
                        value=initial_model_value,
                    ).classes("w-full")
                    model_cost_label = ui.label("Pricing unavailable").classes(
                        "text-xs text-slate-500 mt-1"
                    )
                llm_timeout_input = ui.number(
                    label="LLM Timeout (seconds)",
                    value=config.get("llm_timeout_seconds", 300),
                    min=10,
                    step=10,
                ).classes("w-full md:flex-1").props(
                    "hint='Max wait for a single OpenRouter request; reasoning models may need 180-300s' persistent-hint"
                )
                llm_reasoning_effort_select = ui.select(
                    options={"low": "Low (fast)", "medium": "Medium", "high": "High (thorough)"},
                    label="Reasoning Effort",
                    value=config.get("llm_reasoning_effort", "low"),
                ).classes("w-full md:flex-1").props(
                    "hint='Only applies to reasoning models (deepseek-r1, o1, etc.)' persistent-hint"
                )
            ui.separator().classes("w-full my-4")
            ui.label("Launcher").classes("text-sm text-slate-500")
            _gov_cfg = (config.get("launcher") or {})
            with ui.row().classes("w-full flex-wrap gap-4 items-start"):
                gov_mode_select = ui.select(
                    options={
                        "disabled": "Disabled",
                        "launcher_only": "Launcher only (rule-based trades)",
                        "llm_with_filter": "LLM + Launcher filter",
                    },
                    value=str(_gov_cfg.get("mode") or "disabled"),
                    label="Launcher mode",
                ).classes("w-full md:w-72").props(
                    "hint='disabled: Launcher inactive | launcher_only: rule-based entries on own schedule | llm_with_filter: LLM runs but Launcher vetos/amends each trade' persistent-hint"
                )
                gov_schedule_select = ui.select(
                    options={
                        "timer": "Timer (fixed interval)",
                        "on_close": "On close (all positions cleared)",
                    },
                    value=str(_gov_cfg.get("schedule") or "timer"),
                    label="Entry schedule",
                ).classes("w-full md:w-64").props(
                    "hint='launcher_only mode only — timer: check every N seconds; on_close: fire when all positions clear' persistent-hint"
                )
                _gov_interval_raw = _gov_cfg.get("entry_interval_seconds", 300.0)
                gov_interval_input = ui.number(
                    label="Entry interval (seconds)",
                    value=float(_gov_interval_raw) if _gov_interval_raw is not None else 300.0,
                    min=30,
                    step=60,
                    precision=0,
                ).classes("w-full md:w-48").props(
                    "hint='launcher_only + timer mode: seconds between entry evaluations' persistent-hint suffix='s'"
                )
                _gov_notional_raw = _gov_cfg.get("notional_usd")
                gov_notional_input = ui.number(
                    label="Notional per trade (USDT)",
                    value=float(_gov_notional_raw) if _gov_notional_raw is not None else None,
                    min=1.0,
                    step=10.0,
                    precision=2,
                ).classes("w-full md:w-48").props(
                    "hint='Fixed USDT size per Launcher entry (also used to amend LLM trades in filter mode)' persistent-hint clearable"
                )
                gov_trade_mode_select = ui.select(
                    options={"isolated": "Isolated", "cross": "Cross"},
                    value=str(_gov_cfg.get("trade_mode") or "isolated"),
                    label="Trade mode",
                ).classes("w-full md:w-40").props(
                    "hint='Margin mode for Launcher-opened positions' persistent-hint"
                )
            ui.label("Signal filters and TP/SL are configured on the STRATEGY page → Mean Reversion Scalping section.").classes("text-xs text-slate-400 mt-1")
            ui.separator().classes("w-full my-4")
            ui.label("Candle settings").classes("text-sm text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                ohlcv_fetch_limit_input = ui.number(
                    label="Candle Fetch Limit",
                    value=config.get("ohlcv_fetch_limit", 200),
                    min=50,
                    max=300,
                    step=10,
                ).classes("w-full md:flex-1 md:min-w-72").props(
                    "hint='Candles fetched from OKX per poll for both timeframes (50–300). Extra candles beyond what the LLM sees are used for indicator accuracy (e.g. EMA-200 needs 200 points).' persistent-hint"
                )
                ohlcv_snapshot_candles_input = ui.number(
                    label="Analysis TF Candles to LLM",
                    value=config.get("ohlcv_snapshot_candles", 96),
                    min=10,
                    max=300,
                    step=5,
                ).classes("w-full md:flex-1 md:min-w-72").props(
                    "hint='Candles at the Analysis Timeframe (e.g. 1H) included in the LLM prompt. Fewer = cheaper; more = better pattern context.' persistent-hint"
                )
                ohlcv_snapshot_htf_candles_input = ui.number(
                    label="Higher TF Candles to LLM",
                    value=config.get("ohlcv_snapshot_htf_candles", 48),
                    min=5,
                    max=300,
                    step=5,
                ).classes("w-full md:flex-1 md:min-w-72").props(
                    "hint='Candles at the auto-selected higher timeframe (e.g. 4H when Analysis TF is 1H) included in the LLM prompt for trend context.' persistent-hint"
                )
            raw_pairs = config.get("trading_pairs", ["BTC-USDT-SWAP"])
            selected_trading_pairs: list[str] = []
            for symbol in raw_pairs or []:
                normalized = str(symbol).strip().upper()
                if not normalized or normalized in selected_trading_pairs:
                    continue
                selected_trading_pairs.append(normalized)
            config["trading_pairs"] = selected_trading_pairs.copy()
            trading_pair_checkboxes: dict[str, ui.checkbox] = {}
            with ui.column().classes("w-full flex-1 gap-2"):
                ui.label("Enabled Trading Pairs").classes("text-xs text-slate-500")
                trading_pairs_select = (
                    ui.select(
                        options=[],
                        label="Add trading pair",
                        with_input=True,
                    )
                    .classes("w-full")
                    .props("use-input fill-input input-debounce='0' clearable")
                )
                trading_pairs_select.disable()
                trading_pairs_list = ui.column().classes(
                    "w-full gap-2 rounded-xl border border-slate-200 bg-slate-50/70 p-3"
                )

            def _normalize_trading_pair(symbol: Any) -> str | None:
                if symbol is None:
                    return None
                normalized = str(symbol).strip().upper()
                return normalized or None

            def render_trading_pair_rows() -> None:
                trading_pairs_list.clear()
                trading_pair_checkboxes.clear()
                config["trading_pairs"] = selected_trading_pairs.copy()
                with trading_pairs_list:
                    if not selected_trading_pairs:
                        ui.label("No trading pairs configured. Add one using the dropdown above.").classes(
                            "text-xs text-slate-400 italic"
                        )
                        return
                    grid_container = ui.element("div").classes(
                        "grid gap-2 w-full grid-cols-1 sm:grid-cols-2 xl:grid-cols-3"
                    )
                    for symbol in selected_trading_pairs:
                        with grid_container:
                            with ui.row().classes(
                                "w-full items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-1"
                            ):
                                checkbox = ui.checkbox(symbol, value=True).classes(
                                    "flex-1 font-mono text-sm"
                                )
                                trading_pair_checkboxes[symbol] = checkbox

                                def _handler_factory(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(event: Any) -> None:
                                        if event.value:
                                            return
                                        if len(selected_trading_pairs) <= 1:
                                            checkbox = trading_pair_checkboxes.get(sym_key)
                                            if checkbox:
                                                checkbox.value = True
                                                checkbox.update()
                                            ui.notify("At least one trading pair required", color="warning")
                                            return
                                        selected_trading_pairs[:] = [
                                            sym for sym in selected_trading_pairs if sym != sym_key
                                        ]
                                        min_size_overrides.pop(sym_key, None)
                                        symbol_cap_overrides.pop(sym_key, None)
                                        isolated_seed_overrides.pop(sym_key, None)
                                        config["trading_pairs"] = selected_trading_pairs.copy()
                                        render_trading_pair_rows()
                                        render_min_size_rows()
                                        render_symbol_cap_rows()
                                        render_isolated_seed_rows()

                                    return _handler

                                checkbox.on_value_change(_handler_factory(symbol))

            def add_trading_pair(symbol: Any) -> None:
                normalized = _normalize_trading_pair(symbol)
                if not normalized:
                    return
                if normalized in selected_trading_pairs:
                    ui.notify(f"{normalized} already enabled", color="info")
                    return
                selected_trading_pairs.append(normalized)
                config["trading_pairs"] = selected_trading_pairs.copy()
                render_trading_pair_rows()
                render_min_size_rows()
                render_symbol_cap_rows()
                render_isolated_seed_rows()

            def on_trading_pair_select(event: Any) -> None:
                add_trading_pair(getattr(event, "value", None))
                trading_pairs_select.value = None
                trading_pairs_select.update()

            trading_pairs_select.on_value_change(on_trading_pair_select)
            render_trading_pair_rows()

            def _sync_screener_pairs() -> None:
                """Refresh the CFG trading-pairs list when the screener fires.
                The flag _screener_pairs_changed is set by the scheduler after a
                screener run so this callback does nothing in the common case."""
                rt: dict[str, Any] = getattr(app.state, "runtime_config", {}) or {}
                if not rt.get("_screener_pairs_changed"):
                    return
                rt["_screener_pairs_changed"] = False
                latest = [
                    str(s).strip().upper()
                    for s in (rt.get("trading_pairs") or [])
                    if str(s).strip()
                ]
                if not latest or set(latest) == set(selected_trading_pairs):
                    return
                selected_trading_pairs[:] = latest
                config["trading_pairs"] = latest.copy()
                render_trading_pair_rows()
                # render_min_size_rows / render_symbol_cap_rows / render_isolated_seed_rows
                # are defined later in the same scope; Python late-binding means they are
                # already resolved by the time this timer fires.
                try:
                    render_min_size_rows()  # type: ignore[name-defined]  # noqa: F821
                    render_symbol_cap_rows()  # type: ignore[name-defined]  # noqa: F821
                    render_isolated_seed_rows()  # type: ignore[name-defined]  # noqa: F821
                except NameError:
                    pass  # guard during initial page construction
                ui.notify(
                    f"Trading pairs updated by screener: {', '.join(latest)}",
                    color="info",
                    timeout=6000,
                )

            _t_screener = ui.timer(5.0, _sync_screener_pairs)
            client.on_disconnect(_t_screener.deactivate)
            client.on_delete(_t_screener.deactivate)

            ui.label("Live Execution").classes("text-sm font-semibold text-rose-600 mt-2")
            with ui.row().classes("w-full flex-wrap gap-4"):
                execution_switch = ui.switch(
                    "Auto-Execute OKX Trades",
                    value=config.get("execution_enabled", False),
                ).classes("w-full md:w-64 text-rose-700")
                execution_trade_mode_select = ui.select(
                    ["cross", "isolated"],
                    label="Trade Mode",
                    value=config.get("execution_trade_mode", "cross"),
                ).classes("w-full md:w-40")
                execution_min_size_input = ui.number(
                    label="Min Order Size",
                    value=config.get("execution_min_size", 1.0),
                    min=0.0001,
                    step=0.0001,
                ).classes("w-full md:w-48").props(
                    "hint='Prevents dust trades; measured in contracts/base units' persistent-hint"
                )
                okx_env_select = ui.select(
                    {
                        "0": "Live (Production)",
                        "1": "Paper / Demo",
                    },
                    label="OKX Environment",
                    value=str(config.get("okx_api_flag", "0") or "0"),
                ).classes("w-full md:w-48").props(
                    "hint='Flag=0 targets live trading; Flag=1 targets OKX simulated trading endpoints.' persistent-hint"
                )
                okx_sub_account_input = ui.input(
                    label="OKX Sub-Account",
                    value=config.get("okx_sub_account") or "",
                    placeholder="Leave blank for primary",
                ).classes("w-full md:w-56").props(
                    "hint='Orders + balances will target this sub-account' persistent-hint"
                )
                okx_master_routing_switch = ui.switch(
                    "API key created on parent account",
                    value=config.get("okx_sub_account_use_master", False),
                ).classes("w-full md:w-64").props(
                    "hint='Enable when using parent-account API keys that need the subAcct flag to reach this sub-account.' persistent-hint"
                )
            with ui.column().classes("w-full gap-2"):
                ui.label("Per-Symbol Overrides (optional)").classes("text-xs text-slate-500")
                min_size_rows = ui.column().classes("w-full gap-2")
                min_size_overrides = {
                    str(symbol).upper(): float(value)
                    for symbol, value in (config.get("execution_min_sizes", {}) or {}).items()
                    if isinstance(value, (int, float)) and value > 0
                }
                override_widgets: dict[str, ui.number] = {}
                quote_inputs: dict[str, ui.number] = {}
                price_labels: dict[str, ui.label] = {}
                symbol_cap_overrides = {}
                for symbol, value in (guardrails.get("symbol_position_caps", {}) or {}).items():
                    numeric = _normalize_fraction(value)
                    if numeric is None or numeric <= 0:
                        continue
                    symbol_cap_overrides[str(symbol).upper()] = numeric
                symbol_cap_widgets: dict[str, ui.number] = {}
                isolated_seed_overrides: dict[str, float] = {}
                for symbol, value in (guardrails.get("isolated_margin_symbol_seeds_usd", {}) or {}).items():
                    numeric = _safe_float(value)
                    if numeric is None or numeric <= 0:
                        continue
                    isolated_seed_overrides[str(symbol).upper()] = numeric
                isolated_seed_widgets: dict[str, ui.number] = {}

                def sync_widget(sym_key: str, value: float | None) -> None:
                    widget = override_widgets.get(sym_key)
                    if widget is None:
                        return
                    widget.value = value
                    widget.update()

                def update_price_label(sym_key: str, price: float | None) -> None:
                    label = price_labels.get(sym_key)
                    if not label:
                        return
                    if price:
                        label.set_text(f"Price ${price:,.2f}")
                    else:
                        label.set_text("Price --")
                    label.update()

                async def handle_usdt_conversion(sym_key: str) -> None:
                    widget = quote_inputs.get(sym_key)
                    if widget is None:
                        return
                    amount = widget.value
                    if amount in (None, "") or float(amount) <= 0:
                        with client:
                            ui.notify("Enter a positive USDT amount", color="warning")
                        return
                    price = await lookup_symbol_price(sym_key)
                    if not price:
                        with client:
                            ui.notify(f"No live price for {sym_key}", color="warning")
                        return
                    size = max(0.0001, round(float(amount) / price, 6))
                    min_size_overrides[sym_key] = size
                    sync_widget(sym_key, size)
                    update_price_label(sym_key, price)
                    with client:
                        ui.notify(f"{sym_key} min size set to {size}", color="positive")

                async def refresh_price_label(sym_key: str) -> None:
                    price = await lookup_symbol_price(sym_key)
                    update_price_label(sym_key, price)

                def render_min_size_rows() -> None:
                    min_size_rows.clear()
                    override_widgets.clear()
                    quote_inputs.clear()
                    price_labels.clear()
                    symbols_list = sorted(config.get("trading_pairs", []))
                    pending_price_fetch: list[str] = []
                    for symbol in symbols_list:
                        normalized = str(symbol).upper()
                        with min_size_rows:
                            with ui.row().classes("w-full items-start gap-2"):
                                ui.label(symbol).classes("text-sm font-semibold text-slate-600 w-32")
                                with ui.column().classes("flex-1 gap-2"):
                                    input_widget = ui.number(
                                        label="Min size (base units)",
                                        value=min_size_overrides.get(normalized),
                                        min=0.0001,
                                        step=0.0001,
                                        placeholder="Follow default",
                                    ).classes("w-full").props("outlined dense")
                                    override_widgets[normalized] = input_widget

                                    def create_handler(sym_key: str) -> Callable[[Any], None]:
                                        def handler(e: Any) -> None:
                                            try:
                                                if e.value in (None, ""):
                                                    min_size_overrides.pop(sym_key, None)
                                                else:
                                                    value = float(e.value)
                                                    if value <= 0:
                                                        min_size_overrides.pop(sym_key, None)
                                                    else:
                                                        min_size_overrides[sym_key] = value
                                            except (TypeError, ValueError):
                                                min_size_overrides.pop(sym_key, None)
                                        return handler

                                    input_widget.on_value_change(create_handler(normalized))

                                    with ui.row().classes("w-full items-center gap-2 flex-wrap"):
                                        usdt_input = ui.number(
                                            label="Budget (USDT)",
                                            value=None,
                                            min=0.01,
                                            step=1,
                                            placeholder="e.g. 25",
                                        ).classes("flex-1 md:w-48").props("outlined dense")
                                        quote_inputs[normalized] = usdt_input
                                        ui.button(
                                            "Convert",
                                            icon="currency_exchange",
                                            on_click=lambda sym_key=normalized: asyncio.create_task(
                                                handle_usdt_conversion(sym_key)
                                            ),
                                        ).props("outline dense")
                                        price_label = ui.label("Price --").classes(
                                            "text-xs text-slate-500 w-32 text-right"
                                        )
                                        price_labels[normalized] = price_label
                                    pending_price_fetch.append(normalized)

                                def clear_override(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(_: Any) -> None:
                                        min_size_overrides.pop(sym_key, None)
                                        sync_widget(sym_key, None)
                                        update_price_label(sym_key, None)
                                        quote_widget = quote_inputs.get(sym_key)
                                        if quote_widget:
                                            quote_widget.value = None
                                            quote_widget.update()
                                    return _handler

                                ui.button("Clear", on_click=clear_override(normalized)).props("flat dense")

                    for sym_key in pending_price_fetch:
                        asyncio.create_task(refresh_price_label(sym_key))

                render_min_size_rows()
                ui.label("Per-Symbol Position Caps (% of equity)").classes("text-xs text-slate-500")
                ui.label(
                    "When these caps trim a trade, the minimum leverage guardrail is temporarily relaxed for that order."
                ).classes("text-xs text-slate-500 italic")
                symbol_cap_rows = ui.column().classes("w-full gap-2")

                def render_symbol_cap_rows() -> None:
                    symbol_cap_rows.clear()
                    symbol_cap_widgets.clear()
                    symbols_list = sorted(config.get("trading_pairs", []))
                    for symbol in symbols_list:
                        normalized = str(symbol).upper()
                        with symbol_cap_rows:
                            with ui.row().classes("w-full items-start gap-2"):
                                ui.label(symbol).classes("text-sm font-semibold text-slate-600 w-32")
                                cap_input = ui.number(
                                    label="Max position %",
                                    value=_fraction_to_percent(symbol_cap_overrides.get(normalized)),
                                    min=0.1,
                                    max=100.0,
                                    step=0.1,
                                    placeholder="Inherit global cap",
                                ).classes("flex-1").props(
                                    "outlined dense hint='Percent of equity allocated to this symbol (e.g., 12.5 = 12.5%)' persistent-hint"
                                )
                                symbol_cap_widgets[normalized] = cap_input

                                def cap_handler(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(event: Any) -> None:
                                        try:
                                            value = event.value
                                            if value in (None, ""):
                                                symbol_cap_overrides.pop(sym_key, None)
                                            else:
                                                numeric = float(value)
                                                if numeric <= 0:
                                                    symbol_cap_overrides.pop(sym_key, None)
                                                else:
                                                    symbol_cap_overrides[sym_key] = min(numeric / 100.0, 1.0)
                                        except (TypeError, ValueError):
                                            symbol_cap_overrides.pop(sym_key, None)
                                        update_payload_preview()
                                    return _handler

                                cap_input.on_value_change(cap_handler(normalized))

                                def clear_cap(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(_: Any) -> None:
                                        symbol_cap_overrides.pop(sym_key, None)
                                        widget = symbol_cap_widgets.get(sym_key)
                                        if widget:
                                            widget.value = None
                                            widget.update()
                                        update_payload_preview()
                                    return _handler

                                ui.button("Clear", on_click=clear_cap(normalized)).props("flat dense")

                render_symbol_cap_rows()
                ui.label("Per-Symbol Auto-Seed (USDT)").classes("text-xs text-slate-500")
                ui.label(
                    "Limits how much USDT each symbol may borrow from the funding account during auto-seed retries."
                ).classes("text-xs text-slate-500 italic")
                isolated_seed_rows = ui.column().classes("w-full gap-2")

                def render_isolated_seed_rows() -> None:
                    isolated_seed_rows.clear()
                    isolated_seed_widgets.clear()
                    symbols_list = sorted(config.get("trading_pairs", []))
                    for symbol in symbols_list:
                        normalized = str(symbol).upper()
                        with isolated_seed_rows:
                            with ui.row().classes("w-full items-start gap-2"):
                                ui.label(symbol).classes("text-sm font-semibold text-slate-600 w-32")
                                seed_input = ui.number(
                                    label="Transfer cap (USDT)",
                                    value=isolated_seed_overrides.get(normalized),
                                    min=0,
                                    step=1,
                                    placeholder="Follow default",
                                ).classes("flex-1").props(
                                    "outlined dense hint='Max USDT auto-moved for this symbol when isolated margin is empty' persistent-hint"
                                )
                                isolated_seed_widgets[normalized] = seed_input

                                def seed_handler(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(event: Any) -> None:
                                        try:
                                            value = event.value
                                            if value in (None, ""):
                                                isolated_seed_overrides.pop(sym_key, None)
                                            else:
                                                numeric = float(value)
                                                if numeric <= 0:
                                                    isolated_seed_overrides.pop(sym_key, None)
                                                else:
                                                    isolated_seed_overrides[sym_key] = numeric
                                        except (TypeError, ValueError):
                                            isolated_seed_overrides.pop(sym_key, None)
                                        update_payload_preview()
                                    return _handler

                                seed_input.on_value_change(seed_handler(normalized))

                                def clear_seed(sym_key: str) -> Callable[[Any], None]:
                                    def _handler(_: Any) -> None:
                                        isolated_seed_overrides.pop(sym_key, None)
                                        widget = isolated_seed_widgets.get(sym_key)
                                        if widget:
                                            widget.value = None
                                            widget.update()
                                        update_payload_preview()
                                    return _handler

                                ui.button("Clear", on_click=clear_seed(normalized)).props("flat dense")

                render_isolated_seed_rows()
            ui.label(
                "Orders are sent as market orders on OKX. Enable only on funded accounts."
            ).classes("text-xs text-rose-600")

        def _sync_scheduler_ui() -> None:
            """Enable/disable LLM scheduler controls based on current selections.

            The Scheduler toggle is the master on/off.  When off, all
            scheduling controls are disabled regardless of other settings.
            When on, secondary rules apply:
            - Launcher-only mode: trigger and interval are irrelevant (the
              Launcher owns its own schedule) — both disabled.
            - Consecutive trigger: interval is irrelevant — disabled.
            - Scheduled LLM/filter mode: both trigger and interval active.
            """
            scheduler_on = bool(auto_prompt_switch.value)
            launcher_only = str(gov_mode_select.value or "disabled") == "launcher_only"
            consecutive = str(auto_prompt_trigger_select.value or "scheduled") == "consecutive"
            if not scheduler_on:
                # Master off — nothing is scheduled, grey everything out.
                auto_prompt_trigger_select.disable()
                auto_prompt_interval_input.disable()
            elif launcher_only:
                # Launcher owns its schedule; these controls are irrelevant.
                auto_prompt_trigger_select.disable()
                auto_prompt_interval_input.disable()
            elif consecutive:
                # No fixed interval needed — re-runs as soon as positions close.
                auto_prompt_trigger_select.enable()
                auto_prompt_interval_input.disable()
            else:
                # Scheduled LLM / filter mode — all controls active.
                auto_prompt_trigger_select.enable()
                auto_prompt_interval_input.enable()

        _sync_scheduler_ui()
        if not execution_switch.value:
            execution_trade_mode_select.disable()
            execution_min_size_input.disable()

        def describe_model_cost(model_id: str | None) -> str:
            if not model_id:
                return "Select a model to view pricing"
            entry = model_metadata.get(model_id)
            if not entry:
                return "Pricing unavailable for this model"
            pricing = entry.get("pricing")
            if not pricing:
                return "Pricing unavailable for this model"
            prompt = _format_price(pricing.get("prompt"))
            completion = _format_price(pricing.get("completion"))
            currency = (pricing.get("currency") or "USD").upper()
            symbol = "$" if currency == "USD" else f"{currency} "
            unit = pricing.get("unit") or "per 1M tokens"
            parts = []
            if prompt:
                parts.append(f"prompt {symbol}{prompt}")
            if completion:
                parts.append(f"completion {symbol}{completion}")
            if not parts:
                return "Pricing unavailable for this model"
            joined = " / ".join(parts)
            return f"Cost: {joined} ({unit})"

        def update_model_cost_label(model_id: str | None) -> None:
            model_cost_label.set_text(describe_model_cost(model_id))
            model_cost_label.update()

        def on_auto_prompt_toggle(_: Any) -> None:
            _sync_scheduler_ui()

        auto_prompt_switch.on_value_change(on_auto_prompt_toggle)

        def on_trigger_change(_: Any) -> None:
            _sync_scheduler_ui()

        auto_prompt_trigger_select.on_value_change(on_trigger_change)

        def on_gov_mode_change(_: Any) -> None:
            _sync_scheduler_ui()

        gov_mode_select.on_value_change(on_gov_mode_change)

        def on_execution_toggle(e: Any) -> None:
            if e.value:
                execution_trade_mode_select.enable()
                execution_min_size_input.enable()
            else:
                execution_trade_mode_select.disable()
                execution_min_size_input.disable()

        execution_switch.on_value_change(on_execution_toggle)

        def _clean_symbol_caps() -> dict[str, float]:
            cleaned: dict[str, float] = {}
            for symbol, value in symbol_cap_overrides.items():
                numeric = _normalize_fraction(value)
                if numeric is None or numeric <= 0:
                    continue
                cleaned[symbol] = numeric
            return cleaned

        def _clean_isolated_seed_overrides() -> dict[str, float]:
            cleaned: dict[str, float] = {}
            for symbol, value in isolated_seed_overrides.items():
                numeric = _safe_float(value)
                if numeric is None or numeric <= 0:
                    continue
                cleaned[symbol] = numeric
            return cleaned

        def build_guardrails_snapshot() -> dict[str, Any]:
            snapshot_max_pct = _percent_to_fraction(max_position_pct_input.value)
            if snapshot_max_pct is None:
                snapshot_max_pct = guardrails.get("max_position_pct")
            snapshot_daily_limit = _percent_to_fraction(daily_loss_limit_input.value)
            if snapshot_daily_limit is None:
                snapshot_daily_limit = guardrails.get("daily_loss_limit_pct")
            snapshot = {
                "min_leverage": _safe_float(min_leverage_input.value),
                "max_leverage": _safe_float(max_leverage_input.value),
                "max_position_pct": snapshot_max_pct,
                "daily_loss_limit_pct": snapshot_daily_limit,
                "min_hold_seconds": _safe_int(min_hold_seconds_input.value),
                "max_trades_per_hour": _safe_int(max_trades_per_hour_input.value),
                "max_trades_to_open": _safe_int(max_trades_to_open_input.value),
                "trade_window_seconds": _safe_int(trade_window_seconds_input.value),
                "risk_model": guardrails.get("risk_model", "ATR based stops x1.5"),
                "require_position_alignment": bool(require_alignment_switch.value),
                "wait_for_tp_sl": bool(wait_for_tp_sl_switch.value),
                "fallback_orders_enabled": bool(fallback_orders_switch.value),
                "require_reward_risk_ratio": bool(require_rr_switch.value),
                "require_protection": bool(require_protection_switch.value),
                "flip_llm_decision": bool(flip_llm_decision_switch.value),
                "adjust_invalid_tp": bool(adjust_invalid_tp_switch.value),
                "adjust_invalid_tp_pct": (_safe_float(adjust_invalid_tp_pct_input.value) or 10.0) / 100,
                "snapshot_max_age_seconds": _safe_int(snapshot_max_age_input.value)
                or config.get("snapshot_max_age_seconds"),
            }
            symbol_caps_preview = _clean_symbol_caps()
            snapshot["symbol_position_caps"] = symbol_caps_preview or None
            snapshot["isolated_margin_seed_usd"] = _safe_float(isolated_seed_default_input.value)
            snapshot["isolated_margin_max_transfer_usd"] = _safe_float(isolated_seed_max_input.value)
            seed_overrides_preview = _clean_isolated_seed_overrides()
            snapshot["isolated_margin_symbol_seeds_usd"] = seed_overrides_preview or None
            bootstrap_pct_snapshot = _percent_to_fraction(isolated_bootstrap_pct_input.value)
            if bootstrap_pct_snapshot is not None:
                bootstrap_pct_snapshot = min(max(bootstrap_pct_snapshot, 0.0), 1.0)
            snapshot["isolated_wallet_bootstrap_pct"] = bootstrap_pct_snapshot
            snapshot["llm_notional_mode"] = llm_notional_mode_select.value
            snapshot["min_trade_notional_usd"] = _safe_float(min_trade_notional_usd_input.value)
            snapshot["footprint"] = {
                "poc_risk_delta": _safe_float(fp_poc_risk_input.value) if _safe_float(fp_poc_risk_input.value) is not None else 0.05,
                "net_delta_confidence_delta": _safe_float(fp_net_delta_conf_input.value) if _safe_float(fp_net_delta_conf_input.value) is not None else 0.02,
                "imbalance_zone_confidence_delta": _safe_float(fp_imbalance_conf_input.value) if _safe_float(fp_imbalance_conf_input.value) is not None else 0.03,
                "imbalance_zone_proximity_pct": _safe_float(fp_proximity_input.value) if _safe_float(fp_proximity_input.value) is not None else 0.3,
            }
            return snapshot

        async def hydrate_execution_settings() -> None:
            try:
                stored = await load_execution_settings()
            except Exception as exc:  # pragma: no cover - optional DB
                with client:
                    ui.notify(f"Failed to load execution settings: {exc}", color="warning")
                return
            if not stored:
                return
            with client:
                min_size = stored.get("min_size")
                if isinstance(min_size, (int, float)) and min_size > 0:
                    config["execution_min_size"] = float(min_size)
                    execution_min_size_input.value = float(min_size)
                    execution_min_size_input.update()
                stored_min_sizes = stored.get("min_sizes")
                if isinstance(stored_min_sizes, dict) and stored_min_sizes:
                    cleaned = {
                        str(symbol).upper(): float(value)
                        for symbol, value in stored_min_sizes.items()
                        if isinstance(value, (int, float)) and value > 0
                    }
                    if cleaned:
                        config["execution_min_sizes"] = cleaned
                        min_size_overrides.clear()
                        min_size_overrides.update(cleaned)
                        render_min_size_rows()

        update_model_cost_label(initial_model_value)
        asyncio.create_task(hydrate_execution_settings())

        async def hydrate_candle_settings() -> None:
            try:
                stored = await load_candle_settings()
            except Exception:  # pragma: no cover - optional DB
                return
            if not stored:
                return
            with client:
                for key, widget in [
                    ("fetch_limit", ohlcv_fetch_limit_input),
                    ("snapshot_candles", ohlcv_snapshot_candles_input),
                    ("snapshot_htf_candles", ohlcv_snapshot_htf_candles_input),
                ]:
                    val = stored.get(key)
                    if val is not None:
                        config[f"ohlcv_{key}"] = int(val)
                        widget.value = int(val)
                        widget.update()

        asyncio.create_task(hydrate_candle_settings())

        async def load_trading_pairs() -> None:
            market_service = getattr(app.state, "market_service", None)
            pairs = config.get("trading_pairs", ["BTC-USDT-SWAP"])
            if market_service:
                try:
                    pairs = await market_service.list_available_symbols()
                except Exception as exc:  # pragma: no cover - network call
                    ui.notify(f"Failed to load pairs: {exc}", color="warning")
            trading_pairs_select.options = pairs
            trading_pairs_select.enable()
            trading_pairs_select.value = None
            trading_pairs_select.update()

        async def hydrate_model_select() -> None:
            try:
                records = await list_openrouter_models(app)
            except Exception as exc:  # pragma: no cover - optional network
                ui.notify(f"Failed to load OpenRouter models: {exc}", color="warning")
                return
            if not records:
                return
            model_metadata.clear()
            for entry in records:
                model_metadata[entry["id"]] = entry
            options = {entry["id"]: _option_label(entry) for entry in records}
            with client:
                model_select.options = options
                if model_select.value not in options and options:
                    model_select.value = next(iter(options))
                    config["llm_model_id"] = model_select.value
                    apply_model_change(model_select.value)
                model_select.update()
                update_model_cost_label(model_select.value)

        def apply_model_change(model_id: str | None) -> None:
            update_model_cost_label(model_id)

        def on_model_change(e: Any) -> None:
            apply_model_change(getattr(e, "value", None))

        model_select.on_value_change(on_model_change)

        # ── Config export / import ────────────────────────────────────────────
        # Keys excluded from export because they are account/environment-specific
        # (OKX routing), runtime-only state, or prompt text (managed separately).
        _CONFIG_EXPORT_EXCLUDED = {
            "okx_sub_account",
            "okx_sub_account_use_master",
            "okx_api_flag",
            "risk_locks",
            "llm_response_schemas",
            "llm_system_prompt",
            "llm_decision_prompt",
            "prompt_sections",
            "prompt_version_id",
            "prompt_version_name",
        }

        def _build_export_payload() -> dict[str, Any]:
            """Return a sanitized snapshot of runtime_config safe for export."""
            runtime = getattr(app.state, "runtime_config", {}) or {}
            sanitized = {
                key: value
                for key, value in runtime.items()
                if key not in _CONFIG_EXPORT_EXCLUDED
            }
            return {
                "version": 1,
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "config": sanitized,
            }

        def export_config() -> None:
            """Serialize the current runtime config to a downloadable JSON file."""
            try:
                payload = _build_export_payload()
                content = json.dumps(payload, indent=2, default=str, sort_keys=True)
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                filename = f"tai2-config-{stamp}.json"
                ui.download(content.encode("utf-8"), filename=filename)
                ui.notify(f"Exported config to {filename}", color="positive")
            except Exception as exc:  # pragma: no cover - defensive
                ui.notify(f"Failed to export config: {exc}", color="negative")

        def _trigger_upload() -> None:
            """Programmatically open the hidden upload's file picker."""
            try:
                import_upload.run_method("pickFiles")
            except Exception as exc:  # pragma: no cover - defensive
                ui.notify(f"Failed to open file picker: {exc}", color="negative")

        async def _handle_import(e: Any) -> None:
            """Read the uploaded JSON, validate, and apply to runtime_config."""
            try:
                file_obj = getattr(e, "file", None)
                if file_obj is None:
                    ui.notify("No file content received", color="negative")
                    return
                # FileUpload.read() is async in NiceGUI 3.x
                raw = await file_obj.read()
                text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                ui.notify(f"Invalid JSON: {exc}", color="negative")
                return
            except Exception as exc:  # pragma: no cover - defensive
                ui.notify(f"Failed to read import file: {exc}", color="negative")
                return
            if not isinstance(payload, dict) or payload.get("version") != 1:
                ui.notify(
                    "Unrecognized config file (missing version=1). Import aborted.",
                    color="negative",
                )
                return
            imported_config = payload.get("config")
            if not isinstance(imported_config, dict) or not imported_config:
                ui.notify("Config file contains no config object", color="negative")
                return
            # Reject any keys that should never be imported.
            forbidden = imported_config.keys() & _CONFIG_EXPORT_EXCLUDED
            if forbidden:
                ui.notify(
                    f"Config file contains protected keys ({', '.join(sorted(forbidden))}); "
                    "import aborted.",
                    color="negative",
                )
                return
            # Replace all included keys in runtime_config (missing keys keep current values).
            runtime = getattr(app.state, "runtime_config", {}) or {}
            runtime.update(imported_config)
            app.state.runtime_config = runtime
            # Mirror into the local `config` dict so the current page reflects the
            # imported values until the next full reload.
            config.update(imported_config)
            ui.notify(
                "Config imported into runtime. Click Save to persist to the database, "
                "or reload the page to review the applied values.",
                color="positive",
                timeout=8000,
            )

        import_upload.on_upload(_handle_import)

        async def save_settings(event: Any | None = None) -> None:
            def _safe_notify(message: str, **kwargs: Any) -> None:
                """Silently ignore RuntimeError when the NiceGUI slot has been deleted."""
                try:
                    ui.notify(message, **kwargs)
                except RuntimeError:
                    pass

            config["notifications"] = {
                "trade_open": bool(notify_trade_open_switch.value),
                "trade_close": bool(notify_trade_close_switch.value),
            }
            config["poll_interval"] = int(ws_interval_input.value or 180)
            config["ohlcv_fetch_limit"] = max(50, int(ohlcv_fetch_limit_input.value or 200))
            config["ohlcv_snapshot_candles"] = max(10, int(ohlcv_snapshot_candles_input.value or 50))
            config["ohlcv_snapshot_htf_candles"] = max(5, int(ohlcv_snapshot_htf_candles_input.value or 25))
            config["enable_websocket"] = bool(websocket_switch.value)
            config["auto_prompt_enabled"] = bool(auto_prompt_switch.value)
            config["execution_enabled"] = bool(execution_switch.value)
            config["wait_for_tp_sl"] = bool(wait_for_tp_sl_switch.value)
            config["fallback_orders_enabled"] = bool(fallback_orders_switch.value)
            config["llm_model_id"] = model_select.value
            config["llm_timeout_seconds"] = int(llm_timeout_input.value or 300)
            config["llm_reasoning_effort"] = llm_reasoning_effort_select.value or "low"
            timeframe_value = (
                ta_timeframe_select_cfg.value
                or config.get("ta_timeframe")
                or "4H"
            )
            if timeframe_value not in TA_TIMEFRAME_OPTIONS:
                timeframe_value = "4H"
            config["ta_timeframe"] = timeframe_value
            timezone_value = (
                timezone_select.value
                or config.get("frontend_timezone")
                or DEFAULT_FRONTEND_TIMEZONE
            )
            config["frontend_timezone"] = timezone_value
            try:
                await save_poll_interval(config["poll_interval"])
            except Exception as exc:  # pragma: no cover - optional DB
                _safe_notify(f"Failed to persist poll interval: {exc}", color="warning")
            try:
                await save_candle_settings(
                    config["ohlcv_fetch_limit"],
                    config["ohlcv_snapshot_candles"],
                    config["ohlcv_snapshot_htf_candles"],
                )
            except Exception as exc:  # pragma: no cover - optional DB
                _safe_notify(f"Failed to persist candle settings: {exc}", color="warning")
            try:
                await save_frontend_timezone(timezone_value)
            except Exception as exc:  # pragma: no cover - optional DB
                _safe_notify(f"Failed to persist timezone: {exc}", color="warning")
            try:
                await save_llm_model(config["llm_model_id"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist default model: {exc}", color="warning")
            try:
                await save_ta_timeframe(config["ta_timeframe"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist timeframe: {exc}", color="warning")

            def _coerce(value: Any, fallback: Any, caster: Any) -> Any:
                try:
                    if value is None:
                        raise ValueError
                    return caster(value)
                except (TypeError, ValueError):
                    return caster(fallback)

            # When launcher_only mode is active the Launcher's own "Entry
            # schedule" setting is the single source of truth for how the
            # scheduler loops.  Automatically sync the two scheduler controls
            # so they can never conflict.
            _gov_mode_save = str(gov_mode_select.value or "disabled")
            if _gov_mode_save == "launcher_only":
                _gov_sched_save = str(gov_schedule_select.value or "timer")
                config["auto_prompt_trigger"] = (
                    "consecutive" if _gov_sched_save == "on_close" else "scheduled"
                )
                _gov_interval_save = _coerce(
                    gov_interval_input.value,
                    300.0,
                    float,
                )
                config["auto_prompt_interval"] = max(30, int(_gov_interval_save))
            else:
                config["auto_prompt_interval"] = max(
                    30,
                    _coerce(
                        auto_prompt_interval_input.value,
                        config.get("auto_prompt_interval", 300),
                        int,
                    ),
                )
                config["auto_prompt_trigger"] = str(auto_prompt_trigger_select.value or "scheduled")
            try:
                await save_prompt_interval(config["auto_prompt_interval"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist prompt interval: {exc}", color="warning")
            config["execution_trade_mode"] = execution_trade_mode_select.value or "cross"
            config["execution_order_type"] = "market"
            config["execution_min_size"] = max(
                0.0001,
                _coerce(
                    execution_min_size_input.value,
                    config.get("execution_min_size", 1.0),
                    float,
                ),
            )
            config["execution_min_sizes"] = {
                str(symbol).upper(): float(value)
                for symbol, value in min_size_overrides.items()
                if isinstance(value, (int, float)) and value > 0
            }
            try:
                await save_execution_settings(
                    {
                        "enabled": config["execution_enabled"],
                        "trade_mode": config["execution_trade_mode"],
                        "order_type": config["execution_order_type"],
                        "min_size": config["execution_min_size"],
                        "min_sizes": config["execution_min_sizes"],
                    }
                )
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist execution settings: {exc}", color="warning")
            config["fee_window_hours"] = max(
                1.0,
                _coerce(
                    fee_window_input.value,
                    config.get("fee_window_hours", 24.0),
                    float,
                ),
            )
            sub_account_value = (okx_sub_account_input.value or "").strip()
            config["okx_sub_account"] = sub_account_value or None
            config["okx_sub_account_use_master"] = bool(okx_master_routing_switch.value)
            api_flag_value = str(okx_env_select.value or config.get("okx_api_flag") or "0").strip()
            config["okx_api_flag"] = api_flag_value if api_flag_value in {"0", "1"} else "0"
            try:
                await save_okx_sub_account(
                    config["okx_sub_account"],
                    config["okx_sub_account_use_master"],
                    config["okx_api_flag"],
                )
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist OKX sub-account: {exc}", color="warning")

            new_max_pct = _percent_to_fraction(max_position_pct_input.value)
            if new_max_pct is None:
                new_max_pct = guardrails.get("max_position_pct", 0.2)
            else:
                new_max_pct = max(0.0, min(1.0, new_max_pct))
            new_daily_loss_limit = _percent_to_fraction(daily_loss_limit_input.value)
            if new_daily_loss_limit is None:
                new_daily_loss_limit = guardrails.get("daily_loss_limit_pct", 0.03)
            else:
                new_daily_loss_limit = max(0.0, min(1.0, new_daily_loss_limit))
            bootstrap_pct_fraction = _percent_to_fraction(isolated_bootstrap_pct_input.value)
            if bootstrap_pct_fraction is not None:
                bootstrap_pct_fraction = max(0.0, min(1.0, bootstrap_pct_fraction))

            config["guardrails"] = {
                "min_leverage": _coerce(min_leverage_input.value, guardrails.get("min_leverage", 1), float),
                "max_leverage": _coerce(max_leverage_input.value, guardrails.get("max_leverage", 5), float),
                "min_leverage_confidence_gate": max(
                    0.0,
                    min(
                        1.0,
                        _coerce(
                            min_leverage_conf_gate_input.value,
                            guardrails.get("min_leverage_confidence_gate", 0.5),
                            float,
                        ),
                    ),
                ),
                "max_position_pct": new_max_pct,
                "daily_loss_limit_pct": new_daily_loss_limit,
                "atr_risk_per_trade_pct": _safe_float(atr_risk_per_trade_input.value),
                "cvd_guard": {
                    "enabled": bool(cvd_guard_enabled_toggle.value),
                    "lookback": _coerce(
                        cvd_guard_lookback_input.value,
                        (guardrails.get("cvd_guard") or {}).get("lookback", 10),
                        int,
                    ),
                    "min_slope_pct": _safe_float(cvd_guard_min_slope_input.value) or 0.0,
                },
                "ob_wall_guard": {
                    "enabled": bool(ob_wall_enabled_toggle.value),
                    "proximity_pct": _safe_float(ob_wall_proximity_input.value) or 1.0,
                    "wall_ratio": _safe_float(ob_wall_ratio_input.value) or 3.0,
                },
                "min_hold_seconds": _coerce(
                    min_hold_seconds_input.value,
                    guardrails.get("min_hold_seconds", 180),
                    int,
                ),
                "max_trades_per_hour": _coerce(
                    max_trades_per_hour_input.value,
                    guardrails.get("max_trades_per_hour", 2),
                    int,
                ),
                "max_trades_to_open": _coerce(
                    max_trades_to_open_input.value,
                    guardrails.get("max_trades_to_open", 0),
                    int,
                ),
                "trade_window_seconds": _coerce(
                    trade_window_seconds_input.value,
                    guardrails.get("trade_window_seconds", 3600),
                    int,
                ),
                "risk_model": guardrails.get("risk_model", "ATR based stops x1.5"),
                "require_position_alignment": bool(require_alignment_switch.value),
                "wait_for_tp_sl": bool(wait_for_tp_sl_switch.value),
                "fallback_orders_enabled": bool(fallback_orders_switch.value),
                "require_reward_risk_ratio": bool(require_rr_switch.value),
                "require_protection": bool(require_protection_switch.value),
                "flip_llm_decision": bool(flip_llm_decision_switch.value),
                "adjust_invalid_tp": bool(adjust_invalid_tp_switch.value),
                "adjust_invalid_tp_pct": (_safe_float(adjust_invalid_tp_pct_input.value) or 10.0) / 100,
                "snapshot_max_age_seconds": _coerce(
                    snapshot_max_age_input.value,
                    config.get("snapshot_max_age_seconds", settings.snapshot_max_age_seconds),
                    int,
                ),
                "execution_feedback_ttl_seconds": max(
                    0,
                    _coerce(
                        execution_feedback_ttl_input.value,
                        guardrails.get("execution_feedback_ttl_seconds", 600),
                        int,
                    ),
                ),
                "symbol_position_caps": _clean_symbol_caps(),
                "isolated_margin_seed_usd": _safe_float(isolated_seed_default_input.value),
                "isolated_margin_max_transfer_usd": _safe_float(isolated_seed_max_input.value),
                "isolated_margin_symbol_seeds_usd": _clean_isolated_seed_overrides(),
                "isolated_wallet_bootstrap_pct": bootstrap_pct_fraction,
                "llm_notional_mode": llm_notional_mode_select.value,
                "min_trade_notional_usd": _safe_float(min_trade_notional_usd_input.value),
                "footprint": {
                    "bucket_pct": _safe_float(fp_bucket_pct_input.value) if _safe_float(fp_bucket_pct_input.value) is not None else 0.1,
                    "poc_risk_delta": _safe_float(fp_poc_risk_input.value) if _safe_float(fp_poc_risk_input.value) is not None else 0.05,
                    "net_delta_confidence_delta": _safe_float(fp_net_delta_conf_input.value) if _safe_float(fp_net_delta_conf_input.value) is not None else 0.02,
                    "imbalance_zone_confidence_delta": _safe_float(fp_imbalance_conf_input.value) if _safe_float(fp_imbalance_conf_input.value) is not None else 0.03,
                    "imbalance_zone_proximity_pct": _safe_float(fp_proximity_input.value) if _safe_float(fp_proximity_input.value) is not None else 0.3,
                },
            }
            config["snapshot_max_age_seconds"] = config["guardrails"].get(
                "snapshot_max_age_seconds",
                settings.snapshot_max_age_seconds,
            )
            config["wait_for_tp_sl"] = bool(
                config["guardrails"].get("wait_for_tp_sl", False)
            )
            config["fallback_orders_enabled"] = bool(
                config["guardrails"].get("fallback_orders_enabled", True)
            )
            symbols: list[str] = []
            for item in selected_trading_pairs:
                normalized = str(item).strip().upper()
                if not normalized or normalized in symbols:
                    continue
                symbols.append(normalized)
            if not symbols:
                _safe_notify("At least one trading pair must be selected before saving.", color="negative")
                return
            selected_trading_pairs[:] = symbols
            config["trading_pairs"] = symbols
            try:
                await set_enabled_trading_pairs(symbols)
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist trading pairs: {exc}", color="warning")
            try:
                await save_guardrails(config["guardrails"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist guardrails: {exc}", color="warning")
            config["screener"] = {
                "enabled": bool(auto_select_symbols_switch.value),
                "universe_filter": str(screener_universe_input.value or "*-USDT-SWAP").strip(),
                "max_symbols": max(1, _coerce(screener_max_symbols_input.value, 5, int)),
                "interval_minutes": max(5, _coerce(screener_interval_input.value, 60, int)),
                "min_volume_usd": max(0.0, _coerce(screener_min_volume_input.value, 0.0, float) * 1_000_000),
                "min_momentum_pct": max(0.0, _coerce(screener_min_momentum_input.value, 0.0, float)),
                "min_hl_range_pct": max(0.0, _coerce(screener_min_hl_range_input.value, 0.0, float)),
            }
            # Preserve signal fields from config (edited on STRATEGY page) and
            # only overwrite the operational fields managed on this page.
            _existing_launcher = config.get("launcher") or {}
            config["launcher"] = {
                **_existing_launcher,
                "mode": str(gov_mode_select.value or "disabled"),
                "schedule": str(gov_schedule_select.value or "timer"),
                "entry_interval_seconds": max(30.0, _coerce(gov_interval_input.value, 300, float)),
                "trade_mode": str(gov_trade_mode_select.value or "isolated"),
                "notional_usd": float(gov_notional_input.value) if gov_notional_input.value not in (None, "") else None,
            }
            try:
                await save_launcher_config(config["launcher"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist launcher config: {exc}", color="warning")
            try:
                await save_notifications_config(config.get("notifications") or {})
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist notifications config: {exc}", color="warning")
            app.state.runtime_config = config
            llm_service = getattr(app.state, "llm_service", None)
            if llm_service:
                llm_service.set_model(model_select.value)
                llm_service.set_timeout(config["llm_timeout_seconds"])
                llm_service.set_reasoning_effort(config["llm_reasoning_effort"])
            market_service = getattr(app.state, "market_service", None)
            if market_service:
                market_service.set_notifications_config(config.get("notifications") or {})
                market_service.set_wait_for_tp_sl(config.get("wait_for_tp_sl", False))
                market_service.set_flip_llm_decision(config["guardrails"].get("flip_llm_decision", False))
                market_service.set_footprint_config(config["guardrails"].get("footprint") or {})
                market_service.set_screener_config(config["screener"])
                market_service.set_launcher_config(config["launcher"])
                await market_service.set_okx_flag(config.get("okx_api_flag"))
                await market_service.set_sub_account(
                    config.get("okx_sub_account"),
                    config.get("okx_sub_account_use_master"),
                )
                await market_service.set_ohlc_bar(config["ta_timeframe"])
                market_service.set_poll_interval(config["poll_interval"])
                market_service.set_ohlcv_fetch_limit(config["ohlcv_fetch_limit"])
                await market_service.set_websocket_enabled(config.get("enable_websocket", True))
                await market_service.update_symbols(symbols)
            try:
                await save_screener_config(config["screener"])
            except Exception as exc:  # pragma: no cover - db optional
                _safe_notify(f"Failed to persist screener config: {exc}", color="warning")
            scheduler = getattr(app.state, "prompt_scheduler", None)
            if scheduler:
                await scheduler.update_interval(config["auto_prompt_interval"])
                await scheduler.set_trigger_mode(config.get("auto_prompt_trigger", "scheduled"))
                await scheduler.set_enabled(config["auto_prompt_enabled"])
            _safe_notify("Configuration saved", color="positive")
            app.state.frontend_events.append("CFG updated")

        save_button.on("click", save_settings)
        asyncio.create_task(load_trading_pairs())
        asyncio.create_task(hydrate_model_select())

    @ui.page("/prompt")
    def prompt_page() -> None:
        render_prompt_page()

    @ui.page("/")
    def home() -> None:
        render_live_page()

    @ui.page("/live")
    def live() -> None:
        render_live_page()

    @ui.page("/ta")
    def ta() -> None:
        render_ta_page()

    @ui.page("/strategy")
    def strategy() -> None:
        render_strategy_page()

    @ui.page("/history")
    def history() -> None:
        render_history_page()

    @ui.page("/debug")
    def debug() -> None:
        render_debug_page()

    @ui.page("/cfg")
    def cfg() -> None:
        render_cfg_page()

    @ui.page("/backtest")
    def backtest() -> None:
        render_backtest_page()


__all__ = ["register_pages"]
