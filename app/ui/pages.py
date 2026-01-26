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
    fetch_equity_history,
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
    save_ta_timeframe,
    save_frontend_timezone,
    set_enabled_trading_pairs,
)
from app.services.prompt_builder import (
    DEFAULT_DECISION_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    RESPONSE_SCHEMA,
    PromptBuilder,
)
from app.services.openrouter_service import (
    DEFAULT_MODEL_OPTIONS,
    fetch_openrouter_credits,
    list_openrouter_models,
)
from app.ui.components import SnapshotStore, badge_stat

NAV_LINKS = [
    ("LIVE", "/live"),
    ("TA", "/ta"),
    ("ENGINE", "/engine"),
    ("HISTORY", "/history"),
    ("DEBUG", "/debug"),
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
        config = getattr(app.state, "runtime_config", {}) or {}
        interval = config.get("ws_update_interval", 10)
        return max(3.0, float(interval) / 2.0)

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
                        f"Auto Prompt Scheduler · {'ON' if auto_prompt_enabled else 'OFF'}"
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

        def set_ws_status(active: bool) -> None:
            label = status_label["widget"]
            if not label:
                return
            label.set_text("WS: LIVE" if active else "WS: IDLE")

        with wrapper:
            with ui.row().classes("w-full gap-6 flex-col xl:flex-row xl:flex-nowrap xl:items-start"):
                with ui.column().classes("flex-[7] w-full gap-4"):
                    header_row = ui.row().classes(
                        "w-full justify-between items-start flex-wrap gap-4"
                    )
                    with header_row:
                        with ui.column().classes("gap-1"):
                            ui.label("Live Market Overview").classes("text-2xl font-bold")
                            ui.label("Account & execution snapshot").classes(
                                "text-sm text-slate-500"
                            )
                        with ui.column().classes("items-end gap-1"):
                            status_label["widget"] = ui.label("WS: IDLE").classes(
                                "text-xs font-semibold text-slate-500"
                            )
                            refresh_label["widget"] = ui.label("Last refresh: --").classes(
                                "text-xs text-slate-500"
                            )
                            notice = (
                                ui.label("Snapshot stale")
                                .classes("text-xs font-semibold text-red-600 uppercase tracking-wide")
                            )
                            notice.set_visibility(False)
                            stale_indicator["widget"] = notice
                            action_column = ui.column().classes(
                                "gap-2 w-full sm:w-auto items-stretch"
                            )
                            with action_column:
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
                        risk_lock_refs["meta"] = ui.label("Auto Prompt Scheduler · OFF").classes(
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

                    with ui.row().classes("w-full gap-4"):
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
                    credit_hint_label = ui.label("OpenRouter credits unavailable").classes(
                        "text-xs text-slate-500"
                    )
                    credit_hint_label.set_visibility(False)
                    fee_hint_label = ui.label("OKX fees unavailable").classes(
                        "text-xs text-slate-500"
                    )
                    fee_hint_label.set_visibility(False)

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
                        <q-td :props="props">
                            <span :class="props.row.pnl_cls">{{ props.value }}</span>
                        </q-td>
                        """,
                    )
                    positions_table.add_slot(
                        "body-cell-pnl_pct",
                        """
                        <q-td :props="props">
                            <span :class="props.row.pnl_pct_cls">{{ props.value }}</span>
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

                with ui.column().classes("flex-[3] w-full gap-4"):
                    with ui.card().classes(
                        "w-full p-4 gap-3 bg-slate-50 border border-slate-200 shadow-sm"
                    ):
                        ui.label("LLM Insight Feed").classes("text-xl font-semibold")
                        ui.label(
                            "Latest response_schema guidance per tracked symbol"
                        ).classes("text-sm text-slate-500")
                        llm_empty_state = ui.label("No LLM interactions yet.").classes(
                            "text-sm text-slate-400"
                        )
                        llm_card_container = ui.column().classes("w-full gap-3")

                    with ui.card().classes(
                        "w-full p-4 gap-3 bg-white border border-slate-200 rounded-2xl shadow-[0_18px_40px_rgba(15,23,42,0.08)]"
                    ):
                        ui.label("Execution Alerts").classes("text-xl font-semibold text-slate-900")
                        ui.label(
                            "Guardrail warnings, OKX errors, and margin tips"
                        ).classes("text-sm text-slate-500")
                        execution_empty_state = ui.label(
                            "No execution feedback in the last window."
                        ).classes("text-sm text-slate-400")
                        execution_feed_refs["empty"] = execution_empty_state
                        execution_feed_refs["container"] = ui.column().classes(
                            "w-full gap-3"
                        )

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

        def refresh_llm_cards() -> None:
            llm_card_container.clear()
            interactions = getattr(app.state, "llm_interactions", {}) or {}
            items = sorted(
                interactions.values(),
                key=lambda entry: entry.get("timestamp") or "",
                reverse=True,
            )
            if not items:
                llm_empty_state.set_visibility(True)
                return
            llm_empty_state.set_visibility(False)
            with llm_card_container:
                for entry in items:
                    symbol = entry.get("symbol") or "--"
                    decision = entry.get("decision") or {}
                    action = (decision.get("action") or "--").upper()
                    header = f"{symbol} · {action} · {format_llm_timestamp(entry.get('timestamp'))}"
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
                    card = ui.expansion(header).classes(
                        "w-full bg-white rounded-xl border border-slate-200 shadow-sm"
                    )
                    with card:
                        ui.label(
                            f"Decision: {action} (conf {confidence_label})"
                        ).classes("text-sm font-semibold text-slate-700")
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
        render_execution_feedback(last_snapshot["value"])
        render_risk_lock_status()
        ui.timer(15, refresh_llm_cards)

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
                credit_hint_label.set_text(hint)
                credit_hint_label.set_visibility(True)
            else:
                credit_hint_label.set_visibility(False)

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
                fee_hint_label.set_text(error)
                fee_hint_label.set_visibility(True)
            elif total_fee is not None:
                okx_fee_card.value_label.set_text(f"${total_fee:,.2f}")
                fee_hint_label.set_text(
                    f"OKX fees · last {window_hours:g}h"
                )
                fee_hint_label.set_visibility(True)
            else:
                okx_fee_card.value_label.set_text("--")
                fee_hint_label.set_visibility(False)

        async def refresh_openrouter_credits(force: bool = False) -> None:
            try:
                usage = await fetch_openrouter_credits(app, force_refresh=force)
            except Exception:
                usage = None
            _update_credit_display(usage)

        asyncio.create_task(refresh_openrouter_credits(True))
        ui.timer(300, lambda: asyncio.create_task(refresh_openrouter_credits(True)))

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
        ui.timer(300, lambda: asyncio.create_task(refresh_okx_fees()))

        async def refresh_equity_chart() -> None:
            try:
                history = await fetch_equity_history(limit=200)
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
                        render_execution_feedback(last_snapshot["value"])
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

        def update(snapshot: dict[str, Any] | None) -> None:
            last_snapshot["value"] = snapshot
            set_ws_status(snapshot is not None)
            refresh_llm_cards()
            render_risk_lock_status()
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
            render_execution_feedback(snapshot)

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
        ui.timer(5, lambda: update_snapshot_health(last_snapshot["value"]))
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

    def render_engine_page() -> None:
        navigation("ENGINE")
        wrapper = page_container()
        store = make_snapshot_store()

        with wrapper:
            ui.label("Reasoning Engine").classes("text-2xl font-bold")
            symbol_select = ui.select(options=[], label="Symbol").classes("w-full md:w-64")
            symbol_select.disable()
            decision_card = ui.card().classes("w-full p-4")
            with decision_card:
                action_label = ui.label("Action: HOLD").classes("text-xl font-semibold")
                justification_label = ui.label("Reasoning pending...")

            ui.label("Interactive Chat").classes("text-lg font-semibold mt-4")
            chat_column = ui.column().classes("w-full gap-2")
            with chat_column:
                question_input = ui.textarea(
                    value="Explain why you haven't traded in the last hour based on the current CVD.",
                    placeholder="Ask the LLM",
                ).classes("w-full")
                send_button = ui.button("Ask", icon="send")

        current_symbol = {"value": None}
        last_snapshot = {"value": None}

        def update(snapshot: dict[str, Any] | None) -> None:
            last_snapshot["value"] = snapshot
            if not snapshot:
                return
            symbols = snapshot.get("symbols") or []
            market_data = snapshot.get("market_data") or {}
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
            custom = market_entry.get("custom_metrics") or snapshot.get("custom_metrics") or {}
            cvd = custom.get("cumulative_volume_delta", 0)
            ofi_block = custom.get("order_flow_imbalance")
            if isinstance(ofi_block, dict):
                ofi_value = ofi_block.get("net")
            else:
                ofi_value = ofi_block
            action = "BUY" if cvd and cvd > 1 else "SELL" if cvd and cvd < -1 else "HOLD"
            action_label.set_text(f"Action: {action}")
            if isinstance(ofi_value, (int, float)):
                imbalance_text = f"{ofi_value:.2f}"
            elif ofi_value is None:
                imbalance_text = "--"
            else:
                imbalance_text = str(ofi_value)
            justification_label.set_text(
                f"Derived from current order flow imbalance {imbalance_text}."
            )

        async def handle_question() -> None:
            snapshot = store.snapshot or {}
            symbols = snapshot.get("symbols") or []
            selected_symbol = current_symbol["value"] or snapshot.get("symbol")
            market_entry = (snapshot.get("market_data") or {}).get(
                selected_symbol or (symbols[0] if symbols else None),
                {},
            )
            custom = market_entry.get("custom_metrics") or snapshot.get("custom_metrics") or {}
            cvd = custom.get("cumulative_volume_delta", 0)
            ofi_block = custom.get("order_flow_imbalance")
            ofi_value = ofi_block.get("net") if isinstance(ofi_block, dict) else ofi_block
            response = (
                "No trades executed because cumulative volume delta is stable "
                f"({cvd:.2f}) and liquidity imbalance {ofi_value or '--'} doesn't justify action."
            )
            with chat_column:
                ui.chat_message(question_input.value or "", name="user").classes(
                    "self-end bg-slate-200 text-slate-900"
                )
                ui.chat_message(response, name="engine").classes("bg-emerald-50 text-emerald-900")
            app.state.frontend_events.append(
                f"User Q at {format_now('%Y-%m-%d %H:%M:%S %Z')}"
            )

        send_button.on("click", lambda _: asyncio.create_task(handle_question()))
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

        def on_symbol_change(e: Any) -> None:
            current_symbol["value"] = e.value
            update(last_snapshot["value"])

        symbol_select.on_value_change(on_symbol_change)

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
                        ],
                        rows=[],
                    ).classes("w-full")
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
            with ui.column().classes(
                "w-full gap-4"
            ):
                with ui.card().classes(
                    "w-full p-4 gap-2 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    ui.label("Backend Logs").classes("text-lg font-semibold")
                    ui.label("Engine + scheduler diagnostics").classes("text-xs text-slate-500")
                    backend_log = (
                        ui.log(max_lines=2000)
                        .classes(
                            "w-full font-mono text-xs bg-slate-900/90 text-white rounded-xl"
                        )
                        .style("min-height: 32rem; max-height: 32rem; overflow-y: auto;")
                    )
                with ui.card().classes(
                    "w-full p-4 gap-2 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    with ui.row().classes("w-full items-center justify-between"):
                        ui.label("Frontend Logs").classes("text-lg font-semibold")
                        ui.button(
                            "Emit Event",
                            icon="bolt",
                            on_click=lambda: app.state.frontend_events.append("Frontend heartbeat triggered"),
                        ).props("outlined dense")
                    ui.label("UI level actions + notifications").classes("text-xs text-slate-500")
                    frontend_log = ui.log(max_lines=1000).classes(
                        "w-full h-64 font-mono text-xs bg-slate-900/90 text-white rounded-xl"
                    )
                with ui.card().classes(
                    "w-full p-4 gap-2 bg-slate-50 border border-slate-200 shadow-sm"
                ):
                    ui.label("WebSocket Updates").classes("text-lg font-semibold")
                    ui.label("Last snapshots streamed to clients").classes(
                        "text-xs text-slate-500"
                    )
                    websocket_log = ui.log(max_lines=1000).classes(
                        "w-full h-64 font-mono text-xs bg-slate-900/90 text-white rounded-xl"
                    )

        backend_seen = {"idx": len(getattr(app.state, "backend_events", []))}
        frontend_seen = {"idx": len(getattr(app.state, "frontend_events", []))}
        websocket_seen = {"idx": len(getattr(app.state, "websocket_events", []))}

        for line in list(getattr(app.state, "backend_log_buffer", [])):
            backend_log.push(line)
        for line in list(getattr(app.state, "frontend_log_buffer", [])):
            frontend_log.push(line)
        for line in list(getattr(app.state, "websocket_log_buffer", [])):
            websocket_log.push(line)

        def _render_entry(entry: Any) -> str:
            now_label = format_now("%H:%M:%S %Z")
            if isinstance(entry, dict):
                message = entry.get("message") or entry.get("detail")
                if not message:
                    message = json.dumps(entry, ensure_ascii=False)
                ts_raw = entry.get("timestamp") or entry.get("ts")
                if ts_raw:
                    label = format_iso_timestamp(ts_raw, fmt="%H:%M:%S %Z")
                else:
                    label = now_label
                symbol = entry.get("symbol")
                if symbol:
                    message = f"{symbol}: {message}"
                return f"{label} · {message}"
            return f"{now_label} · {entry}"

        def _looks_like_websocket(entry: Any) -> bool:
            if isinstance(entry, dict):
                if entry.get("source") == "websocket":
                    return True
                text_value = entry.get("message") or entry.get("detail")
            else:
                text_value = entry
            if not isinstance(text_value, str):
                return False
            lowered = text_value.lower()
            return lowered.startswith("ws ") or "websocket" in lowered

        def push_backend() -> None:
            events = list(getattr(app.state, "backend_events", []))
            new_events = events[backend_seen["idx"] :]
            for entry in new_events:
                if _looks_like_websocket(entry):
                    rendered_ws = _render_entry(entry)
                    websocket_log.push(rendered_ws)
                    buffer = getattr(app.state, "websocket_log_buffer", None)
                    if buffer is not None:
                        buffer.append(rendered_ws)
                    continue
                rendered = _render_entry(entry)
                backend_log.push(rendered)
                buffer = getattr(app.state, "backend_log_buffer", None)
                if buffer is not None:
                    buffer.append(rendered)
            backend_seen["idx"] = len(events)

        def push_frontend() -> None:
            events = list(getattr(app.state, "frontend_events", []))
            new_events = events[frontend_seen["idx"] :]
            for entry in new_events:
                rendered = _render_entry(entry)
                frontend_log.push(rendered)
                buffer = getattr(app.state, "frontend_log_buffer", None)
                if buffer is not None:
                    buffer.append(rendered)
            frontend_seen["idx"] = len(events)

        def push_websocket() -> None:
            events = list(getattr(app.state, "websocket_events", []))
            new_events = events[websocket_seen["idx"] :]
            for entry in new_events:
                rendered = _render_entry(entry)
                websocket_log.push(rendered)
                buffer = getattr(app.state, "websocket_log_buffer", None)
                if buffer is not None:
                    buffer.append(rendered)
            websocket_seen["idx"] = len(events)

        ui.timer(3, push_backend)
        ui.timer(3, push_frontend)
        ui.timer(3, push_websocket)
        push_backend()
        push_frontend()
        push_websocket()

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
        response_schemas = config.setdefault("llm_response_schemas", {})
        guardrails = config.setdefault("guardrails", PromptBuilder._default_guardrails())
        guardrails.setdefault(
            "snapshot_max_age_seconds", config.get("snapshot_max_age_seconds")
        )
        guardrails.setdefault("symbol_position_caps", {})
        guardrails.setdefault("min_leverage_confidence_gate", 0.5)
        guardrails.setdefault("isolated_margin_seed_usd", None)
        guardrails.setdefault("isolated_margin_max_transfer_usd", None)
        guardrails.setdefault("isolated_margin_symbol_seeds_usd", {})
        if "wait_for_tp_sl" not in config:
            config["wait_for_tp_sl"] = bool(guardrails.get("wait_for_tp_sl", False))
        guardrails.setdefault("wait_for_tp_sl", bool(config.get("wait_for_tp_sl")))
        guardrails.setdefault(
            "fallback_orders_enabled",
            bool(config.get("fallback_orders_enabled", settings.allow_fallback_orders)),
        )
        config["fallback_orders_enabled"] = bool(guardrails.get("fallback_orders_enabled", True))
        config.setdefault("prompt_version_name", None)
        prompt_versions_cache: dict[str, dict[str, Any]] = {}
        prompt_version_options: dict[str, str] = {}
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

        timeframe_default = config.get("ta_timeframe") or "4H"
        if timeframe_default not in TA_TIMEFRAME_OPTIONS:
            timeframe_default = "4H"
        with wrapper:
            ui.label("Engine Configuration").classes("text-2xl font-bold")
            ui.label("Execution Guardrails").classes("text-xl font-semibold")
            ui.label("Limits enforced before orders are placed").classes("text-sm text-slate-500")
            max_position_pct_value = _fraction_to_percent(guardrails.get("max_position_pct"))
            if max_position_pct_value is None:
                max_position_pct_value = 20.0
            daily_loss_limit_value = _fraction_to_percent(guardrails.get("daily_loss_limit_pct"))
            if daily_loss_limit_value is None:
                daily_loss_limit_value = 3.0
            with ui.row().classes("w-full flex-wrap gap-4"):
                max_leverage_input = ui.number(
                    label="Max Leverage",
                    value=guardrails.get("max_leverage", 5),
                    min=1,
                ).classes("w-full md:w-48").props(
                    "hint='Hard cap on leverage multiples for new positions' persistent-hint"
                )
                min_leverage_input = ui.number(
                    label="Min Leverage",
                    value=guardrails.get("min_leverage", 1),
                    min=0,
                    step=0.1,
                ).classes("w-full md:w-48").props(
                    "hint='Confidence-scaling floor applied before execution' persistent-hint"
                )
                min_leverage_conf_gate_input = ui.number(
                    label="Min Leverage Confidence Gate",
                    value=guardrails.get("min_leverage_confidence_gate", 0.5),
                    min=0,
                    max=1,
                    step=0.05,
                ).classes("w-full md:w-48").props(
                    "hint='LLM confidence required before execution upsizes small orders' persistent-hint"
                )
                max_position_pct_input = ui.number(
                    label="Max Position % of Equity",
                    value=max_position_pct_value,
                    step=0.1,
                    min=0.1,
                    max=100,
                ).classes("w-full md:w-48").props(
                    "hint='Percent of equity allowed per symbol (e.g., 15 = 15%)' persistent-hint"
                )
                daily_loss_limit_input = ui.number(
                    label="Daily Loss Limit %",
                    value=daily_loss_limit_value,
                    step=0.1,
                    min=0.1,
                    max=100,
                ).classes("w-full md:w-48").props(
                    "hint='Soft kill switch when daily drawdown breaches this percent (enter 3 for 3%)' persistent-hint"
                )
            with ui.row().classes("w-full flex-wrap gap-4"):
                min_hold_seconds_input = ui.number(
                    label="Min Hold / Cooldown (sec)",
                    value=guardrails.get("min_hold_seconds", 180),
                    min=0,
                ).classes("w-full md:w-48").props(
                    "hint='Minimum time to wait before allowing another trade on the same symbol' persistent-hint"
                )
                max_trades_per_hour_input = ui.number(
                    label="Max Trades Per Hour",
                    value=guardrails.get("max_trades_per_hour", 2),
                    min=0,
                ).classes("w-full md:w-48").props(
                    "hint='Prevents over-trading by capping per-symbol order count in any rolling hour' persistent-hint"
                )
                trade_window_seconds_input = ui.number(
                    label="Trade Window (sec)",
                    value=guardrails.get("trade_window_seconds", 3600),
                    min=60,
                    step=60,
                ).classes("w-full md:w-48").props(
                    "hint='Window used for trade limit and activity metrics' persistent-hint"
                )
            require_alignment_switch = ui.switch(
                "Require Position Alignment",
                value=guardrails.get("require_position_alignment", True),
            ).classes("mt-2").props(
                "hint='Blocks conflicting orders unless an opposite signal closes the position' persistent-hint"
            )
            wait_for_tp_sl_switch = ui.switch(
                "Wait for TP/SL to Hit",
                value=guardrails.get("wait_for_tp_sl", False),
            ).classes("mt-2").props(
                "hint='When enabled, opposing signals are ignored until the current position\'s TP or SL executes' persistent-hint"
            )
            fallback_orders_switch = ui.switch(
                "Allow Fallback Orders",
                value=config.get("fallback_orders_enabled", settings.allow_fallback_orders),
            ).classes("mt-2").props(
                "hint='Permit heuristic backup trades when LLM calls fail; disable to ignore fallback orders entirely' persistent-hint"
            )
            snapshot_max_age_input = ui.number(
                label="Snapshot Max Age (sec)",
                value=config.get(
                    "snapshot_max_age_seconds",
                    settings.snapshot_max_age_seconds,
                ),
                min=60,
            ).classes("w-full md:w-48").props(
                "hint='Blocks LLM prompts whenever Redis snapshot is older than this' persistent-hint"
            )
            execution_feedback_ttl_input = ui.number(
                label="Execution Feedback TTL (sec)",
                value=guardrails.get("execution_feedback_ttl_seconds", 600),
                min=0,
            ).classes("w-full md:w-48").props(
                "hint='How long warnings/errors stay in prompts before auto-expiring; set 0 to disable' persistent-hint"
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
            ui.separator().classes("w-full my-4")
            ui.label("Model, cadence, and prompt controls").classes("text-sm text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                ws_interval_input = ui.number(
                    label="WS Update Interval (seconds)",
                    value=config.get("ws_update_interval", 180),
                    min=1,
                ).classes("w-full md:w-48")
                websocket_switch = ui.switch(
                    "Live Websocket Stream",
                    value=config.get("enable_websocket", True),
                ).classes("w-full md:w-48").props(
                    "hint='Disabling falls back to REST polling every interval' persistent-hint"
                )
                fee_window_input = ui.number(
                    label="Fee Window (hours)",
                    value=config.get("fee_window_hours", 24.0),
                    min=1,
                    step=1,
                ).classes("w-full md:w-48").props(
                    "hint='Rolling hours of OKX fees shown on LIVE' persistent-hint"
                )
                auto_prompt_switch = ui.switch(
                    "Auto Prompt Scheduler",
                    value=config.get("auto_prompt_enabled", False),
                ).classes("w-full md:w-48")
                auto_prompt_interval_input = ui.number(
                    label="Prompt Interval (seconds)",
                    value=config.get("auto_prompt_interval", 300),
                    min=30,
                ).classes("w-full md:w-48")
                ta_timeframe_select_cfg = ui.select(
                    options=TA_TIMEFRAME_OPTIONS,
                    label="Analysis Timeframe",
                    value=timeframe_default,
                ).classes("w-full md:w-40")
                timezone_select = (
                    ui.select(
                        options=TIMEZONE_OPTIONS,
                        label="Display Timezone",
                        value=config.get("frontend_timezone", DEFAULT_FRONTEND_TIMEZONE),
                        with_input=True,
                    )
                    .classes("w-full md:w-60")
                    .props("use-input fill-input input-debounce='0' clearable")
                )
                model_select = ui.select(
                    model_options,
                    label="Model",
                    value=initial_model_value,
                ).classes("w-full md:w-64")
                model_cost_label = ui.label("Pricing unavailable").classes(
                    "text-xs text-slate-500"
                )
                raw_pairs = config.get("trading_pairs", ["BTC-USDT-SWAP"])
                selected_trading_pairs: list[str] = []
                for symbol in raw_pairs or []:
                    normalized = str(symbol).strip().upper()
                    if not normalized or normalized in selected_trading_pairs:
                        continue
                    selected_trading_pairs.append(normalized)
                if not selected_trading_pairs:
                    selected_trading_pairs.append("BTC-USDT-SWAP")
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
                if not selected_trading_pairs:
                    selected_trading_pairs.append("BTC-USDT-SWAP")
                config["trading_pairs"] = selected_trading_pairs.copy()
                with trading_pairs_list:
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
                okx_sub_account_input = ui.input(
                    label="OKX Sub-Account",
                    value=config.get("okx_sub_account") or "",
                    placeholder="Leave blank for primary",
                ).classes("w-full md:w-64").props(
                    "hint='Orders + balances will target this sub-account' persistent-hint"
                )
                okx_master_routing_switch = ui.switch(
                    "API key created on parent account",
                    value=config.get("okx_sub_account_use_master", False),
                ).classes("w-full md:w-64").props(
                    "hint='Enable when using parent-account API keys that need the subAcct flag to reach this sub-account.' persistent-hint"
                )
                okx_env_select = ui.select(
                    {
                        "0": "Live (Production)",
                        "1": "Paper / Demo",
                    },
                    label="OKX Environment",
                    value=str(config.get("okx_api_flag", "0") or "0"),
                ).classes("w-full md:w-64").props(
                    "hint='Flag=0 targets live trading; Flag=1 targets OKX simulated trading endpoints.' persistent-hint"
                )
            ui.label(
                "Orders are sent as market orders on OKX. Enable only on funded accounts."
            ).classes("text-xs text-rose-600")
            ui.label("Choose all perpetual instruments to monitor").classes(
                "text-sm text-slate-500"
            )
            prompt_input = ui.textarea(
                label="System Prompt",
                value=config.get("llm_system_prompt", DEFAULT_SYSTEM_PROMPT),
            ).classes("w-full h-48")
            decision_prompt_input = ui.textarea(
                label="Decision Prompt",
                value=config.get("llm_decision_prompt", DEFAULT_DECISION_PROMPT),
            ).classes("w-full h-48")
            with ui.row().classes("w-full flex-wrap gap-4 items-end"):
                prompt_version_select = ui.select(
                    options=[],
                    label="Prompt Version",
                    with_input=False,
                ).classes("w-full md:w-64")
                prompt_version_select.disable()
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
                value=schema_to_text(initial_model_value),
                placeholder="Leave blank to use default schema",
            ).classes("w-full h-40 font-mono text-sm")
            with ui.column().classes(
                "w-full gap-2 mt-4 bg-slate-50/80 p-4 rounded-xl border border-slate-200"
            ):
                ui.label("LLM Payload Preview").classes("text-lg font-semibold")
                ui.label(
                    "Exact prompt + context skeleton sent to the LLM"
                ).classes("text-xs text-slate-500")
                payload_preview = (
                    ui.textarea(label="Prompt Payload", value="")
                    .props("readonly outlined autogrow")
                    .classes("w-full font-mono text-xs bg-white h-full")
                    .style("min-height: 22rem; height: 100%;")
                )
            save_button = ui.button("Save", icon="save", color="primary")

        if not auto_prompt_switch.value:
            auto_prompt_interval_input.disable()
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

        def on_auto_prompt_toggle(e: Any) -> None:
            if e.value:
                auto_prompt_interval_input.enable()
            else:
                auto_prompt_interval_input.disable()

        auto_prompt_switch.on_value_change(on_auto_prompt_toggle)

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
                "trade_window_seconds": _safe_int(trade_window_seconds_input.value),
                "risk_model": guardrails.get("risk_model", "ATR based stops x1.5"),
                "require_position_alignment": bool(require_alignment_switch.value),
                "wait_for_tp_sl": bool(wait_for_tp_sl_switch.value),
                "fallback_orders_enabled": bool(fallback_orders_switch.value),
                "snapshot_max_age_seconds": _safe_int(snapshot_max_age_input.value)
                or config.get("snapshot_max_age_seconds"),
            }
            symbol_caps_preview = _clean_symbol_caps()
            snapshot["symbol_position_caps"] = symbol_caps_preview or None
            snapshot["isolated_margin_seed_usd"] = _safe_float(isolated_seed_default_input.value)
            snapshot["isolated_margin_max_transfer_usd"] = _safe_float(isolated_seed_max_input.value)
            seed_overrides_preview = _clean_isolated_seed_overrides()
            snapshot["isolated_margin_symbol_seeds_usd"] = seed_overrides_preview or None
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

        def build_context_structure() -> dict[str, Any]:
            return {
                "generated_at": "<ISO8601 timestamp from latest snapshot>",
                "symbol": "<primary trading symbol>",
                "timeframe": config.get("ta_timeframe") or "4H",
                "market": {
                    "last_price": "<float>",
                    "bid": "<float>",
                    "ask": "<float>",
                    "spread": "<float>",
                    "spread_pct": "<float>",
                    "change_24h": "<float>",
                    "volume_24h": "<float>",
                    "funding_rate": "<float>",
                    "next_funding": "<timestamp>",
                    "open_interest": {
                        "contracts": "<float>",
                        "usd": "<float>",
                    },
                    "order_flow": {
                        "imbalance": "<float>",
                        "cvd": "<float>",
                        "bid_depth": "<float>",
                        "ask_depth": "<float>",
                        "cvd_series": "<list>",
                        "ofi_ratio_series": "<list>",
                    },
                },
                "history": {
                    "candles": "[[ts, open, high, low, close, volume] ... up to 120 rows]",
                    "vwap_series": "<list>",
                    "volume_series": "<list>",
                    "volume_rsi_series": "<list>",
                },
                "indicators": {
                    "rsi": "<dict>",
                    "stoch_rsi": "<dict>",
                    "macd": {
                        "value": "<float>",
                        "signal": "<float>",
                        "hist": "<float>",
                        "series": "<list>",
                    },
                    "bollinger_bands": "<dict>",
                    "moving_averages": "<dict>",
                    "adx": {
                        "value": "<float>",
                        "di_plus": "<float>",
                        "di_minus": "<float>",
                        "series": "<list>",
                    },
                    "obv": "<dict>",
                    "cmf": "<dict>",
                    "vwap": "<dict>",
                    "atr": "<float>",
                    "atr_pct": "<float>",
                    "volume": "<dict>",
                },
                "strategy_signal": "<custom engine signal block>",
                "risk_metrics": "<dict of real-time risk metrics>",
                "positions": [
                    {
                        "symbol": "<position symbol>",
                        "side": "LONG/SHORT",
                        "size": "<float>",
                        "avg_px": "<float>",
                        "leverage": "<float>",
                        "margin_mode": "<cross/isolated>",
                    }
                ],
                "account": {
                    "account_equity": "<float>",
                    "total_account_value": "<float>",
                    "total_eq_usd": "<float>",
                },
                "guardrails": build_guardrails_snapshot(),
                "execution": {
                    "enabled": config.get("execution_enabled", False),
                    "trade_mode": config.get("execution_trade_mode", "cross"),
                    "order_type": config.get("execution_order_type", "market"),
                    "min_size": config.get("execution_min_size", 1.0),
                    "min_sizes": config.get("execution_min_sizes", {}),
                },
                "notes": config.get("llm_notes") or "<optional runtime notes>",
                "prompt_version_id": config.get("prompt_version_id"),
                "prompt_version_name": config.get("prompt_version_name"),
            }

        def build_payload_preview() -> str:
            schema_text = response_schema_input.value or ""
            if schema_text.strip():
                try:
                    schema_value: Any = json.loads(schema_text)
                except json.JSONDecodeError as exc:
                    schema_value = f"<invalid JSON: {exc.msg} (line {exc.lineno}, col {exc.colno})>"
            else:
                schema_value = RESPONSE_SCHEMA
            payload = {
                "prompt": {
                    "system": (prompt_input.value or "").strip(),
                    "task": (decision_prompt_input.value or "").strip(),
                    "model": model_select.value,
                    "response_schema": schema_value,
                },
                "context": build_context_structure(),
            }
            return json.dumps(payload, indent=2)

        def update_payload_preview() -> None:
            payload_preview.value = build_payload_preview()
            payload_preview.update()

        def register_preview_listeners() -> None:
            listeners = [
                prompt_input,
                decision_prompt_input,
                response_schema_input,
                max_leverage_input,
                min_leverage_input,
                max_position_pct_input,
                daily_loss_limit_input,
                min_hold_seconds_input,
                max_trades_per_hour_input,
                trade_window_seconds_input,
                require_alignment_switch,
                wait_for_tp_sl_switch,
                fallback_orders_switch,
                isolated_seed_default_input,
                isolated_seed_max_input,
            ]
            for widget in listeners:
                widget.on_value_change(lambda _: update_payload_preview())

        register_preview_listeners()
        update_payload_preview()
        update_model_cost_label(initial_model_value)
        asyncio.create_task(hydrate_execution_settings())

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
            response_schema_input.value = schema_to_text(model_id)
            response_schema_input.update()
            update_payload_preview()
            update_model_cost_label(model_id)

        def on_model_change(e: Any) -> None:
            apply_model_change(getattr(e, "value", None))

        model_select.on_value_change(on_model_change)

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
            prompt_version_select.options = options
            if options:
                prompt_version_select.enable()
            else:
                prompt_version_select.disable()
            _set_prompt_version_value(config.get("prompt_version_id"))

        def apply_prompt_version(version_id: str | None) -> None:
            record = prompt_versions_cache.get(version_id or "")
            if not record:
                return
            prompt_input.value = record.get("system_prompt", prompt_input.value)
            decision_prompt_input.value = record.get(
                "decision_prompt", decision_prompt_input.value
            )
            prompt_version_name_input.value = record.get("name", "")
            prompt_input.update()
            decision_prompt_input.update()
            prompt_version_name_input.update()
            config["llm_system_prompt"] = prompt_input.value
            config["llm_decision_prompt"] = decision_prompt_input.value
            config["prompt_version_id"] = record.get("id")
            config["prompt_version_name"] = record.get("name")
            update_prompt_version_param(record.get("id"))
            update_payload_preview()

        def on_prompt_version_change(e: Any) -> None:
            label = e.value
            version_id = prompt_version_options.get(label)
            if not version_id:
                return
            apply_prompt_version(version_id)

        prompt_version_select.on_value_change(on_prompt_version_change)

        async def save_settings(event: Any | None = None) -> None:
            config["ws_update_interval"] = int(ws_interval_input.value or 5)
            config["enable_websocket"] = bool(websocket_switch.value)
            config["auto_prompt_enabled"] = bool(auto_prompt_switch.value)
            config["execution_enabled"] = bool(execution_switch.value)
            config["wait_for_tp_sl"] = bool(wait_for_tp_sl_switch.value)
            config["fallback_orders_enabled"] = bool(fallback_orders_switch.value)
            config["llm_system_prompt"] = prompt_input.value
            config["llm_decision_prompt"] = decision_prompt_input.value
            config["llm_model_id"] = model_select.value
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
                await save_frontend_timezone(timezone_value)
            except Exception as exc:  # pragma: no cover - optional DB
                ui.notify(f"Failed to persist timezone: {exc}", color="warning")
            schema_text = response_schema_input.value or ""
            if schema_text.strip():
                try:
                    response_schemas[model_select.value] = json.loads(schema_text)
                except json.JSONDecodeError as exc:
                    ui.notify(f"Response schema invalid JSON: {exc}", color="warning")
                    return
            else:
                response_schemas.pop(model_select.value, None)
            config["llm_response_schemas"] = response_schemas
            try:
                await save_llm_model(config["llm_model_id"])
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Failed to persist default model: {exc}", color="warning")
            try:
                await save_ta_timeframe(config["ta_timeframe"])
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Failed to persist timeframe: {exc}", color="warning")

            def _coerce(value: Any, fallback: Any, caster: Any) -> Any:
                try:
                    if value is None:
                        raise ValueError
                    return caster(value)
                except (TypeError, ValueError):
                    return caster(fallback)

            config["auto_prompt_interval"] = max(
                30,
                _coerce(
                    auto_prompt_interval_input.value,
                    config.get("auto_prompt_interval", 300),
                    int,
                ),
            )
            try:
                await save_prompt_interval(config["auto_prompt_interval"])
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Failed to persist prompt interval: {exc}", color="warning")
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
                ui.notify(f"Failed to persist execution settings: {exc}", color="warning")
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
                ui.notify(f"Failed to persist OKX sub-account: {exc}", color="warning")

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
                "trade_window_seconds": _coerce(
                    trade_window_seconds_input.value,
                    guardrails.get("trade_window_seconds", 3600),
                    int,
                ),
                "risk_model": guardrails.get("risk_model", "ATR based stops x1.5"),
                "require_position_alignment": bool(require_alignment_switch.value),
                "wait_for_tp_sl": bool(wait_for_tp_sl_switch.value),
                "fallback_orders_enabled": bool(fallback_orders_switch.value),
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
            version_name = (prompt_version_name_input.value or "").strip()
            created_version_id: str | None = None
            selected_label = prompt_version_select.value
            selected_version_id = (
                prompt_version_options.get(selected_label)
                if selected_label
                else config.get("prompt_version_id")
            )
            if version_name:
                metadata = {
                    "guardrails": config.get("guardrails"),
                    "model_id": model_select.value,
                }
                try:
                    created_version_id = await insert_prompt_version(
                        name=version_name,
                        system_prompt=config["llm_system_prompt"],
                        decision_prompt=config["llm_decision_prompt"],
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
            symbols: list[str] = []
            for item in selected_trading_pairs:
                normalized = str(item).strip().upper()
                if not normalized or normalized in symbols:
                    continue
                symbols.append(normalized)
            if not symbols:
                symbols = ["BTC-USDT-SWAP"]
            selected_trading_pairs[:] = symbols
            config["trading_pairs"] = symbols
            try:
                await set_enabled_trading_pairs(symbols)
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Failed to persist trading pairs: {exc}", color="warning")
            try:
                await save_guardrails(config["guardrails"])
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Failed to persist guardrails: {exc}", color="warning")
            app.state.runtime_config = config
            llm_service = getattr(app.state, "llm_service", None)
            if llm_service:
                llm_service.set_model(model_select.value)
            market_service = getattr(app.state, "market_service", None)
            if market_service:
                market_service.set_wait_for_tp_sl(config.get("wait_for_tp_sl", False))
                await market_service.set_okx_flag(config.get("okx_api_flag"))
                await market_service.set_sub_account(
                    config.get("okx_sub_account"),
                    config.get("okx_sub_account_use_master"),
                )
                await market_service.set_ohlc_bar(config["ta_timeframe"])
                market_service.set_poll_interval(config["ws_update_interval"])
                await market_service.set_websocket_enabled(config.get("enable_websocket", True))
                await market_service.update_symbols(symbols)
            scheduler = getattr(app.state, "prompt_scheduler", None)
            if scheduler:
                await scheduler.update_interval(config["auto_prompt_interval"])
                await scheduler.set_enabled(config["auto_prompt_enabled"])
            _set_prompt_version_value(config.get("prompt_version_id"))
            ui.notify("Configuration saved", color="positive")
            app.state.frontend_events.append("CFG updated")

        save_button.on("click", save_settings)
        asyncio.create_task(load_trading_pairs())
        asyncio.create_task(hydrate_model_select())
        asyncio.create_task(load_prompt_versions_list())

    @ui.page("/")
    def home() -> None:
        render_live_page()

    @ui.page("/live")
    def live() -> None:
        render_live_page()

    @ui.page("/ta")
    def ta() -> None:
        render_ta_page()

    @ui.page("/engine")
    def engine() -> None:
        render_engine_page()

    @ui.page("/history")
    def history() -> None:
        render_history_page()

    @ui.page("/debug")
    def debug() -> None:
        render_debug_page()

    @ui.page("/cfg")
    def cfg() -> None:
        render_cfg_page()


__all__ = ["register_pages"]
