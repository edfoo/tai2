from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from nicegui import ui

from app.db.postgres import (
    fetch_equity_history,
    fetch_prompt_versions,
    fetch_prompt_runs,
    fetch_recent_trades,
    insert_prompt_version,
    save_guardrails,
    set_enabled_trading_pairs,
)
from app.services.prompt_builder import (
    DEFAULT_DECISION_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    PromptBuilder,
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

def register_pages(app: FastAPI) -> None:
    def get_refresh_interval() -> float:
        config = getattr(app.state, "runtime_config", {}) or {}
        interval = config.get("ws_update_interval", 10)
        return max(3.0, float(interval) / 2.0)

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
                ui.label(datetime.utcnow().strftime("%H:%M UTC")).classes(
                    "text-xs text-white/70"
                )
        return nav_refs

    def render_live_page() -> None:
        navigation("LIVE")
        wrapper = page_container()
        store = make_snapshot_store()

        current_symbol = {"value": None}
        last_snapshot = {"value": None}
        equity_refresh = {"last": 0.0}
        refresh_label: dict[str, ui.label | None] = {"widget": None}
        status_label: dict[str, ui.label | None] = {"widget": None}

        def set_ws_status(active: bool) -> None:
            label = status_label["widget"]
            if not label:
                return
            label.set_text("WS: LIVE" if active else "WS: IDLE")

        with wrapper:
            header_row = ui.row().classes("w-full justify-between items-start flex-wrap gap-4")
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

            symbol_select = ui.select(options=[], label="Symbol").classes("w-full md:w-64")
            symbol_select.disable()
            with ui.row().classes("w-full gap-4"):
                balance_card = badge_stat("Account Equity", "--")
                position_card = badge_stat("Active Positions", "--", color="accent")
                ticker_card = badge_stat("Last Price", "--", color="warning")

            equity_chart = ui.echart(
                {
                    "tooltip": {"trigger": "axis"},
                    "grid": {"left": 40, "right": 20, "top": 20, "bottom": 30},
                    "xAxis": {"type": "category", "data": [], "axisLabel": {"color": "#475569"}},
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
                    {"name": "size", "label": "Size", "field": "size"},
                    {"name": "entry", "label": "Entry", "field": "entry"},
                    {"name": "current", "label": "Current", "field": "current"},
                    {"name": "pnl", "label": "PNL", "field": "pnl"},
                    {"name": "pnl_pct", "label": "PNL %", "field": "pnl_pct"},
                    {"name": "leverage", "label": "Leverage", "field": "leverage"},
                ],
                rows=[],
            ).classes("w-full font-semibold")

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

            ticker_details = ui.expansion("Ticker & Funding").classes("w-full")
            with ticker_details:
                ticker_label = ui.label("--")
                funding_label = ui.label("--")

        async def refresh_equity_chart() -> None:
            try:
                history = await fetch_equity_history(limit=200)
            except Exception:
                return
            if not history:
                return
            labels: list[str] = []
            values: list[float] = []
            for entry in history:
                ts = entry.get("observed_at")
                if ts:
                    try:
                        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        labels.append(parsed.strftime("%H:%M"))
                    except ValueError:
                        labels.append(ts)
                else:
                    labels.append("--")
                value = entry.get("total_eq_usd") or entry.get("account_equity")
                values.append(round(float(value), 2) if value is not None else None)
            option = equity_chart.options
            option["xAxis"]["data"] = labels
            option["series"][0]["data"] = values
            equity_chart.update()

        def update(snapshot: dict[str, Any] | None) -> None:
            last_snapshot["value"] = snapshot
            set_ws_status(snapshot is not None)
            label = refresh_label["widget"]
            if label:
                if snapshot:
                    label.set_text(f"Last refresh: {datetime.utcnow().strftime('%H:%M:%S UTC')}")
                else:
                    label.set_text("Last refresh: --")
            if not snapshot:
                return
            positions = snapshot.get("positions") or []
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
            ticker = market_entry.get("ticker") or snapshot.get("ticker") or {}
            funding = market_entry.get("funding_rate") or snapshot.get("funding_rate") or {}
            equity_value = snapshot.get("total_eq_usd") or snapshot.get("account_equity")
            try:
                total_equity = float(equity_value or 0)
            except (TypeError, ValueError):
                total_equity = 0.0
            balance_card.value_label.set_text(f"${total_equity:,.2f}")
            position_card.value_label.set_text(str(len(positions)))
            ticker_value = ticker.get("last") or ticker.get("px")
            ticker_card.value_label.set_text(
                f"${float(ticker_value):,.2f}" if ticker_value else "--"
            )
            position_lookup: dict[str, dict[str, Any]] = {}
            for pos in positions:
                key = pos.get("instId") or pos.get("symbol")
                if not key or key in position_lookup:
                    continue
                position_lookup[key] = pos

            def to_float(value: Any) -> float | None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            rows: list[dict[str, Any]] = []
            for symbol, pos in position_lookup.items():
                ticker_info = (market_data.get(symbol) or {}).get("ticker") or {}
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
                current_price = to_float(ticker_info.get("last") or ticker_info.get("px"))
                leverage_raw = pos.get("lever") or pos.get("leverage")
                leverage_display = str(leverage_raw) if leverage_raw not in (None, "") else "--"
                leverage_value = to_float(leverage_raw)

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

                row = {
                    "symbol": symbol,
                    "side": side if side != "" else "--",
                    "size": f"{size_abs:,.4f}" if size_abs is not None else "--",
                    "entry": f"{entry_price:,.2f}" if entry_price is not None else "--",
                    "current": f"{current_price:,.2f}" if current_price is not None else "--",
                    "pnl": f"{pnl:,.2f}" if pnl is not None else "--",
                    "pnl_cls": pnl_color,
                    "pnl_pct": f"{pnl_pct:,.2f}%" if pnl_pct is not None else "--",
                    "pnl_pct_cls": pnl_pct_color,
                    "leverage": leverage_display,
                }
                rows.append(row)

            positions_table.rows = rows
            positions_table.update()
            ticker_label.set_text(
                f"Mark: {ticker.get('last', '--')} / 24h Vol: {ticker.get('volCcy24h', '--')}"
            )
            funding_label.set_text(f"Funding Rate: {funding.get('fundingRate', '--')}")

            now = time.monotonic()
            if now - equity_refresh["last"] > 30:
                equity_refresh["last"] = now
                asyncio.create_task(refresh_equity_chart())

        store.subscribe(update)
        asyncio.create_task(refresh_equity_chart())

        def on_symbol_change(e: Any) -> None:
            current_symbol["value"] = e.value
            update(last_snapshot["value"])

        symbol_select.on_value_change(on_symbol_change)

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
                timestamp = datetime.utcnow().strftime("%H:%M:%S")
                strategy_feed.push(
                    f"{timestamp} | {selected_symbol} | {action_text} ({conf_display.split(': ')[1]})"
                )
                last_logged_signal["value"] = summary

            ohlcv = indicators.get("ohlcv") or []
            categories = [
                datetime.fromtimestamp(entry.get("ts", 0) / 1000).strftime("%H:%M") for entry in ohlcv
            ]
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

        store.subscribe(update)

        def log_trade_event(kind: str) -> None:
            signal = current_signal.get("value") or {}
            symbol = current_signal.get("symbol") or current_symbol.get("value")
            action = signal.get("action")
            if not action or action == "--":
                ui.notify("No strategy signal available yet", color="warning")
                return
            confidence = signal.get("confidence")
            confidence_pct = f"{confidence * 100:.0f}%" if isinstance(confidence, (int, float)) else "--"
            timestamp = datetime.utcnow().strftime("%H:%M:%S")
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
                ui.chat_message("user", text=(question_input.value or "")).classes(
                    "self-end bg-slate-200 text-slate-900"
                )
                ui.chat_message("engine", text=response).classes("bg-emerald-50 text-emerald-900")
            app.state.frontend_events.append(
                f"User Q at {datetime.utcnow().isoformat()}"
            )

        send_button.on("click", lambda _: asyncio.create_task(handle_question()))
        store.subscribe(update)

        def on_symbol_change(e: Any) -> None:
            current_symbol["value"] = e.value
            update(last_snapshot["value"])

        symbol_select.on_value_change(on_symbol_change)

    def render_history_page() -> None:
        navigation("HISTORY")
        wrapper = page_container()
        wrapper.style("max-width: 100%; width: 100%; margin-left: 0; margin-right: 0;")

        with wrapper:
            ui.label("History").classes("text-2xl font-bold")
            with ui.row().classes("w-full gap-6 flex-col xl:flex-row items-stretch"):
                with ui.card().classes("w-full flex-1 p-4 gap-3"):
                    ui.label("Executed Trades").classes("text-lg font-semibold")
                    trades_table = ui.table(
                        columns=[
                            {"name": "timestamp", "label": "Timestamp", "field": "timestamp"},
                            {"name": "symbol", "label": "Symbol", "field": "symbol"},
                            {"name": "side", "label": "Side", "field": "side"},
                            {"name": "price", "label": "Price", "field": "price"},
                            {"name": "amount", "label": "Amount", "field": "amount"},
                        ],
                        rows=[],
                    ).classes("w-full")
                    reload_button = ui.button("Reload Trades", icon="refresh").classes("self-end")
                with ui.card().classes("w-full flex-1 p-4 gap-3"):
                    ui.label("Prompt Runs").classes("text-lg font-semibold")
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
                    prompt_reload_button = ui.button("Reload Prompts", icon="refresh").classes("self-end")

        async def refresh_trades() -> None:
            try:
                rows = await fetch_recent_trades(200)
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Unable to load trades: {exc}", color="warning")
                return
            trades_table.rows = rows
            trades_table.update()

        async def refresh_prompts() -> None:
            try:
                rows = await fetch_prompt_runs(200)
            except Exception as exc:  # pragma: no cover - db optional
                ui.notify(f"Unable to load prompts: {exc}", color="warning")
                return
            formatted: list[dict[str, Any]] = []
            for entry in rows:
                decision = entry.get("decision") or {}
                version_label = entry.get("prompt_version_name") or entry.get("prompt_version_id")
                if version_label:
                    version_label = str(version_label)
                formatted.append(
                    {
                        "created_at": entry.get("created_at"),
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

        reload_button.on("click", lambda _: asyncio.create_task(refresh_trades()))
        prompt_reload_button.on("click", lambda _: asyncio.create_task(refresh_prompts()))
        asyncio.create_task(refresh_trades())
        asyncio.create_task(refresh_prompts())

    def render_debug_page() -> None:
        navigation("DEBUG")
        wrapper = page_container()
        config = getattr(app.state, "runtime_config", {}) or {}
        active_version = config.get("prompt_version_name") or config.get("prompt_version_id") or "default"
        with wrapper:
            ui.label(f"Active Prompt Version: {active_version}").classes(
                "text-sm font-semibold text-slate-600"
            )
            backend_log = ui.log(max_lines=200).classes("w-full h-72")
            frontend_log = ui.log(max_lines=200).classes("w-full h-72")
            ui.button(
                "Emit Frontend Event",
                on_click=lambda: app.state.frontend_events.append("Frontend heartbeat triggered"),
            )

        backend_seen = {"idx": 0}
        frontend_seen = {"idx": 0}

        def push_backend() -> None:
            events = list(getattr(app.state, "backend_events", []))
            new_events = events[backend_seen["idx"] :]
            for entry in new_events:
                backend_log.push(entry)
            backend_seen["idx"] = len(events)

        def push_frontend() -> None:
            events = list(getattr(app.state, "frontend_events", []))
            new_events = events[frontend_seen["idx"] :]
            for entry in new_events:
                frontend_log.push(entry)
            frontend_seen["idx"] = len(events)

        ui.timer(3, push_backend)
        ui.timer(3, push_frontend)

        # button defined within wrapper

    def render_cfg_page() -> None:
        navigation("CFG")
        wrapper = page_container()
        config = getattr(app.state, "runtime_config", {})
        response_schemas = config.setdefault("llm_response_schemas", {})
        guardrails = config.setdefault("guardrails", PromptBuilder._default_guardrails())
        config.setdefault("prompt_version_name", None)
        prompt_versions_cache: dict[str, dict[str, Any]] = {}
        prompt_version_options: dict[str, str] = {}

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
        with wrapper:
            ui.label("Engine Configuration").classes("text-2xl font-bold")
            ui.label("Model, cadence, and prompt controls").classes("text-sm text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                ws_interval_input = ui.number(
                    label="WS Update Interval (seconds)",
                    value=config.get("ws_update_interval", 180),
                    min=1,
                ).classes("w-full md:w-48")
                model_select = ui.select(
                    {
                        "openrouter/gpt-4o-mini": "GPT-4o mini",
                        "openrouter/claude-3.5": "Claude 3.5",
                        "openrouter/gpt-4o": "GPT-4o",
                    },
                    label="Model",
                    value=config.get("llm_model_id", "openrouter/gpt-4o-mini"),
                ).classes("w-full md:w-64")
                trading_pairs_select = ui.select(
                    options=[],
                    value=config.get("trading_pairs", ["BTC-USDT-SWAP"]),
                    multiple=True,
                    label="Trading Pairs",
                ).classes("w-full flex-1")
                trading_pairs_select.disable()
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
                value=schema_to_text(config.get("llm_model_id", "openrouter/gpt-4o-mini")),
                placeholder="Leave blank to use default schema",
            ).classes("w-full h-40 font-mono text-sm")
            ui.separator().classes("w-full my-4")
            ui.label("Execution Guardrails").classes("text-xl font-semibold")
            ui.label("Limits enforced before orders are placed").classes("text-sm text-slate-500")
            with ui.row().classes("w-full flex-wrap gap-4"):
                max_leverage_input = ui.number(
                    label="Max Leverage",
                    value=guardrails.get("max_leverage", 5),
                    min=1,
                ).classes("w-full md:w-48")
                max_position_pct_input = ui.number(
                    label="Max Position % of Equity",
                    value=guardrails.get("max_position_pct", 0.2),
                    step=0.01,
                    min=0.01,
                ).classes("w-full md:w-48")
                daily_loss_limit_input = ui.number(
                    label="Daily Loss Limit %",
                    value=guardrails.get("daily_loss_limit_pct", 3),
                    step=0.1,
                    min=0.1,
                ).classes("w-full md:w-48")
            with ui.row().classes("w-full flex-wrap gap-4"):
                min_hold_seconds_input = ui.number(
                    label="Min Hold / Cooldown (sec)",
                    value=guardrails.get("min_hold_seconds", 180),
                    min=0,
                ).classes("w-full md:w-48")
                max_trades_per_hour_input = ui.number(
                    label="Max Trades Per Hour",
                    value=guardrails.get("max_trades_per_hour", 2),
                    min=0,
                ).classes("w-full md:w-48")
                trade_window_seconds_input = ui.number(
                    label="Trade Window (sec)",
                    value=guardrails.get("trade_window_seconds", 3600),
                    min=60,
                    step=60,
                ).classes("w-full md:w-48")
            require_alignment_switch = ui.switch(
                "Require Position Alignment",
                value=guardrails.get("require_position_alignment", True),
            ).classes("mt-2")
            save_button = ui.button("Save", icon="save", color="primary")

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
            current = trading_pairs_select.value or []
            if isinstance(current, str):
                current = [current]
            current = [item for item in current if item in pairs]
            if not current:
                current = pairs[:1] if pairs else []
            trading_pairs_select.value = current

        def on_model_change(e: Any) -> None:
            response_schema_input.value = schema_to_text(e.value)
            response_schema_input.update()

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

        def on_prompt_version_change(e: Any) -> None:
            label = e.value
            version_id = prompt_version_options.get(label)
            if not version_id:
                return
            apply_prompt_version(version_id)

        prompt_version_select.on_value_change(on_prompt_version_change)

        async def save_settings(event: Any | None = None) -> None:
            config["ws_update_interval"] = int(ws_interval_input.value or 5)
            config["llm_system_prompt"] = prompt_input.value
            config["llm_decision_prompt"] = decision_prompt_input.value
            config["llm_model_id"] = model_select.value
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
            def _coerce(value: Any, fallback: Any, caster: Any) -> Any:
                try:
                    if value is None:
                        raise ValueError
                    return caster(value)
                except (TypeError, ValueError):
                    return caster(fallback)

            config["guardrails"] = {
                "max_leverage": _coerce(max_leverage_input.value, guardrails.get("max_leverage", 5), float),
                "max_position_pct": _coerce(
                    max_position_pct_input.value,
                    guardrails.get("max_position_pct", 0.2),
                    float,
                ),
                "daily_loss_limit_pct": _coerce(
                    daily_loss_limit_input.value,
                    guardrails.get("daily_loss_limit_pct", 3),
                    float,
                ),
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
            }
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
            selected_pairs = trading_pairs_select.value or []
            if isinstance(selected_pairs, str):
                selected_pairs = [selected_pairs]
            symbols = [str(item).strip().upper() for item in selected_pairs if str(item).strip()]
            if not symbols:
                symbols = ["BTC-USDT-SWAP"]
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
            if app.state.market_service:
                app.state.market_service.set_poll_interval(config["ws_update_interval"])
                await app.state.market_service.update_symbols(symbols)
            _set_prompt_version_value(config.get("prompt_version_id"))
            ui.notify("Configuration saved", color="positive")
            app.state.frontend_events.append("CFG updated")

        save_button.on("click", save_settings)
        asyncio.create_task(load_trading_pairs())
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
