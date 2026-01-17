from __future__ import annotations

import asyncio
import functools
import contextlib
import json
import logging
import math
import secrets
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Deque, Dict, Iterable, Optional

import httpx
import inspect

import pandas as pd
import pandas_ta as ta

from app.core.config import get_settings
from app.db.postgres import insert_equity_point, insert_executed_trade
from app.models.trade import ExecutedTrade
from app.services.okx_sdk_adapter import OkxAccountAdapter, OkxTradeAdapter
from app.services.state_service import StateService


def _ensure_httpx_proxies_compat() -> None:
    """Allow legacy 'proxies' kwarg with httpx>=0.28."""

    def _patch(cls: type) -> None:
        if cls is None or getattr(cls, "_tai2_proxies_patched", False):
            return
        try:
            signature = inspect.signature(cls.__init__)
        except (TypeError, ValueError):  # pragma: no cover - CPython internals
            return
        if "proxies" in signature.parameters:
            return
        original_init = cls.__init__

        @functools.wraps(original_init)
        def patched_init(self, *args, proxies=None, **kwargs):
            if proxies is not None:
                if "proxy" in kwargs and kwargs["proxy"] is not None:
                    raise TypeError("Cannot supply both 'proxy' and 'proxies'")
                kwargs["proxy"] = proxies
            return original_init(self, *args, **kwargs)

        cls.__init__ = patched_init  # type: ignore[assignment]
        setattr(cls, "_tai2_proxies_patched", True)

    for attr in ("Client", "AsyncClient"):
        _patch(getattr(httpx, attr, None))


_ensure_httpx_proxies_compat()

try:  # pragma: no cover - import guarded for optional dependency
    import okx.Account as OkxAccount
    import okx.Trade as OkxTrade
    import okx.MarketData as OkxMarket
    import okx.PublicData as OkxPublic
    import okx.TradingData as OkxTrading
    from okx.websocket.WsPublicAsync import WsPublicAsync
except ImportError:  # pragma: no cover
    OkxAccount = None
    OkxTrade = None
    OkxMarket = None
    OkxPublic = None
    OkxTrading = None
    WsPublicAsync = None

if WsPublicAsync is not None:  # pragma: no cover - exercised only when dependency installed
    class SafeWsPublicAsync(WsPublicAsync):
        async def stop(self) -> None:  # type: ignore[override]
            if getattr(self, "factory", None) is not None:
                await self.factory.close()
            if getattr(self, "websocket", None):
                try:
                    await self.websocket.close()
                except Exception:
                    logger.debug("Websocket close failed", exc_info=True)
            # intentionally avoid stopping the global event loop


else:  # pragma: no cover - optional dependency
    SafeWsPublicAsync = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
STABLE_CURRENCIES = {"USD", "USDT", "USDC", "USDK", "DAI"}


class MarketService:
    """Streams OKX market data and publishes normalized snapshots to Redis."""

    _TIMEFRAME_CHOICES = {
        "15m": "15m",
        "1h": "1H",
        "4h": "4H",
        "1d": "1D",
    }
    SUPPORTED_TIMEFRAMES = set(_TIMEFRAME_CHOICES.values())
    DEFAULT_TIMEFRAME = "4H"
    PROTECTION_ERROR_CODES = {
        "51047",
        "51048",
        "51049",
        "51050",
        "51051",
        "51052",
    }
    TIER_CACHE_TTL_SECONDS = 600

    def __init__(
        self,
        *,
        state_service: StateService,
        symbol: str = "BTC-USDT-SWAP",
        symbols: list[str] | None = None,
        sub_account: str | None = None,
        sub_account_use_master: bool = False,
        okx_flag: str | int | None = None,
        account_api: Any | None = None,
        market_api: Any | None = None,
        public_api: Any | None = None,
        trading_api: Any | None = None,
        trade_api: Any | None = None,
        websocket_factory: Callable[[str], WsPublicAsync] | None = None,
        enable_websocket: bool = True,
        log_sink: Callable[[str], None] | None = None,
        ohlc_bar: str | None = None,
    ) -> None:
        self.settings = get_settings()
        self.symbols = self._normalize_symbols(symbols) or [symbol]
        self.symbol = self.symbols[0]
        self._sub_account = (sub_account or self.settings.okx_sub_account or "").strip() or None
        self._sub_account_use_master = bool(sub_account_use_master)
        self._okx_flag = self._normalize_okx_flag(okx_flag or self.settings.okx_api_flag)
        self.state_service = state_service
        self._account_api = account_api or self._build_account_api()
        self._market_api = market_api or self._build_market_api()
        self._public_api = public_api or self._build_public_api()
        self._trading_api = trading_api or self._build_trading_api()
        self._trade_api = trade_api or self._build_trade_api()
        default_ws_class = SafeWsPublicAsync or WsPublicAsync
        self._websocket_factory = websocket_factory or default_ws_class
        self._enable_websocket = enable_websocket
        self._poller_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_client: Optional[WsPublicAsync] = None
        self._latest_order_book: dict[str, dict[str, Any]] = {}
        self._latest_depth_metrics: dict[str, list[float]] = {}
        self._latest_ticker: dict[str, dict[str, Any]] = {}
        self._latest_funding: dict[str, dict[str, Any]] = {}
        self._latest_open_interest: dict[str, dict[str, Any]] = {}
        self._latest_liquidations: dict[str, list[dict[str, Any]]] = {}
        self._latest_ohlcv: dict[str, list[list[Any]]] = {}
        self._latest_long_short_ratio: dict[str, dict[str, Any]] = {}
        self._last_long_short_fetch: dict[str, float] = {}
        self._trade_buffers: dict[str, Deque[dict[str, float]]] = {}
        self._decision_state: dict[str, dict[str, Any]] = {}
        self._recent_trades: dict[str, Deque[float]] = {}
        self._position_activity: dict[str, float] = {}
        self._position_protection: dict[str, dict[str, Any]] = {}
        self._protection_sync_ts: dict[str, float] = {}
        self._execution_feedback: Deque[dict[str, Any]] = deque(maxlen=50)
        self._latest_execution_limits: dict[str, dict[str, Any]] = {}
        self._position_tiers: dict[str, dict[str, Any]] = {}
        self._subscribed_symbols: set[str] = set()
        self._available_symbols: list[str] = []
        self._instrument_specs: dict[str, dict[str, float]] = {}
        self._poll_interval = max(1, self.settings.ws_update_interval)
        self._ohlc_bar = self._normalize_bar(ohlc_bar)
        self._log_sink = log_sink or (lambda msg: None)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._ws_last_debug: Dict[str, float] = {}
        self._wait_for_tp_sl = False

    async def start(self) -> None:
        if self._poller_task:
            return
        await self._hydrate_cached_annotations()
        if self._enable_websocket:
            self._ws_task = asyncio.create_task(self._run_public_ws(), name="okx-ws")
        self._poller_task = asyncio.create_task(self._poll_loop(), name="okx-market-poller")
        logger.info("MarketService started for %s", ", ".join(self.symbols))

    async def stop(self) -> None:
        if self._poller_task:
            self._poller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poller_task
            self._poller_task = None
        if self._ws_task:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None
        if self._ws_client:
            await self._ws_client.stop()
            self._ws_client = None
            self._subscribed_symbols.clear()
        logger.info("MarketService stopped for %s", ", ".join(self.symbols))

    async def _poll_loop(self) -> None:
        while True:
            interval = max(1, self._poll_interval)
            try:
                await self.refresh_snapshot(reason="poller")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Failed to refresh market snapshot: %s", exc)
            await asyncio.sleep(interval)

    async def _run_public_ws(self) -> None:
        client_factory = self._websocket_factory or WsPublicAsync
        if client_factory is None:
            logger.warning("python-okx websocket modules not available; skipping public WS")
            return
        self._ws_client = client_factory(PUBLIC_WS_URL)
        await self._ws_client.connect()
        channels = self._build_channel_args(self.symbols)
        await self._ws_client.subscribe(channels, self._handle_ws_message)
        self._subscribed_symbols = set(self.symbols)
        try:
            await self._ws_client.consume()
        finally:  # pragma: no cover - network resource cleanup
            await self._ws_client.stop()
            self._ws_client = None
            self._subscribed_symbols.clear()

    def _handle_ws_message(self, message: Any) -> None:
        if isinstance(message, (bytes, bytearray)):
            try:
                message = message.decode()
            except Exception:  # pragma: no cover - defensive decoding
                return
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except json.JSONDecodeError:
                logger.debug("WS message not JSON: %s", message)
                return
        if not isinstance(message, dict):
            return

        arg = message.get("arg") or {}
        channel = arg.get("channel")
        base_symbol = arg.get("instId")
        data = message.get("data") or []
        if not channel or not data:
            return

        for entry in data:
            symbol = (entry.get("instId") if isinstance(entry, dict) else None) or base_symbol
            if not symbol or symbol not in self.symbols:
                continue
            if channel == "tickers" and isinstance(entry, dict):
                self._latest_ticker[symbol] = entry
            elif channel == "books" and isinstance(entry, dict):
                self._latest_order_book[symbol] = self._normalize_order_book(entry)
            elif channel == "trades" and isinstance(entry, dict):
                buffer = self._get_trade_buffer(symbol)
                buffer.append(
                    {
                        "side": 1.0 if entry.get("side") == "buy" else -1.0,
                        "volume": float(entry.get("sz") or entry.get("vol") or 0.0),
                    }
                )
            elif channel == "funding-rate" and isinstance(entry, dict):
                self._latest_funding[symbol] = entry
            elif channel == "open-interest" and isinstance(entry, dict):
                self._latest_open_interest[symbol] = entry
            elif channel == "liquidation-orders" and isinstance(entry, dict):
                self._latest_liquidations[symbol] = data if isinstance(data, list) else []
            else:
                continue
            if self._should_emit_ws_debug(channel, symbol):
                self._emit_debug(f"WS update: {channel}::{symbol}", mirror_logger=False)

    def _should_emit_ws_debug(self, channel: str, symbol: str) -> bool:
        key = f"{channel}:{symbol}"
        interval = max(1.0, self._ws_debug_interval)
        now = time.time()
        last = self._ws_last_debug.get(key)
        if last is None or now - last >= interval:
            self._ws_last_debug[key] = now
            return True
        return False

    @staticmethod
    def _build_channel_args(symbols: Iterable[str]) -> list[dict[str, str]]:
        channels: list[dict[str, str]] = []
        for symbol in symbols:
            channels.extend(
                [
                    {"channel": "tickers", "instId": symbol},
                    {"channel": "books", "instId": symbol},
                    {"channel": "trades", "instId": symbol},
                    {"channel": "funding-rate", "instId": symbol},
                    {"channel": "open-interest", "instId": symbol},
                    {"channel": "liquidation-orders", "instId": symbol},
                ]
            )
        return channels

    async def _build_snapshot(self) -> dict[str, Any]:
        positions_raw = await self._fetch_positions()
        await self._sync_position_protection_entries(positions_raw)
        positions = self._annotate_positions(positions_raw)
        account_payload = await self._fetch_account_balance()
        self._refresh_execution_limits_from_account(account_payload)
        total_account_value = account_payload.get("total_account_value", 0.0)
        total_equity_value = account_payload.get("total_equity", 0.0)
        total_eq_usd = account_payload.get("total_eq_usd", total_equity_value)
        available_equity = account_payload.get("available_equity")
        available_eq_usd = account_payload.get("available_eq_usd")
        available_balances = account_payload.get("available_balances") or {}
        account = account_payload.get("details", []) or []
        account_equity = float(total_eq_usd or total_equity_value or total_account_value or 0.0)
        market_data: dict[str, dict[str, Any]] = {}
        instrument_specs: dict[str, dict[str, float]] = {}
        for symbol in self.symbols:
            order_book = await self._fetch_order_book(symbol)
            ticker = await self._fetch_ticker(symbol)
            funding = await self._fetch_funding_rate(symbol)
            open_interest = await self._fetch_open_interest(symbol)
            ohlcv = await self._fetch_ohlcv(symbol)
            indicators = self._compute_indicators(ohlcv)
            custom_metrics = self._compute_custom_metrics(symbol, order_book)
            market_ls_ratio = await self._fetch_long_short_ratio(symbol)
            if market_ls_ratio:
                custom_metrics["market_long_short_ratio"] = market_ls_ratio
            strategy_signal = self._derive_strategy_signal(indicators, custom_metrics, ticker)
            risk_metrics = self._derive_risk_metrics(indicators, ticker)
            market_data[symbol] = {
                "order_book": order_book,
                "ticker": ticker,
                "funding_rate": funding,
                "open_interest": open_interest,
                "indicators": indicators,
                "custom_metrics": custom_metrics,
                "liquidations": self._latest_liquidations.get(symbol, []),
                "strategy_signal": strategy_signal,
                "risk_metrics": risk_metrics,
            }
            spec = self._instrument_specs.get(symbol)
            if spec:
                instrument_specs[symbol] = {
                    "lot_size": self._extract_float(spec.get("lot_size")),
                    "min_size": self._extract_float(spec.get("min_size")),
                    "tick_size": self._extract_float(spec.get("tick_size")),
                }

        primary_symbol = self.symbols[0]
        primary_market = market_data.get(primary_symbol, {})
        position_activity = {
            symbol: {
                "last_trade": datetime.fromtimestamp(ts, timezone.utc).isoformat()
            }
            for symbol, ts in self._position_activity.items()
            if ts > 0
        }
        position_protection = self._snapshot_position_protection()
        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": primary_symbol,
            "symbols": list(self.symbols),
            "positions": positions,
            "account": account,
            "account_equity": account_equity,
            "total_account_value": total_account_value,
            "total_eq_usd": total_eq_usd,
            "available_equity": available_equity,
            "available_eq_usd": available_eq_usd,
            "available_balances": available_balances,
            "order_book": primary_market.get("order_book", {}),
            "ticker": primary_market.get("ticker", {}),
            "funding_rate": primary_market.get("funding_rate", {}),
            "open_interest": primary_market.get("open_interest", {}),
            "liquidations": primary_market.get("liquidations", []),
            "indicators": primary_market.get("indicators", {}),
            "custom_metrics": primary_market.get("custom_metrics", {}),
            "strategy_signal": primary_market.get("strategy_signal", {}),
            "risk_metrics": primary_market.get("risk_metrics", {}),
            "ws_update_interval": self.settings.ws_update_interval,
            "market_data": market_data,
            "position_activity": position_activity,
            "position_protection": position_protection,
            "instrument_specs": instrument_specs,
        }
        if self._latest_execution_limits:
            snapshot["execution_limits"] = {
                key: dict(meta)
                for key, meta in self._latest_execution_limits.items()
                if isinstance(meta, dict)
            }
        snapshot["execution_feedback"] = list(self._execution_feedback)
        return snapshot

    def _snapshot_position_protection(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for symbol, meta in self._position_protection.items():
            payload[symbol] = {
                "take_profit": meta.get("take_profit"),
                "stop_loss": meta.get("stop_loss"),
                "updated_at": meta.get("updated_at"),
                "algo_cl_ord_id": meta.get("algo_cl_ord_id"),
            }
        return payload

    def _annotate_positions(self, positions: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        if not positions:
            return []
        annotated: list[dict[str, Any]] = []
        for entry in positions:
            if not isinstance(entry, dict):
                annotated.append(entry)
                continue
            symbol_value = entry.get("instId") or entry.get("symbol")
            symbol_key = str(symbol_value).strip().upper() if symbol_value else None
            enriched = dict(entry)
            if symbol_key:
                protection_meta = self._position_protection.get(symbol_key)
                if isinstance(protection_meta, dict):
                    enriched.setdefault("tai2_take_profit", protection_meta.get("take_profit"))
                    enriched.setdefault("tai2_stop_loss", protection_meta.get("stop_loss"))
                    enriched.setdefault("tai2_protection_updated_at", protection_meta.get("updated_at"))
                activity_ts = self._position_activity.get(symbol_key)
                if activity_ts:
                    enriched.setdefault(
                        "tai2_last_trade",
                        datetime.fromtimestamp(activity_ts, timezone.utc).isoformat(),
                    )
            annotated.append(enriched)
        return annotated

    def _position_side_sizes(self, positions: list[dict[str, Any]] | None, symbol: str) -> dict[str, float]:
        totals = {"long": 0.0, "short": 0.0}
        if not positions:
            return totals
        normalized_symbol = symbol.upper()
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            entry_symbol = str(entry.get("instId") or entry.get("symbol") or "").upper()
            if entry_symbol != normalized_symbol:
                continue
            size_value = self._extract_float(entry.get("pos") or entry.get("size"))
            if size_value is None:
                continue
            side_value = str(entry.get("posSide") or entry.get("side") or "").lower()
            if not side_value:
                side_value = "long" if size_value >= 0 else "short"
            side_key = "long" if side_value == "long" else "short"
            totals[side_key] += abs(size_value)
        return totals

    async def _sync_position_protection_entries(self, positions: list[dict[str, Any]] | None) -> None:
        if not positions or not self._trade_api:
            return
        symbol_map: dict[str, str | None] = {}
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            symbol_key = self._normalize_symbol_key(entry.get("instId") or entry.get("symbol"))
            if not symbol_key:
                continue
            pos_side = (entry.get("posSide") or entry.get("side") or "").upper() or None
            symbol_map.setdefault(symbol_key, pos_side)
        if not symbol_map:
            return
        now = time.time()
        for symbol, pos_side in symbol_map.items():
            last_sync = self._protection_sync_ts.get(symbol, 0.0)
            if now - last_sync < 15:
                continue
            try:
                remote_entry = await self._fetch_latest_symbol_protection(symbol, pos_side=pos_side)
                self._protection_sync_ts[symbol] = time.time()
            except Exception as exc:  # pragma: no cover - network safety
                self._emit_debug(f"Protection sync failed for {symbol}: {exc}")
                continue
            if remote_entry:
                tp_value = self._extract_float(remote_entry.get("tpTriggerPx"))
                sl_value = self._extract_float(remote_entry.get("slTriggerPx"))
                if tp_value is None and sl_value is None:
                    continue
                updated_at = (
                    self._format_okx_timestamp(
                        remote_entry.get("updateTime")
                        or remote_entry.get("uTime")
                        or remote_entry.get("cTime")
                    )
                    or datetime.now(timezone.utc).isoformat()
                )
                meta: dict[str, Any] = {
                    "take_profit": tp_value,
                    "stop_loss": sl_value,
                    "algo_id": remote_entry.get("algoId"),
                    "algo_cl_ord_id": remote_entry.get("algoClOrdId"),
                    "updated_at": updated_at,
                    "synced": True,
                    "method": "okx-sync",
                }
                remote_side = remote_entry.get("posSide")
                if remote_side:
                    meta["pos_side"] = remote_side
                self._position_protection[symbol] = meta
            else:
                self._position_protection.pop(symbol, None)

    @staticmethod
    def _normalize_symbol_key(symbol: Any) -> str | None:
        if symbol is None:
            return None
        value = str(symbol).strip().upper()
        return value or None

    @staticmethod
    def _parse_cached_timestamp(value: Any) -> float | None:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    @staticmethod
    def _format_okx_timestamp(value: Any) -> str | None:
        if value in (None, ""):
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric > 1e15:  # guard against seconds vs milliseconds
            numeric /= 1000.0
        elif numeric > 1e12:  # milliseconds typical
            numeric /= 1000.0
        elif numeric > 1e9 * 10:  # microseconds
            numeric /= 1_000_000.0
        if numeric <= 0:
            return None
        return datetime.fromtimestamp(numeric, timezone.utc).isoformat()

    async def _hydrate_cached_annotations(self) -> None:
        try:
            snapshot = await self.state_service.get_market_snapshot()
        except Exception as exc:  # pragma: no cover - Redis/network safety
            self._emit_debug(f"Snapshot hydrate skipped: {exc}")
            return
        if not snapshot:
            return
        restored_protection = 0
        restored_activity = 0

        protection_block = snapshot.get("position_protection")
        if isinstance(protection_block, dict):
            for symbol, meta in protection_block.items():
                symbol_key = self._normalize_symbol_key(symbol)
                if not symbol_key or not isinstance(meta, dict):
                    continue
                if symbol_key in self._position_protection:
                    continue
                tp_value = self._extract_float(
                    meta.get("take_profit")
                    or meta.get("tpTriggerPx")
                    or meta.get("tp")
                )
                sl_value = self._extract_float(
                    meta.get("stop_loss")
                    or meta.get("slTriggerPx")
                    or meta.get("sl")
                )
                hydrated_meta: dict[str, Any] = {}
                if tp_value is not None:
                    hydrated_meta["take_profit"] = tp_value
                if sl_value is not None:
                    hydrated_meta["stop_loss"] = sl_value
                algo_id = meta.get("algo_id") or meta.get("algoId")
                if algo_id:
                    hydrated_meta["algo_id"] = algo_id
                algo_cl_ord_id = meta.get("algo_cl_ord_id") or meta.get("algoClOrdId")
                if algo_cl_ord_id:
                    hydrated_meta["algo_cl_ord_id"] = algo_cl_ord_id
                attached_ord_id = meta.get("attached_ord_id")
                if attached_ord_id:
                    hydrated_meta["attached_ord_id"] = attached_ord_id
                updated_at = meta.get("updated_at") or meta.get("updatedAt")
                if updated_at:
                    hydrated_meta["updated_at"] = updated_at
                if "synced" in meta:
                    hydrated_meta["synced"] = bool(meta.get("synced"))
                method = meta.get("method")
                if method:
                    hydrated_meta["method"] = method
                if not hydrated_meta:
                    continue
                self._position_protection[symbol_key] = hydrated_meta
                restored_protection += 1

        activity_block = snapshot.get("position_activity")
        if isinstance(activity_block, dict):
            for symbol, meta in activity_block.items():
                symbol_key = self._normalize_symbol_key(symbol)
                if not symbol_key:
                    continue
                if symbol_key in self._position_activity:
                    continue
                raw_timestamp = meta.get("last_trade") if isinstance(meta, dict) else meta
                parsed_ts = self._parse_cached_timestamp(raw_timestamp)
                if parsed_ts is None:
                    continue
                self._position_activity[symbol_key] = parsed_ts
                restored_activity += 1

        if restored_protection or restored_activity:
            self._emit_debug(
                f"Hydrated {restored_protection} TP/SL entries and {restored_activity} last-trade marks from cached snapshot"
            )

    def set_poll_interval(self, seconds: int) -> None:
        self._poll_interval = max(1, seconds)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._emit_debug(f"Poll interval updated to {self._poll_interval}s")

    def set_wait_for_tp_sl(self, enabled: bool) -> None:
        flag = bool(enabled)
        if flag == self._wait_for_tp_sl:
            return
        self._wait_for_tp_sl = flag
        state = "enabled" if flag else "disabled"
        self._emit_debug(f"Wait-for-TP/SL guard {state}")

    async def set_websocket_enabled(self, enabled: bool) -> None:
        flag = bool(enabled)
        if flag == self._enable_websocket:
            return
        self._enable_websocket = flag
        if not flag:
            self._emit_debug("Websocket streaming disabled; relying on REST poller")
            if self._ws_task:
                self._ws_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._ws_task
                self._ws_task = None
            if self._ws_client:
                await self._ws_client.stop()
                self._ws_client = None
            self._subscribed_symbols.clear()
            return
        self._emit_debug("Websocket streaming enabled; starting listener")
        if not self._poller_task:
            return
        if self._ws_task:
            return
        self._ws_task = asyncio.create_task(self._run_public_ws(), name="okx-ws")

    async def set_sub_account(self, value: str | None, use_master: bool | None = None) -> None:
        normalized = (value or "").strip() or None
        updated = False
        if normalized != self._sub_account:
            self._sub_account = normalized
            updated = True
        if use_master is not None and bool(use_master) != self._sub_account_use_master:
            self._sub_account_use_master = bool(use_master)
            updated = True
        if not updated:
            return
        label = normalized or "<primary>"
        mode = "master routing" if self._sub_account_use_master else "scoped credentials"
        self._emit_debug(f"Sub-account preference updated to {label} ({mode})")
        await self._publish_snapshot()

    async def refresh_snapshot(self, reason: str | None = None) -> dict[str, Any] | None:
        snapshot = await self._build_snapshot()
        if not snapshot:
            return None
        await self.state_service.set_market_snapshot(snapshot)
        await self._persist_equity(snapshot)
        ticker = snapshot.get("ticker") or {}
        price = ticker.get("last") or ticker.get("px") or "n/a"
        label = reason or "manual"
        self._emit_debug(
            f"Snapshot[{label}] @ {snapshot.get('generated_at')} price={price}"
        )
        return snapshot

    @staticmethod
    def _normalize_okx_flag(flag: str | int | None) -> str:
        if isinstance(flag, str) and flag.strip() == "1":
            return "1"
        if isinstance(flag, int) and flag == 1:
            return "1"
        return "0"

    async def set_okx_flag(self, value: str | int | None) -> None:
        normalized = self._normalize_okx_flag(value)
        if normalized == self._okx_flag:
            return
        self._okx_flag = normalized
        env_label = "LIVE" if normalized == "0" else "PAPER"
        self._emit_debug(f"OKX API environment set to {env_label} (flag={normalized})")
        self._rebuild_okx_clients()
        await self._publish_snapshot()

    async def update_symbols(self, symbols: list[str]) -> None:
        cleaned = self._normalize_symbols(symbols)
        if not cleaned:
            return
        if cleaned == self.symbols:
            return
        previous = list(self.symbols)
        cache_added = [symbol for symbol in cleaned if symbol not in previous]
        cache_removed = [symbol for symbol in previous if symbol not in cleaned]
        self.symbols = cleaned
        self.symbol = cleaned[0]
        if self._available_symbols:
            merged = set(self._available_symbols)
            merged.update(cleaned)
            self._available_symbols = sorted(merged)
        affected = list({*cache_added, *cache_removed})
        if affected:
            self._reset_symbol_state(affected)
        ws_added = [symbol for symbol in cleaned if symbol not in self._subscribed_symbols]
        ws_removed = [symbol for symbol in self._subscribed_symbols if symbol not in cleaned]
        if self._enable_websocket and self._ws_client:
            await self._update_ws_subscriptions(ws_added, ws_removed)
        self._emit_debug(f"Symbols updated: {', '.join(self.symbols)}")
        await self._publish_snapshot()

    async def _update_ws_subscriptions(self, added: list[str], removed: list[str]) -> None:
        if not self._ws_client:
            return
        if removed:
            args = self._build_channel_args(removed)
            await self._ws_client.unsubscribe(args, self._handle_ws_message)
        if added:
            args = self._build_channel_args(added)
            await self._ws_client.subscribe(args, self._handle_ws_message)
        self._subscribed_symbols = set(self.symbols)

    def _reset_symbol_state(self, symbols: list[str] | None = None) -> None:
        if symbols:
            targets = set(symbols)
        else:
            targets = set().union(
                self._latest_order_book.keys(),
                self._latest_ticker.keys(),
                self._latest_funding.keys(),
                self._latest_open_interest.keys(),
                self._latest_liquidations.keys(),
                self._trade_buffers.keys(),
            )
        for symbol in targets:
            self._latest_order_book.pop(symbol, None)
            self._latest_ticker.pop(symbol, None)
            self._latest_funding.pop(symbol, None)
            self._latest_open_interest.pop(symbol, None)
            self._latest_liquidations.pop(symbol, None)
            self._latest_ohlcv.pop(symbol, None)
            self._trade_buffers.pop(symbol, None)
            self._recent_trades.pop(symbol, None)
            self._decision_state.pop(symbol, None)
            self._position_activity.pop(symbol, None)
            self._latest_long_short_ratio.pop(symbol, None)
            self._last_long_short_fetch.pop(symbol, None)
            self._latest_execution_limits.pop(symbol, None)

    def get_cached_ticker(self, symbol: str | None) -> dict[str, Any] | None:
        normalized = self._normalize_symbols([symbol]) if symbol else []
        if not normalized:
            return None
        return self._latest_ticker.get(normalized[0])

    def get_last_price(self, symbol: str | None) -> float | None:
        ticker = self.get_cached_ticker(symbol)
        return self._price_from_ticker(ticker)

    async def _publish_snapshot(self) -> None:
        try:
            snapshot = await self._build_snapshot()
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to publish snapshot: %s", exc)
            return
        if snapshot:
            await self.state_service.set_market_snapshot(snapshot)
            await self._persist_equity(snapshot)

    @staticmethod
    def _normalize_symbols(symbols: list[str] | None) -> list[str]:
        if not symbols:
            return []
        cleaned: list[str] = []
        seen = set()
        for symbol in symbols:
            value = str(symbol).strip().upper()
            if not value or value in seen:
                continue
            seen.add(value)
            cleaned.append(value)
        return cleaned

    async def list_available_symbols(self) -> list[str]:
        if self._available_symbols:
            return list(self._available_symbols)
        symbols = await self._fetch_available_symbols()
        self._available_symbols = symbols
        return list(self._available_symbols)

    async def _fetch_available_symbols(self) -> list[str]:
        if not self._public_api:
            return list(self.symbols)
        response = await asyncio.to_thread(
            self._public_api.get_instruments,
            instType="SWAP",
        )
        data = self._safe_data(response)
        pairs: list[str] = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            inst_id = entry.get("instId")
            if inst_id:
                symbol = str(inst_id).upper()
                pairs.append(symbol)
                lot_size = self._extract_float(entry.get("lotSz"))
                min_size = self._extract_float(entry.get("minSz"))
                tick_size = self._extract_float(entry.get("tickSz"))
                self._instrument_specs[symbol] = {
                    "lot_size": lot_size or 0.0,
                    "min_size": min_size or 0.0,
                    "tick_size": tick_size or 0.0,
                }
        if not pairs:
            return list(self.symbols)
        return sorted(set(pairs))

    def _tier_cache_key(self, symbol: str, trade_mode: str) -> str:
        normalized_symbol = (symbol or "").upper()
        normalized_mode = (trade_mode or "cross").lower()
        return f"{normalized_mode}:{normalized_symbol}"

    @staticmethod
    def _instrument_family(symbol: str | None) -> str | None:
        if not symbol:
            return None
        parts = str(symbol).upper().split("-")
        if len(parts) >= 2:
            return "-".join(parts[:2])
        return None

    @staticmethod
    def _quote_currency_from_symbol(symbol: str | None) -> str | None:
        if not symbol:
            return None
        parts = str(symbol).upper().split("-")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
        return None

    async def _get_position_tiers(self, symbol: str, trade_mode: str = "cross") -> list[dict[str, Any]]:
        cache_key = self._tier_cache_key(symbol, trade_mode)
        cached = self._position_tiers.get(cache_key)
        now = time.time()
        if cached and now - cached.get("timestamp", 0.0) < self.TIER_CACHE_TTL_SECONDS:
            tiers = cached.get("tiers")
            if isinstance(tiers, list):
                return tiers
        tiers = await self._fetch_position_tiers(symbol, trade_mode=trade_mode)
        if tiers:
            self._position_tiers[cache_key] = {"tiers": tiers, "timestamp": now}
        return tiers or []

    async def _fetch_position_tiers(self, symbol: str, trade_mode: str = "cross") -> list[dict[str, Any]]:
        if not self._public_api:
            return []
        kwargs: dict[str, Any] = {
            "instType": "SWAP",
            "tdMode": trade_mode,
        }
        if symbol:
            kwargs["instId"] = symbol
            family = self._instrument_family(symbol)
            if family:
                kwargs["instFamily"] = family
        try:
            response = await asyncio.to_thread(
                self._public_api.get_position_tiers,
                **kwargs,
            )
        except Exception as exc:  # pragma: no cover - network dependent
            self._emit_debug(f"Position tier fetch failed for {symbol}: {exc}")
            return []
        data = self._safe_data(response)
        if not data and isinstance(response, dict):
            code = response.get("code") or response.get("sCode")
            msg = response.get("msg") or response.get("sMsg")
            detail = f" code={code} msg={msg}" if (code or msg) else ""
            self._emit_debug(f"Position tier fetch returned no data for {symbol}{detail}")
        return data

    def _select_position_tier(
        self,
        tiers: list[dict[str, Any]],
        size: float,
    ) -> dict[str, Any] | None:
        if not tiers or size is None:
            return None
        target = abs(size)
        fallback: dict[str, Any] | None = None
        for tier in tiers:
            min_size = self._extract_float(tier.get("minSz"))
            max_size = self._extract_float(tier.get("maxSz"))
            if min_size is not None and target < min_size:
                continue
            if max_size not in (None, 0.0) and target > max_size:
                fallback = tier
                continue
            return tier
        return fallback or (tiers[-1] if tiers else None)

    async def _apply_tier_margin_guard(
        self,
        *,
        symbol: str,
        trade_mode: str,
        pos_side: str,
        existing_side_size: float,
        additional_size: float,
        last_price: float,
        available_margin_usd: float | None,
    ) -> dict[str, Any]:
        tiers = await self._get_position_tiers(symbol, trade_mode)
        if not tiers or last_price is None or last_price <= 0:
            return {"size": additional_size}
        resulting_size = max(0.0, existing_side_size) + max(0.0, additional_size)
        tier = self._select_position_tier(tiers, resulting_size)
        if not tier:
            return {"size": additional_size}
        imr = self._extract_float(tier.get("imr"))
        tier_max_leverage = self._extract_float(tier.get("maxLever"))
        if (imr is None or imr <= 0) and tier_max_leverage:
            if tier_max_leverage > 0:
                imr = 1.0 / tier_max_leverage
        if (tier_max_leverage is None or tier_max_leverage <= 0) and imr and imr > 0:
            tier_max_leverage = 1.0 / imr
        if imr is None or imr <= 0 or available_margin_usd is None or available_margin_usd <= 0:
            return {"size": additional_size, "tier": tier, "tier_max_leverage": tier_max_leverage}
        proposed_notional = additional_size * last_price
        required_margin = proposed_notional * imr
        max_notional_allowed = available_margin_usd / imr if imr > 0 else None
        final_size = additional_size
        clipped = False
        blocked = False
        if max_notional_allowed is not None and proposed_notional > max_notional_allowed:
            if max_notional_allowed <= 0:
                final_size = 0.0
                blocked = True
            else:
                final_size = max_notional_allowed / last_price
            clipped = True
        return {
            "size": final_size,
            "tier": tier,
            "tier_max_leverage": tier_max_leverage,
            "tier_imr": imr,
            "required_margin": required_margin,
            "max_notional_allowed": max_notional_allowed,
            "clipped": clipped,
            "blocked": blocked,
            "pos_side": pos_side,
        }

    async def _fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if not self._account_api:
            return []
        kwargs = {"instType": "SWAP"}
        if self._sub_account and self._sub_account_use_master:
            kwargs["subAcct"] = self._sub_account
        if symbol:
            kwargs["instId"] = symbol
        response = await asyncio.to_thread(
            self._account_api.get_positions,
            **kwargs,
        )
        data = self._safe_data(response)
        return data

    async def _position_size(self, symbol: str, pos_side: str | None = None) -> float | None:
        records = await self._fetch_positions(symbol)
        normalized = symbol.upper()
        normalized_side = (pos_side or "").lower()
        for entry in records:
            inst_id = str(entry.get("instId") or "").upper()
            if inst_id != normalized:
                continue
            entry_side = str(entry.get("posSide") or "").lower()
            if normalized_side and entry_side != normalized_side:
                continue
            size = self._extract_float(entry.get("pos") or entry.get("size"))
            if size is not None:
                return size
        return None

    async def _wait_for_position(
        self,
        symbol: str,
        *,
        pos_side: str | None = None,
        attempts: int = 6,
        delay: float = 0.25,
    ) -> float | None:
        normalized = symbol.upper()
        for attempt in range(attempts):
            size = await self._position_size(normalized, pos_side=pos_side)
            if size is not None and abs(size) > 0:
                return size
            if attempt < attempts - 1:
                await asyncio.sleep(delay)
        return None

    async def _fetch_account_balance(self) -> dict[str, Any]:
        if not self._account_api:
            return {
                "details": [],
                "total_equity": 0.0,
                "total_account_value": 0.0,
                "total_eq_usd": 0.0,
            }
        if self._sub_account and self._sub_account_use_master:
            response = await asyncio.to_thread(
                self._account_api.get_account_balance,
                subAcct=self._sub_account,
            )
        else:
            response = await asyncio.to_thread(self._account_api.get_account_balance)
        data = self._safe_data(response)
        return self._normalize_account_balances(data)

    async def _fetch_order_book(self, symbol: str) -> dict[str, Any]:
        cached = self._latest_order_book.get(symbol)
        if cached:
            return cached
        if not self._market_api:
            return {}
        response = await asyncio.to_thread(
            self._market_api.get_orderbook,
            instId=symbol,
            sz=20,
        )
        data = self._safe_data(response)
        if not data:
            return {}
        normalized = self._normalize_order_book(data[0])
        self._latest_order_book[symbol] = normalized
        return normalized

    async def _fetch_ticker(self, symbol: str) -> dict[str, Any]:
        cached = self._latest_ticker.get(symbol)
        if cached:
            return cached
        if not self._market_api:
            return {}
        response = await asyncio.to_thread(self._market_api.get_ticker, symbol)
        data = self._safe_data(response)
        if not data:
            return {}
        self._latest_ticker[symbol] = data[0]
        return data[0]

    async def _fetch_funding_rate(self, symbol: str) -> dict[str, Any]:
        cached = self._latest_funding.get(symbol)
        if cached:
            return cached
        if not self._public_api:
            return {}
        response = await asyncio.to_thread(self._public_api.get_funding_rate, symbol)
        data = self._safe_data(response)
        if not data:
            return {}
        self._latest_funding[symbol] = data[0]
        return data[0]

    async def _fetch_open_interest(self, symbol: str) -> dict[str, Any]:
        cached = self._latest_open_interest.get(symbol)
        if cached:
            return cached
        if not self._public_api:
            return {}
        response = await asyncio.to_thread(
            self._public_api.get_open_interest,
            "SWAP",
            instId=symbol,
        )
        data = self._safe_data(response)
        if not data:
            return {}
        self._latest_open_interest[symbol] = data[0]
        return data[0]

    async def _fetch_ohlcv(self, symbol: str) -> list[list[Any]]:
        cached = self._latest_ohlcv.get(symbol)
        if not self._market_api:
            return cached or []
        try:
            response = await asyncio.to_thread(
                self._market_api.get_candlesticks,
                instId=symbol,
                bar=self._ohlc_bar,
                limit=200,
            )
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("OHLCV fetch failed for %s: %s", symbol, exc)
            self._emit_debug(f"OHLCV fetch fallback for {symbol}: {exc}")
            return cached or []
        data = self._safe_data(response)
        if data:
            self._latest_ohlcv[symbol] = data
            return data
        return cached or []

    def _compute_custom_metrics(self, symbol: str, order_book: dict[str, Any]) -> dict[str, Any]:
        cvd = self._calculate_cvd(symbol)
        ofi = self._calculate_ofi(symbol, order_book)
        cvd_series = self._build_cvd_series(symbol)
        ofi_ratio_series = self._latest_depth_metrics.get(symbol, [])[-200:]
        return {
            "cumulative_volume_delta": cvd,
            "order_flow_imbalance": ofi,
            "cvd_series": cvd_series,
            "ofi_ratio_series": ofi_ratio_series,
        }

    @staticmethod
    def _compute_indicators(ohlcv: list[list[Any]]) -> dict[str, Any]:
        if not ohlcv:
            return {
                "bollinger_bands": {},
                "stoch_rsi": {},
                "adx": {},
                "obv": {},
                "cmf": {},
                "vwap": None,
                "volume": {},
            }

        normalized_rows = [row[:6] for row in ohlcv if len(row) >= 6]
        if not normalized_rows:
            return {
                "bollinger_bands": {},
                "stoch_rsi": {},
                "adx": {},
                "obv": {},
                "cmf": {},
                "vwap": None,
                "volume": {},
            }

        df = pd.DataFrame(normalized_rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", errors="coerce")
        for column in ["open", "high", "low", "close", "volume"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.sort_values("ts").set_index("ts")
        bb = ta.bbands(close=df["close"], length=20)
        stoch = ta.stochrsi(close=df["close"])
        rsi_series = ta.rsi(df["close"], length=14)
        macd_df = ta.macd(df["close"])
        ema_50 = ta.ema(df["close"], length=50)
        ema_200 = ta.ema(df["close"], length=200)
        adx_df = ta.adx(high=df["high"], low=df["low"], close=df["close"], length=14)
        obv_series = ta.obv(close=df["close"], volume=df["volume"])
        cmf_series = ta.cmf(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"], length=20)
        vwap_series = ta.vwap(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"])
        volume_rsi_series = ta.rsi(df["volume"], length=14)
        atr_series = ta.atr(high=df["high"], low=df["low"], close=df["close"], length=14)
        volume_avg = float(df["volume"].tail(20).mean()) if not df.empty else 0.0
        tail_df = df.tail(200)
        last_close = float(df["close"].iloc[-1]) if not df.empty else None
        atr_value = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else None
        atr_pct = (atr_value / last_close * 100) if atr_value and last_close else None
        ohlcv_compact = [
            {
                "ts": int(idx.timestamp() * 1000),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            for idx, row in tail_df.iterrows()
        ]
        indicators = {
            "bollinger_bands": {
                "lower": MarketService._column_value(bb, ["BBL_20_2.0", "BBL_20_2.0_2.0"]),
                "middle": MarketService._column_value(bb, ["BBM_20_2.0", "BBM_20_2.0_2.0"]),
                "upper": MarketService._column_value(bb, ["BBU_20_2.0", "BBU_20_2.0_2.0"]),
            },
            "stoch_rsi": {
                "k": MarketService._last_value(stoch, "STOCHRSIk_14_14_3_3"),
                "d": MarketService._last_value(stoch, "STOCHRSId_14_14_3_3"),
            },
            "rsi": float(rsi_series.iloc[-1]) if rsi_series is not None and not rsi_series.empty else None,
            "macd": {
                "value": MarketService._last_value(macd_df, "MACD_12_26_9"),
                "signal": MarketService._last_value(macd_df, "MACDs_12_26_9"),
                "hist": MarketService._last_value(macd_df, "MACDh_12_26_9"),
                "series": MarketService._frame_column_to_list(macd_df, "MACD_12_26_9"),
            }
            if macd_df is not None
            else {},
            "adx": {
                "value": MarketService._last_value(adx_df, "ADX_14"),
                "di_plus": MarketService._last_value(adx_df, "DMP_14"),
                "di_minus": MarketService._last_value(adx_df, "DMN_14"),
                "series": MarketService._frame_column_to_list(adx_df, "ADX_14"),
            }
            if adx_df is not None
            else {},
            "obv": {
                "value": float(obv_series.iloc[-1]) if obv_series is not None and not obv_series.empty else None,
                "series": MarketService._series_to_list(obv_series),
            },
            "cmf": {
                "value": float(cmf_series.iloc[-1]) if cmf_series is not None and not cmf_series.empty else None,
                "series": MarketService._series_to_list(cmf_series),
            },
            "moving_averages": {
                "ema_50": float(ema_50.iloc[-1]) if ema_50 is not None and not ema_50.empty else None,
                "ema_200": float(ema_200.iloc[-1]) if ema_200 is not None and not ema_200.empty else None,
            },
            "vwap": float(vwap_series.iloc[-1]) if vwap_series is not None and not vwap_series.empty else None,
            "vwap_series": MarketService._series_to_list(vwap_series),
            "volume": {
                "last": float(df["volume"].iloc[-1]) if not df.empty else 0.0,
                "average": volume_avg,
                "series": MarketService._series_to_list(df["volume"]),
            },
            "volume_rsi_series": MarketService._series_to_list(volume_rsi_series),
            "ohlcv": ohlcv_compact,
            "atr": atr_value,
            "atr_pct": atr_pct,
        }
        return indicators

    def _get_trade_buffer(self, symbol: str) -> Deque[dict[str, float]]:
        buffer = self._trade_buffers.get(symbol)
        if buffer is None:
            buffer = deque(maxlen=500)
            self._trade_buffers[symbol] = buffer
        return buffer

    def _calculate_cvd(self, symbol: str) -> float:
        value = 0.0
        for trade in self._get_trade_buffer(symbol):
            volume = trade.get("volume", 0.0)
            direction = trade.get("side", 0.0)
            value += direction * volume
        return value

    def _build_cvd_series(self, symbol: str, limit: int = 200) -> list[float]:
        values: list[float] = []
        running = 0.0
        for trade in self._get_trade_buffer(symbol):
            volume = trade.get("volume", 0.0)
            direction = trade.get("side", 0.0)
            running += direction * volume
            values.append(running)
        return values[-limit:]

    async def _fetch_long_short_ratio(self, symbol: str) -> dict[str, Any]:
        cache = self._latest_long_short_ratio.get(symbol, {})
        if not self._trading_api:
            return cache
        now = time.time()
        last_fetch = self._last_long_short_fetch.get(symbol, 0.0)
        if cache and now - last_fetch < 60:
            return cache
        base_ccy = (symbol or "").split("-")[0]
        if not base_ccy:
            return cache
        period = "5m"
        try:
            response = await asyncio.to_thread(
                self._trading_api.get_long_short_ratio,
                base_ccy,
                "",
                "",
                period,
            )
        except Exception as exc:  # pragma: no cover - network dependency
            logger.debug("Long/short ratio fetch failed for %s: %s", symbol, exc)
            return cache
        data = self._safe_data(response)
        if not data:
            return cache
        trimmed = data[-200:]
        ratios: list[float] = []
        timestamps: list[int] = []
        for entry in trimmed:
            ratio_val = None
            ts_val: int | None = None
            if isinstance(entry, dict):
                ratio_val = self._extract_float(entry.get("ratio"))
                ts_raw = entry.get("ts")
                try:
                    ts_val = int(ts_raw)
                except (TypeError, ValueError):
                    ts_val = None
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                ratio_val = self._extract_float(entry[1])
                try:
                    ts_val = int(entry[0])
                except (TypeError, ValueError):
                    ts_val = None
            if ratio_val is None:
                continue
            ratios.append(ratio_val)
            timestamps.append(ts_val or 0)
        if not ratios:
            return cache
        record = {
            "value": ratios[-1],
            "series": ratios,
            "timestamps": timestamps,
            "period": period,
        }
        self._latest_long_short_ratio[symbol] = record
        self._last_long_short_fetch[symbol] = now
        return record

    def _derive_strategy_signal(
        self,
        indicators: dict[str, Any],
        custom_metrics: dict[str, Any],
        ticker: dict[str, Any],
    ) -> dict[str, Any]:
        rsi = indicators.get("rsi")
        macd_value = (indicators.get("macd") or {}).get("value")
        stoch = indicators.get("stoch_rsi") or {}
        stoch_k = stoch.get("k")
        stoch_d = stoch.get("d")
        ofi = custom_metrics.get("order_flow_imbalance")
        cvd = custom_metrics.get("cumulative_volume_delta")

        score = 0.0
        reasons: list[str] = []
        if rsi is not None:
            if rsi < 35:
                score += 1.0 + (35 - rsi) / 50
                reasons.append(f"RSI oversold ({rsi:.1f})")
            elif rsi > 65:
                score -= 1.0 + (rsi - 65) / 50
                reasons.append(f"RSI overbought ({rsi:.1f})")
        if macd_value is not None:
            if macd_value > 0:
                score += 0.6
                reasons.append("MACD bullish")
            elif macd_value < 0:
                score -= 0.6
                reasons.append("MACD bearish")
        if stoch_k is not None and stoch_d is not None:
            if stoch_k > stoch_d + 5:
                score += 0.4
                reasons.append("Stoch RSI crossing up")
            elif stoch_k + 5 < stoch_d:
                score -= 0.4
                reasons.append("Stoch RSI crossing down")
        if isinstance(ofi, dict):
            net_value = ofi.get("net")
        else:
            net_value = ofi
        if net_value is not None:
            if net_value > 0:
                score += 0.3
                reasons.append("Order flow favors buyers")
            elif net_value < 0:
                score -= 0.3
                reasons.append("Order flow favors sellers")
        if cvd is not None:
            if cvd > 0:
                score += 0.2
                reasons.append("CVD accumulation")
            elif cvd < 0:
                score -= 0.2
                reasons.append("CVD distribution")

        if score > 0.2:
            action = "BUY"
        elif score < -0.2:
            action = "SELL"
        else:
            action = "HOLD"
        confidence = max(0.2, min(1.0, abs(score) / 2.0 + 0.2)) if action != "HOLD" else min(0.4, abs(score) / 2.0 + 0.2)
        last_price = self._extract_float((ticker or {}).get("last"))
        summary_reason = " & ".join(reasons[:3]) if reasons else "Awaiting clear signal"
        return {
            "action": action,
            "confidence": round(confidence, 3),
            "score": round(score, 3),
            "reason": summary_reason,
            "last_price": last_price,
        }

    def _derive_risk_metrics(self, indicators: dict[str, Any], ticker: dict[str, Any]) -> dict[str, Any]:
        atr = indicators.get("atr")
        atr_pct = indicators.get("atr_pct")
        last_price = self._extract_float((ticker or {}).get("last"))
        suggested_stop = atr * 1.5 if atr and last_price else None
        risk_perc = (suggested_stop / last_price * 100) if suggested_stop and last_price else None
        return {
            "atr": atr,
            "atr_pct": atr_pct,
            "suggested_stop": suggested_stop,
            "suggested_stop_pct": risk_perc,
        }

    def _calculate_ofi(self, symbol: str, order_book: dict[str, Any]) -> dict[str, Any]:
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])
        bid_volume = sum(float(level[1]) for level in bids[:20])
        ask_volume = sum(float(level[1]) for level in asks[:20])
        depth_ratio = (bid_volume / ask_volume) if ask_volume else None
        weighted_bids = sum(float(level[0]) * float(level[1]) for level in bids[:20])
        weighted_asks = sum(float(level[0]) * float(level[1]) for level in asks[:20])
        price_imbalance = weighted_bids - weighted_asks
        ratio_series = self._latest_depth_metrics.setdefault(symbol, [])
        if depth_ratio is not None:
            ratio_series.append(depth_ratio)
        if len(ratio_series) > 500:
            del ratio_series[:-500]
        return {
            "net": bid_volume - ask_volume,
            "ratio": depth_ratio,
            "weighted": price_imbalance,
        }

    @staticmethod
    def _calculate_account_equity(rows: list[dict[str, Any]]) -> float:
        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            nested = row.get("details")
            if isinstance(nested, list) and nested:
                for detail in nested:
                    if not isinstance(detail, dict):
                        continue
                    value = MarketService._extract_equity_value(detail)
                    if value is not None:
                        total += value
                continue
            value = MarketService._extract_equity_value(row)
            if value is not None:
                total += value
        return total

    @staticmethod
    def _normalize_account_balances(entries: list[Any]) -> dict[str, Any]:
        details: list[dict[str, Any]] = []
        total_equity = 0.0
        total_account_value = 0.0
        total_eq_usd = 0.0
        balances: dict[str, dict[str, float]] = {}
        available_equity_total = 0.0
        available_eq_usd_total = 0.0
        def track_balance(record: dict[str, Any]) -> None:
            nonlocal available_equity_total, available_eq_usd_total
            if not isinstance(record, dict):
                return
            currency_raw = record.get("ccy") or record.get("currency")
            currency = str(currency_raw).upper() if currency_raw else None
            if not currency:
                return
            bucket = balances.setdefault(
                currency,
                {
                    "currency": currency,
                    "equity": 0.0,
                    "equity_usd": 0.0,
                    "available": 0.0,
                    "available_usd": 0.0,
                    "cash": 0.0,
                },
            )
            eq_value = MarketService._extract_float(record.get("eq"))
            eq_usd_value = MarketService._extract_float(record.get("eqUsd"))
            avail_eq_value = MarketService._extract_float(record.get("availEq"))
            avail_bal_value = MarketService._extract_float(record.get("availBal"))
            avail_usd_value = MarketService._extract_float(
                record.get("availEqUsd") or record.get("availUsd")
            )
            if eq_usd_value is None and eq_value is not None and currency in STABLE_CURRENCIES:
                eq_usd_value = eq_value
            if avail_usd_value is None and avail_eq_value is not None:
                px = None
                if eq_value and eq_value > 0 and eq_usd_value:
                    px = eq_usd_value / eq_value if eq_value else None
                if px:
                    avail_usd_value = avail_eq_value * px
                elif currency in STABLE_CURRENCIES:
                    avail_usd_value = avail_eq_value
            if eq_value is not None:
                bucket["equity"] += eq_value
            if eq_usd_value is not None:
                bucket["equity_usd"] += eq_usd_value
            if avail_eq_value is not None:
                bucket["available"] += avail_eq_value
                available_equity_total += avail_eq_value
            if avail_usd_value is not None:
                bucket["available_usd"] += avail_usd_value
                available_eq_usd_total += avail_usd_value
            if avail_bal_value is not None:
                bucket["cash"] += avail_bal_value

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_total = 0.0
            entry_value = MarketService._extract_float(entry.get("totalAccountValue"))
            if entry_value is not None:
                total_account_value += entry_value
            eq_usd_value = MarketService._extract_float(entry.get("totalEq"))
            if eq_usd_value is not None:
                total_eq_usd += eq_usd_value
            nested = entry.get("details")
            if isinstance(nested, list) and nested:
                for detail in nested:
                    if not isinstance(detail, dict):
                        continue
                    details.append(detail)
                    track_balance(detail)
                    value = MarketService._extract_equity_value(detail)
                    if value is not None:
                        entry_total += value
            else:
                details.append(entry)
                track_balance(entry)
                value = MarketService._extract_equity_value(entry)
                if value is not None:
                    entry_total += value

            if entry_total == 0.0:
                fallback = entry.get("totalEq")
                if fallback is not None:
                    try:
                        entry_total = float(fallback)
                    except (TypeError, ValueError):
                        entry_total = 0.0

            total_equity += entry_total

        cleaned_balances: dict[str, dict[str, float]] = {}
        for currency, stats in balances.items():
            cleaned_balances[currency] = {
                "currency": currency,
                "equity": stats.get("equity", 0.0),
                "equity_usd": stats.get("equity_usd", 0.0),
                "available": stats.get("available", 0.0),
                "available_usd": stats.get("available_usd", 0.0),
                "cash": stats.get("cash", 0.0),
            }

        return {
            "details": details,
            "total_equity": total_equity,
            "total_account_value": total_account_value or total_equity,
            "total_eq_usd": total_eq_usd or total_equity,
            "available_equity": available_equity_total or 0.0,
            "available_eq_usd": available_eq_usd_total or 0.0,
            "available_balances": cleaned_balances,
        }

    @staticmethod
    def _extract_equity_value(record: dict[str, Any]) -> float | None:
        for key in ("eq", "eqUsd", "cashBal", "availEq", "availBal"):
            value = record.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _emit_debug(self, message: str, *, mirror_logger: bool = True) -> None:
        text = str(message)
        try:
            self._log_sink(text)
        except Exception:  # pragma: no cover - defensive
            logger.debug("Debug sink failed", exc_info=True)
        if mirror_logger:
            logger.debug(text)

    def _record_execution_limits(
        self,
        symbol: str,
        *,
        available_margin_usd: float | None,
        account_equity_usd: float | None,
        quote_currency: str | None,
        quote_available_usd: float | None,
        quote_cash_usd: float | None,
        max_leverage: float | None,
        max_notional_usd: float | None,
        source: str = "execution",
        tier_max_notional_usd: float | None = None,
        tier_initial_margin_ratio: float | None = None,
        tier_source: str | None = None,
    ) -> None:
        normalized = self._normalize_symbol_key(symbol)
        if not normalized:
            return
        existing = self._latest_execution_limits.get(normalized)
        quote_symbol = self._normalize_symbol_key(quote_currency)
        if quote_symbol is None and isinstance(existing, dict):
            quote_symbol = existing.get("quote_currency")

        payload: dict[str, Any] = {
            "available_margin_usd": available_margin_usd,
            "account_equity_usd": account_equity_usd,
            "quote_currency": quote_symbol,
            "quote_available_usd": quote_available_usd,
            "quote_cash_usd": quote_cash_usd,
            "max_leverage": max_leverage,
            "max_notional_usd": max_notional_usd,
            "source": source,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if tier_max_notional_usd is not None:
            payload["tier_max_notional_usd"] = tier_max_notional_usd
        if tier_initial_margin_ratio is not None:
            payload["tier_initial_margin_ratio"] = tier_initial_margin_ratio
        if tier_source:
            payload["tier_source"] = tier_source
        if isinstance(existing, dict):
            for key in (
                "max_leverage",
                "max_notional_usd",
                "tier_max_notional_usd",
                "tier_initial_margin_ratio",
                "tier_source",
                "quote_currency",
                "quote_available_usd",
                "quote_cash_usd",
                "available_margin_usd",
                "account_equity_usd",
            ):
                if payload.get(key) is None and existing.get(key) is not None:
                    payload[key] = existing.get(key)
            if not source and existing.get("source"):
                payload["source"] = existing.get("source")
        self._latest_execution_limits[normalized] = payload

    def _record_execution_feedback(
        self,
        symbol: str,
        message: str,
        *,
        level: str = "info",
        meta: dict[str, Any] | None = None,
    ) -> None:
        entry = {
            "symbol": symbol,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
        }
        if meta:
            entry["meta"] = meta
        self._execution_feedback.append(entry)
        if level in {"warning", "error"}:
            meta_suffix = ""
            if meta:
                try:
                    meta_suffix = f" meta={json.dumps(meta, default=str)}"
                except Exception:
                    meta_suffix = f" meta={meta}"
            self._emit_debug(
                f"Execution feedback ({level}) {symbol}: {message}{meta_suffix}"
            )

    def _refresh_execution_limits_from_account(self, account_payload: dict[str, Any] | None) -> None:
        if not isinstance(account_payload, dict):
            return
        available_margin_usd = self._extract_float(account_payload.get("available_eq_usd"))
        account_equity_usd = self._extract_float(
            account_payload.get("total_eq_usd")
            or account_payload.get("total_equity")
            or account_payload.get("total_account_value")
        )
        balances = account_payload.get("available_balances")
        if not isinstance(balances, dict):
            balances = {}
        if (
            available_margin_usd is None
            and account_equity_usd is None
            and not balances
        ):
            return

        for symbol in self.symbols:
            quote_currency = self._quote_currency_from_symbol(symbol)
            quote_available_usd = None
            quote_cash_usd = None
            if quote_currency and balances:
                quote_meta = balances.get(quote_currency)
                if isinstance(quote_meta, dict):
                    quote_available_usd = self._extract_float(
                        quote_meta.get("available_usd")
                        or quote_meta.get("equity_usd")
                    )
                    if quote_available_usd is None and quote_currency in STABLE_CURRENCIES:
                        quote_available_usd = self._extract_float(quote_meta.get("available"))
                    quote_cash = self._extract_float(quote_meta.get("cash"))
                    if quote_cash is not None:
                        if quote_currency in STABLE_CURRENCIES:
                            quote_cash_usd = quote_cash
                        else:
                            last_px = self._extract_float(
                                (self._latest_ticker.get(symbol) or {}).get("last")
                            )
                            if last_px:
                                quote_cash_usd = quote_cash * last_px

            effective_margin = available_margin_usd
            for candidate in (quote_cash_usd, quote_available_usd):
                if candidate is None:
                    continue
                if effective_margin is None or candidate > effective_margin:
                    effective_margin = candidate

            if (
                effective_margin is None
                and account_equity_usd is None
                and quote_available_usd is None
                and quote_cash_usd is None
            ):
                continue

            self._record_execution_limits(
                symbol,
                available_margin_usd=effective_margin,
                account_equity_usd=account_equity_usd,
                quote_currency=quote_currency,
                quote_available_usd=quote_available_usd,
                quote_cash_usd=quote_cash_usd,
                max_leverage=None,
                max_notional_usd=None,
                source="balance-snapshot",
            )

    @staticmethod
    def _prune_trade_history(history: Deque[float], now: float, window: int) -> None:
        cutoff = max(60, window or 3600)
        while history and now - history[0] > cutoff:
            history.popleft()

    def _detect_position_side(self, positions: list[dict[str, Any]], symbol: str) -> str:
        if not positions:
            return "FLAT"
        target = symbol.upper()
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            pos_symbol = str(pos.get("instId") or pos.get("symbol") or "").upper()
            if pos_symbol != target:
                continue
            size = self._extract_float(
                pos.get("size") or pos.get("pos") or pos.get("posQty") or pos.get("position")
            )
            if not size:
                continue
            raw_side = pos.get("posSide") or pos.get("side")
            if isinstance(raw_side, str):
                side = raw_side.upper()
            else:
                side = "LONG" if size > 0 else "SHORT"
            if side not in {"LONG", "SHORT"}:
                side = "LONG" if size > 0 else "SHORT"
            return side
        return "FLAT"

    @staticmethod
    def _transition_allowed(current_side: str, action: str) -> bool:
        if action == "HOLD":
            return True
        if current_side == "FLAT":
            return True
        if current_side == "LONG":
            return action == "SELL"
        if current_side == "SHORT":
            return action == "BUY"
        return True

    @staticmethod
    def _normalize_confidence(value: Any) -> float:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            return 0.5
        if math.isnan(confidence):
            return 0.5
        if confidence < 0:
            return 0.0
        if confidence > 1:
            return 1.0
        return confidence

    @staticmethod
    def _compute_leverage_adjusted_size(
        *,
        size_hint: float | None,
        account_equity: float | None,
        last_price: float | None,
        min_leverage: float,
        max_leverage: float,
        confidence: float,
        confidence_gate: float | None = None,
    ) -> float | None:
        size_hint_value = size_hint if size_hint and size_hint > 0 else None
        equity = account_equity if account_equity and account_equity > 0 else None
        price = last_price if last_price and last_price > 0 else None
        if equity is None or price is None:
            return size_hint_value
        min_lev = max(0.0, float(min_leverage))
        max_lev = max(min_lev, float(max_leverage) if max_leverage > 0 else (min_lev or 1.0))
        if max_lev <= 0:
            max_lev = 1.0
        if min_lev > max_lev:
            min_lev, max_lev = max_lev, min_lev
        try:
            normalized_conf = float(confidence)
        except (TypeError, ValueError):
            normalized_conf = 0.5
        if math.isnan(normalized_conf):
            normalized_conf = 0.5
        normalized_conf = min(max(normalized_conf, 0.0), 1.0)
        gate_value: float | None = None
        if confidence_gate is not None and not math.isnan(confidence_gate):
            gate_value = min(max(float(confidence_gate), 0.0), 1.0)
        if gate_value is not None:
            if gate_value >= 1.0:
                confidence_factor = 1.0 if normalized_conf >= 1.0 else 0.0
            elif normalized_conf <= gate_value:
                confidence_factor = 0.0
            else:
                confidence_factor = (normalized_conf - gate_value) / (1.0 - gate_value)
        else:
            confidence_factor = normalized_conf
        span = max_lev - min_lev
        if span <= 0:
            target_leverage = max_lev
        else:
            target_leverage = min_lev + span * confidence_factor
        target_leverage = max(min_lev, min(max_lev, target_leverage))
        if target_leverage <= 0:
            target_leverage = max(max_lev, 1.0)
        allow_upscale = (
            gate_value is None
            or normalized_conf >= gate_value
        )
        if size_hint_value:
            implied = (size_hint_value * price) / equity
            if implied > 0:
                adjusted = size_hint_value * (target_leverage / implied)
                if adjusted > size_hint_value and not allow_upscale:
                    return size_hint_value
                if adjusted > 0:
                    return adjusted
        notional = equity * target_leverage
        size_from_target = notional / price if price else None
        if size_from_target and size_from_target > 0:
            return size_from_target
        return size_hint_value

    async def handle_llm_decision(
        self,
        decision: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> bool:
        if not decision:
            return False
        context = context or {}
        action = (decision.get("action") or "HOLD").upper()
        if action not in {"BUY", "SELL", "HOLD"}:
            self._emit_debug(f"Ignoring unsupported action {action}")
            return False
        symbol = str(context.get("symbol") or decision.get("symbol") or self.symbol).upper()
        symbol_parts = symbol.split("-")
        quote_currency = symbol_parts[1].upper() if len(symbol_parts) >= 2 else None
        guardrails = context.get("guardrails") or {}
        cooldown_seconds = int(
            self._extract_float(
                guardrails.get("min_hold_seconds") or guardrails.get("cooldown_seconds")
            )
            or self._poll_interval
        )
        trade_limit = int(self._extract_float(guardrails.get("max_trades_per_hour")) or 0)
        trade_window = int(self._extract_float(guardrails.get("trade_window_seconds")) or 3600)
        require_alignment = bool(guardrails.get("require_position_alignment", True))
        wait_for_tp_sl = guardrails.get("wait_for_tp_sl")
        if wait_for_tp_sl is None:
            wait_for_tp_sl = self._wait_for_tp_sl
        else:
            wait_for_tp_sl = bool(wait_for_tp_sl)
        cooldown_seconds = max(0, cooldown_seconds)
        trade_limit = max(0, trade_limit)
        trade_window = max(60, trade_window)

        snapshot_positions: list[dict[str, Any]] = []
        if self.state_service:
            try:
                latest_snapshot = await self.state_service.get_market_snapshot()
            except Exception as exc:  # pragma: no cover - redis dependency
                logger.debug("Guardrail snapshot fetch failed: %s", exc)
                latest_snapshot = None
            if latest_snapshot:
                snapshot_positions = latest_snapshot.get("positions") or []

        positions = context.get("positions") or snapshot_positions
        if not positions:
            try:
                positions = await self._fetch_positions()
            except Exception as exc:  # pragma: no cover - network fallback
                self._emit_debug(f"Position fetch failed for {symbol}: {exc}")
                positions = []
        current_side = self._detect_position_side(positions, symbol)
        dual_side_mode = False
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            pos_symbol = str(pos.get("instId") or pos.get("symbol") or "").upper()
            if pos_symbol != symbol:
                continue
            if isinstance(pos.get("posSide"), str):
                dual_side_mode = True
                break
        side_sizes = self._position_side_sizes(positions, symbol)
        now = time.time()
        summary = (
            f"LLM decision {action} size={decision.get('position_size', '--')} "
            f"conf={decision.get('confidence', '--')} symbol={symbol}"
        )

        if action == "HOLD":
            self._decision_state[symbol] = {"action": action, "timestamp": now}
            self._emit_debug(summary)
            return False

        if wait_for_tp_sl and current_side in {"LONG", "SHORT"}:
            closing_action = (
                (current_side == "LONG" and action == "SELL")
                or (current_side == "SHORT" and action == "BUY")
            )
            if closing_action:
                symbol_key = symbol.upper()
                protection_meta = self._position_protection.get(symbol_key)
                has_protection = False
                if protection_meta and protection_meta.get("synced"):
                    tp_value = self._extract_float(protection_meta.get("take_profit"))
                    sl_value = self._extract_float(protection_meta.get("stop_loss"))
                    has_protection = bool(
                        (tp_value is not None and tp_value > 0)
                        or (sl_value is not None and sl_value > 0)
                    )
                if has_protection:
                    self._emit_debug(
                        f"Wait-for-TP/SL guard blocked {action} for {symbol}: protection active"
                    )
                    return False

        if require_alignment and not self._transition_allowed(current_side, action):
            self._emit_debug(
                f"Guardrail blocked {action} for {symbol}: current side={current_side}"
            )
            self._decision_state[symbol] = {"action": current_side, "timestamp": now}
            return False

        last_decision = self._decision_state.get(symbol)
        if cooldown_seconds > 0 and last_decision:
            last_ts = last_decision.get("timestamp")
            if isinstance(last_ts, (int, float)) and now - float(last_ts) < cooldown_seconds:
                remaining = cooldown_seconds - (now - float(last_ts))
                self._emit_debug(
                    f"Guardrail cooldown active for {symbol}; skipping {action} ({remaining:.0f}s left)"
                )
                return False

        history = self._recent_trades.setdefault(symbol, deque())
        self._prune_trade_history(history, now, trade_window)
        if trade_limit > 0 and len(history) >= trade_limit:
            self._emit_debug(
                f"Guardrail trade limit hit for {symbol}; skipping {action}"
            )
            return False

        history.append(now)
        self._decision_state[symbol] = {"action": action, "timestamp": now}
        self._emit_debug(summary)

        execution_cfg = context.get("execution") or {}
        execution_enabled = bool(execution_cfg.get("enabled"))
        if not execution_enabled:
            self._emit_debug(f"Execution disabled for {symbol}; skipping OKX order")
            return False
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot execute decision")
            return False

        trade_mode = str(execution_cfg.get("trade_mode") or "cross").lower()
        if trade_mode not in {"cross", "isolated"}:
            trade_mode = "cross"
        isolated_mode = trade_mode == "isolated"
        order_type = str(execution_cfg.get("order_type") or "market").lower()
        if order_type != "market":
            order_type = "market"
        min_size = self._extract_float(execution_cfg.get("min_size")) or 0.001

        market_block = context.get("market") or {}
        last_price = (
            self._extract_float(market_block.get("last_price"))
            or self._extract_float(market_block.get("bid"))
            or self._extract_float(market_block.get("ask"))
        )
        if last_price is None:
            ticker_snapshot = self._latest_ticker.get(symbol, {})
            last_price = self._extract_float(ticker_snapshot.get("last"))

        account_block = context.get("account") or {}
        account_equity = self._extract_float(
            account_block.get("account_equity")
            or account_block.get("total_account_value")
            or account_block.get("total_eq_usd")
        )
        base_max_pct = self._extract_float(guardrails.get("max_position_pct"))
        symbol_cap_pct = None
        symbol_caps = guardrails.get("symbol_position_caps")
        if isinstance(symbol_caps, dict):
            symbol_cap_pct = self._extract_float(
                symbol_caps.get(symbol)
                or symbol_caps.get(symbol.upper())
            )
        effective_max_pct = None
        for candidate in (base_max_pct, symbol_cap_pct):
            if candidate and candidate > 0:
                effective_max_pct = candidate if effective_max_pct is None else min(effective_max_pct, candidate)
        if effective_max_pct is None:
            effective_max_pct = base_max_pct
        leverage_override_reason: str | None = None
        min_leverage = self._extract_float(guardrails.get("min_leverage"))
        max_leverage = self._extract_float(guardrails.get("max_leverage"))
        if min_leverage is None:
            min_leverage = 0.0
        if max_leverage is None or max_leverage <= 0:
            max_leverage = max(min_leverage or 0.0, 1.0)
        if max_leverage < min_leverage:
            min_leverage, max_leverage = max_leverage, min_leverage
        confidence_gate = self._extract_float(guardrails.get("min_leverage_confidence_gate"))
        if confidence_gate is None:
            confidence_gate = 0.5
        confidence_gate = min(max(confidence_gate, 0.0), 1.0)
        available_balances_block = account_block.get("available_balances")
        available_margin_usd = self._extract_float(account_block.get("available_eq_usd"))

        live_account_balances: dict[str, Any] | None = None
        if self._account_api:
            try:
                live_account_balances = await self._fetch_account_balance()
            except Exception as exc:  # pragma: no cover - network variance
                self._emit_debug(f"Live balance refresh failed: {exc}")

        if live_account_balances:
            live_equity = self._extract_float(
                live_account_balances.get("total_eq_usd")
                or live_account_balances.get("total_equity")
                or live_account_balances.get("total_account_value")
            )
            if live_equity is not None and live_equity > 0:
                account_equity = live_equity
            live_available_margin = self._extract_float(live_account_balances.get("available_eq_usd"))
            if live_available_margin is not None:
                available_margin_usd = live_available_margin
            live_balances_block = live_account_balances.get("available_balances")
            if isinstance(live_balances_block, dict) and live_balances_block:
                available_balances_block = live_balances_block
            self._refresh_execution_limits_from_account(live_account_balances)

        quote_available_usd = None
        quote_cash_usd = None
        if isinstance(available_balances_block, dict) and quote_currency:
            quote_meta = available_balances_block.get(quote_currency)
            if isinstance(quote_meta, dict):
                quote_available_usd = self._extract_float(
                    quote_meta.get("available_usd") or quote_meta.get("equity_usd")
                )
                if quote_available_usd is None and quote_currency in STABLE_CURRENCIES:
                    quote_available_usd = self._extract_float(quote_meta.get("available"))
                quote_cash = self._extract_float(quote_meta.get("cash"))
                if quote_cash is not None:
                    if quote_currency in STABLE_CURRENCIES:
                        quote_cash_usd = quote_cash
                    elif last_price:
                        quote_cash_usd = quote_cash * last_price
        quote_margin_candidates = [
            value
            for value in (quote_cash_usd, quote_available_usd)
            if value is not None and value > 0
        ]
        if isolated_mode:
            if quote_margin_candidates:
                available_margin_usd = max(quote_margin_candidates)
            else:
                label = f"{quote_currency} margin" if quote_currency else "quote margin"
                self._emit_debug(
                    f"Execution skipped for {symbol}: isolated mode requires {label} but none is available"
                )
                self._record_execution_feedback(
                    symbol,
                    "Isolated margin unavailable",
                    level="warning",
                    meta={
                        "trade_mode": trade_mode,
                        "quote_currency": quote_currency,
                    },
                )
                return False
        else:
            for candidate in quote_margin_candidates:
                if available_margin_usd is None or candidate > available_margin_usd:
                    available_margin_usd = candidate
        if available_margin_usd is None and account_equity is not None:
            available_margin_usd = account_equity
        if available_margin_usd is None or available_margin_usd <= 0:
            margin_text = f"{(available_margin_usd or 0.0):.4f}"
            self._emit_debug(
                f"Execution skipped for {symbol}: insufficient available margin ({margin_text} USD)"
            )
            self._record_execution_feedback(
                symbol,
                f"Insufficient available margin ({margin_text} USD)",
                level="warning",
                meta={"available_margin_usd": available_margin_usd},
            )
            return False
        confidence_value = self._normalize_confidence(decision.get("confidence"))
        size_hint = self._extract_float(decision.get("position_size"))
        raw_size = self._compute_leverage_adjusted_size(
            size_hint=size_hint,
            account_equity=account_equity,
            last_price=last_price,
            min_leverage=min_leverage,
            max_leverage=max_leverage,
            confidence=confidence_value,
            confidence_gate=confidence_gate,
        ) or 0.0
        if raw_size <= 0:
            self._emit_debug(
                f"Execution skipped for {symbol}: unable to derive valid position size"
            )
            return False

        take_profit_price = self._normalize_take_profit(
            action,
            self._extract_float(decision.get("take_profit")),
            last_price,
        )
        stop_loss_price = self._normalize_stop_loss(
            action,
            self._extract_float(decision.get("stop_loss")),
            last_price,
        )
        if take_profit_price:
            prefer_up = action == "BUY"
            take_profit_price = self._quantize_price(
                symbol,
                take_profit_price,
                prefer_up=prefer_up,
            )
        if stop_loss_price:
            prefer_up = action == "SELL"
            stop_loss_price = self._quantize_price(
                symbol,
                stop_loss_price,
                prefer_up=prefer_up,
            )

        requested_take_profit = take_profit_price
        requested_stop_loss = stop_loss_price

        guardrail_notional_cap = None
        clipped_by_cap = False
        cap_reason: str | None = None
        if account_equity and effective_max_pct:
            guardrail_notional_cap = max(0.0, account_equity * max_leverage * effective_max_pct)
            if last_price and guardrail_notional_cap > 0:
                contract_cap = guardrail_notional_cap / last_price
                if contract_cap and contract_cap > 0 and raw_size > contract_cap:
                    raw_size = contract_cap
                    clipped_by_cap = True
                    symbol_limit_active = (
                        symbol_cap_pct is not None
                        and effective_max_pct is not None
                        and abs(effective_max_pct - symbol_cap_pct) < 1e-9
                        and (base_max_pct is None or symbol_cap_pct <= base_max_pct)
                    )
                    cap_reason = "symbol cap" if symbol_limit_active else "max position % limit"

        if (
            clipped_by_cap
            and account_equity
            and last_price
            and min_leverage
            and min_leverage > 0
        ):
            achieved_leverage = (raw_size * last_price) / account_equity
            if achieved_leverage < min_leverage:
                label = cap_reason or "max position % limit"
                self._emit_debug(
                    f"{symbol} leverage clipped to {achieved_leverage:.2f}x by {label}"
                )
                leverage_override_reason = cap_reason or label

        max_notional_from_margin = None
        if (
            available_margin_usd is not None
            and available_margin_usd > 0
            and last_price
            and max_leverage
            and max_leverage > 0
        ):
            max_notional_from_margin = available_margin_usd * max_leverage
            current_notional = raw_size * last_price
            if current_notional > max_notional_from_margin:
                raw_size = max_notional_from_margin / last_price
                self._emit_debug(
                    f"{symbol} size clipped by available margin ({available_margin_usd:,.4f} USD)"
                )
                self._record_execution_feedback(
                    symbol,
                    "Size clipped by available margin",
                    level="info",
                    meta={
                        "available_margin_usd": available_margin_usd,
                        "requested_notional": current_notional,
                        "max_notional": max_notional_from_margin,
                    },
                )

        side = "buy" if action == "BUY" else "sell"
        pos_side = "long" if action == "BUY" else "short"
        reduce_only = False
        if action == "SELL" and current_side == "LONG":
            pos_side = "long"
            reduce_only = True
        elif action == "BUY" and current_side == "SHORT":
            pos_side = "short"
            reduce_only = True

        tier_cap_limit = None
        tier_max_leverage_used = None
        tier_initial_margin_ratio = None
        if (
            not reduce_only
            and last_price
            and last_price > 0
            and raw_size > 0
        ):
            existing_side_size = side_sizes.get(pos_side, 0.0)
            tier_result = await self._apply_tier_margin_guard(
                symbol=symbol,
                trade_mode=trade_mode,
                pos_side=pos_side,
                existing_side_size=existing_side_size,
                additional_size=raw_size,
                last_price=last_price,
                available_margin_usd=available_margin_usd,
            )
            tier_cap_limit = tier_result.get("max_notional_allowed")
            tier_max_leverage_used = tier_result.get("tier_max_leverage")
            tier_initial_margin_ratio = tier_result.get("tier_imr")
            if tier_result.get("blocked"):
                self._emit_debug(
                    f"Execution skipped for {symbol}: insufficient margin at OKX tier requirements"
                )
                self._record_execution_feedback(
                    symbol,
                    "Insufficient margin at OKX tier",
                    level="warning",
                    meta={
                        "tier_imr": tier_initial_margin_ratio,
                        "tier_leverage": tier_max_leverage_used,
                    },
                )
                return False
            adjusted_size = self._extract_float(tier_result.get("size"))
            if adjusted_size is not None and adjusted_size >= 0:
                if tier_result.get("clipped") and adjusted_size < raw_size:
                    self._emit_debug(
                        f"{symbol} size clipped by OKX tier margin limit"
                    )
                    self._record_execution_feedback(
                        symbol,
                        "Size clipped by OKX tier margin",
                        level="info",
                        meta={
                            "tier_imr": tier_initial_margin_ratio,
                            "previous_size": raw_size,
                            "adjusted_size": adjusted_size,
                        },
                    )
                raw_size = adjusted_size
        if tier_cap_limit is not None:
            if max_notional_from_margin is None or tier_cap_limit < max_notional_from_margin:
                max_notional_from_margin = tier_cap_limit

        self._record_execution_limits(
            symbol,
            available_margin_usd=available_margin_usd,
            account_equity_usd=account_equity,
            quote_currency=quote_currency,
            quote_available_usd=quote_available_usd,
            quote_cash_usd=quote_cash_usd,
            max_leverage=tier_max_leverage_used or max_leverage,
            max_notional_usd=max_notional_from_margin,
            tier_max_notional_usd=tier_cap_limit,
            tier_initial_margin_ratio=tier_initial_margin_ratio,
            tier_source="position-tiers" if tier_cap_limit is not None else None,
        )

        if raw_size < min_size:
            self._emit_debug(
                f"Execution skipped for {symbol}: computed size {raw_size:.6f} below minimum {min_size}"
            )
            return False

        quantized_size = self._quantize_order_size(symbol, raw_size)
        if quantized_size is None or quantized_size <= 0:
            self._emit_debug(f"Execution skipped for {symbol}: size {raw_size:.6f} below lot size")
            return False
        if quantized_size < min_size:
            self._emit_debug(
                f"Execution skipped for {symbol}: quantized size {quantized_size:.6f} below minimum {min_size}"
            )
            return False
        raw_size = quantized_size

        if (
            not reduce_only
            and min_leverage
            and min_leverage > 0
            and account_equity
            and account_equity > 0
            and last_price
            and last_price > 0
        ):
            achieved_leverage = (raw_size * last_price) / account_equity
            if achieved_leverage < min_leverage:
                if leverage_override_reason:
                    self._emit_debug(
                        f"{symbol} leverage {achieved_leverage:.2f}x below minimum {min_leverage:.2f}x but proceeding due to {leverage_override_reason}"
                    )
                else:
                    self._emit_debug(
                        f"Execution skipped for {symbol}: leverage {achieved_leverage:.2f}x below minimum {min_leverage:.2f}x"
                    )
                    self._record_execution_feedback(
                        symbol,
                        "Blocked by minimum leverage guardrail",
                        level="warning",
                        meta={
                            "min_leverage": min_leverage,
                            "achieved_leverage": achieved_leverage,
                            "account_equity": account_equity,
                            "price": last_price,
                        },
                    )
                    return False

        attach_algo_orders: list[dict[str, Any]] | None = None
        attachments_take_profit = None
        attachments_stop_loss = None
        if reduce_only:
            await self._cancel_position_protection(symbol)
            take_profit_price = None
            stop_loss_price = None
            requested_take_profit = None
            requested_stop_loss = None
        else:
            attachments_take_profit = requested_take_profit
            attachments_stop_loss = requested_stop_loss
            if attachments_take_profit or attachments_stop_loss:
                await self._cancel_position_protection(symbol)
                if last_price and last_price > 0:
                    attachments_take_profit = self._drop_conflicting_target(
                        symbol=symbol,
                        action=action,
                        target=attachments_take_profit,
                        reference_price=last_price,
                        kind="take-profit",
                        stage="pre-order attachment",
                    )
                    attachments_stop_loss = self._drop_conflicting_target(
                        symbol=symbol,
                        action=action,
                        target=attachments_stop_loss,
                        reference_price=last_price,
                        kind="stop-loss",
                        stage="pre-order attachment",
                    )
                    if attachments_take_profit or attachments_stop_loss:
                        attach_algo_orders = self._build_attach_algo_orders(
                            take_profit_price=attachments_take_profit,
                            stop_loss_price=attachments_stop_loss,
                        )
                else:
                    self._emit_debug(
                        f"Skipping attached TP/SL for {symbol}: missing last price for validation"
                    )

        client_order_id = self._generate_client_order_id()
        order, attachments_used = await self._submit_order(
            symbol=symbol,
            side=side,
            pos_side=pos_side,
            size=raw_size,
            trade_mode=trade_mode,
            order_type=order_type,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            attach_algo_orders=attach_algo_orders,
        )
        if not order:
            self._emit_debug(f"Order placement failed for {symbol}")
            return False

        order_id = order.get("ordId") or order.get("orderId") or client_order_id
        self._emit_debug(
            f"OKX order submitted {side.upper()} {raw_size:.4f} {symbol} ({order_id})"
        )

        executed_size = self._extract_float(order.get("fillSz") or order.get("sz")) or raw_size
        executed_price = (
            self._extract_float(order.get("fillPx") or order.get("avgPx"))
            or last_price
        )
        fee_value = self._extract_float(order.get("fee") or order.get("fillFee"))
        if fee_value is not None:
            fee_value = abs(fee_value)

        take_profit_price = requested_take_profit
        stop_loss_price = requested_stop_loss
        reference_for_protection = executed_price or last_price
        take_profit_price = self._drop_conflicting_target(
            symbol=symbol,
            action=action,
            target=take_profit_price,
            reference_price=reference_for_protection,
            kind="take-profit",
            stage="post-fill",
        )
        stop_loss_price = self._drop_conflicting_target(
            symbol=symbol,
            action=action,
            target=stop_loss_price,
            reference_price=reference_for_protection,
            kind="stop-loss",
            stage="post-fill",
        )
        final_targets_present = bool(take_profit_price or stop_loss_price)

        if not reduce_only and final_targets_present:
            protection_ready = False
            if attachments_used and (attachments_take_profit or attachments_stop_loss):
                protection_ready = await self._confirm_attached_protection(
                    symbol=symbol,
                    order_id=str(order_id),
                    take_profit_price=attachments_take_profit,
                    stop_loss_price=attachments_stop_loss,
                )
                if not protection_ready:
                    self._emit_debug(
                        f"Attached TP/SL for {symbol} not confirmed; falling back to standalone algo"
                    )
            if not protection_ready:
                await self._refresh_position_protection(
                    symbol=symbol,
                    trade_mode=trade_mode,
                    action=action,
                    take_profit_price=take_profit_price,
                    stop_loss_price=stop_loss_price,
                    dual_side_mode=dual_side_mode,
                    pos_side=pos_side,
                )

        await self._record_trade_execution(
            symbol=symbol,
            side=side,
            price=executed_price,
            amount=executed_size,
            rationale=decision.get("rationale"),
            fee=fee_value,
        )
        return True

    async def _persist_equity(self, snapshot: dict[str, Any]) -> None:
        if not self.settings.database_url:
            return
        try:
            await insert_equity_point(
                account_equity=snapshot.get("account_equity"),
                total_account_value=snapshot.get("total_account_value"),
                total_eq_usd=snapshot.get("total_eq_usd"),
            )
        except Exception as exc:  # pragma: no cover - persistence best-effort
            logger.debug("Failed to persist equity point: %s", exc)

    @staticmethod
    def _last_value(frame: pd.DataFrame | None, column: str) -> float | None:
        if frame is None or column not in frame or frame.empty:
            return None
        series = frame[column]
        return float(series.iloc[-1]) if not series.empty else None

    @staticmethod
    def _column_value(frame: pd.DataFrame | None, candidates: list[str]) -> float | None:
        for name in candidates:
            value = MarketService._last_value(frame, name)
            if value is not None:
                return value
        return None

    @staticmethod
    def _series_to_list(series: pd.Series | None, limit: int = 200) -> list[float]:
        if series is None:
            return []
        return [float(val) for val in series.dropna().tolist()[-limit:]]

    @staticmethod
    def _frame_column_to_list(frame: pd.DataFrame | None, column: str, limit: int = 200) -> list[float]:
        if frame is None or column not in frame:
            return []
        return [float(val) for val in frame[column].dropna().tolist()[-limit:]]

    def _normalize_bar(self, value: str | None) -> str:
        if not value:
            return self.DEFAULT_TIMEFRAME
        candidate = value.strip()
        return self._TIMEFRAME_CHOICES.get(candidate.lower(), self.DEFAULT_TIMEFRAME)

    async def set_ohlc_bar(self, value: str) -> None:
        bar = self._normalize_bar(value)
        if bar == self._ohlc_bar:
            return
        self._ohlc_bar = bar
        self._emit_debug(f"OHLC timeframe set to {bar}")
        await self._publish_snapshot()

    @staticmethod
    def _safe_data(response: Any) -> list[Any]:
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, list):
                return data
        if isinstance(response, list):
            return response
        return []

    @staticmethod
    def _price_from_ticker(ticker: dict[str, Any] | None) -> float | None:
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

    @staticmethod
    def _normalize_order_book(data: dict[str, Any]) -> dict[str, Any]:
        bids = [[float(price), float(size)] for price, size, *_ in data.get("bids", [])][:20]
        asks = [[float(price), float(size)] for price, size, *_ in data.get("asks", [])][:20]
        return {"bids": bids, "asks": asks, "ts": data.get("ts")}

    def _build_account_api(self) -> Any | None:
        if OkxAccount is None:
            logger.warning("okx SDK not installed; AccountAPI unavailable")
            return None
        if not (self.settings.okx_api_key and self.settings.okx_secret_key and self.settings.okx_passphrase):
            logger.warning("OKX credentials missing; AccountAPI disabled")
            return None
        raw_api = OkxAccount.AccountAPI(
            api_key=self.settings.okx_api_key,
            api_secret_key=self.settings.okx_secret_key,
            passphrase=self.settings.okx_passphrase,
            flag=self._okx_flag,
        )
        return OkxAccountAdapter(raw_api)

    def _build_market_api(self) -> Any | None:
        if OkxMarket is None:
            logger.warning("python-okx not installed; MarketAPI unavailable")
            return None
        return OkxMarket.MarketAPI(flag=self._okx_flag)

    def _build_public_api(self) -> Any | None:
        if OkxPublic is None:
            logger.warning("python-okx not installed; PublicAPI unavailable")
            return None
        return OkxPublic.PublicAPI(flag=self._okx_flag)

    def _build_trading_api(self) -> Any | None:
        if OkxTrading is None:
            logger.warning("python-okx not installed; TradingDataAPI unavailable")
            return None
        return OkxTrading.TradingDataAPI(flag=self._okx_flag)

    def _build_trade_api(self) -> Any | None:
        if OkxTrade is None:
            logger.warning("okx SDK not installed; TradeAPI unavailable")
            return None
        if not (self.settings.okx_api_key and self.settings.okx_secret_key and self.settings.okx_passphrase):
            logger.warning("OKX credentials missing; TradeAPI disabled")
            return None
        raw_api = OkxTrade.TradeAPI(
            api_key=self.settings.okx_api_key,
            api_secret_key=self.settings.okx_secret_key,
            passphrase=self.settings.okx_passphrase,
            flag=self._okx_flag,
        )
        return OkxTradeAdapter(raw_api)

    def _rebuild_okx_clients(self) -> None:
        self._account_api = self._build_account_api()
        self._market_api = self._build_market_api()
        self._public_api = self._build_public_api()
        self._trading_api = self._build_trading_api()
        self._trade_api = self._build_trade_api()

    @staticmethod
    def _format_size(value: float) -> str:
        return (f"{value:.6f}".rstrip("0").rstrip(".") or "0") if value is not None else "0"

    @staticmethod
    def _format_price(value: float) -> str:
        return (f"{value:.8f}".rstrip("0").rstrip(".") or "0") if value is not None else "0"

    @staticmethod
    def _generate_client_order_id(prefix: str = "tai2") -> str:
        safe_prefix = "".join(ch for ch in (prefix or "") if ch.isalnum()) or "tai2"
        timestamp = str(int(time.time() * 1000))
        random_suffix = secrets.token_hex(3)
        value = f"{safe_prefix}{timestamp}{random_suffix}"
        return value[:32]

    def _normalize_order_response(self, response: Any) -> dict[str, Any] | None:
        if not isinstance(response, dict):
            return None
        top_code = str(response.get("code", ""))
        if top_code not in {"0", "200", ""}:
            detail = response.get("msg") or response
            self._emit_debug(f"OKX order rejected: code={top_code} detail={detail}")
            return None
        data = response.get("data")
        if isinstance(data, list) and data:
            entry = data[0]
            sub_code = str(entry.get("sCode", top_code))
            if sub_code not in {"0", "200", ""}:
                self._emit_debug(
                    f"OKX order failed: sCode={sub_code} sMsg={entry.get('sMsg')}"
                )
                self._emit_debug(f"OKX order failure payload: {entry}")
                return None
            return entry
        return response

    def _extract_order_error(self, response: Any) -> tuple[str, dict[str, Any]]:
        if not isinstance(response, dict):
            return ("OKX rejected the order", {})
        code = response.get("code")
        msg = response.get("msg")
        data = response.get("data")
        s_code = None
        s_msg = None
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                s_code = first.get("sCode")
                s_msg = first.get("sMsg") or first.get("msg")
        detail = s_msg or msg or "OKX rejected the order"
        suffix = f" (code={s_code or code})" if (s_code or code) else ""
        meta = {
            key: value
            for key, value in {
                "code": code,
                "message": msg,
                "sCode": s_code,
                "sMsg": s_msg,
            }.items()
            if value
        }
        return (f"{detail}{suffix}", meta)

    @staticmethod
    def _response_indicates_pos_side_error(response: Any) -> bool:
        def _entry_has_issue(entry: Any) -> bool:
            if entry is None:
                return False
            if isinstance(entry, dict):
                msg = str(entry.get("sMsg") or entry.get("msg") or "").lower()
                code = str(entry.get("sCode") or entry.get("code") or "")
                if "posside" in msg:
                    return True
                if code == "51000" and ("pos" in msg or not msg):
                    return True
                flattened = json.dumps(entry, default=str).lower()
                return "posside" in flattened
            if isinstance(entry, (list, tuple, set)):
                return any(_entry_has_issue(item) for item in entry)
            try:
                text = str(entry).lower()
            except Exception:
                return False
            return "posside" in text

        if isinstance(response, dict):
            if _entry_has_issue(response.get("data")):
                return True
            if _entry_has_issue(response.get("msg") or response.get("sMsg")):
                return True
            return _entry_has_issue(response)
        return _entry_has_issue(response)

    def _quantize_order_size(self, symbol: str, size: float) -> float | None:
        if size is None or size <= 0:
            return None
        spec = self._instrument_specs.get(symbol)
        if not spec:
            return size
        lot = spec.get("lot_size") or 0.0
        min_size = spec.get("min_size") or 0.0
        if lot > 0:
            multiples = math.floor((size + 1e-9) / lot)
            quantized = multiples * lot
        else:
            quantized = size
        if quantized < min_size and min_size > 0:
            return None
        return quantized if quantized > 0 else None

    def _quantize_price(self, symbol: str, price: float | None, *, prefer_up: bool) -> float | None:
        if price is None or price <= 0:
            return None
        spec = self._instrument_specs.get(symbol)
        tick = (spec or {}).get("tick_size") or 0.0
        if tick > 0:
            scaled = price / tick
            if prefer_up:
                quantized = math.ceil(scaled - 1e-9) * tick
            else:
                quantized = math.floor(scaled + 1e-9) * tick
            if quantized <= 0:
                quantized = tick
            return quantized
        return price

    def _normalize_take_profit(
        self,
        action: str,
        take_profit: float | None,
        reference_price: float | None,
    ) -> float | None:
        if take_profit is None or take_profit <= 0:
            return None
        if reference_price and reference_price > 0:
            if action == "BUY" and take_profit <= reference_price:
                self._emit_debug(
                    f"Ignoring take profit {take_profit:.6f}: BUY action expects target above {reference_price:.6f}"
                )
                return None
            if action == "SELL" and take_profit >= reference_price:
                self._emit_debug(
                    f"Ignoring take profit {take_profit:.6f}: SELL action expects target below {reference_price:.6f}"
                )
                return None
        return take_profit

    def _normalize_stop_loss(
        self,
        action: str,
        stop_loss: float | None,
        reference_price: float | None,
    ) -> float | None:
        if stop_loss is None or stop_loss <= 0:
            return None
        if reference_price and reference_price > 0:
            if action == "BUY" and stop_loss >= reference_price:
                self._emit_debug(
                    f"Ignoring stop loss {stop_loss:.6f}: BUY action expects protection below {reference_price:.6f}"
                )
                return None
            if action == "SELL" and stop_loss <= reference_price:
                self._emit_debug(
                    f"Ignoring stop loss {stop_loss:.6f}: SELL action expects protection above {reference_price:.6f}"
                )
                return None
        return stop_loss

    def _response_indicates_protection_error(self, response: Any) -> bool:
        def _match_entry(entry: dict[str, Any]) -> bool:
            code = str(entry.get("sCode") or entry.get("code") or "").strip()
            if code and code in self.PROTECTION_ERROR_CODES:
                return True
            message = str(entry.get("sMsg") or entry.get("msg") or "").lower()
            if not message:
                return False
            keywords = (
                "tp price",
                "stop price",
                "trigger price",
                "take profit",
                "stop loss",
            )
            return any(keyword in message for keyword in keywords)

        if isinstance(response, dict):
            if _match_entry(response):
                return True
            data = response.get("data")
            if isinstance(data, list):
                return any(_match_entry(entry) for entry in data if isinstance(entry, dict))
        return False

    @staticmethod
    def _target_conflicts_with_price(
        action: str,
        *,
        target: float | None,
        reference_price: float | None,
        kind: str,
    ) -> bool:
        if target is None or reference_price is None or reference_price <= 0:
            return False
        if kind == "take-profit":
            if action == "BUY":
                return target <= reference_price
            return target >= reference_price
        if kind == "stop-loss":
            if action == "BUY":
                return target >= reference_price
            return target <= reference_price
        return False

    def _drop_conflicting_target(
        self,
        *,
        symbol: str,
        action: str,
        target: float | None,
        reference_price: float | None,
        kind: str,
        stage: str,
    ) -> float | None:
        if not self._target_conflicts_with_price(
            action,
            target=target,
            reference_price=reference_price,
            kind=kind,
        ):
            return target
        if target is None or reference_price is None:
            return None
        direction = "above" if (action == "BUY" and kind == "take-profit") or (action == "SELL" and kind == "stop-loss") else "below"
        message = (
            f"{symbol} {kind} {target:.6f} invalid {stage}: must be {direction} entry price {reference_price:.6f}"
        )
        self._emit_debug(message)
        self._record_execution_feedback(
            symbol,
            message,
            level="warning",
            meta={
                "stage": stage,
                "kind": kind,
                "target": target,
                "reference_price": reference_price,
                "action": action,
            },
        )
        return None

    async def _submit_order(
        self,
        *,
        symbol: str,
        side: str,
        pos_side: str | None,
        size: float,
        trade_mode: str,
        order_type: str,
        reduce_only: bool,
        client_order_id: str,
        attach_algo_orders: list[dict[str, Any]] | None,
    ) -> tuple[dict[str, Any] | None, bool]:
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot place order")
            return None
        include_pos_side = pos_side
        attachments_to_use = attach_algo_orders
        attempt = 0
        while True:
            payload = {
                "instId": symbol,
                "tdMode": trade_mode,
                "side": side,
                "ordType": order_type,
                "sz": self._format_size(size),
                "clOrdId": client_order_id,
            }
            if include_pos_side:
                payload["posSide"] = include_pos_side
            if reduce_only:
                payload["reduceOnly"] = "true"
            if attachments_to_use:
                payload["attachAlgoOrds"] = attachments_to_use
            if self._sub_account and self._sub_account_use_master:
                payload["subAcct"] = self._sub_account

            trace_payload = {
                "instId": payload["instId"],
                "side": payload["side"],
                "tdMode": payload["tdMode"],
                "ordType": payload["ordType"],
                "sz": payload["sz"],
                "posSide": payload.get("posSide"),
                "reduceOnly": payload.get("reduceOnly"),
                "subAcct": bool(payload.get("subAcct")),
                "clientOrderId": payload.get("clOrdId"),
            }
            self._emit_debug(f"OKX order payload: {trace_payload}")

            def _place() -> Any:
                return self._trade_api.place_order(**payload)

            try:
                response = await asyncio.to_thread(_place)
            except Exception as exc:  # pragma: no cover - network dependency
                self._emit_debug(f"OKX place_order exception: {exc}")
                return None, False
            self._emit_debug(f"OKX order response raw: {response}")
            normalized = self._normalize_order_response(response)
            if normalized:
                return normalized, bool(attachments_to_use)
            if include_pos_side and self._response_indicates_pos_side_error(response):
                self._emit_debug("Retrying OKX order without posSide for net-mode account")
                include_pos_side = None
                attempt += 1
                continue
            if attachments_to_use and self._response_indicates_protection_error(response):
                self._emit_debug(
                    f"OKX rejected TP/SL attachment for {symbol}; retrying order without protection"
                )
                self._record_execution_feedback(
                    symbol,
                    "OKX rejected TP/SL attachment; order retried without protection",
                    level="warning",
                )
                attachments_to_use = None
                continue
            error_message, error_meta = self._extract_order_error(response)
            self._record_execution_feedback(
                symbol,
                error_message,
                level="error",
                meta=error_meta or None,
            )
            return None, False

    async def _record_trade_execution(
        self,
        *,
        symbol: str,
        side: str,
        price: float,
        amount: float,
        rationale: str | None,
        fee: float | None,
    ) -> None:
        if price is None or amount is None:
            return
        symbol_key = symbol.upper()
        self._position_activity[symbol_key] = time.time()
        if not self.settings.database_url:
            return
        try:
            trade = ExecutedTrade(
                symbol=symbol,
                instrument=symbol,
                size=Decimal(str(amount)) if amount is not None else None,
                side=side,
                price=Decimal(str(price)),
                amount=Decimal(str(amount)),
                llm_reasoning=rationale,
                fee=Decimal(str(fee)) if fee is not None else None,
            )
            await insert_executed_trade(trade)
        except Exception as exc:  # pragma: no cover - persistence best-effort
            self._emit_debug(f"Failed to persist executed trade: {exc}")

    @staticmethod
    def _build_tpsl_client_id(symbol: str) -> str:
        sanitized = "".join(ch for ch in str(symbol) if ch.isalnum()).lower() or "symbol"
        value = f"tai2{sanitized}tpsl"
        return value[:32]

    async def _refresh_position_protection(
        self,
        *,
        symbol: str,
        trade_mode: str,
        action: str,
        take_profit_price: float | None,
        stop_loss_price: float | None,
        dual_side_mode: bool,
        pos_side: str | None,
    ) -> None:
        if not self._trade_api:
            return
        symbol_key = symbol.upper()
        await self._cancel_position_protection(symbol)
        if not (take_profit_price or stop_loss_price):
            self._position_protection.pop(symbol_key, None)
            return
        pending_meta = {
            "take_profit": take_profit_price,
            "stop_loss": stop_loss_price,
            "algo_id": None,
            "algo_cl_ord_id": self._build_tpsl_client_id(symbol),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synced": False,
        }
        placement = await self._place_position_protection(
            symbol=symbol,
            trade_mode=trade_mode,
            action=action,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            dual_side_mode=dual_side_mode,
            pos_side=pos_side,
        )
        if placement:
            confirmed = bool(placement.get("confirmed"))
            pending_meta.update(
                {
                    "algo_id": placement.get("algo_id"),
                    "algo_cl_ord_id": placement.get("algo_cl_ord_id")
                    or pending_meta.get("algo_cl_ord_id"),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "synced": confirmed,
                }
            )
            if not confirmed:
                self._emit_debug(
                    f"OKX pending list does not show TP/SL algo for {symbol}; guard left unsynced"
                )
        self._position_protection[symbol_key] = pending_meta

    async def _cancel_position_protection(self, symbol: str) -> None:
        if not self._trade_api:
            return
        meta = self._position_protection.pop(symbol.upper(), None) or {}
        client_id = (meta.get("algo_cl_ord_id") or self._build_tpsl_client_id(symbol))
        payload_entry: dict[str, Any] = {"instId": symbol}
        if meta.get("algo_id"):
            payload_entry["algoId"] = meta["algo_id"]
        else:
            payload_entry["algoClOrdId"] = client_id
        payload = [payload_entry]
        if self._sub_account and self._sub_account_use_master:
            payload[0]["subAcct"] = self._sub_account
        try:
            await asyncio.to_thread(self._trade_api.cancel_algo_order, payload)
        except Exception as exc:  # pragma: no cover - network dependency
            self._emit_debug(f"Failed to cancel TP/SL algo for {symbol}: {exc}")

    async def _fetch_latest_symbol_protection(self, symbol: str, *, pos_side: str | None = None) -> dict[str, Any] | None:
        if not self._trade_api:
            return None
        symbol_key = symbol.upper()

        def _call(state: str) -> Any:
            kwargs: dict[str, Any] = {
                "state": state,
                "instId": symbol,
                "ordType": "conditional",
            }
            if self._sub_account and self._sub_account_use_master:
                kwargs["subAcct"] = self._sub_account
            return self._trade_api.list_algo_orders(**kwargs)

        candidates: list[dict[str, Any]] = []
        for state in ("live",):
            try:
                response = await asyncio.to_thread(_call, state)
            except Exception as exc:  # pragma: no cover - network dependency
                self._emit_debug(f"Failed to query {state} protection for {symbol}: {exc}")
                continue
            entries = self._safe_data(response)
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_symbol = str(entry.get("instId") or "").upper()
                if entry_symbol != symbol_key:
                    continue
                ord_type = str(entry.get("ordType") or "").lower()
                if ord_type not in {"conditional", "oco"}:
                    continue
                reduce_only = str(entry.get("reduceOnly") or "").strip().lower()
                if reduce_only not in {"true", "1", "yes"}:
                    continue
                if pos_side:
                    remote_side = str(entry.get("posSide") or "").upper()
                    if remote_side and remote_side != pos_side:
                        continue
                candidates.append(entry)
        if not candidates:
            return None

        def _updated(entry: dict[str, Any]) -> float:
            timestamp = self._extract_float(
                entry.get("updateTime") or entry.get("uTime") or entry.get("cTime")
            )
            return timestamp or 0.0

        candidates.sort(key=_updated, reverse=True)
        return candidates[0]

    async def _fetch_algo_order(
        self,
        *,
        symbol: str,
        algo_client_id: str | None = None,
        order_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not self._trade_api:
            return None
        if not (algo_client_id or order_id):
            return None

        def _call(state: str) -> Any:
            sub_account = self._sub_account if self._sub_account_use_master else None
            return self._trade_api.list_algo_orders(
                state=state,
                instId=symbol,
                ordType="conditional",
                algoClOrdId=algo_client_id,
                ordId=order_id,
                subAcct=sub_account,
                history_state="triggered" if state != "live" else None,
            )

        def _select_entry(response: Any) -> dict[str, Any] | None:
            entries = self._safe_data(response)
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                client_match = False
                if algo_client_id:
                    algo_keys = [
                        str(entry.get("algoClOrdId") or ""),
                        str(entry.get("algoId") or ""),
                    ]
                    client_match = algo_client_id in algo_keys
                order_match = False
                if order_id:
                    order_match = str(entry.get("ordId") or entry.get("orderId") or "") == order_id
                if client_match or order_match:
                    return entry
            return entries[0] if entries else None

        for state in ("live", "history"):
            try:
                response = await asyncio.to_thread(_call, state)
            except Exception as exc:  # pragma: no cover - network dependency
                self._emit_debug(
                    f"Failed to query {state} TP/SL algos for {symbol}: {exc}"
                )
                continue
            match = _select_entry(response)
            if match:
                match["_source_state"] = state
                return match
        return None

    def _build_attach_algo_orders(
        self,
        *,
        take_profit_price: float | None,
        stop_loss_price: float | None,
    ) -> list[dict[str, Any]] | None:
        if not (take_profit_price or stop_loss_price):
            return None
        attach_payload: dict[str, Any] = {}
        if take_profit_price:
            attach_payload.update(
                {
                    "tpTriggerPx": self._format_price(take_profit_price),
                    "tpOrdPx": "-1",
                    "tpTriggerPxType": "last",
                }
            )
        if stop_loss_price:
            attach_payload.update(
                {
                    "slTriggerPx": self._format_price(stop_loss_price),
                    "slOrdPx": "-1",
                    "slTriggerPxType": "last",
                }
            )
        return [attach_payload] if attach_payload else None

    async def _place_position_protection(
        self,
        *,
        symbol: str,
        trade_mode: str,
        action: str,
        take_profit_price: float | None,
        stop_loss_price: float | None,
        dual_side_mode: bool,
        pos_side: str | None,
    ) -> dict[str, str] | None:
        if not (take_profit_price or stop_loss_price):
            return None
        if not self._trade_api:
            return None
        detected_size = await self._wait_for_position(
            symbol,
            pos_side=pos_side if dual_side_mode else None,
        )
        if detected_size is None:
            self._emit_debug(
                f"Skipping TP/SL algo for {symbol}: position not yet confirmed"
            )
            return None
        close_size = abs(detected_size)
        quantized_close_size = self._quantize_order_size(symbol, close_size) or close_size
        payload: dict[str, Any] = {
            "instId": symbol,
            "tdMode": trade_mode,
            "side": "sell" if action == "BUY" else "buy",
            "ordType": "conditional",
            "reduceOnly": "true",
            "algoClOrdId": self._build_tpsl_client_id(symbol),
            "cxlOnClosePos": "true",
        }
        if quantized_close_size > 0:
            payload["sz"] = self._format_size(quantized_close_size)
        if take_profit_price:
            payload["tpTriggerPx"] = self._format_price(take_profit_price)
            payload["tpOrdPx"] = "-1"
            payload["tpTriggerPxType"] = "last"
        if stop_loss_price:
            payload["slTriggerPx"] = self._format_price(stop_loss_price)
            payload["slOrdPx"] = "-1"
            payload["slTriggerPxType"] = "last"
        self._emit_debug(
            f"Submitting TP/SL algo for {symbol} | sz={payload.get('sz')} tp={payload.get('tpTriggerPx')} sl={payload.get('slTriggerPx')} posSide={pos_side or 'unset'}"
        )
        include_pos_side = pos_side if pos_side else None
        tried_with_pos_side = False
        tried_without_pos_side = False
        while True:
            submission = dict(payload)
            if include_pos_side:
                submission["posSide"] = include_pos_side
                tried_with_pos_side = True
            else:
                tried_without_pos_side = True
            if self._sub_account and self._sub_account_use_master:
                submission["subAcct"] = self._sub_account
            try:
                response = await asyncio.to_thread(self._trade_api.place_algo_order, **submission)
            except Exception as exc:  # pragma: no cover - network dependency
                self._emit_debug(f"Failed to place TP/SL algo for {symbol}: {exc}")
                return None
            normalized = self._normalize_order_response(response)
            if normalized:
                algo_id = normalized.get("algoId") or normalized.get("algoClOrdId")
                self._emit_debug(
                    f"Registered TP/SL algo {algo_id or payload['algoClOrdId']} for {symbol}"
                )
                remote_entry = await self._fetch_algo_order(
                    symbol=symbol,
                    algo_client_id=payload["algoClOrdId"],
                )
                if remote_entry:
                    source_state = remote_entry.get("_source_state", "live")
                    tp_px = remote_entry.get("tpTriggerPx") or payload.get("tpTriggerPx")
                    sl_px = remote_entry.get("slTriggerPx") or payload.get("slTriggerPx")
                    self._emit_debug(
                        f"OKX reports TP/SL algo {algo_id or payload['algoClOrdId']} for {symbol}: "
                        f"state={source_state} tp={tp_px} sl={sl_px}"
                    )
                else:
                    self._emit_debug(
                        f"Unable to find TP/SL algo {payload['algoClOrdId']} for {symbol} via OKX pending/history APIs"
                    )
                return {
                    "algo_id": algo_id,
                    "algo_cl_ord_id": payload["algoClOrdId"],
                    "confirmed": bool(remote_entry),
                }
            if include_pos_side and not tried_without_pos_side:
                self._emit_debug(
                    f"Retrying TP/SL algo for {symbol} without posSide after OKX rejection"
                )
                include_pos_side = None
                continue
            if pos_side and not tried_with_pos_side:
                self._emit_debug(
                    f"Retrying TP/SL algo for {symbol} with posSide after OKX rejection"
                )
                include_pos_side = pos_side
                continue
            self._emit_debug(f"OKX rejected TP/SL algo for {symbol}: {response}")
            return None

    async def _confirm_attached_protection(
        self,
        *,
        symbol: str,
        order_id: str,
        take_profit_price: float | None,
        stop_loss_price: float | None,
    ) -> bool:
        attempts = 3
        remote_entry: dict[str, Any] | None = None
        for attempt in range(attempts):
            remote_entry = await self._fetch_algo_order(symbol=symbol, order_id=str(order_id))
            if remote_entry:
                break
            await asyncio.sleep(0.35)
        if not remote_entry:
            self._emit_debug(
                f"Attached TP/SL for {symbol} not visible on OKX after {attempts} checks"
            )
            return False
        tp_value = self._extract_float(remote_entry.get("tpTriggerPx")) or take_profit_price
        sl_value = self._extract_float(remote_entry.get("slTriggerPx")) or stop_loss_price
        symbol_key = symbol.upper()
        meta = {
            "take_profit": tp_value,
            "stop_loss": sl_value,
            "algo_id": remote_entry.get("algoId"),
            "algo_cl_ord_id": remote_entry.get("algoClOrdId"),
            "attached_ord_id": str(order_id),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "synced": True,
            "method": "attach",
        }
        self._position_protection[symbol_key] = meta
        self._emit_debug(
            f"Attached TP/SL confirmed for {symbol}: algo={meta.get('algo_id') or meta.get('algo_cl_ord_id')}"
        )
        return True


__all__ = ["MarketService"]
