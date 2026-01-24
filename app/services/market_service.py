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
    import okx.Funding as OkxFunding
    from okx.websocket.WsPublicAsync import WsPublicAsync
except ImportError:  # pragma: no cover
    OkxAccount = None
    OkxTrade = None
    OkxMarket = None
    OkxPublic = None
    OkxTrading = None
    OkxFunding = None
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
FUNDING_ACCOUNT_TYPE = "6"
TRADING_ACCOUNT_TYPE = "18"
INSUFFICIENT_MARGIN_CODES = {"59300"}
ORDER_INSUFFICIENT_MARGIN_CODES = {"51008"}
 

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
    PROTECTION_MIN_OFFSET_RATIO = 0.001  # 0.1% of entry price
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
        funding_api: Any | None = None,
        websocket_factory: Callable[[str], WsPublicAsync] | None = None,
        enable_websocket: bool = True,
        log_sink: Callable[[str], None] | None = None,
        ohlc_bar: str | None = None,
    ) -> None:
        """Configure in-memory caches, API clients, and symbol set for a service run."""
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
        self._funding_api = funding_api or self._build_funding_api()
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
        self._last_margin_guidance: dict[str, dict[str, Any]] = {}
        self._isolated_leverage_cache: dict[str, float] = {}
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
        """Launch the market snapshot poller and websocket consumers if not already running."""
        if self._poller_task:
            return
        await self._hydrate_cached_annotations()
        if self._enable_websocket:
            self._ws_task = asyncio.create_task(self._run_public_ws(), name="okx-ws")
        self._poller_task = asyncio.create_task(self._poll_loop(), name="okx-market-poller")
        logger.info("MarketService started for %s", ", ".join(self.symbols))

    async def stop(self) -> None:
        """Cancel background tasks, tear down websockets, and reset runtime bookkeeping."""
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
        """Continuously refresh state snapshots on a fixed interval until cancelled."""
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
        """Subscribe to public OKX feeds and stream updates into the in-memory caches."""
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
        """Route websocket frames into ticker/order book/liquidation caches for active symbols."""
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
        """Throttle websocket debug logging so repeated updates do not spam the log sink."""
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
        """Return the standardized OKX channel subscription payload for the provided symbols."""
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
        """Collect positions, balances, and market data, returning the synced snapshot payload."""
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
        """Serialize in-memory TP/SL metadata so it can be persisted with the snapshot."""
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
        """Attach cached TP/SL and last-trade details onto OKX position rows."""
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
        """Aggregate long/short exposure for a single instrument from raw OKX position entries."""
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

    def _compute_open_position_notional(
        self,
        positions: list[dict[str, Any]] | None,
        *,
        price_hints: dict[str, float] | None = None,
    ) -> float:
        """Estimate total notional tied up across all open positions using avg/mark prices."""
        if not positions:
            return 0.0
        total = 0.0
        hints = price_hints or {}
        breakdown: list[dict[str, Any]] = []
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            size_value = self._extract_float(entry.get("pos") or entry.get("size"))
            if size_value is None or size_value == 0:
                continue
            abs_size = abs(size_value)
            instrument = str(entry.get("instId") or entry.get("symbol") or "").upper()
            price_value = None
            price_source = None
            for candidate_key in ("avgPx", "markPx", "last", "px"):
                candidate_value = self._extract_float(entry.get(candidate_key))
                if candidate_value is not None and candidate_value > 0:
                    price_value = candidate_value
                    price_source = candidate_key
                    break
            if (price_value is None or price_value <= 0) and instrument:
                hint_value = hints.get(instrument)
                if hint_value is not None and hint_value > 0:
                    price_value = hint_value
                    price_source = "hint"
            if (price_value is None or price_value <= 0) and instrument:
                ticker = self._latest_ticker.get(instrument)
                if isinstance(ticker, dict):
                    ticker_value = self._extract_float(ticker.get("last") or ticker.get("markPx"))
                    if ticker_value is not None and ticker_value > 0:
                        price_value = ticker_value
                        price_source = "ticker"
            if price_value is None or price_value <= 0:
                continue
            notional = abs_size * price_value
            total += notional
            breakdown.append(
                {
                    "symbol": instrument,
                    "abs_size": abs_size,
                    "price": price_value,
                    "notional": notional,
                    "price_source": price_source,
                }
            )
        if breakdown:
            try:
                self._emit_debug(
                    f"Open notional breakdown: {json.dumps({'positions': breakdown, 'total': total})}",
                    mirror_logger=False,
                )
            except Exception:  # pragma: no cover - defensive
                self._emit_debug(
                    f"Open notional breakdown: positions={breakdown} total={total}",
                    mirror_logger=False,
                )
        return total

    async def _sync_position_protection_entries(self, positions: list[dict[str, Any]] | None) -> None:
        """Periodically reconcile cached TP/SL state with OKX Algo order records."""
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
        """Normalize symbol identifiers to upper-case keys suitable for dict indexing."""
        if symbol is None:
            return None
        value = str(symbol).strip().upper()
        return value or None

    @staticmethod
    def _parse_cached_timestamp(value: Any) -> float | None:
        """Convert cached ISO timestamps into epoch seconds for comparison logic."""
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
        """Convert OKX-provided millisecond timestamps into RFC3339 strings."""
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
        """Reload TP/SL and last-trade hints from Redis so warm restarts retain context."""
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
        """Update the REST polling cadence and matching websocket debug interval."""
        self._poll_interval = max(1, seconds)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._emit_debug(f"Poll interval updated to {self._poll_interval}s")

    def set_wait_for_tp_sl(self, enabled: bool) -> None:
        """Toggle the guardrail that delays new entries until TP/SL anchors exist."""
        flag = bool(enabled)
        if flag == self._wait_for_tp_sl:
            return
        self._wait_for_tp_sl = flag
        state = "enabled" if flag else "disabled"
        self._emit_debug(f"Wait-for-TP/SL guard {state}")

    async def set_websocket_enabled(self, enabled: bool) -> None:
        """Enable or disable websocket streaming at runtime, rebuilding tasks as needed."""
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
        """Update the sub-account routing preferences and publish a fresh snapshot."""
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
        """Force a snapshot rebuild, push it to Redis, and record latest equity metrics."""
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
        """Ensure the OKX environment flag is either "0" (live) or "1" (paper)."""
        if isinstance(flag, str) and flag.strip() == "1":
            return "1"
        if isinstance(flag, int) and flag == 1:
            return "1"
        return "0"

    async def set_okx_flag(self, value: str | int | None) -> None:
        """Switch between live and paper API environments and rebuild API clients."""
        normalized = self._normalize_okx_flag(value)
        if normalized == self._okx_flag:
            return
        self._okx_flag = normalized
        env_label = "LIVE" if normalized == "0" else "PAPER"
        self._emit_debug(f"OKX API environment set to {env_label} (flag={normalized})")
        self._rebuild_okx_clients()
        await self._publish_snapshot()

    async def update_symbols(self, symbols: list[str]) -> None:
        """Replace the tracked symbol list, syncing caches and websocket subscriptions."""
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
        """Resubscribe the websocket client to reflect symbol changes."""
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
        """Clear cached market and execution data for the provided symbol set."""
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
        """Return the latest known ticker for a symbol without triggering a network call."""
        normalized = self._normalize_symbols([symbol]) if symbol else []
        if not normalized:
            return None
        return self._latest_ticker.get(normalized[0])

    def get_last_price(self, symbol: str | None) -> float | None:
        """Shortcut helper to fetch the cached last-traded price for a symbol."""
        ticker = self.get_cached_ticker(symbol)
        return self._price_from_ticker(ticker)

    async def _publish_snapshot(self) -> None:
        """Build and persist a snapshot, logging but ignoring publish failures."""
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
        """Deduplicate and upper-case user-provided instrument identifiers."""
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
        """Return cached OKX instrument list, fetching from the API once if needed."""
        if self._available_symbols:
            return list(self._available_symbols)
        symbols = await self._fetch_available_symbols()
        self._available_symbols = symbols
        return list(self._available_symbols)

    async def _fetch_available_symbols(self) -> list[str]:
        """Call the public instruments endpoint and hydrate instrument specs cache."""
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
                max_market_size = self._extract_float(entry.get("maxMktSz"))
                max_limit_size = self._extract_float(entry.get("maxLmtSz"))
                self._instrument_specs[symbol] = {
                    "lot_size": lot_size or 0.0,
                    "min_size": min_size or 0.0,
                    "tick_size": tick_size or 0.0,
                    "max_market_size": max_market_size or 0.0,
                    "max_limit_size": max_limit_size or 0.0,
                }
        if not pairs:
            return list(self.symbols)
        return sorted(set(pairs))

    def _tier_cache_key(self, symbol: str, trade_mode: str) -> str:
        """Key helper for memoizing tier metadata per instrument and trade mode."""
        normalized_symbol = (symbol or "").upper()
        normalized_mode = (trade_mode or "cross").lower()
        return f"{normalized_mode}:{normalized_symbol}"

    @staticmethod
    def _instrument_family(symbol: str | None) -> str | None:
        """Reduce an instrument ID into its family identifier (e.g., BTC-USDT)."""
        if not symbol:
            return None
        parts = str(symbol).upper().split("-")
        if len(parts) >= 2:
            return "-".join(parts[:2])
        return None

    @staticmethod
    def _quote_currency_from_symbol(symbol: str | None) -> str | None:
        """Extract the quote currency (middle segment) from an OKX instrument ID."""
        if not symbol:
            return None
        parts = str(symbol).upper().split("-")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
        return None

    async def _get_position_tiers(self, symbol: str, trade_mode: str = "cross") -> list[dict[str, Any]]:
        """Return cached or freshly fetched OKX position tier definitions."""
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
        """Hit the OKX public tier endpoint and normalize its payload."""
        if not self._public_api or not hasattr(self._public_api, "get_position_tiers"):
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
        """Pick the tier whose [min,max] bracket would include the contemplated position size."""
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
        """Clamp requested size against tier IMR constraints and report the adjusted sizing metadata."""
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
        """Pull current SWAP positions from the account API, optionally filtering by instrument."""
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
        """Return the numeric position size for a given instrument/side if one exists."""
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
        """Retry position lookups for a short window to confirm execution took effect."""
        normalized = symbol.upper()
        for attempt in range(attempts):
            size = await self._position_size(normalized, pos_side=pos_side)
            if size is not None and abs(size) > 0:
                return size
            if attempt < attempts - 1:
                await asyncio.sleep(delay)
        return None

    async def _fetch_account_balance(self) -> dict[str, Any]:
        """Fetch account balances (respecting sub-account routing) and normalize the payload."""
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
        """Return the cached order book or fetch the latest depth snapshot for a symbol."""
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
        """Return the cached ticker or fetch the most recent OKX ticker for a symbol."""
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
        """Return current funding-rate metadata for the provided instrument."""
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
        """Return open-interest stats, preferring cached values when fresh."""
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
        """Fetch OHLCV candles (cached fallback) for use in indicator calculations."""
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
        """Derive proprietary metrics (CVD/OFI/etc.) from cached trades and current depth."""
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
        """Build the technical indicator bundle consumed by downstream strategy logic."""
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
        """Return (and lazily create) the rolling trade buffer for a symbol."""
        buffer = self._trade_buffers.get(symbol)
        if buffer is None:
            buffer = deque(maxlen=500)
            self._trade_buffers[symbol] = buffer
        return buffer

    def _calculate_cvd(self, symbol: str) -> float:
        """Compute the cumulative volume delta for the instrument's buffered trades."""
        value = 0.0
        for trade in self._get_trade_buffer(symbol):
            volume = trade.get("volume", 0.0)
            direction = trade.get("side", 0.0)
            value += direction * volume
        return value

    def _build_cvd_series(self, symbol: str, limit: int = 200) -> list[float]:
        """Return the historical CVD series (bounded) for visualization/analytics."""
        values: list[float] = []
        running = 0.0
        for trade in self._get_trade_buffer(symbol):
            volume = trade.get("volume", 0.0)
            direction = trade.get("side", 0.0)
            running += direction * volume
            values.append(running)
        return values[-limit:]

    async def _fetch_long_short_ratio(self, symbol: str) -> dict[str, Any]:
        """Fetch or return cached OKX long/short ratio telemetry for the symbol."""
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
        """Blend indicator and custom metric inputs into a simplified BUY/SELL/HOLD signal."""
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
        """Summarize ATR-based risk metrics that power downstream risk guidance."""
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
        """Calculate order-flow imbalance metrics from the latest order book snapshot."""
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
        """Aggregate equity values from OKX account payload rows (recursing into details)."""
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
        """Normalize OKX balance payloads into a flattened structure the service expects."""
        details: list[dict[str, Any]] = []
        total_equity = 0.0
        total_account_value = 0.0
        total_eq_usd = 0.0
        balances: dict[str, dict[str, float]] = {}
        available_equity_total: float | None = None
        available_eq_usd_total: float | None = None
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
                available_equity_total = (
                    avail_eq_value
                    if available_equity_total is None
                    else available_equity_total + avail_eq_value
                )
            if avail_usd_value is not None:
                bucket["available_usd"] += avail_usd_value
                available_eq_usd_total = (
                    avail_usd_value
                    if available_eq_usd_total is None
                    else available_eq_usd_total + avail_usd_value
                )
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
            "available_equity": available_equity_total,
            "available_eq_usd": available_eq_usd_total,
            "available_balances": cleaned_balances,
        }

    @staticmethod
    def _extract_equity_value(record: dict[str, Any]) -> float | None:
        """Extract the most relevant numeric equity figure from a balance record."""
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
        """Safely coerce arbitrary types to float, returning None on failure."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _emit_debug(self, message: str, *, mirror_logger: bool = True) -> None:
        """Send human-friendly diagnostics to the injected sink and optionally the logger."""
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
        """Persist computed execution caps (margin, leverage, quote liquidity) per symbol."""
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
        recommendation: dict[str, Any] | None = None,
    ) -> None:
        """Append a feedback entry and echo warnings/errors through the debug sink."""
        entry = {
            "symbol": symbol,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
        }
        if meta:
            entry["meta"] = meta
        if recommendation:
            entry["recommendation"] = recommendation
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

    def _set_margin_guidance(self, symbol: str, payload: dict[str, Any] | None) -> None:
        """Cache or clear the latest isolated-margin guidance for a symbol."""
        key = self._normalize_symbol_key(symbol)
        if not key:
            return
        if payload:
            self._last_margin_guidance[key] = payload
        else:
            self._last_margin_guidance.pop(key, None)

    def _get_margin_guidance(self, symbol: str) -> dict[str, Any] | None:
        """Return any cached margin guidance metadata for the instrument."""
        key = self._normalize_symbol_key(symbol)
        if not key:
            return None
        return self._last_margin_guidance.get(key)

    def _merge_margin_guidance(self, symbol: str, updates: dict[str, Any] | None) -> None:
        """Merge partial guidance telemetry into the cache without discarding previous fields."""
        if not updates:
            return
        key = self._normalize_symbol_key(symbol)
        if not key:
            return
        existing = self._last_margin_guidance.get(key)
        if not isinstance(existing, dict):
            existing = {}
        else:
            existing = dict(existing)
        mutated = False
        for field, value in updates.items():
            if value is None:
                continue
            if existing.get(field) == value:
                continue
            existing[field] = value
            mutated = True
        if mutated:
            self._last_margin_guidance[key] = existing

    def _build_margin_meta_snapshot(self, symbol: str) -> dict[str, Any] | None:
        """Return a trimmed, telemetry-friendly view of the latest margin guidance."""
        context = self._get_margin_guidance(symbol)
        if not isinstance(context, dict):
            return None
        keys_of_interest = {
            "quote_currency",
            "required_gap",
            "seed_limit",
            "auto_seed_attempted",
            "auto_seed_success",
            "auto_seed_configured",
            "blocked_reason",
            "funding_available",
            "updated_at",
            "requested_size",
            "requested_notional",
            "initial_requested_size",
            "initial_requested_notional",
            "auto_downsize_previous_size",
            "auto_downsize_target_size",
            "auto_downsize_previous_notional",
            "auto_downsize_target_notional",
            "auto_downsize_notional_delta",
            "auto_downsize_required_gap",
            "auto_downsize_scale",
            "auto_downsize_price",
            "account_equity",
            "open_position_notional",
            "equity_available_for_trade",
            "equity_clip_active",
            "equity_clip_reason",
            "equity_clip_requested_size",
            "equity_clip_target_size",
            "equity_clip_requested_notional",
            "equity_clip_target_notional",
            "equity_clip_notional_delta",
        }
        snapshot: dict[str, Any] = {}
        for key in keys_of_interest:
            if key in context:
                snapshot[key] = context.get(key)
        return snapshot or None

    def _log_margin_guidance_snapshot(self, symbol: str, *, context: str) -> None:
        """Emit the currently stored margin guidance for observability troubleshooting."""
        guidance = self._get_margin_guidance(symbol)
        if guidance:
            try:
                payload = json.dumps(guidance, default=str, sort_keys=True)
            except Exception:
                payload = str(guidance)
            self._emit_debug(f"Margin guidance snapshot ({context}) {symbol}: {payload}")
        else:
            self._emit_debug(f"Margin guidance snapshot ({context}) {symbol}: <empty>")

    def _should_attach_margin_recommendation(
        self,
        error_meta: dict[str, Any] | None,
        error_message: str | None,
    ) -> bool:
        """Detect whether an order failure corresponds to insufficient margin scenarios."""
        codes: set[str] = set()
        if error_meta:
            for key in ("sCode", "code"):
                value = error_meta.get(key)
                if value in (None, ""):
                    continue
                codes.add(str(value))
        for code in codes:
            if code in ORDER_INSUFFICIENT_MARGIN_CODES:
                return True
        text_blocks = []
        if error_message:
            text_blocks.append(str(error_message))
        if error_meta:
            text_blocks.append(json.dumps(error_meta, default=str))
        combined = " ".join(text_blocks).lower()
        if "insufficient" in combined and "margin" in combined:
            return True
        return False

    def _build_margin_recommendation(self, symbol: str) -> dict[str, Any] | None:
        """Craft a human-readable remediation note leveraging cached guidance context."""
        context = self._get_margin_guidance(symbol)
        if not context:
            return None
        needed = self._extract_float(context.get("required_gap"))
        if not needed or needed <= 0:
            return None
        quote_currency = str(context.get("quote_currency") or "USDT").upper()
        limit = self._extract_float(context.get("seed_limit"))
        funding_available = self._extract_float(context.get("funding_available"))
        attempted = bool(context.get("auto_seed_attempted"))
        success = bool(context.get("auto_seed_success"))
        blocked_reason = context.get("blocked_reason")
        configured = bool(context.get("auto_seed_configured")) or bool(limit and limit > 0)
        message: str
        if not attempted:
            if not configured:
                message = (
                    f"Configure isolated-margin auto-seed for {symbol} to cover ~{needed:.2f} {quote_currency} by "
                    "setting 'isolated_margin_seed_usd' or a symbol override."
                )
            else:
                cap_text = f"{limit:.2f} {quote_currency}" if limit else "the current limit"
                message = (
                    f"Auto-seed cap {cap_text} was not triggered; raise the limit or reduce request so at least {needed:.2f} {quote_currency} can move into isolated margin."
                )
        elif not success:
            if blocked_reason == "limit_exceeded":
                if limit and limit > 0:
                    message = (
                        f"Increase isolated seed cap for {symbol} above {needed:.2f} {quote_currency} "
                        f"(current cap {limit:.2f})."
                    )
                else:
                    message = (
                        f"Increase isolated seed cap to at least {needed:.2f} {quote_currency}."
                    )
            elif blocked_reason == "funding_insufficient":
                available = funding_available if funding_available is not None else 0.0
                message = (
                    f"Funding wallet only has {available:.2f} {quote_currency}; deposit or transfer ≥{needed:.2f} to enable auto-seed."
                )
            elif blocked_reason == "transfer_error":
                message = (
                    "Funding transfer failed; verify Funding API permissions and account status."
                )
            elif blocked_reason == "funding_api_unavailable":
                message = (
                    "Enable Funding API credentials so the engine can transfer collateral automatically."
                )
            elif blocked_reason == "transfer_rejected":
                message = (
                    "OKX rejected the funding transfer; confirm sub-account permissions and daily transfer caps."
                )
            elif blocked_reason == "no_limit_configured":
                message = (
                    f"Set 'isolated_margin_seed_usd' or add a {symbol} override so the engine can move {needed:.2f} {quote_currency}."
                )
            else:
                message = (
                    f"Auto-seed blocked ({blocked_reason or 'unknown reason'}); ensure guardrail limits cover {needed:.2f} {quote_currency}."
                )
        else:
            message = (
                f"OKX still reported insufficient margin after transferring {needed:.2f} {quote_currency}. Increase guardrail limits or reduce position size."
            )
        recommendation = dict(context)
        recommendation.update(
            {
                "message": message,
                "needed": needed,
                "quote_currency": quote_currency,
                "seed_limit": limit,
                "funding_available": funding_available,
            }
        )
        return recommendation

    def _fallback_margin_recommendation(self, symbol: str) -> dict[str, Any]:
        """Produce a generic margin recommendation when detailed guidance is unavailable."""
        quote_currency = self._quote_currency_from_symbol(symbol) or "USDT"
        guidance = self._get_margin_guidance(symbol)
        payload: dict[str, Any] = {}
        if isinstance(guidance, dict):
            payload.update(guidance)
        payload.update(
            {
                "message": (
                    f"Transfer additional {quote_currency} collateral into the trading account "
                    f"or raise the isolated margin seed guardrail for {symbol}."
                ),
                "quote_currency": quote_currency,
            }
        )
        if "needed" not in payload:
            required_gap = self._extract_float(payload.get("required_gap"))
            if required_gap is not None:
                payload["needed"] = required_gap
        if "seed_limit" not in payload and guidance:
            seed_limit = self._extract_float(guidance.get("seed_limit"))
            if seed_limit is not None:
                payload["seed_limit"] = seed_limit
        if "funding_available" not in payload and guidance:
            funding_available = self._extract_float(guidance.get("funding_available"))
            if funding_available is not None:
                payload["funding_available"] = funding_available
        return payload

    def _summarize_margin_recommendation(self, recommendation: dict[str, Any] | None) -> str | None:
        """Condense a recommendation dict into a compact semicolon-delimited summary."""
        if not isinstance(recommendation, dict):
            return None
        bits: list[str] = []
        currency = str(recommendation.get("quote_currency") or "").upper()

        def _fmt_amount(value: Any) -> str | None:
            numeric = self._extract_float(value)
            if numeric is None:
                return None
            label = f"{numeric:,.2f}"
            if currency:
                label = f"{label} {currency}"
            return label

        need_label = _fmt_amount(recommendation.get("needed"))
        if need_label:
            bits.append(f"need≈{need_label}")
        cap_label = _fmt_amount(recommendation.get("seed_limit"))
        if cap_label:
            bits.append(f"cap={cap_label}")
        funding_label = _fmt_amount(recommendation.get("funding_available"))
        if funding_label:
            bits.append(f"funding={funding_label}")

        target_size = self._extract_float(
            recommendation.get("auto_downsize_target_size")
            or recommendation.get("auto_downsize_previous_size")
        )
        if target_size is not None:
            bits.append(f"target_size={target_size:,.4f}")
        scale = self._extract_float(recommendation.get("auto_downsize_scale"))
        if scale is not None and scale > 0:
            bits.append(f"scale={scale:.3f}")

        if recommendation.get("auto_seed_attempted"):
            success_text = "ok" if recommendation.get("auto_seed_success") else "failed"
            bits.append(f"auto-seed={success_text}")

        blocked_reason = recommendation.get("blocked_reason")
        if blocked_reason:
            bits.append(f"blocked={blocked_reason}")

        if not bits:
            return None
        return "; ".join(bits)

    def clear_execution_feedback(self, symbol: str | None = None) -> int:
        """Remove stored execution feedback (optionally scoped to a symbol)."""
        if not self._execution_feedback:
            return 0
        normalized = self._normalize_symbol_key(symbol) if symbol else None
        if not normalized:
            removed = len(self._execution_feedback)
            self._execution_feedback.clear()
            return removed
        kept: list[dict[str, Any]] = []
        for entry in self._execution_feedback:
            entry_symbol = self._normalize_symbol_key(entry.get("symbol")) if isinstance(entry, dict) else None
            if entry_symbol == normalized:
                continue
            kept.append(entry)
        removed = len(self._execution_feedback) - len(kept)
        self._execution_feedback.clear()
        self._execution_feedback.extend(kept)
        return removed

    @staticmethod
    def _response_success(response: Any) -> bool:
        """Return True if the OKX response and all nested entries report success codes."""
        def _entry_ok(entry: dict[str, Any]) -> bool:
            code = str(entry.get("sCode") or entry.get("code") or "").strip()
            return (not code) or code == "0"

        if isinstance(response, dict):
            if not _entry_ok(response):
                return False
            data = response.get("data")
            if isinstance(data, list):
                return all(
                    _entry_ok(item)
                    for item in data
                    if isinstance(item, dict)
                )
        return True

    @staticmethod
    def _extract_response_codes(response: Any) -> tuple[str | None, str | None, str | None]:
        """Extract (code, subcode, message) triad from OKX API responses."""
        code: str | None = None
        sub_code: str | None = None
        message: str | None = None
        if isinstance(response, dict):
            raw_code = response.get("code")
            code = str(raw_code).strip() if raw_code not in (None, "") else None
            message = response.get("msg")
            data_block = response.get("data")
            if isinstance(data_block, list) and data_block:
                first = data_block[0]
                if isinstance(first, dict):
                    raw_sub_code = first.get("sCode") or first.get("code")
                    sub_code = (
                        str(raw_sub_code).strip()
                        if raw_sub_code not in (None, "")
                        else None
                    )
                    message = first.get("sMsg") or first.get("msg") or message
        return code, sub_code, message

    def _response_indicates_insufficient_margin(self, response: Any) -> bool:
        """Check if an OKX response points to insufficient margin error codes."""
        code, sub_code, _ = self._extract_response_codes(response)
        for candidate in (code, sub_code):
            if candidate and candidate in INSUFFICIENT_MARGIN_CODES:
                return True
        return False

    def _estimate_isolated_margin_requirement(
        self,
        *,
        size: float | None,
        price: float | None,
        min_leverage: float | None,
        account_equity: float | None,
        max_position_pct: float | None,
        symbol_cap_pct: float | None,
        max_notional_usd: float | None,
        tier_initial_margin_ratio: float | None = None,
        tier_max_notional: float | None = None,
    ) -> float | None:
        """Estimate how much isolated margin the requested notional would consume."""
        size_value = self._extract_float(size)
        price_value = self._extract_float(price)
        if not size_value or size_value <= 0 or not price_value or price_value <= 0:
            return None
        leverage_floor = self._extract_float(min_leverage)
        if not leverage_floor or leverage_floor < 1.0:
            leverage_floor = 1.0
        requested_notional = size_value * price_value

        notional_caps: list[float] = []
        equity_value = self._extract_float(account_equity)
        if equity_value and equity_value > 0:
            for pct in (max_position_pct, symbol_cap_pct):
                pct_value = self._extract_float(pct)
                if pct_value and pct_value > 0:
                    notional_caps.append(equity_value * pct_value)
        if max_notional_usd:
            cap_value = self._extract_float(max_notional_usd)
            if cap_value:
                notional_caps.append(cap_value)
        tier_cap_value = self._extract_float(tier_max_notional)
        if tier_cap_value:
            notional_caps.append(tier_cap_value)
        if notional_caps:
            requested_notional = min(requested_notional, max(notional_caps))
        tier_imr = self._extract_float(tier_initial_margin_ratio)
        if tier_imr and tier_imr > 0:
            margin = requested_notional * tier_imr
        else:
            margin = requested_notional / leverage_floor
        buffer = max(price_value * 0.01, margin * 0.05, 10.0)
        return margin + buffer

    def _resolve_isolated_seed_limit(
        self,
        guardrails: dict[str, Any] | None,
        symbol: str,
        *,
        account_equity: float | None = None,
    ) -> float | None:
        """Resolve guardrail-configured isolated margin transfer caps for a symbol."""
        if not isinstance(guardrails, dict):
            return None
        symbol_key = self._normalize_symbol_key(symbol) or symbol
        overrides = guardrails.get("isolated_margin_symbol_seeds_usd")
        limit = None
        if isinstance(overrides, dict) and symbol_key:
            for candidate_key in {symbol_key, symbol}:
                if not candidate_key:
                    continue
                value = overrides.get(candidate_key)
                parsed = self._extract_float(value)
                if parsed and parsed > 0:
                    limit = parsed
                    break
        if limit is None:
            limit = self._extract_float(guardrails.get("isolated_margin_seed_usd"))
        global_cap = self._extract_float(guardrails.get("isolated_margin_max_transfer_usd"))
        if limit is None:
            limit = global_cap
        elif global_cap and global_cap > 0:
            limit = min(limit, global_cap)
        pct_limit: float | None = None
        pct_overrides = guardrails.get("isolated_margin_symbol_seed_pct")
        pct_value = None
        if isinstance(pct_overrides, dict) and symbol_key:
            for candidate_key in {symbol_key, symbol}:
                if not candidate_key:
                    continue
                pct_candidate = self._extract_float(pct_overrides.get(candidate_key))
                if pct_candidate and pct_candidate > 0:
                    pct_value = pct_candidate
                    break
        if pct_value is None:
            pct_value = self._extract_float(guardrails.get("isolated_margin_seed_pct"))
        if (
            pct_value
            and pct_value > 0
            and account_equity is not None
            and account_equity > 0
        ):
            pct_limit = account_equity * pct_value
        if pct_limit is not None:
            if limit is None:
                limit = pct_limit
            else:
                limit = min(limit, pct_limit)
        if limit is None or limit <= 0:
            return None
        return limit

    async def _fetch_funding_balance(self, currency: str) -> float | None:
        """Return the available balance for a currency in the funding wallet."""
        if not self._funding_api:
            return None
        try:
            response = await asyncio.to_thread(
                self._funding_api.get_balances,
                currency,
            )
        except Exception as exc:  # pragma: no cover - network dependency
            self._emit_debug(f"Funding balance fetch failed for {currency}: {exc}")
            return None
        entries = self._safe_data(response)
        if not entries:
            return None
        target = (currency or "").upper()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_currency = str(entry.get("ccy") or "").upper()
            if entry_currency != target:
                continue
            for key in ("availBal", "availEq", "cashBal", "bal", "available"):
                value = self._extract_float(entry.get(key))
                if value is not None:
                    return value
        return None

    async def _seed_isolated_margin_from_funding(
        self,
        *,
        symbol: str,
        quote_currency: str | None,
        required_gap: float,
        guardrails: dict[str, Any] | None,
        seed_limit: float | None = None,
        account_equity: float | None = None,
    ) -> dict[str, Any]:
        """Attempt to transfer collateral from funding to trading to fill isolated margin gaps."""
        seed_result = {
            "success": False,
            "needed": required_gap,
            "limit": seed_limit,
            "funding_available": None,
            "blocked_reason": None,
            "currency": (quote_currency or "").upper() if quote_currency else None,
        }
        if not self._funding_api:
            seed_result["blocked_reason"] = "funding_api_unavailable"
            return seed_result
        if not quote_currency or required_gap <= 0:
            seed_result["blocked_reason"] = "no_gap"
            return seed_result
        limit = seed_limit
        if limit is None:
            limit = self._resolve_isolated_seed_limit(
                guardrails,
                symbol,
                account_equity=account_equity,
            )
        seed_result["limit"] = limit
        if limit is None or limit <= 0:
            seed_result["blocked_reason"] = "no_limit_configured"
            return seed_result
        if required_gap - limit > 1e-6:
            seed_result["blocked_reason"] = "limit_exceeded"
            self._record_execution_feedback(
                symbol,
                "Auto-seed skipped: required transfer exceeds configured limit",
                level="warning",
                meta={
                    "needed_usd": required_gap,
                    "limit_usd": limit,
                },
            )
            self._emit_debug(
                f"Isolated auto-seed blocked for {symbol}: needed {required_gap:.4f} exceeds limit {limit:.4f}"
            )
            return seed_result
        currency = quote_currency.upper()
        funding_available = await self._fetch_funding_balance(currency)
        seed_result["funding_available"] = funding_available
        if funding_available is not None and funding_available + 1e-6 < required_gap:
            seed_result["blocked_reason"] = "funding_insufficient"
            self._record_execution_feedback(
                symbol,
                "Auto-seed skipped: funding balance insufficient",
                level="warning",
                meta={
                    "needed_usd": required_gap,
                    "available_funding": funding_available,
                },
            )
            self._emit_debug(
                f"Funding balance insufficient for {symbol}: need {required_gap:.4f} {currency}, have {funding_available:.4f}"
            )
            return seed_result
        formatted_amount = self._format_price(required_gap)
        sub_account = self._sub_account if self._sub_account_use_master else None
        try:
            response = await asyncio.to_thread(
                self._funding_api.funds_transfer,
                ccy=currency,
                amt=formatted_amount,
                from_=FUNDING_ACCOUNT_TYPE,
                to=TRADING_ACCOUNT_TYPE,
                type="0",
                subAcct=sub_account or "",
            )
        except Exception as exc:  # pragma: no cover - network dependency
            seed_result["blocked_reason"] = "transfer_error"
            self._emit_debug(f"Funding transfer failed for {symbol}: {exc}")
            self._record_execution_feedback(
                symbol,
                "Auto-seed transfer failed",
                level="error",
                meta={"error": str(exc)},
            )
            return seed_result
        if not self._response_success(response):
            seed_result["blocked_reason"] = "transfer_rejected"
            code, sub_code, message = self._extract_response_codes(response)
            self._record_execution_feedback(
                symbol,
                "Auto-seed transfer rejected",
                level="error",
                meta={
                    "code": code,
                    "sCode": sub_code,
                    "message": message,
                },
            )
            self._emit_debug(
                f"Auto-seed transfer rejected for {symbol}: code={code} sCode={sub_code} detail={message or response}"
            )
            return seed_result
        self._emit_debug(
            f"Auto-seeded {formatted_amount} {currency} from funding to trading for {symbol}"
        )
        self._record_execution_feedback(
            symbol,
            f"Auto-seeded {formatted_amount} {currency} to restore isolated margin",
            level="info",
            meta={"currency": currency, "amount": formatted_amount},
        )
        seed_result["success"] = True
        return seed_result

    async def _ensure_isolated_margin_buffer(
        self,
        *,
        symbol: str,
        action: str,
        dual_side_mode: bool,
        trade_mode: str,
        pos_side: str | None = None,
        existing_side_size: float | None = None,
        min_leverage: float | None,
        size: float | None,
        last_price: float | None,
        quote_currency: str | None,
        available_margin_usd: float | None,
        account_equity: float | None,
        max_position_pct: float | None,
        symbol_cap_pct: float | None,
        max_notional_usd: float | None,
        guardrails: dict[str, Any] | None = None,
        min_size: float | None = None,
        tier_entries: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any] | None, float | None]:
        """Top up isolated margin (auto-downsizing or seeding if needed) before placing an order."""
        if not self._account_api:
            return None, None

        quote_currency = str(
            quote_currency
            or self._quote_currency_from_symbol(symbol)
            or "USDT"
        ).upper()
        price_value = self._extract_float(last_price)

        def _compute_notional(quantity: float | None) -> float | None:
            if quantity is None or price_value is None:
                return None
            return quantity * price_value

        initial_size = self._extract_float(size)
        initial_notional = _compute_notional(initial_size)
        existing_side_value = self._extract_float(existing_side_size) or 0.0
        normalized_trade_mode = (trade_mode or "isolated").lower()
        tier_dataset: list[dict[str, Any]] = tier_entries or []
        if (
            not tier_dataset
            and price_value
            and price_value > 0
            and normalized_trade_mode == "isolated"
        ):
            tier_dataset = await self._get_position_tiers(symbol, trade_mode)

        adjusted_size: float | None = None
        current_size = initial_size
        current_margin = self._extract_float(available_margin_usd) or 0.0
        iteration = 0
        final_required_gap: float | None = None
        final_seed_limit: float | None = None
        final_guidance: dict[str, Any] | None = None

        while True:
            iteration += 1
            tier_metadata: dict[str, Any] | None = None
            tier_imr: float | None = None
            tier_max_leverage: float | None = None
            tier_max_notional: float | None = None
            if tier_dataset:
                resulting_size = max(0.0, existing_side_value) + max(0.0, current_size or 0.0)
                tier_metadata = self._select_position_tier(tier_dataset, resulting_size)
                if tier_metadata:
                    tier_imr = self._extract_float(tier_metadata.get("imr"))
                    tier_max_leverage = self._extract_float(tier_metadata.get("maxLever"))
                    if (
                        (tier_imr is None or tier_imr <= 0)
                        and tier_max_leverage
                        and tier_max_leverage > 0
                    ):
                        tier_imr = 1.0 / tier_max_leverage
                    tier_max_size = self._extract_float(tier_metadata.get("maxSz"))
                    if (
                        tier_max_size
                        and tier_max_size > 0
                        and price_value
                        and price_value > 0
                    ):
                        tier_max_notional = tier_max_size * price_value
            amount = self._estimate_isolated_margin_requirement(
                size=current_size,
                price=last_price,
                min_leverage=min_leverage,
                account_equity=account_equity,
                max_position_pct=max_position_pct,
                symbol_cap_pct=symbol_cap_pct,
                max_notional_usd=max_notional_usd,
                tier_initial_margin_ratio=tier_imr,
                tier_max_notional=tier_max_notional,
            )
            if not amount or amount <= 0:
                return None, adjusted_size
            required_gap = amount - current_margin
            if required_gap <= 0:
                return None, adjusted_size
            seed_limit = (
                self._resolve_isolated_seed_limit(
                    guardrails,
                    symbol,
                    account_equity=account_equity,
                )
                if guardrails
                else None
            )
            margin_guidance_payload = {
                "quote_currency": quote_currency,
                "required_gap": required_gap,
                "seed_limit": seed_limit,
                "auto_seed_attempted": False,
                "auto_seed_configured": bool(seed_limit and seed_limit > 0),
                "price_reference": price_value,
                "requested_size": current_size,
                "requested_notional": _compute_notional(current_size),
                "initial_requested_size": initial_size,
                "initial_requested_notional": initial_notional,
            }
            if tier_imr:
                margin_guidance_payload["tier_initial_margin_ratio"] = tier_imr
            if tier_max_leverage:
                margin_guidance_payload["tier_max_leverage"] = tier_max_leverage
            if tier_max_notional:
                margin_guidance_payload["tier_max_notional_usd"] = tier_max_notional
            self._set_margin_guidance(symbol, margin_guidance_payload)

            if (
                seed_limit is not None
                and seed_limit > 0
                and required_gap - seed_limit > 1e-6
            ):
                if not current_size or current_size <= 0:
                    self._record_execution_feedback(
                        symbol,
                        "Auto-seed skipped: required transfer exceeds configured limit",
                        level="warning",
                        meta={
                            "needed_usd": required_gap,
                            "limit_usd": seed_limit,
                        },
                    )
                    self._emit_debug(
                        f"Isolated auto-seed blocked for {symbol}: needed {required_gap:.4f} exceeds limit {seed_limit:.4f}"
                    )
                    return None, adjusted_size
                scale = max(min(seed_limit / required_gap, 1.0), 0.0)
                clipped_size = current_size * scale
                quantized = self._quantize_order_size(symbol, clipped_size) if clipped_size > 0 else None
                if quantized is None or quantized <= 0:
                    margin_guidance_payload.update({"blocked_reason": "limit_exceeded"})
                    self._set_margin_guidance(symbol, margin_guidance_payload)
                    self._record_execution_feedback(
                        symbol,
                        "Auto-seed skipped: required transfer exceeds configured limit",
                        level="warning",
                        meta={
                            "needed_usd": required_gap,
                            "limit_usd": seed_limit,
                            "auto_downsize_failed": True,
                        },
                    )
                    self._emit_debug(
                        f"Isolated auto-seed blocked for {symbol}: unable to downsize below lot size"
                    )
                    return None, adjusted_size
                if min_size and quantized < min_size:
                    margin_guidance_payload.update(
                        {
                            "blocked_reason": "limit_exceeded",
                            "auto_downsize_blocked": "min_size",
                        }
                    )
                    self._set_margin_guidance(symbol, margin_guidance_payload)
                    self._record_execution_feedback(
                        symbol,
                        "Auto-downsize blocked by instrument minimum size",
                        level="warning",
                        meta={
                            "min_size": min_size,
                            "target_size": quantized,
                            "limit_usd": seed_limit,
                        },
                    )
                    return None, adjusted_size
                if current_size and abs(quantized - current_size) <= max(1e-9, current_size * 1e-6):
                    self._emit_debug(
                        f"Auto-downsize for {symbol} stalled; quantized target ({quantized:.6f}) matches current size"
                    )
                    margin_guidance_payload.update({"blocked_reason": "limit_exceeded"})
                    self._set_margin_guidance(symbol, margin_guidance_payload)
                    return None, adjusted_size

                adjusted_size = quantized
                previous_notional = _compute_notional(current_size)
                target_notional = _compute_notional(quantized)
                notional_delta = (
                    previous_notional - target_notional
                    if previous_notional is not None and target_notional is not None
                    else None
                )
                downsized_payload = dict(margin_guidance_payload)
                downsized_payload.update(
                    {
                        "auto_downsize_active": True,
                        "auto_downsize_scale": scale,
                        "auto_downsize_previous_size": current_size,
                        "auto_downsize_target_size": quantized,
                        "auto_downsize_previous_notional": previous_notional,
                        "auto_downsize_target_notional": target_notional,
                        "auto_downsize_notional_delta": notional_delta,
                        "auto_downsize_required_gap": required_gap,
                        "auto_downsize_price": price_value,
                    }
                )
                self._set_margin_guidance(symbol, downsized_payload)
                self._record_execution_feedback(
                    symbol,
                    "Size clipped to fit isolated margin seed limit",
                    level="warning",
                    meta={
                        "previous_size": current_size,
                        "target_size": quantized,
                        "previous_notional": previous_notional,
                        "target_notional": target_notional,
                        "notional_delta": notional_delta,
                        "required_gap": required_gap,
                        "seed_limit": seed_limit,
                        "quote_currency": quote_currency,
                    },
                )
                current_size = quantized
                continue

            final_required_gap = required_gap
            final_seed_limit = seed_limit
            final_guidance = margin_guidance_payload
            break

        if final_required_gap is None:
            return None, adjusted_size

        required_gap = final_required_gap
        seed_limit = final_seed_limit
        margin_guidance_payload = final_guidance or {}

        pos_side_value = pos_side or ("long" if action == "BUY" else "short")
        if not dual_side_mode:
            pos_side_value = "net"
        formatted_amount = self._format_price(required_gap)
        sub_account = self._sub_account if self._sub_account_use_master else None

        async def _call_adjustment() -> Any | None:
            try:
                return await asyncio.to_thread(
                    self._account_api.adjust_isolated_margin,
                    symbol,
                    pos_side_value,
                    formatted_amount,
                    type="add",
                    subAcct=sub_account,
                )
            except Exception as exc:  # pragma: no cover - network dependency
                self._emit_debug(f"Isolated margin top-up failed for {symbol}: {exc}")
                self._record_execution_feedback(
                    symbol,
                    "Failed to add isolated margin",
                    level="error",
                    meta={"error": str(exc)},
                )
                return None

        response = await _call_adjustment()
        if response is None:
            return None, adjusted_size
        if not self._response_success(response):
            if guardrails and self._response_indicates_insufficient_margin(response):
                seed_result = await self._seed_isolated_margin_from_funding(
                    symbol=symbol,
                    quote_currency=quote_currency,
                    required_gap=required_gap,
                    guardrails=guardrails,
                    seed_limit=seed_limit,
                    account_equity=account_equity,
                )
                updated_guidance = dict(margin_guidance_payload)
                updated_guidance.update(
                    {
                        "auto_seed_attempted": True,
                        "auto_seed_success": bool(seed_result.get("success")),
                        "funding_available": seed_result.get("funding_available"),
                        "blocked_reason": seed_result.get("blocked_reason"),
                        "seed_limit": seed_result.get("limit", seed_limit),
                    }
                )
                self._set_margin_guidance(symbol, updated_guidance)
                if seed_result.get("success"):
                    response = await _call_adjustment()
                    if response is None:
                        return None, adjusted_size
                    if not self._response_success(response):
                        self._emit_debug(
                            f"Isolated margin retry rejected for {symbol} after auto-seed: {response}"
                        )
                        self._record_execution_feedback(
                            symbol,
                            "Isolated margin top-up rejected",
                            level="error",
                            meta={"response": response},
                        )
                        return None, adjusted_size
                else:
                    self._emit_debug(
                        f"Isolated margin top-up rejected for {symbol}: {response}"
                    )
                    self._record_execution_feedback(
                        symbol,
                        "Isolated margin top-up rejected",
                        level="error",
                        meta={"response": response},
                    )
                    return None, adjusted_size
            else:
                self._emit_debug(f"Isolated margin top-up rejected for {symbol}: {response}")
                self._record_execution_feedback(
                    symbol,
                    "Isolated margin top-up rejected",
                    level="error",
                    meta={"response": response},
                )
                return None, adjusted_size
        label = quote_currency or "margin"
        self._record_execution_feedback(
            symbol,
            f"Allocated {formatted_amount} {label} to isolated wallet",
            level="info",
            meta={"amount": formatted_amount, "pos_side": pos_side},
        )
        refreshed = await self._fetch_account_balance()
        if refreshed:
            self._refresh_execution_limits_from_account(refreshed)
        return refreshed, adjusted_size

    def _refresh_execution_limits_from_account(self, account_payload: dict[str, Any] | None) -> None:
        """Update per-symbol execution caps based on the latest account snapshot."""
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
        """Drop stale trade timestamps so guardrail windows stay bounded."""
        cutoff = max(60, window or 3600)
        while history and now - history[0] > cutoff:
            history.popleft()

    def _detect_position_side(self, positions: list[dict[str, Any]], symbol: str) -> str:
        """Return LONG/SHORT/FLAT by inspecting current OKX positions for the symbol."""
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
        """Enforce basic position transition rules for alignment guardrails."""
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
        """Clamp arbitrary confidence inputs into the [0,1] range with sane defaults."""
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
        """Scale desired size toward leverage bounds using confidence-driven interpolation."""
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
        """Apply guardrails, size calculations, and OKX submission for an LLM-issued action."""
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
        risk_locks_block = context.get("risk_locks") or {}
        daily_loss_state = risk_locks_block.get("daily_loss") if isinstance(risk_locks_block, dict) else None
        if isinstance(daily_loss_state, dict) and daily_loss_state.get("active"):
            drop_pct = daily_loss_state.get("change_pct")
            threshold_pct = daily_loss_state.get("threshold_pct")
            drop_label = f"{drop_pct * 100:.2f}%" if isinstance(drop_pct, (int, float)) else "configured"
            limit_label = f"{threshold_pct * 100:.2f}%" if isinstance(threshold_pct, (int, float)) else "limit"
            self._emit_debug(
                f"Daily loss guard active; skipping {action} for {symbol} (drop {drop_label} vs {limit_label})"
            )
            self._record_execution_feedback(
                symbol,
                "Daily loss limit active; execution blocked",
                level="warning",
                meta={
                    "change_pct": drop_pct,
                    "threshold_pct": threshold_pct,
                    "window_hours": daily_loss_state.get("window_hours"),
                },
            )
            return False
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
        market_block = context.get("market") or {}
        last_price = self._extract_float(
            market_block.get("last_price")
            or market_block.get("last")
            or market_block.get("price")
        )
        if last_price is None:
            last_price = self._extract_float(decision.get("last_price"))
        if last_price is None:
            last_price = self._extract_float(
                (self._latest_ticker.get(symbol) or {}).get("last")
            )
        price_hints: dict[str, float] = {}
        if last_price and last_price > 0:
            price_hints[symbol] = last_price
        account_block = context.get("account") or {}
        account_equity = self._extract_float(
            account_block.get("account_equity")
            or account_block.get("total_eq_usd")
            or account_block.get("total_equity")
            or account_block.get("total_account_value")
        )
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
        desired_pos_side = "long" if action == "BUY" else "short"
        if not dual_side_mode:
            desired_pos_side = "net"
        open_position_notional = self._compute_open_position_notional(positions, price_hints=price_hints)
        available_equity_for_trade = None
        if (
            account_equity is not None
            and account_equity > 0
            and open_position_notional is not None
        ):
            available_equity_for_trade = max(account_equity - open_position_notional, 0.0)
        self._merge_margin_guidance(
            symbol,
            {
                "account_equity": account_equity,
                "open_position_notional": open_position_notional,
                "equity_available_for_trade": available_equity_for_trade,
            },
        )
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
        instrument_spec = self._instrument_specs.get(symbol) or {}
        execution_trade_mode = execution_cfg.get("trade_mode") or "cross"
        trade_mode = str(execution_trade_mode).lower()
        if trade_mode not in {"isolated", "cross"}:
            trade_mode = "cross"
        isolated_mode = trade_mode == "isolated"
        order_type = str(execution_cfg.get("order_type") or "market").lower()
        min_size = self._extract_float(execution_cfg.get("min_size"))
        if min_size is None:
            min_size = self._extract_float(
                instrument_spec.get("min_size")
                or instrument_spec.get("lot_size")
            )
        if min_size is None:
            min_size = 0.0
        base_max_pct = self._extract_float(guardrails.get("max_position_pct"))
        symbol_caps = guardrails.get("symbol_position_caps")
        if not isinstance(symbol_caps, dict):
            symbol_caps = {}
        symbol_cap_pct: float | None = None
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
        guardrail_notional_cap = None
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

        if (
            account_equity is not None
            and account_equity > 0
            and effective_max_pct
            and max_leverage
        ):
            guardrail_notional_cap = max(0.0, account_equity * max_leverage * effective_max_pct)

        confidence_value = self._normalize_confidence(decision.get("confidence"))
        size_hint = self._extract_float(decision.get("position_size"))
        target_leverage: float | None = None
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

        def _extract_quote_balances(
            balances: dict[str, Any] | None,
        ) -> tuple[float | None, float | None]:
            if not isinstance(balances, dict) or not quote_currency:
                return None, None
            quote_meta = balances.get(quote_currency)
            if not isinstance(quote_meta, dict):
                return None, None
            available_usd = self._extract_float(
                quote_meta.get("available_usd")
                or quote_meta.get("equity_usd")
            )
            if available_usd is None and quote_currency in STABLE_CURRENCIES:
                available_usd = self._extract_float(quote_meta.get("available"))
            quote_cash = self._extract_float(quote_meta.get("cash"))
            cash_usd = None
            if quote_cash is not None:
                if quote_currency in STABLE_CURRENCIES:
                    cash_usd = quote_cash
                elif last_price:
                    cash_usd = quote_cash * last_price
            return available_usd, cash_usd

        async def _refresh_margin_snapshot() -> dict[str, Any] | None:
            if not self._account_api:
                return None
            try:
                refreshed_snapshot = await self._fetch_account_balance()
            except Exception as exc:  # pragma: no cover - network variance
                self._emit_debug(f"Margin availability refresh failed: {exc}")
                return None
            self._refresh_execution_limits_from_account(refreshed_snapshot)
            if refreshed_snapshot:
                self._record_execution_feedback(
                    symbol,
                    "Account balances refreshed",
                    level="info",
                    meta={"source": "okx"},
                )
            return refreshed_snapshot

        quote_available_usd, quote_cash_usd = _extract_quote_balances(available_balances_block)
        quote_margin_candidates = [
            value
            for value in (quote_cash_usd, quote_available_usd)
            if value is not None and value > 0
        ]
        isolated_margin_available = max(quote_margin_candidates) if quote_margin_candidates else None
        if isolated_mode:
            tier_entries: list[dict[str, Any]] | None = None
            tier_imr_for_check: float | None = None
            tier_max_notional_for_check: float | None = None
            if last_price and last_price > 0:
                tier_entries = await self._get_position_tiers(symbol, trade_mode)
                tier_pool = tier_entries or []
                if tier_pool:
                    resulting_size = max(0.0, side_sizes.get(desired_pos_side, 0.0)) + max(0.0, raw_size)
                    tier_meta = self._select_position_tier(tier_pool, resulting_size)
                    if tier_meta:
                        tier_imr_for_check = self._extract_float(tier_meta.get("imr"))
                        tier_max_leverage = self._extract_float(tier_meta.get("maxLever"))
                        if (
                            (tier_imr_for_check is None or tier_imr_for_check <= 0)
                            and tier_max_leverage
                            and tier_max_leverage > 0
                        ):
                            tier_imr_for_check = 1.0 / tier_max_leverage
                        tier_max_size = self._extract_float(tier_meta.get("maxSz"))
                        if tier_max_size and tier_max_size > 0:
                            tier_max_notional_for_check = tier_max_size * last_price
            required_isolated_margin = self._estimate_isolated_margin_requirement(
                size=raw_size,
                price=last_price,
                min_leverage=min_leverage,
                account_equity=account_equity,
                max_position_pct=base_max_pct,
                symbol_cap_pct=symbol_cap_pct,
                max_notional_usd=guardrail_notional_cap,
                tier_initial_margin_ratio=tier_imr_for_check,
                tier_max_notional=tier_max_notional_for_check,
            )
            if required_isolated_margin and (
                isolated_margin_available is None
                or isolated_margin_available < required_isolated_margin
            ):
                refreshed_balances, downsized_size = await self._ensure_isolated_margin_buffer(
                    symbol=symbol,
                    action=action,
                    dual_side_mode=dual_side_mode,
                    trade_mode=trade_mode,
                    pos_side=desired_pos_side,
                    existing_side_size=side_sizes.get(desired_pos_side, 0.0),
                    min_leverage=min_leverage,
                    size=raw_size,
                    last_price=last_price,
                    quote_currency=quote_currency,
                    available_margin_usd=isolated_margin_available,
                    account_equity=account_equity,
                    max_position_pct=base_max_pct,
                    symbol_cap_pct=symbol_cap_pct,
                    max_notional_usd=guardrail_notional_cap,
                    guardrails=guardrails,
                    min_size=min_size,
                    tier_entries=tier_entries,
                )
                if downsized_size is not None and downsized_size > 0:
                    raw_size = downsized_size
                if refreshed_balances:
                    balances_block = refreshed_balances.get("available_balances")
                    if isinstance(balances_block, dict):
                        available_balances_block = balances_block
                    quote_available_usd, quote_cash_usd = _extract_quote_balances(available_balances_block)
                    quote_margin_candidates = [
                        value
                        for value in (quote_cash_usd, quote_available_usd)
                        if value is not None and value > 0
                    ]
                    isolated_margin_available = max(quote_margin_candidates) if quote_margin_candidates else None
                    refreshed_available = self._extract_float(refreshed_balances.get("available_eq_usd"))
                    if refreshed_available is not None:
                        available_margin_usd = refreshed_available
                    refreshed_equity = self._extract_float(
                        refreshed_balances.get("total_eq_usd")
                        or refreshed_balances.get("total_equity")
                        or refreshed_balances.get("total_account_value")
                    )
                    if refreshed_equity is not None and refreshed_equity > 0:
                        account_equity = refreshed_equity
            if isolated_margin_available:
                available_margin_usd = isolated_margin_available
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
        if available_margin_usd is None:
            refreshed_margin_snapshot = await _refresh_margin_snapshot()
            if refreshed_margin_snapshot:
                refreshed_balances = refreshed_margin_snapshot.get("available_balances")
                if isinstance(refreshed_balances, dict) and refreshed_balances:
                    available_balances_block = refreshed_balances
                    quote_available_usd, quote_cash_usd = _extract_quote_balances(available_balances_block)
                    quote_margin_candidates = [
                        value
                        for value in (quote_cash_usd, quote_available_usd)
                        if value is not None and value > 0
                    ]
                    if not isolated_mode and quote_margin_candidates:
                        refreshed_candidate = max(quote_margin_candidates)
                        if refreshed_candidate is not None and (
                            available_margin_usd is None or refreshed_candidate > available_margin_usd
                        ):
                            available_margin_usd = refreshed_candidate
                refreshed_available = self._extract_float(refreshed_margin_snapshot.get("available_eq_usd"))
                if refreshed_available is not None and (
                    available_margin_usd is None or refreshed_available > available_margin_usd
                ):
                    available_margin_usd = refreshed_available
                refreshed_equity = self._extract_float(
                    refreshed_margin_snapshot.get("total_eq_usd")
                    or refreshed_margin_snapshot.get("total_equity")
                    or refreshed_margin_snapshot.get("total_account_value")
                )
                if refreshed_equity is not None and refreshed_equity > 0:
                    account_equity = refreshed_equity
        if available_margin_usd is None:
            self._emit_debug(
                f"Execution skipped for {symbol}: unable to determine available margin"
            )
            self._record_execution_feedback(
                symbol,
                "Available margin unknown; execution paused",
                level="warning",
                meta={
                    "trade_mode": trade_mode,
                    "quote_currency": quote_currency,
                },
            )
            return False
        if available_margin_usd <= 0:
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

        if max_leverage and max_leverage > 0:
            margin_driven_cap = available_margin_usd * max_leverage
            pct_multiplier = effective_max_pct if effective_max_pct and effective_max_pct > 0 else None
            if pct_multiplier:
                margin_driven_cap *= pct_multiplier
            if margin_driven_cap and margin_driven_cap > 0:
                if guardrail_notional_cap is None or margin_driven_cap > guardrail_notional_cap:
                    guardrail_notional_cap = margin_driven_cap
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

        clipped_by_cap = False
        cap_reason: str | None = None
        if guardrail_notional_cap and guardrail_notional_cap > 0 and last_price:
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

        if (
            not reduce_only
            and account_equity is not None
            and account_equity > 0
            and last_price
            and last_price > 0
        ):
            margin_headroom = None
            if available_margin_usd is not None:
                margin_headroom = max(available_margin_usd, 0.0)
            free_equity = available_equity_for_trade
            if free_equity is None:
                free_equity = max(account_equity - (open_position_notional or 0.0), 0.0)
            if (
                margin_headroom is not None
                and margin_headroom > 0
                and max_leverage
                and max_leverage > 0
            ):
                margin_based_notional = margin_headroom * max_leverage
                if margin_based_notional > (free_equity or 0.0):
                    free_equity = margin_based_notional
            equity_updates = {
                "account_equity": account_equity,
                "open_position_notional": open_position_notional,
                "equity_available_for_trade": free_equity,
            }
            if margin_headroom is not None:
                equity_updates["margin_available_usd"] = margin_headroom
            if quote_currency:
                equity_updates.setdefault("quote_currency", str(quote_currency).upper())
            self._merge_margin_guidance(symbol, equity_updates)
            margin_exhausted = margin_headroom is None or margin_headroom <= 0
            notional_exhausted = free_equity is None or free_equity <= 0
            if margin_exhausted and notional_exhausted:
                self._emit_debug(
                    f"Execution skipped for {symbol}: no free equity after accounting for open positions"
                )
                self._record_execution_feedback(
                    symbol,
                    "Blocked: all account equity deployed",
                    level="warning",
                    meta={
                        "account_equity": account_equity,
                        "open_position_notional": open_position_notional,
                        "equity_available_for_trade": free_equity,
                        "margin_available_usd": margin_headroom,
                    },
                )
                block_payload = dict(equity_updates)
                block_payload["blocked_reason"] = "free_equity_exhausted"
                self._merge_margin_guidance(symbol, block_payload)
                return False
            current_notional = raw_size * last_price
            if free_equity is not None and current_notional > free_equity + 1e-9:
                previous_size = raw_size
                max_size_from_equity = free_equity / last_price if last_price else 0.0
                if max_size_from_equity <= 0:
                    self._emit_debug(
                        f"Execution skipped for {symbol}: requested notional {current_notional:.4f} exceeds free equity {free_equity:.4f}"
                    )
                    self._record_execution_feedback(
                        symbol,
                        "Blocked: insufficient free equity",
                        level="warning",
                        meta={
                            "account_equity": account_equity,
                            "open_position_notional": open_position_notional,
                            "equity_available_for_trade": free_equity,
                            "requested_notional": current_notional,
                            "margin_available_usd": margin_headroom,
                        },
                    )
                    failure_payload = dict(equity_updates)
                    failure_payload.update(
                        {
                            "blocked_reason": "free_equity_limit",
                            "equity_clip_active": False,
                            "equity_clip_requested_size": previous_size,
                            "equity_clip_requested_notional": current_notional,
                        }
                    )
                    self._merge_margin_guidance(symbol, failure_payload)
                    return False
                clipped_notional = max_size_from_equity * last_price
                clip_delta = current_notional - clipped_notional
                self._emit_debug(
                    f"{symbol} size clipped by available equity (free {free_equity:.4f} USD, requested {current_notional:.4f} USD)"
                )
                self._record_execution_feedback(
                    symbol,
                    "Size clipped by available equity",
                    level="info",
                    meta={
                        "account_equity": account_equity,
                        "open_position_notional": open_position_notional,
                        "equity_available_for_trade": free_equity,
                        "margin_available_usd": margin_headroom,
                        "requested_notional": current_notional,
                        "clipped_notional": clipped_notional,
                    },
                )
                clip_payload = dict(equity_updates)
                clip_payload.update(
                    {
                        "equity_clip_active": True,
                        "equity_clip_reason": "free_equity_limit",
                        "equity_clip_requested_size": previous_size,
                        "equity_clip_target_size": max_size_from_equity,
                        "equity_clip_requested_notional": current_notional,
                        "equity_clip_target_notional": clipped_notional,
                        "equity_clip_notional_delta": clip_delta,
                    }
                )
                self._merge_margin_guidance(symbol, clip_payload)
                raw_size = max_size_from_equity

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

        spec = self._instrument_specs.get(symbol) or {}
        per_order_limit = None
        if order_type == "market":
            per_order_limit = spec.get("max_market_size") or spec.get("max_limit_size")
        else:
            per_order_limit = spec.get("max_limit_size") or spec.get("max_market_size")
        if per_order_limit and per_order_limit > 0 and raw_size > per_order_limit:
            previous_size = raw_size
            raw_size = per_order_limit
            self._emit_debug(
                f"{symbol} size clipped to {per_order_limit:.6f} by OKX per-order limit"
            )
            self._record_execution_feedback(
                symbol,
                "Size clipped by OKX per-order limit",
                level="info",
                meta={
                    "order_type": order_type,
                    "previous_size": previous_size,
                    "per_order_limit": per_order_limit,
                },
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
            target_leverage = achieved_leverage
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
        if not reduce_only and (target_leverage is None or target_leverage <= 0) and account_equity and account_equity > 0 and last_price and last_price > 0:
            target_leverage = (raw_size * last_price) / account_equity
        if not reduce_only and target_leverage is not None:
            if tier_max_leverage_used and tier_max_leverage_used > 0:
                target_leverage = min(target_leverage, tier_max_leverage_used)
            if max_leverage and max_leverage > 0:
                target_leverage = min(target_leverage, max_leverage)
            if min_leverage and min_leverage > 0:
                target_leverage = max(target_leverage, min_leverage)
        if not reduce_only and (target_leverage is None or target_leverage <= 0):
            fallback = None
            if max_leverage and max_leverage > 0:
                fallback = max_leverage
            elif min_leverage and min_leverage > 0:
                fallback = min_leverage
            if fallback:
                target_leverage = max(fallback, 1.0)

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
            margin_currency=quote_currency,
            leverage=target_leverage,
            dual_side_mode=dual_side_mode,
            reference_price=last_price,
        )
        if not order:
            self._emit_debug(f"Order placement failed for {symbol}")
            return False

        order_id = order.get("ordId") or order.get("orderId") or client_order_id
        self._emit_debug(
            f"OKX order submitted {side.upper()} {raw_size:.4f} {symbol} ({order_id})"
        )
        self._record_execution_feedback(
            symbol,
            "Order submitted",
            level="info",
            meta={
                "order_id": order_id,
                "side": side.upper(),
                "size": round(raw_size, 6),
                "trade_mode": trade_mode,
            },
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
        """Best-effort persistence of equity metrics so historical curves can be plotted."""
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
        """Return the most recent numeric value from a pandas DataFrame column."""
        if frame is None or column not in frame or frame.empty:
            return None
        series = frame[column]
        return float(series.iloc[-1]) if not series.empty else None

    @staticmethod
    def _column_value(frame: pd.DataFrame | None, candidates: list[str]) -> float | None:
        """Return the first available column value from a list of candidate names."""
        for name in candidates:
            value = MarketService._last_value(frame, name)
            if value is not None:
                return value
        return None

    @staticmethod
    def _series_to_list(series: pd.Series | None, limit: int = 200) -> list[float]:
        """Convert a pandas Series into a bounded list of floats, dropping NaNs."""
        if series is None:
            return []
        return [float(val) for val in series.dropna().tolist()[-limit:]]

    @staticmethod
    def _frame_column_to_list(frame: pd.DataFrame | None, column: str, limit: int = 200) -> list[float]:
        """Convert a DataFrame column into a bounded list of floats."""
        if frame is None or column not in frame:
            return []
        return [float(val) for val in frame[column].dropna().tolist()[-limit:]]

    def _normalize_bar(self, value: str | None) -> str:
        """Map user-provided timeframe strings to OKX-compatible bar identifiers."""
        if not value:
            return self.DEFAULT_TIMEFRAME
        candidate = value.strip()
        return self._TIMEFRAME_CHOICES.get(candidate.lower(), self.DEFAULT_TIMEFRAME)

    async def set_ohlc_bar(self, value: str) -> None:
        """Update the OHLC timeframe used for indicator calculations and republish snapshot."""
        bar = self._normalize_bar(value)
        if bar == self._ohlc_bar:
            return
        self._ohlc_bar = bar
        self._emit_debug(f"OHLC timeframe set to {bar}")
        await self._publish_snapshot()

    @staticmethod
    def _safe_data(response: Any) -> list[Any]:
        """Normalize OKX responses into list form, regardless of nested structure."""
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, list):
                return data
        if isinstance(response, list):
            return response
        return []

    @staticmethod
    def _price_from_ticker(ticker: dict[str, Any] | None) -> float | None:
        """Extract a usable price field from heterogeneous OKX ticker payloads."""
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
        """Truncate and coerce order book arrays into floats for downstream math."""
        bids = [[float(price), float(size)] for price, size, *_ in data.get("bids", [])][:20]
        asks = [[float(price), float(size)] for price, size, *_ in data.get("asks", [])][:20]
        return {"bids": bids, "asks": asks, "ts": data.get("ts")}

    def _build_account_api(self) -> Any | None:
        """Instantiate the OKX AccountAPI adapter when credentials and SDK are available."""
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
        """Create the MarketAPI client used for ticker/order-book retrievals."""
        if OkxMarket is None:
            logger.warning("python-okx not installed; MarketAPI unavailable")
            return None
        return OkxMarket.MarketAPI(flag=self._okx_flag)

    def _build_public_api(self) -> Any | None:
        """Create the PublicAPI client for instruments, funding, and open-interest info."""
        if OkxPublic is None:
            logger.warning("python-okx not installed; PublicAPI unavailable")
            return None
        return OkxPublic.PublicAPI(flag=self._okx_flag)

    def _build_trading_api(self) -> Any | None:
        """Instantiate the TradingDataAPI client for long/short ratios and analytics."""
        if OkxTrading is None:
            logger.warning("python-okx not installed; TradingDataAPI unavailable")
            return None
        return OkxTrading.TradingDataAPI(flag=self._okx_flag)

    def _build_trade_api(self) -> Any | None:
        """Build the TradeAPI adapter that routes order placement through okx-sdk."""
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

    def _build_funding_api(self) -> Any | None:
        """Instantiate the FundingAPI client for wallet transfers and balance queries."""
        if OkxFunding is None:
            logger.warning("python-okx not installed; FundingAPI unavailable")
            return None
        if not (self.settings.okx_api_key and self.settings.okx_secret_key and self.settings.okx_passphrase):
            logger.warning("OKX credentials missing; FundingAPI disabled")
            return None
        return OkxFunding.FundingAPI(
            api_key=self.settings.okx_api_key,
            api_secret_key=self.settings.okx_secret_key,
            passphrase=self.settings.okx_passphrase,
            flag=self._okx_flag,
        )

    def _rebuild_okx_clients(self) -> None:
        """Recreate all OKX REST clients, typically after flipping env flags or credentials."""
        self._account_api = self._build_account_api()
        self._market_api = self._build_market_api()
        self._public_api = self._build_public_api()
        self._trading_api = self._build_trading_api()
        self._trade_api = self._build_trade_api()
        self._funding_api = self._build_funding_api()

    @staticmethod
    def _format_size(value: float) -> str:
        """Format contract sizes with trimmed trailing zeros for OKX payloads."""
        return (f"{value:.6f}".rstrip("0").rstrip(".") or "0") if value is not None else "0"

    @staticmethod
    def _format_price(value: float) -> str:
        """Format prices to 8 decimals, removing redundant zeros."""
        return (f"{value:.8f}".rstrip("0").rstrip(".") or "0") if value is not None else "0"

    @staticmethod
    def _format_leverage(value: float) -> str:
        """Format leverage inputs while preventing non-positive values."""
        if value is None or value <= 0:
            return "1"
        return (f"{value:.4f}".rstrip("0").rstrip(".") or "1")

    @staticmethod
    def _leverage_cache_key(symbol: str, pos_side: str) -> str:
        """Return the dictionary key used to memoize last-set leverage per symbol/side."""
        safe_symbol = (symbol or "").upper()
        safe_side = (pos_side or "net").lower()
        return f"{safe_symbol}::{safe_side}"

    @staticmethod
    def _generate_client_order_id(prefix: str = "tai2") -> str:
        """Generate a short, unique client order ID compatible with OKX limits."""
        safe_prefix = "".join(ch for ch in (prefix or "") if ch.isalnum()) or "tai2"
        timestamp = str(int(time.time() * 1000))
        random_suffix = secrets.token_hex(3)
        value = f"{safe_prefix}{timestamp}{random_suffix}"
        return value[:32]

    def _normalize_order_response(self, response: Any) -> dict[str, Any] | None:
        """Return the first OKX data entry if the envelope and sub-codes signal success."""
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
        """Extract a human-readable error string plus metadata from an OKX response."""
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
        """Detect whether an error payload suggests posSide mismatches for net accounts."""
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
        """Snap requested size to the instrument's lot size and enforce min order size."""
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
        """Align a target price to the instrument's tick size, nudging up or down as requested."""
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
        """Validate that the TP respects directional constraints relative to entry price."""
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
        """Ensure stop-loss inputs are on the protective side of the entry price."""
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
        """Check whether an order response indicates TP/SL validation failures."""
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
        """Return True when a TP/SL target sits on the wrong side of the entry price."""
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

    def _compute_min_protection_offset(
        self,
        symbol: str,
        reference_price: float | None,
    ) -> float:
        """Compute the minimum tick/ratio offset required when nudging invalid TP/SL levels."""
        if reference_price is None or reference_price <= 0:
            return 0.0
        spec = self._instrument_specs.get(symbol) or {}
        tick = spec.get("tick_size") or 0.0
        reference_component = reference_price * self.PROTECTION_MIN_OFFSET_RATIO
        offsets = [value for value in (tick, reference_component) if value and value > 0]
        return max(offsets) if offsets else 0.0

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
        """Drop or adjust TP/SL targets that violate OKX constraints relative to entry price."""
        if not self._target_conflicts_with_price(
            action,
            target=target,
            reference_price=reference_price,
            kind=kind,
        ):
            return target
        if target is None or reference_price is None:
            return None
        prefer_up = (kind == "take-profit" and action == "BUY") or (
            kind == "stop-loss" and action == "SELL"
        )
        adjusted_target = None
        offset = self._compute_min_protection_offset(symbol, reference_price)
        if offset > 0:
            candidate = reference_price + offset if prefer_up else reference_price - offset
            if candidate and candidate > 0:
                adjusted_target = self._quantize_price(
                    symbol,
                    candidate,
                    prefer_up=prefer_up,
                )
        if adjusted_target and not self._target_conflicts_with_price(
            action,
            target=adjusted_target,
            reference_price=reference_price,
            kind=kind,
        ):
            direction = "above" if prefer_up else "below"
            message = (
                f"{symbol} {kind} adjusted from {target:.6f} to {adjusted_target:.6f} {stage}: nudged {direction} entry price {reference_price:.6f}"
            )
            self._emit_debug(message)
            self._record_execution_feedback(
                symbol,
                message,
                level="info",
                meta={
                    "stage": stage,
                    "kind": kind,
                    "target": target,
                    "adjusted_target": adjusted_target,
                    "reference_price": reference_price,
                    "action": action,
                },
            )
            return adjusted_target
        direction = "above" if prefer_up else "below"
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

    async def _ensure_isolated_leverage_setting(
        self,
        *,
        symbol: str,
        pos_side: str | None,
        dual_side_mode: bool,
        leverage: float | None,
    ) -> None:
        """Call the account API to set leverage prior to submitting isolated orders."""
        if not self._account_api:
            return
        setter = getattr(self._account_api, "set_leverage", None)
        if setter is None:
            return
        target_leverage = self._extract_float(leverage)
        if not target_leverage or target_leverage <= 0:
            return
        target_leverage = max(1.0, float(target_leverage))
        if dual_side_mode and pos_side and pos_side.lower() in {"long", "short"}:
            pos_designator = pos_side.lower()
        else:
            pos_designator = "net"
        cache_key = self._leverage_cache_key(symbol, pos_designator)
        cached_value = self._isolated_leverage_cache.get(cache_key)
        if cached_value is not None and abs(cached_value - target_leverage) <= 1e-3:
            return
        payload = {
            "instId": symbol,
            "lever": self._format_leverage(target_leverage),
            "mgnMode": "isolated",
            "posSide": pos_designator,
        }
        if self._sub_account and self._sub_account_use_master:
            payload["subAcct"] = self._sub_account
        self._emit_debug(
            f"Setting isolated leverage for {symbol} ({pos_designator}) -> {target_leverage:.2f}x"
        )
        try:
            await asyncio.to_thread(setter, **payload)
        except Exception as exc:
            self._emit_debug(f"Failed to set leverage for {symbol}: {exc}")
            self._record_execution_feedback(
                symbol,
                "Failed to set isolated leverage",
                level="warning",
                meta={
                    "requested_leverage": target_leverage,
                    "pos_side": pos_designator,
                },
            )
            return
        self._isolated_leverage_cache[cache_key] = target_leverage

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
        margin_currency: str | None = None,
        leverage: float | None = None,
        dual_side_mode: bool = False,
        reference_price: float | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Place an order via the trade API, retrying without posSide if needed."""
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot place order")
            return None
        include_pos_side = pos_side
        attachments_to_use = attach_algo_orders
        attempt = 0
        resolved_margin_currency = str(
            margin_currency or self._quote_currency_from_symbol(symbol) or ""
        ).upper()
        reference_price_value = self._extract_float(reference_price)
        requested_size_value = self._extract_float(size)
        requested_notional = (
            requested_size_value * reference_price_value
            if requested_size_value and reference_price_value
            else None
        )
        margin_snapshot = {
            "quote_currency": resolved_margin_currency or None,
            "price_reference": reference_price_value,
            "requested_size": requested_size_value,
            "requested_notional": requested_notional,
            "initial_requested_size": requested_size_value,
            "initial_requested_notional": requested_notional,
            "trade_mode": trade_mode,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }
        self._merge_margin_guidance(symbol, margin_snapshot)
        if trade_mode == "isolated" and not reduce_only:
            await self._ensure_isolated_leverage_setting(
                symbol=symbol,
                pos_side=pos_side,
                dual_side_mode=dual_side_mode,
                leverage=leverage,
            )
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
            if trade_mode == "isolated":
                if resolved_margin_currency:
                    payload["ccy"] = resolved_margin_currency

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
                "ccy": payload.get("ccy"),
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
            recommendation: dict[str, Any] | None = None
            if self._should_attach_margin_recommendation(error_meta, error_message):
                recommendation = self._build_margin_recommendation(symbol)
                if recommendation is None:
                    recommendation = self._fallback_margin_recommendation(symbol)
            summary_note = self._summarize_margin_recommendation(recommendation)
            if summary_note:
                error_message = f"{error_message} [{summary_note}]"
            self._log_margin_guidance_snapshot(symbol, context="insufficient-margin")
            combined_meta: dict[str, Any] | None = None
            if error_meta:
                combined_meta = dict(error_meta)
            margin_meta = self._build_margin_meta_snapshot(symbol)
            if margin_meta:
                if combined_meta is None:
                    combined_meta = {}
                combined_meta["margin_details"] = margin_meta
            self._record_execution_feedback(
                symbol,
                error_message,
                level="error",
                meta=combined_meta,
                recommendation=recommendation,
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
        """Persist successful fills so downstream analytics and the UI have context."""
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
        """Create a stable OKX client ID for TP/SL algos, respecting the 32 char limit."""
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
        """Rebuild TP/SL protection for a symbol, replacing any prior algo orders."""
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
        """Cancel the active TP/SL algo for `symbol` if one is known."""
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
        """Return the newest reduce-only conditional algo for the requested symbol."""
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
        """Lookup a TP/SL algo either by client ID or OKX order ID across live/history."""
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
        """Construct the attach list for `place_order` when TP and/or SL prices exist."""
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
        """Submit a standalone TP/SL algo sized to the detected open position."""
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
        """Confirm that OKX accepted the TP/SL attachment and cache its metadata."""
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
