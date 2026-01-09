from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Deque, Dict, Iterable, Optional

import pandas as pd
import pandas_ta as ta

from app.core.config import get_settings
from app.db.postgres import insert_equity_point, insert_executed_trade
from app.models.trade import ExecutedTrade
from app.services.okx_sdk_adapter import OkxAccountAdapter, OkxTradeAdapter
from app.services.state_service import StateService

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
        self._subscribed_symbols: set[str] = set()
        self._available_symbols: list[str] = []
        self._poll_interval = max(1, self.settings.ws_update_interval)
        self._ohlc_bar = self._normalize_bar(ohlc_bar)
        self._log_sink = log_sink or (lambda msg: None)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._ws_last_debug: Dict[str, float] = {}

    async def start(self) -> None:
        if self._poller_task:
            return
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
        positions = await self._fetch_positions()
        account_payload = await self._fetch_account_balance()
        total_account_value = account_payload.get("total_account_value", 0.0)
        total_equity_value = account_payload.get("total_equity", 0.0)
        total_eq_usd = account_payload.get("total_eq_usd", total_equity_value)
        account = account_payload.get("details", []) or []
        account_equity = float(total_eq_usd or total_equity_value or total_account_value or 0.0)
        market_data: dict[str, dict[str, Any]] = {}
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

        primary_symbol = self.symbols[0]
        primary_market = market_data.get(primary_symbol, {})
        snapshot = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": primary_symbol,
            "symbols": list(self.symbols),
            "positions": positions,
            "account": account,
            "account_equity": account_equity,
            "total_account_value": total_account_value,
            "total_eq_usd": total_eq_usd,
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
        }
        return snapshot

    def set_poll_interval(self, seconds: int) -> None:
        self._poll_interval = max(1, seconds)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._emit_debug(f"Poll interval updated to {self._poll_interval}s")

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
            self._latest_long_short_ratio.pop(symbol, None)
            self._last_long_short_fetch.pop(symbol, None)

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
                pairs.append(str(inst_id).upper())
        if not pairs:
            return list(self.symbols)
        return sorted(set(pairs))

    async def _fetch_positions(self) -> list[dict[str, Any]]:
        if not self._account_api:
            return []
        kwargs = {"instType": "SWAP"}
        if self._sub_account and self._sub_account_use_master:
            kwargs["subAcct"] = self._sub_account
        response = await asyncio.to_thread(
            self._account_api.get_positions,
            **kwargs,
        )
        data = self._safe_data(response)
        return data

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
                    value = MarketService._extract_equity_value(detail)
                    if value is not None:
                        entry_total += value
            else:
                details.append(entry)
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

        return {
            "details": details,
            "total_equity": total_equity,
            "total_account_value": total_account_value or total_equity,
            "total_eq_usd": total_eq_usd or total_equity,
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

    async def handle_llm_decision(
        self,
        decision: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> None:
        if not decision:
            return
        context = context or {}
        action = (decision.get("action") or "HOLD").upper()
        if action not in {"BUY", "SELL", "HOLD"}:
            self._emit_debug(f"Ignoring unsupported action {action}")
            return
        symbol = str(context.get("symbol") or decision.get("symbol") or self.symbol).upper()
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
        now = time.time()
        summary = (
            f"LLM decision {action} size={decision.get('position_size', '--')} "
            f"conf={decision.get('confidence', '--')} symbol={symbol}"
        )

        if action == "HOLD":
            self._decision_state[symbol] = {"action": action, "timestamp": now}
            self._emit_debug(summary)
            return

        if require_alignment and not self._transition_allowed(current_side, action):
            self._emit_debug(
                f"Guardrail blocked {action} for {symbol}: current side={current_side}"
            )
            self._decision_state[symbol] = {"action": current_side, "timestamp": now}
            return

        last_decision = self._decision_state.get(symbol)
        if cooldown_seconds > 0 and last_decision:
            last_ts = last_decision.get("timestamp")
            if isinstance(last_ts, (int, float)) and now - float(last_ts) < cooldown_seconds:
                remaining = cooldown_seconds - (now - float(last_ts))
                self._emit_debug(
                    f"Guardrail cooldown active for {symbol}; skipping {action} ({remaining:.0f}s left)"
                )
                return

        history = self._recent_trades.setdefault(symbol, deque())
        self._prune_trade_history(history, now, trade_window)
        if trade_limit > 0 and len(history) >= trade_limit:
            self._emit_debug(
                f"Guardrail trade limit hit for {symbol}; skipping {action}"
            )
            return

        history.append(now)
        self._decision_state[symbol] = {"action": action, "timestamp": now}
        self._emit_debug(summary)

        execution_cfg = context.get("execution") or {}
        execution_enabled = bool(execution_cfg.get("enabled"))
        if not execution_enabled:
            self._emit_debug(f"Execution disabled for {symbol}; skipping OKX order")
            return
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot execute decision")
            return

        raw_size = self._extract_float(decision.get("position_size")) or 0.0
        if raw_size <= 0:
            self._emit_debug(f"Execution skipped for {symbol}: invalid position size {raw_size}")
            return

        trade_mode = str(execution_cfg.get("trade_mode") or "cross").lower()
        if trade_mode not in {"cross", "isolated"}:
            trade_mode = "cross"
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
        max_pct = self._extract_float(guardrails.get("max_position_pct"))
        if account_equity and max_pct and last_price:
            notional_cap = max(0.0, account_equity * max_pct)
            contract_cap = notional_cap / last_price if last_price else None
            if contract_cap and contract_cap > 0:
                raw_size = min(raw_size, contract_cap)

        if raw_size < min_size:
            self._emit_debug(
                f"Execution skipped for {symbol}: computed size {raw_size:.6f} below minimum {min_size}"
            )
            return

        side = "buy" if action == "BUY" else "sell"
        pos_side = "long" if action == "BUY" else "short"
        reduce_only = False
        if action == "SELL" and current_side == "LONG":
            pos_side = "long"
            reduce_only = True
        elif action == "BUY" and current_side == "SHORT":
            pos_side = "short"
            reduce_only = True

        client_order_id = f"tai2-{int(now * 1000)}"
        order = await self._submit_order(
            symbol=symbol,
            side=side,
            pos_side=pos_side if dual_side_mode else None,
            size=raw_size,
            trade_mode=trade_mode,
            order_type=order_type,
            reduce_only=reduce_only,
            client_order_id=client_order_id,
        )
        if not order:
            self._emit_debug(f"Order placement failed for {symbol}")
            return

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
        await self._record_trade_execution(
            symbol=symbol,
            side=side,
            price=executed_price,
            amount=executed_size,
            rationale=decision.get("rationale"),
            fee=fee_value,
        )
        await self._publish_snapshot()

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

    def _normalize_order_response(self, response: Any) -> dict[str, Any] | None:
        if not isinstance(response, dict):
            return None
        top_code = str(response.get("code", ""))
        if top_code not in {"0", "200", ""}:
            detail = response.get("msg") or response
            self._emit_debug(f"OKX order rejected: {detail}")
            return None
        data = response.get("data")
        if isinstance(data, list) and data:
            entry = data[0]
            sub_code = str(entry.get("sCode", top_code))
            if sub_code not in {"0", "200", ""}:
                self._emit_debug(f"OKX order failed: {entry.get('sMsg')}")
                return None
            return entry
        return response

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
    ) -> dict[str, Any] | None:
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot place order")
            return None
        payload = {
            "instId": symbol,
            "tdMode": trade_mode,
            "side": side,
            "ordType": order_type,
            "sz": self._format_size(size),
            "clOrdId": client_order_id,
        }
        if pos_side:
            payload["posSide"] = pos_side
        if reduce_only:
            payload["reduceOnly"] = "true"
        if self._sub_account and self._sub_account_use_master:
            payload["subAcct"] = self._sub_account

        def _place() -> Any:
            return self._trade_api.place_order(**payload)

        try:
            response = await asyncio.to_thread(_place)
        except Exception as exc:  # pragma: no cover - network dependency
            self._emit_debug(f"OKX place_order exception: {exc}")
            return None
        return self._normalize_order_response(response)

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
        if not self.settings.database_url:
            return
        if price is None or amount is None:
            return
        try:
            trade = ExecutedTrade(
                symbol=symbol,
                side=side,
                price=Decimal(str(price)),
                amount=Decimal(str(amount)),
                llm_reasoning=rationale,
                fee=Decimal(str(fee)) if fee is not None else None,
            )
            await insert_executed_trade(trade)
        except Exception as exc:  # pragma: no cover - persistence best-effort
            self._emit_debug(f"Failed to persist executed trade: {exc}")


__all__ = ["MarketService"]
