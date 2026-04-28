from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import functools
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
from app.db.postgres import (
    fetch_unreconciled_trades,
    insert_equity_point,
    insert_executed_trade,
    update_entry_fee,
    update_trade_pnl,
)
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
    from okx.websocket.WsPrivateAsync import WsPrivateAsync
except ImportError:  # pragma: no cover
    OkxAccount = None
    OkxTrade = None
    OkxMarket = None
    OkxPublic = None
    OkxTrading = None
    OkxFunding = None
    WsPublicAsync = None
    WsPrivateAsync = None

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

    class SafeWsPrivateAsync(WsPrivateAsync):  # type: ignore[misc]
        """WsPrivateAsync subclass that never stops the shared event loop on close."""

        async def stop(self) -> None:  # type: ignore[override]
            if getattr(self, "factory", None) is not None:
                await self.factory.close()
            if getattr(self, "websocket", None):
                try:
                    await self.websocket.close()
                except Exception:
                    logger.debug("Private WS close failed", exc_info=True)
            # intentionally avoid stopping the global event loop


else:  # pragma: no cover - optional dependency
    SafeWsPublicAsync = None
    SafeWsPrivateAsync = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
PRIVATE_WS_URL_LIVE = "wss://ws.okx.com:8443/ws/v5/private"
PRIVATE_WS_URL_DEMO = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
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
    # Maps each base bar to (higher-TF bar, candle limit).
    # 200 candles gives enough history for EMA_50 and EMA_200 on any HTF.
    # OKX allows up to 300 candles per request.
    _HTF_MAP: dict[str, tuple[str, int]] = {
        "15m": ("1H",  200),
        "1H":  ("4H",  200),
        "4H":  ("1D",  200),
        "1D":  ("",     0),
    }
    SUPPORTED_TIMEFRAMES = set(_TIMEFRAME_CHOICES.values())
    DEFAULT_TIMEFRAME = "4H"
    ISOLATED_WALLET_BOOTSTRAP_PCT = 0.25
    # OKX will reject a new isolated-mode order with 51008 when the required
    # initial margin is below its internal floor.  Block such trades pre-emptively.
    # 5 USDT is a more realistic minimum that prevents pointless downsize retries
    # when the tier-based dynamic floor is unavailable.
    OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD = 5.0
    # After exhausting all 51008 retries for a bootstrap order, block further
    # attempts for this many seconds so we stop hammering OKX.
    BOOTSTRAP_BLOCK_SECONDS = 3600  # 1 hour
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
    # Footprint chart: sliding-window price-level volume profile.
    # Window: how far back (seconds) to include trades in the profile.
    # Bucket ticks: number of instrument tick_size units per price bucket
    # (100 ticks ≈ $10 for BTC, $1 for ETH — produces ~30–60 buckets in a
    # typical 15-minute range, which is concise enough to send to the LLM).
    FOOTPRINT_WINDOW_SECONDS: int = 900   # 15-minute sliding window
    FOOTPRINT_BUCKET_TICKS: int = 100     # bucket width = tick_size × 100

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
        self._ws_private_task: Optional[asyncio.Task] = None
        self._ws_private_client: Any = None  # SafeWsPrivateAsync when connected
        self._latest_positions_raw: list[dict[str, Any]] | None = None
        self._latest_account_raw: list[Any] | None = None
        # Cache of the most-recent successful REST account balance fetch.
        # Used as a fallback when the live refresh returns empty (e.g. during
        # temporary OKX API auth glitches while the private WS is also down).
        self._last_known_account_balance: dict[str, Any] | None = None
        # Last full snapshot; used by _patch_and_publish_snapshot to avoid a
        # full REST round-trip when the private WS delivers fresh account/position data.
        self._last_full_snapshot: dict[str, Any] | None = None
        self._private_ws_patch_pending: bool = False
        self._latest_order_book: dict[str, dict[str, Any]] = {}
        self._latest_depth_metrics: dict[str, list[float]] = {}
        self._latest_ticker: dict[str, dict[str, Any]] = {}
        self._latest_funding: dict[str, dict[str, Any]] = {}
        self._latest_open_interest: dict[str, dict[str, Any]] = {}
        self._latest_liquidations: dict[str, list[dict[str, Any]]] = {}
        self._latest_ohlcv: dict[str, list[list[Any]]] = {}
        self._latest_ohlcv_htf: dict[str, list[list[Any]]] = {}
        self._latest_long_short_ratio: dict[str, dict[str, Any]] = {}
        self._last_long_short_fetch: dict[str, float] = {}
        self._trade_buffers: dict[str, Deque[dict[str, float]]] = {}
        # Footprint chart: per-symbol deque of timestamped, price-tagged trades.
        # Populated alongside _trade_buffers in _handle_ws_message.
        # Each entry: {"ts": float (epoch s), "px": float, "vol": float, "side": float}
        self._footprint_buffers: dict[str, Deque[dict[str, float]]] = {}
        self._decision_state: dict[str, dict[str, Any]] = {}
        self._recent_trades: dict[str, Deque[float]] = {}
        self._position_activity: dict[str, float] = {}
        self._position_protection: dict[str, dict[str, Any]] = {}
        self._protection_sync_ts: dict[str, float] = {}
        self._execution_feedback: Deque[dict[str, Any]] = deque(maxlen=50)
        self._latest_execution_limits: dict[str, dict[str, Any]] = {}
        self._last_margin_guidance: dict[str, dict[str, Any]] = {}
        self._isolated_leverage_cache: dict[str, float] = {}
        self._missing_isolated_wallet_symbols: set[str] = set()
        self._position_tiers: dict[str, dict[str, Any]] = {}
        self._subscribed_symbols: set[str] = set()
        self._available_symbols: list[str] = []
        self._instrument_specs: dict[str, dict[str, float]] = {}
        self._poll_interval = max(1, self.settings.poll_interval)
        self._ohlcv_fetch_limit: int = 200
        self._ohlc_bar = self._normalize_bar(ohlc_bar)
        self._log_sink = log_sink or (lambda msg: None)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._ws_last_debug: Dict[str, float] = {}
        self._wait_for_tp_sl = False
        self._flip_llm_decision = False
        # Symbol screener state
        self._screener_config: dict[str, Any] = {}
        self._screener_last_run: float = 0.0
        self._screener_selected_symbols: list[str] = []
        # Rolling 24h volume history for spike detection (symbol → deque of vol samples).
        self._screener_vol_history: dict[str, deque] = {}
        self._reconcile_task: Optional[asyncio.Task] = None
        self._positions_refresh_task: Optional[asyncio.Task] = None
        self._positions_refresh_interval: int = 10  # seconds between fast position/equity refreshes
        # Notional (USD) reserved for in-flight orders, keyed by symbol.
        # Concurrent handle_llm_decision coroutines deduct these before sizing
        # so they never collectively over-commit the same USDT pool.
        self._pending_notional: dict[str, float] = {}
        # Symbols whose bootstrap (first isolated-wallet seed) has exhausted all
        # 51008 retries.  Mapped to the epoch time the block was set.  Cleared
        # automatically after BOOTSTRAP_BLOCK_SECONDS so the bot can retry later.
        self._bootstrap_blocked: dict[str, float] = {}
        self._wake_poll: asyncio.Event = asyncio.Event()
        self._strategy_config: dict[str, Any] = {}
        self._footprint_config: dict[str, Any] = {}
        self._skimming_triggered: set[str] = set()
        # Shotgun strategy: equity baseline captured at each prompt run.
        # _check_shotgun() compares current equity against this anchor and
        # closes all (or only losing) positions when a TP/SL threshold is hit.
        self._shotgun_baseline_equity: float | None = None
        self._shotgun_fired: bool = False
        self._shotgun_closing: set[str] = set()  # symbols with in-flight Shotgun close orders
        # Protector strategy: tracks symbols whose SL update task is in-flight
        # so concurrent refresh ticks don't spawn duplicate amendment tasks.
        self._protector_updating: set[str] = set()
        # Commutator strategy: when a position's loss hits the configured threshold,
        # close it and open the reversed side.  Tracks flips per position lifecycle
        # (counts are cleared automatically when the symbol leaves open positions).
        self._commutator_flip_counts: dict[str, int] = {}
        self._commutator_flipping: set[str] = set()
        # Alternator strategy: oscillate between long/short on profit/loss thresholds.
        # Mutually exclusive with Skimming and Commutator.
        self._alternator_flip_counts: dict[str, int] = {}
        self._alternator_flipping: set[str] = set()
        self._alternator_riding: set[str] = set()
        # Trailing-reverse state: track peak PnL once threshold is crossed.
        self._alternator_above_threshold: set[str] = set()
        self._alternator_peak_pnl_pct: dict[str, float] = {}
        self._alternator_peak_pnl_usd: dict[str, float] = {}
        # Trailing close state: track peak PnL for flat-close on pullback.
        self._alternator_close_above_threshold: set[str] = set()
        self._alternator_close_peak_pnl_pct: dict[str, float] = {}
        self._alternator_close_peak_pnl_usd: dict[str, float] = {}
        self._alternator_ws_check_pending: bool = False
        # Continuous LLM supervision (optional, controlled by alternator config).
        # The LLMService instance is injected after construction via set_llm_service().
        self._llm_service: Any = None
        self._llm_mandate: dict[str, dict[str, Any]] = {}  # per-symbol active mandate
        self._llm_supervision_running: set[str] = set()  # prevents concurrent calls
        # Launcher: rule-based entry/exit + optional LLM trade filter.
        # Stored separately from _strategy_config (lives in runtime_config["launcher"]).
        self._launcher_config: dict[str, Any] = {}
        # _launcher_entering: per-symbol in-flight entry guard.
        self._launcher_entering: set[str] = set()
        # _launcher_in_position: symbols where Launcher opened a trade.
        #   value → {"side": "long"|"short", "pos_side": str|None}
        self._launcher_in_position: dict[str, dict[str, Any]] = {}
        # Scheduling state (launcher_only mode).
        self._launcher_last_entry_check: float = 0.0
        # on_close mode: symbols that just had a Launcher-close accepted.
        self._launcher_trigger_symbols: set[str] = set()
        # on_close mode: were there any open positions on the previous tick?
        self._launcher_had_positions: bool = False

    async def start(self) -> None:
        """Launch the market snapshot poller and websocket consumers if not already running."""
        if self._poller_task:
            return
        await self._hydrate_cached_annotations()
        if self._enable_websocket:
            self._ws_task = asyncio.create_task(self._run_public_ws(), name="okx-ws")
            self._ws_private_task = asyncio.create_task(self._run_private_ws(), name="okx-ws-private")
        self._poller_task = asyncio.create_task(self._poll_loop(), name="okx-market-poller")
        self._positions_refresh_task = asyncio.create_task(
            self._positions_refresh_loop(), name="okx-positions-refresh"
        )
        logger.info("MarketService started for %s", ", ".join(self.symbols))

    async def stop(self) -> None:
        """Cancel background tasks, tear down websockets, and reset runtime bookkeeping."""
        if self._poller_task:
            self._poller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poller_task
            self._poller_task = None
        if self._reconcile_task:
            self._reconcile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconcile_task
            self._reconcile_task = None
        if self._ws_task:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None
        if self._ws_client:
            await self._ws_client.stop()
            self._ws_client = None
            self._subscribed_symbols.clear()
        if self._ws_private_task:
            self._ws_private_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_private_task
            self._ws_private_task = None
        if self._ws_private_client:
            await self._ws_private_client.stop()
            self._ws_private_client = None
        if self._positions_refresh_task:
            self._positions_refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._positions_refresh_task
            self._positions_refresh_task = None
        logger.info("MarketService stopped for %s", ", ".join(self.symbols))

    async def _poll_loop(self) -> None:
        """Continuously refresh state snapshots on a fixed interval until cancelled."""
        loop_count = 0
        while True:
            interval = max(1, self._poll_interval)
            try:
                await self.refresh_snapshot(reason="poller")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Failed to refresh market snapshot: %s", exc)
            loop_count += 1
            # Run fill reconciliation every other poll to keep realized PnL up-to-date
            # without hammering the REST endpoint on every tick.
            if loop_count % 2 == 0 and self.settings.database_url:
                try:
                    await self._reconcile_fills()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - best-effort
                    logger.warning("Fill reconciliation error: %s", exc)
            # Sleep for the configured interval, but allow set_poll_interval() to
            # interrupt the wait immediately so the new cadence kicks in right away.
            try:
                await asyncio.wait_for(self._wake_poll.wait(), timeout=interval)
                self._wake_poll.clear()
            except asyncio.TimeoutError:
                pass

    async def _run_public_ws(self) -> None:
        """Subscribe to public OKX feeds and stream updates into the in-memory caches.

        Runs a reconnect loop with exponential backoff so transient network errors
        and OKX server-side disconnects do not permanently stop the feed.
        """
        client_factory = self._websocket_factory or WsPublicAsync
        if client_factory is None:
            logger.warning("python-okx websocket modules not available; skipping public WS")
            return
        delay = 5.0
        while True:
            try:
                self._ws_client = client_factory(PUBLIC_WS_URL)
                await self._ws_client.connect()
                channels = self._build_channel_args(self.symbols)
                await self._ws_client.subscribe(channels, self._handle_ws_message)
                self._subscribed_symbols = set(self.symbols)
                delay = 5.0  # reset backoff on successful connect
                logger.info("Public WS connected and subscribed to %s", list(self._subscribed_symbols))

                # OKX requires an application-level "ping" every ≤25 s or the
                # server closes the connection.  Run a concurrent ping task.
                async def _ping_loop(client: Any) -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await client.websocket.send("ping")
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping_loop(self._ws_client))
                try:
                    await self._ws_client.consume()
                finally:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task
                    await self._ws_client.stop()
                    self._ws_client = None
                    self._subscribed_symbols.clear()

                logger.warning("Public WS consume() returned; reconnecting in %.0fs", delay)
            except asyncio.CancelledError:
                logger.info("Public WS task cancelled")
                return
            except Exception as exc:
                logger.warning("Public WS error (%s); reconnecting in %.0fs", exc, delay)
                if self._ws_client is not None:
                    try:
                        await self._ws_client.stop()
                    except Exception:
                        pass
                    self._ws_client = None
                    self._subscribed_symbols.clear()

            await asyncio.sleep(delay)
            delay = min(delay * 2, 120.0)  # cap at 2 minutes

    async def _run_private_ws(self) -> None:
        """Authenticate and subscribe to private OKX feeds (account, positions).

        Runs a reconnect loop with exponential backoff so transient network errors
        and OKX server-side disconnects do not permanently stop the feed.
        """
        WsPrivateClass = SafeWsPrivateAsync if SafeWsPrivateAsync is not None else None  # type: ignore[name-defined]
        if WsPrivateClass is None:
            logger.warning("WsPrivateAsync not available; private WS stream disabled")
            return
        if not (
            self.settings.okx_api_key
            and self.settings.okx_secret_key
            and self.settings.okx_passphrase
        ):
            logger.warning("OKX credentials missing; private WS stream disabled")
            return
        url = PRIVATE_WS_URL_DEMO if str(self._okx_flag) == "1" else PRIVATE_WS_URL_LIVE
        delay = 5.0
        while True:
            try:
                logger.info("Private WS connecting to %s", url)
                self._ws_private_client = WsPrivateClass(
                    apiKey=self.settings.okx_api_key,
                    passphrase=self.settings.okx_passphrase,
                    secretKey=self.settings.okx_secret_key,
                    url=url,
                    useServerTime=False,
                )
                await self._ws_private_client.connect()
                channels = [
                    {"channel": "account"},
                    {"channel": "positions", "instType": "SWAP"},
                ]
                # subscribe() performs login + 5 s wait + channel subscription internally
                await self._ws_private_client.subscribe(channels, self._handle_private_ws_message)
                delay = 5.0  # reset backoff on successful connect
                logger.info("Private WS connected and subscribed")

                # OKX requires an application-level "ping" every ≤25 s.
                async def _ping_loop_priv(client: Any) -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await client.websocket.send("ping")
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping_loop_priv(self._ws_private_client))
                try:
                    await self._ws_private_client.consume()
                finally:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task
                    await self._ws_private_client.stop()
                    self._ws_private_client = None
                    self._latest_positions_raw = None
                    self._latest_account_raw = None

                logger.warning("Private WS consume() returned; reconnecting in %.0fs", delay)
            except asyncio.CancelledError:
                logger.info("Private WS task cancelled")
                return
            except Exception as exc:
                logger.warning("Private WS error (%s); reconnecting in %.0fs", exc, delay)
                if self._ws_private_client is not None:
                    try:
                        await self._ws_private_client.stop()
                    except Exception:
                        pass
                    self._ws_private_client = None
                    self._latest_positions_raw = None
                    self._latest_account_raw = None

            await asyncio.sleep(delay)
            delay = min(delay * 2, 120.0)  # cap at 2 minutes

    def _handle_private_ws_message(self, message: Any) -> None:
        """Route private WS frames into positions and account balance caches."""
        if isinstance(message, (bytes, bytearray)):
            try:
                message = message.decode()
            except Exception:  # pragma: no cover - defensive decoding
                return
        if isinstance(message, str):
            if message == "pong":  # OKX keepalive response — discard
                return
            try:
                message = json.loads(message)
            except json.JSONDecodeError:
                return
        if not isinstance(message, dict):
            return
        # Skip control frames: login acknowledgements, subscribe confirmations, pings.
        if message.get("event"):
            return
        arg = message.get("arg") or {}
        channel = arg.get("channel")
        action = message.get("action", "snapshot")
        data = message.get("data") or []
        if channel == "positions" and isinstance(data, list):
            if action == "snapshot":
                # Full replacement — OKX sends a complete list on subscribe
                self._latest_positions_raw = list(data)
            elif action == "update":
                if self._latest_positions_raw is None:
                    self._latest_positions_raw = list(data)
                else:
                    # Upsert by (instId, posSide); remove positions where pos == "0"
                    key_to_idx: dict[tuple[str, str], int] = {
                        (str(p.get("instId", "")), str(p.get("posSide", ""))): i
                        for i, p in enumerate(self._latest_positions_raw)
                        if isinstance(p, dict)
                    }
                    updated: list[dict[str, Any] | None] = list(self._latest_positions_raw)  # type: ignore[assignment]
                    for entry in data:
                        if not isinstance(entry, dict):
                            continue
                        k = (str(entry.get("instId", "")), str(entry.get("posSide", "")))
                        pos_val = self._extract_float(entry.get("pos"))
                        if pos_val is not None and pos_val == 0:
                            # Position closed — purge from cache
                            if k in key_to_idx:
                                updated[key_to_idx[k]] = None
                        elif k in key_to_idx:
                            updated[key_to_idx[k]] = entry
                        else:
                            updated.append(entry)
                    self._latest_positions_raw = [p for p in updated if p is not None]  # type: ignore[misc]
            self._emit_debug(
                f"Private WS: positions cache updated ({len(self._latest_positions_raw)} records)",
                mirror_logger=False,
            )
            if not self._private_ws_patch_pending:
                self._private_ws_patch_pending = True
                asyncio.create_task(
                    self._schedule_patch_publish(), name="okx-ws-private-patch"
                )
            # Trigger high-frequency alternator check for trailing-reverse mode.
            _altr_cfg = self._strategy_config.get("alternator") or {}
            if (
                _altr_cfg.get("enabled")
                and _altr_cfg.get("trailing_reverse")
                and not self._alternator_ws_check_pending
            ):
                self._alternator_ws_check_pending = True
                asyncio.create_task(
                    self._schedule_alternator_ws_check(),
                    name="alternator-ws-check",
                )
        elif channel == "account" and isinstance(data, list) and data:
            # Account channel always delivers a full snapshot of the top-level account
            self._latest_account_raw = list(data)
            _ws_balance = self._normalize_account_balances(self._latest_account_raw)
            if _ws_balance.get("total_eq_usd", 0.0) or _ws_balance.get("total_equity", 0.0):
                self._last_known_account_balance = _ws_balance
            self._emit_debug("Private WS: account balance cache updated", mirror_logger=False)
            if not self._private_ws_patch_pending:
                self._private_ws_patch_pending = True
                asyncio.create_task(
                    self._schedule_patch_publish(), name="okx-ws-private-patch"
                )

    def _handle_ws_message(self, message: Any) -> None:
        """Route websocket frames into ticker/order book/liquidation caches for active symbols."""
        if isinstance(message, (bytes, bytearray)):
            try:
                message = message.decode()
            except Exception:  # pragma: no cover - defensive decoding
                return
        if isinstance(message, str):
            if message == "pong":  # OKX keepalive response — discard
                return
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
                vol_val = float(entry.get("sz") or entry.get("vol") or 0.0)
                side_val = 1.0 if entry.get("side") == "buy" else -1.0
                buffer.append(
                    {
                        "side": side_val,
                        "volume": vol_val,
                    }
                )
                # Feed the footprint buffer with full price + timestamp data.
                px_val = self._extract_float(entry.get("px"))
                if px_val and vol_val:
                    ts_ms = self._extract_float(entry.get("ts"))
                    ts_epoch = (ts_ms / 1000.0) if ts_ms else time.time()
                    fp_buf = self._get_footprint_buffer(symbol)
                    fp_buf.append(
                        {"ts": ts_epoch, "px": px_val, "vol": vol_val, "side": side_val}
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
            indicators["structure"] = self._compute_structure(ohlcv)
            ohlcv_htf = await self._fetch_ohlcv_htf(symbol)
            if ohlcv_htf:
                htf_bar, _ = self._HTF_MAP.get(self._ohlc_bar, ("", 0))
                indicators["ohlcv_htf"] = ohlcv_htf
                indicators["htf_indicators"] = self._compute_indicators(ohlcv_htf)
                if htf_bar:
                    indicators["ohlcv_htf_bar"] = htf_bar
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
                    "ct_val": spec.get("ct_val") or 1.0,
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
            "poll_interval": self.settings.poll_interval,
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

    def _isolated_position_margin(
        self,
        positions: list[dict[str, Any]] | None,
        symbol: str,
        pos_side: str | None = None,
    ) -> tuple[float | None, dict[str, Any] | None]:
        """Return the isolated wallet balance for a symbol/side if OKX reported one."""
        if not positions:
            return None, None
        normalized_symbol = symbol.upper()
        normalized_side = (pos_side or "").lower()
        fallback_entry: dict[str, Any] | None = None
        for entry in positions:
            if not isinstance(entry, dict):
                continue
            entry_symbol = str(entry.get("instId") or entry.get("symbol") or "").upper()
            if entry_symbol != normalized_symbol:
                continue
            mode = str(entry.get("mgnMode") or entry.get("marginMode") or "isolated").lower()
            if mode != "isolated":
                continue
            # Skip closed-position stale entries (pos=0).  OKX retains these in the
            # positions list briefly after closure; attempting to add margin to them
            # returns 59300 "Position does not exist" and would block new entries.
            pos_size = self._extract_float(entry.get("pos"))
            if pos_size is not None and pos_size == 0:
                continue
            entry_side = str(entry.get("posSide") or "").lower()
            if normalized_side and entry_side and entry_side != normalized_side:
                if fallback_entry is None:
                    fallback_entry = entry
                continue
            target_entry = entry
            value = None
            for field in ("margin", "cashBal", "availEq", "equity", "eq", "availBal"):
                candidate = self._extract_float(target_entry.get(field))
                if candidate is not None:
                    value = candidate
                    break
            if value is not None:
                return value, target_entry
            return 0.0, target_entry
        if fallback_entry:
            for field in ("margin", "cashBal", "availEq", "equity", "eq", "availBal"):
                candidate = self._extract_float(fallback_entry.get(field))
                if candidate is not None:
                    return candidate, fallback_entry
            return 0.0, fallback_entry
        return None, None

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

    def set_strategy_config(self, config: dict[str, Any]) -> None:
        """Apply an updated strategy configuration (e.g. skimming settings)."""
        self._strategy_config = config or {}
        self._emit_debug(f"Strategy config updated: {self._strategy_config}")

    def set_footprint_config(self, config: dict[str, Any]) -> None:
        """Apply updated footprint guardrail config (e.g. bucket_pct)."""
        self._footprint_config = config or {}

    def set_launcher_config(self, config: dict[str, Any]) -> None:
        """Update the Launcher configuration at runtime (called from CFG save)."""
        self._launcher_config = config or {}
        self._emit_debug(f"Launcher config updated: {self._launcher_config}")

    def set_llm_service(self, llm_service: Any) -> None:
        """Inject the shared LLMService instance for continuous supervision calls."""
        self._llm_service = llm_service

    async def _check_skimming(self) -> None:
        """Close any open position whose unrealised PnL ratio meets the skimming threshold.

        Reads positions from the in-memory snapshot that _patch_and_publish_snapshot() just
        wrote (mirrors Redis) — no extra REST or WS fetch is needed.
        """
        skimming = self._strategy_config.get("skimming") or {}
        if not skimming.get("enabled"):
            return
        if (self._strategy_config.get("alternator") or {}).get("enabled"):
            return
        threshold = self._extract_float(skimming.get("threshold_pct"))
        if threshold is None or threshold <= 0:
            self._emit_debug(f"Skimming: invalid threshold ({skimming.get('threshold_pct')!r}) — skipping")
            return
        threshold_ratio = threshold / 100.0
        sl_pct = self._extract_float(skimming.get("stop_loss_pct"))
        sl_ratio = (-abs(sl_pct) / 100.0) if (sl_pct is not None and sl_pct > 0) else None
        snapshot = self._last_full_snapshot
        if not snapshot:
            self._emit_debug("Skimming: no snapshot available yet — skipping")
            return
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            self._emit_debug("Skimming: snapshot has no open positions")
            return
        # Remove closed positions from the triggered guard set.
        # Only count symbols with a non-zero pos — OKX transiently returns pos=0 rows
        # for recently-closed positions which would otherwise keep the guard stuck forever.
        active_symbols = {
            str(p.get("instId", "")).upper()
            for p in positions
            if isinstance(p, dict) and self._extract_float(p.get("pos"))
        }
        self._skimming_triggered &= active_symbols
        self._emit_debug(
            f"Skimming: checking {len(positions)} position(s), threshold={threshold:.2f}% "
            f"({threshold_ratio:.4f}), sl_ratio={sl_ratio}, already-triggered={self._skimming_triggered or 'none'}"
        )
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                self._emit_debug("Skimming: skipping position with no instId")
                continue
            if symbol in self._skimming_triggered:
                self._emit_debug(f"Skimming: {symbol} — already triggered, awaiting close confirmation")
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                self._emit_debug(f"Skimming: {symbol} — position size is zero, skipping")
                continue
            upl_ratio = self._extract_float(pos.get("uplRatio"))
            self._emit_debug(
                f"Skimming: {symbol} uplRatio={upl_ratio!r} ({(upl_ratio * 100) if upl_ratio is not None else 'n/a'}%), "
                f"threshold={threshold:.2f}%, sl_pct={sl_pct!r}, pos={pos_val!r}, "
                f"mgnMode={pos.get('mgnMode')!r}, posSide={pos.get('posSide')!r}"
            )
            if upl_ratio is None:
                self._emit_debug(f"Skimming: {symbol} — uplRatio missing from position data, cannot evaluate")
                continue
            hit_tp = upl_ratio >= threshold_ratio
            hit_sl = sl_ratio is not None and upl_ratio <= sl_ratio
            if not hit_tp and not hit_sl:
                self._emit_debug(
                    f"Skimming: {symbol} — uplRatio {upl_ratio:.4%} within range "
                    f"[{sl_ratio:.4%} .. {threshold_ratio:.4%}], no action"
                    if sl_ratio is not None else
                    f"Skimming: {symbol} — uplRatio {upl_ratio:.4%} below threshold {threshold_ratio:.4%}, no action"
                )
                continue
            trigger_reason = "TP" if hit_tp else "SL"
            pos_side = str(pos.get("posSide", "")).lower()
            trade_mode = str(pos.get("mgnMode") or "").lower() or None
            if pos_side in ("long",):
                close_side, effective_pos_side = "sell", "long"
            elif pos_side in ("short",):
                close_side, effective_pos_side = "buy", "short"
            else:  # "net" or unset — determine direction from sign of pos
                close_side = "sell" if pos_val > 0 else "buy"
                effective_pos_side = None
            contracts = abs(pos_val)
            self._skimming_triggered.add(symbol)
            self._emit_debug(
                f"Skimming {trigger_reason} triggered: {symbol} uplRatio={upl_ratio:.4%} "
                f"({'>='+str(round(threshold_ratio*100,4))+'%' if hit_tp else '<='+str(round(sl_ratio*100,4))+'%'}); "
                f"submitting {close_side} close, contracts={contracts}, posSide={effective_pos_side!r}, tdMode={trade_mode!r}"
            )
            asyncio.create_task(
                self._skim_close_position(symbol, close_side, effective_pos_side, contracts, trade_mode),
                name=f"skim-close-{symbol}",
            )

    async def _shotgun_close_position(
        self,
        symbol: str,
        close_side: str,
        pos_side: str | None,
        contracts: float,
        trade_mode: str | None,
    ) -> None:
        """Submit a market reduce-only order on behalf of the Shotgun strategy.

        On success the symbol stays in ``_shotgun_closing`` until the position
        settles and is pruned by the Alternator's ``active_symbols`` logic.
        On rejection or exception the symbol is removed immediately so the
        Alternator can resume protecting the still-open position.
        """
        coid = self._generate_client_order_id("shot")
        resolved_trade_mode = trade_mode or "cross"
        accepted = False
        try:
            result = await self._submit_order(
                symbol=symbol,
                side=close_side,
                pos_side=pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=True,
                client_order_id=coid,
                attach_algo_orders=None,
            )
            if result is None:
                self._emit_debug(f"Shotgun: {symbol} — trade API unavailable")
            else:
                order_result = result[0] if isinstance(result, tuple) else result
                if order_result:
                    self._emit_debug(f"Shotgun: {symbol} close order accepted")
                    accepted = True
                else:
                    self._emit_debug(f"Shotgun: {symbol} close order rejected — Alternator will resume")
        except Exception as exc:
            logger.warning("Shotgun close order error for %s: %s", symbol, exc)
        finally:
            if not accepted:
                self._shotgun_closing.discard(symbol)

    async def _skim_close_position(
        self,
        symbol: str,
        close_side: str,
        pos_side: str | None,
        contracts: float,
        trade_mode: str | None,
    ) -> None:
        """Submit a market reduce-only order to close a position that hit the skimming threshold."""
        coid = self._generate_client_order_id("skim")
        # Honor the margin mode of the position itself (isolated or cross). OKX requires the
        # close order tdMode to match the open position.  Fall back to "cross" only if absent.
        resolved_trade_mode = trade_mode or "cross"
        try:
            result = await self._submit_order(
                symbol=symbol,
                side=close_side,
                pos_side=pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=True,
                client_order_id=coid,
                attach_algo_orders=None,
            )
            if result is None:
                self._emit_debug(f"Skimming: {symbol} — trade API unavailable, will retry")
                self._skimming_triggered.discard(symbol)
                return
            order_result = result[0] if isinstance(result, tuple) else result
            if order_result:
                self._emit_debug(f"Skimming: {symbol} close order accepted")
            else:
                self._emit_debug(f"Skimming: {symbol} close order rejected, will retry")
                self._skimming_triggered.discard(symbol)
        except Exception as exc:
            logger.warning("Skimming close order error for %s: %s", symbol, exc)
            self._skimming_triggered.discard(symbol)

    # ── Shotgun strategy ──────────────────────────────────────────────────────

    def record_shotgun_baseline(self, equity: float) -> None:
        """Capture the equity anchor at the start of each prompt-scheduler cycle.

        Called by the prompt scheduler immediately after refreshing the snapshot.
        Resets ``_shotgun_fired`` so the strategy can trigger once per cycle.
        """
        if equity <= 0:
            return
        self._shotgun_baseline_equity = float(equity)
        self._shotgun_fired = False
        self._emit_debug(f"Shotgun: baseline equity set to {equity:.4f} USDT")

    async def _check_shotgun(self) -> None:
        """Close positions when total account equity has moved past configured TP/SL thresholds.

        Compares ``account_equity`` in the in-memory snapshot against
        ``_shotgun_baseline_equity`` (set at the last prompt-scheduler run).
        Fires at most once per scheduler cycle (guarded by ``_shotgun_fired``).

        Config keys (under strategy.shotgun):
          enabled            – bool, must be True
          tp_pct             – float | None, close ALL when equity gained ≥ this %
          tp_usd             – float | None, close ALL when equity gained ≥ this USDT
          sl_pct             – float | None, close when equity dropped ≥ this %
          sl_usd             – float | None, close when equity dropped ≥ this USDT
          close_only_negative – bool (default False), on SL: close only positions
                               with uplRatio < 0 instead of all positions
        """
        shotgun = self._strategy_config.get("shotgun") or {}
        if not shotgun.get("enabled"):
            return
        if self._shotgun_fired:
            return
        snapshot = self._last_full_snapshot
        if not snapshot:
            return

        # No point tracking equity when there's nothing to close.
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            self._shotgun_baseline_equity = None
            self._shotgun_fired = False
            return

        current_equity = self._extract_float(snapshot.get("account_equity"))
        if current_equity is None:
            return

        if self._shotgun_baseline_equity is None:
            # Bootstrap baseline from current equity so the strategy starts
            # tracking immediately, even before the scheduler has run a tick.
            self._shotgun_baseline_equity = current_equity
            self._emit_debug(
                f"Shotgun: baseline auto-bootstrapped to {current_equity:.4f} USDT"
            )
            return

        baseline = self._shotgun_baseline_equity
        delta_usd = current_equity - baseline
        delta_pct = (delta_usd / baseline * 100.0) if baseline > 0 else 0.0

        tp_pct = self._extract_float(shotgun.get("tp_pct"))
        tp_usd = self._extract_float(shotgun.get("tp_usd"))
        sl_pct = self._extract_float(shotgun.get("sl_pct"))
        sl_usd = self._extract_float(shotgun.get("sl_usd"))
        close_only_negative = bool(shotgun.get("close_only_negative", False))

        self._emit_debug(
            f"Shotgun: equity baseline={baseline:.4f} current={current_equity:.4f} "
            f"delta={delta_usd:+.4f} USDT ({delta_pct:+.4f}%); "
            f"tp_pct={tp_pct} tp_usd={tp_usd} sl_pct={sl_pct} sl_usd={sl_usd} "
            f"close_only_negative={close_only_negative}"
        )

        hit_tp = (tp_pct is not None and delta_pct >= tp_pct) or (
            tp_usd is not None and delta_usd >= tp_usd
        )
        hit_sl = (sl_pct is not None and delta_pct <= -abs(sl_pct)) or (
            sl_usd is not None and delta_usd <= -abs(sl_usd)
        )

        if not hit_tp and not hit_sl:
            return

        trigger_reason = "TP" if hit_tp else "SL"
        if not positions:
            self._emit_debug(f"Shotgun {trigger_reason}: no open positions to close")
            self._shotgun_fired = True
            return

        self._shotgun_fired = True
        self._emit_debug(
            f"Shotgun {trigger_reason} triggered: delta={delta_usd:+.4f} USDT ({delta_pct:+.4f}%); "
            f"closing {'negative-PnL' if (hit_sl and close_only_negative) else 'all'} positions"
        )

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                continue

            # SL + close_only_negative: skip positions with positive unrealized PnL
            if hit_sl and close_only_negative:
                upl_ratio = self._extract_float(pos.get("uplRatio"))
                if upl_ratio is not None and upl_ratio >= 0:
                    self._emit_debug(
                        f"Shotgun SL: skipping {symbol} (uplRatio={upl_ratio:.4%} ≥ 0, close_only_negative=True)"
                    )
                    continue

            pos_side = str(pos.get("posSide", "")).lower()
            trade_mode = str(pos.get("mgnMode") or "").lower() or None
            if pos_side == "long":
                close_side, effective_pos_side = "sell", "long"
            elif pos_side == "short":
                close_side, effective_pos_side = "buy", "short"
            else:
                close_side = "sell" if (pos_val > 0) else "buy"
                effective_pos_side = None

            contracts = abs(pos_val)
            self._emit_debug(
                f"Shotgun {trigger_reason}: closing {symbol} side={close_side} "
                f"contracts={contracts} posSide={effective_pos_side!r}"
            )
            self._shotgun_closing.add(symbol)
            asyncio.create_task(
                self._shotgun_close_position(symbol, close_side, effective_pos_side, contracts, trade_mode),
                name=f"shotgun-close-{symbol}",
            )

    # ── Protector strategy ───────────────────────────────────────────────────

    async def _check_protector(self) -> None:
        """Ratchet each position's stop-loss into profit as unrealised PnL climbs.

        Runs every ``_positions_refresh_interval`` seconds inside
        ``_positions_refresh_loop``.  Fires an amendment task for a symbol at
        most once at a time (guarded by ``_protector_updating``).

        Config keys (under strategy.protector):
          enabled      – bool, must be True
          activate_pct – float, minimum uplRatio % before the strategy activates
          step_pct     – float, PnL % increment at which SL is re-evaluated
          lock_ratio   – float, fraction of the reached step to lock in as SL

        Formula (long example, activate=10, step=10, lock=0.5):
          uplRatio 12 % → step 1 → effective_lock=50%  → SL at entry × (1 + 5 %)
          uplRatio 22 % → step 2 → effective_lock=75%  → SL at entry × (1 + 15 %)
          uplRatio 32 % → step 3 → effective_lock=83%  → SL at entry × (1 + 25 %)
        SL only ever moves in the profitable direction (ratchet).
        effective_lock = 1 − (1 − lock_ratio) / step_number
        """
        protector = self._strategy_config.get("protector") or {}
        if not protector.get("enabled"):
            return
        activate_pct = self._extract_float(protector.get("activate_pct"))
        step_pct = self._extract_float(protector.get("step_pct"))
        lock_ratio = self._extract_float(protector.get("lock_ratio"))
        if not activate_pct or not step_pct or not lock_ratio:
            self._emit_debug(
                f"Protector: invalid config (activate_pct={activate_pct!r}, "
                f"step_pct={step_pct!r}, lock_ratio={lock_ratio!r}) — skipping"
            )
            return
        if activate_pct <= 0 or step_pct <= 0 or lock_ratio <= 0:
            self._emit_debug(
                f"Protector: config values must be > 0 "
                f"(activate_pct={activate_pct}, step_pct={step_pct}, lock_ratio={lock_ratio}) — skipping"
            )
            return

        snapshot = self._last_full_snapshot
        if not snapshot:
            self._emit_debug("Protector: no snapshot available yet — skipping")
            return
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            self._emit_debug("Protector: snapshot has no open positions")
            return

        self._emit_debug(
            f"Protector: checking {len(positions)} position(s), "
            f"activate_pct={activate_pct}%, step_pct={step_pct}%, lock_ratio={lock_ratio}"
        )
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                continue
            if symbol in self._protector_updating:
                self._emit_debug(f"Protector: {symbol} update already in-flight — skipping")
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                continue
            upl_ratio = self._extract_float(pos.get("uplRatio"))
            upl_pct = upl_ratio * 100.0 if upl_ratio is not None else None
            self._emit_debug(
                f"Protector: {symbol} uplRatio={upl_ratio!r} "
                f"({f'{upl_pct:.2f}%' if upl_pct is not None else 'n/a'}), "
                f"activate_pct={activate_pct}%, avgPx={pos.get('avgPx')!r}"
            )
            if upl_ratio is None or upl_ratio <= 0:
                continue
            if upl_pct < activate_pct:
                continue

            # Snap down to the nearest step boundary.
            step_level = math.floor(upl_pct / step_pct) * step_pct
            if step_level <= 0:
                continue
            # Progressive lock: at step N the effective lock ratio grows towards 1.0
            # so the SL trails price more tightly as profits compound.
            # Formula: effective_lock = 1 - (1 - lock_ratio) / step_number
            # At step 1: lock_ratio (base); at step 2: midpoint; approaches 1.0.
            step_number = step_level / step_pct  # e.g. 1.0, 2.0, 3.0 …
            effective_lock = 1.0 - (1.0 - lock_ratio) / step_number
            new_sl_pct = step_level * effective_lock

            avg_px = self._extract_float(pos.get("avgPx"))
            if not avg_px or avg_px <= 0:
                continue

            pos_side = str(pos.get("posSide", "")).lower()
            if pos_side == "long" or (not pos_side and pos_val > 0):
                new_sl_price = avg_px * (1.0 + new_sl_pct / 100.0)
                is_long = True
            elif pos_side == "short" or (not pos_side and pos_val < 0):
                new_sl_price = avg_px * (1.0 - new_sl_pct / 100.0)
                is_long = False
            else:
                continue

            # Ratchet: only update if the new SL is strictly better than current.
            current_protection = self._position_protection.get(symbol) or {}
            current_sl = self._extract_float(current_protection.get("stop_loss"))
            if current_sl is not None:
                if is_long and new_sl_price <= current_sl:
                    self._emit_debug(
                        f"Protector: {symbol} SL already at {current_sl:.6f} "
                        f"\u2265 new {new_sl_price:.6f} \u2014 no update"
                    )
                    continue
                if not is_long and new_sl_price >= current_sl:
                    self._emit_debug(
                        f"Protector: {symbol} SL already at {current_sl:.6f} "
                        f"\u2264 new {new_sl_price:.6f} \u2014 no update"
                    )
                    continue

            trade_mode = str(pos.get("mgnMode") or "").lower() or "cross"
            action = "BUY" if is_long else "SELL"
            resolved_pos_side = pos_side if pos_side in ("long", "short") else None
            self._emit_debug(
                f"Protector: {symbol} uplRatio={upl_pct:.2f}% step={step_level:.1f}% "
                f"lock={effective_lock:.2%} (base={lock_ratio:.2%}) \u2192 new SL {new_sl_pct:.2f}% above entry "
                f"({avg_px:.6f}) = {new_sl_price:.6f}"
            )
            self._protector_updating.add(symbol)
            asyncio.create_task(
                self._protector_update_sl(
                    symbol=symbol,
                    new_sl_price=new_sl_price,
                    pos_side=resolved_pos_side,
                    trade_mode=trade_mode,
                    action=action,
                ),
                name=f"protector-sl-{symbol}",
            )

    async def _protector_update_sl(
        self,
        *,
        symbol: str,
        new_sl_price: float,
        pos_side: str | None,
        trade_mode: str,
        action: str,
    ) -> None:
        """Cancel the existing TP/SL algo and re-place it with the ratcheted SL price."""
        symbol_key = symbol.upper()
        try:
            # Capture TP before cancellation removes the protection cache entry.
            current_protection = self._position_protection.get(symbol_key) or {}
            current_tp = self._extract_float(current_protection.get("take_profit"))
            await self._cancel_position_protection(symbol)
            # Brief pause so OKX registers the cancellation before the new order arrives.
            await asyncio.sleep(0.5)
            result = await self._place_position_protection(
                symbol=symbol,
                trade_mode=trade_mode,
                action=action,
                take_profit_price=current_tp,
                stop_loss_price=new_sl_price,
                dual_side_mode=bool(pos_side),
                pos_side=pos_side,
            )
            if result:
                self._emit_debug(
                    f"Protector: {symbol_key} SL moved to {new_sl_price:.6f} "
                    f"(tp preserved={current_tp}, algo={result.get('algo_cl_ord_id')})"
                )
            else:
                self._emit_debug(
                    f"Protector: failed to place updated SL for {symbol_key}"
                )
        except Exception as exc:  # pragma: no cover - network safety
            logger.warning("Protector SL update error for %s: %s", symbol_key, exc)
        finally:
            self._protector_updating.discard(symbol_key)

    # ── Commutator strategy ──────────────────────────────────────────────────

    async def _check_commutator(self) -> None:
        """Reverse a losing position when its unrealised PnL drops past a threshold.

        Runs every ``_positions_refresh_interval`` seconds inside
        ``_positions_refresh_loop``.  Fires a flip task for a symbol at most once
        at a time (guarded by ``_commutator_flipping``).

        Config keys (under strategy.commutator):
          enabled               – bool, must be True
          reverse_at_loss_pct   – float | None, flip when uplRatio ≤ -abs(X)%
          reverse_at_loss_usd   – float | None, flip when upl ≤ -abs(X) USDT
          max_flips             – int, max reversals (0 = close without reversing)
          post_reversal_tp_pct  – float | None, after flip: TP at last_price ± X%
        """
        commutator = self._strategy_config.get("commutator") or {}
        if not commutator.get("enabled"):
            return
        if (self._strategy_config.get("alternator") or {}).get("enabled"):
            return

        loss_pct = self._extract_float(commutator.get("reverse_at_loss_pct"))
        loss_usd = self._extract_float(commutator.get("reverse_at_loss_usd"))
        max_flips_raw = commutator.get("max_flips")
        max_flips = int(max_flips_raw) if max_flips_raw is not None else 1
        post_tp_pct = self._extract_float(commutator.get("post_reversal_tp_pct"))

        if loss_pct is None and loss_usd is None:
            self._emit_debug(
                "Commutator: no threshold configured "
                "(both reverse_at_loss_pct and reverse_at_loss_usd are blank) — skipping"
            )
            return
        if max_flips < 0:
            self._emit_debug(f"Commutator: invalid max_flips ({max_flips}) — skipping")
            return

        snapshot = self._last_full_snapshot
        if not snapshot:
            self._emit_debug("Commutator: no snapshot available yet — skipping")
            return
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            self._emit_debug("Commutator: snapshot has no open positions")
            return

        # Prune flip counts for symbols that are no longer open.
        active_symbols = {
            str(p.get("instId", "")).upper()
            for p in positions
            if isinstance(p, dict) and self._extract_float(p.get("pos"))
        }
        for gone in set(self._commutator_flip_counts) - active_symbols:
            del self._commutator_flip_counts[gone]
            self._emit_debug(f"Commutator: {gone} no longer open — flip count cleared")

        self._emit_debug(
            f"Commutator: checking {len(positions)} position(s), "
            f"loss_pct={loss_pct}% loss_usd={loss_usd} "
            f"max_flips={max_flips} post_tp_pct={post_tp_pct}"
        )
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                continue
            if symbol in self._commutator_flipping:
                self._emit_debug(f"Commutator: {symbol} flip already in-flight — skipping")
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                continue

            upl_ratio = self._extract_float(pos.get("uplRatio"))
            upl_usd = self._extract_float(pos.get("upl"))
            upl_pct = upl_ratio * 100.0 if upl_ratio is not None else None
            flip_count = self._commutator_flip_counts.get(symbol, 0)
            self._emit_debug(
                f"Commutator: {symbol} uplRatio={upl_ratio!r} "
                f"({f'{upl_pct:.2f}%' if upl_pct is not None else 'n/a'}), "
                f"upl_usd={upl_usd!r}, flip_count={flip_count}/{max_flips}, "
                f"posSide={pos.get('posSide')!r}"
            )

            hit_pct = loss_pct is not None and upl_pct is not None and upl_pct <= -abs(loss_pct)
            hit_usd = loss_usd is not None and upl_usd is not None and upl_usd <= -abs(loss_usd)
            if not hit_pct and not hit_usd:
                continue

            will_flip = flip_count < max_flips
            trigger_reason = (
                f"loss_pct={upl_pct:.2f}%" if hit_pct else f"upl_usd={upl_usd:.4f}"
            )
            self._emit_debug(
                f"Commutator triggered: {symbol} {trigger_reason} "
                f"flip_count={flip_count}/{max_flips} "
                f"action={'FLIP' if will_flip else 'CLOSE'}"
            )

            pos_side = str(pos.get("posSide", "")).lower()
            trade_mode = str(pos.get("mgnMode") or "").lower() or None
            contracts = abs(pos_val)
            if pos_side == "long":
                close_side, close_pos_side = "sell", "long"
                new_entry_side, new_entry_pos_side = "sell", "short"
            elif pos_side == "short":
                close_side, close_pos_side = "buy", "short"
                new_entry_side, new_entry_pos_side = "buy", "long"
            else:
                # Net mode: infer from sign of pos
                close_side = "sell" if pos_val > 0 else "buy"
                close_pos_side = None
                new_entry_side = close_side
                new_entry_pos_side = None

            self._commutator_flipping.add(symbol)
            asyncio.create_task(
                self._commutator_flip(
                    symbol=symbol,
                    close_side=close_side,
                    close_pos_side=close_pos_side,
                    new_entry_side=new_entry_side if will_flip else None,
                    new_entry_pos_side=new_entry_pos_side if will_flip else None,
                    contracts=contracts,
                    trade_mode=trade_mode,
                    post_tp_pct=post_tp_pct,
                    flip_count=flip_count,
                ),
                name=f"commutator-flip-{symbol}",
            )

    async def _commutator_flip(
        self,
        *,
        symbol: str,
        close_side: str,
        close_pos_side: str | None,
        new_entry_side: str | None,
        new_entry_pos_side: str | None,
        contracts: float,
        trade_mode: str | None,
        post_tp_pct: float | None,
        flip_count: int,
    ) -> None:
        """Close a losing position and optionally open the reversed entry.

        When ``new_entry_side`` is None (max_flips exhausted) only the close
        order is submitted and no reversal is attempted.
        """
        symbol_key = symbol.upper()
        resolved_trade_mode = trade_mode or "cross"
        try:
            # Step 1: close existing position
            close_coid = self._generate_client_order_id("cmtr-c")
            close_result = await self._submit_order(
                symbol=symbol,
                side=close_side,
                pos_side=close_pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=True,
                client_order_id=close_coid,
                attach_algo_orders=None,
            )
            if close_result is None:
                self._emit_debug(
                    f"Commutator: {symbol_key} close order failed (no trade API)"
                )
                return
            close_ok = close_result[0] if isinstance(close_result, tuple) else close_result
            if not close_ok:
                self._emit_debug(
                    f"Commutator: {symbol_key} close order rejected — aborting"
                )
                return
            self._emit_debug(
                f"Commutator: {symbol_key} close order submitted "
                f"(side={close_side}, contracts={contracts})"
            )

            if new_entry_side is None:
                # max_flips exhausted: close only, no reversal
                self._emit_debug(
                    f"Commutator: {symbol_key} max_flips reached — "
                    "position closed without reversal"
                )
                return

            # Step 2: brief pause to let OKX register the close
            await asyncio.sleep(1.0)

            # Step 3: compute post-reversal TP if configured
            attach: list[dict[str, Any]] | None = None
            if post_tp_pct is not None and post_tp_pct > 0:
                ticker = self._latest_ticker.get(symbol_key) or {}
                last_price = self._extract_float(
                    ticker.get("last") or ticker.get("lastPr")
                )
                if last_price and last_price > 0:
                    is_new_long = new_entry_pos_side == "long" or (
                        new_entry_pos_side is None and new_entry_side == "buy"
                    )
                    tp_price = (
                        last_price * (1.0 + post_tp_pct / 100.0)
                        if is_new_long
                        else last_price * (1.0 - post_tp_pct / 100.0)
                    )
                    attach = self._build_attach_algo_orders(
                        take_profit_price=tp_price,
                        stop_loss_price=None,
                    )
                    self._emit_debug(
                        f"Commutator: {symbol_key} post-reversal TP at {tp_price:.6f} "
                        f"({'+' if is_new_long else '-'}{post_tp_pct:.2f}% "
                        f"from last {last_price:.6f})"
                    )
                else:
                    self._emit_debug(
                        f"Commutator: {symbol_key} cannot compute TP — "
                        "no last price in ticker cache"
                    )

            # Step 4: open reversed position
            entry_coid = self._generate_client_order_id("cmtr-e")
            entry_result = await self._submit_order(
                symbol=symbol,
                side=new_entry_side,
                pos_side=new_entry_pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=False,
                client_order_id=entry_coid,
                attach_algo_orders=attach,
            )
            new_flip_count = flip_count + 1
            self._commutator_flip_counts[symbol_key] = new_flip_count
            entry_ok = entry_result[0] if isinstance(entry_result, tuple) else entry_result
            if entry_ok:
                new_side_label = (
                    "long"
                    if new_entry_pos_side == "long" or new_entry_side == "buy"
                    else "short"
                )
                self._emit_debug(
                    f"Commutator: {symbol_key} reversed to {new_side_label} "
                    f"(flip #{new_flip_count}, contracts={contracts}"
                    f"{', TP attached' if attach else ''})"
                )
            else:
                self._emit_debug(
                    f"Commutator: {symbol_key} reversed-entry order rejected "
                    f"(flip #{new_flip_count})"
                )
        except Exception as exc:
            logger.warning("Commutator flip error for %s: %s", symbol_key, exc)
        finally:
            self._commutator_flipping.discard(symbol_key)

    # ── Alternator strategy ──────────────────────────────────────────────────

    def _compute_avg_amplitude_pct(self, symbol: str, lookback: int = 20) -> float | None:
        """Average candle amplitude as a % of mid-price over the last ``lookback`` HTF candles.

        Formula per candle: (H − L) / ((H + L) / 2) × 100.
        The average over the lookback window equals the typical % swing per bar
        and is used as an adaptive reversal threshold for the Alternator when
        ``dynamic_threshold`` is enabled.

        Falls back to LTF candles when HTF data is unavailable.
        Returns ``None`` when there are fewer than 3 usable candles.
        """
        candles: list[list[Any]] = self._latest_ohlcv_htf.get(symbol) or []
        if len(candles) < 3:
            candles = self._latest_ohlcv.get(symbol) or []
        if len(candles) < 3:
            return None
        recent = candles[-lookback:]
        amplitudes: list[float] = []
        for c in recent:
            try:
                high = float(c[2])
                low = float(c[3])
                mid = (high + low) * 0.5
                if mid > 0 and high > low:
                    amplitudes.append((high - low) / mid * 100.0)
            except (IndexError, TypeError, ValueError):
                continue
        return sum(amplitudes) / len(amplitudes) if amplitudes else None

    def _compute_range_position(self, symbol: str, lookback: int = 20) -> float | None:
        """Return where the current last_price sits within the N-bar LTF closed-candle range.

        Uses the last ``lookback`` *closed* LTF bars (excludes the live bar at
        index -1).  Returns a value in [0.0, 1.0] where 0.0 = range low and
        1.0 = range high.  Returns ``None`` when data is insufficient or the
        range is flat.

        Used by the Alternator candle-position filter to avoid entering a LONG
        near the top of the recent range or a SHORT near the bottom.
        """
        candles: list[list[Any]] = self._latest_ohlcv.get(symbol) or []
        # Exclude the live (incomplete) bar — skip index -1
        closed = candles[:-1]
        if len(closed) < 2:
            return None
        window = closed[-lookback:]
        highs: list[float] = []
        lows: list[float] = []
        for c in window:
            try:
                highs.append(float(c[2]))
                lows.append(float(c[3]))
            except (IndexError, TypeError, ValueError):
                continue
        if not highs or not lows:
            return None
        range_high = max(highs)
        range_low = min(lows)
        if range_high <= range_low:
            return None
        # Use the most recent close as the reference price
        try:
            last_price = float(candles[-1][4])  # close of live bar ≈ current price
        except (IndexError, TypeError, ValueError):
            return None
        return (last_price - range_low) / (range_high - range_low)

    # ── Launcher ──────────────────────────────────────────────────────────────

    def _launcher_evaluate_signal(self, symbol: str) -> str | None:
        """Return "buy", "sell", or None based on the current snapshot indicators.

        Used both by ``_check_launcher`` (standalone entries) and by
        ``handle_llm_decision`` (LLM trade filter).

        A signal fires only when ALL enabled filters agree:
          - RSI below oversold threshold (buy) or above overbought threshold (sell)
          - CMF positive (buy) / negative (sell) when require_cmf is True
          - HTF EMA50 > EMA200 (buy) / < EMA200 (sell) when require_htf_trend is True
          - ADX ≥ min_adx when min_adx > 0
          - 15-min footprint net_delta > 0 (buy) / < 0 (sell) when require_footprint_delta is True
        Returns None when indicators are neutral or any filter disagrees.
        """
        gov = self._launcher_config
        rsi_oversold = self._extract_float(gov.get("rsi_oversold")) or 35.0
        rsi_overbought = self._extract_float(gov.get("rsi_overbought")) or 65.0
        require_htf_trend = bool(gov.get("require_htf_trend", True))
        require_cmf = bool(gov.get("require_cmf", True))
        require_footprint_delta = bool(gov.get("require_footprint_delta", False))
        min_adx = self._extract_float(gov.get("min_adx")) or 0.0

        snapshot = self._last_full_snapshot
        if not snapshot:
            return None
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        rsi = self._extract_float(indicators.get("rsi"))
        cmf = self._extract_float((indicators.get("cmf") or {}).get("value"))
        adx = self._extract_float((indicators.get("adx") or {}).get("value"))

        htf_indicators = sym_data.get("indicators_htf") or indicators
        htf_ma = htf_indicators.get("moving_averages") or {}
        htf_ema50 = self._extract_float(htf_ma.get("ema_50"))
        htf_ema200 = self._extract_float(htf_ma.get("ema_200"))
        htf_bullish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 > htf_ema200
        htf_bearish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 < htf_ema200

        # Footprint net delta from the live market metrics (populated by _compute_custom_metrics)
        fp_net_delta: float | None = None
        if require_footprint_delta:
            fp_data = (sym_data.get("custom_metrics") or {}).get("footprint") or self._compute_footprint(symbol)
            if fp_data:
                fp_net_delta = self._extract_float(fp_data.get("net_delta"))

        if rsi is None:
            self._emit_debug(f"Launcher: {symbol} — no entry signal (RSI unavailable)")
            return None
        if min_adx > 0 and (adx is None or adx < min_adx):
            self._emit_debug(
                f"Launcher: {symbol} — no entry signal "
                f"(ADX={adx:.1f} < min={min_adx:.1f})"
            )
            return None

        buy_signal = (
            rsi < rsi_oversold
            and (not require_cmf or (cmf is not None and cmf > 0))
            and (not require_htf_trend or htf_bullish)
            and (not require_footprint_delta or (fp_net_delta is not None and fp_net_delta > 0))
        )
        sell_signal = (
            rsi > rsi_overbought
            and (not require_cmf or (cmf is not None and cmf < 0))
            and (not require_htf_trend or htf_bearish)
            and (not require_footprint_delta or (fp_net_delta is not None and fp_net_delta < 0))
        )
        if buy_signal:
            return "buy"
        if sell_signal:
            return "sell"

        # Build a human-readable breakdown of which filters blocked the signal.
        rsi_str = f"RSI={rsi:.1f} (need <{rsi_oversold} or >{rsi_overbought})"
        parts = [rsi_str]
        if require_cmf:
            parts.append(f"CMF={cmf:.3f}" if cmf is not None else "CMF=n/a")
        if require_htf_trend:
            if htf_ema50 is not None and htf_ema200 is not None:
                parts.append(f"HTF EMA50={htf_ema50:.4g}/EMA200={htf_ema200:.4g} ({'bull' if htf_bullish else 'bear' if htf_bearish else 'flat'})")
            else:
                parts.append("HTF EMA=n/a")
        if require_footprint_delta:
            parts.append(f"fp_delta={fp_net_delta:.2f}" if fp_net_delta is not None else "fp_delta=n/a")
        self._emit_debug(f"Launcher: {symbol} — no entry signal ({', '.join(parts)})")
        return None

    async def _check_launcher(self) -> None:
        """Update Launcher position-tracking state for the scheduler's on_close trigger.

        Entry decisions for launcher_only mode are now driven by
        PromptScheduler._tick() via build_launcher_decision().  This method
        only prunes _launcher_in_position and updates _launcher_had_positions
        so the on_close trigger can detect the transition to no open positions.
        """
        gov = self._launcher_config
        mode = str(gov.get("mode") or "disabled").lower()
        if mode != "launcher_only":
            return

        snapshot = self._last_full_snapshot
        if not snapshot:
            return

        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        active_symbols = {
            str(p.get("instId", "")).upper()
            for p in positions
            if isinstance(p, dict) and self._extract_float(p.get("pos"))
        }

        # Prune stale tracking (position settled externally)
        for sym in list(self._launcher_in_position):
            if sym not in active_symbols:
                self._emit_debug(f"Launcher: {sym} no longer in positions — clearing tracking")
                self._launcher_in_position.pop(sym, None)

        self._launcher_had_positions = bool(active_symbols)

    async def _launcher_open_position(
        self,
        symbol: str,
        side: str,
        pos_side: str | None,
        contracts: float,
        trade_mode: str,
        attach_algo_orders: list[dict[str, Any]] | None,
    ) -> None:
        """Submit a Launcher entry order and record position tracking on success."""
        coid = self._generate_client_order_id("gov")
        try:
            result = await self._submit_order(
                symbol=symbol,
                side=side,
                pos_side=pos_side,
                size=contracts,
                trade_mode=trade_mode,
                order_type="market",
                reduce_only=False,
                client_order_id=coid,
                attach_algo_orders=attach_algo_orders,
            )
            if result is None:
                self._emit_debug(f"Launcher: {symbol} — trade API unavailable")
                return
            order_result = result[0] if isinstance(result, tuple) else result
            if order_result:
                self._emit_debug(f"Launcher: {symbol} entry accepted ({side})")
                self._launcher_in_position[symbol] = {
                    "side": "long" if side == "buy" else "short",
                    "pos_side": pos_side,
                }
            else:
                self._emit_debug(f"Launcher: {symbol} entry rejected")
        except Exception as exc:
            logger.warning("Launcher entry error for %s: %s", symbol, exc)
        finally:
            self._launcher_entering.discard(symbol)

    async def _launcher_close_position(
        self,
        symbol: str,
        close_side: str,
        pos_side: str | None,
        contracts: float,
        trade_mode: str,
    ) -> None:
        """Submit a Launcher TP/SL close order."""
        coid = self._generate_client_order_id("gov-c")
        try:
            result = await self._submit_order(
                symbol=symbol,
                side=close_side,
                pos_side=pos_side,
                size=contracts,
                trade_mode=trade_mode or "isolated",
                order_type="market",
                reduce_only=True,
                client_order_id=coid,
                attach_algo_orders=None,
            )
            if result is None:
                self._emit_debug(f"Launcher: {symbol} close — trade API unavailable")
            else:
                order_result = result[0] if isinstance(result, tuple) else result
                if order_result:
                    self._emit_debug(f"Launcher: {symbol} close accepted")
                else:
                    self._emit_debug(f"Launcher: {symbol} close rejected")
        except Exception as exc:
            logger.warning("Launcher close error for %s: %s", symbol, exc)

    def build_launcher_decision(self, symbol: str) -> dict[str, Any] | None:
        """Build a synthetic decision dict using the Launcher's signal evaluation.

        Called by PromptScheduler._tick() in launcher_only mode.  Returns a
        decision compatible with handle_llm_decision(), or None if no signal
        fires or the symbol is not eligible for entry.
        """
        gov = self._launcher_config
        snapshot = self._last_full_snapshot
        if not snapshot:
            return None

        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        active_symbols = {
            str(p.get("instId", "")).upper()
            for p in positions
            if isinstance(p, dict) and self._extract_float(p.get("pos"))
        }
        symbol_upper = symbol.upper()

        if symbol_upper in self._launcher_entering:
            self._emit_debug(f"Launcher: {symbol} entry in-flight — skipping")
            return None
        if symbol_upper in self._launcher_in_position:
            self._emit_debug(f"Launcher: {symbol} already tracked — skipping")
            return None
        if symbol_upper in active_symbols:
            self._emit_debug(f"Launcher: {symbol} has open position — skipping")
            return None

        signal = self._launcher_evaluate_signal(symbol)
        if signal is None:
            return None

        notional_usd = self._extract_float(gov.get("notional_usd"))
        if not notional_usd or notional_usd <= 0:
            self._emit_debug(f"Launcher: notional_usd not configured — skipping {symbol}")
            return None

        last_price = self.get_last_price(symbol)
        if not last_price or last_price <= 0:
            self._emit_debug(f"Launcher: {symbol} no last price — skipping")
            return None

        tp_pct = self._extract_float(gov.get("tp_pct"))
        sl_pct = self._extract_float(gov.get("sl_pct"))
        tp_price: float | None = None
        sl_price: float | None = None
        if tp_pct and tp_pct > 0:
            tp_price = (
                last_price * (1 + tp_pct / 100.0)
                if signal == "buy"
                else last_price * (1 - tp_pct / 100.0)
            )
        if sl_pct and sl_pct > 0:
            sl_price = (
                last_price * (1 - sl_pct / 100.0)
                if signal == "buy"
                else last_price * (1 + sl_pct / 100.0)
            )

        self._emit_debug(
            f"Launcher signal: {symbol} {signal.upper()} last={last_price} "
            f"notional={notional_usd} tp={tp_price} sl={sl_price}"
        )
        return {
            "action": "BUY" if signal == "buy" else "SELL",
            "symbol": symbol,
            "notional_usd": notional_usd,
            "confidence": 1.0,
            "risk_score": 0.5,
            "take_profit": tp_price,
            "stop_loss": sl_price,
            "rationale": f"Launcher signal: {signal.upper()}",
            "_decision_origin": "launcher",
        }

    async def _check_alternator(self) -> None:
        """Oscillate between long/short positions on profit and loss thresholds.

        Runs every ``_positions_refresh_interval`` seconds inside
        ``_positions_refresh_loop``.  Mutually exclusive with Skimming and
        Commutator (enforced at the config layer; also checked defensively here).

        Check order per position (highest priority first):
          1. Ride condition: if profit ≥ ride threshold → hand off to Protector,
             stop reversing for this position lifecycle.
          2. Hard stop: if loss ≥ stop threshold → close without reversal.
          3. Reverse at profit: if profit ≥ reverse threshold → flip side,
             track count against max_reversals.
          4. Restart at loss: if loss ≥ restart threshold → flip back,
             track count against max_reversals.

        Config keys (under strategy.alternator):
          enabled                – bool, must be True
          reverse_at_profit_pct  – float | None, flip when uplRatio ≥ X%
          reverse_at_profit_usd  – float | None, flip when upl ≥ X USDT
          max_reversals          – int | None, max total flips (None = unlimited)
          restart_at_loss_pct    – float | None, flip back when uplRatio ≤ -abs(X)%
          restart_at_loss_usd    – float | None, flip back when upl ≤ -abs(X) USDT
          ride_at_profit_pct     – float | None, hand to Protector; stop reversing
          ride_at_profit_usd     – float | None, same, USD basis
          stop_at_loss_pct       – float | None, hard close, no flip
          stop_at_loss_usd       – float | None, hard close, USD basis
        """
        alternator = self._strategy_config.get("alternator") or {}
        if not alternator.get("enabled"):
            return
        # Defensive mutual-exclusion guard (UI enforces this, but check here too)
        strategy = self._strategy_config
        if (strategy.get("skimming") or {}).get("enabled") or (
            strategy.get("commutator") or {}
        ).get("enabled"):
            self._emit_debug(
                "Alternator: skipped — Skimming or Commutator is also enabled (config conflict)"
            )
            return


        trailing_reverse = bool(alternator.get("trailing_reverse", False))
        trailing_pullback_pct = abs(self._extract_float(alternator.get("trailing_pullback_pct")) or 0.0)
        dynamic_threshold = bool(alternator.get("dynamic_threshold", False))
        dynamic_factor = abs(self._extract_float(alternator.get("dynamic_threshold_factor")) or 1.0)
        dynamic_lookback = int(alternator.get("dynamic_threshold_lookback") or 20)
        dynamic_loss_threshold = bool(alternator.get("dynamic_loss_threshold", False))
        dynamic_loss_factor = abs(self._extract_float(alternator.get("dynamic_loss_factor")) or 1.0)
        dynamic_loss_lookback = int(alternator.get("dynamic_loss_lookback") or 20)
        candle_position_filter = bool(alternator.get("candle_position_filter", False))
        candle_position_long_max = float(alternator.get("candle_position_long_max") or 0.75)
        candle_position_short_min = float(alternator.get("candle_position_short_min") or 0.25)
        candle_position_lookback = int(alternator.get("candle_position_lookback") or 20)
        footprint_delta_filter = bool(alternator.get("footprint_delta_filter", False))
        footprint_delta_min_ratio = float(alternator.get("footprint_delta_min_ratio") or 0.0)
        ob_wall_suppress = bool(alternator.get("ob_wall_suppress", False))
        ob_wall_proximity_pct = self._extract_float(alternator.get("ob_wall_proximity_pct")) or 1.0
        ob_wall_ratio = self._extract_float(alternator.get("ob_wall_ratio")) or 3.0
        continuous_llm = bool(alternator.get("continuous_llm", False))
        trailing_close = bool(alternator.get("trailing_close", False))
        trailing_close_activate_pct = abs(
            self._extract_float(alternator.get("trailing_close_activate_pct")) or 0.0
        )
        trailing_close_activate_usd = abs(
            self._extract_float(alternator.get("trailing_close_activate_usd")) or 0.0
        )
        trailing_close_pullback_pct = abs(
            self._extract_float(alternator.get("trailing_close_pullback_pct")) or 0.0
        )
        _rev_profit_pct_static = self._extract_float(alternator.get("reverse_at_profit_pct"))
        rev_profit_pct = _rev_profit_pct_static  # may be overridden per-symbol in the loop below
        rev_profit_usd = self._extract_float(alternator.get("reverse_at_profit_usd"))
        max_reversals_raw = alternator.get("max_reversals")
        max_reversals: int | None = (
            int(max_reversals_raw) if max_reversals_raw is not None else None
        )
        close_on_max_reversals = bool(alternator.get("close_on_max_reversals", False))
        restart_loss_pct = self._extract_float(alternator.get("restart_at_loss_pct"))
        restart_loss_usd = self._extract_float(alternator.get("restart_at_loss_usd"))
        ride_profit_pct = self._extract_float(alternator.get("ride_at_profit_pct"))
        ride_profit_usd = self._extract_float(alternator.get("ride_at_profit_usd"))
        stop_loss_pct = self._extract_float(alternator.get("stop_at_loss_pct"))
        stop_loss_usd = self._extract_float(alternator.get("stop_at_loss_usd"))

        if (
            _rev_profit_pct_static is None
            and rev_profit_usd is None
            and restart_loss_pct is None
            and restart_loss_usd is None
            and not dynamic_threshold
            and not dynamic_loss_threshold
            and not trailing_close
            and not close_on_max_reversals
        ):
            self._emit_debug("Alternator: no trigger thresholds configured — skipping")
            return

        snapshot = self._last_full_snapshot
        if not snapshot:
            self._emit_debug("Alternator: no snapshot available yet — skipping")
            return
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            self._emit_debug("Alternator: snapshot has no open positions")
            return

        # Prune state for symbols no longer open.
        active_symbols = {
            str(p.get("instId", "")).upper()
            for p in positions
            if isinstance(p, dict) and self._extract_float(p.get("pos"))
        }
        for gone in set(self._alternator_flip_counts) - active_symbols:
            del self._alternator_flip_counts[gone]
            self._emit_debug(f"Alternator: {gone} no longer open — flip count cleared")
        self._alternator_riding &= active_symbols
        stale_trailing = set(self._alternator_peak_pnl_pct) | set(self._alternator_peak_pnl_usd) | self._alternator_above_threshold
        for gone in stale_trailing - active_symbols:
            self._alternator_above_threshold.discard(gone)
            self._alternator_peak_pnl_pct.pop(gone, None)
            self._alternator_peak_pnl_usd.pop(gone, None)
        stale_trailing_close = (
            set(self._alternator_close_peak_pnl_pct)
            | set(self._alternator_close_peak_pnl_usd)
            | self._alternator_close_above_threshold
        )
        for gone in stale_trailing_close - active_symbols:
            self._alternator_close_above_threshold.discard(gone)
            self._alternator_close_peak_pnl_pct.pop(gone, None)
            self._alternator_close_peak_pnl_usd.pop(gone, None)
        # Prune stale Shotgun-closing entries for positions that have settled.
        self._shotgun_closing &= active_symbols

        if not active_symbols:
            return

        self._emit_debug(
            f"Alternator: checking {len(active_symbols)} position(s), "
            f"rev_profit_pct={_rev_profit_pct_static!r} dyn={dynamic_threshold} rev_profit_usd={rev_profit_usd} "
            f"max_reversals={max_reversals} restart_loss_pct={restart_loss_pct} "
            f"restart_loss_usd={restart_loss_usd} ride_profit_pct={ride_profit_pct} "
            f"stop_loss_pct={stop_loss_pct}"
        )

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                continue
            if symbol in self._alternator_flipping:
                self._emit_debug(f"Alternator: {symbol} flip already in-flight — skipping")
                continue
            if symbol in self._alternator_riding:
                self._emit_debug(f"Alternator: {symbol} handed to Protector — skipping")
                continue
            if symbol in self._shotgun_closing:
                self._emit_debug(f"Alternator: {symbol} Shotgun close in-flight — skipping")
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                continue

            # ── Per-symbol effective profit threshold ────────────────────────
            rev_profit_pct = _rev_profit_pct_static
            if dynamic_threshold:
                _dyn_amp = self._compute_avg_amplitude_pct(symbol, lookback=dynamic_lookback)
                if _dyn_amp is not None:
                    rev_profit_pct = _dyn_amp * dynamic_factor
                    self._emit_debug(
                        f"Alternator: {symbol} dynamic rev_profit_pct = {rev_profit_pct:.3f}% "
                        f"(avg_amplitude={_dyn_amp:.3f}% \u00d7 factor={dynamic_factor})"
                    )
                else:
                    self._emit_debug(
                        f"Alternator: {symbol} dynamic threshold: insufficient candle data, "
                        f"using static ({_rev_profit_pct_static!r}%)"
                    )

            # ── Per-symbol effective loss threshold ──────────────────────────
            _restart_loss_pct_effective = restart_loss_pct
            if dynamic_loss_threshold:
                _dyn_loss_amp = self._compute_avg_amplitude_pct(symbol, lookback=dynamic_loss_lookback)
                if _dyn_loss_amp is not None:
                    _restart_loss_pct_effective = _dyn_loss_amp * dynamic_loss_factor
                    self._emit_debug(
                        f"Alternator: {symbol} dynamic restart_loss_pct = {_restart_loss_pct_effective:.3f}% "
                        f"(avg_amplitude={_dyn_loss_amp:.3f}% \u00d7 factor={dynamic_loss_factor})"
                    )
                else:
                    self._emit_debug(
                        f"Alternator: {symbol} dynamic loss threshold: insufficient candle data, "
                        f"using static ({restart_loss_pct!r}%)"
                    )

            upl_ratio = self._extract_float(pos.get("uplRatio"))
            upl_usd = self._extract_float(pos.get("upl"))
            upl_pct = upl_ratio * 100.0 if upl_ratio is not None else None
            flip_count = self._alternator_flip_counts.get(symbol, 0)

            self._emit_debug(
                f"Alternator: {symbol} uplRatio={upl_ratio!r} "
                f"({f'{upl_pct:.2f}%' if upl_pct is not None else 'n/a'}), "
                f"upl_usd={upl_usd!r}, flip_count={flip_count}, "
                f"posSide={pos.get('posSide')!r}"
            )

            pos_side = str(pos.get("posSide", "")).lower()
            trade_mode = str(pos.get("mgnMode") or "").lower() or None
            contracts = abs(pos_val)
            if pos_side == "long":
                close_side, close_pos_side = "sell", "long"
                new_entry_side, new_entry_pos_side = "sell", "short"
            elif pos_side == "short":
                close_side, close_pos_side = "buy", "short"
                new_entry_side, new_entry_pos_side = "buy", "long"
            else:
                # Net mode: infer from sign of pos
                close_side = "sell" if pos_val > 0 else "buy"
                close_pos_side = None
                new_entry_side = "buy" if pos_val > 0 else "sell"
                new_entry_pos_side = None

            # ── Continuous LLM supervision (optional) ─────────────────────────
            if continuous_llm and self._llm_service is not None:
                mandate = self._llm_mandate.get(symbol) or {}
                mandate_valid = mandate.get("_expires_at", 0.0) > time.monotonic()
                # Fire fresh supervision in background if mandate is absent or expired.
                if not mandate_valid and symbol not in self._llm_supervision_running:
                    asyncio.create_task(
                        self._run_llm_supervision(
                            symbol,
                            {
                                "pos_side": pos_side,
                                "upl_pct": upl_pct,
                                "upl_usd": upl_usd,
                                "flip_count": flip_count,
                            },
                        ),
                        name=f"altr-supervision-{symbol}",
                    )
                # Apply mandate if still valid (mandate from a previous call).
                if mandate_valid:
                    m_action = str(mandate.get("action") or "continue").lower()
                    if m_action == "close":
                        self._emit_debug(
                            f"Alternator: {symbol} LLM mandate=close — closing without reversal"
                        )
                        self._alternator_flipping.add(symbol)
                        asyncio.create_task(
                            self._alternator_flip(
                                symbol=symbol,
                                close_side=close_side,
                                close_pos_side=close_pos_side,
                                new_entry_side=None,
                                new_entry_pos_side=None,
                                contracts=contracts,
                                trade_mode=trade_mode,
                                flip_count=flip_count,
                                trigger="llm_close",
                            ),
                            name=f"alternator-flip-{symbol}",
                        )
                        continue
                    if m_action == "pause":
                        self._emit_debug(
                            f"Alternator: {symbol} LLM mandate=pause — skipping reversals this cycle"
                        )
                        continue
                    # Override max_reversals if the LLM mandated a tighter cap.
                    m_max_rev = mandate.get("max_reversals_override")
                    if m_max_rev is not None:
                        max_reversals = int(m_max_rev)
                    # Soft stop: close if loss exceeds the LLM-set threshold.
                    m_soft_stop = mandate.get("soft_stop_pct")
                    if (
                        m_soft_stop is not None
                        and upl_pct is not None
                        and upl_pct <= -abs(float(m_soft_stop))
                    ):
                        self._emit_debug(
                            f"Alternator: {symbol} LLM soft_stop={m_soft_stop:.2f}% triggered "
                            f"(upl_pct={upl_pct:.2f}%) — closing"
                        )
                        self._alternator_flipping.add(symbol)
                        asyncio.create_task(
                            self._alternator_flip(
                                symbol=symbol,
                                close_side=close_side,
                                close_pos_side=close_pos_side,
                                new_entry_side=None,
                                new_entry_pos_side=None,
                                contracts=contracts,
                                trade_mode=trade_mode,
                                flip_count=flip_count,
                                trigger="llm_soft_stop",
                            ),
                            name=f"alternator-flip-{symbol}",
                        )
                        continue

            # ── Priority 0.5: Close flat when max reversals exhausted ─────────
            # When close_on_max_reversals is enabled and flip_count has reached
            # the configured cap, close immediately without waiting for a
            # profit or loss threshold to fire again.
            if (
                close_on_max_reversals
                and max_reversals is not None
                and flip_count >= max_reversals
            ):
                self._emit_debug(
                    f"Alternator: {symbol} max reversals exhausted "
                    f"(flip_count={flip_count} >= max_reversals={max_reversals}) — "
                    "closing flat"
                )
                self._alternator_flipping.add(symbol)
                asyncio.create_task(
                    self._alternator_flip(
                        symbol=symbol,
                        close_side=close_side,
                        close_pos_side=close_pos_side,
                        new_entry_side=None,
                        new_entry_pos_side=None,
                        contracts=contracts,
                        trade_mode=trade_mode,
                        flip_count=flip_count,
                        trigger="max_reversals_exhausted",
                    ),
                    name=f"alternator-flip-{symbol}",
                )
                continue

            # ── Priority 1: Ride condition ────────────────────────────────────
            hit_ride_pct = (
                ride_profit_pct is not None
                and upl_pct is not None
                and upl_pct >= abs(ride_profit_pct)
            )
            hit_ride_usd = (
                ride_profit_usd is not None
                and upl_usd is not None
                and upl_usd >= abs(ride_profit_usd)
            )
            if hit_ride_pct or hit_ride_usd:
                ride_trigger = (
                    f"upl_pct={upl_pct:.2f}% >= {abs(ride_profit_pct):.2f}%"
                    if hit_ride_pct
                    else f"upl_usd={upl_usd:.4f} >= {abs(ride_profit_usd):.4f}"
                )
                self._emit_debug(
                    f"Alternator: {symbol} RIDE triggered ({ride_trigger}) — "
                    "handing off to Protector, no further reversals"
                )
                self._alternator_riding.add(symbol)
                continue

            # ── Priority 2: Hard stop ─────────────────────────────────────────
            hit_stop_pct = (
                stop_loss_pct is not None
                and upl_pct is not None
                and upl_pct <= -abs(stop_loss_pct)
            )
            hit_stop_usd = (
                stop_loss_usd is not None
                and upl_usd is not None
                and upl_usd <= -abs(stop_loss_usd)
            )
            if hit_stop_pct or hit_stop_usd:
                stop_trigger = (
                    f"upl_pct={upl_pct:.2f}% <= -{abs(stop_loss_pct):.2f}%"
                    if hit_stop_pct
                    else f"upl_usd={upl_usd:.4f} <= -{abs(stop_loss_usd):.4f}"
                )
                self._emit_debug(
                    f"Alternator: {symbol} HARD STOP triggered ({stop_trigger}) — "
                    "closing without reversal"
                )
                self._alternator_flipping.add(symbol)
                asyncio.create_task(
                    self._alternator_flip(
                        symbol=symbol,
                        close_side=close_side,
                        close_pos_side=close_pos_side,
                        new_entry_side=None,
                        new_entry_pos_side=None,
                        contracts=contracts,
                        trade_mode=trade_mode,
                        flip_count=flip_count,
                        trigger="stop",
                    ),
                    name=f"alternator-flip-{symbol}",
                )
                continue

            # ── Priority 2.5: Trailing close (flat exit in profit) ────────────
            # When enabled, positive PnL is allowed to run and closes on a
            # pullback from the peak — no reversal, just a flat close.
            # Takes priority over Priority 3 (reverse at profit) so the two
            # mechanisms don't compete on the profit side.
            if trailing_close and trailing_close_pullback_pct > 0:
                activate_close_pct = trailing_close_activate_pct if trailing_close_activate_pct > 0 else None
                activate_close_usd = trailing_close_activate_usd if trailing_close_activate_usd > 0 else None
                hit_close_activate_pct = (
                    activate_close_pct is not None
                    and upl_pct is not None
                    and upl_pct >= activate_close_pct
                )
                hit_close_activate_usd = (
                    activate_close_usd is not None
                    and upl_usd is not None
                    and upl_usd >= activate_close_usd
                )
                in_trailing_close = symbol in self._alternator_close_above_threshold
                if hit_close_activate_pct or hit_close_activate_usd or in_trailing_close:
                    # Mark that we are in trailing-close territory.
                    if hit_close_activate_pct or hit_close_activate_usd:
                        self._alternator_close_above_threshold.add(symbol)
                    # Update peak PnL (only upward).
                    if upl_pct is not None:
                        self._alternator_close_peak_pnl_pct[symbol] = max(
                            self._alternator_close_peak_pnl_pct.get(symbol, upl_pct), upl_pct
                        )
                    if upl_usd is not None:
                        self._alternator_close_peak_pnl_usd[symbol] = max(
                            self._alternator_close_peak_pnl_usd.get(symbol, upl_usd), upl_usd
                        )
                    # Check pullback from peak.
                    tc_pullback_factor = 1.0 - trailing_close_pullback_pct / 100.0
                    tc_peak_pct = self._alternator_close_peak_pnl_pct.get(symbol)
                    tc_peak_usd = self._alternator_close_peak_pnl_usd.get(symbol)
                    tc_pullback_by_pct = (
                        tc_peak_pct is not None
                        and upl_pct is not None
                        and activate_close_pct is not None
                        and upl_pct < tc_peak_pct * tc_pullback_factor
                    )
                    tc_pullback_by_usd = (
                        tc_peak_usd is not None
                        and upl_usd is not None
                        and activate_close_usd is not None
                        and upl_usd < tc_peak_usd * tc_pullback_factor
                    )
                    if tc_pullback_by_pct or tc_pullback_by_usd:
                        tc_trigger = (
                            f"upl_pct={upl_pct:.2f}% < peak {tc_peak_pct:.2f}% × {tc_pullback_factor:.3f}"
                            if tc_pullback_by_pct
                            else f"upl_usd={upl_usd:.4f} < peak {tc_peak_usd:.4f} × {tc_pullback_factor:.3f}"
                        )
                        self._emit_debug(
                            f"Alternator: {symbol} TRAILING CLOSE triggered ({tc_trigger}) — "
                            "closing flat (no reversal)"
                        )
                        # Clean up trailing-close state.
                        self._alternator_close_above_threshold.discard(symbol)
                        self._alternator_close_peak_pnl_pct.pop(symbol, None)
                        self._alternator_close_peak_pnl_usd.pop(symbol, None)
                        self._alternator_flipping.add(symbol)
                        asyncio.create_task(
                            self._alternator_flip(
                                symbol=symbol,
                                close_side=close_side,
                                close_pos_side=close_pos_side,
                                new_entry_side=None,
                                new_entry_pos_side=None,
                                contracts=contracts,
                                trade_mode=trade_mode,
                                flip_count=flip_count,
                                trigger="trailing_close",
                            ),
                            name=f"alternator-flip-{symbol}",
                        )
                    else:
                        self._emit_debug(
                            f"Alternator: {symbol} trailing close — "
                            f"peak_pct={tc_peak_pct!r} current_pct={upl_pct!r} "
                            f"peak_usd={tc_peak_usd!r} current_usd={upl_usd!r} "
                            f"pullback_needed={trailing_close_pullback_pct}% — waiting"
                        )
                    # Whether we fired or are still waiting, skip Priority 3 (reverse at profit)
                    # to prevent the profit side from triggering a reversal instead of a close.
                    continue

            # ── Priority 3: Reverse at profit ─────────────────────────────────
            hit_rev_profit_pct = (
                rev_profit_pct is not None
                and upl_pct is not None
                and upl_pct >= abs(rev_profit_pct)
            )
            hit_rev_profit_usd = (
                rev_profit_usd is not None
                and upl_usd is not None
                and upl_usd >= abs(rev_profit_usd)
            )
            in_trailing = symbol in self._alternator_above_threshold

            if hit_rev_profit_pct or hit_rev_profit_usd or in_trailing:
                if not trailing_reverse:
                    # ── Immediate mode (original behaviour) ──────────────────
                    if hit_rev_profit_pct or hit_rev_profit_usd:
                        will_flip = max_reversals is None or flip_count < max_reversals
                        rev_trigger = (
                            f"upl_pct={upl_pct:.2f}% >= {abs(rev_profit_pct):.2f}%"
                            if hit_rev_profit_pct
                            else f"upl_usd={upl_usd:.4f} >= {abs(rev_profit_usd):.4f}"
                        )
                        self._emit_debug(
                            f"Alternator: {symbol} PROFIT triggered ({rev_trigger}), "
                            f"flip_count={flip_count} max_reversals={max_reversals} "
                            f"action={'FLIP' if will_flip else 'CLOSE'}"
                        )
                        if candle_position_filter and will_flip and new_entry_side is not None:
                            _rp = self._compute_range_position(symbol, candle_position_lookback)
                            _cpf_blocked = _rp is not None and (
                                (new_entry_side == "buy" and _rp > candle_position_long_max)
                                or (new_entry_side == "sell" and _rp < candle_position_short_min)
                            )
                            if _cpf_blocked:
                                _cpf_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                self._emit_debug(
                                    f"Alternator: {symbol} {_cpf_dir} reversal blocked by "
                                    f"candle-position filter — range_pos={_rp:.3f} "
                                    f"(long_max={candle_position_long_max}, "
                                    f"short_min={candle_position_short_min}) — waiting"
                                )
                                continue
                        if footprint_delta_filter and will_flip and new_entry_side is not None:
                            _fp = self._compute_footprint(symbol)
                            _fp_ask = float(_fp.get("total_ask_vol") or 0.0)
                            _fp_bid = float(_fp.get("total_bid_vol") or 0.0)
                            _fp_total = _fp_ask + _fp_bid
                            if _fp_total > 0:
                                _fp_delta = float(_fp.get("net_delta") or 0.0)
                                _fp_ratio = abs(_fp_delta) / _fp_total
                                _fp_dir_conflict = (
                                    (new_entry_side == "buy" and _fp_delta < 0)
                                    or (new_entry_side == "sell" and _fp_delta > 0)
                                )
                                if _fp_dir_conflict and _fp_ratio >= footprint_delta_min_ratio:
                                    _fp_entry_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                    self._emit_debug(
                                        f"Alternator: {symbol} {_fp_entry_dir} reversal blocked by "
                                        f"footprint delta filter — net_delta={_fp_delta:+.4f} "
                                        f"(ask={_fp_ask:.4f}, bid={_fp_bid:.4f}, "
                                        f"ratio={_fp_ratio:.3f}) — waiting"
                                    )
                                    continue
                            else:
                                self._emit_debug(
                                    f"Alternator: {symbol} footprint delta filter — "
                                    "no trade data in window, skipping"
                                )
                        if ob_wall_suppress and will_flip and new_entry_side is not None:
                            _ob_ticker = self._latest_ticker.get(symbol) or {}
                            _ob_price = self._extract_float(
                                _ob_ticker.get("last") or _ob_ticker.get("lastPr")
                            )
                            if _ob_price and _ob_price > 0:
                                _snap_altr = self._last_full_snapshot
                                _altr_ob_book = (
                                    ((_snap_altr.get("market_data") or {}).get(symbol) or {})
                                    .get("order_book", {})
                                    if _snap_altr else {}
                                )
                                _altr_ob_levels = (
                                    _altr_ob_book.get("asks") if new_entry_side == "buy"
                                    else _altr_ob_book.get("bids")
                                ) or []
                                _altr_ob_nearby = [
                                    s for p, s in _altr_ob_levels
                                    if (
                                        _ob_price <= p <= _ob_price * (1 + ob_wall_proximity_pct / 100.0)
                                        if new_entry_side == "buy"
                                        else _ob_price * (1 - ob_wall_proximity_pct / 100.0) <= p <= _ob_price
                                    )
                                ]
                                if _altr_ob_levels and _altr_ob_nearby:
                                    _altr_avg = sum(s for _, s in _altr_ob_levels) / len(_altr_ob_levels)
                                    _altr_wall = max(_altr_ob_nearby)
                                    if _altr_avg > 0 and _altr_wall >= ob_wall_ratio * _altr_avg:
                                        _wall_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                        self._emit_debug(
                                            f"Alternator: {symbol} {_wall_dir} profit reversal "
                                            f"blocked by OB wall — wall_size={_altr_wall:.2f} "
                                            f"({_altr_wall / _altr_avg:.1f}x avg={_altr_avg:.2f}) "
                                            f"within {ob_wall_proximity_pct}% of price — waiting"
                                        )
                                        continue
                        self._alternator_flipping.add(symbol)
                        asyncio.create_task(
                            self._alternator_flip(
                                symbol=symbol,
                                close_side=close_side,
                                close_pos_side=close_pos_side,
                                new_entry_side=new_entry_side if will_flip else None,
                                new_entry_pos_side=new_entry_pos_side if will_flip else None,
                                contracts=contracts,
                                trade_mode=trade_mode,
                                flip_count=flip_count,
                                trigger="profit",
                            ),
                            name=f"alternator-flip-{symbol}",
                        )
                        continue
                    if hit_rev_profit_pct or hit_rev_profit_usd:
                        self._alternator_above_threshold.add(symbol)
                    # Update peak PnL (only upward)
                    if upl_pct is not None:
                        self._alternator_peak_pnl_pct[symbol] = max(
                            self._alternator_peak_pnl_pct.get(symbol, upl_pct), upl_pct
                        )
                    if upl_usd is not None:
                        self._alternator_peak_pnl_usd[symbol] = max(
                            self._alternator_peak_pnl_usd.get(symbol, upl_usd), upl_usd
                        )
                    # Check whether PnL has pulled back enough from peak
                    pullback_factor = 1.0 - trailing_pullback_pct / 100.0
                    peak_pct = self._alternator_peak_pnl_pct.get(symbol)
                    peak_usd = self._alternator_peak_pnl_usd.get(symbol)
                    pullback_by_pct = (
                        peak_pct is not None
                        and upl_pct is not None
                        and rev_profit_pct is not None
                        and upl_pct < peak_pct * pullback_factor
                    )
                    pullback_by_usd = (
                        peak_usd is not None
                        and upl_usd is not None
                        and rev_profit_usd is not None
                        and upl_usd < peak_usd * pullback_factor
                    )
                    if pullback_by_pct or pullback_by_usd:
                        will_flip = max_reversals is None or flip_count < max_reversals
                        rev_trigger = (
                            f"upl_pct={upl_pct:.2f}% < peak {peak_pct:.2f}% × {pullback_factor:.3f}"
                            if pullback_by_pct
                            else f"upl_usd={upl_usd:.4f} < peak {peak_usd:.4f} × {pullback_factor:.3f}"
                        )
                        self._emit_debug(
                            f"Alternator: {symbol} TRAILING PROFIT triggered ({rev_trigger}), "
                            f"flip_count={flip_count} max_reversals={max_reversals} "
                            f"action={'FLIP' if will_flip else 'CLOSE'}"
                        )
                        if candle_position_filter and will_flip and new_entry_side is not None:
                            _rp = self._compute_range_position(symbol, candle_position_lookback)
                            _cpf_blocked = _rp is not None and (
                                (new_entry_side == "buy" and _rp > candle_position_long_max)
                                or (new_entry_side == "sell" and _rp < candle_position_short_min)
                            )
                            if _cpf_blocked:
                                _cpf_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                self._emit_debug(
                                    f"Alternator: {symbol} {_cpf_dir} trailing reversal blocked by "
                                    f"candle-position filter — range_pos={_rp:.3f} "
                                    f"(long_max={candle_position_long_max}, "
                                    f"short_min={candle_position_short_min}) — waiting"
                                )
                                continue
                        if footprint_delta_filter and will_flip and new_entry_side is not None:
                            _fp = self._compute_footprint(symbol)
                            _fp_ask = float(_fp.get("total_ask_vol") or 0.0)
                            _fp_bid = float(_fp.get("total_bid_vol") or 0.0)
                            _fp_total = _fp_ask + _fp_bid
                            if _fp_total > 0:
                                _fp_delta = float(_fp.get("net_delta") or 0.0)
                                _fp_ratio = abs(_fp_delta) / _fp_total
                                _fp_dir_conflict = (
                                    (new_entry_side == "buy" and _fp_delta < 0)
                                    or (new_entry_side == "sell" and _fp_delta > 0)
                                )
                                if _fp_dir_conflict and _fp_ratio >= footprint_delta_min_ratio:
                                    _fp_entry_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                    self._emit_debug(
                                        f"Alternator: {symbol} {_fp_entry_dir} trailing reversal "
                                        f"blocked by footprint delta filter — "
                                        f"net_delta={_fp_delta:+.4f} "
                                        f"(ask={_fp_ask:.4f}, bid={_fp_bid:.4f}, "
                                        f"ratio={_fp_ratio:.3f}) — waiting"
                                    )
                                    continue
                            else:
                                self._emit_debug(
                                    f"Alternator: {symbol} footprint delta filter — "
                                    "no trade data in window, skipping"
                                )
                        if ob_wall_suppress and will_flip and new_entry_side is not None:
                            _ob_ticker = self._latest_ticker.get(symbol) or {}
                            _ob_price = self._extract_float(
                                _ob_ticker.get("last") or _ob_ticker.get("lastPr")
                            )
                            if _ob_price and _ob_price > 0:
                                _snap_altr = self._last_full_snapshot
                                _altr_ob_book = (
                                    ((_snap_altr.get("market_data") or {}).get(symbol) or {})
                                    .get("order_book", {})
                                    if _snap_altr else {}
                                )
                                _altr_ob_levels = (
                                    _altr_ob_book.get("asks") if new_entry_side == "buy"
                                    else _altr_ob_book.get("bids")
                                ) or []
                                _altr_ob_nearby = [
                                    s for p, s in _altr_ob_levels
                                    if (
                                        _ob_price <= p <= _ob_price * (1 + ob_wall_proximity_pct / 100.0)
                                        if new_entry_side == "buy"
                                        else _ob_price * (1 - ob_wall_proximity_pct / 100.0) <= p <= _ob_price
                                    )
                                ]
                                if _altr_ob_levels and _altr_ob_nearby:
                                    _altr_avg = sum(s for _, s in _altr_ob_levels) / len(_altr_ob_levels)
                                    _altr_wall = max(_altr_ob_nearby)
                                    if _altr_avg > 0 and _altr_wall >= ob_wall_ratio * _altr_avg:
                                        _wall_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                        self._emit_debug(
                                            f"Alternator: {symbol} {_wall_dir} trailing reversal "
                                            f"blocked by OB wall — wall_size={_altr_wall:.2f} "
                                            f"({_altr_wall / _altr_avg:.1f}x avg={_altr_avg:.2f}) "
                                            f"within {ob_wall_proximity_pct}% of price — waiting"
                                        )
                                        continue
                        self._alternator_above_threshold.discard(symbol)
                        self._alternator_peak_pnl_pct.pop(symbol, None)
                        self._alternator_peak_pnl_usd.pop(symbol, None)
                        self._alternator_flipping.add(symbol)
                        asyncio.create_task(
                            self._alternator_flip(
                                symbol=symbol,
                                close_side=close_side,
                                close_pos_side=close_pos_side,
                                new_entry_side=new_entry_side if will_flip else None,
                                new_entry_pos_side=new_entry_pos_side if will_flip else None,
                                contracts=contracts,
                                trade_mode=trade_mode,
                                flip_count=flip_count,
                                trigger="profit_trailing",
                            ),
                            name=f"alternator-flip-{symbol}",
                        )
                    else:
                        self._emit_debug(
                            f"Alternator: {symbol} trailing profit — "
                            f"peak_pct={peak_pct!r} current_pct={upl_pct!r} "
                            f"peak_usd={peak_usd!r} current_usd={upl_usd!r} "
                            f"pullback_needed={trailing_pullback_pct}% — waiting"
                        )
                    continue

            # ── Priority 4: Restart at loss ───────────────────────────────────
            hit_restart_pct = (
                _restart_loss_pct_effective is not None
                and upl_pct is not None
                and upl_pct <= -abs(_restart_loss_pct_effective)
            )
            hit_restart_usd = (
                restart_loss_usd is not None
                and upl_usd is not None
                and upl_usd <= -abs(restart_loss_usd)
            )
            if hit_restart_pct or hit_restart_usd:
                will_flip = max_reversals is None or flip_count < max_reversals
                restart_trigger = (
                    f"upl_pct={upl_pct:.2f}% <= -{abs(_restart_loss_pct_effective):.2f}%"
                    if hit_restart_pct
                    else f"upl_usd={upl_usd:.4f} <= -{abs(restart_loss_usd):.4f}"
                )
                self._emit_debug(
                    f"Alternator: {symbol} LOSS triggered ({restart_trigger}), "
                    f"flip_count={flip_count} max_reversals={max_reversals} "
                    f"action={'FLIP' if will_flip else 'CLOSE'}"
                )
                if candle_position_filter and will_flip and new_entry_side is not None:
                    _rp = self._compute_range_position(symbol, candle_position_lookback)
                    _cpf_blocked = _rp is not None and (
                        (new_entry_side == "buy" and _rp > candle_position_long_max)
                        or (new_entry_side == "sell" and _rp < candle_position_short_min)
                    )
                    if _cpf_blocked:
                        _cpf_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                        self._emit_debug(
                            f"Alternator: {symbol} {_cpf_dir} loss-restart blocked by "
                            f"candle-position filter — range_pos={_rp:.3f} "
                            f"(long_max={candle_position_long_max}, "
                            f"short_min={candle_position_short_min}) — waiting"
                        )
                        continue
                if footprint_delta_filter and will_flip and new_entry_side is not None:
                    _fp = self._compute_footprint(symbol)
                    _fp_ask = float(_fp.get("total_ask_vol") or 0.0)
                    _fp_bid = float(_fp.get("total_bid_vol") or 0.0)
                    _fp_total = _fp_ask + _fp_bid
                    if _fp_total > 0:
                        _fp_delta = float(_fp.get("net_delta") or 0.0)
                        _fp_ratio = abs(_fp_delta) / _fp_total
                        _fp_dir_conflict = (
                            (new_entry_side == "buy" and _fp_delta < 0)
                            or (new_entry_side == "sell" and _fp_delta > 0)
                        )
                        if _fp_dir_conflict and _fp_ratio >= footprint_delta_min_ratio:
                            _fp_entry_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                            self._emit_debug(
                                f"Alternator: {symbol} {_fp_entry_dir} loss-restart blocked by "
                                f"footprint delta filter — net_delta={_fp_delta:+.4f} "
                                f"(ask={_fp_ask:.4f}, bid={_fp_bid:.4f}, "
                                f"ratio={_fp_ratio:.3f}) — waiting"
                            )
                            continue
                    else:
                        self._emit_debug(
                            f"Alternator: {symbol} footprint delta filter — "
                            "no trade data in window, skipping"
                        )
                if ob_wall_suppress and will_flip and new_entry_side is not None:
                    _ob_ticker = self._latest_ticker.get(symbol) or {}
                    _ob_price = self._extract_float(
                        _ob_ticker.get("last") or _ob_ticker.get("lastPr")
                    )
                    if _ob_price and _ob_price > 0:
                        _snap_altr = self._last_full_snapshot
                        _altr_ob_book = (
                            ((_snap_altr.get("market_data") or {}).get(symbol) or {})
                            .get("order_book", {})
                            if _snap_altr else {}
                        )
                        _altr_ob_levels = (
                            _altr_ob_book.get("asks") if new_entry_side == "buy"
                            else _altr_ob_book.get("bids")
                        ) or []
                        _altr_ob_nearby = [
                            s for p, s in _altr_ob_levels
                            if (
                                _ob_price <= p <= _ob_price * (1 + ob_wall_proximity_pct / 100.0)
                                if new_entry_side == "buy"
                                else _ob_price * (1 - ob_wall_proximity_pct / 100.0) <= p <= _ob_price
                            )
                        ]
                        if _altr_ob_levels and _altr_ob_nearby:
                            _altr_avg = sum(s for _, s in _altr_ob_levels) / len(_altr_ob_levels)
                            _altr_wall = max(_altr_ob_nearby)
                            if _altr_avg > 0 and _altr_wall >= ob_wall_ratio * _altr_avg:
                                _wall_dir = "LONG" if new_entry_side == "buy" else "SHORT"
                                self._emit_debug(
                                    f"Alternator: {symbol} {_wall_dir} loss-restart blocked by OB wall "
                                    f"— wall_size={_altr_wall:.2f} "
                                    f"({_altr_wall / _altr_avg:.1f}x avg={_altr_avg:.2f}) "
                                    f"within {ob_wall_proximity_pct}% of price — waiting"
                                )
                                continue
                self._alternator_flipping.add(symbol)
                asyncio.create_task(
                    self._alternator_flip(
                        symbol=symbol,
                        close_side=close_side,
                        close_pos_side=close_pos_side,
                        new_entry_side=new_entry_side if will_flip else None,
                        new_entry_pos_side=new_entry_pos_side if will_flip else None,
                        contracts=contracts,
                        trade_mode=trade_mode,
                        flip_count=flip_count,
                        trigger="loss",
                    ),
                    name=f"alternator-flip-{symbol}",
                )

    async def _alternator_flip(
        self,
        *,
        symbol: str,
        close_side: str,
        close_pos_side: str | None,
        new_entry_side: str | None,
        new_entry_pos_side: str | None,
        contracts: float,
        trade_mode: str | None,
        flip_count: int,
        trigger: str,
    ) -> None:
        """Close the current position and optionally open the reversed entry.

        When ``new_entry_side`` is None (hard stop or max_reversals exhausted)
        only the close order is submitted and no reversal is attempted.
        """
        symbol_key = symbol.upper()
        resolved_trade_mode = trade_mode or "cross"
        try:
            # Step 1: close existing position
            close_coid = self._generate_client_order_id("altr-c")
            close_result = await self._submit_order(
                symbol=symbol,
                side=close_side,
                pos_side=close_pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=True,
                client_order_id=close_coid,
                attach_algo_orders=None,
            )
            if close_result is None:
                self._emit_debug(
                    f"Alternator: {symbol_key} close order failed (no trade API)"
                )
                return
            close_ok = close_result[0] if isinstance(close_result, tuple) else close_result
            if not close_ok:
                self._emit_debug(
                    f"Alternator: {symbol_key} close order rejected — aborting"
                )
                return
            # Record the close leg so the fee card and history page include it.
            _close_ticker = self._latest_ticker.get(symbol_key) or {}
            _close_price = self._price_from_ticker(_close_ticker) or 0.0
            _close_fee_raw = self._extract_float(
                close_ok.get("fee") or close_ok.get("fillFee")
                if isinstance(close_ok, dict) else None
            )
            _close_fee = abs(_close_fee_raw) if _close_fee_raw is not None else None
            await self._record_trade_execution(
                symbol=symbol,
                side=close_side,
                price=_close_price,
                amount=contracts,
                rationale=f"Alternator close (trigger={trigger})",
                fee=_close_fee,
            )
            self._emit_debug(
                f"Alternator: {symbol_key} close order submitted "
                f"(side={close_side}, contracts={contracts}, trigger={trigger})"
            )

            if new_entry_side is None:
                action_label = "hard stop" if trigger == "stop" else "max_reversals reached"
                self._emit_debug(
                    f"Alternator: {symbol_key} {action_label} — "
                    "position closed without reversal"
                )
                return

            # Step 2: brief pause to let OKX register the close
            await asyncio.sleep(1.0)

            # Step 3: open reversed position
            entry_coid = self._generate_client_order_id("altr-e")
            entry_result = await self._submit_order(
                symbol=symbol,
                side=new_entry_side,
                pos_side=new_entry_pos_side,
                size=contracts,
                trade_mode=resolved_trade_mode,
                order_type="market",
                reduce_only=False,
                client_order_id=entry_coid,
                attach_algo_orders=None,
            )
            new_flip_count = flip_count + 1
            self._alternator_flip_counts[symbol_key] = new_flip_count
            entry_ok = entry_result[0] if isinstance(entry_result, tuple) else entry_result
            if entry_ok:
                new_side_label = (
                    "long"
                    if new_entry_pos_side == "long" or new_entry_side == "buy"
                    else "short"
                )
                self._emit_debug(
                    f"Alternator: {symbol_key} reversed to {new_side_label} "
                    f"(flip #{new_flip_count}, trigger={trigger}, contracts={contracts})"
                )
                # Record the entry leg; fee will be back-filled by the reconciler.
                _entry_ticker = self._latest_ticker.get(symbol_key) or {}
                _entry_price = self._price_from_ticker(_entry_ticker) or 0.0
                await self._record_trade_execution(
                    symbol=symbol,
                    side=new_entry_side,
                    price=_entry_price,
                    amount=contracts,
                    rationale=f"Alternator entry (trigger={trigger}, flip=#{new_flip_count})",
                    fee=None,
                )
            else:
                self._emit_debug(
                    f"Alternator: {symbol_key} reversed-entry order rejected "
                    f"(flip #{new_flip_count}, trigger={trigger})"
                )
        except Exception as exc:
            logger.warning("Alternator flip error for %s: %s", symbol_key, exc)
        finally:
            self._alternator_flipping.discard(symbol_key)

    async def _run_llm_supervision(
        self,
        symbol: str,
        pos_info: dict[str, Any],
    ) -> None:
        """Fire a stripped LLM call to produce a supervision mandate for a live Alternator position.

        Runs as a fire-and-forget background task.  The mandate is stored in
        ``self._llm_mandate[symbol]`` and read on the *next* ``_check_alternator()`` cycle
        so execution is never blocked waiting for the LLM.

        The payload is deliberately compact — labeled signal summary only, no raw candle
        series — so the round trip is fast (~1–3 s with Grok 4 Fast).
        """
        symbol_key = symbol.upper()
        self._llm_supervision_running.add(symbol_key)
        try:
            # Gather labeled signals from the stored snapshot via PromptBuilder.
            market_signals: dict[str, Any] = {}
            pre_computed: dict[str, Any] = {}
            try:
                snapshot = await self.state_service.get_market_snapshot()
                if snapshot:
                    from app.services.prompt_builder import PromptBuilder
                    pb = PromptBuilder(snapshot=snapshot)
                    full_ctx = (pb.build(symbol=symbol_key) or {}).get("context") or {}
                    market_signals = full_ctx.get("market_signals") or {}
                    pre_computed = full_ctx.get("pre_computed_modifiers") or {}
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Supervision signal extraction failed for %s: %s", symbol_key, exc)

            ticker = self._latest_ticker.get(symbol_key) or {}
            last_price = self._price_from_ticker(ticker) or 0.0

            supervision_context = {
                "symbol": symbol_key,
                "last_price": last_price,
                "position": {
                    "side": pos_info.get("pos_side"),
                    "upl_pct": pos_info.get("upl_pct"),
                    "upl_usd": pos_info.get("upl_usd"),
                    "flip_count": pos_info.get("flip_count"),
                },
                "market": {
                    "regime": market_signals.get("market_regime"),
                    "htf_alignment": pre_computed.get("htf_alignment_class"),
                    "obv_trend": market_signals.get("obv_trend"),
                    "cvd_trend": market_signals.get("cvd_trend"),
                    "funding_rate_pct": market_signals.get("funding_rate_pct"),
                    "rsi_zone": market_signals.get("rsi_zone"),
                    "adx": market_signals.get("adx"),
                    "atr_pct": market_signals.get("atr_pct"),
                },
            }

            supervision_system = (
                "You are a real-time position supervisor for a crypto perpetual futures Alternator bot. "
                "The Alternator oscillates between long and short positions on configurable profit/loss "
                "thresholds. Your job is to decide whether it should keep running, pause, or close now. "
                "Respond with ONLY a valid JSON object — no markdown, no explanation outside the JSON."
            )

            supervision_prompt = (
                "Given the position state and labeled market signals below, choose ONE action:\n"
                "  continue  — Alternator runs normally (default when signals are mixed or unclear)\n"
                "  pause     — skip reversals this cycle; re-evaluate next cycle\n"
                "  close     — close position immediately without reversing (only for high-conviction reversal signals)\n\n"
                "You may also set:\n"
                "  max_reversals_override: int or null — cap total future flips; null = use bot config\n"
                "  soft_stop_pct: float or null — close if PnL drops below -X% from here; null = disabled\n"
                "  expires_in_seconds: 60–600 — how long this mandate stays valid\n\n"
                f"Context:\n{json.dumps(supervision_context, default=str)}"
            )

            supervision_schema = {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["continue", "pause", "close"],
                    },
                    "max_reversals_override": {
                        "anyOf": [{"type": "integer", "minimum": 0}, {"type": "null"}]
                    },
                    "soft_stop_pct": {
                        "anyOf": [{"type": "number", "minimum": 0}, {"type": "null"}]
                    },
                    "rationale": {"type": "string"},
                    "expires_in_seconds": {
                        "type": "integer",
                        "minimum": 60,
                        "maximum": 600,
                    },
                },
                "required": [
                    "action",
                    "max_reversals_override",
                    "soft_stop_pct",
                    "rationale",
                    "expires_in_seconds",
                ],
            }

            payload = {
                "system": supervision_system,
                "prompt": supervision_prompt,
                "response_schema": supervision_schema,
            }

            decision = await self._llm_service.run(payload)

            action = str(decision.get("action") or "continue").lower()
            if action not in ("continue", "pause", "close"):
                action = "continue"
            expires_in = max(60, min(600, int(decision.get("expires_in_seconds") or 300)))

            mandate: dict[str, Any] = {
                "action": action,
                "max_reversals_override": decision.get("max_reversals_override"),
                "soft_stop_pct": decision.get("soft_stop_pct"),
                "rationale": str(decision.get("rationale") or ""),
                "_expires_at": time.monotonic() + expires_in,
            }
            self._llm_mandate[symbol_key] = mandate
            self._emit_debug(
                f"Alternator LLM supervision: {symbol_key} → action={action} "
                f"expires_in={expires_in}s | {mandate['rationale'][:120]}"
            )

        except Exception as exc:  # pragma: no cover - best-effort; never crash the alternator
            logger.warning("LLM supervision call failed for %s: %s", symbol_key, exc)
        finally:
            self._llm_supervision_running.discard(symbol_key)

    @property
    def ws_connection_status(self) -> tuple[bool, bool, bool]:
        """Return (enabled, public_connected, private_connected) for the OKX WebSocket."""
        if not self._enable_websocket:
            return False, False, False
        pub = (
            self._ws_client is not None
            and self._ws_task is not None
            and not self._ws_task.done()
        )
        priv = (
            self._ws_private_client is not None
            and self._ws_private_task is not None
            and not self._ws_private_task.done()
        )
        return True, pub, priv

    def set_poll_interval(self, seconds: int) -> None:
        """Update the REST polling cadence and matching websocket debug interval."""
        self._poll_interval = max(1, seconds)
        self._ws_debug_interval = max(5.0, float(self._poll_interval))
        self._emit_debug(f"Poll interval updated to {self._poll_interval}s")
        self._wake_poll.set()  # interrupt the current sleep so the new interval takes effect immediately

    def set_ohlcv_fetch_limit(self, limit: int) -> None:
        """Update how many OHLCV candles are fetched from OKX per poll cycle."""
        self._ohlcv_fetch_limit = max(50, int(limit))
        self._emit_debug(f"OHLCV fetch limit updated to {self._ohlcv_fetch_limit}")

    def set_wait_for_tp_sl(self, enabled: bool) -> None:
        """Toggle the guardrail that delays new entries until TP/SL anchors exist."""
        flag = bool(enabled)
        if flag == self._wait_for_tp_sl:
            return
        self._wait_for_tp_sl = flag
        state = "enabled" if flag else "disabled"
        self._emit_debug(f"Wait-for-TP/SL guard {state}")

    def set_flip_llm_decision(self, enabled: bool) -> None:
        """Toggle the flag that inverts BUY/SELL and swaps TP/SL before opening a trade."""
        flag = bool(enabled)
        if flag == self._flip_llm_decision:
            return
        self._flip_llm_decision = flag
        state = "enabled" if flag else "disabled"
        self._emit_debug(f"Flip-LLM-decision {state}")

    def set_screener_config(self, config: dict[str, Any]) -> None:
        """Update the symbol screener configuration at runtime."""
        self._screener_config = dict(config or {})
        self._emit_debug(
            f"Symbol screener config updated: enabled={self._screener_config.get('enabled')} "
            f"max={self._screener_config.get('max_symbols')} "
            f"interval={self._screener_config.get('interval_minutes')}min"
        )

    async def _fetch_all_swap_tickers(self) -> list[dict[str, Any]]:
        """Batch-fetch all SWAP tickers from OKX for screener scoring."""
        if not self._market_api:
            return []
        try:
            response = await asyncio.to_thread(self._market_api.get_tickers, "SWAP")
            return self._safe_data(response)
        except Exception as exc:
            self._emit_debug(f"Screener ticker fetch failed: {exc}")
            return []

    async def run_screener_if_due(self, *, force: bool = False) -> bool:
        """Run the symbol screener if enabled and its interval has elapsed.

        Scores all USDT-SWAP tickers using three components:

          vol_spike_ratio (50%) — current 24h volume divided by the rolling
              average of recent 24h volumes for that symbol.  Highlights *unusual*
              activity rather than raw market-cap size.  Falls back to normalised
              raw volume until sufficient history has accumulated (≥2 samples).

          hl_range_pct (30%) — (high24h − low24h) / open24h.  Measures the
              oscillation amplitude over the last 24 h, which directly reflects
              Alternator profit potential.  All fields are already present in
              the OKX ticker response — no extra API calls required.

          momentum_pct (20%) — abs((last − open24h) / open24h).  Kept as a
              minor component for recency; downweighted because a large 24 h body
              indicates a trending move (bad for alternation) rather than
              volatility.  Retaining it avoids selecting flat, dead coins.

        Applies configured filters (universe pattern, min volume, min momentum,
        min HL range), then replaces the active symbol list with the top-N
        winners.  Returns True when the active symbol list was modified.

        When *force* is True the interval gate is skipped — the screener runs on
        every call regardless of when it last fired.  Use this from the prompt
        scheduler so the active symbol list is always fresh before each LLM tick.
        """
        cfg = self._screener_config
        if not cfg or not cfg.get("enabled"):
            return False
        now = time.time()
        if not force:
            interval_secs = max(300, int(cfg.get("interval_minutes") or 60) * 60)
            if now - self._screener_last_run < interval_secs:
                return False

        tickers = await self._fetch_all_swap_tickers()
        if not tickers:
            self._emit_debug("Screener: no tickers returned from OKX")
            return False

        universe_pattern = str(cfg.get("universe_filter") or "*-USDT-SWAP").strip().upper()
        max_symbols = max(1, int(cfg.get("max_symbols") or 5))
        min_volume_usd = float(cfg.get("min_volume_usd") or 0.0)
        min_momentum_pct = float(cfg.get("min_momentum_pct") or 0.0)
        min_hl_range_pct = float(cfg.get("min_hl_range_pct") or 0.0)
        vol_history_window = max(2, int(cfg.get("vol_history_window") or 8))

        candidates: list[dict[str, Any]] = []
        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            inst_id = str(ticker.get("instId") or "").upper()
            if not inst_id:
                continue
            if not fnmatch.fnmatch(inst_id, universe_pattern):
                continue
            last = self._extract_float(ticker.get("last"))
            open24h = self._extract_float(ticker.get("open24h"))
            high24h = self._extract_float(ticker.get("high24h"))
            low24h = self._extract_float(ticker.get("low24h"))
            vol_ccy_24h = self._extract_float(ticker.get("volCcy24h"))
            if not last or last <= 0:
                continue
            if vol_ccy_24h is None or vol_ccy_24h < min_volume_usd:
                continue
            momentum_pct = (
                abs((last - open24h) / open24h * 100)
                if open24h and open24h > 0
                else 0.0
            )
            if momentum_pct < min_momentum_pct:
                continue
            hl_range_pct = (
                (high24h - low24h) / open24h * 100
                if open24h and open24h > 0 and high24h is not None and low24h is not None
                else 0.0
            )
            if hl_range_pct < min_hl_range_pct:
                continue
            # Update rolling volume history, resizing deque if window changed.
            hist = self._screener_vol_history.get(inst_id)
            if hist is None or hist.maxlen != vol_history_window:
                hist = deque(hist or [], maxlen=vol_history_window)
                self._screener_vol_history[inst_id] = hist
            hist.append(vol_ccy_24h)
            # vol_spike_ratio: current vol / rolling average (needs ≥2 samples).
            if len(hist) >= 2:
                avg_vol = sum(hist) / len(hist)
                vol_spike_ratio: float | None = vol_ccy_24h / avg_vol if avg_vol > 0 else 1.0
            else:
                vol_spike_ratio = None  # fall back to raw vol until history builds
            candidates.append(
                {
                    "symbol": inst_id,
                    "vol_ccy_24h": vol_ccy_24h,
                    "vol_spike_ratio": vol_spike_ratio,
                    "hl_range_pct": hl_range_pct,
                    "momentum_pct": momentum_pct,
                }
            )

        self._emit_debug(
            f"Screener: {len(candidates)} candidates from {len(tickers)} tickers "
            f"(vol>={min_volume_usd:.0f} USD, mom>={min_momentum_pct:.2f}%, "
            f"hl_range>={min_hl_range_pct:.2f}%)"
        )
        if not candidates:
            self._screener_last_run = now
            return False

        spike_candidates = [c for c in candidates if c["vol_spike_ratio"] is not None]
        max_spike   = max((c["vol_spike_ratio"] for c in spike_candidates), default=None) or 1.0
        max_raw_vol = max(c["vol_ccy_24h"]  for c in candidates) or 1.0
        max_hl      = max(c["hl_range_pct"] for c in candidates) or 1.0
        max_mom     = max(c["momentum_pct"] for c in candidates) or 1.0

        for c in candidates:
            if c["vol_spike_ratio"] is not None and spike_candidates:
                norm_vol = c["vol_spike_ratio"] / max_spike
            else:
                norm_vol = c["vol_ccy_24h"] / max_raw_vol  # fallback until history builds
            norm_hl  = c["hl_range_pct"] / max_hl
            norm_mom = c["momentum_pct"] / max_mom
            c["score"] = norm_vol * 0.5 + norm_hl * 0.3 + norm_mom * 0.2

        candidates.sort(key=lambda x: x["score"], reverse=True)
        selected = [c["symbol"] for c in candidates[:max_symbols]]

        top_parts = []
        for c in candidates[:max_symbols]:
            spike_str = f"{c['vol_spike_ratio']:.2f}x" if c["vol_spike_ratio"] is not None else "n/a"
            top_parts.append(
                f"{c['symbol']}(score={c['score']:.3f} spike={spike_str} "
                f"hl={c['hl_range_pct']:.2f}% mom={c['momentum_pct']:.2f}%)"
            )
        self._emit_debug(f"Screener selected: {selected} | {', '.join(top_parts)}")

        self._screener_last_run = now
        self._screener_selected_symbols = selected

        # Prune vol history for symbols no longer in universe.
        active_universe = {str(t.get("instId", "")).upper() for t in tickers if isinstance(t, dict)}
        for gone in set(self._screener_vol_history) - active_universe:
            del self._screener_vol_history[gone]

        if set(selected) == set(self.symbols):
            return False
        await self.update_symbols(selected)
        return True

    def is_symbol_blocked(self, symbol: str, guardrails: dict[str, Any] | None = None) -> str | None:
        """Return a human-readable reason string if the symbol cannot be traded right now
        due to cooldown or trade-rate-limit guardrails, or None if clear.
        This is a cheap synchronous read of in-memory state, used to skip the LLM
        call entirely when execution would be blocked regardless of the decision.
        """
        g: dict[str, Any] = guardrails or {}
        cooldown_seconds = int(
            self._extract_float(
                g.get("min_hold_seconds") or g.get("cooldown_seconds")
            )
            or self._poll_interval
        )
        cooldown_seconds = max(0, cooldown_seconds)
        trade_limit = int(self._extract_float(g.get("max_trades_per_hour")) or 0)
        trade_window = int(self._extract_float(g.get("trade_window_seconds")) or 3600)
        now = time.time()
        if cooldown_seconds > 0:
            last_decision = self._decision_state.get(symbol)
            if last_decision:
                last_ts = last_decision.get("timestamp")
                if isinstance(last_ts, (int, float)) and now - float(last_ts) < cooldown_seconds:
                    remaining = cooldown_seconds - (now - float(last_ts))
                    return f"cooldown active ({remaining:.0f}s remaining)"
        if trade_limit > 0:
            history = self._recent_trades.get(symbol)
            if history:
                cutoff = now - max(60, trade_window)
                recent_count = sum(1 for ts in history if ts >= cutoff)
                if recent_count >= trade_limit:
                    return f"trade rate limit reached ({recent_count}/{trade_limit} in window)"
        # Check whether the isolated-wallet bootstrap for this symbol was
        # recently exhausted (all 51008 retries burned through).  We block for
        # BOOTSTRAP_BLOCK_SECONDS so we don't hammer OKX with guaranteed-failing
        # orders.  The block lifts automatically after the TTL expires.
        if symbol in self._bootstrap_blocked:
            blocked_at = self._bootstrap_blocked[symbol]
            remaining = self.BOOTSTRAP_BLOCK_SECONDS - (now - blocked_at)
            if remaining > 0:
                mins = remaining / 60
                return (
                    f"bootstrap blocked ({mins:.0f}m remaining — add USDT to the account "
                    "or reduce isolated_wallet_bootstrap_pct in guardrails)"
                )
            else:
                # TTL expired — remove stale entry and allow retry
                del self._bootstrap_blocked[symbol]
        return None

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
            if self._ws_private_task:
                self._ws_private_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._ws_private_task
                self._ws_private_task = None
            if self._ws_private_client:
                await self._ws_private_client.stop()
                self._ws_private_client = None
            self._latest_positions_raw = None
            self._latest_account_raw = None
            return
        self._emit_debug("Websocket streaming enabled; starting listener")
        if not self._poller_task:
            return
        if self._ws_task:
            return
        self._ws_task = asyncio.create_task(self._run_public_ws(), name="okx-ws")
        if not self._ws_private_task or self._ws_private_task.done():
            self._ws_private_task = asyncio.create_task(self._run_private_ws(), name="okx-ws-private")

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
        self._last_full_snapshot = snapshot
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
            self._latest_ohlcv_htf.pop(symbol, None)
            self._trade_buffers.pop(symbol, None)
            self._footprint_buffers.pop(symbol, None)
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
            self._last_full_snapshot = snapshot
            await self.state_service.set_market_snapshot(snapshot)
            await self._persist_equity(snapshot)

    async def _positions_refresh_loop(self) -> None:
        """Lightweight loop that refreshes positions, equity and ticker prices every few seconds.

        This runs independently of both the 180-second full poll and the private WS
        event callbacks, providing a reliable heartbeat so the LIVE page always shows
        fresh data even when WebSocket events are sparse.
        """
        # Wait for the first full snapshot before starting to patch.
        await asyncio.sleep(self._positions_refresh_interval)
        while True:
            try:
                await self._patch_and_publish_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Positions fast-refresh error: %s", exc)
            try:
                await self._check_skimming()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Skimming check error: %s", exc)
            try:
                await self._check_protector()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Protector check error: %s", exc)
            try:
                await self._check_commutator()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Commutator check error: %s", exc)
            try:
                await self._check_alternator()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Alternator check error: %s", exc)
            try:
                await self._check_launcher()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("Launcher check error: %s", exc)
            try:
                await self._check_ob_wall_stops()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("OB wall stops check error: %s", exc)
            await asyncio.sleep(self._positions_refresh_interval)

    async def _check_ob_wall_stops(self) -> None:
        """Dynamically anchor stop-losses to the nearest supporting OB wall.

        For LONG positions: scans the bid side for a dominant wall within
        ``proximity_pct``% below the current price and places the stop just
        below it (wall_price × (1 − sl_buffer_pct/100)), using the large
        resting orders as a physical barrier.  For SHORT positions: scans the
        ask side for a dominant wall above price and places the stop just above.

        The SL is only updated when:
          1. The proposed SL is tighter than the current one (moves toward
             profit — never loosens an existing stop).
          2. The improvement ≥ min_sl_improvement_pct of current price
             (prevents micro-churning of OKX algo orders).

        Config lives under strategy.ob_wall_stops:
          enabled                 – bool
          proximity_pct           – % below/above price to scan (default 2.0)
          wall_ratio              – N× avg level size to call it a wall (default 3.0)
          min_sl_improvement_pct  – min % price improvement before re-placing (default 0.1)
          sl_buffer_pct           – % behind the wall where stop is placed (default 0.1)
        """
        ob_stops_cfg = self._strategy_config.get("ob_wall_stops") or {}
        if not ob_stops_cfg.get("enabled"):
            return
        proximity_pct = self._extract_float(ob_stops_cfg.get("proximity_pct")) or 2.0
        wall_ratio = self._extract_float(ob_stops_cfg.get("wall_ratio")) or 3.0
        min_improvement_pct = self._extract_float(ob_stops_cfg.get("min_sl_improvement_pct")) or 0.1
        sl_buffer_pct = self._extract_float(ob_stops_cfg.get("sl_buffer_pct")) or 0.1

        snapshot = self._last_full_snapshot
        if not snapshot:
            return
        positions: list[dict[str, Any]] = snapshot.get("positions") or []
        if not positions:
            return

        for pos in positions:
            if not isinstance(pos, dict):
                continue
            symbol = str(pos.get("instId", "")).upper()
            if not symbol:
                continue
            pos_val = self._extract_float(pos.get("pos"))
            if not pos_val or pos_val == 0:
                continue
            pos_side = str(pos.get("posSide", "")).lower()
            trade_mode = str(pos.get("mgnMode") or "").lower() or "cross"

            # Determine position direction
            if pos_side == "long":
                is_long = True
            elif pos_side == "short":
                is_long = False
            else:
                # Net-mode: infer from sign of pos
                is_long = pos_val > 0

            # Current market price
            ticker = self._latest_ticker.get(symbol) or {}
            last_price = self._extract_float(ticker.get("last") or ticker.get("lastPr"))
            if not last_price or last_price <= 0:
                continue

            # Current protection state
            symbol_key = symbol.upper()
            protection = self._position_protection.get(symbol_key) or {}
            current_sl = self._extract_float(protection.get("stop_loss"))
            current_tp = self._extract_float(protection.get("take_profit"))

            # Order book for this symbol
            ob_book = (
                (snapshot.get("market_data") or {}).get(symbol, {}).get("order_book", {})
            )
            if is_long:
                # Bid walls BELOW current price provide support
                levels = ob_book.get("bids") or []
                nearby = [
                    (p, s) for p, s in levels
                    if last_price * (1 - proximity_pct / 100.0) <= p < last_price
                ]
            else:
                # Ask walls ABOVE current price act as resistance / ceiling
                levels = ob_book.get("asks") or []
                nearby = [
                    (p, s) for p, s in levels
                    if last_price < p <= last_price * (1 + proximity_pct / 100.0)
                ]

            if not levels or not nearby:
                continue
            avg_size = sum(s for _, s in levels) / len(levels)
            if avg_size <= 0:
                continue

            # Find the single dominant wall (largest level in range)
            wall_price, wall_size = max(nearby, key=lambda x: x[1])
            if wall_size < wall_ratio * avg_size:
                continue  # no wall significant enough to anchor to

            # Place stop just behind the wall
            if is_long:
                proposed_sl = wall_price * (1 - sl_buffer_pct / 100.0)
            else:
                proposed_sl = wall_price * (1 + sl_buffer_pct / 100.0)

            # Only update if it meaningfully tightens the stop
            if current_sl is not None:
                if is_long and proposed_sl <= current_sl:
                    continue  # would loosen stop — never downgrade
                if not is_long and proposed_sl >= current_sl:
                    continue
                improvement = abs(proposed_sl - current_sl) / last_price * 100.0
                if improvement < min_improvement_pct:
                    continue  # improvement too small to justify a re-place

            action = "BUY" if is_long else "SELL"
            dual_side = pos_side in {"long", "short"}
            effective_pos_side = pos_side if dual_side else None

            self._emit_debug(
                f"OB wall stops: {symbol} {'LONG' if is_long else 'SHORT'} — "
                f"wall at {wall_price:.4f} "
                f"(size={wall_size:.2f}, {wall_size / avg_size:.1f}× avg={avg_size:.2f}) "
                f"— moving SL {current_sl!r} → {proposed_sl:.4f}"
            )
            success = await self._refresh_position_protection(
                symbol=symbol,
                trade_mode=trade_mode,
                action=action,
                take_profit_price=current_tp,
                stop_loss_price=proposed_sl,
                dual_side_mode=dual_side,
                pos_side=effective_pos_side,
            )
            if not success:
                self._emit_debug(
                    f"OB wall stops: {symbol} SL update to {proposed_sl:.4f} failed"
                )

    async def _schedule_patch_publish(self) -> None:
        """Debounce helper: coalesce rapid private-WS frames before patching Redis."""
        await asyncio.sleep(0.3)
        await self._patch_and_publish_snapshot()

    async def _schedule_alternator_ws_check(self) -> None:
        """Run _check_alternator shortly after a private WS position update settles.

        Triggered from ``_handle_private_ws_message`` when the positions channel
        delivers fresh data and the Alternator trailing-reverse mode is active.
        The 0.5 s delay ensures ``_patch_and_publish_snapshot`` has updated
        ``_last_full_snapshot`` before the check reads it.
        """
        await asyncio.sleep(0.5)
        self._alternator_ws_check_pending = False
        try:
            await self._check_alternator()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Alternator WS-triggered check error: %s", exc)

    async def _patch_and_publish_snapshot(self) -> None:
        """Republish the cached snapshot with fresh positions/account from the private WS.

        This is a lightweight path that avoids a full REST round-trip for OHLCV,
        indicators, and other market data.  It only replaces the fields that the
        private WS channel owns: positions, account balance, and equity figures.
        """
        self._private_ws_patch_pending = False
        snapshot = self._last_full_snapshot
        if not snapshot:
            # No baseline yet — wait for the first full poll to run.
            return
        try:
            positions_raw = await self._fetch_positions()
            positions = self._annotate_positions(positions_raw)
            account_payload = await self._fetch_account_balance()
            self._refresh_execution_limits_from_account(account_payload)
        except Exception as exc:  # pragma: no cover - best-effort
            logger.debug("Private WS snapshot patch failed: %s", exc)
            return
        patched = dict(snapshot)
        patched["generated_at"] = datetime.now(timezone.utc).isoformat()
        patched["positions"] = positions
        patched["account"] = account_payload.get("details", [])
        patched["account_equity"] = float(
            account_payload.get("total_eq_usd")
            or account_payload.get("total_equity")
            or 0.0
        )
        patched["total_account_value"] = float(account_payload.get("total_account_value") or 0.0)
        patched["total_eq_usd"] = float(account_payload.get("total_eq_usd") or 0.0)
        patched["available_equity"] = account_payload.get("available_equity")
        patched["available_eq_usd"] = account_payload.get("available_eq_usd")
        patched["available_balances"] = account_payload.get("available_balances") or {}
        if self._latest_execution_limits:
            patched["execution_limits"] = {
                key: dict(meta)
                for key, meta in self._latest_execution_limits.items()
                if isinstance(meta, dict)
            }
        # Always carry the latest feedback so the UI reflects entries generated
        # since the last full snapshot build (execution_limits gets the same treatment).
        patched["execution_feedback"] = list(self._execution_feedback)
        # Inject live ticker prices so current-price and PnL columns stay fresh.
        if self._latest_ticker and "market_data" in patched:
            patched["market_data"] = dict(patched["market_data"])
            for symbol, ticker in self._latest_ticker.items():
                if symbol in patched["market_data"] and ticker:
                    patched["market_data"][symbol] = dict(patched["market_data"][symbol])
                    patched["market_data"][symbol]["ticker"] = ticker
            # Also refresh the top-level ticker shortcut used by single-pair layouts.
            primary = self.symbols[0] if self.symbols else None
            if primary and self._latest_ticker.get(primary):
                patched["ticker"] = self._latest_ticker[primary]
        self._last_full_snapshot = patched
        await self.state_service.set_market_snapshot(patched)
        try:
            await self._check_shotgun()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - best-effort
            logger.debug("Shotgun check error (patch): %s", exc)

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
                # ctVal: how many units of the base asset constitute one OKX contract.
                # For BTC-USDT-SWAP this is 0.01 (so 100 contracts = 1 BTC).
                # For micro-priced tokens like NEIRO/BONK it can be in the hundreds or
                # thousands so that each contract has a reasonable USD value.
                # CRITICAL: the order `sz` field is in CONTRACTS, not base-token units.
                # Notional = sz × ctVal × last_price (NOT sz × last_price).
                ct_val = self._extract_float(entry.get("ctVal"))
                self._instrument_specs[symbol] = {
                    "lot_size": lot_size or 0.0,
                    "min_size": min_size or 0.0,
                    "tick_size": tick_size or 0.0,
                    "max_market_size": max_market_size or 0.0,
                    "max_limit_size": max_limit_size or 0.0,
                    "ct_val": ct_val if ct_val and ct_val > 0 else 1.0,
                }
        if not pairs:
            return list(self.symbols)
        return sorted(set(pairs))

    def _tier_cache_key(self, symbol: str, trade_mode: str) -> str:
        """Key helper for memoizing tier metadata per instrument and trade mode."""
        normalized_symbol = (symbol or "").upper()
        normalized_mode = (trade_mode or "isolated").lower()
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

    async def _get_position_tiers(self, symbol: str, trade_mode: str = "isolated") -> list[dict[str, Any]]:
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

    async def _fetch_position_tiers(self, symbol: str, trade_mode: str = "isolated") -> list[dict[str, Any]]:
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
    ) -> dict[str, Any]:
        """Clamp requested size against tier-defined size limits while surfacing IMR metadata."""
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
        tier_max_size = self._extract_float(tier.get("maxSz"))
        tier_max_notional = None
        if tier_max_size and tier_max_size > 0:
            tier_max_notional = tier_max_size * last_price
        final_size = additional_size
        clipped = False
        blocked = False
        if tier_max_size and tier_max_size > 0:
            max_side_allowance = max(tier_max_size - max(0.0, existing_side_size), 0.0)
            if resulting_size - 1e-9 > tier_max_size:
                final_size = max_side_allowance
                clipped = True
                if max_side_allowance <= 0:
                    blocked = True
        required_margin = (additional_size * last_price * imr) if (imr and imr > 0) else None
        return {
            "size": final_size,
            "tier": tier,
            "tier_max_leverage": tier_max_leverage,
            "tier_imr": imr,
            "required_margin": required_margin,
            "tier_max_notional_usd": tier_max_notional,
            "clipped": clipped,
            "blocked": blocked,
            "pos_side": pos_side,
        }

    async def _fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """Pull current SWAP positions from the account API, optionally filtering by instrument."""
        # Use private WS cache when populated, unless master-key sub-account routing is
        # active (in that case the WS connection reflects the master account, not the sub).
        _master_routing = bool(self._sub_account and self._sub_account_use_master)
        if self._enable_websocket and not _master_routing and self._latest_positions_raw is not None:
            if symbol:
                normalized_sym = symbol.upper()
                return [
                    p for p in self._latest_positions_raw
                    if isinstance(p, dict) and str(p.get("instId", "")).upper() == normalized_sym
                ]
            return list(self._latest_positions_raw)
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
        attempts: int = 10,
        delay: float = 0.5,
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
        # Use private WS cache when populated, unless master-key sub-account routing is active.
        _master_routing = bool(self._sub_account and self._sub_account_use_master)
        if self._enable_websocket and not _master_routing and self._latest_account_raw is not None:
            return self._normalize_account_balances(self._latest_account_raw)
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
        if isinstance(response, dict) and response.get("code") not in (None, "0", 0):
            _err_code = response.get("code")
            _err_msg = response.get("msg") or ""
            logger.warning(
                "Account balance REST API error (code=%s%s) — balance unavailable; "
                "will use last-known-good cache if available",
                _err_code,
                f" {_err_msg}" if _err_msg else "",
            )
            if self._last_known_account_balance is not None:
                return self._last_known_account_balance
        data = self._safe_data(response)
        result = self._normalize_account_balances(data)
        # Cache if the result carries meaningful equity data so we can fall back
        # to it when the next REST call fails.
        if result.get("total_eq_usd", 0.0) or result.get("total_equity", 0.0):
            self._last_known_account_balance = result
        return result

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
                limit=self._ohlcv_fetch_limit,
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

    async def _fetch_ohlcv_htf(self, symbol: str) -> list[list[Any]]:
        """Fetch OHLCV candles at the next-higher timeframe for the same wall-clock window."""
        htf_bar, _ = self._HTF_MAP.get(self._ohlc_bar, ("", 0))
        if not htf_bar:
            return []
        limit = self._ohlcv_fetch_limit
        cached = self._latest_ohlcv_htf.get(symbol)
        if not self._market_api:
            return cached or []
        try:
            response = await asyncio.to_thread(
                self._market_api.get_candlesticks,
                instId=symbol,
                bar=htf_bar,
                limit=limit,
            )
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("HTF OHLCV fetch failed for %s (%s): %s", symbol, htf_bar, exc)
            return cached or []
        data = self._safe_data(response)
        if data:
            self._latest_ohlcv_htf[symbol] = data
            return data
        return cached or []

    def _compute_custom_metrics(self, symbol: str, order_book: dict[str, Any]) -> dict[str, Any]:
        """Derive proprietary metrics (CVD/OFI/footprint/etc.) from cached trades and current depth."""
        cvd = self._calculate_cvd(symbol)
        ofi = self._calculate_ofi(symbol, order_book)
        cvd_series = self._build_cvd_series(symbol)
        ofi_ratio_series = self._latest_depth_metrics.get(symbol, [])[-200:]
        metrics: dict[str, Any] = {
            "cumulative_volume_delta": cvd,
            "order_flow_imbalance": ofi,
            "cvd_series": cvd_series,
            "ofi_ratio_series": ofi_ratio_series,
        }
        footprint = self._compute_footprint(symbol)
        if footprint:
            metrics["footprint"] = footprint
        return metrics

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
        tail_df = df
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

    @staticmethod
    def _compute_structure(ohlcv: list[list[Any]], swing_lookback: int = 5) -> dict[str, Any]:
        """Detect swing highs/lows, liquidity sweeps, and Market Structure Shifts (MSS).

        A swing high is a candle whose high is the highest within ``swing_lookback``
        candles on each side.  A swing low is the symmetric opposite.

        A **liquidity sweep** occurs when price wicks below a recent swing low (long
        sweep) or above a recent swing high (short sweep) and then closes back inside
        the range — indicating stop-loss hunting.

        A **Market Structure Shift (MSS)** is confirmed on the *next candle* after
        the sweep candle: the close must be back above the swept swing low (bullish
        MSS) or below the swept swing high (bearish MSS).

        Returns
        -------
        dict with:
          swing_highs   – list of {index, price, ts_ms} for recent pivot highs (newest last)
          swing_lows    – list of {index, price, ts_ms} for recent pivot lows (newest last)
          last_sweep    – {direction, level, candle_index, ts_ms} | None
          mss_confirmed – bool: True when the candle immediately after the sweep
                          closes on the "recovery" side of the swept level
          mss_direction – "bullish" | "bearish" | None
        """
        empty: dict[str, Any] = {
            "swing_highs": [],
            "swing_lows": [],
            "last_sweep": None,
            "mss_confirmed": False,
            "mss_direction": None,
        }
        if not ohlcv or len(ohlcv) < swing_lookback * 2 + 3:
            return empty

        normalized = [row[:6] for row in ohlcv if len(row) >= 6]
        if not normalized:
            return empty

        highs = [float(r[2]) for r in normalized]
        lows = [float(r[3]) for r in normalized]
        closes = [float(r[4]) for r in normalized]
        ts_list = [float(r[0]) for r in normalized]
        n = len(normalized)

        # ── Pivot detection ──────────────────────────────────────────────────
        swing_highs: list[dict[str, Any]] = []
        swing_lows: list[dict[str, Any]] = []
        lb = swing_lookback
        # Leave the last ``lb`` candles unpinned (not enough right-side context yet)
        for i in range(lb, n - lb):
            left_h = highs[i - lb : i]
            right_h = highs[i + 1 : i + lb + 1]
            if highs[i] > max(left_h) and highs[i] > max(right_h):
                swing_highs.append({"index": i, "price": highs[i], "ts_ms": ts_list[i]})
            left_l = lows[i - lb : i]
            right_l = lows[i + 1 : i + lb + 1]
            if lows[i] < min(left_l) and lows[i] < min(right_l):
                swing_lows.append({"index": i, "price": lows[i], "ts_ms": ts_list[i]})

        # ── Sweep + MSS detection ────────────────────────────────────────────
        # Scan from the most recent candle backward for the first sweep event.
        # We need at least 2 candles after a pivot for an MSS confirmation check.
        last_sweep: dict[str, Any] | None = None
        mss_confirmed = False
        mss_direction: str | None = None

        # Check sweep of swing lows (bullish scenario: wick below → close above)
        if swing_lows:
            # Use the most recent swing low as the reference liquidity level
            ref_low = swing_lows[-1]
            level = ref_low["price"]
            pivot_idx = ref_low["index"]
            # Look for a sweep candle after the pivot
            for i in range(pivot_idx + 1, n):
                if lows[i] < level:
                    # Wick swept below the swing low
                    last_sweep = {
                        "direction": "long",
                        "level": level,
                        "candle_index": i,
                        "ts_ms": ts_list[i],
                    }
                    # MSS: next candle (or same candle) closes back above the level
                    if closes[i] > level:
                        mss_confirmed = True
                        mss_direction = "bullish"
                    elif i + 1 < n and closes[i + 1] > level:
                        mss_confirmed = True
                        mss_direction = "bullish"
                    break  # use the earliest sweep after the pivot

        # Check sweep of swing highs (bearish scenario: wick above → close below)
        if swing_highs:
            ref_high = swing_highs[-1]
            level_h = ref_high["price"]
            pivot_idx_h = ref_high["index"]
            for i in range(pivot_idx_h + 1, n):
                if highs[i] > level_h:
                    sweep_candidate = {
                        "direction": "short",
                        "level": level_h,
                        "candle_index": i,
                        "ts_ms": ts_list[i],
                    }
                    # Prefer the more *recent* sweep event
                    if last_sweep is None or i > last_sweep["candle_index"]:
                        mss_candidate = False
                        mss_dir_candidate: str | None = None
                        if closes[i] < level_h:
                            mss_candidate = True
                            mss_dir_candidate = "bearish"
                        elif i + 1 < n and closes[i + 1] < level_h:
                            mss_candidate = True
                            mss_dir_candidate = "bearish"
                        last_sweep = sweep_candidate
                        mss_confirmed = mss_candidate
                        mss_direction = mss_dir_candidate
                    break

        return {
            "swing_highs": swing_highs[-10:],  # keep last 10 pivots
            "swing_lows": swing_lows[-10:],
            "last_sweep": last_sweep,
            "mss_confirmed": mss_confirmed,
            "mss_direction": mss_direction,
        }

    def _get_trade_buffer(self, symbol: str) -> Deque[dict[str, float]]:
        """Return (and lazily create) the rolling trade buffer for a symbol."""
        buffer = self._trade_buffers.get(symbol)
        if buffer is None:
            buffer = deque(maxlen=500)
            self._trade_buffers[symbol] = buffer
        return buffer

    def _get_footprint_buffer(self, symbol: str) -> Deque[dict[str, float]]:
        """Return (and lazily create) the footprint trade buffer for a symbol."""
        buf = self._footprint_buffers.get(symbol)
        if buf is None:
            # 20 000 entries holds several hours of liquid-pair traffic at burst rates.
            buf = deque(maxlen=20000)
            self._footprint_buffers[symbol] = buf
        return buf

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

    def _compute_footprint(self, symbol: str) -> dict[str, Any]:
        """Build a sliding-window footprint chart profile for the given symbol.

        Aggregates trades from ``_footprint_buffers`` over the last
        ``FOOTPRINT_WINDOW_SECONDS`` seconds into price buckets of width
        ``tick_size × FOOTPRINT_BUCKET_TICKS``.  For each bucket records the
        ask-side volume (buy aggressor) and bid-side volume (sell aggressor).

        Returns a compact dict suitable for inclusion in the LLM context:
          poc_price           – price bucket with highest total volume
          value_area_high/low – price range containing 70% of total volume
          net_delta           – total (ask_vol − bid_vol) across all buckets
          total_ask_vol       – total buy-aggressor volume in the window
          total_bid_vol       – total sell-aggressor volume in the window
          delta_imbalance_zones – top-5 buckets by |ask_vol − bid_vol|,
                                  tagged "buy_pressure" or "sell_pressure"
          window_seconds      – actual window used
          bucket_size         – price width of each bucket

        Returns ``{}`` when there is insufficient data to build a profile.
        """
        buf = self._footprint_buffers.get(symbol.upper()) or self._footprint_buffers.get(symbol)
        if not buf:
            return {}

        spec = self._instrument_specs.get(symbol.upper())
        tick_size = float((spec or {}).get("tick_size") or 0.0)

        # Determine bucket size: percentage-based (preferred) or tick-based fallback.
        _bucket_pct = float(self._footprint_config.get("bucket_pct") or 0.0)
        if _bucket_pct > 0.0:
            # Use the most recent trade price from the buffer as the reference price.
            _ref_px = buf[-1]["px"] if buf else 0.0
            bucket_size = _ref_px * (_bucket_pct / 100.0) if _ref_px > 0 else 0.0
        else:
            if tick_size <= 0:
                return {}
            bucket_size = tick_size * self.FOOTPRINT_BUCKET_TICKS

        if bucket_size <= 0:
            return {}

        cutoff = time.time() - self.FOOTPRINT_WINDOW_SECONDS

        # profile: bucket_index (int) -> {"ask": float, "bid": float}
        # Using integer keys avoids floating-point drift when keying a dict.
        profile: dict[int, dict[str, float]] = {}
        total_ask = 0.0
        total_bid = 0.0

        for t in buf:
            if t["ts"] < cutoff:
                continue
            idx = int(round(t["px"] / bucket_size))
            vol = t["vol"]
            entry = profile.get(idx)
            if entry is None:
                entry = {"ask": 0.0, "bid": 0.0}
                profile[idx] = entry
            if t["side"] > 0:  # buy aggressor hits the ask
                entry["ask"] += vol
                total_ask += vol
            else:              # sell aggressor hits the bid
                entry["bid"] += vol
                total_bid += vol

        if not profile:
            return {}

        total_vol = total_ask + total_bid

        # ── Point of Control ──────────────────────────────────────────────────
        poc_idx = max(profile, key=lambda i: profile[i]["ask"] + profile[i]["bid"])
        poc_price = round(poc_idx * bucket_size, 8)

        # ── Value Area (70% of total volume) expanding outward from POC ───────
        sorted_idxs = sorted(profile.keys())
        poc_pos = sorted_idxs.index(poc_idx)
        va_vol = profile[poc_idx]["ask"] + profile[poc_idx]["bid"]
        lo_pos = poc_pos
        hi_pos = poc_pos
        target = total_vol * 0.70

        while va_vol < target:
            can_expand_lo = lo_pos > 0
            can_expand_hi = hi_pos < len(sorted_idxs) - 1
            if not can_expand_lo and not can_expand_hi:
                break
            add_lo = (
                (profile[sorted_idxs[lo_pos - 1]]["ask"] + profile[sorted_idxs[lo_pos - 1]]["bid"])
                if can_expand_lo else 0.0
            )
            add_hi = (
                (profile[sorted_idxs[hi_pos + 1]]["ask"] + profile[sorted_idxs[hi_pos + 1]]["bid"])
                if can_expand_hi else 0.0
            )
            if add_hi >= add_lo and can_expand_hi:
                hi_pos += 1
                va_vol += add_hi
            elif can_expand_lo:
                lo_pos -= 1
                va_vol += add_lo
            else:
                hi_pos += 1
                va_vol += add_hi

        vah = round(sorted_idxs[hi_pos] * bucket_size, 8)
        val = round(sorted_idxs[lo_pos] * bucket_size, 8)

        # ── Delta imbalance zones (top 5 by |ask_vol − bid_vol|) ─────────────
        imbalances = []
        for idx, data in profile.items():
            delta = data["ask"] - data["bid"]
            if delta == 0.0:
                continue
            imbalances.append({
                "price": round(idx * bucket_size, 8),
                "ask_vol": round(data["ask"], 4),
                "bid_vol": round(data["bid"], 4),
                "delta": round(delta, 4),
                "type": "buy_pressure" if delta > 0 else "sell_pressure",
            })
        imbalances.sort(key=lambda z: -abs(z["delta"]))

        return {
            "window_seconds": self.FOOTPRINT_WINDOW_SECONDS,
            "bucket_size": bucket_size,
            "poc_price": poc_price,
            "value_area_high": vah,
            "value_area_low": val,
            "total_ask_vol": round(total_ask, 4),
            "total_bid_vol": round(total_bid, 4),
            "net_delta": round(total_ask - total_bid, 4),
            "delta_imbalance_zones": imbalances[:5],
        }

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
            _log_msg = f"Execution feedback ({level}) {symbol}: {message}{meta_suffix}"
            # Use the appropriate Python log level so this always appears in the
            # log file even when debug output is suppressed (e.g. --log-level warning).
            if level == "error":
                logger.error(_log_msg)
            else:
                logger.warning(_log_msg)
            # Also echo to the debug sink (backend_events / Debug page).
            try:
                self._log_sink(_log_msg)
            except Exception:  # pragma: no cover - defensive
                pass

    def record_execution_feedback(
        self,
        symbol: str,
        message: str,
        *,
        level: str = "info",
        meta: dict[str, Any] | None = None,
        recommendation: dict[str, Any] | None = None,
    ) -> None:
        """Public wrapper so other services (scheduler/UI) can emit execution alerts."""
        self._record_execution_feedback(
            symbol,
            message,
            level=level,
            meta=meta,
            recommendation=recommendation,
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
            "max_tradeable_notional_usd",
            "margin_available_usd",
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
            if candidate and (
                candidate in INSUFFICIENT_MARGIN_CODES
                or candidate in ORDER_INSUFFICIENT_MARGIN_CODES
            ):
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
                "current_margin": current_margin,
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
        # OKX ctVal: how many base-token units constitute one *contract*.
        # raw_size throughout this function is kept in base-token units so that
        # `raw_size × last_price` always gives the correct USD notional.
        # The final conversion to OKX `sz` (contracts) happens just before _submit_order.
        ct_val = self._contract_value(symbol)
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
        # Conviction floor — hard-block BUY/SELL if LLM confidence is below threshold.
        # Repurposes min_leverage_confidence_gate as a door-closing guardrail on ALL
        # execution paths (not just the legacy leverage-scaling path).
        conviction_floor = min(
            max(self._extract_float(guardrails.get("min_leverage_confidence_gate")) or 0.5, 0.0),
            1.0,
        )
        _early_confidence = self._normalize_confidence(decision.get("confidence"))
        if action in {"BUY", "SELL"} and _early_confidence < conviction_floor:
            self._emit_debug(
                f"{symbol} conviction {_early_confidence:.2f} below floor {conviction_floor:.2f}; blocking {action}"
            )
            self._record_execution_feedback(
                symbol,
                f"Conviction {_early_confidence:.2f} below floor {conviction_floor:.2f}; trade blocked",
                level="warning",
                meta={"confidence": _early_confidence, "conviction_floor": conviction_floor},
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
        require_protection = bool(guardrails.get("require_protection", False))
        if wait_for_tp_sl is None:
            wait_for_tp_sl = self._wait_for_tp_sl
        else:
            wait_for_tp_sl = bool(wait_for_tp_sl)
        flip_llm_decision = guardrails.get("flip_llm_decision")
        if flip_llm_decision is None:
            flip_llm_decision = self._flip_llm_decision
        else:
            flip_llm_decision = bool(flip_llm_decision)
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

        # Prefer the live Redis snapshot positions over the (potentially stale)
        # context positions.  With slow models (e.g. Gemma 4 31B taking ~10 min),
        # the context was assembled before the LLM call and a position can be
        # closed in the interim.  Using stale context positions would cause
        # current_side to show an already-closed side, resulting in reduce_only=True
        # being set and OKX rejecting the order with 51169.
        positions = snapshot_positions or context.get("positions") or []
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
        # Never flip Launcher-originated decisions: the Launcher already chose
        # the direction based on its own signal; flipping it would contradict that.
        _is_launcher_decision = str(decision.get("_decision_origin") or "").lower() == "launcher"
        if flip_llm_decision and not _is_launcher_decision and action in {"BUY", "SELL"}:
            action = "SELL" if action == "BUY" else "BUY"
            decision = dict(decision)
            orig_tp = self._extract_float(decision.get("take_profit"))
            orig_sl = self._extract_float(decision.get("stop_loss"))
            if last_price and last_price > 0 and (orig_tp or orig_sl):
                # Mirror each level through the current price so that
                # the absolute distance — and therefore R:R — is preserved.
                # new_level = 2 * price - original_level
                decision["take_profit"] = (
                    round(2 * last_price - orig_tp, 10) if orig_tp else None
                )
                decision["stop_loss"] = (
                    round(2 * last_price - orig_sl, 10) if orig_sl else None
                )
                self._emit_debug(
                    f"Flip-LLM-decision applied: flipped to {action}; "
                    f"TP {orig_tp} -> {decision['take_profit']}, "
                    f"SL {orig_sl} -> {decision['stop_loss']} "
                    f"(mirrored around {last_price})"
                )
            else:
                # No price available – fall back to raw swap and warn.
                decision["take_profit"], decision["stop_loss"] = orig_sl, orig_tp
                self._emit_debug(
                    f"Flip-LLM-decision applied: flipped to {action}; "
                    f"TP/SL raw-swapped (no reference price available)"
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
        # Detect hedge/dual-side mode from ANY position in the account, not
        # just the target symbol.  When opening a brand-new position (bootstrap)
        # there are no existing entries for this symbol, so a symbol-specific
        # check always returns False — even on hedge-mode accounts.
        dual_side_mode = False
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            if pos.get("posSide") in {"long", "short"}:
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
        _origin_label = "Launcher" if str(decision.get("_decision_origin") or "").lower() == "launcher" else "LLM"
        summary = (
            f"{_origin_label} decision {action} "
            f"notional_usd={decision.get('notional_usd', decision.get('position_size', '--'))} "
            f"conf={decision.get('confidence', '--')} symbol={symbol}"
        )

        if action == "HOLD":
            self._decision_state[symbol] = {"action": action, "timestamp": now}
            self._emit_debug(summary)
            return False

        # ── Launcher LLM filter (llm_with_filter mode) ───────────────────────
        _gov = self._launcher_config
        if str(_gov.get("mode") or "disabled").lower() == "llm_with_filter":
            gov_signal = self._launcher_evaluate_signal(symbol)
            llm_direction = "buy" if action == "BUY" else "sell"
            if gov_signal != llm_direction:
                # Distinguish: no data vs neutral vs conflicting.
                _snap = self._last_full_snapshot
                _sym_indicators = (
                    ((_snap.get("market_data") or {}).get(symbol) or {}).get("indicators") or {}
                    if _snap else {}
                )
                _rsi = self._extract_float(_sym_indicators.get("rsi"))
                if _snap is None or _rsi is None:
                    veto_detail = "no indicator data available"
                elif gov_signal is None:
                    veto_detail = f"indicators neutral (RSI={_rsi:.1f}), no directional alignment"
                else:
                    veto_detail = f"conflicting indicator signal={gov_signal!r}"
                veto_reason = (
                    f"Launcher vetoed LLM {action} for {symbol}: {veto_detail}"
                )
                self._emit_debug(veto_reason)
                self._record_execution_feedback(
                    symbol,
                    veto_reason,
                    level="warning",
                    meta={"action": action, "launcher_signal": gov_signal},
                )
                return False
            # Launcher agrees — amend trade with Launcher's TP/SL if configured.
            gov_tp_pct = self._extract_float(_gov.get("tp_pct"))
            gov_sl_pct = self._extract_float(_gov.get("sl_pct"))
            gov_notional = self._extract_float(_gov.get("notional_usd"))
            _amended: list[str] = []
            if gov_notional and gov_notional > 0:
                decision = dict(decision)
                decision["notional_usd"] = gov_notional
                _amended.append(f"notional_usd={gov_notional}")
            if last_price and last_price > 0 and (gov_tp_pct or gov_sl_pct):
                decision = dict(decision)
                if gov_tp_pct and gov_tp_pct > 0:
                    tp_val = last_price * (1 + gov_tp_pct / 100.0) if action == "BUY" else last_price * (1 - gov_tp_pct / 100.0)
                    decision["take_profit"] = tp_val
                    _amended.append(f"take_profit={tp_val:.4f}")
                if gov_sl_pct and gov_sl_pct > 0:
                    sl_val = last_price * (1 - gov_sl_pct / 100.0) if action == "BUY" else last_price * (1 + gov_sl_pct / 100.0)
                    decision["stop_loss"] = sl_val
                    _amended.append(f"stop_loss={sl_val:.4f}")
            if _amended:
                self._emit_debug(
                    f"Launcher approved LLM {action} for {symbol}; amended: {', '.join(_amended)}"
                )
            else:
                self._emit_debug(f"Launcher approved LLM {action} for {symbol} (no amendments)")

        # ── CVD / Order-Flow Guard ────────────────────────────────────────────
        # Implements spec section 3: Order Flow & Heatmap Integration.
        # Validates that CVD momentum (slope of the recent cvd_series) agrees
        # with the LLM's intended direction.  Neutral CVD is never a blocker;
        # only a clear conflict (bearish CVD + BUY, or bullish CVD + SELL) vetoes.
        _cvd_cfg = guardrails.get("cvd_guard") or {}
        if _cvd_cfg.get("enabled") and action in ("BUY", "SELL"):
            _cvd_lookback = max(2, int(self._extract_float(_cvd_cfg.get("lookback")) or 10))
            _cvd_min_slope = self._extract_float(_cvd_cfg.get("min_slope_pct")) or 0.0
            _snap_cvd = self._last_full_snapshot
            _cvd_series = (
                ((_snap_cvd.get("market_data") or {}).get(symbol) or {})
                .get("custom_metrics", {})
                .get("cvd_series")
                if _snap_cvd
                else None
            )
            if isinstance(_cvd_series, list) and len(_cvd_series) >= 2:
                _window = _cvd_series[-_cvd_lookback:]
                _cvd_first, _cvd_last = _window[0], _window[-1]
                _denom = max(abs(_cvd_first), 1.0)
                _slope_pct = (_cvd_last - _cvd_first) / _denom * 100.0
                if _slope_pct > _cvd_min_slope:
                    _cvd_trend = "bullish"
                elif _slope_pct < -_cvd_min_slope:
                    _cvd_trend = "bearish"
                else:
                    _cvd_trend = "neutral"
                _cvd_conflict = (
                    (action == "BUY" and _cvd_trend == "bearish")
                    or (action == "SELL" and _cvd_trend == "bullish")
                )
                if _cvd_conflict:
                    _cvd_reason = (
                        f"CVD guard vetoed {action} for {symbol}: "
                        f"cvd_trend={_cvd_trend} (slope={_slope_pct:+.1f}% over "
                        f"{len(_window)} bars) conflicts with LLM direction"
                    )
                    self._emit_debug(_cvd_reason)
                    self._record_execution_feedback(
                        symbol,
                        _cvd_reason,
                        level="warning",
                        meta={
                            "action": action,
                            "cvd_trend": _cvd_trend,
                            "cvd_slope_pct": round(_slope_pct, 2),
                        },
                    )
                    return False
                else:
                    self._emit_debug(
                        f"{symbol} CVD guard: trend={_cvd_trend} slope={_slope_pct:+.1f}% "
                        f"— aligned with {action}; proceeding"
                    )
            else:
                self._emit_debug(
                    f"{symbol} CVD guard: insufficient CVD data "
                    f"(series len={len(_cvd_series) if isinstance(_cvd_series, list) else 'none'}); skipping"
                )

        # ── Order-Book Wall Guard ─────────────────────────────────────────────
        # Implements spec section 3 (Heatmap / limit-order wall detection).
        # Scans the L2 order book for abnormally large resting orders on the
        # *opposing* side within a configurable % of the current price.
        # A dominant wall nearby = price barrier the trade must punch through;
        # vetoes until the wall is consumed or price moves away from it.
        _ob_cfg = guardrails.get("ob_wall_guard") or {}
        if _ob_cfg.get("enabled") and action in ("BUY", "SELL") and last_price and last_price > 0:
            _ob_proximity_pct = self._extract_float(_ob_cfg.get("proximity_pct")) or 1.0
            _ob_wall_ratio    = self._extract_float(_ob_cfg.get("wall_ratio"))    or 3.0
            _snap_ob = self._last_full_snapshot
            _ob_book = (
                ((_snap_ob.get("market_data") or {}).get(symbol) or {})
                .get("order_book", {})
                if _snap_ob
                else {}
            )
            # For BUY we check the ask side (sell walls above price).
            # For SELL we check the bid side (buy walls below price).
            if action == "BUY":
                _ob_levels = _ob_book.get("asks") or []  # [[price, size], ...]
                _ob_nearby = [
                    s for p, s in _ob_levels
                    if last_price <= p <= last_price * (1 + _ob_proximity_pct / 100.0)
                ]
            else:
                _ob_levels = _ob_book.get("bids") or []  # [[price, size], ...]
                _ob_nearby = [
                    s for p, s in _ob_levels
                    if last_price * (1 - _ob_proximity_pct / 100.0) <= p <= last_price
                ]
            if _ob_levels and _ob_nearby:
                _ob_avg_size = sum(s for _, s in _ob_levels) / len(_ob_levels)
                _ob_wall_size = max(_ob_nearby)
                if _ob_avg_size > 0 and _ob_wall_size >= _ob_wall_ratio * _ob_avg_size:
                    _ob_reason = (
                        f"OB wall guard vetoed {action} for {symbol}: "
                        f"opposing wall size={_ob_wall_size:.2f} "
                        f"({_ob_wall_size / _ob_avg_size:.1f}x avg={_ob_avg_size:.2f}) "
                        f"within {_ob_proximity_pct}% of price={last_price}"
                    )
                    self._emit_debug(_ob_reason)
                    self._record_execution_feedback(
                        symbol,
                        _ob_reason,
                        level="warning",
                        meta={
                            "action": action,
                            "wall_size": _ob_wall_size,
                            "avg_level_size": round(_ob_avg_size, 4),
                            "wall_ratio": round(_ob_wall_size / _ob_avg_size, 2),
                        },
                    )
                    return False
                else:
                    self._emit_debug(
                        f"{symbol} OB wall guard: no dominant wall within {_ob_proximity_pct}% "
                        f"(max_nearby={_ob_wall_size:.2f}, avg={_ob_avg_size:.2f}); proceeding"
                    )
            else:
                self._emit_debug(
                    f"{symbol} OB wall guard: no {('ask' if action == 'BUY' else 'bid')} levels "
                    f"within {_ob_proximity_pct}% of price; skipping"
                )

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
                    self._record_execution_feedback(
                        symbol,
                        f"Blocked: wait-for-TP/SL guard active; existing protection prevents {action}",
                        level="warning",
                        meta={"current_side": current_side, "action": action},
                    )
                    return False

        if require_alignment and not self._transition_allowed(current_side, action):
            self._emit_debug(
                f"Guardrail blocked {action} for {symbol}: current side={current_side}"
            )
            self._decision_state[symbol] = {"action": current_side, "timestamp": now}
            self._record_execution_feedback(
                symbol,
                f"Blocked by position-alignment guardrail: {action} not allowed while {current_side}",
                level="warning",
                meta={"current_side": current_side, "action": action},
            )
            return False

        last_decision = self._decision_state.get(symbol)
        if cooldown_seconds > 0 and last_decision:
            last_ts = last_decision.get("timestamp")
            if isinstance(last_ts, (int, float)) and now - float(last_ts) < cooldown_seconds:
                remaining = cooldown_seconds - (now - float(last_ts))
                self._emit_debug(
                    f"Guardrail cooldown active for {symbol}; skipping {action} ({remaining:.0f}s left)"
                )
                self._record_execution_feedback(
                    symbol,
                    f"Blocked by cooldown guardrail: {action} requires {cooldown_seconds}s between trades ({remaining:.0f}s remaining)",
                    level="warning",
                    meta={"cooldown_seconds": cooldown_seconds, "remaining_seconds": remaining, "action": action},
                )
                return False

        history = self._recent_trades.setdefault(symbol, deque())
        self._prune_trade_history(history, now, trade_window)
        if trade_limit > 0 and len(history) >= trade_limit:
            self._emit_debug(
                f"Guardrail trade limit hit for {symbol}; skipping {action}"
            )
            self._record_execution_feedback(
                symbol,
                f"Blocked by trade-rate guardrail: {action} exceeds {trade_limit} trades per {trade_window}s window",
                level="warning",
                meta={"trade_limit": trade_limit, "trade_window_seconds": trade_window, "action": action},
            )
            return False

        history.append(now)
        self._decision_state[symbol] = {"action": action, "timestamp": now}
        self._emit_debug(summary)

        execution_cfg = context.get("execution") or {}
        execution_enabled = bool(execution_cfg.get("enabled"))
        if not execution_enabled:
            self._emit_debug(f"Execution disabled for {symbol}; skipping OKX order")
            self._record_execution_feedback(
                symbol,
                f"Execution is disabled — {action} decision recorded but no order placed (enable execution in CFG)",
                level="warning",
                meta={"action": action},
            )
            return False
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot execute decision")
            self._record_execution_feedback(
                symbol,
                "Trade API unavailable; cannot place order (check OKX API credentials)",
                level="error",
                meta={"action": action},
            )
            return False
        instrument_spec = self._instrument_specs.get((symbol or "").upper()) or {}
        execution_trade_mode = execution_cfg.get("trade_mode") or "isolated"
        trade_mode = str(execution_trade_mode).lower()
        if trade_mode not in {"isolated", "cross"}:
            self._emit_debug(
                f"Invalid trade_mode '{execution_trade_mode}' provided; forcing isolated"
            )
            trade_mode = "isolated"
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

        # For isolated contracts (e.g. RESOLV-USDT-SWAP), OKX only accepts the
        # actual quote currency (USDT) as margin — it cannot borrow from or
        # convert other assets.  available_eq_usd is the USD-equivalent of ALL
        # holdings (RESOLV tokens, BTC, etc.) and will overstate tradeable
        # capacity, producing a notional that triggers code-51008.
        # Cap account_equity and available_margin_usd to the real quote-currency
        # balance so that every downstream sizing step stays within what OKX
        # will actually accept.

        # --- diagnostic dump so we can see exactly what OKX returned ---
        try:
            _diag = {
                "isolated_mode": isolated_mode,
                "quote_currency": quote_currency,
                "account_equity_before_cap": account_equity,
                "available_margin_usd_before_cap": available_margin_usd,
                "available_balances_keys": (
                    list(available_balances_block.keys())
                    if isinstance(available_balances_block, dict)
                    else None
                ),
                "usdt_balance_entry": (
                    available_balances_block.get(quote_currency)
                    if isinstance(available_balances_block, dict) and quote_currency
                    else None
                ),
            }
            self._emit_debug(
                f"[51008-diag] {symbol} pre-cap balance state: {json.dumps(_diag, default=str)}"
            )
        except Exception:
            pass
        # -----------------------------------------------------------------

        if isolated_mode and quote_currency and isinstance(available_balances_block, dict):
            _iso_quote_meta = available_balances_block.get(quote_currency)
            if isinstance(_iso_quote_meta, dict):
                _iso_avail = self._extract_float(
                    _iso_quote_meta.get("available_usd")
                    or _iso_quote_meta.get("equity_usd")
                )
                if _iso_avail is None and quote_currency in STABLE_CURRENCIES:
                    _iso_avail = self._extract_float(_iso_quote_meta.get("available"))
                _iso_cash = self._extract_float(_iso_quote_meta.get("cash"))
                if _iso_cash is not None and quote_currency in STABLE_CURRENCIES:
                    _iso_avail = (
                        max(_iso_avail, _iso_cash) if _iso_avail is not None else _iso_cash
                    )
                if _iso_avail is not None and _iso_avail > 0:
                    if account_equity is None or _iso_avail < account_equity:
                        self._emit_debug(
                            f"{symbol} account_equity capped {account_equity:.4f} → "
                            f"{_iso_avail:.4f} (actual {quote_currency} balance; "
                            "prevents over-sizing against non-USDT assets)"
                        )
                        account_equity = _iso_avail
                    if available_margin_usd is None or _iso_avail < available_margin_usd:
                        available_margin_usd = _iso_avail
                elif quote_currency in STABLE_CURRENCIES:
                    # Quote currency key exists but balance is 0 (or negative).
                    # Force available_margin_usd to 0 so the downstream
                    # "insufficient available margin" guard blocks this trade
                    # cleanly instead of reaching OKX and getting a 51008.
                    self._emit_debug(
                        f"{symbol} {quote_currency} balance is zero or absent; "
                        "forcing available_margin_usd=0 to block trade"
                    )
                    available_margin_usd = 0.0
            else:
                # No quote-currency entry at all in the balances block.
                if quote_currency in STABLE_CURRENCIES:
                    self._emit_debug(
                        f"{symbol} no {quote_currency} entry in available_balances; "
                        "forcing available_margin_usd=0 to block trade"
                    )
                    available_margin_usd = 0.0

        # Reduce effective budget by notionals already reserved for other symbols
        # that are concurrently in-flight.  This prevents two simultaneous orders
        # from each sizing to 50 % of the same USDT pool and together consuming
        # 100 % of available balance (which OKX rejects with code 51008).
        _other_pending = sum(
            v for k, v in self._pending_notional.items() if k != symbol
        )
        if _other_pending > 0:
            _prev_equity = account_equity
            _prev_margin = available_margin_usd
            if account_equity is not None:
                account_equity = max(0.0, account_equity - _other_pending)
            if available_margin_usd is not None:
                available_margin_usd = max(0.0, available_margin_usd - _other_pending)
            self._emit_debug(
                f"{symbol} budget reduced by {_other_pending:.2f} USDT "
                f"(other in-flight orders): equity {_prev_equity:.2f} "
                f"→ {account_equity:.2f}, margin {_prev_margin:.2f} "
                f"→ {available_margin_usd:.2f}"
            )

        equity_based_cap = None
        if (
            account_equity is not None
            and account_equity > 0
            and effective_max_pct
        ):
            equity_based_cap = max(0.0, account_equity * effective_max_pct)
            guardrail_notional_cap = equity_based_cap

        # ── ATR risk-per-trade cap ──────────────────────────────────────────
        # Implements the 1% risk model from master_technical.md:
        #   max_notional = (equity × risk_pct%) / (ATR_stop / entry_price)
        # This caps the position notional so that a full stop-out never costs
        # more than risk_pct% of equity, regardless of what the LLM requested.
        # Only fires when guardrails.atr_risk_per_trade_pct is configured (>0).
        _atr_risk_pct_cfg = self._extract_float(guardrails.get("atr_risk_per_trade_pct"))
        if (
            _atr_risk_pct_cfg
            and _atr_risk_pct_cfg > 0
            and account_equity is not None
            and account_equity > 0
            and last_price
            and last_price > 0
        ):
            _snap_rm = self._last_full_snapshot
            _risk_metrics = (
                ((_snap_rm.get("market_data") or {}).get(symbol) or {}).get("risk_metrics") or {}
                if _snap_rm
                else {}
            )
            _suggested_stop = self._extract_float(_risk_metrics.get("suggested_stop"))
            if _suggested_stop and _suggested_stop > 0:
                _stop_fraction = _suggested_stop / last_price
                _atr_max_notional = round(
                    (account_equity * _atr_risk_pct_cfg / 100.0) / _stop_fraction, 2
                )
                _prev_cap = guardrail_notional_cap
                if guardrail_notional_cap is None or _atr_max_notional < guardrail_notional_cap:
                    guardrail_notional_cap = _atr_max_notional
                    self._emit_debug(
                        f"{symbol} ATR risk cap: equity={account_equity:.2f} "
                        f"× risk={_atr_risk_pct_cfg}% / stop_dist={_stop_fraction:.4f} "
                        f"= {_atr_max_notional:.2f}"
                        + (
                            f" (tighter than equity cap {_prev_cap:.2f})"
                            if _prev_cap and _atr_max_notional < _prev_cap
                            else ""
                        )
                    )
                else:
                    self._emit_debug(
                        f"{symbol} ATR risk cap {_atr_max_notional:.2f} "
                        f"is looser than equity cap {_prev_cap:.2f}; equity cap kept"
                    )
            else:
                self._emit_debug(
                    f"{symbol} ATR risk cap: no ATR data available (suggested_stop missing); skipping"
                )

        confidence_value = self._normalize_confidence(decision.get("confidence"))
        equity_pct = self._extract_float(decision.get("equity_pct"))
        llm_notional_usd = self._extract_float(decision.get("notional_usd"))
        # Legacy field: position_size was historically in base-token units.
        # Kept for backward compatibility; new LLM output prefers notional_usd.
        explicit_size_hint = self._extract_float(decision.get("position_size"))
        raw_size: float = 0.0

        # -----------------------------------------------------------------------
        # Execution-layer sizing formula.
        # Computes notional deterministically from confidence and risk_score
        # rather than trusting the LLM to perform the arithmetic:
        #   notional = max_safe × confidence × (1 − risk_score)
        # where max_safe = available_margin × max_leverage, capped by any
        # active guardrail_notional_cap.
        #
        # Only fires when the decision contains NO legacy sizing fields
        # (position_size, equity_pct).  Decisions that include those fields
        # continue to use the legacy _compute_leverage_adjusted_size path so
        # that backward compatibility is fully preserved.
        # -----------------------------------------------------------------------
        risk_score_value = max(
            0.0, min(1.0, self._extract_float(decision.get("risk_score")) or 0.0)
        )
        llm_notional_mode = (
            (guardrails.get("llm_notional_mode") or "post_leverage").lower()
            if isinstance(guardrails, dict)
            else "post_leverage"
        )
        if (
            explicit_size_hint is None
            and (equity_pct is None or equity_pct <= 0)
            and confidence_value > 0
            and available_margin_usd is not None
            and available_margin_usd > 0
            and max_leverage > 0
            and llm_notional_mode != "pre_leverage"  # pre-leverage semantics differ
        ):
            _max_safe = available_margin_usd * max_leverage
            if guardrail_notional_cap and guardrail_notional_cap > 0:
                _max_safe = min(_max_safe, guardrail_notional_cap)
            _computed = round(_max_safe * confidence_value * (1.0 - risk_score_value), 2)
            # Apply min_leverage floor: when confidence is at/above the gate the
            # position must be sized to at least available_margin × min_leverage.
            if min_leverage > 0 and confidence_value >= confidence_gate:
                _min_notional = available_margin_usd * min_leverage
                if guardrail_notional_cap and guardrail_notional_cap > 0:
                    _min_notional = min(_min_notional, guardrail_notional_cap)
                if _min_notional > 0 and _computed < _min_notional:
                    self._emit_debug(
                        f"{symbol} notional floored by min_leverage={min_leverage:.2f}: "
                        f"{_computed:.2f} → {round(_min_notional, 2):.2f}"
                    )
                    _computed = round(_min_notional, 2)
            if _computed > 0:
                self._emit_debug(
                    f"{symbol} notional (exec_layer): "
                    f"max_safe={_max_safe:.2f} × conf={confidence_value:.2f} "
                    f"× (1−risk={risk_score_value:.2f}) = {_computed:.2f}"
                )
                llm_notional_usd = _computed
        if llm_notional_usd and llm_notional_usd > 0 and last_price and last_price > 0:
            # LLM expressed position size as a dollar amount — convert directly to
            # base-token units and skip the leverage-scaling path entirely.  All
            # downstream guardrail caps still apply via raw_size × last_price.
            #
            # llm_notional_mode controls the semantic of that dollar amount:
            #   post_leverage (default) — notional_usd IS the position notional
            #     raw_size = notional_usd / price
            #   pre_leverage — notional_usd is the MARGIN to commit;
            #     the bot multiplies by max_leverage to get position notional
            #     raw_size = notional_usd × max_leverage / price
            if llm_notional_mode == "pre_leverage":
                effective_leverage = max(max_leverage, 1.0) if max_leverage and max_leverage > 0 else 1.0
                raw_size = (llm_notional_usd * effective_leverage) / last_price
                self._emit_debug(
                    f"{symbol} sizing from LLM margin notional_usd={llm_notional_usd:.2f} "
                    f"(pre-leverage ×{effective_leverage:.1f}) "
                    f"→ raw_size={raw_size:.6f} base-tokens"
                )
            else:
                raw_size = llm_notional_usd / last_price
                self._emit_debug(
                    f"{symbol} sizing from LLM notional_usd={llm_notional_usd:.2f} "
                    f"→ raw_size={raw_size:.6f} base-tokens"
                )
        else:
            size_hint = explicit_size_hint
            equity_pct_size_hint = None
            if (
                equity_pct is not None
                and equity_pct > 0
                and equity_pct <= 1
                and account_equity is not None
                and account_equity > 0
                and last_price
                and last_price > 0
            ):
                target_notional = account_equity * equity_pct
                equity_pct_size_hint = target_notional / last_price if last_price else None
            if (
                explicit_size_hint
                and explicit_size_hint > 0
                and equity_pct_size_hint
                and equity_pct_size_hint > 0
            ):
                larger = max(explicit_size_hint, equity_pct_size_hint)
                smaller = min(explicit_size_hint, equity_pct_size_hint)
                if smaller > 0:
                    ratio = larger / smaller
                    if ratio >= 2.0:
                        self._record_execution_feedback(
                            symbol,
                            "LLM equity_pct disagrees with position_size",
                            level="warning",
                            meta={
                                "position_size": explicit_size_hint,
                                "equity_pct_size": equity_pct_size_hint,
                                "equity_pct": equity_pct,
                                "account_equity": account_equity,
                                "last_price": last_price,
                                "ratio": ratio,
                            },
                        )
            if (size_hint is None or size_hint <= 0) and equity_pct_size_hint and equity_pct_size_hint > 0:
                size_hint = equity_pct_size_hint
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
            self._record_execution_feedback(
                symbol,
                "Blocked: could not compute a valid position size (check notional_usd, equity, and leverage config)",
                level="warning",
                meta={"action": action, "last_price": last_price, "account_equity": account_equity},
            )
            return False
        target_leverage: float | None = None

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
        isolated_margin_available: float | None = None
        isolated_margin_entry: dict[str, Any] | None = None
        has_isolated_wallet: bool = False
        _bootstrap_min_notional: float | None = None  # set in bootstrap path, threaded to _submit_order
        _bootstrap_order_chunks: int = 1  # >1 when per-order cap requires multiple orders to seed isolated wallet
        if isolated_mode:
            wallet_side_key = desired_pos_side if dual_side_mode else None
            if wallet_side_key == "net":
                wallet_side_key = None
            margin_value, isolated_margin_entry = self._isolated_position_margin(
                positions,
                symbol,
                wallet_side_key,
            )
            has_isolated_wallet = isolated_margin_entry is not None
            if has_isolated_wallet:
                tier_entries: list[dict[str, Any]] | None = None
                tier_imr_for_check: float | None = None
                tier_max_notional_for_check: float | None = None
                if margin_value is not None:
                    isolated_margin_available = max(margin_value, 0.0)
                else:
                    isolated_margin_available = 0.0
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
                        try:
                            positions = await self._fetch_positions()
                        except Exception as exc:  # pragma: no cover - network fallback
                            self._emit_debug(f"Position refresh failed for {symbol}: {exc}")
                        else:
                            margin_value, isolated_margin_entry = self._isolated_position_margin(
                                positions,
                                symbol,
                                wallet_side_key,
                            )
                            if margin_value is not None:
                                isolated_margin_available = max(margin_value, 0.0)
                            else:
                                isolated_margin_available = 0.0
                        if (
                            (isolated_margin_available is None or isolated_margin_available <= 0)
                            and required_isolated_margin
                        ):
                            isolated_margin_available = max(required_isolated_margin, 0.0)
                self._merge_margin_guidance(
                    symbol,
                    {
                        "isolated_margin_balance": isolated_margin_available,
                        "isolated_pos_side": wallet_side_key,
                    },
                )
                if isolated_margin_available is not None and isolated_margin_available > 0:
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
                self._merge_margin_guidance(
                    symbol,
                    {
                        "isolated_margin_balance": None,
                        "isolated_pos_side": wallet_side_key,
                        "isolated_wallet_status": "missing",
                    },
                )
                if symbol not in self._missing_isolated_wallet_symbols:
                    self._missing_isolated_wallet_symbols.add(symbol)
                    self._record_execution_feedback(
                        symbol,
                        "Isolated wallet missing; falling back to quote margin",
                        level="info",
                        meta={
                            "trade_mode": trade_mode,
                            "quote_currency": quote_currency,
                        },
                    )
                    self._emit_debug(
                        f"No isolated wallet entry for {symbol}; using quote margin fallback until first trade"
                    )
                fallback_margin = None
                if quote_margin_candidates:
                    fallback_margin = max(quote_margin_candidates)
                if fallback_margin is None:
                    fallback_margin = available_margin_usd or account_equity

                # --- bootstrap diagnostic ---
                try:
                    self._emit_debug(
                        f"[51008-diag] {symbol} bootstrap path: "
                        f"quote_margin_candidates={quote_margin_candidates} "
                        f"fallback_margin={fallback_margin} "
                        f"account_equity={account_equity} "
                        f"available_margin_usd={available_margin_usd} "
                        f"quote_currency={quote_currency}"
                    )
                except Exception:
                    pass

                # If we're in isolated mode with a USDT-margined contract but have
                # NO usable quote-currency balance at all, block the trade immediately
                # rather than letting OKX return 51008.
                if not quote_margin_candidates and quote_currency:
                    self._emit_debug(
                        f"Execution skipped for {symbol}: no free {quote_currency} balance "
                        "available to fund isolated margin (account equity is non-USDT assets)"
                    )
                    self._record_execution_feedback(
                        symbol,
                        f"No free {quote_currency} balance; cannot fund isolated margin",
                        level="warning",
                        meta={
                            "trade_mode": trade_mode,
                            "quote_currency": quote_currency,
                            "account_equity": account_equity,
                            "available_margin_usd": available_margin_usd,
                        },
                    )
                    return False

                wallet_cap = self._resolve_isolated_seed_limit(
                    guardrails,
                    symbol,
                    account_equity=account_equity,
                )
                bootstrap_pct = None
                if isinstance(guardrails, dict):
                    bootstrap_pct = self._extract_float(guardrails.get("isolated_wallet_bootstrap_pct"))
                if bootstrap_pct is None or bootstrap_pct <= 0:
                    bootstrap_pct = self.ISOLATED_WALLET_BOOTSTRAP_PCT
                bootstrap_pct = min(max(bootstrap_pct, 0.0), 1.0)
                if (wallet_cap is None or wallet_cap <= 0) and bootstrap_pct > 0:
                    baseline = fallback_margin or account_equity
                    if baseline and baseline > 0:
                        wallet_cap = baseline * bootstrap_pct
                # Cap wallet_cap by the actual available quote-currency balance
                # (e.g., real USDT, not just USD-equivalent of all assets).
                # Without this, the system sizes the order against the configured
                # seed limit while OKX has less of the specific margin currency
                # available, causing code-51008 "insufficient USDT balance".
                if wallet_cap and wallet_cap > 0 and quote_margin_candidates:
                    actual_quote_balance = max(quote_margin_candidates)
                    if actual_quote_balance > 0 and actual_quote_balance < wallet_cap:
                        self._emit_debug(
                            f"{symbol} isolated seed cap reduced from {wallet_cap:.4f} to "
                            f"{actual_quote_balance:.4f} by actual {quote_currency or 'quote'} balance"
                        )
                        self._record_execution_feedback(
                            symbol,
                            "Seed cap reduced to available quote balance",
                            level="info",
                            meta={
                                "configured_wallet_cap": wallet_cap,
                                "actual_quote_balance": actual_quote_balance,
                                "quote_currency": quote_currency,
                            },
                        )
                        wallet_cap = actual_quote_balance
                # Apply bootstrap_pct as a hard upper bound on wallet_cap even when
                # a larger isolated_margin_seed_usd / isolated_margin_seed_pct is
                # configured.  OKX market orders for new isolated positions require
                # the full initial margin at WORST-CASE fill price, which for illiquid
                # small-cap pairs can be substantially above the mark price.  Sizing
                # to 50 % of equity consistently triggers 51008 in practice; keeping
                # bootstrap orders at ≤ bootstrap_pct of the free quote balance gives
                # OKX the headroom it needs for fees, slippage, and its internal
                # margin calculations.  Users who want larger bootstraps should raise
                # isolated_wallet_bootstrap_pct in the guardrails config.
                if bootstrap_pct > 0 and quote_margin_candidates:
                    actual_quote_balance = max(quote_margin_candidates)
                    if actual_quote_balance > 0:
                        bootstrap_budget = actual_quote_balance * bootstrap_pct
                        if bootstrap_budget > 0 and (wallet_cap is None or wallet_cap > bootstrap_budget):
                            _prev_cap_str = f"{wallet_cap:.4f}" if wallet_cap is not None else "None"
                            self._emit_debug(
                                f"{symbol} bootstrap notional cap: "
                                f"{_prev_cap_str} → {bootstrap_budget:.4f} "
                                f"(bootstrap_pct={bootstrap_pct:.2f} × "
                                f"quote_balance={actual_quote_balance:.4f})"
                            )
                            self._record_execution_feedback(
                                symbol,
                                "Bootstrap notional capped by bootstrap_pct guardrail",
                                level="info",
                                meta={
                                    "previous_wallet_cap": wallet_cap,
                                    "bootstrap_budget": bootstrap_budget,
                                    "bootstrap_pct": bootstrap_pct,
                                    "actual_quote_balance": actual_quote_balance,
                                },
                            )
                            wallet_cap = bootstrap_budget
                if wallet_cap and wallet_cap > 0:
                    guardrail_notional_cap = wallet_cap if guardrail_notional_cap is None else min(guardrail_notional_cap, wallet_cap)
                    # Bootstrap mode inherently limits notional to a fraction of equity
                    # (bootstrap_pct × quote_balance), making it structurally impossible
                    # to reach the min_leverage threshold.  Set the override here so the
                    # leverage guardrail proceeds rather than blocking the first seed order.
                    if not leverage_override_reason:
                        leverage_override_reason = "isolated-wallet-bootstrap"
                    # Do NOT cap available_margin_usd by wallet_cap / leverage here.
                    # That produces a nonsense budget (e.g. 29.50 / 10 = 2.95) which
                    # does not reflect the real usable USDT balance and confuses
                    # the pre-submit margin guidance snapshot without preventing 51008.
                    # The notional cap (guardrail_notional_cap = wallet_cap) is the
                    # correct place to limit the trade size in the bootstrap path.
                    if last_price and last_price > 0:
                        fallback_contract_cap = wallet_cap / last_price
                        if fallback_contract_cap > 0 and raw_size > fallback_contract_cap:
                            previous_size = raw_size
                            raw_size = fallback_contract_cap
                            clip_meta = {
                                "previous_size": previous_size,
                                "target_size": fallback_contract_cap,
                                "price_reference": last_price,
                                "fallback_cap_usd": wallet_cap,
                            }
                            self._record_execution_feedback(
                                symbol,
                                "Size clipped while isolated wallet missing",
                                level="info",
                                meta=clip_meta,
                            )
                            self._emit_debug(
                                f"{symbol} size clipped to {fallback_contract_cap:.4f} while waiting for isolated wallet"
                            )
                            leverage_override_reason = "isolated-wallet-bootstrap"
                # Guard: OKX requires a minimum notional to seed a brand-new
                # isolated sub-account.  We compute this floor dynamically from
                # the position-tiers API (tier-1 IMR × min contracts × price) so
                # illiquid instruments with high margin requirements are handled
                # correctly.  The static OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD is
                # used only as a safety fallback when tier data is unavailable.
                if last_price and last_price > 0:
                    # --- dynamic minimum-seed calculation from position tiers ---
                    _dyn_min_notional: float | None = None
                    _dyn_min_contracts: float | None = None
                    try:
                        _seed_tiers = await self._get_position_tiers(symbol, trade_mode)
                        if _seed_tiers:
                            _t1 = _seed_tiers[0]  # tier-1 = smallest position bracket
                            _t1_imr = self._extract_float(_t1.get("imr"))
                            _t1_max_lever = self._extract_float(_t1.get("maxLever"))
                            _t1_min_sz = self._extract_float(_t1.get("minSz"))
                            # Derive IMR from maxLever if not directly available
                            if (_t1_imr is None or _t1_imr <= 0) and _t1_max_lever and _t1_max_lever > 0:
                                _t1_imr = 1.0 / _t1_max_lever
                            # min_seed_notional = contracts × price such that the
                            # margin required (notional × IMR) ≥ 1 USDT minimum.
                            # Equivalently: min_contracts = ceil(1 / (price × IMR))
                            # but at least minSz from the tier definition.
                            if _t1_imr and _t1_imr > 0 and _t1_min_sz and _t1_min_sz > 0:
                                # The seed order must provide enough margin for tier-1:
                                # required_margin = notional × imr  ≥  1 USDT
                                # ⟹  min_notional = 1 / imr
                                # But at a minimum we also need minSz contracts to be
                                # a valid order, so take the larger of the two floors.
                                _floor_from_imr = 1.0 / _t1_imr
                                # _t1_min_sz is in CONTRACTS; multiply by ct_val to get
                                # base-token units and then by price for USD notional.
                                _floor_from_min_sz = _t1_min_sz * ct_val * last_price
                                _dyn_min_notional = max(_floor_from_imr, _floor_from_min_sz)
                                # Add 10 % buffer for fees, slippage, OKX internal checks
                                _dyn_min_notional *= 1.10
                                _dyn_min_contracts = _t1_min_sz
                                self._emit_debug(
                                    f"[bootstrap] {symbol} OKX dynamic minimum seed: "
                                    f"tier-1 imr={_t1_imr:.4f} maxLever={_t1_max_lever} "
                                    f"minSz={_t1_min_sz} → min_notional={_dyn_min_notional:.4f} USDT"
                                )
                    except Exception as _tier_exc:
                        self._emit_debug(f"[bootstrap] tier fetch for min-seed failed: {_tier_exc}")

                    _effective_min_notional = _dyn_min_notional or self.OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD
                    _bootstrap_min_notional = _effective_min_notional

                    # If our wallet_cap is already below the minimum seed
                    # notional, we cannot bootstrap at all — block cleanly.
                    if wallet_cap and wallet_cap > 0 and wallet_cap < _effective_min_notional:
                        self._emit_debug(
                            f"Execution skipped for {symbol}: wallet_cap {wallet_cap:.4f} USDT "
                            f"is below OKX minimum seed notional {_effective_min_notional:.4f} USDT; "
                            "adding to bootstrap blocklist"
                        )
                        self._bootstrap_blocked[symbol] = time.time()
                        self._record_execution_feedback(
                            symbol,
                            f"Cannot seed isolated position: available USDT "
                            f"({wallet_cap:.2f}) is below OKX minimum "
                            f"({_effective_min_notional:.2f} USDT). "
                            "Add USDT to the account or raise isolated_wallet_bootstrap_pct. "
                            f"Bot will retry in {self.BOOTSTRAP_BLOCK_SECONDS // 60} minutes.",
                            level="warning",
                            meta={
                                "wallet_cap": wallet_cap,
                                "min_seed_notional_usd": _effective_min_notional,
                                "source": "position-tiers" if _dyn_min_notional else "static-fallback",
                                "isolated_wallet_status": "missing",
                            },
                        )
                        return False

                    # Ensure raw_size (in base tokens) is at least the minimum
                    # contracts required by the tier.  _dyn_min_contracts is in
                    # OKX contract units so multiply by ct_val to convert to
                    # base-token units for comparison with raw_size.
                    if _dyn_min_contracts and _dyn_min_contracts > 0:
                        if raw_size is not None and raw_size < _dyn_min_contracts * ct_val:
                            self._emit_debug(
                                f"{symbol} bootstrap: raw_size {raw_size:.4f} base-tokens "
                                f"< tier minSz {_dyn_min_contracts:.4f} contracts "
                                f"(≡ {_dyn_min_contracts * ct_val:.4f} base-tokens); "
                                f"raising to meet minimum"
                            )
                            raw_size = _dyn_min_contracts * ct_val

                    _bootstrap_notional = raw_size * last_price
                    if _bootstrap_notional < _effective_min_notional:
                        self._emit_debug(
                            f"Execution skipped for {symbol}: notional "
                            f"{_bootstrap_notional:.4f} USDT is below OKX isolated "
                            f"minimum {_effective_min_notional:.2f} USDT (no existing wallet)"
                        )
                        self._bootstrap_blocked[symbol] = time.time()
                        self._record_execution_feedback(
                            symbol,
                            f"Trade notional {_bootstrap_notional:.2f} USDT is too small "
                            f"to seed a new isolated position "
                            f"(OKX minimum \u2248 {_effective_min_notional:.2f} USDT). "
                            f"Bot will retry in {self.BOOTSTRAP_BLOCK_SECONDS // 60} minutes.",
                            level="warning",
                            meta={
                                "bootstrap_notional": _bootstrap_notional,
                                "min_notional_usd": _effective_min_notional,
                                "source": "position-tiers" if _dyn_min_notional else "static-fallback",
                                "raw_size": raw_size,
                                "last_price": last_price,
                                "isolated_wallet_status": "missing",
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

        if max_leverage and max_leverage > 0 and available_margin_usd is not None:
            margin_driven_cap = available_margin_usd * max_leverage
            pct_multiplier = effective_max_pct if effective_max_pct and effective_max_pct > 0 else None
            if pct_multiplier:
                margin_driven_cap *= pct_multiplier
            if margin_driven_cap and margin_driven_cap > 0:
                if guardrail_notional_cap is None or margin_driven_cap < guardrail_notional_cap:
                    guardrail_notional_cap = margin_driven_cap
        take_profit_price = self._normalize_take_profit(
            action,
            self._extract_float(decision.get("take_profit")),
            last_price,
            symbol=symbol,
        )
        # Preserve the raw LLM value (may have been a wrong-direction TP that
        # normalization dropped).  Used later to snap at require_protection.
        _llm_raw_take_profit = self._extract_float(decision.get("take_profit"))
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
        take_profit_ratio = self._calculate_target_ratio(
            action,
            last_price,
            take_profit_price,
            "take-profit",
        )
        stop_loss_ratio = self._calculate_target_ratio(
            action,
            last_price,
            stop_loss_price,
            "stop-loss",
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

        # When the Alternator strategy is active, exits are managed by reversals.
        # Strip TP/SL from the initial entry so the order goes through cleanly
        # and the guardrails below (stop_loss_required, require_protection) don't
        # block a perfectly valid Alternator entry that has no protective levels.
        _alternator_entry = not reduce_only and bool(
            (self._strategy_config.get("alternator") or {}).get("enabled")
        )
        if _alternator_entry:
            if take_profit_price or stop_loss_price:
                self._emit_debug(
                    f"{symbol} Alternator enabled: stripping TP ({take_profit_price}) "
                    f"and SL ({stop_loss_price}) from initial entry order"
                )
            take_profit_price = None
            stop_loss_price = None
            requested_take_profit = None
            requested_stop_loss = None
            take_profit_ratio = None
            stop_loss_ratio = None

        if require_protection and not reduce_only and not _alternator_entry and (stop_loss_price is None or not isinstance(stop_loss_price, (int, float)) or stop_loss_price <= 0):
            self._record_execution_feedback(
                symbol,
                "Blocked: stop-loss required",
                level="warning",
                meta={
                    "guardrail": "stop_loss_required",
                    "action": action,
                    "requested_stop_loss": stop_loss_price,
                },
            )
            self._emit_debug(f"Execution skipped for {symbol}: stop-loss required for entries")
            return False

        if require_protection and not reduce_only and not _alternator_entry and (
            take_profit_price is None
            or not isinstance(take_profit_price, (int, float))
            or take_profit_price <= 0
        ):
            self._record_execution_feedback(
                symbol,
                "Blocked: take-profit required",
                level="warning",
                meta={
                    "guardrail": "require_protection",
                    "action": action,
                    "requested_take_profit": take_profit_price,
                },
            )
            self._emit_debug(f"Execution skipped for {symbol}: take-profit required for entries")
            return False

        # Reward-to-risk guard: take-profit distance must be >= min_reward_risk_ratio * stop-loss distance.
        # This prevents trades where the potential loss greatly outweighs the potential gain.
        if (
            guardrails.get("require_reward_risk_ratio", True)
            and not reduce_only
            and take_profit_price
            and isinstance(take_profit_price, (int, float))
            and take_profit_price > 0
            and last_price
            and last_price > 0
            and stop_loss_price
            and isinstance(stop_loss_price, (int, float))
            and stop_loss_price > 0
        ):
            min_rr = self._extract_float(guardrails.get("min_reward_risk_ratio")) or 1.0
            if action == "BUY":
                tp_dist = take_profit_price - last_price
                sl_dist = last_price - stop_loss_price
            else:  # SELL
                tp_dist = last_price - take_profit_price
                sl_dist = stop_loss_price - last_price
            if sl_dist > 0 and tp_dist <= 0:
                # TP is on the wrong side of entry — emit a directional message
                # rather than a confusing "R:R below minimum" one.
                self._record_execution_feedback(
                    symbol,
                    f"Blocked: take-profit {take_profit_price:.6f} is on the wrong side of entry {last_price:.6f} for {action}",
                    level="warning",
                    meta={
                        "guardrail": "min_reward_risk_ratio",
                        "action": action,
                        "last_price": last_price,
                        "take_profit_price": take_profit_price,
                        "stop_loss_price": stop_loss_price,
                    },
                )
                self._emit_debug(
                    f"Execution skipped for {symbol}: TP {take_profit_price} wrong side of entry {last_price} for {action}"
                )
                return False
            if sl_dist > 0 and tp_dist / sl_dist < min_rr:
                rr_actual = tp_dist / sl_dist
                self._record_execution_feedback(
                    symbol,
                    f"Blocked: reward-to-risk ratio {rr_actual:.2f} below minimum {min_rr:.2f}",
                    level="warning",
                    meta={
                        "guardrail": "min_reward_risk_ratio",
                        "action": action,
                        "last_price": last_price,
                        "take_profit_price": take_profit_price,
                        "stop_loss_price": stop_loss_price,
                        "tp_dist": tp_dist,
                        "sl_dist": sl_dist,
                        "rr_ratio": rr_actual,
                        "min_reward_risk_ratio": min_rr,
                    },
                )
                self._emit_debug(
                    f"Execution skipped for {symbol}: reward-to-risk {rr_actual:.2f} < {min_rr:.2f} "
                    f"(TP {take_profit_price}, SL {stop_loss_price}, entry ~{last_price})"
                )
                return False

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
                        "equity_pct": equity_pct,
                    },
                )

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
            # Keep the real (un-leveraged) equity for display / feedback so the guidance
            # snapshot never shows a value larger than account_equity.
            real_free_equity = free_equity
            if (
                margin_headroom is not None
                and margin_headroom > 0
                and max_leverage
                and max_leverage > 0
            ):
                margin_based_notional = margin_headroom * max_leverage
                if margin_based_notional > (free_equity or 0.0):
                    # Override free_equity with the leveraged notional so size-clipping
                    # below allows trades that require leverage beyond raw equity.
                    free_equity = margin_based_notional
            equity_updates = {
                "account_equity": account_equity,
                "open_position_notional": open_position_notional,
                # Store real equity so the guidance display is never inflated to
                # a leveraged notional (which would be > account_equity).
                "equity_available_for_trade": real_free_equity,
                # Store the leveraged capacity under a separate, clearly-named key.
                "max_tradeable_notional_usd": free_equity,
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
            )
            tier_cap_limit = tier_result.get("tier_max_notional_usd")
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
                    # If the clipped size would fall below the min_leverage
                    # threshold, proceed anyway — the tier limit is an OKX
                    # constraint, not a dust-trade signal.  Matches the
                    # existing override for position-cap clipping.
                    if (
                        account_equity
                        and account_equity > 0
                        and last_price
                        and last_price > 0
                        and min_leverage
                        and min_leverage > 0
                    ):
                        clipped_leverage = (adjusted_size * last_price) / account_equity
                        if clipped_leverage < min_leverage:
                            leverage_override_reason = "OKX tier margin limit"
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

        spec = self._instrument_specs.get((symbol or "").upper()) or {}
        per_order_limit = None
        if order_type == "market":
            per_order_limit = spec.get("max_market_size") or spec.get("max_limit_size")
        else:
            per_order_limit = spec.get("max_limit_size") or spec.get("max_market_size")
        if per_order_limit and per_order_limit > 0 and (raw_size / ct_val) > per_order_limit:
            previous_size = raw_size
            # per_order_limit is in OKX contracts; convert back to base-token units
            # so raw_size stays in base-token units throughout this function.
            raw_size = per_order_limit * ct_val
            self._emit_debug(
                f"{symbol} size clipped to {per_order_limit:.6f} contracts "
                f"({raw_size:.4f} base-tokens) by OKX per-order limit"
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
            # Re-check bootstrap minimum after per-order clip: for micro-priced
            # tokens (e.g. BONK at 6.8e-6 USDT), OKX's max order size may cap
            # the notional of a single order well below the isolated-wallet seed
            # floor.  Instead of blocking, we split the bootstrap into multiple
            # sequential max-size orders until the cumulative notional reaches
            # the floor — after the first order succeeds the wallet exists, so
            # subsequent orders are regular top-ups.
            if (
                isolated_mode
                and not has_isolated_wallet
                and last_price
                and last_price > 0
            ):
                _post_clip_notional = raw_size * last_price
                _boot_floor = _bootstrap_min_notional or self.OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD
                if _post_clip_notional > 0 and _post_clip_notional < _boot_floor:
                    _bootstrap_order_chunks = min(10, math.ceil(_boot_floor / _post_clip_notional))
                    self._emit_debug(
                        f"{symbol} bootstrap: single order notional {_post_clip_notional:.4f} USDT "
                        f"< floor {_boot_floor:.2f} USDT; "
                        f"will submit {_bootstrap_order_chunks} × {per_order_limit:.0f}-contract "
                        f"orders to seed isolated wallet"
                    )
                    self._record_execution_feedback(
                        symbol,
                        f"Bootstrap multi-chunk: submitting {_bootstrap_order_chunks} sequential "
                        f"{per_order_limit:.0f}-contract orders "
                        f"({_post_clip_notional:.4f} USDT each) to reach seed floor "
                        f"({_boot_floor:.2f} USDT)",
                        level="info",
                        meta={
                            "chunk_notional": _post_clip_notional,
                            "chunks": _bootstrap_order_chunks,
                            "bootstrap_floor_usd": _boot_floor,
                            "per_order_limit_contracts": per_order_limit,
                        },
                    )

        if raw_size < min_size:
            self._emit_debug(
                f"Execution skipped for {symbol}: computed size {raw_size:.6f} below minimum {min_size}"
            )
            self._record_execution_feedback(
                symbol,
                f"Blocked: computed size {raw_size:.6f} is below instrument minimum {min_size} (too little capital or notional too small)",
                level="warning",
                meta={"raw_size": raw_size, "min_size": min_size, "action": action},
            )
            return False

        quantized_size = self._quantize_order_size(symbol, raw_size)
        if quantized_size is None or quantized_size <= 0:
            self._emit_debug(f"Execution skipped for {symbol}: size {raw_size:.6f} below lot size")
            self._record_execution_feedback(
                symbol,
                f"Blocked: size {raw_size:.6f} rounds to zero after lot-size quantization (too small to place)",
                level="warning",
                meta={"raw_size": raw_size, "action": action},
            )
            return False
        if quantized_size < min_size:
            self._emit_debug(
                f"Execution skipped for {symbol}: quantized size {quantized_size:.6f} below minimum {min_size}"
            )
            self._record_execution_feedback(
                symbol,
                f"Blocked: quantized size {quantized_size:.6f} is below instrument minimum {min_size}",
                level="warning",
                meta={"quantized_size": quantized_size, "min_size": min_size, "action": action},
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
                    # Proceed despite low achieved leverage, but still request
                    # min_leverage on OKX so the instrument is correctly configured
                    # (e.g., bootstrap trades must pre-set the leverage even if the
                    # seed notional is smaller than min_leverage × equity).
                    target_leverage = min_leverage
                    self._emit_debug(
                        f"{symbol} leverage {achieved_leverage:.2f}x below minimum {min_leverage:.2f}x but proceeding due to {leverage_override_reason}; setting OKX leverage to {min_leverage:.2f}x"
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
                            "leverage_override_reason": leverage_override_reason,
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
            if require_protection and attachments_stop_loss is None:
                self._record_execution_feedback(
                    symbol,
                    "Blocked: stop-loss required by guardrail",
                    level="warning",
                    meta={
                        "guardrail": "require_protection",
                        "take_profit_supplied": attachments_take_profit is not None,
                        "stop_loss_supplied": attachments_stop_loss is not None,
                    },
                )
                self._emit_debug(
                    f"Execution skipped for {symbol}: stop-loss required by guardrail"
                )
                return False
            if require_protection and attachments_take_profit is None:
                # If the LLM supplied a TP but got the direction wrong, snap it
                # to the nearest valid value rather than blocking the trade.
                # Only snap when LLM provided something; no TP at all → block.
                if _llm_raw_take_profit and _llm_raw_take_profit > 0 and last_price and last_price > 0:
                    _snapped_tp = self._snap_take_profit_to_valid(action, last_price, symbol)
                    if _snapped_tp:
                        attachments_take_profit = _snapped_tp
                        self._emit_debug(
                            f"{symbol} require_protection: snapped wrong-direction TP "
                            f"{_llm_raw_take_profit:.6f} → {_snapped_tp:.6f}"
                        )
                if attachments_take_profit is None:
                    self._record_execution_feedback(
                        symbol,
                        "Blocked: take-profit required by guardrail",
                        level="warning",
                        meta={
                            "guardrail": "require_protection",
                            "take_profit_supplied": False,
                            "stop_loss_supplied": attachments_stop_loss is not None,
                        },
                    )
                    self._emit_debug(
                        f"Execution skipped for {symbol}: take-profit required by guardrail"
                    )
                    return False
            if attachments_take_profit or attachments_stop_loss:
                await self._cancel_position_protection(symbol)
                if last_price and last_price > 0:
                    _adjust_tp_enabled = bool(guardrails.get("adjust_invalid_tp", False))
                    _adjust_tp_pct_okx = self._extract_float(guardrails.get("adjust_invalid_tp_pct")) or 0.10
                    _adjust_tp_lev = max(target_leverage or 1.0, 1.0)
                    _adjust_tp_pct = _adjust_tp_pct_okx / _adjust_tp_lev
                    attachments_take_profit = self._drop_conflicting_target(
                        symbol=symbol,
                        action=action,
                        target=attachments_take_profit,
                        reference_price=last_price,
                        kind="take-profit",
                        stage="pre-order attachment",
                        adjust_pct=_adjust_tp_pct if _adjust_tp_enabled else None,
                    )
                    attachments_stop_loss = self._drop_conflicting_target(
                        symbol=symbol,
                        action=action,
                        target=attachments_stop_loss,
                        reference_price=last_price,
                        kind="stop-loss",
                        stage="pre-order attachment",
                    )
                    if require_protection and attachments_stop_loss is None:
                        self._record_execution_feedback(
                            symbol,
                            "Blocked: stop-loss rejected by guardrail",
                            level="warning",
                            meta={
                                "guardrail": "require_protection",
                                "take_profit_supplied": attachments_take_profit is not None,
                                "stop_loss_supplied": False,
                                "reason": "dropped during validation",
                            },
                        )
                        self._emit_debug(
                            f"Execution skipped for {symbol}: stop-loss dropped during validation"
                        )
                        return False
                    if require_protection and attachments_take_profit is None:
                        self._record_execution_feedback(
                            symbol,
                            "Blocked: take-profit dropped by guardrail",
                            level="warning",
                            meta={
                                "guardrail": "require_protection",
                                "take_profit_supplied": False,
                                "stop_loss_supplied": attachments_stop_loss is not None,
                                "reason": "dropped during validation",
                            },
                        )
                        self._emit_debug(
                            f"Execution skipped for {symbol}: take-profit dropped during validation"
                        )
                        return False
                    if attachments_take_profit or attachments_stop_loss:
                        attach_algo_orders = self._build_attach_algo_orders(
                            take_profit_price=attachments_take_profit,
                            stop_loss_price=attachments_stop_loss,
                        )
                else:
                    if require_protection:
                        self._record_execution_feedback(
                            symbol,
                            "Blocked: cannot validate TP/SL without current price (require_protection enabled)",
                            level="warning",
                            meta={
                                "guardrail": "require_protection",
                                "action": action,
                                "reason": "last price unavailable for direction validation",
                            },
                        )
                        self._emit_debug(
                            f"Execution skipped for {symbol}: require_protection=True but last price is unavailable"
                        )
                        return False
                    self._emit_debug(
                        f"Skipping attached TP/SL for {symbol}: missing last price for validation"
                    )

        client_order_id = self._generate_client_order_id()
        try:
            _pre_order_diag = {
                "symbol": symbol,
                "side": side,
                "raw_size": raw_size,
                "last_price": last_price,
                "notional": round(raw_size * last_price, 4) if last_price else None,
                "trade_mode": trade_mode,
                "isolated_mode": isolated_mode,
                "available_margin_usd": available_margin_usd,
                "account_equity": account_equity,
                "min_leverage": min_leverage,
                "max_leverage": max_leverage,
                "target_leverage": target_leverage,
                "guardrail_notional_cap": guardrail_notional_cap,
                "quote_currency": quote_currency,
            }
            self._emit_debug(
                f"[51008-diag] {symbol} pre-submit state: {json.dumps(_pre_order_diag, default=str)}"
            )
        except Exception:
            pass
        # Reserve this order's notional so any concurrent symbol handler that
        # hasn't yet computed its equity cap will see the commitment and reduce
        # its own budget accordingly (avoids double-spending the USDT pool).
        # Convert raw_size (base-token units) to OKX contract units for submission.
        # raw_size = notional / last_price (base tokens); OKX sz = raw_size / ct_val (contracts).
        # Re-quantize at contract granularity so the sz field is a valid lot multiple.
        if ct_val > 1.0:
            _okx_sz = raw_size / ct_val
            _okx_sz_quantized = self._quantize_order_size(symbol, _okx_sz)
            if _okx_sz_quantized is not None and _okx_sz_quantized > 0:
                okx_sz: float = _okx_sz_quantized
            else:
                okx_sz = math.floor(_okx_sz) if _okx_sz >= 1 else _okx_sz
            if okx_sz <= 0:
                self._emit_debug(
                    f"Execution skipped for {symbol}: contract count {_okx_sz:.4f} rounded "
                    f"to zero after ctVal={ct_val} division (raw_size={raw_size:.4f})"
                )
                return False
            self._emit_debug(
                f"{symbol} ctVal={ct_val}: raw_size {raw_size:.4f} base-tokens "
                f"→ {okx_sz:.4f} OKX contracts "
                f"(notional ≈ {okx_sz * ct_val * last_price:.4f} USDT)"
            )
        else:
            # ct_val == 1.0: raw_size is already in contract units.
            # Re-quantize defensively to guard against any floating-point drift
            # introduced by chunking, equity-clip, or downsize logic above.
            _okx_sz_quantized = self._quantize_order_size(symbol, raw_size)
            okx_sz = _okx_sz_quantized if (_okx_sz_quantized is not None and _okx_sz_quantized > 0) else raw_size
        if not reduce_only and raw_size and last_price and last_price > 0:
            self._pending_notional[symbol] = raw_size * last_price * _bootstrap_order_chunks
        try:
            order: dict[str, Any] | None = None
            attachments_used: bool = False
            for _chunk_idx in range(_bootstrap_order_chunks):
                _chunk_order_id = (
                    client_order_id
                    if _chunk_idx == 0
                    else self._generate_client_order_id()
                )
                # Only the first chunk is a true bootstrap (no wallet yet);
                # subsequent chunks go in as normal isolated orders.
                _is_bootstrap_chunk = (isolated_mode and not has_isolated_wallet and _chunk_idx == 0)
                _chunk_order, _chunk_attachments = await self._submit_order(
                    symbol=symbol,
                    side=side,
                    pos_side=pos_side,
                    size=okx_sz,
                    trade_mode=trade_mode,
                    order_type=order_type,
                    reduce_only=reduce_only,
                    client_order_id=_chunk_order_id,
                    attach_algo_orders=attach_algo_orders if _chunk_idx == 0 else None,
                    margin_currency=quote_currency,
                    leverage=target_leverage,
                    dual_side_mode=dual_side_mode,
                    reference_price=ct_val * last_price,
                    isolated_bootstrap=_is_bootstrap_chunk,
                    bootstrap_min_notional=_bootstrap_min_notional,
                )
                if not _chunk_order:
                    if _chunk_idx == 0:
                        # First chunk failed — nothing opened, treat as full failure
                        order = None
                    else:
                        # Partial success: wallet seeded, but extra top-up chunk failed.
                        # Use the last successful order as the result.
                        self._emit_debug(
                            f"{symbol} bootstrap chunk {_chunk_idx + 1}/{_bootstrap_order_chunks} "
                            f"failed; using partial seed from {_chunk_idx} chunk(s)"
                        )
                    break
                order = _chunk_order
                attachments_used = _chunk_attachments
                if _bootstrap_order_chunks > 1:
                    _cid = _chunk_order.get("ordId") or _chunk_order_id
                    self._emit_debug(
                        f"{symbol} bootstrap chunk {_chunk_idx + 1}/{_bootstrap_order_chunks} "
                        f"placed ({_cid})"
                    )
        finally:
            self._pending_notional.pop(symbol, None)
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
                "bootstrap_chunks": _bootstrap_order_chunks if _bootstrap_order_chunks > 1 else None,
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
        reprice_reference = reference_for_protection or last_price
        if not reduce_only and reprice_reference and reprice_reference > 0:
            take_profit_price = self._reprice_target_from_ratio(
                symbol=symbol,
                action=action,
                kind="take-profit",
                reference_price=reprice_reference,
                existing_target=take_profit_price,
                ratio_hint=take_profit_ratio,
            )
            stop_loss_price = self._reprice_target_from_ratio(
                symbol=symbol,
                action=action,
                kind="stop-loss",
                reference_price=reprice_reference,
                existing_target=stop_loss_price,
                ratio_hint=stop_loss_ratio,
            )
        _adjust_tp_enabled = bool(guardrails.get("adjust_invalid_tp", False))
        _adjust_tp_pct_okx = self._extract_float(guardrails.get("adjust_invalid_tp_pct")) or 0.10
        _adjust_tp_lev = max(target_leverage or 1.0, 1.0)
        _adjust_tp_pct = _adjust_tp_pct_okx / _adjust_tp_lev
        take_profit_price = self._drop_conflicting_target(
            symbol=symbol,
            action=action,
            target=take_profit_price,
            reference_price=reference_for_protection,
            kind="take-profit",
            stage="post-fill",
            adjust_pct=_adjust_tp_pct if _adjust_tp_enabled else None,
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
                protection_placed = await self._refresh_position_protection(
                    symbol=symbol,
                    trade_mode=trade_mode,
                    action=action,
                    take_profit_price=take_profit_price,
                    stop_loss_price=stop_loss_price,
                    dual_side_mode=dual_side_mode,
                    pos_side=pos_side,
                )
                if not protection_placed and require_protection:
                    self._record_execution_feedback(
                        symbol,
                        "WARNING: trade executed but TP/SL protection could not be placed on OKX",
                        level="warning",
                        meta={
                            "guardrail": "require_protection",
                            "action": action,
                            "take_profit": take_profit_price,
                            "stop_loss": stop_loss_price,
                        },
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

    def _contract_value(self, symbol: str) -> float:
        """Return OKX ctVal for a symbol: how many base-token units make one contract.

        For BTC-USDT-SWAP this is 0.01 (100 contracts = 1 BTC).  For micro-priced meme
        tokens it can be in the hundreds or thousands.  Returns 1.0 as a safe default when
        the spec has not been fetched yet (e.g. in tests or config-only mode).

        CRITICAL: OKX's ``sz`` order field is in *contracts*.  Correct conversion::

            sz_contracts  = notional_usd / (ct_val × last_price)
            notional_usd  = sz_contracts × ct_val × last_price
        """
        spec = self._instrument_specs.get((symbol or "").upper()) or {}
        ct_val = spec.get("ct_val")
        return float(ct_val) if ct_val and ct_val > 0 else 1.0

    def _quantize_order_size(self, symbol: str, size: float) -> float | None:
        """Snap requested size to the instrument's lot size and enforce min order size."""
        if size is None or size <= 0:
            return None
        spec = self._instrument_specs.get((symbol or "").upper())
        if not spec:
            self._emit_debug(
                f"_quantize_order_size: no instrument spec for {symbol!r}; "
                f"returning size unquantized — this may cause 51121 if lot_size != 1"
            )
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
        spec = self._instrument_specs.get((symbol or "").upper())
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
        *,
        symbol: str | None = None,
    ) -> float | None:
        """Ensure the TP is on the profitable side of entry; return None if wrong direction.

        A None return does NOT block the trade by itself — that only happens
        when ``require_protection`` is True.  When the LLM supplies a TP but
        gets the direction slightly wrong, callers that enforce
        ``require_protection`` should snap to the nearest valid value rather
        than discarding it.
        """
        if take_profit is None or take_profit <= 0:
            return None

        if reference_price and reference_price > 0:
            if action == "BUY" and take_profit <= reference_price:
                if symbol:
                    self._record_execution_feedback(
                        symbol,
                        f"TP {take_profit:.6f} dropped (BUY requires TP above entry {reference_price:.6f}); trade will proceed without TP",
                        level="info",
                        meta={
                            "action": action,
                            "take_profit": take_profit,
                            "reference_price": reference_price,
                        },
                    )
                self._emit_debug(
                    f"Rejected take profit {take_profit:.6f}: BUY action requires TP above entry {reference_price:.6f}"
                )
                return None
            if action == "SELL" and take_profit >= reference_price:
                if symbol:
                    self._record_execution_feedback(
                        symbol,
                        f"TP {take_profit:.6f} dropped (SELL requires TP below entry {reference_price:.6f}); trade will proceed without TP",
                        level="info",
                        meta={
                            "action": action,
                            "take_profit": take_profit,
                            "reference_price": reference_price,
                        },
                    )
                self._emit_debug(
                    f"Rejected take profit {take_profit:.6f}: SELL action requires TP below entry {reference_price:.6f}"
                )
                return None
        return take_profit

    def _snap_take_profit_to_valid(
        self,
        action: str,
        reference_price: float,
        symbol: str,
    ) -> float | None:
        """Return the minimum valid TP just beyond entry for use as a last-resort fallback.

        Called only when ``require_protection`` is True and the LLM supplied a
        TP that was wrong-direction (so normalization dropped it).  Snapping
        is preferable to blocking the trade entirely.
        """
        tick = (self._instrument_specs.get(symbol) or {}).get("tick_size") or 0.0
        min_offset = max(tick, reference_price * self.PROTECTION_MIN_OFFSET_RATIO)
        if action == "BUY":
            snapped = reference_price + min_offset
            final = self._quantize_price(symbol, snapped, prefer_up=True) or snapped
        else:
            snapped = reference_price - min_offset
            if snapped <= 0:
                return None
            final = self._quantize_price(symbol, snapped, prefer_up=False) or snapped
        self._record_execution_feedback(
            symbol,
            "LLM take-profit adjusted to honor trade direction",
            level="warning",
            meta={
                "action": action,
                "adjusted_take_profit": final,
                "reference_price": reference_price,
                "reason": "require_protection: snapped wrong-direction TP to nearest valid value",
            },
        )
        self._emit_debug(
            f"{symbol} take-profit snapped to {final:.6f} (require_protection last-resort)"
        )
        return final

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
        spec = self._instrument_specs.get((symbol or "").upper()) or {}
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
        adjust_pct: float | None = None,
    ) -> float | None:
        """Drop or adjust TP/SL targets that violate OKX constraints relative to entry price.

        When *adjust_pct* is provided (a positive fraction, e.g. 0.015 = 1.5 %) and the
        target cannot be nudged by a minimum tick offset, a meaningful fallback take-profit
        is computed at ``reference_price * (1 ± adjust_pct)`` rather than returning None.
        The adjustment only applies to take-profit targets; stop-loss is never auto-adjusted.
        """
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
        # ── Pct-based fallback for invalid take-profit ────────────────────────
        if kind == "take-profit" and adjust_pct and adjust_pct > 0:
            pct_candidate = reference_price * (1.0 + adjust_pct) if prefer_up else reference_price * (1.0 - adjust_pct)
            if pct_candidate and pct_candidate > 0:
                pct_adjusted = self._quantize_price(symbol, pct_candidate, prefer_up=prefer_up)
                if pct_adjusted and not self._target_conflicts_with_price(
                    action,
                    target=pct_adjusted,
                    reference_price=reference_price,
                    kind=kind,
                ):
                    direction = "above" if prefer_up else "below"
                    message = (
                        f"{symbol} {kind} replaced from invalid {target:.6f} to {pct_adjusted:.6f} {stage}: "
                        f"computed {adjust_pct * 100:.2f}% {direction} entry price {reference_price:.6f}"
                    )
                    self._emit_debug(message)
                    self._record_execution_feedback(
                        symbol,
                        message,
                        level="info",
                        meta={
                            "stage": stage,
                            "kind": kind,
                            "original_target": target,
                            "adjusted_target": pct_adjusted,
                            "reference_price": reference_price,
                            "action": action,
                            "adjust_pct": adjust_pct,
                        },
                    )
                    return pct_adjusted
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

    @staticmethod
    def _calculate_target_ratio(
        action: str,
        reference_price: float | None,
        target_price: float | None,
        kind: str,
    ) -> float | None:
        """Return the absolute percentage gap between target and reference for repricing."""
        ref_value = MarketService._extract_float(reference_price)
        target_value = MarketService._extract_float(target_price)
        if ref_value is None or ref_value <= 0 or target_value is None or target_value <= 0:
            return None
        if kind == "take-profit":
            delta = target_value - ref_value
            if action == "BUY" and delta <= 0:
                return None
            if action == "SELL" and delta >= 0:
                return None
            return abs(delta) / ref_value
        if kind == "stop-loss":
            if action == "BUY":
                delta = ref_value - target_value
                if delta <= 0:
                    return None
                return delta / ref_value
            delta = target_value - ref_value
            if delta <= 0:
                return None
            return delta / ref_value
        return None

    @staticmethod
    def _target_from_ratio(
        action: str,
        reference_price: float | None,
        ratio: float | None,
        kind: str,
    ) -> float | None:
        """Reconstruct a TP/SL target by applying the stored ratio to a new reference price."""
        ref_value = MarketService._extract_float(reference_price)
        if ref_value is None or ref_value <= 0 or ratio is None or ratio <= 0:
            return None
        if kind == "take-profit":
            if action == "BUY":
                return ref_value * (1.0 + ratio)
            return ref_value * (1.0 - ratio)
        if kind == "stop-loss":
            if action == "BUY":
                return ref_value * (1.0 - ratio)
            return ref_value * (1.0 + ratio)
        return None

    def _reprice_target_from_ratio(
        self,
        *,
        symbol: str,
        action: str,
        kind: str,
        reference_price: float | None,
        existing_target: float | None,
        ratio_hint: float | None,
    ) -> float | None:
        """Apply stored percentage offsets to the latest reference price and quantize the result."""
        target_value = existing_target
        recalculated = self._target_from_ratio(action, reference_price, ratio_hint, kind)
        if recalculated is not None:
            target_value = recalculated
        if target_value is None:
            return None
        prefer_up = (kind == "take-profit" and action == "BUY") or (
            kind == "stop-loss" and action == "SELL"
        )
        quantized = self._quantize_price(symbol, target_value, prefer_up=prefer_up)
        return quantized if quantized and quantized > 0 else None

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

        def _leverage_ok(resp: Any) -> bool:
            """Return True only when OKX confirms the leverage change succeeded."""
            if not isinstance(resp, dict):
                return False
            return str(resp.get("code", "1")) == "0"

        try:
            resp = await asyncio.to_thread(setter, **payload)
        except Exception as exc:
            self._emit_debug(f"Failed to set leverage for {symbol}: {exc}")
            self._record_execution_feedback(
                symbol,
                "Failed to set isolated leverage",
                level="warning",
                meta={"requested_leverage": target_leverage, "pos_side": pos_designator},
            )
            return

        if not _leverage_ok(resp):
            # posSide="net" is rejected by OKX for hedge-mode accounts.  When
            # the order itself will use a directional posSide, retry with that
            # side so the leverage is correctly registered before the order fires.
            if pos_designator == "net" and pos_side and pos_side.lower() in {"long", "short"}:
                retry_designator = pos_side.lower()
                retry_payload = dict(payload)
                retry_payload["posSide"] = retry_designator
                retry_cache_key = self._leverage_cache_key(symbol, retry_designator)
                retry_cached = self._isolated_leverage_cache.get(retry_cache_key)
                if retry_cached is not None and abs(retry_cached - target_leverage) <= 1e-3:
                    # Already confirmed for this direction; treat as success.
                    return
                self._emit_debug(
                    f"Retrying set-leverage for {symbol} with posSide={retry_designator} "
                    f"(net was rejected — hedge-mode account)"
                )
                try:
                    retry_resp = await asyncio.to_thread(setter, **retry_payload)
                except Exception as exc2:
                    self._emit_debug(f"Failed to set leverage for {symbol} ({retry_designator}): {exc2}")
                    self._record_execution_feedback(
                        symbol,
                        "Failed to set isolated leverage (hedge-mode retry)",
                        level="warning",
                        meta={"requested_leverage": target_leverage, "pos_side": retry_designator},
                    )
                    return
                if _leverage_ok(retry_resp):
                    self._isolated_leverage_cache[retry_cache_key] = target_leverage
                    # Also record under the original cache key so subsequent calls
                    # with pos_designator="net" don't retry the rejected call.
                    self._isolated_leverage_cache[cache_key] = target_leverage
                    # Also set leverage for the OPPOSITE direction so the account
                    # is fully configured for isolated hedge-mode trading.  OKX
                    # tracks long/short leverage independently and having only one
                    # side configured can cause order rejections on some accounts.
                    opposite = "short" if retry_designator == "long" else "long"
                    opp_cache_key = self._leverage_cache_key(symbol, opposite)
                    if self._isolated_leverage_cache.get(opp_cache_key) is None:
                        opp_payload = dict(retry_payload)
                        opp_payload["posSide"] = opposite
                        try:
                            opp_resp = await asyncio.to_thread(setter, **opp_payload)
                            if _leverage_ok(opp_resp):
                                self._isolated_leverage_cache[opp_cache_key] = target_leverage
                                self._emit_debug(
                                    f"Set leverage for {symbol} ({opposite}) -> {target_leverage:.2f}x "
                                    f"(hedge-mode complement)"
                                )
                        except Exception:  # pragma: no cover - best-effort
                            pass
                else:
                    self._emit_debug(
                        f"set-leverage rejected for {symbol} ({retry_designator}): {retry_resp}"
                    )
                    self._record_execution_feedback(
                        symbol,
                        "Failed to set isolated leverage (both net and directional failed)",
                        level="warning",
                        meta={
                            "requested_leverage": target_leverage,
                            "net_response": resp,
                            "directional_response": retry_resp,
                        },
                    )
            else:
                self._emit_debug(f"set-leverage rejected for {symbol} ({pos_designator}): {resp}")
                self._record_execution_feedback(
                    symbol,
                    "Failed to set isolated leverage",
                    level="warning",
                    meta={"requested_leverage": target_leverage, "pos_side": pos_designator, "response": resp},
                )
            return

        self._isolated_leverage_cache[cache_key] = target_leverage

    async def _ensure_cross_leverage_setting(
        self,
        *,
        symbol: str,
        leverage: float | None,
    ) -> None:
        """Call the account API to set leverage for cross-margin orders."""
        if not self._account_api:
            return
        setter = getattr(self._account_api, "set_leverage", None)
        if setter is None:
            return
        target_leverage = self._extract_float(leverage)
        if not target_leverage or target_leverage <= 0:
            return
        target_leverage = max(1.0, float(target_leverage))
        cache_key = self._leverage_cache_key(symbol, "cross")
        cached_value = self._isolated_leverage_cache.get(cache_key)
        if cached_value is not None and abs(cached_value - target_leverage) <= 1e-3:
            return
        payload: dict[str, Any] = {
            "instId": symbol,
            "lever": self._format_leverage(target_leverage),
            "mgnMode": "cross",
        }
        if self._sub_account and self._sub_account_use_master:
            payload["subAcct"] = self._sub_account
        self._emit_debug(
            f"Setting cross leverage for {symbol} -> {target_leverage:.2f}x"
        )
        try:
            resp = await asyncio.to_thread(setter, **payload)
        except Exception as exc:
            self._emit_debug(f"Failed to set cross leverage for {symbol}: {exc}")
            return
        if isinstance(resp, dict) and str(resp.get("code", "1")) == "0":
            self._isolated_leverage_cache[cache_key] = target_leverage
        else:
            self._emit_debug(f"set-cross-leverage rejected for {symbol}: {resp}")
            self._record_execution_feedback(
                symbol,
                "Failed to set cross leverage",
                level="warning",
                meta={"requested_leverage": target_leverage, "response": resp},
            )

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
        isolated_bootstrap: bool = False,
        bootstrap_min_notional: float | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Place an order via the trade API, retrying without posSide if needed."""
        if not self._trade_api:
            self._emit_debug("Trade API unavailable; cannot place order")
            return None
        include_pos_side = pos_side
        attachments_to_use = attach_algo_orders
        attempt = 0
        _margin_retry_count = 0
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
        elif trade_mode == "cross" and not reduce_only:
            await self._ensure_cross_leverage_setting(
                symbol=symbol,
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
            # Per OKX API docs, 'ccy' (margin currency) is only applicable to
            # cross MARGIN orders in single-currency margin mode.  Sending it
            # on isolated SWAP/FUTURES orders routes the request through a
            # borrow-path check that triggers sCode 51008 even when the
            # account holds sufficient quote-currency balance.
            # Rule: only attach ccy for cross-mode non-SWAP/FUTURES instruments.
            _is_perpetual = "-SWAP" in symbol
            _is_futures = not _is_perpetual and (
                len(symbol.split("-")) >= 3 and symbol.split("-")[-1].isdigit()
            )
            _is_spot_margin = not _is_perpetual and not _is_futures
            if trade_mode == "cross" and _is_spot_margin and resolved_margin_currency:
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
            }
            if "ccy" in payload:
                trace_payload["ccy"] = payload["ccy"]
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
            if (
                not reduce_only
                # Bootstrap orders: allow only 1 downsize retry.  Creating a new
                # isolated wallet requires a minimum notional; a second halving
                # almost always undershoots that floor and just wastes API quota.
                and _margin_retry_count < (1 if isolated_bootstrap else 2)
                and self._response_indicates_insufficient_margin(response)
            ):
                _cur_size = self._extract_float(size)
                if _cur_size and _cur_size > 1:
                    _reduced_size = max(1.0, _cur_size * 0.5)
                    # Snap to lot size so OKX doesn't reject with 51121
                    # "Order quantity must be a multiple of the lot size".
                    _quantized_reduced = self._quantize_order_size(symbol, _reduced_size)
                    if _quantized_reduced and _quantized_reduced > 0:
                        _reduced_size = _quantized_reduced
                    else:
                        _reduced_size = math.floor(_reduced_size)
                    if _reduced_size <= 0:
                        _reduced_size = 1.0
                    # For bootstrap orders (no existing isolated wallet), check
                    # that the reduced notional would still clear the minimum
                    # seeding floor.  Retrying below the floor would just fail
                    # again with the same 51008 and produce a confusing card.
                    if isolated_bootstrap and reference_price_value and reference_price_value > 0:
                        _reduced_notional = _reduced_size * reference_price_value
                        _boot_floor = bootstrap_min_notional or self.OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD
                        if _reduced_notional < _boot_floor:
                            self._emit_debug(
                                f"51008 bootstrap: reduced notional {_reduced_notional:.4f} "
                                f"would be below floor {_boot_floor:.2f}; "
                                f"skipping downsize retry"
                            )
                            # Fall through to the normal error-reporting path.
                        else:
                            _margin_retry_count += 1
                            self._emit_debug(
                                f"51008 margin error for {symbol} (bootstrap); auto-downsize "
                                f"{_cur_size:.0f} -> {_reduced_size:.0f} "
                                f"(margin retry {_margin_retry_count}/2)"
                            )
                            self._record_execution_feedback(
                                symbol,
                                f"Margin insufficient for size {_cur_size:.0f}; "
                                f"auto-downsized to {_reduced_size:.0f} and retrying",
                                level="warning",
                            )
                            size = _reduced_size
                            continue
                    else:
                        _margin_retry_count += 1
                        self._emit_debug(
                            f"51008 margin error for {symbol}; auto-downsize "
                            f"{_cur_size:.0f} -> {_reduced_size:.0f} "
                            f"(margin retry {_margin_retry_count}/2)"
                        )
                        self._record_execution_feedback(
                            symbol,
                            f"Margin insufficient for size {_cur_size:.0f}; "
                            f"auto-downsized to {_reduced_size:.0f} and retrying",
                            level="warning",
                        )
                        size = _reduced_size
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
            # If this was a bootstrap order, add to the blocklist regardless of
            # how many retries ran.  When the floor-check prevents the downsize
            # _margin_retry_count stays 0, but the symbol still can't be seeded
            # and would fail again every scheduler tick without a block.
            if isolated_bootstrap and self._response_indicates_insufficient_margin(response):
                self._bootstrap_blocked[symbol] = time.time()
                block_mins = self.BOOTSTRAP_BLOCK_SECONDS // 60
                if _margin_retry_count > 0:
                    self._emit_debug(
                        f"[bootstrap] {symbol} exhausted 51008 retries; "
                        f"adding to bootstrap blocklist for {block_mins} minutes"
                    )
                    error_message = (
                        f"{error_message} — all bootstrap retries exhausted. "
                        f"Bot will pause trying to seed {symbol} for {block_mins} minutes. "
                        "Add USDT to your account or reduce isolated_wallet_bootstrap_pct."
                    )
                else:
                    self._emit_debug(
                        f"[bootstrap] {symbol} initial 51008 not retryable; "
                        f"adding to bootstrap blocklist for {block_mins} minutes"
                    )
                    error_message = (
                        f"{error_message} — bootstrap order not retryable (notional below floor). "
                        f"Bot will pause trying to seed {symbol} for {block_mins} minutes. "
                        "Add USDT to your account or reduce isolated_wallet_bootstrap_pct."
                    )
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

    async def _reconcile_fills(self) -> None:
        """Poll OKX fills-history and back-fill realized PnL on locally recorded entry trades.

        Flow
        ----
        1. Fetch the 100 most recent fills every call (no cursor — always fresh).
        2. Keep only fills where ``pnl != "0"`` — those are closing/reduce fills that carry
           actual realized profit/loss.
        3. For each such fill look up the most recent unreconciled ``executed_trades`` row for
           the same symbol whose side is the **opposite** of the fill side (the original open
           order) and update its ``pnl``, ``fee``, and ``okx_fill_id`` columns.

        Note: no cursor/pagination is used here.  OKX ``after`` moves *backwards* in time
        (returns fills older than the given ID), so tracking it caused the reconciler to
        drift into the past and miss new closing fills.  Instead we simply re-fetch the
        latest 100 fills every call; the ``okx_fill_id IS NULL`` guard in
        ``FETCH_UNRECONCILED_SQL`` ensures already-reconciled trades are never touched twice.
        """
        if not self._trade_api:
            return
        sub_acct = self._sub_account if self._sub_account_use_master else None
        try:
            response = await asyncio.to_thread(
                self._trade_api.get_fills_history,
                inst_type="SWAP",
                after="",
                limit=100,
                sub_acct=sub_acct,
            )
        except Exception as exc:  # pragma: no cover - network variance
            logger.warning("fills-history fetch failed: %s", exc)
            return

        fills = self._safe_data(response)
        if not fills:
            return

        reconciled = 0
        for fill in fills:
            raw_pnl = fill.get("pnl") or fill.get("fillPnl") or "0"
            try:
                pnl_value = float(raw_pnl)
            except (TypeError, ValueError):
                continue
            if pnl_value == 0.0:
                # Entry fills always have pnl=0; nothing to record yet.
                continue

            inst_id = str(fill.get("instId") or "").upper()
            fill_side = str(fill.get("side") or "").lower()  # "buy" or "sell"
            fill_id = str(fill.get("fillId") or fill.get("tradeId") or "")
            raw_fee = fill.get("fee") or fill.get("fillFee") or None
            try:
                fee_value: float | None = float(raw_fee) if raw_fee is not None else None
                # OKX fees are negative (cost); store as positive absolute value.
                if fee_value is not None:
                    fee_value = abs(fee_value)
            except (TypeError, ValueError):
                fee_value = None

            if not inst_id or not fill_side:
                continue

            try:
                candidates = await fetch_unreconciled_trades(
                    symbol=inst_id,
                    side=fill_side,  # helper queries the *opposite* side (open direction)
                    lookback_hours=48.0,
                )
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("unreconciled trade lookup failed for %s: %s", inst_id, exc)
                continue

            if not candidates:
                continue

            # Take the most recent unreconciled entry as the match.
            target = candidates[0]
            try:
                await update_trade_pnl(
                    trade_id=target["id"],
                    pnl=pnl_value,
                    fee=fee_value,
                    okx_fill_id=fill_id or None,
                )
                reconciled += 1
                self._emit_debug(
                    f"Reconciled PnL for {inst_id}: {pnl_value:+.4f} USDT "
                    f"(fill {fill_id}, trade {target['id']})"
                )
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning("Failed to update trade PnL for %s: %s", inst_id, exc)

        if reconciled:
            logger.info("Fill reconciliation: updated PnL for %d trade(s)", reconciled)

        # Second pass: capture entry-leg taker fees (pnl == 0 fills).
        # The entry row was inserted with fee=NULL; once the exchange reports the fill
        # we can store the fee so the fee-card window query counts it promptly.
        entry_fees_stored = 0
        for fill in fills:
            raw_pnl = fill.get("pnl") or fill.get("fillPnl") or "0"
            try:
                pnl_value = float(raw_pnl)
            except (TypeError, ValueError):
                continue
            if pnl_value != 0.0:
                # Closing fills already handled in the loop above.
                continue

            raw_fee = fill.get("fee") or fill.get("fillFee") or None
            if raw_fee is None:
                continue
            try:
                fee_value = abs(float(raw_fee))
            except (TypeError, ValueError):
                continue
            if fee_value == 0.0:
                continue

            inst_id = str(fill.get("instId") or "").upper()
            fill_side = str(fill.get("side") or "").lower()
            fill_id = str(fill.get("fillId") or fill.get("tradeId") or "")
            if not inst_id or not fill_side:
                continue

            try:
                # For entry fills the row side matches the fill side (same direction).
                candidates = await fetch_unreconciled_trades(
                    symbol=inst_id,
                    side=fill_side,
                    lookback_hours=48.0,
                    same_side=True,
                )
            except TypeError:
                # fetch_unreconciled_trades may not support same_side yet; skip gracefully.
                continue
            except Exception as exc:  # pragma: no cover - best-effort
                logger.debug("entry fee lookup failed for %s: %s", inst_id, exc)
                continue

            if not candidates:
                continue

            target = candidates[0]
            try:
                await update_entry_fee(
                    trade_id=target["id"],
                    fee=fee_value,
                    okx_fill_id=fill_id or None,
                )
                entry_fees_stored += 1
                self._emit_debug(
                    f"Stored entry fee for {inst_id}: {fee_value:.4f} USDT "
                    f"(fill {fill_id}, trade {target['id']})"
                )
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning("Failed to update entry fee for %s: %s", inst_id, exc)

        if entry_fees_stored:
            logger.info("Fill reconciliation: stored entry fees for %d trade(s)", entry_fees_stored)

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
    ) -> bool:
        """Rebuild TP/SL protection for a symbol, replacing any prior algo orders.

        Returns True when placement was accepted by OKX, False otherwise.
        """
        if not self._trade_api:
            return False
        symbol_key = symbol.upper()
        await self._cancel_position_protection(symbol)
        if not (take_profit_price or stop_loss_price):
            self._position_protection.pop(symbol_key, None)
            return False
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
        if not placement:
            self._record_execution_feedback(
                symbol,
                "Protection placement failed: OKX did not accept TP/SL algo",
                level="warning",
                meta={
                    "take_profit": take_profit_price,
                    "stop_loss": stop_loss_price,
                    "reason": "_place_position_protection returned None",
                },
            )
            self._position_protection[symbol_key] = pending_meta
            return False
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
        return True

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
            self._record_execution_feedback(
                symbol,
                "TP/SL algo skipped: position not detected after wait (protection not placed)",
                level="warning",
                meta={"symbol": symbol, "pos_side": pos_side},
            )
            self._emit_debug(
                f"Skipping TP/SL algo for {symbol}: position not confirmed after wait"
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
