"""Build synthetic market snapshots from historical candles.

The live ``MarketService._build_snapshot()`` produces a large dict that
strategies navigate as ``snapshot["market_data"][symbol]["indicators"]``.
For backtesting we build a **minimal** synthetic snapshot that contains
exactly the keys the strategies read, computed from historical candles
using the same ``_compute_indicators`` / ``_compute_structure`` static
methods.

HTF alignment: at each LTF step, the most recent *closed* HTF candle is
used for ``htf_indicators`` — matching live behaviour where the bot only
sees confirmed HTF candles.
"""

from __future__ import annotations

import logging
from typing import Any

from app.services.backtest.data_fetcher import htf_for
from app.services.backtest.models import Candle
from app.services.market_service import MarketService

logger = logging.getLogger(__name__)


class SnapshotBuilder:
    """Builds synthetic snapshots for backtesting.

    The builder is stateless — it receives the full LTF and HTF candle arrays
    and a window index, and returns a snapshot dict matching the shape
    strategies expect.
    """

    def __init__(
        self,
        symbol: str,
        ltf_candles: list[Candle],
        htf_candles: list[Candle] | None,
        ltf_timeframe: str,
        *,
        tf_candles: dict[str, list[Candle]] | None = None,
    ) -> None:
        self._symbol = symbol
        self._ltf_candles = ltf_candles
        self._htf_candles = htf_candles or []
        self._ltf_timeframe = ltf_timeframe
        self._htf_timeframe = htf_for(ltf_timeframe)
        # Per-strategy analysis timeframes: {bar: candles}.  Each block's HTF
        # is derived from that bar via htf_for().
        self._tf_candles: dict[str, list[Candle]] = tf_candles or {}

    @staticmethod
    def _build_block(
        candle_window: list[Candle],
        current_ts: int,
        htf_candles: list[Candle] | None,
        htf_timeframe: str,
    ) -> dict[str, Any]:
        """Compute an indicator block from a candle window + its HTF.

        Mirrors ``MarketService._build_timeframes_block``: builds the same
        shape as ``indicators`` (with structure and flattened swing scalars)
        and attaches the HTF layer (``htf_indicators`` / ``adx_htf`` /
        ``choppiness_htf``) from the most recent closed HTF candle at or
        before ``current_ts``.
        """
        raw = [SnapshotBuilder._candle_to_row(c) for c in candle_window]
        block = MarketService._compute_indicators(raw)
        block["structure"] = MarketService._compute_structure(raw)
        _structure = block.get("structure") or {}
        _sw_highs = _structure.get("swing_highs") or []
        _sw_lows = _structure.get("swing_lows") or []
        block["swing_high"] = (
            _sw_highs[-1].get("price") if _sw_highs and isinstance(_sw_highs[-1], dict) else None
        )
        block["swing_low"] = (
            _sw_lows[-1].get("price") if _sw_lows and isinstance(_sw_lows[-1], dict) else None
        )
        if htf_candles and htf_timeframe:
            htf_window = [c for c in htf_candles if c.ts <= current_ts]
            if htf_window:
                htf_raw = [SnapshotBuilder._candle_to_row(c) for c in htf_window]
                block["ohlcv_htf"] = htf_raw
                block["htf_indicators"] = MarketService._compute_indicators(htf_raw)
                block["ohlcv_htf_bar"] = htf_timeframe
                _htf = block.get("htf_indicators") or {}
                block["adx_htf"] = ((_htf.get("adx") or {}).get("value"))
                block["choppiness_htf"] = _htf.get("choppiness")
        return block

    def build(self, window_end_idx: int) -> dict[str, Any]:
        """Build a snapshot for the LTF candle at ``window_end_idx``.

        ``window_end_idx`` is the index of the *current* candle in
        ``ltf_candles``.  All candles up to and including this index are
        used for indicator computation (matching live behaviour where
        the current candle is included).
        """
        # ── LTF indicators ────────────────────────────────────────────
        ltf_window = self._ltf_candles[: window_end_idx + 1]
        ltf_raw = [self._candle_to_row(c) for c in ltf_window]
        indicators = MarketService._compute_indicators(ltf_raw)
        indicators["structure"] = MarketService._compute_structure(ltf_raw)

        # ── HTF alignment ─────────────────────────────────────────────
        # Use the most recent closed HTF candle at or before the current
        # LTF candle's timestamp.
        current_ts = self._ltf_candles[window_end_idx].ts
        htf_window_raw: list[list[Any]] = []
        htf_bar = ""
        if self._htf_candles and self._htf_timeframe:
            htf_window = [c for c in self._htf_candles if c.ts <= current_ts]
            if htf_window:
                htf_window_raw = [self._candle_to_row(c) for c in htf_window]
                htf_bar = self._htf_timeframe
                indicators["ohlcv_htf"] = htf_window_raw
                indicators["htf_indicators"] = MarketService._compute_indicators(htf_window_raw)
                indicators["ohlcv_htf_bar"] = htf_bar

        # ── Per-strategy analysis timeframes ─────────────────────────
        # Build a ``timeframes`` map for each requested bar, aligning each
        # bar's candles to the current LTF step (closed candles at or before
        # current_ts) and attaching that bar's own HTF.
        timeframes: dict[str, dict[str, Any]] = {}
        for tf, tf_candles in self._tf_candles.items():
            tf_window = [c for c in tf_candles if c.ts <= current_ts]
            if not tf_window:
                continue
            tf_htf = htf_for(tf)
            tf_htf_candles: list[Candle] = []
            if tf_htf:
                tf_htf_candles = [c for c in tf_candles if c.ts <= current_ts]
                # The HTF of a strategy tf is a separate bar; use the generic
                # per-tf candle set if present, else reuse the strategy tf's
                # own candles (best-effort).
                tf_htf_candles = self._tf_candles.get(tf_htf) or tf_htf_candles
            timeframes[tf] = self._build_block(
                tf_window, current_ts, tf_htf_candles, tf_htf
            )

        # ── Assemble snapshot ─────────────────────────────────────────
        last_price = float(self._ltf_candles[window_end_idx].close)
        return {
            "generated_at": self._ltf_candles[window_end_idx].dt.isoformat(),
            "symbol": self._symbol,
            "symbols": [self._symbol],
            "last_price": last_price,
            "market_data": {
                self._symbol: {
                    "ticker": {"last": str(last_price)},
                    "indicators": indicators,
                    "timeframes": timeframes,
                    "custom_metrics": {
                        # Footprint/CVD/OFI are not available in backtest.
                        "cumulative_volume_delta": 0.0,
                        "cvd_series": [],
                        "order_flow_imbalance": {},
                        "footprint": {},
                        "market_long_short_ratio": {},
                    },
                },
            },
            # Strategies don't read positions from the snapshot for signal
            # generation — the launcher uses its own _launcher_in_position
            # tracking.  We include an empty list for completeness.
            "positions": [],
            "account_equity": 0.0,
            "total_account_value": 0.0,
        }

    def build_with_incomplete_ltf(
        self,
        closed_ltf_window: list[Candle],
        incomplete_candle: Candle,
        current_ts: int,
    ) -> dict[str, Any]:
        """Build a snapshot where the last LTF candle is INCOMPLETE.

        This mirrors live behaviour: the bot polls mid-candle and the
        snapshot's last_price = real-time ticker price.  Here, the
        ``incomplete_candle`` is a synthetic LTF bar whose close = the
        current eval-candle close (the real-time proxy).

        Parameters
        ----------
        closed_ltf_window:
            Fully-closed LTF candles occurring before the current LTF bucket.
            These are appended before ``incomplete_candle`` for indicator
            computation.
        incomplete_candle:
            Synthetic LTF candle representing the in-progress bar.  Its
            ``close`` is the current eval-candle close; ``open``/``high``/
            ``low``/``volume`` are aggregated from eval candles seen so far
            in this LTF bucket.
        current_ts:
            Timestamp of the current eval candle (ms epoch).  Used for HTF
            alignment — HTF candles with ``ts <= current_ts`` are included.
        """
        # ── LTF indicators (closed window + incomplete last candle) ───
        ltf_raw = [self._candle_to_row(c) for c in closed_ltf_window]
        ltf_raw.append(self._candle_to_row(incomplete_candle))
        indicators = MarketService._compute_indicators(ltf_raw)
        indicators["structure"] = MarketService._compute_structure(ltf_raw)

        # ── HTF alignment ─────────────────────────────────────────────
        htf_window_raw: list[list[Any]] = []
        htf_bar = ""
        if self._htf_candles and self._htf_timeframe:
            htf_window = [c for c in self._htf_candles if c.ts <= current_ts]
            if htf_window:
                htf_window_raw = [self._candle_to_row(c) for c in htf_window]
                htf_bar = self._htf_timeframe
                indicators["ohlcv_htf"] = htf_window_raw
                indicators["htf_indicators"] = MarketService._compute_indicators(htf_window_raw)
                indicators["ohlcv_htf_bar"] = htf_bar

        # ── Per-strategy analysis timeframes ─────────────────────────
        timeframes: dict[str, dict[str, Any]] = {}
        for tf, tf_candles in self._tf_candles.items():
            tf_window = [c for c in tf_candles if c.ts <= current_ts]
            if not tf_window:
                continue
            tf_htf = htf_for(tf)
            tf_htf_candles: list[Candle] = []
            if tf_htf:
                tf_htf_candles = self._tf_candles.get(tf_htf) or tf_window
            timeframes[tf] = self._build_block(
                tf_window, current_ts, tf_htf_candles, tf_htf
            )

        # ── Assemble snapshot ─────────────────────────────────────────
        last_price = float(incomplete_candle.close)
        return {
            "generated_at": incomplete_candle.dt.isoformat(),
            "symbol": self._symbol,
            "symbols": [self._symbol],
            "last_price": last_price,
            "market_data": {
                self._symbol: {
                    "ticker": {"last": str(last_price)},
                    "indicators": indicators,
                    "timeframes": timeframes,
                    "custom_metrics": {
                        "cumulative_volume_delta": 0.0,
                        "cvd_series": [],
                        "order_flow_imbalance": {},
                        "footprint": {},
                        "market_long_short_ratio": {},
                    },
                },
            },
            "positions": [],
            "account_equity": 0.0,
            "total_account_value": 0.0,
        }

    @staticmethod
    def _candle_to_row(c: Candle) -> list[Any]:
        """Convert a Candle to the raw OKX row format ``[ts, o, h, l, c, v]``."""
        return [c.ts, c.open, c.high, c.low, c.close, c.volume]
