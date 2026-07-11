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
    ) -> None:
        self._symbol = symbol
        self._ltf_candles = ltf_candles
        self._htf_candles = htf_candles or []
        self._ltf_timeframe = ltf_timeframe
        self._htf_timeframe = htf_for(ltf_timeframe)

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

    @staticmethod
    def _candle_to_row(c: Candle) -> list[Any]:
        """Convert a Candle to the raw OKX row format ``[ts, o, h, l, c, v]``."""
        return [c.ts, c.open, c.high, c.low, c.close, c.volume]
