"""Build synthetic market snapshots from historical candles.

The live ``MarketService._build_snapshot()`` produces a large dict that
strategies navigate as ``snapshot["market_data"][symbol]["indicators"]``.
For backtesting we build a **minimal** synthetic snapshot that contains
exactly the keys the strategies read, computed from historical candles
using the same ``_compute_indicators`` / ``_compute_structure`` static
methods.

Closed-candle alignment (no look-ahead): a snapshot describes one *moment*.
Per-strategy ``timeframes[<tf>]`` blocks and every HTF layer contain only
candles that have **closed** by that moment (``ts + bar_ms <= moment``),
matching live, where ``MarketService._build_closed_candle_snapshot`` trims
the forming candle of each timeframe.  OKX candle timestamps are the bar's
*open* time, so selecting candles by ``ts <= now`` would expose the in-progress
bar with its final high/low/close — future data.

* ``build(i)`` — the moment is the close of LTF candle ``i`` (``ts_i + ltf_ms``).
* ``build_with_incomplete_ltf`` — the moment is the close of the current eval
  candle (``current_ts + eval_ms``).  Each timeframe's in-progress candle is
  aggregated from the eval candles seen so far and exposed as
  ``forming_candle`` (``None`` when the bar has just closed).
"""

from __future__ import annotations

import logging
from bisect import bisect_left, bisect_right
from typing import Any

from app.services.backtest.data_fetcher import htf_for, timeframe_ms
from app.services.backtest.indicator_cache import IndicatorCache
from app.services.backtest.models import Candle
from app.services.market_service import MarketService

logger = logging.getLogger(__name__)


class SnapshotBuilder:
    """Builds synthetic snapshots for backtesting.

    The builder is stateless — it receives the full LTF and HTF candle arrays
    and a window index, and returns a snapshot dict matching the shape
    strategies expect.

    Indicator series are precomputed **once** per candle array via
    :class:`IndicatorCache` and served per step by slicing (the pandas-ta
    indicators are causal, so a sliced full-array series equals a prefix
    recompute).  This turns the O(n²) per-step recompute into O(n).
    """

    def __init__(
        self,
        symbol: str,
        ltf_candles: list[Candle],
        htf_candles: list[Candle] | None,
        ltf_timeframe: str,
        *,
        tf_candles: dict[str, list[Candle]] | None = None,
        eval_candles: list[Candle] | None = None,
        eval_timeframe: str | None = None,
    ) -> None:
        self._symbol = symbol
        self._ltf_candles = ltf_candles
        self._htf_candles = htf_candles or []
        self._ltf_timeframe = ltf_timeframe
        self._ltf_ms = timeframe_ms(ltf_timeframe)
        self._htf_timeframe = htf_for(ltf_timeframe)
        # Finer evaluation candles (finer_ltf mode): the source for each
        # timeframe's in-progress candle.  ``eval_ms`` turns an eval candle's
        # open ts into the moment its close is known.
        self._eval_candles: list[Candle] = eval_candles or []
        self._eval_ts: list[int] = [c.ts for c in self._eval_candles]
        self._eval_ms = timeframe_ms(eval_timeframe) if eval_timeframe else 0
        # bucket_ts -> (eval end index, aggregated candle); extended
        # incrementally as the engine steps forward through a bucket.
        self._forming_memo: dict[int, tuple[int, dict[str, Any]]] = {}
        # Per-strategy analysis timeframes: {bar: candles}.  Each block's HTF
        # is derived from that bar via htf_for().
        self._tf_candles: dict[str, list[Candle]] = tf_candles or {}

        # Precomputed indicator caches (one per candle array).
        self._ltf_cache = IndicatorCache([self._candle_to_row(c) for c in ltf_candles])
        self._htf_cache = (
            IndicatorCache([self._candle_to_row(c) for c in self._htf_candles])
            if self._htf_candles
            else None
        )
        self._tf_caches: dict[str, IndicatorCache] = {
            tf: IndicatorCache([self._candle_to_row(c) for c in candles])
            for tf, candles in self._tf_candles.items()
        }

    @staticmethod
    def _attach_structure(block: dict[str, Any], raw: list[list[Any]], *, with_swings: bool) -> None:
        """Attach ``structure`` (and optionally flattened swing scalars) to *block*."""
        block["structure"] = MarketService._compute_structure(raw)
        if not with_swings:
            return
        _structure = block.get("structure") or {}
        _sw_highs = _structure.get("swing_highs") or []
        _sw_lows = _structure.get("swing_lows") or []
        block["swing_high"] = (
            _sw_highs[-1].get("price") if _sw_highs and isinstance(_sw_highs[-1], dict) else None
        )
        block["swing_low"] = (
            _sw_lows[-1].get("price") if _sw_lows and isinstance(_sw_lows[-1], dict) else None
        )

    @staticmethod
    def _closed_idx(cache: IndicatorCache, bar_ms: int, moment_ts: int) -> int:
        """Index of the last candle closed by ``moment_ts`` (``ts + bar_ms <= moment``)."""
        return cache.end_idx_for_ts(moment_ts - bar_ms)

    @staticmethod
    def _attach_htf(
        block: dict[str, Any],
        htf_cache: IndicatorCache | None,
        htf_timeframe: str,
        moment_ts: int,
    ) -> None:
        """Attach the HTF layer from the HTF candles closed by ``moment_ts``."""
        if htf_cache is None or not htf_timeframe:
            return
        htf_idx = SnapshotBuilder._closed_idx(htf_cache, timeframe_ms(htf_timeframe), moment_ts)
        if htf_idx < 0:
            return
        htf_block = htf_cache.block_at(htf_idx)
        block["ohlcv_htf"] = htf_cache.rows_upto(htf_idx)
        block["htf_indicators"] = htf_block
        block["ohlcv_htf_bar"] = htf_timeframe
        block["adx_htf"] = (htf_block.get("adx") or {}).get("value")
        block["choppiness_htf"] = htf_block.get("choppiness")

    def _cached_block(
        self,
        cache: IndicatorCache,
        end_idx: int,
        moment_ts: int,
        htf_cache: IndicatorCache | None,
        htf_timeframe: str,
        *,
        with_swings: bool,
    ) -> dict[str, Any]:
        """Build an indicator block for ``cache``'s window ``[:end_idx+1]``."""
        block = cache.block_at(end_idx)
        self._attach_structure(block, cache.rows_upto(end_idx), with_swings=with_swings)
        self._attach_htf(block, htf_cache, htf_timeframe, moment_ts)
        return block

    def _timeframe_blocks(
        self, moment_ts: int, *, forming_from_ts: int | None
    ) -> dict[str, dict[str, Any]]:
        """Closed-candle ``timeframes`` blocks at ``moment_ts``.

        ``forming_from_ts`` is the open ts of the current eval candle
        (finer_ltf mode); when set, each block also carries its in-progress
        candle as ``forming_candle``.  ``None`` (closed mode) → no forming
        candle.
        """
        timeframes: dict[str, dict[str, Any]] = {}
        for tf, tf_cache in self._tf_caches.items():
            tf_ms = timeframe_ms(tf)
            tf_idx = self._closed_idx(tf_cache, tf_ms, moment_ts)
            if tf_idx < 0:
                continue
            tf_htf = htf_for(tf)
            # The HTF of a strategy tf is a separate bar; use the generic
            # per-tf candle set if present, else reuse the strategy tf's
            # own candles (best-effort).
            tf_htf_cache = self._tf_caches.get(tf_htf) if tf_htf else None
            if tf_htf and tf_htf_cache is None:
                tf_htf_cache = tf_cache
            block = self._cached_block(
                tf_cache, tf_idx, moment_ts, tf_htf_cache, tf_htf, with_swings=True
            )
            block["forming_candle"] = None
            if forming_from_ts is not None:
                block["forming_candle"] = self._forming_candle(
                    tf_cache, tf_ms, tf_idx, forming_from_ts, moment_ts
                )
            timeframes[tf] = block
        return timeframes

    def _forming_candle(
        self,
        tf_cache: IndicatorCache,
        tf_ms: int,
        closed_idx: int,
        current_ts: int,
        moment_ts: int,
    ) -> dict[str, Any] | None:
        """The ``tf`` candle in progress at ``moment_ts``, built from eval candles.

        The bucket is located from the tf candle list itself (robust to bars
        that are not aligned to UTC multiples, e.g. OKX's HK-time ``1D``).
        Returns ``None`` when the bar containing ``current_ts`` has closed by
        ``moment_ts`` or no eval candles are available.
        """
        cur_idx = tf_cache.end_idx_for_ts(current_ts)
        if cur_idx < 0 or cur_idx <= closed_idx:
            return None
        bucket_ts = int(tf_cache.ts_at(cur_idx))
        if bucket_ts + tf_ms <= moment_ts:
            return None
        return self._aggregate_eval(bucket_ts, current_ts)

    def _aggregate_eval(self, bucket_ts: int, current_ts: int) -> dict[str, Any] | None:
        """Aggregate eval candles with ``bucket_ts <= ts <= current_ts``."""
        if not self._eval_candles:
            return None
        lo = bisect_left(self._eval_ts, bucket_ts)
        hi = bisect_right(self._eval_ts, current_ts)
        if hi <= lo:
            return None
        memo = self._forming_memo.get(bucket_ts)
        if memo is not None and memo[0] <= hi:
            start, agg = memo[0], dict(memo[1])
        else:
            first = self._eval_candles[lo]
            start = lo + 1
            agg = {
                "ts": bucket_ts,
                "open": float(first.open),
                "high": float(first.high),
                "low": float(first.low),
                "close": float(first.close),
                "volume": float(first.volume),
            }
        for candle in self._eval_candles[start:hi]:
            agg["high"] = max(agg["high"], float(candle.high))
            agg["low"] = min(agg["low"], float(candle.low))
            agg["close"] = float(candle.close)
            agg["volume"] += float(candle.volume)
        if len(self._forming_memo) > 16:
            self._forming_memo.clear()
        self._forming_memo[bucket_ts] = (hi, dict(agg))
        return agg

    def build(self, window_end_idx: int) -> dict[str, Any]:
        """Build a snapshot for the LTF candle at ``window_end_idx``.

        ``window_end_idx`` is the index of the *current* candle in
        ``ltf_candles``.  All candles up to and including this index are
        used for indicator computation (matching live behaviour where
        the current candle is included).
        """
        # ── LTF indicators ────────────────────────────────────────────
        # The step represents the close of LTF candle ``window_end_idx``.
        moment_ts = self._ltf_candles[window_end_idx].ts + self._ltf_ms
        indicators = self._cached_block(
            self._ltf_cache,
            window_end_idx,
            moment_ts,
            self._htf_cache,
            self._htf_timeframe,
            with_swings=False,
        )
        # Closed mode never has an in-progress candle.
        indicators["forming_candle"] = None

        # ── Per-strategy analysis timeframes ─────────────────────────
        # Closed candles only, each with that bar's own HTF.
        timeframes = self._timeframe_blocks(moment_ts, forming_from_ts=None)

        # ── Assemble snapshot ─────────────────────────────────────────
        last_price = float(self._ltf_candles[window_end_idx].close)
        return {
            "generated_at": self._ltf_candles[window_end_idx].dt.isoformat(),
            # The moment this snapshot describes (ms epoch); live sets the same key.
            "as_of_ts": int(moment_ts),
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
            Open timestamp of the current eval candle (ms epoch).  The
            snapshot's moment is that candle's close, ``current_ts + eval_ms``;
            timeframe and HTF blocks hold only candles closed by then.
        """
        moment_ts = current_ts + self._eval_ms
        # ── LTF indicators (closed window + incomplete last candle) ───
        ltf_raw = [self._candle_to_row(c) for c in closed_ltf_window]
        ltf_raw.append(self._candle_to_row(incomplete_candle))
        indicators = MarketService._compute_indicators(ltf_raw)
        indicators["structure"] = MarketService._compute_structure(ltf_raw)
        indicators["forming_candle"] = (
            self._candle_to_dict(incomplete_candle)
            if incomplete_candle.ts + self._ltf_ms > moment_ts
            else None
        )

        # ── HTF alignment (closed HTF candles only) ───────────────────
        # Flattened regime scalars (adx_htf / choppiness_htf) mirror live.
        self._attach_htf(indicators, self._htf_cache, self._htf_timeframe, moment_ts)

        # ── Per-strategy analysis timeframes ─────────────────────────
        timeframes = self._timeframe_blocks(moment_ts, forming_from_ts=current_ts)

        # ── Assemble snapshot ─────────────────────────────────────────
        last_price = float(incomplete_candle.close)
        return {
            "generated_at": incomplete_candle.dt.isoformat(),
            "as_of_ts": int(moment_ts),
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
    def _candle_to_dict(c: Candle) -> dict[str, Any]:
        """Candle as the compact ``{ts, open, high, low, close, volume}`` dict."""
        return {
            "ts": int(c.ts),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
        }

    @staticmethod
    def _candle_to_row(c: Candle) -> list[Any]:
        """Convert a Candle to the raw OKX row format ``[ts, o, h, l, c, v]``."""
        return [c.ts, c.open, c.high, c.low, c.close, c.volume]
