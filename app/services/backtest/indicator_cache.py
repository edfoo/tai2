"""Precomputed indicator cache for the backtest engine.

The backtest loop rebuilds the full pandas-ta indicator set on the growing
candle window at every step (``SnapshotBuilder.build`` →
``MarketService._compute_indicators``).  That is O(n²) and dominates runtime
(~30 ms per call × 3-4 calls per step), which makes parameter sweeps
impractical.  This module precomputes each indicator series **once** over the
full candle array and serves per-step blocks by slicing.

Correctness
-----------
Every pandas-ta indicator used by ``_compute_indicators`` is *causal* — the
value at index ``i`` depends only on data at indices ``<= i``.  Slicing a
series precomputed over the full array to ``[:i+1]`` therefore yields exactly
the same values as recomputing over that prefix.  ``value_area`` and
``session_vwap`` are window-dependent but cheap, so they are recomputed per
step from the sliced frame (inside ``_assemble_indicators``).
``_compute_structure`` is likewise cheap and recomputed per step.

The cache is only valid for **pure-prefix** windows (``rows[:i+1]``).  The
finer-LTF loop's synthetic in-progress candle is *not* a prefix of the fetched
array, so that one block is still computed directly (see
``SnapshotBuilder.build_with_incomplete_ltf``).
"""

from __future__ import annotations

from bisect import bisect_right
from typing import Any

import pandas as pd

from app.services.market_service import MarketService


class IndicatorCache:
    """Serves per-step indicator blocks from a single precomputed series set.

    Parameters
    ----------
    rows:
        Raw OKX candle rows ``[ts, open, high, low, close, volume, ...]`` in
        ascending time order (extra columns are ignored).
    """

    def __init__(self, rows: list[list[Any]]) -> None:
        normalized = [list(r[:6]) for r in rows if len(r) >= 6]
        self._rows = normalized
        self._ts = [int(float(r[0])) for r in normalized]
        # The cache is only valid when the input is already in ascending time
        # order: then every prefix ``rows[:i+1]`` is also a prefix of the
        # sorted array, so a sliced full-array series equals a prefix
        # recompute.  If the input is unsorted (or has duplicate timestamps
        # that reorder under a stable sort), fall back to exact per-step
        # recompute — preserving the original positional-window semantics.
        self._sorted = all(
            self._ts[i] <= self._ts[i + 1] for i in range(len(self._ts) - 1)
        )
        if self._sorted:
            self._df = self._build_df(normalized)
            self._series: dict[str, Any] = (
                MarketService._compute_indicator_series(self._df)
                if not self._df.empty
                else {}
            )
            # Precompute the ``ohlcv`` compact list once; per-step blocks slice
            # it instead of rebuilding an O(n) list of dicts every call.
            self._ohlcv_all: list[dict[str, Any]] = [
                {
                    "ts": int(ts.timestamp() * 1000),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": float(v),
                }
                for ts, o, h, l, c, v in zip(
                    self._df.index,
                    self._df["open"],
                    self._df["high"],
                    self._df["low"],
                    self._df["close"],
                    self._df["volume"],
                )
            ]
        else:
            self._df = pd.DataFrame()
            self._series = {}
            self._ohlcv_all = []
        # 1-slot memo of the last assembled block.  Within a single snapshot
        # build the same (cache, end_idx) is requested more than once (the LTF
        # block and a strategy's own analysis-timeframe block often share the
        # same window), and ``_assemble_indicators`` rebuilds the full
        # ``ohlcv`` list each call — memoising halves that work.
        self._memo_idx: int | None = None
        self._memo_block: dict[str, Any] | None = None

    @staticmethod
    def _build_df(rows: list[list[Any]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", errors="coerce")
        for column in ["open", "high", "low", "close", "volume"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        return df.sort_values("ts").set_index("ts")

    def __len__(self) -> int:
        return len(self._rows)

    def rows_upto(self, end_idx: int) -> list[list[Any]]:
        """Raw rows for the window ``rows[:end_idx+1]``."""
        return self._rows[: end_idx + 1]

    def end_idx_for_ts(self, ts: int) -> int:
        """Index of the last candle with ``ts <= ts`` (``-1`` if none)."""
        return bisect_right(self._ts, ts) - 1

    def block_at(self, end_idx: int) -> dict[str, Any]:
        """Indicator block for the window ``rows[:end_idx+1]``.

        Byte-for-byte equivalent to
        ``MarketService._compute_indicators(rows[:end_idx+1])``.
        """
        if end_idx < 0 or not self._rows:
            return MarketService._empty_indicators()
        end_idx = min(end_idx, len(self._rows) - 1)
        if not self._sorted:
            # Unsorted input: positional prefixes are not sorted prefixes, so
            # the precomputed series cannot be sliced.  Recompute exactly.
            return MarketService._compute_indicators(self._rows[: end_idx + 1])
        if self._memo_idx == end_idx and self._memo_block is not None:
            # Return a shallow copy so callers can attach keys (structure,
            # HTF layer) without corrupting the memo.
            return dict(self._memo_block)
        df = self._df.iloc[: end_idx + 1]
        series: dict[str, Any] = {}
        for key, value in self._series.items():
            if value is None:
                series[key] = None
                continue
            sliced = value.iloc[: end_idx + 1]
            # pandas-ta returns ``None`` (not an all-NaN series) when the
            # window is shorter than the indicator's required length.  A
            # sliced full-array series would instead be all-NaN, so collapse
            # it back to ``None`` to match the recompute exactly.
            na_all = sliced.isna().all()
            if hasattr(na_all, "all"):  # DataFrame → Series of per-column bools
                na_all = na_all.all()
            series[key] = None if bool(na_all) else sliced
        block = MarketService._assemble_indicators(
            df, series, ohlcv_compact=self._ohlcv_all[: end_idx + 1]
        )
        self._memo_idx = end_idx
        self._memo_block = block
        return dict(block)

    def block_for_ts(self, ts: int) -> dict[str, Any] | None:
        """Indicator block for the largest prefix with candle ``ts <= ts``."""
        idx = self.end_idx_for_ts(ts)
        if idx < 0:
            return None
        return self.block_at(idx)
