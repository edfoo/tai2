"""Pluggable strategy interface for the Launcher.

Each strategy evaluates the current market snapshot and returns a directional
signal ("buy", "sell", or None).  All enabled strategies run concurrently on
each scheduler tick — multiple strategies can fire on the same symbol at the
same time, each opening its own position with its own TP/SL.

To add a new strategy:
  1. Create a new file in ``app/services/strategies/`` implementing the
     ``Strategy`` protocol.
  2. Register it in ``MarketService._strategies``.
  3. Add a card on the STRATEGY page in ``pages.py`` with its own
     enable/disable switch and config fields.
  4. The config is namespaced under ``config["strategies"][<strategy_name>]``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


def compute_bb_bandwidth_percentile(
    ohlcv_compact: list[dict[str, Any]],
    current_bandwidth: float | None,
    lookback: int = 50,
) -> float | None:
    """Compute the percentile rank of the current BB bandwidth.

    Uses a simple rolling standard deviation of close prices over a 20-bar
    window as a proxy for BB bandwidth at each historical bar.  This avoids
    recomputing full pandas-ta BB for every historical candle (expensive in
    backtest where this runs on every step).

    Parameters
    ----------
    ohlcv_compact:
        List of candle dicts with ``"close"`` keys (from the snapshot's
        ``indicators["ohlcv"]``).
    current_bandwidth:
        The current BB bandwidth % (from ``bollinger_bands``).  If None,
        the function returns None (regime gate will be skipped).
    lookback:
        Number of historical candles to compute the percentile over.

    Returns
    -------
    Percentile rank (0–100) of the current bandwidth relative to the
    last ``lookback`` bars' bandwidths.  Low values (< 30) = low-volatility
    chop regime (good for mean reversion).  High values (> 60) = volatility
    expansion (good for spike continuation).
    """
    if current_bandwidth is None or not ohlcv_compact or lookback < 5:
        return None

    closes: list[float] = []
    for c in ohlcv_compact:
        if isinstance(c, dict):
            _cl = c.get("close")
            if _cl is not None:
                try:
                    closes.append(float(_cl))
                except (TypeError, ValueError):
                    pass

    if len(closes) < lookback + 20:
        return None

    # Compute rolling 20-bar standard deviation as a BB-bandwidth proxy.
    # BB bandwidth ≈ 4 * stdev / mean * 100 (for 2-std BB).
    window = 20
    bandwidths: list[float] = []
    for i in range(window - 1, len(closes)):
        chunk = closes[i - window + 1 : i + 1]
        _mean = sum(chunk) / window
        if _mean <= 0:
            continue
        _var = sum((x - _mean) ** 2 for x in chunk) / window
        _std = _var ** 0.5
        _bw = (4.0 * _std / _mean) * 100.0  # 2-std upper+lower / middle * 100
        bandwidths.append(_bw)

    if len(bandwidths) < 5:
        return None

    # Use only the last `lookback` bandwidths for the percentile.
    recent = bandwidths[-lookback:]
    count_below = sum(1 for b in recent if b < current_bandwidth)
    return (count_below / len(recent)) * 100.0


def fractal_swings(
    candles: list[dict[str, Any]],
    *,
    pivot_bars: int = 3,
) -> tuple[float | None, float | None, bool]:
    """Identify fractal pivot swing low/high from a candle list.

    A candle ``i`` is a **pivot low** when its ``low`` is the strict minimum of
    the window ``[i - pivot_bars, i + pivot_bars]``, and a **pivot high** when
    its ``high`` is the strict maximum of that window.  These are real, visible,
    freshly-respected levels that stop-hunters actually cluster around — unlike
    a trailing N-bar min/max which is a range edge inside the noise.

    Parameters
    ----------
    candles:
        List of candle dicts with ``"low"``/``"high"`` keys.  The most recent
        candle (the potential sweep candle) is excluded from pivot detection
        so it cannot qualify a forming wick as structure.
    pivot_bars:
        Number of bars on each side of a pivot to require (so a fractal needs
        2 * pivot_bars + 1 candles).

    Returns
    -------
    ``(swing_low, swing_high, used_fallback)``:
        - ``swing_low`` / ``swing_high``: the extreme pivot low/high, or
          ``None`` when no pivots of that type were found.
        - ``used_fallback``: ``True`` when no pivot structure existed and the
          values were derived from the trailing min/max instead (so callers
          can log that the legacy path was used).
    """
    pivot_lows: list[float] = []
    pivot_highs: list[float] = []
    n = int(pivot_bars) if pivot_bars else 0

    # Exclude the last candle: it is the forming/current sweep candle.
    body = candles[:-1]
    for i in range(1, len(body) - 1):
        lo = body[i].get("low") if isinstance(body[i], dict) else None
        hi = body[i].get("high") if isinstance(body[i], dict) else None
        # Neighbours on both sides, excluding the pivot candle itself.
        lo_idx = list(range(max(0, i - n), i)) + list(range(i + 1, min(len(body), i + n + 1)))
        if lo is not None:
            window_lows = [
                body[j].get("low") if isinstance(body[j], dict) else None
                for j in lo_idx
            ]
            try:
                if all(w is not None and lo < w for w in window_lows):
                    pivot_lows.append(float(lo))
            except TypeError:
                pass
        if hi is not None:
            window_highs = [
                body[j].get("high") if isinstance(body[j], dict) else None
                for j in lo_idx
            ]
            try:
                if all(w is not None and hi > w for w in window_highs):
                    pivot_highs.append(float(hi))
            except TypeError:
                pass

    swing_low = min(pivot_lows) if pivot_lows else None
    swing_high = max(pivot_highs) if pivot_highs else None
    used_fallback = False

    if swing_low is None or swing_high is None:
        # Legacy fallback: trailing min/max (excludes the current candle).
        lows = [
            float(c["low"])
            for c in body
            if isinstance(c, dict) and c.get("low") is not None
        ]
        highs = [
            float(c["high"])
            for c in body
            if isinstance(c, dict) and c.get("high") is not None
        ]
        used_fallback = True
        if swing_low is None and lows:
            swing_low = min(lows)
        if swing_high is None and highs:
            swing_high = max(highs)

    return swing_low, swing_high, used_fallback


def resolve_analysis_block(
    sym_data: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Return the indicator block a strategy should analyze.

    A strategy may set ``analysis_timeframe`` in its config to analyze on a
    specific bar.  When set, the snapshot exposes that bar's indicator block
    under ``sym_data["timeframes"][<tf>]``; this helper resolves and returns
    it.  When ``analysis_timeframe`` is unset (or the block is unavailable),
    the global ``sym_data["indicators"]`` block is returned — preserving
    legacy behaviour.

    Parameters
    ----------
    sym_data:
        The per-symbol snapshot entry (``snapshot["market_data"][symbol]``).
    cfg:
        This strategy's merged config (must contain ``analysis_timeframe``).

    Returns
    -------
    The indicator dict to read signal inputs from.
    """
    tf = cfg.get("analysis_timeframe")
    if tf:
        timeframes = sym_data.get("timeframes") or {}
        block = timeframes.get(str(tf))
        if block is not None:
            return block
    return sym_data.get("indicators") or {}


@dataclass
class StrategySignal:
    """Signal returned by a strategy evaluation.

    Attributes
    ----------
    direction:
        "buy" or "sell".
    strategy_name:
        Name of the strategy that fired (set automatically by the registry).
    tp_pct:
        Optional take-profit % for this strategy.  If None, the Launcher's
        global TP/SL or algo orders are used.
    sl_pct:
        Optional stop-loss % for this strategy.
    rationale:
        Human-readable reason for the signal (shown in logs/notifications).
    """

    direction: str
    strategy_name: str = ""
    tp_pct: float | None = None
    sl_pct: float | None = None
    rationale: str = ""


@runtime_checkable
class Strategy(Protocol):
    """Minimal interface that every Launcher strategy must implement."""

    name: str

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal, or None if no signal fires.

        Parameters
        ----------
        symbol:
            Trading pair, e.g. ``"BTC-USDT-SWAP"``.
        snapshot:
            The full market snapshot (``_last_full_snapshot``).
        config:
            This strategy's own config dict, already namespaced
            (e.g. ``config["strategies"]["mean_reversion"]``).
        helpers:
            Shared utility methods from MarketService (price extraction,
            debug emit, etc.) so strategies don't need a direct reference
            to MarketService.

        Returns
        -------
        A ``StrategySignal`` with direction "buy" or "sell", or ``None``.
        """
        ...


class StrategyHelpers:
    """Lightweight helper bag passed to every strategy.

    Avoids giving strategies a full MarketService reference while still
    providing the utilities they need.
    """

    def __init__(
        self,
        *,
        extract_float: Any,
        emit_debug: Any,
        get_last_price: Any,
        compute_footprint: Any | None = None,
    ) -> None:
        self._extract_float = extract_float
        self._emit_debug = emit_debug
        self._get_last_price = get_last_price
        self._compute_footprint = compute_footprint

    def extract_float(self, value: Any) -> float | None:
        return self._extract_float(value)

    def emit_debug(self, msg: str) -> None:
        self._emit_debug(msg)

    def get_last_price(self, symbol: str) -> float | None:
        return self._get_last_price(symbol)

    def compute_footprint(self, symbol: str) -> dict[str, Any]:
        """Compute footprint data for a symbol, or return empty dict if unavailable."""
        if self._compute_footprint is not None:
            return self._compute_footprint(symbol)
        return {}
