"""Spike Continuation (Momentum Scalp) strategy.

Rides short-lived volatility spikes for 3-5% before they revert.
Enters WITH the spike (not against it) when volume confirms strong momentum,
exits before exhaustion signs appear.

This is the mirror image of Mean Reversion: MR waits for exhaustion, this
strategy enters on the explosion and exits before the reversion.
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal


class SpikeContinuationStrategy:
    """Momentum scalp strategy that rides volume-driven spikes.

    Config keys (all live under ``config["strategies"]["spike_continuation"]``):
      - ``enabled`` (bool): master switch
      - ``volume_rsi_min`` (float, default 75): volume RSI must be above this
        to confirm the spike is volume-driven (not just a thin wick)
      - ``rsi_min`` (float, default 55): RSI must be above this for buys
        (momentum confirmed but not yet extreme — we enter before RSI > 80)
      - ``rsi_max`` (float, default 75): don't enter if RSI is already extreme
        (that's Mean Reversion territory)
      - ``require_bb_breakout`` (bool, default True): price must be beyond BB band
      - ``require_candle_strength`` (bool, default True): candle must close near
        its high (for buys) or low (for sells) — strong momentum, no rejection
      - ``candle_strength_pct`` (float, default 70): close must be within this
        % of the candle range from the direction (70 = close is in top 30% for buys)
      - ``min_bb_bandwidth`` (float, default 3.0): only enter when bands are wide
        enough to suggest a real volatility expansion
      - ``tp_pct`` (float, default 3.0): take-profit as % price move
      - ``sl_pct`` (float, default 5.0): stop-loss as % price move
      - ``max_adx`` (float, default 40): don't enter if trend is too strong
        (we want momentum spikes, not full trends — those won't revert)
    """

    name = "spike_continuation"

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal for spike continuation, or None."""
        if not bool(config.get("enabled", False)):
            return None

        volume_rsi_min = helpers.extract_float(config.get("volume_rsi_min")) or 75.0
        rsi_min = helpers.extract_float(config.get("rsi_min")) or 55.0
        rsi_max = helpers.extract_float(config.get("rsi_max")) or 75.0
        require_bb_breakout = bool(config.get("require_bb_breakout", True))
        require_candle_strength = bool(config.get("require_candle_strength", True))
        candle_strength_pct = helpers.extract_float(config.get("candle_strength_pct")) or 70.0
        min_bb_bandwidth = helpers.extract_float(config.get("min_bb_bandwidth")) or 3.0
        max_adx = helpers.extract_float(config.get("max_adx")) or 40.0

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        rsi = helpers.extract_float(indicators.get("rsi"))
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))

        # Volume RSI — confirms the spike is volume-driven
        volume_rsi_series = indicators.get("volume_rsi_series") or []
        volume_rsi_value: float | None = None
        if volume_rsi_series:
            volume_rsi_value = helpers.extract_float(volume_rsi_series[-1])

        # Bollinger Bands — confirms breakout
        _bb = indicators.get("bollinger_bands") or {}
        bb_lower = helpers.extract_float(_bb.get("lower"))
        bb_upper = helpers.extract_float(_bb.get("upper"))
        bb_middle = helpers.extract_float(_bb.get("middle"))
        bb_last_price = helpers.get_last_price(symbol)
        bb_bandwidth: float | None = (
            (bb_upper - bb_lower) / bb_middle * 100.0
            if bb_upper is not None and bb_lower is not None and bb_middle and bb_middle > 0
            else None
        )

        # Candle strength — close should be near the high (buy) or low (sell)
        candle_strong_buy = False
        candle_strong_sell = False
        if require_candle_strength:
            ohlcv_compact = indicators.get("ohlcv") or []
            if ohlcv_compact and isinstance(ohlcv_compact[-1], dict):
                _c = ohlcv_compact[-1]
                _high = helpers.extract_float(_c.get("high"))
                _low = helpers.extract_float(_c.get("low"))
                _close = helpers.extract_float(_c.get("close"))
                if _high is not None and _low is not None and _close is not None:
                    _range = _high - _low
                    if _range > 0:
                        # Close position in candle range: 0 = at low, 100 = at high
                        _close_pos = (_close - _low) / _range * 100.0
                        # For buys: close should be in top (100 - candle_strength_pct)% of range
                        # e.g. candle_strength_pct=70 → close_pos must be > 70
                        candle_strong_buy = _close_pos >= candle_strength_pct
                        candle_strong_sell = _close_pos <= (100.0 - candle_strength_pct)

        # BB breakout checks
        bb_breakout_buy = (
            not require_bb_breakout
            or (bb_last_price is not None and bb_upper is not None and bb_last_price >= bb_upper)
        )
        bb_breakout_sell = (
            not require_bb_breakout
            or (bb_last_price is not None and bb_lower is not None and bb_last_price <= bb_lower)
        )

        # Early exits with debug
        if rsi is None:
            helpers.emit_debug(f"SpikeContinuation: {symbol} — no signal (RSI unavailable)")
            return None
        if volume_rsi_value is None or volume_rsi_value < volume_rsi_min:
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal "
                f"(vol_rsi={volume_rsi_value:.1f} < min={volume_rsi_min:.1f})"
                if volume_rsi_value is not None else
                f"SpikeContinuation: {symbol} — no signal (vol_rsi unavailable)"
            )
            return None
        if min_bb_bandwidth > 0 and (bb_bandwidth is None or bb_bandwidth < min_bb_bandwidth):
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal "
                f"(BB bandwidth={bb_bandwidth:.2f}% < min={min_bb_bandwidth:.2f}%)"
                if bb_bandwidth is not None else
                f"SpikeContinuation: {symbol} — no signal (BB bandwidth unavailable)"
            )
            return None
        if max_adx > 0 and adx is not None and adx > max_adx:
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal "
                f"(ADX={adx:.1f} > max={max_adx:.1f} — trend too strong, won't revert)"
            )
            return None

        # Buy signal: RSI in momentum zone (not yet extreme), volume confirms,
        # price beyond BB upper, candle closes strong
        buy_signal = (
            rsi_min <= rsi <= rsi_max
            and bb_breakout_buy
            and (not require_candle_strength or candle_strong_buy)
        )
        # Sell signal: mirror
        sell_signal = (
            (100.0 - rsi_max) <= rsi <= (100.0 - rsi_min)
            and bb_breakout_sell
            and (not require_candle_strength or candle_strong_sell)
        )

        if buy_signal:
            return StrategySignal(
                direction="buy",
                strategy_name=self.name,
                tp_pct=helpers.extract_float(config.get("tp_pct")),
                sl_pct=helpers.extract_float(config.get("sl_pct")),
                rationale=f"SpikeContinuation BUY: RSI={rsi:.1f} vol_rsi={volume_rsi_value:.1f}",
            )
        if sell_signal:
            return StrategySignal(
                direction="sell",
                strategy_name=self.name,
                tp_pct=helpers.extract_float(config.get("tp_pct")),
                sl_pct=helpers.extract_float(config.get("sl_pct")),
                rationale=f"SpikeContinuation SELL: RSI={rsi:.1f} vol_rsi={volume_rsi_value:.1f}",
            )

        # Debug breakdown
        parts = [f"RSI={rsi:.1f} (need {rsi_min}-{rsi_max} or {100-rsi_max}-{100-rsi_min})"]
        parts.append(f"vol_rsi={volume_rsi_value:.1f}" if volume_rsi_value is not None else "vol_rsi=n/a")
        if require_bb_breakout:
            parts.append(f"BB breakout={'ok' if (bb_breakout_buy or bb_breakout_sell) else 'blocked'}")
        if require_candle_strength:
            parts.append(f"candle_strong={'ok' if (candle_strong_buy or candle_strong_sell) else 'blocked'}")
        if min_bb_bandwidth > 0:
            parts.append(f"BB_bw={bb_bandwidth:.2f}%" if bb_bandwidth is not None else "BB_bw=n/a")
        if max_adx > 0:
            parts.append(f"ADX={adx:.1f}" if adx is not None else "ADX=n/a")
        helpers.emit_debug(f"SpikeContinuation: {symbol} — no signal ({', '.join(parts)})")
        return None
