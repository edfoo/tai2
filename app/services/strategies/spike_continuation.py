"""Spike Continuation (Momentum Scalp) strategy.

Rides short-lived volatility spikes for 3-5% before they revert.
Enters WITH the spike (not against it) when volume confirms strong momentum,
exits before exhaustion signs appear.

This is the mirror image of Mean Reversion: MR waits for exhaustion, this
strategy enters on the explosion and exits before the reversion.

Critical design goal: avoid entering at the TOP of a spike. The strategy
verifies that momentum is still ACCELERATING (not peaking) before entering.
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile
from .defaults import merged_config


class SpikeContinuationStrategy:
    """Momentum scalp strategy that rides volume-driven spikes.

    Config keys (all live under ``config["strategies"]["spike_continuation"]``):
      - ``enabled`` (bool): master switch
      - ``volume_rsi_min`` (float, default 75): volume RSI must be above this
        to confirm the spike is volume-driven (not just a thin wick)
      - ``rsi_min`` (float, default 58): RSI must be above this for buys
        (momentum confirmed but not yet extreme)
      - ``rsi_max`` (float, default 70): don't enter if RSI is already extreme
        (that's Mean Reversion territory / late top)
      - ``require_bb_breakout`` (bool, default True): price must be beyond BB band
      - ``require_candle_strength`` (bool, default True): candle must close near
        its high (for buys) or low (for sells) — strong momentum, no rejection
      - ``candle_strength_pct`` (float, default 75): close must be within this
        % of the candle range from the direction (75 = close is in top 25% for buys)
      - ``min_bb_bandwidth`` (float, default 3.0): only enter when bands are wide
        enough to suggest a real volatility expansion
      - ``tp_pct`` (float, default 4.0): take-profit as % price move
      - ``sl_pct`` (float, default 3.0): stop-loss as % price move
      - ``max_adx`` (float, default 0): legacy hard ADX ceiling. 0 = disabled.
      - ``max_adx_for_entry`` (float, default 30): late-entry killer. Blocks when
        ADX is already this high (trend is mature). 0 = disabled.

    Momentum acceleration filters (prevent entering at the top of a spike):
      - ``require_momentum_acceleration`` (bool, default True): current candle
        body must be larger than the average body of the last N candles
      - ``acceleration_lookback`` (int, default 3): number of prior candles to
        average for the acceleration comparison
      - ``acceleration_min_ratio`` (float, default 1.5): current body must be
        at least this multiple of the average recent body
      - ``require_rsi_rising`` (bool, default True): RSI must be rising vs the
        previous candle (momentum still building, not fading). Uses the actual
        RSI series, not a candle-direction proxy.
      - ``require_volume_rsi_rising`` (bool, default True): volume RSI must be
        rising vs the previous candle (volume momentum still building)
      - ``max_spike_extension_pct`` (float, default 2.5): block entry if price
        has already moved more than this % from the start of the spike.
        Prevents entering at the top of an extended move. 0 = disabled.
      - ``spike_lookback`` (int, default 5): candles to look back to find the
        spike origin (lowest low for buys, highest high for sells)
      - ``require_regime`` (bool, default True)
      - ``min_bb_bandwidth_percentile`` (float, default 60)
      - ``use_atr_sizing`` (bool, default True)
      - ``atr_tp_multiplier`` (float, default 2.2)
      - ``atr_sl_multiplier`` (float, default 2.0)
      - ``min_atr_pct`` (float, default 1.2)
      - ``flip_launcher_direction`` (str, default None): invert the
        Launcher's trade direction before execution. One of "both",
        "from_long" (only BUY→SELL), "from_short" (only SELL→BUY),
        or None to disable. TP/SL are mirrored around last_price so
        they land on the correct side for the flipped direction.
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

        # Merge caller config over the canonical defaults so any missing key
        # falls back to an acceptable, validated value.
        cfg = merged_config(config, self.name)

        volume_rsi_min = helpers.extract_float(cfg.get("volume_rsi_min"))
        if volume_rsi_min is None:
            volume_rsi_min = 72.0
        rsi_min = helpers.extract_float(cfg.get("rsi_min"))
        if rsi_min is None:
            rsi_min = 55.0
        rsi_max = helpers.extract_float(cfg.get("rsi_max"))
        if rsi_max is None:
            rsi_max = 72.0
        require_bb_breakout = bool(cfg.get("require_bb_breakout", True))
        require_candle_strength = bool(cfg.get("require_candle_strength", True))
        candle_strength_pct = helpers.extract_float(cfg.get("candle_strength_pct"))
        if candle_strength_pct is None:
            candle_strength_pct = 70.0
        min_bb_bandwidth = helpers.extract_float(cfg.get("min_bb_bandwidth"))
        if min_bb_bandwidth is None:
            min_bb_bandwidth = 3.0
        max_adx = helpers.extract_float(cfg.get("max_adx"))
        if max_adx is None:
            max_adx = 0.0
        # Late-entry killer: ADX already this high means the move is mature.
        max_adx_for_entry = helpers.extract_float(cfg.get("max_adx_for_entry"))
        if max_adx_for_entry is None:
            max_adx_for_entry = 32.0
        # ── Regime gate (BB bandwidth percentile) ──────────────────────
        # SC works best in high-volatility expansion.  When require_regime
        # is True, the current BB bandwidth must be above
        # min_bb_bandwidth_percentile relative to the last N candles
        # (e.g. > 60th percentile = volatility expansion).
        require_regime = bool(cfg.get("require_regime", True))
        min_bb_bandwidth_percentile = helpers.extract_float(cfg.get("min_bb_bandwidth_percentile"))
        if min_bb_bandwidth_percentile is None:
            min_bb_bandwidth_percentile = 55.0
        regime_lookback = helpers.extract_float(cfg.get("regime_lookback"))
        if regime_lookback is None:
            regime_lookback = 50
        # ── ATR-scaled TP/SL ────────────────────────────────────────────
        # When use_atr_sizing is True, TP/SL are computed as
        # multiplier × ATR% instead of fixed percentages.
        # SC uses wider SL (~2.0 ATR) to avoid being stopped by noise.
        use_atr_sizing = bool(cfg.get("use_atr_sizing", True))
        atr_tp_multiplier = helpers.extract_float(cfg.get("atr_tp_multiplier"))
        if atr_tp_multiplier is None:
            atr_tp_multiplier = 2.2
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 2.0
        # ── Minimum ATR% filter ───────────────────────────────────────
        # Skip entries on coins with ATR% below this threshold — too quiet
        # for a real spike.  0 = disabled.
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0

        # Momentum acceleration filters
        require_momentum_acceleration = bool(cfg.get("require_momentum_acceleration", True))
        _acceleration_lookback = helpers.extract_float(cfg.get("acceleration_lookback"))
        acceleration_lookback = int(_acceleration_lookback) if _acceleration_lookback is not None else 3
        acceleration_min_ratio = helpers.extract_float(cfg.get("acceleration_min_ratio"))
        if acceleration_min_ratio is None:
            acceleration_min_ratio = 1.3
        require_rsi_rising = bool(cfg.get("require_rsi_rising", True))
        require_volume_rsi_rising = bool(cfg.get("require_volume_rsi_rising", True))
        max_spike_extension_pct = helpers.extract_float(cfg.get("max_spike_extension_pct"))
        if max_spike_extension_pct is None:
            max_spike_extension_pct = 3.5
        _spike_lookback = helpers.extract_float(cfg.get("spike_lookback"))
        spike_lookback = int(_spike_lookback) if _spike_lookback is not None else 5

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        rsi = helpers.extract_float(indicators.get("rsi"))
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))

        # Volume RSI — confirms the spike is volume-driven
        volume_rsi_series = indicators.get("volume_rsi_series") or []
        volume_rsi_value: float | None = None
        volume_rsi_prev: float | None = None
        if volume_rsi_series:
            volume_rsi_value = helpers.extract_float(volume_rsi_series[-1])
            if len(volume_rsi_series) >= 2:
                volume_rsi_prev = helpers.extract_float(volume_rsi_series[-2])

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

        # OHLCV candles — for candle strength, momentum acceleration, and spike extension
        ohlcv_compact = indicators.get("ohlcv") or []

        # Candle strength — close should be near the high (buy) or low (sell)
        candle_strong_buy = False
        candle_strong_sell = False
        current_body: float | None = None
        current_close: float | None = None
        current_open: float | None = None
        if ohlcv_compact and isinstance(ohlcv_compact[-1], dict):
            _c = ohlcv_compact[-1]
            _high = helpers.extract_float(_c.get("high"))
            _low = helpers.extract_float(_c.get("low"))
            _close = helpers.extract_float(_c.get("close"))
            _open = helpers.extract_float(_c.get("open"))
            if _high is not None and _low is not None and _close is not None:
                current_close = _close
                _range = _high - _low
                if _range > 0:
                    # Close position in candle range: 0 = at low, 100 = at high
                    _close_pos = (_close - _low) / _range * 100.0
                    candle_strong_buy = _close_pos >= candle_strength_pct
                    candle_strong_sell = _close_pos <= (100.0 - candle_strength_pct)
                if _open is not None:
                    current_body = abs(_close - _open)
                    current_open = _open

        # Momentum acceleration — current candle body must be larger than recent average
        # This prevents entering at the top of a spike where the last candle is small
        momentum_accelerating = False
        avg_recent_body: float | None = None
        body_ratio: float | None = None
        if require_momentum_acceleration and current_body is not None and len(ohlcv_compact) >= 2:
            lookback_candles = ohlcv_compact[-(acceleration_lookback + 1):-1]
            recent_bodies: list[float] = []
            for c in lookback_candles:
                if isinstance(c, dict):
                    _o = helpers.extract_float(c.get("open"))
                    _cl = helpers.extract_float(c.get("close"))
                    if _o is not None and _cl is not None:
                        recent_bodies.append(abs(_cl - _o))
            if recent_bodies:
                avg_recent_body = sum(recent_bodies) / len(recent_bodies)
                if avg_recent_body > 0:
                    body_ratio = current_body / avg_recent_body
                    momentum_accelerating = body_ratio >= acceleration_min_ratio

        # RSI rising — momentum still building (direction-aware)
        # Uses the actual RSI series: rsi_series[-1] > rsi_series[-2] for buys,
        # rsi_series[-1] < rsi_series[-2] for sells. Falls back to candle
        # direction (close > open) when the RSI series is unavailable.
        rsi_series_vals: list[float] = indicators.get("rsi_series") or []
        rsi_rising_buy = False
        rsi_rising_sell = False
        if require_rsi_rising:
            if len(rsi_series_vals) >= 2:
                _rsi_prev = helpers.extract_float(rsi_series_vals[-2])
                if rsi is not None and _rsi_prev is not None:
                    rsi_rising_buy = rsi > _rsi_prev
                    rsi_rising_sell = rsi < _rsi_prev
            elif current_close is not None and current_open is not None:
                # Fallback: no RSI series available (e.g. insufficient warmup)
                rsi_rising_buy = current_close > current_open
                rsi_rising_sell = current_close < current_open

        # Volume RSI rising — volume momentum still building
        volume_rsi_rising = False
        if require_volume_rsi_rising and volume_rsi_value is not None and volume_rsi_prev is not None:
            volume_rsi_rising = volume_rsi_value > volume_rsi_prev

        # Spike extension — don't enter if price has already moved too far from spike origin
        spike_extension_buy: float | None = None
        spike_extension_sell: float | None = None
        spike_extension_ok_buy = True
        spike_extension_ok_sell = True
        if max_spike_extension_pct > 0 and current_close is not None and len(ohlcv_compact) >= 2:
            lookback_candles = ohlcv_compact[-(spike_lookback + 1):-1]
            lows = [
                helpers.extract_float(c.get("low"))
                for c in lookback_candles
                if isinstance(c, dict) and helpers.extract_float(c.get("low")) is not None
            ]
            highs = [
                helpers.extract_float(c.get("high"))
                for c in lookback_candles
                if isinstance(c, dict) and helpers.extract_float(c.get("high")) is not None
            ]
            if lows:
                spike_origin_low = min(lows)
                if spike_origin_low > 0:
                    spike_extension_buy = (current_close - spike_origin_low) / spike_origin_low * 100.0
                    spike_extension_ok_buy = spike_extension_buy <= max_spike_extension_pct
            if highs:
                spike_origin_high = max(highs)
                if spike_origin_high > 0:
                    spike_extension_sell = (spike_origin_high - current_close) / spike_origin_high * 100.0
                    spike_extension_ok_sell = spike_extension_sell <= max_spike_extension_pct

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
        # Late-entry killer: ADX already elevated means the move is mature.
        if max_adx_for_entry > 0 and adx is not None and adx > max_adx_for_entry:
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal "
                f"(ADX={adx:.1f} > max_adx_for_entry={max_adx_for_entry:.1f} — late entry, trend mature)"
            )
            return None

        # Momentum acceleration gate — prevents entering at the top of a spike
        if require_momentum_acceleration:
            if current_body is None or avg_recent_body is None or avg_recent_body <= 0:
                helpers.emit_debug(
                    f"SpikeContinuation: {symbol} — no signal (body data unavailable for acceleration check)"
                )
                return None
            if body_ratio is not None and body_ratio < acceleration_min_ratio:
                helpers.emit_debug(
                    f"SpikeContinuation: {symbol} — no signal "
                    f"(momentum decelerating: body_ratio={body_ratio:.2f} < min={acceleration_min_ratio:.2f} — "
                    f"spike is peaking, not accelerating)"
                )
                return None

        # RSI rising gate (direction-aware)
        if require_rsi_rising and not rsi_rising_buy and not rsi_rising_sell:
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal (RSI not rising/falling in direction — momentum fading)"
            )
            return None

        # Volume RSI rising gate
        if require_volume_rsi_rising and not volume_rsi_rising:
            helpers.emit_debug(
                f"SpikeContinuation: {symbol} — no signal "
                f"(vol_rsi falling: {volume_rsi_prev:.1f} → {volume_rsi_value:.1f} — volume momentum fading)"
                if volume_rsi_prev is not None else
                f"SpikeContinuation: {symbol} — no signal (vol_rsi_prev unavailable)"
            )
            return None

        # Spike extension gate — don't enter at the top of an extended move
        if max_spike_extension_pct > 0:
            if not spike_extension_ok_buy and not spike_extension_ok_sell:
                _ext = spike_extension_buy if spike_extension_buy is not None else spike_extension_sell
                helpers.emit_debug(
                    f"SpikeContinuation: {symbol} — no signal "
                    f"(spike already extended: {_ext:.2f}% > max={max_spike_extension_pct:.2f}% — "
                    f"entering at the top, not the start)"
                )
                return None

        # Buy signal: RSI in momentum zone (not yet extreme), volume confirms,
        # price beyond BB upper, candle closes strong, momentum accelerating

        # ── Regime gate: BB bandwidth percentile ──────────────────────
        # SC works best in high-volatility expansion (high bandwidth percentile).
        bw_percentile = compute_bb_bandwidth_percentile(
            ohlcv_compact, bb_bandwidth, lookback=int(regime_lookback)
        )
        regime_ok = (
            not require_regime
            or (bw_percentile is not None and bw_percentile >= min_bb_bandwidth_percentile)
        )

        # ── Minimum ATR% filter ──────────────────────────────────────
        # Skip entries on coins too quiet for a real spike.
        atr_pct_value = helpers.extract_float(indicators.get("atr_pct"))
        atr_ok = (
            min_atr_pct <= 0
            or (atr_pct_value is not None and atr_pct_value >= min_atr_pct)
        )

        buy_signal = (
            rsi_min <= rsi <= rsi_max
            and bb_breakout_buy
            and (not require_candle_strength or candle_strong_buy)
            and (not require_momentum_acceleration or momentum_accelerating)
            and (not require_rsi_rising or rsi_rising_buy)
            and (not require_volume_rsi_rising or volume_rsi_rising)
            and spike_extension_ok_buy
            and regime_ok
            and atr_ok
        )
        # Sell signal: mirror
        sell_signal = (
            (100.0 - rsi_max) <= rsi <= (100.0 - rsi_min)
            and bb_breakout_sell
            and (not require_candle_strength or candle_strong_sell)
            and (not require_momentum_acceleration or momentum_accelerating)
            and (not require_rsi_rising or rsi_rising_sell)
            and (not require_volume_rsi_rising or volume_rsi_rising)
            and spike_extension_ok_sell
            and regime_ok
            and atr_ok
        )

        # ── Compute effective TP/SL ────────────────────────────────────
        # ATR-scaled TP/SL overrides the static config values when enabled.
        _static_tp = helpers.extract_float(config.get("tp_pct"))
        _static_sl = helpers.extract_float(config.get("sl_pct"))
        _effective_tp = _static_tp
        _effective_sl = _static_sl
        if use_atr_sizing:
            atr_pct = helpers.extract_float(indicators.get("atr_pct"))
            if atr_pct is not None and config.get("use_adaptive_atr", False):
                if atr_pct < 1.5:
                    atr_pct *= 1.20
                elif atr_pct < 3.0:
                    atr_pct *= 1.80
                else:
                    atr_pct *= 2.50
            if atr_pct is not None and atr_pct > 0:
                _effective_tp = atr_tp_multiplier * atr_pct
                _effective_sl = atr_sl_multiplier * atr_pct

        if buy_signal:
            return StrategySignal(
                direction="buy",
                strategy_name=self.name,
                tp_pct=_effective_tp,
                sl_pct=_effective_sl,
                rationale=f"SpikeContinuation BUY: RSI={rsi:.1f} vol_rsi={volume_rsi_value:.1f} "
                          f"(accelerating, not peaking)",
            )
        if sell_signal:
            return StrategySignal(
                direction="sell",
                strategy_name=self.name,
                tp_pct=_effective_tp,
                sl_pct=_effective_sl,
                rationale=f"SpikeContinuation SELL: RSI={rsi:.1f} vol_rsi={volume_rsi_value:.1f} "
                          f"(accelerating, not peaking)",
            )

        # Debug breakdown
        parts = [f"RSI={rsi:.1f} (need {rsi_min}-{rsi_max} or {100-rsi_max}-{100-rsi_min})"]
        parts.append(f"vol_rsi={volume_rsi_value:.1f}" if volume_rsi_value is not None else "vol_rsi=n/a")
        if require_bb_breakout:
            parts.append(f"BB breakout={'ok' if (bb_breakout_buy or bb_breakout_sell) else 'blocked'}")
        if require_candle_strength:
            parts.append(f"candle_strong={'ok' if (candle_strong_buy or candle_strong_sell) else 'blocked'}")
        if require_momentum_acceleration and body_ratio is not None:
            parts.append(f"accel={body_ratio:.2f}x (min {acceleration_min_ratio:.2f}x)")
        if require_rsi_rising:
            parts.append(f"rsi_rising={'ok' if (rsi_rising_buy or rsi_rising_sell) else 'blocked'}")
        if require_volume_rsi_rising:
            parts.append(f"vol_rsi_rising={'ok' if volume_rsi_rising else 'blocked'}")
        if max_spike_extension_pct > 0:
            _ext = spike_extension_buy if spike_extension_buy is not None else spike_extension_sell
            parts.append(f"spike_ext={_ext:.2f}%" if _ext is not None else "spike_ext=n/a")
        if min_bb_bandwidth > 0:
            parts.append(f"BB_bw={bb_bandwidth:.2f}%" if bb_bandwidth is not None else "BB_bw=n/a")
        if max_adx > 0 or max_adx_for_entry > 0:
            parts.append(f"ADX={adx:.1f}" if adx is not None else "ADX=n/a")
            if max_adx_for_entry > 0:
                parts.append(f"max_adx_entry={max_adx_for_entry:.0f}")
        helpers.emit_debug(f"SpikeContinuation: {symbol} — no signal ({', '.join(parts)})")
        return None
