"""Liquidity Sweep strategy.

Detects stop-hunt wicks: price pierces a recent swing low/high (triggering
stops) then closes back inside the range.  Enters in the opposite direction
of the sweep, expecting a rapid reversal as the stop-run exhausts.

This is the dominant microstructure pattern on alt-coin 15m charts: market
makers push price through a visible support/resistance level to trigger
stop-loss orders, fill their own orders at better prices, then reverse.

Entry conditions (long sweep example):
  1. Current candle low breaks below the prior N-bar low (sweep).
  2. Current candle closes back above that prior low (reclaim).
  3. Close is in the upper portion of the candle range (rejection wick).
  4. Optional: HTF trend alignment, volume confirmation, regime gate.

Exit:
  - TP: opposite end of the swept range (or ATR-scaled).
  - SL: just beyond the sweep wick (or ATR-scaled).
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile


class LiquiditySweepStrategy:
    """Stop-hunt reversal strategy.

    Config keys (all live under ``config["strategies"]["liquidity_sweep"]``):
      - ``enabled`` (bool): master switch
      - ``lookback`` (int, default 20): bars to identify the swing high/low
        that gets swept.
      - ``sweep_buffer_pct`` (float, default 0.1): minimum % beyond the swing
        level the wick must penetrate to qualify as a sweep (0.1 = 0.1%).
        Prevents triggering on noise that barely touches the level.
      - ``reclaim_ratio`` (float, default 0.5): close must reclaim at least
        this fraction of the candle range back inside the range.
        0.5 = close in the upper 50% of the candle (for longs).
      - ``require_htf_trend`` (bool, default True): only take longs in HTF
        uptrends, shorts in downtrends.  Auto-disabled when no HTF data.
      - ``require_volume_spike`` (bool, default True): swept candle volume
        must exceed the recent average by ``volume_spike_ratio``.
      - ``volume_spike_ratio`` (float, default 1.5): current volume / avg
        recent volume must exceed this.
      - ``volume_lookback`` (int, default 10): bars for average volume.
      - ``max_adx`` (float, default 35): skip when ADX is very high (strong
        trend — sweep is more likely a real breakout, not a stop hunt).
      - ``require_regime`` (bool, default True): only enter when BB bandwidth
        percentile is below ``max_bb_bandwidth_percentile`` (chop/range
        regime where sweeps are most reliable).
      - ``max_bb_bandwidth_percentile`` (float, default 60): upper bandwidth
        percentile for regime gate.
      - ``regime_lookback`` (int, default 50): bars for percentile.
      - ``use_structural_sizing`` (bool, default True): use structural TP/SL
        based on the swept swing levels instead of (or clamped by) ATR.
        TP targets the opposite swing extreme (swing_high for longs,
        swing_low for shorts).  SL sits just beyond the sweep wick.
        ATR clamps both to a sane range (see below).  When structural
        levels are unavailable, falls back to ATR sizing.
      - ``structural_sl_buffer_atr`` (float, default 0.15): SL is placed
        this many ATR units *beyond* the sweep wick low/high so it's not
        sitting exactly at the wick (which would get wicked out easily).
      - ``atr_min_tp_mult`` (float, default 0.5): structural TP distance
        must be at least this × ATR% from entry (prevents tiny TPs).
      - ``atr_max_tp_mult`` (float, default 4.0): structural TP distance
        is capped at this × ATR% from entry (prevents unreachable TPs).
      - ``atr_min_sl_mult`` (float, default 0.3): structural SL distance
        must be at least this × ATR% from entry (prevents instant stops).
      - ``atr_max_sl_mult`` (float, default 3.0): structural SL distance
        is capped at this × ATR% from entry (prevents huge risk).
      - ``use_atr_sizing`` (bool, default True): use ATR-scaled TP/SL.
        Used as fallback when structural levels are unavailable or when
        ``use_structural_sizing`` is False.
      - ``atr_tp_multiplier`` (float, default 1.5): TP = multiplier × ATR%.
      - ``atr_sl_multiplier`` (float, default 1.2): SL = multiplier × ATR%.
        Tighter than MR/SC because the sweep wick is the invalidation.
      - ``min_atr_pct`` (float, default 0.8): skip dead coins.
      - ``tp_pct`` (float, default None): static TP % fallback.
      - ``sl_pct`` (float, default None): static SL % fallback.
      - ``flip_launcher_direction`` (str, default None): invert the
        Launcher's trade direction before execution. One of "both",
        "from_long" (only BUY→SELL), "from_short" (only SELL→BUY),
        or None to disable. TP/SL are mirrored around last_price so
        they land on the correct side for the flipped direction.
    """

    name = "liquidity_sweep"

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal for a liquidity sweep, or None."""
        if not bool(config.get("enabled", False)):
            return None

        # ── Config ────────────────────────────────────────────────────
        _lookback = helpers.extract_float(config.get("lookback"))
        lookback = int(_lookback) if _lookback is not None else 20
        sweep_buffer_pct = helpers.extract_float(config.get("sweep_buffer_pct"))
        if sweep_buffer_pct is None:
            sweep_buffer_pct = 0.1
        reclaim_ratio = helpers.extract_float(config.get("reclaim_ratio"))
        if reclaim_ratio is None:
            reclaim_ratio = 0.5
        require_htf_trend = bool(config.get("require_htf_trend", True))
        require_volume_spike = bool(config.get("require_volume_spike", True))
        volume_spike_ratio = helpers.extract_float(config.get("volume_spike_ratio"))
        if volume_spike_ratio is None:
            volume_spike_ratio = 1.5
        _vol_lookback = helpers.extract_float(config.get("volume_lookback"))
        volume_lookback = int(_vol_lookback) if _vol_lookback is not None else 10
        max_adx = helpers.extract_float(config.get("max_adx"))
        if max_adx is None:
            max_adx = 35.0
        require_regime = bool(config.get("require_regime", True))
        max_bb_bandwidth_percentile = helpers.extract_float(
            config.get("max_bb_bandwidth_percentile")
        )
        if max_bb_bandwidth_percentile is None:
            max_bb_bandwidth_percentile = 60.0
        _regime_lookback = helpers.extract_float(config.get("regime_lookback"))
        regime_lookback = int(_regime_lookback) if _regime_lookback is not None else 50
        use_structural_sizing = bool(config.get("use_structural_sizing", True))
        structural_sl_buffer_atr = helpers.extract_float(config.get("structural_sl_buffer_atr"))
        if structural_sl_buffer_atr is None:
            structural_sl_buffer_atr = 0.15
        atr_min_tp_mult = helpers.extract_float(config.get("atr_min_tp_mult"))
        if atr_min_tp_mult is None:
            atr_min_tp_mult = 0.5
        atr_max_tp_mult = helpers.extract_float(config.get("atr_max_tp_mult"))
        if atr_max_tp_mult is None:
            atr_max_tp_mult = 4.0
        atr_min_sl_mult = helpers.extract_float(config.get("atr_min_sl_mult"))
        if atr_min_sl_mult is None:
            atr_min_sl_mult = 0.3
        atr_max_sl_mult = helpers.extract_float(config.get("atr_max_sl_mult"))
        if atr_max_sl_mult is None:
            atr_max_sl_mult = 3.0
        use_atr_sizing = bool(config.get("use_atr_sizing", True))
        atr_tp_multiplier = helpers.extract_float(config.get("atr_tp_multiplier"))
        if atr_tp_multiplier is None:
            atr_tp_multiplier = 1.5
        atr_sl_multiplier = helpers.extract_float(config.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.2
        min_atr_pct = helpers.extract_float(config.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 0.8

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        ohlcv_compact = indicators.get("ohlcv") or []
        # Need at least lookback+1 candles: `lookback` prior + current.
        if len(ohlcv_compact) < lookback + 1:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal (insufficient candles: "
                f"{len(ohlcv_compact)} < {lookback + 1})"
            )
            return None

        # Current candle (the potential sweep candle).
        curr = ohlcv_compact[-1]
        if not isinstance(curr, dict):
            return None
        curr_high = helpers.extract_float(curr.get("high"))
        curr_low = helpers.extract_float(curr.get("low"))
        curr_close = helpers.extract_float(curr.get("close"))
        curr_open = helpers.extract_float(curr.get("open"))
        curr_volume = helpers.extract_float(curr.get("volume"))
        if any(v is None for v in (curr_high, curr_low, curr_close, curr_open)):
            helpers.emit_debug(f"LiquiditySweep: {symbol} — no signal (current candle OHLC incomplete)")
            return None

        # Prior N candles (excluding current).
        prior = ohlcv_compact[-(lookback + 1):-1]
        prior_lows = [
            helpers.extract_float(c.get("low"))
            for c in prior
            if isinstance(c, dict) and helpers.extract_float(c.get("low")) is not None
        ]
        prior_highs = [
            helpers.extract_float(c.get("high"))
            for c in prior
            if isinstance(c, dict) and helpers.extract_float(c.get("high")) is not None
        ]
        if not prior_lows or not prior_highs:
            helpers.emit_debug(f"LiquiditySweep: {symbol} — no signal (prior highs/lows unavailable)")
            return None

        swing_low = min(prior_lows)
        swing_high = max(prior_highs)

        # ── Sweep detection ───────────────────────────────────────────
        # Long sweep: wick below swing_low, close reclaims above it.
        # Short sweep: wick above swing_high, close reclaims below it.
        sweep_buffer = sweep_buffer_pct / 100.0

        long_sweep_pierced = (
            swing_low > 0
            and curr_low is not None
            and curr_low < swing_low * (1.0 - sweep_buffer)
        )
        short_sweep_pierced = (
            swing_high > 0
            and curr_high is not None
            and curr_high > swing_high * (1.0 + sweep_buffer)
        )

        if not long_sweep_pierced and not short_sweep_pierced:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(no sweep: low={curr_low:.6g} vs swing_low={swing_low:.6g}, "
                f"high={curr_high:.6g} vs swing_high={swing_high:.6g})"
            )
            return None

        # Reclaim check: close must be back inside the range.
        candle_range = curr_high - curr_low
        if candle_range <= 0:
            helpers.emit_debug(f"LiquiditySweep: {symbol} — no signal (zero-range candle)")
            return None

        # For longs: close should be in the upper portion of the candle
        # (rejection of the lower wick).  reclaim_ratio=0.5 → upper 50%.
        close_pos = (curr_close - curr_low) / candle_range  # 0=at low, 1=at high
        long_reclaim_ok = long_sweep_pierced and close_pos >= reclaim_ratio
        short_reclaim_ok = short_sweep_pierced and close_pos <= (1.0 - reclaim_ratio)

        if not long_reclaim_ok and not short_reclaim_ok:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(sweep pierced but no reclaim: close_pos={close_pos:.2f}, "
                f"need >={reclaim_ratio} for long or <={1.0 - reclaim_ratio:.2f} for short)"
            )
            return None

        # ── ADX gate: skip strong trends (sweep likely a real breakout) ──
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))
        if max_adx > 0 and adx is not None and adx > max_adx:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(ADX={adx:.1f} > max={max_adx:.1f} — strong trend, sweep likely real breakout)"
            )
            return None

        # ── HTF trend alignment ───────────────────────────────────────
        htf_indicators: dict[str, Any] = indicators.get("htf_indicators") or {}
        htf_ma = htf_indicators.get("moving_averages") or {}
        htf_ema50 = helpers.extract_float(htf_ma.get("ema_50"))
        htf_ema200 = helpers.extract_float(htf_ma.get("ema_200"))
        htf_bullish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 > htf_ema200
        htf_bearish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 < htf_ema200
        htf_available = bool(htf_indicators)
        if require_htf_trend and not htf_available:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — HTF unavailable, auto-disabling require_htf_trend"
            )

        # ── Volume spike confirmation ─────────────────────────────────
        volume_ok = True
        vol_ratio: float | None = None
        if require_volume_spike and curr_volume is not None:
            prior_volumes = [
                helpers.extract_float(c.get("volume"))
                for c in prior[-volume_lookback:]
                if isinstance(c, dict) and helpers.extract_float(c.get("volume")) is not None
            ]
            if prior_volumes:
                avg_vol = sum(prior_volumes) / len(prior_volumes)
                if avg_vol > 0:
                    vol_ratio = curr_volume / avg_vol
                    volume_ok = vol_ratio >= volume_spike_ratio
            # If no prior volume data, skip the filter (don't block).

        if require_volume_spike and not volume_ok:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(volume spike: ratio={vol_ratio:.2f} < min={volume_spike_ratio:.2f})"
                if vol_ratio is not None else
                f"LiquiditySweep: {symbol} — no signal (volume data unavailable)"
            )
            return None

        # ── Regime gate: BB bandwidth percentile ──────────────────────
        _bb = indicators.get("bollinger_bands") or {}
        bb_lower = helpers.extract_float(_bb.get("lower"))
        bb_upper = helpers.extract_float(_bb.get("upper"))
        bb_middle = helpers.extract_float(_bb.get("middle"))
        bb_bandwidth: float | None = (
            (bb_upper - bb_lower) / bb_middle * 100.0
            if bb_upper is not None and bb_lower is not None and bb_middle and bb_middle > 0
            else None
        )
        bw_percentile = compute_bb_bandwidth_percentile(
            ohlcv_compact, bb_bandwidth, lookback=regime_lookback
        )
        regime_ok = (
            not require_regime
            or (bw_percentile is not None and bw_percentile <= max_bb_bandwidth_percentile)
        )
        if require_regime and not regime_ok:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(regime: BW pct={bw_percentile:.0f} > max={max_bb_bandwidth_percentile:.0f})"
                if bw_percentile is not None else
                f"LiquiditySweep: {symbol} — no signal (regime: BW percentile unavailable)"
            )
            return None

        # ── Min ATR% filter ───────────────────────────────────────────
        atr_pct_value = helpers.extract_float(indicators.get("atr_pct"))
        atr_ok = (
            min_atr_pct <= 0
            or (atr_pct_value is not None and atr_pct_value >= min_atr_pct)
        )
        if not atr_ok:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(ATR%={atr_pct_value:.2f} < min={min_atr_pct:.2f})"
                if atr_pct_value is not None else
                f"LiquiditySweep: {symbol} — no signal (ATR% unavailable)"
            )
            return None

        # ── Direction decision ────────────────────────────────────────
        # Long sweep: low pierced swing_low, close reclaimed.  Take long.
        # Short sweep: high pierced swing_high, close reclaimed.  Take short.
        # If both somehow true (rare), prefer the one with stronger reclaim.
        buy_signal = long_reclaim_ok and (
            not require_htf_trend or not htf_available or htf_bullish
        )
        sell_signal = short_reclaim_ok and (
            not require_htf_trend or not htf_available or htf_bearish
        )

        # If both directions qualify (rare on a doji), pick the stronger reclaim.
        if buy_signal and sell_signal:
            if close_pos >= 0.5:
                sell_signal = False
            else:
                buy_signal = False

        if not buy_signal and not sell_signal:
            htf_str = "n/a"
            if htf_available and htf_ema50 is not None and htf_ema200 is not None:
                htf_str = "bull" if htf_bullish else "bear" if htf_bearish else "flat"
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(sweep+reclaim ok but HTF trend blocks: long={long_reclaim_ok}, "
                f"short={short_reclaim_ok}, HTF={htf_str})"
            )
            return None

        # ── Compute effective TP/SL ───────────────────────────────────
        # Structural mode: TP at opposite swing extreme, SL beyond sweep wick.
        # ATR mode (fallback): TP/SL = multiplier × ATR%.
        # In both modes, ATR clamps the structural distances to a sane range.
        _static_tp = helpers.extract_float(config.get("tp_pct"))
        _static_sl = helpers.extract_float(config.get("sl_pct"))
        _effective_tp = _static_tp
        _effective_sl = _static_sl
        _sizing_source = "static"

        atr_pct = helpers.extract_float(indicators.get("atr_pct"))
        last_price = helpers.get_last_price(symbol)

        # ATR fallback / clamp values.
        if use_atr_sizing and atr_pct is not None and atr_pct > 0:
            _effective_tp = atr_tp_multiplier * atr_pct
            _effective_sl = atr_sl_multiplier * atr_pct
            _sizing_source = "atr"

        # Structural sizing: use the swing levels we already computed.
        if use_structural_sizing and last_price and last_price > 0 and atr_pct is not None and atr_pct > 0:
            atr_price = (atr_pct / 100.0) * last_price
            if atr_price > 0:
                if buy_signal:
                    # Long: TP at swing_high (opposite extreme), SL beyond sweep wick low.
                    raw_tp_dist = swing_high - last_price if swing_high > last_price else None
                    # SL: sweep wick low minus a small ATR buffer beyond it.
                    raw_sl_dist = last_price - (curr_low - structural_sl_buffer_atr * atr_price)
                else:
                    # Short: TP at swing_low (opposite extreme), SL beyond sweep wick high.
                    raw_tp_dist = last_price - swing_low if swing_low < last_price else None
                    raw_sl_dist = (curr_high + structural_sl_buffer_atr * atr_price) - last_price

                # Convert distances to % of price for clamping.
                tp_pct_raw = (raw_tp_dist / last_price * 100.0) if raw_tp_dist is not None and raw_tp_dist > 0 else None
                sl_pct_raw = (raw_sl_dist / last_price * 100.0) if raw_sl_dist is not None and raw_sl_dist > 0 else None

                # Clamp TP to [atr_min_tp_mult × ATR%, atr_max_tp_mult × ATR%].
                tp_clamped = None
                if tp_pct_raw is not None:
                    tp_min = atr_min_tp_mult * atr_pct
                    tp_max = atr_max_tp_mult * atr_pct
                    tp_clamped = max(tp_min, min(tp_max, tp_pct_raw))

                # Clamp SL to [atr_min_sl_mult × ATR%, atr_max_sl_mult × ATR%].
                sl_clamped = None
                if sl_pct_raw is not None:
                    sl_min = atr_min_sl_mult * atr_pct
                    sl_max = atr_max_sl_mult * atr_pct
                    sl_clamped = max(sl_min, min(sl_max, sl_pct_raw))

                if tp_clamped is not None and sl_clamped is not None:
                    _effective_tp = tp_clamped
                    _effective_sl = sl_clamped
                    _sizing_source = (
                        f"structural(tp={'%.2f' % tp_pct_raw}→{'%.2f' % tp_clamped}%, "
                        f"sl={'%.2f' % sl_pct_raw}→{'%.2f' % sl_clamped}%)"
                    )
                elif tp_clamped is not None:
                    _effective_tp = tp_clamped
                    _sizing_source = f"structural(tp={'%.2f' % tp_clamped}%, sl=atr)"
                elif sl_clamped is not None:
                    _effective_sl = sl_clamped
                    _sizing_source = f"structural(sl={'%.2f' % sl_clamped}%, tp=atr)"
                # If neither clamped value is available, keep ATR/static fallback.

        sweep_type = "low" if buy_signal else "high"
        direction = "buy" if buy_signal else "sell"
        vol_str = f" vol_ratio={vol_ratio:.2f}" if vol_ratio is not None else ""
        return StrategySignal(
            direction=direction,
            strategy_name=self.name,
            tp_pct=_effective_tp,
            sl_pct=_effective_sl,
            rationale=(
                f"LiquiditySweep {direction.upper()}: swept {sweep_type} "
                f"(swing={'%.6g' % (swing_low if buy_signal else swing_high)}, "
                f"close_pos={close_pos:.2f}{vol_str}) [{_sizing_source}]"
            ),
        )
