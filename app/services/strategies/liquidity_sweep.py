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

from . import (
    StrategyHelpers,
    StrategySignal,
    compute_bb_bandwidth_percentile,
    fractal_swings,
    resolve_analysis_block,
)
from .defaults import merged_config
from .liquidity_helpers import order_book_imbalance


class LiquiditySweepStrategy:
    """Stop-hunt reversal strategy.

    Config keys (all live under ``config["strategies"]["liquidity_sweep"]``):
      - ``enabled`` (bool): master switch
      - ``lookback`` (int, default 20): bars to identify the swing high/low
        that gets swept.
      - ``pivot_bars`` (int, default 3): the swing is derived from fractal
        pivots (a candle is a pivot when its low/high is the min/max of the
        ``2 * pivot_bars + 1`` window).  Falls back to the trailing min/max
        over ``lookback`` bars when too few pivots exist.
      - ``sweep_penetration_mode`` (str, default "atr"): how far beyond the
        swing level the wick must go to qualify as a sweep.  ``"atr"`` scales
        the threshold by ``sweep_buffer_atr`` × ATR; ``"pct"`` uses the legacy
        flat ``sweep_buffer_pct``.
      - ``sweep_buffer_atr`` (float, default 0.25): penetration threshold in
        ATR units when ``sweep_penetration_mode == "atr"``.
      - ``sweep_buffer_pct`` (float, default 0.1): minimum % beyond the swing
        level the wick must penetrate to qualify as a sweep (0.1 = 0.1%).
        Used when ``sweep_penetration_mode == "pct"``.
      - ``reclaim_buffer_pct`` (float, default 0.1): the close must reclaim the
        swept level by at least this % (symmetrised with the sweep buffer) —
        not merely sit high in its own candle body while still beneath/above
        the level.  A close that stays past the level is a breakdown, not a
        reclaimed stop-run.
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
      - ``max_adx`` (float, default 28): skip when ADX is very high (strong
        trend — sweep is more likely a real breakout, not a stop hunt).
        Tightened from 35 to reduce strong-trend losses.
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

        # Merge caller config over the canonical defaults so any missing key
        # falls back to an acceptable, validated value.
        cfg = merged_config(config, self.name)

        # ---- HTF regime gate (chop preferred by default) -------------------
        # Configurable per-strategy: "chop" (block when HTF trending — the
        # legacy sweep behaviour), "trend", or "off".  Neutral (no HTF data)
        # never blocks.
        from app.services.indicator_service import htf_regime_allows

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        adx_htf = helpers.extract_float(indicators.get("adx_htf"))
        chop_htf = helpers.extract_float(indicators.get("choppiness_htf"))
        htf_pref = cfg.get("htf_regime_preference", "chop")

        if not htf_regime_allows(adx_htf, chop_htf, htf_pref):
            # In a trending environment sweeps often become breakouts.
            return None

        # ── Config ────────────────────────────────────────────────────
        _lookback = helpers.extract_float(cfg.get("lookback"))
        lookback = int(_lookback) if _lookback is not None else 20
        _pivot_bars = helpers.extract_float(cfg.get("pivot_bars"))
        pivot_bars = int(_pivot_bars) if _pivot_bars is not None else 3
        sweep_pen_mode = str(cfg.get("sweep_penetration_mode", "atr")).lower()
        sweep_buffer_atr = helpers.extract_float(cfg.get("sweep_buffer_atr"))
        if sweep_buffer_atr is None:
            sweep_buffer_atr = 0.25
        sweep_buffer_pct = helpers.extract_float(cfg.get("sweep_buffer_pct"))
        if sweep_buffer_pct is None:
            sweep_buffer_pct = 0.1
        reclaim_buffer_pct = helpers.extract_float(cfg.get("reclaim_buffer_pct"))
        if reclaim_buffer_pct is None:
            reclaim_buffer_pct = sweep_buffer_pct
        reclaim_ratio = helpers.extract_float(cfg.get("reclaim_ratio"))
        if reclaim_ratio is None:
            reclaim_ratio = 0.5
        require_htf_trend = bool(cfg.get("require_htf_trend", True))
        require_volume_spike = bool(cfg.get("require_volume_spike", True))
        volume_spike_ratio = helpers.extract_float(cfg.get("volume_spike_ratio"))
        if volume_spike_ratio is None:
            volume_spike_ratio = 1.5
        _vol_lookback = helpers.extract_float(cfg.get("volume_lookback"))
        volume_lookback = int(_vol_lookback) if _vol_lookback is not None else 10
        max_adx = helpers.extract_float(cfg.get("max_adx"))
        if max_adx is None:
            max_adx = 28.0
        require_regime = bool(cfg.get("require_regime", True))
        max_bb_bandwidth_percentile = helpers.extract_float(
            cfg.get("max_bb_bandwidth_percentile")
        )
        if max_bb_bandwidth_percentile is None:
            max_bb_bandwidth_percentile = 60.0
        _regime_lookback = helpers.extract_float(cfg.get("regime_lookback"))
        regime_lookback = int(_regime_lookback) if _regime_lookback is not None else 50
        use_structural_sizing = bool(cfg.get("use_structural_sizing", True))
        structural_sl_buffer_atr = helpers.extract_float(cfg.get("structural_sl_buffer_atr"))
        if structural_sl_buffer_atr is None:
            structural_sl_buffer_atr = 0.15
        atr_min_tp_mult = helpers.extract_float(cfg.get("atr_min_tp_mult"))
        if atr_min_tp_mult is None:
            atr_min_tp_mult = 0.5
        atr_max_tp_mult = helpers.extract_float(cfg.get("atr_max_tp_mult"))
        if atr_max_tp_mult is None:
            atr_max_tp_mult = 4.0
        atr_min_sl_mult = helpers.extract_float(cfg.get("atr_min_sl_mult"))
        if atr_min_sl_mult is None:
            atr_min_sl_mult = 0.3
        atr_max_sl_mult = helpers.extract_float(cfg.get("atr_max_sl_mult"))
        if atr_max_sl_mult is None:
            atr_max_sl_mult = 3.0
        use_atr_sizing = bool(cfg.get("use_atr_sizing", True))
        atr_tp_multiplier = helpers.extract_float(cfg.get("atr_tp_multiplier"))
        if atr_tp_multiplier is None:
            atr_tp_multiplier = 1.5
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.2
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 0.8
        # R:R floor for structural sizing (mirrors DEFAULT_LIQUIDITY_SWEEP).
        min_reward_risk_ratio = helpers.extract_float(cfg.get("min_reward_risk_ratio"))
        if min_reward_risk_ratio is None:
            min_reward_risk_ratio = 1.0
        # ── Liquidity-aware gates (§3) ────────────────────────────────
        # ``require_close_in_va`` (default off): the sweep candle must close
        # *back inside* the value area after its wick, confirming the stop-run
        # was absorbed within value rather than a true break of value.
        # ``require_macro_sl`` (default off): place SL at the macro swing
        # (look-back ``macro_sl_lookback`` candles) instead of the immediate
        # wick, giving the reversal room to breathe.
        require_close_in_va = bool(cfg.get("require_close_in_va", False))
        require_macro_sl = bool(cfg.get("require_macro_sl", False))
        _macro_lookback = helpers.extract_float(cfg.get("macro_sl_lookback"))
        macro_sl_lookback = int(_macro_lookback) if _macro_lookback is not None else 50
        # Order-book imbalance gate (§3): a fade into a stop-run wants the
        # book supportive after the reclaim — bid-heavy for a long sweep
        # (imbalance >= min_for_long), ask-heavy for a short sweep
        # (imbalance <= max_for_short).  Degrades gracefully when the book is
        # unavailable or degenerate (imbalance=None → gate passes).
        require_book_imbalance = bool(cfg.get("require_book_imbalance", False))
        imbalance_min_for_long = helpers.extract_float(cfg.get("imbalance_min_for_long"))
        if imbalance_min_for_long is None:
            imbalance_min_for_long = 1.0
        imbalance_max_for_short = helpers.extract_float(cfg.get("imbalance_max_for_short"))
        if imbalance_max_for_short is None:
            imbalance_max_for_short = 1.0

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

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

        # Swing detection (F2): prefer fractal pivots — real, visible,
        # freshly-respected levels that stop-hunters cluster at.  Fall back to
        # the trailing min/max when pivot structure is unavailable (e.g. short
        # history), logging the fallback so /debug distinguishes the two.
        swing_low, swing_high, swing_fallback = fractal_swings(
            ohlcv_compact[-(lookback + 1):],
            pivot_bars=pivot_bars,
        )
        if swing_low is None or swing_high is None:
            swing_low = min(prior_lows)
            swing_high = max(prior_highs)
            swing_fallback = True
        if swing_fallback:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — fractal pivots unavailable, using min/max swing "
                f"({swing_low:.6g}/{swing_high:.6g})"
            )

        # ── Sweep detection ───────────────────────────────────────────
        # Long sweep: wick below swing_low, close reclaims above it.
        # Short sweep: wick above swing_high, close reclaims below it.
        # Penetration threshold is ATR-scaled by default (volatility-scaled
        # for volatile alts) with a legacy flat-% mode for backtest comparison.
        atr_pct_base = helpers.extract_float(indicators.get("atr_pct")) or 0.0
        if sweep_pen_mode == "atr" and atr_pct_base > 0:
            atr_price = (atr_pct_base / 100.0) * max(swing_low, swing_high)
            pen_long = sweep_buffer_atr * atr_price
            pen_short = sweep_buffer_atr * atr_price
        else:
            sweep_buffer = sweep_buffer_pct / 100.0
            pen_long = sweep_buffer * swing_low
            pen_short = sweep_buffer * swing_high

        long_sweep_pierced = (
            swing_low > 0
            and curr_low is not None
            and curr_low < swing_low - pen_long
        )
        short_sweep_pierced = (
            swing_high > 0
            and curr_high is not None
            and curr_high > swing_high + pen_short
        )

        if not long_sweep_pierced and not short_sweep_pierced:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(no sweep: low={curr_low:.6g} vs swing_low={swing_low:.6g}, "
                f"high={curr_high:.6g} vs swing_high={swing_high:.6g})"
            )
            return None

        # Reclaim check: close must be back inside the range AND genuinely
        # reclaim the swept level (F1).  The body-position filter says where in
        # the candle the close lands; the level check ensures the close is
        # actually *past* the level, so a breakdown-close cannot qualify.
        candle_range = curr_high - curr_low
        if candle_range <= 0:
            helpers.emit_debug(f"LiquiditySweep: {symbol} — no signal (zero-range candle)")
            return None

        # For longs: close should be in the upper portion of the candle
        # (rejection of the lower wick).  reclaim_ratio=0.5 → upper 50%.
        close_pos = (curr_close - curr_low) / candle_range  # 0=at low, 1=at high
        body_ok_long = close_pos >= reclaim_ratio
        body_ok_short = close_pos <= (1.0 - reclaim_ratio)

        # The close must reclaim the swept level by a margin.  Symmetrised with
        # the sweep buffer so a candle that only returns to the *wrong* side of
        # the level (a breakdown continuation) cannot pass.
        reclaim_margin = reclaim_buffer_pct / 100.0
        level_ok_long = (
            curr_close > swing_low * (1.0 + reclaim_margin)
        )
        level_ok_short = (
            curr_close < swing_high * (1.0 - reclaim_margin)
        )

        long_reclaim_ok = long_sweep_pierced and body_ok_long and level_ok_long
        short_reclaim_ok = short_sweep_pierced and body_ok_short and level_ok_short

        if not long_reclaim_ok and not short_reclaim_ok:
            _why = []
            if long_sweep_pierced and not body_ok_long:
                _why.append(f"long body close_pos={close_pos:.2f}<{reclaim_ratio:.2f}")
            if long_sweep_pierced and not level_ok_long:
                _why.append(f"long no-reclaim close={curr_close:.6g}<=swing_low*1+{reclaim_margin:.4f}")
            if short_sweep_pierced and not body_ok_short:
                _why.append(f"short body close_pos={close_pos:.2f}>{1.0 - reclaim_ratio:.2f}")
            if short_sweep_pierced and not level_ok_short:
                _why.append(f"short no-reclaim close={curr_close:.6g}>=swing_high*1-{reclaim_margin:.4f}")
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(sweep pierced but no reclaim: {'; '.join(_why) or 'n/a'})"
            )
            return None

        # ── Close-back-inside-VA gate (§3) ────────────────────────────
        # The stop-run is only "absorbed within value" when the candle closes
        # back inside the value area.  A close below/above value means the sweep
        # broke through — treat as a true breakout, not a reversal.
        va_high = helpers.extract_float(indicators.get("value_area_high"))
        va_low = helpers.extract_float(indicators.get("value_area_low"))
        close_in_va = (
            va_high is None
            or va_low is None
            or (va_low <= curr_close <= va_high)
        )
        if require_close_in_va and not close_in_va:
            helpers.emit_debug(
                f"LiquiditySweep: {symbol} — no signal "
                f"(close={curr_close:.6g} outside VA [{va_low:.6g}, {va_high:.6g}] — "
                f"probably a real break, not a stop-run)"
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

        # ── Order-book imbalance gate (§3) ────────────────────────────
        # A fade into a stop-run wants the book supportive after the reclaim:
        # bid-heavy for a long sweep (imbalance >= min_for_long), ask-heavy for
        # a short sweep (imbalance <= max_for_short).  Emits the computed ratio
        # when the gate is enabled so /debug can audit it.  Degrades gracefully
        # (passes) when the book is absent or degenerate.
        if require_book_imbalance:
            imbalance = order_book_imbalance(sym_data.get("order_book") or {})
            imbalance_ok = True
            if imbalance is not None:
                if buy_signal:
                    imbalance_ok = imbalance >= imbalance_min_for_long
                else:
                    imbalance_ok = imbalance <= imbalance_max_for_short
            if not imbalance_ok:
                helpers.emit_debug(
                    f"LiquiditySweep: {symbol} — no signal "
                    f"(book imbalance={imbalance:.2f} rejects "
                    f"{'long (need >=' + str(imbalance_min_for_long) + ')' if buy_signal else 'short (need <=' + str(imbalance_max_for_short) + ')'})"
                )
                return None

        sweep_type = "low" if buy_signal else "high"
        direction = "buy" if buy_signal else "sell"
        # ------------------------------------------------------------------
        # Unified TP/SL via trade_management
        # ------------------------------------------------------------------

        from app.services.trade_management import OrderContext, compute_tp_sl_pct

        entry_price = helpers.get_last_price(symbol)
        if entry_price is None:
            return None

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        atr_tf_pct = helpers.extract_float(indicators.get("atr_pct")) or 1.0
        atr_htf_pct = helpers.extract_float(indicators.get("atr_pct_htf")) or atr_tf_pct
        # Preserve the adaptive-ATR regime scaling in the surviving single
        # sizing path (F5): widening TP/SL distances for higher-volatility
        # regimes that used to be handled by the removed dead sizing block.
        if config.get("use_adaptive_atr", False) and atr_tf_pct > 0:
            if atr_tf_pct < 1.5:
                atr_tf_pct *= 1.20
            elif atr_tf_pct < 3.0:
                atr_tf_pct *= 1.80
            else:
                atr_tf_pct *= 2.50
        vpoc = helpers.extract_float(indicators.get("vpoc"))
        va_width = helpers.extract_float(indicators.get("value_area_width"))
        swing_high_val = helpers.extract_float(indicators.get("swing_high"))
        swing_low_val = helpers.extract_float(indicators.get("swing_low"))

        static_tp = helpers.extract_float(config.get("tp_pct"))
        static_sl = helpers.extract_float(config.get("sl_pct"))

        # Thesis-specific structural levels: TP at the opposite swing extreme,
        # SL beyond the sweep wick (with an ATR buffer so it's not sitting
        # exactly at the wick).  When ``require_macro_sl``, the SL uses the
        # macro swing (look-back ``macro_sl_lookback`` candles) instead of the
        # immediate wick, giving the reversal room to breathe (§3).
        tp_target: float | None = None
        sl_level: float | None = None
        if use_structural_sizing and entry_price and entry_price > 0:
            atr_price = (atr_tf_pct / 100.0) * entry_price
            if buy_signal:
                if swing_high is not None and swing_high > entry_price:
                    tp_target = swing_high
                if curr_low is not None:
                    sl_level = curr_low - structural_sl_buffer_atr * atr_price
            else:
                if swing_low is not None and swing_low < entry_price:
                    tp_target = swing_low
                if curr_high is not None:
                    sl_level = curr_high + structural_sl_buffer_atr * atr_price

            # Macro-SL override: SL at the macro swing extreme, not the wick.
            if require_macro_sl:
                macro_sweep_low = None
                macro_sweep_high = None
                _macro_candles = ohlcv_compact[-macro_sl_lookback:] if macro_sl_lookback > 0 else []
                _macro_lows = [
                    helpers.extract_float(c.get("low"))
                    for c in _macro_candles
                    if isinstance(c, dict) and helpers.extract_float(c.get("low")) is not None
                ]
                _macro_highs = [
                    helpers.extract_float(c.get("high"))
                    for c in _macro_candles
                    if isinstance(c, dict) and helpers.extract_float(c.get("high")) is not None
                ]
                if _macro_lows:
                    macro_sweep_low = min(_macro_lows)
                if _macro_highs:
                    macro_sweep_high = max(_macro_highs)
                if buy_signal and macro_sweep_low is not None:
                    sl_level = macro_sweep_low - structural_sl_buffer_atr * atr_price
                elif not buy_signal and macro_sweep_high is not None:
                    sl_level = macro_sweep_high + structural_sl_buffer_atr * atr_price

        tp_pct_final, sl_pct_final = compute_tp_sl_pct(
            entry=entry_price,
            side="long" if buy_signal else "short",
            ctx=OrderContext(
                atr_tf_pct=atr_tf_pct,
                atr_htf_pct=atr_htf_pct,
                vpoc=vpoc,
                value_area_width=va_width,
                swing_high=swing_high_val,
                swing_low=swing_low_val,
                last_price=entry_price,
                tp_target=tp_target,
                sl_level=sl_level,
                structural_sl_buffer_atr=structural_sl_buffer_atr,
                atr_min_tp_mult=atr_min_tp_mult,
                atr_max_tp_mult=atr_max_tp_mult,
                atr_min_sl_mult=atr_min_sl_mult,
                atr_max_sl_mult=atr_max_sl_mult,
                min_reward_risk_ratio=min_reward_risk_ratio,
            ),
            static_tp_pct=static_tp,
            static_sl_pct=static_sl,
            atr_tp_multiplier=atr_tp_multiplier if use_atr_sizing else None,
            atr_sl_multiplier=atr_sl_multiplier if use_atr_sizing else None,
        )

        vol_str = f" vol_ratio={vol_ratio:.2f}" if vol_ratio is not None else ""

        return StrategySignal(
            direction=direction,
            strategy_name=self.name,
            tp_pct=tp_pct_final,
            sl_pct=sl_pct_final,
            rationale=(
                f"LiquiditySweep {direction.upper()}: swept {sweep_type}{vol_str} [trade_mgmt]"
            ),
        )
