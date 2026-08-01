"""Trend Pullback strategy.

Enters on pullbacks to value (EMA21 or VWAP) in an established HTF trend.
SC catches breakouts; MR catches extremes.  Neither catches the highest-
probability move: a pullback to value in an established trend.  Trend-aligned
entries have a higher base win rate than counter-trend MR.

Entry conditions (long example):
  1. HTF trend confirmed: EMA50 > EMA200 (uptrend) on the higher timeframe.
  2. LTF pullback: price pulls back to EMA21 or VWAP on 15m.
  3. Entry: bullish candle off the level (close > prev close, lower wick).

Exit:
  - TP: 2 ATR (recent swing high proxy).
  - SL: 1.5 ATR (below pullback low).
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal


class TrendPullbackStrategy:
    """Trend-aligned pullback strategy.

    Config keys (all live under ``config["strategies"]["trend_pullback"]``):
      - ``enabled`` (bool): master switch
      - ``pullback_ema`` (int, default 21): LTF EMA length used as the
        pullback level.  The corresponding indicator must exist under
        ``indicators["moving_averages"]["ema_<n>"]``.
      - ``use_vwap_as_level`` (bool, default True): also accept VWAP as a
        valid pullback level (price near VWAP qualifies).
            - ``pullback_proximity_pct`` (float, default 0.3): how close (in %)
        price must be to the EMA/VWAP level to qualify as a pullback.
        Tightened from 0.4 to reduce weak pullback entries.
      - ``require_htf_trend`` (bool, default True): HTF EMA50/EMA200 must
        confirm the trend direction.  Auto-disabled when no HTF data.
      - ``require_bullish_candle`` (bool, default True): the trigger candle
        must close above its prev close (longs) / below (shorts) AND show a
        rejection wick off the level.
      - ``candle_rejection_pct`` (float, default 25): minimum wick size as %
        of candle range for the rejection confirmation.
            - ``max_adx_for_entry`` (float, default 28): block when ADX is too high
        (trend already extended — pullback likely a reversal).  0 = disabled.
        Tightened from 35 to avoid late entries in mature trends.
            - ``min_adx`` (float, default 20): require a minimum trend strength so
        we only enter in real trends, not chop.  0 = disabled.
      - ``use_structural_sizing`` (bool, default True): use structural TP/SL
        based on swing highs/lows from ``indicators["structure"]`` instead
        of (or clamped by) ATR.  TP targets the nearest swing high (longs) /
        swing low (shorts).  SL sits just beyond the pullback candle's
        low (longs) / high (shorts).  ATR clamps both to a sane range.
        Falls back to ATR sizing when structural levels are unavailable.
      - ``structural_sl_buffer_atr`` (float, default 0.15): SL is placed
        this many ATR units *beyond* the pullback candle's low/high.
      - ``atr_min_tp_mult`` (float, default 0.5): structural TP distance
        must be at least this × ATR% from entry.
      - ``atr_max_tp_mult`` (float, default 4.0): structural TP distance
        capped at this × ATR% from entry.
      - ``atr_min_sl_mult`` (float, default 0.3): structural SL distance
        must be at least this × ATR% from entry.
      - ``atr_max_sl_mult`` (float, default 3.0): structural SL distance
        capped at this × ATR% from entry.
      - ``use_atr_sizing`` (bool, default True): ATR-scaled TP/SL fallback
        when structural levels are unavailable or ``use_structural_sizing``
        is False.
      - ``atr_tp_multiplier`` (float, default 2.0): TP = multiplier × ATR%.
      - ``atr_sl_multiplier`` (float, default 1.5): SL = multiplier × ATR%.
      - ``min_atr_pct`` (float, default 1.0): skip dead coins.
      - ``tp_pct`` (float, default None): static TP % fallback.
      - ``sl_pct`` (float, default None): static SL % fallback.
      - ``flip_launcher_direction`` (str, default None): invert the
        Launcher's trade direction before execution. One of "both",
        "from_long" (only BUY→SELL), "from_short" (only SELL→BUY),
        or None to disable. TP/SL are mirrored around last_price so
        they land on the correct side for the flipped direction.
    """

    name = "trend_pullback"

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal for a trend pullback, or None."""
        if not bool(config.get("enabled", False)):
            return None

        # ── Config ────────────────────────────────────────────────────
        _pullback_ema = helpers.extract_float(config.get("pullback_ema"))
        pullback_ema_len = int(_pullback_ema) if _pullback_ema is not None else 21
        use_vwap_as_level = bool(config.get("use_vwap_as_level", True))
        pullback_proximity_pct = helpers.extract_float(config.get("pullback_proximity_pct"))
        if pullback_proximity_pct is None:
            pullback_proximity_pct = 0.3
        require_htf_trend = bool(config.get("require_htf_trend", True))
        require_bullish_candle = bool(config.get("require_bullish_candle", True))
        candle_rejection_pct = helpers.extract_float(config.get("candle_rejection_pct"))
        if candle_rejection_pct is None:
            candle_rejection_pct = 25.0
        max_adx_for_entry = helpers.extract_float(config.get("max_adx_for_entry"))
        if max_adx_for_entry is None:
            max_adx_for_entry = 28.0
        min_adx = helpers.extract_float(config.get("min_adx"))
        if min_adx is None:
            min_adx = 20.0
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
            atr_tp_multiplier = 2.0
        atr_sl_multiplier = helpers.extract_float(config.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.5
        min_atr_pct = helpers.extract_float(config.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        last_price = helpers.get_last_price(symbol)
        atr_pct = helpers.extract_float(indicators.get("atr_pct"))
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))

        if last_price is None:
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal (price unavailable)"
            )
            return None

        # ── Min ATR% filter ───────────────────────────────────────────
        if min_atr_pct > 0 and (atr_pct is None or atr_pct < min_atr_pct):
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal "
                f"(ATR%={atr_pct:.2f} < min={min_atr_pct:.2f})"
                if atr_pct is not None else
                f"TrendPullback: {symbol} — no signal (ATR% unavailable)"
            )
            return None

        # ── ADX gates ─────────────────────────────────────────────────
        if min_adx > 0 and (adx is None or adx < min_adx):
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal "
                f"(ADX={adx:.1f} < min={min_adx:.1f} — not a real trend)"
                if adx is not None else
                f"TrendPullback: {symbol} — no signal (ADX unavailable)"
            )
            return None
        if max_adx_for_entry > 0 and adx is not None and adx > max_adx_for_entry:
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal "
                f"(ADX={adx:.1f} > max={max_adx_for_entry:.1f} — trend extended, pullback likely reversal)"
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
        # Auto-disable require_htf_trend when no HTF data is available (e.g.
        # 1D LTF has no higher timeframe). Without this guard, require_htf_trend
        # would silently block every signal on 1D because htf_bullish/bearish
        # are both False when htf_indicators is absent.
        effective_require_htf_trend = require_htf_trend and htf_available
        if require_htf_trend and not htf_available:
            helpers.emit_debug(
                f"TrendPullback: {symbol} — HTF unavailable, auto-disabling require_htf_trend"
            )

        # If HTF trend is required and available but flat, no signal.
        if effective_require_htf_trend and not htf_bullish and not htf_bearish:
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal (HTF trend flat: "
                f"EMA50={htf_ema50}, EMA200={htf_ema200})"
            )
            return None

        # Direction from HTF trend.  When HTF trend is required and available,
        # only take the trend direction.  Otherwise allow both directions.
        if effective_require_htf_trend:
            want_long = htf_bullish
            want_short = htf_bearish
        else:
            want_long = True
            want_short = True

        # ── Pullback level (LTF EMA + optional VWAP) ─────────────────
        ltf_ma = indicators.get("moving_averages") or {}
        # Look up ema_<pullback_ema_len>; fall back to ema_21 if absent.
        ema_key = f"ema_{pullback_ema_len}"
        pullback_ema = helpers.extract_float(ltf_ma.get(ema_key))
        if pullback_ema is None and pullback_ema_len != 21:
            pullback_ema = helpers.extract_float(ltf_ma.get("ema_21"))
        vwap_value = helpers.extract_float(indicators.get("vwap"))

        # A level is "touched" if price is within pullback_proximity_pct of it.
        proximity = pullback_proximity_pct / 100.0
        long_level_touched = False
        short_level_touched = False
        touched_levels: list[str] = []

        def _near(level: float | None) -> bool:
            return (
                level is not None
                and level > 0
                and abs(last_price - level) / level <= proximity
            )

        if pullback_ema is not None and _near(pullback_ema):
            long_level_touched = True
            short_level_touched = True
            touched_levels.append(f"EMA{pullback_ema_len}")
        if use_vwap_as_level and _near(vwap_value):
            long_level_touched = True
            short_level_touched = True
            touched_levels.append("VWAP")

        if not touched_levels:
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal "
                f"(no pullback level touched: price={last_price:.6g}, "
                f"EMA{pullback_ema_len}={pullback_ema}, VWAP={vwap_value}, "
                f"proximity={pullback_proximity_pct:.2f}%)"
            )
            return None

        # ── Bullish/bearish candle confirmation ─────────────────────
        candle_long_ok = True
        candle_short_ok = True
        if require_bullish_candle:
            ohlcv_compact = indicators.get("ohlcv") or []
            if len(ohlcv_compact) < 2 or not isinstance(ohlcv_compact[-1], dict):
                helpers.emit_debug(
                    f"TrendPullback: {symbol} — no signal (insufficient candles for confirmation)"
                )
                return None
            _curr = ohlcv_compact[-1]
            _prev = ohlcv_compact[-2]
            curr_open = helpers.extract_float(_curr.get("open"))
            curr_high = helpers.extract_float(_curr.get("high"))
            curr_low = helpers.extract_float(_curr.get("low"))
            curr_close = helpers.extract_float(_curr.get("close"))
            prev_close = helpers.extract_float(_prev.get("close"))
            if any(v is None for v in (curr_open, curr_high, curr_low, curr_close, prev_close)):
                helpers.emit_debug(
                    f"TrendPullback: {symbol} — no signal (confirmation OHLC incomplete)"
                )
                return None
            _range = curr_high - curr_low
            # Bullish candle: close > prev close AND lower wick (rejection off level).
            lower_wick_pct = ((curr_close - curr_low) / _range * 100.0) if _range > 0 else 0.0
            upper_wick_pct = ((curr_high - curr_close) / _range * 100.0) if _range > 0 else 0.0
            candle_long_ok = curr_close > prev_close and lower_wick_pct >= candle_rejection_pct
            candle_short_ok = curr_close < prev_close and upper_wick_pct >= candle_rejection_pct

        # ── Direction decision ────────────────────────────────────────
        buy_signal = want_long and long_level_touched and candle_long_ok
        sell_signal = want_short and short_level_touched and candle_short_ok

        if not buy_signal and not sell_signal:
            parts = [
                f"levels={'+'.join(touched_levels)}",
                f"price={last_price:.6g}",
            ]
            if require_htf_trend:
                if not htf_available:
                    parts.append("HTF=skipped(no data)")
                elif not htf_bullish and not htf_bearish:
                    parts.append("HTF=flat")
                else:
                    parts.append(f"HTF={'bull' if htf_bullish else 'bear'}")
            if require_bullish_candle:
                parts.append(
                    f"candle(long={'ok' if candle_long_ok else 'blocked'}, "
                    f"short={'ok' if candle_short_ok else 'blocked'})"
                )
            helpers.emit_debug(
                f"TrendPullback: {symbol} — no signal ({', '.join(parts)})"
            )
            return None

        # ── Compute effective TP/SL ───────────────────────────────────
        # Structural mode: TP at nearest swing high/low, SL beyond pullback candle.
        # ATR mode (fallback): TP/SL = multiplier × ATR%.
        _static_tp = helpers.extract_float(config.get("tp_pct"))
        _static_sl = helpers.extract_float(config.get("sl_pct"))
        _effective_tp = _static_tp
        _effective_sl = _static_sl
        _sizing_source = "static"

        if use_atr_sizing and atr_pct is not None and atr_pct > 0:
            _effective_tp = atr_tp_multiplier * atr_pct
            _effective_sl = atr_sl_multiplier * atr_pct
            _sizing_source = "atr"

        # Structural sizing: use swing highs/lows from indicators["structure"].
        if use_structural_sizing and last_price and last_price > 0 and atr_pct is not None and atr_pct > 0:
            atr_price = (atr_pct / 100.0) * last_price
            if atr_price > 0:
                structure = indicators.get("structure") or {}
                swing_highs = structure.get("swing_highs") or []
                swing_lows = structure.get("swing_lows") or []

                # Pullback candle OHLC (already extracted above for confirmation).
                _curr = (ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {})
                curr_low = helpers.extract_float(_curr.get("low"))
                curr_high = helpers.extract_float(_curr.get("high"))

                if buy_signal:
                    # Long: TP at nearest swing high above price, SL beyond pullback candle low.
                    tp_target = None
                    for sh in swing_highs:
                        sh_price = helpers.extract_float(sh.get("price")) if isinstance(sh, dict) else None
                        if sh_price is not None and sh_price > last_price:
                            tp_target = sh_price
                            break
                    raw_tp_dist = (tp_target - last_price) if tp_target is not None else None
                    raw_sl_dist = (last_price - (curr_low - structural_sl_buffer_atr * atr_price)) if curr_low is not None else None
                else:
                    # Short: TP at nearest swing low below price, SL beyond pullback candle high.
                    tp_target = None
                    for sl in swing_lows:
                        sl_price = helpers.extract_float(sl.get("price")) if isinstance(sl, dict) else None
                        if sl_price is not None and sl_price < last_price:
                            tp_target = sl_price
                            break
                    raw_tp_dist = (last_price - tp_target) if tp_target is not None else None
                    raw_sl_dist = ((curr_high + structural_sl_buffer_atr * atr_price) - last_price) if curr_high is not None else None

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

        direction = "buy" if buy_signal else "sell"
        level_str = '+'.join(touched_levels)
        trend_word = "bullish" if buy_signal else "bearish"
        adx_str = f"{adx:.1f}" if adx is not None else "n/a"
        return StrategySignal(
            direction=direction,
            strategy_name=self.name,
            tp_pct=_effective_tp,
            sl_pct=_effective_sl,
            rationale=(
                f"TrendPullback {direction.upper()}: pullback to {level_str} "
                f"in {trend_word} HTF trend "
                f"(price={last_price:.6g}, ADX={adx_str}) [{_sizing_source}]"
            ),
        )
