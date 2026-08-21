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
  - TP: 3 ATR (recent swing high proxy).
  - SL: 2 ATR (below structural invalidation).
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal, resolve_analysis_block
from .defaults import DEFAULT_TREND_PULLBACK, merged_config


class TrendPullbackStrategy:
    """Trend-aligned pullback strategy.

    Config keys (all live under ``config["strategies"]["trend_pullback"]``):
      - ``enabled`` (bool): master switch
      - ``pullback_ema`` (int, default 21): LTF EMA length used as the
        pullback level.  The corresponding indicator must exist under
        ``indicators["moving_averages"]["ema_<n>"]``.
      - ``use_vwap_as_level`` (bool, default True): also accept VWAP as a
        valid pullback level (price near VWAP qualifies).
            - ``pullback_proximity_pct`` (float, default 0.3): hard floor (in %)
        for how close price must be to the EMA/VWAP level to qualify as a
        pullback.  The effective band is the wider of this floor and
        ``pullback_proximity_atr × ATR%`` (volatility-normalised, Fix 2).
            - ``pullback_proximity_atr`` (float, default 0.5): volatility-
        normalised proximity — a level is touched when
        ``abs(last_price - level) / level <= pullback_proximity_atr × ATR%``.
        Keeps the band from collapsing to zero on dead coins (the fixed %
        floor applies) while scaling with the market on volatile alts.
      - ``require_htf_trend`` (bool, default True): HTF EMA50/EMA200 must
        confirm the trend direction.  Auto-disabled when no HTF data.
      - ``require_bullish_candle`` (bool, default True): the trigger candle
        must close above its prev close (longs) / below (shorts) AND show a
        rejection wick off the level.
      - ``candle_rejection_pct`` (float, default 25): minimum wick size as %
        of candle range for the rejection confirmation.
            - ``max_adx_for_entry`` (float, default 40): block when ADX is too high
        (trend already extended — pullback likely a reversal).  0 = disabled.
        Widened from 28 because ADX is lagging on volatile alts; the primary
        anti-late-entry filter is the ATR-anchored extension gate below.
            - ``min_adx`` (float, default 18): require a minimum trend strength so
        we only enter in real trends, not chop.  0 = disabled.
            - ``max_pullback_extension_atr`` (float, default 2.0): volatility-
        normalised extension gate — price must not be more than this × ATR%
        past the pullback level (blocks late entries where the pullback has
        already run too far).  0 = disabled.
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
      - ``atr_tp_multiplier`` (float, default 3.0): TP = multiplier × ATR%.
      - ``atr_sl_multiplier`` (float, default 2.0): SL = multiplier × ATR%.
        Together these yield ≥ 1.5 R:R when ATR sizing is active (Fix 1).
      - ``use_adaptive_atr`` (bool, default False): when enabled, scales the
        ATR distance used for SIZING only (never the min_atr_pct gate or the
        proximity/extension checks) by volatility regime (Fix 5).
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

        # Merge caller config over the canonical defaults so any missing key
        # falls back to an acceptable, validated value.
        cfg = merged_config(config, self.name)

        # ---- HTF regime gate (must BE trending by default) -----------------
        # Configurable per-strategy: "trend" (block when HTF not trending —
        # the legacy trend-pullback behaviour), "chop", or "off".  Neutral
        # (no HTF data) never blocks.
        from app.services.indicator_service import htf_regime_allows

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        adx_htf = helpers.extract_float(indicators.get("adx_htf"))
        chop_htf = helpers.extract_float(indicators.get("choppiness_htf"))
        htf_pref = cfg.get("htf_regime_preference", "trend")

        # Trend pullback only makes sense if the HTF is in a trend. Only block
        # on a definitive non-trending signal; neutral (no HTF data) passes.
        if not htf_regime_allows(adx_htf, chop_htf, htf_pref):
            return None

        # ── Config ────────────────────────────────────────────────────
        _pullback_ema = helpers.extract_float(cfg.get("pullback_ema"))
        pullback_ema_len = int(_pullback_ema) if _pullback_ema is not None else 21
        use_vwap_as_level = bool(cfg.get("use_vwap_as_level", True))
        pullback_proximity_pct = helpers.extract_float(cfg.get("pullback_proximity_pct"))
        if pullback_proximity_pct is None:
            pullback_proximity_pct = DEFAULT_TREND_PULLBACK["pullback_proximity_pct"]
        pullback_proximity_atr = helpers.extract_float(cfg.get("pullback_proximity_atr"))
        if pullback_proximity_atr is None:
            pullback_proximity_atr = DEFAULT_TREND_PULLBACK["pullback_proximity_atr"]
        require_htf_trend = bool(cfg.get("require_htf_trend", True))
        require_bullish_candle = bool(cfg.get("require_bullish_candle", True))
        candle_rejection_pct = helpers.extract_float(cfg.get("candle_rejection_pct"))
        if candle_rejection_pct is None:
            candle_rejection_pct = 25.0
        max_adx_for_entry = helpers.extract_float(cfg.get("max_adx_for_entry"))
        if max_adx_for_entry is None:
            max_adx_for_entry = DEFAULT_TREND_PULLBACK["max_adx_for_entry"]
        min_adx = helpers.extract_float(cfg.get("min_adx"))
        if min_adx is None:
            min_adx = DEFAULT_TREND_PULLBACK["min_adx"]
        max_pullback_extension_atr = helpers.extract_float(cfg.get("max_pullback_extension_atr"))
        if max_pullback_extension_atr is None:
            max_pullback_extension_atr = DEFAULT_TREND_PULLBACK["max_pullback_extension_atr"]
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
            atr_tp_multiplier = DEFAULT_TREND_PULLBACK["atr_tp_multiplier"]
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = DEFAULT_TREND_PULLBACK["atr_sl_multiplier"]
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0
        # ── Liquidity-aware gate (§3) ────────────────────────────────
        # ``require_poc_proximity`` (default off): the pullback must occur at a
        # POC / value-area node — price within ``poc_proximity_va_width`` × the
        # value-area width of POC / VA-high / VA-low.  Adds a liquidity
        # confluence confirmation on top of the 21-EMA touch.
        require_poc_proximity = bool(cfg.get("require_poc_proximity", False))
        poc_proximity_va_width = helpers.extract_float(cfg.get("poc_proximity_va_width"))
        if poc_proximity_va_width is None:
            poc_proximity_va_width = 0.2

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        last_price = helpers.get_last_price(symbol)
        # ``atr_pct`` stays UNSCALED for the min_atr_pct gate and the
        # volatility-normalised proximity/extension checks (Fix 5).  Adaptive
        # ATR scaling is applied to SIZING only, later in the exit block.
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

        # A level is "touched" if price is within the effective proximity band.
        # The band is volatility-normalised (Fix 2): the wider of the fixed %
        # floor and ``pullback_proximity_atr × ATR%``, so it scales with the
        # market instead of being a fixed % that is too tight on volatile alts.
        effective_proximity_pct = pullback_proximity_pct
        if atr_pct is not None:
            effective_proximity_pct = max(
                pullback_proximity_pct,
                pullback_proximity_atr * atr_pct,
            )
        proximity = effective_proximity_pct / 100.0
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
                f"proximity={effective_proximity_pct:.2f}%)"
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

        # ── POC / value-area proximity gate (§3) ─────────────────────
        # Confirm the pullback sits at a liquidity node — within
        # ``poc_proximity_va_width`` × VA-width of POC / VA-high / VA-low.
        # Neutral (no VA data) passes.
        poc_proximity_ok = True
        if require_poc_proximity:
            vpoc = helpers.extract_float(indicators.get("vpoc"))
            va_high = helpers.extract_float(indicators.get("value_area_high"))
            va_low = helpers.extract_float(indicators.get("value_area_low"))
            va_width = helpers.extract_float(indicators.get("value_area_width"))
            if va_width is not None and va_width > 0:
                threshold = poc_proximity_va_width * va_width
                nodes = [n for n in (vpoc, va_high, va_low) if n is not None]
                if nodes:
                    poc_proximity_ok = any(
                        abs(last_price - n) <= threshold for n in nodes
                    )
            # No usable VA → leave True (neutral).

        # ── Volatility-normalised extension gate (Fix 4) ─────────────
        # Price must not be more than ``max_pullback_extension_atr × ATR%``
        # past the pullback level — blocks late entries where the pullback
        # has already run too far (the failure mode a raw ADX cap misses).
        extension_ok_long = True
        extension_ok_short = True
        if max_pullback_extension_atr > 0 and atr_pct is not None:
            ext_limit = max_pullback_extension_atr * (atr_pct / 100.0) * last_price
            for level in (pullback_ema, vwap_value):
                if level is not None and level > 0:
                    if last_price - level > ext_limit:
                        extension_ok_long = False
                    if level - last_price > ext_limit:
                        extension_ok_short = False

        # ── Direction decision ────────────────────────────────────────
        buy_signal = (
            want_long
            and long_level_touched
            and candle_long_ok
            and poc_proximity_ok
            and extension_ok_long
        )
        sell_signal = (
            want_short
            and short_level_touched
            and candle_short_ok
            and poc_proximity_ok
            and extension_ok_short
        )

        if not buy_signal and not sell_signal:
            parts = [
                f"levels={'+'.join(touched_levels)}",
                f"price={last_price:.6g}",
            ]
            if require_poc_proximity:
                parts.append(f"poc_prox={'ok' if poc_proximity_ok else 'blocked'}")
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
            if max_pullback_extension_atr > 0:
                parts.append(
                    f"ext(long={'ok' if extension_ok_long else 'blocked'}, "
                    f"short={'ok' if extension_ok_short else 'blocked'})"
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

        # ------------------------------------------------------------------
        # Unified TP/SL via trade_management
        # ------------------------------------------------------------------

        from app.services.trade_management import OrderContext, compute_tp_sl_pct

        entry_price = helpers.get_last_price(symbol)
        if entry_price is None:
            return None

        side = "long" if buy_signal else "short" if sell_signal else None
        if side is None:
            return None

        # Fix 3: size the exits to the ANALYSIS timeframe (now 15m), not the
        # global LTF.  ``indicators`` is the resolved analysis block, so its
        # ATR% matches the timeframe on which the fill occurs.  The HTF ATR
        # (``atr_pct_htf``) is only used for the volatility multiplier.
        atr_tf_pct = helpers.extract_float(indicators.get("atr_pct")) or 1.0
        atr_htf_pct = helpers.extract_float(indicators.get("atr_pct_htf")) or atr_tf_pct
        vpoc = helpers.extract_float(indicators.get("vpoc"))
        va_width = helpers.extract_float(indicators.get("value_area_width"))
        swing_high_val = helpers.extract_float(indicators.get("swing_high"))
        swing_low_val = helpers.extract_float(indicators.get("swing_low"))

        static_tp = helpers.extract_float(config.get("tp_pct"))
        static_sl = helpers.extract_float(config.get("sl_pct"))

        # Fix 5: adaptive ATR scaling applies to SIZING only (the ATR fallback
        # and clamps).  The min_atr_pct gate and the proximity/extension checks
        # above use the UNSCALED ``atr_pct``, so high-volatility regimes no
        # longer simultaneously starve entries and widen stops.
        sizing_atr_pct = atr_tf_pct
        if cfg.get("use_adaptive_atr", False):
            if sizing_atr_pct < 1.5:
                sizing_atr_pct *= 1.20
            elif sizing_atr_pct < 3.0:
                sizing_atr_pct *= 1.80
            else:
                sizing_atr_pct *= 2.50

        # Thesis-specific structural levels: TP at the nearest swing high/low
        # beyond price, SL anchored to structural invalidation (Fix 6).
        tp_target: float | None = None
        sl_level: float | None = None
        if use_structural_sizing and entry_price and entry_price > 0:
            atr_price = (sizing_atr_pct / 100.0) * entry_price
            structure = indicators.get("structure") or {}
            swing_highs = structure.get("swing_highs") or []
            swing_lows = structure.get("swing_lows") or []
            ohlcv_compact = indicators.get("ohlcv") or []
            _curr = ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {}
            curr_low = helpers.extract_float(_curr.get("low"))
            curr_high = helpers.extract_float(_curr.get("high"))

            # The pullback level (the value price pulled back to) for SL anchoring.
            if side == "long":
                _levels = [l for l in (pullback_ema, vwap_value) if l is not None and l > 0 and l <= entry_price]
                pullback_level = max(_levels) if _levels else None
            else:
                _levels = [l for l in (pullback_ema, vwap_value) if l is not None and l > 0 and l >= entry_price]
                pullback_level = min(_levels) if _levels else None

            if side == "long":
                # Trend-following geometry: target the FARTHEST swing high above
                # entry (within the ATR max clamp) so the trend has room to run,
                # instead of the nearest swing high which produces a razor-thin
                # TP (the anti-trend-following failure mode: tight TP / wide SL).
                _tp_cands = [
                    helpers.extract_float(sh.get("price"))
                    for sh in swing_highs if isinstance(sh, dict)
                ]
                _tp_cands = [p for p in _tp_cands if p is not None and p > entry_price]
                if _tp_cands:
                    _max_tp_dist = atr_max_tp_mult * atr_price
                    _within = [p for p in _tp_cands if p - entry_price <= _max_tp_dist]
                    tp_target = max(_within if _within else _tp_cands)
                # Fix 6: anchor SL to structural invalidation — the nearest
                # swing low below entry, else below the pullback level by a
                # volatility buffer, else (last resort) the candle wick.
                _below = [
                    helpers.extract_float(sl.get("price"))
                    for sl in swing_lows if isinstance(sl, dict)
                ]
                _below = [p for p in _below if p is not None and p < entry_price]
                _sl_anchor = max(_below) if _below else None
                if _sl_anchor is not None:
                    sl_level = _sl_anchor - structural_sl_buffer_atr * atr_price
                elif pullback_level is not None:
                    sl_level = pullback_level - structural_sl_buffer_atr * atr_price
                elif curr_low is not None:
                    sl_level = curr_low - structural_sl_buffer_atr * atr_price
                    helpers.emit_debug(
                        f"TrendPullback: {symbol} — structural SL fell back to wick anchor (no swing/level)"
                    )
            else:
                # Symmetric short-side fix: target the FARTHEST swing low below
                # entry (within the ATR max clamp) so the downtrend can run.
                _tp_cands = [
                    helpers.extract_float(sl.get("price"))
                    for sl in swing_lows if isinstance(sl, dict)
                ]
                _tp_cands = [p for p in _tp_cands if p is not None and p < entry_price]
                if _tp_cands:
                    _max_tp_dist = atr_max_tp_mult * atr_price
                    _within = [p for p in _tp_cands if entry_price - p <= _max_tp_dist]
                    tp_target = min(_within if _within else _tp_cands)
                _above = [
                    helpers.extract_float(sh.get("price"))
                    for sh in swing_highs if isinstance(sh, dict)
                ]
                _above = [p for p in _above if p is not None and p > entry_price]
                _sl_anchor = min(_above) if _above else None
                if _sl_anchor is not None:
                    sl_level = _sl_anchor + structural_sl_buffer_atr * atr_price
                elif pullback_level is not None:
                    sl_level = pullback_level + structural_sl_buffer_atr * atr_price
                elif curr_high is not None:
                    sl_level = curr_high + structural_sl_buffer_atr * atr_price
                    helpers.emit_debug(
                        f"TrendPullback: {symbol} — structural SL fell back to wick anchor (no swing/level)"
                    )

        tp_pct_final, sl_pct_final = compute_tp_sl_pct(
            entry=entry_price,
            side=side,
            ctx=OrderContext(
                atr_tf_pct=sizing_atr_pct,
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
            ),
            static_tp_pct=static_tp,
            static_sl_pct=static_sl,
            atr_tp_multiplier=atr_tp_multiplier if use_atr_sizing else None,
            atr_sl_multiplier=atr_sl_multiplier if use_atr_sizing else None,
            audit=lambda msg: helpers.emit_debug(
                f"TrendPullback: {symbol} — {msg}"
            ),
        )

        level_str = '+'.join(touched_levels)
        trend_word = "bullish" if buy_signal else "bearish"
        adx_str = f"{adx:.1f}" if adx is not None else "n/a"

        return StrategySignal(
            direction="buy" if side == "long" else "sell",
            strategy_name=self.name,
            tp_pct=tp_pct_final,
            sl_pct=sl_pct_final,
            rationale=(
                f"TrendPullback {'BUY' if side=='long' else 'SELL'}: pullback to {level_str} "
                f"in {trend_word} HTF trend (ADX={adx_str}) [trade_mgmt]"
            ),
        )
