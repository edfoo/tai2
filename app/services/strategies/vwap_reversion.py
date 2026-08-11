"""VWAP Reversion strategy.

Enters when price is extended >N ATR from VWAP and the current candle closes
back toward VWAP.  VWAP is a strong magnet on alt-coins; intraday deviations
mean-revert hard.  This catches setups RSI misses — price can be near VWAP
with neutral RSI but still extended on a longer lookback.

Entry conditions (long example):
  1. Price is > ``vwap_min_distance_atr`` × ATR below VWAP (extended).
  2. Current candle closes back toward VWAP (close > prev close).
  3. Optional: HTF trend alignment, regime gate (BB bandwidth percentile).

Exit:
  - TP: reach VWAP (or ATR-scaled).
  - SL: extend further (``atr_sl_multiplier`` × ATR beyond entry).
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile, resolve_analysis_block
from .defaults import merged_config
from .liquidity_helpers import funding_is_blocked


class VWAPReversionStrategy:
    """VWAP-distance mean-reversion strategy.

    Config keys (all live under ``config["strategies"]["vwap_reversion"]``):
      - ``enabled`` (bool): master switch
      - ``vwap_min_distance_atr`` (float, default 2.0): minimum distance from
        VWAP in ATR units to qualify as "extended".
            - ``vwap_max_distance_atr`` (float, default 3.0): maximum distance from
        VWAP in ATR units.  Beyond this, the extension is likely a genuine
        trend/breakout, not a reversion setup — entering is catching a
        falling knife.  0 = disabled.
            - ``max_adx`` (float, default 25.0): block entry when ADX is above this
        — a strong trend means the VWAP deviation is a real move, not noise
        that will revert.  0 = disabled.
      - ``require_closeback`` (bool, default True): current candle must close
        back toward VWAP (close > prev close for longs, close < prev close
        for shorts) — confirms the reversion has started.
      - ``require_htf_trend`` (bool, default True): auto-disabled when no
        HTF data is available.
      - ``require_regime`` (bool, default True): only enter when BB bandwidth
        percentile is below ``max_bb_bandwidth_percentile`` (chop regime
        where VWAP reversion is most reliable).
      - ``max_bb_bandwidth_percentile`` (float, default 55): upper bandwidth
        percentile for the regime gate.
      - ``regime_lookback`` (int, default 50): bars for the percentile.
      - ``use_structural_sizing`` (bool, default True): use structural TP/SL
        based on VWAP and the extension candle instead of (or clamped by)
        ATR.  TP targets VWAP (the magnet price reverts to).  SL sits just
        beyond the extension candle's extreme (the invalidation).  ATR
        clamps both to a sane range.  Falls back to ATR sizing when VWAP
        or candle data is unavailable.
      - ``structural_sl_buffer_atr`` (float, default 0.15): SL is placed
        this many ATR units *beyond* the extension candle's low/high.
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
      - ``atr_tp_multiplier`` (float, default 1.8): TP = multiplier × ATR%.
        Must be >= atr_sl_multiplier so R:R >= 1.0.
      - ``atr_sl_multiplier`` (float, default 1.0): SL = multiplier × ATR%.
        Tighter than TP — the extension is the invalidation; if it continues
        further, the reversion thesis is wrong.
      - ``min_atr_pct`` (float, default 1.0): skip dead coins.
      - ``tp_pct`` (float, default None): static TP % fallback.
      - ``sl_pct`` (float, default None): static SL % fallback.
      - ``flip_launcher_direction`` (str, default None): invert the
        Launcher's trade direction before execution. One of "both",
        "from_long" (only BUY→SELL), "from_short" (only SELL→BUY),
        or None to disable. TP/SL are mirrored around last_price so
        they land on the correct side for the flipped direction.
    """

    name = "vwap_reversion"

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal for VWAP reversion, or None."""
        if not bool(config.get("enabled", False)):
            return None

        # Merge caller config over the canonical defaults so any missing key
        # falls back to an acceptable, validated value.
        cfg = merged_config(config, self.name)

        # ---- HTF regime gate (chop preferred by default) -------------------
        # Configurable per-strategy: "chop" (block when HTF trending — the
        # legacy VWAP behaviour), "trend", or "off".  Neutral (no HTF data)
        # never blocks.
        from app.services.indicator_service import htf_regime_allows

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        adx_htf = helpers.extract_float(indicators.get("adx_htf"))
        chop_htf = helpers.extract_float(indicators.get("choppiness_htf"))
        htf_pref = cfg.get("htf_regime_preference", "chop")

        if not htf_regime_allows(adx_htf, chop_htf, htf_pref):
            return None  # Avoid fading VWAP on strong trend days

        # ── Config ────────────────────────────────────────────────────
        vwap_min_distance_atr = helpers.extract_float(cfg.get("vwap_min_distance_atr"))
        if vwap_min_distance_atr is None:
            vwap_min_distance_atr = 2.0
        vwap_max_distance_atr = helpers.extract_float(cfg.get("vwap_max_distance_atr"))
        if vwap_max_distance_atr is None:
            vwap_max_distance_atr = 3.0
        max_adx = helpers.extract_float(cfg.get("max_adx"))
        if max_adx is None:
            max_adx = 25.0
        require_closeback = bool(cfg.get("require_closeback", True))
        require_htf_trend = bool(cfg.get("require_htf_trend", True))
        require_regime = bool(cfg.get("require_regime", True))
        max_bb_bandwidth_percentile = helpers.extract_float(
            cfg.get("max_bb_bandwidth_percentile")
        )
        if max_bb_bandwidth_percentile is None:
            max_bb_bandwidth_percentile = 55.0
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
            atr_tp_multiplier = 1.8
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.0
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0
        # ── Regime consolidation (F4) ──────────────────────────────────
        # ``regime_primary_gate`` picks the *single* "not-trending" filter
        # that actually blocks.  Default "adx" keeps the LTF ADX gate as the
        # primary trend guard (it reads the analysis timeframe directly);
        # the BB-bandwidth gate is then treated as soft (logged, not
        # blocking).  Set "bb" to make the BB-bandwidth percentile the
        # primary chop gate and demote ADX to soft.
        regime_primary_gate = str(cfg.get("regime_primary_gate", "adx")).lower()
        # ── Liquidity-aware gates (§3) ────────────────────────────────
        # ``require_no_funding_bias`` (default off): block reversion against a
        # strong funding-rate crowding in the trade direction.  Mirrors the
        # guide's |funding_z| < 0.7 bias filter using an absolute rate so it
        # works from a single snapshot (no 30-day history required).
        require_no_funding_bias = bool(cfg.get("require_no_funding_bias", False))
        funding_max_abs_rate = helpers.extract_float(cfg.get("funding_max_abs_rate"))
        if funding_max_abs_rate is None:
            funding_max_abs_rate = 0.0007  # ≈ 0.07 % — mirrors |funding_z| < 0.7

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        # ── Reference price (F2): ONE price for all entry gates ───────
        # All signal gates (distance from VWAP, closeback, HTF alignment)
        # must share the same reference so a live ticker print cannot
        # arm-bandit the entry.  We use the previous *closed* analysis-frame
        # candle's close.  The live ticker is reserved strictly for execution
        # sizing / order construction below.
        gate_ohlcv = indicators.get("ohlcv") or []
        ref_price: float | None = None
        if isinstance(gate_ohlcv, list) and gate_ohlcv and isinstance(gate_ohlcv[-1], dict):
            ref_price = helpers.extract_float(gate_ohlcv[-1].get("close"))
        if ref_price is None or ref_price <= 0:
            # Fall back to the live ticker and make the path transparent.
            ref_price = helpers.get_last_price(symbol)
            if ref_price is None:
                helpers.emit_debug(
                    f"VWAPReversion: {symbol} — no signal (no closed-close reference price)"
                )
                return None
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — reference price fell back to live ticker "
                f"(no closed candle available)"
            )

        vwap_value = helpers.extract_float(indicators.get("vwap"))
        last_price = helpers.get_last_price(symbol)
        atr_pct = helpers.extract_float(indicators.get("atr_pct"))
        if atr_pct is None or atr_pct <= 0:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal (ATR% unavailable or zero)"
            )
            return None

        if vwap_value is None or vwap_value <= 0 or last_price is None:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal (VWAP or price unavailable)"
            )
            return None

        # ── ATR in price units, measured at the reference price ───────
        # atr_pct is "ATR as % of price", so ATR_price = atr_pct/100 * price.
        # Use the reference (closed-close) price so the ATR-scaling is
        # consistent with the gate reference (F2/F5).
        atr_price = (atr_pct / 100.0) * ref_price
        if atr_price <= 0:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal (ATR price <= 0)"
            )
            return None

        # ── Min ATR% filter ───────────────────────────────────────────
        if min_atr_pct > 0 and atr_pct < min_atr_pct:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(ATR%={atr_pct:.2f} < min={min_atr_pct:.2f})"
            )
            return None

        # ── ADX gate (F4): blocking only when it is the primary gate ──
        # A high ADX means the VWAP deviation is a real directional move,
        # not noise that will revert.  Entering is catching a falling knife.
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))
        adx_blocks = regime_primary_gate == "adx" and max_adx > 0 and adx is not None and adx > max_adx
        if adx_blocks:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(ADX={adx:.1f} > max={max_adx:.1f} — strong trend, reversion unlikely)"
            )
            return None
        if max_adx > 0 and adx is not None and adx > max_adx:
            # Soft (secondary) ADX filter — surfaced in /debug, does not block.
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — soft ADX={adx:.1f} > max={max_adx:.1f} "
                f"(secondary filter; primary gate={regime_primary_gate})"
            )

        # ── Distance from VWAP in ATR units (F2: reference price) ─────
        distance = ref_price - vwap_value  # positive = above VWAP
        distance_atr = abs(distance) / atr_price
        if distance_atr < vwap_min_distance_atr:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(distance={distance_atr:.2f} ATR < min={vwap_min_distance_atr:.2f} ATR, "
                f"VWAP={vwap_value:.6g}, ref_price={ref_price:.6g})"
            )
            return None
        if vwap_max_distance_atr > 0 and distance_atr > vwap_max_distance_atr:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(distance={distance_atr:.2f} ATR > max={vwap_max_distance_atr:.2f} ATR "
                f"— extension too far, likely a real breakout not a reversion)"
            )
            return None

        # ── Closeback confirmation ───────────────────────────────────
        # For longs: price below VWAP, current candle closes up (toward VWAP).
        # For shorts: price above VWAP, current candle closes down (toward VWAP).
        # Uses the same closed-close reference as the distance gate (F2).
        closeback_long_ok = True
        closeback_short_ok = True
        if require_closeback:
            if len(gate_ohlcv) < 2 or not isinstance(gate_ohlcv[-1], dict) or not isinstance(gate_ohlcv[-2], dict):
                helpers.emit_debug(
                    f"VWAPReversion: {symbol} — no signal "
                    f"(insufficient candles for closeback check)"
                )
                return None
            _prev_close = helpers.extract_float(gate_ohlcv[-2].get("close"))
            _curr_close = helpers.extract_float(gate_ohlcv[-1].get("close"))
            if _prev_close is None or _curr_close is None:
                helpers.emit_debug(
                    f"VWAPReversion: {symbol} — no signal (closeback OHLC unavailable)"
                )
                return None
            # Long: price below VWAP, closing up toward VWAP.
            closeback_long_ok = distance < 0 and _curr_close > _prev_close
            # Short: price above VWAP, closing down toward VWAP.
            closeback_short_ok = distance > 0 and _curr_close < _prev_close

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
                f"VWAPReversion: {symbol} — HTF unavailable, auto-disabling require_htf_trend"
            )

        # ── Regime gate: BB bandwidth percentile (F4) ────────────────
        # Reads the *analysis timeframe* (15m) and directly encodes "chop
        # where VWAP reversion is reliable".  When ``regime_primary_gate`` is
        # "bb" this blocks; otherwise it is a soft rationale-only filter.
        # The BB gate is the recommended primary chop filter per the tuning
        # guide because it reads the analysis TF.
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
            gate_ohlcv, bb_bandwidth, lookback=regime_lookback
        )
        bb_fail = (
            require_regime
            and bw_percentile is not None
            and bw_percentile > max_bb_bandwidth_percentile
        )
        bb_primary = regime_primary_gate == "bb"
        if bb_primary and bb_fail:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(regime: BW pct={bw_percentile:.0f} > max={max_bb_bandwidth_percentile:.0f})"
                if bw_percentile is not None else
                f"VWAPReversion: {symbol} — no signal (regime: BW percentile unavailable)"
            )
            return None
        if bb_fail:
            # Soft (secondary) BB filter — surfaced, does not block.
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — soft regime BW pct={bw_percentile:.0f} > "
                f"max={max_bb_bandwidth_percentile:.0f} (secondary filter; "
                f"primary gate={regime_primary_gate})"
            )

        # ── Funding bias gate (§3) ────────────────────────────────────
        # Block reversion against heavy funding crowding in the trade
        # direction (e.g. don't buy the dip when longs are already extremely
        # crowded).  Neutral (no funding data) passes.
        funding = sym_data.get("funding_rate") or {}
        # Phase 0d: use the pre-computed 30-day rolling z-score when available;
        # falls back to the absolute-rate proxy when history is not yet seeded.
        _funding_z: float | None = sym_data.get("funding_z")
        funding_blocked_long, f_info = funding_is_blocked(
            funding, direction="long", max_abs_rate=funding_max_abs_rate, funding_z=_funding_z
        )
        funding_blocked_short, _ = funding_is_blocked(
            funding, direction="short", max_abs_rate=funding_max_abs_rate, funding_z=_funding_z
        )

        # ── Direction decision ────────────────────────────────────────
        # Long: price extended below VWAP, closing back up.
        # Short: price extended above VWAP, closing back down.
        buy_signal = (
            distance < 0
            and closeback_long_ok
            and (not require_htf_trend or not htf_available or htf_bullish)
            and (not require_no_funding_bias or not funding_blocked_long)
        )
        sell_signal = (
            distance > 0
            and closeback_short_ok
            and (not require_htf_trend or not htf_available or htf_bearish)
            and (not require_no_funding_bias or not funding_blocked_short)
        )

        if not buy_signal and not sell_signal:
            parts = [
                f"dist={distance_atr:.2f} ATR (need >={vwap_min_distance_atr:.2f})",
                f"VWAP={vwap_value:.6g}/ref_price={ref_price:.6g}",
            ]
            if require_closeback:
                parts.append(
                    f"closeback(long={'ok' if closeback_long_ok else 'blocked'}, "
                    f"short={'ok' if closeback_short_ok else 'blocked'})"
                )
            if require_htf_trend:
                if not htf_available:
                    parts.append("HTF=skipped(no data)")
                elif htf_ema50 is not None and htf_ema200 is not None:
                    parts.append(
                        f"HTF={'bull' if htf_bullish else 'bear' if htf_bearish else 'flat'}"
                    )
                else:
                    parts.append("HTF=n/a")
            if require_regime:
                parts.append(
                    f"BW_pct={bw_percentile:.0f}" if bw_percentile is not None else "BW_pct=n/a"
                )
            if regime_primary_gate == "adx" and max_adx > 0 and adx is not None:
                parts.append(
                    f"ADX={adx:.1f}" + ("(soft)" if adx <= max_adx else "(primary-block)")
                )
            if require_no_funding_bias:
                if f_info.get("available"):
                    parts.append(f"funding={f_info['rate']:.5g}")
                else:
                    parts.append("funding=n/a")
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal ({', '.join(parts)})"
            )
            return None

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

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        # ── Sizing ATR (F5): adaptive scaling applies ONLY to the risk/sizing
        # ATR, never the entry distance gate (which uses the unscaled ATR% so
        # the extension threshold is stable).  This decouples "how far is the
        # entry" from "how wide is the risk".
        atr_tf_pct = helpers.extract_float(indicators.get("atr_pct")) or 1.0
        if atr_tf_pct > 0 and cfg.get("use_adaptive_atr", False):
            _scaled = atr_tf_pct
            if _scaled < 1.5:
                _scaled *= 1.20
            elif _scaled < 3.0:
                _scaled *= 1.80
            else:
                _scaled *= 2.50
            atr_tf_pct = _scaled
        atr_htf_pct = helpers.extract_float(indicators.get("atr_pct_htf")) or atr_tf_pct
        vpoc = helpers.extract_float(indicators.get("vpoc"))
        va_width = helpers.extract_float(indicators.get("value_area_width"))
        swing_high = helpers.extract_float(indicators.get("swing_high"))
        swing_low = helpers.extract_float(indicators.get("swing_low"))

        static_tp = helpers.extract_float(config.get("tp_pct"))
        static_sl = helpers.extract_float(config.get("sl_pct"))

        # Thesis-specific structural levels: TP at VWAP (the magnet), SL beyond
        # the extension candle's extreme (further extension = thesis wrong).
        tp_target: float | None = None
        sl_level: float | None = None
        if use_structural_sizing and entry_price and entry_price > 0 and vwap_value and vwap_value > 0:
            atr_price = (atr_tf_pct / 100.0) * entry_price
            ohlcv_compact = indicators.get("ohlcv") or []
            _curr = ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {}
            curr_low = helpers.extract_float(_curr.get("low"))
            curr_high = helpers.extract_float(_curr.get("high"))
            if side == "long":
                tp_target = vwap_value
                if curr_low is not None:
                    sl_level = curr_low - structural_sl_buffer_atr * atr_price
            else:
                tp_target = vwap_value
                if curr_high is not None:
                    sl_level = curr_high + structural_sl_buffer_atr * atr_price

        def _audit(msg: str) -> None:
            helpers.emit_debug(f"VWAPReversion: {symbol} — {msg} [trade_mgmt]")

        tp_pct_final, sl_pct_final = compute_tp_sl_pct(
            entry=entry_price,
            side=side,
            ctx=OrderContext(
                atr_tf_pct=atr_tf_pct,
                atr_htf_pct=atr_htf_pct,
                vpoc=vpoc,
                value_area_width=va_width,
                swing_high=swing_high,
                swing_low=swing_low,
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
            audit=_audit,
        )

        return StrategySignal(
            direction="buy" if side == "long" else "sell",
            strategy_name=self.name,
            tp_pct=tp_pct_final,
            sl_pct=sl_pct_final,
            rationale=(
                f"VWAPReversion {'BUY' if side=='long' else 'SELL'}: dist={distance_atr:.2f} ATR [trade_mgmt]"
            ),
        )
