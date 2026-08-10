"""Mean Reversion Scalping strategy.

Enters when RSI is oversold/overbought and optional confirming filters
(CMF, HTF trend, ADX, BB position, footprint delta) agree.
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile
from .defaults import merged_config
from .liquidity_helpers import funding_is_blocked, order_book_imbalance


class MeanReversionStrategy:
    """Rule-based RSI mean-reversion strategy.

    Config keys (all live under ``config["strategies"]["mean_reversion"]``):
      - ``enabled`` (bool): master switch
      - ``rsi_oversold`` (float, default 28): BUY when RSI < this
      - ``rsi_overbought`` (float, default 72): SELL when RSI > this
      - ``require_htf_trend`` (bool, default True): auto-disabled when no
        HTF data is available (e.g. 1D LTF has no higher timeframe).
      - ``require_cmf`` (bool, default True): requires CMF already positive
        for BUY / negative for SELL — enters AFTER the turn. For catching
        the bottom, prefer ``require_cmf_cross`` instead.
      - ``require_htf_cmf`` (bool, default False)
      - ``require_cmf_cross`` (bool, default True): CMF must have just
        crossed zero this bar — catches the turn earlier than require_cmf.
      - ``require_cmf_no_divergence`` (bool, default False)
      - ``require_footprint_delta`` (bool, default False)
      - ``require_bb_position`` (bool, default True)
      - ``bb_proximity_pct`` (float, default 0.25)
      - ``min_bb_bandwidth`` (float, default 2.0)
      - ``max_bb_bandwidth`` (float, default 0.0)
      - ``min_adx`` (float, default 0.0)
      - ``max_adx`` (float, default 25.0): chop-only gate; lower = stricter
        no-trend filter (recommended ~22-25).
      - ``tp_pct`` (float, default None): strategy-level TP %. Mean reversion
        typically wants a tight TP (reversion to midline). Falls back to
        launcher-level if None.
      - ``sl_pct`` (float, default None): strategy-level SL %. Mean reversion
        typically wants a wider SL (allow exhaustion wick to extend).
      - ``require_candle_rejection`` (bool, default True): require upper wick
        for shorts, lower wick for longs (exhaustion confirmation)
      - ``candle_rejection_pct`` (float, default 30): minimum wick size as %
        of candle range (30 = wick is 30%+ of the candle)
      - ``require_vwap_reversion`` (bool, default True): require price extended
        from VWAP AND closing back toward it
      - ``vwap_min_distance_pct`` (float, default 1.0): minimum % distance from
        VWAP to qualify as "extended"
      - ``require_volume_cooling`` (bool, default True): require volume RSI
        below threshold (volume momentum fading)
      - ``volume_rsi_max`` (float, default 70.0): maximum volume RSI to allow entry
      - ``require_regime`` (bool, default True)
      - ``max_bb_bandwidth_percentile`` (float, default 40)
      - ``use_structural_sizing`` (bool, default True): use structural TP/SL
        based on Bollinger Bands and the entry candle instead of (or
        clamped by) ATR.  TP targets the BB middle band (the mean price
        reverts to).  SL sits just beyond the entry candle's wick (the
        exhaustion extreme).  ATR clamps both to a sane range.  Falls back
        to ATR sizing when BB or candle data is unavailable.
      - ``structural_sl_buffer_atr`` (float, default 0.15): SL is placed
        this many ATR units *beyond* the entry candle's low/high.
      - ``atr_min_tp_mult`` (float, default 0.5): structural TP distance
        must be at least this × ATR% from entry.
      - ``atr_max_tp_mult`` (float, default 4.0): structural TP distance
        capped at this × ATR% from entry.
      - ``atr_min_sl_mult`` (float, default 0.3): structural SL distance
        must be at least this × ATR% from entry.
      - ``atr_max_sl_mult`` (float, default 3.0): structural SL distance
        capped at this × ATR% from entry.
      - ``use_atr_sizing`` (bool, default True)
      - ``atr_tp_multiplier`` (float, default 2.0): TP = multiplier × ATR%.
        Must be >= atr_sl_multiplier so the reward-to-risk ratio is >= 1.0;
        an inverted R:R (TP < SL) mathematically guarantees a losing bias
        because MR win rates are naturally < 50%.
      - ``atr_sl_multiplier`` (float, default 1.5): SL = multiplier × ATR%.
        Must be <= atr_tp_multiplier (see above).
      - ``min_atr_pct`` (float, default 1.3)
    """

    name = "mean_reversion"

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal for mean-reversion, or None."""
        if not bool(config.get("enabled", False)):
            return None

        # Merge caller config over the canonical defaults so any missing key
        # falls back to an acceptable, validated value.
        cfg = merged_config(config, self.name)

        rsi_oversold = helpers.extract_float(cfg.get("rsi_oversold"))
        if rsi_oversold is None:
            rsi_oversold = 30.0
        rsi_overbought = helpers.extract_float(cfg.get("rsi_overbought"))
        if rsi_overbought is None:
            rsi_overbought = 70.0
        require_htf_trend = bool(cfg.get("require_htf_trend", True))
        require_cmf = bool(cfg.get("require_cmf", True))
        require_htf_cmf = bool(cfg.get("require_htf_cmf", False))
        # CMF cross is a rare event that cuts frequency ~90%.  BB position +
        # candle rejection already confirm extension+exhaustion, so the cross
        # is redundant.  Default off for frequency; enable for ultra-strict.
        require_cmf_cross = bool(cfg.get("require_cmf_cross", False))
        require_cmf_no_divergence = bool(cfg.get("require_cmf_no_divergence", False))
        require_footprint_delta = bool(cfg.get("require_footprint_delta", False))
        require_bb_position = bool(cfg.get("require_bb_position", True))
        require_candle_rejection = bool(cfg.get("require_candle_rejection", True))
        candle_rejection_pct = helpers.extract_float(cfg.get("candle_rejection_pct"))
        if candle_rejection_pct is None:
            candle_rejection_pct = 30.0
        # VWAP reversion is redundant with BB position + candle rejection
        # (all three confirm "price extended and snapping back").  Default
        # off to avoid triple-gating the same signal.
        require_vwap_reversion = bool(cfg.get("require_vwap_reversion", False))
        vwap_min_distance_pct = helpers.extract_float(cfg.get("vwap_min_distance_pct"))
        if vwap_min_distance_pct is None:
            vwap_min_distance_pct = 1.0
        # Volume cooling is redundant with candle rejection (both confirm
        # spike is fading).  Default off; enable for ultra-strict.
        require_volume_cooling = bool(cfg.get("require_volume_cooling", False))
        volume_rsi_max = helpers.extract_float(cfg.get("volume_rsi_max"))
        if volume_rsi_max is None:
            volume_rsi_max = 80.0
        # ── Regime gate (BB bandwidth percentile) ──────────────────────
        # MR works best in low-volatility chop.  When require_regime is True,
        # the current BB bandwidth must be below max_bb_bandwidth_percentile
        # relative to the last N candles (e.g. < 55th percentile = chop).
        require_regime = bool(cfg.get("require_regime", True))
        max_bb_bandwidth_percentile = helpers.extract_float(cfg.get("max_bb_bandwidth_percentile"))
        if max_bb_bandwidth_percentile is None:
            max_bb_bandwidth_percentile = 55.0
        regime_lookback = helpers.extract_float(cfg.get("regime_lookback"))
        if regime_lookback is None:
            regime_lookback = 50
        # ── ATR-scaled TP/SL ────────────────────────────────────────────
        # When use_atr_sizing is True, TP/SL are computed as
        # multiplier × ATR% instead of fixed percentages.  This adapts to
        # the volatility regime.  Recommended: wider SL (survive noise)
        # and modest TP (bank the snapback).
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
            atr_tp_multiplier = 2.0
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.5
        # ── Minimum ATR% filter ───────────────────────────────────────
        # Skip entries on coins with ATR% below this threshold — too quiet
        # for a meaningful reversion.  0 = disabled.
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0
        bb_proximity_pct = helpers.extract_float(cfg.get("bb_proximity_pct"))
        if bb_proximity_pct is None:
            bb_proximity_pct = 0.5
        min_bb_bandwidth = helpers.extract_float(cfg.get("min_bb_bandwidth"))
        if min_bb_bandwidth is None:
            min_bb_bandwidth = 2.0
        max_bb_bandwidth = helpers.extract_float(cfg.get("max_bb_bandwidth"))
        if max_bb_bandwidth is None:
            max_bb_bandwidth = 0.0
        min_adx = helpers.extract_float(cfg.get("min_adx"))
        if min_adx is None:
            min_adx = 0.0
        max_adx = helpers.extract_float(cfg.get("max_adx"))
        if max_adx is None:
            max_adx = 28.0
        # ── Liquidity-aware gates (§3) ────────────────────────────────
        # All disabled by default so existing live behaviour is unchanged until
        # individually tuned.  ``require_price_in_va``: only enter when price is
        # inside the 70 % value area (mean reversion wants price fading toward
        # the mean, not trending out of value).  ``require_no_extreme_funding``:
        # block extreme crowdfunding in the trade direction.  ``require_balanced_book``:
        # block when order-book imbalance is extreme (no liquidity to fade into).
        require_price_in_va = bool(cfg.get("require_price_in_va", False))
        require_no_extreme_funding = bool(cfg.get("require_no_extreme_funding", False))
        funding_max_abs_rate = helpers.extract_float(cfg.get("funding_max_abs_rate"))
        if funding_max_abs_rate is None:
            funding_max_abs_rate = 0.001
        require_balanced_book = bool(cfg.get("require_balanced_book", False))
        imbalance_min = helpers.extract_float(cfg.get("imbalance_min"))
        imbalance_max = helpers.extract_float(cfg.get("imbalance_max"))
        if imbalance_min is None:
            imbalance_min = 0.6
        if imbalance_max is None:
            imbalance_max = 1.4

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        # ── Liquidity-aware data extraction (§3) ──────────────────────
        # Read the same per-symbol fields the broader launcher uses so the
        # gates work in both live and backtest snapshots.  All gates are
        # opt-in (default off) and degrade to "pass" when data is absent.
        last_price = helpers.get_last_price(symbol)

        def _in_value_area() -> bool:
            va_high = helpers.extract_float(indicators.get("value_area_high"))
            va_low = helpers.extract_float(indicators.get("value_area_low"))
            if va_high is None or va_low is None or last_price is None:
                return True  # no VA data → neutral (don't block)
            return va_low <= last_price <= va_high

        va_ok = _in_value_area()

        funding = sym_data.get("funding_rate") or {}
        # Phase 0d: use the pre-computed 30-day rolling z-score when available;
        # falls back to the absolute-rate proxy when history is not yet seeded.
        _funding_z: float | None = sym_data.get("funding_z")
        funding_blocked_long, _f_info = funding_is_blocked(
            funding, direction="long", max_abs_rate=funding_max_abs_rate, funding_z=_funding_z
        )
        funding_blocked_short, _ = funding_is_blocked(
            funding, direction="short", max_abs_rate=funding_max_abs_rate, funding_z=_funding_z
        )

        order_book = sym_data.get("order_book") or {}
        imbalance = order_book_imbalance(order_book)
        imbalance_ok = (
            not require_balanced_book
            or imbalance is None
            or (imbalance_min <= imbalance <= imbalance_max)
        )

        # ---- Higher-timeframe regime gate ----------------------------------
        # Configurable per-strategy: "chop" (block when HTF trending — the
        # legacy MR behaviour), "trend" (block when HTF not trending), or
        # "off" (disable the gate).  Neutral (no HTF data) never blocks.
        from app.services.indicator_service import htf_regime_allows  # local import to avoid circulars

        adx_htf = helpers.extract_float(indicators.get("adx_htf"))
        chop_htf = helpers.extract_float(indicators.get("choppiness_htf"))
        htf_pref = cfg.get("htf_regime_preference", "chop")

        if not htf_regime_allows(adx_htf, chop_htf, htf_pref):
            # Market is trending → disable mean-reversion entry prematurely.
            return None

        rsi = helpers.extract_float(indicators.get("rsi"))
        # Use 14-period CMF for LTF entries (gap 2); fall back to 20-period if unavailable.
        _cmf_14_block = indicators.get("cmf_14") or indicators.get("cmf") or {}
        cmf = helpers.extract_float(_cmf_14_block.get("value"))
        cmf_series_vals: list[float] = _cmf_14_block.get("series") or []
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))

        # Bollinger Band position filter: entry must be near/beyond the band.
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
        bb_long_ok = (
            bb_last_price is not None
            and bb_lower is not None
            and bb_last_price <= bb_lower * (1.0 + bb_proximity_pct / 100.0)
        )
        bb_short_ok = (
            bb_last_price is not None
            and bb_upper is not None
            and bb_last_price >= bb_upper * (1.0 - bb_proximity_pct / 100.0)
        )

        # HTF indicators are nested inside the LTF indicators dict.
        htf_indicators: dict[str, Any] = indicators.get("htf_indicators") or {}
        htf_ma = htf_indicators.get("moving_averages") or {}
        htf_ema50 = helpers.extract_float(htf_ma.get("ema_50"))
        htf_ema200 = helpers.extract_float(htf_ma.get("ema_200"))
        htf_bullish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 > htf_ema200
        htf_bearish = htf_ema50 is not None and htf_ema200 is not None and htf_ema50 < htf_ema200

        # Auto-disable HTF filters when no HTF data is available (e.g. 1D LTF
        # has no higher timeframe). Without this guard, require_htf_trend=True
        # would silently block every signal on 1D because htf_bullish/bearish
        # are both False when htf_indicators is absent.
        htf_available = bool(htf_indicators)
        if require_htf_trend and not htf_available:
            helpers.emit_debug(
                f"MeanReversion: {symbol} — HTF indicators unavailable, "
                f"auto-disabling require_htf_trend for this evaluation"
            )
        if require_htf_cmf and not htf_available:
            helpers.emit_debug(
                f"MeanReversion: {symbol} — HTF indicators unavailable, "
                f"auto-disabling require_htf_cmf for this evaluation"
            )

        # HTF CMF governor (gap 1): HTF CMF must agree with trade direction.
        htf_cmf = helpers.extract_float((htf_indicators.get("cmf") or {}).get("value"))
        htf_cmf_bullish = htf_cmf is not None and htf_cmf > 0
        htf_cmf_bearish = htf_cmf is not None and htf_cmf < 0

        # CMF zero-line cross (gap 3a): CMF must have just crossed zero on this bar.
        cmf_crossed_up = False
        cmf_crossed_down = False
        if require_cmf_cross and len(cmf_series_vals) >= 2:
            prev_cmf = cmf_series_vals[-2]
            if cmf is not None:
                cmf_crossed_up = cmf > 0 and prev_cmf <= 0
                cmf_crossed_down = cmf < 0 and prev_cmf >= 0

        # CMF divergence (gap 3b): price direction vs CMF direction over last 5 bars.
        bearish_div = False
        bullish_div = False
        if require_cmf_no_divergence and len(cmf_series_vals) >= 5:
            ohlcv_compact = indicators.get("ohlcv") or []
            closes = [c["close"] for c in ohlcv_compact if isinstance(c, dict) and "close" in c]
            if len(closes) >= 5:
                price_up = closes[-1] > closes[-5]
                cmf_up = cmf_series_vals[-1] > cmf_series_vals[-5]
                bearish_div = price_up and not cmf_up   # price higher, CMF lower → exhaustion
                bullish_div = not price_up and cmf_up   # price lower, CMF higher → hidden strength

        # Footprint net delta from the live market metrics (populated by _compute_custom_metrics)
        # When footprint data is structurally absent (e.g. backtest — no historical
        # trade tape), the filter is skipped rather than blocking every signal.
        # When footprint data IS present, the delta must agree with direction.
        fp_net_delta: float | None = None
        fp_data_available = False
        if require_footprint_delta:
            fp_data = (sym_data.get("custom_metrics") or {}).get("footprint") or helpers.compute_footprint(symbol)
            if fp_data:
                fp_data_available = True
                fp_net_delta = helpers.extract_float(fp_data.get("net_delta"))

        # ── Candle rejection filter ──────────────────────────────────────────
        # Requires the current candle to show a rejection wick — i.e., the close
        # is significantly below the high (for shorts) or above the low (for longs).
        # This prevents entering mid-spike; the candle must show exhaustion.
        # candle_rejection_pct = minimum wick size as % of candle range.
        candle_rejection_long_ok = False
        candle_rejection_short_ok = False
        if require_candle_rejection:
            ohlcv_compact = indicators.get("ohlcv") or []
            if ohlcv_compact and isinstance(ohlcv_compact[-1], dict):
                _c = ohlcv_compact[-1]
                _high = helpers.extract_float(_c.get("high"))
                _low = helpers.extract_float(_c.get("low"))
                _close = helpers.extract_float(_c.get("close"))
                if _high is not None and _low is not None and _close is not None:
                    _range = _high - _low
                    if _range > 0:
                        # Upper wick = high - max(close, open); we use close for simplicity.
                        _upper_wick = _high - _close
                        _lower_wick = _close - _low
                        _upper_wick_pct = (_upper_wick / _range) * 100.0
                        _lower_wick_pct = (_lower_wick / _range) * 100.0
                        candle_rejection_long_ok = _lower_wick_pct >= candle_rejection_pct
                        candle_rejection_short_ok = _upper_wick_pct >= candle_rejection_pct

        # ── VWAP reversion filter ────────────────────────────────────────────
        # Requires price to be extended from VWAP (confirming the spike) AND
        # the current candle closing back toward VWAP (confirming reversion started).
        # vwap_min_distance_pct = minimum % distance from VWAP to qualify as "extended".
        vwap_value = helpers.extract_float(indicators.get("vwap"))
        vwap_long_ok = False
        vwap_short_ok = False
        if require_vwap_reversion and vwap_value is not None and vwap_value > 0 and bb_last_price is not None:
            _vwap_dist_pct = abs(bb_last_price - vwap_value) / vwap_value * 100.0
            if _vwap_dist_pct >= vwap_min_distance_pct:
                # Price is extended from VWAP. Now check if this candle is closing
                # back toward VWAP (reversion started).
                ohlcv_compact = indicators.get("ohlcv") or []
                if len(ohlcv_compact) >= 2 and isinstance(ohlcv_compact[-1], dict):
                    _prev_close = helpers.extract_float(ohlcv_compact[-2].get("close"))
                    _curr_close = helpers.extract_float(ohlcv_compact[-1].get("close"))
                    if _prev_close is not None and _curr_close is not None:
                        # For longs: price below VWAP, closing up toward it
                        vwap_long_ok = (
                            bb_last_price < vwap_value
                            and _curr_close > _prev_close
                        )
                        # For shorts: price above VWAP, closing down toward it
                        vwap_short_ok = (
                            bb_last_price > vwap_value
                            and _curr_close < _prev_close
                        )

        # ── Volume RSI cooling filter ────────────────────────────────────────
        # Volume RSI measures volume momentum. When extremely high, the spike
        # is still being driven by heavy volume. Wait for it to cool below
        # volume_rsi_max before entering — this signals buying pressure is fading.
        volume_rsi_series = indicators.get("volume_rsi_series") or []
        volume_rsi_value: float | None = None
        if volume_rsi_series:
            volume_rsi_value = helpers.extract_float(volume_rsi_series[-1])
        volume_cooling_ok = (
            not require_volume_cooling
            or (volume_rsi_value is not None and volume_rsi_value < volume_rsi_max)
        )

        # ── Regime gate: BB bandwidth percentile ──────────────────────
        # MR works best in low-volatility chop (low bandwidth percentile).
        ohlcv_compact = indicators.get("ohlcv") or []
        bw_percentile = compute_bb_bandwidth_percentile(
            ohlcv_compact, bb_bandwidth, lookback=int(regime_lookback)
        )
        regime_ok = (
            not require_regime
            or (bw_percentile is not None and bw_percentile <= max_bb_bandwidth_percentile)
        )

        # ── Minimum ATR% filter ──────────────────────────────────────
        # Skip entries on coins too quiet for a meaningful reversion.
        atr_pct_value = helpers.extract_float(indicators.get("atr_pct"))
        atr_ok = (
            min_atr_pct <= 0
            or (atr_pct_value is not None and atr_pct_value >= min_atr_pct)
        )

        if rsi is None:
            helpers.emit_debug(f"MeanReversion: {symbol} — no entry signal (RSI unavailable)")
            return None
        if min_adx > 0 and (adx is None or adx < min_adx):
            helpers.emit_debug(
                f"MeanReversion: {symbol} — no entry signal "
                f"(ADX={adx:.1f} < min={min_adx:.1f})"
            )
            return None
        if max_adx > 0 and adx is not None and adx > max_adx:
            helpers.emit_debug(
                f"MeanReversion: {symbol} — no entry signal "
                f"(ADX={adx:.1f} > max={max_adx:.1f})"
            )
            return None
        if min_bb_bandwidth > 0 and (bb_bandwidth is None or bb_bandwidth < min_bb_bandwidth):
            helpers.emit_debug(
                f"MeanReversion: {symbol} — no entry signal "
                f"(BB bandwidth={bb_bandwidth:.2f}% < min={min_bb_bandwidth:.2f}%)"
                if bb_bandwidth is not None else
                f"MeanReversion: {symbol} — no entry signal (BB bandwidth unavailable, min={min_bb_bandwidth:.2f}%)"
            )
            return None
        if max_bb_bandwidth > 0 and bb_bandwidth is not None and bb_bandwidth > max_bb_bandwidth:
            helpers.emit_debug(
                f"MeanReversion: {symbol} — no entry signal "
                f"(BB bandwidth={bb_bandwidth:.2f}% > max={max_bb_bandwidth:.2f}%)"
            )
            return None

        buy_signal = (
            rsi < rsi_oversold
            and (not require_cmf or (cmf is not None and cmf > 0))
            and (not require_htf_cmf or not htf_available or htf_cmf_bullish)
            and (not require_cmf_cross or cmf_crossed_up)
            and (not require_cmf_no_divergence or not bearish_div)
            and (not require_htf_trend or not htf_available or htf_bullish)
            and (not require_footprint_delta or not fp_data_available or (fp_net_delta is not None and fp_net_delta > 0))
            and (not require_bb_position or bb_long_ok)
            and (not require_candle_rejection or candle_rejection_long_ok)
            and (not require_vwap_reversion or vwap_long_ok)
            and volume_cooling_ok
            and regime_ok
            and atr_ok
            and (not require_price_in_va or va_ok)
            and (not require_no_extreme_funding or not funding_blocked_long)
            and imbalance_ok
        )
        sell_signal = (
            rsi > rsi_overbought
            and (not require_cmf or (cmf is not None and cmf < 0))
            and (not require_htf_cmf or not htf_available or htf_cmf_bearish)
            and (not require_cmf_cross or cmf_crossed_down)
            and (not require_cmf_no_divergence or not bullish_div)
            and (not require_htf_trend or not htf_available or htf_bearish)
            and (not require_footprint_delta or not fp_data_available or (fp_net_delta is not None and fp_net_delta < 0))
            and (not require_bb_position or bb_short_ok)
            and (not require_candle_rejection or candle_rejection_short_ok)
            and (not require_vwap_reversion or vwap_short_ok)
            and volume_cooling_ok
            and regime_ok
            and atr_ok
            and (not require_price_in_va or va_ok)
            and (not require_no_extreme_funding or not funding_blocked_short)
            and imbalance_ok
        )
        # ── Compute effective TP/SL ────────────────────────────────────
        # Structural mode: TP at BB middle band (the mean), SL beyond entry candle wick.
        # ATR mode (fallback): TP/SL = multiplier × ATR%.
        _static_tp = helpers.extract_float(config.get("tp_pct"))
        _static_sl = helpers.extract_float(config.get("sl_pct"))
        _effective_tp = _static_tp
        _effective_sl = _static_sl
        _sizing_source = "static"
        atr_pct = helpers.extract_float(indicators.get("atr_pct"))
        # Optional adaptive ATR regime – wider stops/targets in volatile regimes.
        if atr_pct is not None and config.get("use_adaptive_atr", False):
            if atr_pct < 1.5:
                atr_pct *= 1.20
            elif atr_pct < 3.0:
                atr_pct *= 1.80
            else:
                atr_pct *= 2.50
        if use_atr_sizing and atr_pct is not None and atr_pct > 0:
            _effective_tp = atr_tp_multiplier * atr_pct
            _effective_sl = atr_sl_multiplier * atr_pct
            _sizing_source = "atr"

        # Structural sizing: TP at BB middle, SL beyond entry candle wick.
        if use_structural_sizing and bb_last_price and bb_last_price > 0 and atr_pct is not None and atr_pct > 0:
            atr_price = (atr_pct / 100.0) * bb_last_price
            if atr_price > 0 and bb_middle and bb_middle > 0:
                ohlcv_compact = indicators.get("ohlcv") or []
                _curr = ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {}
                curr_low = helpers.extract_float(_curr.get("low"))
                curr_high = helpers.extract_float(_curr.get("high"))

                if buy_signal:
                    # Long: TP at BB middle (reversion to mean), SL beyond entry candle low.
                    raw_tp_dist = bb_middle - bb_last_price
                    raw_sl_dist = (bb_last_price - (curr_low - structural_sl_buffer_atr * atr_price)) if curr_low is not None else None
                else:
                    # Short: TP at BB middle (reversion to mean), SL beyond entry candle high.
                    raw_tp_dist = bb_last_price - bb_middle
                    raw_sl_dist = ((curr_high + structural_sl_buffer_atr * atr_price) - bb_last_price) if curr_high is not None else None

                # Convert distances to % of price for clamping.
                tp_pct_raw = (raw_tp_dist / bb_last_price * 100.0) if raw_tp_dist is not None and raw_tp_dist > 0 else None
                sl_pct_raw = (raw_sl_dist / bb_last_price * 100.0) if raw_sl_dist is not None and raw_sl_dist > 0 else None

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

        # ------------------------------------------------------------------
        # Unified TP/SL sizing via trade_management
        # ------------------------------------------------------------------

        from app.services.trade_management import OrderContext, compute_tp_sl_pct

        entry_price = bb_last_price or helpers.get_last_price(symbol)

        atr_tf_pct = helpers.extract_float(indicators.get("atr_pct")) or 1.0  # fallback to small >0
        atr_htf_pct = helpers.extract_float(indicators.get("atr_pct_htf")) or atr_tf_pct

        vpoc = helpers.extract_float(indicators.get("vpoc"))
        va_width = helpers.extract_float(indicators.get("value_area_width"))
        swing_high = helpers.extract_float(indicators.get("swing_high"))
        swing_low = helpers.extract_float(indicators.get("swing_low"))

        side = "long" if buy_signal else "short" if sell_signal else None

        if side is not None:
            static_tp = helpers.extract_float(config.get("tp_pct"))
            static_sl = helpers.extract_float(config.get("sl_pct"))

            # Thesis-specific structural levels: TP at BB middle (the mean
            # price reverts to), SL beyond the entry candle's wick (the
            # exhaustion extreme).
            tp_target: float | None = None
            sl_level: float | None = None
            if use_structural_sizing and entry_price and bb_middle and bb_middle > 0:
                atr_price = (atr_tf_pct / 100.0) * entry_price
                ohlcv_compact = indicators.get("ohlcv") or []
                _curr = ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {}
                curr_low = helpers.extract_float(_curr.get("low"))
                curr_high = helpers.extract_float(_curr.get("high"))
                if side == "long":
                    tp_target = bb_middle
                    if curr_low is not None:
                        sl_level = curr_low - structural_sl_buffer_atr * atr_price
                else:
                    tp_target = bb_middle
                    if curr_high is not None:
                        sl_level = curr_high + structural_sl_buffer_atr * atr_price

            tp_pct_final: float | None = None
            sl_pct_final: float | None = None
            if entry_price is not None:
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
                )

            return StrategySignal(
                direction="buy" if side == "long" else "sell",
                strategy_name=self.name,
                    tp_pct=tp_pct_final,
                    sl_pct=sl_pct_final,
                    rationale=(
                        f"MeanReversion {'BUY' if side=='long' else 'SELL'}: RSI={rsi:.1f}"
                        f" {'<' if side=='long' else '>'}{rsi_oversold if side=='long' else rsi_overbought} [trade_mgmt]"
                    ),
                )

        # Build a human-readable breakdown of which filters blocked the signal.
        rsi_str = f"RSI={rsi:.1f} (need <{rsi_oversold} or >{rsi_overbought})"
        parts = [rsi_str]
        if require_cmf:
            parts.append(f"CMF14={cmf:.3f}" if cmf is not None else "CMF14=n/a")
        if require_htf_cmf:
            if not htf_available:
                parts.append("HTF_CMF=skipped(no HTF data)")
            elif htf_cmf is not None:
                parts.append(f"HTF_CMF={htf_cmf:.3f}")
            else:
                parts.append("HTF_CMF=n/a")
        if require_cmf_cross:
            parts.append(f"CMF_cross={'up' if cmf_crossed_up else 'down' if cmf_crossed_down else 'none'}")
        if require_cmf_no_divergence:
            if bearish_div:
                parts.append("CMF_div=bearish")
            elif bullish_div:
                parts.append("CMF_div=bullish")
            else:
                parts.append("CMF_div=none")
        if require_htf_trend:
            if not htf_available:
                parts.append("HTF EMA=skipped(no HTF data)")
            elif htf_ema50 is not None and htf_ema200 is not None:
                parts.append(f"HTF EMA50={htf_ema50:.4g}/EMA200={htf_ema200:.4g} ({'bull' if htf_bullish else 'bear' if htf_bearish else 'flat'})")
            else:
                parts.append("HTF EMA=n/a")
        if require_footprint_delta:
            if not fp_data_available:
                parts.append("fp_delta=skipped(no data)")
            elif fp_net_delta is not None:
                parts.append(f"fp_delta={fp_net_delta:.2f}")
            else:
                parts.append("fp_delta=n/a")
        if require_bb_position:
            if bb_last_price is not None and bb_lower is not None and bb_upper is not None:
                parts.append(
                    f"BB lower={bb_lower:.4g}/upper={bb_upper:.4g}/price={bb_last_price:.4g} "
                    f"(long={'ok' if bb_long_ok else 'blocked'}, short={'ok' if bb_short_ok else 'blocked'})"
                )
            else:
                parts.append("BB=n/a")
        if min_bb_bandwidth > 0 or max_bb_bandwidth > 0:
            parts.append(
                f"BB_bw={bb_bandwidth:.2f}%" if bb_bandwidth is not None else "BB_bw=n/a"
            )
        if require_candle_rejection:
            parts.append(f"candle_rej={'ok' if (candle_rejection_long_ok or candle_rejection_short_ok) else 'blocked'}")
        if require_vwap_reversion:
            if vwap_value is not None and bb_last_price is not None:
                _vwap_dist = abs(bb_last_price - vwap_value) / vwap_value * 100.0 if vwap_value > 0 else 0.0
                parts.append(f"VWAP={vwap_value:.4g}/dist={_vwap_dist:.2f}% (long={'ok' if vwap_long_ok else 'blocked'}, short={'ok' if vwap_short_ok else 'blocked'})")
            else:
                parts.append("VWAP=n/a")
        if require_volume_cooling:
            parts.append(f"vol_rsi={volume_rsi_value:.1f}" if volume_rsi_value is not None else "vol_rsi=n/a")
        if require_price_in_va:
            if helpers.extract_float(indicators.get("value_area_high")) is not None:
                parts.append(
                    f"in_va={'ok' if va_ok else 'blocked'}"
                    f"(va_low={helpers.extract_float(indicators.get('value_area_low'))}, "
                    f"va_high={helpers.extract_float(indicators.get('value_area_high'))})"
                )
            else:
                parts.append("in_va=n/a")
        if require_no_extreme_funding:
            if _f_info.get("available"):
                parts.append(f"funding={_f_info['rate']:.5g} (blocked={'yes' if (funding_blocked_long or funding_blocked_short) else 'no'})")
            else:
                parts.append("funding=n/a")
        if require_balanced_book:
            parts.append(f"imbalance={imbalance:.3f}" if imbalance is not None else "imbalance=n/a")
        helpers.emit_debug(f"MeanReversion: {symbol} — no entry signal ({', '.join(parts)})")
        return None
