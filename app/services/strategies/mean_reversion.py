"""Mean Reversion Scalping strategy.

Enters when RSI is oversold/overbought and optional confirming filters
(CMF, HTF trend, ADX, BB position, footprint delta) agree.
"""

from __future__ import annotations

from typing import Any

from . import StrategyHelpers, StrategySignal


class MeanReversionStrategy:
    """Rule-based RSI mean-reversion strategy.

    Config keys (all live under ``config["strategies"]["mean_reversion"]``):
      - ``enabled`` (bool): master switch
      - ``rsi_oversold`` (float, default 35): BUY when RSI < this
      - ``rsi_overbought`` (float, default 65): SELL when RSI > this
      - ``require_htf_trend`` (bool, default True)
      - ``require_cmf`` (bool, default True)
      - ``require_htf_cmf`` (bool, default False)
      - ``require_cmf_cross`` (bool, default False)
      - ``require_cmf_no_divergence`` (bool, default False)
      - ``require_footprint_delta`` (bool, default False)
      - ``require_bb_position`` (bool, default False)
      - ``bb_proximity_pct`` (float, default 0.0)
      - ``min_bb_bandwidth`` (float, default 0.0)
      - ``max_bb_bandwidth`` (float, default 0.0)
      - ``min_adx`` (float, default 0.0)
      - ``max_adx`` (float, default 0.0)
      - ``require_candle_rejection`` (bool, default False): require upper wick
        for shorts, lower wick for longs (exhaustion confirmation)
      - ``candle_rejection_pct`` (float, default 0.3): minimum wick size as %
        of candle range (30 = wick is 30%+ of the candle)
      - ``require_vwap_reversion`` (bool, default False): require price extended
        from VWAP AND closing back toward it
      - ``vwap_min_distance_pct`` (float, default 1.0): minimum % distance from
        VWAP to qualify as "extended"
      - ``require_volume_cooling`` (bool, default False): require volume RSI
        below threshold (volume momentum fading)
      - ``volume_rsi_max`` (float, default 70.0): maximum volume RSI to allow entry
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

        rsi_oversold = helpers.extract_float(config.get("rsi_oversold")) or 35.0
        rsi_overbought = helpers.extract_float(config.get("rsi_overbought")) or 65.0
        require_htf_trend = bool(config.get("require_htf_trend", True))
        require_cmf = bool(config.get("require_cmf", True))
        require_htf_cmf = bool(config.get("require_htf_cmf", False))
        require_cmf_cross = bool(config.get("require_cmf_cross", False))
        require_cmf_no_divergence = bool(config.get("require_cmf_no_divergence", False))
        require_footprint_delta = bool(config.get("require_footprint_delta", False))
        require_bb_position = bool(config.get("require_bb_position", False))
        require_candle_rejection = bool(config.get("require_candle_rejection", False))
        candle_rejection_pct = helpers.extract_float(config.get("candle_rejection_pct")) or 0.3
        require_vwap_reversion = bool(config.get("require_vwap_reversion", False))
        vwap_min_distance_pct = helpers.extract_float(config.get("vwap_min_distance_pct")) or 1.0
        require_volume_cooling = bool(config.get("require_volume_cooling", False))
        volume_rsi_max = helpers.extract_float(config.get("volume_rsi_max")) or 70.0
        bb_proximity_pct = helpers.extract_float(config.get("bb_proximity_pct")) or 0.0
        min_bb_bandwidth = helpers.extract_float(config.get("min_bb_bandwidth")) or 0.0
        max_bb_bandwidth = helpers.extract_float(config.get("max_bb_bandwidth")) or 0.0
        min_adx = helpers.extract_float(config.get("min_adx")) or 0.0
        max_adx = helpers.extract_float(config.get("max_adx")) or 0.0

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

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
            and (not require_htf_cmf or htf_cmf_bullish)
            and (not require_cmf_cross or cmf_crossed_up)
            and (not require_cmf_no_divergence or not bearish_div)
            and (not require_htf_trend or htf_bullish)
            and (not require_footprint_delta or not fp_data_available or (fp_net_delta is not None and fp_net_delta > 0))
            and (not require_bb_position or bb_long_ok)
            and (not require_candle_rejection or candle_rejection_long_ok)
            and (not require_vwap_reversion or vwap_long_ok)
            and volume_cooling_ok
        )
        sell_signal = (
            rsi > rsi_overbought
            and (not require_cmf or (cmf is not None and cmf < 0))
            and (not require_htf_cmf or htf_cmf_bearish)
            and (not require_cmf_cross or cmf_crossed_down)
            and (not require_cmf_no_divergence or not bullish_div)
            and (not require_htf_trend or htf_bearish)
            and (not require_footprint_delta or not fp_data_available or (fp_net_delta is not None and fp_net_delta < 0))
            and (not require_bb_position or bb_short_ok)
            and (not require_candle_rejection or candle_rejection_short_ok)
            and (not require_vwap_reversion or vwap_short_ok)
            and volume_cooling_ok
        )
        if buy_signal:
            return StrategySignal(
                direction="buy",
                strategy_name=self.name,
                tp_pct=helpers.extract_float(config.get("tp_pct")),
                sl_pct=helpers.extract_float(config.get("sl_pct")),
                rationale=f"MeanReversion BUY: RSI={rsi:.1f}<{rsi_oversold}",
            )
        if sell_signal:
            return StrategySignal(
                direction="sell",
                strategy_name=self.name,
                tp_pct=helpers.extract_float(config.get("tp_pct")),
                sl_pct=helpers.extract_float(config.get("sl_pct")),
                rationale=f"MeanReversion SELL: RSI={rsi:.1f}>{rsi_overbought}",
            )

        # Build a human-readable breakdown of which filters blocked the signal.
        rsi_str = f"RSI={rsi:.1f} (need <{rsi_oversold} or >{rsi_overbought})"
        parts = [rsi_str]
        if require_cmf:
            parts.append(f"CMF14={cmf:.3f}" if cmf is not None else "CMF14=n/a")
        if require_htf_cmf:
            parts.append(f"HTF_CMF={htf_cmf:.3f}" if htf_cmf is not None else "HTF_CMF=n/a")
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
            if htf_ema50 is not None and htf_ema200 is not None:
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
        helpers.emit_debug(f"MeanReversion: {symbol} — no entry signal ({', '.join(parts)})")
        return None
