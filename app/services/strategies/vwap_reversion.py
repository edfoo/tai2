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

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile


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
      - ``use_atr_sizing`` (bool, default True): use ATR-scaled TP/SL.
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

        # ── Config ────────────────────────────────────────────────────
        vwap_min_distance_atr = helpers.extract_float(config.get("vwap_min_distance_atr"))
        if vwap_min_distance_atr is None:
            vwap_min_distance_atr = 2.0
        vwap_max_distance_atr = helpers.extract_float(config.get("vwap_max_distance_atr"))
        if vwap_max_distance_atr is None:
            vwap_max_distance_atr = 3.0
        max_adx = helpers.extract_float(config.get("max_adx"))
        if max_adx is None:
            max_adx = 25.0
        require_closeback = bool(config.get("require_closeback", True))
        require_htf_trend = bool(config.get("require_htf_trend", True))
        require_regime = bool(config.get("require_regime", True))
        max_bb_bandwidth_percentile = helpers.extract_float(
            config.get("max_bb_bandwidth_percentile")
        )
        if max_bb_bandwidth_percentile is None:
            max_bb_bandwidth_percentile = 55.0
        _regime_lookback = helpers.extract_float(config.get("regime_lookback"))
        regime_lookback = int(_regime_lookback) if _regime_lookback is not None else 50
        use_atr_sizing = bool(config.get("use_atr_sizing", True))
        atr_tp_multiplier = helpers.extract_float(config.get("atr_tp_multiplier"))
        if atr_tp_multiplier is None:
            atr_tp_multiplier = 1.8
        atr_sl_multiplier = helpers.extract_float(config.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 1.0
        min_atr_pct = helpers.extract_float(config.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0

        # ── Snapshot data ─────────────────────────────────────────────
        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = sym_data.get("indicators") or {}

        vwap_value = helpers.extract_float(indicators.get("vwap"))
        last_price = helpers.get_last_price(symbol)
        atr_pct = helpers.extract_float(indicators.get("atr_pct"))

        if vwap_value is None or vwap_value <= 0 or last_price is None:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal (VWAP or price unavailable)"
            )
            return None

        # ATR in price units = (atr_pct / 100) * last_price.
        # atr_pct is "ATR as % of price", so ATR_price = atr_pct/100 * price.
        if atr_pct is None or atr_pct <= 0:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal (ATR% unavailable or zero)"
            )
            return None
        atr_price = (atr_pct / 100.0) * last_price
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

        # ── ADX gate: block in strong trends ───────────────────────────
        # A high ADX means the VWAP deviation is a real directional move,
        # not noise that will revert.  Entering is catching a falling knife.
        adx = helpers.extract_float((indicators.get("adx") or {}).get("value"))
        if max_adx > 0 and adx is not None and adx > max_adx:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(ADX={adx:.1f} > max={max_adx:.1f} — strong trend, reversion unlikely)"
            )
            return None

        # ── Distance from VWAP in ATR units ───────────────────────────
        distance = last_price - vwap_value  # positive = above VWAP
        distance_atr = abs(distance) / atr_price
        if distance_atr < vwap_min_distance_atr:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(distance={distance_atr:.2f} ATR < min={vwap_min_distance_atr:.2f} ATR, "
                f"VWAP={vwap_value:.6g}, price={last_price:.6g})"
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
        closeback_long_ok = True
        closeback_short_ok = True
        if require_closeback:
            ohlcv_compact = indicators.get("ohlcv") or []
            if len(ohlcv_compact) < 2 or not isinstance(ohlcv_compact[-1], dict):
                helpers.emit_debug(
                    f"VWAPReversion: {symbol} — no signal "
                    f"(insufficient candles for closeback check)"
                )
                return None
            _prev_close = helpers.extract_float(ohlcv_compact[-2].get("close"))
            _curr_close = helpers.extract_float(ohlcv_compact[-1].get("close"))
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

        # ── Regime gate: BB bandwidth percentile ─────────────────────
        _bb = indicators.get("bollinger_bands") or {}
        bb_lower = helpers.extract_float(_bb.get("lower"))
        bb_upper = helpers.extract_float(_bb.get("upper"))
        bb_middle = helpers.extract_float(_bb.get("middle"))
        bb_bandwidth: float | None = (
            (bb_upper - bb_lower) / bb_middle * 100.0
            if bb_upper is not None and bb_lower is not None and bb_middle and bb_middle > 0
            else None
        )
        ohlcv_compact = indicators.get("ohlcv") or []
        bw_percentile = compute_bb_bandwidth_percentile(
            ohlcv_compact, bb_bandwidth, lookback=regime_lookback
        )
        regime_ok = (
            not require_regime
            or (bw_percentile is not None and bw_percentile <= max_bb_bandwidth_percentile)
        )
        if require_regime and not regime_ok:
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal "
                f"(regime: BW pct={bw_percentile:.0f} > max={max_bb_bandwidth_percentile:.0f})"
                if bw_percentile is not None else
                f"VWAPReversion: {symbol} — no signal (regime: BW percentile unavailable)"
            )
            return None

        # ── Direction decision ────────────────────────────────────────
        # Long: price extended below VWAP, closing back up.
        # Short: price extended above VWAP, closing back down.
        buy_signal = (
            distance < 0
            and closeback_long_ok
            and (not require_htf_trend or not htf_available or htf_bullish)
        )
        sell_signal = (
            distance > 0
            and closeback_short_ok
            and (not require_htf_trend or not htf_available or htf_bearish)
        )

        if not buy_signal and not sell_signal:
            parts = [
                f"dist={distance_atr:.2f} ATR (need >={vwap_min_distance_atr:.2f})",
                f"VWAP={vwap_value:.6g}/price={last_price:.6g}",
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
            helpers.emit_debug(
                f"VWAPReversion: {symbol} — no signal ({', '.join(parts)})"
            )
            return None

        # ── Compute effective TP/SL ───────────────────────────────────
        _static_tp = helpers.extract_float(config.get("tp_pct"))
        _static_sl = helpers.extract_float(config.get("sl_pct"))
        _effective_tp = _static_tp
        _effective_sl = _static_sl
        if use_atr_sizing and atr_pct is not None and atr_pct > 0:
            _effective_tp = atr_tp_multiplier * atr_pct
            _effective_sl = atr_sl_multiplier * atr_pct

        direction = "buy" if buy_signal else "sell"
        side = "below" if buy_signal else "above"
        return StrategySignal(
            direction=direction,
            strategy_name=self.name,
            tp_pct=_effective_tp,
            sl_pct=_effective_sl,
            rationale=(
                f"VWAPReversion {direction.upper()}: price {side} VWAP "
                f"(dist={distance_atr:.2f} ATR, VWAP={vwap_value:.6g}, "
                f"price={last_price:.6g})"
            ),
        )
