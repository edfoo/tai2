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

from . import StrategyHelpers, StrategySignal, compute_bb_bandwidth_percentile, resolve_analysis_block
from .defaults import merged_config
from .liquidity_helpers import oi_confirms_momentum, volume_participation_ok


class SpikeContinuationStrategy:
    """Momentum scalp strategy that rides volume-driven spikes.

    Config keys (all live under ``config["strategies"]["spike_continuation"]``):
      - ``enabled`` (bool): master switch
      - ``volume_rsi_min`` (float, default 72): volume RSI must be above this
        to confirm the spike is volume-driven (not just a thin wick)
      - ``rsi_min`` (float, default 55): RSI must be above this for buys
        (momentum confirmed but not yet extreme)
      - ``rsi_max`` (float, default 80): don't enter if RSI is already extreme
        (that's Mean Reversion territory / late top). Sell band mirrors to
        ``100 - rsi_max``.
      - ``require_bb_breakout`` (bool, default True): price must be beyond BB band
      - ``require_candle_strength`` (bool, default True): candle must close near
        its high (for buys) or low (for sells) — strong momentum, no rejection
      - ``candle_strength_pct`` (float, default 60): close must be within this
        % of the candle range from the direction (60 = close is in top 40% for buys)
      - ``min_bb_bandwidth`` (float, default 3.0): only enter when bands are wide
        enough to suggest a real volatility expansion
      - ``tp_pct`` (float, default 6.0): take-profit as % price move (static fallback)
      - ``sl_pct`` (float, default 4.0): stop-loss as % price move (static fallback)
      - ``max_adx`` (float, default 0): legacy hard ADX ceiling. 0 = disabled.
      - ``max_adx_for_entry`` (float, default 32): late-entry killer. Blocks when
        ADX is already this high (trend is mature). 0 = disabled.

    Momentum acceleration filters (prevent entering at the top of a spike):
      - ``require_momentum_acceleration`` (bool, default False): current candle
        body must be larger than the average body of the last N candles.  This
        is OPT-IN and default-off: it encodes the same "not too late" intent as
        the ATR-anchored extension gate, so it must not compound it.
      - ``acceleration_lookback`` (int, default 3): number of prior candles to
        average for the acceleration comparison
      - ``acceleration_min_ratio`` (float, default 1.3): current body must be
        at least this multiple of the average recent body
      - ``require_rsi_rising`` (bool, default True): RSI must be rising vs the
        previous candle (momentum still building, not fading). Uses the actual
        RSI series, not a candle-direction proxy.
      - ``require_volume_rsi_rising`` (bool, default False): volume RSI must be
        rising vs the previous candle (volume momentum still building).  OPT-IN
        and default-off; ``volume_rsi_min`` is the primary volume gate.
      - ``max_spike_extension_atr`` (float, default 2.0): volatility-normalised
        anti-late-entry gate.  Block entry if price has already travelled more
        than this multiple of ATR% from the volume-expansion origin.  The
        origin is anchored to the candle where volume expansion began (Fix 3),
        not a trailing window extreme.  0 = disabled.
      - ``spike_lookback`` (int, default 5): candles to look back to find the
        volume-expansion candle that anchors the spike origin
      - ``require_regime`` (bool, default True)
      - ``min_bb_bandwidth_percentile`` (float, default 50)
      - ``use_atr_sizing`` (bool, default True)
      - ``atr_tp_multiplier`` (float, default 3.0)
      - ``atr_sl_multiplier`` (float, default 2.0)
      - ``min_atr_pct`` (float, default 1.0)
      - ``flip_launcher_direction`` (str, default None): invert the
        Launcher's trade direction before execution. One of "both",
        "from_long" (only BUY→SELL), "from_short" (only SELL→BUY),
        or None to disable. TP/SL are mirrored around last_price so
        they land on the correct side for the flipped direction.

    ATR exit multipliers — SINGLE SOURCE OF TRUTH
    --------------------------------------------
    The canonical values live in ``DEFAULT_SPIKE_CONTINUATION``
    (``app/services/strategies/defaults.py``): ``atr_tp_multiplier = 3.0`` and
    ``atr_sl_multiplier = 2.0`` (≥ 1.5 R:R).  The inline fallbacks below and
    this docstring MUST stay in sync with that dict.  When ``use_atr_sizing``
    is disabled the static fallbacks (``tp_pct: 6.0``, ``sl_pct: 4.0``) keep
    the same 1.5 R:R floor.
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

        # ---- HTF regime gate (requires trending by default) ----------------
        # Configurable per-strategy: "trend" (block when HTF not trending —
        # the legacy spike-continuation behaviour), "chop", or "off".
        # Neutral (no HTF data) never blocks.
        from app.services.indicator_service import htf_regime_allows

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

        adx_htf = helpers.extract_float(indicators.get("adx_htf"))
        chop_htf = helpers.extract_float(indicators.get("choppiness_htf"))
        htf_pref = cfg.get("htf_regime_preference", "trend")

        # Spike continuation wants *trending* conditions. Only block on a
        # definitive non-trending signal; neutral (no HTF data) passes.
        if not htf_regime_allows(adx_htf, chop_htf, htf_pref):
            return None

        volume_rsi_min = helpers.extract_float(cfg.get("volume_rsi_min"))
        if volume_rsi_min is None:
            volume_rsi_min = 72.0
        rsi_min = helpers.extract_float(cfg.get("rsi_min"))
        if rsi_min is None:
            rsi_min = 55.0
        rsi_max = helpers.extract_float(cfg.get("rsi_max"))
        if rsi_max is None:
            rsi_max = 80.0
        require_bb_breakout = bool(cfg.get("require_bb_breakout", True))
        require_candle_strength = bool(cfg.get("require_candle_strength", True))
        candle_strength_pct = helpers.extract_float(cfg.get("candle_strength_pct"))
        if candle_strength_pct is None:
            candle_strength_pct = 60.0
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
        # (e.g. > 50th percentile = volatility expansion).
        require_regime = bool(cfg.get("require_regime", True))
        min_bb_bandwidth_percentile = helpers.extract_float(cfg.get("min_bb_bandwidth_percentile"))
        if min_bb_bandwidth_percentile is None:
            min_bb_bandwidth_percentile = 50.0
        regime_lookback = helpers.extract_float(cfg.get("regime_lookback"))
        if regime_lookback is None:
            regime_lookback = 50
        # ── ATR-scaled TP/SL ────────────────────────────────────────────
        # When use_atr_sizing is True, TP/SL are computed as
        # multiplier × ATR% instead of fixed percentages.
        # SC uses a wide SL (2.0 ATR) to avoid being stopped by noise and a
        # 3.0 ATR TP to keep ≥ 1.5 R:R — matching DEFAULT_SPIKE_CONTINUATION.
        use_atr_sizing = bool(cfg.get("use_atr_sizing", True))
        atr_tp_multiplier = helpers.extract_float(cfg.get("atr_tp_multiplier"))
        if atr_tp_multiplier is None:
            atr_tp_multiplier = 3.0
        atr_sl_multiplier = helpers.extract_float(cfg.get("atr_sl_multiplier"))
        if atr_sl_multiplier is None:
            atr_sl_multiplier = 2.0
        # ── Minimum ATR% filter ───────────────────────────────────────
        # Skip entries on coins with ATR% below this threshold — too quiet
        # for a real spike.  0 = disabled.
        min_atr_pct = helpers.extract_float(cfg.get("min_atr_pct"))
        if min_atr_pct is None:
            min_atr_pct = 1.0

        # Momentum acceleration filters (opt-in, default OFF).  The ATR
        # extension gate below is the primary anti-late-entry filter; this
        # body-ratio check must not compound it.
        require_momentum_acceleration = bool(cfg.get("require_momentum_acceleration", False))
        _acceleration_lookback = helpers.extract_float(cfg.get("acceleration_lookback"))
        acceleration_lookback = int(_acceleration_lookback) if _acceleration_lookback is not None else 3
        acceleration_min_ratio = helpers.extract_float(cfg.get("acceleration_min_ratio"))
        if acceleration_min_ratio is None:
            acceleration_min_ratio = 1.3
        require_rsi_rising = bool(cfg.get("require_rsi_rising", True))
        require_volume_rsi_rising = bool(cfg.get("require_volume_rsi_rising", False))
        # Volatility-normalised spike extension (ATR-anchored, 0 = disabled).
        max_spike_extension_atr = helpers.extract_float(cfg.get("max_spike_extension_atr"))
        if max_spike_extension_atr is None:
            max_spike_extension_atr = 2.0
        _spike_lookback = helpers.extract_float(cfg.get("spike_lookback"))
        spike_lookback = int(_spike_lookback) if _spike_lookback is not None else 5
        # ── Liquidity-aware gates (§3) ────────────────────────────────
        # ``require_oi_confirmation`` (default off): momentum entries need
        # fresh leverage — rising open interest (oi_zscore > 1 in direction).
        # Degrades to pass when OI data is unavailable.
        require_oi_confirmation = bool(cfg.get("require_oi_confirmation", False))
        oi_min_zscore = helpers.extract_float(cfg.get("oi_min_zscore"))
        if oi_min_zscore is None:
            oi_min_zscore = 1.0
        # ── Volume-participation gate ────────────────────────────────
        # Block a spike entry on a dead-volume candle (no participation behind
        # the continuation).  SC already gates on volume RSI; this adds a floor
        # against the raw volume collapse.
        require_min_volume = bool(cfg.get("require_min_volume", False))
        min_volume_ratio = helpers.extract_float(cfg.get("min_volume_ratio"))
        if min_volume_ratio is None:
            min_volume_ratio = 0.7
        _vol_lookback = helpers.extract_float(cfg.get("volume_lookback"))
        volume_lookback = int(_vol_lookback) if _vol_lookback is not None else 20

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        indicators = resolve_analysis_block(sym_data, cfg)

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

        # Spike extension — don't enter if price has already travelled too far
        # from the volume-expansion origin.  The origin is anchored to the
        # candle where volume expansion began (first candle in the lookback
        # whose volume-RSI exceeded volume_rsi_min), NOT a trailing window
        # extreme.  Extension is measured in ATR multiples (volatility
        # normalised) so a fresh accelerating impulse reads as low extension
        # while a move already > max_spike_extension_atr × ATR% is blocked.
        spike_extension_buy: float | None = None
        spike_extension_sell: float | None = None
        spike_extension_ok_buy = True
        spike_extension_ok_sell = True
        spike_origin_buy_ts: int | None = None
        spike_origin_sell_ts: int | None = None
        atr_pct_value = helpers.extract_float(indicators.get("atr_pct"))
        if max_spike_extension_atr > 0 and current_close is not None and len(ohlcv_compact) >= 2:
            lookback_candles = ohlcv_compact[-(spike_lookback + 1):-1]
            # volume_rsi_series is aligned 1:1 with ohlcv_compact (both derived
            # from the same df), so the first lookback candle sits at index
            # len(ohlcv_compact) - (spike_lookback + 1) in the series (clamped
            # to 0 to match Python slice clamping on a short window).
            _series_offset = max(0, len(ohlcv_compact) - (spike_lookback + 1))
            # Find the first candle in the window whose volume-RSI exceeded
            # volume_rsi_min — the candle that started the spike.
            origin_low: float | None = None
            origin_high: float | None = None
            origin_low_ts: int | None = None
            origin_high_ts: int | None = None
            for i, c in enumerate(lookback_candles):
                if not isinstance(c, dict):
                    continue
                _vrsi = None
                _series_idx = _series_offset + i
                if volume_rsi_series and 0 <= _series_idx < len(volume_rsi_series):
                    _vrsi = helpers.extract_float(volume_rsi_series[_series_idx])
                if _vrsi is None or _vrsi < volume_rsi_min:
                    continue
                # This candle qualifies as the volume-expansion start.
                if origin_low is None:
                    _lo = helpers.extract_float(c.get("low"))
                    if _lo is not None:
                        origin_low = _lo
                        origin_low_ts = helpers.extract_float(c.get("ts"))
                if origin_high is None:
                    _hi = helpers.extract_float(c.get("high"))
                    if _hi is not None:
                        origin_high = _hi
                        origin_high_ts = helpers.extract_float(c.get("ts"))
                if origin_low is not None and origin_high is not None:
                    break

            # ATR distance in price units (ATR% is a percentage of price).
            _atr_price = (atr_pct_value / 100.0) if atr_pct_value and atr_pct_value > 0 else None

            if origin_low is not None and origin_low > 0:
                spike_origin_buy_ts = int(origin_low_ts) if origin_low_ts is not None else None
                if _atr_price is not None:
                    spike_extension_buy = (current_close - origin_low) / (origin_low * _atr_price)
                    spike_extension_ok_buy = spike_extension_buy <= max_spike_extension_atr
                else:
                    # No ATR% available → cannot normalise → treat as not confirmed.
                    spike_extension_ok_buy = False
            if origin_high is not None and origin_high > 0:
                spike_origin_sell_ts = int(origin_high_ts) if origin_high_ts is not None else None
                if _atr_price is not None:
                    spike_extension_sell = (origin_high - current_close) / (origin_high * _atr_price)
                    spike_extension_ok_sell = spike_extension_sell <= max_spike_extension_atr
                else:
                    spike_extension_ok_sell = False

        # BB breakout checks
        bb_breakout_buy = (
            not require_bb_breakout
            or (bb_last_price is not None and bb_upper is not None and bb_last_price >= bb_upper)
        )
        bb_breakout_sell = (
            not require_bb_breakout
            or (bb_last_price is not None and bb_lower is not None and bb_last_price <= bb_lower)
        )

        # ── Open-interest momentum confirmation (§3) ──────────────────
        # Momentum entries need fresh leverage: OI should be rising (buy) or
        # falling (short) beyond the z-score threshold.  Granting when data is
        # absent (oi_ok True) keeps the gate from blocking on no-OI symbols.
        # The direction-specific z-score sign is applied inside the helper, so
        # we evaluate it SEPARATELY for each direction (Fix 2) — a short must
        # see falling OI (oi_zscore < -min_zscore), never the long confirmation.
        oi_ok_buy = True
        oi_ok_sell = True
        oi_info_buy: dict = {}
        oi_info_sell: dict = {}
        if require_oi_confirmation:
            open_interest = sym_data.get("open_interest") or {}
            # Phase 0d: use the pre-computed OI delta z-score when available;
            # falls back to the flat-delta proxy when history is not yet seeded.
            _oi_zscore: float | None = sym_data.get("oi_zscore")
            oi_ok_buy, oi_info_buy = oi_confirms_momentum(
                open_interest,
                direction="long",
                oi_zscore=_oi_zscore,
                min_zscore=oi_min_zscore,
            )
            oi_ok_sell, oi_info_sell = oi_confirms_momentum(
                open_interest,
                direction="short",
                oi_zscore=_oi_zscore,
                min_zscore=oi_min_zscore,
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
        if max_spike_extension_atr > 0:
            if not spike_extension_ok_buy and not spike_extension_ok_sell:
                _ext = spike_extension_buy if spike_extension_buy is not None else spike_extension_sell
                helpers.emit_debug(
                    f"SpikeContinuation: {symbol} — no signal "
                    f"(spike already extended: {_ext:.2f} ATR > max={max_spike_extension_atr:.2f} ATR — "
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
        atr_ok = (
            min_atr_pct <= 0
            or (atr_pct_value is not None and atr_pct_value >= min_atr_pct)
        )

        # ── Volume-participation gate ────────────────────────────────
        volume_ok = True
        if require_min_volume:
            volume_ok, _ = volume_participation_ok(
                indicators, min_ratio=min_volume_ratio, lookback=volume_lookback
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
            and (not require_oi_confirmation or oi_ok_buy)
            and volume_ok
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
            and (not require_oi_confirmation or oi_ok_sell)
            and volume_ok
        )

        # ── Unified TP/SL via trade_management ─────────────────────────

        from app.services.trade_management import OrderContext, compute_tp_sl_pct

        side: str | None = "long" if buy_signal else "short" if sell_signal else None
        if side is not None:
            entry_price = helpers.get_last_price(symbol)
            if entry_price is not None:
                static_tp = helpers.extract_float(config.get("tp_pct"))
                static_sl = helpers.extract_float(config.get("sl_pct"))

                # Read sizing inputs from the same ``indicators`` block the
                # entry gates used (via ``resolve_analysis_block``) so a
                # non-default ``analysis_timeframe`` does not silently mix ATR
                # from two different bars.
                tp_pct_val, sl_pct_val = compute_tp_sl_pct(
                    entry=entry_price,
                    side=side,
                    ctx=OrderContext(
                        atr_tf_pct=helpers.extract_float(indicators.get("atr_pct")) or 1.0,
                        atr_htf_pct=helpers.extract_float(indicators.get("atr_pct_htf")) or 1.0,
                        vpoc=helpers.extract_float(indicators.get("vpoc")),
                        value_area_width=helpers.extract_float(indicators.get("value_area_width")),
                        swing_high=helpers.extract_float(indicators.get("swing_high")),
                        swing_low=helpers.extract_float(indicators.get("swing_low")),
                        last_price=entry_price,
                    ),
                    static_tp_pct=static_tp,
                    static_sl_pct=static_sl,
                    atr_tp_multiplier=atr_tp_multiplier if use_atr_sizing else None,
                    atr_sl_multiplier=atr_sl_multiplier if use_atr_sizing else None,
                )

                return StrategySignal(
                    direction="buy" if side == "long" else "sell",
                    strategy_name=self.name,
                    tp_pct=tp_pct_val,
                    sl_pct=sl_pct_val,
                    rationale="SpikeContinuation: volume/momentum spike [trade_mgmt]",
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
        if max_spike_extension_atr > 0:
            _ext = spike_extension_buy if spike_extension_buy is not None else spike_extension_sell
            parts.append(f"spike_ext={_ext:.2f}ATR" if _ext is not None else "spike_ext=n/a")
            if spike_origin_buy_ts is not None:
                parts.append(f"origin_buy_ts={spike_origin_buy_ts}")
            if spike_origin_sell_ts is not None:
                parts.append(f"origin_sell_ts={spike_origin_sell_ts}")
        if min_bb_bandwidth > 0:
            parts.append(f"BB_bw={bb_bandwidth:.2f}%" if bb_bandwidth is not None else "BB_bw=n/a")
        if max_adx > 0 or max_adx_for_entry > 0:
            parts.append(f"ADX={adx:.1f}" if adx is not None else "ADX=n/a")
            if max_adx_for_entry > 0:
                parts.append(f"max_adx_entry={max_adx_for_entry:.0f}")
        if require_oi_confirmation:
            if oi_info_buy.get("available") or oi_info_sell.get("available"):
                _oi = oi_info_buy.get("zscore") or oi_info_sell.get("zscore")
                parts.append(
                    f"oi_z={_oi:.2f} (buy={'true' if oi_ok_buy else 'blocked'}, "
                    f"sell={'true' if oi_ok_sell else 'blocked'})"
                    if _oi is not None
                    else f"oi_delta(buy={'true' if oi_ok_buy else 'blocked'}, "
                    f"sell={'true' if oi_ok_sell else 'blocked'})"
                )
            else:
                parts.append("oi=skipped(no data)")
        if require_min_volume:
            parts.append(f"vol_participation={'ok' if volume_ok else 'blocked'}")
        helpers.emit_debug(f"SpikeContinuation: {symbol} — no signal ({', '.join(parts)})")
        return None
