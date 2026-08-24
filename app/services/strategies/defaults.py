"""Canonical default configuration for all Launcher strategies.

This module is the single source of truth for acceptable default values for
every pluggable strategy.  Each strategy's ``evaluate()`` merges the caller's
config over these defaults, so any missing key falls back to a sane, validated
value instead of a hardcoded inline default scattered across the strategy code.

The values here mirror the "Set Recommended Defaults" buttons on the STRATEGY
page (``app/ui/pages.py``), which were validated against live/backtest
behaviour (see ``.github/skills/strategy-tuning-playbook`` and
``.github/skills/launcher-strategy-reference``).

Config lives under ``config["launcher"]["strategies"][<strategy_name>]``.
"""

from __future__ import annotations

from typing import Any

# ── Mean Reversion ──────────────────────────────────────────────────────────
DEFAULT_MEAN_REVERSION: dict[str, Any] = {
    "enabled": False,
    "tp_pct": 2.0,
    "sl_pct": 3.0,
    "rsi_oversold": 30.0,
    "rsi_overbought": 70.0,
    "min_adx": 0.0,
    "max_adx": 28.0,
    "require_htf_trend": True,
    # Per-strategy analysis timeframe. None = use the global LTF. Set to e.g.
    # "15m" to analyze on that bar; the snapshot exposes a ``timeframes[<tf>]``
    # map for the distinct set of timeframes requested by enabled strategies.
    # MR fades 15m overextension → analysis on 15m.
    "analysis_timeframe": "15m",
    # HTF regime gate preference: "chop" (block when HTF trending),
    # "trend" (block when HTF not trending), or "off" (disable gate).
    # MR wants a ranging HTF → default "chop" (preserves legacy behaviour).
    "htf_regime_preference": "chop",
    "require_cmf": False,
    "require_htf_cmf": False,
    "require_cmf_cross": False,
    "require_cmf_no_divergence": False,
    "require_footprint_delta": False,
    "require_bb_position": True,
    "bb_proximity_pct": 0.5,
    "min_bb_bandwidth": 2.0,
    "max_bb_bandwidth": 0.0,
    "require_candle_rejection": True,
    "candle_rejection_pct": 30.0,
    "require_vwap_reversion": False,
    "vwap_min_distance_pct": 1.0,
    "require_volume_cooling": False,
    "volume_rsi_max": 80.0,
    # Volume-participation gate (default OFF): block entries on a candle whose
    # volume has collapsed below ``min_volume_ratio`` × the recent average —
    # the "price action halted" failure mode.  Neutral (pass) when volume data
    # is absent.
    "require_min_volume": False,
    "min_volume_ratio": 0.7,
    "volume_lookback": 20,
    "require_regime": True,
    "max_bb_bandwidth_percentile": 55.0,
    "regime_lookback": 50,
    "use_atr_sizing": True,
    "use_structural_sizing": True,
    "structural_sl_buffer_atr": 1.0,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    "atr_min_sl_mult": 0.3,
    "atr_max_sl_mult": 3.0,
    "atr_tp_multiplier": 1.8,
    "atr_sl_multiplier": 1.0,
    "min_atr_pct": 1.0,
    "min_reward_risk_ratio": 1.0,
    # Flatten an open MR position when the HTF regime flips from chop to
    # trend while the position is underwater (the reversion thesis is
    # invalidated).  Opt-in (default False) to preserve live behaviour until
    # tuned.
    "exit_on_regime_breakdown": False,
    "flip_launcher_direction": None,
    # Liquidity-aware gates (§3).  ``require_price_in_va`` (default ON): only enter
    # when price is inside the 70% value area — mean reversion wants price fading
    # toward the mean, not trending out of value.  Prevents catching falling
    # knives that have already broken below value on thin alt books.
    "require_price_in_va": True,
    "require_no_extreme_funding": False,
    "funding_max_abs_rate": 0.001,
    "require_balanced_book": False,
    "imbalance_min": 0.6,
    "imbalance_max": 1.4,
    "dynamic_tp": False,
    "dynamic_tp_fraction": 0.7,
}

# ── Spike Continuation ──────────────────────────────────────────────────────
# Single source of truth for the ATR exit multipliers.  The class docstring
# and the inline fallbacks in ``spike_continuation.py`` MUST reference these
# values (3.0 TP / 2.0 SL → ≥ 1.5 R:R) so they cannot drift again.
DEFAULT_SPIKE_CONTINUATION: dict[str, Any] = {
    "enabled": False,
    "tp_pct": 6.0,
    "sl_pct": 4.0,
    "volume_rsi_min": 72.0,
    "rsi_min": 55.0,
    "rsi_max": 80.0,
    "require_bb_breakout": True,
    "require_candle_strength": True,
    "candle_strength_pct": 60.0,
    "min_bb_bandwidth": 3.0,
    "max_adx": 0.0,
    "max_adx_for_entry": 32.0,
    # Anti-late-entry filter is the ATR-anchored extension gate below.
    # The body-ratio acceleration check is opt-in and default-off so it
    # cannot compound the extension gate and collapse the entry window.
    "require_momentum_acceleration": False,
    "acceleration_lookback": 3,
    "acceleration_min_ratio": 1.3,
    "require_rsi_rising": True,
    "require_volume_rsi_rising": False,
    # Volatility-normalised spike extension: price may be at most
    # max_spike_extension_atr × ATR% from the volume-expansion origin.
    "max_spike_extension_atr": 2.0,
    "spike_lookback": 5,
    "require_regime": True,
    "min_bb_bandwidth_percentile": 50.0,
    "regime_lookback": 50,
    "use_atr_sizing": True,
    "atr_tp_multiplier": 3.0,
    "atr_sl_multiplier": 2.0,
    "min_atr_pct": 1.0,
    "min_reward_risk_ratio": 1.5,
    "flip_launcher_direction": None,
    # Per-strategy analysis timeframe. SC rides 15m impulses → analysis on 15m.
    "analysis_timeframe": "15m",
    # HTF regime gate preference: SC wants a trending HTF → default "trend".
    "htf_regime_preference": "trend",
    # Liquidity-aware gates (§3) — OFF by default (opt-in).
    "require_oi_confirmation": False,
    "oi_min_zscore": 1.0,
    # Volume-participation gate (default OFF): SC already gates on volume RSI,
    # but this additionally blocks a spike entry on a dead-volume candle.
    "require_min_volume": False,
    "min_volume_ratio": 0.7,
    "volume_lookback": 20,
}

# ── Liquidity Sweep ─────────────────────────────────────────────────────────
DEFAULT_LIQUIDITY_SWEEP: dict[str, Any] = {
    "enabled": False,
    "tp_pct": 3.0,
    "sl_pct": 2.0,
    "lookback": 20,
    "sweep_buffer_pct": 0.1,
    # Penetration mode: "atr" (volatility-scaled) or "pct" (legacy flat %).
    # Volatile alt-coin 15m wicks routinely breach a level by >0.1%, so the
    # legacy % buffer is nearly a no-op at default; ATR-scaled penetration is
    # the correct discriminator.  ``sweep_buffer_pct`` still applies in "pct"
    # mode (and is kept as the backtest-comparison baseline).
    "sweep_penetration_mode": "atr",
    "sweep_buffer_atr": 0.25,
    # Reclaim: the close must genuinely reclaim the swept level (by this %
    # margin, symmetrised with sweep_buffer_pct) — not merely sit high in its
    # own candle body while remaining below/above the level (breakdown).
    "reclaim_buffer_pct": 0.1,
    "reclaim_ratio": 0.5,
    # Fractal pivot swing: ``swing_low/high`` come from real local pivots
    # (candle i is a pivot when its low/high is the min/max of [i-n, i+n])
    # rather than a trailing 20-bar min/max.  Falls back to min/max when too
    # few pivots are available.
    "pivot_bars": 3,
    "require_htf_trend": True,
    "require_volume_spike": True,
    "volume_spike_ratio": 1.5,
    "volume_lookback": 10,
    "max_adx": 28.0,
    "require_regime": True,
    "max_bb_bandwidth_percentile": 60.0,
    "regime_lookback": 50,
    "use_atr_sizing": True,
    "use_structural_sizing": True,
    "structural_sl_buffer_atr": 0.15,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    "atr_min_sl_mult": 0.3,
    "atr_max_sl_mult": 3.0,
    "atr_tp_multiplier": 1.2,
    "atr_sl_multiplier": 1.0,
    "min_atr_pct": 0.8,
    "min_reward_risk_ratio": 1.0,
    "flip_launcher_direction": None,
    # Per-strategy analysis timeframe. Sweeps are a 15m microstructure pattern.
    "analysis_timeframe": "15m",
    # HTF regime gate preference: sweep wants a ranging HTF → default "chop".
    "htf_regime_preference": "chop",
    # Liquidity-aware gates (§3).  The stop-hunt thesis only holds when the wick
    # closes back *inside* value (absorbed) rather than breaking through it.
    # Enabled by default: a bare "wick pierces swing + close reclaims" on 15m
    # alts is far more often a real breakdown than a stop-run (27% live win rate
    # on the ungated version).  A close outside value is treated as a breakout.
    "require_close_in_va": True,
    "require_macro_sl": False,
    "macro_sl_lookback": 50,
    # Order-book imbalance gate (§3) — OFF by default (opt-in).  A fade into a
    # stop-run wants the book supportive after the reclaim: bid-heavy for a
    # long sweep, ask-heavy for a short sweep.
    "require_book_imbalance": False,
    "imbalance_min_for_long": 1.0,
    "imbalance_max_for_short": 1.0,
}

# ── VWAP Reversion ──────────────────────────────────────────────────────────
DEFAULT_VWAP_REVERSION: dict[str, Any] = {
    "enabled": False,
    "tp_pct": 2.0,
    "sl_pct": 3.0,
    # Minimum extension from VWAP (in ATR). Raised to 2.5 so the TP-hop back
    # to VWAP is meaningful relative to the structural SL → better structural
    # R:R and fewer guardrail blocks (F3).
    "vwap_min_distance_atr": 2.5,
    # Loosened 3.0 → 3.25: VWAP reversion was the best per-trade performer
    # (75% win rate) and under-firing; widening the cap captures more edge
    # without entering knife-catch territory (ADX gate still guards trends).
    "vwap_max_distance_atr": 3.25,
    "max_adx": 25.0,
    "require_closeback": True,
    "require_htf_trend": True,
    "require_regime": True,
    "max_bb_bandwidth_percentile": 55.0,
    "regime_lookback": 50,
    # F4: ONE primary "not-trending" filter. "bb" makes the LTF BB-bandwidth
    # percentile the blocking chop gate (reads the analysis TF); ADX then
    # reports soft/secondary only. "adx" swaps them.
    "regime_primary_gate": "bb",
    "use_atr_sizing": True,
    "use_structural_sizing": True,
    "structural_sl_buffer_atr": 0.15,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    # SL floor raised from 0.3 so the stop survives an ordinary 15m wick
    # instead of being clamped into a wick-able gap (F3).
    "atr_min_sl_mult": 0.5,
    "atr_max_sl_mult": 3.0,
    "atr_tp_multiplier": 1.0,
    "atr_sl_multiplier": 1.0,
    "min_atr_pct": 1.0,
    "min_reward_risk_ratio": 1.0,
    "flip_launcher_direction": None,
    # Per-strategy analysis timeframe. VWAP reversion on 15m deviations.
    "analysis_timeframe": "15m",
    # HTF regime gate preference: VWAP reversion wants a ranging HTF → "chop".
    "htf_regime_preference": "chop",
    # Liquidity-aware gates (§3) — OFF by default (opt-in).
    "require_no_funding_bias": False,
    "funding_max_abs_rate": 0.0007,
    # Volume-participation gate (default OFF): block a VWAP reversion on a
    # dead-volume candle where the snap-back has no participation behind it.
    "require_min_volume": False,
    "min_volume_ratio": 0.7,
    "volume_lookback": 20,
}

# ── Trend Pullback ──────────────────────────────────────────────────────────
# Single source of truth for the ATR exit multipliers and the R:R floor.  The
# class docstring and the inline fallbacks in ``trend_pullback.py`` MUST
# reference these values (1.5 TP / 1.0 SL → ≥ 1.5 R:R) so they cannot drift.
#
# The multipliers were rebalanced to 2.25/1.5 (R:R unchanged at 1.5).  The SL
# was widened from 1.0→1.5×ATR because a 1.0×ATR stop sat inside single-candle
# alt-coin noise (wick-bait); the TP was widened 1.5→2.25 proportionally to
# hold the 1.5 R:R floor.  Both distances grow, keeping the ratio unchanged.
DEFAULT_TREND_PULLBACK: dict[str, Any] = {
    "enabled": False,
    # Static fallback floor: 6.0 / 4.0 → 1.5 R:R (matches the ATR floor).
    "tp_pct": 6.0,
    "sl_pct": 4.0,
    "pullback_ema": 21,
    # Fixed % proximity floor (never collapses to zero on dead coins).
    "pullback_proximity_pct": 0.3,
    # Volatility-normalised proximity: effective band = max(floor, atr × ATR%).
    "pullback_proximity_atr": 0.5,
    "use_vwap_as_level": True,
    "require_htf_trend": True,
    "require_bullish_candle": True,
    "candle_rejection_pct": 25.0,
    # ADX band widened (ADX is lagging on volatile alts); the primary
    # anti-late-entry filter is the ATR-anchored extension gate below.
    # Tightened from 40 → 30: 40 admitted late/mature-trend entries (the
    # documented failure mode — entering as the move exhausts).
    "min_adx": 18.0,
    "max_adx_for_entry": 30.0,
    # Volatility-normalised extension gate: price must not be more than this
    # × ATR% past the pullback level (blocks late entries). 0 = disabled.
    "max_pullback_extension_atr": 2.0,
    "use_atr_sizing": True,
    "use_structural_sizing": True,
    "structural_sl_buffer_atr": 0.15,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    "atr_min_sl_mult": 0.3,
    "atr_max_sl_mult": 3.0,
    # Unified exit model: 2.25 / 1.5 → 1.5 R:R when ATR sizing active.
    # SL widened 1.0→1.5 so the stop sits outside single-candle alt-coin noise
    # (the 1.0×ATR stop was wick-bait); TP widened 1.5→2.25 to preserve the
    # 1.5 R:R floor.  Both distances grow together, keeping the ratio unchanged.
    "atr_tp_multiplier": 2.25,
    "atr_sl_multiplier": 1.5,
    # Adaptive ATR applies to SIZING only (never the min_atr_pct gate).
    "use_adaptive_atr": False,
    "min_atr_pct": 1.0,
    # R:R floor raised to 1.5 so structurally sub-1.5 exits are rejected, not
    # silently degraded.
    "min_reward_risk_ratio": 1.5,
    "flip_launcher_direction": None,
    # Per-strategy analysis timeframe. Trend pullbacks execute at 15m, so the
    # pullback level, candle confirmation, and ATR% are all 15m values (Fix 3).
    "analysis_timeframe": "15m",
    # HTF regime gate preference: trend pullback wants a trending HTF → "trend".
    "htf_regime_preference": "trend",
    # Liquidity-aware gates (§3).  ``require_poc_proximity`` (default ON): the
    # pullback must occur at a POC / value-area node — adds a liquidity-confluence
    # confirmation on top of the 21-EMA/VWAP touch, filtering pullbacks that are
    # merely noise on thin books (the dominant trend_pullback drag in live logs).
    "require_poc_proximity": True,
    "poc_proximity_va_width": 0.2,
    # Volume-participation gate (default OFF): block a pullback entry on a
    # candle whose volume has collapsed below ``min_volume_ratio`` × the recent
    # average.  This is the "price action halted" failure mode that tight
    # 1–1.5×ATR stops are especially vulnerable to on thin alt books.
    "require_min_volume": False,
    "min_volume_ratio": 0.7,
    "volume_lookback": 20,
}

# Registry keyed by strategy name.
DEFAULT_STRATEGY_CONFIG: dict[str, dict[str, Any]] = {
    "mean_reversion": DEFAULT_MEAN_REVERSION,
    "spike_continuation": DEFAULT_SPIKE_CONTINUATION,
    "liquidity_sweep": DEFAULT_LIQUIDITY_SWEEP,
    "vwap_reversion": DEFAULT_VWAP_REVERSION,
    "trend_pullback": DEFAULT_TREND_PULLBACK,
}

# ── Trade management (position-overlay) ─────────────────────────────────────
# Canonical defaults for the trade-management sub-config.  This is NOT one of
# the pluggable launcher strategies — it lives under
# ``config["strategy"]["trade_management"]`` — but its "Set Recommended
# Defaults" button in ``pages.py`` reads from here so the same drift-proof
# single source of truth applies.
DEFAULT_TRADE_MANAGEMENT: dict[str, Any] = {
    "enabled": True,
    "breakeven_enabled": True,
    "breakeven_at_r": 0.7,
    "breakeven_buffer_pct": 0.05,
    "partial_tp_enabled": True,
    "partial_tp_at_r": 0.8,
    "partial_tp_fraction": 0.5,
    "time_stop_enabled": True,
    "time_stop_seconds": 1800.0,
    "time_stop_candles": 5,
    "time_stop_min_r": 0.3,
    "time_stop_underwater_only": True,
    "reentry_cooldown_seconds": 1800.0,
    "trailing_enabled": True,
    # Lowered 1.0→0.8 so the runner is protected the moment the partial TP
    # fires (partial_tp_at_r=0.8), instead of drifting from 0.8R→1.0R with no
    # stop and giving its profit back to the time-stop.
    "trailing_activate_r": 0.8,
    "trailing_distance_atr": 1.5,
    "trailing_floor_r": 0.5,
    "trailing_step_r": 0.2,
    "software_stop_loss_enabled": True,
}


def strategy_defaults(name: str) -> dict[str, Any]:
    """Return a shallow copy of the canonical defaults for a strategy."""
    return dict(DEFAULT_STRATEGY_CONFIG.get(name, {}))


def trade_management_defaults() -> dict[str, Any]:
    """Return a shallow copy of the canonical trade-management defaults."""
    return dict(DEFAULT_TRADE_MANAGEMENT)


def merged_config(config: dict[str, Any] | None, name: str) -> dict[str, Any]:
    """Merge caller config over the canonical defaults for a strategy.

    Any key the caller did not provide falls back to the acceptable default.
    The returned dict is a fresh copy; the caller's dict is never mutated.
    """
    merged = strategy_defaults(name)
    if config:
        merged.update(config)
    return merged
