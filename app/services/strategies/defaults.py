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
    # Liquidity-aware gates (§3) — all OFF by default (opt-in).
    "require_price_in_va": False,
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
    # Liquidity-aware gates (§3) — OFF by default (opt-in).
    "require_close_in_va": False,
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
    "vwap_min_distance_atr": 2.0,
    "vwap_max_distance_atr": 3.0,
    "max_adx": 25.0,
    "require_closeback": True,
    "require_htf_trend": True,
    "require_regime": True,
    "max_bb_bandwidth_percentile": 55.0,
    "regime_lookback": 50,
    "use_atr_sizing": True,
    "use_structural_sizing": False,
    "structural_sl_buffer_atr": 0.15,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    "atr_min_sl_mult": 0.3,
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
}

# ── Trend Pullback ──────────────────────────────────────────────────────────
DEFAULT_TREND_PULLBACK: dict[str, Any] = {
    "enabled": False,
    "tp_pct": 4.0,
    "sl_pct": 3.0,
    "pullback_ema": 21,
    "pullback_proximity_pct": 0.3,
    "use_vwap_as_level": True,
    "require_htf_trend": True,
    "require_bullish_candle": True,
    "candle_rejection_pct": 25.0,
    "min_adx": 20.0,
    "max_adx_for_entry": 28.0,
    "use_atr_sizing": True,
    "use_structural_sizing": True,
    "structural_sl_buffer_atr": 0.15,
    "atr_min_tp_mult": 0.5,
    "atr_max_tp_mult": 4.0,
    "atr_min_sl_mult": 0.3,
    "atr_max_sl_mult": 3.0,
    "atr_tp_multiplier": 1.2,
    "atr_sl_multiplier": 1.0,
    "min_atr_pct": 1.0,
    "min_reward_risk_ratio": 1.0,
    "flip_launcher_direction": None,
    # Per-strategy analysis timeframe. Trend pullbacks need cleaner 1H structure.
    "analysis_timeframe": "1H",
    # HTF regime gate preference: trend pullback wants a trending HTF → "trend".
    "htf_regime_preference": "trend",
    # Liquidity-aware gates (§3) — OFF by default (opt-in).
    "require_poc_proximity": False,
    "poc_proximity_va_width": 0.2,
}

# Registry keyed by strategy name.
DEFAULT_STRATEGY_CONFIG: dict[str, dict[str, Any]] = {
    "mean_reversion": DEFAULT_MEAN_REVERSION,
    "spike_continuation": DEFAULT_SPIKE_CONTINUATION,
    "liquidity_sweep": DEFAULT_LIQUIDITY_SWEEP,
    "vwap_reversion": DEFAULT_VWAP_REVERSION,
    "trend_pullback": DEFAULT_TREND_PULLBACK,
}


def strategy_defaults(name: str) -> dict[str, Any]:
    """Return a shallow copy of the canonical defaults for a strategy."""
    return dict(DEFAULT_STRATEGY_CONFIG.get(name, {}))


def merged_config(config: dict[str, Any] | None, name: str) -> dict[str, Any]:
    """Merge caller config over the canonical defaults for a strategy.

    Any key the caller did not provide falls back to the acceptable default.
    The returned dict is a fresh copy; the caller's dict is never mutated.
    """
    merged = strategy_defaults(name)
    if config:
        merged.update(config)
    return merged
