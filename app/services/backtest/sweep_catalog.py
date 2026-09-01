"""Canonical parameter-sweep catalogue for the backtest UI and CLI.

This module is the single source of truth for *which* strategy/launcher
parameters are interesting to sweep (the options of the Cartesian product)
and their default candidate values.  It feeds:

  * the ``Parameter Sweep`` preset dropdown on the BACKTEST page
    (``app/ui/pages.py``), and
  * any headless sweep scripts (``scripts/run_*_sweep.py``).

Each strategy key maps to an ordered dict of ``param -> comma-separated
candidate values``.  Boolean candidates are written as ``true``/``false`` and
parsed by :func:`parse_values_text` into real Python ``bool`` values (a bare
``"False"`` string would otherwise be truthy and silently break the sweep).

Keys are emitted as dotted paths of the form
``strategies.<strategy_name>.<param>`` (launcher-level keys are bare), matching
what :class:`~app.services.backtest.models.GridParamDef` expects.
"""

from __future__ import annotations

from itertools import product
from typing import Any, Iterable

# Human-readable short prefixes for the preset dropdown labels.
STRATEGY_DISPLAY_NAMES: dict[str, str] = {
    "mean_reversion": "MR",
    "spike_continuation": "SC",
    "liquidity_sweep": "LS",
    "vwap_reversion": "VWAP",
    "trend_pullback": "TP",
}

# ── Per-strategy sweep parameters ──────────────────────────────────────────
# Ordered so the preset dropdown reads in a stable, logical order.
STRATEGY_SWEEP_PARAMS: dict[str, dict[str, str]] = {
    "mean_reversion": {
        "rsi_oversold": "25, 30, 35, 40",
        "rsi_overbought": "65, 70, 75, 80",
        "min_adx": "0, 5, 10",
        "max_adx": "20, 25, 28, 35",
        "min_atr_pct": "0.5, 1.0, 1.5, 2.0",
        "bb_proximity_pct": "0.3, 0.5, 0.7",
        "min_bb_bandwidth": "1.0, 2.0, 3.0",
        "max_bb_bandwidth": "0, 4.0, 6.0",
        "candle_rejection_pct": "20, 30, 40",
        "min_reward_risk_ratio": "0.5, 1.0, 1.5",
        "require_htf_trend": "true, false",
        "require_bb_position": "true, false",
        "require_candle_rejection": "true, false",
        "require_price_in_va": "true, false",
        "dynamic_tp": "true, false",
        "dynamic_tp_fraction": "0.4, 0.7, 0.9",
        "exit_on_regime_breakdown": "true, false",
        # Volume-participation gate (§3).
        "require_min_volume": "true, false",
        "min_volume_ratio": "0.5, 0.7, 0.9",
        "volume_lookback": "10, 20, 40",
        "require_volume_cooling": "true, false",
        "volume_rsi_max": "70, 80, 90",
        "require_vwap_reversion": "true, false",
        "vwap_min_distance_pct": "0.5, 1.0, 1.5",
        # CMF / footprint gates (§3).
        "require_cmf": "true, false",
        "require_htf_cmf": "true, false",
        "require_cmf_cross": "true, false",
        "require_cmf_no_divergence": "true, false",
        "require_footprint_delta": "true, false",
        # Funding / book-imbalance gates (§3).
        "require_no_extreme_funding": "true, false",
        "funding_max_abs_rate": "0.0005, 0.001, 0.002",
        "require_balanced_book": "true, false",
        "imbalance_min": "0.4, 0.6, 0.8",
        "imbalance_max": "1.2, 1.4, 1.6",
        # Regime + ATR/structural sizing.
        "require_regime": "true, false",
        "regime_lookback": "30, 50, 80",
        "max_bb_bandwidth_percentile": "40, 55, 70",
        "use_atr_sizing": "true, false",
        "use_structural_sizing": "true, false",
        "use_adaptive_atr": "true, false",
        "structural_sl_buffer_atr": "0.5, 1.0, 1.5",
        "atr_min_tp_mult": "0.3, 0.5, 1.0",
        "atr_max_tp_mult": "3.0, 4.0, 5.0",
        "atr_min_sl_mult": "0.2, 0.3, 0.5",
        "atr_max_sl_mult": "2.0, 3.0, 4.0",
        "atr_tp_multiplier": "1.2, 1.8, 2.5",
        "atr_sl_multiplier": "0.7, 1.0, 1.5",
        # Analysis/regime selectors (read by the engine's analyse path).
        "analysis_timeframe": "15m, 1H, 4H",
        "htf_regime_preference": "chop, trend, off",
        # TP/SL are optional — blank drives dynamic (ATR/structural) sizing.
        "tp_pct": "1.0, 2.0, 3.0",
        "sl_pct": "1.0, 2.0, 3.0, 4.0",
    },
    "spike_continuation": {
        "volume_rsi_min": "70, 75, 80, 85",
        "rsi_min": "50, 55, 60, 65",
        "rsi_max": "75, 80, 85",
        "min_bb_bandwidth": "2.0, 3.0, 4.0",
        "max_adx": "0, 20, 30",
        "max_adx_for_entry": "25, 32, 40",
        "max_spike_extension_atr": "1.0, 1.5, 2.0, 3.0",
        "spike_lookback": "3, 5, 8",
        "acceleration_min_ratio": "1.0, 1.2, 1.5, 2.0",
        "acceleration_lookback": "2, 3, 5",
        "candle_strength_pct": "40, 50, 60, 70",
        "min_atr_pct": "0.5, 1.0, 2.0",
        "min_reward_risk_ratio": "1.5, 2.0, 2.5",
        "require_bb_breakout": "true, false",
        "require_rsi_rising": "true, false",
        "require_candle_strength": "true, false",
        "require_momentum_acceleration": "true, false",
        "require_volume_rsi_rising": "true, false",
        "require_oi_confirmation": "true, false",
        "oi_min_zscore": "0.3, 0.7, 1.0",
        "exit_on_momentum_rollover": "true, false",
        # Volume-participation gate (§3).
        "require_min_volume": "true, false",
        "min_volume_ratio": "0.5, 0.7, 0.9",
        "volume_lookback": "10, 20, 40",
        # Regime + ATR sizing.
        "require_regime": "true, false",
        "regime_lookback": "30, 50, 80",
        "min_bb_bandwidth_percentile": "50, 65, 80",
        "use_atr_sizing": "true, false",
        "atr_tp_multiplier": "3.0, 4.0, 5.0",
        "atr_sl_multiplier": "1.5, 2.0, 3.0",
        # Analysis/regime selectors (read by the engine's analyse path).
        "analysis_timeframe": "15m, 1H, 4H",
        "htf_regime_preference": "chop, trend, off",
        "tp_pct": "3.0, 5.0, 7.0, 10.0",
        "sl_pct": "2.0, 3.0, 4.0, 5.0",
    },
    "liquidity_sweep": {
        "lookback": "10, 20, 30",
        "sweep_penetration_mode": "atr, pct",
        "sweep_buffer_pct": "0.05, 0.1, 0.2",
        "sweep_buffer_atr": "0.15, 0.25, 0.4",
        "reclaim_buffer_pct": "0.05, 0.1, 0.2",
        "reclaim_ratio": "0.3, 0.5, 0.7",
        "pivot_bars": "2, 3, 5",
        "require_volume_spike": "true, false",
        "volume_spike_ratio": "1.2, 1.5, 2.0",
        "volume_lookback": "5, 10, 20",
        "max_adx": "25, 28, 35",
        "min_atr_pct": "0.5, 0.8, 1.5",
        "min_reward_risk_ratio": "0.7, 1.0, 1.5",
        "require_htf_trend": "true, false",
        "require_close_in_va": "true, false",
        "require_macro_sl": "true, false",
        "macro_sl_lookback": "30, 50, 80",
        "require_book_imbalance": "false, true",
        "imbalance_min_for_long": "0.7, 1.0, 1.3",
        "imbalance_max_for_short": "0.7, 1.0, 1.3",
        # Regime + ATR/structural sizing.
        "require_regime": "true, false",
        "regime_lookback": "30, 50, 80",
        "max_bb_bandwidth_percentile": "45, 60, 75",
        "use_atr_sizing": "true, false",
        "use_structural_sizing": "true, false",
        "structural_sl_buffer_atr": "0.1, 0.15, 0.3",
        "atr_min_tp_mult": "0.3, 0.5, 1.0",
        "atr_max_tp_mult": "3.0, 4.0, 5.0",
        "atr_min_sl_mult": "0.2, 0.3, 0.5",
        "atr_max_sl_mult": "2.0, 3.0, 4.0",
        "atr_tp_multiplier": "0.8, 1.2, 1.8",
        "atr_sl_multiplier": "0.7, 1.0, 1.5",
        # Analysis/regime selectors (read by the engine's analyse path).
        "analysis_timeframe": "15m, 1H, 4H",
        "htf_regime_preference": "chop, trend, off",
        "tp_pct": "2.0, 3.0, 4.0",
        "sl_pct": "1.0, 2.0, 3.0",
    },
    "vwap_reversion": {
        "vwap_min_distance_atr": "1.5, 2.5, 3.0",
        "vwap_max_distance_atr": "2.5, 3.25, 4.0",
        "max_adx": "20, 25, 30",
        "min_atr_pct": "0.5, 1.0, 2.0",
        "min_reward_risk_ratio": "0.7, 1.0, 1.5",
        "require_closeback": "true, false",
        "require_htf_trend": "true, false",
        "regime_primary_gate": "adx, bb",
        # Volume-participation + funding gates (§3).
        "require_min_volume": "true, false",
        "min_volume_ratio": "0.5, 0.7, 0.9",
        "volume_lookback": "10, 20, 40",
        "require_no_funding_bias": "true, false",
        "funding_max_abs_rate": "0.0005, 0.0007, 0.001",
        # Regime + ATR/structural sizing.
        "require_regime": "true, false",
        "regime_lookback": "30, 50, 80",
        "max_bb_bandwidth_percentile": "40, 55, 70",
        "use_atr_sizing": "true, false",
        "use_structural_sizing": "true, false",
        "structural_sl_buffer_atr": "0.1, 0.15, 0.3",
        "atr_min_tp_mult": "0.3, 0.5, 1.0",
        "atr_max_tp_mult": "3.0, 4.0, 5.0",
        "atr_min_sl_mult": "0.3, 0.5, 0.75",
        "atr_max_sl_mult": "2.0, 3.0, 4.0",
        "atr_tp_multiplier": "0.7, 1.0, 1.5",
        "atr_sl_multiplier": "0.7, 1.0, 1.5",
        # Analysis/regime selectors (read by the engine's analyse path).
        "analysis_timeframe": "15m, 1H, 4H",
        "htf_regime_preference": "chop, trend, off",
        "tp_pct": "1.0, 2.0, 3.0",
        "sl_pct": "1.5, 2.0, 3.0",
    },
    "trend_pullback": {
        "pullback_ema": "13, 21, 34",
        "pullback_proximity_pct": "0.2, 0.3, 0.5",
        "pullback_proximity_atr": "0.3, 0.5, 0.8",
        "min_adx": "15, 18, 22",
        "max_adx_for_entry": "25, 30, 35",
        "max_pullback_extension_atr": "1.0, 1.5, 2.0, 3.0",
        "candle_rejection_pct": "20, 25, 35",
        "use_vwap_as_level": "true, false",
        "require_bullish_candle": "true, false",
        "require_htf_trend": "true, false",
        "require_poc_proximity": "true, false",
        "poc_proximity_va_width": "0.1, 0.2, 0.3",
        "require_completed_pullback": "false, true",
        "min_atr_pct": "0.5, 1.0, 2.0",
        "min_reward_risk_ratio": "1.0, 1.5, 2.0",
        "flip_tp_sl": "true, false",
        # Volume-participation gate (§3).
        "require_min_volume": "true, false",
        "min_volume_ratio": "0.5, 0.7, 0.9",
        "volume_lookback": "10, 20, 40",
        # Volume-deceleration gate (§3).
        "require_volume_deceleration": "true, false",
        "min_volume_decel_ratio": "0.5, 0.7, 0.8",
        "volume_decel_recent_bars": "2, 4, 6",
        "volume_decel_prior_bars": "8, 16, 24",
        # ATR/structural sizing + fast-ATR lever.
        "use_atr_sizing": "true, false",
        "use_structural_sizing": "true, false",
        "use_adaptive_atr": "true, false",
        "use_fast_atr": "true, false",
        "fast_atr_length": "3, 4, 5",
        "structural_sl_buffer_atr": "0.1, 0.15, 0.3",
        "atr_min_tp_mult": "0.3, 0.5, 1.0",
        "atr_max_tp_mult": "3.0, 4.0, 5.0",
        "atr_min_sl_mult": "0.2, 0.3, 0.5",
        "atr_max_sl_mult": "2.0, 3.0, 4.0",
        "atr_tp_multiplier": "1.5, 2.25, 3.0",
        "atr_sl_multiplier": "1.0, 1.5, 2.0",
        # Analysis/regime selectors (read by the engine's analyse path).
        "analysis_timeframe": "15m, 1H, 4H",
        "htf_regime_preference": "chop, trend, off",
        "tp_pct": "3.0, 5.0, 6.0, 8.0",
        "sl_pct": "2.0, 3.0, 4.0, 5.0",
    },
}

# ── Launcher-level sweep parameters (bare keys, not under ``strategies``) ──
LAUNCHER_SWEEP_PARAMS: dict[str, str] = {
    "tp_pct": "1.0, 2.0, 3.0, 5.0",
    "sl_pct": "1.0, 2.0, 3.0, 4.0",
    "notional_usd": "10, 25, 50, 100",
}


def parse_values_text(text: str) -> list[Any]:
    """Parse a comma-separated candidate-value string into a typed list.

    Rules (in order):
      * ``true`` / ``false`` (case-insensitive) → ``bool``
      * otherwise a ``float`` literal → ``float``
      * otherwise the stripped token → ``str``

    Boolean handling matters: a bare ``"False"`` string is truthy, so without
    converting it to ``False`` a bool sweep would collapse every run to the
    same (enabled) state.
    """
    out: list[Any] = []
    for tok in text.split(","):
        tok = tok.strip()
        if not tok:
            continue
        low = tok.lower()
        if low in ("true", "false"):
            out.append(low == "true")
            continue
        try:
            out.append(float(tok))
        except ValueError:
            out.append(tok)
    return out


def build_sweep_presets() -> dict[tuple[str, str], str]:
    """Flatten the catalogue into ``{(dotted_key, values_str): label}``.

    NiceGUI ``ui.select`` uses a ``{value: label}`` dict convention: the
    *value* is what ``e.value`` returns on selection, the *label* is what is
    displayed.  Here the value is the ``(parameter key, comma-separated
    candidate values)`` tuple — which the UI's "Add Parameter" row decomposes
    into its key/value inputs — and the label is a human-friendly name.
    """
    presets: dict[tuple[str, str], str] = {}
    for name, params in STRATEGY_SWEEP_PARAMS.items():
        prefix = STRATEGY_DISPLAY_NAMES.get(name, name)
        for param, values in params.items():
            label = f"{prefix} {param}"
            key = f"strategies.{name}.{param}"
            presets[(key, values)] = label
    for param, values in LAUNCHER_SWEEP_PARAMS.items():
        presets[(param, values)] = f"Launcher {param}"
    return presets


def sweep_strategy_names() -> list[str]:
    """Return the strategy names present in the sweep catalogue."""
    return list(STRATEGY_SWEEP_PARAMS.keys())


def sweep_groups(
    strategy_names: Iterable[str] | None = None,
) -> list[tuple[str, list[tuple[str, str, str]]]]:
    """Return the catalogue grouped for a nested preset menu.

    Each element is ``(group_label, items)`` where ``items`` is a list of
    ``(dotted_key, values_str, leaf_label)`` tuples.  The strategy's display
    prefix (``MR``/``SC``/…) becomes the group label; each parameter becomes
    a leaf.  A trailing ``"Launcher"`` group carries the launcher-level keys.

    Passing ``strategy_names`` restricts the output to those strategies (in
    that order), which is what the UI uses to show only the strategies the
    user has enabled.  ``None`` → every strategy in catalogue order.
    """
    names = list(strategy_names) if strategy_names is not None else sweep_strategy_names()
    groups: list[tuple[str, list[tuple[str, str, str]]]] = []
    for name in names:
        params = STRATEGY_SWEEP_PARAMS.get(name)
        if not params:
            continue
        label = STRATEGY_DISPLAY_NAMES.get(name, name)
        items = [
            (f"strategies.{name}.{param}", values, param)
            for param, values in params.items()
        ]
        groups.append((label, items))
    if LAUNCHER_SWEEP_PARAMS:
        groups.append((
            "Launcher",
            [(param, values, param) for param, values in LAUNCHER_SWEEP_PARAMS.items()],
        ))
    return groups


def grid_param_defs(
    strategy_names: Iterable[str] | None = None,
    *,
    include_launcher: bool = True,
) -> list[Any]:
    """Return the catalogue as an ordered list of ``GridParamDef``.

    This is the CLI analogue of :func:`build_sweep_presets`: the UI renders
    presets from the catalogue, and headless sweep scripts build their
    Cartesian product from these same ``GridParamDef`` objects, so both paths
    always sweep the identical set of options.

    Parameters
    ----------
    strategy_names:
        Which strategies to include (any order).  ``None`` → every strategy
        in the catalogue, in catalogue order.
    include_launcher:
        Include the launcher-level sweep params (``tp_pct`` / ``sl_pct`` /
        ``notional_usd``).  Default ``True``.

    Returns
    -------
    A list of :class:`~app.services.backtest.models.GridParamDef`, one per
    parameter, with values parsed by :func:`parse_values_text` (booleans
    become real ``bool``, numerics become ``float``).
    """
    from app.services.backtest.models import GridParamDef

    names = list(strategy_names) if strategy_names is not None else sweep_strategy_names()
    defs: list[Any] = []
    for name in names:
        params = STRATEGY_SWEEP_PARAMS.get(name)
        if not params:
            continue
        for param, values in params.items():
            defs.append(GridParamDef(
                key=f"strategies.{name}.{param}",
                values=parse_values_text(values),
            ))
    if include_launcher:
        for param, values in LAUNCHER_SWEEP_PARAMS.items():
            defs.append(GridParamDef(key=param, values=parse_values_text(values)))
    return defs


def cartesian_combinations(param_defs: Iterable[Any]) -> list[dict[str, Any]]:
    """Return the Cartesian product of ``param_defs`` as ``{key: value}`` dicts.

    Each element is one full parameter assignment (one row of the sweep grid).
    The order is deterministic: the left-most parameter varies slowest.
    """
    defs = list(param_defs)
    if not defs:
        return []
    keys = [p.key for p in defs]
    value_lists = [list(p.values) for p in defs]
    return [dict(zip(keys, combo)) for combo in product(*value_lists)]