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

from typing import Any

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
        # TP/SL are optional — blank drives dynamic (ATR/structural) sizing.
        "tp_pct": "1.0, 2.0, 3.0",
        "sl_pct": "1.0, 2.0, 3.0, 4.0",
    },
    "spike_continuation": {
        "volume_rsi_min": "70, 75, 80, 85",
        "rsi_min": "50, 55, 60, 65",
        "rsi_max": "75, 80, 85",
        "min_bb_bandwidth": "2.0, 3.0, 4.0",
        "max_adx_for_entry": "25, 32, 40",
        "max_spike_extension_atr": "1.0, 1.5, 2.0, 3.0",
        "acceleration_min_ratio": "1.0, 1.2, 1.5, 2.0",
        "candle_strength_pct": "40, 50, 60, 70",
        "require_bb_breakout": "true, false",
        "require_rsi_rising": "true, false",
        "require_oi_confirmation": "true, false",
        "exit_on_momentum_rollover": "true, false",
        "tp_pct": "3.0, 5.0, 7.0, 10.0",
        "sl_pct": "2.0, 3.0, 4.0, 5.0",
    },
    "liquidity_sweep": {
        "lookback": "10, 20, 30",
        "sweep_buffer_pct": "0.05, 0.1, 0.2",
        "sweep_buffer_atr": "0.15, 0.25, 0.4",
        "reclaim_buffer_pct": "0.05, 0.1, 0.2",
        "reclaim_ratio": "0.3, 0.5, 0.7",
        "pivot_bars": "2, 3, 5",
        "volume_spike_ratio": "1.2, 1.5, 2.0",
        "max_adx": "25, 28, 35",
        "require_htf_trend": "true, false",
        "require_close_in_va": "true, false",
        "require_macro_sl": "true, false",
        "require_book_imbalance": "false, true",
        "tp_pct": "2.0, 3.0, 4.0",
        "sl_pct": "1.0, 2.0, 3.0",
    },
    "vwap_reversion": {
        "vwap_min_distance_atr": "1.5, 2.5, 3.0",
        "vwap_max_distance_atr": "2.5, 3.25, 4.0",
        "max_adx": "20, 25, 30",
        "require_closeback": "true, false",
        "require_htf_trend": "true, false",
        "regime_primary_gate": "adx, bb",
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
        "require_poc_proximity": "true, false",
        "require_completed_pullback": "false, true",
        "min_reward_risk_ratio": "1.0, 1.5, 2.0",
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