"""Tests for the backtest parameter-sweep catalogue.

Covers:
  - the catalogue covers every strategy in the engine registry
  - every parameter key is a well-formed dotted path matching its strategy
  - preset labels are unique and value-tuples carry the dotted key
  - ``parse_values_text`` converts bool / float / string tokens correctly
    (in particular ``"true"``/``"false"`` → ``bool``, never truthy strings)
"""

from __future__ import annotations

import pytest

from app.services.backtest.engine import available_strategy_names
from app.services.backtest.sweep_catalog import (
    STRATEGY_SWEEP_PARAMS,
    build_sweep_presets,
    cartesian_combinations,
    grid_param_defs,
    parse_values_text,
)


class TestSweepCatalogCoverage:
    """The catalogue should know about every engine-registered strategy."""

    def test_catalog_covers_all_strategies(self) -> None:
        engine_strategies = set(available_strategy_names())
        catalog_strategies = set(STRATEGY_SWEEP_PARAMS.keys())
        # The catalogue must not reference strategies the engine doesn't know,
        # and must cover every strategy the engine exposes for sweeping.
        assert catalog_strategies <= engine_strategies
        # Every engine strategy should be sweepable (otherwise the UI omits
        # part of the Cartesian-product options the user asked for).
        assert engine_strategies <= catalog_strategies

    def test_every_strategy_has_params(self) -> None:
        for name, params in STRATEGY_SWEEP_PARAMS.items():
            assert params, f"strategy {name!r} has no sweep parameters"
            assert "enabled" not in params, (
                "do not sweep 'enabled' — it is controlled by the strategy toggle"
            )


class TestPresetShape:
    """Presets should map (dotted_key, values) → unique label."""

    def test_preset_keys_are_dotted_and_match_strategy(self) -> None:
        presets = build_sweep_presets()
        assert presets, "presets must not be empty"
        for (key, values) in presets:
            assert values.strip(), f"preset {key!r} has empty candidate values"
            if key.startswith("strategies."):
                # key == "strategies.<name>.<param>"
                parts = key.split(".")
                assert len(parts) == 3, f"unexpected dotted key {key!r}"
                assert parts[1] in STRATEGY_SWEEP_PARAMS, f"unknown strategy in {key!r}"
            else:
                # launcher-level keys are bare, e.g. "tp_pct"
                assert "." not in key, f"launcher key {key!r} should be bare"

    def test_preset_labels_are_unique(self) -> None:
        presets = build_sweep_presets()
        labels = list(presets.values())
        assert len(labels) == len(set(labels)), "preset labels must be unique"

    def test_common_sweeps_present(self) -> None:
        """The canonical presets from the old hardcoded UI list still exist."""
        presets = build_sweep_presets()
        keys = {k for (k, _v) in presets}
        # Spot-check the parameters the prior UI exposed, now covered by the
        # catalogue (so no regression in the options available to the user).
        assert "strategies.mean_reversion.rsi_oversold" in keys
        assert "strategies.mean_reversion.max_adx" in keys
        assert "strategies.spike_continuation.volume_rsi_min" in keys
        assert "strategies.spike_continuation.max_spike_extension_atr" in keys
        assert "strategies.spike_continuation.tp_pct" in keys
        assert "strategies.spike_continuation.sl_pct" in keys
        assert "tp_pct" in keys
        assert "sl_pct" in keys


class TestParseValuesText:
    """Boolean-aware parsing of the comma-separated candidate strings."""

    def test_floats(self) -> None:
        assert parse_values_text("25, 30, 35, 40") == [25.0, 30.0, 35.0, 40.0]
        assert parse_values_text("1.0, 2.5") == [1.0, 2.5]

    def test_bools_are_real_bools(self) -> None:
        # The critical case: "true, false" must yield bools, not the
        # truthy strings that would silently collapse the sweep.
        assert parse_values_text("true, false") == [True, False]

    def test_strings_fall_back(self) -> None:
        assert parse_values_text("adx, bb") == ["adx", "bb"]

    def test_mixed_types(self) -> None:
        assert parse_values_text("0, 4.0, 6.0") == [0.0, 4.0, 6.0]

    def test_empty_and_blank_tokens_skipped(self) -> None:
        assert parse_values_text("") == []
        assert parse_values_text("1.0, , 2.0") == [1.0, 2.0]

    def test_case_insensitive_bools(self) -> None:
        assert parse_values_text("TRUE, False") == [True, False]


class TestGridParamDefs:
    """The CLI-side catalogue helpers mirror the UI presets."""

    def test_defs_match_presets(self) -> None:
        # The number of GridParamDefs (excluding launcher) must equal the
        # number of strategy preset entries, so CLI and UI sweep identically.
        defs = grid_param_defs(include_launcher=False)
        strategy_keys = {k for (k, _v) in build_sweep_presets() if k.startswith("strategies.")}
        assert {d.key for d in defs} == strategy_keys

    def test_defs_include_launcher_when_requested(self) -> None:
        defs = grid_param_defs(include_launcher=True)
        keys = {d.key for d in defs}
        assert "tp_pct" in keys
        assert "sl_pct" in keys
        assert "notional_usd" in keys

    def test_defs_parse_booleans_and_floats(self) -> None:
        defs = {d.key: d.values for d in grid_param_defs(["mean_reversion"], include_launcher=False)}
        assert defs["strategies.mean_reversion.require_htf_trend"] == [True, False]
        assert defs["strategies.mean_reversion.rsi_oversold"] == [25.0, 30.0, 35.0, 40.0]

    def test_defs_respect_strategy_subset(self) -> None:
        defs = grid_param_defs(["vwap_reversion"], include_launcher=False)
        assert all(d.key.startswith("strategies.vwap_reversion.") for d in defs)
        assert defs, "vwap_reversion should have sweep params"


class TestCartesianCombinations:
    """The Cartesian product helper drives the grid the UI also builds."""

    def test_empty(self) -> None:
        assert cartesian_combinations([]) == []

    def test_product_size_is_product_of_value_counts(self) -> None:
        defs = grid_param_defs(["mean_reversion"], include_launcher=False)
        # Restrict to two params with known arities for a fast, exact check.
        rsi = [d for d in defs if d.key == "strategies.mean_reversion.rsi_oversold"][0]
        adx = [d for d in defs if d.key == "strategies.mean_reversion.max_adx"][0]
        combos = cartesian_combinations([rsi, adx])
        assert len(combos) == len(rsi.values) * len(adx.values)
        assert len(combos) == len(set(tuple(sorted(c.items())) for c in combos)), \
            "combinations must be unique"

    def test_combinations_are_full_assignments(self) -> None:
        defs = grid_param_defs(["mean_reversion"], include_launcher=False)[:2]
        combos = cartesian_combinations(defs)
        keys = {d.key for d in defs}
        for combo in combos:
            assert set(combo.keys()) == keys