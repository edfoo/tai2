"""Tests for the shared trade-management decision rules.

The arithmetic in ``app/services/trade_management_rules.py`` is the single
source of truth for live and backtest breakeven / trailing / software-stop
math.  These tests pin the exact stop prices so neither side can drift.
"""

from __future__ import annotations

import pytest

from app.services.trade_management_rules import (
    compute_breakeven_sl,
    compute_far_tp,
    compute_trade_management_decision,
    compute_trailing_sl,
    resolve_tm_params,
    should_ratchet_sl,
)


class TestBreakeven:
    def test_long_breakeven_buffer_above_entry(self) -> None:
        assert compute_breakeven_sl(is_long=True, entry_price=100.0, breakeven_buffer_pct=0.05) == pytest.approx(100.05)

    def test_short_breakeven_buffer_below_entry(self) -> None:
        assert compute_breakeven_sl(is_long=False, entry_price=100.0, breakeven_buffer_pct=0.05) == pytest.approx(99.95)


class TestTrailing:
    def test_long_trail_clamps_to_floor(self) -> None:
        # mark=103, atr=1.0, distance=1.5 → trail = 101.455; floor at 0.5*2% = 99.
        new_sl, floor = compute_trailing_sl(
            is_long=True,
            entry_price=100.0,
            mark_price=103.0,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
        )
        assert new_sl == pytest.approx(101.455, abs=0.01)
        assert floor == pytest.approx(99.0)

    def test_short_trail_clamps_to_floor(self) -> None:
        new_sl, _ = compute_trailing_sl(
            is_long=False,
            entry_price=100.0,
            mark_price=97.0,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
        )
        # trail = 97 + 1.5*1%*97 = 98.455; floor = 100 + 0.5*2% = 101 → min = 98.455
        assert new_sl == pytest.approx(98.455, abs=0.01)

    def test_long_profit_floor_locks_stop_above_entry(self) -> None:
        # mark barely above entry → the raw trail sits below entry, so the
        # profit floor must bind and place the stop in profit.
        new_sl, floor = compute_trailing_sl(
            is_long=True,
            entry_price=100.0,
            mark_price=100.5,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
            trailing_floor_profit_pct=0.5,
        )
        assert floor == pytest.approx(100.5)
        assert new_sl == pytest.approx(100.5)
        assert new_sl > 100.0  # in profit, not a loss

    def test_short_profit_floor_locks_stop_below_entry(self) -> None:
        # mark 1% in profit (below entry) → the 0.5% floor is reachable.
        new_sl, floor = compute_trailing_sl(
            is_long=False,
            entry_price=100.0,
            mark_price=99.0,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
            trailing_floor_profit_pct=0.5,
        )
        assert floor == pytest.approx(99.5)
        assert new_sl == pytest.approx(99.5)
        assert new_sl < 100.0  # in profit for a short

    def test_profit_floor_scales_with_price(self) -> None:
        # 0.5% of notional is scale-invariant: same % on a $0.50 and a
        # $100,000 symbol (the fixed-dollar floor could not do this).
        _lo_sl, lo_floor = compute_trailing_sl(
            is_long=True, entry_price=0.50, mark_price=0.51,
            risk_pct=2.0, atr_pct=1.0, trailing_distance_atr=1.5,
            trailing_floor_r=0.5, trailing_floor_profit_pct=0.5,
        )
        _hi_sl, hi_floor = compute_trailing_sl(
            is_long=True, entry_price=100_000.0, mark_price=102_000.0,
            risk_pct=2.0, atr_pct=1.0, trailing_distance_atr=1.5,
            trailing_floor_r=0.5, trailing_floor_profit_pct=0.5,
        )
        assert lo_floor == pytest.approx(0.5025)
        assert hi_floor == pytest.approx(100_500.0)

    def test_profit_floor_never_crosses_mark(self) -> None:
        # A floor beyond the mark would trigger instantly; it must be ignored
        # and the trail distance used instead.
        new_sl, floor = compute_trailing_sl(
            is_long=True,
            entry_price=100.0,
            mark_price=100.01,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
            trailing_floor_profit_pct=5.0,
        )
        # trail = 100.01 - 1.5*1%*100.01 = 98.50985
        assert floor == pytest.approx(98.50985, abs=0.01)
        assert new_sl == pytest.approx(98.50985, abs=0.01)
        assert new_sl < 100.01

    def test_no_profit_floor_uses_legacy_r_floor(self) -> None:
        # trailing_floor_profit_pct=None restores the R-based (below-entry) floor.
        _new_sl, floor = compute_trailing_sl(
            is_long=True,
            entry_price=100.0,
            mark_price=103.0,
            risk_pct=2.0,
            atr_pct=1.0,
            trailing_distance_atr=1.5,
            trailing_floor_r=0.5,
            trailing_floor_profit_pct=None,
        )
        assert floor == pytest.approx(99.0)


class TestRatchet:
    def test_no_current_sl_always_updates(self) -> None:
        assert should_ratchet_sl(is_long=True, current_sl=None, new_sl=101.0, step_distance=0.2) is True

    def test_long_requires_improvement_beyond_step(self) -> None:
        assert should_ratchet_sl(is_long=True, current_sl=100.0, new_sl=100.1, step_distance=0.2) is False
        assert should_ratchet_sl(is_long=True, current_sl=100.0, new_sl=100.3, step_distance=0.2) is True

    def test_short_requires_improvement_beyond_step(self) -> None:
        assert should_ratchet_sl(is_long=False, current_sl=100.0, new_sl=99.9, step_distance=0.2) is False
        assert should_ratchet_sl(is_long=False, current_sl=100.0, new_sl=99.7, step_distance=0.2) is True


class TestDecision:
    def test_software_stop_fires_when_pnl_below_risk(self) -> None:
        p = resolve_tm_params({"enabled": True, "software_stop_loss_enabled": True})
        d = compute_trade_management_decision(
            p,
            is_long=True,
            entry_price=100.0,
            mark_price=97.0,
            risk_pct=2.0,
            pnl_pct=-3.0,
            r_multiple=-1.5,
            atr_pct=1.0,
            current_sl=98.0,
            breakeven_done=False,
            partial_done=False,
        )
        assert d.software_stop is True

    def test_breakeven_fires_at_r_threshold(self) -> None:
        p = resolve_tm_params({"enabled": True, "breakeven_at_r": 0.7})
        d = compute_trade_management_decision(
            p,
            is_long=True,
            entry_price=100.0,
            mark_price=101.5,
            risk_pct=2.0,
            pnl_pct=1.5,
            r_multiple=0.75,
            atr_pct=1.0,
            current_sl=98.0,
            breakeven_done=False,
            partial_done=False,
        )
        assert d.breakeven_new_sl is not None
        assert d.breakeven_new_sl == pytest.approx(100.05)

    def test_disabled_params_produce_no_decision(self) -> None:
        p = resolve_tm_params({"enabled": False})
        d = compute_trade_management_decision(
            p,
            is_long=True,
            entry_price=100.0,
            mark_price=97.0,
            risk_pct=2.0,
            pnl_pct=-3.0,
            r_multiple=-1.5,
            atr_pct=1.0,
            current_sl=98.0,
            breakeven_done=False,
            partial_done=False,
        )
        assert d.software_stop is False
        assert d.breakeven_new_sl is None
        assert d.timeout is False

    def test_far_tp_computed_when_remove_tp_disabled(self) -> None:
        p = resolve_tm_params({
            "enabled": True,
            "trailing_enabled": True,
            "trailing_activate_r": 0.8,
            "trailing_remove_tp": False,
            "trailing_far_tp_mult": 2.0,
        })
        d = compute_trade_management_decision(
            p,
            is_long=True,
            entry_price=100.0,
            mark_price=103.0,
            risk_pct=2.0,
            pnl_pct=3.0,
            r_multiple=1.5,
            atr_pct=1.0,
            current_sl=98.0,
            breakeven_done=False,
            partial_done=False,
        )
        assert d.trailing_new_sl is not None
        assert d.trailing_remove_tp is False
        assert d.trailing_far_tp == pytest.approx(104.0)

    def test_decision_threads_profit_floor(self) -> None:
        # Parity guard: the decision path must pass the profit floor through to
        # compute_trailing_sl (live and backtest share this path).
        p = resolve_tm_params({
            "enabled": True,
            "trailing_enabled": True,
            "trailing_activate_r": 0.8,
            "trailing_floor_profit_pct": 0.5,
        })
        assert p.trailing_floor_profit_pct == pytest.approx(0.5)
        d = compute_trade_management_decision(
            p,
            is_long=True,
            entry_price=100.0,
            mark_price=100.5,
            risk_pct=2.0,
            pnl_pct=0.5,
            r_multiple=1.0,
            atr_pct=1.0,
            current_sl=98.0,
            breakeven_done=False,
            partial_done=False,
        )
        assert d.trailing_new_sl == pytest.approx(100.5)


class TestResolveProfitFloor:
    def test_default_is_half_percent(self) -> None:
        p = resolve_tm_params({"enabled": True})
        assert p.trailing_floor_profit_pct == pytest.approx(0.5)

    def test_explicit_value_is_honoured(self) -> None:
        p = resolve_tm_params({"enabled": True, "trailing_floor_profit_pct": 1.25})
        assert p.trailing_floor_profit_pct == pytest.approx(1.25)

    def test_zero_disables_floor(self) -> None:
        p = resolve_tm_params({"enabled": True, "trailing_floor_profit_pct": 0})
        assert p.trailing_floor_profit_pct is None

    def test_negative_disables_floor(self) -> None:
        p = resolve_tm_params({"enabled": True, "trailing_floor_profit_pct": -1.0})
        assert p.trailing_floor_profit_pct is None
