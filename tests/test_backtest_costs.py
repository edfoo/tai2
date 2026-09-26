"""Tests for the backtest cost model (fees / slippage / funding)."""

from __future__ import annotations

import pytest

from app.services.backtest.costs import CostModel
from app.services.backtest.models import Candle
from app.services.backtest.metrics import compute_metrics
from app.services.backtest.simulator import (
    Simulator,
    compute_isolated_liquidation_price,
)


class TestCostModel:
    def test_taker_fee(self) -> None:
        m = CostModel(taker_fee_bps=5.0)
        # 0.05% of 1000 = 0.5
        assert m.fee_for(1000.0, taker=True) == pytest.approx(0.5)
        assert m.fee_for(1000.0, taker=False) == pytest.approx(0.0)

    def test_entry_slippage_long_raises_price(self) -> None:
        m = CostModel(slippage_bps=10.0)  # 0.1%
        assert m.entry_price_for(100.0, is_long=True) == pytest.approx(100.1)

    def test_exit_slippage_long_lowers_price(self) -> None:
        m = CostModel(slippage_bps=10.0)
        assert m.exit_price_for(100.0, is_long=True) == pytest.approx(99.9)

    def test_funding_long_pays_positive_rate(self) -> None:
        m = CostModel(funding_rate_pct=0.01)  # 0.01% per interval
        # long pays: notional 1000 * 0.01% * 2 intervals = 0.2
        assert m.funding_payment(1000.0, is_long=True, intervals=2) == pytest.approx(0.2)

    def test_funding_short_receives_positive_rate(self) -> None:
        m = CostModel(funding_rate_pct=0.01)
        assert m.funding_payment(1000.0, is_long=False, intervals=2) == pytest.approx(-0.2)

    def test_zero_cost_defaults(self) -> None:
        m = CostModel()
        assert m.fee_for(1000.0) == 0.0
        assert m.entry_price_for(100.0, True) == 100.0

    def test_slippage_stress_multiplier_scales_estimate(self) -> None:
        m = CostModel(slippage_stress_multiplier=2.0)
        assert m.stressed_slippage_bps(10.0) == pytest.approx(20.0)
        # Zero multiplier removes slippage entirely.
        assert CostModel(slippage_stress_multiplier=0.0).stressed_slippage_bps(10.0) == 0.0
        # Negative estimates are clamped to zero before scaling.
        assert m.stressed_slippage_bps(-5.0) == 0.0

    def test_liquidation_fee_for(self) -> None:
        m = CostModel(liquidation_fee_bps=125.0)
        assert m.liquidation_fee_for(1000.0) == pytest.approx(12.5)
        assert CostModel().liquidation_fee_for(1000.0) == 0.0

    def test_isolated_liquidation_price_matches_tier_formula(self) -> None:
        # Long: (entry*size - initial_margin - deduction) / (size*(1 - mmr))
        long_price = compute_isolated_liquidation_price(
            direction="long",
            size=1.0,
            entry_price=100.0,
            initial_margin=35.0,
            maintenance_margin_ratio=0.1,
        )
        assert long_price == pytest.approx((100.0 - 35.0) / 0.9)

        # Short: (entry*size + initial_margin + deduction) / (size*(1 + mmr))
        short_price = compute_isolated_liquidation_price(
            direction="short",
            size=1.0,
            entry_price=100.0,
            initial_margin=35.0,
            maintenance_margin_ratio=0.1,
        )
        assert short_price == pytest.approx((100.0 + 35.0) / 1.1)

        # Maintenance deduction raises the long liquidation price.
        with_deduction = compute_isolated_liquidation_price(
            direction="long",
            size=1.0,
            entry_price=100.0,
            initial_margin=35.0,
            maintenance_margin_ratio=0.1,
            maintenance_margin_deduction=5.0,
        )
        assert with_deduction == pytest.approx((100.0 - 35.0 - 5.0) / 0.9)

        # Degenerate inputs return None rather than a non-positive price.
        assert compute_isolated_liquidation_price(
            direction="long", size=0.0, entry_price=100.0,
            initial_margin=35.0, maintenance_margin_ratio=0.1,
        ) is None
        assert compute_isolated_liquidation_price(
            direction="long", size=1.0, entry_price=100.0,
            initial_margin=200.0, maintenance_margin_ratio=0.1,
        ) is None

    def test_historical_funding_charges_only_settlement_events_after_entry(self) -> None:
        model = CostModel(
            funding_mode="historical",
            historical_funding_rates={
                "BTC-USDT-SWAP": [
                    {"ts": 100, "rate": 0.01},
                    {"ts": 200, "rate": -0.002},
                    {"ts": 300, "rate": 0.003},
                ]
            },
        )

        long_payment = model.funding_payment_between(
            1000.0, symbol="BTC-USDT-SWAP", is_long=True, entry_ts=100, close_ts=250
        )
        short_payment = model.funding_payment_between(
            1000.0, symbol="BTC-USDT-SWAP", is_long=False, entry_ts=100, close_ts=250
        )

        assert long_payment == pytest.approx(-2.0)
        assert short_payment == pytest.approx(2.0)

    def test_funding_mode_off_and_constant_fallback(self) -> None:
        off = CostModel(funding_mode="off", funding_rate_pct=0.01)
        assert off.funding_payment_between(
            1000.0, symbol="BTC-USDT-SWAP", is_long=True, entry_ts=0, close_ts=90_000
        ) == 0.0

        fallback = CostModel(
            funding_mode="historical",
            funding_rate_pct=0.01,
            funding_interval_ms=10,
        )
        assert fallback.funding_payment_between(
            1000.0, symbol="BTC-USDT-SWAP", is_long=True, entry_ts=0, close_ts=20
        ) == pytest.approx(0.2)


class TestCostAwareSimulator:
    def test_isolated_position_rounds_protection_and_uses_tier_liquidation(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={
                "guardrails": {"max_leverage": 10.0},
                "instrument_specs": {
                    "BTC-USDT-SWAP": {
                        "ct_val": 1.0,
                        "tick_size": 0.1,
                        "position_tiers": [{
                            "min_size": 0.0,
                            "max_size": 10.0,
                            "initial_margin_ratio": 0.35,
                            "maintenance_margin_ratio": 0.1,
                            "max_leverage": 3.0,
                        }],
                    }
                },
            },
            cost_model=CostModel(slippage_mode="fixed"),
        )
        position = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            tp_price=110.06,
            sl_price=90.04,
            strategy_name="margin-test",
        )

        assert position is not None
        assert position.tp_price == pytest.approx(110.0)
        assert position.sl_price == pytest.approx(90.1)
        assert position.leverage == pytest.approx(3.0)
        assert position.initial_margin == pytest.approx(35.0)
        assert position.liquidation_price == pytest.approx((100.0 - 35.0) / 0.9)

        sim.update_multi({
            "BTC-USDT-SWAP": Candle(
                ts=1, open=70.0, high=75.0, low=65.0, close=70.0, volume=1.0
            )
        })

        assert sim.closed_positions[0].close_reason == "liquidation"
        assert sim.closed_positions[0].close_price == pytest.approx(70.0)

    def test_liquidation_charges_extra_fee(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={
                "guardrails": {"max_leverage": 10.0},
                "instrument_specs": {
                    "BTC-USDT-SWAP": {
                        "ct_val": 1.0,
                        "position_tiers": [{
                            "min_size": 0.0,
                            "max_size": 10.0,
                            "initial_margin_ratio": 0.35,
                            "maintenance_margin_ratio": 0.1,
                            "max_leverage": 3.0,
                        }],
                    }
                },
            },
            cost_model=CostModel(
                slippage_mode="fixed",
                taker_fee_bps=5.0,
                liquidation_fee_bps=125.0,
            ),
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="liq-fee",
        )
        sim.update_multi({
            "BTC-USDT-SWAP": Candle(
                ts=1, open=70.0, high=75.0, low=65.0, close=70.0, volume=1.0
            )
        })

        trade = sim.closed_positions[0]
        assert trade.close_reason == "liquidation"
        # exit fee = taker (5 bps) + liquidation (125 bps) on the exit notional.
        expected = trade.size * trade.close_price * (5.0 + 125.0) / 10_000.0
        assert trade.exit_fee == pytest.approx(expected)

    def test_slippage_stress_multiplier_applies_to_fixed_mode(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_bps=10.0,
                slippage_mode="fixed",
                slippage_stress_multiplier=3.0,
            ),
        )
        position = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="stress",
        )
        assert position is not None
        # 10 bps × 3 = 30 bps → entry fill 100 * 1.003
        assert position.entry_price == pytest.approx(100.3)


class TestCrossMargin:
    def test_cross_position_has_no_per_position_liquidation_price(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={"guardrails": {"max_leverage": 5.0}},
            cost_model=CostModel(slippage_mode="fixed"),
            margin_mode="cross",
        )
        position = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="cross",
        )
        assert position is not None
        assert position.margin_mode == "cross"
        # Cross margin liquidates at the account level, not per position.
        assert position.liquidation_price is None

    def test_cross_margin_shares_equity_across_positions(self) -> None:
        # Isolated would reserve 100 margin for the first position and block
        # the second; cross margin shares equity so both can open.
        sim = Simulator(
            initial_capital=100.0,
            notional_per_trade=100.0,
            strategy_config={"guardrails": {"max_leverage": 5.0}},
            cost_model=CostModel(slippage_mode="fixed"),
            margin_mode="cross",
        )
        first = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="first",
        )
        second = sim.open_position(
            symbol="ETH-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=1,
            strategy_name="second",
        )
        assert first is not None
        assert second is not None
        assert len(sim.open_positions) == 2

    def test_cross_margin_account_liquidation_closes_all_positions(self) -> None:
        sim = Simulator(
            initial_capital=100.0,
            notional_per_trade=100.0,
            strategy_config={
                "guardrails": {"max_leverage": 5.0},
                "instrument_specs": {
                    "BTC-USDT-SWAP": {
                        "ct_val": 1.0,
                        "position_tiers": [{
                            "min_size": 0.0,
                            "max_size": 100.0,
                            "initial_margin_ratio": 0.2,
                            "maintenance_margin_ratio": 0.1,
                            "max_leverage": 5.0,
                        }],
                    },
                    "ETH-USDT-SWAP": {
                        "ct_val": 1.0,
                        "position_tiers": [{
                            "min_size": 0.0,
                            "max_size": 100.0,
                            "initial_margin_ratio": 0.2,
                            "maintenance_margin_ratio": 0.1,
                            "max_leverage": 5.0,
                        }],
                    },
                },
            },
            cost_model=CostModel(slippage_mode="fixed"),
            margin_mode="cross",
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP", direction="long",
            entry_price=100.0, entry_ts=0, strategy_name="a",
        )
        sim.open_position(
            symbol="ETH-USDT-SWAP", direction="long",
            entry_price=100.0, entry_ts=0, strategy_name="b",
        )
        assert len(sim.open_positions) == 2

        # A large adverse move pushes account equity below the aggregate
        # maintenance requirement → both positions liquidate together.
        sim.update_multi({
            "BTC-USDT-SWAP": Candle(ts=1, open=60, high=60, low=55, close=55, volume=1),
            "ETH-USDT-SWAP": Candle(ts=1, open=60, high=60, low=55, close=55, volume=1),
        })

        assert len(sim.open_positions) == 0
        assert all(t.close_reason == "liquidation" for t in sim.closed_positions)
        assert len(sim.closed_positions) == 2

    def test_cross_margin_healthy_account_is_not_liquidated(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={"guardrails": {"max_leverage": 5.0}},
            cost_model=CostModel(slippage_mode="fixed"),
            margin_mode="cross",
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP", direction="long",
            entry_price=100.0, entry_ts=0, strategy_name="a",
        )
        sim.update_multi({
            "BTC-USDT-SWAP": Candle(ts=1, open=100, high=101, low=99, close=100, volume=1),
        })
        assert len(sim.open_positions) == 1
        assert sim.closed_positions == []

    def test_maintenance_requirement_uses_tier_mmr(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={
                "guardrails": {"max_leverage": 5.0},
                "instrument_specs": {
                    "BTC-USDT-SWAP": {
                        "ct_val": 1.0,
                        "position_tiers": [{
                            "min_size": 0.0,
                            "max_size": 100.0,
                            "initial_margin_ratio": 0.2,
                            "maintenance_margin_ratio": 0.1,
                            "max_leverage": 5.0,
                        }],
                    },
                },
            },
            cost_model=CostModel(slippage_mode="fixed"),
            margin_mode="cross",
        )
        position = sim.open_position(
            symbol="BTC-USDT-SWAP", direction="long",
            entry_price=100.0, entry_ts=0, strategy_name="a",
        )
        assert position is not None
        requirement = sim.maintenance_margin_requirement({"BTC-USDT-SWAP": 100.0})
        assert requirement == pytest.approx(position.size * 100.0 * 0.1)

    def test_initial_margin_reservation_caps_following_positions(self) -> None:
        sim = Simulator(
            initial_capital=100.0,
            notional_per_trade=500.0,
            strategy_config={"guardrails": {"max_leverage": 5.0}},
            cost_model=CostModel(slippage_mode="fixed"),
        )
        first = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="first",
        )

        assert first is not None
        assert first.leverage == pytest.approx(5.0)
        assert first.initial_margin == pytest.approx(100.0)
        assert sim.open_position(
            symbol="ETH-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=1,
            strategy_name="second",
        ) is None

    def test_simulator_uses_historical_funding_events_not_elapsed_bar_count(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                funding_mode="historical",
                funding_rate_pct=0.0,
                historical_funding_rates={
                    "BTC-USDT-SWAP": [
                        {"ts": 50, "rate": 0.001},
                        {"ts": 100, "rate": -0.0005},
                    ]
                },
            ),
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=50,
            strategy_name="funding-test",
        )
        sim.close_all_at_market({"BTC-USDT-SWAP": 100.0}, ts=100)

        trade = sim.closed_positions[0]
        assert trade.funding == pytest.approx(-0.05)
        assert sim.cash == pytest.approx(1000.05)

    def test_historical_funding_updates_equity_at_settlement_timestamp(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                funding_mode="historical",
                historical_funding_rates={"BTC-USDT-SWAP": [{"ts": 10, "rate": 0.001}]},
            ),
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="funding-test",
        )
        sim.update_multi({
            "BTC-USDT-SWAP": Candle(ts=10, open=100, high=100.1, low=99.9, close=100, volume=1)
        })

        assert sim.open_positions
        assert sim.open_positions[0].funding == pytest.approx(0.1)
        assert sim.cash == pytest.approx(999.9)

        sim.close_all_at_market({"BTC-USDT-SWAP": 100.0}, ts=20)
        assert sim.closed_positions[0].funding == pytest.approx(0.1)
        assert sim.cash == pytest.approx(999.9)

    def test_fees_reduce_cash_on_round_trip(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(taker_fee_bps=5.0),
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            tp_price=110.0,
            sl_price=90.0,
            strategy_name="test",
        )
        # Entry fee = 100 * 0.05% = 0.05
        assert sim.cash == pytest.approx(999.95)
        # Close at TP 110: PnL = +10, exit fee = 110 * 0.05% = 0.055
        candle = Candle(ts=1, open=105, high=111, low=104, close=110, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})
        trade = sim.closed_positions[0]
        assert trade.pnl == pytest.approx(10.0)
        assert trade.exit_fee == pytest.approx(0.055, abs=1e-3)
        # cash = 999.95 - 0.055 (entry fee already deducted) + 10 (pnl)
        assert sim.cash == pytest.approx(1009.895, abs=1e-3)

    def test_net_pnl_accounts_for_fees(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(taker_fee_bps=5.0),
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            tp_price=110.0,
            sl_price=90.0,
            strategy_name="test",
        )
        candle = Candle(ts=1, open=105, high=111, low=104, close=110, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})
        trade = sim.closed_positions[0]
        # gross pnl 10, fees ~0.105 → net slightly below 10
        assert trade.net_pnl < trade.pnl
        assert trade.net_pnl == pytest.approx(9.895, abs=1e-3)

    def test_slippage_baked_into_pnl(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(slippage_bps=100.0),  # 1% slippage
        )
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            tp_price=110.0,
            sl_price=80.0,
            strategy_name="test",
        )
        # entry fill = 100 * 1.01 = 101
        trade = sim.open_positions[0]
        assert trade.entry_price == pytest.approx(101.0)
        assert trade.slippage_cost == pytest.approx(1.0 * trade.size)

    def test_partial_close_allocates_entry_fee_slippage_and_funding(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                taker_fee_bps=5.0,
                slippage_bps=100.0,
                funding_rate_pct=0.01,
                funding_interval_ms=8,
            ),
        )
        position = sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=100.0,
            entry_ts=0,
            strategy_name="test",
        )
        assert position is not None

        sim._close_position(position, 110.0, 8, "partial", size_fraction=0.5)
        sim._close_position(position, 110.0, 16, "final")
        trades = sim.closed_positions

        assert len(trades) == 2
        assert all(trade.entry_fee > 0 for trade in trades)
        assert all(trade.slippage_cost > 0 for trade in trades)
        assert all(trade.funding > 0 for trade in trades)
        assert sum(trade.net_pnl for trade in trades) == pytest.approx(sim.cash - 1000.0)
        metrics = compute_metrics(trades, [], 1000.0)
        assert metrics["total_trades"] == 1
        assert metrics["closed_trade_legs"] == 2
        assert metrics["net_expectancy_standard_error"] is None

    def test_ohlcv_liquidity_slippage_rises_as_turnover_falls_and_is_capped(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            strategy_config={
                "instrument_specs": {
                    "BTC-USDT-SWAP": {"ct_val": 1.0},
                    "THIN-USDT-SWAP": {"ct_val": 1.0},
                }
            },
            cost_model=CostModel(
                slippage_mode="ohlcv_liquidity",
                liquidity_impact_coefficient=0.05,
                candle_range_slippage_fraction=0.1,
                max_liquidity_slippage_bps=80.0,
            ),
        )
        candles = {
            "BTC-USDT-SWAP": Candle(ts=1, open=100, high=101, low=99, close=100, volume=100_000),
            "THIN-USDT-SWAP": Candle(ts=1, open=100, high=101, low=99, close=100, volume=1),
        }
        sim.update_multi(candles)

        liquid_bps = sim._slippage_bps("BTC-USDT-SWAP", 100.0)
        thin_bps = sim._slippage_bps("THIN-USDT-SWAP", 100.0)

        assert thin_bps > liquid_bps
        assert thin_bps == pytest.approx(80.0)

    def test_liquidity_slippage_excludes_execution_candle_and_fixed_mode_is_exact(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_bps=4.0,
                slippage_mode="ohlcv_liquidity",
                liquidity_impact_coefficient=0.0,
                candle_range_slippage_fraction=0.1,
            ),
        )
        sim._recent_candles["BTC-USDT-SWAP"] = [
            Candle(ts=1, open=100, high=101, low=99, close=100, volume=1000),
            Candle(ts=2, open=100, high=200, low=1, close=100, volume=1_000_000),
        ]

        prior_only_bps = sim._slippage_bps("BTC-USDT-SWAP", 10.0, execution_ts=2)
        fixed = CostModel(slippage_bps=4.0, slippage_mode="fixed")

        assert prior_only_bps == pytest.approx(24.0)
        assert fixed.entry_price_for(100.0, True, slippage_bps=4.0) == pytest.approx(100.04)
