"""Tests for the backtest cost model (fees / slippage / funding)."""

from __future__ import annotations

import pytest

from app.services.backtest.costs import CostModel
from app.services.backtest.models import Candle
from app.services.backtest.simulator import Simulator


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


class TestCostAwareSimulator:
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
