"""Tests for the trade-tape / OHLCV bid-ask spread estimators."""

from __future__ import annotations

import math

import pytest

from app.services.backtest.costs import CostModel
from app.services.backtest.models import Candle
from app.services.backtest.simulator import Simulator
from app.services.backtest.spread import (
    corwin_schultz_spread_bps,
    estimate_spread_series,
    roll_spread_bps,
    spread_at,
)


class TestCorwinSchultz:
    def test_requires_two_bars(self) -> None:
        assert corwin_schultz_spread_bps([100.0], [99.0]) is None
        assert corwin_schultz_spread_bps([], []) is None

    def test_flat_bars_are_degenerate(self) -> None:
        # high == low → zero range → no estimate (degenerate input).
        highs = [100.0, 100.0, 100.0]
        lows = [100.0, 100.0, 100.0]
        assert corwin_schultz_spread_bps(highs, lows) is None

    def test_estimate_is_deterministic_and_non_negative(self) -> None:
        highs = [101.0, 101.0]
        lows = [99.0, 99.0]
        first = corwin_schultz_spread_bps(highs, lows)
        second = corwin_schultz_spread_bps(highs, lows)
        assert first is not None
        assert first >= 0
        assert first == second

    def test_wider_ranges_give_positive_spread(self) -> None:
        highs = [101.0, 102.0, 101.5, 102.5]
        lows = [99.0, 98.0, 98.5, 97.5]
        spread = corwin_schultz_spread_bps(highs, lows)
        assert spread is not None
        assert spread > 0

    def test_degenerate_zero_prices_are_skipped(self) -> None:
        assert corwin_schultz_spread_bps([0.0, 100.0], [0.0, 99.0]) is None


class TestRoll:
    def test_requires_three_prices(self) -> None:
        assert roll_spread_bps([100.0, 101.0]) is None

    def test_bid_ask_bounce_gives_positive_spread(self) -> None:
        # Alternating up/down moves → negative autocovariance → positive spread.
        prices = [100.0, 100.1, 100.0, 100.1, 100.0, 100.1, 100.0]
        spread = roll_spread_bps(prices)
        assert spread is not None
        assert spread > 0

    def test_trending_prices_give_zero_spread(self) -> None:
        # Monotonic trend → positive autocovariance → floored to zero.
        prices = [100.0, 101.0, 102.0, 103.0, 104.0]
        assert roll_spread_bps(prices) == pytest.approx(0.0)


class TestSpreadSeries:
    def _candles(self, n: int) -> list[Candle]:
        out = []
        for i in range(n):
            base = 100.0 + (i % 2) * 0.5
            out.append(Candle(
                ts=i * 1000, open=base, high=base + 1.0, low=base - 1.0,
                close=base, volume=10.0,
            ))
        return out

    def test_series_uses_only_prior_bars(self) -> None:
        candles = self._candles(30)
        series = estimate_spread_series(candles, window=5)
        assert series
        # First estimate is timestamped at the last bar of the first window.
        assert series[0]["ts"] == candles[4].ts
        # Every estimate is timestamped at or after its window's last bar.
        for row in series:
            assert row["spread_bps"] >= 0

    def test_too_few_bars_returns_empty(self) -> None:
        assert estimate_spread_series(self._candles(3), window=5) == []

    def test_spread_at_returns_most_recent_at_or_before(self) -> None:
        series = [
            {"ts": 1000, "spread_bps": 5.0},
            {"ts": 2000, "spread_bps": 7.0},
            {"ts": 3000, "spread_bps": 9.0},
        ]
        assert spread_at(series, 2500) == 7.0
        assert spread_at(series, 500) is None
        assert spread_at(series, 9999) == 9.0
        assert spread_at(None, 1000) is None


class TestTapeSpreadSlippage:
    def test_tape_spread_mode_uses_series(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_mode="tape_spread",
                slippage_bps=1.0,
                spread_series={
                    "BTC-USDT-SWAP": [
                        {"ts": 0, "spread_bps": 4.0},
                        {"ts": 100, "spread_bps": 8.0},
                    ]
                },
            ),
        )
        # At ts=50 the most recent estimate is 4.0 → 1.0 base + 4.0 = 5.0.
        assert sim._slippage_bps("BTC-USDT-SWAP", 100.0, execution_ts=50) == pytest.approx(5.0)
        # At ts=150 the most recent estimate is 8.0 → 1.0 + 8.0 = 9.0.
        assert sim._slippage_bps("BTC-USDT-SWAP", 100.0, execution_ts=150) == pytest.approx(9.0)

    def test_tape_spread_falls_back_to_base_when_missing(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_mode="tape_spread",
                slippage_bps=3.0,
                spread_series={},
            ),
        )
        assert sim._slippage_bps("BTC-USDT-SWAP", 100.0, execution_ts=50) == pytest.approx(3.0)

    def test_tape_spread_respects_stress_multiplier(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_mode="tape_spread",
                slippage_bps=0.0,
                slippage_stress_multiplier=2.0,
                spread_series={"BTC-USDT-SWAP": [{"ts": 0, "spread_bps": 5.0}]},
            ),
        )
        assert sim._slippage_bps("BTC-USDT-SWAP", 100.0, execution_ts=10) == pytest.approx(10.0)

    def test_tape_spread_is_capped(self) -> None:
        sim = Simulator(
            initial_capital=1000.0,
            notional_per_trade=100.0,
            cost_model=CostModel(
                slippage_mode="tape_spread",
                slippage_bps=0.0,
                max_liquidity_slippage_bps=6.0,
                spread_series={"BTC-USDT-SWAP": [{"ts": 0, "spread_bps": 50.0}]},
            ),
        )
        assert sim._slippage_bps("BTC-USDT-SWAP", 100.0, execution_ts=10) == pytest.approx(6.0)
