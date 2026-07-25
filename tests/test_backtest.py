"""Tests for the backtesting engine.

Covers:
  - Simulator TP/SL close logic (long & short, TP hit, SL hit, both hit)
  - Simulator equity tracking and position lifecycle
  - Metrics computation (net profit, win rate, profit factor, drawdown)
  - Per-strategy metrics breakdown
  - SnapshotBuilder produces the expected dict shape
  - DataFetcher cache key generation and HTF mapping
  - Engine strategy registry and TP/SL price computation
"""

from __future__ import annotations

import pytest

from app.services.backtest.data_fetcher import htf_for, _timeframe_to_ms
from app.services.backtest.engine import available_strategy_names, _extract_float
from app.services.backtest.metrics import compute_metrics, compute_per_strategy_metrics
from app.services.backtest.models import Candle, EquityPoint, SimPosition
from app.services.backtest.simulator import Simulator
from app.services.backtest.snapshot_builder import SnapshotBuilder


# ── Simulator tests ──────────────────────────────────────────────────────────


class TestSimulatorTPSL:
    """Test the simulated broker's TP/SL close logic."""

    def test_long_position_hits_tp(self) -> None:
        """A long position should close at tp_price when candle high reaches it."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        # Candle with high >= 51000 (TP hit)
        candle = Candle(ts=2000, open=50500, high=51100, low=50400, close=51050, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        assert len(sim.open_positions) == 0
        assert len(sim.closed_positions) == 1
        trade = sim.closed_positions[0]
        assert trade.close_reason == "tp"
        assert trade.close_price == 51000.0
        assert trade.pnl > 0
        # PnL = (51000 - 50000) * (100/50000) = 2.0
        assert trade.pnl == pytest.approx(2.0)
        assert trade.pnl_pct == pytest.approx(2.0)

    def test_long_position_hits_sl(self) -> None:
        """A long position should close at sl_price when candle low reaches it."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        # Candle with low <= 49500 (SL hit)
        candle = Candle(ts=2000, open=49900, high=49950, low=49400, close=49450, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        assert len(sim.open_positions) == 0
        trade = sim.closed_positions[0]
        assert trade.close_reason == "sl"
        assert trade.close_price == 49500.0
        assert trade.pnl < 0
        # PnL = (49500 - 50000) * (100/50000) = -1.0
        assert trade.pnl == pytest.approx(-1.0)

    def test_short_position_hits_tp(self) -> None:
        """A short position should close at tp_price when candle low reaches it."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="short",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=49000.0,
            sl_price=50500.0,
            strategy_name="test",
        )
        # Candle with low <= 49000 (TP hit for short)
        candle = Candle(ts=2000, open=49500, high=49600, low=48900, close=49050, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        assert len(sim.open_positions) == 0
        trade = sim.closed_positions[0]
        assert trade.close_reason == "tp"
        assert trade.close_price == 49000.0
        assert trade.pnl > 0
        # PnL = (50000 - 49000) * (100/50000) = 2.0
        assert trade.pnl == pytest.approx(2.0)

    def test_short_position_hits_sl(self) -> None:
        """A short position should close at sl_price when candle high reaches it."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="short",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=49000.0,
            sl_price=50500.0,
            strategy_name="test",
        )
        # Candle with high >= 50500 (SL hit for short)
        candle = Candle(ts=2000, open=50100, high=50600, low=50050, close=50550, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        assert len(sim.open_positions) == 0
        trade = sim.closed_positions[0]
        assert trade.close_reason == "sl"
        assert trade.close_price == 50500.0
        assert trade.pnl < 0
        # PnL = (50000 - 50500) * (100/50000) = -1.0
        assert trade.pnl == pytest.approx(-1.0)

    def test_both_tp_sl_hit_sl_wins(self) -> None:
        """When both TP and SL are within the candle range, SL should win (pessimistic)."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        # Candle where both TP (high >= 51000) and SL (low <= 49500) are hit
        candle = Candle(ts=2000, open=50000, high=51200, low=49400, close=50000, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        trade = sim.closed_positions[0]
        assert trade.close_reason == "sl"
        assert trade.close_price == 49500.0

    def test_position_stays_open_when_no_hit(self) -> None:
        """Position should remain open when neither TP nor SL is hit."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        candle = Candle(ts=2000, open=50100, high=50300, low=49900, close=50200, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})

        assert len(sim.open_positions) == 1
        assert len(sim.closed_positions) == 0

    def test_close_all_at_market(self) -> None:
        """Remaining open positions should close at market price at end of data."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        sim.close_all_at_market({"BTC-USDT-SWAP": 50500.0}, ts=2000)

        assert len(sim.open_positions) == 0
        trade = sim.closed_positions[0]
        assert trade.close_reason == "end_of_data"
        assert trade.close_price == 50500.0

    def test_has_open_position(self) -> None:
        """has_open_position should detect existing positions per symbol/strategy."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            strategy_name="mean_reversion",
        )
        assert sim.has_open_position("BTC-USDT-SWAP")
        assert sim.has_open_position("BTC-USDT-SWAP", "mean_reversion")
        assert not sim.has_open_position("BTC-USDT-SWAP", "spike_continuation")
        assert not sim.has_open_position("ETH-USDT-SWAP")

    def test_equity_tracking(self) -> None:
        """Equity should be cash + unrealised PnL of open positions."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        # After opening, equity = 1000 (cash unchanged, no realised PnL yet)
        eq = sim.equity({"BTC-USDT-SWAP": 50000.0})
        assert eq == pytest.approx(1000.0)
        # Price moves up 2% → unrealised PnL = +2.0
        eq_up = sim.equity({"BTC-USDT-SWAP": 51000.0})
        assert eq_up == pytest.approx(1002.0)
        # Price moves down 1% → unrealised PnL = -1.0
        eq_down = sim.equity({"BTC-USDT-SWAP": 49500.0})
        assert eq_down == pytest.approx(999.0)

    def test_cash_updates_on_close(self) -> None:
        """Cash should increase/decrease by realised PnL when a position closes."""
        sim = Simulator(initial_capital=1000.0, notional_per_trade=100.0)
        sim.open_position(
            symbol="BTC-USDT-SWAP",
            direction="long",
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name="test",
        )
        candle = Candle(ts=2000, open=50500, high=51100, low=50400, close=51050, volume=1.0)
        sim.update_multi({"BTC-USDT-SWAP": candle})
        # PnL = +2.0, so cash should be 1002.0
        assert sim.cash == pytest.approx(1002.0)


# ── Metrics tests ────────────────────────────────────────────────────────────


class TestMetrics:
    """Test performance metrics computation."""

    def _make_trade(
        self,
        pnl: float = 2.0,
        pnl_pct: float = 2.0,
        strategy_name: str = "test",
    ) -> SimPosition:
        return SimPosition(
            symbol="BTC-USDT-SWAP",
            direction="long",
            size=0.002,
            entry_price=50000.0,
            entry_ts=1000,
            tp_price=51000.0,
            sl_price=49500.0,
            strategy_name=strategy_name,
            close_price=51000.0,
            close_ts=2000,
            close_reason="tp",
            pnl=pnl,
            pnl_pct=pnl_pct,
        )

    def test_empty_metrics(self) -> None:
        """Metrics with no trades should return zeros."""
        m = compute_metrics([], [], 1000.0)
        assert m["total_trades"] == 0
        assert m["win_rate"] == 0.0
        assert m["net_profit"] == 0.0

    def test_all_wins(self) -> None:
        """Metrics with only winning trades."""
        trades = [self._make_trade(pnl=2.0), self._make_trade(pnl=3.0)]
        m = compute_metrics(trades, [], 1000.0)
        assert m["total_trades"] == 2
        assert m["winning_trades"] == 2
        assert m["losing_trades"] == 0
        assert m["win_rate"] == 100.0
        assert m["net_profit"] == 5.0
        assert m["profit_factor"] == float("inf")

    def test_all_losses(self) -> None:
        """Metrics with only losing trades."""
        trades = [self._make_trade(pnl=-1.0), self._make_trade(pnl=-2.0)]
        m = compute_metrics(trades, [], 1000.0)
        assert m["total_trades"] == 2
        assert m["winning_trades"] == 0
        assert m["losing_trades"] == 2
        assert m["win_rate"] == 0.0
        assert m["net_profit"] == -3.0
        assert m["profit_factor"] == 0.0

    def test_mixed_trades(self) -> None:
        """Metrics with a mix of wins and losses."""
        trades = [
            self._make_trade(pnl=3.0, strategy_name="mr"),
            self._make_trade(pnl=-1.0, strategy_name="sc"),
            self._make_trade(pnl=2.0, strategy_name="mr"),
        ]
        m = compute_metrics(trades, [], 1000.0)
        assert m["total_trades"] == 3
        assert m["winning_trades"] == 2
        assert m["losing_trades"] == 1
        assert m["win_rate"] == pytest.approx(66.67, abs=0.1)
        assert m["net_profit"] == 4.0
        assert m["gross_profit"] == 5.0
        assert m["gross_loss"] == 1.0
        assert m["profit_factor"] == 5.0

    def test_equity_curve_metrics(self) -> None:
        """Max drawdown and total return from equity curve."""
        curve = [
            EquityPoint(ts=1000, equity=1000.0, open_positions=0),
            EquityPoint(ts=2000, equity=1100.0, open_positions=1),
            EquityPoint(ts=3000, equity=1050.0, open_positions=1),
            EquityPoint(ts=4000, equity=1080.0, open_positions=0),
        ]
        m = compute_metrics([], curve, 1000.0)
        assert m["final_equity"] == 1080.0
        assert m["total_return_pct"] == pytest.approx(8.0, abs=0.1)
        # Peak = 1100, trough = 1050 → DD = 50, DD% = 50/1100 ≈ 4.55%
        assert m["max_drawdown"] == 50.0
        assert m["max_drawdown_pct"] == pytest.approx(4.55, abs=0.1)

    def test_per_strategy_breakdown(self) -> None:
        """Per-strategy metrics should group by strategy name."""
        trades = [
            self._make_trade(pnl=2.0, strategy_name="mean_reversion"),
            self._make_trade(pnl=-1.0, strategy_name="mean_reversion"),
            self._make_trade(pnl=3.0, strategy_name="spike_continuation"),
        ]
        result = compute_per_strategy_metrics(trades)
        assert "mean_reversion" in result
        assert "spike_continuation" in result
        assert result["mean_reversion"]["trades"] == 2
        assert result["spike_continuation"]["trades"] == 1
        assert result["mean_reversion"]["net_profit"] == 1.0
        assert result["spike_continuation"]["net_profit"] == 3.0


# ── SnapshotBuilder tests ────────────────────────────────────────────────────


class TestSnapshotBuilder:
    """Test synthetic snapshot construction from historical candles."""

    def _make_candles(self, n: int, base_price: float = 100.0) -> list[Candle]:
        """Create n synthetic candles with a simple price pattern."""
        candles = []
        for i in range(n):
            ts = 1000 + i * 3600_000  # 1H candles
            price = base_price + i * 0.1
            candles.append(Candle(
                ts=ts, open=price, high=price + 0.5, low=price - 0.5,
                close=price, volume=1000.0,
            ))
        return candles

    def test_build_returns_expected_shape(self) -> None:
        """Snapshot should contain market_data[symbol].indicators."""
        candles = self._make_candles(250)
        builder = SnapshotBuilder("BTC-USDT-SWAP", candles, None, "4H")
        snapshot = builder.build(window_end_idx=249)

        assert "market_data" in snapshot
        assert "BTC-USDT-SWAP" in snapshot["market_data"]
        sym_data = snapshot["market_data"]["BTC-USDT-SWAP"]
        assert "indicators" in sym_data
        indicators = sym_data["indicators"]
        # Key indicators should be present
        assert "rsi" in indicators
        assert "bollinger_bands" in indicators
        assert "adx" in indicators
        assert "structure" in indicators

    def test_build_last_price(self) -> None:
        """Snapshot last_price should match the current candle's close."""
        candles = self._make_candles(250)
        builder = SnapshotBuilder("BTC-USDT-SWAP", candles, None, "4H")
        snapshot = builder.build(window_end_idx=249)
        assert snapshot["last_price"] == candles[249].close

    def test_build_with_htf(self) -> None:
        """Snapshot should include HTF indicators when HTF candles are provided."""
        ltf_candles = self._make_candles(250)
        htf_candles = self._make_candles(100, base_price=100.0)
        # Adjust HTF timestamps to be before LTF timestamps
        htf_candles = [
            Candle(ts=c.ts // 4, open=c.open, high=c.high, low=c.low, close=c.close, volume=c.volume)
            for c in htf_candles
        ]
        builder = SnapshotBuilder("BTC-USDT-SWAP", ltf_candles, htf_candles, "4H")
        snapshot = builder.build(window_end_idx=249)
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        assert "htf_indicators" in indicators
        assert "ohlcv_htf" in indicators
        assert indicators["ohlcv_htf_bar"] == "1D"


# ── DataFetcher helper tests ──────────────────────────────────────────────────


class TestDataFetcherHelpers:
    """Test data fetcher utility functions."""

    def test_htf_for_4h(self) -> None:
        assert htf_for("4H") == "1D"

    def test_htf_for_1h(self) -> None:
        assert htf_for("1H") == "4H"

    def test_htf_for_15m(self) -> None:
        assert htf_for("15m") == "1H"

    def test_htf_for_1d_no_htf(self) -> None:
        assert htf_for("1D") == ""

    def test_timeframe_to_ms_minutes(self) -> None:
        assert _timeframe_to_ms("15m") == 900_000

    def test_timeframe_to_ms_hours(self) -> None:
        assert _timeframe_to_ms("4H") == 14_400_000

    def test_timeframe_to_ms_days(self) -> None:
        assert _timeframe_to_ms("1D") == 86_400_000


# ── Engine helper tests ───────────────────────────────────────────────────────


class TestEngineHelpers:
    """Test engine utility functions."""

    def test_available_strategy_names(self) -> None:
        names = available_strategy_names()
        assert "mean_reversion" in names
        assert "spike_continuation" in names

    def test_extract_float_valid(self) -> None:
        assert _extract_float(3.14) == 3.14
        assert _extract_float("2.5") == 2.5
        assert _extract_float(10) == 10.0

    def test_extract_float_none(self) -> None:
        assert _extract_float(None) is None
        assert _extract_float("") is None

    def test_extract_float_invalid(self) -> None:
        assert _extract_float("abc") is None
        assert _extract_float([]) is None


# ── Model tests ──────────────────────────────────────────────────────────────


class TestModels:
    """Test data model behaviour."""

    def test_candle_dt(self) -> None:
        """Candle.dt should return a UTC datetime."""
        c = Candle(ts=1609459200000, open=100, high=101, low=99, close=100.5, volume=1000)
        # 1609459200000 ms = 2021-01-01 00:00:00 UTC
        assert c.dt.year == 2021
        assert c.dt.month == 1
        assert c.dt.day == 1

    def test_sim_position_unrealised_pnl_long(self) -> None:
        pos = SimPosition(
            symbol="BTC-USDT-SWAP", direction="long", size=0.1,
            entry_price=50000.0, entry_ts=1000,
        )
        # Price up 1000 → PnL = 1000 * 0.1 = 100
        assert pos.unrealised_pnl(51000.0) == 100.0
        assert pos.unrealised_pnl_pct(51000.0) == 2.0

    def test_sim_position_unrealised_pnl_short(self) -> None:
        pos = SimPosition(
            symbol="BTC-USDT-SWAP", direction="short", size=0.1,
            entry_price=50000.0, entry_ts=1000,
        )
        # Price down 1000 → PnL = 1000 * 0.1 = 100 (profit for short)
        assert pos.unrealised_pnl(49000.0) == 100.0
        assert pos.unrealised_pnl_pct(49000.0) == 2.0

    def test_sim_position_is_open(self) -> None:
        pos = SimPosition(
            symbol="BTC-USDT-SWAP", direction="long", size=0.1,
            entry_price=50000.0, entry_ts=1000,
        )
        assert pos.is_open is True
        pos.close_price = 51000.0
        assert pos.is_open is False


# ── Finer-LTF evaluation tests ───────────────────────────────────────────────


class TestFinerLtfHelpers:
    """Test the finer-LTF helper functions in data_fetcher."""

    def test_timeframe_ms_public_alias(self) -> None:
        """Public timeframe_ms should match _timeframe_to_ms."""
        from app.services.backtest.data_fetcher import timeframe_ms
        assert timeframe_ms("1m") == 60_000
        assert timeframe_ms("15m") == 900_000
        assert timeframe_ms("1H") == 3_600_000
        assert timeframe_ms("4H") == 14_400_000

    def test_ltf_bucket_ts_aligned(self) -> None:
        """ltf_bucket_ts should round down to the LTF period boundary."""
        from app.services.backtest.data_fetcher import ltf_bucket_ts
        # 15m bucket: 00:15:00 → 00:15:00 (already aligned)
        assert ltf_bucket_ts(0, "15m") == 0
        # 00:07:30 (450_000 ms) → 00:00:00
        assert ltf_bucket_ts(450_000, "15m") == 0
        # 00:17:30 (1_050_000 ms) → 00:15:00 (900_000 ms)
        assert ltf_bucket_ts(1_050_000, "15m") == 900_000
        # 01:00:00 (3_600_000 ms) → 01:00:00
        assert ltf_bucket_ts(3_600_000, "1H") == 3_600_000

    def test_is_finer_than(self) -> None:
        """is_finer_than should compare timeframe durations."""
        from app.services.backtest.data_fetcher import is_finer_than
        assert is_finer_than("1m", "15m") is True
        assert is_finer_than("5m", "15m") is True
        assert is_finer_than("15m", "1H") is True
        assert is_finer_than("15m", "15m") is False
        assert is_finer_than("1H", "15m") is False


class TestSnapshotBuilderIncompleteLtf:
    """Test SnapshotBuilder.build_with_incomplete_ltf."""

    def _make_candles(self, n: int, base_price: float = 100.0, tf_ms: int = 3_600_000) -> list[Candle]:
        candles = []
        for i in range(n):
            ts = 1000 + i * tf_ms
            price = base_price + i * 0.1
            candles.append(Candle(
                ts=ts, open=price, high=price + 0.5, low=price - 0.5,
                close=price, volume=1000.0,
            ))
        return candles

    def test_build_with_incomplete_returns_expected_shape(self) -> None:
        """Snapshot should contain market_data[symbol].indicators."""
        closed = self._make_candles(250)
        incomplete = Candle(ts=closed[-1].ts + 3_600_000, open=125.0, high=126.0,
                           low=124.0, close=125.5, volume=500.0)
        builder = SnapshotBuilder("BTC-USDT-SWAP", closed, None, "4H")
        snapshot = builder.build_with_incomplete_ltf(
            closed_ltf_window=closed,
            incomplete_candle=incomplete,
            current_ts=incomplete.ts,
        )
        assert "market_data" in snapshot
        assert "BTC-USDT-SWAP" in snapshot["market_data"]
        indicators = snapshot["market_data"]["BTC-USDT-SWAP"]["indicators"]
        assert "rsi" in indicators
        assert "bollinger_bands" in indicators
        assert "adx" in indicators
        assert "structure" in indicators

    def test_build_with_incomplete_last_price_uses_incomplete_close(self) -> None:
        """last_price should be the incomplete candle's close, not the last closed candle."""
        closed = self._make_candles(250)
        incomplete = Candle(ts=closed[-1].ts + 3_600_000, open=125.0, high=126.0,
                           low=124.0, close=125.5, volume=500.0)
        builder = SnapshotBuilder("BTC-USDT-SWAP", closed, None, "4H")
        snapshot = builder.build_with_incomplete_ltf(
            closed_ltf_window=closed,
            incomplete_candle=incomplete,
            current_ts=incomplete.ts,
        )
        assert snapshot["last_price"] == 125.5

    def test_build_with_incomplete_includes_incomplete_in_indicators(self) -> None:
        """The incomplete candle should be part of the indicator window.

        We verify this by checking that the OHLCV compact series in the
        indicators dict has one more entry than the closed window alone.
        """
        closed = self._make_candles(250)
        incomplete = Candle(ts=closed[-1].ts + 3_600_000, open=125.0, high=126.0,
                           low=124.0, close=125.5, volume=500.0)
        builder = SnapshotBuilder("BTC-USDT-SWAP", closed, None, "4H")

        # Build with closed-only (legacy) for comparison.
        snapshot_closed = builder.build(window_end_idx=249)
        ohlcv_closed = snapshot_closed["market_data"]["BTC-USDT-SWAP"]["indicators"]["ohlcv"]

        # Build with incomplete candle appended.
        snapshot_inc = builder.build_with_incomplete_ltf(
            closed_ltf_window=closed,
            incomplete_candle=incomplete,
            current_ts=incomplete.ts,
        )
        ohlcv_inc = snapshot_inc["market_data"]["BTC-USDT-SWAP"]["indicators"]["ohlcv"]

        assert len(ohlcv_inc) == len(ohlcv_closed) + 1
        # The last entry should be the incomplete candle.
        last_row = ohlcv_inc[-1]
        assert last_row["close"] == 125.5
        assert last_row["open"] == 125.0


class TestFinerLtfEngineLoop:
    """Integration test: finer-LTF loop fires signals that closed mode misses."""

    def test_finer_ltf_signal_fires_mid_candle(self) -> None:
        """A signal that fires mid-LTF-candle should be caught in finer_ltf mode.

        We construct a scenario where:
          - 250 warmup candles at a steady price (RSI ~50, no signal).
          - Then a sharp drop on the first eval candle of a new LTF bucket
            that pushes RSI below 30 mid-candle.
          - Then a recovery by the LTF candle close that brings RSI back above 30.

        In finer_ltf mode, the signal should fire on the eval candle where
        RSI < 30.  In closed mode, the LTF candle's close has RSI >= 30, so
        no signal fires.

        This is a regression test for the OPN-USDT divergence.
        """
        from app.services.backtest.engine import BacktestEngine
        from app.services.backtest.models import BacktestConfig

        # Build 1m eval candles: 250 warmup (steady), then a sharp drop,
        # then recovery within the same 15m bucket.
        # 15m bucket = 15 * 1m candles = 900_000 ms.
        tf_ms = 60_000  # 1m
        base_price = 100.0
        eval_candles: list[Candle] = []
        # 250 warmup candles at steady price 100
        for i in range(250):
            ts = i * tf_ms
            eval_candles.append(Candle(
                ts=ts, open=base_price, high=base_price + 0.1,
                low=base_price - 0.1, close=base_price, volume=1000.0,
            ))
        # Now a new 15m bucket starts at ts=250*60_000 = 15_000_000.
        # First eval candle: sharp drop to 90 (RSI should dip below 30).
        eval_candles.append(Candle(
            ts=250 * tf_ms, open=100.0, high=100.0, low=90.0, close=90.0, volume=5000.0,
        ))
        # Remaining 14 eval candles in this 15m bucket: recovery to 99.
        for j in range(1, 15):
            ts = (250 + j) * tf_ms
            eval_candles.append(Candle(
                ts=ts, open=90.0 + j * 0.6, high=91.0 + j * 0.6,
                low=89.0 + j * 0.6, close=90.0 + j * 0.6, volume=1000.0,
            ))

        # Build the corresponding 15m LTF candles.  The warmup 250 1m candles
        # span 250 minutes ≈ 16.67 15m candles, so we need ~17 warmup 15m
        # candles.  We'll build 20 to be safe, all at steady price 100.
        ltf_ms = 900_000  # 15m
        ltf_candles: list[Candle] = []
        for i in range(20):
            ts = i * ltf_ms
            ltf_candles.append(Candle(
                ts=ts, open=base_price, high=base_price + 0.1,
                low=base_price - 0.1, close=base_price, volume=15_000.0,
            ))
        # The 15m bucket starting at ts=15_000_000 (bucket index 16) contains
        # our drop+recovery.  Its closed form: open=100, low=90, close=99.
        ltf_candles.append(Candle(
            ts=16 * ltf_ms, open=100.0, high=100.0, low=90.0, close=99.0, volume=19_000.0,
        ))

        # We need a fake fetcher that returns our candles.  Patch the engine's
        # _fetcher to return the right candles per symbol/timeframe.
        class _FakeFetcher:
            async def fetch_candles(self, *, symbol, timeframe, start_ts, end_ts,
                                    warmup_candles=0, progress_cb=None):
                if timeframe == "15m":
                    return ltf_candles
                return eval_candles

            async def fetch_htf_candles(self, *, symbol, ltf_timeframe, htf_timeframe,
                                        start_ts, end_ts, warmup_candles=0, progress_cb=None):
                return []

        launcher_config = {
            "notional_usd": 10.0,
            "tp_pct": 5.0,
            "sl_pct": 3.0,
            "strategies": {
                "mean_reversion": {
                    "enabled": True,
                    "rsi_oversold": 30.0,
                    "rsi_overbought": 70.0,
                    "require_htf_trend": False,  # no HTF in this test
                    "require_cmf": False,  # simplify: only RSI gate
                    # Disable default-on confirmation filters so the test
                    # isolates the RSI signal without needing BB/candle data.
                    "require_bb_position": False,
                    "require_candle_rejection": False,
                    "require_vwap_reversion": False,
                    "require_volume_cooling": False,
                    "require_regime": False,
                    "use_atr_sizing": False,
                    "min_atr_pct": 0.0,
                    "max_adx": 0.0,
                    "min_bb_bandwidth": 0.0,
                    "max_bb_bandwidth": 0.0,
                    "bb_proximity_pct": 0.0,
                },
                "spike_continuation": {"enabled": False},
                "liquidity_sweep": {"enabled": False},
            },
        }

        # ── Finer-LTF mode ──
        config_finer = BacktestConfig(
            symbols=["BTC-USDT-SWAP"],
            timeframe="15m",
            start_ts=250 * tf_ms,  # start at the drop
            end_ts=(250 + 15) * tf_ms,
            initial_capital=1000.0,
            strategy_names=["mean_reversion"],
            launcher_config=launcher_config,
            strategy_config={},
            warmup_candles=200,
            evaluation_mode="finer_ltf",
            evaluation_timeframe="1m",
        )
        engine_finer = BacktestEngine(config_finer)
        engine_finer._fetcher = _FakeFetcher()
        result_finer = asyncio_run(engine_finer.run())

        # ── Closed mode ──
        config_closed = BacktestConfig(
            symbols=["BTC-USDT-SWAP"],
            timeframe="15m",
            start_ts=16 * ltf_ms,  # start at the 15m bucket with the drop
            end_ts=17 * ltf_ms,
            initial_capital=1000.0,
            strategy_names=["mean_reversion"],
            launcher_config=launcher_config,
            strategy_config={},
            warmup_candles=200,
            evaluation_mode="closed",
            evaluation_timeframe="15m",
        )
        engine_closed = BacktestEngine(config_closed)
        engine_closed._fetcher = _FakeFetcher()
        result_closed = asyncio_run(engine_closed.run())

        # The finer-LTF mode should have opened at least one trade (the
        # signal fired when RSI dipped below 30 on the 1m drop candle).
        # Closed mode should have opened zero trades (RSI recovered by close).
        # NOTE: this is the core regression test — if it fails, the finer-LTF
        # loop is not replicating live intra-candle behaviour.
        assert len(result_finer.trades) >= 1, (
            f"Finer-LTF mode should have opened a trade on the intra-candle "
            f"RSI dip, but got {len(result_finer.trades)} trades. "
            f"Error: {result_finer.error}"
        )
        assert len(result_closed.trades) == 0, (
            f"Closed mode should have opened 0 trades (RSI recovered by close), "
            f"but got {len(result_closed.trades)} trades."
        )

    def test_legacy_closed_mode_unchanged(self) -> None:
        """Closed mode should produce the same results as before the finer-LTF change.

        Runs a simple backtest in closed mode and verifies the engine doesn't
        crash and produces a valid result structure.
        """
        from app.services.backtest.engine import BacktestEngine
        from app.services.backtest.models import BacktestConfig

        # 250 steady candles, no signals expected.
        candles: list[Candle] = []
        for i in range(250):
            ts = i * 3_600_000
            candles.append(Candle(
                ts=ts, open=100.0, high=100.1, low=99.9, close=100.0, volume=1000.0,
            ))

        class _FakeFetcher:
            async def fetch_candles(self, *, symbol, timeframe, start_ts, end_ts,
                                    warmup_candles=0, progress_cb=None):
                return candles

            async def fetch_htf_candles(self, *, symbol, ltf_timeframe, htf_timeframe,
                                        start_ts, end_ts, warmup_candles=0, progress_cb=None):
                return []

        config = BacktestConfig(
            symbols=["BTC-USDT-SWAP"],
            timeframe="1H",
            start_ts=0,
            end_ts=250 * 3_600_000,
            initial_capital=1000.0,
            strategy_names=["mean_reversion"],
            launcher_config={
                "notional_usd": 10.0,
                "tp_pct": 5.0,
                "sl_pct": 3.0,
                "strategies": {
                    "mean_reversion": {"enabled": True, "require_htf_trend": False, "require_cmf": False},
                    "spike_continuation": {"enabled": False},
                },
            },
            strategy_config={},
            warmup_candles=200,
            evaluation_mode="closed",
            evaluation_timeframe="1H",
        )
        engine = BacktestEngine(config)
        engine._fetcher = _FakeFetcher()
        result = asyncio_run(engine.run())

        assert result.error is None, f"Closed-mode backtest failed: {result.error}"
        assert result.candles_processed > 0
        assert len(result.trades) == 0  # steady price → no signals


def asyncio_run(coro):
    """Helper to run an async coroutine in a test (no pytest-asyncio needed)."""
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
