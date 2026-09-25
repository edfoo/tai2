"""Performance metrics for backtest results.

Computes standard trading metrics from the list of closed trades and the
equity curve: total return, Sharpe ratio, max drawdown, win rate, profit
factor, average win/loss, etc.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable

from app.services.backtest.models import EquityPoint, SimPosition


def compute_metrics(
    trades: list[SimPosition],
    equity_curve: list[EquityPoint],
    initial_capital: float,
    *,
    candles_per_year: int = 0,
) -> dict[str, Any]:
    """Compute aggregate performance metrics.

    Parameters
    ----------
    trades:
        List of closed positions.
    equity_curve:
        Equity curve points (ts, equity, open_positions).
    initial_capital:
        Starting capital for return calculations.
    candles_per_year:
        Number of bar periods per year for annualising per-candle Sharpe.
        0 disables annualisation (raw per-candle Sharpe is still emitted).
    """
    metrics: dict[str, Any] = {}

    # ── Trade-level metrics ──────────────────────────────────────────
    position_groups: dict[tuple[str, int], list[SimPosition]] = {}
    for index, trade in enumerate(trades):
        key = ("trade_id", trade.trade_id) if trade.trade_id else ("row", index)
        position_groups.setdefault(key, []).append(trade)
    positions = list(position_groups.values())
    position_gross_pnls = [sum(leg.pnl for leg in legs) for legs in positions]
    net_trade_pnls = [sum(leg.net_pnl for leg in legs) for legs in positions]
    position_entry_notionals = [
        legs[0].entry_price * (
            legs[0].initial_size
            if legs[0].initial_size is not None
            else sum(leg.size for leg in legs)
        )
        for legs in positions
    ]
    total_trades = len(positions)
    pnl_after_slippage = sum(t.pnl for t in trades)
    slippage_cost = sum(t.slippage_cost for t in trades)
    pnl_before_costs = pnl_after_slippage + slippage_cost
    wins = [pnl for pnl in position_gross_pnls if pnl > 0]
    losses = [pnl for pnl in position_gross_pnls if pnl < 0]
    break_even = [pnl for pnl in position_gross_pnls if pnl == 0]

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    net_profit = gross_profit - gross_loss

    metrics["total_trades"] = total_trades
    metrics["closed_trade_legs"] = len(trades)
    metrics["winning_trades"] = len(wins)
    metrics["losing_trades"] = len(losses)
    metrics["break_even_trades"] = len(break_even)
    metrics["win_rate"] = (len(wins) / total_trades * 100.0) if total_trades > 0 else 0.0
    metrics["gross_profit"] = round(gross_profit, 4)
    metrics["gross_loss"] = round(gross_loss, 4)
    metrics["net_profit"] = round(net_profit, 4)
    metrics["net_profit_pct"] = (
        round(net_profit / initial_capital * 100.0, 2) if initial_capital > 0 else 0.0
    )
    metrics["profit_factor"] = (
        round(gross_profit / gross_loss, 4) if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0
    )
    metrics["average_win"] = round(gross_profit / len(wins), 4) if wins else 0.0
    metrics["average_loss"] = round(-gross_loss / len(losses), 4) if losses else 0.0
    metrics["average_trade"] = round(net_profit / total_trades, 4) if total_trades > 0 else 0.0
    metrics["largest_win"] = round(max(wins, default=0.0), 4)
    metrics["largest_loss"] = round(min(losses, default=0.0), 4)

    # ── Trading costs (fees / funding) ───────────────────────────────
    total_fees = sum(t.entry_fee + t.exit_fee for t in trades)
    total_funding = sum(t.funding for t in trades)
    net_profit_after_cost = pnl_after_slippage - total_fees - total_funding
    metrics["total_fees"] = round(total_fees, 4)
    metrics["total_funding"] = round(total_funding, 4)
    metrics["total_slippage_cost"] = round(slippage_cost, 4)
    metrics["total_cost"] = round(total_fees + total_funding + slippage_cost, 4)
    metrics["gross_pnl_before_costs"] = round(pnl_before_costs, 4)
    metrics["pnl_after_slippage_before_fees_funding"] = round(pnl_after_slippage, 4)
    metrics["net_profit_after_cost"] = round(net_profit_after_cost, 4)
    metrics["net_profit_after_cost_pct"] = (
        round(net_profit_after_cost / initial_capital * 100.0, 4)
        if initial_capital > 0 else 0.0
    )
    metrics["net_win_rate_after_cost_pct"] = (
        sum(pnl > 0 for pnl in net_trade_pnls) / total_trades * 100.0
        if total_trades else 0.0
    )
    net_gross_profit = sum(pnl for pnl in net_trade_pnls if pnl > 0)
    net_gross_loss = abs(sum(pnl for pnl in net_trade_pnls if pnl < 0))
    metrics["net_profit_factor_after_cost"] = (
        round(net_gross_profit / net_gross_loss, 4)
        if net_gross_loss > 0 else float("inf") if net_gross_profit > 0 else 0.0
    )

    # Expectancy: average PnL per trade
    metrics["expectancy"] = metrics["average_trade"]
    if net_trade_pnls:
        net_expectancy = sum(net_trade_pnls) / total_trades
        net_variance = (
            sum((pnl - net_expectancy) ** 2 for pnl in net_trade_pnls) / (total_trades - 1)
            if total_trades > 1 else 0.0
        )
        net_std = math.sqrt(net_variance)
        standard_error = net_std / math.sqrt(total_trades) if total_trades > 1 else None
        metrics["net_expectancy_after_cost"] = round(net_expectancy, 6)
        metrics["net_trade_pnl_stddev"] = round(net_std, 6)
        metrics["net_expectancy_standard_error"] = (
            round(standard_error, 6) if standard_error is not None else None
        )
        metrics["net_expectancy_t_stat"] = (
            round(net_expectancy / standard_error, 4)
            if standard_error is not None and standard_error > 0 else None
        )
        metrics["net_expectancy_ci95_low_normal_approx"] = (
            round(net_expectancy - 1.96 * standard_error, 6)
            if standard_error is not None else None
        )
        metrics["net_expectancy_ci95_high_normal_approx"] = (
            round(net_expectancy + 1.96 * standard_error, 6)
            if standard_error is not None else None
        )
        trade_return_pcts = [
            pnl / notional * 100.0
            for pnl, notional in zip(net_trade_pnls, position_entry_notionals)
            if notional > 0
        ]
        if len(trade_return_pcts) > 1:
            return_mean = sum(trade_return_pcts) / len(trade_return_pcts)
            metrics["net_trade_return_stddev_pct"] = round(math.sqrt(
                sum((value - return_mean) ** 2 for value in trade_return_pcts)
                / (len(trade_return_pcts) - 1)
            ), 6)
        else:
            metrics["net_trade_return_stddev_pct"] = 0.0
    else:
        metrics["net_expectancy_after_cost"] = 0.0
        metrics["net_trade_pnl_stddev"] = 0.0
        metrics["net_expectancy_standard_error"] = None
        metrics["net_expectancy_t_stat"] = None
        metrics["net_expectancy_ci95_low_normal_approx"] = None
        metrics["net_expectancy_ci95_high_normal_approx"] = None
        metrics["net_trade_return_stddev_pct"] = 0.0

    # ── Win/loss streaks ─────────────────────────────────────────────
    max_win_streak = 0
    max_loss_streak = 0
    current_streak = 0
    for pnl in position_gross_pnls:
        if pnl > 0:
            current_streak = current_streak + 1 if current_streak > 0 else 1
            max_win_streak = max(max_win_streak, current_streak)
        elif pnl < 0:
            current_streak = current_streak - 1 if current_streak < 0 else -1
            max_loss_streak = max(max_loss_streak, abs(current_streak))
        else:
            current_streak = 0
    metrics["max_win_streak"] = max_win_streak
    metrics["max_loss_streak"] = max_loss_streak

    # ── MAE / MFE ────────────────────────────────────────────────────
    mfe_pcts = [max(leg.max_favorable_pct for leg in legs) for legs in positions]
    mae_pcts = [min(leg.max_adverse_pct for leg in legs) for legs in positions]
    metrics["avg_mfe_pct"] = round(sum(mfe_pcts) / total_trades, 4) if total_trades else 0.0
    metrics["avg_mae_pct"] = round(sum(mae_pcts) / total_trades, 4) if total_trades else 0.0
    metrics["max_mfe_pct"] = round(max(mfe_pcts, default=0.0), 4)
    metrics["max_mae_pct"] = round(min(mae_pcts, default=0.0), 4)
    # How much of the favorable excursion is typically given back.
    giveback = [
        max(leg.max_favorable_pct for leg in legs)
        - (
            sum(leg.pnl for leg in legs)
            / position_entry_notionals[index] * 100.0
            if position_entry_notionals[index] > 0 else 0.0
        )
        for index, legs in enumerate(positions)
        if max(leg.max_favorable_pct for leg in legs) > 0
    ]
    metrics["avg_giveback_pct"] = round(sum(giveback) / len(giveback), 4) if giveback else 0.0

    # ── R-multiple distribution ──────────────────────────────────────
    # R = |pnl_pct| / |risk_pct| where risk_pct is inferred from SL distance.
    r_multiples: list[float] = []
    for index, legs in enumerate(positions):
        t = legs[0]
        if t.sl_price is not None and t.entry_price > 0:
            risk_pct = abs(t.entry_price - t.sl_price) / t.entry_price * 100.0
            if risk_pct > 0:
                realized_pct = (
                    sum(leg.pnl for leg in legs) / position_entry_notionals[index] * 100.0
                    if position_entry_notionals[index] > 0 else 0.0
                )
                r_multiples.append(realized_pct / risk_pct)
    if r_multiples:
        metrics["avg_r_multiple"] = round(sum(r_multiples) / len(r_multiples), 4)
        metrics["median_r_multiple"] = round(sorted(r_multiples)[len(r_multiples) // 2], 4)
        metrics["max_r_multiple"] = round(max(r_multiples), 4)
        metrics["min_r_multiple"] = round(min(r_multiples), 4)
    else:
        metrics["avg_r_multiple"] = 0.0
        metrics["median_r_multiple"] = 0.0
        metrics["max_r_multiple"] = 0.0
        metrics["min_r_multiple"] = 0.0

    # ── Exit-reason breakdown ────────────────────────────────────────
    reason_counts: dict[str, int] = defaultdict(int)
    for t in trades:
        reason_counts[t.close_reason or "unknown"] += 1
    metrics["exit_reasons"] = dict(reason_counts)

    # ── Time-in-trade ────────────────────────────────────────────────
    held_candles = [max(leg.candles_held for leg in legs) for legs in positions]
    if held_candles:
        metrics["avg_candles_held"] = round(sum(held_candles) / len(held_candles), 2)
        metrics["max_candles_held"] = max(held_candles)
    else:
        metrics["avg_candles_held"] = 0.0
        metrics["max_candles_held"] = 0

    if equity_curve:
        active_points = [point for point in equity_curve if point.open_positions > 0]
        metrics["time_in_market_pct"] = round(len(active_points) / len(equity_curve) * 100.0, 4)
        metrics["time_in_market_basis"] = "fraction_of_equity_curve_observations_with_open_positions"
        metrics["average_concurrent_positions"] = round(
            sum(point.open_positions for point in equity_curve) / len(equity_curve), 4
        )
        metrics["max_concurrent_positions"] = max(point.open_positions for point in equity_curve)
    else:
        metrics["time_in_market_pct"] = 0.0
        metrics["time_in_market_basis"] = "fraction_of_equity_curve_observations_with_open_positions"
        metrics["average_concurrent_positions"] = 0.0
        metrics["max_concurrent_positions"] = 0

    # ── Equity curve metrics ─────────────────────────────────────────
    if equity_curve:
        equities = [p.equity for p in equity_curve]
        final_equity = equities[-1]
        metrics["final_equity"] = round(final_equity, 4)
        metrics["total_return_pct"] = (
            round((final_equity - initial_capital) / initial_capital * 100.0, 2)
            if initial_capital > 0
            else 0.0
        )

        # Max drawdown
        peak = equities[0]
        peak_idx = 0
        peak_ts = equity_curve[0].ts
        max_dd = 0.0
        max_dd_pct = 0.0
        max_dd_duration_ms = 0
        max_dd_duration_bars = 0
        current_dd_start_ts: int | None = None
        current_dd_start_idx: int | None = None
        for idx, eq in enumerate(equities):
            if eq >= peak:
                if current_dd_start_ts is not None:
                    max_dd_duration_ms = max(
                        max_dd_duration_ms,
                        equity_curve[idx].ts - current_dd_start_ts,
                    )
                    max_dd_duration_bars = max(
                        max_dd_duration_bars,
                        idx - (current_dd_start_idx or 0),
                    )
                peak = eq
                peak_idx = idx
                peak_ts = equity_curve[idx].ts
                current_dd_start_ts = None
                current_dd_start_idx = None
            else:
                if current_dd_start_ts is None:
                    current_dd_start_ts = peak_ts
                    current_dd_start_idx = peak_idx
                max_dd_duration_ms = max(
                    max_dd_duration_ms,
                    equity_curve[idx].ts - current_dd_start_ts,
                )
                max_dd_duration_bars = max(
                    max_dd_duration_bars,
                    idx - (current_dd_start_idx or 0),
                )
            dd = peak - eq
            dd_pct = dd / peak * 100.0 if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct
        metrics["max_drawdown"] = round(max_dd, 4)
        metrics["max_drawdown_pct"] = round(max_dd_pct, 2)
        metrics["max_drawdown_duration_ms"] = int(max_dd_duration_ms)
        metrics["max_drawdown_duration_bars"] = int(max_dd_duration_bars)

        # ── Return series & risk-adjusted ratios ─────────────────────
        returns: list[float] = []
        for i in range(1, len(equities)):
            prev = equities[i - 1]
            if prev > 0:
                returns.append((equities[i] - prev) / prev)

        # Annualisation factor: sqrt(candles_per_year). 0 → raw per-candle.
        annual_factor = math.sqrt(candles_per_year) if candles_per_year > 0 else 1.0

        if len(returns) > 1:
            mean_return = sum(returns) / len(returns)
            std_return = math.sqrt(
                sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
            )
            metrics["mean_return_per_candle"] = round(mean_return, 8)
            metrics["return_stddev_per_candle"] = round(std_return, 8)
            metrics["sharpe_per_candle"] = (
                round(mean_return / std_return, 4) if std_return > 0 else 0.0
            )
            metrics["sharpe_annualized"] = (
                round(metrics["sharpe_per_candle"] * annual_factor, 4)
                if std_return > 0
                else 0.0
            )
            metrics["sharpe_basis"] = "mark_to_market_equity_returns_per_evaluation_bar"
            metrics["sharpe_annualization_candles_per_year"] = candles_per_year
            metrics["sharpe_serial_correlation_caveat"] = (
                "Bar returns are serially correlated; annualized Sharpe may be overstated."
            )

            # Sortino: penalise only downside deviation (returns < 0).
            downside = [r for r in returns if r < 0]
            downside_dev = math.sqrt(
                sum((r - mean_return) ** 2 for r in downside) / len(downside)
            ) if downside else 0.0
            metrics["sortino_annualized"] = (
                round(mean_return / downside_dev * annual_factor, 4)
                if downside_dev > 0
                else 0.0
            )

            # Calmar: annualised return / max drawdown %.
            if max_dd_pct > 0 and candles_per_year > 0:
                ann_return = mean_return * candles_per_year
                metrics["calmar_ratio"] = round(ann_return / (max_dd_pct / 100.0), 4)
            else:
                metrics["calmar_ratio"] = 0.0
        else:
            metrics["mean_return_per_candle"] = 0.0
            metrics["return_stddev_per_candle"] = 0.0
            metrics["sharpe_per_candle"] = 0.0
            metrics["sharpe_annualized"] = 0.0
            metrics["sortino_annualized"] = 0.0
            metrics["calmar_ratio"] = 0.0
            metrics["sharpe_basis"] = "mark_to_market_equity_returns_per_evaluation_bar"
            metrics["sharpe_annualization_candles_per_year"] = candles_per_year
            metrics["sharpe_serial_correlation_caveat"] = (
                "Bar returns are serially correlated; annualized Sharpe may be overstated."
            )
    else:
        metrics["final_equity"] = round(initial_capital, 4)
        metrics["total_return_pct"] = 0.0
        metrics["max_drawdown"] = 0.0
        metrics["max_drawdown_pct"] = 0.0
        metrics["sharpe_per_candle"] = 0.0
        metrics["sharpe_annualized"] = 0.0
        metrics["sortino_annualized"] = 0.0
        metrics["calmar_ratio"] = 0.0
        metrics["max_drawdown_duration_ms"] = 0
        metrics["max_drawdown_duration_bars"] = 0
        metrics["sharpe_basis"] = "mark_to_market_equity_returns_per_evaluation_bar"
        metrics["sharpe_annualization_candles_per_year"] = candles_per_year
        metrics["mean_return_per_candle"] = 0.0
        metrics["return_stddev_per_candle"] = 0.0
        metrics["sharpe_serial_correlation_caveat"] = (
            "Bar returns are serially correlated; annualized Sharpe may be overstated."
        )

    return metrics


def compute_buy_and_hold(
    candles: list[Any],
    initial_capital: float,
) -> dict[str, Any]:
    """Return buy-and-hold benchmark metrics for a single symbol's candles.

    ``candles`` may be a list of ``Candle`` objects or dicts with a ``close``
    key.  Returns ``total_return_pct``, ``max_drawdown_pct``, and
    ``final_equity`` for a position held from the first to the last close.
    """
    if not candles or initial_capital <= 0:
        return {"total_return_pct": 0.0, "max_drawdown_pct": 0.0, "final_equity": initial_capital}
    closes: list[float] = []
    for c in candles:
        price = c.close if hasattr(c, "close") else c.get("close")
        if price:
            closes.append(float(price))
    if not closes:
        return {"total_return_pct": 0.0, "max_drawdown_pct": 0.0, "final_equity": initial_capital}
    first, last = closes[0], closes[-1]
    total_return_pct = (last - first) / first * 100.0 if first > 0 else 0.0
    final_equity = initial_capital * (last / first) if first > 0 else initial_capital

    # Max drawdown on the mark-to-market equity of the held position.
    peak = closes[0]
    max_dd_pct = 0.0
    for price in closes:
        peak = max(peak, price)
        if peak > 0:
            max_dd_pct = max(max_dd_pct, (peak - price) / peak * 100.0)
    return {
        "total_return_pct": round(total_return_pct, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "final_equity": round(final_equity, 4),
    }


def compute_equal_weight_buy_and_hold(
    candles_by_symbol: dict[str, list[Any]],
    initial_capital: float,
) -> dict[str, Any]:
    """Compute an equal-weight, synchronized spot buy-and-hold portfolio benchmark."""
    symbols = sorted(candles_by_symbol)
    price_maps: dict[str, dict[int, float]] = {}
    for symbol in symbols:
        prices: dict[int, float] = {}
        for candle in candles_by_symbol[symbol]:
            ts = candle.ts if hasattr(candle, "ts") else candle.get("ts")
            close = candle.close if hasattr(candle, "close") else candle.get("close")
            if ts is not None and close is not None and float(close) > 0:
                prices[int(ts)] = float(close)
        if not prices:
            return {
                "benchmark_method": "equal_weight_buy_and_hold",
                "symbols": symbols,
                "error": f"No valid close prices for {symbol}",
            }
        price_maps[symbol] = prices
    if not symbols or initial_capital <= 0:
        return {
            "benchmark_method": "equal_weight_buy_and_hold",
            "symbols": symbols,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "final_equity": initial_capital,
            "capital_allocation": "equal_weight",
            "capital_basis": "unlevered spot-style benchmark; no trading costs",
            "costs_included": False,
        }

    common_ts = sorted(set.intersection(*(set(prices) for prices in price_maps.values())))
    if not common_ts:
        return {
            "benchmark_method": "equal_weight_buy_and_hold",
            "symbols": symbols,
            "error": "No common candle timestamps across benchmark symbols",
        }
    baseline = {symbol: price_maps[symbol][common_ts[0]] for symbol in symbols}
    equity_curve = [
        initial_capital * sum(
            price_maps[symbol][ts] / baseline[symbol] for symbol in symbols
        ) / len(symbols)
        for ts in common_ts
    ]
    peak = equity_curve[0]
    max_drawdown_pct = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown_pct = max(max_drawdown_pct, (peak - equity) / peak * 100.0)
    return {
        "benchmark_method": "equal_weight_buy_and_hold",
        "symbols": symbols,
        "capital_allocation": "equal_weight",
        "capital_basis": "unlevered spot-style benchmark; no trading costs",
        "costs_included": False,
        "start_ts": common_ts[0],
        "end_ts": common_ts[-1],
        "aligned_candles": len(common_ts),
        "total_return_pct": round((equity_curve[-1] / initial_capital - 1.0) * 100.0, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "final_equity": round(equity_curve[-1], 4),
    }


def compute_group_metrics(
    trades: list[SimPosition],
    key_fn: Callable[[SimPosition], str],
) -> dict[str, dict[str, Any]]:
    """Group trades by ``key_fn`` and compute per-group aggregate metrics.

    Shared by the per-strategy and per-symbol breakdowns so both rollups stay
    consistent.  Each group's value is the full ``compute_metrics`` output plus
    a ``trades`` count (``initial_capital`` is 0 so return/Drawdown fields are
    not meaningful per group — they are aggregate-only).
    """
    groups: dict[str, list[SimPosition]] = defaultdict(list)
    for t in trades:
        groups[key_fn(t) or "unknown"].append(t)

    result: dict[str, dict[str, Any]] = {}
    for name, group_trades in groups.items():
        result[name] = compute_metrics(group_trades, [], 0.0)
        result[name]["trades"] = len(group_trades)
    return result


def compute_per_strategy_metrics(trades: list[SimPosition]) -> dict[str, dict[str, Any]]:
    """Compute metrics broken down by strategy name."""
    return compute_group_metrics(trades, key_fn=lambda t: t.strategy_name)


def compute_per_symbol_metrics(trades: list[SimPosition]) -> dict[str, dict[str, Any]]:
    """Compute metrics broken down by symbol (token)."""
    return compute_group_metrics(trades, key_fn=lambda t: t.symbol)
