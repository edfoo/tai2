"""Performance metrics for backtest results.

Computes standard trading metrics from the list of closed trades and the
equity curve: total return, Sharpe ratio, max drawdown, win rate, profit
factor, average win/loss, etc.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from app.services.backtest.models import EquityPoint, SimPosition


def compute_metrics(
    trades: list[SimPosition],
    equity_curve: list[EquityPoint],
    initial_capital: float,
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
    """
    metrics: dict[str, Any] = {}

    # ── Trade-level metrics ──────────────────────────────────────────
    total_trades = len(trades)
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    break_even = [t for t in trades if t.pnl == 0]

    gross_profit = sum(t.pnl for t in wins)
    gross_loss = abs(sum(t.pnl for t in losses))
    net_profit = gross_profit - gross_loss

    metrics["total_trades"] = total_trades
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
    metrics["largest_win"] = round(max((t.pnl for t in wins), default=0.0), 4)
    metrics["largest_loss"] = round(min((t.pnl for t in losses), default=0.0), 4)

    # Expectancy: average PnL per trade
    metrics["expectancy"] = metrics["average_trade"]

    # ── Win/loss streaks ─────────────────────────────────────────────
    max_win_streak = 0
    max_loss_streak = 0
    current_streak = 0
    for t in trades:
        if t.pnl > 0:
            current_streak = current_streak + 1 if current_streak > 0 else 1
            max_win_streak = max(max_win_streak, current_streak)
        elif t.pnl < 0:
            current_streak = current_streak - 1 if current_streak < 0 else -1
            max_loss_streak = max(max_loss_streak, abs(current_streak))
        else:
            current_streak = 0
    metrics["max_win_streak"] = max_win_streak
    metrics["max_loss_streak"] = max_loss_streak

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
        max_dd = 0.0
        max_dd_pct = 0.0
        for eq in equities:
            if eq > peak:
                peak = eq
            dd = peak - eq
            dd_pct = dd / peak * 100.0 if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct
        metrics["max_drawdown"] = round(max_dd, 4)
        metrics["max_drawdown_pct"] = round(max_dd_pct, 2)

        # Sharpe ratio (annualised, assuming ~252 trading days)
        # Based on per-candle returns.
        returns: list[float] = []
        for i in range(1, len(equities)):
            prev = equities[i - 1]
            if prev > 0:
                returns.append((equities[i] - prev) / prev)
        if len(returns) > 1:
            mean_return = sum(returns) / len(returns)
            std_return = math.sqrt(
                sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
            )
            # Annualisation factor depends on timeframe — we use a simple
            # sqrt(n) scaling where n = number of candles per year.
            # For 4H candles: 6/day * 365 = 2190/year.
            # We leave the raw Sharpe (per-candle) and let the UI annualise.
            metrics["sharpe_per_candle"] = (
                round(mean_return / std_return, 4) if std_return > 0 else 0.0
            )
        else:
            metrics["sharpe_per_candle"] = 0.0
    else:
        metrics["final_equity"] = round(initial_capital, 4)
        metrics["total_return_pct"] = 0.0
        metrics["max_drawdown"] = 0.0
        metrics["max_drawdown_pct"] = 0.0
        metrics["sharpe_per_candle"] = 0.0

    return metrics


def compute_per_strategy_metrics(trades: list[SimPosition]) -> dict[str, dict[str, Any]]:
    """Compute metrics broken down by strategy name."""
    by_strategy: dict[str, list[SimPosition]] = defaultdict(list)
    for t in trades:
        name = t.strategy_name or "unknown"
        by_strategy[name].append(t)

    result: dict[str, dict[str, Any]] = {}
    for name, strat_trades in by_strategy.items():
        result[name] = compute_metrics(strat_trades, [], 0.0)
        result[name]["trades"] = len(strat_trades)
    return result
