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

    # ── Trading costs (fees / funding) ───────────────────────────────
    total_fees = sum(t.entry_fee + t.exit_fee for t in trades)
    total_funding = sum(t.funding for t in trades)
    metrics["total_fees"] = round(total_fees, 4)
    metrics["total_funding"] = round(total_funding, 4)
    metrics["total_cost"] = round(total_fees + total_funding, 4)
    metrics["net_profit_after_cost"] = round(net_profit - total_fees - total_funding, 4)

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

    # ── MAE / MFE ────────────────────────────────────────────────────
    mfe_pcts = [t.max_favorable_pct for t in trades]
    mae_pcts = [t.max_adverse_pct for t in trades]
    metrics["avg_mfe_pct"] = round(sum(mfe_pcts) / total_trades, 4) if total_trades else 0.0
    metrics["avg_mae_pct"] = round(sum(mae_pcts) / total_trades, 4) if total_trades else 0.0
    metrics["max_mfe_pct"] = round(max(mfe_pcts, default=0.0), 4)
    metrics["max_mae_pct"] = round(min(mae_pcts, default=0.0), 4)
    # How much of the favorable excursion is typically given back.
    giveback = [t.max_favorable_pct - t.pnl_pct for t in trades if t.max_favorable_pct > 0]
    metrics["avg_giveback_pct"] = round(sum(giveback) / len(giveback), 4) if giveback else 0.0

    # ── R-multiple distribution ──────────────────────────────────────
    # R = |pnl_pct| / |risk_pct| where risk_pct is inferred from SL distance.
    r_multiples: list[float] = []
    for t in trades:
        if t.sl_price is not None and t.entry_price > 0:
            risk_pct = abs(t.entry_price - t.sl_price) / t.entry_price * 100.0
            if risk_pct > 0:
                r_multiples.append(t.pnl_pct / risk_pct)
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
    held_candles = [t.candles_held for t in trades]
    if held_candles:
        metrics["avg_candles_held"] = round(sum(held_candles) / len(held_candles), 2)
        metrics["max_candles_held"] = max(held_candles)
    else:
        metrics["avg_candles_held"] = 0.0
        metrics["max_candles_held"] = 0

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
            metrics["sharpe_per_candle"] = (
                round(mean_return / std_return, 4) if std_return > 0 else 0.0
            )
            metrics["sharpe_annualized"] = (
                round(metrics["sharpe_per_candle"] * annual_factor, 4)
                if std_return > 0
                else 0.0
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
            metrics["sharpe_per_candle"] = 0.0
            metrics["sharpe_annualized"] = 0.0
            metrics["sortino_annualized"] = 0.0
            metrics["calmar_ratio"] = 0.0
    else:
        metrics["final_equity"] = round(initial_capital, 4)
        metrics["total_return_pct"] = 0.0
        metrics["max_drawdown"] = 0.0
        metrics["max_drawdown_pct"] = 0.0
        metrics["sharpe_per_candle"] = 0.0
        metrics["sharpe_annualized"] = 0.0
        metrics["sortino_annualized"] = 0.0
        metrics["calmar_ratio"] = 0.0

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
