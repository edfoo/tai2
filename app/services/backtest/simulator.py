"""Simulated broker for backtesting.

Replicates the behaviour of OKX algo orders: when a position is opened with
``tp_price`` / ``sl_price``, the simulator checks each subsequent candle's
high/low and closes the position if the price crosses either level.

The simulator also includes **extension hooks** for position-management
strategies (skimming, protector, alternator, etc.) that will be implemented
in a future phase.  The hooks are no-ops now but their presence means
position-management strategies can be added without touching the simulator
core.

Close logic (conservative — assumes worst case within the candle):

    Long position:
        if candle.low <= sl_price  → close at sl_price (loss)
        if candle.high >= tp_price → close at tp_price (profit)

    Short position:
        if candle.high >= sl_price  → close at sl_price (loss)
        if candle.low <= tp_price   → close at tp_price (profit)

If both TP and SL are hit within the same candle, SL is assumed to have
triggered first (pessimistic assumption).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from app.services.backtest.models import Candle, EquityPoint, SimPosition

logger = logging.getLogger(__name__)


# ── Position-management strategy protocol (future phase) ───────────────


class PMAction:
    """Base action returned by a position-management strategy."""

    pass


@dataclass(slots=True)
class CloseAction(PMAction):
    reason: str
    price: float | None = None  # None = close at market (candle close)


@dataclass(slots=True)
class UpdateStopLossAction(PMAction):
    new_sl_price: float


@dataclass(slots=True)
class FlipAction(PMAction):
    new_direction: str
    new_tp_price: float | None = None
    new_sl_price: float | None = None


class PositionManagementStrategy(Protocol):
    """Protocol for position-management strategies (future phase)."""

    name: str

    def on_entry(self, position: SimPosition, config: dict[str, Any]) -> None:
        """Called when a position is opened.  May modify TP/SL (e.g. Alternator strips them)."""
        ...

    def check(
        self,
        position: SimPosition,
        candle: Candle,
        config: dict[str, Any],
        sim: "Simulator",
    ) -> PMAction | None:
        """Called each candle for each open position.  Return an action or None."""
        ...


# ── Simulator ──────────────────────────────────────────────────────────


class Simulator:
    """Simulated broker that tracks positions, fills, and equity.

    Parameters
    ----------
    initial_capital:
        Starting account equity in quote currency (e.g. USDT).
    notional_per_trade:
        Fixed notional size per trade in quote currency.  Matches the
        launcher's ``notional_usd`` setting.
    """

    def __init__(
        self,
        *,
        initial_capital: float = 1000.0,
        notional_per_trade: float = 10.0,
        strategy_config: dict[str, Any] | None = None,
    ) -> None:
        self._initial_capital = initial_capital
        self._notional_per_trade = notional_per_trade
        self._strategy_config = strategy_config or {}
        self._open_positions: list[SimPosition] = []
        self._closed_positions: list[SimPosition] = []
        self._equity_curve: list[EquityPoint] = []
        self._cash = initial_capital
        self._pm_strategies: list[PositionManagementStrategy] = []  # future phase
        # Max candles a position may be held before forced close (0 = disabled).
        # Read from the strategy config: launcher-level or per-strategy.
        _launcher = self._strategy_config or {}
        self._max_hold_candles = int(_launcher.get("max_hold_candles") or 0)

    # ── Properties ────────────────────────────────────────────────────

    @property
    def open_positions(self) -> list[SimPosition]:
        return list(self._open_positions)

    @property
    def closed_positions(self) -> list[SimPosition]:
        return list(self._closed_positions)

    @property
    def all_positions(self) -> list[SimPosition]:
        return self._closed_positions + self._open_positions

    @property
    def equity_curve(self) -> list[EquityPoint]:
        return list(self._equity_curve)

    @property
    def cash(self) -> float:
        return self._cash

    def equity(self, current_prices: dict[str, float]) -> float:
        """Total equity = cash + unrealised PnL of all open positions."""
        unrealised = 0.0
        for pos in self._open_positions:
            price = current_prices.get(pos.symbol, 0.0)
            unrealised += pos.unrealised_pnl(price)
        return self._cash + unrealised

    # ── Position management ──────────────────────────────────────────

    def add_pm_strategy(self, strategy: PositionManagementStrategy) -> None:
        """Register a position-management strategy (future phase)."""
        self._pm_strategies.append(strategy)

    def has_open_position(self, symbol: str, strategy_name: str = "") -> bool:
        """Check if there's an open position for *symbol* (optionally per-strategy)."""
        for pos in self._open_positions:
            if pos.symbol != symbol:
                continue
            if strategy_name and pos.strategy_name != strategy_name:
                continue
            return True
        return False

    # ── Open / close ──────────────────────────────────────────────────

    def open_position(
        self,
        *,
        symbol: str,
        direction: str,
        entry_price: float,
        entry_ts: int,
        tp_price: float | None = None,
        sl_price: float | None = None,
        strategy_name: str = "",
    ) -> SimPosition:
        """Open a new simulated position.

        Size is derived from ``notional_per_trade / entry_price``.
        """
        size = self._notional_per_trade / entry_price if entry_price > 0 else 0.0
        position = SimPosition(
            symbol=symbol,
            direction=direction,
            size=size,
            entry_price=entry_price,
            entry_ts=entry_ts,
            tp_price=tp_price,
            sl_price=sl_price,
            strategy_name=strategy_name,
        )
        # Entry-time hook for position-management strategies (future phase).
        for pm in self._pm_strategies:
            pm.on_entry(position, self._strategy_config)
        self._open_positions.append(position)
        return position

    def _close_position(
        self,
        position: SimPosition,
        close_price: float,
        close_ts: int,
        reason: str,
    ) -> None:
        """Close a position and record realised PnL."""
        position.close_price = close_price
        position.close_ts = close_ts
        position.close_reason = reason
        position.pnl = position.unrealised_pnl(close_price)
        if position.entry_price > 0:
            position.pnl_pct = position.unrealised_pnl_pct(close_price)
        # Realise PnL into cash.
        self._cash += position.pnl
        self._open_positions.remove(position)
        self._closed_positions.append(position)

    # ── Per-candle update ────────────────────────────────────────────

    def update(self, candle: Candle) -> None:
        """Process one candle: check TP/SL and position-management strategies.

        This is the main loop called by the engine for each historical candle.
        """
        # 1. Check TP/SL (algo-order simulation) for each open position.
        for position in list(self._open_positions):
            if position.symbol != candle.ts and not self._matches_symbol(position, candle):
                # This position is for a different symbol — skip TP/SL check
                # (multi-symbol backtests pass candles for each symbol).
                continue
            position.candles_held += 1
            if self._check_tp_sl(position, candle):
                continue  # position was closed
            # 1b. Max-hold-time timeout — close at candle close.
            if self._max_hold_candles > 0 and position.candles_held >= self._max_hold_candles:
                self._close_position(position, candle.close, candle.ts, "timeout")
                continue
            # 2. Position-management strategies (future phase — no-ops now).
            for pm in self._pm_strategies:
                action = pm.check(position, candle, self._strategy_config, self)
                if action is not None:
                    self._apply_pm_action(action, position, candle)
                    break

        # 3. Record equity curve point.
        eq = self.equity({candle.ts: candle.close})  # simplified — engine passes prices
        self._equity_curve.append(
            EquityPoint(ts=candle.ts, equity=eq, open_positions=len(self._open_positions))
        )

    def update_multi(self, prices: dict[str, Candle]) -> None:
        """Process one time-step across multiple symbols.

        ``prices`` maps symbol → Candle for the current time-step.
        """
        # 1. Check TP/SL for each open position against its symbol's candle.
        for position in list(self._open_positions):
            candle = prices.get(position.symbol)
            if candle is None:
                continue
            position.candles_held += 1
            if self._check_tp_sl(position, candle):
                continue
            # 1b. Max-hold-time timeout — close at candle close.
            if self._max_hold_candles > 0 and position.candles_held >= self._max_hold_candles:
                self._close_position(position, candle.close, candle.ts, "timeout")
                continue
            for pm in self._pm_strategies:
                action = pm.check(position, candle, self._strategy_config, self)
                if action is not None:
                    self._apply_pm_action(action, position, candle)
                    break

        # 2. Record equity curve point.
        current_prices = {sym: c.close for sym, c in prices.items()}
        eq = self.equity(current_prices)
        ts = next(iter(prices.values())).ts if prices else 0
        self._equity_curve.append(
            EquityPoint(ts=ts, equity=eq, open_positions=len(self._open_positions))
        )

    def _matches_symbol(self, position: SimPosition, candle: Candle) -> bool:
        """Check if a candle belongs to a position's symbol.

        Since :class:`Candle` doesn't carry a symbol, the engine uses
        ``update_multi`` for multi-symbol backtests.  For single-symbol
        backtests using ``update``, all candles belong to the same symbol.
        """
        return True  # single-symbol mode: assume candle matches

    def _check_tp_sl(self, position: SimPosition, candle: Candle) -> bool:
        """Check if TP or SL is hit.  Returns True if position was closed.

        Conservative: if both TP and SL are within the candle's range, SL
        is assumed to trigger first (pessimistic).
        """
        if position.is_long:
            # Stop loss: price dropped to/below SL
            if position.sl_price is not None and candle.low <= position.sl_price:
                self._close_position(position, position.sl_price, candle.ts, "sl")
                return True
            # Take profit: price rose to/above TP
            if position.tp_price is not None and candle.high >= position.tp_price:
                self._close_position(position, position.tp_price, candle.ts, "tp")
                return True
        else:  # short
            # Stop loss: price rose to/above SL
            if position.sl_price is not None and candle.high >= position.sl_price:
                self._close_position(position, position.sl_price, candle.ts, "sl")
                return True
            # Take profit: price dropped to/below TP
            if position.tp_price is not None and candle.low <= position.tp_price:
                self._close_position(position, position.tp_price, candle.ts, "tp")
                return True
        return False

    def _apply_pm_action(self, action: PMAction, position: SimPosition, candle: Candle) -> None:
        """Apply a position-management action (future phase)."""
        if isinstance(action, CloseAction):
            price = action.price if action.price is not None else candle.close
            self._close_position(position, price, candle.ts, action.reason)
        elif isinstance(action, UpdateStopLossAction):
            position.sl_price = action.new_sl_price
        elif isinstance(action, FlipAction):
            # Close current position, open reversed.
            self._close_position(position, candle.close, candle.ts, f"flip_{action.new_direction}")
            self.open_position(
                symbol=position.symbol,
                direction=action.new_direction,
                entry_price=candle.close,
                entry_ts=candle.ts,
                tp_price=action.new_tp_price,
                sl_price=action.new_sl_price,
                strategy_name=position.strategy_name,
            )

    # ── End-of-data cleanup ──────────────────────────────────────────

    def close_all_at_market(self, prices: dict[str, float], ts: int) -> None:
        """Close all remaining open positions at the current market price."""
        for position in list(self._open_positions):
            price = prices.get(position.symbol, position.entry_price)
            self._close_position(position, price, ts, "end_of_data")
