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
import math
from dataclasses import dataclass, field
from typing import Any, Protocol

from app.services.backtest.models import Candle, EquityPoint, SimPosition
from app.services.backtest.sizing import compute_order_size
from app.services.backtest.costs import CostModel
from app.services.indicator_service import htf_regime_allows
from app.services.strategies import resolve_analysis_block
from app.services.strategies.defaults import merged_config
from app.services.trade_management_rules import (
    compute_trade_management_decision,
    resolve_tm_params,
)

logger = logging.getLogger(__name__)

# Rolling window (ms) used by the daily-loss lockout, matching live's
# ``DAILY_LOSS_WINDOW_HOURS = 24`` in prompt_runner.py.
DAILY_LOSS_WINDOW_MS = 24 * 60 * 60 * 1000


def _to_positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


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
        timeframe_seconds: float | None = None,
        cost_model: CostModel | None = None,
    ) -> None:
        self._initial_capital = initial_capital
        self._notional_per_trade = notional_per_trade
        self._strategy_config = strategy_config or {}
        self._cost_model = cost_model or CostModel()
        self._open_positions: list[SimPosition] = []
        self._closed_positions: list[SimPosition] = []
        self._equity_curve: list[EquityPoint] = []
        self._cash = initial_capital
        self._pm_strategies: list[PositionManagementStrategy] = []  # future phase
        self._last_close_ts: dict[str, int] = {}
        self._recent_candles: dict[str, list[Candle]] = {}
        # Rolling equity history (ts, equity) for the daily-loss lockout.
        self._equity_history: list[tuple[int, float]] = []
        # Max candles a position may be held before forced close (0 = disabled).
        # Read from the strategy config: launcher-level or per-strategy.
        _launcher = self._strategy_config or {}
        self._max_hold_candles = int(_launcher.get("max_hold_candles") or 0)
        # Trade-management config (mirrors live strategy.trade_management).
        # May live under strategy_config["trade_management"] or top-level.
        self._tm = (
            (_launcher.get("trade_management") or {})
            if isinstance(_launcher.get("trade_management"), dict)
            else {}
        )
        # Time-stop in candles.  Live defaults to a wall-clock ``time_stop_seconds``
        # (45m); the backtest only steps on candles, so when ``time_stop_candles``
        # is unset/0 we derive it from ``time_stop_seconds`` on the configured
        # timeframe.  Without this the default backtest would hold underwater
        # trades indefinitely while live cuts them at ~45m.
        self._time_stop_candles = int(self._tm.get("time_stop_candles") or 0)
        if self._time_stop_candles <= 0:
            _ts_seconds = _to_positive_float(self._tm.get("time_stop_seconds"))
            if _ts_seconds and timeframe_seconds and timeframe_seconds > 0:
                self._time_stop_candles = max(1, int(math.ceil(_ts_seconds / timeframe_seconds)))

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

    def can_enter(self, symbol: str, entry_ts: int) -> bool:
        """Return whether live-style re-entry cooldown permits a new entry."""
        _g = self._guardrails()
        cooldown_seconds = float(
            self._tm.get("reentry_cooldown_seconds")
            or _g.get("min_hold_seconds")
            or _g.get("cooldown_seconds")
            or 0.0
        )
        if cooldown_seconds <= 0:
            return True
        last_close_ts = self._last_close_ts.get(symbol)
        if last_close_ts is None:
            return True
        return entry_ts - last_close_ts >= cooldown_seconds * 1000.0

    def _guardrails(self) -> dict[str, Any]:
        return self._strategy_config.get("guardrails") or {}

    def open_position_notional(self) -> float:
        """Sum of entry notional across all currently-open positions."""
        total = 0.0
        for pos in self._open_positions:
            total += pos.entry_price * pos.size
        return total

    def is_daily_loss_locked(self, current_ts: int) -> bool:
        """Return whether the live daily-loss guardrail would block new entries.

        Mirrors ``compute_daily_loss_guard_state``: the reference is the first
        equity observed within the rolling 24h window, and the lock activates
        when the drop from that reference reaches ``daily_loss_limit_pct``.
        """
        limit = _to_positive_float(self._guardrails().get("daily_loss_limit_pct"))
        if limit is None or not self._equity_history:
            return False
        current_equity = self._equity_history[-1][1]
        cutoff = current_ts - DAILY_LOSS_WINDOW_MS
        window = [eq for ts, eq in self._equity_history if ts >= cutoff]
        if not window:
            return False
        reference = window[0]
        if reference <= 0:
            return False
        change_pct = (reference - current_equity) / reference
        return change_pct >= limit

    def available_equity_for_trade(self, account_equity: float) -> float:
        """Free equity after existing positions, mirroring live sizing."""
        return max(account_equity - self.open_position_notional(), 0.0)

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
    ) -> SimPosition | None:
        """Open a new simulated position, or return None if a guardrail blocks.

        Size is derived from ``notional_per_trade / entry_price``, subject to
        the live daily-loss lockout and portfolio free-equity cap.
        """
        guardrails = self._strategy_config.get("guardrails") or {}
        instrument_specs = self._strategy_config.get("instrument_specs") or {}
        instrument = instrument_specs.get(symbol) or instrument_specs.get(symbol.upper()) or {}

        # Daily-loss lockout — block all new entries once the rolling 24h
        # equity drop reaches the configured threshold.
        if self.is_daily_loss_locked(entry_ts):
            return None

        # Portfolio free-equity cap — clip (or block) the requested notional
        # so total deployed notional never exceeds account equity.
        current_equity = self._equity_history[-1][1] if self._equity_history else self._cash
        free_equity = max(current_equity - self.open_position_notional(), 0.0)
        if free_equity <= 0:
            return None
        requested_notional = min(self._notional_per_trade, free_equity)

        size, _actual_notional = compute_order_size(
            requested_notional=requested_notional,
            entry_price=entry_price,
            equity=self._cash,
            stop_price=sl_price,
            guardrails=guardrails,
            instrument=instrument,
            symbol=symbol,
        )
        if size <= 0:
            return None

        # Effective entry fill price after slippage (slippage is baked into
        # fill_price, so it is already reflected in the position's PnL).
        fill_price = self._cost_model.entry_price_for(entry_price, direction == "long")
        notional = size * fill_price
        entry_fee = self._cost_model.fee_for(notional, taker=True)
        # Informational only — entry slippage cost (already in fill_price).
        entry_slippage = size * (fill_price - entry_price)

        position = SimPosition(
            symbol=symbol,
            direction=direction,
            size=size,
            entry_price=fill_price,
            entry_ts=entry_ts,
            tp_price=tp_price,
            sl_price=sl_price,
            strategy_name=strategy_name,
            initial_size=size,
            entry_fee=entry_fee,
            slippage_cost=entry_slippage,
        )
        self._cash -= entry_fee
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
        *,
        size_fraction: float = 1.0,
    ) -> None:
        """Close a position (fully or partially) and record realised PnL.

        Parameters
        ----------
        size_fraction:
            1.0 = full close (default).  Values in (0, 1) close that fraction
            of the remaining size and leave the rest open (partial TP).
        """
        if size_fraction <= 0:
            return
        if size_fraction >= 1.0:
            # Effective exit fill after slippage.
            exit_px = self._cost_model.exit_price_for(close_price, position.is_long)
            exit_notional = position.size * exit_px
            exit_fee = self._cost_model.fee_for(exit_notional, taker=True)
            # Funding accrued over the holding period (settles every interval).
            held_ms = max(close_ts - position.entry_ts, 0)
            intervals = held_ms // self._cost_model.funding_interval_ms
            funding = self._cost_model.funding_payment(
                position.size * position.entry_price,
                is_long=position.is_long,
                intervals=intervals,
            )

            position.close_price = exit_px
            position.close_ts = close_ts
            position.close_reason = reason
            position.exit_fee = exit_fee
            position.funding = funding
            # Accumulate exit slippage into the informational total.
            position.slippage_cost += position.size * abs(exit_px - close_price)
            position.pnl = position.unrealised_pnl(exit_px)
            if position.entry_price > 0:
                position.pnl_pct = position.unrealised_pnl_pct(exit_px)
            self._cash += position.pnl - exit_fee - funding
            self._open_positions.remove(position)
            self._closed_positions.append(position)
            self._last_close_ts[position.symbol] = close_ts
            return

        # Partial close: realise PnL on the closed fraction, keep remainder open.
        closed_size = position.size * size_fraction
        exit_px = self._cost_model.exit_price_for(close_price, position.is_long)
        if position.is_long:
            partial_pnl = (exit_px - position.entry_price) * closed_size
        else:
            partial_pnl = (position.entry_price - exit_px) * closed_size
        partial_fee = self._cost_model.fee_for(closed_size * exit_px, taker=True)
        self._cash += partial_pnl - partial_fee
        # Record a closed leg for metrics.
        closed_leg = SimPosition(
            symbol=position.symbol,
            direction=position.direction,
            size=closed_size,
            entry_price=position.entry_price,
            entry_ts=position.entry_ts,
            tp_price=position.tp_price,
            sl_price=position.sl_price,
            strategy_name=position.strategy_name,
            close_price=exit_px,
            close_ts=close_ts,
            close_reason=reason,
            pnl=partial_pnl,
            pnl_pct=(
                (exit_px - position.entry_price) / position.entry_price * 100.0
                if position.is_long and position.entry_price > 0
                else (
                    (position.entry_price - exit_px) / position.entry_price * 100.0
                    if position.entry_price > 0
                    else 0.0
                )
            ),
            exit_fee=partial_fee,
            candles_held=position.candles_held,
            initial_size=position.initial_size,
            breakeven_done=position.breakeven_done,
            partial_done=True,
        )
        self._closed_positions.append(closed_leg)
        position.size = position.size - closed_size
        position.partial_done = True

    # ── Per-candle update ────────────────────────────────────────────

    def _track_excursion(self, position: SimPosition, candle: Candle) -> None:
        """Update MAE/MFE for an open position from a candle's high/low."""
        if position.entry_price <= 0:
            return
        if position.is_long:
            fav_pct = (candle.high - position.entry_price) / position.entry_price * 100.0
            adv_pct = (candle.low - position.entry_price) / position.entry_price * 100.0
        else:
            fav_pct = (position.entry_price - candle.low) / position.entry_price * 100.0
            adv_pct = (position.entry_price - candle.high) / position.entry_price * 100.0
        position.max_favorable_pct = max(position.max_favorable_pct, fav_pct)
        position.max_adverse_pct = min(position.max_adverse_pct, adv_pct)

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
            self._track_excursion(position, candle)
            if self._check_tp_sl(position, candle):
                continue  # position was closed
            # 1b. Trade management (breakeven / partial / time-stop).
            if self._apply_trade_management(position, candle):
                continue
            # 1c. Max-hold-time timeout — close at candle close.
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
        self._equity_history.append((candle.ts, eq))

    def update_multi(self, prices: dict[str, Candle]) -> None:
        """Process one time-step across multiple symbols.

        ``prices`` maps symbol → Candle for the current time-step.
        """
        for symbol, candle in prices.items():
            history = self._recent_candles.setdefault(symbol, [])
            history.append(candle)
            if len(history) > 100:
                del history[:-100]

        # 1. Check TP/SL for each open position against its symbol's candle.
        for position in list(self._open_positions):
            candle = prices.get(position.symbol)
            if candle is None:
                continue
            position.candles_held += 1
            self._track_excursion(position, candle)
            if self._check_tp_sl(position, candle):
                continue
            # 1b. Trade management (breakeven / partial / time-stop).
            if self._apply_trade_management(position, candle):
                continue
            # 1c. Max-hold-time timeout — close at candle close.
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
        if ts:
            self._equity_history.append((ts, eq))

    def _apply_trade_management(self, position: SimPosition, candle: Candle) -> bool:
        """Apply breakeven / partial TP / trailing / software-stop / time-stop.

        Delegates the threshold + stop-price arithmetic to
        ``compute_trade_management_decision`` (shared with live); only the
        intrabar fill estimate and position mutation stay here.
        """
        tm = self._tm or {}
        if not tm.get("enabled"):
            return False

        entry = position.entry_price
        if entry <= 0:
            return False

        # Risk distance from entry to SL (used as 1R).
        risk_pct: float | None = None
        if position.sl_price is not None and position.sl_price > 0:
            if position.is_long:
                risk_pct = (entry - position.sl_price) / entry * 100.0
            else:
                risk_pct = (position.sl_price - entry) / entry * 100.0
            if risk_pct is not None and risk_pct <= 0:
                risk_pct = None

        # Mark = candle close; R-multiple uses the best excursion this candle
        # (high/low) so intrabar BE/partial/trailing triggers are detected.
        mark = candle.close
        if position.is_long:
            pnl_pct = (mark - entry) / entry * 100.0
            best_pct = (candle.high - entry) / entry * 100.0
        else:
            pnl_pct = (entry - mark) / entry * 100.0
            best_pct = (entry - candle.low) / entry * 100.0

        r_multiple = (best_pct / risk_pct) if (risk_pct and risk_pct > 0) else None

        # Time-stop (backtest = candle-count, progress + underwater conditions).
        time_stop_candles = self._time_stop_candles
        progress_r = (pnl_pct / risk_pct) if (risk_pct and risk_pct > 0) else None
        underwater_only = bool(tm.get("time_stop_underwater_only", True))
        timed_out = (
            bool(tm.get("time_stop_enabled", True))
            and time_stop_candles > 0
            and position.candles_held >= time_stop_candles
            and (progress_r is None or progress_r < float(tm.get("time_stop_min_r") or 0.3))
            and (not underwater_only or pnl_pct < 0.0)
        )

        atr_pct = self._atr_pct(position.symbol)

        decision = compute_trade_management_decision(
            resolve_tm_params(tm),
            is_long=position.is_long,
            entry_price=entry,
            mark_price=mark,
            risk_pct=risk_pct,
            pnl_pct=pnl_pct,
            r_multiple=r_multiple,
            atr_pct=atr_pct,
            current_sl=position.sl_price,
            breakeven_done=position.breakeven_done,
            partial_done=position.partial_done,
            timed_out=timed_out,
        )

        if decision.software_stop:
            self._close_position(position, candle.close, candle.ts, "software_sl")
            return True

        if decision.breakeven_new_sl is not None:
            position.sl_price = decision.breakeven_new_sl
            position.breakeven_done = True
            if self._check_tp_sl(position, candle):
                return True

        if decision.partial_trigger:
            partial_at_r = float(tm.get("partial_tp_at_r") or 0.8)
            if position.is_long:
                partial_px = min(candle.high, entry * (1.0 + (risk_pct or 0) * partial_at_r / 100.0))
                if partial_px < candle.low:
                    partial_px = candle.close
            else:
                partial_px = max(candle.low, entry * (1.0 - (risk_pct or 0) * partial_at_r / 100.0))
                if partial_px > candle.high:
                    partial_px = candle.close
            self._close_position(
                position, partial_px, candle.ts, "partial_tp",
                size_fraction=decision.partial_fraction,
            )

        if decision.trailing_new_sl is not None:
            position.sl_price = decision.trailing_new_sl
            if decision.trailing_remove_tp and position.partial_done:
                position.tp_price = None
            if self._check_tp_sl(position, candle):
                return True

        if decision.timeout:
            self._close_position(position, candle.close, candle.ts, "timeout")
            return True

        return False

    def _atr_pct(self, symbol: str, period: int = 14) -> float | None:
        """Estimate ATR percentage from the historical OHLC stream."""
        candles = self._recent_candles.get(symbol, [])
        if len(candles) < 2:
            return None
        window = candles[-(period + 1):]
        true_ranges: list[float] = []
        for previous, current in zip(window, window[1:]):
            true_ranges.append(max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            ))
        if not true_ranges or window[-1].close <= 0:
            return None
        return sum(true_ranges) / len(true_ranges) / window[-1].close * 100.0

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

    def apply_strategy_exits(
        self,
        symbol_snapshots: dict[str, dict[str, Any]],
        prices: dict[str, Candle],
    ) -> None:
        """Flatten open positions when a strategy-specific exit condition fires.

        Mirrors live ``_check_strategy_regime_exits`` (mean_reversion
        ``exit_on_regime_breakdown``) and
        ``_check_spike_continuation_rollover_exits`` (spike_continuation
        ``exit_on_momentum_rollover``).  Runs after TP/SL and trade-management
        checks, so positions already closed by those are skipped.
        """
        strategies_cfg = self._strategy_config.get("strategies") or {}

        for position in list(self._open_positions):
            candle = prices.get(position.symbol)
            if candle is None:
                continue
            snapshot = symbol_snapshots.get(position.symbol)
            if not snapshot:
                continue
            sym_data = (snapshot.get("market_data") or {}).get(position.symbol) or {}
            indicators = sym_data.get("indicators") or {}

            if position.strategy_name == "mean_reversion":
                if self._regime_breakdown_exit(position, candle, indicators, strategies_cfg):
                    continue
            elif position.strategy_name == "spike_continuation":
                if self._momentum_rollover_exit(position, candle, sym_data, strategies_cfg):
                    continue

    def _regime_breakdown_exit(
        self,
        position: SimPosition,
        candle: Candle,
        indicators: dict[str, Any],
        strategies_cfg: dict[str, Any],
    ) -> bool:
        """Flatten an underwater mean-reversion position on HTF chop→trend flip."""
        mr_cfg = strategies_cfg.get("mean_reversion") or {}
        if not bool(mr_cfg.get("exit_on_regime_breakdown", False)):
            return False
        htf_pref = str(mr_cfg.get("htf_regime_preference", "chop"))
        if htf_pref != "chop":
            return False

        adx_htf = _to_float(indicators.get("adx_htf"))
        chop_htf = _to_float(indicators.get("choppiness_htf"))
        if htf_regime_allows(adx_htf, chop_htf, htf_pref):
            return False  # still chop — thesis intact

        entry = position.entry_price
        last = candle.close
        if entry <= 0 or last <= 0:
            return False
        underwater = last < entry if position.is_long else last > entry
        if not underwater:
            return False

        self._close_position(position, last, candle.ts, "regime_breakdown")
        return True

    def _momentum_rollover_exit(
        self,
        position: SimPosition,
        candle: Candle,
        sym_data: dict[str, Any],
        strategies_cfg: dict[str, Any],
    ) -> bool:
        """Flatten a profitable spike-continuation position on momentum rollover."""
        sc_cfg = strategies_cfg.get("spike_continuation") or {}
        if not bool(sc_cfg.get("exit_on_momentum_rollover", False)):
            return False

        entry = position.entry_price
        last = candle.close
        if entry <= 0 or last <= 0:
            return False
        # Only exit when above breakeven — fading momentum is profit-protection.
        if position.is_long and last <= entry:
            return False
        if not position.is_long and last >= entry:
            return False

        block = resolve_analysis_block(sym_data, merged_config(sc_cfg, "spike_continuation"))
        vrsi_series = block.get("volume_rsi_series") or []
        rsi_series = block.get("rsi_series") or []
        if len(vrsi_series) < 2 or len(rsi_series) < 2:
            return False
        vrsi_cur = _to_float(vrsi_series[-1])
        vrsi_prev = _to_float(vrsi_series[-2])
        rsi_cur = _to_float(rsi_series[-1])
        rsi_prev = _to_float(rsi_series[-2])
        if None in (vrsi_cur, vrsi_prev, rsi_cur, rsi_prev):
            return False

        volume_fading = vrsi_cur < vrsi_prev
        rsi_rolled = (rsi_cur < rsi_prev) if position.is_long else (rsi_cur > rsi_prev)
        if not (volume_fading and rsi_rolled):
            return False

        self._close_position(position, last, candle.ts, "momentum_rollover")
        return True

    # ── End-of-data cleanup ──────────────────────────────────────────

    def close_all_at_market(self, prices: dict[str, float], ts: int) -> None:
        """Close all remaining open positions at the current market price."""
        for position in list(self._open_positions):
            price = prices.get(position.symbol, position.entry_price)
            self._close_position(position, price, ts, "end_of_data")
