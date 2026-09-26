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
from app.services.backtest.spread import spread_at
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


def _quantize_price(price: float | None, tick_size: float, rounding: str) -> float | None:
    if price is None or price <= 0 or tick_size <= 0:
        return price
    units = price / tick_size
    if rounding == "up":
        return math.ceil(units - 1e-12) * tick_size
    return math.floor(units + 1e-12) * tick_size


def compute_isolated_liquidation_price(
    *,
    direction: str,
    size: float,
    entry_price: float,
    initial_margin: float,
    maintenance_margin_ratio: float,
    maintenance_margin_deduction: float = 0.0,
) -> float | None:
    """Estimate the isolated-margin liquidation price for a linear swap.

    OKX liquidates an isolated position when its margin ratio reaches the
    maintenance-margin requirement.  For a linear (USDT-margined) contract the
    maintenance amount is ``size * price * mmr - mmrDeduction`` and the
    position equity is ``initial_margin + unrealised_pnl``.  Solving
    ``equity == maintenance`` for price gives:

        long :  price = (entry*size - initial_margin - deduction) / (size*(1 - mmr))
        short:  price = (entry*size + initial_margin + deduction) / (size*(1 + mmr))

    This is an approximation: it ignores fees, funding accrued in the
    liquidation equation, and exchange-specific risk adjustments.  Returns
    ``None`` when the inputs cannot produce a positive price.
    """
    if size <= 0 or entry_price <= 0:
        return None
    if direction == "long":
        denominator = size * max(1.0 - maintenance_margin_ratio, 1e-9)
        price = (entry_price * size - initial_margin - maintenance_margin_deduction) / denominator
    else:
        denominator = size * (1.0 + maintenance_margin_ratio)
        price = (
            entry_price * size + initial_margin + maintenance_margin_deduction
        ) / denominator
    return price if price > 0 else None


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
        margin_mode: str = "isolated",
    ) -> None:
        self._initial_capital = initial_capital
        self._notional_per_trade = notional_per_trade
        self._strategy_config = strategy_config or {}
        self._cost_model = cost_model or CostModel()
        # "isolated" reserves per-position initial margin and liquidates each
        # position independently; "cross" shares account equity across all
        # positions and liquidates the whole account when equity falls to the
        # aggregate maintenance requirement.
        self._margin_mode = margin_mode if margin_mode in ("isolated", "cross") else "isolated"
        self._open_positions: list[SimPosition] = []
        self._position_sequence = 0
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

    def set_instrument_specs(self, specs: dict[str, dict[str, Any]]) -> None:
        """Install exchange contract rules fetched for this backtest run."""
        self._strategy_config["instrument_specs"] = specs

    def _slippage_bps(
        self, symbol: str, order_notional: float, execution_ts: int | None = None
    ) -> float:
        """Estimate adverse slippage from recent OHLCV range and quote turnover."""
        model = self._cost_model
        base_bps = model.slippage_bps
        if model.slippage_mode == "tape_spread":
            return self._tape_spread_bps(symbol, base_bps, execution_ts)
        if model.slippage_mode != "ohlcv_liquidity":
            return model.stressed_slippage_bps(base_bps)
        candles = self._recent_candles.get(symbol, [])
        if execution_ts is not None:
            candles = [candle for candle in candles if candle.ts < execution_ts]
        if not candles or order_notional <= 0:
            return model.stressed_slippage_bps(base_bps)
        specs = self._strategy_config.get("instrument_specs") or {}
        spec = specs.get(symbol) or specs.get(symbol.upper()) or {}
        contract_value = _to_positive_float(spec.get("ct_val")) or 1.0
        quote_turnovers = sorted(
            max(c.volume, 0.0) * c.close * contract_value
            for c in candles if c.close > 0
        )
        range_bps = sorted(
            (c.high - c.low) / c.close * 10_000.0
            for c in candles if c.close > 0 and c.high >= c.low
        )
        if not quote_turnovers or not range_bps:
            return model.stressed_slippage_bps(base_bps)
        median_volume = quote_turnovers[len(quote_turnovers) // 2]
        median_range = range_bps[len(range_bps) // 2]
        impact_bps = (
            model.liquidity_impact_coefficient
            * math.sqrt(order_notional / max(median_volume, 1e-12))
            * 10_000.0
        )
        estimated = (
            base_bps
            + impact_bps
            + median_range * model.candle_range_slippage_fraction
        )
        capped = min(max(estimated, 0.0), model.max_liquidity_slippage_bps)
        return model.stressed_slippage_bps(capped)

    def _tape_spread_bps(
        self, symbol: str, base_bps: float, execution_ts: int | None
    ) -> float:
        """Estimate slippage from a trade-tape/OHLCV spread series.

        Uses the most recent spread estimate at or before the execution time
        (no look-ahead).  Falls back to the base bps when no estimate exists.
        The spread is a *spread* estimate, not order-book depth/impact.
        """
        model = self._cost_model
        series = (model.spread_series or {}).get(symbol)
        if not series:
            return model.stressed_slippage_bps(base_bps)
        ts = execution_ts if execution_ts is not None else 2**63 - 1
        spread = spread_at(series, ts)
        if spread is None:
            return model.stressed_slippage_bps(base_bps)
        estimated = base_bps + spread
        capped = min(max(estimated, 0.0), model.max_liquidity_slippage_bps)
        return model.stressed_slippage_bps(capped)

    def open_position_notional(self) -> float:
        """Sum of entry notional across all currently-open positions."""
        total = 0.0
        for pos in self._open_positions:
            total += pos.entry_price * pos.size
        return total

    def used_initial_margin(self) -> float:
        return sum(position.initial_margin for position in self._open_positions)

    @property
    def margin_mode(self) -> str:
        return self._margin_mode

    def maintenance_margin_requirement(self, prices: dict[str, float]) -> float:
        """Aggregate maintenance margin for all open positions at ``prices``.

        For a linear swap the maintenance amount is
        ``notional * mmr - mmrDeduction`` (floored at zero).  Used by the
        cross-margin account-level liquidation check.
        """
        total = 0.0
        for position in self._open_positions:
            price = prices.get(position.symbol)
            if price is None or price <= 0:
                price = position.entry_price
            notional = position.size * price
            requirement = (
                notional * position.maintenance_margin_ratio
                - position.maintenance_margin_deduction
            )
            total += max(requirement, 0.0)
        return total

    def cross_margin_liquidation_triggered(self, prices: dict[str, float]) -> bool:
        """Return whether account equity has fallen to the maintenance requirement.

        Cross margin shares all account equity across positions, so liquidation
        is account-level: it triggers when total equity (cash + unrealised PnL)
        is at or below the aggregate maintenance margin.
        """
        if self._margin_mode != "cross" or not self._open_positions:
            return False
        requirement = self.maintenance_margin_requirement(prices)
        if requirement <= 0:
            return False
        return self.equity(prices) <= requirement

    def _liquidate_cross_account(self, prices: dict[str, float], ts: int) -> None:
        """Close every open position at the current price (account liquidation)."""
        for position in list(self._open_positions):
            price = prices.get(position.symbol)
            if price is None or price <= 0:
                price = position.entry_price
            self._close_position(position, price, ts, "liquidation")

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
        max_leverage = _to_positive_float(guardrails.get("max_leverage")) or 1.0
        if self._margin_mode == "cross":
            # Cross margin: all account equity backs all positions, so headroom
            # is equity * leverage minus already-deployed notional (no per-
            # position initial-margin reservation).
            headroom = max(
                current_equity * max_leverage - self.open_position_notional(), 0.0
            )
            if headroom <= 0:
                return None
            requested_notional = min(self._notional_per_trade, headroom)
        else:
            free_margin = max(current_equity - self.used_initial_margin(), 0.0)
            if free_margin <= 0:
                return None
            requested_notional = min(self._notional_per_trade, free_margin * max_leverage)
        instrument_tiers = [
            tier for tier in instrument.get("position_tiers", [])
            if isinstance(tier, dict)
        ]
        tier_leverage_caps = [
            value for tier in instrument_tiers
            if (value := _to_positive_float(tier.get("max_leverage"))) is not None
        ]
        if tier_leverage_caps:
            max_leverage = min(max_leverage, max(tier_leverage_caps))
        max_leverage = max(max_leverage, 1.0)
        if self._margin_mode == "cross":
            requested_notional = min(
                self._notional_per_trade,
                max(current_equity * max_leverage - self.open_position_notional(), 0.0),
            )
        else:
            requested_notional = min(self._notional_per_trade, free_margin * max_leverage)

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

        contract_value = _to_positive_float(instrument.get("ct_val")) or 1.0
        contracts = size / contract_value
        selected_tier = next((
            tier for tier in instrument_tiers
            if contracts <= (_to_positive_float(tier.get("max_size")) or float("inf"))
        ), instrument_tiers[-1] if instrument_tiers else {})
        tier_leverage = _to_positive_float(selected_tier.get("max_leverage"))
        effective_leverage = min(max_leverage, tier_leverage) if tier_leverage else max_leverage
        effective_leverage = max(effective_leverage, 1.0)
        tier_imr = _to_positive_float(selected_tier.get("initial_margin_ratio"))
        margin_ratio = max(tier_imr or (1.0 / effective_leverage), 1.0 / effective_leverage)
        if effective_leverage < max_leverage:
            if self._margin_mode == "cross":
                requested_notional = min(
                    self._notional_per_trade,
                    max(current_equity * effective_leverage - self.open_position_notional(), 0.0),
                )
            else:
                requested_notional = min(self._notional_per_trade, free_margin * effective_leverage)
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
            contracts = size / contract_value
            selected_tier = next((
                tier for tier in instrument_tiers
                if contracts <= (_to_positive_float(tier.get("max_size")) or float("inf"))
            ), selected_tier)
            tier_leverage = _to_positive_float(selected_tier.get("max_leverage"))
            if tier_leverage:
                effective_leverage = min(effective_leverage, tier_leverage)
            tier_imr = _to_positive_float(selected_tier.get("initial_margin_ratio"))
            margin_ratio = max(tier_imr or (1.0 / effective_leverage), 1.0 / effective_leverage)

        self._position_sequence += 1
        trade_id = f"{symbol}:{entry_ts}:{strategy_name}:{self._position_sequence}"

        # Effective entry fill price after slippage (slippage is baked into
        # fill_price, so it is already reflected in the position's PnL).
        fill_price = self._cost_model.entry_price_for(
            entry_price,
            direction == "long",
            slippage_bps=self._slippage_bps(symbol, requested_notional, entry_ts),
        )
        notional = size * fill_price
        entry_fee = self._cost_model.fee_for(notional, taker=True)
        # Positive informational cost; the adverse fill is also reflected in PnL.
        entry_slippage = abs(size * (fill_price - entry_price))
        tick_size = _to_positive_float(instrument.get("tick_size")) or 0.0
        if tick_size > 0:
            if direction == "long":
                tp_price = _quantize_price(tp_price, tick_size, "down")
                sl_price = _quantize_price(sl_price, tick_size, "up")
            else:
                tp_price = _quantize_price(tp_price, tick_size, "up")
                sl_price = _quantize_price(sl_price, tick_size, "down")
        maintenance_ratio = (
            _to_positive_float(selected_tier.get("maintenance_margin_ratio"))
            or _to_positive_float(instrument.get("fallback_maintenance_margin_ratio"))
            or 0.005
        )
        maintenance_deduction = (
            _to_positive_float(selected_tier.get("maintenance_deduction")) or 0.0
        )
        initial_margin = size * fill_price * margin_ratio
        liquidation_price = None
        if self._margin_mode == "isolated" and effective_leverage > 1.0:
            liquidation_price = compute_isolated_liquidation_price(
                direction=direction,
                size=size,
                entry_price=fill_price,
                initial_margin=initial_margin,
                maintenance_margin_ratio=maintenance_ratio,
                maintenance_margin_deduction=maintenance_deduction,
            )

        position = SimPosition(
            symbol=symbol,
            direction=direction,
            size=size,
            entry_price=fill_price,
            entry_ts=entry_ts,
            trade_id=trade_id,
            funding_settled_through_ts=entry_ts,
            margin_mode=self._margin_mode,
            leverage=effective_leverage,
            initial_margin=size * fill_price * margin_ratio,
            maintenance_margin_ratio=maintenance_ratio,
            maintenance_margin_deduction=maintenance_deduction,
            liquidation_price=liquidation_price,
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
            self._settle_funding(position, close_ts)
            # Effective exit fill after slippage.
            exit_px = self._cost_model.exit_price_for(
                close_price,
                position.is_long,
                slippage_bps=self._slippage_bps(
                    position.symbol, position.size * close_price, close_ts
                ),
            )
            exit_notional = position.size * exit_px
            exit_fee = self._cost_model.fee_for(exit_notional, taker=True)
            if reason == "liquidation":
                exit_fee += self._cost_model.liquidation_fee_for(exit_notional)
            position.close_price = exit_px
            position.close_ts = close_ts
            position.close_reason = reason
            position.exit_fee = exit_fee
            # Slippage is tracked as a positive cost, separate from fill PnL.
            position.slippage_cost += position.size * abs(exit_px - close_price)
            position.pnl = position.unrealised_pnl(exit_px)
            if position.entry_price > 0:
                position.pnl_pct = position.unrealised_pnl_pct(exit_px)
            self._cash += position.pnl - exit_fee
            self._open_positions.remove(position)
            self._closed_positions.append(position)
            self._last_close_ts[position.symbol] = close_ts
            return

        # Partial close: realise PnL on the closed fraction, keep remainder open.
        self._settle_funding(position, close_ts)
        closed_size = position.size * size_fraction
        exit_px = self._cost_model.exit_price_for(
            close_price,
            position.is_long,
            slippage_bps=self._slippage_bps(
                position.symbol, position.size * close_price, close_ts
            ),
        )
        if position.is_long:
            partial_pnl = (exit_px - position.entry_price) * closed_size
        else:
            partial_pnl = (position.entry_price - exit_px) * closed_size
        partial_fee = self._cost_model.fee_for(closed_size * exit_px, taker=True)
        close_fraction = closed_size / position.size
        allocated_entry_fee = position.entry_fee * close_fraction
        allocated_entry_slippage = position.slippage_cost * close_fraction
        allocated_margin = position.initial_margin * close_fraction
        partial_slippage = allocated_entry_slippage + closed_size * abs(exit_px - close_price)
        partial_funding = position.funding * close_fraction
        self._cash += partial_pnl - partial_fee
        # Record a closed leg for metrics.
        closed_leg = SimPosition(
            symbol=position.symbol,
            direction=position.direction,
            size=closed_size,
            entry_price=position.entry_price,
            entry_ts=position.entry_ts,
            trade_id=position.trade_id,
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
            entry_fee=allocated_entry_fee,
            exit_fee=partial_fee,
            slippage_cost=partial_slippage,
            funding=partial_funding,
            candles_held=position.candles_held,
            initial_size=position.initial_size,
            initial_margin=allocated_margin,
            leverage=position.leverage,
            maintenance_margin_ratio=position.maintenance_margin_ratio,
            maintenance_margin_deduction=position.maintenance_margin_deduction,
            liquidation_price=position.liquidation_price,
            breakeven_done=position.breakeven_done,
            partial_done=True,
        )
        self._closed_positions.append(closed_leg)
        position.size = position.size - closed_size
        position.entry_fee -= allocated_entry_fee
        position.funding -= partial_funding
        position.slippage_cost -= allocated_entry_slippage
        position.initial_margin -= allocated_margin
        position.partial_done = True

    def _settle_funding(self, position: SimPosition, timestamp: int) -> None:
        """Apply settlement events crossed since the previous simulator timestamp."""
        model = self._cost_model
        if model.funding_mode == "off" or timestamp <= position.entry_ts:
            return
        historical = (model.historical_funding_rates or {}).get(position.symbol) or []
        if model.funding_mode == "historical" and historical:
            rate_sum = sum(
                float(record["rate"])
                for record in historical
                if position.entry_ts < int(record["ts"])
                and position.funding_settled_through_ts < int(record["ts"]) <= timestamp
            )
            payment = position.size * position.entry_price * rate_sum
            if not position.is_long:
                payment = -payment
        else:
            interval_ms = model.funding_interval_ms
            due_intervals = (
                max(timestamp - position.entry_ts, 0) // interval_ms
                if interval_ms > 0 else 0
            )
            new_intervals = max(due_intervals - position.funding_intervals_paid, 0)
            payment = model.funding_payment(
                position.size * position.entry_price,
                is_long=position.is_long,
                intervals=int(new_intervals),
            )
            position.funding_intervals_paid = int(due_intervals)
        if payment:
            position.funding += payment
            self._cash -= payment
        position.funding_settled_through_ts = timestamp

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
            if candle.ts <= position.entry_ts:
                continue
            self._settle_funding(position, candle.ts)
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

        # 2b. Cross-margin account-level liquidation (all positions share equity).
        if self._margin_mode == "cross" and self._open_positions:
            prices = {candle.ts: candle.close}
            if self.cross_margin_liquidation_triggered(prices):
                self._liquidate_cross_account(prices, candle.ts)

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
            if len(history) > 1440:
                del history[:-1440]

        # 1. Check TP/SL for each open position against its symbol's candle.
        for position in list(self._open_positions):
            candle = prices.get(position.symbol)
            if candle is None:
                continue
            if candle.ts <= position.entry_ts:
                continue
            self._settle_funding(position, candle.ts)
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

        # 2. Cross-margin account-level liquidation (all positions share equity).
        current_prices = {sym: c.close for sym, c in prices.items()}
        ts = next(iter(prices.values())).ts if prices else 0
        if self._margin_mode == "cross" and self._open_positions:
            if self.cross_margin_liquidation_triggered(current_prices):
                self._liquidate_cross_account(current_prices, ts)

        # 3. Record equity curve point.
        eq = self.equity(current_prices)
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
        if position.liquidation_price is not None:
            liq = position.liquidation_price
            if position.is_long and candle.low <= liq:
                self._close_position(position, min(candle.open, liq), candle.ts, "liquidation")
                return True
            if not position.is_long and candle.high >= liq:
                self._close_position(position, max(candle.open, liq), candle.ts, "liquidation")
                return True

        if position.is_long:
            # Stop loss: price dropped to/below SL
            if position.sl_price is not None and candle.low <= position.sl_price:
                stop_fill = min(candle.open, position.sl_price)
                self._close_position(position, stop_fill, candle.ts, "sl")
                return True
            # Take profit: price rose to/above TP
            if position.tp_price is not None and candle.high >= position.tp_price:
                target_fill = max(candle.open, position.tp_price)
                self._close_position(position, target_fill, candle.ts, "tp")
                return True
        else:  # short
            # Stop loss: price rose to/above SL
            if position.sl_price is not None and candle.high >= position.sl_price:
                stop_fill = max(candle.open, position.sl_price)
                self._close_position(position, stop_fill, candle.ts, "sl")
                return True
            # Take profit: price dropped to/below TP
            if position.tp_price is not None and candle.low <= position.tp_price:
                target_fill = min(candle.open, position.tp_price)
                self._close_position(position, target_fill, candle.ts, "tp")
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
            if candle.ts <= position.entry_ts:
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
