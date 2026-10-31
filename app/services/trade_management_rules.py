"""Pure trade-management decision rules — shared by live and backtest.

Extracted from ``MarketService._check_trade_management`` (live) and
``Simulator._apply_trade_management`` (backtest) so the breakeven /
partial-TP / trailing / software-stop / time-stop *arithmetic* lives in one
place and cannot drift apart. This module has no I/O and no dependency on
``MarketService`` or ``Simulator`` state; every input is passed explicitly.

Environment-specific concerns stay in each caller:

* Live computes ``pnl_pct`` / ``r_multiple`` from the current tick mark and
  applies decisions by submitting/moving exchange orders.
* Backtest computes them from the candle and applies decisions directly to a
  ``SimPosition`` (including an intrabar partial fill estimate).

The *thresholds and stop-price math* are identical in both.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# Canonical fallbacks — mirror the values live uses in
# ``_check_trade_management`` when a key is missing/blank. These deliberately
# do NOT follow ``strategies.defaults.DEFAULT_TRADE_MANAGEMENT`` (the UI's
# "recommended defaults") so this refactor does not change live behaviour.
_DEFAULTS: dict[str, Any] = {
    "breakeven_enabled": True,
    "breakeven_at_r": 0.7,
    "breakeven_buffer_pct": 0.05,
    "partial_tp_enabled": True,
    "partial_tp_at_r": 0.8,
    "partial_tp_fraction": 0.5,
    "time_stop_enabled": True,
    "time_stop_seconds": 2700.0,
    "time_stop_candles": 0,
    "time_stop_min_r": 0.3,
    "time_stop_underwater_only": True,
    "trailing_enabled": True,
    "trailing_activate_r": 0.8,
    "trailing_distance_atr": 1.5,
    "trailing_floor_r": 0.5,
    # Profit floor for the trailing stop as a PERCENTAGE OF NOTIONAL (== % of
    # entry price, since size cancels).  0.5 = lock the runner's stop 0.5%
    # above entry (long) / below entry (short).  When set it REPLACES the
    # R-based ``trailing_floor_r`` floor so the stop is always in profit.
    # 0/negative disables it and restores the legacy R-based floor.
    "trailing_floor_profit_pct": 0.5,
    "trailing_step_r": 0.2,
    "trailing_remove_tp": True,
    "trailing_far_tp_mult": None,
    "software_stop_loss_enabled": True,
}


def _extract_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class TradeMgmtParams:
    """Resolved trade-management parameters with defaults applied."""

    enabled: bool
    breakeven_enabled: bool
    breakeven_at_r: float
    breakeven_buffer_pct: float
    partial_tp_enabled: bool
    partial_tp_at_r: float
    partial_tp_fraction: float
    time_stop_enabled: bool
    time_stop_seconds: float
    time_stop_candles: int
    time_stop_min_r: float
    time_stop_underwater_only: bool
    trailing_enabled: bool
    trailing_activate_r: float
    trailing_distance_atr: float
    trailing_floor_r: float
    trailing_floor_profit_pct: float | None
    trailing_step_r: float
    trailing_remove_tp: bool
    trailing_far_tp_mult: float | None
    software_stop_loss_enabled: bool


def resolve_tm_params(tm: dict[str, Any] | None) -> TradeMgmtParams:
    """Resolve a ``trade_management`` config dict into typed params + defaults."""
    tm = tm or {}

    def f(key: str) -> float | None:
        return _extract_float(tm.get(key))

    def fb(key: str) -> float:
        val = f(key)
        return _DEFAULTS[key] if val is None else val

    far_tp = f("trailing_far_tp_mult")
    if far_tp is not None and far_tp <= 0:
        far_tp = None

    # Profit floor as a percentage of notional.  Defaults to the canonical
    # 0.5% so the runner's stop is locked in profit out of the box; an explicit
    # non-positive value disables it and restores the R-based floor.
    floor_profit_pct = fb("trailing_floor_profit_pct")
    if floor_profit_pct <= 0:
        floor_profit_pct = None

    return TradeMgmtParams(
        enabled=bool(tm.get("enabled", True)),
        breakeven_enabled=bool(tm.get("breakeven_enabled", _DEFAULTS["breakeven_enabled"])),
        breakeven_at_r=fb("breakeven_at_r"),
        breakeven_buffer_pct=fb("breakeven_buffer_pct"),
        partial_tp_enabled=bool(tm.get("partial_tp_enabled", _DEFAULTS["partial_tp_enabled"])),
        partial_tp_at_r=fb("partial_tp_at_r"),
        partial_tp_fraction=fb("partial_tp_fraction"),
        time_stop_enabled=bool(tm.get("time_stop_enabled", _DEFAULTS["time_stop_enabled"])),
        time_stop_seconds=fb("time_stop_seconds"),
        time_stop_candles=int(tm.get("time_stop_candles") or 0),
        time_stop_min_r=fb("time_stop_min_r"),
        time_stop_underwater_only=bool(
            tm.get("time_stop_underwater_only", _DEFAULTS["time_stop_underwater_only"])
        ),
        trailing_enabled=bool(tm.get("trailing_enabled", _DEFAULTS["trailing_enabled"])),
        trailing_activate_r=fb("trailing_activate_r"),
        trailing_distance_atr=fb("trailing_distance_atr"),
        trailing_floor_r=fb("trailing_floor_r"),
        trailing_floor_profit_pct=floor_profit_pct,
        trailing_step_r=fb("trailing_step_r"),
        trailing_remove_tp=bool(tm.get("trailing_remove_tp", _DEFAULTS["trailing_remove_tp"])),
        trailing_far_tp_mult=far_tp,
        software_stop_loss_enabled=bool(
            tm.get("software_stop_loss_enabled", _DEFAULTS["software_stop_loss_enabled"])
        ),
    )


def compute_breakeven_sl(
    *,
    is_long: bool,
    entry_price: float,
    breakeven_buffer_pct: float,
) -> float:
    """Return the breakeven stop price for a position."""
    if is_long:
        return entry_price * (1.0 + breakeven_buffer_pct / 100.0)
    return entry_price * (1.0 - breakeven_buffer_pct / 100.0)


def compute_trailing_sl(
    *,
    is_long: bool,
    entry_price: float,
    mark_price: float,
    risk_pct: float,
    atr_pct: float,
    trailing_distance_atr: float,
    trailing_floor_r: float,
    trailing_floor_profit_pct: float | None = None,
) -> tuple[float, float]:
    """Return ``(new_sl, floor_sl)`` for the trailing stop.

    Mirrors live: the trail distance is ``trailing_distance_atr × atr%`` behind
    the mark, clamped so it never crosses the floor.

    If ``trailing_floor_profit_pct`` is set (a PERCENTAGE OF NOTIONAL, which
    equals a percentage of entry price because size cancels), the floor is
    placed that far above entry (long) or below entry (short) so the runner's
    stop is always locked in profit.  Otherwise it falls back to
    ``trailing_floor_r`` (legacy R-based floor, which sits *below* entry for a
    long and is therefore a loss).

    A floor at/above the mark (long) or at/below it (short) would trigger
    instantly, so when the profit floor is unreachable — e.g. a 0.5% floor on a
    position whose mark has not yet moved that far — it is ignored and the
    trail distance (the tightest valid stop) is used instead.
    """
    trail_dist = trailing_distance_atr * atr_pct / 100.0 * mark_price
    if is_long:
        trail_sl = mark_price - trail_dist
        if trailing_floor_profit_pct is not None:
            floor_sl = entry_price * (1.0 + trailing_floor_profit_pct / 100.0)
        else:
            floor_sl = entry_price * (1.0 - trailing_floor_r * risk_pct / 100.0)
        # Unreachable floor → fall back to the trail (never place at/above mark).
        if floor_sl >= mark_price:
            floor_sl = trail_sl
        return max(trail_sl, floor_sl), floor_sl
    trail_sl = mark_price + trail_dist
    if trailing_floor_profit_pct is not None:
        floor_sl = entry_price * (1.0 - trailing_floor_profit_pct / 100.0)
    else:
        floor_sl = entry_price * (1.0 + trailing_floor_r * risk_pct / 100.0)
    # Unreachable floor → fall back to the trail (never place at/below mark).
    if floor_sl <= mark_price:
        floor_sl = trail_sl
    return min(trail_sl, floor_sl), floor_sl


def should_ratchet_sl(
    *,
    is_long: bool,
    current_sl: float | None,
    new_sl: float,
    step_distance: float,
) -> bool:
    """Return whether ``new_sl`` is a strict enough improvement to re-place the stop."""
    if current_sl is None:
        return True
    if is_long:
        return new_sl > current_sl + step_distance
    return new_sl < current_sl - step_distance


def compute_far_tp(
    *,
    is_long: bool,
    entry_price: float,
    risk_pct: float,
    trailing_far_tp_mult: float,
) -> float:
    """Return the far-out safety TP (in R multiples beyond entry)."""
    if is_long:
        return entry_price * (1.0 + trailing_far_tp_mult * risk_pct / 100.0)
    return entry_price * (1.0 - trailing_far_tp_mult * risk_pct / 100.0)


@dataclass(slots=True)
class TradeMgmtDecision:
    """The sequence of trade-management actions to apply for a position.

    All fields are computed from pure inputs.  ``software_stop`` / ``timeout``
    are full-close triggers; the rest are state adjustments.
    """

    software_stop: bool = False
    breakeven_new_sl: float | None = None
    partial_trigger: bool = False
    partial_fraction: float = 0.0
    trailing_new_sl: float | None = None
    trailing_remove_tp: bool = False
    trailing_far_tp: float | None = None
    timeout: bool = False


def compute_trade_management_decision(
    params: TradeMgmtParams,
    *,
    is_long: bool,
    entry_price: float,
    mark_price: float,
    risk_pct: float | None,
    pnl_pct: float,
    r_multiple: float | None,
    atr_pct: float | None,
    current_sl: float | None,
    breakeven_done: bool,
    partial_done: bool,
    timed_out: bool = False,
) -> TradeMgmtDecision:
    """Compute trade-management actions for one position.

    Parameters
    ----------
    is_long:
        Position direction.
    entry_price:
        Average entry price.
    mark_price:
        Current mark / last price.  Used for the trailing-stop distance so the
        arithmetic matches live exactly (no derivation from ``pnl_pct``).
    risk_pct:
        SL distance from entry as a percentage (the "1R" unit).  None/<=0
        disables R-relative rules (breakeven, partial, trailing, software stop).
    pnl_pct:
        Current PnL as a percentage (mark-relative; the caller chooses the mark).
    r_multiple:
        ``pnl_pct / risk_pct`` (or the caller's excursion-based equivalent).
    atr_pct:
        ATR percentage on the trade timeframe; used for the trailing distance.
    current_sl:
        The position's current SL price (for ratchet comparisons).
    breakeven_done / partial_done:
        Whether the breakeven / partial-TP rungs have already fired.
    timed_out:
        Whether the position has exceeded its hold time.  Computed by the caller
        (live = wall-clock ``time_stop_seconds``, backtest = ``time_stop_candles``).
    """
    d = TradeMgmtDecision()

    if not params.enabled:
        return d

    risk = risk_pct if (risk_pct is not None and risk_pct > 0) else None
    r = r_multiple if (r_multiple is not None and risk is not None) else None

    # ── Software-stop loss ────────────────────────────────────────────
    if (
        params.software_stop_loss_enabled
        and risk is not None
        and pnl_pct <= -risk
    ):
        d.software_stop = True
        return d

    # ── Breakeven stop ────────────────────────────────────────────────
    if (
        params.breakeven_enabled
        and not breakeven_done
        and r is not None
        and r >= params.breakeven_at_r
    ):
        new_sl = compute_breakeven_sl(
            is_long=is_long,
            entry_price=entry_price,
            breakeven_buffer_pct=params.breakeven_buffer_pct,
        )
        should_move = True
        if current_sl is not None:
            if is_long and new_sl <= current_sl:
                should_move = False
            if not is_long and new_sl >= current_sl:
                should_move = False
        if should_move:
            d.breakeven_new_sl = new_sl

    # ── Partial take-profit ───────────────────────────────────────────
    if (
        params.partial_tp_enabled
        and not partial_done
        and r is not None
        and r >= params.partial_tp_at_r
        and 0.0 < params.partial_tp_fraction < 1.0
    ):
        d.partial_trigger = True
        d.partial_fraction = params.partial_tp_fraction

    # ── Trailing stop (asymmetric exit on the remainder) ─────────────
    if (
        params.trailing_enabled
        and params.trailing_distance_atr > 0
        and r is not None
        and r >= params.trailing_activate_r
        and risk is not None
    ):
        atr = atr_pct if (atr_pct is not None and atr_pct > 0) else risk
        new_sl, _floor_sl = compute_trailing_sl(
            is_long=is_long,
            entry_price=entry_price,
            mark_price=mark_price,
            risk_pct=risk,
            atr_pct=atr,
            trailing_distance_atr=params.trailing_distance_atr,
            trailing_floor_r=params.trailing_floor_r,
            trailing_floor_profit_pct=params.trailing_floor_profit_pct,
        )
        step_distance = params.trailing_step_r * risk / 100.0 * entry_price
        if should_ratchet_sl(
            is_long=is_long,
            current_sl=current_sl,
            new_sl=new_sl,
            step_distance=step_distance,
        ):
            d.trailing_new_sl = new_sl
            d.trailing_remove_tp = params.trailing_remove_tp and partial_done
            if not d.trailing_remove_tp and params.trailing_far_tp_mult is not None:
                d.trailing_far_tp = compute_far_tp(
                    is_long=is_long,
                    entry_price=entry_price,
                    risk_pct=risk,
                    trailing_far_tp_mult=params.trailing_far_tp_mult,
                )

    # ── Time stop ─────────────────────────────────────────────────────
    # ``timed_out`` is fully computed by the caller (duration + progress +
    # underwater conditions) because the trigger differs by environment:
    # live uses wall-clock seconds + mark-based R, backtest uses candle count.
    if timed_out:
        d.timeout = True

    return d
