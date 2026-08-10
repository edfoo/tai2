"""Centralised Stop-Loss / Take-Profit manager.

The previous codebase scattered TP/SL sizing logic across individual
strategies.  This module consolidates all placement rules so that risk
management becomes consistent and testable.

Public API
==========

``calculate(entry, side, ctx) -> tuple[float, float]``
    Compute *tp_price* and *sl_price* for a given trade direction.

Design Goals
------------
1. **Structure-first**  – If swing levels or volume-profile nodes are
   available, anchor stops beyond those levels; otherwise fall back to ATR.
2. **Volatility regime scaling** – ATR multipliers widen during expansion
   phases (`atr_htf` above its rolling median × 1.3) and tighten in chop.
3. **Reward-to-risk sanity** – Return *None* when the computed RR < 1.8 so
   the caller can abort the trade.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Final, Literal, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


Side = Literal["long", "short"]


@dataclass(slots=True)
class OrderContext:
    """Lightweight context object passed from the strategy/launcher.

    The strategy supplies its *own* structural levels (``tp_target`` /
    ``sl_level``) that encode its thesis (e.g. TP at VWAP, SL beyond the
    sweep wick).  When those are absent, ``calculate()`` falls back to the
    generic swing/VPOC structure, then to ATR.
    """

    atr_tf_pct: float  # ATR% on trade timeframe (e.g. 15m)
    atr_htf_pct: float  # ATR% on higher timeframe (e.g. 4H). 0 if unavailable.

    vpoc: Optional[float]  # volume POC price of the current session/day
    value_area_width: Optional[float]  # VAH – VAL distance

    swing_high: Optional[float]  # recent swing high price
    swing_low: Optional[float]  # recent swing low price

    last_price: float  # current midpoint/close

    # Strategy-provided structural levels (encode the strategy thesis).
    tp_target: Optional[float] = None  # absolute take-profit price
    sl_level: Optional[float] = None  # absolute stop-loss price

    # ATR clamp / buffer parameters (mirror the old per-strategy config).
    structural_sl_buffer_atr: float = 0.15
    atr_min_tp_mult: float = 0.5
    atr_max_tp_mult: float = 4.0
    atr_min_sl_mult: float = 0.3
    atr_max_sl_mult: float = 3.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _volatility_multiplier(atr_htf: float) -> float:
    """Return dynamic SL/TP ATR multiplier based on HTF volatility."""

    if math.isnan(atr_htf) or atr_htf <= 0:
        return 1.8  # fallback

    # Compare to rolling median would require history; assume caller passed
    # *relative* ratio (>1 = expansion). For now treat >1.3 as expansion.
    return 3.0 if atr_htf > 1.3 else 1.8


def _ensure_rr(entry: float, tp: float, sl: float, side: Side) -> bool:
    rr = (abs(tp - entry) / abs(sl - entry)) if abs(sl - entry) > 0 else 0
    return rr >= 1.8 and ((tp > entry and side == "long") or (tp < entry and side == "short"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def calculate(entry: float, side: Side, ctx: OrderContext) -> Tuple[float, float]:
    """Return (tp_price, sl_price).

    Placement priority (top-down):
      1. Strategy-provided structural levels (``ctx.tp_target`` /
         ``ctx.sl_level``) — these encode the strategy thesis.
      2. Generic structure (swing levels / VPOC).
      3. ATR fallback.

    All distances are clamped to ``[atr_min_*_mult, atr_max_*_mult] × ATR%``
    so extreme structural levels can't produce unreachable or razor-thin
    exits.

    Raises
    ------
    ValueError
        When the reward-to-risk is < 1.8 or insufficient data supplied.
    """

    atr_mult: Final[float] = _volatility_multiplier(ctx.atr_htf_pct)
    atr_price = (ctx.atr_tf_pct / 100.0) * ctx.last_price

    # -------------------------------------------------------------------
    # 1. Strategy-provided structural levels (thesis-specific)
    # -------------------------------------------------------------------
    sl = ctx.sl_level
    tp = ctx.tp_target

    # -------------------------------------------------------------------
    # 2. Generic structure fallback (swing / VPOC)
    # -------------------------------------------------------------------
    if sl is None:
        if side == "long" and ctx.swing_low is not None:
            sl = ctx.swing_low - (ctx.value_area_width or 0.0)
        elif side == "short" and ctx.swing_high is not None:
            sl = ctx.swing_high + (ctx.value_area_width or 0.0)

    if tp is None:
        if ctx.vpoc is not None and ctx.value_area_width is not None:
            tp = ctx.vpoc + (0.5 * ctx.value_area_width if side == "long" else -0.5 * ctx.value_area_width)

    # -------------------------------------------------------------------
    # 3. ATR fallback
    # -------------------------------------------------------------------
    atr_distance = atr_mult * ctx.atr_tf_pct * ctx.last_price / 100.0

    if sl is None:
        sl = entry - atr_distance if side == "long" else entry + atr_distance
    if tp is None:
        tp = entry + atr_distance if side == "long" else entry - atr_distance

    # -------------------------------------------------------------------
    # 4. Clamp structural distances to sane ATR bounds
    # -------------------------------------------------------------------
    if ctx.sl_level is None and ctx.swing_low is None and ctx.swing_high is None:
        # Pure ATR fallback — no clamping needed (already ATR-scaled).
        pass
    else:
        min_d = ctx.atr_min_sl_mult * atr_price
        max_d = ctx.atr_max_sl_mult * atr_price
        dist = abs(sl - entry)
        if dist < min_d:
            sl = entry - min_d if side == "long" else entry + min_d
        elif dist > max_d:
            sl = entry - max_d if side == "long" else entry + max_d

    if ctx.tp_target is None and ctx.vpoc is None:
        pass
    else:
        min_t = ctx.atr_min_tp_mult * atr_price
        max_t = ctx.atr_max_tp_mult * atr_price
        tdist = abs(tp - entry)
        if tdist < min_t:
            tp = entry + min_t if side == "long" else entry - min_t
        elif tdist > max_t:
            tp = entry + max_t if side == "long" else entry - max_t

    # -------------------------------------------------------------------
    # 5. Reward-to-risk sanity
    # -------------------------------------------------------------------
    if not _ensure_rr(entry, tp, sl, side):
        raise ValueError("Unacceptable reward-to-risk < 1.8x")

    return round(tp, 6), round(sl, 6)


def compute_tp_sl_pct(
    entry: float,
    side: Side,
    ctx: OrderContext,
    static_tp_pct: float | None = None,
    static_sl_pct: float | None = None,
    atr_tp_multiplier: float | None = None,
    atr_sl_multiplier: float | None = None,
) -> tuple[float | None, float | None]:
    """Compute strategy-level TP% / SL%, honouring explicit static overrides.

    Priority
    --------
    1. If both ``static_tp_pct`` and ``static_sl_pct`` are provided, use them
       verbatim (user explicitly configured the exits).
    2. If both ATR multipliers are provided, use them directly:
       ``tp_pct = atr_tp_multiplier × atr_tf_pct``,
       ``sl_pct = atr_sl_multiplier × atr_tf_pct``.  This preserves the
       strategy's own ATR-scaled sizing (e.g. TP 2.0×ATR, SL 1.5×ATR).
    3. Otherwise derive dynamically from ``calculate()``.
    4. If dynamic sizing is rejected (reward-to-risk too low OR structure
       data insufficient), gracefully fall back to whatever static % is
       available (``None`` otherwise), rather than raising.

    This deliberately does *not* drop the trade on an unacceptable R:R at the
    strategy layer — the launcher guardrail (``require_reward_risk_ratio``)
    owns that decision, so valid signals reach it for evaluation.
    """

    if static_tp_pct is not None and static_sl_pct is not None:
        return static_tp_pct, static_sl_pct

    if atr_tp_multiplier is not None and atr_sl_multiplier is not None and ctx.atr_tf_pct > 0:
        return atr_tp_multiplier * ctx.atr_tf_pct, atr_sl_multiplier * ctx.atr_tf_pct

    try:
        tp_price, sl_price = calculate(entry, side, ctx)
        tp_pct = abs(tp_price - entry) / entry * 100.0
        sl_pct = abs(sl_price - entry) / entry * 100.0
        return tp_pct, sl_pct
    except ValueError as exc:
        # The dynamic (structural) exit was rejected — most commonly because
        # the reward-to-risk fell below the 1.8× floor in `calculate()`.  We
        # deliberately do NOT drop the trade here: the launcher guardrail
        # (`require_reward_risk_ratio`) is the single owner of the R:R
        # decision.  But we must surface the degradation so it is auditable
        # in /debug and the log file — otherwise positions execute at 1:1 or
        # worse without ever satisfying the intended reward-to-risk.
        logger.warning(
            "compute_tp_sl_pct: dynamic exit rejected for %s @ %.6f — %s. "
            "Falling back to static/ATR exits (tp=%s, sl=%s). The launcher "
            "R:R guardrail will re-evaluate the final geometry.",
            side,
            entry,
            exc,
            static_tp_pct,
            static_sl_pct,
        )
        return static_tp_pct, static_sl_pct
