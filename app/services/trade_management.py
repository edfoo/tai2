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
2. **Volatility adaptation** – ATR-scaled exits use ``atr_tf_pct`` so they
   scale with the current volatility regime.
3. **Reward-to-risk sanity** – Raise when the computed RR falls below the
   strategy's ``min_reward_risk_ratio`` floor (default 1.8).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Final, Literal, Optional, Tuple

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

    # Minimum reward-to-risk the computed geometry must satisfy.  Strategies
    # may lower this (e.g. trend_pullback = 1.5); the default 1.8 is the
    # generic structural floor.
    min_reward_risk_ratio: float = 1.8


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ATR fallback multiplier for ``calculate()`` step 3 (pure ATR fallback).
# Historically this was a ``_volatility_multiplier(atr_htf)`` that tried to
# widen exits during "expansion" — but ``atr_htf_pct`` is an *absolute* ATR%
# (typically 2-8%), so a naive ``> 1.3`` comparison always resolved to 3.0
# and the "regime scaling" never actually happened.  Volatility adaptation is
# already provided by ``atr_tf_pct`` in the ATR sizing path, so this fallback
# uses a single honest constant.
ATR_FALLBACK_MULT: Final[float] = 1.8


def _ensure_rr(entry: float, tp: float, sl: float, side: Side, min_rr: float = 1.8) -> bool:
    rr = (abs(tp - entry) / abs(sl - entry)) if abs(sl - entry) > 0 else 0
    # Epsilon on the floor so an exact-boundary ratio (e.g. 1.5/1.0 multipliers
    # → 1.5 vs min_rr 1.5) is not rejected by floating-point round-off
    # (1.5 == 1.4999999... < 1.5).
    return rr >= min_rr - 1e-6 and ((tp > entry and side == "long") or (tp < entry and side == "short"))


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
        When the reward-to-risk is below ``ctx.min_reward_risk_ratio`` or
        insufficient data is supplied.
    """

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
    atr_distance = ATR_FALLBACK_MULT * ctx.atr_tf_pct * ctx.last_price / 100.0

    if sl is None:
        sl = entry - atr_distance if side == "long" else entry + atr_distance
    if tp is None:
        tp = entry + atr_distance if side == "long" else entry - atr_distance

    # -------------------------------------------------------------------
    # 4. Clamp distances to sane ATR bounds
    # -------------------------------------------------------------------
    min_d = ctx.atr_min_sl_mult * atr_price
    max_d = ctx.atr_max_sl_mult * atr_price
    dist = abs(sl - entry)
    if dist < min_d:
        sl = entry - min_d if side == "long" else entry + min_d
    elif dist > max_d:
        sl = entry - max_d if side == "long" else entry + max_d

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
    if not _ensure_rr(entry, tp, sl, side, ctx.min_reward_risk_ratio):
        raise ValueError(f"Unacceptable reward-to-risk < {ctx.min_reward_risk_ratio}x")

    return round(tp, 6), round(sl, 6)


def compute_tp_sl_pct(
    entry: float,
    side: Side,
    ctx: OrderContext,
    static_tp_pct: float | None = None,
    static_sl_pct: float | None = None,
    atr_tp_multiplier: float | None = None,
    atr_sl_multiplier: float | None = None,
    audit: Callable[[str], None] | None = None,
) -> tuple[float | None, float | None]:
    """Compute strategy-level TP% / SL%.

    Priority (structure-first, volatility-aware; static is a *fallback*):
    1. **Structural sizing** — the strategy's own thesis-specific levels
       (``ctx.tp_target`` / ``ctx.sl_level``, e.g. TP at VWAP / BB middle /
       swing, SL beyond the invalidation).  Computed via ``calculate()``,
       which clamps to ATR bounds and enforces R:R >= 1.8.  If the structural
       geometry is rejected (R:R too low or insufficient structure), fall
       through to ATR.
    2. **ATR sizing** — ``tp_pct = atr_tp_multiplier × atr_tf_pct``,
       ``sl_pct = atr_sl_multiplier × atr_tf_pct`` (volatility-adaptive).
    3. **Static %** — used only when neither structural nor ATR sizing is
       active (i.e. the user explicitly opted out of both).  This is a
       *fallback*, not the default: the canonical strategy defaults always
       populate ``tp_pct``/``sl_pct``, so a static-first priority would
       silently bypass the ATR/structural sizing the strategies are
       configured for (the live path was executing static 6%/4% or inverted
       2%/3% geometry instead of the tuned ATR/structural exits).
    4. **Last resort** — derive dynamically from ``calculate()`` (generic
       structure/ATR fallback) when no structural levels, ATR multipliers,
       or static % are available.

    This deliberately does *not* drop the trade on an unacceptable R:R at the
    strategy layer — the launcher guardrail (``require_reward_risk_ratio``)
    owns that decision, so valid signals reach it for evaluation.

    ``audit`` (optional) is called with a human-readable line describing which
    sizing source was used (structural/dynamic vs fallback), so strategies can
    surface the sizing source in /debug.  This makes it explicit *why* a
    structurally-sized trade was downgraded instead of silently re-sizing.
    """

    # 1. Structural sizing (thesis-specific levels present).
    if ctx.tp_target is not None or ctx.sl_level is not None:
        try:
            tp_price, sl_price = calculate(entry, side, ctx)
            tp_pct = abs(tp_price - entry) / entry * 100.0
            sl_pct = abs(sl_price - entry) / entry * 100.0
            if audit is not None:
                audit(f"sizing=structural tp_pct={tp_pct:.3f} sl_pct={sl_pct:.3f}")
            return tp_pct, sl_pct
        except ValueError as exc:
            # The structural exit was rejected — most commonly because the
            # reward-to-risk fell below the 1.8× floor in `calculate()`.  We
            # deliberately do NOT drop the trade here: the launcher guardrail
            # (`require_reward_risk_ratio`) is the single owner of the R:R
            # decision.  Fall through to ATR sizing (then static) so the
            # strategy's configured volatility-adaptive exits still apply.
            if audit is not None:
                audit(
                    f"sizing=structural-rejected ({exc}) — falling back to ATR/static"
                )
            logger.warning(
                "compute_tp_sl_pct: structural exit rejected for %s @ %.6f — %s. "
                "Falling back to ATR/static exits. The launcher R:R guardrail "
                "will re-evaluate the final geometry.",
                side,
                entry,
                exc,
            )

    # 2. ATR sizing — ``mult × ATR%``, defensively clamped to the strategy's
    # ``atr_max_tp_mult`` / ``atr_max_sl_mult`` so a wide multiplier on a
    # high-ATR asset cannot inflate the exit beyond intent.  (A raw
    # ``mult × ATR%`` early-return previously bypassed the clamp, producing
    # unreachably wide take-profits — e.g. a 3.0× multiplier on a ~6.6% ATR%.
    # The primary width correction is the tightened trend_pullback defaults;
    # this clamp is a backstop for any misconfigured/mis-merged multipliers.)
    if atr_tp_multiplier is not None and atr_sl_multiplier is not None and ctx.atr_tf_pct > 0:
        atr_price = (ctx.atr_tf_pct / 100.0) * entry
        tp_dist = min(atr_tp_multiplier * atr_price, ctx.atr_max_tp_mult * atr_price)
        sl_dist = min(atr_sl_multiplier * atr_price, ctx.atr_max_sl_mult * atr_price)
        tp_pct = tp_dist / entry * 100.0
        sl_pct = sl_dist / entry * 100.0
        if audit is not None:
            audit(
                f"sizing=atr tp_mult={atr_tp_multiplier} sl_mult={atr_sl_multiplier} "
                f"atr_pct={ctx.atr_tf_pct:.2f} (clamped)"
            )
        return tp_pct, sl_pct

    # 3. Static fallback (only when neither structural nor ATR is active).
    if static_tp_pct is not None and static_sl_pct is not None:
        if audit is not None:
            audit(f"sizing=static tp={static_tp_pct} sl={static_sl_pct}")
        return static_tp_pct, static_sl_pct

    # 4. Last resort: derive dynamically (generic structure/ATR fallback).
    try:
        tp_price, sl_price = calculate(entry, side, ctx)
        tp_pct = abs(tp_price - entry) / entry * 100.0
        sl_pct = abs(sl_price - entry) / entry * 100.0
        if audit is not None:
            audit(f"sizing=structural tp_pct={tp_pct:.3f} sl_pct={sl_pct:.3f}")
        return tp_pct, sl_pct
    except ValueError as exc:
        if audit is not None:
            audit(
                f"sizing=fallback ({exc}) tp={static_tp_pct} sl={static_sl_pct} "
                "(structural geometry rejected)"
            )
        return static_tp_pct, static_sl_pct
