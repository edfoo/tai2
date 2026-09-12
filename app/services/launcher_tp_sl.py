"""Pure launcher TP/SL resolution — shared by live and backtest.

Extracted from ``MarketService.build_launcher_decisions`` so that live
(``market_service.py``) and the backtest engine (``backtest/engine.py``) run
the *identical* decision logic instead of two hand-mirrored copies that can
silently drift apart. This module has no dependency on ``MarketService``
state; every input is passed in explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.services.strategies import resolve_analysis_block


def _extract_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class LauncherTpSl:
    """Result of resolving a strategy signal into launcher TP/SL prices."""

    action: str  # "BUY" | "SELL" (post-flip)
    tp_price: float | None
    sl_price: float | None
    disable_protection: bool = False
    skip_rr_guard: bool = False
    flipped: bool = False
    flip_tp_sl_active: bool = False
    dynamic_tp_source: str = "static"
    debug_lines: list[str] = field(default_factory=list)


def resolve_launcher_tp_sl(
    *,
    strategy_name: str,
    direction: str,  # "buy" | "sell" — pre-flip, from the strategy signal
    signal_tp_pct: float | None,
    signal_sl_pct: float | None,
    last_price: float,
    launcher_config: dict[str, Any],
    guardrails_config: dict[str, Any],
    sym_data: dict[str, Any] | None,
) -> LauncherTpSl:
    """Resolve a strategy signal's TP/SL prices exactly as the live launcher does.

    Mirrors (and replaces) the inline block previously in
    ``MarketService.build_launcher_decisions``: static/ATR/structural
    fallback, PnL%-mode conversion, Mean Reversion dynamic-TP, and the
    per-strategy direction-flip (``flip_launcher_direction`` /
    ``flip_tp_sl``).
    """
    gov = launcher_config or {}
    strategies_cfg = gov.get("strategies") or {}
    strat_cfg = strategies_cfg.get(strategy_name) or {}
    sym_data = sym_data or {}
    debug_lines: list[str] = []

    # TP/SL from the strategy signal, falling back to launcher-level.
    tp_pct = signal_tp_pct if signal_tp_pct is not None else _extract_float(gov.get("tp_pct"))
    sl_pct = signal_sl_pct if signal_sl_pct is not None else _extract_float(gov.get("sl_pct"))

    # Mean Reversion opt-out: if both adaptive sizing modes are explicitly
    # disabled and static TP/SL are both blank, do not attach protection.
    # This enables external trade management (e.g., Skimming-like logic)
    # without placing TP/SL algos on the order book.
    disable_protection = False
    if strategy_name == "mean_reversion":
        _mr_use_atr = bool(strat_cfg.get("use_atr_sizing", True))
        _mr_use_struct = bool(strat_cfg.get("use_structural_sizing", True))
        _mr_static_tp = _extract_float(strat_cfg.get("tp_pct"))
        _mr_static_sl = _extract_float(strat_cfg.get("sl_pct"))
        if not _mr_use_atr and not _mr_use_struct and _mr_static_tp is None and _mr_static_sl is None:
            disable_protection = True
            tp_pct = None
            sl_pct = None

    # PnL% mode: when enabled, the strategy TP/SL fields are interpreted
    # as Floating PnL % on margin (after leverage). Convert them to price
    # distances using the guardrail leverage: price% = PnL% / leverage.
    # This ONLY applies to static percentage targets. ATR and structural
    # sizing compute price-based levels directly (already in price %),
    # so they must NOT be divided by leverage.
    _pnl_pct_mode = bool(gov.get("tp_sl_in_pnl_pct", False))
    _guard_leverage = _extract_float((guardrails_config or {}).get("max_leverage"))
    if not _guard_leverage or _guard_leverage <= 0:
        _guard_leverage = _extract_float((guardrails_config or {}).get("min_leverage"))
    _uses_atr = bool(strat_cfg.get("use_atr_sizing", False))
    _uses_struct = bool(strat_cfg.get("use_structural_sizing", False))
    if (
        _pnl_pct_mode
        and _guard_leverage
        and _guard_leverage > 0
        and not _uses_atr
        and not _uses_struct
    ):
        if tp_pct is not None:
            tp_pct = tp_pct / _guard_leverage
        if sl_pct is not None:
            sl_pct = sl_pct / _guard_leverage
        debug_lines.append(
            f"PnL%→price: leverage={_guard_leverage:.1f}x → tp_pct={tp_pct} sl_pct={sl_pct}"
        )

    # Dynamic TP (Mean Reversion only): tighten TP using BB bandwidth.
    # Disabled when use_atr_sizing is True — ATR sizing already adapts
    # TP to volatility, so dynamic_tp would double-tighten it.
    effective_tp_pct = tp_pct
    dynamic_tp_source = "static"
    _mr_cfg = strategies_cfg.get("mean_reversion") or {}
    if strategy_name == "mean_reversion":
        dynamic_tp = bool(_mr_cfg.get("dynamic_tp", False))
        _mr_use_atr = bool(_mr_cfg.get("use_atr_sizing", False))
        if _mr_use_atr and dynamic_tp:
            dynamic_tp = False
            dynamic_tp_source = "skipped(atr_sizing active)"
        dynamic_tp_fraction = _extract_float(_mr_cfg.get("dynamic_tp_fraction")) or 0.7
        if dynamic_tp and tp_pct and tp_pct > 0:
            sym_indicators = resolve_analysis_block(sym_data, _mr_cfg)
            _bb = sym_indicators.get("bollinger_bands") or {}
            _bb_lower = _extract_float(_bb.get("lower"))
            _bb_upper = _extract_float(_bb.get("upper"))
            _bb_middle = _extract_float(_bb.get("middle"))
            if _bb_lower is not None and _bb_upper is not None and _bb_middle and _bb_middle > 0:
                _bw = (_bb_upper - _bb_lower) / _bb_middle * 100.0
                _lever = 1.0
                _dyn = (_bw / 2.0) * dynamic_tp_fraction * _lever
                if _dyn > 0:
                    effective_tp_pct = min(tp_pct, _dyn)
                    dynamic_tp_source = (
                        f"dynamic(bw={_bw:.2f}%,frac={dynamic_tp_fraction}) "
                        f"→ {_dyn:.2f}% → min={effective_tp_pct:.2f}%"
                    )
            else:
                dynamic_tp_source = "dynamic(BB unavailable, fallback to static)"

    tp_price: float | None = None
    sl_price: float | None = None
    if effective_tp_pct and effective_tp_pct > 0:
        tp_price = (
            last_price * (1 + effective_tp_pct / 100.0)
            if direction == "buy"
            else last_price * (1 - effective_tp_pct / 100.0)
        )
    if sl_pct and sl_pct > 0:
        sl_price = (
            last_price * (1 - sl_pct / 100.0)
            if direction == "buy"
            else last_price * (1 + sl_pct / 100.0)
        )

    action = "BUY" if direction == "buy" else "SELL"

    # Flip direction (Mean Reversion / VWAP Reversion / Liquidity Sweep /
    # Trend Pullback / Spike Continuation). Mirrors TP/SL around last_price
    # so they land on the correct side of entry for the flipped direction.
    _flip_cfg: dict[str, Any] = {}
    if strategy_name == "mean_reversion":
        _flip_cfg = _mr_cfg
    elif strategy_name in (
        "vwap_reversion", "liquidity_sweep", "trend_pullback", "spike_continuation",
    ):
        _flip_cfg = strategies_cfg.get(strategy_name) or {}
    # When flip_tp_sl is active, the TP/SL distances are swapped and the
    # resulting trade is deliberately inverted-R:R (taking the small profit
    # where it would normally stop out). We flag that trade so the execution
    # R:R guardrail is bypassed for it and it only.
    flip_tp_sl_active = False
    flipped = False
    if _flip_cfg:
        flip_dir = str(_flip_cfg.get("flip_launcher_direction") or "").strip().lower()
        if flip_dir in ("both", "from_long", "from_short"):
            should_flip = (
                flip_dir == "both"
                or (flip_dir == "from_long" and action == "BUY")
                or (flip_dir == "from_short" and action == "SELL")
            )
            if should_flip:
                flipped = True
                orig_action = action
                action = "SELL" if action == "BUY" else "BUY"
                if last_price and last_price > 0:
                    if bool(_flip_cfg.get("flip_tp_sl", False)):
                        flip_tp_sl_active = True
                        # Swap TP/SL distances instead of mirroring: the old
                        # TP distance becomes the new SL distance and vice
                        # versa. This inverts the R:R geometry of the flipped
                        # trade rather than preserving it.
                        tp_dist = abs(tp_price - last_price) if tp_price else None
                        sl_dist = abs(sl_price - last_price) if sl_price else None
                        if action == "BUY":
                            tp_price = round(last_price + sl_dist, 10) if sl_dist is not None else None
                            sl_price = round(last_price - tp_dist, 10) if tp_dist is not None else None
                        else:
                            tp_price = round(last_price - sl_dist, 10) if sl_dist is not None else None
                            sl_price = round(last_price + tp_dist, 10) if tp_dist is not None else None
                    else:
                        tp_price = round(2 * last_price - tp_price, 10) if tp_price else None
                        sl_price = round(2 * last_price - sl_price, 10) if sl_price else None
                debug_lines.append(
                    f"flip ({flip_dir}): {orig_action} → {action} tp={tp_price} sl={sl_price}"
                )

    return LauncherTpSl(
        action=action,
        tp_price=tp_price,
        sl_price=sl_price,
        disable_protection=disable_protection,
        skip_rr_guard=flip_tp_sl_active,
        flipped=flipped,
        flip_tp_sl_active=flip_tp_sl_active,
        dynamic_tp_source=dynamic_tp_source,
        debug_lines=debug_lines,
    )
