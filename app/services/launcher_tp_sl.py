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
from app.services.strategies.defaults import merged_config
from app.services.trade_management_rules import StrategyExitParams


def _extract_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp_leverage_to_guardrails(
    leverage: float, guardrails_config: dict[str, Any] | None
) -> float:
    """Clamp a strategy leverage to ``[min_leverage, max_leverage]``.

    Reads the guardrails the same way ``handle_llm_decision`` does: a missing
    ``min_leverage`` is 0, a missing or non-positive ``max_leverage`` is
    ``max(min_leverage, 1)``, and an inverted pair is swapped.
    """
    g = guardrails_config or {}
    min_lev = _extract_float(g.get("min_leverage")) or 0.0
    max_lev = _extract_float(g.get("max_leverage"))
    if max_lev is None or max_lev <= 0:
        max_lev = max(min_lev, 1.0)
    if max_lev < min_lev:
        min_lev, max_lev = max_lev, min_lev
    return max(min(leverage, max_lev), min_lev, 1.0)


def resolve_strategy_sizing(
    strat_cfg: dict[str, Any],
    guardrails_config: dict[str, Any] | None,
    equity: float | None,
) -> tuple[float | None, float | None, float | None, list[str]]:
    """Per-strategy leverage + margin sizing (wick_rejection plan §3.4).

    Returns ``(leverage, margin_usd, notional_usd, debug_lines)``.  All three
    are ``None`` when the strategy sets no ``leverage``.  ``leverage`` is
    clamped to the guardrails; notional is ``margin × clamped leverage`` so
    the margin at risk stays as configured when the clamp changes it.
    ``margin_usd``/``notional_usd`` are ``None`` when ``pct_equity`` mode has
    no equity to size from.
    """
    lines: list[str] = []
    lev_cfg = _extract_float(strat_cfg.get("leverage"))
    if lev_cfg is None or lev_cfg <= 0:
        return None, None, None, lines
    lev = clamp_leverage_to_guardrails(lev_cfg, guardrails_config)
    if abs(lev - lev_cfg) > 1e-9:
        lines.append(
            f"leverage {lev_cfg:g}x clamped to {lev:g}x by guardrails "
            f"(margin kept, notional recomputed)"
        )
    mode = str(strat_cfg.get("margin_mode") or "fixed_usdt").strip().lower()
    margin: float | None
    if mode == "pct_equity":
        pct = _extract_float(strat_cfg.get("margin_pct")) or 0.0
        if equity is None or equity <= 0 or pct <= 0:
            lines.append(f"margin_pct={pct:g}% but equity unavailable — cannot size")
            return lev, None, None, lines
        margin = equity * pct / 100.0
    else:
        margin = _extract_float(strat_cfg.get("margin_usdt"))
        if margin is None or margin <= 0:
            lines.append("margin_usdt not configured — cannot size")
            return lev, None, None, lines
    return lev, margin, margin * lev, lines


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
    # "full" (TP + SL required), "tp_only" (TP order only; SL handled in
    # software or off) or "none" (no protection orders).
    protection: str = "full"
    # Per-strategy sizing (wick_rejection §3.4); None → launcher.notional_usd
    # and the guardrail leverage.
    leverage_override: float | None = None
    margin_usd: float | None = None
    notional_override: float | None = None
    # Strategy sizing was configured but could not be resolved (no equity /
    # margin) — the caller must skip the entry rather than fall back.
    sizing_failed: bool = False
    # Strategy-owned exits (time stop, software emergency stop, own re-entry
    # cooldown), fixed at entry; None → global trade management only.
    strategy_exit: StrategyExitParams | None = None


def strategy_exit_params(
    merged: dict[str, Any], leverage: float | None
) -> StrategyExitParams | None:
    """Exit parameters for a strategy that owns its exits, at ``leverage``.

    ``merged`` is the strategy's ``merged_config``; None when the strategy has
    no ``emergency_stop_mode`` (global trade management only).  The software
    stop converts margin units at ``leverage`` (the leverage actually used);
    without one, the config ``leverage``.  Used at entry and when a position is
    re-hydrated after a restart.
    """
    if "emergency_stop_mode" not in merged:
        return None
    from app.services.strategies.wick_rejection import exit_price_pcts

    cfg = dict(merged)
    if leverage is not None and leverage > 0:
        cfg["leverage"] = leverage
    _tp, sl_pct = exit_price_pcts(cfg)
    stop_mode = str(merged.get("emergency_stop_mode") or "exchange").strip().lower()
    return StrategyExitParams(
        max_hold_seconds=max(0.0, (_extract_float(merged.get("max_hold_minutes")) or 0.0) * 60.0),
        software_stop_pct=sl_pct if stop_mode == "software" else None,
        reentry_cooldown_seconds=max(
            0.0, (_extract_float(merged.get("reentry_cooldown_minutes")) or 0.0) * 60.0
        ),
        use_global_trade_management=bool(merged.get("use_global_trade_management", False)),
    )


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
    equity: float | None = None,
) -> LauncherTpSl:
    """Resolve a strategy signal's TP/SL prices exactly as the live launcher does.

    Mirrors (and replaces) the inline block previously in
    ``MarketService.build_launcher_decisions``: static/ATR/structural
    fallback, PnL%-mode conversion, Mean Reversion dynamic-TP, and the
    per-strategy direction-flip (``flip_launcher_direction`` /
    ``flip_tp_sl``).

    Strategies with strategy-owned exits (``emergency_stop_mode`` in their
    defaults — wick_rejection) resolve TP and the emergency stop from their own
    config with unit conversion at the effective leverage, skip the PnL% mode,
    and may request TP-only protection.  ``equity`` is only used for
    ``margin_mode = "pct_equity"`` sizing.
    """
    gov = launcher_config or {}
    strategies_cfg = gov.get("strategies") or {}
    strat_cfg = strategies_cfg.get(strategy_name) or {}
    sym_data = sym_data or {}
    debug_lines: list[str] = []

    # TP/SL from the strategy signal, falling back to launcher-level.
    tp_pct = signal_tp_pct if signal_tp_pct is not None else _extract_float(gov.get("tp_pct"))
    sl_pct = signal_sl_pct if signal_sl_pct is not None else _extract_float(gov.get("sl_pct"))

    # Strategy-owned exits + per-strategy sizing (wick_rejection §3.2–3.4).
    merged = merged_config(strat_cfg, strategy_name)
    owned_exits = "emergency_stop_mode" in merged
    protection = "full"
    leverage_override = margin_usd = notional_override = None
    sizing_failed = False
    strategy_exit: StrategyExitParams | None = None
    if owned_exits:
        from app.services.strategies.wick_rejection import exit_price_pcts

        leverage_override, margin_usd, notional_override, _size_lines = resolve_strategy_sizing(
            merged, guardrails_config, equity
        )
        debug_lines.extend(_size_lines)
        sizing_failed = leverage_override is not None and notional_override is None
        # Margin-unit targets convert at the leverage actually used.
        _exit_cfg = dict(merged)
        if leverage_override is not None:
            _exit_cfg["leverage"] = leverage_override
        tp_pct, sl_pct = exit_price_pcts(_exit_cfg)
        stop_mode = str(merged.get("emergency_stop_mode") or "exchange").strip().lower()
        if stop_mode in ("software", "off"):
            protection = "tp_only"
            sl_pct = None
        strategy_exit = strategy_exit_params(merged, leverage_override)
        software_stop_pct = strategy_exit.software_stop_pct if strategy_exit else None
        debug_lines.append(
            f"strategy exits: tp={tp_pct}% stop={sl_pct if sl_pct is not None else software_stop_pct}% "
            f"({stop_mode}) hold={strategy_exit.max_hold_seconds if strategy_exit else 0:.0f}s "
            f"lev={leverage_override} margin={margin_usd} notional={notional_override}"
        )

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
    # Generic opt-out (any strategy): no TP/SL orders at all.
    if bool(merged.get("disable_protection", False)):
        disable_protection = True
        tp_pct = None
        sl_pct = None
    if disable_protection:
        protection = "none"

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
        and not owned_exits
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
        skip_rr_guard=flip_tp_sl_active or bool(merged.get("skip_rr_guard", False)),
        flipped=flipped,
        flip_tp_sl_active=flip_tp_sl_active,
        dynamic_tp_source=dynamic_tp_source,
        debug_lines=debug_lines,
        protection=protection,
        leverage_override=leverage_override,
        margin_usd=margin_usd,
        notional_override=notional_override,
        sizing_failed=sizing_failed,
        strategy_exit=strategy_exit,
    )


def snapshot_equity(snapshot: dict[str, Any] | None) -> float | None:
    """Account equity from a MarketService snapshot, or None when unknown.

    The equity figures sit at the top of the snapshot; ``snapshot["account"]``
    is the per-currency details list (a dict only in older snapshots).
    """
    snapshot = snapshot or {}
    account = snapshot.get("account")
    sources = [snapshot] + ([account] if isinstance(account, dict) else [])
    for src in sources:
        for key in ("account_equity", "total_eq_usd", "total_equity", "total_account_value"):
            try:
                value = float(src.get(key))
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
    return None


def wick_rejection_risk_summary(
    cfg: dict[str, Any], guardrails_config: dict[str, Any] | None, equity: float | None
) -> str:
    """Plain-text risk read-out for the STRATEGY card (plan §3.3 / §7).

    Uses the same sizing and exit maths as execution
    (``resolve_strategy_sizing`` / ``exit_price_pcts``): margin, leverage after
    the guardrail clamp, position size, TP and emergency-stop distance in
    USDT, the ``max_position_pct`` cap and the worst case across the
    concurrent positions.  ``cfg`` is a merged wick_rejection config.
    """
    from app.services.strategies.wick_rejection import exit_price_pcts

    guards = guardrails_config or {}
    lev, margin, notional, _ = resolve_strategy_sizing(cfg, guards, equity)
    lines: list[str] = []
    eq_txt = f"{equity:,.2f} USDT" if equity else "unknown"
    lines.append(f"Account equity: {eq_txt}")
    if lev is None:
        return "\n".join(lines + ["No leverage configured — launcher sizing applies."])
    if margin is None or notional is None:
        return "\n".join(lines + [
            "Margin is a % of equity and equity is unknown: entries are skipped until it is."
        ])
    cfg_lev = _extract_float(cfg.get("leverage")) or lev
    clamp = (
        f" (clamped from {cfg_lev:g}× by guardrail min/max leverage)"
        if abs(cfg_lev - lev) > 1e-9 else ""
    )
    lines.append(
        f"Per entry: margin {margin:,.2f} USDT × {lev:g}× = position {notional:,.2f} USDT{clamp}"
    )
    tp_pct, sl_pct = exit_price_pcts({**cfg, "leverage": lev})
    if tp_pct:
        lines.append(f"Take profit: {tp_pct:.2f}% price ≈ +{notional * tp_pct / 100:,.2f} USDT")
    mode = str(cfg.get("emergency_stop_mode") or "exchange").lower()
    stop_loss: float | None = None
    if sl_pct:
        stop_loss = notional * sl_pct / 100
        where = "OKX stop order" if mode == "exchange" else "bot closes at market; unprotected while the app is down"
        lines.append(
            f"Emergency stop ({mode}): {sl_pct:.2f}% price ≈ −{stop_loss:,.2f} USDT "
            f"({stop_loss / margin * 100:.0f}% of margin; {where})"
        )
    else:
        lines.append("Emergency stop: OFF — the loss is limited only by the time stop and liquidation")
    max_pos = _extract_float(guards.get("max_position_pct"))
    if max_pos and max_pos > 0 and equity:
        cap = max_pos * equity
        verdict = "OK" if notional <= cap + 1e-9 else f"position cut to {cap:,.2f} USDT"
        lines.append(f"Guardrail max_position_pct {max_pos:g} × equity = {cap:,.2f} USDT cap → {verdict}")
    n = int(_extract_float(cfg.get("max_concurrent_positions")) or 1)
    total_pct = _extract_float(cfg.get("max_total_margin_pct"))
    if equity and total_pct and total_pct > 0:
        fit = int((equity * total_pct / 100 + 1e-9) // margin) if margin > 0 else n
        if fit < n:
            lines.append(
                f"Total margin cap {total_pct:g}% of equity fits {fit} of {n} positions"
            )
            n = max(0, fit)
    if stop_loss is not None and n > 0:
        worst = stop_loss * n
        pct = f" ({worst / equity * 100:.1f}% of equity)" if equity else ""
        lines.append(f"Worst case, {n} position(s) all stopped: −{worst:,.2f} USDT{pct}")
    return "\n".join(lines)
