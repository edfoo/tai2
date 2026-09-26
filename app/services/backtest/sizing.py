"""Deterministic position sizing shared by live-compatible backtest paths."""

from __future__ import annotations

import math
from typing import Any


def compute_order_size(
    *,
    requested_notional: float,
    entry_price: float,
    equity: float,
    stop_price: float | None = None,
    guardrails: dict[str, Any] | None = None,
    instrument: dict[str, Any] | None = None,
    symbol: str | None = None,
) -> tuple[float, float]:
    """Return ``(base_units, notional)`` after live sizing caps.

    ``ct_val`` converts OKX contracts to base units and ``lot_size`` is the
    exchange contract increment. ``max_position_pct`` and
    ``symbol_position_caps`` follow the live config's *fraction* convention
    (``max_position_pct=0.2`` means 20 percent — see
    ``ui/pages.py::_percent_to_fraction`` and ``market_service.py``'s
    ``base_max_pct``/``symbol_cap_pct``), unlike ``atr_risk_per_trade_pct``
    below, which live stores as a percent (divided by 100).
    """
    if requested_notional <= 0 or entry_price <= 0:
        return 0.0, 0.0
    rules = guardrails or {}
    spec = instrument or {}
    notional = requested_notional

    base_max_pct = _positive_float(rules.get("max_position_pct"))
    symbol_caps = rules.get("symbol_position_caps")
    symbol_cap_pct: float | None = None
    if isinstance(symbol_caps, dict) and symbol:
        symbol_cap_pct = _positive_float(
            symbol_caps.get(symbol) or symbol_caps.get(symbol.upper())
        )
    effective_max_pct: float | None = None
    for candidate in (base_max_pct, symbol_cap_pct):
        if candidate:
            effective_max_pct = candidate if effective_max_pct is None else min(effective_max_pct, candidate)
    if effective_max_pct and equity > 0:
        notional = min(notional, equity * effective_max_pct)

    risk_pct = _positive_float(rules.get("atr_risk_per_trade_pct"))
    if risk_pct and equity > 0 and stop_price and stop_price > 0:
        stop_fraction = abs(entry_price - stop_price) / entry_price
        if stop_fraction > 0:
            notional = min(notional, (equity * risk_pct / 100.0) / stop_fraction)

    contract_value = _positive_float(spec.get("ct_val")) or 1.0
    lot_size = _positive_float(spec.get("lot_size")) or 0.0
    contracts = notional / (entry_price * contract_value)
    max_contracts = _positive_float(spec.get("max_market_size"))
    tiers = spec.get("position_tiers") or []
    valid_tiers = [tier for tier in tiers if isinstance(tier, dict)]
    if valid_tiers:
        tier = next((
            tier for tier in valid_tiers
            if contracts <= (_positive_float(tier.get("max_size")) or float("inf"))
        ), valid_tiers[-1])
        tier_max_size = _positive_float(tier.get("max_size"))
        if tier_max_size:
            max_contracts = min(max_contracts, tier_max_size) if max_contracts else tier_max_size
    if max_contracts:
        contracts = min(contracts, max_contracts)
    if lot_size > 0:
        contracts = math.floor(contracts / lot_size) * lot_size
    min_size = _positive_float(spec.get("min_size")) or 0.0
    if min_size > 0 and contracts + 1e-12 < min_size:
        return 0.0, 0.0
    base_units = contracts * contract_value
    return base_units, contracts * contract_value * entry_price


def quantize_contracts(size: float, lot_size: float) -> float:
    """Floor an OKX contract quantity to its valid lot increment."""
    if size <= 0 or lot_size <= 0:
        return max(0.0, size)
    return math.floor(size / lot_size) * lot_size


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
