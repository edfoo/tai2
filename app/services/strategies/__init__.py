"""Pluggable strategy interface for the Launcher.

Each strategy evaluates the current market snapshot and returns a directional
signal ("buy", "sell", or None).  All enabled strategies run concurrently on
each scheduler tick — multiple strategies can fire on the same symbol at the
same time, each opening its own position with its own TP/SL.

To add a new strategy:
  1. Create a new file in ``app/services/strategies/`` implementing the
     ``Strategy`` protocol.
  2. Register it in ``MarketService._strategies``.
  3. Add a card on the STRATEGY page in ``pages.py`` with its own
     enable/disable switch and config fields.
  4. The config is namespaced under ``config["strategies"][<strategy_name>]``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass
class StrategySignal:
    """Signal returned by a strategy evaluation.

    Attributes
    ----------
    direction:
        "buy" or "sell".
    strategy_name:
        Name of the strategy that fired (set automatically by the registry).
    tp_pct:
        Optional take-profit % for this strategy.  If None, the Launcher's
        global TP/SL or algo orders are used.
    sl_pct:
        Optional stop-loss % for this strategy.
    rationale:
        Human-readable reason for the signal (shown in logs/notifications).
    """

    direction: str
    strategy_name: str = ""
    tp_pct: float | None = None
    sl_pct: float | None = None
    rationale: str = ""


@runtime_checkable
class Strategy(Protocol):
    """Minimal interface that every Launcher strategy must implement."""

    name: str

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        """Return a StrategySignal, or None if no signal fires.

        Parameters
        ----------
        symbol:
            Trading pair, e.g. ``"BTC-USDT-SWAP"``.
        snapshot:
            The full market snapshot (``_last_full_snapshot``).
        config:
            This strategy's own config dict, already namespaced
            (e.g. ``config["strategies"]["mean_reversion"]``).
        helpers:
            Shared utility methods from MarketService (price extraction,
            debug emit, etc.) so strategies don't need a direct reference
            to MarketService.

        Returns
        -------
        A ``StrategySignal`` with direction "buy" or "sell", or ``None``.
        """
        ...


class StrategyHelpers:
    """Lightweight helper bag passed to every strategy.

    Avoids giving strategies a full MarketService reference while still
    providing the utilities they need.
    """

    def __init__(
        self,
        *,
        extract_float: Any,
        emit_debug: Any,
        get_last_price: Any,
        compute_footprint: Any | None = None,
    ) -> None:
        self._extract_float = extract_float
        self._emit_debug = emit_debug
        self._get_last_price = get_last_price
        self._compute_footprint = compute_footprint

    def extract_float(self, value: Any) -> float | None:
        return self._extract_float(value)

    def emit_debug(self, msg: str) -> None:
        self._emit_debug(msg)

    def get_last_price(self, symbol: str) -> float | None:
        return self._get_last_price(symbol)

    def compute_footprint(self, symbol: str) -> dict[str, Any]:
        """Compute footprint data for a symbol, or return empty dict if unavailable."""
        if self._compute_footprint is not None:
            return self._compute_footprint(symbol)
        return {}
