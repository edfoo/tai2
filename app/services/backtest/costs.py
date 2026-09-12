"""Trading-cost model for backtesting (fees, slippage, funding).

Live trades on OKX perpetual swaps incur:
  * taker/maker fees on every fill (entry and exit), and
  * periodic funding payments while a position is open (every 8h for most
    perp contracts).

The backtest previously assumed zero cost, which flatters scalping strategies
(mean_reversion / spike_continuation) with tight, frequent TPs.  This module
provides a deterministic cost model so backtest PnL reflects what live would
actually net.

Config knobs (all opt-in with conservative defaults):
  * ``taker_fee_bps``  — fee per taker fill, in basis points (default 5 = 0.05%).
  * ``maker_fee_bps``  — fee per maker fill (default 0; launcher uses market/taker).
  * ``slippage_bps``   — adverse price move on entry/exit (default 0).
  * ``funding_rate_pct``— assumed funding rate per interval (default 0.0 = off;
                          OKX funding is paid every 8h, so a value like 0.01
                          means 0.01% of notional per 8h interval).
  * ``funding_interval_ms`` — funding cadence (default 8h = 28_800_000 ms).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CostModel:
    taker_fee_bps: float = 0.0
    maker_fee_bps: float = 0.0
    slippage_bps: float = 0.0
    funding_rate_pct: float = 0.0
    funding_interval_ms: int = 8 * 60 * 60 * 1000

    def entry_price_for(self, price: float, is_long: bool) -> float:
        """Return the effective fill price after slippage."""
        if self.slippage_bps <= 0:
            return price
        slip = self.slippage_bps / 10000.0
        # Longs buy at a slightly worse (higher) price; shorts sell lower.
        return price * (1.0 + slip) if is_long else price * (1.0 - slip)

    def exit_price_for(self, price: float, is_long: bool) -> float:
        """Return the effective exit fill price after slippage.

        Exits are adverse in the opposite direction of entries.
        """
        if self.slippage_bps <= 0:
            return price
        slip = self.slippage_bps / 10000.0
        # Longs sell at a slightly worse (lower) price; shorts buy higher.
        return price * (1.0 - slip) if is_long else price * (1.0 + slip)

    def fee_for(self, notional: float, *, taker: bool = True) -> float:
        """Return the fee for a fill of ``notional`` (quote currency)."""
        bps = self.taker_fee_bps if taker else self.maker_fee_bps
        if bps <= 0:
            return 0.0
        return notional * bps / 10000.0

    def funding_payment(
        self,
        notional: float,
        *,
        is_long: bool,
        intervals: int,
    ) -> float:
        """Return the funding payment accrued over ``intervals`` for a position.

        Positive funding rate → longs pay, shorts receive.  We model a single
        assumed rate for the whole backtest (no historical funding series).
        """
        if self.funding_rate_pct <= 0 or intervals <= 0:
            return 0.0
        per_interval = notional * self.funding_rate_pct / 100.0
        total = per_interval * intervals
        # Longs pay positive funding; shorts receive it.
        return total if is_long else -total
