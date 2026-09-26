"""Trading-cost model for backtesting (fees, slippage, funding).

Live trades on OKX perpetual swaps incur:
  * taker/maker fees on every fill (entry and exit), and
  * periodic funding payments while a position is open (every 8h for most
    perp contracts).

The backtest previously assumed zero cost, which flatters scalping strategies
(mean_reversion / spike_continuation) with tight, frequent TPs.  This module
provides a deterministic cost model so backtest PnL reflects what live would
actually net.

Config knobs:
  * ``taker_fee_bps``  — fee per taker fill, in basis points (default 5 = 0.05%).
  * ``maker_fee_bps``  — fee per maker fill (default 0; launcher uses market/taker).
    * ``slippage_bps``   — base adverse price move on entry/exit (default 0).
    * ``slippage_mode``  — fixed bps or a prior-OHLCV range/turnover proxy.
    * ``slippage_stress_multiplier`` — scales the *estimated* slippage (both
      modes) for adverse stress tests; 1.0 = unmodified estimate.
    * ``liquidation_fee_bps`` — extra fee charged on a liquidation fill
      (OKX charges a liquidation fee on top of the taker fee).
    * ``funding_mode``   — historical settlement events, constant fallback, or off.
    * ``funding_rate_pct``— constant fallback per interval (default 0.0 = zero;
                                                    0.01 means 0.01% of notional per interval).
  * ``funding_interval_ms`` — funding cadence (default 8h = 28_800_000 ms).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CostModel:
    taker_fee_bps: float = 0.0
    maker_fee_bps: float = 0.0
    slippage_bps: float = 0.0
    slippage_mode: str = "fixed"
    slippage_stress_multiplier: float = 1.0
    liquidity_impact_coefficient: float = 0.05
    candle_range_slippage_fraction: float = 0.1
    max_liquidity_slippage_bps: float = 500.0
    liquidation_fee_bps: float = 0.0
    funding_rate_pct: float = 0.0
    funding_interval_ms: int = 8 * 60 * 60 * 1000
    funding_mode: str = "historical"
    historical_funding_rates: dict[str, list[dict[str, float | int]]] | None = None

    def entry_price_for(
        self, price: float, is_long: bool, *, slippage_bps: float | None = None
    ) -> float:
        """Return the effective fill price after slippage."""
        bps = self.slippage_bps if slippage_bps is None else slippage_bps
        if bps <= 0:
            return price
        slip = bps / 10000.0
        # Longs buy at a slightly worse (higher) price; shorts sell lower.
        return price * (1.0 + slip) if is_long else price * (1.0 - slip)

    def exit_price_for(
        self, price: float, is_long: bool, *, slippage_bps: float | None = None
    ) -> float:
        """Return the effective exit fill price after slippage.

        Exits are adverse in the opposite direction of entries.
        """
        bps = self.slippage_bps if slippage_bps is None else slippage_bps
        if bps <= 0:
            return price
        slip = bps / 10000.0
        # Longs sell at a slightly worse (lower) price; shorts buy higher.
        return price * (1.0 - slip) if is_long else price * (1.0 + slip)

    def fee_for(self, notional: float, *, taker: bool = True) -> float:
        """Return the fee for a fill of ``notional`` (quote currency)."""
        bps = self.taker_fee_bps if taker else self.maker_fee_bps
        if bps <= 0:
            return 0.0
        return notional * bps / 10000.0

    def stressed_slippage_bps(self, bps: float) -> float:
        """Scale an estimated slippage figure by the stress multiplier.

        The multiplier is an explicit adverse-scenario knob: 1.0 leaves the
        estimate unchanged, 2.0 doubles it, 0.0 removes it.  It applies to
        both ``fixed`` and ``ohlcv_liquidity`` estimates so a candidate can be
        re-run under a harsher execution assumption without changing the
        underlying model.
        """
        if self.slippage_stress_multiplier <= 0:
            return 0.0
        return max(bps, 0.0) * self.slippage_stress_multiplier

    def liquidation_fee_for(self, notional: float) -> float:
        """Return the extra liquidation fee charged on a liquidation fill."""
        if self.liquidation_fee_bps <= 0:
            return 0.0
        return notional * self.liquidation_fee_bps / 10000.0

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
        if self.funding_rate_pct == 0 or intervals <= 0:
            return 0.0
        per_interval = notional * self.funding_rate_pct / 100.0
        total = per_interval * intervals
        # Longs pay positive funding; shorts receive it.
        return total if is_long else -total

    def funding_payment_between(
        self,
        notional: float,
        *,
        symbol: str,
        is_long: bool,
        entry_ts: int,
        close_ts: int,
    ) -> float:
        """Return funding charged at settlement timestamps in ``(entry, close]``.

        Historical rates are OKX decimal fractions and may be positive or
        negative. If historical mode has no records for the symbol, the
        configured constant rate is used as an explicit fallback.
        """
        if self.funding_mode == "off" or close_ts <= entry_ts:
            return 0.0
        rates = (self.historical_funding_rates or {}).get(symbol) or []
        if self.funding_mode == "historical" and rates:
            amount = sum(
                notional * float(row["rate"])
                for row in rates
                if entry_ts < int(row["ts"]) <= close_ts
            )
            return amount if is_long else -amount
        elapsed_ms = close_ts - entry_ts
        intervals = elapsed_ms // self.funding_interval_ms if self.funding_interval_ms > 0 else 0
        return self.funding_payment(
            notional,
            is_long=is_long,
            intervals=int(intervals),
        )
