"""Bid-ask spread estimators from the public trade tape.

OKX does not expose historical order-book depth, so true market-impact
calibration is impossible.  It *does* expose the historical public trade tape
(``/api/v5/market/history-trades``), from which the **effective bid-ask
spread** can be estimated with standard microstructure estimators:

  * **Corwin-Schultz (2012)** — high-low spread estimator.  Uses the fact that
    a bid-ask bounce inflates the observed high-low range relative to the true
    variance.  Works from OHLC bars (no tape needed) and is robust to sparse
    trades.
  * **Roll (1984)** — serial-covariance estimator.  The bid-ask bounce induces
    negative first-order autocovariance in transaction-price changes; the
    spread is recovered from that covariance.  Needs the trade tape.

Both return a spread in **basis points** of price.  This is a *spread*
estimate, not order-book depth or market impact: it captures the cost of
crossing the spread but not the price concession from consuming size.  It is
still a genuine improvement over an arbitrary constant, because it is derived
from observed market data and varies over time.

The estimators are pure functions over candle/trade sequences so they can be
unit-tested without network access.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Sequence

# Corwin-Schultz constant: 3 - 2*sqrt(2).
_CS_K = 3.0 - 2.0 * math.sqrt(2.0)


def _clamp_non_negative(value: float) -> float:
    return value if value > 0 else 0.0


def corwin_schultz_spread_bps(
    highs: Sequence[float],
    lows: Sequence[float],
) -> float | None:
    """Estimate the average bid-ask spread (bps) via Corwin-Schultz.

    ``highs``/``lows`` are consecutive bar highs/lows.  Returns ``None`` when
    fewer than two bars are supplied or the inputs are degenerate.  Negative
    per-pair estimates (which occur when the range is not inflated) are
    floored at zero, matching the paper's recommendation.
    """
    if len(highs) < 2 or len(lows) < 2 or len(highs) != len(lows):
        return None
    estimates: list[float] = []
    for i in range(len(highs) - 1):
        h1, l1 = highs[i], lows[i]
        h2, l2 = highs[i + 1], lows[i + 1]
        if min(h1, l1, h2, l2) <= 0:
            continue
        beta = math.log(h1 / l1) ** 2 + math.log(h2 / l2) ** 2
        gamma = math.log(max(h1, h2) / min(l1, l2)) ** 2
        if beta <= 0:
            continue
        alpha = (
            (math.sqrt(2.0 * beta) - math.sqrt(beta)) / _CS_K
            - math.sqrt(gamma / _CS_K)
        )
        spread = 2.0 * (math.exp(alpha) - 1.0) / (1.0 + math.exp(alpha))
        estimates.append(_clamp_non_negative(spread))
    if not estimates:
        return None
    return sum(estimates) / len(estimates) * 10_000.0


def roll_spread_bps(prices: Sequence[float]) -> float | None:
    """Estimate the bid-ask spread (bps) via Roll's serial-covariance method.

    ``prices`` are consecutive transaction prices.  The spread is
    ``2 * sqrt(-cov(Δp_t, Δp_{t-1}))`` when that covariance is negative
    (the bid-ask bounce signature); otherwise the estimate is zero.  Returns
    ``None`` when there are too few prices or the covariance is undefined.
    """
    if len(prices) < 3:
        return None
    deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
    if len(deltas) < 2:
        return None
    mean_delta = sum(deltas) / len(deltas)
    cov = sum(
        (deltas[i] - mean_delta) * (deltas[i + 1] - mean_delta)
        for i in range(len(deltas) - 1)
    ) / (len(deltas) - 1)
    if cov >= 0:
        return 0.0
    spread = 2.0 * math.sqrt(-cov)
    mid = sum(prices) / len(prices)
    if mid <= 0:
        return None
    return spread / mid * 10_000.0


def estimate_spread_series(
    candles: Iterable[Any],
    *,
    method: str = "corwin_schultz",
    window: int = 20,
) -> list[dict[str, float | int]]:
    """Return a rolling spread series ``[{"ts", "spread_bps"}, ...]``.

    ``candles`` are objects with ``ts``/``high``/``low``/``close`` attributes
    (the backtest :class:`~app.services.backtest.models.Candle`).  For each bar
    at index ``i >= window`` the spread is estimated over the preceding
    ``window`` bars, so the value at ``ts`` uses only *completed* bars before
    it (no look-ahead).  Bars with an undefined estimate are skipped.
    """
    rows = list(candles)
    if len(rows) < 2 or window < 2:
        return []
    series: list[dict[str, float | int]] = []
    for i in range(window, len(rows) + 1):
        chunk = rows[i - window:i]
        highs = [float(c.high) for c in chunk]
        lows = [float(c.low) for c in chunk]
        if method == "roll":
            prices = [float(c.close) for c in chunk]
            spread = roll_spread_bps(prices)
        else:
            spread = corwin_schultz_spread_bps(highs, lows)
        if spread is None:
            continue
        series.append({"ts": int(rows[i - 1].ts), "spread_bps": round(spread, 6)})
    return series


def spread_at(
    series: Sequence[dict[str, float | int]] | None,
    ts: int,
) -> float | None:
    """Return the most recent spread (bps) at or before ``ts``, or ``None``.

    ``series`` must be ascending by ``ts``.  Uses a linear scan from the end
    (series are short and this is called per fill).
    """
    if not series:
        return None
    result: float | None = None
    for row in series:
        if int(row["ts"]) <= ts:
            result = float(row["spread_bps"])
        else:
            break
    return result
