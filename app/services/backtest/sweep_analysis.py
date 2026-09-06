"""Shared post-processing for parameter-sweep (grid) backtest results.

Both the NiceGUI BACKTEST page and the headless CLI runners produce a flat list
of per-combination results (each a ``params`` dict + a ``metrics`` dict).  That
flat list answers "which *combination* ranked highest", but not "which
*parameter settings* are most profitable".  This module turns it into the
analytical view the user actually wants:

  * the single **best combination** (by a profitability-first ranking),
  * a **per-parameter sensitivity** table — for each swept knob, the value
    whose *average* outcome (across the whole grid) is highest,
  * a **robustness** summary that flags overfit single-point optima vs. a
    plateau of near-equal results.

The input is deliberately a plain list of dicts so both callers can feed it
without depending on each other's types:

  * UI  → ``GridRunResult.params`` + ``GridRunResult.result.metrics``
  * CLI → the ``params`` key + ``m_*`` summary keys (prefix stripped here)

All metric keys used below are the *raw* (unprefixed) names produced by
:func:`app.services.backtest.metrics.compute_metrics`.
"""

from __future__ import annotations

from typing import Any, Iterable

# When ranking "most profitable", prefer size-normalised / risk-adjusted fields
# over absolute PnL (absolute PnL is dominated by swept ``notional_usd``).
DEFAULT_RANK_BY = "net_profit_pct"

# Fields copied onto the "best" entry for display.
_BEST_METRIC_FIELDS = (
    "total_trades",
    "win_rate",
    "net_profit",
    "net_profit_pct",
    "profit_factor",
    "expectancy",
    "max_drawdown_pct",
    "sharpe_per_candle",
)


def _num(v: Any) -> float:
    """Coerce a value to float, treating None/bool/NaN as 0.0."""
    if v is None or isinstance(v, bool):
        return 0.0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f if f == f else 0.0  # NaN -> 0.0


def _avg(vals: Iterable[float]) -> float:
    vals = [v for v in vals]
    return sum(vals) / len(vals) if vals else 0.0


def analyze_sweep(
    entries: Iterable[dict[str, Any]],
    *,
    rank_by: str = DEFAULT_RANK_BY,
    min_trades: int = 0,
) -> dict[str, Any]:
    """Analyse a set of sweep combinations.

    Parameters
    ----------
    entries:
        Iterable of ``{"params": {key: value}, "metrics": {name: value}}``
        where ``metrics`` uses raw (unprefixed) metric names.
    rank_by:
        Metric used to pick the "best" value per parameter (descending).
    min_trades:
        Combinations with fewer trades are excluded from "best" and from the
        sensitivity aggregation (low-sample results are noise on a short
        window).

    Returns a dict::

        {
          "rank_by": str,
          "best": {"params": {...}, "metrics": {...}} | None,
          "robustness": {...},
          "sensitivity": [
              {"key": str, "best_value": str,
               "values": [{"value", "n", "avg_rank",
                           "avg_net_profit_pct", "avg_profit_factor",
                           "avg_win_rate"}, ...]},
          ],
        }
    """
    rows: list[dict[str, Any]] = []
    for e in entries:
        params = e.get("params") or {}
        metrics = e.get("metrics") or {}
        if min_trades and _num(metrics.get("total_trades")) < min_trades:
            continue
        rows.append({"params": dict(params), "metrics": dict(metrics)})

    if not rows:
        return {"rank_by": rank_by, "best": None, "robustness": {}, "sensitivity": []}

    # ── Best combination ──────────────────────────────────────────────
    def _score(r: dict[str, Any]) -> float:
        return _num(r["metrics"].get(rank_by))

    best = max(rows, key=_score)
    best_score = _score(best)

    # ── Robustness ────────────────────────────────────────────────────
    plateau_pct = 5.0
    base = abs(best_score) if abs(best_score) > 1e-9 else 1.0
    within = [r for r in rows if _score(r) >= best_score - base * plateau_pct / 100.0]

    # Single-point optimum: best is materially better than ALL its 1-knob
    # neighbours (combinations differing in exactly one parameter).
    param_keys = list(best["params"].keys())
    neighbours: list[dict[str, Any]] = []
    for r in rows:
        if r is best:
            continue
        bp = best["params"]
        rp = r["params"]
        diff = [k for k in param_keys if k in rp and str(rp[k]) != str(bp[k])]
        if len(diff) == 1:
            neighbours.append(r)
    best_neighbour = max((_score(r) for r in neighbours), default=None)
    is_single_point = (
        best_neighbour is not None
        and best_neighbour < best_score - base * plateau_pct / 100.0
    )

    robustness = {
        "best_score": round(best_score, 4),
        "combinations": len(rows),
        "within_5pct": len(within),
        "plateau": len(within) > 1,
        "single_point_optimum": is_single_point,
        "note": (
            "Best is a lone spike — likely overfit. Prefer settings inside a "
            "plateau of near-equal results and confirm out-of-sample."
            if is_single_point
            else "Best sits within a plateau of near-equal results (more robust)."
        ),
    }

    # ── Per-parameter sensitivity ─────────────────────────────────────
    sensitivity: list[dict[str, Any]] = []
    for key in param_keys:
        by_value: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            v = str(r["params"].get(key))
            by_value.setdefault(v, []).append(r)

        value_rows: list[dict[str, Any]] = []
        for v, group in by_value.items():
            value_rows.append({
                "value": v,
                "n": len(group),
                "avg_rank": round(_avg(_num(r["metrics"].get(rank_by)) for r in group), 4),
                "avg_net_profit_pct": round(
                    _avg(_num(r["metrics"].get("net_profit_pct")) for r in group), 2),
                "avg_profit_factor": round(
                    _avg(_num(r["metrics"].get("profit_factor")) for r in group), 3),
                "avg_win_rate": round(
                    _avg(_num(r["metrics"].get("win_rate")) for r in group), 1),
            })
        value_rows.sort(key=lambda x: x["avg_rank"], reverse=True)
        sensitivity.append({
            "key": key,
            "best_value": value_rows[0]["value"] if value_rows else None,
            "values": value_rows,
        })

    return {
        "rank_by": rank_by,
        "best": {
            "params": best["params"],
            "metrics": {k: best["metrics"].get(k) for k in _BEST_METRIC_FIELDS},
        },
        "robustness": robustness,
        "sensitivity": sensitivity,
    }
