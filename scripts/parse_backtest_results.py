#!/usr/bin/env python3
"""Parse and compare backtest CLI results into a readable side-by-side table.

Reads the outputs produced by ``run_backtest_cli.py``:

  * ``backtest_cache/cli/*_results.json``   full per-run results
  * ``backtest_cache/cli/comparison.csv``   one row per run (cumulative)
  * ``backtest_cache/cli/overview.json``    run summaries

It prints a formatted risk/return table (Sharpe, win rate, profit factor,
max drawdown, expectancy, etc.) for every available run, and can filter /
rank rows.

Usage
-----
    .venv/bin/python scripts/parse_backtest_results.py [--rank-by m_sharpe_per_candle] [--json]

Options
-------
    --ltf TEXT    Only show runs for a given LTF (e.g. 15m or 1H).
    --sort-by     Metric column to sort by (default m_sharpe_per_candle).
    --json        Output machine-readable JSON (one dict of run rows) instead
                  of the human table.
    --source      'csv' (default), 'json', or 'auto'. When reading the JSON
                  result files, the full metrics dict is available; CSV only
                  carries the flattened ``m_*`` columns written by the runner.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "backtest_cache" / "cli"
CSV_PATH = OUTPUT_DIR / "comparison.csv"

# Headline metrics worth displaying in the human table, in a sensible order.
HEADLINE_KEYS = [
    ("total_trades", "Trades"),
    ("win_rate", "Win%"),
    ("profit_factor", "PF"),
    ("net_profit", "NetPnL"),
    ("total_return_pct", "Return%"),
    ("max_drawdown_pct", "MaxDD%"),
    ("sharpe_per_candle", "Sharpe"),
    ("expectancy", "Expectancy"),
    ("average_trade", "AvgTrade"),
]
# If present, show these extra per-strategy breakdown columns too.
STRATEGY_METRICS = ["win_rate", "profit_factor", "net_profit", "total_trades"]


def load_from_csv() -> list[dict[str, Any]]:
    """Load all runs from the cumulative comparison.csv (flattened m_* keys)."""
    if not CSV_PATH.exists():
        print(f"┌──────────────────────────────────────────────────────────────┐")
        print(f"│  No comparison.csv found at {CSV_PATH}           │")
        print(f"│  Run `run_backtest_cli.py` first to generate results.       │")
        print(f"└──────────────────────────────────────────────────────────────┘", file=sys.stderr)
        return []
    rows: list[dict[str, Any]] = []
    with open(CSV_PATH, newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(row)
    return rows


def load_from_json(results_dir: Path = OUTPUT_DIR) -> list[dict[str, Any]]:
    """Load from ``*_results.json`` files, preserving unflattened metrics."""
    rows: list[dict[str, Any]] = []
    for p in sorted(results_dir.glob("*_results.json")):
        try:
            data = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("error"):
            continue
        rows.append(_unflatten(data))
    return rows


def _unflatten(data: dict[str, Any]) -> dict[str, Any]:
    """Rebuild a row dict from a *_results.json payload.

    Sets the flattened ``m_*`` keys (so downstream code and --json output
    stay consistent with the CSV view) while keeping a reference to the
    unflattened ``metrics`` dict.
    """
    cfg = data.get("config") or {}
    ltf = cfg.get("timeframe", "")
    summ = {
        "run_id": f"{ltf}",
        "ltf": ltf,
        "htf": "",
        "symbols": ",".join(cfg.get("symbols") or []),
        "strategies": ",".join(cfg.get("strategy_names") or []),
        "trades_count": data.get("trades_count"),
        "metrics": data.get("metrics") or {},
        "per_strategy": data.get("per_strategy") or {},
    }
    for k, v in (data.get("metrics") or {}).items():
        summ[f"m_{k}"] = v
    return summ


def selected_rows(source: str, ltf_filter: str | None) -> list[dict[str, Any]]:
    if source == "json":
        rows = load_from_json()
    elif source == "csv":
        rows = load_from_csv()
    else:  # auto
        rows = load_from_json()
        if not rows:
            rows = load_from_csv()
    if ltf_filter:
        target = ltf_filter.strip().upper()
        rows = [r for r in rows if str(r.get("ltf", "")).upper() == target]
    return rows


def metric(row: dict[str, Any], key: str) -> Any:
    """Read a metric value, preferring unflattened then flattened form."""
    if key in row.get("metrics", {}):
        return row["metrics"].get(key)
    flat = f"m_{key}"
    if flat in row:
        return row.get(flat)
    return None


def _fmt(val: Any, nd: int = 4) -> str:
    if val is None or val == "":
        return "—"
    try:
        f = float(val)
    except (TypeError, ValueError):
        return str(val)
    # Percentages / ratios display reasonably with different precision.
    if abs(f) >= 100:
        return f"{f:,.0f}"
    return f"{f:.{nd}f}"


def render_table(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("No matching runs found.")
        return
    # Build a merged list of strategy names so a per-strategy breakdown can be
    # shown for framed runs.
    col_keys = HEADLINE_KEYS
    headers = ["LTF", "HTF", "Strategies"] + [short for _, short in col_keys]
    widths = {
        "LTF": 5, "HTF": 5, "Strategies": max(12, max(len(r.get("strategies", "")) for r in rows)),
    }
    for key, short in col_keys:
        widths[short] = max(len(short), max(len(_fmt(metric(r, key))) for r in rows)) + 1

    def line(row: dict[str, Any]) -> list[str]:
        strat = row.get("strategies", "")
        cells = [str(row.get("ltf", "")), str(row.get("htf", "")), strat]
        for key, short in col_keys:
            cells.append(_fmt(metric(row, key)))
        return cells

    # Header
    print("  ".join(h.ljust(widths.get(h, len(h))) for h in headers))
    print("  ".join("-" * widths.get(h, len(h)) for h in headers))
    for r in rows:
        cells = line(r)
        print("  ".join(c.ljust(widths.get(h, len(c))) for c, h in zip(cells, headers)))

    # Per-strategy breakdown for the first (most relevant) run, if present.
    per = rows[0].get("per_strategy") if rows else None
    if per:
        print("\n── Per-strategy breakdown (first run) ──")
        print("  " + "  ".join(["Strategy", "Trades", "Win%", "PF", "NetPnL"]))
        for name, stat in per.items():
            print(
                f"  {name:<12}{_fmt(stat.get('total_trades')):>8}"
                f"{_fmt(stat.get('win_rate')):>7}"
                f"{_fmt(stat.get('profit_factor')):>7}"
                f"{_fmt(stat.get('net_profit')):>10}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse and compare backtest CLI results."
    )
    parser.add_argument("--source", choices=["csv", "json", "auto"], default="auto",
                        help="Data source: csv (comparison.csv), json (*_results.json), or auto. Default auto.")
    parser.add_argument("--ltf", default=None,
                        help="Only show runs for this LTF, e.g. 15m or 1H.")
    parser.add_argument("--sort-by", default="m_sharpe_per_candle",
                        help="Sort key for the table (metric name, use m_ prefix for flat columns). Default m_sharpe_per_candle.")
    parser.add_argument("--json", action="store_true",
                        help="Emit machine-readable JSON instead of a table.")
    parser.add_argument("--top", type=int, default=0,
                        help="Show only the top N rows after sorting (0 = all).")
    args = parser.parse_args()

    rows = selected_rows(args.source, args.ltf)
    if not rows:
        print("No matching runs found — check --source / --ltf and that results exist.")
        return 1

    # Sort (descending) by the requested metric.
    sort_key = args.sort_by
    if not sort_key.startswith("m_") and not any(sort_key in r.get("metrics", {}) for r in rows):
        # Try unflattened key.
        sort_key = f"m_{sort_key}"
    rows_sorted = sorted(
        rows,
        key=lambda r: (metric(r, sort_key.replace("m_", "")) is not None, metric(r, sort_key.replace("m_", "")) or -1e18),
        reverse=True,
    )
    if args.top > 0:
        rows_sorted = rows_sorted[: args.top]

    if args.json:
        # Build a compact machine-readable dict keyed by run_id.
        out = {}
        for r in rows_sorted:
            key = r.get("run_id") or f"{r.get('ltf')}"
            out[key] = {
                "ltf": r.get("ltf"),
                "htf": r.get("htf"),
                "metrics": r.get("metrics", {}),
                "per_strategy": r.get("per_strategy", {}),
            }
        print(json.dumps(out, indent=2, default=str))
        return 0

    render_table(rows_sorted)
    return 0


if __name__ == "__main__":
    sys.exit(main())