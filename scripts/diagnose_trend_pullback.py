#!/usr/bin/env python3
"""Step-0 diagnostic: test the "TP/SL too wide on fading coins" hypothesis.

Reads persisted backtest results (``backtest_cache/cli/*_results.json``) and
buckets every trend_pullback trade by its ``close_reason``.  For each cohort it
measures:

  * TP distance %        — how far the take-profit was from entry.
  * SL distance %        — how far the stop-loss was from entry.
  * realized favorable % — the maximum distance price actually travelled in the
                           trade's favour while it was open (from cached OHLCV).
  * volume decel ratio   — mean(last 4 bars volume) / mean(prior 16 bars volume)
                           at entry.  < 1.0 means volume was already decaying.
  * range decel ratio    — same ratio on (high - low), the candle-range analog.

The hypothesis under test:

    Trades that never hit TP ("timeout" / "end_of_data") are opened on coins
    whose activity has already decayed (volume + range decel < 1), with a TP
    distance far larger than the move the coin could actually make after entry.

If that holds, the timeout cohort should show *wider* TP vs realized move and
*lower* decel ratios than the TP-hit cohort.

Run from the repo root with the project venv:

    .venv/bin/python scripts/diagnose_trend_pullback.py

Flags:
    --results-dir   Directory containing *_results.json (default backtest_cache/cli).
    --cache-dir     Directory containing candle JSON (default backtest_cache).
    --strategy      Strategy to analyse (default trend_pullback).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_RESULTS_DIR = ROOT / "backtest_cache" / "cli"
DEFAULT_CACHE_DIR = ROOT / "backtest_cache"

_CACHE_FN_RE = re.compile(
    r"^(?P<symbol>.+)_(?P<tf>\d+[mMhHdDwW])_(?P<start>\d+)_(?P<end>\d+)_w\d+\.json$"
)


def _median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def _mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _fmt(x: float | None, pct: bool = False, digits: int = 2) -> str:
    if x is None:
        return "  n/a  "
    suffix = "%" if pct else ""
    return f"{x * 100 if pct else x:.{digits}f}{suffix}"


def load_results(results_dir: Path) -> list[dict[str, Any]]:
    """Load every *_results.json under results_dir (recursive)."""
    results: list[dict[str, Any]] = []
    for path in sorted(results_dir.rglob("*_results.json")):
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(data, dict) and "trades" in data:
            results.append(data)
    return results


def index_caches(cache_dir: Path) -> dict[tuple[str, str], list[tuple[int, int, Path]]]:
    """Map (symbol, timeframe) -> [(start_ts, end_ts, path)] for candle caches."""
    index: dict[tuple[str, str], list[tuple[int, int, Path]]] = {}
    for path in cache_dir.glob("*.json"):
        m = _CACHE_FN_RE.match(path.name)
        if not m:
            continue
        symbol = m.group("symbol")
        tf = m.group("tf")
        start = int(m.group("start"))
        end = int(m.group("end"))
        index.setdefault((symbol, tf), []).append((start, end, path))
    return index


def load_candles(
    symbol: str,
    tf: str,
    entry_ts: int,
    index: dict[tuple[str, str], list[tuple[int, int, Path]]],
) -> list[dict[str, Any]] | None:
    """Return the candle list covering entry_ts for a symbol/tf, or None."""
    tf_upper = tf.upper()
    # Match either exact case or any case (files use e.g. '15m', '1H').
    candidates = []
    for (sym, t), files in index.items():
        if sym == symbol and t.upper() == tf_upper:
            candidates.extend(files)
    for start, end, path in sorted(candidates, key=lambda x: x[0]):
        if start <= entry_ts <= end:
            try:
                rows = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                return None
            if isinstance(rows, list):
                return rows
    return None


def decel_ratios(
    candles: list[dict[str, Any]], entry_idx: int
) -> tuple[float | None, float | None]:
    """Return (volume decel, range decel) for the entry candle at entry_idx.

    "last 4" = the entry candle and the 3 before it; "prior 16" = the 16 before
    those.  A ratio < 1.0 means activity was already decaying into the entry.
    """
    if entry_idx < 20:
        return None, None
    recent = candles[entry_idx - 3 : entry_idx + 1]
    prior = candles[entry_idx - 19 : entry_idx - 3]
    if len(prior) < 4:
        return None, None

    def _ratio(key: str) -> float | None:
        r_vals = [float(c.get(key, 0.0)) for c in recent]
        p_vals = [float(c.get(key, 0.0)) for c in prior]
        r_avg = sum(r_vals) / len(r_vals)
        p_avg = sum(p_vals) / len(p_vals)
        if p_avg <= 0:
            return None
        return r_avg / p_avg

    vol_ratio = _ratio("volume")

    def _rng(c: dict[str, Any]) -> float:
        return float(c.get("high", 0.0)) - float(c.get("low", 0.0))

    r_vals = [_rng(c) for c in recent]
    p_vals = [_rng(c) for c in prior]
    p_avg = sum(p_vals) / len(p_vals)
    rng_ratio = (sum(r_vals) / len(r_vals)) / p_avg if p_avg > 0 else None

    return vol_ratio, rng_ratio


def realized_favorable(
    candles: list[dict[str, Any]],
    entry_idx: int,
    close_idx: int,
    direction: str,
    entry_price: float,
) -> float | None:
    """Max distance price moved in the trade's favour, as % of entry."""
    if entry_idx < 0 or close_idx < entry_idx or entry_price <= 0:
        return None
    window = candles[entry_idx : close_idx + 1]
    if not window:
        return None
    highs = [float(c["high"]) for c in window]
    lows = [float(c["low"]) for c in window]
    if direction == "long":
        fav = max(highs) - entry_price
    else:
        fav = entry_price - min(lows)
    return fav / entry_price


def analyze(
    results: list[dict[str, Any]],
    cache_index: dict[tuple[str, str], list[tuple[int, int, Path]]],
    strategy: str,
) -> dict[str, list[dict[str, Any]]]:
    """Bucket trend_pullback trades by close_reason, enriching with metrics."""
    cohorts: dict[str, list[dict[str, Any]]] = {}
    skipped_missing_candles = 0

    for result in results:
        cfg = result.get("config") or {}
        tf = str(cfg.get("timeframe") or "")
        trades = result.get("trades") or []
        for t in trades:
            if t.get("strategy") != strategy:
                continue
            symbol = t.get("symbol", "")
            direction = t.get("direction", "long")
            entry_price = float(t.get("entry_price") or 0.0)
            tp_price = t.get("tp_price")
            sl_price = t.get("sl_price")
            entry_ts = int(t.get("entry_ts") or 0)
            close_ts = int(t.get("close_ts") or 0)
            reason = t.get("close_reason") or "unknown"

            if entry_price <= 0:
                continue

            tp_dist = abs(float(tp_price) - entry_price) / entry_price if tp_price else None
            sl_dist = abs(float(sl_price) - entry_price) / entry_price if sl_price else None

            row: dict[str, Any] = {
                "symbol": symbol,
                "direction": direction,
                "tp_dist": tp_dist,
                "sl_dist": sl_dist,
                "reason": reason,
                "vol_decel": None,
                "rng_decel": None,
                "realized_fav": None,
            }

            candles = load_candles(symbol, tf, entry_ts, cache_index)
            if candles:
                entry_idx = next(
                    (i for i, c in enumerate(candles) if int(c.get("ts", 0)) >= entry_ts),
                    None,
                )
                if entry_idx is not None:
                    vol_d, rng_d = decel_ratios(candles, entry_idx)
                    row["vol_decel"] = vol_d
                    row["rng_decel"] = rng_d
                    close_idx = next(
                        (
                            i
                            for i, c in enumerate(candles)
                            if int(c.get("ts", 0)) >= close_ts
                        ),
                        len(candles) - 1,
                    )
                    row["realized_fav"] = realized_favorable(
                        candles, entry_idx, close_idx, direction, entry_price
                    )
            else:
                skipped_missing_candles += 1

            cohorts.setdefault(reason, []).append(row)

    if skipped_missing_candles:
        print(f"  (skipped {skipped_missing_candles} trades with no matching candle cache)")
    return cohorts


def _print_cohort(label: str, rows: list[dict[str, Any]]) -> None:
    n = len(rows)
    if n == 0:
        print(f"  {label:<12} — no trades")
        return
    tp = _median([r["tp_dist"] for r in rows if r["tp_dist"] is not None])
    sl = _median([r["sl_dist"] for r in rows if r["sl_dist"] is not None])
    fav = _median([r["realized_fav"] for r in rows if r["realized_fav"] is not None])
    vol = _median([r["vol_decel"] for r in rows if r["vol_decel"] is not None])
    rng = _median([r["rng_decel"] for r in rows if r["rng_decel"] is not None])
    # Coverage of realized-fav: how often did price reach the TP distance?
    reached = sum(
        1
        for r in rows
        if r["realized_fav"] is not None and r["tp_dist"] is not None and r["realized_fav"] >= r["tp_dist"]
    )
    cov = reached / n * 100 if n else 0.0
    print(
        f"  {label:<12} n={n:<4} "
        f"TPdist={_fmt(tp, True):>9} SLdist={_fmt(sl, True):>9} "
        f"realizedFav={_fmt(fav, True):>9} TPreach%={cov:5.1f} "
        f"volDecel={_fmt(vol):>7} rngDecel={_fmt(rng):>7}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS_DIR))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--strategy", default="trend_pullback")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    cache_dir = Path(args.cache_dir)

    results = load_results(results_dir)
    if not results:
        print(f"No *_results.json found under {results_dir}")
        return 1

    cache_index = index_caches(cache_dir)
    if not cache_index:
        print(f"No candle cache files found under {cache_dir}")

    print(f"Analysing strategy '{args.strategy}' across {len(results)} result file(s)\n")

    cohorts = analyze(results, cache_index, args.strategy)
    total = sum(len(r) for r in cohorts.values())
    print(f"Total {args.strategy} trades: {total}\n")

    # Fixed order so the key cohorts print first.
    order = ["tp", "sl", "timeout", "end_of_data", "pm_skimming"]
    for reason in order:
        if reason in cohorts:
            _print_cohort(reason, cohorts[reason])
    for reason in sorted(cohorts):
        if reason not in order:
            _print_cohort(reason, cohorts[reason])

    print(
        "\nLegend: volDecel/rngDecel < 1.0 => activity already decaying at entry; "
        "TPreach% => how often price actually travelled far enough to hit TP."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
