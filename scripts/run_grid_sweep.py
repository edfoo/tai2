#!/usr/bin/env python3
"""Headless Cartesian-product parameter sweep over every strategy in the catalogue.

Why this exists
---------------
The BACKTEST page's "Parameter Sweep" panel builds a grid from the shared
sweep catalogue (``app/services/backtest/sweep_catalog.py``) and runs it
inside the NiceGUI process.  This script is the headless, parallelised
counterpart: it drives the **same** catalogue (so the CLI and UI always sweep
the identical set of options) but dispatches each combination to a separate
CPU process, so a multi-parameter Cartesian product finishes in a fraction of
the single-core time.

The sweep catalogue is the single source of truth for *which* parameters are
interesting per strategy and their candidate values.  By default this script
sweeps the full Cartesian product of every parameter for the selected
strategies (plus the launcher-level ``tp_pct`` / ``sl_pct`` / ``notional_usd``).
Use ``--params`` to restrict to a subset of dotted keys.

Usage
-----
Run from the repo root with the project venv::

    .venv/bin/python scripts/run_grid_sweep.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframes 15m \\
        --strategies mean_reversion,trend_pullback \\
        --days 60 \\
        --capital 1000 \\
        --workers 8

Sweep only a couple of knobs (keeps the grid small)::

    .venv/bin/python scripts/run_grid_sweep.py \\
        --symbols BTC-USDT-SWAP --timeframes 15m \\
        --strategies mean_reversion \\
        --params strategies.mean_reversion.rsi_oversold,strategies.mean_reversion.max_adx

Results are persisted under ``backtest_cache/cli/grid/``:
  * ``<tag>/<run_id>_results.json``   full per-combination result
  * ``comparison.csv``                 one row per combination
  * ``overview.json``                  machine-readable run summary

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.backtest.engine import BacktestEngine, available_strategy_names  # noqa: E402
from app.services.backtest.grid import _apply_params  # noqa: E402
from app.services.backtest.models import BacktestConfig, BacktestResult  # noqa: E402
from app.services.backtest.persistence import (  # noqa: E402
    result_summary_row,
    result_to_dict,
    write_comparison_csv,
)
from app.services.backtest.sweep_catalog import (  # noqa: E402
    cartesian_combinations,
    grid_param_defs,
)
from app.services.strategies.defaults import strategy_defaults  # noqa: E402

OUTPUT_DIR = ROOT / "backtest_cache" / "cli" / "grid"
OVERVIEW_FILENAME = "overview.json"
_MS = 1_000


def parse_timeframe(tf: str, ctx: str) -> str:
    """Normalise a timeframe string to engine form ('15m' / '1H' / '4H')."""
    t = tf.strip().upper()
    mapping = {
        "1M": "1m", "1MIN": "1m", "5M": "5m", "5MIN": "5m",
        "15M": "15m", "15MIN": "15m",
        "1H": "1H", "1HOUR": "1H", "1HR": "1H",
        "4H": "4H", "4HOUR": "4H", "4HR": "4H",
        "1D": "1D", "1DAY": "1D",
    }
    if t not in mapping:
        raise SystemExit(f"Unsupported timeframe '{tf}' for {ctx}. Use 1m/5m/15m/1H/4H/1D.")
    return mapping[t]


def _htf_for(tf: str) -> str:
    return {"1m": "5m", "5m": "15m", "15m": "1H", "1H": "4H", "4H": "1D", "1D": "1W"}.get(tf, "")


def _strategies_cfg(strategy_names: list[str]) -> dict[str, Any]:
    """Seed every requested strategy from its canonical defaults (enabled)."""
    cfg: dict[str, Any] = {}
    for name in strategy_names:
        s = dict(strategy_defaults(name))
        s["enabled"] = True
        cfg[name] = s
    return cfg


def _build_config(
    *,
    symbol: str,
    ltf: str,
    strategy_names: list[str],
    param_values: dict[str, Any],
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
) -> BacktestConfig:
    """Build a BacktestConfig for one combination, applying swept params."""
    launcher_config: dict[str, Any] = {
        "mode": "launcher_only",
        "notional_usd": float(capital),
        "strategies": _strategies_cfg(strategy_names),
    }
    config = BacktestConfig(
        symbols=[symbol],
        timeframe=ltf,
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=capital,
        strategy_names=strategy_names,
        launcher_config=launcher_config,
        strategy_config={},
        warmup_candles=warmup,
        disable_live_execution=True,
        evaluation_mode="finer_ltf",
        evaluation_timeframe="1m",
    )
    # Apply the swept parameter values (dotted keys, e.g.
    # "strategies.mean_reversion.rsi_oversold" → launcher_config).
    _apply_params(config, param_values)
    return config


def _worker_job(job: dict[str, Any]) -> dict[str, Any]:
    """Run one Cartesian-product combination in a worker process.

    Module-level (and thus picklable by reference) so it can be dispatched to
    a ``ProcessPoolExecutor`` on another CPU core.
    """
    symbol = job["symbol"]
    ltf = job["ltf"]
    tag = job["tag"]
    param_values = job["param_values"]
    try:
        config = _build_config(
            symbol=symbol,
            ltf=ltf,
            strategy_names=job["strategy_names"],
            param_values=param_values,
            start_ts=job["start_ts"],
            end_ts=job["end_ts"],
            capital=job["capital"],
            warmup=job["warmup"],
        )
        result: BacktestResult = asyncio.run(BacktestEngine(config).run())
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗ [{symbol} {ltf}] {param_values} errored: {exc}")
        return {
            "run_id": tag, "symbol": symbol, "ltf": ltf, "error": str(exc),
            "params": param_values,
        }

    if result.error:
        print(f"  ⚠ [{symbol} {ltf}] engine error: {result.error}")

    # Persist the full result JSON (skips save_result's comparison.csv append
    # to avoid a read-modify-write race across worker processes).
    try:
        oc = Path(job["oc"])
        oc.mkdir(parents=True, exist_ok=True)
        (oc / f"{tag}_results.json").write_text(
            json.dumps(result_to_dict(result), indent=2, default=str))
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ [{symbol} {ltf}] persist failed: {exc}")

    htf = _htf_for(ltf)
    summary = result_summary_row(result, run_id=tag, ltf=ltf, htf=htf)
    summary.update({
        "symbol": symbol,
        "strategies": ",".join(job["strategy_names"]),
        "params": param_values,
    })
    m = result.metrics or {}
    print(f"  ✓ [{symbol} {ltf}] trades={m.get('total_trades')} "
          f"win={m.get('win_rate')} net={m.get('net_profit')} "
          f"{param_values}")
    return summary


def _param_label(pv: dict[str, Any]) -> str:
    """Compact, filesystem-safe label for a parameter assignment."""
    parts = []
    for k, v in pv.items():
        leaf = k.split(".")[-1]
        parts.append(f"{leaf}={v}")
    return "_".join(parts) or "baseline"


def _amain(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    run_tag = now.strftime("%Y%m%d_%H%M%S")
    OC = OUTPUT_DIR / run_tag
    OC.mkdir(parents=True, exist_ok=True)

    tfms = [parse_timeframe(t, "ltf") for t in args.timeframes]
    end_ts = int(now.timestamp() * _MS)
    start_ts = int((now - timedelta(days=args.days)).timestamp() * _MS)

    # ── Build the parameter grid from the shared catalogue ────────────
    all_defs = grid_param_defs(args.strategies, include_launcher=not args.no_launcher)
    if args.params:
        wanted = {p.strip() for p in args.params.split(",") if p.strip()}
        defs = [d for d in all_defs if d.key in wanted]
        missing = wanted - {d.key for d in defs}
        if missing:
            print(f"Unknown sweep keys (not in catalogue): {sorted(missing)}")
            print(f"Available keys: {sorted(d.key for d in all_defs)}")
            return 2
    else:
        defs = all_defs

    if not defs:
        print("No sweep parameters selected — nothing to sweep.")
        return 2

    combinations = cartesian_combinations(defs)
    total_per_ltf = len(combinations)
    grand_total = total_per_ltf * len(tfms) * len(args.symbols)

    print(f"▶ Sweep parameters ({len(defs)}):")
    for d in defs:
        print(f"    {d.key}  ←  {d.values}")
    print(f"▶ {total_per_ltf} combinations × {len(tfms)} timeframe(s) × "
          f"{len(args.symbols)} symbol(s) = {grand_total} runs")

    if grand_total > args.max_combinations:
        print(f"✗ {grand_total} runs exceeds --max-combinations "
              f"({args.max_combinations}). Raise it or narrow with --params.")
        return 2

    # ── Dispatch combinations across worker processes ─────────────────
    jobs: list[dict[str, Any]] = []
    for symbol in args.symbols:
        for ltf in tfms:
            for pv in combinations:
                tag = f"{run_tag}_{symbol}_{ltf}_{_param_label(pv)}"
                jobs.append({
                    "tag": tag, "oc": str(OC), "symbol": symbol, "ltf": ltf,
                    "strategy_names": args.strategies, "param_values": pv,
                    "start_ts": start_ts, "end_ts": end_ts,
                    "capital": args.capital, "warmup": args.warmup,
                })

    summaries: list[dict[str, Any]] = []
    exit_code = 0
    print(f"▶ Running {len(jobs)} job(s) across {args.workers} worker(s)...")
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(_worker_job, j) for j in jobs]
        for fut in as_completed(futures):
            row = fut.result()
            summaries.append(row)
            if row.get("error"):
                exit_code = 1

    # ── Persist comparison + overview ────────────────────────────────
    csv_path = write_comparison_csv(summaries, output_dir=OUTPUT_DIR, append=False)
    overview_path = OUTPUT_DIR / OVERVIEW_FILENAME
    overview_path.write_text(json.dumps({
        "generated_at": run_tag,
        "params": {d.key: d.values for d in defs},
        "runs": summaries,
    }, indent=2, default=str))

    # ── Ranked summary ────────────────────────────────────────────────
    rank_key = args.rank_by
    ranked = sorted(
        (s for s in summaries if not s.get("error") and s.get(rank_key) is not None),
        key=lambda s: s.get(rank_key) or 0,
        reverse=True,
    )
    print(f"\n── Top combinations by {rank_key} ──")
    for s in ranked[:args.top]:
        pv = {k.split('.')[-1]: v for k, v in s.get("params", {}).items()}
        print(f"  {s.get('symbol')} {s.get('ltf')} {pv}  →  "
              f"{rank_key}={s.get(rank_key)} trades={s.get('m_total_trades')} "
              f"win={s.get('m_win_rate')} net={s.get('m_net_profit')}")

    print(f"\nFull results:   {OC}")
    print(f"Comparison CSV: {csv_path}")
    print(f"Overview:       {overview_path}")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Headless Cartesian-product parameter sweep (parallelised).")
    parser.add_argument("--symbols", default="BTC-USDT-SWAP",
                        help="Comma-separated OKX symbols.")
    parser.add_argument("--timeframes", default="15m",
                        help="Comma-separated LTFs to backtest.")
    parser.add_argument("--strategies",
                        default="mean_reversion,spike_continuation,liquidity_sweep,"
                                "vwap_reversion,trend_pullback",
                        help="Comma-separated strategy names to sweep.")
    parser.add_argument("--params", default=None,
                        help="Comma-separated dotted keys to sweep (default: every "
                             "catalogue param for the selected strategies).")
    parser.add_argument("--no-launcher", action="store_true",
                        help="Exclude launcher-level params (tp_pct/sl_pct/notional_usd).")
    parser.add_argument("--days", type=int, default=60, help="Trailing window in days.")
    parser.add_argument("--capital", type=float, default=1000.0, help="Initial capital / notional.")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup candles before start.")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1,
                        help="Number of parallel worker processes (default: all CPU cores).")
    parser.add_argument("--max-combinations", type=int, default=10000,
                        help="Safety cap on total runs (default 10000).")
    parser.add_argument("--top", type=int, default=10,
                        help="How many top combinations to print (default 10).")
    parser.add_argument("--rank-by", default="m_sharpe_per_candle",
                        help="Metric to rank by (CSV columns are m_*).")
    args = parser.parse_args()

    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]

    valid = available_strategy_names()
    requested = [s.strip() for s in args.strategies.split(",") if s.strip()]
    unknown = [s for s in requested if s not in valid]
    if unknown:
        print(f"Unknown strategies: {unknown}. Available: {valid}")
        return 2
    args.strategies = requested

    return _amain(args)


if __name__ == "__main__":
    sys.exit(main())
