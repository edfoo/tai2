#!/usr/bin/env python3
"""CLI backtest comparison runner for LTF 15m/HTF 1H vs LTF 1H/HTF 4H.

Why this exists
---------------
Running backtests in the NiceGUI UI is ephemeral — results live on
``app.state`` and are lost on refresh.  This script runs backtests headless
and persists:

  * full per-run results (JSON) under ``backtest_cache/cli/``
  * a single comparison CSV across all runs (easy to diff in a spreadsheet)

Usage
-----
Run from the repo root with the project venv::

    .venv/bin/python scripts/run_backtest_cli.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframes 15m,1h \\
        --strategies liquidity_sweep,trend_pullback \\
        --days 60 \\
        --capital 1000

``--timeframes`` selects which LTF to backtest (each maps to its own HTF via
``htf_for``).  ``--days`` is the trailing window (default 60 days) ending now.

Outputs are written to ``backtest_cache/cli/``:
  * ``<run_id>.json``         full result for one LTF/HTF combo
  * ``comparison.csv``        one row per run with headline metrics
  * ``overview.json``         machine-readable list of all run summaries

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Allow running without the package installed: repo root on sys.path.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.backtest.engine import BacktestEngine, available_strategy_names  # noqa: E402
from app.services.backtest.models import BacktestResult  # noqa: E402
from app.services.backtest.persistence import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    OVERVIEW_FILENAME,
    result_summary_row,
    save_result,
    write_comparison_csv,
)
from app.services.backtest.runner import (  # noqa: E402
    build_backtest_config,
    htf_for,
    parse_timeframe,
)

OUTPUT_DIR = DEFAULT_OUTPUT_DIR
_MS = 1_000


def summarize(result: BacktestResult, run_id: str, ltf: str, htf: str) -> dict[str, Any]:
    """Extract a one-row summary for the comparison CSV (shares persistence module)."""
    return result_summary_row(result, run_id=run_id, ltf=ltf, htf=htf)


async def run_one(
    *,
    symbols: list[str],
    ltf: str,
    strategy_names: list[str],
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
) -> tuple[str, BacktestResult]:
    """Run the backtest engine once for a given LTF and return its result."""
    config = build_backtest_config(
        symbols=symbols,
        timeframe=ltf,
        strategy_names=strategy_names,
        start_ts=start_ts,
        end_ts=end_ts,
        capital=capital,
        warmup=warmup,
    )
    engine = BacktestEngine(config)
    result = await engine.run()
    return ltf, result


async def _amain(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    run_tag = now.strftime("%Y%m%d_%H%M%S")
    OC = OUTPUT_DIR / run_tag
    OC.mkdir(parents=True, exist_ok=True)

    tfms = [parse_timeframe(t, "ltf") for t in args.timeframes]
    end_ts = int(now.timestamp() * _MS)
    start_ts = int((now - timedelta(days=args.days)).timestamp() * _MS)

    summaries: list[dict[str, Any]] = []
    exit_code = 0

    for ltf in tfms:
        run_id = f"{run_tag}_{ltf}"
        print(f"▶ Running LTF={ltf} ...")
        t0 = time.time()
        try:
            _, result = await run_one(
                symbols=args.symbols,
                ltf=ltf,
                strategy_names=args.strategies,
                start_ts=start_ts,
                end_ts=end_ts,
                capital=args.capital,
                warmup=args.warmup,
            )
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  ✗ LTF={ltf} errored: {exc}")
            err_id = f"{run_id}_error_{uuid.uuid4().hex[:8]}"
            err_path = OC / f"{err_id}.json"
            err_path.write_text(json.dumps({"error": str(exc)}, indent=2))
            summaries.append({
                "run_id": err_id, "ltf": ltf, "htf": "", "error": str(exc),
                "symbols": ",".join(args.symbols), "strategies": ",".join(args.strategies),
            })
            exit_code = 1
            continue

        htf = _htf_for(ltf)
        print(f"  ✓ done in {result.duration_seconds:.1f}s, "
              f"{result.candles_processed} candles, "
              f"{len(result.trades)} trades")
        if result.error:
            print(f"  ⚠ engine set error: {result.error}")

        # Persist the full result + per-strategy breakdown + CSV row using
        # the shared persistence module (same format the UI writes).
        try:
            save_result(result, run_id=run_id, output_dir=OC)
        except Exception as exc:  # noqa: BLE001 - persistence failure shouldn't kill the run
            print(f"  ⚠ failed to persist result to {OC}: {exc}")

        summaries.append(summarize(result, run_id, ltf, htf))

    # Comparison CSV + overview.
    csv_path = write_comparison_csv(enabled_or_all(summaries), output_dir=OUTPUT_DIR, append=False)

    overview_path = OUTPUT_DIR / OVERVIEW_FILENAME
    overview = {"generated_at": run_tag, "days": args.days, "runs": summaries}
    overview_path.write_text(json.dumps(overview, indent=2, default=str))

    # Print a human-readable diff for the headline metric(s).
    print("\n── Comparison ──")
    if summaries and args.rank_by in summaries[0]:
        ordered = sorted(enabled_or_all(summaries), key=lambda s: s.get(args.rank_by) or -1e18, reverse=True)
        for s in ordered:
            print(f"  {s.get('ltf')}/{s.get('htf'):<4} {args.rank_by}={s.get(args.rank_by)} "
                  f"trades={s.get('m_total_trades')} win_rate={s.get('m_win_rate')}")
    else:
        for s in summaries:
            print(f"  {s.get('ltf')}/{s.get('htf'):<4} trades={s.get('m_total_trades')} "
                  f"win_rate={s.get('m_win_rate')} net_profit={s.get('m_net_profit')}")

    print(f"\nFull results:      {OC}")
    print(f"Comparison CSV:    {csv_path}")
    print(f"Overview:          {overview_path}")
    return exit_code


def enabled_or_all(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Placeholder keep-alive; for now simply return every summary row."""
    return summaries


def _htf_for(tf: str) -> str:
    """Alias to the shared runner's LTF→HTF map."""
    return htf_for(tf)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Headless backtest comparison across timeframes."
    )
    parser.add_argument(
        "--symbols",
        default="BTC-USDT-SWAP",
        help="Comma-separated OKX symbols (default BTC-USDT-SWAP).",
    )
    parser.add_argument(
        "--timeframes",
        default="15m,1H",
        help="Comma-separated LTFs to backtest. Each picks its own HTF. Default '15m,1H'.",
    )
    parser.add_argument(
        "--strategies",
        default="mean_reversion,liquidity_sweep,trend_pullback,vwap_reversion,spike_continuation",
        help="Comma-separated strategy names to enable.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Trailing window in days ending now (default 60).",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=1000.0,
        help="Initial capital and per-trade notional (default 1000).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=200,
        help="Warmup candles fetched before start for indicator stabilisation.",
    )
    parser.add_argument(
        "--rank-by",
        default="m_sharpe_per_candle",
        help="Metric used for the printed comparison sort (metric name prefixed m_).",
    )
    args = parser.parse_args()

    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    valid = available_strategy_names()
    requested = [s.strip() for s in args.strategies.split(",") if s.strip()]
    unknown = [s for s in requested if s not in valid]
    if unknown:
        print(f"Unknown strategies: {unknown}. Available: {valid}")
        return 2
    args.strategies = requested
    args.timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]

    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())