#!/usr/bin/env python3
"""Headless A/B harness for the two trend_pullback "wide TP/SL" levers.

Why this exists
---------------
The Step-0 diagnostic (``scripts/diagnose_trend_pullback.py``) found that
trend_pullback stop-outs / timeouts correlate with volume having *decayed*
into the entry (stop-out cohort volDecel=0.64 vs TP cohort 0.90), i.e. a coin
that was hot a few hours ago and has since gone quiet — leaving a wide TP
unreachable.

Two opt-in levers were added to address this:

  1. ``require_volume_deceleration`` — a multi-bar "activity is over" veto
     (mean recent bars / mean prior bars < min ratio).  A *filter*: it can
     only drop entries, never create them.
  2. ``use_fast_atr`` — caps the SIZING ATR by a short-lookback ATR so TP/SL
     track the current realized range instead of a lagging 14-bar ATR.  A
     *sizing* change: it keeps the same entries but moves TP/SL closer.

This script runs controlled A/B comparisons headless and persists results,
mirroring ``run_gate_ab_sweep.py``.  Because the two levers are different in
kind (filter vs sizing), it reports BOTH stop-out count AND timeout count —
the timeout count is the primary signal for "TP too wide".

Variants (per symbol × timeframe):

  * baseline            — both toggles OFF
  * fast_atr            — use_fast_atr=True (sweep fast_atr_length)
  * vol_decel           — require_volume_deceleration=True (sweep min ratio)
  * both                — both ON (sweep over the two knobs jointly)

Usage
-----
Run from the repo root with the project venv::

    .venv/bin/python scripts/run_trend_pullback_ab.py \\
        --symbols AEON-USDT-SWAP,BICO-USDT-SWAP \\
        --timeframes 15m \\
        --days 60 \\
        --capital 1000

Options mirror ``run_gate_ab_sweep.py`` plus:

  --levers  fast_atr,vol_decel,both   (which variants to run; default all three)

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.backtest.engine import BacktestEngine  # noqa: E402
from app.services.backtest.models import BacktestConfig, BacktestResult  # noqa: E402
from app.services.backtest.persistence import (  # noqa: E402
    result_summary_row,
    save_result,
    write_comparison_csv,
)
from app.services.strategies.defaults import strategy_defaults  # noqa: E402

OUTPUT_DIR = ROOT / "backtest_cache" / "cli" / "tp_ab"
OVERVIEW_FILENAME = "overview.json"
_MS = 1_000

STRATEGY = "trend_pullback"


def parse_timeframe(tf: str, ctx: str) -> str:
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


def _strategy_cfg() -> dict[str, Any]:
    cfg = dict(strategy_defaults(STRATEGY))
    cfg["enabled"] = True
    return cfg


def _count_by_reason(result: BacktestResult, *needles: str) -> int:
    """Count trades whose close_reason contains any of ``needles``."""
    n = 0
    for t in result.trades:
        reason = (t.close_reason or "").lower()
        if any(ndl in reason for ndl in needles):
            n += 1
    return n


def _count_stop_out(result: BacktestResult) -> int:
    return _count_by_reason(result, "stop", "sl")


def _count_timeout(result: BacktestResult) -> int:
    # "timeout" (max-hold) and "end_of_data" both mean the TP was never reached.
    return _count_by_reason(result, "timeout", "end_of_data")


async def run_one(
    *,
    symbol: str,
    ltf: str,
    overrides: dict[str, Any],
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
) -> BacktestResult:
    """Run one variant (a full strategy-config override) and return its result."""
    strat_cfg = _strategy_cfg()
    strat_cfg.update(overrides)
    strategies_cfg = {STRATEGY: strat_cfg}
    launcher_config: dict[str, Any] = {
        "mode": "launcher_only",
        "notional_usd": float(capital),
        "strategies": strategies_cfg,
    }
    config = BacktestConfig(
        symbols=[symbol],
        timeframe=ltf,
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=capital,
        strategy_names=[STRATEGY],
        launcher_config=launcher_config,
        strategy_config={},
        warmup_candles=warmup,
        disable_live_execution=True,
        evaluation_mode="finer_ltf",
        evaluation_timeframe="1m",
    )
    engine = BacktestEngine(config)
    return await engine.run()


async def _run_and_record(
    *,
    summaries: list[dict[str, Any]],
    OC: Path,
    symbol: str,
    ltf: str,
    variant: str,
    overrides: dict[str, Any],
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
    tag: str,
) -> BacktestResult | None:
    try:
        result = await run_one(
            symbol=symbol, ltf=ltf, overrides=overrides,
            start_ts=start_ts, end_ts=end_ts,
            capital=capital, warmup=warmup,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗ errored: {exc}")
        summaries.append({
            "run_id": tag, "symbol": symbol, "ltf": ltf,
            "strategy": STRATEGY, "variant": variant, "error": str(exc),
        })
        return None

    if result.error:
        print(f"  ⚠ engine error: {result.error}")

    htf = _htf_for(ltf)
    stop_out = _count_stop_out(result)
    timeout = _count_timeout(result)
    metrics = result.metrics or {}
    summary = result_summary_row(result, run_id=tag, ltf=ltf, htf=htf)
    summary.update({
        "symbol": symbol,
        "strategy": STRATEGY,
        "variant": variant,
        "stop_out_count": stop_out,
        "timeout_count": timeout,
    })
    summaries.append(summary)

    try:
        save_result(result, run_id=tag, output_dir=OC)
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ persist failed: {exc}")

    print(f"  ✓ trades={metrics.get('total_trades')} "
          f"win={metrics.get('win_rate')} "
          f"net={metrics.get('net_profit')} "
          f"stop_out={stop_out} timeout={timeout}")
    return result


async def _amain(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    run_tag = now.strftime("%Y%m%d_%H%M%S")
    OC = OUTPUT_DIR / run_tag
    OC.mkdir(parents=True, exist_ok=True)

    end_ts = int(now.timestamp() * _MS)
    start_ts = int((now - timedelta(days=args.days)).timestamp() * _MS)

    summaries: list[dict[str, Any]] = []
    exit_code = 0

    for symbol in args.symbols:
        for ltf in args.timeframes:
            # ── 0. Baseline (both OFF) ────────────────────────────────
            base_tag = f"{run_tag}_{symbol}_{ltf}_baseline"
            print(f"▶ [baseline] [{symbol} {ltf}] both toggles OFF")
            base = await _run_and_record(
                summaries=summaries, OC=OC, symbol=symbol, ltf=ltf,
                variant="baseline", overrides={},
                start_ts=start_ts, end_ts=end_ts,
                capital=args.capital, warmup=args.warmup, tag=base_tag,
            )
            if base is None:
                exit_code = 1
                continue
            base_trades = (base.metrics or {}).get("total_trades", 0) or 0
            if base_trades < args.min_trades:
                print(f"  ⏭  baseline has {base_trades} trade(s) < min_trades={args.min_trades}; "
                      f"skipping variants for [{symbol} {ltf}]")
                continue

            # ── 1. Fast-ATR sizing only ───────────────────────────────
            if "fast_atr" in args.levers:
                for fast_len in args.fast_atr_lengths:
                    overrides = {"use_fast_atr": True, "fast_atr_length": fast_len}
                    tag = f"{run_tag}_{symbol}_{ltf}_fastatr{fast_len}"
                    print(f"▶ [fast_atr] [{symbol} {ltf}] length={fast_len}")
                    r = await _run_and_record(
                        summaries=summaries, OC=OC, symbol=symbol, ltf=ltf,
                        variant=f"fast_atr@{fast_len}", overrides=overrides,
                        start_ts=start_ts, end_ts=end_ts,
                        capital=args.capital, warmup=args.warmup, tag=tag,
                    )
                    if r is None:
                        exit_code = 1

            # ── 2. Volume-deceleration filter only ────────────────────
            if "vol_decel" in args.levers:
                for min_ratio in args.decel_ratios:
                    overrides = {
                        "require_volume_deceleration": True,
                        "min_volume_decel_ratio": min_ratio,
                    }
                    tag = f"{run_tag}_{symbol}_{ltf}_voldecel{min_ratio}"
                    print(f"▶ [vol_decel] [{symbol} {ltf}] min_ratio={min_ratio}")
                    r = await _run_and_record(
                        summaries=summaries, OC=OC, symbol=symbol, ltf=ltf,
                        variant=f"vol_decel@{min_ratio}", overrides=overrides,
                        start_ts=start_ts, end_ts=end_ts,
                        capital=args.capital, warmup=args.warmup, tag=tag,
                    )
                    if r is None:
                        exit_code = 1

            # ── 3. Both levers ────────────────────────────────────────
            if "both" in args.levers:
                for min_ratio in args.decel_ratios:
                    for fast_len in args.fast_atr_lengths:
                        overrides = {
                            "require_volume_deceleration": True,
                            "min_volume_decel_ratio": min_ratio,
                            "use_fast_atr": True,
                            "fast_atr_length": fast_len,
                        }
                        tag = f"{run_tag}_{symbol}_{ltf}_both{min_ratio}_{fast_len}"
                        print(f"▶ [both] [{symbol} {ltf}] decel={min_ratio} fast_atr={fast_len}")
                        r = await _run_and_record(
                            summaries=summaries, OC=OC, symbol=symbol, ltf=ltf,
                            variant=f"both@{min_ratio}@{fast_len}", overrides=overrides,
                            start_ts=start_ts, end_ts=end_ts,
                            capital=args.capital, warmup=args.warmup, tag=tag,
                        )
                        if r is None:
                            exit_code = 1

    csv_path = write_comparison_csv(summaries, output_dir=OUTPUT_DIR, append=False)
    overview_path = OUTPUT_DIR / OVERVIEW_FILENAME
    overview_path.write_text(json.dumps({"generated_at": run_tag, "runs": summaries}, indent=2, default=str))

    # ── Quality diff ─────────────────────────────────────────────────
    print("\n── Trend Pullback A/B quality diff ──")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for s in summaries:
        groups.setdefault((s["symbol"], s["ltf"]), []).append(s)

    rank_key = args.rank_by
    for (symbol, ltf), runs in sorted(groups.items()):
        print(f"\n{symbol} {ltf}")
        base = next((r for r in runs if r.get("variant") == "baseline"), None)
        if base:
            print(f"  baseline        : trades={base.get('m_total_trades')} "
                  f"win={base.get('m_win_rate')} net={base.get('m_net_profit')} "
                  f"stop={base.get('stop_out_count')} timeout={base.get('timeout_count')}")
        for r in runs:
            if r.get("variant") == "baseline":
                continue
            delta = ""
            if base:
                bt = base.get("m_total_trades") or 0
                ct = r.get("m_total_trades") or 0
                delta = f" (Δ{ct - bt:+d})"
            print(f"  {r.get('variant'):<16}: trades={r.get('m_total_trades')}{delta} "
                  f"win={r.get('m_win_rate')} net={r.get('m_net_profit')} "
                  f"stop={r.get('stop_out_count')} timeout={r.get('timeout_count')}")

    print(f"\nFull results:  {OC}")
    print(f"Comparison CSV: {csv_path}")
    print(f"Overview:       {overview_path}")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Headless A/B of trend_pullback fast-ATR + volume-deceleration levers.")
    parser.add_argument("--symbols", default="BTC-USDT-SWAP", help="Comma-separated OKX symbols.")
    parser.add_argument("--timeframes", default="15m", help="Comma-separated LTFs to backtest.")
    parser.add_argument("--levers", default="fast_atr,vol_decel,both",
                        help="Comma-separated variants: fast_atr,vol_decel,both.")
    parser.add_argument("--days", type=int, default=60, help="Trailing window in days (default 60).")
    parser.add_argument("--capital", type=float, default=1000.0, help="Initial capital / notional.")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup candles before start.")
    parser.add_argument("--min-trades", type=int, default=1,
                        help="Skip variants when baseline yields fewer than this many trades.")
    parser.add_argument("--fast-atr-lengths", default="3,4,5", help="Comma-separated fast ATR lengths to sweep.")
    parser.add_argument("--decel-ratios", default="0.6,0.7,0.8", help="Comma-separated min decel ratios to sweep.")
    parser.add_argument("--rank-by", default="m_sharpe_per_candle", help="Metric to print/sort by.")
    args = parser.parse_args()

    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    args.levers = [l.strip() for l in args.levers.split(",") if l.strip()]
    args.fast_atr_lengths = [int(x) for x in args.fast_atr_lengths.split(",") if x.strip()]
    args.decel_ratios = [float(x) for x in args.decel_ratios.split(",") if x.strip()]
    valid_levers = {"fast_atr", "vol_decel", "both"}
    if not set(args.levers) <= valid_levers:
        print(f"Levers must be a subset of {valid_levers}")
        return 2
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())
