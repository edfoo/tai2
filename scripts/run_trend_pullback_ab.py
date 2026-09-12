#!/usr/bin/env python3
"""REST-client A/B harness for the two trend_pullback "wide TP/SL" levers.

Thin client driving the tai2 backtest REST API (``POST /backtest/run``).  The
two levers under test:

  1. ``require_volume_deceleration`` — a multi-bar "activity is over" veto
     (mean recent bars / mean prior bars < min ratio).  A *filter*: it can
     only drop entries, never create them.
  2. ``use_fast_atr`` — caps the SIZING ATR by a short-lookback ATR so TP/SL
     track the current realized range instead of a lagging 14-bar ATR.

Because the two levers are different in kind (filter vs sizing), this reports
BOTH stop-out count AND timeout count — the timeout count is the primary
signal for "TP too wide".

Variants (per symbol × timeframe):

  * baseline            — both toggles OFF
  * fast_atr            — use_fast_atr=True (sweep fast_atr_length)
  * vol_decel           — require_volume_deceleration=True (sweep min ratio)
  * both                — both ON (sweep over the two knobs jointly)

Usage (server must be running)::

    .venv/bin/python scripts/run_trend_pullback_ab.py \\
        --symbols AEON-USDT-SWAP,BICO-USDT-SWAP \\
        --timeframes 15m \\
        --days 60 \\
        --capital 1000 \\
        --base-url http://localhost:8000

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from app.services.backtest.client import (
    BacktestClientError,
    build_single_strategy_launcher,
    count_stop_outs,
    count_timeouts,
    submit_and_poll,
    summary_row,
)

STRATEGY = "trend_pullback"


def _run_one(
    *,
    base_url: str,
    symbol: str,
    ltf: str,
    variant: str,
    overrides: dict[str, Any],
    days: int,
    capital: float,
    warmup: int,
    run_id: str,
) -> dict[str, Any]:
    launcher = build_single_strategy_launcher(
        strategy_name=STRATEGY, capital=capital, overrides=overrides,
    )
    payload = {
        "symbols": [symbol],
        "timeframe": ltf,
        "strategy_names": [STRATEGY],
        "days": days,
        "capital": capital,
        "warmup": warmup,
        "launcher_config": launcher,
    }
    envelope = submit_and_poll(base_url=base_url, payload=payload)
    return summary_row(
        envelope,
        run_id=run_id,
        ltf=ltf,
        htf="",
        symbols=symbol,
        strategies=STRATEGY,
        extra={
            "symbol": symbol,
            "strategy": STRATEGY,
            "variant": variant,
            "stop_out_count": count_stop_outs(envelope),
            "timeout_count": count_timeouts(envelope),
        },
    )


def _main(args: argparse.Namespace) -> int:
    summaries: list[dict[str, Any]] = []
    exit_code = 0

    def run_variant(symbol: str, ltf: str, variant: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
        tag = f"{symbol}_{ltf}_{variant}"
        try:
            return _run_one(
                base_url=args.base_url, symbol=symbol, ltf=ltf, variant=variant,
                overrides=overrides, days=args.days, capital=args.capital,
                warmup=args.warmup, run_id=tag,
            )
        except BacktestClientError as exc:
            print(f"  ✗ [{symbol} {ltf} {variant}] errored: {exc}")
            return None

    for symbol in args.symbols:
        for ltf in args.timeframes:
            base = run_variant(symbol, ltf, "baseline", {})
            if base is None:
                exit_code = 1
                continue
            summaries.append(base)
            print(f"  ✓ baseline: trades={base.get('m_total_trades')} "
                  f"win={base.get('m_win_rate')} net={base.get('m_net_profit')} "
                  f"stop={base.get('stop_out_count')} timeout={base.get('timeout_count')}")

            base_trades = base.get("m_total_trades") or 0
            if base_trades < args.min_trades:
                print(f"  ⏭  baseline has {base_trades} trade(s) < min_trades={args.min_trades}; skipping variants")
                continue

            if "fast_atr" in args.levers:
                for fast_len in args.fast_atr_lengths:
                    overrides = {"use_fast_atr": True, "fast_atr_length": fast_len}
                    row = run_variant(symbol, ltf, f"fast_atr@{fast_len}", overrides)
                    if row is not None:
                        summaries.append(row)
                    else:
                        exit_code = 1

            if "vol_decel" in args.levers:
                for min_ratio in args.decel_ratios:
                    overrides = {
                        "require_volume_deceleration": True,
                        "min_volume_decel_ratio": min_ratio,
                    }
                    row = run_variant(symbol, ltf, f"vol_decel@{min_ratio}", overrides)
                    if row is not None:
                        summaries.append(row)
                    else:
                        exit_code = 1

            if "both" in args.levers:
                for min_ratio in args.decel_ratios:
                    for fast_len in args.fast_atr_lengths:
                        overrides = {
                            "require_volume_deceleration": True,
                            "min_volume_decel_ratio": min_ratio,
                            "use_fast_atr": True,
                            "fast_atr_length": fast_len,
                        }
                        row = run_variant(symbol, ltf, f"both@{min_ratio}@{fast_len}", overrides)
                        if row is not None:
                            summaries.append(row)
                        else:
                            exit_code = 1

    # ── Quality diff ─────────────────────────────────────────────────
    print("\n── Trend Pullback A/B quality diff ──")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for s in summaries:
        groups.setdefault((s["symbol"], s["ltf"]), []).append(s)

    for (symbol, ltf), runs in sorted(groups.items()):
        print(f"\n{symbol} {ltf}")
        ordered = sorted(runs, key=lambda r: (r.get("variant") != "baseline", str(r.get("variant") or "")))
        base = next((r for r in ordered if r.get("variant") == "baseline"), None)
        if base:
            print(f"  baseline        : trades={base.get('m_total_trades')} "
                  f"win={base.get('m_win_rate')} net={base.get('m_net_profit')} "
                  f"stop={base.get('stop_out_count')} timeout={base.get('timeout_count')}")
        for r in ordered:
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

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="REST-client A/B of trend_pullback fast-ATR + volume-deceleration levers.")
    parser.add_argument("--base-url", default="http://localhost:8000")
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
    return _main(args)


if __name__ == "__main__":
    sys.exit(main())
