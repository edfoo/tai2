#!/usr/bin/env python3
"""REST-client A/B sweep of the §3 liquidity-aware entry gates.

Thin client that drives the tai2 backtest REST API (``POST /backtest/run``).
The gate catalogue (which strategy/gate/switch/threshold to sweep) lives here;
each combination is submitted as a single-strategy backtest to the running
server and the results are polled back for comparison.

Why a client (not a local engine run)
-------------------------------------
Running backtests in-process duplicated the engine + config plumbing that the
server already exposes over REST.  This client reuses that single interface so
live and headless runs can never drift apart.

Supported gates (the ones that consume only OHLCV-derived data, so they are
faithfully reproducible in backtest — funding/OI/imbalance gates silently
skip in backtest because their data is never injected by snapshot_builder):

  mean_reversion.require_price_in_va
  trend_pullback.require_poc_proximity       (+ poc_proximity_va_width)
  liquidity_sweep.require_close_in_va
  liquidity_sweep.require_macro_sl           (+ macro_sl_lookback)

Usage (server must be running)::

    .venv/bin/python scripts/run_gate_ab_sweep.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframes 15m,1H \\
        --strategy mean_reversion \\
        --gate require_price_in_va \\
        --days 60 \\
        --capital 1000 \\
        --base-url http://localhost:8000

Run ALL backtest-ready gates for a strategy (switch ON/OFF x threshold)::

    .venv/bin/python scripts/run_gate_ab_sweep.py \\
        --strategy liquidity_sweep \\
        --gate all

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
    submit_and_poll,
    summary_row,
)

# Maps strategy -> gate -> (switch_key, threshold_key, ordered threshold candidates).
GATE_CATALOGUE: dict[str, dict[str, dict[str, Any]]] = {
    "mean_reversion": {
        "require_price_in_va": {"switch": "require_price_in_va", "threshold_key": None, "thresholds": []},
    },
    "trend_pullback": {
        "require_poc_proximity": {
            "switch": "require_poc_proximity",
            "threshold_key": "poc_proximity_va_width",
            "thresholds": [0.1, 0.2, 0.3],
        },
    },
    "liquidity_sweep": {
        "require_close_in_va": {"switch": "require_close_in_va", "threshold_key": None, "thresholds": []},
        "require_macro_sl": {
            "switch": "require_macro_sl",
            "threshold_key": "macro_sl_lookback",
            "thresholds": [30, 50, 80],
        },
    },
}


def _run_one(
    *,
    base_url: str,
    symbol: str,
    ltf: str,
    strategy: str,
    gate_cfg: dict[str, Any],
    threshold: Any,
    switch_on: bool,
    days: int,
    capital: float,
    warmup: int,
    run_id: str,
) -> dict[str, Any]:
    """Submit one gate configuration and return its summary row."""
    overrides: dict[str, Any] = {gate_cfg["switch"]: bool(switch_on)}
    if switch_on and gate_cfg["threshold_key"] and threshold is not None:
        overrides[gate_cfg["threshold_key"]] = threshold

    launcher = build_single_strategy_launcher(
        strategy_name=strategy, capital=capital, overrides=overrides,
    )
    payload = {
        "symbols": [symbol],
        "timeframe": ltf,
        "strategy_names": [strategy],
        "days": days,
        "capital": capital,
        "warmup": warmup,
        "launcher_config": launcher,
    }
    envelope = submit_and_poll(base_url=base_url, payload=payload)
    thr_str = "None" if threshold is None else str(threshold)
    return summary_row(
        envelope,
        run_id=run_id,
        ltf=ltf,
        htf="",
        symbols=symbol,
        strategies=strategy,
        extra={
            "symbol": symbol,
            "strategy": strategy,
            "gate": gate_cfg["switch"],
            "switch": switch_on,
            "threshold": thr_str,
            "stop_out_count": count_stop_outs(envelope),
        },
    )


def _main(args: argparse.Namespace) -> int:
    strategy = args.strategy

    # Resolve which gate(s) to sweep.
    if args.gate == "all":
        if strategy not in GATE_CATALOGUE:
            print(f"Strategy '{strategy}' has no backtest-ready gates. "
                  f"Available: {list(GATE_CATALOGUE.keys())}")
            return 1
        gates = list(GATE_CATALOGUE[strategy].items())
    else:
        if strategy not in GATE_CATALOGUE or args.gate not in GATE_CATALOGUE[strategy]:
            print(f"Gate '{args.gate}' not backtest-ready for '{strategy}'. "
                  f"Catalogue: {GATE_CATALOGUE.get(strategy, {})}")
            return 1
        gates = [(args.gate, GATE_CATALOGUE[strategy][args.gate])]

    summaries: list[dict[str, Any]] = []
    exit_code = 0

    for gate_name, gate_cfg in gates:
        switch_key = gate_cfg["switch"]
        thresholds = gate_cfg["thresholds"]

        for symbol in args.symbols:
            for ltf in args.timeframes:
                # ── 1. OFF baseline ──────────────────────────────────
                off_tag = f"{symbol}_{ltf}_{gate_name}_off"
                print(f"▶ [baseline OFF] [{symbol} {ltf}] {strategy}.{switch_key}=off")
                try:
                    base_row = _run_one(
                        base_url=args.base_url, symbol=symbol, ltf=ltf, strategy=strategy,
                        gate_cfg=gate_cfg, threshold=None, switch_on=False,
                        days=args.days, capital=args.capital, warmup=args.warmup,
                        run_id=off_tag,
                    )
                except BacktestClientError as exc:
                    print(f"  ✗ errored: {exc}")
                    exit_code = 1
                    continue
                summaries.append(base_row)
                base_trades = base_row.get("m_total_trades") or 0
                print(f"  ✓ trades={base_row.get('m_total_trades')} "
                      f"win={base_row.get('m_win_rate')} net={base_row.get('m_net_profit')} "
                      f"stop_out={base_row.get('stop_out_count')}")

                # ── 2. Skip ON variants if baseline is sterile ────────
                if base_trades < args.min_trades:
                    print(f"  ⏭  skipping ON variants: baseline has only {base_trades} trade(s) "
                          f"< min_trades={args.min_trades} (a pure filter cannot create trades).")
                    continue

                on_variants: list[tuple[bool, Any | None]] = [(True, thr) for thr in thresholds]
                if not thresholds:
                    on_variants.append((True, None))

                for (switch_on, threshold) in on_variants:
                    thr_str = "None" if threshold is None else str(threshold)
                    tag = f"{symbol}_{ltf}_{gate_name}_on" + (f"_t{threshold}" if threshold is not None else "")
                    print(f"▶ [ON] [{symbol} {ltf}] {strategy}.{switch_key}=on"
                          + (f" @{gate_cfg['threshold_key']}={thr_str}" if gate_cfg["threshold_key"] else ""))
                    try:
                        row = _run_one(
                            base_url=args.base_url, symbol=symbol, ltf=ltf, strategy=strategy,
                            gate_cfg=gate_cfg, threshold=threshold, switch_on=True,
                            days=args.days, capital=args.capital, warmup=args.warmup,
                            run_id=tag,
                        )
                    except BacktestClientError as exc:
                        print(f"  ✗ errored: {exc}")
                        exit_code = 1
                        continue
                    summaries.append(row)
                    print(f"  ✓ trades={row.get('m_total_trades')} "
                          f"win={row.get('m_win_rate')} net={row.get('m_net_profit')} "
                          f"stop_out={row.get('stop_out_count')}")

    # ── Print a per-gate quality diff ─────────────────────────────────
    print("\n── Gate A/B quality diff ──")
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for s in summaries:
        groups.setdefault((s["symbol"], s["ltf"], s["gate"]), []).append(s)

    rank_key = args.rank_by
    for (symbol, ltf, gate), runs in sorted(groups.items()):
        runs_with = [r for r in runs if r.get("switch")]
        runs_off = [r for r in runs if not r.get("switch")]
        print(f"\n{symbol} {ltf} :: {strategy}.{gate}")
        if runs_off:
            base = runs_off[0]
            print(f"  OFF: trades={base.get('m_total_trades')} "
                  f"win={base.get('m_win_rate')} {rank_key}={base.get(rank_key)} "
                  f"net={base.get('m_net_profit')} stop_out={base.get('stop_out_count')}")
        for r in runs_with:
            delta_trades = ""
            if runs_off:
                b = (runs_off[0].get('m_total_trades') or 0)
                cur = (r.get('m_total_trades') or 0)
                delta_trades = f" ({cur - b:+d} trades)"
            print(f"  ON: trades={r.get('m_total_trades')}{delta_trades} "
                  f"win={r.get('m_win_rate')} {rank_key}={r.get(rank_key)} "
                  f"net={r.get('m_net_profit')} stop_out={r.get('stop_out_count')}")

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="REST-client A/B sweep of §3 liquidity-aware gates.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--symbols", default="BTC-USDT-SWAP", help="Comma-separated OKX symbols.")
    parser.add_argument("--timeframes", default="15m,1H", help="Comma-separated LTFs to backtest.")
    parser.add_argument("--strategy", required=True,
                        help="Strategy to sweep. Choose from: mean_reversion, trend_pullback, liquidity_sweep.")
    parser.add_argument("--gate", required=True,
                        help="Gate to A/B. Use a gate name or 'all' to sweep every backtest-ready gate.")
    parser.add_argument("--days", type=int, default=60, help="Trailing window in days (default 60).")
    parser.add_argument("--capital", type=float, default=1000.0, help="Initial capital / notional.")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup candles before start.")
    parser.add_argument("--min-trades", type=int, default=1,
                        help="Skip ON variants when the OFF baseline yields fewer than this many trades.")
    parser.add_argument("--rank-by", default="m_sharpe_per_candle", help="Metric to print/sort by.")
    args = parser.parse_args()

    valid = {"mean_reversion", "trend_pullback", "liquidity_sweep"}
    if args.strategy not in valid:
        print(f"Strategy must be one of {valid}")
        return 2
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    return _main(args)


if __name__ == "__main__":
    sys.exit(main())
