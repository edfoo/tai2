#!/usr/bin/env python3
"""REST-client A/B sweep for the VWAP Reversion strategy.

Thin client driving the tai2 backtest REST API (``POST /backtest/run``).
Focused on the three parameter families identified as the highest-leverage
profitability levers for ``vwap_reversion``:

  Phase 1 — trend-veto (``regime_primary_gate`` × ``max_adx``).
  Phase 2 — structural stop width (``structural_sl_buffer_atr`` ×
      ``atr_min_sl_mult``), coupled with the entry-distance floor
      (``vwap_min_distance_atr``) to preserve R:R.
  Phase 3 — liquidity gates (``require_min_volume`` / ``require_no_funding_bias``).

Each phase prints an OFF/ON (or A/B) quality diff: win rate, profit factor,
net profit, stop-out count, and TP count.

Usage (server must be running)::

    .venv/bin/python scripts/run_vwap_ab_sweep.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP,XRP-USDT-SWAP,LTC-USDT-SWAP,ADA-USDT-SWAP \\
        --timeframe 15m --days 60 --capital 1000 \\
        --base-url http://localhost:8000

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from app.services.backtest.client import (
    build_single_strategy_launcher,
    count_stop_outs,
    count_tp,
    fmt_cell,
    submit_many_and_poll_timed,
    summary_row,
)

STRATEGY = "vwap_reversion"


def _payload_for(
    *,
    base_url: str,
    symbol: str,
    ltf: str,
    overrides: dict[str, Any],
    days: int,
    capital: float,
    warmup: int,
) -> dict[str, Any]:
    launcher = build_single_strategy_launcher(
        strategy_name=STRATEGY, capital=capital, overrides=overrides,
    )
    return {
        "symbols": [symbol],
        "timeframe": ltf,
        "strategy_names": [STRATEGY],
        "days": days,
        "capital": capital,
        "warmup": warmup,
        "launcher_config": launcher,
    }


def _row_from_envelope(
    envelope: dict[str, Any],
    *,
    symbol: str,
    ltf: str,
    label: str,
    run_id: str,
) -> dict[str, Any]:
    return summary_row(
        envelope,
        run_id=run_id,
        ltf=ltf,
        htf="",
        symbols=symbol,
        strategies=STRATEGY,
        extra={
            "symbol": symbol,
            "label": label,
            "strategy": STRATEGY,
            "stop_out_count": count_stop_outs(envelope),
            "tp_count": count_tp(envelope),
        },
    )


def _fmt(summary: dict[str, Any]) -> str:
    return (
        f"trades={fmt_cell(summary.get('m_total_trades'), 3)}  "
        f"win={fmt_cell(summary.get('m_win_rate'), 5)}%  "
        f"PF={fmt_cell(summary.get('m_profit_factor'), 5)}  "
        f"net={fmt_cell(summary.get('m_net_profit'), 8)}  "
        f"avg_trade={fmt_cell(summary.get('m_average_trade'), 7)}  "
        f"stop_out={fmt_cell(summary.get('stop_out_count'), 3)}  "
        f"tp={fmt_cell(summary.get('tp_count'), 3)}  "
        f"sharpe={fmt_cell(summary.get('m_sharpe_per_candle'), 7)}"
    )


def _main(args: argparse.Namespace) -> int:
    ltf = args.timeframe
    exit_code = 0

    for symbol in args.symbols:
        print(f"\n{'=' * 70}\n{symbol}  {ltf}  ({args.days}d, capital={args.capital})\n{'=' * 70}")

        def make_payload(overrides: dict[str, Any]) -> dict[str, Any]:
            return _payload_for(
                base_url=args.base_url, symbol=symbol, ltf=ltf, overrides=overrides,
                days=args.days, capital=args.capital, warmup=args.warmup,
            )

        def submit(label: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
            ((envelope, err),) = submit_many_and_poll_timed(
                base_url=args.base_url, payloads=[make_payload(overrides)],
                label=f"baseline {symbol}", max_workers=args.workers,
            )
            if err is not None:
                print(f"    ✗ {label} errored: {err}")
                return None
            return _row_from_envelope(envelope, symbol=symbol, ltf=ltf, label=label, run_id=f"{symbol}_{label}")

        # ── Baseline (canonical defaults) ─────────────────────────────
        base = submit("baseline", {})
        if base is None:
            exit_code = 1
            continue
        print(f"  [baseline] {_fmt(base)}")

        # ── Build all variant specs, then submit concurrently ─────────
        phase1 = [
            (f"p1_gate={gate}_max_adx={max_adx}", {"regime_primary_gate": gate, "max_adx": max_adx})
            for gate in ("adx", "bb") for max_adx in (22.0, 25.0, 28.0)
        ]
        phase2 = [
            (f"p2_slbuf={ov['structural_sl_buffer_atr']}_minsl={ov['atr_min_sl_mult']}_mindist={ov['vwap_min_distance_atr']}", ov)
            for ov in [
                {"structural_sl_buffer_atr": 0.15, "atr_min_sl_mult": 0.5, "vwap_min_distance_atr": 2.5},
                {"structural_sl_buffer_atr": 0.25, "atr_min_sl_mult": 0.75, "vwap_min_distance_atr": 2.75},
                {"structural_sl_buffer_atr": 0.35, "atr_min_sl_mult": 1.0, "vwap_min_distance_atr": 3.0},
                {"structural_sl_buffer_atr": 0.35, "atr_min_sl_mult": 1.0, "vwap_min_distance_atr": 3.25},
            ]
        ]
        phase3 = [
            ("_".join(f"{k}={v}" for k, v in ov.items()), ov)
            for ov in [
                {"require_min_volume": True},
                {"require_no_funding_bias": True},
            ]
        ]
        all_variants = phase1 + phase2 + phase3

        results = submit_many_and_poll_timed(
            base_url=args.base_url,
            payloads=[make_payload(ov) for (_, ov) in all_variants],
            label=f"variants {symbol}",
            max_workers=args.workers,
        )
        for (label, _ov), (envelope, err) in zip(all_variants, results):
            if err is not None:
                print(f"    ✗ {label} errored: {err}")
                exit_code = 1
                continue
            row = _row_from_envelope(envelope, symbol=symbol, ltf=ltf, label=label, run_id=f"{symbol}_{label}")
            print(f"    [{label:>48}] {_fmt(row)}")

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="REST-client A/B sweep for VWAP Reversion.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--symbols", default="BTC-USDT-SWAP,ETH-USDT-SWAP,XRP-USDT-SWAP,LTC-USDT-SWAP,ADA-USDT-SWAP",
                        help="Comma-separated OKX symbols.")
    parser.add_argument("--timeframe", default="15m", help="LTF to backtest (default 15m).")
    parser.add_argument("--days", type=int, default=60, help="Trailing window in days (default 60).")
    parser.add_argument("--capital", type=float, default=1000.0, help="Initial capital / notional (default 1000).")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup candles (default 200).")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1,
                        help="Max concurrent submissions to the server (default = CPU count).")
    args = parser.parse_args()

    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return _main(args)


if __name__ == "__main__":
    sys.exit(main())
