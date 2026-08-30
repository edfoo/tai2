#!/usr/bin/env python3
"""Headless A/B sweep for the VWAP Reversion strategy.

Focused on the three parameter families identified as the highest-leverage
profitability levers for ``vwap_reversion``:

  Phase 1 — trend-veto (``regime_primary_gate`` × ``max_adx``):
      The default ``regime_primary_gate="bb"`` demotes ADX to a soft filter,
      so a low-volatility grinding trend passes the BB-bandwidth chop gate and
      the strategy knife-catches.  Compare "adx" vs "bb" as primary.

  Phase 2 — structural stop width (``structural_sl_buffer_atr`` ×
      ``atr_min_sl_mult``), coupled with the entry-distance floor
      (``vwap_min_distance_atr``) to preserve R:R (wider SL needs a bigger
      TP-hop back to VWAP).

  Phase 3 — liquidity gates (``require_min_volume`` / ``require_no_funding_bias``).

Each phase prints an OFF/ON (or A/B) quality diff: win rate, profit factor,
net profit, stop-out count, and expectancy.  Results are persisted under
``backtest_cache/cli/vwap/`` and a comparison CSV is written.

Usage
-----
    .venv/bin/python scripts/run_vwap_ab_sweep.py \
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP,XRP-USDT-SWAP,LTC-USDT-SWAP,ADA-USDT-SWAP \
        --timeframe 15m --days 60 --capital 1000

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
from app.services.backtest.persistence import result_summary_row, write_comparison_csv  # noqa: E402
from app.services.strategies.defaults import strategy_defaults  # noqa: E402

OUTPUT_DIR = ROOT / "backtest_cache" / "cli" / "vwap"
_MS = 1_000

STRATEGY = "vwap_reversion"


def _htf_for(tf: str) -> str:
    return {"1m": "5m", "5m": "15m", "15m": "1H", "1H": "4H", "4H": "1D", "1D": "1W"}.get(tf, "")


def _strategy_cfg(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = dict(strategy_defaults(STRATEGY))
    cfg["enabled"] = True
    if overrides:
        cfg.update(overrides)
    return cfg


def _count_stop_out(result: BacktestResult) -> int:
    n = 0
    for t in result.trades:
        reason = (t.close_reason or "").lower()
        if "stop" in reason or "sl" in reason:
            n += 1
    return n


def _count_tp(result: BacktestResult) -> int:
    n = 0
    for t in result.trades:
        reason = (t.close_reason or "").lower()
        if reason in ("tp", "take_profit") or reason.startswith("tp"):
            n += 1
    return n


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
    strategies_cfg = {STRATEGY: _strategy_cfg(overrides)}
    launcher_config = {
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


def _row(result: BacktestResult, run_id: str, ltf: str, symbol: str, label: str) -> dict[str, Any]:
    htf = _htf_for(ltf)
    m = result.metrics or {}
    summary = result_summary_row(result, run_id=run_id, ltf=ltf, htf=htf)
    summary.update({
        "symbol": symbol,
        "label": label,
        "strategy": STRATEGY,
        "stop_out_count": _count_stop_out(result),
        "tp_count": _count_tp(result),
    })
    return summary


def _fmt(summary: dict[str, Any]) -> str:
    m = summary
    return (
        f"trades={m.get('m_total_trades'):>3}  win={m.get('m_win_rate'):>5}%  "
        f"PF={m.get('m_profit_factor'):>5}  net={m.get('m_net_profit'):>8}  "
        f"avg_trade={m.get('m_average_trade'):>7}  stop_out={summary.get('stop_out_count'):>3}  "
        f"sharpe={m.get('m_sharpe_per_candle'):>7}"
    )


async def _amain(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    run_tag = now.strftime("%Y%m%d_%H%M%S")
    OC = OUTPUT_DIR / run_tag
    OC.mkdir(parents=True, exist_ok=True)

    end_ts = int(now.timestamp() * _MS)
    start_ts = int((now - timedelta(days=args.days)).timestamp() * _MS)
    ltf = args.timeframe

    summaries: list[dict[str, Any]] = []
    exit_code = 0

    for symbol in args.symbols:
        print(f"\n{'=' * 70}\n{symbol}  {ltf}  ({args.days}d, capital={args.capital})\n{'=' * 70}")

        # ── Baseline (canonical defaults) ─────────────────────────────
        try:
            base = await run_one(symbol=symbol, ltf=ltf, overrides={},
                                 start_ts=start_ts, end_ts=end_ts,
                                 capital=args.capital, warmup=args.warmup)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ baseline errored: {exc}")
            exit_code = 1
            continue
        base_row = _row(base, f"{run_tag}_{symbol}_baseline", ltf, symbol, "baseline")
        summaries.append(base_row)
        print(f"  [baseline] {_fmt(base_row)}")
        if base.error:
            print(f"    ⚠ engine error: {base.error}")

        # ── Phase 1: trend veto ───────────────────────────────────────
        print("\n  -- Phase 1: regime_primary_gate × max_adx --")
        for gate in ("adx", "bb"):
            for max_adx in (22.0, 25.0, 28.0):
                label = f"p1_gate={gate}_max_adx={max_adx}"
                try:
                    r = await run_one(
                        symbol=symbol, ltf=ltf,
                        overrides={"regime_primary_gate": gate, "max_adx": max_adx},
                        start_ts=start_ts, end_ts=end_ts,
                        capital=args.capital, warmup=args.warmup,
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"    ✗ {label} errored: {exc}")
                    exit_code = 1
                    continue
                row = _row(r, f"{run_tag}_{symbol}_{label}", ltf, symbol, label)
                summaries.append(row)
                print(f"    [{label:>30}] {_fmt(row)}")

        # ── Phase 2: structural stop width × distance floor ───────────
        print("\n  -- Phase 2: SL buffer / min-SL × vwap_min_distance_atr --")
        phase2 = [
            {"structural_sl_buffer_atr": 0.15, "atr_min_sl_mult": 0.5, "vwap_min_distance_atr": 2.5},
            {"structural_sl_buffer_atr": 0.25, "atr_min_sl_mult": 0.75, "vwap_min_distance_atr": 2.75},
            {"structural_sl_buffer_atr": 0.35, "atr_min_sl_mult": 1.0, "vwap_min_distance_atr": 3.0},
            {"structural_sl_buffer_atr": 0.35, "atr_min_sl_mult": 1.0, "vwap_min_distance_atr": 3.25},
        ]
        for i, ov in enumerate(phase2):
            label = (f"p2_slbuf={ov['structural_sl_buffer_atr']}_"
                     f"minsl={ov['atr_min_sl_mult']}_mindist={ov['vwap_min_distance_atr']}")
            try:
                r = await run_one(symbol=symbol, ltf=ltf, overrides=ov,
                                  start_ts=start_ts, end_ts=end_ts,
                                  capital=args.capital, warmup=args.warmup)
            except Exception as exc:  # noqa: BLE001
                print(f"    ✗ {label} errored: {exc}")
                exit_code = 1
                continue
            row = _row(r, f"{run_tag}_{symbol}_{label}", ltf, symbol, label)
            summaries.append(row)
            print(f"    [{label:>48}] {_fmt(row)}")

        # ── Phase 3: liquidity gates ─────────────────────────────────
        print("\n  -- Phase 3: liquidity gates --")
        phase3 = [
            {"require_min_volume": True},
            {"require_no_funding_bias": True},
        ]
        for ov in phase3:
            label = "_".join(f"{k}={v}" for k, v in ov.items())
            try:
                r = await run_one(symbol=symbol, ltf=ltf, overrides=ov,
                                  start_ts=start_ts, end_ts=end_ts,
                                  capital=args.capital, warmup=args.warmup)
            except Exception as exc:  # noqa: BLE001
                print(f"    ✗ {label} errored: {exc}")
                exit_code = 1
                continue
            row = _row(r, f"{run_tag}_{symbol}_{label}", ltf, symbol, label)
            summaries.append(row)
            print(f"    [{label:>28}] {_fmt(row)}")

    # ── Persist comparison ───────────────────────────────────────────
    csv_path = write_comparison_csv(summaries, output_dir=OUTPUT_DIR, append=False)
    overview = OC / "overview.json"
    overview.write_text(json.dumps({"generated_at": run_tag, "runs": summaries}, indent=2, default=str))
    print(f"\nFull results:  {OC}")
    print(f"Comparison CSV: {csv_path}")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="A/B sweep for VWAP Reversion.")
    parser.add_argument("--symbols", default="BTC-USDT-SWAP,ETH-USDT-SWAP,XRP-USDT-SWAP,LTC-USDT-SWAP,ADA-USDT-SWAP",
                        help="Comma-separated OKX symbols.")
    parser.add_argument("--timeframe", default="15m", help="LTF to backtest (default 15m).")
    parser.add_argument("--days", type=int, default=60, help="Trailing window in days (default 60).")
    parser.add_argument("--capital", type=float, default=1000.0, help="Initial capital / notional (default 1000).")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup candles (default 200).")
    args = parser.parse_args()

    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())
