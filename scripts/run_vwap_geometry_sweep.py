#!/usr/bin/env python3
"""Focused TP/SL geometry sweep for the VWAP Reversion strategy (in-process).

Motivation (2026-10-30 performance review): ``vwap_reversion`` is the only
net-losing strategy in the live book (35 trades, 12W/23L, -2.58 USDT) with a
payoff ratio of ~0.37 — it wins small (TP = VWAP, only ~2.5 ATR away) and
loses big (SL sits beyond the extension candle).  This sweep tests the three
geometry levers that control that asymmetry:

  * ``vwap_min_distance_atr``    — deeper extension required to enter
    (farther TP, better R:R, fewer knife-catches).
  * ``structural_sl_buffer_atr`` — SL width beyond the extension candle.
  * ``max_adx``                  — trend veto (block fading strong trends).

Runs the grid **in-process** via :class:`BacktestGrid` (each combination in
its own subprocess) so it does not depend on the REST server — important
because the live server runs with ``uvicorn --reload`` and restarts on every
file save, wiping its in-memory job registry.

Usage::

    .venv/bin/python scripts/run_vwap_geometry_sweep.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP,ADA-USDT-SWAP,DOGE-USDT-SWAP \\
        --timeframe 15m --days 60

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from typing import Any

from app.services.backtest.client import build_single_strategy_launcher
from app.services.backtest.grid import BacktestGrid
from app.services.backtest.models import GridConfig, GridParamDef
from app.services.backtest.runner import build_backtest_config

STRATEGY = "vwap_reversion"


def _fmt_metrics(m: dict[str, Any]) -> str:
    def g(k: str, w: int = 8) -> str:
        v = m.get(k)
        if v is None:
            return "—".rjust(w)
        if isinstance(v, float):
            return f"{v:.4f}".rjust(w)
        return f"{v}".rjust(w)

    return (
        f"trades={g('total_trades', 4)}  win%={g('win_rate', 6)}  "
        f"PF={g('profit_factor', 6)}  net={g('net_profit_after_cost', 9)}  "
        f"avg={g('average_trade', 8)}  maxDD%={g('max_drawdown_pct', 7)}"
    )


def _build_base(
    *,
    symbols: list[str],
    ltf: str,
    days: int,
    capital: float,
    warmup: int,
    evaluation_mode: str,
) -> Any:
    now_ms = int(time.time() * 1000)
    launcher = build_single_strategy_launcher(strategy_name=STRATEGY, capital=capital)
    return build_backtest_config(
        symbols=symbols,
        timeframe=ltf,
        strategy_names=[STRATEGY],
        start_ts=now_ms - days * 86_400_000,
        end_ts=now_ms,
        capital=capital,
        warmup=warmup,
        launcher_config=launcher,
        # "closed" steps once per closed LTF candle (fast, good for relative
        # parameter ranking).  "finer_ltf" steps on 1m candles — far more
        # realistic but ~15x slower, which makes a 27-combo grid impractical.
        evaluation_mode=evaluation_mode,
    )


async def _run_baseline(base: Any) -> dict[str, Any] | None:
    from app.services.backtest.engine import BacktestEngine

    res = await BacktestEngine(base).run()
    if res.error:
        print(f"  ✗ baseline errored: {res.error}")
        return None
    return res.metrics


async def _run_grid(
    base: Any,
    params: list[GridParamDef],
    *,
    rank_by: str,
    min_trades: int,
) -> Any:
    cfg = GridConfig(
        base_config=base,
        params=params,
        rank_by=rank_by,
        min_trades=min_trades,
        top_n_detail=0,
    )
    grid = BacktestGrid(cfg)
    return await grid.run()


def _main(args: argparse.Namespace) -> int:
    base = _build_base(
        symbols=args.symbols, ltf=args.timeframe, days=args.days,
        capital=args.capital, warmup=args.warmup,
        evaluation_mode=args.evaluation_mode,
    )

    print(f"\n{'=' * 78}")
    print(f"VWAP Reversion geometry sweep — {len(args.symbols)} symbols, {args.timeframe}, {args.days}d")
    print(f"symbols: {', '.join(args.symbols)}")
    print(f"{'=' * 78}")

    print("\n[baseline] canonical defaults")
    bm = asyncio.run(_run_baseline(base))
    if bm is not None:
        print(f"  {_fmt_metrics(bm)}")

    params = [
        GridParamDef("strategies.vwap_reversion.vwap_min_distance_atr", list(args.min_dist), "min_dist_atr"),
        GridParamDef("strategies.vwap_reversion.structural_sl_buffer_atr", list(args.sl_buffer), "sl_buffer_atr"),
        GridParamDef("strategies.vwap_reversion.max_adx", list(args.max_adx), "max_adx"),
    ]
    n_combos = 1
    for p in params:
        n_combos *= len(p.values)
    print(f"\n[grid] {n_combos} combinations, rank_by={args.rank_by}")

    t0 = time.monotonic()
    grid = asyncio.run(_run_grid(base, params, rank_by=args.rank_by, min_trades=args.min_trades))
    print(f"  grid finished in {time.monotonic() - t0:.1f}s")

    if grid.error:
        print(f"  ✗ grid error: {grid.error}")
        return 1

    runs = grid.runs
    ranked = grid.ranked

    print(f"\n{'rank':>4}  {'min_dist':>8} {'sl_buf':>7} {'max_adx':>7}  metrics")
    print("-" * 78)
    for rank, run in enumerate(ranked[: args.top], start=1):
        p = run.params or {}
        m = (run.result.metrics if run.result is not None else {}) or {}
        md = p.get("strategies.vwap_reversion.vwap_min_distance_atr")
        sb = p.get("strategies.vwap_reversion.structural_sl_buffer_atr")
        ax = p.get("strategies.vwap_reversion.max_adx")
        print(f"{rank:>4}  {str(md):>8} {str(sb):>7} {str(ax):>7}  {_fmt_metrics(m)}")

    if not ranked:
        print("  (no combinations met min_trades)")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="VWAP Reversion TP/SL geometry sweep (in-process).")
    p.add_argument("--symbols", default="BTC-USDT-SWAP,ETH-USDT-SWAP,ADA-USDT-SWAP,DOGE-USDT-SWAP,ANIME-USDT-SWAP,CRV-USDT-SWAP,ARX-USDT-SWAP,AVNT-USDT-SWAP")
    p.add_argument("--timeframe", default="15m")
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--capital", type=float, default=1000.0)
    p.add_argument("--warmup", type=int, default=200)
    p.add_argument(
        "--evaluation-mode", default="closed", choices=["closed", "finer_ltf"],
        help="closed = step per closed LTF candle (fast); finer_ltf = step on 1m (slow).",
    )
    p.add_argument("--rank-by", default="net_profit_after_cost_pct")
    p.add_argument("--min-trades", type=int, default=5)
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--min-dist", type=float, nargs="+", default=[2.5, 3.0, 3.5])
    p.add_argument("--sl-buffer", type=float, nargs="+", default=[0.15, 0.35, 0.6])
    p.add_argument("--max-adx", type=float, nargs="+", default=[20.0, 25.0, 30.0])
    args = p.parse_args()
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return _main(args)


if __name__ == "__main__":
    sys.exit(main())
