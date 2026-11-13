#!/usr/bin/env python3
"""Generic in-process parameter sweep for a single launcher strategy.

Generalises ``run_vwap_geometry_sweep.py``: pass ``--strategy`` and one or more
``--param <name>=<v1,v2,...>`` specs and it runs the Cartesian product through
:class:`BacktestGrid` (each combination in its own subprocess).

Runs **in-process** (no REST server) because the live server runs with
``uvicorn --reload`` and restarts on every file save, wiping its in-memory job
registry.

CRITICAL — ``--capital`` is the PER-TRADE notional (``notional_usd``), NOT
account equity.  It MUST match the live per-trade size (30 USDT) or the
``ohlcv_liquidity`` slippage model maxes out at 500 bps on illiquid alts and
swamps the strategy edge (verified: a 1000-USDT notional is 33x live and turns
a 28.6%-WR / -3.68 run into a 0%-WR / -475 artifact).

Usage::

    .venv/bin/python scripts/run_strategy_geometry_sweep.py \\
        --strategy trend_pullback \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP,ADA-USDT-SWAP \\
        --param structural_sl_buffer_atr=0.15,0.35,0.6 \\
        --param atr_tp_multiplier=1.75,2.25,3.0 \\
        --days 45 --capital 30

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


def _parse_param_spec(spec: str) -> tuple[str, list[float]]:
    """Parse ``name=v1,v2,v3`` into ``(name, [floats])``."""
    if "=" not in spec:
        raise SystemExit(f"--param must be name=v1,v2,... (got {spec!r})")
    name, raw = spec.split("=", 1)
    name = name.strip()
    values: list[float] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(float(part))
        except ValueError:
            raise SystemExit(f"--param {name}: non-numeric value {part!r}")
    if not name or not values:
        raise SystemExit(f"--param must be name=v1,v2,... (got {spec!r})")
    return name, values


def _build_base(
    *,
    strategy: str,
    symbols: list[str],
    ltf: str,
    days: int,
    capital: float,
    warmup: int,
    evaluation_mode: str,
) -> Any:
    now_ms = int(time.time() * 1000)
    launcher = build_single_strategy_launcher(strategy_name=strategy, capital=capital)
    return build_backtest_config(
        symbols=symbols,
        timeframe=ltf,
        strategy_names=[strategy],
        start_ts=now_ms - days * 86_400_000,
        end_ts=now_ms,
        capital=capital,
        warmup=warmup,
        launcher_config=launcher,
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
    return await BacktestGrid(cfg).run()


def _main(args: argparse.Namespace) -> int:
    base = _build_base(
        strategy=args.strategy,
        symbols=args.symbols,
        ltf=args.timeframe,
        days=args.days,
        capital=args.capital,
        warmup=args.warmup,
        evaluation_mode=args.evaluation_mode,
    )

    print(f"\n{'=' * 90}")
    print(
        f"{args.strategy} geometry sweep — {len(args.symbols)} symbols, "
        f"{args.timeframe}, {args.days}d, notional={args.capital}"
    )
    print(f"symbols: {', '.join(args.symbols)}")
    print(f"{'=' * 90}")

    print("\n[baseline] canonical defaults")
    bm = asyncio.run(_run_baseline(base))
    if bm is not None:
        print(f"  {_fmt_metrics(bm)}")

    params: list[GridParamDef] = []
    for spec in args.param:
        name, values = _parse_param_spec(spec)
        params.append(
            GridParamDef(f"strategies.{args.strategy}.{name}", values, name)
        )
    n_combos = 1
    for p in params:
        n_combos *= len(p.values)
    print(f"\n[grid] {n_combos} combinations, rank_by={args.rank_by}")

    t0 = time.monotonic()
    grid = asyncio.run(
        _run_grid(base, params, rank_by=args.rank_by, min_trades=args.min_trades)
    )
    print(f"  grid finished in {time.monotonic() - t0:.1f}s")

    if grid.error:
        print(f"  ✗ grid error: {grid.error}")
        return 1

    ranked = grid.ranked
    labels = [p.label or p.key for p in params]
    header = "  ".join(f"{lab:>10}" for lab in labels)
    print(f"\n{'rank':>4}  {header}  metrics")
    print("-" * 90)
    for rank, run in enumerate(ranked[: args.top], start=1):
        p = run.params or {}
        m = (run.result.metrics if run.result is not None else {}) or {}
        cells = "  ".join(f"{str(p.get(lab)):>10}" for lab in labels)
        print(f"{rank:>4}  {cells}  {_fmt_metrics(m)}")

    if not ranked:
        print("  (no combinations met min_trades)")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Generic in-process parameter sweep for one launcher strategy."
    )
    p.add_argument("--strategy", required=True, help="Launcher strategy name.")
    p.add_argument(
        "--param",
        action="append",
        required=True,
        metavar="NAME=V1,V2,...",
        help="Parameter to sweep (repeatable). Values are strategy-config keys.",
    )
    p.add_argument(
        "--symbols",
        default="BTC-USDT-SWAP,ETH-USDT-SWAP,ADA-USDT-SWAP,DOGE-USDT-SWAP",
    )
    p.add_argument("--timeframe", default="15m")
    p.add_argument("--days", type=int, default=45)
    # Per-trade notional — MUST match live (30 USDT).  See module docstring.
    p.add_argument("--capital", type=float, default=30.0)
    p.add_argument("--warmup", type=int, default=200)
    p.add_argument(
        "--evaluation-mode",
        default="closed",
        choices=["closed", "finer_ltf"],
        help="closed = step per closed LTF candle (fast); finer_ltf = step on 1m (slow).",
    )
    p.add_argument("--rank-by", default="net_profit_after_cost_pct")
    p.add_argument("--min-trades", type=int, default=5)
    p.add_argument("--top", type=int, default=20)
    args = p.parse_args()
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    return _main(args)


if __name__ == "__main__":
    sys.exit(main())
