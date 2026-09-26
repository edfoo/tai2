#!/usr/bin/env python3
"""Thin REST client for the backtest API.

Replaces the previous headless scripts that imported :class:`BacktestEngine`
directly.  This client builds a ``POST /backtest/run`` or ``POST /backtest/grid``
request, polls the job until completion, and prints a human-readable summary
of the result.  All heavy lifting (fetch → snapshot → simulate → metrics) runs
inside the running tai2 server.

Usage (server must be running)::

    .venv/bin/python scripts/backtest_client.py run \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframe 15m \\
        --strategies mean_reversion,trend_pullback \\
        --days 60 --capital 1000 \\
        --base-url http://localhost:8000

    .venv/bin/python scripts/backtest_client.py grid \\
        --symbols BTC-USDT-SWAP --timeframe 15m \\
        --strategies mean_reversion \\
        --days 60 --capital 1000 \\
        --params strategies.mean_reversion.rsi_oversold=25,30,35 \\
        --params strategies.mean_reversion.max_adx=20,25,30

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import httpx


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_param(value: str) -> tuple[str, list[Any]]:
    key, _, raw = value.partition("=")
    if not key or not raw:
        raise SystemExit(f"Invalid --params value '{value}' (expected key=a,b,c)")
    key = key.strip()
    values: list[Any] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            values.append(float(tok) if "." in tok or "e" in tok.lower() else int(tok))
        except ValueError:
            values.append(tok)
    return key, values


def _post(base_url: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    try:
        resp = httpx.post(url, json=payload, timeout=30.0)
    except httpx.HTTPError as exc:
        raise SystemExit(f"request failed: {exc}")
    if resp.status_code not in (200, 202):
        raise SystemExit(f"{path} returned {resp.status_code}: {resp.text}")
    return resp.json()


def _poll(base_url: str, job_id: str, *, interval: float = 1.5) -> dict[str, Any]:
    status_path = f"/backtest/status/{job_id}"
    while True:
        try:
            resp = httpx.get(base_url.rstrip("/") + status_path, timeout=30.0)
        except httpx.HTTPError as exc:
            raise SystemExit(f"status poll failed: {exc}")
        if resp.status_code == 404:
            raise SystemExit(f"job {job_id} not found")
        status = resp.json()
        state = status.get("status")
        if state == "completed":
            return status
        if state == "failed":
            raise SystemExit(f"job {job_id} failed: {status.get('error')}")
        time.sleep(interval)


def _print_result(base_url: str, job_id: str) -> None:
    resp = httpx.get(base_url.rstrip("/") + f"/backtest/result/{job_id}", timeout=60.0)
    if resp.status_code != 200:
        raise SystemExit(f"result fetch returned {resp.status_code}: {resp.text}")
    payload = resp.json()
    result = payload.get("result") or {}
    if result.get("result_type") == "grid":
        config = result.get("config") or {}
        print("\n── Grid Result ──")
        print(f"  rank_by:       {config.get('rank_by')}")
        print(f"  search:        {result.get('search_mode')} seed={result.get('random_seed')}")
        print(
            f"  combinations:  {result.get('attempted_combinations')}/"
            f"{result.get('total_combinations')}"
        )
        print(f"  validation:    {config.get('validation_folds')} folds")
        runs = result.get("runs", [])
        ranked_indexes = result.get("ranked_indexes") or []
        top = next((
            runs[idx] for idx in ranked_indexes
            if isinstance(idx, int)
            and 0 <= idx < len(runs)
            and runs[idx].get("rank_score") is not None
            and not runs[idx].get("below_min_trades")
        ), None)
        if top:
            print(f"  best params:   {json.dumps(top.get('params') or {}, sort_keys=True)}")
            print(f"  validation:    score={top.get('rank_score')}")
            for fold in top.get("fold_metrics") or []:
                metrics = fold.get("metrics") or {}
                print(
                    f"  fold {fold.get('fold')}: {fold.get('status')} "
                    f"trades={fold.get('trade_count')} "
                    f"net_after_cost={metrics.get('net_profit_after_cost')} "
                    f"drawdown={metrics.get('max_drawdown_pct')}%"
                )
        holdout = result.get("final_holdout")
        if holdout:
            holdout_fold = (holdout.get("fold_metrics") or [{}])[0]
            holdout_metrics = (holdout.get("result") or {}).get("metrics") or {}
            print("  untouched final holdout (not used for ranking):")
            print(
                f"    {holdout_fold.get('status')} trades={holdout_fold.get('trade_count')} "
                f"net_after_cost={holdout_metrics.get('net_profit_after_cost')} "
                f"return={holdout_metrics.get('net_profit_after_cost_pct')}%"
            )
        for source in result.get("data_provenance") or []:
            print(
                f"  source:        {source.get('symbol')} {source.get('timeframe')} "
                f"{source.get('source')} sha256={str(source.get('content_sha256', ''))[:16]}"
            )
        if result.get("error"):
            print(f"  error:         {result['error']}")
        return
    metrics = result.get("metrics") or {}
    print("\n── Result ──")
    print(f"  run_id:       {payload.get('run_id')}")
    print(f"  trades:       {metrics.get('total_trades')}")
    print(f"  net_win_rate: {metrics.get('net_win_rate_after_cost_pct')}")
    print(f"  pnl_slippage: {metrics.get('pnl_after_slippage_before_fees_funding')}")
    print(f"  net_after_costs: {metrics.get('net_profit_after_cost')} "
          f"({metrics.get('net_profit_after_cost_pct')}%)")
    print(f"  net_pf:       {metrics.get('net_profit_factor_after_cost')}")
    print(f"  max_drawdown: {metrics.get('max_drawdown_pct')}")
    print(f"  sharpe_ann:   {metrics.get('sharpe_annualized')}")
    print(f"  exit_reasons: {json.dumps(metrics.get('exit_reasons'))}")
    if result.get("error"):
        print(f"  ⚠ error: {result['error']}")


def _build_run_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "symbols": _split_csv(args.symbols),
        "timeframe": args.timeframe,
        "strategy_names": _split_csv(args.strategies),
        "start_ts": args.start_ts,
        "end_ts": args.end_ts,
        "days": args.days,
        "capital": args.capital,
        "warmup": args.warmup,
        "evaluation_mode": args.evaluation_mode,
        "evaluation_timeframe": args.evaluation_timeframe,
        "taker_fee_bps": args.taker_fee_bps,
        "maker_fee_bps": args.maker_fee_bps,
        "slippage_bps": args.slippage_bps,
        "slippage_mode": args.slippage_mode,
        "slippage_stress_multiplier": args.slippage_stress_multiplier,
        "liquidation_fee_bps": args.liquidation_fee_bps,
        "funding_rate_pct": args.funding_rate_pct,
        "funding_mode": args.funding_mode,
        "allow_concurrent_strategies_per_symbol": args.allow_concurrent_strategies_per_symbol,
    }


def _cmd_run(args: argparse.Namespace) -> int:
    payload = _build_run_payload(args)
    accepted = _post(args.base_url, "/backtest/run", payload)
    job_id = accepted["job_id"]
    print(f"▶ queued job {job_id}")
    _poll(args.base_url, job_id)
    _print_result(args.base_url, job_id)
    return 0


def _cmd_grid(args: argparse.Namespace) -> int:
    params = [dict(zip(("key", "values"), _parse_param(p))) for p in args.params]
    payload = {
        "base": _build_run_payload(args),
        "params": params,
        "rank_by": args.rank_by,
        "min_trades": args.min_trades,
        "validation_folds": args.validation_folds,
        "validation_train_ratio": args.validation_train_ratio,
        "final_holdout_fraction": args.final_holdout_fraction,
        "search_mode": args.search_mode,
        "combination_budget": args.combination_budget,
        "random_seed": args.random_seed,
    }
    accepted = _post(args.base_url, "/backtest/grid", payload)
    job_id = accepted["job_id"]
    print(f"▶ queued grid job {job_id}")
    _poll(args.base_url, job_id)
    _print_result(args.base_url, job_id)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run backtests against the tai2 REST API."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    sub = parser.add_subparsers(dest="command", required=True)

    def _add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--symbols", default="BTC-USDT-SWAP")
        p.add_argument("--timeframe", default="15m")
        p.add_argument(
            "--strategies",
            default="mean_reversion,liquidity_sweep,trend_pullback,vwap_reversion,spike_continuation",
        )
        p.add_argument("--start-ts", type=int, default=None)
        p.add_argument("--end-ts", type=int, default=None)
        p.add_argument("--days", type=int, default=30)
        p.add_argument("--capital", type=float, default=1000.0)
        p.add_argument("--warmup", type=int, default=200)
        p.add_argument("--evaluation-mode", default="finer_ltf")
        p.add_argument("--evaluation-timeframe", default="1m")
        p.add_argument("--taker-fee-bps", type=float, default=5.0)
        p.add_argument("--maker-fee-bps", type=float, default=0.0)
        p.add_argument("--slippage-bps", type=float, default=0.0)
        p.add_argument("--slippage-mode", choices=("fixed", "ohlcv_liquidity"), default="ohlcv_liquidity")
        p.add_argument("--slippage-stress-multiplier", type=float, default=1.0,
                   help="Scale the estimated slippage for adverse stress tests (1.0 = unchanged)")
        p.add_argument("--liquidation-fee-bps", type=float, default=0.0,
                   help="Extra fee charged on a liquidation fill, in bps")
        p.add_argument("--funding-mode", choices=("historical", "constant", "off"), default="historical")
        p.add_argument("--funding-rate-pct", type=float, default=0.0,
                   help="Fallback rate per funding interval; 0.01 means 0.01%%")
        p.add_argument("--allow-concurrent-strategies-per-symbol", action="store_true")

    run_p = sub.add_parser("run", help="run a single backtest")
    _add_common(run_p)
    run_p.set_defaults(func=_cmd_run)

    grid_p = sub.add_parser("grid", help="run a parameter sweep")
    _add_common(grid_p)
    grid_p.add_argument(
        "--params", action="append", default=[],
        help="dotted key=a,b,c (repeatable)",
    )
    grid_p.add_argument("--rank-by", default="net_profit_after_cost_pct")
    grid_p.add_argument("--min-trades", type=int, default=5)
    grid_p.add_argument("--validation-folds", type=int, default=0)
    grid_p.add_argument("--validation-train-ratio", type=float, default=0.7)
    grid_p.add_argument("--final-holdout-fraction", type=float, default=0.0)
    grid_p.add_argument("--search-mode", choices=("exhaustive", "random"), default="exhaustive")
    grid_p.add_argument("--combination-budget", type=int, default=0)
    grid_p.add_argument("--random-seed", type=int, default=42)
    grid_p.set_defaults(func=_cmd_grid)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
