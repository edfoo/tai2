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
    metrics = result.get("metrics") or {}
    print("\n── Result ──")
    print(f"  run_id:       {payload.get('run_id')}")
    print(f"  trades:       {metrics.get('total_trades')}")
    print(f"  win_rate:     {metrics.get('win_rate')}")
    print(f"  net_profit:   {metrics.get('net_profit')}")
    print(f"  profit_factor:{metrics.get('profit_factor')}")
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

    run_p = sub.add_parser("run", help="run a single backtest")
    _add_common(run_p)
    run_p.set_defaults(func=_cmd_run)

    grid_p = sub.add_parser("grid", help="run a parameter sweep")
    _add_common(grid_p)
    grid_p.add_argument(
        "--params", action="append", default=[],
        help="dotted key=a,b,c (repeatable)",
    )
    grid_p.add_argument("--rank-by", default="sharpe_per_candle")
    grid_p.add_argument("--min-trades", type=int, default=5)
    grid_p.set_defaults(func=_cmd_grid)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
