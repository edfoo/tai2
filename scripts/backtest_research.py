#!/usr/bin/env python3
"""P3 research workflow CLI: baseline → sweep → stress → bundle.

Drives the tai2 backtest REST API through the plan's decision workflow and
writes an auditable **analysis bundle** (JSON + Markdown) that an expert can
review.  All heavy lifting runs inside the server; this script only orchestrates
requests and assembles the bundle via
:mod:`app.services.backtest.research`.

Workflow (mirrors the plan's P3 steps):

  1. **Baseline** — one run with frozen canonical defaults for the strategy.
  2. **Sweep** — a bounded grid over the strategy's catalogue parameters with
     forward-validation folds and an untouched final holdout.
  3. **Stress** — re-run the best validation candidate under adverse
     execution assumptions (higher fees/slippage/funding).
  4. **Bundle** — combine everything into a JSON + Markdown report with a
     screening verdict (candidate / inconclusive / reject).

Usage (server must be running)::

    .venv/bin/python scripts/backtest_research.py \\
        --strategy mean_reversion \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframe 15m --days 90 --capital 1000 \\
        --validation-folds 4 --final-holdout-fraction 0.15 \\
        --search-mode random --combination-budget 64 \\
        --out-dir backtest_research/mean_reversion

Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from app.services.backtest.client import (
    BacktestClientError,
    build_single_strategy_launcher,
    submit_and_poll,
)
from app.services.backtest.research import (
    build_research_bundle,
    render_bundle_markdown,
)
from app.services.backtest.sweep_catalog import grid_param_defs

# Adverse-scenario stress presets applied to the best validation candidate.
# Each entry is (label, payload overrides).
STRESS_PRESETS: list[tuple[str, dict[str, Any]]] = [
    ("fees_2x", {"taker_fee_bps": 10.0}),
    ("slippage_2x", {"slippage_stress_multiplier": 2.0}),
    ("adverse_funding", {"funding_mode": "constant", "funding_rate_pct": 0.01}),
    (
        "combined_adverse",
        {
            "taker_fee_bps": 10.0,
            "slippage_stress_multiplier": 2.0,
            "funding_mode": "constant",
            "funding_rate_pct": 0.01,
        },
    ),
]


def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _base_payload(args: argparse.Namespace, *, strategy: str) -> dict[str, Any]:
    """Build the common run payload for a single strategy."""
    launcher = build_single_strategy_launcher(
        strategy_name=strategy, capital=args.capital,
    )
    return {
        "symbols": _split_csv(args.symbols),
        "timeframe": args.timeframe,
        "strategy_names": [strategy],
        "days": args.days,
        "capital": args.capital,
        "warmup": args.warmup,
        "evaluation_mode": args.evaluation_mode,
        "evaluation_timeframe": args.evaluation_timeframe,
        "taker_fee_bps": args.taker_fee_bps,
        "maker_fee_bps": args.maker_fee_bps,
        "slippage_bps": args.slippage_bps,
        "slippage_mode": args.slippage_mode,
        "funding_mode": args.funding_mode,
        "funding_rate_pct": args.funding_rate_pct,
        "launcher_config": launcher,
        "universe_mode": getattr(args, "universe_mode", "explicit"),
        "universe_candidate_symbols": _split_csv(getattr(args, "universe_candidates", "") or ""),
    }


def _strategy_overrides(params: dict[str, Any], strategy: str) -> dict[str, Any]:
    """Extract ``strategies.<strategy>.<param>`` dotted keys into overrides."""
    prefix = f"strategies.{strategy}."
    overrides: dict[str, Any] = {}
    for key, value in params.items():
        if key.startswith(prefix):
            overrides[key[len(prefix):]] = value
    return overrides


def _run(
    *,
    base_url: str,
    payload: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    print(f"▶ {label} …")
    try:
        envelope = submit_and_poll(base_url=base_url, payload=payload)
    except BacktestClientError as exc:
        raise SystemExit(f"{label} failed: {exc}") from exc
    result = envelope.get("result") or {}
    metrics = result.get("metrics") or {}
    print(
        f"  trades={metrics.get('total_trades')} "
        f"net_after_cost={metrics.get('net_profit_after_cost')} "
        f"({metrics.get('net_profit_after_cost_pct')}%)"
    )
    return envelope


def _run_grid(
    *,
    base_url: str,
    payload: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    print(f"▶ {label} …")
    url = base_url.rstrip("/") + "/backtest/grid"
    try:
        resp = httpx.post(url, json=payload, timeout=60.0)
    except httpx.HTTPError as exc:
        raise SystemExit(f"{label} request failed: {exc}") from exc
    if resp.status_code not in (200, 202):
        raise SystemExit(f"{label} returned {resp.status_code}: {resp.text}")
    job_id = resp.json()["job_id"]

    while True:
        status_resp = httpx.get(
            base_url.rstrip("/") + f"/backtest/status/{job_id}", timeout=60.0
        )
        if status_resp.status_code == 404:
            raise SystemExit(f"grid job {job_id} not found")
        status = status_resp.json()
        state = status.get("status")
        if state == "completed":
            break
        if state == "failed":
            raise SystemExit(f"grid job {job_id} failed: {status.get('error')}")
        time.sleep(1.5)

    result_resp = httpx.get(
        base_url.rstrip("/") + f"/backtest/result/{job_id}", timeout=60.0
    )
    if result_resp.status_code != 200:
        raise SystemExit(
            f"grid result fetch returned {result_resp.status_code}: {result_resp.text}"
        )
    envelope = result_resp.json()
    grid = envelope.get("result") or {}
    print(
        f"  combinations={grid.get('attempted_combinations')}/"
        f"{grid.get('total_combinations')} "
        f"({grid.get('search_mode')}, seed {grid.get('random_seed')})"
    )
    return envelope


def _cmd_research(args: argparse.Namespace) -> int:
    strategy = args.strategy
    base_payload = _base_payload(args, strategy=strategy)

    # ── 1. Baseline (frozen defaults) ─────────────────────────────────
    baseline_env = _run(
        base_url=args.base_url, payload=base_payload, label="baseline (frozen defaults)"
    )
    baseline = baseline_env.get("result") or {}

    # ── 2. Sweep ──────────────────────────────────────────────────────
    param_defs = grid_param_defs([strategy], include_launcher=False)
    if args.params:
        wanted = set(args.params)
        param_defs = [d for d in param_defs if d.key in wanted]
    if not param_defs:
        raise SystemExit("No sweep parameters selected; pass --params or omit it.")

    grid_payload = {
        "base": base_payload,
        "params": [
            {"key": d.key, "values": d.values, "label": d.label} for d in param_defs
        ],
        "rank_by": args.rank_by,
        "min_trades": args.min_trades,
        "validation_folds": args.validation_folds,
        "validation_train_ratio": args.validation_train_ratio,
        "final_holdout_fraction": args.final_holdout_fraction,
        "search_mode": args.search_mode,
        "combination_budget": args.combination_budget,
        "random_seed": args.random_seed,
    }
    grid_env = _run_grid(
        base_url=args.base_url, payload=grid_payload, label="parameter sweep"
    )
    grid = grid_env.get("result") or {}

    # ── 3. Stress the best validation candidate ───────────────────────
    best_params: dict[str, Any] = {}
    ranked_indexes = grid.get("ranked_indexes") or []
    runs = grid.get("runs") or []
    if ranked_indexes:
        idx = ranked_indexes[0]
        if isinstance(idx, int) and 0 <= idx < len(runs):
            best_params = runs[idx].get("params") or {}

    stress_runs: list[dict[str, Any]] = []
    if best_params and not args.skip_stress:
        overrides = _strategy_overrides(best_params, strategy)
        launcher = build_single_strategy_launcher(
            strategy_name=strategy, capital=args.capital, overrides=overrides,
        )
        for label, extra in STRESS_PRESETS:
            payload = {**base_payload, "launcher_config": launcher, **extra}
            env = _run(
                base_url=args.base_url, payload=payload, label=f"stress: {label}"
            )
            stress_runs.append({
                "label": label,
                "overrides": extra,
                "metrics": (env.get("result") or {}).get("metrics") or {},
            })

    # ── 4. Bundle ─────────────────────────────────────────────────────
    bundle = build_research_bundle(
        baseline=baseline,
        grid=grid,
        stress_runs=stress_runs,
        holdout=grid.get("final_holdout"),
        workflow={
            "strategy": strategy,
            "symbols": _split_csv(args.symbols),
            "timeframe": args.timeframe,
            "days": args.days,
            "capital": args.capital,
            "evaluation_mode": args.evaluation_mode,
            "evaluation_timeframe": args.evaluation_timeframe,
            "rank_by": args.rank_by,
            "validation_folds": args.validation_folds,
            "final_holdout_fraction": args.final_holdout_fraction,
            "search_mode": args.search_mode,
            "random_seed": args.random_seed,
        },
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_bundle.json"
    md_path = out_dir / "research_bundle.md"
    json_path.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_bundle_markdown(bundle), encoding="utf-8")

    verdict = bundle.get("verdict") or {}
    print("\n── Research bundle ──")
    print(f"  verdict:  {verdict.get('status')}")
    for reason in verdict.get("reasons") or []:
        print(f"    - {reason}")
    print(f"  written:  {json_path}")
    print(f"            {md_path}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the P3 research workflow against the tai2 REST API."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--symbols", default="BTC-USDT-SWAP")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--capital", type=float, default=1000.0)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--evaluation-mode", default="finer_ltf")
    parser.add_argument("--evaluation-timeframe", default="1m")
    parser.add_argument("--taker-fee-bps", type=float, default=5.0)
    parser.add_argument("--maker-fee-bps", type=float, default=0.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--slippage-mode", choices=("fixed", "ohlcv_liquidity"), default="ohlcv_liquidity")
    parser.add_argument("--funding-mode", choices=("historical", "constant", "off"), default="historical")
    parser.add_argument("--funding-rate-pct", type=float, default=0.0)
    parser.add_argument(
        "--params", action="append", default=[],
        help="Restrict the sweep to these dotted keys (repeatable). "
             "Default: the full catalogue for the strategy.",
    )
    parser.add_argument("--rank-by", default="net_profit_after_cost_pct")
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--validation-folds", type=int, default=4)
    parser.add_argument("--validation-train-ratio", type=float, default=0.6)
    parser.add_argument("--final-holdout-fraction", type=float, default=0.15)
    parser.add_argument("--search-mode", choices=("exhaustive", "random"), default="random")
    parser.add_argument("--combination-budget", type=int, default=64)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--skip-stress", action="store_true")
    parser.add_argument(
        "--universe-mode", choices=("explicit", "screener"), default="explicit",
        help="'screener' reconstructs the live dual-universe screener from "
             "historical candles and trades the symbols it would have selected.",
    )
    parser.add_argument(
        "--universe-candidates", default="",
        help="Optional comma-separated candidate pool for the screener to rank "
             "over. Empty → full OKX SWAP universe (matches live).",
    )
    parser.add_argument("--out-dir", default="backtest_research")
    parser.set_defaults(func=_cmd_research)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
