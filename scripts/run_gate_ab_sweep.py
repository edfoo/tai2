#!/usr/bin/env python3
"""Headless A/B sweep of the §3 liquidity-aware entry gates.

Why this exists
---------------
Each §3 gate is opt-in (default OFF).  The right way to validate it is a
controlled A/B: run the SAME strategy with the gate OFF vs ON (optionally
sweeping its numeric threshold) and compare *quality* metrics — win rate,
sharpe, net profit, profit factor, and stop-out rate — not just how many
entries it dropped.

This script runs those A/B comparisons headless and persists:

  * full per-run results (JSON) under ``backtest_cache/cli/ab/``
  * a comparison CSV with one row per gate/switch/threshold combination

Supported gates (the ones that consume only OHLCV-derived data, so they are
faithfully reproducible in backtest — funding/OI/imbalance gates silently
skip in backtest because their data is never injected by snapshot_builder):

  mean_reversion.require_price_in_va
  trend_pullback.require_poc_proximity       (+ poc_proximity_va_width)
  liquidity_sweep.require_close_in_va
  liquidity_sweep.require_macro_sl           (+ macro_sl_lookback)

Usage
-----
Run from the repo root with the project venv::

    .venv/bin/python scripts/run_gate_ab_sweep.py \\
        --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \\
        --timeframes 15m,1H \\
        --strategy mean_reversion \\
        --gate require_price_in_va \\
        --days 60 \\
        --capital 1000

Run ALL backtest-ready gates for a strategy (lattice = switch ON/OFF x each
threshold candidate)::

    .venv/bin/python scripts/run_gate_ab_sweep.py \\
        --strategy liquidity_sweep \\
        --gate all

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

OUTPUT_DIR = ROOT / "backtest_cache" / "cli" / "ab"
OVERVIEW_FILENAME = "overview.json"
_MS = 1_000

# ---------------------------------------------------------------------------
# Gate catalogue
# ---------------------------------------------------------------------------
# Maps strategy -> gate -> (switch_key, ordered threshold candidates to sweep).
# The threshold is the *default* used when the switch is ON.  Each candidate
# becomes its own A/B row alongside the OFF baseline.
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


def parse_timeframe(tf: str, ctx: str) -> str:
    """Normalise a timeframe string to engine form ('15m' / '1H' / '4H')."""
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


def _strategy_cfg(name: str, enabled: bool = True) -> dict[str, Any]:
    cfg = dict(strategy_defaults(name))
    cfg["enabled"] = enabled
    return cfg


def _count_stop_out(result: BacktestResult) -> int:
    """Count trades closed by stop-loss (close_reason)."""
    n = 0
    for t in result.trades:
        reason = (t.close_reason or "").lower()
        if "stop" in reason or "sl" in reason:
            n += 1
    return n


async def run_one(
    *,
    symbol: str,
    ltf: str,
    strategy_name: str,
    gate_cfg: dict[str, Any],
    threshold: Any,
    switch_on: bool,
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
) -> tuple[dict[str, Any], BacktestResult]:
    """Run one gate configuration and return (tag, result)."""
    strategies_cfg: dict[str, Any] = {strategy_name: _strategy_cfg(strategy_name, enabled=True)}
    gate_overrides = dict(strategies_cfg[strategy_name])
    switch_key = gate_cfg["switch"]
    gate_overrides[switch_key] = bool(switch_on)
    if switch_on and gate_cfg["threshold_key"] and threshold is not None:
        gate_overrides[gate_cfg["threshold_key"]] = threshold
    strategies_cfg[strategy_name] = gate_overrides

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
        strategy_names=[strategy_name],
        launcher_config=launcher_config,
        strategy_config={},
        warmup_candles=warmup,
        disable_live_execution=True,
        evaluation_mode="finer_ltf",
        evaluation_timeframe="1m",
    )
    engine = BacktestEngine(config)
    result = await engine.run()
    return launcher_config, result


async def _run_and_record(
    *,
    summaries: list[dict[str, Any]],
    OC: Path,
    run_tag: str,
    symbol: str,
    ltf: str,
    strategy: str,
    gate_cfg: dict[str, Any],
    switch_on: bool,
    threshold: Any | None,
    start_ts: int,
    end_ts: int,
    capital: float,
    warmup: int,
    tag: str,
) -> BacktestResult | None:
    """Run one gate configuration, persist it, and append its summary row.

    Returns the ``BacktestResult`` on success (even if it has no trades), or
    ``None`` if the engine errored.
    """
    thr_str = "None" if threshold is None else str(threshold)
    try:
        _, result = await run_one(
            symbol=symbol,
            ltf=ltf,
            strategy_name=strategy,
            gate_cfg=gate_cfg,
            threshold=threshold,
            switch_on=switch_on,
            start_ts=start_ts,
            end_ts=end_ts,
            capital=capital,
            warmup=warmup,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗ errored: {exc}")
        summaries.append({
            "run_id": tag, "symbol": symbol, "ltf": ltf,
            "strategy": strategy, "gate": gate_cfg.get("label", ""),
            "switch": switch_on, "threshold": thr_str, "error": str(exc),
        })
        return None

    if result.error:
        print(f"  ⚠ engine error: {result.error}")

    htf = _htf_for(ltf)
    stop_out = _count_stop_out(result)
    metrics = result.metrics or {}
    summary = result_summary_row(result, run_id=tag, ltf=ltf, htf=htf)
    summary.update({
        "symbol": symbol,
        "strategy": strategy,
        "gate": gate_cfg["switch"],
        "switch": switch_on,
        "threshold": thr_str,
        "stop_out_count": stop_out,
    })
    summaries.append(summary)

    # Persist the full result for later inspection.
    try:
        from app.services.backtest.persistence import save_result
        save_result(result, run_id=tag, output_dir=OC)
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ persist failed: {exc}")

    print(f"  ✓ {metrics.get('total_trades')} trades, "
          f"win={metrics.get('win_rate')}, "
          f"sharpe={metrics.get('sharpe_per_candle')}, "
          f"net={metrics.get('net_profit')}, stop-outs={stop_out}")
    return result


async def _amain(args: argparse.Namespace) -> int:
    now = datetime.now(timezone.utc)
    run_tag = now.strftime("%Y%m%d_%H%M%S")
    OC = OUTPUT_DIR / run_tag
    OC.mkdir(parents=True, exist_ok=True)

    strategy = args.strategy
    end_ts = int(now.timestamp() * _MS)
    start_ts = int((now - timedelta(days=args.days)).timestamp() * _MS)

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
                # ── 1. OFF baseline first ──────────────────────────────
                off_tag = f"{run_tag}_{symbol}_{ltf}_{gate_name}_off"
                print(f"▶ [baseline OFF] [{symbol} {ltf}] {strategy}.{switch_key}= off")
                base_result = await _run_and_record(
                    summaries=summaries,
                    OC=OC,
                    run_tag=run_tag,
                    symbol=symbol,
                    ltf=ltf,
                    strategy=strategy,
                    gate_cfg=gate_cfg,
                    switch_on=False,
                    threshold=None,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    capital=args.capital,
                    warmup=args.warmup,
                    tag=off_tag,
                )
                if base_result is None:
                    exit_code = 1
                    continue
                base_trades = (base_result.metrics or {}).get("total_trades", 0) or 0

                # ── 2. Optional: skip ON variants if baseline is sterile ──
                # A gate only DROPS signals; it can never create them.  If the
                # OFF baseline has (near-)zero trades, every ON threshold variant
                # will also be (near-)zero — running them just wastes time.
                if base_trades < args.min_trades:
                    print(
                        f"  ⏭  skipping ON variants: baseline has only {base_trades} trade(s) "
                        f"< min_trades={args.min_trades} (a pure filter cannot create trades)."
                    )
                    continue

                # Build ON variants: one per threshold candidate; the plain ON
                # (switch only, no threshold) is only emitted when the gate has
                # no numeric threshold knob.
                on_variants: list[tuple[bool, Any | None]] = [
                    (True, thr) for thr in thresholds
                ]
                if not thresholds:
                    on_variants.append((True, None))

                for (switch_on, threshold) in on_variants:
                    thr_str = "None" if threshold is None else str(threshold)
                    tag = (
                        f"{run_tag}_{symbol}_{ltf}_{gate_name}_on"
                        + (f"_t{threshold}" if threshold is not None else "")
                    )
                    print(f"▶ [ON] [{symbol} {ltf}] {strategy}.{switch_key}= on"
                          + (f" @{gate_cfg['threshold_key']}={thr_str}" if gate_cfg["threshold_key"] else ""))
                    on_result = await _run_and_record(
                        summaries=summaries,
                        OC=OC,
                        run_tag=run_tag,
                        symbol=symbol,
                        ltf=ltf,
                        strategy=strategy,
                        gate_cfg=gate_cfg,
                        switch_on=True,
                        threshold=threshold,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        capital=args.capital,
                        warmup=args.warmup,
                        tag=tag,
                    )
                    if on_result is None:
                        exit_code = 1
                        continue

    # Write comparison CSV (overwrite on each full invocation).
    csv_path = write_comparison_csv(summaries, output_dir=OUTPUT_DIR, append=False)
    overview_path = OUTPUT_DIR / OVERVIEW_FILENAME
    overview_path.write_text(json.dumps({"generated_at": run_tag, "runs": summaries}, indent=2, default=str))

    # ── Print a per-gate quality diff ─────────────────────────────────
    print("\n── Gate A/B quality diff ──")
    # Group by (symbol, ltf, gate).
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for s in summaries:
        groups.setdefault((s["symbol"], s["ltf"], s["gate"]), []).append(s)

    rank_key = args.rank_by  # e.g. "m_sharpe_per_candle"
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
                base = runs_off[0]
                b = (base.get('m_total_trades') or 0)
                cur = (r.get('m_total_trades') or 0)
                delta_trades = f" ({cur - b:+d} trades)"
            thr = f" @{r.get('threshold_key')}={r.get('threshold')}" if r.get('threshold_key') and r.get('switch') else ""
            print(f"  ON {thr}: trades={r.get('m_total_trades')}{delta_trades} "
                  f"win={r.get('m_win_rate')} {rank_key}={r.get(rank_key)} "
                  f"net={r.get('m_net_profit')} stop_out={r.get('stop_out_count')}")

    print(f"\nFull results:  {OC}")
    print(f"Comparison CSV: {csv_path}")
    print(f"Overview:       {overview_path}")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Headless A/B sweep of §3 liquidity-aware gates.")
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
                        help="Skip ON variants when the OFF baseline yields fewer than this many "
                             "trades (a pure filter cannot create trades). Default 1.")
    parser.add_argument("--rank-by", default="m_sharpe_per_candle",
                        help="Metric to print/sort by (CSV columns are m_*).")
    args = parser.parse_args()

    valid = {"mean_reversion", "trend_pullback", "liquidity_sweep"}
    if args.strategy not in valid:
        print(f"Strategy must be one of {valid}")
        return 2
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    sys.exit(main())