"""Persistence helpers for backtest results.

Shared by:
  * the headless CLI runners in ``scripts/``
  * the NiceGUI BACKTEST page (app/ui/pages.py)

Both write the same file format so a run produced by either path can be
viewed by the other.  All output lives under ``backtest_cache/cli/``:

  * ``<timestamp>_<ltf>_results.json``   full result (metrics + trades)
  * ``<timestamp>_<ltf>_per_strategy.json`` per-strategy breakdown
  * ``comparison.csv``                   cumulative one-row-per-run summary

These are plain-file formats (no DB dependency).  A ``BacktestResult`` is a
dataclass tree, so we convert to/from plain dicts for JSON.
"""

from __future__ import annotations

import csv
import json
import math
import os
from dataclasses import asdict, dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any, Iterable

# Import here, guarded, so the module can be imported by the UI (which always
# has these) and by the CLI (which also has them).  Kept as plain dataclasses,
# no Pydantic.
from app.services.backtest.models import (
    BacktestConfig,
    BacktestResult,
    EquityPoint,
    GridConfig,
    GridParamDef,
    GridResult,
    GridRunResult,
    SimPosition,
)

DEFAULT_OUTPUT_DIR: Path = (
    Path(__file__).resolve().parent.parent.parent.parent / "backtest_cache" / "cli"
)

CSV_FILENAME = "comparison.csv"
OVERVIEW_FILENAME = "overview.json"


# ---------------------------------------------------------------------------
# Serialisation (result <-> dict)
# ---------------------------------------------------------------------------


def _trade_to_dict(t: SimPosition) -> dict[str, Any]:
    return {
        "symbol": t.symbol,
        "direction": t.direction,
        "strategy": t.strategy_name,
        "size": t.size,
        "initial_size": t.initial_size,
        "trade_id": t.trade_id,
        "margin_mode": t.margin_mode,
        "leverage": t.leverage,
        "initial_margin": t.initial_margin,
        "maintenance_margin_ratio": t.maintenance_margin_ratio,
        "maintenance_margin_deduction": t.maintenance_margin_deduction,
        "liquidation_price": t.liquidation_price,
        "entry_ts": t.entry_ts,
        "entry_price": t.entry_price,
        "tp_price": t.tp_price,
        "sl_price": t.sl_price,
        "close_reason": t.close_reason,
        "close_price": t.close_price,
        "close_ts": t.close_ts,
        "pnl": t.pnl,
        "pnl_pct": t.pnl_pct,
        "entry_fee": t.entry_fee,
        "exit_fee": t.exit_fee,
        "funding": t.funding,
        "funding_settled_through_ts": t.funding_settled_through_ts,
        "funding_intervals_paid": t.funding_intervals_paid,
        "maintenance_margin_deduction": t.maintenance_margin_deduction,
        "slippage_cost": t.slippage_cost,
        "max_favorable_pct": t.max_favorable_pct,
        "max_adverse_pct": t.max_adverse_pct,
        "candles_held": t.candles_held,
        "breakeven_done": t.breakeven_done,
        "partial_done": t.partial_done,
    }


def _sanitize(value: Any) -> Any:
    """Recursively replace non-finite floats with ``None`` (JSON-safe).

    ``compute_metrics`` emits ``float("inf")`` for profit_factor when there are
    winning trades but zero losing trades (and ``-inf``/``nan`` are possible in
    edge cases).  Standard ``json.dumps`` (and FastAPI's ``JSONResponse``, which
    serialises with ``allow_nan=False``) reject non-finite floats, so we convert
    them to ``None`` at the serialisation boundary.  ``None`` is semantically
    "undefined/not applicable" and round-trips cleanly.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


def result_to_dict(result: BacktestResult | GridResult) -> dict[str, Any]:
    """Convert a ``BacktestResult`` into a JSON-serialisable dict.

    All non-finite floats (``inf``/``-inf``/``nan``) are replaced with
    ``None`` so the payload is always valid JSON.
    """
    if isinstance(result, GridResult):
        return grid_result_to_dict(result)
    return _sanitize({
        "schema_version": 2,
        "result_type": "backtest",
        "config": _backtest_config_data(result.config),
        "assumptions": getattr(result, "assumptions", {}) or _backtest_assumptions(
            BacktestConfig(**_backtest_config_kwargs(_config_values(result.config)))
        ),
        "data_provenance": getattr(result, "data_provenance", []) or [],
        "metrics": result.metrics,
        "per_strategy": result.per_strategy,
        "per_symbol": getattr(result, "per_symbol", {}) or {},
        "trades_count": len(result.trades),
        "trades": [_trade_to_dict(t) for t in result.trades],
        "equity_curve": [
            {"ts": p.ts, "equity": p.equity, "open_positions": p.open_positions}
            for p in result.equity_curve
        ],
        "duration_seconds": result.duration_seconds,
        "started_at": getattr(result, "started_at", ""),
        "finished_at": getattr(result, "finished_at", ""),
        "candles_processed": result.candles_processed,
        "error": result.error,
    })


def grid_result_to_dict(result: GridResult) -> dict[str, Any]:
    """Convert a grid result to JSON, storing each candidate once."""
    run_indexes = {id(run): idx for idx, run in enumerate(result.runs)}
    return _sanitize({
        "schema_version": 2,
        "result_type": "grid",
        "config": {
            "base_config": _backtest_config_data(result.config.base_config),
            "params": [asdict(param) for param in result.config.params],
            "rank_by": result.config.rank_by,
            "min_trades": result.config.min_trades,
            "validation_folds": result.config.validation_folds,
            "validation_train_ratio": result.config.validation_train_ratio,
            "final_holdout_fraction": result.config.final_holdout_fraction,
            "search_mode": result.config.search_mode,
            "combination_budget": result.config.combination_budget,
            "random_seed": result.config.random_seed,
        },
        "assumptions": {
            **_backtest_assumptions(result.config.base_config),
            **(result.assumptions or {}),
        },
        "data_provenance": result.data_provenance,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "duration_seconds": result.duration_seconds,
        "total_combinations": result.total_combinations,
        "attempted_combinations": result.attempted_combinations,
        "search_mode": result.search_mode,
        "random_seed": result.random_seed,
        "error": result.error,
        "runs": [
            {
                "params": run.params,
                "result": result_to_dict(run.result) if run.result is not None else None,
                "rank_score": run.rank_score,
                "below_min_trades": run.below_min_trades,
                "fold_metrics": run.fold_metrics,
            }
            for run in result.runs
        ],
        "final_holdout": (
            {
                "params": result.final_holdout.params,
                "result": result_to_dict(result.final_holdout.result)
                if result.final_holdout.result is not None else None,
                "rank_score": result.final_holdout.rank_score,
                "below_min_trades": result.final_holdout.below_min_trades,
                "fold_metrics": result.final_holdout.fold_metrics,
            }
            if result.final_holdout is not None else None
        ),
        "ranked_indexes": [run_indexes[id(run)] for run in result.ranked if id(run) in run_indexes],
    })


def _backtest_assumptions(config: BacktestConfig) -> dict[str, Any]:
    """Describe modeled execution/cost inputs and gates using absent live data."""
    strategies = (getattr(config, "launcher_config", None) or {}).get("strategies") or {}
    unavailable_gates = {
        "require_footprint_delta": ("footprint trade tape", "not evaluated when footprint data is absent"),
        "require_book_imbalance": ("live order book", "passes neutrally when order-book data is absent"),
        "require_balanced_book": ("live order book", "passes neutrally when order-book data is absent"),
        "require_oi_confirmation": ("historical open interest", "passes neutrally when open-interest data is absent"),
        "require_no_extreme_funding": ("historical funding metadata", "cannot be evaluated without funding metadata"),
        "require_no_funding_bias": ("historical funding metadata", "cannot be evaluated without funding metadata"),
    }
    enabled_gates = [
        {
            "strategy": strategy_name,
            "gate": gate_name,
            "unavailable_input": missing_input,
            "backtest_behavior": behavior,
        }
        for strategy_name, strategy_config in strategies.items()
        if isinstance(strategy_config, dict)
        for gate_name, (missing_input, behavior) in unavailable_gates.items()
        if bool(strategy_config.get(gate_name, False))
    ]
    return {
        "historical_market_data": "OKX OHLCV candles; live trade tape and order-book snapshots are not available.",
        "entry_fill": "Signal evaluated at candle close and filled at that close; entry candle excluded from post-entry management.",
        "intrabar_barrier_order": "If stop-loss and take-profit are both crossed within a candle, stop-loss is assumed first.",
        "cost_model": {
            "taker_fee_bps_per_fill": getattr(config, "taker_fee_bps", 5.0),
            "maker_fee_bps_per_fill": getattr(config, "maker_fee_bps", 0.0),
            "slippage_bps_per_fill": getattr(config, "slippage_bps", 0.0),
            "slippage_mode": getattr(config, "slippage_mode", "fixed"),
            "slippage_source": (
                "fixed configured bps per fill"
                if getattr(config, "slippage_mode", "fixed") == "fixed"
                else "fixed base plus prior-bar OHLCV range and quote-turnover proxy"
            ),
            "liquidity_impact_coefficient": getattr(config, "liquidity_impact_coefficient", 0.05),
            "candle_range_slippage_fraction": getattr(config, "candle_range_slippage_fraction", 0.1),
            "max_liquidity_slippage_bps": getattr(config, "max_liquidity_slippage_bps", 500.0),
            "slippage_stress_multiplier": getattr(config, "slippage_stress_multiplier", 1.0),
            "liquidation_fee_bps": getattr(config, "liquidation_fee_bps", 0.0),
            "funding_rate_pct_per_interval": getattr(config, "funding_rate_pct", 0.0),
            "funding_interval_ms": getattr(config, "funding_interval_ms", 28_800_000),
            "funding_mode": getattr(config, "funding_mode", "historical"),
            "funding_source": (
                "timestamped OKX historical rates when available; configured constant-rate fallback otherwise"
                if getattr(config, "funding_mode", "historical") == "historical"
                else "configured constant-rate fallback"
                if getattr(config, "funding_mode", "historical") == "constant"
                else "funding disabled"
            ),
        },
        "unavailable_live_inputs": [
            "footprint / trade-tape delta",
            "live order-book depth and imbalance",
            "historical open-interest series",
            "historical funding-rate series",
        ],
        "execution_model_limitations": [
            "OHLCV range and contract-adjusted turnover are slippage proxies, not historical bid-ask spread/order-book impact; estimates use only completed bars before each fill.",
            "Slippage coefficients are stress-test knobs, not fitted market-impact parameters; the slippage_stress_multiplier scales the estimate for adverse scenarios.",
            "Isolated liquidation is an approximation using tier IMR/MMR and maintenance deduction; funding accrued in the liquidation equation and exchange-specific risk adjustments are omitted.",
            "Cross-margin portfolio liquidation and maintenance-margin offsets are not simulated; configured mode is isolated only.",
            "Exchange tick-size rounding is applied conservatively to TP/SL levels, but other exchange-specific algo-order price rules may differ.",
        ],
        "sizing_model": {
            "instrument_specs_source": "OKX public SWAP instrument and isolated position-tier metadata when available; ct_val fallback is 1.0 and missing lot/min-size constraints are not enforced.",
            "margin_mode": "isolated",
            "leverage_source": "configured guardrail max_leverage capped by the selected OKX tier maxLever",
            "initial_margin_source": "max(tier IMR, reciprocal effective leverage)",
            "liquidation_model": "isolated approximation using initial margin, tier MMR, and maintenance deduction; adverse gap fills use candle open",
            "cross_margin_supported": False,
            "allow_concurrent_strategies_per_symbol": getattr(
                config, "allow_concurrent_strategies_per_symbol", False
            ),
            "position_capital": "Isolated initial margin uses the selected tier IMR or reciprocal leverage floor; open positions reserve initial margin from portfolio free margin.",
        },
        "enabled_gates_with_unavailable_inputs": enabled_gates,
    }


def _config_values(config: Any) -> dict[str, Any]:
    """Return available config attributes for a dataclass or legacy object."""
    if is_dataclass(config):
        return asdict(config)
    return {
        field.name: getattr(config, field.name)
        for field in fields(BacktestConfig)
        if hasattr(config, field.name)
    }


def _backtest_config_data(config: Any) -> dict[str, Any]:
    """Serialize a supported BacktestConfig with defaults filled in."""
    return asdict(BacktestConfig(**_backtest_config_kwargs(_config_values(config))))


def _trade_from_dict(d: dict[str, Any]) -> SimPosition:
    return SimPosition(
        symbol=d.get("symbol", ""),
        direction=d.get("direction", "long"),
        size=d.get("size", 0.0),
        entry_price=d.get("entry_price", 0.0),
        entry_ts=d.get("entry_ts", 0),
        trade_id=d.get("trade_id", ""),
        margin_mode=d.get("margin_mode", "isolated"),
        leverage=d.get("leverage", 1.0),
        initial_margin=d.get("initial_margin", 0.0),
        maintenance_margin_ratio=d.get("maintenance_margin_ratio", 0.0),
        maintenance_margin_deduction=d.get("maintenance_margin_deduction", 0.0),
        liquidation_price=d.get("liquidation_price"),
        tp_price=d.get("tp_price"),
        sl_price=d.get("sl_price"),
        strategy_name=d.get("strategy", ""),
        initial_size=d.get("initial_size"),
        close_price=d.get("close_price"),
        close_ts=d.get("close_ts"),
        close_reason=d.get("close_reason", ""),
        pnl=d.get("pnl", 0.0),
        pnl_pct=d.get("pnl_pct", 0.0),
        entry_fee=d.get("entry_fee", 0.0),
        exit_fee=d.get("exit_fee", 0.0),
        funding=d.get("funding", 0.0),
        funding_settled_through_ts=d.get("funding_settled_through_ts", d.get("entry_ts", 0)),
        funding_intervals_paid=d.get("funding_intervals_paid", 0),
        slippage_cost=d.get("slippage_cost", 0.0),
        max_favorable_pct=d.get("max_favorable_pct", 0.0),
        max_adverse_pct=d.get("max_adverse_pct", 0.0),
        candles_held=d.get("candles_held", 0),
        breakeven_done=bool(d.get("breakeven_done", False)),
        partial_done=bool(d.get("partial_done", False)),
    )


def _eq_from_dict(d: dict[str, Any]) -> EquityPoint:
    return EquityPoint(
        ts=d.get("ts", 0),
        equity=d.get("equity", 0.0),
        open_positions=d.get("open_positions", 0),
    )


def result_from_dict(data: dict[str, Any]) -> BacktestResult | GridResult | None:
    """Rebuild a ``BacktestResult`` from the dict produced by ``result_to_dict``.

    Returns ``None`` if the payload doesn't look like a stored result (so the
    UI can skip corrupt/unrecognised files gracefully).
    """
    if not isinstance(data, dict):
        return None
    if data.get("result_type") == "grid":
        return grid_result_from_dict(data)
    cfg = data.get("config")
    if not isinstance(cfg, dict):
        return None

    try:
        result = BacktestResult(
            config=BacktestConfig(
                **_backtest_config_kwargs(cfg),
            ),
            metrics=dict(data.get("metrics") or {}),
            per_strategy=dict(data.get("per_strategy") or {}),
            per_symbol=dict(data.get("per_symbol") or {}),
            trades=[
                _trade_from_dict(t) for t in (data.get("trades") or [])
                if isinstance(t, dict)
            ],
            equity_curve=[
                _eq_from_dict(p) for p in (data.get("equity_curve") or [])
                if isinstance(p, dict)
            ],
            duration_seconds=float(data.get("duration_seconds") or 0.0),
            started_at=str(data.get("started_at") or ""),
            finished_at=str(data.get("finished_at") or ""),
            assumptions=dict(data.get("assumptions") or {}),
            data_provenance=[
                dict(item) for item in data.get("data_provenance") or []
                if isinstance(item, dict)
            ],
            candles_processed=int(data.get("candles_processed") or 0),
            error=data.get("error"),
        )
        return result
    except (TypeError, ValueError, KeyError):
        return None


def grid_result_from_dict(data: dict[str, Any]) -> GridResult | None:
    """Rebuild a ``GridResult`` from a saved or API grid-result payload."""
    if not isinstance(data, dict) or not isinstance(data.get("config"), dict):
        return None
    try:
        config_data = data["config"]
        base_data = config_data.get("base_config") or {}
        grid_config = GridConfig(
            base_config=BacktestConfig(**_backtest_config_kwargs(base_data)),
            params=[
                GridParamDef(
                    key=str(param.get("key") or ""),
                    values=list(param.get("values") or []),
                    label=str(param.get("label") or ""),
                )
                for param in config_data.get("params") or []
                if isinstance(param, dict)
            ],
            rank_by=str(config_data.get("rank_by") or "net_profit_after_cost_pct"),
            min_trades=int(config_data.get("min_trades") or 0),
            validation_folds=int(config_data.get("validation_folds") or 0),
            validation_train_ratio=float(config_data.get("validation_train_ratio") or 0.7),
            final_holdout_fraction=float(config_data.get("final_holdout_fraction") or 0.0),
            search_mode=str(config_data.get("search_mode") or "exhaustive"),
            combination_budget=int(config_data.get("combination_budget") or 0),
            random_seed=int(config_data.get("random_seed") or 0),
        )
        runs: list[GridRunResult] = []
        for row in data.get("runs") or []:
            if not isinstance(row, dict):
                continue
            bt_data = row.get("result")
            bt_result = result_from_dict(bt_data) if isinstance(bt_data, dict) else None
            if isinstance(bt_result, GridResult):
                continue
            runs.append(GridRunResult(
                params=dict(row.get("params") or {}),
                result=bt_result,
                rank_score=row.get("rank_score"),
                below_min_trades=bool(row.get("below_min_trades", False)),
                fold_metrics=[
                    dict(fold) for fold in row.get("fold_metrics") or []
                    if isinstance(fold, dict)
                ],
            ))
        result = GridResult(
            config=grid_config,
            runs=runs,
            ranked=[],
            started_at=str(data.get("started_at") or ""),
            finished_at=str(data.get("finished_at") or ""),
            duration_seconds=float(data.get("duration_seconds") or 0.0),
            total_combinations=int(data.get("total_combinations") or 0),
            attempted_combinations=int(data.get("attempted_combinations") or 0),
            search_mode=str(data.get("search_mode") or grid_config.search_mode),
            random_seed=int(data.get("random_seed") or grid_config.random_seed),
            assumptions=dict(data.get("assumptions") or _backtest_assumptions(grid_config.base_config)),
            data_provenance=[
                dict(item) for item in data.get("data_provenance") or []
                if isinstance(item, dict)
            ],
            final_holdout=(
                _grid_run_from_dict(data["final_holdout"])
                if isinstance(data.get("final_holdout"), dict) else None
            ),
            error=data.get("error"),
        )
        indexes = data.get("ranked_indexes") or []
        result.ranked = [runs[idx] for idx in indexes if isinstance(idx, int) and 0 <= idx < len(runs)]
        return result
    except (TypeError, ValueError, KeyError):
        return None


def _backtest_config_kwargs(config_data: dict[str, Any]) -> dict[str, Any]:
    """Select known config fields and apply defaults for legacy result files."""
    allowed = {field.name for field in fields(BacktestConfig)}
    values = {key: value for key, value in config_data.items() if key in allowed}
    values["symbols"] = list(values.get("symbols") or [])
    values["timeframe"] = str(values.get("timeframe") or "")
    values["start_ts"] = int(values.get("start_ts") or 0)
    values["end_ts"] = int(values.get("end_ts") or 0)
    values["initial_capital"] = float(values.get("initial_capital") or 0.0)
    values["strategy_names"] = list(values.get("strategy_names") or [])
    return values


def _grid_run_from_dict(row: dict[str, Any]) -> GridRunResult | None:
    bt_data = row.get("result")
    bt_result = result_from_dict(bt_data) if isinstance(bt_data, dict) else None
    if isinstance(bt_result, GridResult):
        return None
    return GridRunResult(
        params=dict(row.get("params") or {}),
        result=bt_result,
        rank_score=row.get("rank_score"),
        below_min_trades=bool(row.get("below_min_trades", False)),
        fold_metrics=[
            dict(fold) for fold in row.get("fold_metrics") or []
            if isinstance(fold, dict)
        ],
    )


# ---------------------------------------------------------------------------
# Result summary row (for comparison.csv + the UI's Saved Runs table)
# ---------------------------------------------------------------------------


def _metrics_summary(result: BacktestResult) -> dict[str, Any]:
    """Flatten headline metrics into ``m_*`` keys for the CSV / table row."""
    m = result.metrics or {}
    row: dict[str, Any] = {}
    for k, v in m.items():
        row[f"m_{k}"] = v
    return row


def result_summary_row(
    result: BacktestResult,
    *,
    run_id: str = "",
    ltf: str = "",
    htf: str = "",
) -> dict[str, Any]:
    """Build a flat summary row for a result (one CSV row / table entry)."""
    cfg = result.config
    return {
        "run_id": run_id or f"{cfg.timeframe}",
        "ltf": ltf or cfg.timeframe,
        "htf": htf,
        "symbols": ",".join(cfg.symbols),
        "strategies": ",".join(cfg.strategy_names),
        "error": result.error or "",
        "duration_seconds": round(result.duration_seconds, 2),
        "candles_processed": result.candles_processed,
        **_metrics_summary(result),
    }


# ---------------------------------------------------------------------------
# CSV writing / reading
# ---------------------------------------------------------------------------


def write_comparison_csv(
    rows: Iterable[dict[str, Any]],
    *,
    output_dir: Path | None = None,
    append: bool = True,
) -> Path:
    """Write (or append) one-or-more summary rows to ``comparison.csv``.

    ``append=True`` (default) adds new rows to the existing file (keyed by
    ``run_id`` so duplicates are skipped).  ``append=False`` overwrites.

    Returns the path written.
    """
    out_dir = output_dir or DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / CSV_FILENAME

    existing: list[dict[str, Any]] = []
    if append and path.exists():
        with open(path, newline="") as fh:
            existing = list(csv.DictReader(fh))

    # Merge, de-duplicating on run_id.
    seen = {r.get("run_id") for r in existing}
    merged = list(existing)
    for row in rows:
        rid = row.get("run_id")
        if rid and rid in seen:
            continue
        seen.add(rid)
        merged.append(row)

    fieldnames = sorted({k for r in merged for k in r.keys()}, reverse=True)
    # Keep a stable, readable column order: identity cols first.
    preferred = ["run_id", "ltf", "htf", "symbols", "strategies", "error",
                 "duration_seconds", "candles_processed"]
    ordered = [c for c in preferred if c in fieldnames] + [c for c in fieldnames if c not in preferred]

    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=ordered)
        writer.writeheader()
        for r in merged:
            writer.writerow(r)
    return path


def read_comparison_csv(output_dir: Path | None = None) -> list[dict[str, Any]]:
    """Read all rows from ``comparison.csv`` (empty list if absent)."""
    path = (output_dir or DEFAULT_OUTPUT_DIR) / CSV_FILENAME
    if not path.exists():
        return []
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Result file discovery / save / load / delete
# ---------------------------------------------------------------------------


def iter_result_files(output_dir: Path | None = None):
    """Yield ``*_results.json`` paths under the output dir, newest first."""
    out_dir = output_dir or DEFAULT_OUTPUT_DIR
    if not out_dir.exists():
        return
    for p in sorted(out_dir.glob("*_results.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        yield p


def save_result(
    result: BacktestResult | GridResult,
    *,
    run_id: str,
    output_dir: Path | None = None,
) -> Path:
    """Persist a result to ``<run_id>_results.json`` and append its CSV row.

    Also writes the per-strategy breakdown.  Returns the results.json path.
    """
    out_dir = output_dir or DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{run_id}_results.json"
    path.write_text(json.dumps(result_to_dict(result), indent=2, default=str))

    if isinstance(result, BacktestResult):
        breakdown = out_dir / f"{run_id}_per_strategy.json"
        breakdown.write_text(
            json.dumps({"per_strategy": result.per_strategy,
                        "per_symbol": result.per_symbol,
                        "metrics": result.metrics},
                       indent=2, default=str)
        )
        write_comparison_csv(
            [result_summary_row(result, run_id=run_id)],
            output_dir=out_dir,
            append=True,
        )
    elif result.ranked:
        best_run = next((
            run for run in result.ranked
            if run.rank_score is not None
            and not run.below_min_trades
            and run.result is not None
        ), None)
        best_result = best_run.result if best_run is not None else None
        if best_result is not None:
            write_comparison_csv(
                [result_summary_row(best_result, run_id=run_id)],
                output_dir=out_dir,
                append=True,
            )
    return path


def load_result(
    path: Path,
) -> BacktestResult | GridResult | None:
    """Load a ``*_results.json`` file into a ``BacktestResult``."""
    try:
        data = json.loads(Path(path).read_text())
    except (json.JSONDecodeError, OSError):
        return None
    result = result_from_dict(data)
    if result is None:
        return None
    # Attach config fields not stored in the summary so downstream rendering
    # (TP/SL prices, equity time) has what it needs.
    return result


def delete_result(
    path: Path,
    *,
    output_dir: Path | None = None,
) -> None:
    """Delete a result file and its matching per-strategy breakdown.

    The CSV row is not removed automatically (it's cumulative history); the
    caller may call :func:`write_comparison_csv` to rewrite if desired.
    """
    p = Path(path)
    if p.exists():
        p.unlink()
    stem = p.name.split("_results.json")[0]
    if stem:
        breakdown = (output_dir or DEFAULT_OUTPUT_DIR) / f"{stem}_per_strategy.json"
        if breakdown.exists():
            breakdown.unlink()


# ---------------------------------------------------------------------------
# Run-id helpers
# ---------------------------------------------------------------------------


def make_run_id(ltf: str) -> str:
    """Build a unique run id for a given LTF, e.g. ``20260809_101530_15m``."""
    try:
        from datetime import datetime, timezone
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    except Exception:  # noqa: BLE001 - fallback to pid+epoch
        stamp = str(os.getpid())
    clean_ltf = str(ltf).replace("/", "_").replace(" ", "")
    return f"{stamp}_{clean_ltf}"