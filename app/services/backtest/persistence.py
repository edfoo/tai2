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
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

# Import here, guarded, so the module can be imported by the UI (which always
# has these) and by the CLI (which also has them).  Kept as plain dataclasses,
# no Pydantic.
from app.services.backtest.models import (
    BacktestConfig,
    BacktestResult,
    EquityPoint,
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
        "entry_ts": t.entry_ts,
        "entry_price": t.entry_price,
        "tp_price": t.tp_price,
        "sl_price": t.sl_price,
        "close_reason": t.close_reason,
        "close_price": t.close_price,
        "close_ts": t.close_ts,
        "pnl": t.pnl,
        "pnl_pct": t.pnl_pct,
        "candles_held": t.candles_held,
    }


def result_to_dict(result: BacktestResult) -> dict[str, Any]:
    """Convert a ``BacktestResult`` into a JSON-serialisable dict."""
    return {
        "config": {
            "timeframe": result.config.timeframe,
            "symbols": result.config.symbols,
            "start_ts": result.config.start_ts,
            "end_ts": result.config.end_ts,
            "strategy_names": result.config.strategy_names,
            "initial_capital": result.config.initial_capital,
        },
        "metrics": result.metrics,
        "per_strategy": result.per_strategy,
        "trades_count": len(result.trades),
        "trades": [_trade_to_dict(t) for t in result.trades],
        "equity_curve": [
            {"ts": p.ts, "equity": p.equity, "open_positions": p.open_positions}
            for p in result.equity_curve
        ],
        "duration_seconds": result.duration_seconds,
        "candles_processed": result.candles_processed,
        "error": result.error,
    }


def _trade_from_dict(d: dict[str, Any]) -> SimPosition:
    return SimPosition(
        symbol=d.get("symbol", ""),
        direction=d.get("direction", "long"),
        size=d.get("size", 0.0),
        entry_price=d.get("entry_price", 0.0),
        entry_ts=d.get("entry_ts", 0),
        tp_price=d.get("tp_price"),
        sl_price=d.get("sl_price"),
        strategy_name=d.get("strategy", ""),
        close_price=d.get("close_price"),
        close_ts=d.get("close_ts"),
        close_reason=d.get("close_reason", ""),
        pnl=d.get("pnl", 0.0),
        pnl_pct=d.get("pnl_pct", 0.0),
        candles_held=d.get("candles_held", 0),
    )


def _eq_from_dict(d: dict[str, Any]) -> EquityPoint:
    return EquityPoint(
        ts=d.get("ts", 0),
        equity=d.get("equity", 0.0),
        open_positions=d.get("open_positions", 0),
    )


def result_from_dict(data: dict[str, Any]) -> BacktestResult | None:
    """Rebuild a ``BacktestResult`` from the dict produced by ``result_to_dict``.

    Returns ``None`` if the payload doesn't look like a stored result (so the
    UI can skip corrupt/unrecognised files gracefully).
    """
    if not isinstance(data, dict):
        return None
    cfg = data.get("config")
    if not isinstance(cfg, dict):
        return None

    try:
        result = BacktestResult(
            config=BacktestConfig(
                symbols=list(cfg.get("symbols") or []),
                timeframe=str(cfg.get("timeframe") or ""),
                start_ts=int(cfg.get("start_ts") or 0),
                end_ts=int(cfg.get("end_ts") or 0),
                initial_capital=float(cfg.get("initial_capital") or 0.0),
                strategy_names=list(cfg.get("strategy_names") or []),
            ),
            metrics=dict(data.get("metrics") or {}),
            per_strategy=dict(data.get("per_strategy") or {}),
            trades=[
                _trade_from_dict(t) for t in (data.get("trades") or [])
                if isinstance(t, dict)
            ],
            equity_curve=[
                _eq_from_dict(p) for p in (data.get("equity_curve") or [])
                if isinstance(p, dict)
            ],
            duration_seconds=float(data.get("duration_seconds") or 0.0),
            candles_processed=int(data.get("candles_processed") or 0),
            error=data.get("error"),
        )
        return result
    except (TypeError, ValueError, KeyError):
        return None


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
    result: BacktestResult,
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

    breakdown = out_dir / f"{run_id}_per_strategy.json"
    breakdown.write_text(
        json.dumps({"per_strategy": result.per_strategy, "metrics": result.metrics},
                   indent=2, default=str)
    )

    write_comparison_csv(
        [result_summary_row(result, run_id=run_id)],
        output_dir=out_dir,
        append=True,
    )
    return path


def load_result(
    path: Path,
) -> BacktestResult | None:
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