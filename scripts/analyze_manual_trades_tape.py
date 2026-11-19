#!/usr/bin/env python3
"""Tick-level momentum before each manual hour-end short (read-only).

Second step after ``scripts/analyze_manual_trades.py``: reads its CSV, keeps the
wick-rejection-style trades (shorts entered in the last ``--max-sec-before-hour``
seconds of the hour), pulls the public OKX trade tape for the ``--lookback``
seconds before each entry, and measures what momentum looked like at the moment
of entry:

  * price change over the last 30 s / 60 s / 2 min / 5 min / 10 min
  * acceleration: the last 60 s change minus the 60 s before it
  * seconds since the lookback window's high, and % below it
  * buy share of traded size over the last 60 s and 5 min
  * trade rate and traded size in the last 60 s vs the window average

These are the candidate inputs for the momentum entry mode (wick_rejection
plan, Phase 9). With a few dozen trades this describes what was traded; it does
not validate a rule.

**Read-only.** Only the public ``/api/v5/market/history-trades`` endpoint is
called (no credentials). OKX keeps about 3 months of tape.

    .venv/bin/python scripts/analyze_manual_trades.py --csv /tmp/manual_trades.csv
    .venv/bin/python scripts/analyze_manual_trades_tape.py /tmp/manual_trades.csv --csv /tmp/tape.csv
"""

from __future__ import annotations

import argparse
import csv
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

HISTORY_TRADES_URL = "https://www.okx.com/api/v5/market/history-trades"
REQUEST_PAUSE_S = 0.12  # endpoint limit is 20 requests / 2 s
MAX_PAGES = 600  # 60k trades per entry window


@dataclass
class TapeRow:
    inst_id: str
    entry_ts: int
    in_window: bool
    win: bool
    realized_pnl_usdt: float
    mae_margin_pct: float | None
    high_offset_pct: float | None
    sec_before_hour: float
    n_trades: int = 0
    ret_30s: float | None = None
    ret_60s: float | None = None
    ret_120s: float | None = None
    ret_300s: float | None = None
    ret_600s: float | None = None
    accel_60s: float | None = None
    secs_since_high: float | None = None
    below_high_pct: float | None = None
    buy_share_60s: float | None = None
    buy_share_300s: float | None = None
    rate_ratio_60s: float | None = None
    size_ratio_60s: float | None = None
    note: str = ""


def fetch_tape(client: httpx.Client, inst_id: str, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
    """All public trades in [start_ts, end_ts], oldest first, paging back by timestamp."""
    trades: dict[str, dict[str, Any]] = {}
    cursor = end_ts + 1
    for _ in range(MAX_PAGES):
        resp = client.get(
            HISTORY_TRADES_URL,
            params={"instId": inst_id, "type": "2", "after": str(cursor), "limit": "100"},
        )
        time.sleep(REQUEST_PAUSE_S)
        payload = resp.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"{inst_id}: OKX {payload.get('code')} {payload.get('msg')}")
        rows = payload.get("data") or []
        if not rows:
            break
        oldest = cursor
        for row in rows:
            ts = int(row["ts"])
            oldest = min(oldest, ts)
            if start_ts <= ts <= end_ts:
                trades[row["tradeId"]] = {
                    "ts": ts,
                    "px": float(row["px"]),
                    "sz": float(row["sz"]),
                    "buy": row.get("side") == "buy",
                }
        if oldest < start_ts or oldest >= cursor:
            break
        cursor = oldest
    else:
        raise RuntimeError(f"{inst_id}: more than {MAX_PAGES * 100} trades in the window; truncated")
    return sorted(trades.values(), key=lambda t: (t["ts"], t["px"]))


def _px_at(tape: list[dict[str, Any]], ts: int) -> float | None:
    """Last traded price at or before ts."""
    price = None
    for trade in tape:
        if trade["ts"] > ts:
            break
        price = trade["px"]
    return price


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def _buy_share(trades: list[dict[str, Any]]) -> float | None:
    total = sum(t["sz"] for t in trades)
    if total <= 0:
        return None
    return sum(t["sz"] for t in trades if t["buy"]) / total


def measure(row: TapeRow, tape: list[dict[str, Any]], lookback_s: int) -> None:
    t0 = row.entry_ts
    before = [t for t in tape if t["ts"] <= t0]  # includes the pre-window buffer
    window = [t for t in before if t["ts"] >= t0 - lookback_s * 1000]
    row.n_trades = len(window)
    if len(window) < 2:
        row.note = "no tape before entry"
        return
    px0 = window[-1]["px"]
    for secs, attr in ((30, "ret_30s"), (60, "ret_60s"), (120, "ret_120s"), (300, "ret_300s"), (600, "ret_600s")):
        if secs <= lookback_s:
            setattr(row, attr, _pct(px0, _px_at(before, t0 - secs * 1000)))
    prev_60 = _pct(_px_at(before, t0 - 60_000), _px_at(before, t0 - 120_000))
    if row.ret_60s is not None and prev_60 is not None:
        row.accel_60s = row.ret_60s - prev_60
    high = max(window, key=lambda t: (t["px"], t["ts"]))
    row.secs_since_high = (t0 - high["ts"]) / 1000.0
    row.below_high_pct = (high["px"] - px0) / high["px"] * 100.0
    last_60 = [t for t in window if t["ts"] > t0 - 60_000]
    last_300 = [t for t in window if t["ts"] > t0 - 300_000]
    row.buy_share_60s = _buy_share(last_60)
    row.buy_share_300s = _buy_share(last_300)
    span_s = max(1.0, (t0 - window[0]["ts"]) / 1000.0)
    windows_60 = span_s / 60.0
    if windows_60 > 1:
        row.rate_ratio_60s = len(last_60) / (len(window) / windows_60)
        total_sz = sum(t["sz"] for t in window)
        if total_sz > 0:
            row.size_ratio_60s = sum(t["sz"] for t in last_60) / (total_sz / windows_60)


def _f(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_trades(path: str, max_sec: float, start: int | None, end: int | None) -> list[TapeRow]:
    rows: list[TapeRow] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh):
            sec = _f(rec.get("sec_before_hour"))
            if rec.get("direction") != "short" or sec is None or sec > max_sec:
                continue
            entry_ts = int(float(rec["entry_ts"]))
            pnl = _f(rec.get("realized_pnl_usdt")) or 0.0
            rows.append(TapeRow(
                inst_id=rec["inst_id"],
                entry_ts=entry_ts,
                in_window=(start is None or entry_ts >= start) and (end is None or entry_ts <= end),
                win=pnl > 0,
                realized_pnl_usdt=pnl,
                mae_margin_pct=_f(rec.get("mae_margin_pct")),
                high_offset_pct=_f(rec.get("high_offset_pct")),
                sec_before_hour=sec,
            ))
    return sorted(rows, key=lambda r: r.entry_ts)


def _stats(label: str, values: list[float | None], fmt: str = "{:.2f}") -> str:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return f"  {label:<38} n=0"
    q = statistics.quantiles(vals, n=4) if len(vals) >= 2 else [vals[0]] * 3
    return (f"  {label:<38} n={len(vals):<3} min {fmt.format(vals[0])}  p25 {fmt.format(q[0])}  "
            f"med {fmt.format(q[1])}  p75 {fmt.format(q[2])}  max {fmt.format(vals[-1])}")


METRICS = (
    ("ret_30s", "price change last 30 s %"),
    ("ret_60s", "price change last 60 s %"),
    ("ret_120s", "price change last 2 min %"),
    ("ret_300s", "price change last 5 min %"),
    ("ret_600s", "price change last 10 min %"),
    ("accel_60s", "accel (last 60 s − prior 60 s) pp"),
    ("secs_since_high", "seconds since window high"),
    ("below_high_pct", "% below window high"),
    ("buy_share_60s", "buy share of size, last 60 s"),
    ("buy_share_300s", "buy share of size, last 5 min"),
    ("rate_ratio_60s", "trade rate last 60 s ÷ window avg"),
    ("size_ratio_60s", "size last 60 s ÷ window avg"),
)


def report(rows: list[TapeRow]) -> None:
    def n(v: float | None, fmt: str = "{:.2f}") -> str:
        return "-" if v is None else fmt.format(v)

    print(f"\n{'entry UTC':<17} {'symbol':<18} {'W':<1} {'pnl':>7} {'r30':>6} {'r60':>6} {'r2m':>6} "
          f"{'r5m':>6} {'acc':>6} {'sHi':>5} {'bHi%':>5} {'buy60':>5} {'rate':>5} {'size':>5}")
    for r in rows:
        ts = datetime.fromtimestamp(r.entry_ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
        flag = ("W" if r.win else "L") + ("" if r.in_window else "*")
        print(f"{ts:<17} {r.inst_id:<18} {flag:<2}{r.realized_pnl_usdt:>6.2f} {n(r.ret_30s):>6} "
              f"{n(r.ret_60s):>6} {n(r.ret_120s):>6} {n(r.ret_300s):>6} {n(r.accel_60s):>6} "
              f"{n(r.secs_since_high, '{:.0f}'):>5} {n(r.below_high_pct):>5} "
              f"{n(r.buy_share_60s):>5} {n(r.rate_ratio_60s, '{:.1f}'):>5} {n(r.size_ratio_60s, '{:.1f}'):>5}"
              f"{'  ' + r.note if r.note else ''}")
    print("  * = outside the --start/--end window")
    groups = (
        ("In window, all", [r for r in rows if r.in_window]),
        ("In window, winners", [r for r in rows if r.in_window and r.win]),
        ("In window, losers", [r for r in rows if r.in_window and not r.win]),
        ("Outside window", [r for r in rows if not r.in_window]),
    )
    for title, group in groups:
        if not group:
            continue
        print(f"\n{title} ({len(group)})")
        for attr, label in METRICS:
            print(_stats(label, [getattr(r, attr) for r in group]))


def _parse_utc(value: str | None) -> int | None:
    if not value:
        return None
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("trades_csv", help="CSV written by analyze_manual_trades.py --csv")
    parser.add_argument("--lookback", type=int, default=600, help="seconds of tape before each entry")
    parser.add_argument("--max-sec-before-hour", type=float, default=180.0)
    parser.add_argument("--start", default="2026-10-24T17:00", help="strategy window start, UTC")
    parser.add_argument("--end", default="2026-11-09T17:00", help="strategy window end, UTC")
    parser.add_argument("--csv", help="write per-trade measures to this CSV")
    args = parser.parse_args()

    rows = load_trades(args.trades_csv, args.max_sec_before_hour, _parse_utc(args.start), _parse_utc(args.end))
    print(f"{len(rows)} hour-end shorts; fetching {args.lookback} s of tape before each …")
    with httpx.Client(timeout=15.0, headers={"User-Agent": "tai2-analysis"}) as client:
        for i, row in enumerate(rows, 1):
            print(f"  {i}/{len(rows)} {row.inst_id}", end="\r", flush=True)
            try:
                # 60 s extra so the price at exactly t0 − lookback is known.
                start_ts = row.entry_ts - (args.lookback + 60) * 1000
                tape = fetch_tape(client, row.inst_id, start_ts, row.entry_ts)
            except (RuntimeError, httpx.HTTPError) as exc:
                row.note = str(exc)
                continue
            measure(row, tape, args.lookback)
    print(" " * 60, end="\r")
    report(rows)
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(asdict(rows[0]).keys()))
            writer.writeheader()
            writer.writerows(asdict(r) for r in rows)
        print(f"\nWrote {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
