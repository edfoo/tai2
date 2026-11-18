#!/usr/bin/env python3
"""Analyse manually-placed OKX swap trades (read-only) to ground the wick-rejection strategy.

Pulls closed positions and fills from the OKX account, keeps the positions whose
opening fills were placed by hand (client order id not carrying a bot prefix),
and for each one reconstructs, from public 1m / 1H candles and funding history:

  * entry timing relative to the hour boundary (seconds before HH:00)
  * hold time, realised result (price %, PnL % on margin, USDT)
  * max adverse / favourable excursion while open (from 1m candles)
  * the dip right after the next hour opens (lowest low in the first 1/5/10 min)
  * the setup at entry: where price sat in the forming 1H candle's range
    (high offset), and the largest green 1H body in the last 4 hours relative
    to the average body of the 8 candles before it
  * the funding rate in force at entry

**Read-only.** Only GET endpoints are called (positions-history, fills-history,
history-candles, funding-rate-history). Nothing is placed, amended or cancelled,
and the app does not need to be running.

Run from the repo root with the project venv:

    .venv/bin/python scripts/analyze_manual_trades.py
    .venv/bin/python scripts/analyze_manual_trades.py --start 2026-10-24T17:00 --end 2026-11-09T17:00
    .venv/bin/python scripts/analyze_manual_trades.py --csv logs/manual_trades.csv

Times are UTC (the defaults are 19:00 SAST = 17:00 UTC). The window is padded by ``--pad-hours`` (default 12) on each side
because the trade times you remember may be in local time; the first and last
trade found are printed so you can confirm the window is right.

OKX keeps positions/fills history for about 3 months.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from okx import Account, MarketData, PublicData, Trade  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.services.market_service import BOT_CLIENT_ORDER_PREFIXES  # noqa: E402

HOUR_MS = 3_600_000
MIN_MS = 60_000
REQUEST_PAUSE_S = 0.25  # stay well inside OKX's per-endpoint rate limits


# ── helpers ──────────────────────────────────────────────────────────────────

def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _fmt_ts(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_utc(text: str) -> datetime:
    dt = datetime.fromisoformat(text)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _data(response: Any, what: str) -> list[dict[str, Any]]:
    if not isinstance(response, dict):
        raise RuntimeError(f"{what}: unexpected response {response!r}")
    if str(response.get("code")) != "0":
        raise RuntimeError(f"{what}: OKX error {response.get('code')} {response.get('msg')}")
    return response.get("data") or []


def _is_bot(cl_ord_id: str | None) -> bool:
    return bool(cl_ord_id) and str(cl_ord_id).lower().startswith(BOT_CLIENT_ORDER_PREFIXES)


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[idx]


# ── OKX fetchers (GET only) ──────────────────────────────────────────────────

def fetch_positions_history(account: Any, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    """Closed SWAP positions updated within [start_ms, end_ms], newest first."""
    rows: list[dict[str, Any]] = []
    after = ""
    while True:
        page = _data(
            account.get_positions_history(instType="SWAP", after=after, limit="100"),
            "positions-history",
        )
        time.sleep(REQUEST_PAUSE_S)
        if not page:
            break
        rows.extend(page)
        oldest = min(int(p["uTime"]) for p in page)
        if oldest < start_ms or len(page) < 100:
            break
        after = str(oldest)
    return [r for r in rows if start_ms <= int(r["cTime"]) <= end_ms]


def fetch_fills_history(trade: Any, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    """SWAP fills with ts within [start_ms, end_ms] (paginated by billId)."""
    rows: list[dict[str, Any]] = []
    after = ""
    while True:
        page = _data(
            trade.get_fills_history(instType="SWAP", after=after, limit="100"),
            "fills-history",
        )
        time.sleep(REQUEST_PAUSE_S)
        if not page:
            break
        rows.extend(page)
        oldest_ts = min(int(p["ts"]) for p in page)
        if oldest_ts < start_ms or len(page) < 100:
            break
        after = page[-1]["billId"]
    return [r for r in rows if start_ms <= int(r["ts"]) <= end_ms]


def fetch_candles(market: Any, inst_id: str, bar: str, start_ms: int, end_ms: int) -> list[list[float]]:
    """Candles [ts, o, h, l, c] covering [start_ms, end_ms], oldest first."""
    out: dict[int, list[float]] = {}
    after = str(end_ms + 1)
    while True:
        page = _data(
            market.get_history_candlesticks(instId=inst_id, after=after, bar=bar, limit="100"),
            f"history-candles {inst_id} {bar}",
        )
        time.sleep(REQUEST_PAUSE_S)
        if not page:
            break
        for row in page:
            ts = int(row[0])
            out[ts] = [ts, float(row[1]), float(row[2]), float(row[3]), float(row[4])]
        oldest = min(int(r[0]) for r in page)
        if oldest <= start_ms or len(page) < 100:
            break
        after = str(oldest)
    return [out[ts] for ts in sorted(out) if start_ms <= ts <= end_ms]


def fetch_funding(public: Any, inst_id: str, start_ms: int, end_ms: int) -> list[tuple[int, float]]:
    """(fundingTime, realizedRate) pairs covering [start_ms, end_ms], oldest first."""
    out: dict[int, float] = {}
    after = str(end_ms + 1)
    for _ in range(20):
        page = _data(
            public.funding_rate_history(instId=inst_id, after=after, limit="100"),
            f"funding-rate-history {inst_id}",
        )
        time.sleep(REQUEST_PAUSE_S)
        if not page:
            break
        for row in page:
            rate = _f(row.get("realizedRate")) if row.get("realizedRate") else _f(row.get("fundingRate"))
            if rate is not None:
                out[int(row["fundingTime"])] = rate
        oldest = min(int(r["fundingTime"]) for r in page)
        if oldest <= start_ms or len(page) < 100:
            break
        after = str(oldest)
    return sorted(out.items())


# ── analysis ─────────────────────────────────────────────────────────────────

@dataclass
class TradeRow:
    inst_id: str
    direction: str
    lever: float | None
    entry_ts: int
    exit_ts: int
    entry_px: float
    exit_px: float | None
    close_type: str
    realized_pnl_usdt: float | None
    fee_usdt: float | None
    funding_fee_usdt: float | None
    sec_before_hour: float = 0.0
    hold_min: float = 0.0
    price_move_pct: float | None = None       # favourable = positive
    pnl_on_margin_pct: float | None = None    # price_move_pct × lever
    mae_pct: float | None = None              # worst adverse price move while open
    mae_margin_pct: float | None = None
    mfe_pct: float | None = None
    dip_1m_pct: float | None = None           # best favourable move in first N min of next hour
    dip_5m_pct: float | None = None
    dip_10m_pct: float | None = None
    high_offset_pct: float | None = None      # (H - entry) / (H - L) × 100 of forming 1H candle
    forming_range_pct: float | None = None
    forming_body_ratio: float | None = None   # forming candle green body ÷ avg body of prior 8
    max_green_body_ratio_4h: float | None = None
    green_bodies_over_5x_4h: int = 0
    funding_rate: float | None = None
    funding_interval_h: float | None = None
    notes: list[str] = field(default_factory=list)


def _classify(position: dict[str, Any], fills: list[dict[str, Any]]) -> tuple[str, list[dict], list[dict]]:
    """Return ('manual'|'bot'|'mixed'|'unknown', opening fills, closing fills)."""
    inst = position["instId"]
    pos_side = position.get("posSide") or "net"
    direction = position.get("direction") or ("short" if pos_side == "short" else "long")
    c_time, u_time = int(position["cTime"]), int(position["uTime"])
    related = [
        f for f in fills
        if f["instId"] == inst
        and (pos_side == "net" or (f.get("posSide") or "net") == pos_side)
        and c_time - MIN_MS <= int(f["ts"]) <= u_time + MIN_MS
    ]
    open_side = "sell" if direction == "short" else "buy"
    opening = [f for f in related if f["side"] == open_side]
    closing = [f for f in related if f["side"] != open_side]
    if not opening:
        return "unknown", opening, closing
    flags = {_is_bot(f.get("clOrdId")) for f in opening}
    if flags == {False}:
        return "manual", opening, closing
    if flags == {True}:
        return "bot", opening, closing
    return "mixed", opening, closing


def _body_ratio(candles: list[list[float]], idx: int, lookback: int = 8) -> float | None:
    """Green body of candles[idx] ÷ mean absolute body of the `lookback` before it."""
    if idx < lookback:
        return None
    _, o, _, _, c = candles[idx]
    if c <= o:
        return 0.0
    prior = [abs(k[4] - k[1]) for k in candles[idx - lookback: idx]]
    avg = sum(prior) / len(prior)
    return (c - o) / avg if avg > 0 else None


def analyse(trade: TradeRow, market: Any, public: Any, funding_cache: dict[str, list]) -> None:
    short = trade.direction == "short"
    sign = -1.0 if short else 1.0
    e = trade.entry_px

    hour_start = trade.entry_ts - trade.entry_ts % HOUR_MS
    next_hour = hour_start + HOUR_MS
    trade.sec_before_hour = (next_hour - trade.entry_ts) / 1000.0
    trade.hold_min = (trade.exit_ts - trade.entry_ts) / MIN_MS
    if trade.exit_px:
        trade.price_move_pct = sign * (trade.exit_px - e) / e * 100.0
        if trade.lever:
            trade.pnl_on_margin_pct = trade.price_move_pct * trade.lever

    # 1m path from the start of the entry hour to exit (capped at 48h).
    path_end = min(trade.exit_ts, trade.entry_ts + 48 * HOUR_MS)
    m1 = fetch_candles(market, trade.inst_id, "1m", hour_start, max(path_end, next_hour + 10 * MIN_MS))
    entry_min = trade.entry_ts - trade.entry_ts % MIN_MS
    held = [k for k in m1 if entry_min <= k[0] <= path_end]
    if held:
        if short:
            trade.mae_pct = (max(k[2] for k in held) - e) / e * 100.0
            trade.mfe_pct = (e - min(k[3] for k in held)) / e * 100.0
        else:
            trade.mae_pct = (e - min(k[3] for k in held)) / e * 100.0
            trade.mfe_pct = (max(k[2] for k in held) - e) / e * 100.0
        if trade.lever:
            trade.mae_margin_pct = trade.mae_pct * trade.lever

    for minutes, attr in ((1, "dip_1m_pct"), (5, "dip_5m_pct"), (10, "dip_10m_pct")):
        window = [k for k in m1 if next_hour <= k[0] < next_hour + minutes * MIN_MS]
        if window:
            best = min(k[3] for k in window) if short else max(k[2] for k in window)
            setattr(trade, attr, sign * (best - e) / e * 100.0)

    # Forming 1H candle at entry, rebuilt from 1m candles up to the entry minute.
    forming = [k for k in m1 if hour_start <= k[0] <= entry_min]
    if forming:
        f_open = forming[0][1]
        f_high = max(max(k[2] for k in forming), e)
        f_low = min(min(k[3] for k in forming), e)
        if f_high > f_low:
            trade.high_offset_pct = (f_high - e) / (f_high - f_low) * 100.0
        trade.forming_range_pct = (f_high - f_low) / f_low * 100.0 if f_low > 0 else None
        h1 = fetch_candles(market, trade.inst_id, "1H", hour_start - 13 * HOUR_MS, hour_start - HOUR_MS)
        if len(h1) >= 8:
            prior = [abs(k[4] - k[1]) for k in h1[-8:]]
            avg = sum(prior) / len(prior)
            if avg > 0 and e > f_open:
                trade.forming_body_ratio = (e - f_open) / avg
            elif avg > 0:
                trade.forming_body_ratio = 0.0
        ratios = [_body_ratio(h1, i) for i in range(max(0, len(h1) - 4), len(h1))]
        ratios = [r for r in ratios if r is not None]
        if ratios:
            trade.max_green_body_ratio_4h = max(ratios)
            trade.green_bodies_over_5x_4h = sum(1 for r in ratios if r >= 5.0)
        else:
            trade.notes.append("not enough 1H history for body ratio")

    # Funding in force at entry (last settlement at or before entry).
    if trade.inst_id not in funding_cache:
        try:
            funding_cache[trade.inst_id] = fetch_funding(
                public, trade.inst_id, trade.entry_ts - 3 * 24 * HOUR_MS, trade.entry_ts + 24 * HOUR_MS
            )
        except RuntimeError as exc:
            funding_cache[trade.inst_id] = []
            trade.notes.append(str(exc))
    history = [h for h in funding_cache[trade.inst_id] if h[0] <= trade.entry_ts + 24 * HOUR_MS]
    before = [h for h in history if h[0] <= trade.entry_ts]
    if before:
        trade.funding_rate = before[-1][1]
        if len(before) >= 2:
            trade.funding_interval_h = (before[-1][0] - before[-2][0]) / HOUR_MS


# ── reporting ────────────────────────────────────────────────────────────────

def _stats(label: str, values: list[float], fmt: str = "{:+.2f}") -> str:
    vals = [v for v in values if v is not None]
    if not vals:
        return f"  {label:<38} n/a"
    parts = [
        f"min {fmt.format(min(vals))}",
        f"p25 {fmt.format(_pct(vals, 0.25))}",
        f"med {fmt.format(statistics.median(vals))}",
        f"p75 {fmt.format(_pct(vals, 0.75))}",
        f"max {fmt.format(max(vals))}",
    ]
    return f"  {label:<38} " + "  ".join(parts) + f"   (n={len(vals)})"


def report(trades: list[TradeRow]) -> None:
    def n(x: float | None, fmt: str = "{:+.2f}") -> str:
        return fmt.format(x) if x is not None else "-"

    print()
    header = (
        f"{'#':>3} {'entry UTC':<19} {'symbol':<18} {'dir':<5} {'lev':>4} {'s<hr':>5} "
        f"{'hold':>6} {'move%':>6} {'mgn%':>7} {'MAE%':>6} {'MAEm%':>7} {'dip1':>6} "
        f"{'dip5':>6} {'dip10':>6} {'hiOff':>6} {'rng%':>5} {'fBody':>6} {'b4h':>6} "
        f"{'fund%':>7} {'close':<8}"
    )
    print(header)
    print("-" * len(header))
    for i, t in enumerate(trades, 1):
        print(
            f"{i:>3} {_fmt_ts(t.entry_ts):<19} {t.inst_id:<18} {t.direction:<5} "
            f"{n(t.lever, '{:.0f}'):>4} {t.sec_before_hour:>5.0f} {t.hold_min:>6.1f} "
            f"{n(t.price_move_pct):>6} {n(t.pnl_on_margin_pct):>7} {n(t.mae_pct):>6} "
            f"{n(t.mae_margin_pct):>7} {n(t.dip_1m_pct):>6} {n(t.dip_5m_pct):>6} "
            f"{n(t.dip_10m_pct):>6} {n(t.high_offset_pct, '{:.0f}'):>6} "
            f"{n(t.forming_range_pct, '{:.1f}'):>5} {n(t.forming_body_ratio, '{:.1f}'):>6} "
            f"{n(t.max_green_body_ratio_4h, '{:.1f}'):>6} "
            f"{n(t.funding_rate * 100 if t.funding_rate is not None else None, '{:+.3f}'):>7} "
            f"{t.close_type:<8}"
            + (f"  [{'; '.join(t.notes)}]" if t.notes else "")
        )

    print(
        "\nColumns: s<hr = seconds before the next hour at entry; hold = minutes; "
        "move% = favourable price move at exit;\nmgn% = move% × leverage (PnL on margin); "
        "MAE% / MAEm% = worst adverse price move while open (price / on margin);\n"
        "dip1/5/10 = best favourable price move in the first 1/5/10 min of the next hour; "
        "hiOff = (high − entry)/(high − low)×100 of the forming 1H candle (0 = at the high);\n"
        "rng% = forming candle range; fBody = forming candle green body ÷ avg body of prior 8 1H candles; "
        "b4h = largest such ratio among the last 4 closed 1H candles;\nfund% = funding rate in force at entry."
    )

    shorts = [t for t in trades if t.direction == "short"]
    for label, group in (("ALL", trades), ("SHORTS", shorts)):
        if not group:
            continue
        wins = [t for t in group if (t.realized_pnl_usdt or 0) > 0]
        total = sum(t.realized_pnl_usdt or 0 for t in group)
        print(f"\n== {label}: {len(group)} trades, {len(wins)} winners "
              f"({len(wins) / len(group) * 100:.0f}%), realised PnL {total:+.2f} USDT ==")
        print(_stats("PnL on margin % (at exit)", [t.pnl_on_margin_pct for t in group]))
        print(_stats("price move % (at exit)", [t.price_move_pct for t in group]))
        print(_stats("MAE on margin % (worst while open)", [t.mae_margin_pct for t in group]))
        print(_stats("MAE price %", [t.mae_pct for t in group]))
        print(_stats("hold minutes", [t.hold_min for t in group], "{:.1f}"))
        print(_stats("seconds before hour at entry", [t.sec_before_hour for t in group], "{:.0f}"))
        print(_stats("dip in first 1 min of next hour %", [t.dip_1m_pct for t in group]))
        print(_stats("dip in first 5 min of next hour %", [t.dip_5m_pct for t in group]))
        print(_stats("dip in first 10 min of next hour %", [t.dip_10m_pct for t in group]))
        print(_stats("high offset % (0 = at high)", [t.high_offset_pct for t in group], "{:.0f}"))
        print(_stats("forming candle range %", [t.forming_range_pct for t in group], "{:.2f}"))
        print(_stats("forming body ratio (x avg prior 8)", [t.forming_body_ratio for t in group], "{:.1f}"))
        print(_stats("max green body ratio, last 4 closed 1H", [t.max_green_body_ratio_4h for t in group], "{:.1f}"))
        print(_stats("funding rate at entry %",
                     [t.funding_rate * 100 for t in group if t.funding_rate is not None], "{:+.4f}"))
        intervals = [t.funding_interval_h for t in group if t.funding_interval_h]
        if intervals:
            counts = defaultdict(int)
            for h in intervals:
                counts[round(h)] += 1
            print(f"  {'funding interval (hours)':<38} "
                  + ", ".join(f"{h}h×{c}" for h, c in sorted(counts.items())))
        losers = [t for t in group if (t.realized_pnl_usdt or 0) <= 0]
        if losers:
            print(_stats("losers: PnL on margin %", [t.pnl_on_margin_pct for t in losers]))
            print(_stats("losers: hold minutes", [t.hold_min for t in losers], "{:.1f}"))
        liq = [t for t in group if t.close_type in {"liq", "part_liq", "adl"}]
        if liq:
            print(f"  liquidations / ADL: {len(liq)}")


CLOSE_TYPES = {"1": "partial", "2": "full", "3": "liq", "4": "part_liq", "5": "adl"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2026-10-24T17:00", help="window start, UTC (ISO)")
    parser.add_argument("--end", default="2026-11-09T17:00", help="window end, UTC (ISO)")
    parser.add_argument("--pad-hours", type=float, default=12.0,
                        help="widen the window on both sides (local-time slack), default 12")
    parser.add_argument("--include-bot", action="store_true",
                        help="also analyse bot-placed positions (default: manual only)")
    parser.add_argument("--csv", default=None, help="optional path to write per-trade rows")
    args = parser.parse_args()

    settings = get_settings()
    if not (settings.okx_api_key and settings.okx_secret_key and settings.okx_passphrase):
        print("ERROR: OKX credentials missing from .env", file=sys.stderr)
        return 1
    flag = str(settings.okx_api_flag or "0")
    creds = dict(
        api_key=settings.okx_api_key,
        api_secret_key=settings.okx_secret_key,
        passphrase=settings.okx_passphrase,
        flag=flag,
        debug=False,
    )
    account = Account.AccountAPI(**creds)
    trade_api = Trade.TradeAPI(**creds)
    market = MarketData.MarketAPI(flag=flag, debug=False)
    public = PublicData.PublicAPI(flag=flag, debug=False)

    pad = timedelta(hours=args.pad_hours)
    start = _parse_utc(args.start) - pad
    end = _parse_utc(args.end) + pad
    start_ms, end_ms = _ms(start), _ms(end)
    print(f"Window (UTC, padded): {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}  "
          f"[{'LIVE' if flag == '0' else 'DEMO'} account, read-only]")

    positions = fetch_positions_history(account, start_ms, end_ms)
    fills = fetch_fills_history(trade_api, start_ms - HOUR_MS, end_ms + 3 * 24 * HOUR_MS)
    print(f"Fetched {len(positions)} closed positions and {len(fills)} fills.")

    counts: dict[str, int] = defaultdict(int)
    trades: list[TradeRow] = []
    for pos in positions:
        origin, opening, closing = _classify(pos, fills)
        counts[origin] += 1
        if origin != "manual" and not (args.include_bot and origin in {"bot", "mixed"}):
            continue
        entry_ts = min(int(f["ts"]) for f in opening) if opening else int(pos["cTime"])
        exit_ts = max(int(f["ts"]) for f in closing) if closing else int(pos["uTime"])
        direction = pos.get("direction") or ("short" if pos.get("posSide") == "short" else "long")
        open_px = _f(pos.get("openAvgPx"))
        if not open_px:
            continue
        trades.append(TradeRow(
            inst_id=pos["instId"],
            direction=direction,
            lever=_f(pos.get("lever")),
            entry_ts=entry_ts,
            exit_ts=exit_ts,
            entry_px=open_px,
            exit_px=_f(pos.get("closeAvgPx")),
            close_type=CLOSE_TYPES.get(str(pos.get("type")), str(pos.get("type"))),
            realized_pnl_usdt=_f(pos.get("realizedPnl")),
            fee_usdt=_f(pos.get("fee")),
            funding_fee_usdt=_f(pos.get("fundingFee")),
        ))

    print("Positions by origin: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if not trades:
        print("No matching trades found.")
        return 0

    trades.sort(key=lambda t: t.entry_ts)
    funding_cache: dict[str, list] = {}
    for i, t in enumerate(trades, 1):
        print(f"  analysing {i}/{len(trades)} {t.inst_id} {_fmt_ts(t.entry_ts)} …", end="\r", flush=True)
        try:
            analyse(t, market, public, funding_cache)
        except RuntimeError as exc:
            t.notes.append(str(exc))
    print(" " * 80, end="\r")
    print(f"First trade: {_fmt_ts(trades[0].entry_ts)} UTC   Last trade: {_fmt_ts(trades[-1].entry_ts)} UTC")

    report(trades)

    if args.csv:
        path = Path(args.csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [asdict(t) | {"entry_utc": _fmt_ts(t.entry_ts), "exit_utc": _fmt_ts(t.exit_ts),
                             "notes": "; ".join(t.notes)} for t in trades]
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nWrote {len(rows)} rows to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
