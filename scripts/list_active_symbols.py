#!/usr/bin/env python3
"""List the symbols that are most active *right now* on OKX.

Uses the screener's recent-activity code path (``build_candidates`` →
``MarketService._fetch_recent_activity`` → ``compute_recent_activity``) so the
output is exactly what the live screener would rank on — not a re-implementation.

Unlike the 24h ticker aggregates, the features here come from the last
``--hours`` of candles, so a coin that pumped hours ago and has since gone quiet
scores low instead of high.

Run from the repo root with the project venv:

    .venv/bin/python scripts/list_active_symbols.py
    .venv/bin/python scripts/list_active_symbols.py --hours 3 --top 25
    .venv/bin/python scripts/list_active_symbols.py --hours 3 --timeframe 15m

Columns
-------
  speed       range% ÷ hours — the primary "how fast is it moving" metric.
              This is the default ranking key.
  eff         |mom%| ÷ range% — directional efficiency.  1.0 = a straight-line
              move, 0.0 = pure chop.  The best discriminator between a trend
              and a range at the same speed.
  vol_ratio   recent volume ÷ the symbol's own 24h-proportional baseline.
              > 1 means it is trading more than usual right now.  This is a
              *participation/liquidity* signal, not a speed signal: a fast move
              on vol_ratio < 0.5 is a thin squeeze where stops slip.
  range       high-low range over the window, as % of last price.
  mom         signed price change over the window, %.
  decay       recent range ÷ the range the 24h figure would imply for this
              window (√-time scaled).  < 0.25 is the "big 24h move, dead now"
              signature; >= 0.5 means the move is current.
  24h_hl      the 24h high-low range, for comparison.

Ranking
-------
Default is ``speed`` (range% per hour).  ``--sort`` selects the key:
``speed`` (default), ``vol_ratio``, ``range``, ``mom``, ``decay``.

Filters (all optional, applied before ranking):
  --min-vol-ratio   drop thin squeezes (e.g. 0.5)
  --min-efficiency  drop chop when hunting trends (e.g. 0.5)
  --max-efficiency  drop trends when hunting ranges (e.g. 0.3)
  --min-decay       drop moves that already happened (e.g. 0.5)
  --min-speed       drop slow movers, in %/hour (e.g. 1.0)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.market_service import MarketService  # noqa: E402
from app.services.screener import (  # noqa: E402
    build_candidates,
    compute_recent_activity,
    timeframe_to_ms,
)
from app.services.state_service import StateService  # noqa: E402
from app.services.strategies.defaults import screener_defaults  # noqa: E402


def _fmt(value: Any, width: int = 7, digits: int = 2) -> str:
    if value is None:
        return "n/a".rjust(width)
    return f"{value:.{digits}f}".rjust(width)


async def _run(args: argparse.Namespace) -> int:
    cfg = screener_defaults()
    # Override the window from the CLI; everything else keeps the live defaults
    # so the candidate pool matches what the bot would actually consider.
    cfg["activity_lookback_hours"] = float(args.hours)
    cfg["activity_timeframe"] = args.timeframe
    cfg["activity_prefilter_top_n"] = int(args.prefilter)
    cfg["activity_cache_seconds"] = 0.0  # always fetch fresh
    if args.min_volume is not None:
        cfg["min_volume_usd"] = float(args.min_volume) * 1_000_000
    if args.max_spread is not None:
        cfg["max_spread_pct"] = float(args.max_spread)

    service = MarketService(
        state_service=StateService(),
        enable_websocket=False,
        okx_flag=args.okx_flag,
    )

    print(f"Fetching SWAP tickers from OKX (flag={service._okx_flag}) ...", flush=True)
    tickers = await service._fetch_all_swap_tickers()
    if not tickers:
        print("ERROR: no tickers returned from OKX", file=sys.stderr)
        return 1

    candidates = build_candidates(tickers, cfg)
    print(
        f"{len(candidates)} candidates from {len(tickers)} tickers "
        f"(vol>={cfg['min_volume_usd'] / 1e6:.1f}M, spread<={cfg['max_spread_pct']}%)",
        flush=True,
    )
    if not candidates:
        print("No candidates passed the liquidity filters.", file=sys.stderr)
        return 1

    print(
        f"Fetching {cfg['activity_timeframe']} candles for the top "
        f"{min(len(candidates), cfg['activity_prefilter_top_n'])} by 24h volume ...",
        flush=True,
    )
    activity = await service._fetch_recent_activity(candidates, cfg)
    if not activity:
        print("ERROR: no activity data returned", file=sys.stderr)
        return 1

    hours = cfg["activity_lookback_hours"]
    rows: list[dict[str, Any]] = []
    for c in candidates:
        act = activity.get(c["symbol"])
        if not act:
            continue
        row = {**c, **act}
        # Derived metrics.
        # speed: % of price travelled per hour — the primary "fast-moving" key.
        row["speed"] = (
            (row["recent_range_pct"] / hours)
            if row.get("recent_range_pct") is not None and hours > 0
            else None
        )
        # efficiency: how much of the range is net displacement.  1.0 = straight
        # line, 0.0 = pure chop.  Undefined for a zero-range window.
        rng = row.get("recent_range_pct")
        mom = row.get("recent_mom_pct")
        row["efficiency"] = (
            min(1.0, abs(mom) / rng) if rng and rng > 0 and mom is not None else None
        )
        rows.append(row)

    # ── Filters ───────────────────────────────────────────────────────
    def _passes(r: dict[str, Any]) -> bool:
        if args.min_vol_ratio is not None:
            vr = r.get("recent_vol_ratio")
            if vr is None or vr < args.min_vol_ratio:
                return False
        if args.min_efficiency is not None:
            eff = r.get("efficiency")
            if eff is None or eff < args.min_efficiency:
                return False
        if args.max_efficiency is not None:
            eff = r.get("efficiency")
            if eff is None or eff > args.max_efficiency:
                return False
        if args.min_decay is not None:
            dec = r.get("decay_ratio")
            if dec is None or dec < args.min_decay:
                return False
        if args.min_speed is not None:
            spd = r.get("speed")
            if spd is None or spd < args.min_speed:
                return False
        return True

    scored = len(rows)
    rows = [r for r in rows if _passes(r)]
    if not rows:
        print("No symbols passed the requested filters.", file=sys.stderr)
        return 1

    # ── Ranking ───────────────────────────────────────────────────────
    sort_keys = {
        "speed": lambda r: r.get("speed") or 0.0,
        "vol_ratio": lambda r: r.get("recent_vol_ratio") or 0.0,
        "range": lambda r: r.get("recent_range_pct") or 0.0,
        "mom": lambda r: abs(r.get("recent_mom_pct") or 0.0),
        "decay": lambda r: r.get("decay_ratio") or 0.0,
    }
    rows.sort(key=sort_keys[args.sort], reverse=True)

    top = rows[: args.top]
    print()
    print(
        f"Most active symbols — last {hours:g}h "
        f"({cfg['activity_timeframe']} candles, {len(rows)}/{scored} scored, "
        f"sorted by {args.sort})"
    )
    print(
        f"{'#':>3}  {'symbol':<20} {'speed':>7} {'eff':>6} {'vol_ratio':>9} "
        f"{'range%':>7} {'mom%':>7} {'decay':>7} {'24h_hl%':>8} {'recent_vol':>12}"
    )
    print("-" * 104)
    for i, r in enumerate(top, 1):
        print(
            f"{i:>3}  {r['symbol']:<20} {_fmt(r.get('speed'))} "
            f"{_fmt(r.get('efficiency'), 6)} {_fmt(r.get('recent_vol_ratio'), 9)} "
            f"{_fmt(r.get('recent_range_pct'))} {_fmt(r.get('recent_mom_pct'))} "
            f"{_fmt(r.get('decay_ratio'))} {_fmt(r.get('hl_range_pct'), 8)} "
            f"{r.get('recent_vol') or 0:>12,.0f}"
        )

    # Show what the activity window is filtering out — the "was hot, now dead"
    # names that the old 24h ranking would have picked.
    faded = [
        r for r in rows
        if r.get("decay_ratio") is not None and r["decay_ratio"] < 0.25
    ]
    faded.sort(key=lambda r: r["decay_ratio"])
    if faded:
        print()
        print(f"Faded names the 24h ranking would have picked ({len(faded)}):")
        for r in faded[: args.top]:
            print(
                f"     {r['symbol']:<20} decay={r['decay_ratio']:.2f} "
                f"24h_hl={r['hl_range_pct']:.1f}% "
                f"recent_range={r['recent_range_pct']:.2f}% "
                f"vol_ratio={_fmt(r.get('recent_vol_ratio'), 5)}"
            )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List the most currently-active OKX SWAP symbols.",
    )
    parser.add_argument(
        "--hours", type=float, default=3.0,
        help="Activity lookback window in hours (default 3).",
    )
    parser.add_argument(
        "--timeframe", default="1H",
        help="Candle timeframe for the window (default 1H).",
    )
    parser.add_argument(
        "--top", type=int, default=20,
        help="How many symbols to print (default 20).",
    )
    parser.add_argument(
        "--prefilter", type=int, default=120,
        help="How many volume-ranked candidates get a candle fetch (default 120).",
    )
    parser.add_argument(
        "--sort", default="speed",
        choices=["speed", "vol_ratio", "range", "mom", "decay"],
        help="Ranking key (default speed = range%% per hour).",
    )
    parser.add_argument(
        "--min-vol-ratio", type=float, default=None,
        help="Drop symbols trading below this multiple of their own 24h average "
             "(e.g. 0.5 removes thin squeezes).",
    )
    parser.add_argument(
        "--min-efficiency", type=float, default=None,
        help="Drop chop: require |mom|/range >= this (e.g. 0.5 for trends).",
    )
    parser.add_argument(
        "--max-efficiency", type=float, default=None,
        help="Drop trends: require |mom|/range <= this (e.g. 0.3 for ranges).",
    )
    parser.add_argument(
        "--min-decay", type=float, default=None,
        help="Drop moves that already happened: require decay >= this (e.g. 0.5).",
    )
    parser.add_argument(
        "--min-speed", type=float, default=None,
        help="Drop slow movers: require range%%/hour >= this (e.g. 1.0).",
    )
    parser.add_argument(
        "--min-volume", type=float, default=None,
        help="Min 24h volume in millions of USDT (default: screener default).",
    )
    parser.add_argument(
        "--max-spread", type=float, default=None,
        help="Max bid/ask spread %% (default: screener default).",
    )
    parser.add_argument(
        "--okx-flag", default=None,
        help="OKX API flag: 0 = live, 1 = demo (default: from .env).",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
