#!/usr/bin/env python3
"""Summarise tai2 trading performance from runtime logs.

Parses the rotating log files in ``logs/`` and produces a performance
report covering:

  - Aggregate PnL, win rate, average win/loss across all log files
  - Per-strategy attribution (heuristic: pair each PnL with the most
    recent prior ``Launcher signal:`` for the same symbol)
  - Per-symbol PnL breakdown
  - Trade-management events (seeded, time_stop, position_closed,
    re-entry cooldown skips)
  - Guardrail blocks (R:R blocks, position-alignment blocks)
  - SL slippage detection: flags trades where the realised loss exceeds
    the SL distance by >1.5x (market-order slippage on thin books)
    - Stop-outs with peak favorable excursion when the logs emit peak/current
        unrealized-PnL tracking lines

Usage::

    python scripts/performance_summary.py            # all logs
    python scripts/performance_summary.py logs/app.log  # single file
    python scripts/performance_summary.py --json       # machine-readable

The script reads only log files — no DB, no Redis, no exchange calls.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── Project root ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = ROOT / "logs"

# ── Log line patterns ─────────────────────────────────────────────────────────
# Example PnL line:
#   2026-08-01 10:43:05,984 UTC · DEBUG:...:Reconciled PnL for SATS-USDT-SWAP: -0.4926 USDT (fill 298022321, trade ad9178d8...)
#
# NOTE: The same closing fill can be reconciled multiple times against
# different unreconciled trades (see market_service fill reconciliation),
# producing several "Reconciled PnL" lines with the SAME fill id but different
# trade ids.  We must deduplicate on fill id or the same realized PnL gets
# counted multiple times (inflating profits / masking losses).
_PNL_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Reconciled PnL for "
    r"([A-Z0-9-]+-USDT-SWAP): ([+-][0-9.]+) USDT "
    r"\(fill (\d+), trade ([0-9a-f-]+)\)"
)

# Example signal line:
#   2026-08-01 10:42:51,951 UTC · DEBUG:...:Launcher signal: SATS-USDT-SWAP BUY [trend_pullback] last=1.1383e-08 notional=30.0 tp=... sl=... [static]
_SIGNAL_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Launcher signal: "
    r"([A-Z0-9-]+-USDT-SWAP) (BUY|SELL) \[(\w+)\] "
    r"last=([0-9.eE+-]+) notional=([0-9.]+) "
    r"tp=([0-9.eE+-]+) sl=([0-9.eE+-]+)"
)

# Peak-favorable-excursion line from profit-trailing supervision.
_PEAK_EXCURSION_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Alternator: "
    r"([A-Z0-9-]+-USDT-SWAP).*peak_pct=([0-9.eE+-]+|None) current_pct=([0-9.eE+-]+|None) "
    r"peak_usd=([0-9.eE+-]+|None) current_usd=([0-9.eE+-]+|None)"
)

# TradeMgmt cleared line:
#   ...TradeMgmt: SATS-USDT-SWAP cleared (time_stop); re-entry cooldown 1800s
_CLEARED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*TradeMgmt: "
    r"([A-Z0-9-]+-USDT-SWAP) cleared \((\w+)\)"
)

# TradeMgmt seeded line:
#   ...TradeMgmt: seeded SATS-USDT-SWAP side=long entry=... tp=... sl=... risk_pct=...
_SEEDED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*TradeMgmt: seeded "
    r"([A-Z0-9-]+-USDT-SWAP) side=(\w+) entry=([0-9.eE+-]+) "
    r"tp=([0-9.eE+-]+) sl=([0-9.eE+-]+) risk_pct=([0-9.]+)"
)

# R:R guardrail block:
#   ...Blocked: reward-to-risk ratio ...
_RR_BLOCK_RE = re.compile(r"Blocked: reward-to-risk ratio")

# Position-alignment guardrail block:
#   ...Blocked by position-alignment guardrail: ...
_ALIGN_BLOCK_RE = re.compile(
    r"Blocked by position-alignment guardrail: (\w+) not allowed while (\w+)"
)

# Re-entry cooldown skip (launcher):
#   ...Launcher: MMT-USDT-SWAP [vwap_reversion] re-entry cooldown (208s remaining) — skipping
_COOLDOWN_SKIP_RE = re.compile(
    r"Launcher: ([A-Z0-9-]+-USDT-SWAP) \[(\w+)\] re-entry cooldown"
)

# Screener summary line:
#   ...Screener: 200 base candidates from 436 tickers (vol>=3000000 USD, spread<=0.50%, dual=True)
_SCREENER_RE = re.compile(
    r"Screener: (\d+) base candidates from (\d+) tickers "
    r"\(vol>=(\d+) USD(?:, spread<=([0-9.]+)%)?(?:, dual=(\w+))?\)"
)

STRATEGIES = (
    "trend_pullback",
    "vwap_reversion",
    "mean_reversion",
    "liquidity_sweep",
    "spike_continuation",
)


# ── Data containers ───────────────────────────────────────────────────────────
@dataclass
class PnLTrade:
    ts: str
    symbol: str
    pnl: float
    fill_id: str = ""
    mfe_peak_pct: Optional[float] = None
    mfe_peak_usd: Optional[float] = None


@dataclass
class Signal:
    ts: str
    symbol: str
    side: str
    strategy: str
    last: float
    notional: float
    tp: float
    sl: float


@dataclass
class SeededEntry:
    ts: str
    symbol: str
    side: str
    entry: float
    tp: float
    sl: float
    risk_pct: float


@dataclass
class ClearedEntry:
    ts: str
    symbol: str
    reason: str  # position_closed, time_stop, breakeven, etc.


@dataclass
class Summary:
    # Time period covered by the parsed logs
    start_ts: Optional[str] = None
    end_ts: Optional[str] = None
    # Aggregate
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    # Per-strategy (heuristic attribution)
    strat_signals: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    strat_trades: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    strat_wins: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    strat_losses: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    strat_pnl: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # Per-symbol
    sym_trades: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sym_wins: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sym_losses: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sym_pnl: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # Trade management
    seeded_count: int = 0
    cleared_reasons: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    cooldown_skips: int = 0
    cooldown_skips_by_strategy: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    # Guardrails
    rr_blocks: int = 0
    align_blocks: int = 0
    # SL slippage
    slippage_trades: list[dict] = field(default_factory=list)
    # Losing trades with observed peak favorable excursion
    stopout_peak_trades: list[dict] = field(default_factory=list)
    # Screener
    screener_runs: int = 0
    screener_last_candidates: int = 0
    screener_last_tickers: int = 0
    screener_last_vol: int = 0
    screener_last_spread: Optional[float] = None
    screener_last_dual: Optional[str] = None
    # Files parsed
    files_parsed: list[str] = field(default_factory=list)


# ── Parsing ───────────────────────────────────────────────────────────────────
def _resolve_log_files(paths: list[str]) -> list[Path]:
    """Resolve the list of log files to parse."""
    if paths:
        return [Path(p) for p in paths if Path(p).exists()]
    # Default: all rotated logs, oldest first for chronological order.
    log_dir = DEFAULT_LOG_DIR
    if not log_dir.exists():
        return []
    files = sorted(log_dir.glob("app.log*"), key=lambda p: p.name)
    # Sort by rotation index so oldest (.5) comes first, current (.log) last.
    def _sort_key(p: Path) -> tuple[int, str]:
        name = p.name
        if name == "app.log":
            return (99, name)
        # app.log.1 → 1, app.log.5 → 5
        try:
            idx = int(name.rsplit(".", 1)[1])
        except (ValueError, IndexError):
            idx = 0
        return (idx, name)

    return sorted(files, key=_sort_key)


def parse_logs(files: list[Path]) -> tuple[list[PnLTrade], list[Signal], list[SeededEntry], list[ClearedEntry], Summary]:
    """Parse all log files and return structured data + summary counters."""
    pnl_trades: list[PnLTrade] = []
    signals: list[Signal] = []
    seeded: list[SeededEntry] = []
    cleared: list[ClearedEntry] = []
    summary = Summary()
    active_peak_pct_by_symbol: dict[str, float] = {}
    active_peak_usd_by_symbol: dict[str, float] = {}
    # Deduplicate realized PnL by OKX fill id: the same closing fill is often
    # reconciled against multiple unreconciled trades, so only the FIRST
    # occurrence of each fill id should count as a real trade.
    seen_fills: set[str] = set()

    def _parse_optional_float(raw: str) -> Optional[float]:
        if raw == "None":
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _track_period(ts: str) -> None:
        """Expand the covered time window to include ``ts``."""
        if summary.start_ts is None or ts < summary.start_ts:
            summary.start_ts = ts
        if summary.end_ts is None or ts > summary.end_ts:
            summary.end_ts = ts

    for fpath in files:
        summary.files_parsed.append(fpath.name)
        try:
            text = fpath.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            # PnL
            m = _PNL_RE.search(line)
            if m:
                symbol = m.group(2)
                fill_id = m.group(4)
                # Skip duplicate reconciliations of the same closing fill.
                if fill_id in seen_fills:
                    continue
                seen_fills.add(fill_id)
                _track_period(m.group(1))
                pnl_trades.append(
                    PnLTrade(
                        ts=m.group(1),
                        symbol=symbol,
                        pnl=float(m.group(3)),
                        fill_id=fill_id,
                        mfe_peak_pct=active_peak_pct_by_symbol.pop(symbol, None),
                        mfe_peak_usd=active_peak_usd_by_symbol.pop(symbol, None),
                    )
                )
                continue
            # Signal
            m = _SIGNAL_RE.search(line)
            if m:
                sig = Signal(
                    ts=m.group(1),
                    symbol=m.group(2),
                    side=m.group(3),
                    strategy=m.group(4),
                    last=float(m.group(5)),
                    notional=float(m.group(6)),
                    tp=float(m.group(7)),
                    sl=float(m.group(8)),
                )
                signals.append(sig)
                _track_period(sig.ts)
                summary.strat_signals[sig.strategy] += 1
                active_peak_pct_by_symbol.pop(sig.symbol, None)
                active_peak_usd_by_symbol.pop(sig.symbol, None)
                continue
            # Peak favorable excursion
            m = _PEAK_EXCURSION_RE.search(line)
            if m:
                symbol = m.group(2)
                _track_period(m.group(1))
                peak_pct = _parse_optional_float(m.group(3))
                peak_usd = _parse_optional_float(m.group(5))
                if peak_pct is not None:
                    active_peak_pct_by_symbol[symbol] = max(
                        active_peak_pct_by_symbol.get(symbol, peak_pct), peak_pct
                    )
                if peak_usd is not None:
                    active_peak_usd_by_symbol[symbol] = max(
                        active_peak_usd_by_symbol.get(symbol, peak_usd), peak_usd
                    )
                continue
            # Seeded
            m = _SEEDED_RE.search(line)
            if m:
                seeded.append(
                    SeededEntry(
                        ts=m.group(1),
                        symbol=m.group(2),
                        side=m.group(3),
                        entry=float(m.group(4)),
                        tp=float(m.group(5)),
                        sl=float(m.group(6)),
                        risk_pct=float(m.group(7)),
                    )
                )
                _track_period(m.group(1))
                summary.seeded_count += 1
                continue
            # Cleared
            m = _CLEARED_RE.search(line)
            if m:
                reason = m.group(3)
                cleared.append(ClearedEntry(ts=m.group(1), symbol=m.group(2), reason=reason))
                _track_period(m.group(1))
                summary.cleared_reasons[reason] += 1
                continue
            # R:R block
            if _RR_BLOCK_RE.search(line):
                summary.rr_blocks += 1
                continue
            # Position-alignment block
            m = _ALIGN_BLOCK_RE.search(line)
            if m:
                summary.align_blocks += 1
                continue
            # Cooldown skip
            m = _COOLDOWN_SKIP_RE.search(line)
            if m:
                summary.cooldown_skips += 1
                summary.cooldown_skips_by_strategy[m.group(2)] += 1
                continue
            # Screener summary
            m = _SCREENER_RE.search(line)
            if m:
                summary.screener_runs += 1
                summary.screener_last_candidates = int(m.group(1))
                summary.screener_last_tickers = int(m.group(2))
                summary.screener_last_vol = int(m.group(3))
                if m.group(4):
                    summary.screener_last_spread = float(m.group(4))
                if m.group(5):
                    summary.screener_last_dual = m.group(5)

    return pnl_trades, signals, seeded, cleared, summary


# ── Attribution & analysis ────────────────────────────────────────────────────
def _attribute_strategy(pnl: PnLTrade, signals: list[Signal]) -> str:
    """Heuristic: most recent prior signal for the same symbol."""
    best = _most_recent_signal(pnl.symbol, pnl.ts, signals)
    return best.strategy if best else "unknown"


def _most_recent_signal(symbol: str, ts: str, signals: list[Signal]) -> Optional[Signal]:
    """Return the most recent prior signal for a symbol."""
    best: Optional[Signal] = None
    for sig in signals:
        if sig.symbol == symbol and sig.ts <= ts:
            if best is None or sig.ts > best.ts:
                best = sig
    return best


def _compute_slippage(
    pnl_trades: list[PnLTrade], signals: list[Signal], seeded: list[SeededEntry]
) -> list[dict]:
    """Detect trades where realised loss >> SL distance (market-order slippage).

    Uses the seeded entry (most accurate TP/SL) when available, else the
    most recent prior signal.  Flags trades where the loss % of notional
    exceeds 1.5x the SL distance %.
    """
    slippage: list[dict] = []
    for trade in pnl_trades:
        if trade.pnl >= 0:
            continue
        # Find the most recent seeded entry for this symbol.
        best_seed: Optional[SeededEntry] = None
        for s in seeded:
            if s.symbol == trade.symbol and s.ts <= trade.ts:
                if best_seed is None or s.ts > best_seed.ts:
                    best_seed = s
        # Find the most recent signal as fallback / for notional.
        best_sig: Optional[Signal] = None
        for s in signals:
            if s.symbol == trade.symbol and s.ts <= trade.ts:
                if best_sig is None or s.ts > best_sig.ts:
                    best_sig = s

        if best_seed is not None and best_seed.entry > 0:
            entry = best_seed.entry
            sl = best_seed.sl
            tp = best_seed.tp
            side = best_seed.side
            strategy = best_sig.strategy if best_sig else "unknown"
        elif best_sig is not None:
            entry = best_sig.last
            sl = best_sig.sl
            tp = best_sig.tp
            side = best_sig.side.lower()
            strategy = best_sig.strategy
        else:
            continue

        if entry <= 0 or sl <= 0:
            continue

        # SL distance as % of entry.
        if side in ("long", "buy"):
            sl_pct = (entry - sl) / entry * 100.0
        else:
            sl_pct = (sl - entry) / entry * 100.0
        if sl_pct <= 0:
            continue

        # Estimate notional: use signal notional (post-leverage target) as a
        # reasonable proxy.  The actual fill notional is not in the PnL line.
        notional = best_sig.notional if best_sig else 30.0
        loss_pct = abs(trade.pnl) / notional * 100.0
        overshoot = loss_pct / sl_pct

        if overshoot > 1.5:
            slippage.append(
                {
                    "ts": trade.ts,
                    "symbol": trade.symbol,
                    "strategy": strategy,
                    "side": side,
                    "entry": entry,
                    "sl": sl,
                    "sl_pct": round(sl_pct, 3),
                    "loss_usdt": round(trade.pnl, 4),
                    "loss_pct": round(loss_pct, 3),
                    "overshoot": round(overshoot, 2),
                    "notional_used": notional,
                }
            )
    return slippage


def build_summary(
    pnl_trades: list[PnLTrade],
    signals: list[Signal],
    seeded: list[SeededEntry],
    cleared: list[ClearedEntry],
    summary: Summary,
) -> Summary:
    """Populate aggregate, per-strategy, per-symbol, and slippage stats."""
    win_pnls: list[float] = []
    loss_pnls: list[float] = []

    for trade in pnl_trades:
        summary.total_trades += 1
        summary.total_pnl += trade.pnl
        summary.sym_trades[trade.symbol] += 1
        summary.sym_pnl[trade.symbol] += trade.pnl
        if trade.pnl > 0:
            summary.wins += 1
            summary.sym_wins[trade.symbol] += 1
            win_pnls.append(trade.pnl)
        else:
            summary.losses += 1
            summary.sym_losses[trade.symbol] += 1
            loss_pnls.append(trade.pnl)

        strat = _attribute_strategy(trade, signals)
        summary.strat_trades[strat] += 1
        summary.strat_pnl[strat] += trade.pnl
        if trade.pnl > 0:
            summary.strat_wins[strat] += 1
        else:
            summary.strat_losses[strat] += 1
            best_sig = _most_recent_signal(trade.symbol, trade.ts, signals)
            mfe_pct = trade.mfe_peak_pct
            if mfe_pct is None and trade.mfe_peak_usd is not None and best_sig is not None:
                if best_sig.notional > 0:
                    mfe_pct = trade.mfe_peak_usd / best_sig.notional * 100.0
            if mfe_pct is not None and mfe_pct > 0:
                summary.stopout_peak_trades.append(
                    {
                        "ts": trade.ts,
                        "symbol": trade.symbol,
                        "strategy": strat,
                        "pnl_usdt": round(trade.pnl, 4),
                        "mfe_pct": round(mfe_pct, 2),
                        "mfe_usd": round(trade.mfe_peak_usd, 4)
                        if trade.mfe_peak_usd is not None
                        else None,
                    }
                )

    summary.avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    summary.avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
    summary.slippage_trades = _compute_slippage(pnl_trades, signals, seeded)
    return summary


# ── Reporting ────────────────────────────────────────────────────────────────
def _fmt_pct(val: float, total: int) -> str:
    return f"{val / total * 100:.1f}%" if total else "n/a"


def print_report(summary: Summary) -> None:
    """Print a human-readable performance report."""
    print("=" * 78)
    print(" tai2 Performance Summary")
    print("=" * 78)
    print(f" Log files parsed: {', '.join(summary.files_parsed)}")
    if summary.start_ts and summary.end_ts:
        print(f" Period covered:  {summary.start_ts}  →  {summary.end_ts}")
    print()

    # ── Aggregate ──
    print("── Aggregate ──────────────────────────────────────────────────────")
    print(
        f"  Trades: {summary.total_trades}   "
        f"Wins: {summary.wins}   Losses: {summary.losses}   "
        f"Win rate: {_fmt_pct(summary.wins, summary.total_trades)}"
    )
    print(
        f"  Net PnL: {summary.total_pnl:+.4f} USDT   "
        f"Avg/trade: {summary.total_pnl / summary.total_trades:+.4f}"
        if summary.total_trades
        else "  No trades."
    )
    print(
        f"  Avg win: {summary.avg_win:+.4f}   "
        f"Avg loss: {summary.avg_loss:+.4f}   "
        f"Break-even win rate: "
        f"{abs(summary.avg_loss) / (summary.avg_win + abs(summary.avg_loss)) * 100:.1f}%"
        if (summary.avg_win + abs(summary.avg_loss)) > 0
        else ""
    )
    print()

    # ── Per-strategy ──
    print("── By strategy (heuristic attribution) ───────────────────────────")
    print(
        f"  {'strategy':<20}{'signals':>9}{'trades':>8}{'W':>5}{'L':>5}"
        f"{'win%':>7}{'net PnL':>11}{'avg':>9}"
    )
    print(f"  {'-' * 20}{'-' * 9}{'-' * 8}{'-' * 5}{'-' * 5}{'-' * 7}{'-' * 11}{'-' * 9}")
    for strat in sorted(
        summary.strat_trades.keys(), key=lambda s: summary.strat_pnl[s]
    ):
        n = summary.strat_trades[strat]
        w = summary.strat_wins[strat]
        l = summary.strat_losses[strat]
        pnl = summary.strat_pnl[strat]
        sig = summary.strat_signals.get(strat, 0)
        wr = w / n * 100 if n else 0
        avg = pnl / n if n else 0
        print(
            f"  {strat:<20}{sig:>9}{n:>8}{w:>5}{l:>5}{wr:>6.1f}%"
            f"{pnl:>+11.4f}{avg:>+9.4f}"
        )
    print()

    # ── Per-symbol ──
    print("── By symbol (worst first) ────────────────────────────────────────")
    print(
        f"  {'symbol':<20}{'trades':>8}{'W':>5}{'L':>5}{'net PnL':>11}"
    )
    print(f"  {'-' * 20}{'-' * 8}{'-' * 5}{'-' * 5}{'-' * 11}")
    for sym in sorted(summary.sym_pnl.keys(), key=lambda s: summary.sym_pnl[s]):
        n = summary.sym_trades[sym]
        w = summary.sym_wins[sym]
        l = summary.sym_losses[sym]
        pnl = summary.sym_pnl[sym]
        print(f"  {sym:<20}{n:>8}{w:>5}{l:>5}{pnl:>+11.4f}")
    print()

    # ── Trade management ──
    print("── Trade management ──────────────────────────────────────────────")
    print(f"  Seeded entries: {summary.seeded_count}")
    if summary.cleared_reasons:
        for reason, count in sorted(
            summary.cleared_reasons.items(), key=lambda x: -x[1]
        ):
            print(f"  Cleared ({reason}): {count}")
    print(f"  Re-entry cooldown skips: {summary.cooldown_skips}")
    if summary.cooldown_skips_by_strategy:
        for strat, count in sorted(
            summary.cooldown_skips_by_strategy.items(), key=lambda x: -x[1]
        ):
            print(f"    {strat}: {count}")
    print()

    # ── Guardrails ──
    print("── Guardrails ────────────────────────────────────────────────────")
    print(f"  R:R blocks: {summary.rr_blocks}")
    print(f"  Position-alignment blocks: {summary.align_blocks}")
    print()

    # ── SL slippage ──
    print("── SL slippage (loss > 1.5x SL distance) ─────────────────────────")
    if summary.slippage_trades:
        print(
            f"  {'ts':<22}{'symbol':<18}{'strategy':<16}{'sl%':>6}"
            f"{'loss%':>7}{'overshoot':>10}{'loss USDT':>11}"
        )
        print(f"  {'-' * 22}{'-' * 18}{'-' * 16}{'-' * 6}{'-' * 7}{'-' * 10}{'-' * 11}")
        for t in summary.slippage_trades:
            print(
                f"  {t['ts']:<22}{t['symbol']:<18}{t['strategy']:<16}"
                f"{t['sl_pct']:>6.2f}{t['loss_pct']:>7.2f}"
                f"{t['overshoot']:>9.1f}x{t['loss_usdt']:>+11.4f}"
            )
    else:
        print("  None detected.")
    print()

    # ── Stop-outs with peak excursion ──
    print("── Stop-outs with peak favorable excursion ─────────────────────")
    if summary.stopout_peak_trades:
        print(
            f"  {'ts':<22}{'symbol':<18}{'strategy':<16}{'loss USDT':>11}{'peak%':>8}"
        )
        print(f"  {'-' * 22}{'-' * 18}{'-' * 16}{'-' * 11}{'-' * 8}")
        for trade in sorted(
            summary.stopout_peak_trades,
            key=lambda item: (item["mfe_pct"], item["pnl_usdt"]),
            reverse=True,
        ):
            print(
                f"  {trade['ts']:<22}{trade['symbol']:<18}{trade['strategy']:<16}"
                f"{trade['pnl_usdt']:>+11.4f}{trade['mfe_pct']:>7.2f}%"
            )
    else:
        print("  None detected in the parsed logs.")
    print()

    # ── Screener ──
    if summary.screener_runs:
        print("── Screener (last run) ──────────────────────────────────────────")
        print(f"  Runs in logs: {summary.screener_runs}")
        print(f"  Last candidates: {summary.screener_last_candidates} / {summary.screener_last_tickers} tickers")
        print(f"  Last volume filter: {summary.screener_last_vol:,} USDT")
        if summary.screener_last_spread is not None:
            print(f"  Last spread filter: {summary.screener_last_spread:.2f}%")
        if summary.screener_last_dual:
            print(f"  Dual universe: {summary.screener_last_dual}")
        print()

    print("=" * 78)


def summary_to_dict(summary: Summary) -> dict:
    """Convert summary to a JSON-serialisable dict."""
    return {
        "files_parsed": summary.files_parsed,
        "period": {
            "start": summary.start_ts,
            "end": summary.end_ts,
        },
        "aggregate": {
            "total_trades": summary.total_trades,
            "wins": summary.wins,
            "losses": summary.losses,
            "win_rate_pct": round(summary.wins / summary.total_trades * 100, 2)
            if summary.total_trades
            else None,
            "net_pnl_usdt": round(summary.total_pnl, 4),
            "avg_win": round(summary.avg_win, 4),
            "avg_loss": round(summary.avg_loss, 4),
            "avg_per_trade": round(summary.total_pnl / summary.total_trades, 4)
            if summary.total_trades
            else None,
        },
        "by_strategy": {
            strat: {
                "signals": summary.strat_signals.get(strat, 0),
                "trades": summary.strat_trades[strat],
                "wins": summary.strat_wins[strat],
                "losses": summary.strat_losses[strat],
                "win_rate_pct": round(
                    summary.strat_wins[strat] / summary.strat_trades[strat] * 100, 2
                )
                if summary.strat_trades[strat]
                else None,
                "net_pnl_usdt": round(summary.strat_pnl[strat], 4),
            }
            for strat in sorted(summary.strat_trades.keys())
        },
        "by_symbol": {
            sym: {
                "trades": summary.sym_trades[sym],
                "wins": summary.sym_wins[sym],
                "losses": summary.sym_losses[sym],
                "net_pnl_usdt": round(summary.sym_pnl[sym], 4),
            }
            for sym in sorted(summary.sym_pnl.keys())
        },
        "trade_management": {
            "seeded_count": summary.seeded_count,
            "cleared_reasons": dict(summary.cleared_reasons),
            "cooldown_skips": summary.cooldown_skips,
            "cooldown_skips_by_strategy": dict(summary.cooldown_skips_by_strategy),
        },
        "guardrails": {
            "rr_blocks": summary.rr_blocks,
            "align_blocks": summary.align_blocks,
        },
        "sl_slippage": summary.slippage_trades,
        "stopout_peak_trades": summary.stopout_peak_trades,
        "screener": {
            "runs": summary.screener_runs,
            "last_candidates": summary.screener_last_candidates,
            "last_tickers": summary.screener_last_tickers,
            "last_volume_usd": summary.screener_last_vol,
            "last_spread_pct": summary.screener_last_spread,
            "last_dual": summary.screener_last_dual,
        },
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Summarise tai2 trading performance from runtime logs."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Log file(s) to parse. Default: all logs/app.log* (oldest first).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON instead of a text report.",
    )
    parser.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help=f"Log directory (default: {DEFAULT_LOG_DIR})",
    )
    args = parser.parse_args(argv)

    # Resolve files.
    if args.paths:
        files = [Path(p) for p in args.paths]
    else:
        log_dir = Path(args.log_dir)
        if not log_dir.exists():
            print(f"Log directory not found: {log_dir}", file=sys.stderr)
            return 1
        files = sorted(log_dir.glob("app.log*"), key=lambda p: p.name)

        def _sort_key(p: Path) -> tuple[int, str]:
            name = p.name
            if name == "app.log":
                return (99, name)
            try:
                idx = int(name.rsplit(".", 1)[1])
            except (ValueError, IndexError):
                idx = 0
            return (idx, name)

        files = sorted(files, key=_sort_key)

    if not files:
        print("No log files found.", file=sys.stderr)
        return 1

    pnl_trades, signals, seeded, cleared, summary = parse_logs(files)
    summary = build_summary(pnl_trades, signals, seeded, cleared, summary)

    if args.json:
        print(json.dumps(summary_to_dict(summary), indent=2))
    else:
        print_report(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
