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
from datetime import datetime
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
    r"(?: flipped=(True|False))?(?: flip_tp_sl=(True|False))?"
)

# Peak/trough excursion line emitted by the TradeMgmt supervision loop.
# Only the TradeMgmt lines carry trough (worst unfavorable) values; the
# Alternator trailing-close/profit lines use a *separate* peak tracker and
# have no trough fields, so we deliberately do NOT match those here.
_PEAK_EXCURSION_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*TradeMgmt: "
    r"([A-Z0-9-]+-USDT-SWAP).*peak_pct=([0-9.eE+-]+|None) current_pct=([0-9.eE+-]+|None) "
    r"peak_usd=([0-9.eE+-]+|None) current_usd=([0-9.eE+-]+|None)"
    r"(?: trough_pct=([0-9.eE+-]+|None) trough_usd=([0-9.eE+-]+|None))?"
)

# TradeMgmt cleared line:
#   ...TradeMgmt: SATS-USDT-SWAP cleared (time_stop); re-entry cooldown 1800s
_CLEARED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*TradeMgmt: "
    r"([A-Z0-9-]+-USDT-SWAP) cleared \((\w+)\)"
)

# TradeMgmt seeded line (tp/sl/risk_pct may be None on auto-seeded entries):
#   ...TradeMgmt: seeded SATS-USDT-SWAP side=long entry=... tp=... sl=... risk_pct=...
_SEEDED_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*TradeMgmt: seeded "
    r"([A-Z0-9-]+-USDT-SWAP) side=(\w+) entry=([0-9.eE+-]+) "
    r"tp=([0-9.eE+-]+|None) sl=([0-9.eE+-]+|None) risk_pct=([0-9.]+|None)"
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

# Entry (taker) fee line, back-filled by the fill reconciler:
#   ...Stored entry fee for USELESS-USDT-SWAP: 0.0092 USDT (fill 103262024, trade 00fe...)
# NOTE: the same trade id can emit this line on multiple reconcile passes, so
# we deduplicate on trade id (not fill id) when summing fees.
_ENTRY_FEE_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Stored entry fee for "
    r"([A-Z0-9-]+-USDT-SWAP): ([0-9.]+) USDT "
    r"\(fill (\d+), trade ([0-9a-f-]+)\)"
)

# Flat-account equity mark (Shotgun baseline, only set when no positions open):
#   ...Shotgun: baseline equity set to 100.7295 USDT
_EQUITY_FLAT_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*Shotgun: baseline equity set to ([0-9.]+) USDT"
)

# Mark-to-market equity captured in the 51008 bootstrap diagnostics.
# Use ``account_equity_before_cap`` (the TOTAL USDT equity, before the per-symbol
# isolated seed cap is applied).  The plain ``account_equity=`` field in the
# "bootstrap path" line is a reduced/per-symbol value and must NOT be used.
#   ..."account_equity_before_cap": 100.72752204765146, ...
_EQUITY_MARK_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*account_equity_before_cap\": ([0-9.]+)"
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
    mae_trough_pct: Optional[float] = None
    mae_trough_usd: Optional[float] = None


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
    flipped: bool = False
    flip_tp_sl: bool = False


@dataclass
class SeededEntry:
    ts: str
    symbol: str
    side: str
    entry: float
    tp: Optional[float]
    sl: Optional[float]
    risk_pct: Optional[float]


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
    # Flipped-direction trades (experimental flip_launcher_direction)
    flipped_signals: int = 0
    flipped_trades: int = 0
    flipped_wins: int = 0
    flipped_losses: int = 0
    flipped_pnl: float = 0.0
    flip_tp_sl_trades: int = 0
    flip_tp_sl_pnl: float = 0.0
    # SL slippage
    slippage_trades: list[dict] = field(default_factory=list)
    # Losing trades with observed peak favorable excursion
    stopout_peak_trades: list[dict] = field(default_factory=list)
    # Winning trades with observed trough unfavorable excursion
    tp_trough_trades: list[dict] = field(default_factory=list)
    # Screener
    screener_runs: int = 0
    screener_last_candidates: int = 0
    screener_last_tickers: int = 0
    screener_last_vol: int = 0
    screener_last_spread: Optional[float] = None
    screener_last_dual: Optional[str] = None
    # Edge / exit-quality metrics
    profit_factor: Optional[float] = None
    payoff_ratio: Optional[float] = None
    avg_mfe_pct: float = 0.0
    avg_mae_pct: float = 0.0
    winners_avg_giveback_pct: Optional[float] = None
    # Reconciliation gaps (seeds ↔ clears ↔ reconciled PnL)
    open_positions: list[dict] = field(default_factory=list)
    missing_pnl: list[dict] = field(default_factory=list)
    # Fees
    total_entry_fees: float = 0.0
    entry_fee_count: int = 0
    avg_entry_fee: float = 0.0
    # Equity & drawdown
    equity_marks: list[dict] = field(default_factory=list)
    daily_pnl: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    realized_max_drawdown_usdt: float = 0.0
    marked_peak_equity: Optional[float] = None
    marked_trough_equity: Optional[float] = None
    marked_max_drawdown_usdt: Optional[float] = None
    marked_max_drawdown_pct: Optional[float] = None
    # Attribution confidence / signal→trade funnel
    no_signal_trades: int = 0
    strat_signals_no_trade: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    # Hold duration (seed→clear, minutes)
    hold_durations_min: list[float] = field(default_factory=list)
    # Exit-type PnL attribution
    exit_pnl_by_reason: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    exit_trades_by_reason: dict[str, int] = field(default_factory=lambda: defaultdict(int))
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
    # Per-symbol peak/trough excursion for the CURRENT trade window.  Reset on
    # each ``TradeMgmt: seeded`` event (a fresh trade = a fresh window), NOT on
    # the Launcher signal — a symbol can run several trades
    # (seed → clear → seed → clear) with no intervening signal, so resetting on
    # signal would leak one trade's excursion into the next.
    excursion: dict[str, dict[str, Optional[float]]] = {}
    # A reconciled PnL trade whose ``cleared`` event hasn't been seen yet.
    # The reconcile can arrive just before cleared, so we hold it and back-fill
    # the final excursion when cleared fires.
    open_trade_by_symbol: dict[str, PnLTrade] = {}
    # Final excursion of a just-cleared trade whose reconcile hasn't arrived
    # yet (cleared can precede reconcile by ~1s).  Consumed by the next
    # reconcile for that symbol.
    closed_excursion_by_symbol: dict[str, dict[str, Optional[float]]] = {}
    # Deduplicate realized PnL by OKX fill id: the same closing fill is often
    # reconciled against multiple unreconciled trades, so only the FIRST
    # occurrence of each fill id should count as a real trade.
    seen_fills: set[str] = set()
    # Deduplicate entry fees by trade id (a single entry can emit the
    # "Stored entry fee" line on multiple reconcile passes).
    seen_fee_trades: set[str] = set()

    def _parse_optional_float(raw: Optional[str]) -> Optional[float]:
        if raw is None or raw == "None":
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _apply_excursion(trade: PnLTrade, exc: dict[str, Optional[float]]) -> None:
        """Back-fill a PnL trade with the final peak/trough excursion values."""
        trade.mfe_peak_pct = exc.get("peak_pct")
        trade.mfe_peak_usd = exc.get("peak_usd")
        trade.mae_trough_pct = exc.get("trough_pct")
        trade.mae_trough_usd = exc.get("trough_usd")

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
                trade = PnLTrade(
                    ts=m.group(1),
                    symbol=symbol,
                    pnl=float(m.group(3)),
                    fill_id=fill_id,
                )
                # If the trade already closed (cleared fired first), its final
                # excursion is buffered — back-fill it now.
                closed = closed_excursion_by_symbol.pop(symbol, None)
                if closed is not None:
                    _apply_excursion(trade, closed)
                else:
                    # Reconcile arrived before cleared: hold it and back-fill
                    # the final excursion when cleared fires.
                    open_trade_by_symbol[symbol] = trade
                pnl_trades.append(trade)
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
                    flipped=m.group(9) == "True",
                    flip_tp_sl=m.group(10) == "True",
                )
                signals.append(sig)
                _track_period(sig.ts)
                summary.strat_signals[sig.strategy] += 1
                if sig.flipped:
                    summary.flipped_signals += 1
                continue
            # Peak favorable excursion (and trough unfavorable excursion)
            m = _PEAK_EXCURSION_RE.search(line)
            if m:
                symbol = m.group(2)
                _track_period(m.group(1))
                peak_pct = _parse_optional_float(m.group(3))
                peak_usd = _parse_optional_float(m.group(5))
                trough_pct = _parse_optional_float(m.group(7))
                trough_usd = _parse_optional_float(m.group(8))
                exc = excursion.setdefault(
                    symbol,
                    {
                        "peak_pct": None,
                        "peak_usd": None,
                        "trough_pct": None,
                        "trough_usd": None,
                    },
                )
                if peak_pct is not None:
                    cur = exc["peak_pct"]
                    exc["peak_pct"] = peak_pct if cur is None else max(cur, peak_pct)
                if peak_usd is not None:
                    cur = exc["peak_usd"]
                    exc["peak_usd"] = peak_usd if cur is None else max(cur, peak_usd)
                if trough_pct is not None:
                    cur = exc["trough_pct"]
                    exc["trough_pct"] = trough_pct if cur is None else min(cur, trough_pct)
                if trough_usd is not None:
                    cur = exc["trough_usd"]
                    exc["trough_usd"] = trough_usd if cur is None else min(cur, trough_usd)
                continue
            # Seeded
            m = _SEEDED_RE.search(line)
            if m:
                symbol = m.group(2)
                seeded.append(
                    SeededEntry(
                        ts=m.group(1),
                        symbol=symbol,
                        side=m.group(3),
                        entry=float(m.group(4)),
                        tp=_parse_optional_float(m.group(5)),
                        sl=_parse_optional_float(m.group(6)),
                        risk_pct=_parse_optional_float(m.group(7)),
                    )
                )
                _track_period(m.group(1))
                summary.seeded_count += 1
                # A new trade begins: reset the excursion window and drop any
                # pending open/closed trade state for this symbol.
                excursion.pop(symbol, None)
                open_trade_by_symbol.pop(symbol, None)
                closed_excursion_by_symbol.pop(symbol, None)
                continue
            # Cleared
            m = _CLEARED_RE.search(line)
            if m:
                reason = m.group(3)
                symbol = m.group(2)
                cleared.append(ClearedEntry(ts=m.group(1), symbol=symbol, reason=reason))
                _track_period(m.group(1))
                summary.cleared_reasons[reason] += 1
                # The trade closes here: freeze the final excursion and either
                # back-fill an already-reconciled PnL or buffer it for the
                # reconcile that is about to arrive.
                exc = excursion.get(symbol)
                final_exc = dict(exc) if exc is not None else None
                open_trade = open_trade_by_symbol.pop(symbol, None)
                if open_trade is not None and final_exc is not None:
                    _apply_excursion(open_trade, final_exc)
                elif final_exc is not None:
                    closed_excursion_by_symbol[symbol] = final_exc
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
                continue
            # Entry (taker) fee
            m = _ENTRY_FEE_RE.search(line)
            if m:
                trade_id = m.group(5)
                if trade_id in seen_fee_trades:
                    continue
                seen_fee_trades.add(trade_id)
                fee = float(m.group(3))
                summary.total_entry_fees += fee
                summary.entry_fee_count += 1
                _track_period(m.group(1))
                continue
            # Flat-account equity mark (Shotgun baseline, only when flat)
            m = _EQUITY_FLAT_RE.search(line)
            if m:
                summary.equity_marks.append(
                    {"ts": m.group(1), "equity": float(m.group(2)), "kind": "flat"}
                )
                continue
            # Mark-to-market equity from bootstrap diagnostics (also during positions)
            m = _EQUITY_MARK_RE.search(line)
            if m:
                summary.equity_marks.append(
                    {"ts": m.group(1), "equity": float(m.group(2)), "kind": "mark"}
                )
                continue

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

        if best_seed is not None and best_seed.entry > 0 and best_seed.sl is not None:
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


def _compute_reconciliation_gaps(
    seeded: list[SeededEntry],
    cleared: list[ClearedEntry],
    pnl_trades: list[PnLTrade],
) -> tuple[list[dict], list[dict]]:
    """Reconcile seed→clear→PnL counts per symbol.

    Returns two lists:

    * ``open_positions`` — seeds with no subsequent clear event.  These are
      still open at the end of the parsed period (or a seed fired just past
      the last clear captured in the logs).

    * ``missing_pnl`` — the *count* of non-``flat`` closes with no matching
      ``Reconciled PnL``.  Reported per-symbol as ``{symbol, cleared, pnl,
      shortfall}`` rather than per-event, because the OKX fill reconciler runs
      on its own poll cadence and can record a ``Reconciled PnL`` line long
      after the ``TradeMgmt: cleared`` event (and even into a later log
      rotation), so timestamp pairing is unreliable.

    ``flat`` closes legitimately produce no PnL fill and are excluded.
    """
    seeds_by_symbol: dict[str, list[str]] = defaultdict(list)
    for s in seeded:
        seeds_by_symbol[s.symbol].append(s.ts)
    clears_by_symbol: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for c in cleared:
        clears_by_symbol[c.symbol].append((c.ts, c.reason))
    pnls_by_symbol: dict[str, list[str]] = defaultdict(list)
    for p in pnl_trades:
        pnls_by_symbol[p.symbol].append(p.ts)

    symbols = set(seeds_by_symbol) | set(clears_by_symbol) | set(pnls_by_symbol)
    open_positions: list[dict] = []
    missing_pnl: list[dict] = []

    for sym in sorted(symbols):
        seeds = sorted(seeds_by_symbol[sym])
        clears = sorted(clears_by_symbol[sym])
        pnls = sorted(pnls_by_symbol[sym])

        # 1. Open positions: a seed that no clear follows.  Since seeds and
        #    clears interleave chronologically per symbol, every clear consumes
        #    one *earlier* seed; any trailing un-consumed seed is still open.
        if len(seeds) > len(clears):
            # The ``len(seeds) - len(clears)`` most recent seeds are still open.
            for sts in seeds[len(clears):]:
                open_positions.append({"symbol": sym, "seeded": sts})

        # 2. Closes that never produced a reconciled PnL.  ``flat`` closes are
        #    excluded (position netted to zero, no PnL fill is emitted).
        non_flat_clears = sum(1 for _, r in clears if r != "flat")
        shortfall = non_flat_clears - len(pnls)
        if shortfall > 0:
            missing_pnl.append(
                {
                    "symbol": sym,
                    "cleared": non_flat_clears,
                    "pnl": len(pnls),
                    "shortfall": shortfall,
                }
            )

    return open_positions, missing_pnl


_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _ts_dt(ts: str) -> datetime:
    """Parse a log timestamp ("YYYY-MM-DD HH:MM:SS") into a datetime."""
    return datetime.strptime(ts, _TS_FMT)


def _compute_lifecycle(
    seeded: list[SeededEntry],
    cleared: list[ClearedEntry],
    pnl_trades: list[PnLTrade],
    summary: Summary,
) -> None:
    """Populate hold durations and exit-type PnL attribution.

    * Hold duration = minutes from the earliest prior seed to each clear.
    * Exit-type PnL = attribute each reconciled PnL to the most recent prior
      cleared reason for the same symbol (consistent with strategy attribution).
    """
    seed_ts_by_symbol: dict[str, list[datetime]] = defaultdict(list)
    for s in seeded:
        seed_ts_by_symbol[s.symbol].append(_ts_dt(s.ts))
    clear_by_symbol: dict[str, list[ClearedEntry]] = defaultdict(list)
    for c in cleared:
        clear_by_symbol[c.symbol].append(c)

    # Hold durations: greedy seed→clear pairing (earliest clear at/after seed).
    for sym, seed_dts in seed_ts_by_symbol.items():
        clears = sorted(clear_by_symbol.get(sym, []), key=lambda c: c.ts)
        ci = 0
        for sdt in sorted(seed_dts):
            while ci < len(clears) and _ts_dt(clears[ci].ts) < sdt:
                ci += 1
            if ci < len(clears):
                dur_min = (_ts_dt(clears[ci].ts) - sdt).total_seconds() / 60.0
                if 0.0 <= dur_min <= 7 * 24 * 60:  # sanity: < 7 days
                    summary.hold_durations_min.append(dur_min)
                ci += 1

    # Exit-type PnL: most recent prior clear reason per symbol.
    for trade in pnl_trades:
        reason = "unattributed"
        tdt = _ts_dt(trade.ts)
        best: Optional[ClearedEntry] = None
        for c in clear_by_symbol.get(trade.symbol, []):
            if _ts_dt(c.ts) <= tdt and (best is None or c.ts > best.ts):
                best = c
        if best is not None:
            reason = best.reason
        summary.exit_pnl_by_reason[reason] += trade.pnl
        summary.exit_trades_by_reason[reason] += 1


def _compute_equity_stats(
    equity_marks: list[dict], summary: Summary
) -> None:
    """Compute marked-peak/trough and max drawdown from the equity series.

    Drawdown is measured on the full mark series (flat + mark-to-market) in
    chronological order; a run of flat-only marks would give a flat-to-flat
    curve.  Missing-mark gaps (positions open) simply contribute no points, so
    drawdown is a lower bound on the true intra-trade excursion.
    """
    if not equity_marks:
        return
    marks = sorted(equity_marks, key=lambda e: e["ts"])
    peak = marks[0]["equity"]
    summary.marked_peak_equity = peak
    summary.marked_trough_equity = peak
    max_dd_usd = 0.0
    for e in marks:
        eq = e["equity"]
        if eq > summary.marked_peak_equity:
            summary.marked_peak_equity = eq
        if eq < summary.marked_trough_equity:
            summary.marked_trough_equity = eq
        dd = summary.marked_peak_equity - eq
        if dd > max_dd_usd:
            max_dd_usd = dd
    summary.marked_max_drawdown_usdt = max_dd_usd
    if summary.marked_peak_equity > 0:
        summary.marked_max_drawdown_pct = (
            max_dd_usd / summary.marked_peak_equity * 100.0
        )


def _compute_daily_pnl(
    pnl_trades: list[PnLTrade], summary: Summary
) -> None:
    """Group realized PnL by calendar date."""
    for trade in pnl_trades:
        day = trade.ts[:10]
        summary.daily_pnl[day] += trade.pnl


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
    all_mfe_pct: list[float] = []
    all_mae_pct: list[float] = []
    winner_giveback: list[float] = []

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
        best_sig = _most_recent_signal(trade.symbol, trade.ts, signals)
        if best_sig is None:
            summary.no_signal_trades += 1
        if best_sig is not None and best_sig.flipped:
            summary.flipped_trades += 1
            summary.flipped_pnl += trade.pnl
            if trade.pnl > 0:
                summary.flipped_wins += 1
            else:
                summary.flipped_losses += 1
            if best_sig.flip_tp_sl:
                summary.flip_tp_sl_trades += 1
                summary.flip_tp_sl_pnl += trade.pnl
        # Precompute normalized MFE/MAE once per trade (in % of notional) and
        # accumulate for the aggregate exit-quality section.
        notional = best_sig.notional if best_sig is not None else 0.0
        mfe_pct = trade.mfe_peak_pct
        if mfe_pct is None and trade.mfe_peak_usd is not None and notional > 0:
            mfe_pct = trade.mfe_peak_usd / notional * 100.0
        mae_pct = trade.mae_trough_pct
        if mae_pct is None and trade.mae_trough_usd is not None and notional > 0:
            mae_pct = trade.mae_trough_usd / notional * 100.0
        if mfe_pct is not None:
            all_mfe_pct.append(mfe_pct)
        if mae_pct is not None:
            all_mae_pct.append(mae_pct)
        if trade.pnl > 0:
            summary.strat_wins[strat] += 1
            # Take-profits with trough unfavorable excursion: flag winning
            # trades that were once deep underwater (nearly stopped out before
            # recovering to TP).  Mirrors the stop-out peak tracking below.
            if mae_pct is not None and mae_pct < 0:
                summary.tp_trough_trades.append(
                    {
                        "ts": trade.ts,
                        "symbol": trade.symbol,
                        "strategy": strat,
                        "pnl_usdt": round(trade.pnl, 4),
                        "mae_pct": round(mae_pct, 2),
                        "mae_usd": round(trade.mae_trough_usd, 4)
                        if trade.mae_trough_usd is not None
                        else None,
                    }
                )
            # Winners give-back: share of their peak profit returned before
            # closing.  High give-back = TP set too far above where price
            # reverses, so the trade banks only a fraction of its move.
            if mfe_pct is not None and notional > 0:
                realized_pct = trade.pnl / notional * 100.0
                if mfe_pct > realized_pct:
                    winner_giveback.append((mfe_pct - realized_pct) / mfe_pct * 100.0)
        else:
            summary.strat_losses[strat] += 1
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
    if summary.entry_fee_count:
        summary.avg_entry_fee = summary.total_entry_fees / summary.entry_fee_count

    # Signal → trade funnel: signals that never produced an attributed trade.
    for strat, sig_count in summary.strat_signals.items():
        summary.strat_signals_no_trade[strat] = max(
            0, sig_count - summary.strat_trades.get(strat, 0)
        )

    # Hold duration + exit-type PnL.
    _compute_lifecycle(seeded, cleared, pnl_trades, summary)
    # Equity curve & drawdown.
    _compute_equity_stats(summary.equity_marks, summary)
    summary.equity_marks.sort(key=lambda e: e["ts"])
    # Daily PnL.
    _compute_daily_pnl(pnl_trades, summary)

    # Edge / exit-quality metrics.
    gross_profit = sum(win_pnls)
    gross_loss = abs(sum(loss_pnls))
    if gross_loss > 0:
        summary.profit_factor = gross_profit / gross_loss
    if summary.avg_loss < 0:
        summary.payoff_ratio = summary.avg_win / abs(summary.avg_loss)
    summary.avg_mfe_pct = sum(all_mfe_pct) / len(all_mfe_pct) if all_mfe_pct else 0.0
    summary.avg_mae_pct = sum(all_mae_pct) / len(all_mae_pct) if all_mae_pct else 0.0
    if winner_giveback:
        summary.winners_avg_giveback_pct = sum(winner_giveback) / len(winner_giveback)

    # Reconciliation gaps (seeds ↔ clears ↔ reconciled PnL).
    summary.open_positions, summary.missing_pnl = _compute_reconciliation_gaps(
        seeded, cleared, pnl_trades
    )
    return summary


# ── Reporting ────────────────────────────────────────────────────────────────
def _fmt_pct(val: float, total: int) -> str:
    return f"{val / total * 100:.1f}%" if total else "n/a"


def _median(values: list[float]) -> Optional[float]:
    """Return the median of a list, or None if empty."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    if n % 2:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


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

    # ── Fees ──
    print("── Fees & net PnL ──────────────────────────────────────────────")
    net_pnl = summary.total_pnl - summary.total_entry_fees
    print(
        f"  Realized PnL (net of close fee): {summary.total_pnl:+.4f} USDT"
    )
    print(
        f"  Entry (taker) fees: {summary.total_entry_fees:.4f} USDT "
        f"({summary.entry_fee_count} legs, avg {summary.avg_entry_fee:.4f})"
    )
    print(f"  PnL after entry fees: {net_pnl:+.4f} USDT")
    print()

    # ── Edge & exit quality ──
    print("── Edge & exit quality ────────────────────────────────────────")
    pf = "n/a" if summary.profit_factor is None else f"{summary.profit_factor:.2f}"
    pay = "n/a" if summary.payoff_ratio is None else f"{summary.payoff_ratio:.2f}"
    print(
        f"  Profit factor (gross profit / gross loss): {pf}    "
        f"Payoff ratio (avg win / |avg loss|): {pay}"
    )
    print(
        f"  Avg MFE (peak favorable excursion): {summary.avg_mfe_pct:+.2f}%   "
        f"Avg MAE (worst adverse excursion): {summary.avg_mae_pct:+.2f}%"
    )
    if summary.winners_avg_giveback_pct is not None:
        print(
            f"  Winners avg give-back of peak profit: "
            f"{summary.winners_avg_giveback_pct:.1f}%"
        )
    else:
        print("  Winners avg give-back of peak profit: n/a")
    print()

    # ── Per-strategy ──
    print("── By strategy (heuristic attribution) ───────────────────────────")
    print(
        f"  {'strategy':<20}{'signals':>9}{'trades':>8}{'conv%':>7}{'W':>5}"
        f"{'L':>5}{'win%':>7}{'net PnL':>11}{'avg':>9}"
    )
    print(
        f"  {'-' * 20}{'-' * 9}{'-' * 8}{'-' * 7}{'-' * 5}{'-' * 5}"
        f"{'-' * 7}{'-' * 11}{'-' * 9}"
    )
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
        conv = n / sig * 100 if sig else 0.0
        print(
            f"  {strat:<20}{sig:>9}{n:>8}{conv:>6.1f}%{w:>5}{l:>5}{wr:>6.1f}%"
            f"{pnl:>+11.4f}{avg:>+9.4f}"
        )
    print()

    # ── Funnel & attribution ──
    print("── Signal → trade funnel ─────────────────────────────────────────")
    print(
        f"  Signals without an attributed trade (blocked/skipped/no-fill), by strategy:"
    )
    for strat, cnt in sorted(
        summary.strat_signals_no_trade.items(), key=lambda x: -x[1]
    ):
        sig = summary.strat_signals.get(strat, 0)
        if sig:
            print(
                f"    {strat:<20} {cnt:>4} of {sig:>4} signals unconverted "
                f"({cnt / sig * 100:>4.1f}%)"
            )
    print(
        f"  Trades with no prior signal (auto-seeded / non-launcher): "
        f"{summary.no_signal_trades}"
    )
    print()

    # ── Hold duration ──
    print("── Hold duration (seed → clear) ────────────────────────────────")
    if summary.hold_durations_min:
        mean = sum(summary.hold_durations_min) / len(summary.hold_durations_min)
        med = _median(summary.hold_durations_min)
        mx = max(summary.hold_durations_min)
        med_str = f"{med:.1f}" if med is not None else "n/a"
        print(
            f"  n={len(summary.hold_durations_min)}   mean={mean:.1f}m   "
            f"median={med_str}m   max={mx:.1f}m"
        )
        print(
            f"  (trend_pullback analyses on 1H candles — mean hold < 60m suggests "
            f"the SL cuts the thesis short)"
        )
    else:
        print("  No paired seed→clear events found.")
    print()

    # ── Exit-type PnL ──
    print("── PnL by exit reason ───────────────────────────────────────────")
    if summary.exit_pnl_by_reason:
        print(f"  {'reason':<20}{'trades':>8}{'net PnL':>11}")
        print(f"  {'-' * 20}{'-' * 8}{'-' * 11}")
        for reason, pnl in sorted(
            summary.exit_pnl_by_reason.items(), key=lambda x: x[1]
        ):
            n = summary.exit_trades_by_reason[reason]
            print(f"  {reason:<20}{n:>8}{pnl:>+11.4f}")
    else:
        print("  No exit attribution available.")
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

    # ── Reconciliation & data integrity ──
    print("── Reconciliation & data integrity ───────────────────────────")
    cleared_total = sum(summary.cleared_reasons.values())
    print(
        f"  Seeded: {summary.seeded_count}   Cleared: {cleared_total}   "
        f"Reconciled PnL: {summary.total_trades}   "
        f"Flat closes (no PnL expected): {summary.cleared_reasons.get('flat', 0)}"
    )
    print(f"  Positions still open (seed without clear): {len(summary.open_positions)}")
    for p in summary.open_positions:
        print(f"    {p['symbol']}  seeded {p['seeded']}")
    print(f"  Symbols with closes missing reconciled PnL: {len(summary.missing_pnl)}")
    for p in summary.missing_pnl:
        print(
            f"    {p['symbol']}  cleared {p['cleared']}  PnL {p['pnl']}  "
            f"shortfall {p['shortfall']}"
        )
    print()

    # ── Guardrails ──
    print("── Guardrails ────────────────────────────────────────────────────")
    print(f"  R:R blocks: {summary.rr_blocks}")
    print(f"  Position-alignment blocks: {summary.align_blocks}")
    print()

    # ── Flipped trades ──
    print("── Flipped-direction trades (experimental) ─────────────────────")
    print(f"  Flipped signals: {summary.flipped_signals}")
    print(
        f"  Flipped trades: {summary.flipped_trades}   "
        f"Wins: {summary.flipped_wins}   Losses: {summary.flipped_losses}   "
        f"Win rate: {_fmt_pct(summary.flipped_wins, summary.flipped_trades)}   "
        f"Net PnL: {summary.flipped_pnl:+.4f} USDT"
    )
    print(
        f"  flip_tp_sl trades: {summary.flip_tp_sl_trades}   "
        f"Net PnL: {summary.flip_tp_sl_pnl:+.4f} USDT"
    )
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

    # ── Take-profits with trough excursion ──
    print("── Take-profits with trough unfavorable excursion ──────────────")
    if summary.tp_trough_trades:
        print(
            f"  {'ts':<22}{'symbol':<18}{'strategy':<16}{'profit USDT':>12}{'trough%':>9}"
        )
        print(f"  {'-' * 22}{'-' * 18}{'-' * 16}{'-' * 12}{'-' * 9}")
        for trade in sorted(
            summary.tp_trough_trades,
            key=lambda item: (item["mae_pct"], item["pnl_usdt"]),
        ):
            print(
                f"  {trade['ts']:<22}{trade['symbol']:<18}{trade['strategy']:<16}"
                f"{trade['pnl_usdt']:>+12.4f}{trade['mae_pct']:>8.2f}%"
            )
    else:
        print("  None detected in the parsed logs.")
    print()

    # ── Equity & drawdown ──
    print("── Equity & drawdown (marked) ──────────────────────────────────")
    if summary.equity_marks:
        first = summary.equity_marks[0]["equity"]
        last = summary.equity_marks[-1]["equity"]
        print(
            f"  Marks: {len(summary.equity_marks)}   first={first:.4f}   "
            f"last={last:.4f}   Δ={last - first:+.4f} USDT"
        )
        if summary.marked_peak_equity is not None:
            print(f"  Peak equity: {summary.marked_peak_equity:.4f} USDT")
        if summary.marked_trough_equity is not None:
            print(f"  Trough equity: {summary.marked_trough_equity:.4f} USDT")
        if summary.marked_max_drawdown_usdt is not None:
            dd = summary.marked_max_drawdown_usdt
            ddp = summary.marked_max_drawdown_pct
            ddp_s = f" ({ddp:.2f}%)" if ddp is not None else ""
            print(f"  Max drawdown (marked): {dd:.4f} USDT{ddp_s}")
    else:
        print("  No equity marks found in logs (Shotgun disabled / no trades).")
    print()

    # ── Daily PnL ──
    print("── Daily realized PnL ───────────────────────────────────────────")
    if summary.daily_pnl:
        for day, pnl in sorted(summary.daily_pnl.items()):
            print(f"  {day}: {pnl:+.4f} USDT")
    else:
        print("  No realized PnL to bucket.")
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
        "fees": {
            "realized_pnl_net_close_fee": round(summary.total_pnl, 4),
            "entry_fees": round(summary.total_entry_fees, 4),
            "entry_fee_legs": summary.entry_fee_count,
            "avg_entry_fee": round(summary.avg_entry_fee, 4),
            "net_pnl_after_entry_fees": round(
                summary.total_pnl - summary.total_entry_fees, 4
            ),
        },
        "by_strategy": {
            strat: {
                "signals": summary.strat_signals.get(strat, 0),
                "trades": summary.strat_trades[strat],
                "signals_no_trade": summary.strat_signals_no_trade.get(strat, 0),
                "conversion_pct": round(
                    summary.strat_trades[strat] / summary.strat_signals[strat] * 100, 2
                )
                if summary.strat_signals.get(strat, 0)
                else None,
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
        "attribution": {
            "no_signal_trades": summary.no_signal_trades,
        },
        "hold_duration": {
            "count": len(summary.hold_durations_min),
            "mean_min": round(
                sum(summary.hold_durations_min) / len(summary.hold_durations_min), 2
            )
            if summary.hold_durations_min
            else None,
            "median_min": round(_median(summary.hold_durations_min), 2)
            if summary.hold_durations_min
            else None,
            "max_min": round(max(summary.hold_durations_min), 2)
            if summary.hold_durations_min
            else None,
        },
        "exit_pnl_by_reason": {
            reason: {
                "trades": summary.exit_trades_by_reason[reason],
                "net_pnl_usdt": round(pnl, 4),
            }
            for reason, pnl in sorted(summary.exit_pnl_by_reason.items())
        },
        "equity": {
            "marks": len(summary.equity_marks),
            "first": round(summary.equity_marks[0]["equity"], 4)
            if summary.equity_marks
            else None,
            "last": round(summary.equity_marks[-1]["equity"], 4)
            if summary.equity_marks
            else None,
            "peak": round(summary.marked_peak_equity, 4)
            if summary.marked_peak_equity is not None
            else None,
            "trough": round(summary.marked_trough_equity, 4)
            if summary.marked_trough_equity is not None
            else None,
            "max_drawdown_usdt": round(summary.marked_max_drawdown_usdt, 4)
            if summary.marked_max_drawdown_usdt is not None
            else None,
            "max_drawdown_pct": round(summary.marked_max_drawdown_pct, 3)
            if summary.marked_max_drawdown_pct is not None
            else None,
        },
        "daily_pnl": {
            day: round(pnl, 4) for day, pnl in sorted(summary.daily_pnl.items())
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
        "flipped": {
            "signals": summary.flipped_signals,
            "trades": summary.flipped_trades,
            "wins": summary.flipped_wins,
            "losses": summary.flipped_losses,
            "win_rate_pct": round(summary.flipped_wins / summary.flipped_trades * 100, 2)
            if summary.flipped_trades
            else None,
            "net_pnl_usdt": round(summary.flipped_pnl, 4),
            "flip_tp_sl_trades": summary.flip_tp_sl_trades,
            "flip_tp_sl_net_pnl_usdt": round(summary.flip_tp_sl_pnl, 4),
        },
        "edge": {
            "profit_factor": round(summary.profit_factor, 3)
            if summary.profit_factor is not None
            else None,
            "payoff_ratio": round(summary.payoff_ratio, 3)
            if summary.payoff_ratio is not None
            else None,
            "avg_mfe_pct": round(summary.avg_mfe_pct, 3),
            "avg_mae_pct": round(summary.avg_mae_pct, 3),
            "winners_avg_giveback_pct": round(summary.winners_avg_giveback_pct, 2)
            if summary.winners_avg_giveback_pct is not None
            else None,
        },
        "reconciliation": {
            "seeded": summary.seeded_count,
            "cleared": sum(summary.cleared_reasons.values()),
            "flat_closes": summary.cleared_reasons.get("flat", 0),
            "reconciled_pnl": summary.total_trades,
            "open_positions": summary.open_positions,
            "closed_without_pnl": summary.missing_pnl,
        },
        "sl_slippage": summary.slippage_trades,
        "stopout_peak_trades": summary.stopout_peak_trades,
        "tp_trough_trades": summary.tp_trough_trades,
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
