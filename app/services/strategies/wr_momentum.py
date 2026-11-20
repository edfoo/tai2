"""Momentum measures for the wick_rejection momentum entry mode (plan §3.8).

Pure functions shared by live and the backtest:

- live feeds ``compute_momentum`` the websocket ``trades`` tape it records
  during the watch window (``MarketService._wr_tape``);
- the backtest feeds it a synthetic tape built from 1m candles
  (``tape_from_candles``) — the "1m approximation": minute-resolution
  returns, ``secs_since_high`` to within a candle, no buy share.

The measures reach the strategy as ``sym_data["wr_momentum"]``
(``attach_momentum``); ``WickRejectionStrategy`` applies
``momentum_entry_ok`` there, so both paths gate identically.

A tape is a time-ordered sequence of ``{"ts": ms, "px": float, "sz": float,
"side": "buy"|"sell"|""}``.
"""

from __future__ import annotations

import bisect
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Sequence

ENTRY_MODES = ("clock", "momentum", "momentum_or_clock")

# Sentinel for "momentum evaluation, but no usable tape" (gap): the strategy
# then vetoes instead of applying the clock rules.
MOMENTUM_MISSING = object()


@dataclass(frozen=True, slots=True)
class MomentumMeasures:
    """§3.8 measures at ``now_ms``; ``None`` when the tape is too short."""

    now_ms: int
    last_px: float
    ret_30s: float | None
    ret_60s: float | None
    ret_120s: float | None
    accel_60s: float | None
    high_px: float
    secs_since_high: float
    below_high_pct: float
    buy_share_60s: float | None
    rate_ratio_60s: float | None
    trades: int
    tape_start_ms: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MomentumMeasures":
        return cls(**{k: data.get(k) for k in cls.__dataclass_fields__})  # type: ignore[arg-type]

    def describe(self) -> str:
        def _p(v: float | None) -> str:
            return "n/a" if v is None else f"{v:+.2f}%"

        share = "n/a" if self.buy_share_60s is None else f"{self.buy_share_60s:.2f}"
        rate = "n/a" if self.rate_ratio_60s is None else f"{self.rate_ratio_60s:.2f}"
        return (
            f"px {self.last_px:g}, high {self.high_px:g} {self.secs_since_high:.0f}s ago "
            f"({self.below_high_pct:.2f}% below), ret30 {_p(self.ret_30s)} "
            f"ret60 {_p(self.ret_60s)} ret120 {_p(self.ret_120s)} "
            f"accel60 {_p(self.accel_60s)}, buy share {share}, rate {rate}"
        )


def _px(row: Any) -> float | None:
    try:
        v = float(row["px"])
    except (KeyError, TypeError, ValueError):
        return None
    return v if v > 0 else None


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b <= 0:
        return None
    return (a / b - 1.0) * 100.0


def compute_momentum(
    tape: Sequence[dict[str, Any]], now_ms: int, window_s: float
) -> MomentumMeasures | None:
    """Measures over the trades with ``now − window_s < ts <= now``.

    ``ret_N`` compares the last price with the last trade at or before
    ``now − N s`` (``None`` when the tape does not reach that far back).
    ``secs_since_high`` counts from the *first* trade at the window high, so
    a later equal print does not reset "stopped making new highs".
    ``rate_ratio_60s`` = trades in the last 60 s ÷ the window's average per
    60 s.  Returns ``None`` when no priced trade is in the window.
    """
    rows = [r for r in tape if _px(r) is not None and r.get("ts") is not None]
    if not rows:
        return None
    ts = [int(r["ts"]) for r in rows]
    # Up to and including now.
    end = bisect.bisect_right(ts, int(now_ms))
    start_ms = int(now_ms - window_s * 1000.0)
    begin = bisect.bisect_right(ts, start_ms)
    window = rows[begin:end]
    if not window:
        return None
    w_ts = ts[begin:end]
    last_px = _px(window[-1])
    assert last_px is not None

    def px_at(t_ms: float) -> float | None:
        # Last trade at or before t (anywhere in the tape before now).
        i = bisect.bisect_right(ts, int(t_ms), 0, end)
        if i == 0:
            return None
        return _px(rows[i - 1])

    p30, p60, p120 = px_at(now_ms - 30_000), px_at(now_ms - 60_000), px_at(now_ms - 120_000)
    ret_60 = _pct(last_px, p60)
    prev_60 = _pct(p60, p120)
    accel = ret_60 - prev_60 if ret_60 is not None and prev_60 is not None else None

    high_px = -1.0
    high_ts = w_ts[0]
    for r, t in zip(window, w_ts):
        p = _px(r)
        if p is not None and p > high_px:
            high_px, high_ts = p, t
    below = (high_px - last_px) / high_px * 100.0 if high_px > 0 else 0.0

    last60 = [r for r, t in zip(window, w_ts) if t > now_ms - 60_000]
    buy = sum(float(r.get("sz") or 0.0) for r in last60 if r.get("side") == "buy")
    total = sum(float(r.get("sz") or 0.0) for r in last60 if r.get("side") in ("buy", "sell"))
    share = buy / total if total > 0 else None

    span_s = (now_ms - max(start_ms, ts[0])) / 1000.0
    per_60 = len(window) / (span_s / 60.0) if span_s >= 60.0 else None
    rate = len(last60) / per_60 if per_60 else None

    return MomentumMeasures(
        now_ms=int(now_ms),
        last_px=last_px,
        ret_30s=_pct(last_px, p30),
        ret_60s=ret_60,
        ret_120s=_pct(last_px, p120),
        accel_60s=accel,
        high_px=high_px,
        secs_since_high=max(0.0, (now_ms - high_ts) / 1000.0),
        below_high_pct=below,
        buy_share_60s=share,
        rate_ratio_60s=rate,
        trades=len(window),
        tape_start_ms=ts[0],
    )


def momentum_entry_ok(
    m: MomentumMeasures | None, cfg: dict[str, Any]
) -> tuple[bool, list[str]]:
    """§3.8 entry conditions.  A ``None`` threshold disables its condition;
    an enabled condition whose measure is missing fails."""
    if m is None:
        return False, ["no tape"]
    fails: list[str] = []

    def _f(key: str) -> float | None:
        v = cfg.get(key)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    checks: list[tuple[str, float | None, float | None, str]] = [
        ("stall_min_seconds", m.secs_since_high, _f("stall_min_seconds"), ">="),
        ("max_below_high_pct", m.below_high_pct, _f("max_below_high_pct"), "<="),
        ("min_ret_60s", m.ret_60s, _f("min_ret_60s"), ">="),
        ("max_ret_60s", m.ret_60s, _f("max_ret_60s"), "<="),
        ("max_buy_share_60s", m.buy_share_60s, _f("max_buy_share_60s"), "<="),
    ]
    for key, value, limit, op in checks:
        if limit is None:
            continue
        if value is None:
            fails.append(f"{key}: no data")
        elif op == ">=" and value < limit:
            fails.append(f"{key}: {value:.2f} < {limit:g}")
        elif op == "<=" and value > limit:
            fails.append(f"{key}: {value:.2f} > {limit:g}")
    return not fails, fails


def tape_from_candles(candles: Iterable[Any], bar_ms: int = 60_000) -> list[dict[str, Any]]:
    """Synthetic tape for the backtest's 1m approximation.

    Each candle becomes four prints: open at ``ts``, the extreme reached first
    at ``ts + bar/3`` (low for a green candle, high for a red one), the other
    at ``ts + 2·bar/3``, and the close at ``ts + bar − 1 ms``.  No sizes or
    sides, so ``buy_share_60s`` is ``None``.  Accepts ``Candle`` objects or
    dicts.
    """
    out: list[dict[str, Any]] = []
    for c in candles:
        g = (lambda k: c[k]) if isinstance(c, dict) else (lambda k: getattr(c, k))
        ts, o, h, lo, cl = int(g("ts")), float(g("open")), float(g("high")), float(g("low")), float(g("close"))
        first, second = (lo, h) if cl >= o else (h, lo)
        third = bar_ms // 3
        for t, p in ((ts, o), (ts + third, first), (ts + 2 * third, second), (ts + bar_ms - 1, cl)):
            out.append({"ts": t, "px": p, "sz": 0.0, "side": ""})
    return out


def attach_momentum(
    snapshot: dict[str, Any], symbol: str, measures: MomentumMeasures | None
) -> dict[str, Any]:
    """Shallow copy of ``snapshot`` with ``market_data[symbol]["wr_momentum"]``.

    ``None`` measures still mark the evaluation as a momentum one
    (``{"missing": True}``), so the strategy vetoes it instead of applying the
    clock rules.
    """
    md = dict(snapshot.get("market_data") or {})
    sym = dict(md.get(symbol) or {})
    sym["wr_momentum"] = measures.to_dict() if measures is not None else {"missing": True}
    md[symbol] = sym
    out = dict(snapshot)
    out["market_data"] = md
    return out
