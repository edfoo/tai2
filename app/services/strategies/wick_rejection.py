"""Hourly Wick Rejection short (``wick_rejection``).

Shorts a strong green 1H candle in the last seconds before it closes, betting
that the close prints below the current price (a wick forms).  Derived from
the user's manual trades (``docs/objectives/wick_rejection_plan.md`` §1).

Funnel (§3.1), each stage vetoes before the next:

  0. ``enabled``
  1. A forming candle is present on the analysis block.
  2. Entry window: ``min_seconds_to_close <= seconds_to_close <= max_seconds_to_close``.
     Momentum entry mode (§3.8, ``entry_mode`` ≠ ``"clock"``): when the caller
     attached ``sym_data["wr_momentum"]`` the window ends at
     ``momentum_entry_max_seconds_to_close`` instead and the measures must pass
     ``wr_momentum.momentum_entry_ok``; without measures the clock rules apply
     (``"momentum_or_clock"``) or nothing enters (``"momentum"``).
  3. Forming candle is green (``require_forming_green``).
  4. Range ``(H − L) / L × 100 >= min_candle_range_pct``.
  5. High offset ``(H − P) / (H − L) × 100 <= max_high_offset_pct``.
  6. Big-body setup: at least ``min_big_bodies`` candles in the setup window
     with ``body_ratio >= min_body_ratio``.
  6b. Prior rise (opt-in, ``max_prior_rise_pct``): skip when the coin already
     rose more than that over the ``prior_rise_hours`` closed candles before
     the forming one (a sustained uptrend, where the short gets stopped).
  6c. Quiet-coin wake profile (opt-in, ``wake_profile_enabled``): the coin
     was quiet over ``wake_base_hours`` (median hourly traded value at most
     ``wake_max_base_volume_usd``), its volume and range grew several times
     in the recent hours and the forming hour, its 1m candles are still
     moving, the price sits a little below the hour's high and the candle has
     not risen ``wake_max_candle_pct`` or more.  Needs the caller's
     ``WR_INPUTS_KEY`` (contract size + 1m candles); without it, veto.
     Measured by ``wake_profile`` / ``wake_profile_fails``.
  7. Optional HTF regime gate (``htf_regime_preference``, default ``"off"``).

``P`` is the forming candle's close: in the backtest the close of the latest
eval candle, live the price at trigger time (the trigger patches the forming
candle with it).  ``seconds_to_close`` is measured from ``snapshot["as_of_ts"]``
so live and backtest agree.

This module is pure: no I/O, no clock reads.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any

from app.services.screener import timeframe_to_ms

from . import StrategyHelpers, StrategySignal, resolve_analysis_block
from .defaults import merged_config
from .wr_momentum import ENTRY_MODES, MomentumMeasures, momentum_entry_ok

STRATEGY_NAME = "wick_rejection"


def _f(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def body_ratio(candles: list[dict[str, Any]], idx: int, window: int) -> float | None:
    """Green body of ``candles[idx]`` ÷ mean ``|body|`` of the ``window`` before it.

    ``0.0`` for a red/doji candle; ``None`` when there is not enough history
    or the average body is zero.  Matches ``_body_ratio`` in
    ``scripts/analyze_manual_trades.py``.
    """
    if window <= 0 or idx < window or idx >= len(candles):
        return None
    o = _f(candles[idx].get("open"))
    c = _f(candles[idx].get("close"))
    if o is None or c is None:
        return None
    if c <= o:
        return 0.0
    prior: list[float] = []
    for k in candles[idx - window: idx]:
        ko, kc = _f(k.get("open")), _f(k.get("close"))
        if ko is None or kc is None:
            return None
        prior.append(abs(kc - ko))
    avg = sum(prior) / len(prior)
    if avg <= 0:
        return None
    return (c - o) / avg


def prior_rise_pct(closed: list[dict[str, Any]], hours: int) -> float | None:
    """Percent change from the close ``hours`` candles before the last closed
    candle to the last closed candle's close.  ``None`` without enough history.
    """
    if hours <= 0 or len(closed) < hours + 1:
        return None
    last = _f(closed[-1].get("close"))
    ref = _f(closed[-1 - hours].get("close"))
    if last is None or ref is None or ref <= 0:
        return None
    return (last / ref - 1.0) * 100.0


# ── Quiet-coin wake profile (opt-in, ``wake_profile_enabled``) ──────────────
#
# The user's manual method: a coin that was quiet for one to three days, whose
# volume and candles grew several times over the last hours, that is still
# moving fast, and that dips just below the hour's high near the close.  The
# caller attaches what the 1H snapshot lacks — the contract size (OKX candle
# volume is in contracts) and the recent 1m candles — under
# ``sym_data[WR_INPUTS_KEY]`` (``attach_wr_inputs``); live the WR trigger
# fetches them, the backtest takes its 1m eval candles.

WR_INPUTS_KEY = "wr_inputs"
MINUTE_MS = 60_000


def _field(candle: Any, key: str) -> float | None:
    if isinstance(candle, dict):
        return _f(candle.get(key))
    return _f(getattr(candle, key, None))


def candles_1m_to_dicts(rows: Any) -> list[dict[str, float]]:
    """1m candles as ``{ts, open, high, low, close}`` dicts, oldest first.

    Accepts ``Candle``-like objects, dicts, or raw OKX rows
    (``[ts, o, h, l, c, ...]``, strings, newest first).  Unparseable rows are
    dropped.
    """
    out: list[dict[str, float]] = []
    for row in rows or []:
        if isinstance(row, (list, tuple)):
            if len(row) < 5:
                continue
            vals = [_f(v) for v in row[:5]]
        else:
            vals = [_field(row, k) for k in ("ts", "open", "high", "low", "close")]
        if any(v is None for v in vals):
            continue
        ts, o, h, lo, c = vals  # type: ignore[misc]
        out.append({"ts": ts, "open": o, "high": h, "low": lo, "close": c})
    out.sort(key=lambda r: r["ts"])
    return out


def attach_wr_inputs(
    snapshot: dict[str, Any],
    symbol: str,
    *,
    ct_val: float | None,
    candles_1m: Any,
) -> dict[str, Any]:
    """Shallow copy of ``snapshot`` with ``market_data[symbol][WR_INPUTS_KEY]``."""
    md = dict(snapshot.get("market_data") or {})
    sym = dict(md.get(symbol) or {})
    sym[WR_INPUTS_KEY] = {
        "ct_val": _f(ct_val),
        "candles_1m": None if candles_1m is None else candles_1m_to_dicts(candles_1m),
    }
    md[symbol] = sym
    out = dict(snapshot)
    out["market_data"] = md
    return out


def avg_1m_range_pct(candles_1m: Any, as_of_ms: float, minutes: int = 15) -> float | None:
    """Mean ``(H − L) / L × 100`` of the 1m candles closed in the ``minutes``
    before the start of the current minute.

    Anchoring on the minute start makes the live trigger (H:59:20) and the
    backtest (H:59:00) read the same candles (H:44–H:58 for 15).  ``None``
    when fewer than half of them are available.
    """
    if minutes <= 0:
        return None
    end = (int(as_of_ms) // MINUTE_MS) * MINUTE_MS
    start = end - minutes * MINUTE_MS
    ranges: list[float] = []
    for c in candles_1m_to_dicts(candles_1m):
        if not (start <= c["ts"] < end):
            continue
        if c["low"] > 0 and c["high"] >= c["low"]:
            ranges.append((c["high"] / c["low"] - 1.0) * 100.0)
    if len(ranges) < max(1, minutes // 2):
        return None
    return sum(ranges) / len(ranges)


@dataclass
class WakeProfile:
    """Quiet-coin wake measures at the entry moment.

    ``*_x`` values are multiples of the quiet baseline: the median hourly
    traded value (USDT) / range of the ``base_hours`` closed candles before
    the ``recent_hours`` most recent ones.
    """

    base_volume_usd: float
    recent_volume_x: float | None   # mean of the recent closed hours ÷ baseline
    hour_volume_x: float | None     # forming hour's value per hour so far ÷ baseline
    hour_range_x: float | None      # forming hour's range ÷ baseline range
    avg_1m_range_pct: float | None  # "still moving"
    below_high_pct: float           # (H / P − 1) × 100
    candle_pct: float               # (P / O − 1) × 100

    def describe(self) -> str:
        def _x(v: float | None) -> str:
            return "n/a" if v is None else f"{v:.1f}x"

        m1 = "n/a" if self.avg_1m_range_pct is None else f"{self.avg_1m_range_pct:.2f}%"
        return (
            f"base {self.base_volume_usd / 1000:.0f}k/h, recent {_x(self.recent_volume_x)}, "
            f"hour vol {_x(self.hour_volume_x)}, hour range {_x(self.hour_range_x)}, "
            f"1m range {m1}, {self.below_high_pct:.2f}% below high, candle +{self.candle_pct:.2f}%"
        )


def wake_profile(
    closed: list[dict[str, Any]],
    forming: dict[str, Any],
    as_of_ms: float,
    ct_val: float,
    candles_1m: Any,
    *,
    base_hours: int = 48,
    recent_hours: int = 3,
    m1_minutes: int = 15,
) -> WakeProfile | None:
    """Measure the wake profile.  ``closed`` are the closed 1H candles oldest
    first; ``forming`` the in-progress candle with ``close`` = the entry price
    and ``high``/``low`` already widened to it.  ``None`` without
    ``base_hours + recent_hours`` closed candles or a usable forming candle.
    """
    if base_hours <= 0 or recent_hours <= 0 or len(closed) < base_hours + recent_hours:
        return None
    f_ts, f_o = _f(forming.get("ts")), _f(forming.get("open"))
    f_h, f_l, p = _f(forming.get("high")), _f(forming.get("low")), _f(forming.get("close"))
    f_v = _f(forming.get("volume")) or 0.0
    if None in (f_ts, f_o, f_h, f_l, p) or not (p and p > 0 and f_o and f_o > 0 and f_l and f_l > 0):
        return None
    assert f_ts is not None and f_h is not None

    def value(c: dict[str, Any]) -> float:
        return (_f(c.get("volume")) or 0.0) * ct_val * (_f(c.get("close")) or 0.0)

    def rng(c: dict[str, Any]) -> float:
        h, lo = _f(c.get("high")), _f(c.get("low"))
        return (h / lo - 1.0) * 100.0 if h is not None and lo and lo > 0 else 0.0

    base = closed[-recent_hours - base_hours:-recent_hours]
    recent = closed[-recent_hours:]
    base_value = statistics.median(value(c) for c in base)
    base_range = statistics.median(rng(c) for c in base)
    minutes = max(1.0, (as_of_ms - f_ts) / MINUTE_MS)
    hour_range = (f_h / f_l - 1.0) * 100.0
    return WakeProfile(
        base_volume_usd=base_value,
        recent_volume_x=(sum(value(c) for c in recent) / len(recent) / base_value) if base_value > 0 else None,
        hour_volume_x=(f_v * ct_val * p * 60.0 / minutes / base_value) if base_value > 0 else None,
        hour_range_x=(hour_range / base_range) if base_range > 0 else None,
        avg_1m_range_pct=avg_1m_range_pct(candles_1m, as_of_ms, m1_minutes),
        below_high_pct=(f_h / p - 1.0) * 100.0,
        candle_pct=(p / f_o - 1.0) * 100.0,
    )


def wake_profile_fails(profile: WakeProfile, cfg: dict[str, Any]) -> list[str]:
    """Failed wake-profile conditions (empty = pass).  A blank threshold is off;
    an unmeasurable value fails its condition."""
    fails: list[str] = []

    def _min(key: str, value: float | None, label: str, unit: str) -> None:
        lim = _f(cfg.get(key))
        if lim is None:
            return
        if value is None:
            fails.append(f"{label} unknown")
        elif value < lim:
            fails.append(f"{label} {value:.2f}{unit} < {lim:g}{unit}")

    max_base = _f(cfg.get("wake_max_base_volume_usd"))
    if max_base is not None and profile.base_volume_usd > max_base:
        fails.append(f"base volume {profile.base_volume_usd / 1000:.0f}k > {max_base / 1000:g}k USDT/h")
    _min("wake_min_recent_volume_x", profile.recent_volume_x, "recent volume", "x")
    _min("wake_min_hour_volume_x", profile.hour_volume_x, "hour volume", "x")
    _min("wake_min_hour_range_x", profile.hour_range_x, "hour range", "x")
    _min("wake_min_avg_1m_range_pct", profile.avg_1m_range_pct, "avg 1m range", "%")
    _min("wake_min_below_high_pct", profile.below_high_pct, "below high", "%")
    max_below = _f(cfg.get("wake_max_below_high_pct"))
    if max_below is not None and profile.below_high_pct > max_below:
        fails.append(f"below high {profile.below_high_pct:.2f}% > {max_below:g}%")
    max_candle = _f(cfg.get("wake_max_candle_pct"))
    if max_candle is not None and profile.candle_pct >= max_candle:
        fails.append(f"candle +{profile.candle_pct:.2f}% >= {max_candle:g}%")
    return fails


def exit_price_pcts(cfg: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return ``(tp_pct, sl_pct)`` as **price** percentages.

    ``tp_unit`` / ``emergency_stop_unit`` = ``"margin"`` divide by
    ``leverage`` (a 35% margin stop at 5× is a 7% price stop).  ``sl_pct`` is
    ``None`` when ``emergency_stop_mode == "off"``.
    """
    lev = _f(cfg.get("leverage")) or 1.0
    if lev <= 0:
        lev = 1.0

    tp = _f(cfg.get("tp_pct"))
    if tp is not None and tp > 0:
        if str(cfg.get("tp_unit") or "price").lower() == "margin":
            tp = tp / lev
    else:
        tp = None

    sl: float | None = None
    if str(cfg.get("emergency_stop_mode") or "exchange").lower() != "off":
        sl = _f(cfg.get("emergency_stop_pct"))
        if sl is not None and sl > 0:
            if str(cfg.get("emergency_stop_unit") or "margin").lower() == "margin":
                sl = sl / lev
        else:
            sl = None
    return tp, sl


@dataclass
class WickRejectionMetrics:
    """Measured values for one evaluation (for logging and ranking)."""

    seconds_to_close: float
    range_pct: float
    offset_pct: float
    body_ratio_max: float
    big_body_age: int  # candles back from the forming candle (0 = forming)

    @property
    def rank_score(self) -> float:
        return self.body_ratio_max * (1.0 - self.offset_pct / 100.0)


class WickRejectionStrategy:
    """Short a strong green 1H candle just before it closes.

    Config lives under ``config["strategies"]["wick_rejection"]``; defaults in
    ``DEFAULT_WICK_REJECTION`` (``strategies/defaults.py``).
    """

    name = STRATEGY_NAME

    def evaluate(
        self,
        symbol: str,
        snapshot: dict[str, Any],
        config: dict[str, Any],
        helpers: StrategyHelpers,
    ) -> StrategySignal | None:
        if not bool(config.get("enabled", False)):
            return None
        cfg = merged_config(config, self.name)

        market_data: dict[str, Any] = snapshot.get("market_data") or {}
        sym_data = market_data.get(symbol) or {}
        block = resolve_analysis_block(sym_data, cfg)

        # ── 1. Forming candle ───────────────────────────────────────────
        forming = block.get("forming_candle")
        as_of = _f(snapshot.get("as_of_ts"))
        if not isinstance(forming, dict) or as_of is None:
            return None
        f_ts = _f(forming.get("ts"))
        f_o, f_h, f_l, p = (
            _f(forming.get("open")), _f(forming.get("high")),
            _f(forming.get("low")), _f(forming.get("close")),
        )
        if None in (f_ts, f_o, f_h, f_l, p):
            return None
        assert f_ts is not None and f_o is not None and f_h is not None
        assert f_l is not None and p is not None
        high = max(f_h, p)
        low = min(f_l, p)

        # ── 2. Entry window ─────────────────────────────────────────────
        tf = str(cfg.get("analysis_timeframe") or "1H")
        bar_ms = timeframe_to_ms(tf)
        # The forming candle must be a ``tf`` candle in progress at as_of.  A
        # missing ``timeframes[tf]`` block makes resolve_analysis_block fall
        # back to the base bar (live: 15m), whose candle would silently fail
        # the window every hour.  Bars up to 4H open on UTC multiples of their
        # length (OKX aligns 1D+ to UTC+8, so those skip the alignment test).
        aligned = bar_ms > 4 * 3_600_000 or f_ts % bar_ms == 0
        if bar_ms > 0 and (not aligned or not f_ts <= as_of < f_ts + bar_ms):
            helpers.emit_debug(
                f"WickRejection {symbol}: veto forming candle is not a {tf} candle "
                f"in progress (opened {int(f_ts)}, as of {int(as_of)})"
            )
            return None
        # Not logged: outside the window this stage vetoes every tick.
        seconds_to_close = (f_ts + bar_ms - as_of) / 1000.0
        min_s = _f(cfg.get("min_seconds_to_close")) or 0.0
        max_s = _f(cfg.get("max_seconds_to_close"))
        # Momentum entry mode (§3.8): measures attached by the caller
        # (``wr_momentum.attach_momentum``) make this a momentum evaluation,
        # with its own, earlier window.  Without measures the clock rules
        # apply, except in pure "momentum" mode, which never enters on them.
        mode = str(cfg.get("entry_mode") or "clock").lower()
        mom_raw = sym_data.get("wr_momentum") if mode != "clock" else None
        momentum: MomentumMeasures | None = None
        if isinstance(mom_raw, dict):
            if not mom_raw.get("missing"):
                momentum = MomentumMeasures.from_dict(mom_raw)
            max_s = _f(cfg.get("momentum_entry_max_seconds_to_close"))
        elif mode == "momentum":
            return None
        if seconds_to_close < min_s or (max_s is not None and seconds_to_close > max_s):
            return None

        tag = f"WickRejection {symbol}"
        if isinstance(mom_raw, dict):
            ok, fails = momentum_entry_ok(momentum, cfg)
            if not ok:
                helpers.emit_debug(f"{tag}: veto momentum ({'; '.join(fails)})")
                return None

        # ── 3. Forming green ────────────────────────────────────────────
        if bool(cfg.get("require_forming_green", True)) and not p > f_o:
            helpers.emit_debug(f"{tag}: veto forming candle not green (open {f_o:g}, price {p:g})")
            return None

        # ── 4. Range ────────────────────────────────────────────────────
        if high <= low or low <= 0:
            helpers.emit_debug(f"{tag}: veto zero range")
            return None
        range_pct = (high - low) / low * 100.0
        min_range = _f(cfg.get("min_candle_range_pct")) or 0.0
        if range_pct < min_range:
            helpers.emit_debug(f"{tag}: veto range {range_pct:.2f}% < {min_range:g}%")
            return None

        # ── 5. High offset (range-relative) ─────────────────────────────
        offset_pct = (high - p) / (high - low) * 100.0
        max_offset = _f(cfg.get("max_high_offset_pct"))
        if max_offset is not None and offset_pct > max_offset:
            helpers.emit_debug(
                f"{tag}: veto high offset {offset_pct:.1f}% of range > {max_offset:g}%"
            )
            return None

        closed = [c for c in (block.get("ohlcv") or []) if isinstance(c, dict)]
        # Defensive: never let a candle at/after the forming bar into "closed".
        closed = [c for c in closed if (_f(c.get("ts")) or 0) < f_ts]

        # ── 5b. Prior rise (opt-in) ─────────────────────────────────────
        max_rise = _f(cfg.get("max_prior_rise_pct"))
        if max_rise is not None:
            hours = int(_f(cfg.get("prior_rise_hours")) or 24)
            rise = prior_rise_pct(closed, hours)
            if rise is None:
                helpers.emit_debug(f"{tag}: veto prior rise unknown (< {hours + 1} closed candles)")
                return None
            if rise > max_rise:
                helpers.emit_debug(f"{tag}: veto prior {hours}h rise {rise:.1f}% > {max_rise:g}%")
                return None

        # ── 5c. Quiet-coin wake profile (opt-in) ────────────────────────
        wake: WakeProfile | None = None
        if bool(cfg.get("wake_profile_enabled", False)):
            inputs = sym_data.get(WR_INPUTS_KEY) or {}
            ct_val = _f(inputs.get("ct_val"))
            c1m = inputs.get("candles_1m")
            if ct_val is None or ct_val <= 0 or c1m is None:
                helpers.emit_debug(f"{tag}: veto wake profile inputs missing (contract size / 1m candles)")
                return None
            base_h = int(_f(cfg.get("wake_base_hours")) or 48)
            recent_h = int(_f(cfg.get("wake_recent_hours")) or 3)
            wake = wake_profile(
                closed,
                {"ts": f_ts, "open": f_o, "high": high, "low": low, "close": p,
                 "volume": forming.get("volume")},
                as_of, ct_val, c1m,
                base_hours=base_h, recent_hours=recent_h,
                m1_minutes=int(_f(cfg.get("wake_1m_minutes")) or 15),
            )
            if wake is None:
                helpers.emit_debug(
                    f"{tag}: veto wake profile history < {base_h + recent_h} closed candles"
                )
                return None
            fails = wake_profile_fails(wake, cfg)
            if fails:
                helpers.emit_debug(f"{tag}: veto wake profile ({'; '.join(fails)})")
                return None

        # ── 6. Big-body setup ───────────────────────────────────────────
        window = int(_f(cfg.get("body_avg_window")) or 8)
        lookback = int(_f(cfg.get("setup_lookback_candles")) or 0)
        include_forming = bool(cfg.get("setup_include_forming", True))
        min_ratio = _f(cfg.get("min_body_ratio")) or 0.0
        min_big = int(_f(cfg.get("min_big_bodies")) or 0)

        series = list(closed)
        if include_forming:
            series.append({"ts": f_ts, "open": f_o, "high": high, "low": low, "close": p})
        n = len(series)
        candidates = list(range(max(0, len(closed) - lookback), len(closed)))
        if include_forming:
            candidates.append(n - 1)

        big = 0
        ratio_max = 0.0
        big_age = -1
        for idx in candidates:
            r = body_ratio(series, idx, window)
            if r is None:
                continue
            if r > ratio_max:
                ratio_max = r
                big_age = len(closed) - idx if idx < len(closed) else 0
            if r >= min_ratio:
                big += 1
        if big < min_big:
            helpers.emit_debug(
                f"{tag}: veto setup {big} big bodies (max {ratio_max:.2f}x) "
                f"< {min_big} at >= {min_ratio:g}x"
            )
            return None

        # ── 7. HTF regime (opt-in) ──────────────────────────────────────
        htf_pref = str(cfg.get("htf_regime_preference") or "off")
        if htf_pref.lower() != "off":
            from app.services.indicator_service import htf_regime_allows

            if not htf_regime_allows(
                _f(block.get("adx_htf")), _f(block.get("choppiness_htf")), htf_pref
            ):
                helpers.emit_debug(f"{tag}: veto HTF regime ({htf_pref})")
                return None

        metrics = WickRejectionMetrics(
            seconds_to_close=seconds_to_close,
            range_pct=range_pct,
            offset_pct=offset_pct,
            body_ratio_max=ratio_max,
            big_body_age=big_age,
        )
        tp_pct, sl_pct = exit_price_pcts(cfg)
        age_txt = "forming" if big_age == 0 else f"{big_age}h ago"
        rationale = (
            f"WickRejection: {seconds_to_close:.0f}s to close, offset "
            f"{offset_pct:.0f}% of {range_pct:.1f}% range, body "
            f"{ratio_max:.1f}x ({age_txt})"
        )
        if wake is not None:
            rationale += f"; wake: {wake.describe()}"
        if momentum is not None:
            rationale += f"; momentum: {momentum.describe()}"
        elif mode != "clock":
            rationale += "; clock entry"
        helpers.emit_debug(f"{tag}: SIGNAL sell — {rationale}")
        return StrategySignal(
            direction="sell",
            strategy_name=self.name,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            rationale=rationale,
            rank_score=metrics.rank_score,
        )


def backtest_entry_timing(
    config: dict[str, Any] | None, *, evaluation_mode: str, evaluation_timeframe: str | None
) -> dict[str, Any]:
    """Describe when a backtest can enter wick_rejection (plan §3.7).

    WR needs a forming candle and a step moment inside
    ``[min_seconds_to_close, max_seconds_to_close]`` before the analysis bar
    closes.  Backtest moments are eval-candle closes, so the candidates are
    ``k × eval step`` seconds before the close (``k ≥ 1``).  The first moment in
    the window (the largest) is where entries land; live enters at
    ``entry_seconds_before_close`` instead (recorded as a parity gap).

    Returns a provenance dict with ``supported`` and, when unsupported, the
    reason WR produces zero trades.
    """
    cfg = merged_config(config, STRATEGY_NAME)
    live_s = _f(cfg.get("entry_seconds_before_close"))
    out: dict[str, Any] = {
        "kind": "wick_rejection_entry_timing",
        "evaluation_mode": evaluation_mode,
        "evaluation_timeframe": evaluation_timeframe,
        "live_seconds_before_close": live_s,
        "backtest_seconds_before_close": None,
        "supported": False,
        "reason": "",
    }
    if evaluation_mode != "finer_ltf":
        out["reason"] = (
            f"evaluation_mode={evaluation_mode!r} has no forming candle; "
            "wick_rejection needs finer_ltf with a 1m evaluation timeframe"
        )
        return out
    step_s = timeframe_to_ms(evaluation_timeframe or "1m") / 1000.0
    bar_s = timeframe_to_ms(str(cfg.get("analysis_timeframe") or "1H")) / 1000.0
    min_s = _f(cfg.get("min_seconds_to_close")) or 0.0
    max_s = _f(cfg.get("max_seconds_to_close"))
    if max_s is None:
        max_s = bar_s
    fits = [
        k * step_s for k in range(1, int(bar_s // step_s) + 1)
        if min_s <= k * step_s <= max_s and k * step_s < bar_s
    ] if step_s > 0 else []
    if not fits:
        out["reason"] = (
            f"no {evaluation_timeframe} step lands {min_s:g}–{max_s:g}s before the "
            f"{cfg.get('analysis_timeframe') or '1H'} close; use a 1m evaluation timeframe"
        )
        return out
    out["supported"] = True
    out["backtest_seconds_before_close"] = max(fits)
    if live_s is not None and live_s != max(fits):
        out["reason"] = (
            f"parity gap: backtest enters {max(fits):g}s before the close "
            f"(eval-candle close), live {live_s:g}s"
        )
    mode = str(cfg.get("entry_mode") or "clock").lower()
    out["entry_mode"] = mode
    if mode in ("momentum", "momentum_or_clock"):
        mom_max = _f(cfg.get("momentum_entry_max_seconds_to_close")) or 0.0
        out["momentum_seconds_before_close"] = [
            k * step_s for k in range(1, int(bar_s // step_s) + 1)
            if min_s <= k * step_s <= mom_max and k * step_s < bar_s
        ]
        out["momentum"] = (
            "1m approximation: measures from a tape built out of the eval candles "
            "(minute-resolution returns, secs_since_high to within a candle, no buy "
            "share); live uses the websocket trade tape every "
            f"{cfg.get('momentum_eval_interval_seconds')}s"
        )
    return out


def select_ranked_entries(
    candidates: list[tuple[Any, float, float]],
    *,
    open_positions: int,
    open_margin: float,
    equity: float | None,
    max_concurrent_positions: Any,
    max_total_margin_pct: Any,
) -> tuple[list[Any], dict[Any, str]]:
    """Rank one evaluation's WR entries and apply the position caps.

    ``candidates`` are ``(key, rank_score, margin_usd)``.  Highest
    ``rank_score`` first (stable for ties); at most
    ``max_concurrent_positions − open_positions`` are kept (0 = no cap), and
    the summed margin of open plus chosen entries stays within
    ``max_total_margin_pct`` of ``equity`` (0 or unknown equity = no cap).
    Returns ``(chosen keys in rank order, {dropped key: reason})``.  Shared by
    the live trigger and the backtest so both cap entries the same way.
    """
    ranked = sorted(candidates, key=lambda c: float(c[1] or 0.0), reverse=True)
    max_conc = int(_f(max_concurrent_positions) or 0)
    slots = max(0, max_conc - int(open_positions)) if max_conc > 0 else len(ranked)
    max_pct = _f(max_total_margin_pct) or 0.0
    budget = equity * max_pct / 100.0 if equity and equity > 0 and max_pct > 0 else None
    used = float(open_margin or 0.0)
    chosen: list[Any] = []
    dropped: dict[Any, str] = {}
    for key, _score, margin in ranked:
        if len(chosen) >= slots:
            dropped[key] = f"max_concurrent_positions {max_conc} ({open_positions} open)"
            continue
        m = float(margin or 0.0)
        if budget is not None and used + m > budget + 1e-9:
            dropped[key] = (
                f"max_total_margin_pct {max_pct:g}% "
                f"({used:.2f}+{m:.2f} > {budget:.2f} USDT)"
            )
            continue
        used += m
        chosen.append(key)
    return chosen, dropped


# ── Trigger timing (shared by the live trigger and the backtest) ────────────

# The vol refresh runs at least this long before the trigger wakes, so the
# snapshot for coins new to the list is ready by then.
VOL_REFRESH_MIN_LEAD_SECONDS = 20.0


def entry_mode(cfg: dict[str, Any]) -> str:
    mode = str(cfg.get("entry_mode") or "clock").lower()
    return mode if mode in ENTRY_MODES else "clock"


def wake_lead_seconds(cfg: dict[str, Any]) -> float:
    """Seconds before the hour the trigger wakes: the clock entry time, or the
    watch start in a momentum mode."""
    if entry_mode(cfg) == "clock":
        return float(cfg.get("entry_seconds_before_close") or 40.0)
    return max(
        float(cfg.get("watch_seconds_before_close") or 300.0),
        float(cfg.get("entry_seconds_before_close") or 40.0),
    )


def vol_refresh_lead_seconds(cfg: dict[str, Any]) -> float | None:
    """Seconds before the hourly close the vol list is rescored, or None (off).

    ``vol_refresh_seconds_before_close``, moved earlier when needed so it runs
    at least ``VOL_REFRESH_MIN_LEAD_SECONDS`` before the trigger wakes.
    """
    raw = _f(cfg.get("vol_refresh_seconds_before_close"))
    if raw is None or raw <= 0:
        return None
    return min(3599.0, max(raw, wake_lead_seconds(cfg) + VOL_REFRESH_MIN_LEAD_SECONDS))


def backtest_vol_refresh_cut_seconds(cfg: dict[str, Any], eval_step_seconds: float = 60.0) -> int | None:
    """Seconds before the close where the backtest cuts the hour in progress
    for the refreshed vol list: the live refresh moment rounded *earlier* to
    an eval-candle boundary (data the live refresh could not have seen is never
    used).  None when the refresh is off."""
    lead = vol_refresh_lead_seconds(cfg)
    if lead is None:
        return None
    step = max(1.0, float(eval_step_seconds))
    k = -(-lead // step)  # ceil
    return int(k * step)
