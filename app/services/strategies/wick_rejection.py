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
  7. Optional HTF regime gate (``htf_regime_preference``, default ``"off"``).

``P`` is the forming candle's close: in the backtest the close of the latest
eval candle, live the price at trigger time (the trigger patches the forming
candle with it).  ``seconds_to_close`` is measured from ``snapshot["as_of_ts"]``
so live and backtest agree.

This module is pure: no I/O, no clock reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.services.screener import timeframe_to_ms

from . import StrategyHelpers, StrategySignal, resolve_analysis_block
from .defaults import merged_config
from .wr_momentum import MomentumMeasures, momentum_entry_ok

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
        # Not logged: outside the window this stage vetoes every tick.
        tf = str(cfg.get("analysis_timeframe") or "1H")
        bar_ms = timeframe_to_ms(tf)
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

        # ── 6. Big-body setup ───────────────────────────────────────────
        window = int(_f(cfg.get("body_avg_window")) or 8)
        lookback = int(_f(cfg.get("setup_lookback_candles")) or 0)
        include_forming = bool(cfg.get("setup_include_forming", True))
        min_ratio = _f(cfg.get("min_body_ratio")) or 0.0
        min_big = int(_f(cfg.get("min_big_bodies")) or 0)

        closed = [c for c in (block.get("ohlcv") or []) if isinstance(c, dict)]
        # Defensive: never let a candle at/after the forming bar into "closed".
        closed = [c for c in closed if (_f(c.get("ts")) or 0) < f_ts]
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
