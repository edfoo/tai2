"""Shared backtest orchestration helpers (single mechanism for CLI + UI).

The heavy lifting — :class:`BacktestEngine`, persistence, metrics, sweep
catalogue, sensitivity analysis — is already shared.  This module consolidates
the *orchestration* layer that was previously duplicated across four call
sites (``run_backtest_cli.py``, ``run_grid_sweep.py``, and the two runners in
``app/ui/pages.py``):

  * ``parse_timeframe``     — one canonical timeframe normaliser.
  * ``htf_for``             — one canonical LTF→HTF map (display/selection).
  * ``resolve_evaluation``  — the "closed" vs "finer_ltf" eval-mode switch.
  * ``seed_strategy_configs`` — seed launcher ``strategies`` from canonical defaults.
  * ``build_backtest_config`` — one :class:`BacktestConfig` constructor.

Both the headless CLI scripts and the NiceGUI runners build their configs
through :func:`build_backtest_config`, so a change to config shape (a new
required field, a default warmup, a new eval mode) is made in exactly one
place and cannot silently drift between CLI and UI.
"""

from __future__ import annotations

from typing import Any

from app.services.backtest.models import BacktestConfig
from app.services.strategies.defaults import strategy_defaults

# Canonical default strategy list (used by the CLI argument defaults).
DEFAULT_STRATEGIES = (
    "mean_reversion",
    "liquidity_sweep",
    "trend_pullback",
    "vwap_reversion",
    "spike_continuation",
)

# Canonical LTF→HTF map (a superset of ``data_fetcher.htf_for``, which only
# resolves 15m/1H/4H for actual HTF *fetching*; this map additionally covers
# the finer/coarser LTFs for display and run labelling).
_HTF_MAP = {
    "1m": "5m",
    "5m": "15m",
    "15m": "1H",
    "1H": "4H",
    "4H": "1D",
    "1D": "1W",
}

# Canonical timeframe aliases → engine form.
_TIMEFRAME_ALIASES = {
    "1M": "1m", "1MIN": "1m",
    "5M": "5m", "5MIN": "5m",
    "15M": "15m", "15MIN": "15m",
    "1H": "1H", "1HOUR": "1H", "1HR": "1H",
    "4H": "4H", "4HOUR": "4H", "4HR": "4H",
    "1D": "1D", "1DAY": "1D",
}


def parse_timeframe(tf: str, ctx: str = "timeframe") -> str:
    """Normalise a timeframe string to engine form (``15m``/``1H``/``4H``).

    Accepts a broad set of aliases (``15min``, ``1h``, ``4hour``, …) and
    raises ``SystemExit`` with a helpful message on unsupported input.
    """
    t = tf.strip().upper()
    if t not in _TIMEFRAME_ALIASES:
        raise SystemExit(
            f"Unsupported timeframe '{tf}' for {ctx}. "
            f"Use one of: 1m, 5m, 15m, 1H, 4H, 1D."
        )
    return _TIMEFRAME_ALIASES[t]


def htf_for(tf: str) -> str:
    """Return the higher timeframe for an LTF (``15m`` → ``1H``, etc.)."""
    return _HTF_MAP.get(tf.strip().lower(), "")


def resolve_evaluation(eval_step: str, timeframe: str) -> tuple[str, str]:
    """Map a UI eval-step selection to ``(evaluation_mode, evaluation_timeframe)``.

    ``"closed"`` selects the legacy closed-candle mode (stepping on the LTF
    itself); any other value selects ``finer_ltf`` stepping on ``eval_step``.
    """
    if eval_step == "closed":
        return "closed", timeframe
    return "finer_ltf", eval_step


def seed_strategy_configs(strategy_names: list[str]) -> dict[str, Any]:
    """Seed launcher ``strategies`` from canonical defaults (all enabled)."""
    cfg: dict[str, Any] = {}
    for name in strategy_names:
        s = dict(strategy_defaults(name))
        s["enabled"] = True
        cfg[name] = s
    return cfg


def build_backtest_config(
    *,
    symbols: list[str],
    timeframe: str,
    strategy_names: list[str],
    start_ts: int,
    end_ts: int,
    capital: float = 1000.0,
    warmup: int = 200,
    evaluation_mode: str = "finer_ltf",
    evaluation_timeframe: str = "1m",
    launcher_config: dict[str, Any] | None = None,
    strategy_config: dict[str, Any] | None = None,
) -> BacktestConfig:
    """Build a :class:`BacktestConfig` — the single config constructor.

    ``launcher_config`` / ``strategy_config``, when provided, are used
    verbatim (the UI passes its live runtime config).  When omitted (headless
    CLI), a launcher config is seeded from canonical strategy defaults with
    ``notional_usd`` = ``capital``.
    """
    if launcher_config is None:
        launcher_config = {
            "mode": "launcher_only",
            "notional_usd": float(capital),  # per-trade size
            "strategies": seed_strategy_configs(strategy_names),
        }

    return BacktestConfig(
        symbols=symbols,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        initial_capital=capital,
        strategy_names=strategy_names,
        launcher_config=launcher_config,
        strategy_config=dict(strategy_config or {}),
        warmup_candles=warmup,
        disable_live_execution=True,
        evaluation_mode=evaluation_mode,
        evaluation_timeframe=evaluation_timeframe,
    )


def build_single_strategy_config(
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    start_ts: int,
    end_ts: int,
    capital: float = 1000.0,
    warmup: int = 200,
    overrides: dict[str, Any] | None = None,
    evaluation_mode: str = "finer_ltf",
    evaluation_timeframe: str = "1m",
) -> BacktestConfig:
    """Build a :class:`BacktestConfig` for a single strategy with overrides.

    Used by the A/B sweep scripts (``run_gate_ab_sweep.py``,
    ``run_trend_pullback_ab.py``, ``run_vwap_ab_sweep.py``) which enable one
    strategy and vary a handful of its parameters per run.  ``overrides`` are
    applied on top of the canonical defaults before seeding the launcher
    config.
    """
    strat_cfg = dict(strategy_defaults(strategy_name))
    strat_cfg["enabled"] = True
    if overrides:
        strat_cfg.update(overrides)

    launcher_config: dict[str, Any] = {
        "mode": "launcher_only",
        "notional_usd": float(capital),
        "strategies": {strategy_name: strat_cfg},
    }

    return build_backtest_config(
        symbols=[symbol],
        timeframe=timeframe,
        strategy_names=[strategy_name],
        start_ts=start_ts,
        end_ts=end_ts,
        capital=capital,
        warmup=warmup,
        evaluation_mode=evaluation_mode,
        evaluation_timeframe=evaluation_timeframe,
        launcher_config=launcher_config,
    )


# ── Trade close-reason helpers (shared by the A/B sweep scripts) ─────────


def count_close_reasons(result: Any, *needles: str) -> int:
    """Count closed trades whose ``close_reason`` contains any of ``needles``."""
    n = 0
    for t in getattr(result, "trades", []):
        reason = (getattr(t, "close_reason", "") or "").lower()
        if any(ndl.lower() in reason for ndl in needles):
            n += 1
    return n


def count_stop_outs(result: Any) -> int:
    """Count trades closed by a stop-loss (close_reason contains 'stop'/'sl')."""
    return count_close_reasons(result, "stop", "sl")


def count_timeouts(result: Any) -> int:
    """Count trades closed by timeout / end-of-data (TP never reached)."""
    return count_close_reasons(result, "timeout", "end_of_data")
