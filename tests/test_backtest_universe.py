"""Tests for historical screener-universe reconstruction in backtests.

Covers:
  - Shared scoring core parity with the live screener
  - Strategy → SC/MR universe routing
  - Historical ticker reconstruction from 1H candles
  - UniverseSchedule interval lookup (bisect)
  - build_universe_schedule end-to-end with a fake fetcher
  - Engine universe gating (_symbol_in_universe)
  - Persistence round-trip of the universe schedule
  - API model accepts universe_mode
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from app.services.backtest.models import BacktestConfig, Candle
from app.services.backtest.universe import (
    UniverseInterval,
    UniverseSchedule,
    _interval_boundaries,
    _ticker_at,
    build_universe_schedule,
)
from app.services.screener import score_universe, universe_for_strategy

_HOUR_MS = 3_600_000


def _cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "enabled": True,
        "dual_universe": True,
        "universe_filter": "*-USDT-SWAP",
        "max_symbols": 10,
        "sc_max_symbols": 3,
        "mr_max_symbols": 3,
        "min_volume_usd": 0,
        "max_spread_pct": 0.0,
        "sc_min_momentum_pct": 0.5,
        "sc_min_hl_range_pct": 0.0,
        "mr_min_hl_range_pct": 1.0,
        "mr_max_momentum_pct": 8.0,
        "min_momentum_pct": 0.5,
        "min_hl_range_pct": 0.0,
        "vol_history_window": 8,
        "interval_minutes": 60,
    }
    cfg.update(overrides)
    return cfg


def _tickers() -> list[dict[str, Any]]:
    return [
        {"instId": "SC1-USDT-SWAP", "last": 110, "open24h": 100, "high24h": 112,
         "low24h": 99, "volCcy24h": 2_000_000},
        {"instId": "SC2-USDT-SWAP", "last": 120, "open24h": 100, "high24h": 125,
         "low24h": 98, "volCcy24h": 3_000_000},
        {"instId": "MR1-USDT-SWAP", "last": 101, "open24h": 100, "high24h": 108,
         "low24h": 95, "volCcy24h": 1_500_000},
        {"instId": "MR2-USDT-SWAP", "last": 100.5, "open24h": 100, "high24h": 106,
         "low24h": 94, "volCcy24h": 1_200_000},
    ]


# ── Shared scoring core ──────────────────────────────────────────────────────


class TestScoreUniverse:
    def test_builds_sc_and_mr_lists(self) -> None:
        result = score_universe(_tickers(), _cfg())
        assert result["sc"]
        assert result["mr"]
        # Union is SC-first then MR-only additions.
        assert result["selected"][: len(result["sc"])] == result["sc"]
        for sym in result["sc"] + result["mr"]:
            assert sym in result["selected"]

    def test_strong_trend_excluded_from_mr(self) -> None:
        result = score_universe(_tickers(), _cfg())
        assert "SC2-USDT-SWAP" not in result["mr"]
        assert "SC2-USDT-SWAP" in result["sc"]

    def test_vol_history_updates_in_place(self) -> None:
        history: dict[str, deque] = {}
        score_universe(_tickers(), _cfg(), vol_history=history)
        assert set(history) == {t["instId"] for t in _tickers()}
        assert all(len(h) == 1 for h in history.values())
        # Second pass builds a spike ratio (needs >= 2 samples).
        result = score_universe(_tickers(), _cfg(), vol_history=history)
        assert all(c["vol_spike_ratio"] is not None for c in result["candidates"])

    def test_no_mutation_when_disabled(self) -> None:
        history: dict[str, deque] = {}
        score_universe(_tickers(), _cfg(), vol_history=history, update_vol_history=False)
        assert all(len(h) == 0 for h in history.values())

    def test_empty_tickers(self) -> None:
        result = score_universe([], _cfg())
        assert result["selected"] == []
        assert result["base_candidate_count"] == 0


class TestUniverseForStrategy:
    def test_sc_strategies(self) -> None:
        for name in ("spike_continuation", "trend_pullback"):
            assert universe_for_strategy(
                name, sc_symbols=["A"], mr_symbols=["B"],
                selected_symbols=["A", "B"], fallback_symbols=["F"],
            ) == ["A"]

    def test_mr_strategies(self) -> None:
        for name in ("mean_reversion", "liquidity_sweep", "vwap_reversion"):
            assert universe_for_strategy(
                name, sc_symbols=["A"], mr_symbols=["B"],
                selected_symbols=["A", "B"], fallback_symbols=["F"],
            ) == ["B"]

    def test_unknown_strategy_gets_union(self) -> None:
        assert universe_for_strategy(
            "other", sc_symbols=["A"], mr_symbols=["B"],
            selected_symbols=["A", "B"], fallback_symbols=["F"],
        ) == ["A", "B"]

    def test_fallback_when_empty(self) -> None:
        assert universe_for_strategy(
            "mean_reversion", sc_symbols=[], mr_symbols=[],
            selected_symbols=[], fallback_symbols=["F"],
        ) == ["F"]

    def test_legacy_single_list(self) -> None:
        # dual_universe=False → union list regardless of strategy.
        assert universe_for_strategy(
            "spike_continuation", sc_symbols=["A"], mr_symbols=["B"],
            selected_symbols=["A", "B"], fallback_symbols=["F"],
            dual_universe=False,
        ) == ["A", "B"]


# ── Historical ticker reconstruction ─────────────────────────────────────────


def _hourly_candles(
    symbol: str, *, start_ts: int, count: int, base: float = 100.0,
    quote_volume: float = 1_000_000.0,
) -> list[Candle]:
    return [
        Candle(
            ts=start_ts + i * _HOUR_MS,
            open=base, high=base + 1, low=base - 1, close=base,
            volume=10.0, quote_volume=quote_volume,
        )
        for i in range(count)
    ]


class TestTickerReconstruction:
    def test_reconstructs_24h_fields(self) -> None:
        start = 1_700_000_000_000
        candles = _hourly_candles("X-USDT-SWAP", start_ts=start, count=48)
        ts_list = [c.ts for c in candles]
        # Interval at the last candle: trailing 24h = 24 candles.
        ts = candles[-1].ts
        ticker = _ticker_at(candles, ts_list, ts)
        assert ticker is not None
        assert ticker["last"] == 100.0
        assert ticker["high24h"] == 101.0
        assert ticker["low24h"] == 99.0
        # 24 candles × 1M quote volume.
        assert ticker["volCcy24h"] == pytest.approx(24_000_000.0)
        # open24h = close of the candle just before the window.
        assert ticker["open24h"] == 100.0

    def test_returns_none_before_first_candle(self) -> None:
        start = 1_700_000_000_000
        candles = _hourly_candles("X-USDT-SWAP", start_ts=start, count=5)
        ts_list = [c.ts for c in candles]
        assert _ticker_at(candles, ts_list, start - _HOUR_MS) is None

    def test_quote_volume_fallback(self) -> None:
        start = 1_700_000_000_000
        candles = [
            Candle(ts=start + i * _HOUR_MS, open=100, high=101, low=99, close=100,
                   volume=10.0, quote_volume=0.0)
            for i in range(30)
        ]
        ts_list = [c.ts for c in candles]
        ticker = _ticker_at(candles, ts_list, candles[-1].ts)
        # Falls back to volume * close = 10 * 100 = 1000 per candle.
        assert ticker is not None
        assert ticker["volCcy24h"] == pytest.approx(24_000.0)


class TestIntervalBoundaries:
    def test_aligned_boundaries(self) -> None:
        # Hour-aligned start so boundaries land exactly on the grid.
        start = 1_700_000_000_000 - (1_700_000_000_000 % _HOUR_MS)
        end = start + 3 * _HOUR_MS
        bounds = _interval_boundaries(start, end, _HOUR_MS)
        assert bounds == [start, start + _HOUR_MS, start + 2 * _HOUR_MS, start + 3 * _HOUR_MS]

    def test_unaligned_start_snaps_forward(self) -> None:
        start = 1_700_000_000_000  # not hour-aligned
        end = start + 3 * _HOUR_MS
        bounds = _interval_boundaries(start, end, _HOUR_MS)
        assert bounds[0] > start
        assert bounds[0] % _HOUR_MS == 0

    def test_empty_window(self) -> None:
        assert _interval_boundaries(100, 100, _HOUR_MS) == []


# ── UniverseSchedule ─────────────────────────────────────────────────────────


class TestUniverseSchedule:
    def _schedule(self) -> UniverseSchedule:
        return UniverseSchedule(
            intervals=[
                UniverseInterval(ts=1000, sc=["A"], mr=["B"], selected=["A", "B"]),
                UniverseInterval(ts=2000, sc=["C"], mr=["D"], selected=["C", "D"]),
            ],
            dual_universe=True,
        )

    def test_universe_at_uses_most_recent_interval(self) -> None:
        sched = self._schedule()
        assert sched.universe_at(1500, "spike_continuation") == ["A"]
        assert sched.universe_at(2500, "mean_reversion") == ["D"]

    def test_universe_at_before_first_returns_none(self) -> None:
        assert self._schedule().universe_at(500, "mean_reversion") is None

    def test_all_symbols_union(self) -> None:
        assert self._schedule().all_symbols == ["A", "B", "C", "D"]

    def test_to_dict_round_trips(self) -> None:
        d = self._schedule().to_dict()
        assert d["interval_count"] == 2
        assert d["all_symbols"] == ["A", "B", "C", "D"]
        assert d["intervals"][0]["sc"] == ["A"]


# ── build_universe_schedule ──────────────────────────────────────────────────


class _FakeFetcher:
    """Minimal fetcher stub returning pre-seeded 1H candles."""

    def __init__(self, candles: dict[str, list[Candle]], universe: list[str]) -> None:
        self._candles = candles
        self._universe = universe
        self.last_universe_provenance = {"source": "test", "symbol_count": len(universe)}

    async def fetch_swap_universe(self, *, force: bool = False) -> list[str]:
        return list(self._universe)

    async def fetch_candles(self, symbol: str, timeframe: str, start_ts: int,
                            end_ts: int, *, warmup_candles: int = 0,
                            progress_cb: Any = None) -> list[Candle]:
        return [c for c in self._candles.get(symbol, []) if start_ts <= c.ts <= end_ts]


class TestBuildUniverseSchedule:
    def test_builds_intervals_and_selects(self) -> None:
        start = 1_700_000_000_000
        # SC1 trends up strongly; MR1 chops.  Build 48h of hourly candles.
        sc1 = [
            Candle(ts=start + i * _HOUR_MS, open=100 + i, high=101 + i, low=99 + i,
                   close=100 + i, volume=10, quote_volume=2_000_000)
            for i in range(48)
        ]
        mr1 = [
            Candle(ts=start + i * _HOUR_MS, open=100, high=105, low=95,
                   close=100 + (1 if i % 2 else -1), volume=10, quote_volume=1_500_000)
            for i in range(48)
        ]
        fetcher = _FakeFetcher(
            {"SC1-USDT-SWAP": sc1, "MR1-USDT-SWAP": mr1},
            ["SC1-USDT-SWAP", "MR1-USDT-SWAP"],
        )
        schedule = asyncio.run(build_universe_schedule(
            fetcher=fetcher,  # type: ignore[arg-type]
            start_ts=start + 24 * _HOUR_MS,
            end_ts=start + 47 * _HOUR_MS,
            screener_config=_cfg(),
        ))
        assert schedule.intervals
        assert schedule.all_symbols
        assert schedule.provenance["universe_source"] == "okx_swap_universe"
        # Spread filter disabled → recorded as such.
        assert schedule.provenance["spread_filter"] == "disabled"

    def test_spread_filter_recorded_as_skipped(self) -> None:
        start = 1_700_000_000_000
        candles = {
            "A-USDT-SWAP": _hourly_candles("A-USDT-SWAP", start_ts=start, count=48),
        }
        fetcher = _FakeFetcher(candles, ["A-USDT-SWAP"])
        schedule = asyncio.run(build_universe_schedule(
            fetcher=fetcher,  # type: ignore[arg-type]
            start_ts=start + 24 * _HOUR_MS,
            end_ts=start + 47 * _HOUR_MS,
            screener_config=_cfg(max_spread_pct=0.5),
        ))
        assert "skipped" in schedule.provenance["spread_filter"]

    def test_explicit_candidate_pool(self) -> None:
        start = 1_700_000_000_000
        candles = {
            "A-USDT-SWAP": _hourly_candles("A-USDT-SWAP", start_ts=start, count=48),
        }
        fetcher = _FakeFetcher(candles, ["SHOULD-NOT-BE-USED"])
        schedule = asyncio.run(build_universe_schedule(
            fetcher=fetcher,  # type: ignore[arg-type]
            start_ts=start + 24 * _HOUR_MS,
            end_ts=start + 47 * _HOUR_MS,
            screener_config=_cfg(),
            candidate_symbols=["A-USDT-SWAP"],
        ))
        assert schedule.provenance["universe_source"] == "explicit_candidate_list"


# ── Engine gating ────────────────────────────────────────────────────────────


class TestEngineUniverseGating:
    def _engine(self, mode: str, schedule: UniverseSchedule | None) -> Any:
        from app.services.backtest.engine import BacktestEngine

        config = BacktestConfig(
            symbols=["FALLBACK-USDT-SWAP"],
            timeframe="1H",
            start_ts=0,
            end_ts=1,
            universe_mode=mode,
        )
        engine = BacktestEngine(config)
        engine._universe_schedule = schedule
        return engine

    def test_explicit_mode_allows_all(self) -> None:
        engine = self._engine("explicit", None)
        assert engine._symbol_in_universe("ANY-USDT-SWAP", "mean_reversion", 5000)

    def test_screener_mode_gates_by_strategy(self) -> None:
        schedule = UniverseSchedule(
            intervals=[UniverseInterval(ts=1000, sc=["SC-USDT-SWAP"],
                                        mr=["MR-USDT-SWAP"],
                                        selected=["SC-USDT-SWAP", "MR-USDT-SWAP"])],
            dual_universe=True,
        )
        engine = self._engine("screener", schedule)
        assert engine._symbol_in_universe("SC-USDT-SWAP", "spike_continuation", 2000)
        assert not engine._symbol_in_universe("MR-USDT-SWAP", "spike_continuation", 2000)
        assert engine._symbol_in_universe("MR-USDT-SWAP", "mean_reversion", 2000)

    def test_screener_mode_falls_back_before_first_interval(self) -> None:
        schedule = UniverseSchedule(
            intervals=[UniverseInterval(ts=1000, sc=["SC-USDT-SWAP"],
                                        mr=["MR-USDT-SWAP"],
                                        selected=["SC-USDT-SWAP", "MR-USDT-SWAP"])],
            dual_universe=True,
        )
        engine = self._engine("screener", schedule)
        # Before the first interval → configured fallback list only.
        assert engine._symbol_in_universe("FALLBACK-USDT-SWAP", "mean_reversion", 500)
        assert not engine._symbol_in_universe("SC-USDT-SWAP", "mean_reversion", 500)


# ── Persistence ──────────────────────────────────────────────────────────────


class TestUniversePersistence:
    def test_result_round_trip(self) -> None:
        from app.services.backtest.models import BacktestResult
        from app.services.backtest.persistence import result_from_dict, result_to_dict

        config = BacktestConfig(
            symbols=["A-USDT-SWAP"], timeframe="1H", start_ts=0, end_ts=1,
            universe_mode="screener", screener_config=_cfg(),
        )
        result = BacktestResult(config=config)
        result.universe_schedule = UniverseSchedule(
            intervals=[UniverseInterval(ts=1000, sc=["A"], mr=["B"], selected=["A", "B"])],
        ).to_dict()

        payload = result_to_dict(result)
        assert payload["universe_schedule"]["interval_count"] == 1
        assert payload["config"]["universe_mode"] == "screener"

        restored = result_from_dict(payload)
        assert restored is not None
        assert restored.universe_schedule["all_symbols"] == ["A", "B"]
        assert restored.config.universe_mode == "screener"

    def test_legacy_result_without_universe(self) -> None:
        from app.services.backtest.models import BacktestResult
        from app.services.backtest.persistence import result_from_dict, result_to_dict

        config = BacktestConfig(symbols=["A-USDT-SWAP"], timeframe="1H", start_ts=0, end_ts=1)
        payload = result_to_dict(BacktestResult(config=config))
        payload.pop("universe_schedule", None)
        restored = result_from_dict(payload)
        assert restored is not None
        assert restored.universe_schedule is None
        assert restored.config.universe_mode == "explicit"


# ── API model ────────────────────────────────────────────────────────────────


class TestApiModel:
    def test_accepts_screener_mode(self) -> None:
        from app.services.backtest.api_models import BacktestRunRequest

        req = BacktestRunRequest(universe_mode="screener", symbols=[])
        assert req.universe_mode == "screener"

    def test_rejects_unknown_mode(self) -> None:
        from pydantic import ValidationError

        from app.services.backtest.api_models import BacktestRunRequest

        with pytest.raises(ValidationError):
            BacktestRunRequest(universe_mode="bogus")


# ── End-to-end engine run in screener mode ───────────────────────────────────


class _EngineFakeFetcher:
    """Fetcher stub covering every call the engine makes in screener mode."""

    def __init__(self, candles: dict[str, list[Candle]], universe: list[str]) -> None:
        self._candles = candles
        self._universe = universe
        self.last_fetch_provenance: dict[str, Any] = {}
        self.last_universe_provenance = {"source": "test", "symbol_count": len(universe)}
        self.last_funding_provenance: dict[str, Any] = {}
        self.last_instrument_provenance: dict[str, Any] = {}

    async def fetch_swap_universe(self, *, force: bool = False) -> list[str]:
        return list(self._universe)

    async def fetch_candles(self, symbol: str, timeframe: str, start_ts: int,
                            end_ts: int, *, warmup_candles: int = 0,
                            progress_cb: Any = None) -> list[Candle]:
        self.last_fetch_provenance = {"symbol": symbol, "timeframe": timeframe}
        return [c for c in self._candles.get(symbol, []) if start_ts <= c.ts <= end_ts]

    async def fetch_htf_candles(self, symbol: str, ltf_timeframe: str,
                                htf_timeframe: str, start_ts: int, end_ts: int,
                                *, warmup_candles: int = 0,
                                progress_cb: Any = None) -> list[Candle]:
        return []

    async def fetch_funding_rates(self, symbol: str, start_ts: int, end_ts: int) -> list[Any]:
        return []

    async def fetch_instrument_specs(self, symbols: list[str]) -> dict[str, Any]:
        self.last_instrument_provenance = {"symbols": symbols}
        return {}


class TestEngineScreenerRun:
    def test_run_populates_universe_schedule(self) -> None:
        from app.services.backtest.engine import BacktestEngine

        start = 1_700_000_000_000
        # Two symbols with 48h of hourly candles; SC1 trends, MR1 chops.
        sc1 = [
            Candle(ts=start + i * _HOUR_MS, open=100 + i, high=101 + i, low=99 + i,
                   close=100 + i, volume=10, quote_volume=2_000_000)
            for i in range(48)
        ]
        mr1 = [
            Candle(ts=start + i * _HOUR_MS, open=100, high=105, low=95,
                   close=100 + (1 if i % 2 else -1), volume=10, quote_volume=1_500_000)
            for i in range(48)
        ]
        fetcher = _EngineFakeFetcher(
            {"SC1-USDT-SWAP": sc1, "MR1-USDT-SWAP": mr1},
            ["SC1-USDT-SWAP", "MR1-USDT-SWAP"],
        )

        config = BacktestConfig(
            symbols=[],
            timeframe="1H",
            start_ts=start + 24 * _HOUR_MS,
            end_ts=start + 47 * _HOUR_MS,
            strategy_names=["spike_continuation", "mean_reversion"],
            launcher_config={
                "notional_usd": 10.0,
                "strategies": {
                    "spike_continuation": {"enabled": True},
                    "mean_reversion": {"enabled": True},
                },
            },
            universe_mode="screener",
            screener_config=_cfg(),
            evaluation_mode="closed",
        )
        engine = BacktestEngine(config)
        engine._fetcher = fetcher  # type: ignore[assignment]
        result = asyncio.run(engine.run())

        assert result.error is None
        assert result.universe_schedule is not None
        assert result.universe_schedule["interval_count"] > 0
        assert result.universe_schedule["all_symbols"]
        # The engine resolved the union of screener-selected symbols.
        assert set(engine._symbols) >= set(result.universe_schedule["all_symbols"])

