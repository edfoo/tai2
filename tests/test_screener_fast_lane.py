"""Tests for the screener's fast lane (short-window move discovery).

The 24h volume gate in ``build_candidates`` is a *lagging* indicator: a coin
starting from a quiet baseline cannot reach ``min_volume_usd`` until the pump's
own candles have printed.  SHELL-USDT-SWAP is the motivating case — it moved
+72% in four hours but only crossed the 10M floor as the move ended.

The fast lane admits symbols on a short-window price move instead, with a much
lower tradability-only volume floor.  These tests pin down:

  - ``_fast_move_from_history`` window selection and guards
  - ``fast_lane_candidates`` gating (threshold, volume floor, spread, pattern)
  - the cold-start ``sodUtc0`` fallback and its staleness guard
  - ``merge_candidates`` dedupe semantics
  - ``score_universe`` accepting lane admissions without changing scoring
  - the activity fetch exempting lane symbols from the volume-ranked top-N cap
  - backtest parity via ``_price_history_at``
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from app.services.screener import (
    _fast_move_from_history,
    fast_lane_candidates,
    merge_candidates,
    score_universe,
)

_HOUR = 3_600.0


def _cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "enabled": True,
        "dual_universe": True,
        "universe_filter": "*-USDT-SWAP",
        "max_symbols": 10,
        "sc_max_symbols": 3,
        "mr_max_symbols": 3,
        "min_volume_usd": 10_000_000,
        "max_spread_pct": 0.0,
        "activity_lookback_hours": 0.0,
        "activity_min_ratio": 0.0,
        "sc_min_momentum_pct": 0.5,
        "sc_min_hl_range_pct": 0.0,
        "mr_min_hl_range_pct": 1.0,
        "mr_max_momentum_pct": 8.0,
        "min_momentum_pct": 0.5,
        "min_hl_range_pct": 0.0,
        "vol_history_window": 8,
        "interval_minutes": 60,
        # Fast lane defaults under test.
        "fast_lane_enabled": True,
        "fast_move_threshold_pct": 4.0,
        "fast_move_window_minutes": 60.0,
        "fast_lane_min_volume_usd": 250_000.0,
        "fast_lane_max_symbols": 40,
        "fast_lane_sod_max_age_mult": 3.0,
    }
    cfg.update(overrides)
    return cfg


def _ticker(
    symbol: str,
    *,
    last: float,
    vol_quote: float = 400_000.0,
    open24h: float | None = None,
    high24h: float | None = None,
    low24h: float | None = None,
    sod: float | None = None,
    bid: float | None = None,
    ask: float | None = None,
) -> dict[str, Any]:
    """Build an OKX-style ticker.

    ``vol_quote`` is the *quote-currency* volume; ``volCcy24h`` carries base
    volume, so it is divided by ``last`` to mimic the live payload.
    """
    ticker: dict[str, Any] = {
        "instId": symbol,
        "last": str(last),
        "volCcy24h": str(vol_quote / last),
        "open24h": str(open24h if open24h is not None else last),
        "high24h": str(high24h if high24h is not None else last),
        "low24h": str(low24h if low24h is not None else last),
    }
    if sod is not None:
        ticker["sodUtc0"] = str(sod)
    if bid is not None:
        ticker["bidPx"] = str(bid)
    if ask is not None:
        ticker["askPx"] = str(ask)
    return ticker


def _history(points: list[tuple[float, float]]) -> deque:
    return deque(points)


# ── _fast_move_from_history ─────────────────────────────────────────────────


class TestFastMoveFromHistory:
    def test_measures_move_across_the_window(self) -> None:
        now = 1_700_000_000.0
        hist = _history(
            [
                (now - 90 * 60, 100.0),
                (now - 60 * 60, 100.0),
                (now - 30 * 60, 110.0),
                (now, 115.0),
            ]
        )
        measured = _fast_move_from_history(hist, now_ts=now, window_seconds=3600.0)
        assert measured is not None
        move_pct, span, source = measured
        # Base is the newest sample at/before now-3600s → the 60m-ago point.
        assert move_pct == pytest.approx(15.0)
        assert span == pytest.approx(3600.0)
        assert source == "history"

    def test_too_few_samples_returns_none(self) -> None:
        now = 1_700_000_000.0
        hist = _history([(now - 3600, 100.0), (now, 110.0)])
        assert _fast_move_from_history(hist, now_ts=now, window_seconds=3600.0) is None

    def test_short_span_is_rejected(self) -> None:
        """Samples covering <50% of the window are noise between adjacent polls."""
        now = 1_700_000_000.0
        hist = _history(
            [(now - 120, 100.0), (now - 60, 101.0), (now, 102.0)]
        )
        assert _fast_move_from_history(hist, now_ts=now, window_seconds=3600.0) is None

    def test_empty_and_none_handled(self) -> None:
        assert _fast_move_from_history([], now_ts=0, window_seconds=3600) is None
        assert _fast_move_from_history([], now_ts=0, window_seconds=3600) is None

    def test_zero_price_samples_skipped(self) -> None:
        now = 1_700_000_000.0
        hist = _history([(now - 3600, 0.0), (now - 1800, 0.0), (now, 100.0)])
        assert _fast_move_from_history(hist, now_ts=now, window_seconds=3600.0) is None


# ── fast_lane_candidates ────────────────────────────────────────────────────


class TestFastLaneCandidates:
    def test_disabled_returns_empty(self) -> None:
        now = 1_700_000_000.0
        tickers = [_ticker("HOT-USDT-SWAP", last=200.0)]
        hist = {"HOT-USDT-SWAP": _history([(now - 3600, 100.0), (now, 200.0), (now, 200.0)])}
        assert fast_lane_candidates(tickers, hist, _cfg(fast_lane_enabled=False), now_ts=now) == []

    def test_admits_symbol_above_threshold_below_volume_gate(self) -> None:
        """The SHELL case: big move, but 24h volume far under min_volume_usd."""
        now = 1_700_000_000.0
        tickers = [_ticker("SHELL-USDT-SWAP", last=130.0, vol_quote=400_000.0)]
        hist = {
            "SHELL-USDT-SWAP": _history(
                [(now - 3600, 100.0), (now - 1800, 115.0), (now, 130.0)]
            )
        }
        admitted = fast_lane_candidates(tickers, hist, _cfg(), now_ts=now)
        assert [c["symbol"] for c in admitted] == ["SHELL-USDT-SWAP"]
        assert admitted[0]["_fast_lane"] is True
        assert admitted[0]["fast_move_pct"] == pytest.approx(30.0)
        assert admitted[0]["fast_source"] == "history"
        # Confirms the base pool would have excluded it.
        assert admitted[0]["vol_ccy_24h"] < _cfg()["min_volume_usd"]

    def test_rejects_small_move(self) -> None:
        now = 1_700_000_000.0
        tickers = [_ticker("CALM-USDT-SWAP", last=102.0, vol_quote=400_000.0)]
        hist = {"CALM-USDT-SWAP": _history([(now - 3600, 100.0), (now, 102.0), (now, 102.0)])}
        assert fast_lane_candidates(tickers, hist, _cfg(), now_ts=now) == []

    def test_rejects_illiquid_symbol(self) -> None:
        """Below the tradability floor, however big the move."""
        now = 1_700_000_000.0
        tickers = [_ticker("THIN-USDT-SWAP", last=200.0, vol_quote=50_000.0)]
        hist = {"THIN-USDT-SWAP": _history([(now - 3600, 100.0), (now, 200.0), (now, 200.0)])}
        assert fast_lane_candidates(tickers, hist, _cfg(), now_ts=now) == []

    def test_rejects_wide_spread(self) -> None:
        now = 1_700_000_000.0
        tickers = [
            _ticker(
                "WIDE-USDT-SWAP",
                last=200.0,
                vol_quote=400_000.0,
                bid=199.0,
                ask=201.0,  # ~1.0% spread
            )
        ]
        hist = {"WIDE-USDT-SWAP": _history([(now - 3600, 100.0), (now, 200.0), (now, 200.0)])}
        cfg = _cfg(max_spread_pct=0.5)
        assert fast_lane_candidates(tickers, hist, cfg, now_ts=now) == []

    def test_rejects_non_matching_pattern(self) -> None:
        now = 1_700_000_000.0
        tickers = [_ticker("HOT-USD-SWAP", last=200.0, vol_quote=400_000.0)]
        hist = {"HOT-USD-SWAP": _history([(now - 3600, 100.0), (now, 200.0), (now, 200.0)])}
        assert fast_lane_candidates(tickers, hist, _cfg(), now_ts=now) == []

    def test_negative_move_also_admitted(self) -> None:
        now = 1_700_000_000.0
        tickers = [_ticker("DUMP-USDT-SWAP", last=70.0, vol_quote=400_000.0)]
        hist = {"DUMP-USDT-SWAP": _history([(now - 3600, 100.0), (now, 85.0), (now, 70.0)])}
        admitted = fast_lane_candidates(tickers, hist, _cfg(), now_ts=now)
        assert [c["symbol"] for c in admitted] == ["DUMP-USDT-SWAP"]
        assert admitted[0]["fast_move_pct"] == pytest.approx(-30.0)

    def test_cap_keeps_biggest_moves(self) -> None:
        now = 1_700_000_000.0
        tickers = []
        hist: dict[str, deque] = {}
        for i, move in enumerate([5.0, 30.0, 12.0, 7.0]):
            sym = f"M{i}-USDT-SWAP"
            tickers.append(_ticker(sym, last=100.0 + move, vol_quote=400_000.0))
            hist[sym] = _history([(now - 3600, 100.0), (now, 100.0 + move), (now, 100.0 + move)])
        admitted = fast_lane_candidates(
            tickers, hist, _cfg(fast_lane_max_symbols=2), now_ts=now
        )
        assert [c["symbol"] for c in admitted] == ["M1-USDT-SWAP", "M2-USDT-SWAP"]

    # ── Cold-start sodUtc0 fallback ─────────────────────────────────
    def test_cold_start_uses_sod_when_history_short(self) -> None:
        # 01:00 UTC → 3600s since midnight, inside the 60m × 3 window.
        now = 1_700_000_000.0
        now -= now % 86_400  # align to midnight
        now += 3_600  # 01:00
        tickers = [_ticker("NEW-USDT-SWAP", last=110.0, vol_quote=400_000.0, sod=100.0)]
        admitted = fast_lane_candidates(
            tickers, {}, _cfg(), now_ts=now
        )
        assert [c["symbol"] for c in admitted] == ["NEW-USDT-SWAP"]
        assert admitted[0]["fast_source"] == "sodUtc0"
        assert admitted[0]["fast_move_pct"] == pytest.approx(10.0)

    def test_cold_start_rejected_when_session_too_old(self) -> None:
        """A move that happened hours ago must not read as fresh."""
        now = 1_700_000_000.0
        now -= now % 86_400
        now += 20 * 3600  # 20:00 — session age 20h ≫ 60m × 3
        tickers = [_ticker("OLD-USDT-SWAP", last=110.0, vol_quote=400_000.0, sod=100.0)]
        assert fast_lane_candidates(tickers, {}, _cfg(), now_ts=now) == []

    def test_cold_start_requires_sod_field(self) -> None:
        now = 1_700_000_000.0
        now -= now % 86_400
        now += 3_600
        tickers = [_ticker("NOSOD-USDT-SWAP", last=110.0, vol_quote=400_000.0)]
        assert fast_lane_candidates(tickers, {}, _cfg(), now_ts=now) == []


# ── merge_candidates ────────────────────────────────────────────────────────


class TestMergeCandidates:
    def test_union_by_symbol(self) -> None:
        base = [{"symbol": "A-USDT-SWAP", "vol_spike_ratio": 2.0}]
        extra = [{"symbol": "B-USDT-SWAP", "_fast_lane": True}]
        merged = merge_candidates(base, extra)
        assert sorted(c["symbol"] for c in merged) == ["A-USDT-SWAP", "B-USDT-SWAP"]

    def test_duplicate_keeps_base_features_and_annotates(self) -> None:
        base = [{"symbol": "A-USDT-SWAP", "vol_spike_ratio": 2.0}]
        extra = [
            {
                "symbol": "A-USDT-SWAP",
                "vol_spike_ratio": None,
                "fast_move_pct": 9.0,
                "fast_span_seconds": 3600.0,
                "fast_source": "history",
                "_fast_lane": True,
            }
        ]
        merged = merge_candidates(base, extra)
        assert len(merged) == 1
        # Base entry wins, so the rolling spike ratio survives.
        assert merged[0]["vol_spike_ratio"] == 2.0
        assert merged[0]["fast_move_pct"] == 9.0
        assert merged[0]["fast_source"] == "history"

    def test_empty_inputs(self) -> None:
        assert merge_candidates([], []) == []
        assert merge_candidates([], [{"symbol": "A-USDT-SWAP"}]) == [{"symbol": "A-USDT-SWAP"}]


# ── score_universe integration ──────────────────────────────────────────────


class TestScoreUniverseWithFastLane:
    def test_lane_symbol_joins_pool_and_can_be_selected(self) -> None:
        """A lane symbol below the volume gate is still scored and selectable."""
        tickers = [
            _ticker("SHELL-USDT-SWAP", last=130.0, vol_quote=400_000.0, open24h=100.0,
                    high24h=131.0, low24h=99.0),
        ]
        cfg = _cfg()
        # Without the lane: nothing passes min_volume_usd.
        base = score_universe(tickers, cfg, vol_ccy_is_quote=False)
        assert base["selected"] == []

        lane = [
            {
                "symbol": "SHELL-USDT-SWAP",
                "vol_ccy_24h": 400_000.0,
                "vol_spike_ratio": None,
                "hl_range_pct": 32.0,
                "momentum_pct": 30.0,
                "spread_pct": None,
                "fast_move_pct": 30.0,
                "fast_span_seconds": 3600.0,
                "fast_source": "history",
                "_fast_lane": True,
            }
        ]
        result = score_universe(tickers, cfg, vol_ccy_is_quote=False, fast_lane=lane)
        assert "SHELL-USDT-SWAP" in result["selected"]
        assert result["fast_lane_count"] == 1
        assert result["fast_lane_symbols"] == ["SHELL-USDT-SWAP"]
        assert any("fast lane" in d for d in result["diagnostics"])

    def test_lane_does_not_bypass_activity_scoring(self) -> None:
        """Admission ≠ selection: a lane symbol with no movement still loses."""
        tickers = [
            _ticker("FLAT-USDT-SWAP", last=100.0, vol_quote=400_000.0, open24h=100.0,
                    high24h=100.1, low24h=99.9),
        ]
        cfg = _cfg(sc_min_momentum_pct=5.0, mr_min_hl_range_pct=5.0)
        lane = [
            {
                "symbol": "FLAT-USDT-SWAP",
                "vol_ccy_24h": 400_000.0,
                "vol_spike_ratio": None,
                "hl_range_pct": 0.2,
                "momentum_pct": 0.0,
                "spread_pct": None,
                "fast_move_pct": 0.5,
                "_fast_lane": True,
            }
        ]
        result = score_universe(tickers, cfg, vol_ccy_is_quote=False, fast_lane=lane)
        assert result["selected"] == []
        assert result["fast_lane_count"] == 1  # still reported in diagnostics

    def test_no_lane_keeps_result_shape(self) -> None:
        result = score_universe([], _cfg(), vol_ccy_is_quote=False)
        assert result["fast_lane_count"] == 0
        assert result["fast_lane_symbols"] == []


# ── Activity fetch cap exemption ────────────────────────────────────────────


class TestActivityFetchCapExemption:
    """Lane symbols must not be squeezed out by the volume-ranked top-N cap."""

    def _service(self):
        from app.services.market_service import MarketService

        class _State:
            async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
                pass

            async def get_market_snapshot(self) -> dict[str, Any]:
                return {"positions": []}

        return MarketService(
            state_service=_State(),
            enable_websocket=False,
            account_api=object(),
            market_api=object(),
            public_api=object(),
            trade_api=object(),
        )

    def test_lane_symbol_fetched_despite_cap(self) -> None:
        service = self._service()
        fetched: list[str] = []

        async def fake_fetch(symbol, bar, cache, *, limit=None):
            fetched.append(symbol)
            return _flat_activity_rows()

        service._fetch_ohlcv_bar = fake_fetch  # type: ignore[method-assign]
        # 5 high-volume symbols + 1 lane symbol with tiny volume.
        candidates = [
            {"symbol": f"BIG{i}-USDT-SWAP", "vol_ccy_24h": 9_000_000.0, "hl_range_pct": 1.0}
            for i in range(5)
        ]
        candidates.append(
            {
                "symbol": "SHELL-USDT-SWAP",
                "vol_ccy_24h": 300_000.0,
                "hl_range_pct": 30.0,
                "_fast_lane": True,
            }
        )
        cfg = _cfg(activity_lookback_hours=4.0, activity_prefilter_top_n=3, activity_cache_seconds=0.0)
        asyncio.run(service._fetch_recent_activity(candidates, cfg))
        assert "SHELL-USDT-SWAP" in fetched
        # Cap still applies to the non-lane candidates.
        assert len([s for s in fetched if s.startswith("BIG")]) == 3


def _flat_activity_rows(n: int = 5) -> list[list[Any]]:
    """n flat confirmed 1H candle rows."""
    start = 1_700_000_000_000
    return [
        [start + i * 3_600_000, 100.0, 100.1, 99.9, 100.0, 100.0, 0.0, 10_000.0, "1"]
        for i in range(n)
    ]


# ── Backtest parity ─────────────────────────────────────────────────────────


class TestBacktestParity:
    def test_price_history_reconstructs_from_candles(self) -> None:
        from app.services.backtest.models import Candle
        from app.services.backtest.universe import _price_history_at

        start = 1_700_000_000_000
        candles = [
            Candle(
                ts=start + i * 3_600_000,
                open=100.0 + i,
                high=100.5 + i,
                low=99.5 + i,
                close=100.0 + i,
                volume=1.0,
                quote_volume=1_000.0,
            )
            for i in range(10)
        ]
        ts_list = [c.ts for c in candles]
        hist = _price_history_at(candles, ts_list, candles[-1].ts, window_minutes=60.0)
        assert len(hist) >= 2
        # Newest sample is the close of the interval candle.
        assert hist[-1] == pytest.approx((candles[-1].ts / 1000.0, 109.0))

    def test_backtest_lane_matches_live_semantics(self) -> None:
        """A reconstructed deque drives the same admission decision as live."""
        now = 1_700_000_000.0
        hist = {
            "SHELL-USDT-SWAP": _history(
                [(now - 3600, 100.0), (now - 1800, 115.0), (now, 130.0)]
            )
        }
        tickers = [_ticker("SHELL-USDT-SWAP", last=130.0, vol_quote=400_000.0)]
        live = fast_lane_candidates(tickers, hist, _cfg(), now_ts=now)
        # The backtest passes the same structure with quote volume flagged.
        bt = fast_lane_candidates(
            [
                {
                    "instId": "SHELL-USDT-SWAP",
                    "last": 130.0,
                    # volCcy24h is already quote volume in the backtest.
                    "volCcy24h": 400_000.0,
                    "open24h": 130.0,
                    "high24h": 130.0,
                    "low24h": 130.0,
                }
            ],
            hist,
            _cfg(),
            now_ts=now,
            vol_ccy_is_quote=True,
        )
        assert [c["symbol"] for c in live] == [c["symbol"] for c in bt]
        assert live[0]["fast_move_pct"] == pytest.approx(bt[0]["fast_move_pct"])