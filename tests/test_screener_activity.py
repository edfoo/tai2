"""Tests for the screener's recent-activity window.

The 24h ticker fields are aggregates, so a coin that pumped hours ago and has
since gone quiet still ranks highly on them.  These tests cover the short-window
features that replace them:

  - ``compute_recent_activity`` feature derivation from raw OKX candle rows
  - ``timeframe_to_ms`` bar parsing
  - ``score_universe`` ranking on recent features instead of 24h aggregates
  - the ``activity_min_ratio`` gate
  - the faded-name diagnostic
  - backtest parity via ``_activity_at``
"""

from __future__ import annotations

from typing import Any

import pytest

from app.services.backtest.models import Candle
from app.services.backtest.universe import _activity_at
from app.services.screener import (
    compute_recent_activity,
    score_universe,
    timeframe_to_ms,
)

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
        "activity_lookback_hours": 4.0,
        "activity_min_ratio": 0.0,
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


def _row(ts: int, o: float, h: float, lo: float, c: float, quote_vol: float) -> list[Any]:
    """Build an OKX-style confirmed candle row."""
    return [ts, o, h, lo, c, quote_vol / c, 0.0, quote_vol, "1"]


def _flat_rows(start_ts: int, n: int, price: float = 100.0, quote_vol: float = 1_000.0):
    """n flat 1H candles — no movement, steady volume."""
    return [
        _row(start_ts + i * _HOUR_MS, price, price * 1.001, price * 0.999, price, quote_vol)
        for i in range(n)
    ]


# ── timeframe_to_ms ─────────────────────────────────────────────────────────


class TestTimeframeToMs:
    @pytest.mark.parametrize(
        ("bar", "expected"),
        [
            ("1m", 60_000),
            ("15m", 900_000),
            ("1H", 3_600_000),
            ("1h", 3_600_000),
            ("4H", 14_400_000),
            ("1D", 86_400_000),
        ],
    )
    def test_known_bars(self, bar: str, expected: int) -> None:
        assert timeframe_to_ms(bar) == expected

    def test_unknown_bar_falls_back_to_one_hour(self) -> None:
        assert timeframe_to_ms("bogus") == _HOUR_MS
        assert timeframe_to_ms(None) == _HOUR_MS


# ── compute_recent_activity ─────────────────────────────────────────────────


class TestComputeRecentActivity:
    def test_returns_none_without_candles(self) -> None:
        assert compute_recent_activity([]) is None
        assert compute_recent_activity(None) is None  # type: ignore[arg-type]

    def test_ignores_unconfirmed_forming_candle(self) -> None:
        start = 1_700_000_000_000
        rows = _flat_rows(start, 4)
        # Append a huge still-forming candle that must be ignored.
        forming = _row(start + 4 * _HOUR_MS, 100, 200, 50, 190, 9_999_999)
        forming[8] = "0"
        rows.append(forming)
        act = compute_recent_activity(rows, lookback_hours=3.0)
        assert act is not None
        assert act["candle_count"] == 3
        assert act["recent_range_pct"] < 1.0

    def test_window_uses_only_last_n_bars(self) -> None:
        start = 1_700_000_000_000
        # 10 quiet hours then 3 active ones.
        rows = _flat_rows(start, 10, quote_vol=1_000.0)
        active_start = start + 10 * _HOUR_MS
        rows += [
            _row(active_start + i * _HOUR_MS, 100, 105, 95, 104, 50_000.0)
            for i in range(3)
        ]
        act = compute_recent_activity(rows, lookback_hours=3.0)
        assert act is not None
        assert act["candle_count"] == 3
        assert act["recent_vol"] == pytest.approx(150_000.0)
        # Range is normalised by the last close (104), not the window open.
        assert act["recent_range_pct"] == pytest.approx(10.0 / 104.0 * 100.0)

    def test_recent_vol_ratio_normalises_against_own_24h_average(self) -> None:
        start = 1_700_000_000_000
        rows = _flat_rows(start, 3, quote_vol=10_000.0)
        # 3h of 10k = 30k.  A 24h volume of 240k implies 30k expected → ratio 1.0.
        act = compute_recent_activity(rows, lookback_hours=3.0, vol_ccy_24h=240_000.0)
        assert act is not None
        assert act["recent_vol_ratio"] == pytest.approx(1.0)

        # Same recent volume against a much larger 24h baseline → faded.
        act2 = compute_recent_activity(rows, lookback_hours=3.0, vol_ccy_24h=2_400_000.0)
        assert act2 is not None
        assert act2["recent_vol_ratio"] == pytest.approx(0.1)

    def test_decay_ratio_flags_faded_names(self) -> None:
        start = 1_700_000_000_000
        # Quiet recent window: 0.2% range.
        rows = _flat_rows(start, 3, quote_vol=10_000.0)
        # 24h range was 20%.  Range scales with sqrt(time), so the 3h baseline
        # is 20% × sqrt(3/24) ≈ 7.07% → decay ≈ 0.03.
        act = compute_recent_activity(rows, lookback_hours=3.0, hl_range_pct=20.0)
        assert act is not None
        assert act["decay_ratio"] is not None
        assert act["decay_ratio"] == pytest.approx(0.2 / (20.0 * (3 / 24) ** 0.5))
        assert act["decay_ratio"] < 0.25

    def test_decay_ratio_uses_sqrt_time_scaling(self) -> None:
        """A window whose range matches the sqrt-scaled baseline reads ~1.0."""
        start = 1_700_000_000_000
        # 3h window, 7.0% range (high 103.5 / low 96.5, normalised by close 100).
        rows = [
            _row(start + i * _HOUR_MS, 100, 103.5, 96.5, 100, 10_000.0)
            for i in range(3)
        ]
        act = compute_recent_activity(rows, lookback_hours=3.0, hl_range_pct=20.0)
        assert act is not None
        # Baseline = 20% × sqrt(3/24) ≈ 7.071%, so 7.0 / 7.071 ≈ 0.99.
        assert act["decay_ratio"] == pytest.approx(7.0 / (20.0 * (3 / 24) ** 0.5))

    def test_decay_ratio_not_inflated_by_linear_scaling(self) -> None:
        """Regression: linear time scaling made every real symbol read > 1."""
        start = 1_700_000_000_000
        # A genuinely quiet 3h window against a big 24h range must read < 1.
        rows = _flat_rows(start, 3, quote_vol=10_000.0)
        act = compute_recent_activity(rows, lookback_hours=3.0, hl_range_pct=7.58)
        assert act is not None
        # Linear scaling would give ~0.26; sqrt scaling gives ~0.15.
        assert act["decay_ratio"] < 0.25

    def test_signed_momentum_preserved(self) -> None:
        start = 1_700_000_000_000
        rows = [
            _row(start, 100, 100.5, 99.5, 100, 1_000),
            _row(start + _HOUR_MS, 100, 100.5, 89.5, 90, 1_000),
            _row(start + 2 * _HOUR_MS, 90, 90.5, 89.5, 90, 1_000),
        ]
        act = compute_recent_activity(rows, lookback_hours=3.0)
        assert act is not None
        assert act["recent_mom_pct"] == pytest.approx(-10.0)

    def test_as_of_ts_excludes_future_candles(self) -> None:
        start = 1_700_000_000_000
        rows = _flat_rows(start, 6)
        cutoff = start + 2 * _HOUR_MS
        act = compute_recent_activity(rows, lookback_hours=3.0, as_of_ts=cutoff)
        assert act is not None
        assert act["candle_count"] == 3

    def test_legacy_rows_fall_back_to_volume_times_close(self) -> None:
        start = 1_700_000_000_000
        # 6-field rows (no volCcyQuote) → quote volume approximated.
        rows = [[start + i * _HOUR_MS, 100, 101, 99, 100, 50.0] for i in range(3)]
        act = compute_recent_activity(rows, lookback_hours=3.0)
        assert act is not None
        assert act["recent_vol"] == pytest.approx(15_000.0)


# ── Volume units ────────────────────────────────────────────────────────────


class TestVolumeUnits:
    """OKX's raw ``volCcy24h`` is base-currency volume, not USDT.

    Comparing it directly against ``min_volume_usd`` silently rejected
    high-priced symbols (BTC's volCcy24h ≈ 55k reads as "55k USDT" against a
    10M floor) while letting low-priced tokens through on raw token counts.
    """

    def test_live_ticker_volume_is_converted_to_quote(self) -> None:
        from app.services.screener import build_candidates

        # BTC-like: 55,622 BTC × 81,000 = ~4.5B USDT.
        tickers = [
            {
                "instId": "BTC-USDT-SWAP",
                "last": 81_000.0,
                "open24h": 80_000.0,
                "high24h": 82_000.0,
                "low24h": 79_000.0,
                "volCcy24h": 55_622.0,  # base currency
            }
        ]
        cfg = _cfg(min_volume_usd=10_000_000)
        cands = build_candidates(tickers, cfg, vol_ccy_is_quote=False)
        assert len(cands) == 1
        assert cands[0]["vol_ccy_24h"] == pytest.approx(55_622.0 * 81_000.0)

    def test_high_priced_symbol_passes_volume_gate(self) -> None:
        """Regression: BTC/ETH/SOL were excluded by the 10M USDT floor."""
        from app.services.screener import build_candidates

        tickers = [
            {
                "instId": "BTC-USDT-SWAP",
                "last": 81_000.0,
                "open24h": 80_000.0,
                "high24h": 82_000.0,
                "low24h": 79_000.0,
                "volCcy24h": 55_622.0,
            }
        ]
        cands = build_candidates(tickers, _cfg(min_volume_usd=10_000_000))
        assert [c["symbol"] for c in cands] == ["BTC-USDT-SWAP"]

    def test_low_priced_symbol_not_admitted_on_token_count(self) -> None:
        """A token with a huge raw count but tiny USDT volume must be rejected."""
        from app.services.screener import build_candidates

        # 1e12 tokens × $0.000005 = $5M USDT — below the 10M floor.
        tickers = [
            {
                "instId": "TINY-USDT-SWAP",
                "last": 0.000005,
                "open24h": 0.000005,
                "high24h": 0.0000051,
                "low24h": 0.0000049,
                "volCcy24h": 1_000_000_000_000.0,
            }
        ]
        cands = build_candidates(tickers, _cfg(min_volume_usd=10_000_000))
        assert cands == []

    def test_backtest_quote_volume_not_double_converted(self) -> None:
        """The backtest already sums volCcyQuote, so it must skip the ×last."""
        from app.services.screener import build_candidates

        tickers = [
            {
                "instId": "BTC-USDT-SWAP",
                "last": 81_000.0,
                "open24h": 80_000.0,
                "high24h": 82_000.0,
                "low24h": 79_000.0,
                "volCcy24h": 4_500_000_000.0,  # already USDT
            }
        ]
        cands = build_candidates(
            tickers, _cfg(min_volume_usd=10_000_000), vol_ccy_is_quote=True
        )
        assert cands[0]["vol_ccy_24h"] == pytest.approx(4_500_000_000.0)

    def test_vol_ratio_uses_quote_units(self) -> None:
        """vol_ratio must not be inflated by the price factor."""
        from app.services.screener import build_candidates

        start = 1_700_000_000_000
        rows = _flat_rows(start, 4, quote_vol=10_000.0)  # 4h × 10k = 40k USDT
        tickers = [
            {
                "instId": "BTC-USDT-SWAP",
                "last": 100.0,
                "open24h": 100.0,
                "high24h": 101.0,
                "low24h": 99.0,
                "volCcy24h": 600.0,  # base units → 60,000 USDT
            }
        ]
        cands = build_candidates(tickers, _cfg(min_volume_usd=0))
        act = compute_recent_activity(
            rows,
            lookback_hours=4.0,
            vol_ccy_24h=cands[0]["vol_ccy_24h"],
        )
        assert act is not None
        # 40k recent vs 60k × (4/24) = 10k expected → ratio 4.0.
        assert act["recent_vol_ratio"] == pytest.approx(4.0)


# ── score_universe with activity ────────────────────────────────────────────


def _ticker(symbol: str, *, last: float, open24h: float, high: float, low: float,
            vol: float) -> dict[str, Any]:
    return {
        "instId": symbol,
        "last": last,
        "open24h": open24h,
        "high24h": high,
        "low24h": low,
        "volCcy24h": vol,
    }


class TestScoreUniverseActivity:
    def test_faded_coin_loses_to_live_coin(self) -> None:
        """A big 24h move that already happened must not outrank a live mover."""
        tickers = [
            # FADED: huge 24h move/range, but flat and quiet in the last 3h.
            _ticker("FADED-USDT-SWAP", last=130, open24h=100, high=135, low=98,
                    vol=50_000_000),
            # LIVE: modest 24h move, but moving hard right now.
            _ticker("LIVE-USDT-SWAP", last=104, open24h=100, high=105, low=99,
                    vol=20_000_000),
        ]
        activity = {
            "FADED-USDT-SWAP": {
                "recent_vol": 100_000.0,
                "recent_vol_ratio": 0.05,
                "recent_range_pct": 0.2,
                "recent_mom_pct": 0.1,
                "decay_ratio": 0.02,
            },
            "LIVE-USDT-SWAP": {
                "recent_vol": 8_000_000.0,
                "recent_vol_ratio": 3.2,
                "recent_range_pct": 4.5,
                "recent_mom_pct": 3.8,
                "decay_ratio": 1.8,
            },
        }
        result = score_universe(tickers, _cfg(), activity=activity)
        assert result["activity_used"] is True
        assert result["sc"][0] == "LIVE-USDT-SWAP"
        assert "FADED-USDT-SWAP" not in result["sc"]

    def test_activity_disabled_uses_24h_aggregates(self) -> None:
        tickers = [
            _ticker("FADED-USDT-SWAP", last=130, open24h=100, high=135, low=98,
                    vol=50_000_000),
            _ticker("LIVE-USDT-SWAP", last=104, open24h=100, high=105, low=99,
                    vol=20_000_000),
        ]
        activity = {
            "FADED-USDT-SWAP": {"recent_range_pct": 0.2, "recent_mom_pct": 0.1,
                                "recent_vol_ratio": 0.05, "decay_ratio": 0.02},
            "LIVE-USDT-SWAP": {"recent_range_pct": 4.5, "recent_mom_pct": 3.8,
                               "recent_vol_ratio": 3.2, "decay_ratio": 1.8},
        }
        # activity_lookback_hours=0 disables the window entirely.
        result = score_universe(
            tickers, _cfg(activity_lookback_hours=0.0), activity=activity
        )
        assert result["activity_used"] is False
        # Legacy behaviour: the big 24h mover wins.
        assert result["sc"][0] == "FADED-USDT-SWAP"

    def test_missing_activity_falls_back_per_symbol(self) -> None:
        tickers = [
            _ticker("A-USDT-SWAP", last=110, open24h=100, high=112, low=99, vol=5_000_000),
            _ticker("B-USDT-SWAP", last=103, open24h=100, high=104, low=99, vol=5_000_000),
        ]
        # Only A has activity data; B must still be scored on 24h features.
        activity = {
            "A-USDT-SWAP": {"recent_range_pct": 0.1, "recent_mom_pct": 0.05,
                            "recent_vol_ratio": 0.1, "decay_ratio": 0.01},
        }
        result = score_universe(tickers, _cfg(), activity=activity)
        assert result["activity_used"] is True
        assert result["sc"][0] == "B-USDT-SWAP"

    def test_activity_min_ratio_gate(self) -> None:
        tickers = [
            _ticker("QUIET-USDT-SWAP", last=110, open24h=100, high=112, low=99,
                    vol=5_000_000),
            _ticker("BUSY-USDT-SWAP", last=105, open24h=100, high=106, low=99,
                    vol=5_000_000),
        ]
        activity = {
            "QUIET-USDT-SWAP": {"recent_range_pct": 5.0, "recent_mom_pct": 4.0,
                                "recent_vol_ratio": 0.3, "decay_ratio": 0.5},
            "BUSY-USDT-SWAP": {"recent_range_pct": 2.0, "recent_mom_pct": 1.5,
                               "recent_vol_ratio": 2.0, "decay_ratio": 1.2},
        }
        result = score_universe(
            tickers, _cfg(activity_min_ratio=1.0), activity=activity
        )
        assert "QUIET-USDT-SWAP" not in result["sc"]
        assert "BUSY-USDT-SWAP" in result["sc"]

    def test_faded_diagnostic_reported(self) -> None:
        tickers = [
            _ticker("FADED-USDT-SWAP", last=130, open24h=100, high=135, low=98,
                    vol=50_000_000),
        ]
        activity = {
            "FADED-USDT-SWAP": {"recent_range_pct": 0.2, "recent_mom_pct": 0.1,
                                "recent_vol_ratio": 0.05, "decay_ratio": 0.02},
        }
        result = score_universe(tickers, _cfg(), activity=activity)
        joined = "\n".join(result["diagnostics"])
        assert "faded" in joined.lower()
        assert "FADED-USDT-SWAP" in joined

    def test_prebuilt_candidates_not_rebuilt(self) -> None:
        """Passing candidates must skip the prefilter (no double vol_history append)."""
        from app.services.screener import build_candidates

        tickers = [
            _ticker("A-USDT-SWAP", last=110, open24h=100, high=112, low=99, vol=5_000_000),
        ]
        history: dict[str, Any] = {}
        candidates = build_candidates(tickers, _cfg(), vol_history=history)
        assert len(history["A-USDT-SWAP"]) == 1
        score_universe(
            tickers, _cfg(), vol_history=history, candidates=candidates
        )
        # Still one sample — the prefilter did not run a second time.
        assert len(history["A-USDT-SWAP"]) == 1


# ── Backtest parity ─────────────────────────────────────────────────────────


class TestBacktestActivityParity:
    def test_activity_at_matches_live_features(self) -> None:
        start = 1_700_000_000_000
        candles = [
            Candle(
                ts=start + i * _HOUR_MS,
                open=100.0,
                high=105.0,
                low=95.0,
                close=104.0,
                volume=1_000.0,
                quote_volume=50_000.0,
            )
            for i in range(6)
        ]
        ts_list = [c.ts for c in candles]
        ts = candles[-1].ts
        act = _activity_at(
            candles,
            ts_list,
            ts,
            lookback_hours=3.0,
            timeframe_ms=_HOUR_MS,
            vol_ccy_24h=1_200_000.0,
            hl_range_pct=10.0,
        )
        assert act is not None
        assert act["candle_count"] == 3
        assert act["recent_vol"] == pytest.approx(150_000.0)
        assert act["recent_range_pct"] == pytest.approx(10.0 / 104.0 * 100.0)
        assert act["recent_vol_ratio"] == pytest.approx(1.0)

    def test_activity_at_returns_none_before_first_candle(self) -> None:
        start = 1_700_000_000_000
        candles = [
            Candle(ts=start, open=100, high=101, low=99, close=100, volume=1, quote_volume=1)
        ]
        assert _activity_at(
            candles,
            [c.ts for c in candles],
            start - _HOUR_MS,
            lookback_hours=3.0,
            timeframe_ms=_HOUR_MS,
            vol_ccy_24h=None,
            hl_range_pct=None,
        ) is None


# ── Live fetch path ─────────────────────────────────────────────────────────


class TestLiveActivityFetch:
    """The live service fetches short-window candles for prefiltered candidates."""

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

    def test_fetches_and_caches_activity(self) -> None:
        import asyncio

        service = self._service()
        start = 1_700_000_000_000
        rows = _flat_rows(start, 5, quote_vol=10_000.0)
        calls: list[str] = []

        async def fake_fetch(symbol, bar, cache, *, limit=None):
            calls.append(symbol)
            return rows

        service._fetch_ohlcv_bar = fake_fetch  # type: ignore[method-assign]
        candidates = [
            {"symbol": "A-USDT-SWAP", "vol_ccy_24h": 240_000.0, "hl_range_pct": 5.0},
            {"symbol": "B-USDT-SWAP", "vol_ccy_24h": 240_000.0, "hl_range_pct": 5.0},
        ]
        cfg = _cfg(activity_cache_seconds=300.0)

        async def scenario():
            first = await service._fetch_recent_activity(candidates, cfg)
            second = await service._fetch_recent_activity(candidates, cfg)
            return first, second

        first, second = asyncio.run(scenario())
        assert set(first) == {"A-USDT-SWAP", "B-USDT-SWAP"}
        # Default 4h window over 1H candles = 4 bars × 10k quote volume.
        assert first["A-USDT-SWAP"]["recent_vol"] == pytest.approx(40_000.0)
        # Second call is served from cache — no additional fetches.
        assert len(calls) == 2
        assert second == first

    def test_disabled_window_skips_fetch(self) -> None:
        import asyncio

        service = self._service()
        calls: list[str] = []

        async def fake_fetch(symbol, bar, cache, *, limit=None):
            calls.append(symbol)
            return []

        service._fetch_ohlcv_bar = fake_fetch  # type: ignore[method-assign]
        candidates = [{"symbol": "A-USDT-SWAP", "vol_ccy_24h": 1.0, "hl_range_pct": 1.0}]
        result = asyncio.run(
            service._fetch_recent_activity(
                candidates, _cfg(activity_lookback_hours=0.0)
            )
        )
        assert result == {}
        assert calls == []

    def test_prefilter_cap_keeps_highest_volume(self) -> None:
        import asyncio

        service = self._service()
        fetched: list[str] = []

        async def fake_fetch(symbol, bar, cache, *, limit=None):
            fetched.append(symbol)
            return _flat_rows(1_700_000_000_000, 5)

        service._fetch_ohlcv_bar = fake_fetch  # type: ignore[method-assign]
        candidates = [
            {"symbol": f"S{i}-USDT-SWAP", "vol_ccy_24h": float(i), "hl_range_pct": 1.0}
            for i in range(10)
        ]
        asyncio.run(
            service._fetch_recent_activity(
                candidates, _cfg(activity_prefilter_top_n=3)
            )
        )
        assert sorted(fetched) == ["S7-USDT-SWAP", "S8-USDT-SWAP", "S9-USDT-SWAP"]
