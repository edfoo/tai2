from __future__ import annotations

import pytest

from app.services.backtest.data_fetcher import HistoricalDataFetcher
from app.services.backtest.models import Candle


@pytest.mark.asyncio
async def test_cached_candle_fetch_exposes_stable_content_provenance(tmp_path) -> None:
    fetcher = HistoricalDataFetcher(cache_dir=tmp_path)
    candles = [
        Candle(ts=1000, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0),
        Candle(ts=2000, open=1.5, high=2.5, low=1.0, close=2.0, volume=12.0),
    ]
    key = fetcher._cache_key("BTC-USDT-SWAP", "1m", 1000, 3000, 50)
    fetcher._save_cache(key, candles)

    result = await fetcher.fetch_candles(
        "BTC-USDT-SWAP", "1m", 1000, 3000, warmup_candles=50
    )
    first_provenance = fetcher.last_fetch_provenance
    await fetcher.fetch_candles(
        "BTC-USDT-SWAP", "1m", 1000, 3000, warmup_candles=50
    )
    second_provenance = fetcher.last_fetch_provenance

    assert result == candles
    assert first_provenance["source"] == "file_cache"
    assert first_provenance["cache_hit"] is True
    assert first_provenance["candle_count"] == 2
    assert first_provenance["first_candle_ts"] == 1000
    assert first_provenance["last_candle_ts"] == 2000
    assert len(first_provenance["content_sha256"]) == 64
    assert first_provenance["content_sha256"] == second_provenance["content_sha256"]


@pytest.mark.asyncio
async def test_unavailable_fetch_records_empty_source_provenance(tmp_path) -> None:
    fetcher = HistoricalDataFetcher(cache_dir=tmp_path)
    fetcher._api = None

    candles = await fetcher.fetch_candles(
        "ETH-USDT-SWAP", "5m", 1000, 3000, warmup_candles=10
    )

    assert candles == []
    assert fetcher.last_fetch_provenance["source"] == "unavailable"
    assert fetcher.last_fetch_provenance["candle_count"] == 0


@pytest.mark.asyncio
async def test_funding_and_instrument_specs_are_cached_and_fingerprinted(tmp_path, monkeypatch) -> None:
    import app.services.okx_metrics as okx_metrics

    async def fake_funding(_symbol, _start, _end):
        return [{"ts": 100, "rate": 0.0001}]

    async def fake_get(path, _params):
        if path.endswith("/position-tiers"):
            return [{
                "minSz": "0",
                "maxSz": "100",
                "imr": "0.2",
                "mmr": "0.1",
                "maxLever": "5",
                "mmrDeduction": "0",
            }]
        return [{
            "instId": "BTC-USDT-SWAP",
            "ctVal": "0.01",
            "lotSz": "1",
            "minSz": "1",
            "tickSz": "0.1",
            "maxMktSz": "1000",
            "maxLmtSz": "2000",
            "ctType": "linear",
        }]

    monkeypatch.setattr(okx_metrics, "fetch_funding_history_records", fake_funding)
    monkeypatch.setattr(okx_metrics, "_get", fake_get)
    fetcher = HistoricalDataFetcher(cache_dir=tmp_path)

    rates = await fetcher.fetch_funding_rates("BTC-USDT-SWAP", 0, 200)
    funding_source = fetcher.last_funding_provenance
    specs = await fetcher.fetch_instrument_specs(["BTC-USDT-SWAP"])
    instrument_source = fetcher.last_instrument_provenance

    assert rates == [{"ts": 100, "rate": 0.0001}]
    assert funding_source["source"] == "okx_public_api"
    assert len(funding_source["content_sha256"]) == 64
    assert specs["BTC-USDT-SWAP"] == {
        "ct_val": 0.01,
        "lot_size": 1.0,
        "min_size": 1.0,
        "tick_size": 0.1,
        "max_market_size": 1000.0,
        "max_limit_size": 2000.0,
        "contract_type": "linear",
        "position_tiers": [{
            "min_size": 0.0,
            "max_size": 100.0,
            "initial_margin_ratio": 0.2,
            "maintenance_margin_ratio": 0.1,
            "max_leverage": 5.0,
            "maintenance_deduction": 0.0,
        }],
    }
    assert instrument_source["source"] == "okx_public_api"
    cached = HistoricalDataFetcher(cache_dir=tmp_path)
    assert await cached.fetch_funding_rates("BTC-USDT-SWAP", 0, 200) == rates
    assert cached.last_funding_provenance["source"] == "file_cache"
    assert await cached.fetch_instrument_specs(["BTC-USDT-SWAP"]) == specs
    assert cached.last_instrument_provenance["source"] == "file_cache"

def _swap_rows(*ids: str) -> list[dict[str, str]]:
    return [{"instId": i} for i in ids]


@pytest.mark.asyncio
async def test_swap_universe_refreshes_stale_cache_and_keeps_delisted(tmp_path, monkeypatch) -> None:
    import json
    import os

    import app.services.okx_metrics as okx_metrics
    from app.services.backtest import data_fetcher as df

    calls: list[str] = []

    async def fake_get(path, _params):
        calls.append(path)
        return _swap_rows("BTC-USDT-SWAP", "NEW-USDT-SWAP", "BTC-USD-SWAP")

    monkeypatch.setattr(okx_metrics, "_get", fake_get)
    cache = tmp_path / "swap_universe.json"
    cache.write_text(json.dumps(["BTC-USDT-SWAP", "GONE-USDT-SWAP"]), encoding="utf-8")
    fetcher = HistoricalDataFetcher(cache_dir=tmp_path)

    # Fresh cache: reused, no API call.
    assert await fetcher.fetch_swap_universe() == ["BTC-USDT-SWAP", "GONE-USDT-SWAP"]
    assert calls == []
    assert fetcher.last_universe_provenance["source"] == "file_cache"

    # Stale cache: refetched; new listing added, delisted coin kept, non-USDT dropped.
    old = cache.stat().st_mtime - df.SWAP_UNIVERSE_MAX_AGE_SECONDS - 60
    os.utime(cache, (old, old))
    syms = await fetcher.fetch_swap_universe()
    assert syms == ["BTC-USDT-SWAP", "GONE-USDT-SWAP", "NEW-USDT-SWAP"]
    assert len(calls) == 1
    prov = fetcher.last_universe_provenance
    assert prov["source"] == "okx_public_api" and prov["cache_age_hours"] == 0.0
    assert json.loads(cache.read_text(encoding="utf-8")) == syms


@pytest.mark.asyncio
async def test_swap_universe_falls_back_to_stale_cache_when_refresh_fails(tmp_path, monkeypatch) -> None:
    import json
    import os

    import app.services.okx_metrics as okx_metrics
    from app.services.backtest import data_fetcher as df

    async def failing_get(_path, _params):
        raise RuntimeError("network down")

    monkeypatch.setattr(okx_metrics, "_get", failing_get)
    cache = tmp_path / "swap_universe.json"
    cache.write_text(json.dumps(["BTC-USDT-SWAP"]), encoding="utf-8")
    old = cache.stat().st_mtime - df.SWAP_UNIVERSE_MAX_AGE_SECONDS - 3600
    os.utime(cache, (old, old))
    fetcher = HistoricalDataFetcher(cache_dir=tmp_path)

    assert await fetcher.fetch_swap_universe() == ["BTC-USDT-SWAP"]
    prov = fetcher.last_universe_provenance
    assert prov["source"] == "stale_file_cache"
    assert prov["cache_hit"] is True
    assert prov["cache_age_hours"] > 24
