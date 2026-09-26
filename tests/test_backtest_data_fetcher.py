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