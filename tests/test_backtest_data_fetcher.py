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