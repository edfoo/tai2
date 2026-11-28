"""Candle client pools (2026-11-28).

Live, the wick_rejection trigger's candle fetches took 30-40s seconds before
the close: every call queued behind the snapshot poller, and once all slots
were busy each waiter queued on the *first* slot only, so a burst ran one at
a time on one client.  Pins that a busy pool is drained on every slot, and
that the trigger's fetches use their own pool.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from app.services import market_service as msmod
from app.services.market_service import MarketService


def _pool(n: int) -> list[tuple[Any, asyncio.Semaphore]]:
    return [(f"client{i}", asyncio.Semaphore(1)) for i in range(n)]


def _bare_service() -> MarketService:
    return MarketService.__new__(MarketService)


class TestAcquirePoolSlot:
    def test_burst_uses_every_slot(self) -> None:
        """9 calls on 3 slots take 3 rounds, never more than 3 at once."""

        async def scenario() -> tuple[float, int, list[asyncio.Semaphore]]:
            svc = _bare_service()
            pool = _pool(3)
            busy = {"now": 0, "max": 0}

            async def call() -> None:
                _client, sem = await svc._acquire_pool_slot(pool)
                busy["now"] += 1
                busy["max"] = max(busy["max"], busy["now"])
                try:
                    await asyncio.sleep(0.05)
                finally:
                    busy["now"] -= 1
                    sem.release()

            t0 = time.perf_counter()
            await asyncio.gather(*(call() for _ in range(9)))
            return time.perf_counter() - t0, busy["max"], [s for _c, s in pool]

        elapsed, peak, sems = asyncio.run(scenario())
        assert peak == 3
        # 3 rounds of 0.05s; the old first-slot wait needed 1 + 6 = 7 rounds.
        assert elapsed < 0.05 * 5
        assert not any(s.locked() for s in sems)

    def test_waiter_gets_the_slot_that_frees(self) -> None:
        """With slot 0 held, a waiter is served by slot 1 when it frees."""

        async def scenario() -> str:
            svc = _bare_service()
            pool = _pool(2)
            await pool[0][1].acquire()
            await pool[1][1].acquire()
            waiter = asyncio.ensure_future(svc._acquire_pool_slot(pool))
            await asyncio.sleep(0)
            pool[1][1].release()
            client, sem = await asyncio.wait_for(waiter, timeout=1.0)
            assert pool[0][1].locked()  # slot 0 still held by its owner
            sem.release()
            pool[0][1].release()
            return client

        assert asyncio.run(scenario()) == "client1"

    def test_cancelled_waiter_leaks_no_slot(self) -> None:
        async def scenario() -> list[asyncio.Semaphore]:
            svc = _bare_service()
            pool = _pool(2)
            for _c, sem in pool:
                await sem.acquire()
            waiter = asyncio.ensure_future(svc._acquire_pool_slot(pool))
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            for _c, sem in pool:
                sem.release()
            return [s for _c, s in pool]

        assert not any(s.locked() for s in asyncio.run(scenario()))


class TestWrPriorityPool:
    def _svc(self) -> MarketService:
        svc = _bare_service()
        svc._candle_pool = _pool(2)
        svc._wr_candle_pool = [("wr0", asyncio.Semaphore(1))]
        return svc

    def test_slot_comes_from_the_wr_pool_only_inside_priority(self) -> None:
        async def scenario() -> tuple[str, str]:
            svc = self._svc()
            client, sem = await svc._acquire_candle_slot()
            sem.release()
            with msmod._wr_candle_priority():
                wr_client, wr_sem = await svc._acquire_candle_slot()
            wr_sem.release()
            return client, wr_client

        assert asyncio.run(scenario()) == ("client0", "wr0")

    def test_trigger_fetches_run_with_priority(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[tuple[str, bool]] = []

        async def fake_fetch(self, symbol, bar, cache, *, limit=None, cache_key=None):
            seen.append((bar, msmod._WR_CANDLE_PRIORITY.get()))
            return [[1]]

        monkeypatch.setattr(MarketService, "_fetch_ohlcv_bar", fake_fetch)
        svc = self._svc()
        svc._latest_ohlcv_tf = {}

        async def scenario() -> None:
            await svc.fetch_wr_1m_candles("A-USDT-SWAP")
            await svc.refresh_timeframe_candles("A-USDT-SWAP", "1H")
            await svc._fetch_ohlcv_bar("A-USDT-SWAP", "5m", {})  # poller-style call

        asyncio.run(scenario())
        assert seen == [("1m", True), ("1H", True), ("5m", False)]
        assert msmod._WR_CANDLE_PRIORITY.get() is False

    def test_service_builds_a_separate_wr_pool(self) -> None:
        svc = _bare_service()
        svc._okx_flag = "0"
        svc._apply_client_timeout = lambda client: None
        if msmod.OkxMarket is None:
            pytest.skip("python-okx not installed")
        main = svc._build_candle_pool()
        wr = svc._build_candle_pool(MarketService.WR_CANDLE_CLIENT_POOL_SIZE)
        assert len(main) == MarketService.CANDLE_CLIENT_POOL_SIZE
        assert len(wr) == MarketService.WR_CANDLE_CLIENT_POOL_SIZE
        assert not {id(c) for c, _ in main} & {id(c) for c, _ in wr}
