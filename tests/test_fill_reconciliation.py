"""Regression tests for fill reconciliation (``MarketService._reconcile_fills``).

Covers three fixes:

1. Bot client-order-id prefix matching must recognise the *sanitized* prefixes
   emitted by ``_generate_client_order_id`` (``tm-p`` -> ``tmp`` etc.), otherwise
   bot trade-management/commit/alternator closing fills are misclassified as
   manual and their realised PnL is never reconciled.
2. The manual-fill skip path must log each distinct fill only once (plus a
   per-run summary) instead of re-logging the same fills on every poll.
3. The entry-fee back-fill pass must skip manual fills and be idempotent (a row
   whose fee is already stored is no longer a candidate).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import app.services.market_service as market_service_module
from app.services.market_service import (
    BOT_CLIENT_ORDER_PREFIXES,
    MarketService,
    _sanitize_client_order_prefix,
)


class DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:  # pragma: no cover
        return None


class FakeTradeApi:
    """Minimal OKX trade adapter returning a canned fills-history page."""

    def __init__(self, fills: list[dict[str, Any]]) -> None:
        self._fills = fills
        self.calls: list[dict[str, Any]] = []

    def get_fills_history(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        # Single page: fewer than 100 rows stops the backward paging loop.
        return {"code": "0", "data": list(self._fills)}


def _make_service(fills: list[dict[str, Any]]) -> tuple[MarketService, list[str]]:
    logs: list[str] = []
    service = MarketService(
        state_service=DummyStateService(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=FakeTradeApi(fills),
        log_sink=logs.append,
    )
    return service, logs


# ── 1. Prefix matching ────────────────────────────────────────────────────────


def test_sanitize_client_order_prefix_strips_non_alphanumerics() -> None:
    assert _sanitize_client_order_prefix("tm-p") == "tmp"
    assert _sanitize_client_order_prefix("cmtr-c") == "cmtrc"
    assert _sanitize_client_order_prefix("altr-e") == "altre"
    assert _sanitize_client_order_prefix("tai2") == "tai2"


@pytest.mark.parametrize(
    "prefix",
    ["tai2", "gov", "gov-c", "shot", "skim", "tm-p", "tm-t", "tm-r", "tm-m",
     "cmtr-c", "cmtr-e", "altr-c", "altr-e"],
)
def test_generated_bot_ids_are_recognised(prefix: str) -> None:
    """Every prefix the bot actually emits must pass the identity guard."""
    generated = MarketService._generate_client_order_id(prefix)
    assert MarketService._is_bot_client_order_id(generated), (
        f"bot-generated id {generated!r} (prefix {prefix!r}) was misclassified as manual"
    )


def test_manual_ids_are_not_recognised() -> None:
    # OKX-generated order ids and arbitrary manual ids must not match.
    assert not MarketService._is_bot_client_order_id("O4046302221330348032")
    assert not MarketService._is_bot_client_order_id("")
    assert not MarketService._is_bot_client_order_id("my-manual-order")


def test_prefix_list_matches_sanitized_forms() -> None:
    for prefix in BOT_CLIENT_ORDER_PREFIXES:
        assert prefix == _sanitize_client_order_prefix(prefix)


# ── 2. Skip-path dedup + summary ──────────────────────────────────────────────


def _manual_fill(fill_id: str, clordid: str = "O4046302221330348032") -> dict[str, Any]:
    return {
        "instId": "HUMA-USDT-SWAP",
        "side": "sell",
        "fillId": fill_id,
        "clOrdId": clordid,
        "pnl": "1.23",
        "fee": "-0.01",
    }


def test_manual_fill_skip_is_deduplicated_across_runs() -> None:
    fills = [_manual_fill("53252611"), _manual_fill("53252612")]
    service, logs = _make_service(fills)

    async def scenario() -> None:
        await service._reconcile_fills()
        first_run = [line for line in logs if "skipping manual fill" in line]
        assert len(first_run) == 2, "each distinct manual fill should log once"

        # Second run re-fetches the same fills — no new per-fill lines.
        await service._reconcile_fills()
        all_skips = [line for line in logs if "skipping manual fill" in line]
        assert len(all_skips) == 2, "repeat runs must not re-log the same fills"

    asyncio.run(scenario())


def test_manual_fill_dedup_set_is_bounded() -> None:
    fills = [_manual_fill(str(i)) for i in range(5)]
    service, _ = _make_service(fills)
    service._reconciled_manual_fill_id_limit = 3

    async def scenario() -> None:
        await service._reconcile_fills()

    asyncio.run(scenario())
    assert len(service._reconciled_manual_fill_ids) == 3
    assert len(service._reconciled_manual_fill_id_set) == 3


def test_bot_fill_is_not_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bot-owned closing fill must reach the PnL reconciliation path."""
    reconciled: list[dict[str, Any]] = []

    async def fake_fetch_unreconciled_trades(**kwargs: Any) -> list[dict[str, Any]]:
        return [{"id": "trade-1"}]

    async def fake_update_trade_pnl(**kwargs: Any) -> None:
        reconciled.append(kwargs)

    monkeypatch.setattr(
        market_service_module, "fetch_unreconciled_trades", fake_fetch_unreconciled_trades
    )
    monkeypatch.setattr(market_service_module, "update_trade_pnl", fake_update_trade_pnl)

    # tm-t prefix -> sanitized "tmt" (the exact case that used to be misclassified).
    bot_fill = {
        "instId": "BEAT-USDT-SWAP",
        "side": "sell",
        "fillId": "418040598",
        "clOrdId": "tmt1793111554934cbf13f",
        "pnl": "2.50",
        "fee": "-0.02",
    }
    service, logs = _make_service([bot_fill])

    async def scenario() -> None:
        await service._reconcile_fills()

    asyncio.run(scenario())
    assert reconciled, "bot fill should have been reconciled, not skipped"
    assert not any("skipping manual fill" in line for line in logs)


# ── 3. Entry-fee pass: manual guard + idempotency ─────────────────────────────


def _entry_fill(fill_id: str, clordid: str, ordid: str = "4052015393023496192") -> dict[str, Any]:
    return {
        "instId": "PEPE-USDT-SWAP",
        "side": "buy",
        "fillId": fill_id,
        "clOrdId": clordid,
        "ordId": ordid,
        "pnl": "0",
        "fee": "-0.0074",
    }


def test_entry_fee_pass_skips_manual_fills(monkeypatch: pytest.MonkeyPatch) -> None:
    """A manual entry fill must never have its fee attributed to a bot row."""
    calls: list[dict[str, Any]] = []

    async def fake_by_order_id(order_id: str, fee_unset_only: bool = False) -> list[dict[str, Any]]:
        calls.append({"order_id": order_id, "fee_unset_only": fee_unset_only})
        return [{"id": "trade-1"}]

    async def fake_by_symbol(**kwargs: Any) -> list[dict[str, Any]]:
        calls.append(kwargs)
        return [{"id": "trade-1"}]

    async def fake_update_entry_fee(trade_id: Any, fee: float | None) -> None:
        calls.append({"updated": trade_id, "fee": fee})

    monkeypatch.setattr(
        market_service_module, "fetch_unreconciled_by_order_id", fake_by_order_id
    )
    monkeypatch.setattr(market_service_module, "fetch_unreconciled_trades", fake_by_symbol)
    monkeypatch.setattr(market_service_module, "update_entry_fee", fake_update_entry_fee)

    fills = [_entry_fill("835718249", clordid="O4046302221330348032")]
    service, _ = _make_service(fills)

    async def scenario() -> None:
        await service._reconcile_fills()

    asyncio.run(scenario())
    assert not any("updated" in c for c in calls), "manual entry fill must not store a fee"


def test_entry_fee_pass_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """The entry-fee lookup must request fee-unset rows so it is idempotent."""
    seen: list[dict[str, Any]] = []

    async def fake_by_order_id(order_id: str, fee_unset_only: bool = False) -> list[dict[str, Any]]:
        seen.append({"order_id": order_id, "fee_unset_only": fee_unset_only})
        return [{"id": "trade-1"}]

    async def fake_update_entry_fee(trade_id: Any, fee: float | None) -> None:
        seen.append({"updated": trade_id, "fee": fee})

    monkeypatch.setattr(
        market_service_module, "fetch_unreconciled_by_order_id", fake_by_order_id
    )
    monkeypatch.setattr(market_service_module, "update_entry_fee", fake_update_entry_fee)

    # Bot-owned entry fill (tai2 prefix) so the manual guard lets it through.
    fills = [_entry_fill("835718249", clordid="tai21793111554934cbf13f")]
    service, _ = _make_service(fills)

    async def scenario() -> None:
        await service._reconcile_fills()

    asyncio.run(scenario())
    lookups = [c for c in seen if "order_id" in c]
    assert lookups, "entry-fee pass should look up the row"
    assert all(c["fee_unset_only"] for c in lookups), (
        "entry-fee lookups must require fee IS NULL for idempotency"
    )
