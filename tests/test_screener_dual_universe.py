"""Tests for dual-universe symbol screener (SC expansion + MR chop).

Covers:
  - Dual scoring / selection from ticker snapshot
  - Soft overlap between SC and MR lists
  - Strong-trend exclusion from MR via max momentum
  - Strategy routing via _strategy_allowed_on_symbol
  - get_screener_universe strategy-specific lists
  - Legacy single-list mode (dual_universe=False)
  - Fallback when screener disabled or lists empty
  - set_screener_config dual keys
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from app.services.market_service import MarketService


class _DummyStateService:
    async def set_market_snapshot(self, snapshot: dict[str, Any]) -> None:
        pass

    async def get_market_snapshot(self) -> dict[str, Any]:
        return {"positions": []}


def _make_service() -> MarketService:
    return MarketService(
        state_service=_DummyStateService(),
        enable_websocket=False,
        account_api=object(),
        market_api=object(),
        public_api=object(),
        trade_api=object(),
    )


def _dual_cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "enabled": True,
        "dual_universe": True,
        "universe_filter": "*-USDT-SWAP",
        "max_symbols": 10,
        "sc_max_symbols": 3,
        "mr_max_symbols": 3,
        "min_volume_usd": 0,
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


def _sample_tickers() -> list[dict[str, Any]]:
    """SC-like (high mom), MR-like (high range, low mom), and dead name."""
    return [
        # mom 10%, hl 13%
        {
            "instId": "SC1-USDT-SWAP",
            "last": 110,
            "open24h": 100,
            "high24h": 112,
            "low24h": 99,
            "volCcy24h": 2_000_000,
        },
        # mom 20%, hl 27% — strong trend, should not land in MR
        {
            "instId": "SC2-USDT-SWAP",
            "last": 120,
            "open24h": 100,
            "high24h": 125,
            "low24h": 98,
            "volCcy24h": 3_000_000,
        },
        # mom 1%, hl 13% — chop / reversion habitat
        {
            "instId": "MR1-USDT-SWAP",
            "last": 101,
            "open24h": 100,
            "high24h": 108,
            "low24h": 95,
            "volCcy24h": 1_500_000,
        },
        # mom 0.5%, hl 12%
        {
            "instId": "MR2-USDT-SWAP",
            "last": 100.5,
            "open24h": 100,
            "high24h": 106,
            "low24h": 94,
            "volCcy24h": 1_200_000,
        },
        # low volume — filtered when min_volume_usd > 0
        {
            "instId": "DEAD-USDT-SWAP",
            "last": 100,
            "open24h": 100,
            "high24h": 100.1,
            "low24h": 99.9,
            "volCcy24h": 50_000,
        },
    ]


async def _run_screener(
    service: MarketService,
    *,
    tickers: list[dict[str, Any]] | None = None,
    cfg: dict[str, Any] | None = None,
    seed_vol_history: bool = True,
) -> bool:
    """Configure service, stub ticker fetch / update_symbols, run screener."""
    tickers = tickers if tickers is not None else _sample_tickers()
    service.set_screener_config(cfg or _dual_cfg())
    service._screener_last_run = 0.0
    service._screener_selected_symbols = []
    service._screener_sc_symbols = []
    service._screener_mr_symbols = []
    service._screener_vol_history = {}
    service.symbols = []

    async def fake_fetch() -> list[dict[str, Any]]:
        return tickers

    async def fake_update(symbols: list[str]) -> None:
        service.symbols = list(symbols)

    service._fetch_all_swap_tickers = fake_fetch  # type: ignore[method-assign]
    service.update_symbols = fake_update  # type: ignore[method-assign]

    if seed_vol_history:
        for t in tickers:
            sym = str(t["instId"]).upper()
            vol = float(t["volCcy24h"])
            service._screener_vol_history[sym] = deque([vol * 0.5, vol], maxlen=8)

    return await service.run_screener_if_due(force=True)


# ── Dual scoring / selection ─────────────────────────────────────────────────


class TestDualUniverseScoring:
    def test_builds_sc_and_mr_lists(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            changed = await _run_screener(service)
            assert changed is True
            assert service._screener_sc_symbols
            assert service._screener_mr_symbols
            # Union is SC-first then MR-only additions.
            assert service._screener_selected_symbols == service.symbols
            for sym in service._screener_sc_symbols:
                assert sym in service._screener_selected_symbols
            for sym in service._screener_mr_symbols:
                assert sym in service._screener_selected_symbols

        asyncio.run(scenario())

    def test_strong_trend_excluded_from_mr(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            await _run_screener(service)
            # SC2 has 20% momentum > mr_max_momentum_pct 8%
            assert "SC2-USDT-SWAP" not in service._screener_mr_symbols
            # Expansion names should still be eligible for SC
            assert "SC2-USDT-SWAP" in service._screener_sc_symbols

        asyncio.run(scenario())

    def test_mr_prefers_chop_names(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            await _run_screener(service)
            mr = service._screener_mr_symbols
            assert "MR1-USDT-SWAP" in mr or "MR2-USDT-SWAP" in mr
            # Soft overlap allowed: low-mom MR1 may also appear in SC
            # but pure expansion SC2 must not dominate MR.
            assert mr[0].startswith("MR") or mr[0] in {
                "MR1-USDT-SWAP",
                "MR2-USDT-SWAP",
                "SC1-USDT-SWAP",
            }

        asyncio.run(scenario())

    def test_soft_overlap_allowed(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            # Raise MR max mom so SC1 (10%) can enter both pools.
            await _run_screener(service, cfg=_dual_cfg(mr_max_momentum_pct=15.0))
            overlap = set(service._screener_sc_symbols) & set(service._screener_mr_symbols)
            # Overlap is allowed; if present it must be in the union once.
            for sym in overlap:
                assert service._screener_selected_symbols.count(sym) == 1

        asyncio.run(scenario())

    def test_min_volume_filter(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            await _run_screener(service, cfg=_dual_cfg(min_volume_usd=100_000))
            union = set(service._screener_selected_symbols)
            assert "DEAD-USDT-SWAP" not in union

        asyncio.run(scenario())

    def test_respects_sc_and_mr_caps(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            await _run_screener(
                service,
                cfg=_dual_cfg(sc_max_symbols=1, mr_max_symbols=1),
            )
            assert len(service._screener_sc_symbols) <= 1
            assert len(service._screener_mr_symbols) <= 1

        asyncio.run(scenario())

    def test_no_change_when_symbols_unchanged(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            first = await _run_screener(service)
            assert first is True
            # Second run with same tickers should not call update_symbols path
            # as a change (set equality short-circuit).
            service._screener_last_run = 0.0
            second = await service.run_screener_if_due(force=True)
            assert second is False

        asyncio.run(scenario())

    def test_disabled_screener_returns_false(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            changed = await _run_screener(service, cfg=_dual_cfg(enabled=False))
            assert changed is False
            assert service._screener_sc_symbols == []
            assert service._screener_mr_symbols == []

        asyncio.run(scenario())


# ── Legacy single-list mode ──────────────────────────────────────────────────


class TestLegacySingleListMode:
    def test_dual_universe_false_fills_both_lists_identically(self) -> None:
        service = _make_service()

        async def scenario() -> None:
            await _run_screener(
                service,
                cfg=_dual_cfg(dual_universe=False, max_symbols=3),
            )
            assert service._screener_selected_symbols
            assert service._screener_sc_symbols == service._screener_selected_symbols
            assert service._screener_mr_symbols == service._screener_selected_symbols
            assert len(service._screener_selected_symbols) <= 3

        asyncio.run(scenario())


# ── Strategy routing ─────────────────────────────────────────────────────────


class TestStrategyUniverseRouting:
    def test_allows_all_when_screener_disabled(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg(enabled=False))
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        assert service._strategy_allowed_on_symbol("spike_continuation", "ANY-USDT-SWAP")
        assert service._strategy_allowed_on_symbol("mean_reversion", "ANY-USDT-SWAP")

    def test_allows_all_when_dual_universe_off(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg(dual_universe=False))
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        assert service._strategy_allowed_on_symbol("spike_continuation", "MR1-USDT-SWAP")
        assert service._strategy_allowed_on_symbol("mean_reversion", "SC1-USDT-SWAP")

    def test_allows_all_when_lists_empty(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = []
        service._screener_mr_symbols = []
        service._screener_selected_symbols = []
        assert service._strategy_allowed_on_symbol("spike_continuation", "BTC-USDT-SWAP")
        assert service._strategy_allowed_on_symbol("mean_reversion", "ETH-USDT-SWAP")

    def test_sc_restricted_to_sc_universe(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["SC1-USDT-SWAP", "SC2-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        service._screener_selected_symbols = [
            "SC1-USDT-SWAP",
            "SC2-USDT-SWAP",
            "MR1-USDT-SWAP",
        ]
        assert service._strategy_allowed_on_symbol("spike_continuation", "SC1-USDT-SWAP")
        assert service._strategy_allowed_on_symbol("spike_continuation", "sc2-usdt-swap")
        assert not service._strategy_allowed_on_symbol(
            "spike_continuation", "MR1-USDT-SWAP"
        )
        assert not service._strategy_allowed_on_symbol(
            "spike_continuation", "ZZZ-USDT-SWAP"
        )

    def test_mr_restricted_to_mr_universe(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP", "MR2-USDT-SWAP"]
        service._screener_selected_symbols = [
            "SC1-USDT-SWAP",
            "MR1-USDT-SWAP",
            "MR2-USDT-SWAP",
        ]
        assert service._strategy_allowed_on_symbol("mean_reversion", "MR1-USDT-SWAP")
        assert not service._strategy_allowed_on_symbol(
            "mean_reversion", "SC1-USDT-SWAP"
        )

    def test_overlap_symbol_allowed_for_both(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["BOTH-USDT-SWAP", "SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["BOTH-USDT-SWAP", "MR1-USDT-SWAP"]
        assert service._strategy_allowed_on_symbol(
            "spike_continuation", "BOTH-USDT-SWAP"
        )
        assert service._strategy_allowed_on_symbol(
            "mean_reversion", "BOTH-USDT-SWAP"
        )

    def test_unknown_strategy_allowed(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        assert service._strategy_allowed_on_symbol("custom_alpha", "ANY-USDT-SWAP")

    def test_launcher_evaluate_signals_skips_wrong_universe(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        service._screener_selected_symbols = ["SC1-USDT-SWAP", "MR1-USDT-SWAP"]
        service._last_full_snapshot = {
            "market_data": {
                "MR1-USDT-SWAP": {"indicators": {}, "custom_metrics": {}},
            },
            "positions": [],
        }
        service._launcher_config = {
            "strategies": {
                "spike_continuation": {"enabled": True},
                "mean_reversion": {"enabled": True},
            }
        }

        # Force strategies to return a signal if evaluate is reached.
        class _AlwaysSignal:
            def __init__(self, name: str) -> None:
                self.name = name

            def evaluate(self, symbol, snapshot, config, helpers):  # noqa: ANN001
                from app.services.strategies import StrategySignal

                return StrategySignal(
                    direction="buy",
                    strategy_name=self.name,
                    rationale=f"{self.name} fired",
                )

        service._strategies = [
            _AlwaysSignal("spike_continuation"),
            _AlwaysSignal("mean_reversion"),
        ]

        # On MR-only symbol, SC must be skipped; MR may fire.
        signals = service._launcher_evaluate_signals("MR1-USDT-SWAP")
        names = {s.strategy_name for s in signals}
        assert "spike_continuation" not in names
        assert "mean_reversion" in names

        # On SC-only symbol, MR must be skipped; SC may fire.
        service._last_full_snapshot = {
            "market_data": {
                "SC1-USDT-SWAP": {"indicators": {}, "custom_metrics": {}},
            },
            "positions": [],
        }
        signals_sc = service._launcher_evaluate_signals("SC1-USDT-SWAP")
        names_sc = {s.strategy_name for s in signals_sc}
        assert "mean_reversion" not in names_sc
        assert "spike_continuation" in names_sc


# ── get_screener_universe ────────────────────────────────────────────────────


class TestGetScreenerUniverse:
    def test_returns_strategy_specific_lists(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        service._screener_selected_symbols = ["SC1-USDT-SWAP", "MR1-USDT-SWAP"]
        service.symbols = list(service._screener_selected_symbols)

        assert service.get_screener_universe("spike_continuation") == ["SC1-USDT-SWAP"]
        assert service.get_screener_universe("mean_reversion") == ["MR1-USDT-SWAP"]
        assert set(service.get_screener_universe()) == {
            "SC1-USDT-SWAP",
            "MR1-USDT-SWAP",
        }

    def test_falls_back_to_selected_when_dual_lists_empty(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg())
        service._screener_sc_symbols = []
        service._screener_mr_symbols = []
        service._screener_selected_symbols = ["AAA-USDT-SWAP"]
        service.symbols = ["AAA-USDT-SWAP"]
        assert service.get_screener_universe("spike_continuation") == ["AAA-USDT-SWAP"]
        assert service.get_screener_universe("mean_reversion") == ["AAA-USDT-SWAP"]

    def test_legacy_mode_returns_selected(self) -> None:
        service = _make_service()
        service.set_screener_config(_dual_cfg(dual_universe=False))
        service._screener_sc_symbols = ["SC1-USDT-SWAP"]
        service._screener_mr_symbols = ["MR1-USDT-SWAP"]
        service._screener_selected_symbols = ["LEGACY-USDT-SWAP"]
        assert service.get_screener_universe("spike_continuation") == [
            "LEGACY-USDT-SWAP"
        ]


# ── Config plumbing ──────────────────────────────────────────────────────────


class TestScreenerConfigPlumbing:
    def test_set_screener_config_stores_dual_keys(self) -> None:
        service = _make_service()
        cfg = _dual_cfg(sc_max_symbols=7, mr_max_symbols=5, dual_universe=True)
        service.set_screener_config(cfg)
        stored = service._screener_config
        assert stored["dual_universe"] is True
        assert stored["sc_max_symbols"] == 7
        assert stored["mr_max_symbols"] == 5
        assert stored["enabled"] is True

    def test_default_runtime_screener_has_dual_keys(self) -> None:
        from fastapi.testclient import TestClient

        from app.main import create_app

        app = create_app(enable_background_services=False)
        # Lifespan populates runtime_config; enter TestClient to run it.
        with TestClient(app):
            screener = app.state.runtime_config.get("screener") or {}
            assert "dual_universe" in screener
            assert screener.get("dual_universe") is True
            assert "sc_max_symbols" in screener
            assert "mr_max_symbols" in screener
            assert "mr_min_hl_range_pct" in screener
            assert "mr_max_momentum_pct" in screener
