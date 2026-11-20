"""Phase 7 of the wick_rejection plan: helpers behind the UI.

``wick_rejection_risk_summary`` (STRATEGY card risk read-out) and the trigger
status used by the LIVE page (``is_armed`` / ``seconds_until_next``).  The
pages themselves were smoke-rendered with NiceGUI's in-process ``User``
simulation during development (no server, OKX blocked).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from app.services.launcher_tp_sl import wick_rejection_risk_summary
from app.services.strategies.defaults import merged_config, strategy_defaults
from app.services.wick_rejection_trigger import WickRejectionTrigger

H0 = 1_700_002_800.0


def _cfg(**over: Any) -> dict[str, Any]:
    return merged_config({**strategy_defaults("wick_rejection"), **over}, "wick_rejection")


WIDE = {"min_leverage": 1, "max_leverage": 10}


class TestRiskSummary:
    def test_defaults(self) -> None:
        text = wick_rejection_risk_summary(_cfg(), WIDE, 117.0)
        assert "margin 20.00 USDT × 5× = position 100.00 USDT" in text
        assert "Take profit: 0.50% price ≈ +0.50 USDT" in text
        assert "Emergency stop (exchange): 7.00% price ≈ −7.00 USDT (35% of margin" in text
        # 3 × 20 USDT fits in 100% of 117 USDT.
        assert "Worst case, 3 position(s) all stopped: −21.00 USDT (17.9% of equity)" in text

    def test_clamp_and_position_cap(self) -> None:
        guards = {"min_leverage": 1, "max_leverage": 3, "max_position_pct": 0.4}
        text = wick_rejection_risk_summary(_cfg(), guards, 100.0)
        assert "× 3× = position 60.00 USDT (clamped from 5×" in text
        assert "11.67% price" in text
        assert "max_position_pct 0.4 × equity = 40.00 USDT cap → position cut to 40.00 USDT" in text

    def test_total_margin_cap_limits_positions(self) -> None:
        text = wick_rejection_risk_summary(_cfg(max_total_margin_pct=30.0), WIDE, 100.0)
        assert "fits 1 of 3 positions" in text
        assert "Worst case, 1 position(s)" in text

    def test_stop_off_and_unknown_equity(self) -> None:
        assert "Emergency stop: OFF" in wick_rejection_risk_summary(
            _cfg(emergency_stop_mode="off"), WIDE, 100.0
        )
        text = wick_rejection_risk_summary(_cfg(margin_mode="pct_equity"), WIDE, None)
        assert "equity is unknown" in text


class TestTriggerStatus:
    def _trigger(self, *, mode: str = "launcher_only", enabled: bool = True,
                 running: bool = True) -> WickRejectionTrigger:
        rc = {"launcher": {"mode": mode, "strategies": {"wick_rejection": {"enabled": enabled}}}}
        app = SimpleNamespace(state=SimpleNamespace(runtime_config=rc, market_service=None))
        trig = WickRejectionTrigger(app, asyncio.Lock(), clock=lambda: H0 + 600)
        if running:
            trig._task = SimpleNamespace(done=lambda: False)  # type: ignore[assignment]
        return trig

    def test_armed_countdown(self) -> None:
        trig = self._trigger()
        assert trig.is_armed()
        assert trig.seconds_until_next() == 3600 - 600 - 40

    def test_not_armed(self) -> None:
        assert self._trigger(mode="disabled").seconds_until_next() is None
        assert self._trigger(enabled=False).seconds_until_next() is None
        assert self._trigger(running=False).seconds_until_next() is None
