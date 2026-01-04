from __future__ import annotations

from typing import Any, Dict


class LLMService:
    """Simple heuristic-based stand-in for an external LLM."""

    def __init__(self, model_id: str | None = None) -> None:
        self.model_id = model_id or "openrouter/gpt-4o-mini"

    def set_model(self, model_id: str | None) -> None:
        if model_id:
            self.model_id = model_id

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        context = payload.get("context") or {}
        strategy = context.get("strategy_signal") or {}
        risk_metrics = context.get("risk_metrics") or {}
        market = context.get("market") or {}
        guardrails = context.get("guardrails") or {}

        action = (strategy.get("action") or "HOLD").upper()
        confidence = float(strategy.get("confidence") or 0.4)
        risk_score = min(1.0, max(0.0, confidence))
        atr = risk_metrics.get("atr") or 0.0
        last_price = market.get("last_price") or market.get("bid") or market.get("ask")
        stop_loss = None
        take_profit = None
        if atr and last_price:
            multiplier = 1.5
            stop_loss = float(last_price) - multiplier * atr if action == "BUY" else float(last_price) + multiplier * atr
            take_profit = float(last_price) + multiplier * atr if action == "BUY" else float(last_price) - multiplier * atr

        position_size = 1.0
        positions = context.get("positions") or []
        if positions:
            size = positions[0].get("size")
            try:
                position_size = float(abs(size)) if size is not None else position_size
            except (TypeError, ValueError):
                position_size = 1.0

        rationale = strategy.get("reason") or "Default strategy output"
        tags = ["model:" + (self.model_id or "unknown"), f"guardrails:max_leverage={guardrails.get('max_leverage', '--')}"]
        notes = context.get("notes") or ""
        decision = {
            "action": action,
            "confidence": round(confidence, 3),
            "position_size": position_size,
            "rationale": rationale,
            "risk_score": round(risk_score, 3),
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "timeframe_alignment": context.get("timeframe"),
            "notes": notes,
            "tags": tags,
        }
        return decision


__all__ = ["LLMService"]
