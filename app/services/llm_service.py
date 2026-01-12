from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
_OPENROUTER_TIMEOUT = 30.0


class LLMService:
    """LLM client with OpenRouter integration and deterministic fallback."""

    def __init__(self, model_id: str | None = None) -> None:
        self.model_id = model_id or "openrouter/gpt-4o-mini"

    def set_model(self, model_id: str | None) -> None:
        if model_id:
            self.model_id = model_id

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        settings = get_settings()
        api_key = settings.openrouter_api_key
        if api_key:
            try:
                return await self._call_openrouter(payload, api_key)
            except Exception as exc:  # pragma: no cover - network/LLM dependency
                logger.warning("OpenRouter prompt failed; falling back to heuristic: %s", exc)
        return self._fallback_decision(payload)

    async def _call_openrouter(self, payload: dict[str, Any], api_key: str) -> dict[str, Any]:
        prompt_block = payload.get("prompt") or {}
        context = payload.get("context") or {}
        model_id = prompt_block.get("model") or self.model_id
        body: dict[str, Any] = {
            "model": model_id,
            "messages": self._build_messages(prompt_block, context),
            "temperature": 0.2,
        }
        response_format = self._build_response_format(prompt_block)
        if response_format:
            body["response_format"] = response_format

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/edfoo/tai2",
            "X-Title": "tai2",
        }

        async with httpx.AsyncClient(timeout=_OPENROUTER_TIMEOUT) as client:
            response = await client.post(_OPENROUTER_CHAT_URL, headers=headers, json=body)
            response.raise_for_status()
            payload = response.json()

        decision = self._parse_response(payload)
        if not isinstance(decision, dict):
            raise ValueError("LLM response missing JSON object")
        return decision

    def _build_messages(self, prompt_block: dict[str, Any], context: dict[str, Any]) -> list[dict[str, str]]:
        system_prompt = (prompt_block.get("system") or "").strip()
        task_prompt = (prompt_block.get("task") or "Provide a trading decision.").strip()
        context_blob = json.dumps(context, ensure_ascii=False, default=str)
        user_content = f"{task_prompt}\n\nContext JSON:\n```json\n{context_blob}\n```"
        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_content})
        return messages

    def _build_response_format(self, prompt_block: dict[str, Any]) -> dict[str, Any] | None:
        schema = prompt_block.get("response_schema")
        if not isinstance(schema, dict):
            return None
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "tai2_decision",
                "schema": schema,
                "strict": True,
            },
        }

    def _parse_response(self, payload: dict[str, Any]) -> dict[str, Any]:
        choices = payload.get("choices") or []
        if not choices:
            raise ValueError("OpenRouter response missing choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, list):
            for chunk in content:
                if isinstance(chunk, dict) and chunk.get("type") == "output_json":
                    output = chunk.get("json")
                    if isinstance(output, dict):
                        return output
        text_payload = self._coerce_text(content)
        if not text_payload:
            raise ValueError("Assistant response empty")
        try:
            return json.loads(text_payload)
        except json.JSONDecodeError:
            block = self._extract_json_block(text_payload)
            if block is None:
                raise
            return block

    @staticmethod
    def _coerce_text(content: Any) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text_value = item.get("text")
                    if isinstance(text_value, str):
                        parts.append(text_value)
            return "\n".join(parts).strip()
        return ""

    @staticmethod
    def _extract_json_block(text_payload: str) -> dict[str, Any] | None:
        start = text_payload.find("{")
        end = text_payload.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        snippet = text_payload[start : end + 1]
        return json.loads(snippet)

    def _fallback_decision(self, payload: dict[str, Any]) -> dict[str, Any]:
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
        return {
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


__all__ = ["LLMService"]
