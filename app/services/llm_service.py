from __future__ import annotations

import copy
import json
import logging
from typing import Any

from openrouter import OpenRouter, errors as openrouter_errors

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_REASONING_MODEL_TOKENS = (
    "deepseek-r1",
    "deepseek-r1:free",
    "deepseek-r1-free",
    "deepseek/r1",
    "o1-mini",
    "o1-preview",
    "o4-mini",
    "o4-preview",
    "r1-distill",
    "reasoning",
)


class LLMService:
    """LLM client with OpenRouter integration and deterministic fallback."""

    def __init__(self, model_id: str | None = None) -> None:
        self.model_id = model_id or "openrouter/gpt-4o-mini"
        self._client: OpenRouter | None = None
        self._client_api_key: str | None = None

    def set_model(self, model_id: str | None) -> None:
        if model_id:
            self.model_id = model_id

    def _get_client(self, api_key: str) -> OpenRouter:
        if self._client and self._client_api_key == api_key:
            return self._client
        self._client = OpenRouter(api_key=api_key)
        self._client_api_key = api_key
        return self._client

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        settings = get_settings()
        api_key = settings.openrouter_api_key
        if api_key:
            try:
                decision = await self._call_openrouter(payload, api_key)
                if isinstance(decision, dict):
                    decision.setdefault("_decision_origin", "openrouter")
                return decision
            except Exception as exc:  # pragma: no cover - network/LLM dependency
                logger.warning("OpenRouter prompt failed; falling back to heuristic: %s", exc)
        fallback_decision = self._fallback_decision(payload)
        if isinstance(fallback_decision, dict):
            fallback_decision.setdefault("_decision_origin", "fallback")
        return fallback_decision

    async def _call_openrouter(self, payload: dict[str, Any], api_key: str) -> dict[str, Any]:
        model_id = payload.get("prompt", {}).get("model") or self.model_id
        prepared_payload = self._prepare_payload_for_model(payload, model_id)

        prompt_block = prepared_payload.get("prompt") or {}
        context = prepared_payload.get("context") or {}
        model_id = prompt_block.get("model") or self.model_id
        request_kwargs: dict[str, Any] = {
            "model": model_id,
            "messages": self._build_messages(prompt_block, context),
            "temperature": 0.2,
        }
        response_format = self._build_response_format(prompt_block)
        response_format_included = False
        if response_format:
            request_kwargs["response_format"] = response_format
            response_format_included = True
        reasoning_config = self._build_reasoning(prompt_block, model_id)
        if reasoning_config:
            request_kwargs["reasoning"] = reasoning_config

        client = self._get_client(api_key)
        payload = await self._dispatch_chat_request(
            client,
            request_kwargs,
            response_format_included=response_format_included,
            model_id=model_id,
        )

        decision = self._parse_response(payload)
        if not isinstance(decision, dict):
            raise ValueError("LLM response missing JSON object")
        return decision

    def _build_messages(self, prompt_block: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
        system_prompt = (prompt_block.get("system") or "").strip()
        task_prompt = (prompt_block.get("task") or "Provide a trading decision.").strip()
        context_blob = json.dumps(context, ensure_ascii=False, default=str)
        user_content = f"{task_prompt}\n\nContext JSON:\n```json\n{context_blob}\n```"
        messages: list[dict[str, Any]] = []

        def _text_chunk(value: str) -> list[dict[str, str]]:
            return [{"type": "text", "text": value}]

        if system_prompt:
            messages.append({"role": "system", "content": _text_chunk(system_prompt)})
        messages.append({"role": "user", "content": _text_chunk(user_content)})
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

    def _build_reasoning(self, prompt_block: dict[str, Any], model_id: str | None) -> dict[str, Any] | None:
        manual = prompt_block.get("reasoning")
        if isinstance(manual, dict):
            return manual
        if self._model_requires_reasoning(model_id):
            return {"effort": "medium"}
        return None

    @staticmethod
    def _model_requires_compact_context(model_id: str | None) -> bool:
        if not model_id:
            return False
        normalized = model_id.lower()
        return any(token in normalized for token in _REASONING_MODEL_TOKENS)

    def _prepare_payload_for_model(self, payload: dict[str, Any], model_id: str | None) -> dict[str, Any]:
        if not self._model_requires_compact_context(model_id):
            return payload
        trimmed = copy.deepcopy(payload)
        context = trimmed.get("context")
        if not isinstance(context, dict):
            return trimmed

        drop_keys = {
            "history",
            "indicators",
            "trend_confirmation",
            "liquidity_context",
            "derivatives_posture",
            "portfolio_heatmap",
            "pending_orders",
            "snapshot_health_details",
        }
        removed_sections: list[str] = []
        for key in sorted(drop_keys):
            if context.pop(key, None) is not None:
                removed_sections.append(key)

        positions = self._compact_positions(context.get("positions"))
        if positions is not None:
            context["positions"] = positions

        exposure = context.get("portfolio_exposure")
        if isinstance(exposure, dict):
            context["portfolio_exposure"] = self._prune_dict(
                exposure, drop_fields={"heatmap", "symbols"}
            )

        market_block = context.get("market")
        if isinstance(market_block, dict):
            context["market"] = self._compact_market_block(market_block)

        notes = context.get("notes")
        if isinstance(notes, str) and len(notes) > 1500:
            context["notes"] = notes[:1500].rstrip() + " ... [trimmed]"

        context["compact_context"] = True
        if removed_sections:
            context["compact_removed_sections"] = removed_sections
        trimmed["context"] = context
        return trimmed

    async def _dispatch_chat_request(
        self,
        client: OpenRouter,
        request_kwargs: dict[str, Any],
        *,
        response_format_included: bool,
        model_id: str | None,
    ) -> dict[str, Any]:
        try:
            response = await client.chat.send_async(**request_kwargs)
        except openrouter_errors.ChatError as exc:
            detail = self._describe_openrouter_error(exc)
            logger.warning(
                "OpenRouter chat error (status=%s): %s", exc.status_code, detail
            )
            body = getattr(exc, "body", None)
            if body:
                logger.debug("OpenRouter error body: %s", body.strip())
            if response_format_included:
                logger.error(
                    "Model '%s' rejected JSON schema responses; please select a model that supports structured output.",
                    model_id or request_kwargs.get("model"),
                )
            raise
        except openrouter_errors.OpenRouterError as exc:
            detail = self._describe_openrouter_error(exc)
            logger.warning(
                "OpenRouter request failed (status=%s): %s", exc.status_code, detail
            )
            body = getattr(exc, "body", None)
            if body:
                logger.debug("OpenRouter error body: %s", body.strip())
            raise
        return self._response_to_dict(response)

    @staticmethod
    def _response_to_dict(response: Any) -> dict[str, Any]:
        if hasattr(response, "model_dump"):
            return response.model_dump()
        raise ValueError("OpenRouter returned unsupported response type")

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
        result = {
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
        result["_decision_origin"] = "fallback"
        return result

    @staticmethod
    def _prune_dict(value: Any, drop_fields: set[str]) -> Any:
        if not isinstance(value, dict):
            return value
        return {k: v for k, v in value.items() if k not in drop_fields}

    def _compact_market_block(self, market: Any) -> Any:
        if not isinstance(market, dict):
            return market
        trimmed = dict(market)
        for key in {"order_book", "liquidations", "recent_trades", "depth_metrics", "order_flow", "custom_metrics"}:
            trimmed.pop(key, None)
        for list_key, limit in {"trades": 10, "levels": 10}.items():
            if list_key in trimmed:
                trimmed[list_key] = self._limit_list(trimmed[list_key], limit)
        return trimmed

    def _compact_positions(self, positions: Any, limit: int = 3) -> Any:
        if not isinstance(positions, list):
            return positions
        keep_keys = {
            "symbol",
            "instrument",
            "side",
            "size",
            "avg_price",
            "mark_price",
            "unrealized_pnl",
            "pnl",
            "leverage",
        }
        compacted: list[Any] = []
        for entry in positions[:limit]:
            if isinstance(entry, dict):
                compacted.append({k: entry.get(k) for k in keep_keys if k in entry})
            else:
                compacted.append(entry)
        remaining = len(positions) - limit
        if remaining > 0:
            compacted.append({"note": f"{remaining} additional positions trimmed"})
        return compacted

    @staticmethod
    def _limit_list(value: Any, limit: int) -> Any:
        if not isinstance(value, list):
            return value
        if len(value) <= limit:
            return value
        return value[:limit]

    @staticmethod
    def _describe_openrouter_error(error: openrouter_errors.OpenRouterError) -> str:
        data = getattr(error, "data", None)
        err_payload = getattr(data, "error", None) if data else None
        if err_payload is not None:
            code = getattr(err_payload, "code", None)
            message = getattr(err_payload, "message", None)
            if code is not None and message:
                return f"{message} (code={code})"
            if message:
                return str(message)
        body = getattr(error, "body", None)
        if body:
            return body.strip()
        return str(error)

    @staticmethod
    def _model_requires_reasoning(model_id: str | None) -> bool:
        if not model_id:
            return False
        normalized = model_id.lower()
        return any(token in normalized for token in _REASONING_MODEL_TOKENS)


__all__ = ["LLMService"]
