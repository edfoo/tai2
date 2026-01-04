from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Optional


DEFAULT_SYSTEM_PROMPT = (
    "You are a professional hedge fund trader. Analyze the provided market context "
    "(positions, technical indicators, order flow, funding, and risk metrics) to decide whether to execute a trade."
)

DEFAULT_DECISION_PROMPT = (
    "You will receive a JSON object under the key 'context' that summarizes the latest market snapshot and guardrails. "
    "Evaluate directional bias, momentum, liquidity, and risk before deciding on BUY/SELL/HOLD. "
    "Respond strictly as JSON that matches 'response_schema'. Include stop/target suggestions and note any blocking risks."
)

RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "action": {
            "type": "string",
            "enum": ["BUY", "SELL", "HOLD"],
            "description": "Primary recommendation aligned to the provided symbol",
        },
        "confidence": {
            "type": "number",
            "minimum": 0,
            "maximum": 1,
            "description": "Confidence value between 0 and 1",
        },
        "position_size": {
            "type": "number",
            "description": "Suggested position size in contracts or base units",
        },
        "rationale": {
            "type": "string",
            "description": "Short explanation citing the strongest signals",
        },
        "risk_score": {
            "type": "number",
            "minimum": 0,
            "maximum": 1,
            "description": "Normalized risk value (higher = riskier)",
        },
        "stop_loss": {
            "type": "number",
            "description": "Recommended stop level in price terms",
        },
        "take_profit": {
            "type": "number",
            "description": "Recommended take profit level in price terms",
        },
        "timeframe_alignment": {
            "type": "string",
            "description": "How the decision aligns with provided timeframe",
        },
        "notes": {
            "type": "string",
            "description": "Additional implementation notes or cautionary flags",
        },
        "tags": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["action", "confidence", "rationale", "risk_score"],
}

def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _percent(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator in (None, 0):
        return None
    return (numerator / denominator) * 100


@dataclass
class PromptBuilder:
    """Assembles structured payloads for LLM trade decisions."""

    snapshot: dict[str, Any] | None
    metadata: Optional[dict[str, Any]] = None
    max_candles: int = 120
    _cache_symbol: Optional[str] = field(init=False, default=None)

    def build(self, *, symbol: str | None = None, timeframe: str | None = None) -> dict[str, Any]:
        snapshot = self.snapshot or {}
        runtime_meta = self.metadata or {}
        resolved_symbol = self._resolve_symbol(symbol)
        market_block = self._select_market(resolved_symbol)
        indicators = market_block.get("indicators") or {}
        ticker = market_block.get("ticker") or snapshot.get("ticker") or {}
        funding = market_block.get("funding_rate") or snapshot.get("funding_rate") or {}
        open_interest = market_block.get("open_interest") or snapshot.get("open_interest") or {}
        custom_metrics = market_block.get("custom_metrics") or snapshot.get("custom_metrics") or {}
        risk_metrics = market_block.get("risk_metrics") or snapshot.get("risk_metrics") or {}
        strategy_signal = market_block.get("strategy_signal") or snapshot.get("strategy_signal") or {}
        order_book = market_block.get("order_book") or snapshot.get("order_book") or {}

        live_section = self._build_live_section(ticker, funding, open_interest, custom_metrics, order_book)
        history_section = self._build_history_section(indicators)
        indicator_section = self._build_indicator_section(indicators)
        positions_section = self._build_positions_section(snapshot.get("positions") or [])
        account_section = self._build_account_section(snapshot)
        guardrails = runtime_meta.get("guardrails") or self._default_guardrails()
        model_id = runtime_meta.get("llm_model_id")
        schema_overrides = runtime_meta.get("llm_response_schemas") or {}
        timeframe_value = timeframe or runtime_meta.get("ta_timeframe") or snapshot.get("timeframe") or "4H"

        context = {
            "generated_at": snapshot.get("generated_at"),
            "symbol": resolved_symbol,
            "timeframe": timeframe_value,
            "market": live_section,
            "history": history_section,
            "indicators": indicator_section,
            "strategy_signal": strategy_signal,
            "risk_metrics": risk_metrics,
            "positions": positions_section,
            "account": account_section,
            "guardrails": guardrails,
            "notes": runtime_meta.get("llm_notes"),
            "prompt_version_id": runtime_meta.get("prompt_version_id"),
            "prompt_version_name": runtime_meta.get("prompt_version_name"),
        }
        prompt_block = {
            "system": (runtime_meta.get("llm_system_prompt") or DEFAULT_SYSTEM_PROMPT).strip(),
            "task": (runtime_meta.get("llm_decision_prompt") or DEFAULT_DECISION_PROMPT).strip(),
            "model": model_id,
            "response_schema": self._response_schema(model_id, schema_overrides),
        }
        return {"prompt": prompt_block, "context": context}

    def _resolve_symbol(self, symbol: str | None) -> str:
        if symbol:
            self._cache_symbol = symbol
            return symbol
        if self._cache_symbol:
            return self._cache_symbol
        snapshot = self.snapshot or {}
        primary = snapshot.get("symbol")
        if primary:
            self._cache_symbol = primary
            return primary
        symbols = snapshot.get("symbols") or []
        if symbols:
            self._cache_symbol = symbols[0]
            return symbols[0]
        return "BTC-USDT-SWAP"

    def _select_market(self, symbol: str) -> dict[str, Any]:
        snapshot = self.snapshot or {}
        market_data = snapshot.get("market_data") or {}
        return market_data.get(symbol) or {}

    def _build_live_section(
        self,
        ticker: dict[str, Any],
        funding: dict[str, Any],
        open_interest: dict[str, Any],
        custom_metrics: dict[str, Any],
        order_book: dict[str, Any],
    ) -> dict[str, Any]:
        last_price = _to_float(ticker.get("last") or ticker.get("px"))
        bid = _to_float(ticker.get("bidPx") or ticker.get("bid1Px"))
        ask = _to_float(ticker.get("askPx") or ticker.get("ask1Px"))
        spread = (ask - bid) if ask is not None and bid is not None else None
        spread_pct = _percent(spread, last_price) if spread is not None else None
        change_24h = _to_float(ticker.get("changeRate") or ticker.get("change24h"))
        volume_24h = _to_float(ticker.get("volCcy24h") or ticker.get("vol24h"))
        funding_rate = _to_float(funding.get("fundingRate"))
        next_funding = funding.get("nextFundingTime") or funding.get("nextFundingRate")
        oi_value = _to_float(open_interest.get("oi"))
        oi_value_ccy = _to_float(open_interest.get("oiCcy"))
        bid_depth = sum(_to_float(level[1]) or 0.0 for level in (order_book.get("bids") or [])[:5])
        ask_depth = sum(_to_float(level[1]) or 0.0 for level in (order_book.get("asks") or [])[:5])
        ofi = custom_metrics.get("order_flow_imbalance")
        cvd = custom_metrics.get("cumulative_volume_delta")
        return {
            "last_price": last_price,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "spread_pct": spread_pct,
            "change_24h": change_24h,
            "volume_24h": volume_24h,
            "funding_rate": funding_rate,
            "next_funding": next_funding,
            "open_interest": {
                "contracts": oi_value,
                "usd": oi_value_ccy,
            },
            "order_flow": {
                "imbalance": ofi,
                "cvd": cvd,
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
                "cvd_series": custom_metrics.get("cvd_series"),
                "ofi_ratio_series": custom_metrics.get("ofi_ratio_series"),
            },
        }

    def _build_history_section(self, indicators: dict[str, Any]) -> dict[str, Any]:
        ohlcv_rows = indicators.get("ohlcv") or []
        trimmed = ohlcv_rows[-self.max_candles :]
        return {
            "candles": trimmed,
            "vwap_series": indicators.get("vwap_series"),
            "volume_series": (indicators.get("volume") or {}).get("series"),
            "volume_rsi_series": indicators.get("volume_rsi_series"),
        }

    def _build_indicator_section(self, indicators: dict[str, Any]) -> dict[str, Any]:
        macd = indicators.get("macd") or {}
        rsi = indicators.get("rsi")
        stoch = indicators.get("stoch_rsi") or {}
        bb = indicators.get("bollinger_bands") or {}
        ma = indicators.get("moving_averages") or {}
        adx = indicators.get("adx") or {}
        obv = indicators.get("obv") or {}
        cmf = indicators.get("cmf") or {}
        return {
            "rsi": rsi,
            "stoch_rsi": stoch,
            "macd": {
                "value": macd.get("value"),
                "signal": macd.get("signal"),
                "hist": macd.get("hist"),
                "series": macd.get("series"),
            },
            "bollinger_bands": bb,
            "moving_averages": ma,
            "adx": {
                "value": adx.get("value"),
                "di_plus": adx.get("di_plus"),
                "di_minus": adx.get("di_minus"),
                "series": adx.get("series"),
            },
            "obv": {
                "value": obv.get("value"),
                "series": obv.get("series"),
            },
            "cmf": {
                "value": cmf.get("value"),
                "series": cmf.get("series"),
            },
            "vwap": indicators.get("vwap"),
            "atr": indicators.get("atr"),
            "atr_pct": indicators.get("atr_pct"),
            "volume": indicators.get("volume"),
        }

    def _build_positions_section(self, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for pos in positions:
            symbol = pos.get("instId") or pos.get("symbol")
            if not symbol:
                continue
            size = _to_float(pos.get("pos") or pos.get("posQty") or pos.get("size"))
            avg_px = _to_float(pos.get("avgPx") or pos.get("avgPxPx"))
            side = (pos.get("posSide") or pos.get("side") or ("LONG" if (size or 0) >= 0 else "SHORT")).upper()
            leverage = pos.get("lever") or pos.get("leverage")
            summary.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "avg_px": avg_px,
                    "leverage": leverage,
                    "margin_mode": pos.get("mgnMode"),
                }
            )
        return summary

    def _build_account_section(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "account_equity": snapshot.get("account_equity"),
            "total_account_value": snapshot.get("total_account_value"),
            "total_eq_usd": snapshot.get("total_eq_usd"),
        }

    @staticmethod
    def _default_guardrails() -> dict[str, Any]:
        return {
            "max_leverage": 5,
            "max_position_pct": 0.2,
            "daily_loss_limit_pct": 3,
            "risk_model": "ATR based stops x1.5",
            "min_hold_seconds": 180,
            "max_trades_per_hour": 2,
            "trade_window_seconds": 3600,
            "require_position_alignment": True,
        }

    @staticmethod
    def _response_schema(model_id: str | None, overrides: dict[str, Any] | None) -> dict[str, Any]:
        if model_id and isinstance(overrides, dict):
            override = overrides.get(model_id)
            if isinstance(override, dict):
                return copy.deepcopy(override)
        return copy.deepcopy(RESPONSE_SCHEMA)