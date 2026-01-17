from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


DEFAULT_SYSTEM_PROMPT = (
    "You are a professional hedge fund trader with strict risk controls. Evaluate the provided snapshot, which includes "
    "positions, exposures, technical indicators, order flow, derivatives, liquidity context, and guardrails. Always "
    "verify how stale the snapshot is, whether open positions already exist on the symbol, and whether guardrails such "
    "as leverage caps or cooldowns might block an order."
)

DEFAULT_DECISION_PROMPT = (
    "You will receive a JSON object under the key 'context' containing the latest market state, snapshot freshness metadata, "
    "account/portfolio exposure, any pending orders, and execution guardrails. Before deciding on BUY/SELL/HOLD, confirm: "
    "(1) the snapshot age is within limits, (2) your recommendation complies with leverage, position size, cooldown, and trade "
    "limit guardrails, and (3) you are not duplicating an existing position or pending order. Explicitly cite snapshot freshness "
    "and guardrail checks in your rationale. Inspect 'context.execution.margin_health' for real-time capital caps and treat "
    "'context.execution_feedback' (and its digest) as hard blockers that must be resolved before sizing up. When existing "
    "stop-loss or take-profit levels are present, reuse or gently tune them unless you can justify a safer alternative. Choose HOLD "
    "whenever cooldowns, capital constraints, fee/credit depletion, or duplicate exposure prevent execution, and describe the "
    "blocker. Respond strictly as JSON matching 'response_schema'."
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
        liquidations = market_block.get("liquidations") or snapshot.get("liquidations") or []
        snapshot_health = self._build_snapshot_health(snapshot, runtime_meta)

        live_section = self._build_live_section(ticker, funding, open_interest, custom_metrics, order_book)
        history_section = self._build_history_section(indicators)
        indicator_section = self._build_indicator_section(indicators)
        positions_section = self._build_positions_section(snapshot.get("positions") or [])
        account_section = self._build_account_section(snapshot, resolved_symbol)
        execution_limits = self._resolve_execution_limits(snapshot, resolved_symbol)
        exposure_section = self._build_exposure_summary(
            positions_section,
            snapshot,
            account_section,
        )
        pending_orders = self._build_pending_orders(snapshot.get("open_orders") or [])
        guardrails = runtime_meta.get("guardrails") or self._default_guardrails()
        model_id = runtime_meta.get("llm_model_id")
        instrument_spec = self._instrument_spec(snapshot, resolved_symbol)
        runtime_min_size = _to_float(runtime_meta.get("execution_min_size"))
        spec_min_size = instrument_spec.get("min_size") if instrument_spec else None
        min_size_value = spec_min_size if spec_min_size is not None else runtime_min_size
        price_hint = self._resolve_last_price(live_section)
        execution_settings = {
            "enabled": bool(runtime_meta.get("execution_enabled")),
            "trade_mode": runtime_meta.get("execution_trade_mode") or "cross",
            "order_type": runtime_meta.get("execution_order_type") or "market",
            "min_size": min_size_value,
        }
        if price_hint is not None:
            execution_settings["price_reference"] = price_hint
        if instrument_spec:
            execution_settings["symbol_rules"] = instrument_spec
        account_equity_usd = _to_float(account_section.get("account_equity")) or _to_float(
            account_section.get("total_eq_usd")
        )
        available_margin_usd = (
            _to_float(account_section.get("available_eq_usd"))
            or _to_float(account_section.get("quote_available_usd"))
        )
        live_account_equity = _to_float(execution_limits.get("account_equity_usd"))
        live_available_margin = _to_float(execution_limits.get("available_margin_usd"))
        margin_cap_usd = _to_float(
            execution_limits.get("max_notional_usd")
            or execution_limits.get("max_notional_from_margin")
        )
        live_max_leverage = _to_float(execution_limits.get("max_leverage"))
        tier_cap_usd = _to_float(execution_limits.get("tier_max_notional_usd"))
        tier_imr = _to_float(execution_limits.get("tier_initial_margin_ratio"))
        tier_source = execution_limits.get("tier_source")
        quote_available_override = _to_float(execution_limits.get("quote_available_usd"))
        quote_cash_override = _to_float(execution_limits.get("quote_cash_usd"))
        if live_account_equity is not None:
            account_equity_usd = live_account_equity
        if live_available_margin is not None:
            available_margin_usd = live_available_margin
        if available_margin_usd is None and price_hint and price_hint > 0:
            quote_available = _to_float(account_section.get("quote_available"))
            if quote_available is not None:
                available_margin_usd = quote_available * price_hint
        if available_margin_usd is not None:
            execution_settings["available_margin_usd"] = available_margin_usd
        if account_equity_usd is not None:
            execution_settings["account_equity_usd"] = account_equity_usd
        guardrail_max_leverage = live_max_leverage
        if guardrail_max_leverage is None:
            guardrail_max_leverage = _to_float(guardrails.get("max_leverage"))
        leverage_for_cap = guardrail_max_leverage if guardrail_max_leverage and guardrail_max_leverage > 0 else 1.0
        max_position_pct = _to_float(guardrails.get("max_position_pct"))
        symbol_cap_pct = None
        symbol_caps = guardrails.get("symbol_position_caps")
        if isinstance(symbol_caps, dict):
            symbol_key = self._normalize_symbol_key(resolved_symbol)
            if symbol_key:
                raw_value = symbol_caps.get(symbol_key)
                if raw_value is None and resolved_symbol:
                    raw_value = symbol_caps.get(resolved_symbol)
                symbol_cap_pct = _to_float(raw_value)
        guardrail_cap = None
        symbol_cap_value = None
        if account_equity_usd is not None and max_position_pct:
            guardrail_cap = account_equity_usd * leverage_for_cap * max_position_pct
            execution_settings["max_position_value_usd"] = guardrail_cap
            if price_hint:
                execution_settings["max_position_contracts"] = guardrail_cap / price_hint
        if max_position_pct is not None:
            execution_settings["max_position_pct"] = max_position_pct
        if symbol_cap_pct is not None:
            execution_settings["symbol_max_position_pct"] = symbol_cap_pct
        if account_equity_usd is not None and symbol_cap_pct:
            symbol_cap_value = account_equity_usd * leverage_for_cap * symbol_cap_pct
            execution_settings["symbol_max_position_value_usd"] = symbol_cap_value
            if price_hint and symbol_cap_value:
                execution_settings["symbol_max_position_contracts"] = symbol_cap_value / price_hint
        if margin_cap_usd is not None:
            execution_settings["margin_max_position_value_usd"] = margin_cap_usd
            if price_hint:
                execution_settings["margin_max_position_contracts"] = margin_cap_usd / price_hint
        if tier_cap_usd is not None:
            execution_settings["tier_max_position_value_usd"] = tier_cap_usd
            if price_hint:
                execution_settings["tier_max_position_contracts"] = tier_cap_usd / price_hint
        effective_candidates = [
            value
            for value in (guardrail_cap, margin_cap_usd, tier_cap_usd, symbol_cap_value)
            if value and value > 0
        ]
        if effective_candidates:
            effective_cap = min(effective_candidates)
            execution_settings["effective_max_position_value_usd"] = effective_cap
            if price_hint:
                execution_settings["effective_max_position_contracts"] = effective_cap / price_hint
        effective_max_leverage = guardrail_max_leverage if guardrail_max_leverage and guardrail_max_leverage > 0 else None
        if effective_max_leverage is not None:
            execution_settings["max_leverage"] = effective_max_leverage
        if tier_imr is not None:
            execution_settings["tier_initial_margin_ratio"] = tier_imr
        if tier_source:
            execution_settings["tier_source"] = tier_source
        live_snapshot: dict[str, Any] = {}
        if live_available_margin is not None:
            live_snapshot["available_margin_usd"] = live_available_margin
        if live_account_equity is not None:
            live_snapshot["account_equity_usd"] = live_account_equity
        if live_max_leverage is not None:
            live_snapshot["max_leverage"] = live_max_leverage
        if margin_cap_usd is not None:
            live_snapshot["max_notional_usd"] = margin_cap_usd
        if tier_cap_usd is not None:
            live_snapshot["tier_max_notional_usd"] = tier_cap_usd
        if tier_imr is not None:
            live_snapshot["tier_initial_margin_ratio"] = tier_imr
        if tier_source:
            live_snapshot["tier_source"] = tier_source
        if quote_available_override is not None:
            live_snapshot["quote_available_usd"] = quote_available_override
        if quote_cash_override is not None:
            live_snapshot["quote_cash_usd"] = quote_cash_override
        quote_currency_override = execution_limits.get("quote_currency")
        if quote_currency_override:
            live_snapshot["quote_currency"] = quote_currency_override
        for key in ("source", "updated_at"):
            value = execution_limits.get(key)
            if value:
                live_snapshot[key] = value
        if live_snapshot:
            execution_settings["live_margin_snapshot"] = live_snapshot
        margin_health = self._build_margin_health_section(execution_settings)
        if margin_health:
            execution_settings["margin_health"] = margin_health
        schema_overrides = runtime_meta.get("llm_response_schemas") or {}
        timeframe_value = timeframe or runtime_meta.get("ta_timeframe") or snapshot.get("timeframe") or "4H"
        trend_section = self._build_trend_confirmation(indicators, ticker, timeframe_value)
        liquidity_section = self._build_liquidity_profile(live_section, indicators, ticker)
        derivatives_section = self._build_derivatives_posture(funding, custom_metrics, liquidations)
        fee_window_summary = self._build_fee_window_summary(account_section)
        credit_availability = self._build_credit_availability()
        execution_feedback = self._format_execution_feedback(
            snapshot.get("execution_feedback"),
            symbol=resolved_symbol,
        )
        feedback_digest = self._build_execution_feedback_digest(execution_feedback)

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
            "portfolio_exposure": exposure_section,
            "portfolio_heatmap": exposure_section.get("heatmap"),
            "guardrails": guardrails,
            "trend_confirmation": trend_section,
            "liquidity_context": liquidity_section,
            "derivatives_posture": derivatives_section,
            "pending_orders": pending_orders,
            "snapshot_health": snapshot_health,
            "notes": runtime_meta.get("llm_notes"),
            "prompt_version_id": runtime_meta.get("prompt_version_id"),
            "prompt_version_name": runtime_meta.get("prompt_version_name"),
            "execution": execution_settings,
            "fee_availability": fee_window_summary,
            "credit_availability": credit_availability,
        }
        if execution_feedback:
            context["execution_feedback"] = execution_feedback
            execution_settings["recent_feedback"] = execution_feedback
        if feedback_digest:
            context["execution_feedback_digest"] = feedback_digest
            execution_settings["feedback_digest"] = feedback_digest
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

    def _build_trend_confirmation(
        self,
        indicators: dict[str, Any],
        ticker: dict[str, Any],
        timeframe: str,
    ) -> dict[str, Any]:
        adx_block = indicators.get("adx") or {}
        ma_block = indicators.get("moving_averages") or {}
        adx_value = _to_float(adx_block.get("value"))
        di_plus = _to_float(adx_block.get("di_plus"))
        di_minus = _to_float(adx_block.get("di_minus"))
        ema_50 = _to_float(ma_block.get("ema_50"))
        ema_200 = _to_float(ma_block.get("ema_200"))
        price = _to_float(ticker.get("last") or ticker.get("px"))

        if ema_50 is not None and ema_200 is not None:
            if ema_50 > ema_200:
                ema_bias = "bullish"
            elif ema_50 < ema_200:
                ema_bias = "bearish"
            else:
                ema_bias = "balanced"
        else:
            ema_bias = "unknown"

        if price is not None and ema_50 is not None:
            price_vs_ema = "above" if price > ema_50 else "below"
        else:
            price_vs_ema = "unknown"

        adx_state = "unknown"
        if adx_value is not None:
            if adx_value >= 25:
                adx_state = "trending"
            elif adx_value >= 18:
                adx_state = "transitioning"
            else:
                adx_state = "range-bound"

        di_state = "neutral"
        if di_plus is not None and di_minus is not None:
            if di_plus > di_minus:
                di_state = "+DI dominance"
            elif di_minus > di_plus:
                di_state = "-DI dominance"

        summary_bits: list[str] = []
        if adx_value is not None:
            summary_bits.append(f"ADX {adx_value:.1f} ({adx_state})")
        if di_state != "neutral":
            summary_bits.append(di_state)
        if ema_bias != "unknown":
            summary_bits.append(f"EMA stack {ema_bias}")
        if price_vs_ema != "unknown":
            summary_bits.append(f"price {price_vs_ema} EMA50")
        summary = ", ".join(summary_bits) or "Trend signals unavailable"

        return {
            "timeframe": timeframe,
            "adx": {
                "value": adx_value,
                "di_plus": di_plus,
                "di_minus": di_minus,
                "state": adx_state,
            },
            "moving_averages": {
                "ema_50": ema_50,
                "ema_200": ema_200,
                "bias": ema_bias,
                "price_vs_ema_50": price_vs_ema,
            },
            "summary": summary,
        }

    def _build_liquidity_profile(
        self,
        market: dict[str, Any],
        indicators: dict[str, Any],
        ticker: dict[str, Any],
    ) -> dict[str, Any]:
        order_flow = market.get("order_flow") or {}
        spread = _to_float(market.get("spread"))
        spread_pct = _to_float(market.get("spread_pct"))
        bid_depth = _to_float(order_flow.get("bid_depth"))
        ask_depth = _to_float(order_flow.get("ask_depth"))
        obv_block = indicators.get("obv") or {}
        cmf_block = indicators.get("cmf") or {}
        volume_block = indicators.get("volume") or {}
        last_price = _to_float(market.get("last_price") or ticker.get("last") or ticker.get("px"))
        depth_floor = None
        if bid_depth is not None and ask_depth is not None:
            depth_floor = min(bid_depth, ask_depth)

        target_usd = 100000.0
        target_units = (target_usd / last_price) if last_price else None
        slippage_bps = None
        if (
            spread is not None
            and spread > 0
            and target_units is not None
            and depth_floor is not None
            and depth_floor > 0
            and last_price
        ):
            depth_ratio = target_units / depth_floor
            impact_multiplier = min(max(depth_ratio, 0.1), 3.0)
            implied_move = spread * impact_multiplier
            slippage_bps = (implied_move / last_price) * 10000

        imbalance_raw = order_flow.get("imbalance")
        if isinstance(imbalance_raw, dict):
            imbalance_value = _to_float(imbalance_raw.get("net"))
        else:
            imbalance_value = _to_float(imbalance_raw)
        if imbalance_value is None:
            liquidity_bias = "balanced"
        elif imbalance_value > 0:
            liquidity_bias = "bid-supported"
        elif imbalance_value < 0:
            liquidity_bias = "ask-heavy"
        else:
            liquidity_bias = "balanced"

        summary_bits: list[str] = []
        if spread_pct is not None:
            summary_bits.append(f"spread {spread_pct:.3f}%")
        if slippage_bps is not None:
            summary_bits.append(f"~{slippage_bps:.1f} bps est. slippage for $100k")
        summary_bits.append(liquidity_bias)

        return {
            "obv": {
                "value": _to_float(obv_block.get("value")),
            },
            "cmf": {
                "value": _to_float(cmf_block.get("value")),
            },
            "volume": {
                "last": _to_float(volume_block.get("last")),
                "average": _to_float(volume_block.get("average")),
            },
            "spread": spread,
            "spread_pct": spread_pct,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "estimated_slippage_bps": round(slippage_bps, 2) if slippage_bps is not None else None,
            "liquidity_bias": liquidity_bias,
            "summary": ", ".join(summary_bits),
        }

    def _build_derivatives_posture(
        self,
        funding: dict[str, Any],
        custom_metrics: dict[str, Any],
        liquidations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_rate = _to_float(funding.get("fundingRate") or funding.get("fundRate"))
        next_rate = _to_float(funding.get("nextFundingRate"))
        previous_rate = _to_float(funding.get("prevFundingRate") or funding.get("fundingRatePrev"))
        funding_delta = None
        if current_rate is not None and next_rate is not None:
            funding_delta = next_rate - current_rate
        elif current_rate is not None and previous_rate is not None:
            funding_delta = current_rate - previous_rate

        long_short = custom_metrics.get("market_long_short_ratio") or {}
        ls_value = _to_float(long_short.get("value"))
        if ls_value is None:
            ls_bias = "balanced"
        elif ls_value > 1:
            ls_bias = "long-heavy"
        elif ls_value < 1:
            ls_bias = "short-heavy"
        else:
            ls_bias = "balanced"

        liquidation_clusters = self._summarize_liquidations(liquidations)

        summary_bits: list[str] = []
        if current_rate is not None:
            summary_bits.append(f"funding {current_rate * 100:.3f}%")
        if funding_delta is not None:
            summary_bits.append(f"delta {funding_delta * 100:.3f}%")
        summary_bits.append(f"L/S {ls_bias}")
        if liquidation_clusters:
            summary_bits.append(
                f"top liq {liquidation_clusters[0]['side']} @ {liquidation_clusters[0]['price']:.0f}"
            )

        return {
            "funding": {
                "current": current_rate,
                "next": next_rate,
                "previous": previous_rate,
                "delta": funding_delta,
                "timestamp": funding.get("fundingTime"),
            },
            "long_short_ratio": long_short,
            "liquidation_clusters": liquidation_clusters,
            "summary": ", ".join(summary_bits),
        }

    def _summarize_liquidations(self, liquidations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        clusters: list[dict[str, Any]] = []
        if not liquidations:
            return clusters
        for entry in liquidations:
            if not isinstance(entry, dict):
                continue
            price = _to_float(entry.get("px") or entry.get("price") or entry.get("fillPx"))
            size = _to_float(entry.get("sz") or entry.get("size") or entry.get("qty"))
            if price is None or size is None:
                continue
            side = str(entry.get("side") or entry.get("posSide") or "").upper() or "UNKNOWN"
            notional = abs(price * size)
            clusters.append(
                {
                    "price": price,
                    "size": size,
                    "side": side,
                    "notional": notional,
                    "raw": {k: entry.get(k) for k in ("px", "sz", "side", "posSide", "ccy", "ts")},
                }
            )
        clusters.sort(key=lambda item: item.get("notional") or 0, reverse=True)
        return clusters[:10]

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

    @staticmethod
    def _quote_currency(symbol: str | None) -> str | None:
        if not symbol:
            return None
        parts = str(symbol).split("-")
        if len(parts) >= 2:
            return parts[1].upper()
        return None

    @staticmethod
    def _normalize_symbol_key(symbol: str | None) -> str | None:
        if not symbol:
            return None
        value = str(symbol).strip().upper()
        return value or None

    def _instrument_spec(self, snapshot: dict[str, Any], symbol: str | None) -> dict[str, float]:
        if not symbol:
            return {}
        specs = snapshot.get("instrument_specs")
        if not isinstance(specs, dict):
            return {}
        symbol_key = str(symbol).strip()
        entry = specs.get(symbol_key)
        if not entry and symbol_key:
            entry = specs.get(symbol_key.upper()) or specs.get(symbol_key.lower())
        if not isinstance(entry, dict):
            return {}
        normalized: dict[str, float] = {}
        for key in ("min_size", "lot_size", "tick_size"):
            value = _to_float(entry.get(key))
            if value is not None and value > 0:
                normalized[key] = value
        return normalized

    def _build_account_section(self, snapshot: dict[str, Any], symbol: str | None) -> dict[str, Any]:
        raw_balances = snapshot.get("available_balances") or {}
        normalized_balances: dict[str, dict[str, Any]] = {}
        for currency, stats in raw_balances.items():
            if not isinstance(stats, dict):
                continue
            key = str(currency).upper()
            normalized_balances[key] = {
                "currency": key,
                "equity": _to_float(stats.get("equity")),
                "equity_usd": _to_float(stats.get("equity_usd")),
                "available": _to_float(stats.get("available")),
                "available_usd": _to_float(stats.get("available_usd")),
                "cash": _to_float(stats.get("cash")),
            }
        quote_currency = self._quote_currency(symbol or snapshot.get("symbol"))
        quote_stats = normalized_balances.get(quote_currency, {}) if quote_currency else {}
        return {
            "account_equity": snapshot.get("account_equity"),
            "total_account_value": snapshot.get("total_account_value"),
            "total_eq_usd": snapshot.get("total_eq_usd"),
            "available_equity": _to_float(snapshot.get("available_equity")),
            "available_eq_usd": _to_float(snapshot.get("available_eq_usd")),
            "available_balances": normalized_balances or None,
            "quote_currency": quote_currency,
            "quote_available": _to_float(quote_stats.get("available")) if quote_stats else None,
            "quote_available_usd": _to_float(quote_stats.get("available_usd")) if quote_stats else None,
        }

    def _resolve_execution_limits(self, snapshot: dict[str, Any], symbol: str | None) -> dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        limits = snapshot.get("execution_limits")
        if not isinstance(limits, dict):
            return {}
        symbol_key = (symbol or snapshot.get("symbol") or "").strip().upper()
        if not symbol_key:
            return {}
        entry = limits.get(symbol_key)
        if isinstance(entry, dict):
            return entry
        return {}

    def _build_snapshot_health(self, snapshot: dict[str, Any], runtime_meta: dict[str, Any]) -> dict[str, Any]:
        timestamp = snapshot.get("generated_at")
        parsed = self._parse_timestamp(timestamp)
        now = datetime.now(timezone.utc)
        age_seconds: Optional[int] = None
        if parsed is not None:
            age_seconds = max(0, int((now - parsed).total_seconds()))
        max_age = runtime_meta.get("snapshot_max_age_seconds")
        try:
            max_age_int = int(max_age) if max_age is not None else None
        except (TypeError, ValueError):
            max_age_int = None
        stale = age_seconds is None or (
            max_age_int is not None and age_seconds is not None and age_seconds > max_age_int
        )
        return {
            "generated_at": timestamp,
            "age_seconds": age_seconds,
            "max_age_seconds": max_age_int,
            "stale": stale,
        }

    def _build_exposure_summary(
        self,
        positions: list[dict[str, Any]],
        snapshot: dict[str, Any],
        account: dict[str, Any],
    ) -> dict[str, Any]:
        market_data = snapshot.get("market_data") or {}
        fallback_ticker = snapshot.get("ticker") or {}
        account_equity = _to_float(account.get("account_equity"))
        long_notional = 0.0
        short_notional = 0.0
        heatmap: list[dict[str, Any]] = []
        for entry in positions:
            symbol = entry.get("symbol")
            if not symbol:
                continue
            size = _to_float(entry.get("size"))
            if size is None:
                continue
            ticker = (market_data.get(symbol) or {}).get("ticker") or fallback_ticker
            price = _to_float(ticker.get("last") if isinstance(ticker, dict) else None)
            if price is None:
                continue
            notional = abs(size * price)
            side = (entry.get("side") or "").upper()
            if side == "SHORT":
                short_notional += notional
            else:
                long_notional += notional
            heatmap.append(
                {
                    "symbol": symbol,
                    "side": side or "LONG",
                    "notional": notional,
                    "pct_of_equity": _percent(notional, account_equity) if account_equity else None,
                }
            )
        net_exposure = long_notional - short_notional
        net_pct = _percent(net_exposure, account_equity) if account_equity else None
        summary_bits: list[str] = []
        if long_notional:
            summary_bits.append(f"Long ${long_notional:,.0f}")
        if short_notional:
            summary_bits.append(f"Short ${short_notional:,.0f}")
        if net_pct is not None:
            summary_bits.append(f"Net {net_pct:.1f}% of equity")
        if not summary_bits:
            summary_bits.append("Flat")
        heatmap.sort(key=lambda item: item.get("notional") or 0, reverse=True)
        heatmap_trimmed = heatmap[:12]
        return {
            "long_notional": long_notional if long_notional else None,
            "short_notional": short_notional if short_notional else None,
            "net_exposure": net_exposure if (long_notional or short_notional) else None,
            "net_pct_of_equity": net_pct,
            "summary": ", ".join(summary_bits),
            "heatmap": heatmap_trimmed or None,
        }

    def _build_fee_window_summary(self, account: dict[str, Any]) -> dict[str, Any] | None:
        metadata = self.metadata or {}
        total_fee = _to_float(metadata.get("okx_fee_window_total"))
        if total_fee is None:
            return None
        window_hours_raw = metadata.get("fee_window_hours")
        try:
            window_hours = float(window_hours_raw) if window_hours_raw is not None else None
        except (TypeError, ValueError):  # pragma: no cover - defensive
            window_hours = None
        account_equity = _to_float(account.get("account_equity"))
        pct = _percent(total_fee, account_equity) if account_equity else None
        return {
            "window_hours": window_hours,
            "total_fee": total_fee,
            "pct_of_equity": pct,
            "note": "Fees gathered from recent OKX fills",
        }

    def _build_credit_availability(self) -> dict[str, Any] | None:
        metadata = self.metadata or {}
        usage = metadata.get("openrouter_usage")
        if not isinstance(usage, dict):
            return None
        remaining = _to_float(usage.get("remaining"))
        granted = _to_float(usage.get("granted"))
        used = _to_float(usage.get("used"))
        if remaining is None and granted is None and used is None:
            return None
        return {
            "remaining": remaining,
            "granted": granted,
            "used": used,
            "currency": usage.get("currency") or "USD",
            "resets_at": usage.get("resets_at"),
        }

    def _build_margin_health_section(self, execution_settings: dict[str, Any]) -> dict[str, Any] | None:
        if not execution_settings:
            return None
        available_margin = _to_float(execution_settings.get("available_margin_usd"))
        account_equity = _to_float(execution_settings.get("account_equity_usd"))
        guardrail_cap = _to_float(execution_settings.get("max_position_value_usd"))
        margin_cap = _to_float(execution_settings.get("margin_max_position_value_usd"))
        tier_cap = _to_float(execution_settings.get("tier_max_position_value_usd"))
        effective_cap = _to_float(execution_settings.get("effective_max_position_value_usd"))
        symbol_cap = _to_float(execution_settings.get("symbol_max_position_value_usd"))
        symbol_cap_pct = _to_float(execution_settings.get("symbol_max_position_pct"))
        live_snapshot = execution_settings.get("live_margin_snapshot") or {}
        tier_imr = _to_float(execution_settings.get("tier_initial_margin_ratio"))
        max_leverage = _to_float(execution_settings.get("max_leverage"))
        updated_at = live_snapshot.get("updated_at") or execution_settings.get("updated_at")
        freshness_seconds: Optional[int] = None
        if updated_at:
            parsed = self._parse_timestamp(updated_at)
            if parsed:
                freshness_seconds = max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))
        caps = [
            ("guardrail", guardrail_cap),
            ("symbol", symbol_cap),
            ("margin", margin_cap),
            ("tier", tier_cap),
        ]
        limiting_factor: Optional[str] = None
        limiting_value = effective_cap
        if limiting_value is None:
            for label, value in caps:
                normalized = _to_float(value)
                if normalized is None or normalized <= 0:
                    continue
                if limiting_value is None or normalized < limiting_value:
                    limiting_value = normalized
                    limiting_factor = label
        else:
            tolerance = max(1.0, abs(limiting_value) * 0.001)
            for label, value in caps:
                normalized = _to_float(value)
                if normalized is None:
                    continue
                if abs(normalized - limiting_value) <= tolerance:
                    limiting_factor = label
                    break
        summary_bits: list[str] = []
        if available_margin is not None:
            summary_bits.append(f"${available_margin:,.0f} free margin")
        if limiting_value:
            label = limiting_factor or "cap"
            summary_bits.append(f"cap ${limiting_value:,.0f} ({label})")
        if symbol_cap is not None and symbol_cap_pct is not None:
            summary_bits.append(f"symbol cap {symbol_cap_pct * 100:.1f}%")
        if tier_imr is not None:
            summary_bits.append(f"tier IMR {tier_imr * 100:.2f}%")
        if freshness_seconds is not None:
            summary_bits.append(f"snapshot age {freshness_seconds}s")
        return {
            "available_margin_usd": available_margin,
            "account_equity_usd": account_equity,
            "effective_cap_usd": limiting_value,
            "limiting_factor": limiting_factor,
            "guardrail_cap_usd": guardrail_cap,
            "symbol_cap_usd": symbol_cap,
            "margin_cap_usd": margin_cap,
            "tier_cap_usd": tier_cap,
            "symbol_cap_pct": symbol_cap_pct,
            "tier_initial_margin_ratio": tier_imr,
            "max_leverage": max_leverage,
            "updated_at": updated_at,
            "freshness_seconds": freshness_seconds,
            "stale": bool(freshness_seconds and freshness_seconds > 600),
            "live_snapshot": live_snapshot or None,
            "summary": ", ".join(summary_bits) if summary_bits else None,
        }

    @staticmethod
    def _resolve_last_price(market_data: dict[str, Any] | None) -> Optional[float]:
        if not isinstance(market_data, dict):
            return None
        price_keys = ("last_price", "mark_px", "mark_price", "mid_price", "last", "px", "ask", "bid")
        for key in price_keys:
            price_value = _to_float(market_data.get(key))
            if price_value:
                return price_value
        return None

    def _format_execution_feedback(
        self,
        feedback: Any,
        limit: int = 5,
        *,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        if not isinstance(feedback, list) or limit <= 0:
            return []
        normalized_symbol = symbol.upper() if symbol else None
        selected: list[dict[str, Any]] = []
        for entry in reversed(feedback):
            if not isinstance(entry, dict):
                continue
            entry_symbol = str(entry.get("symbol") or "").upper()
            if normalized_symbol and entry_symbol and entry_symbol != normalized_symbol:
                continue
            selected.append(entry)
            if len(selected) >= limit:
                break
        formatted: list[dict[str, Any]] = []
        for entry in reversed(selected):
            formatted.append(
                {
                    "timestamp": entry.get("timestamp"),
                    "symbol": entry.get("symbol"),
                    "side": entry.get("side"),
                    "size": entry.get("size"),
                    "message": entry.get("message"),
                    "level": entry.get("level"),
                    "meta": entry.get("meta"),
                }
            )
        return formatted

    def _build_execution_feedback_digest(self, feedback: list[dict[str, Any]] | None) -> dict[str, Any] | None:
        if not feedback:
            return None
        counts: dict[str, int] = {}
        for entry in feedback:
            level = str(entry.get("level") or "info").lower()
            counts[level] = counts.get(level, 0) + 1
        blockers: list[dict[str, Any]] = []
        for entry in reversed(feedback):
            level = str(entry.get("level") or "info").lower()
            if level in {"warning", "error"}:
                blockers.append(entry)
            if len(blockers) >= 3:
                break
        summary_bits: list[str] = []
        if blockers:
            summary_bits.append(
                "; ".join(
                    f"{item.get('level', '').upper()}: {item.get('message')}" for item in blockers if item.get("message")
                )
            )
        latest = feedback[-1]
        if latest.get("timestamp"):
            summary_bits.append(f"Latest feedback @ {latest['timestamp']}")
        summary = " | ".join(part for part in summary_bits if part)
        return {
            "counts": counts,
            "recent_blockers": blockers or None,
            "latest": latest,
            "summary": summary or None,
        }

    def _build_pending_orders(self, orders: list[dict[str, Any]]) -> dict[str, Any]:
        if not orders:
            return {"total": 0, "by_side": {}, "open": []}
        formatted: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for order in orders[:20]:
            if not isinstance(order, dict):
                continue
            symbol = order.get("instId") or order.get("symbol")
            side = str(order.get("side") or order.get("posSide") or "").upper() or "UNKNOWN"
            price = _to_float(order.get("px") or order.get("price"))
            size = _to_float(order.get("sz") or order.get("size"))
            state = order.get("state") or order.get("status")
            formatted.append({
                "symbol": symbol,
                "side": side,
                "price": price,
                "size": size,
                "state": state,
            })
            counts[side] = counts.get(side, 0) + 1
        return {
            "total": len(formatted),
            "by_side": counts,
            "open": formatted,
        }

    @staticmethod
    def _parse_timestamp(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            text = str(value).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return datetime.fromisoformat(text)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _default_guardrails() -> dict[str, Any]:
        return {
            "min_leverage": 1,
            "max_leverage": 5,
            "max_position_pct": 0.2,
            "symbol_position_caps": {},
            "daily_loss_limit_pct": 3,
            "risk_model": "ATR based stops x1.5",
            "min_hold_seconds": 180,
            "max_trades_per_hour": 2,
            "trade_window_seconds": 3600,
            "require_position_alignment": True,
            "snapshot_max_age_seconds": 900,
            "wait_for_tp_sl": False,
            "fallback_orders_enabled": True,
            "min_leverage_confidence_gate": 0.5,
        }

    @staticmethod
    def _response_schema(model_id: str | None, overrides: dict[str, Any] | None) -> dict[str, Any]:
        if model_id and isinstance(overrides, dict):
            override = overrides.get(model_id)
            if isinstance(override, dict):
                return copy.deepcopy(override)
        return copy.deepcopy(RESPONSE_SCHEMA)