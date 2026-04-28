from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from app.services.prompt_utils import sanitize_prompt_text

logger = logging.getLogger(__name__)

# Keys whose values should NOT be rounded even if they are floats (timestamps, IDs, etc.)
_ROUND_FLOAT_SKIP_KEYS: frozenset[str] = frozenset({
    "ts", "timestamp", "generated_at", "updated_at", "observed_at",
    "next_funding", "next_settlement", "resets_at", "locked_at",
    "fundingTime", "nextFundingTime", "series_age_hours",
})


DEFAULT_SYSTEM_PROMPT = (
    "You are a professional hedge fund trader with profitability as primary goal, but with strict risk controls. "
    "Evaluate the provided snapshot using a top-down, hierarchical framework: higher-timeframe trend first, "
    "then volume/participation confirmation, then momentum timing. "
    "Always verify existing positions and whether guardrails such as leverage caps might block an order. "
    "Respond with ONLY a valid JSON object matching 'response_schema'. "
    "Do not include any text, markdown, or explanation outside the JSON."
)

# ---------------------------------------------------------------------------
# Prompt section constants — each is one logical block of the default decision
# prompt.  PromptBuilder.build() assembles them via assemble_decision_prompt()
# so that live guardrail state is always reflected without string-patching.
# All sections end with a trailing space so they concatenate cleanly.
# ---------------------------------------------------------------------------

_SEC_INTRO = (
    "You will receive a JSON object under the key 'context' containing the latest market state, "
    "account/portfolio exposure (including total equity and available margin), any pending orders, and execution guardrails. "
    "RULE PRECEDENCE (in case of conflict): Pre-flight checks > Signal Hierarchy (Step 4) > "
    "Regime Rules > Archetype preference > Confidence penalties. "
)

_SEC_STEP1_PREFLIGHT = (
    "STEP 1 — PRE-FLIGHT: context.pre_computed_modifiers.pre_flight_passed confirms that capital is sufficient to enter. "
    "If execution_feedback_blocks is true, prefer HOLD. "
    "If has_existing_position or has_pending_order is true in your proposed direction, prefer HOLD or close/reverse instead. "
    "If portfolio_overexposed is true (net exposure > 100% of equity), add +0.10 to risk_score and note concentration risk in rationale. "
    "MINIMUM HOLD TIME: positions must be held for at least context.guardrails.min_hold_seconds; "
    "if near-term volatility (e.g. funding settlement within min_hold_seconds) could trigger a stop before the hold period expires, prefer HOLD. "
    "Cite pre-flight status in rationale. "
)

_SEC_STEP2_HTF = (
    "STEP 2 — HTF TREND FILTER (confidence modifier, not a hard veto): "
    "context.pre_computed_modifiers provides pre-computed HTF alignment and penalty values. "
    "htf_alignment_class gives the HTF bias: 'bullish', 'bearish', or 'neutral'/'unavailable' when weak or no data. "
    "If your direction aligns with htf_alignment_class (bullish→BUY, bearish→SELL): apply htf_align_penalty to base confidence. "
    "If your direction contradicts htf_alignment_class: apply htf_contradict_additive to base confidence, then multiply by htf_contradict_multiplicative. "
    "htf_contradict_bracket labels the ADX-based severity bracket for rationale. "
    "PENALTY ARITHMETIC ORDER: first apply the additive penalty to base confidence, then apply the multiplicative scaling. "
    "The ranging penalty (from regime rules) stacks additively with htf_contradict_additive. "
    "A counter-trend trade is allowed only if net confidence after all penalties ≥ 0.45; otherwise HOLD. "
    "State htf_alignment_class, htf_adx, and htf_contradict_bracket (if contradicting) in rationale. "
)

_SEC_STEP3_DIVERGENCE = (
    "STEP 3 — DIVERGENCE & POSITIONING CHECK (strong filter): "
    "context.market_signals.obv_trend and context.market_signals.cvd_trend are pre-computed — use them as the primary signal. "
    "OBV LABEL VERIFICATION: context.market_signals.obv_trend_confidence is pre-computed as 'strong', 'moderate', 'weak', or 'unknown'. "
    "Use pre_computed_modifiers.obv_effective_weight (1.0=strong, 0.75=moderate, 0.5=weak/unknown) to scale OBV signal strength. "
    "You may still cross-check against the last 5 values of the OBV series (context.indicators.obv if present) for additional confirmation. "
    "If recent values are flat or reversing against the label, treat the label as stale and reduce its weight by half. "
    "DIVERGENCE LABEL SANITY CHECK: if obv_trend says 'diverging_X' but the actual price direction does not match the divergence definition "
    "(i.e. 'diverging_bearish' requires price RISING + OBV FALLING; 'diverging_bullish' requires price FALLING + OBV RISING), "
    "reclassify the signal as 'confirming' (price and OBV moving together) and note the mismatch in rationale. "
    "If obv_trend is 'diverging_bearish' (price rising, OBV falling), a BUY is strongly discouraged. "
    "If obv_trend is 'diverging_bullish' (price falling, OBV rising), a SELL is strongly discouraged. "
    "Divergence against your proposed direction should be cited as a risk factor and typically warrants HOLD. "
    "CVD LABEL VERIFICATION: context.market_signals.cvd_trend_confidence is pre-computed as 'strong', 'moderate', 'weak', or 'unknown'. "
    "Use pre_computed_modifiers.cvd_effective_weight similarly to scale CVD signal strength. "
    "You may still cross-check against the last 5 values of context.market.order_flow.cvd_series for additional confirmation. "
    "If the recent slope contradicts the cvd_trend label (e.g. label says 'net_positive_rising' but the series shows a deep negative value and falling), "
    "treat the label as stale and reduce its weight by half. State the observed slope vs. label in rationale. "
    "CVD UNKNOWN RULE: if cvd_trend is 'unknown', apply −0.05 confidence and explicitly note in rationale that order-flow confirmation is absent. "
    "ORDER FLOW RELIABILITY: if pre_computed_modifiers.order_flow_reliable is false, "
    "apply pre_computed_modifiers.order_flow_penalty to confidence and note 'unreliable order flow' in rationale. "
    "SLIPPAGE ABSENT RULE: if context.liquidity_context.estimated_slippage_bps is null, "
    "estimate slippage from spread_pct × 2 as a conservative proxy for risk_score computation, and note this assumption. "
    "If context.liquidity_context.slippage_note is present, it means the system has already provided a conservative "
    "estimate due to degraded data — use the provided value and add it to risk_score considerations. "
    "L/S RATIO: use pre_computed_modifiers.ls_signal for positioning context. "
    "'squeeze_candidate' → add ls_confidence_add to BUY confidence (price should also be rising); "
    "'pressure_candidate' → add ls_confidence_add to SELL confidence. "
    "If ls_signal is 'stale_series', 'absent', or 'insufficient_data', use pre_computed_modifiers.ls_current_value for directional bias only (null means no L/S data available). "
    "If ls_signal is 'absent' and ls_confidence_add > 0, partial Archetype C applies (funding below −0.15%). "
    "Do NOT reproduce the L/S series in rationale — state ls_signal and bias direction only. "
)

_SEC_STEP4_SIGNALS = (
    "STEP 4 — SIGNAL HIERARCHY (for resolving conflicts): "
    "When signals conflict, weight them in this order: "
    "(1) Trend (ADX/DMI): a strong trend (ADX > 30) overrides most oscillators. "
    "(2) Volume/Participation (OBV, CVD): price movement unconfirmed by volume is weak; divergence here is a major red flag. "
    "(3) Momentum (MACD, RSI): use for entry timing, not primary direction. "
    "(4) Order Flow (imbalance, depth): confirms short-term liquidity but is secondary to trend and volume. "
    "If the majority of higher-ranked signals conflict, choose HOLD. "
    "RSI/STOCH RSI EXIT CLAUSE: if context.market_signals.rsi_zone is 'overbought' and you propose a BUY, "
    "do NOT veto the trade if ADX > 30 and DI+ dominates — instead add ≥0.25 to risk_score (the execution layer will reduce size proportionally) "
    "and shift to a limit order placed at the nearest LTF support level from context.market_signals.swing_low_ltf. "
    "Document this adjustment explicitly in 'notes'. "
    "OFI RATIO SERIES: if context.market.order_flow.ofi_ratio_series is present, it shows order-flow imbalance momentum per candle. "
    "Values > 1.5 suggest aggressive directional buying; values near 0.0 suggest inactive or balanced flow. "
    "Use within the Order Flow tier — a sustained run of high OFI values (> 1.5 for 3+ periods) in the trade direction adds modest confirmation (+0.03 confidence); "
    "sustained near-zero values weaken order-flow confirmation. Do NOT treat OFI ratio as a standalone signal. "
)


def _build_footprint_prompt_section(
    footprint_cfg: "dict[str, Any] | None" = None,
) -> str:
    """Return the footprint-chart paragraph with configurable thresholds.

    All four numeric thresholds are read from *footprint_cfg* (the
    ``guardrails.footprint`` sub-dict).  Falls back to the original
    hard-coded defaults when the dict is absent.
    """
    cfg = footprint_cfg or {}
    poc_risk = cfg.get("poc_risk_delta", 0.05)
    nd_conf = cfg.get("net_delta_confidence_delta", 0.02)
    iz_conf = cfg.get("imbalance_zone_confidence_delta", 0.03)
    iz_prox = cfg.get("imbalance_zone_proximity_pct", 0.3)
    return (
        "FOOTPRINT CHART: if context.market.footprint is present, it summarises 15 minutes of live tape as a volume-at-price map. "
        "poc_price is the price level with the highest traded volume in the window — treat it as the strongest near-term support/resistance. "
        "value_area_high and value_area_low bound the 70 % of volume closest to the POC — price inside this band is at fair value; "
        "a break outside the band on volume is a directional signal. "
        "net_delta > 0 means buyers dominated the window (bullish microstructure); net_delta < 0 means sellers dominated. "
        "delta_imbalance_zones lists up to 5 price levels with the largest single-sided imbalance: "
        "type 'buy_pressure' = aggressive buyers at that level (support cluster); 'sell_pressure' = aggressive sellers (resistance cluster). "
        "USAGE RULES: "
        f"(a) poc_price above last_price → overhead resistance — raise risk_score +{poc_risk} for a BUY, or add confirmation for a SELL. "
        f"(b) poc_price below last_price → support — raise risk_score +{poc_risk} for a SELL, or add confirmation for a BUY. "
        f"(c) A 'buy_pressure' imbalance zone within {iz_prox} % of proposed entry adds +{iz_conf} confidence for a BUY; "
        f"a 'sell_pressure' zone within {iz_prox} % of entry adds +{iz_conf} confidence for a SELL. "
        f"(d) net_delta confirming direction adds +{nd_conf} confidence; net_delta opposing direction subtracts {nd_conf}. "
        "Do NOT cite footprint as a standalone signal — always combine with OBV/CVD and trend context. "
        "If footprint is absent, skip these rules silently. "
    )


# Static default (uses hard-coded threshold values) — used by PROMPT_SECTIONS
# and as the fallback when guardrails.footprint is absent.
_SEC_STEP4_FOOTPRINT: str = _build_footprint_prompt_section()

_SEC_STEP5_SIZING = (
    "STEP 5 — CONFIDENCE AND SIZING: "
    "Your confidence score (0–1) reflects the proportion of signals aligning with your directional bias after applying all Step 2 penalties: "
    "0.75–1.0: HTF aligned, LTF momentum and volume confirm — full size. "
    "0.55–0.75: HTF neutral or mildly opposed, but LTF signals are clear — proportionally reduced size. "
    "0.45–0.55: meaningful conflicts — small size only if R:R is compelling. "
    "< 0.45: too many conflicts — you MUST recommend HOLD. "
    "REGIME-ADJUSTED FLOOR: the default minimum confidence to trade is 0.50. "
    "This floor may be lowered to 0.45 for a reduced-size entry ONLY when ALL of the following are true: "
    "  (i) funding rate is below −0.20%, AND "
    "  (ii) obv_trend is 'diverging_bullish' (for a BUY) or 'diverging_bearish' (for a SELL) — confirming a squeeze setup, AND "
    "  (iii) the archetype is Archetype C (funding-rate fade). "
    "If invoking the lowered floor, state it explicitly in rationale: 'Regime-adjusted floor applied: 0.45'. "
    "The risk_score (0–1) reflects current volatility (ATR %), spread width, and proximity to major support/resistance. "
    "Higher risk_score = more risk = significantly smaller position. "
    "The execution layer sizes positions from your confidence and risk_score "
    "(notional = max_safe_notional_usd × confidence × (1 − risk_score)). "
    "context.execution.max_safe_notional_usd and min_notional_usd are provided for reference — do NOT output a dollar amount. "
    "If pre_computed_modifiers.capital_sufficient is false, you MUST choose HOLD. "
)

_SEC_STEP6_TP_BASE = (
    "STEP 6 — TAKE-PROFIT METHODOLOGY: "
    "To determine your take-profit, anchor it to a technically significant level. "
    "For a BUY: use context.market_signals.swing_high_htf (pre-computed HTF swing highs) as the primary target; "
    "fall back to swing_high_ltf or the upper Bollinger Band if no HTF swing is available. "
    "For a SELL: use context.market_signals.swing_low_htf as the primary target; "
    "fall back to swing_low_ltf or the lower Bollinger Band. "
    "SWING EXHAUSTION: if context.market_signals.swing_high_exhaustion or swing_low_exhaustion is present, "
    "the price has moved beyond most available swing references. In that case: "
    "(a) For BUY: consider using the upper Bollinger Band, a round-number level, or a fixed ATR-multiple target (e.g. entry + 2×ATR) instead. "
    "(b) For SELL: consider using the lower Bollinger Band or entry − 2×ATR. "
    "(c) Increase risk_score by +0.10 to account for the lack of proven resistance/support targets. "
    "Note the exhaustion in rationale. "
)

# Conditional: only included when require_reward_risk_ratio is True.
_SEC_STEP6_TP_RR_RULE = (
    "If no clear level exists within a reward-to-risk ratio >= context.guardrails.min_reward_risk_ratio "
    "based on your stop distance, you MUST choose HOLD. "
)

_SEC_STEP6_TP_CLOSE = (
    "When existing stop-loss or take-profit levels are present, reuse or gently tune them unless you can justify a safer alternative. "
)

_SEC_SIZING_RULE_POST = (
    "Contract sizes and exchange multipliers are handled automatically by the execution layer. "
)

# Conditional: only included when llm_notional_mode == "pre_leverage".
_SEC_SIZING_RULE_PRE = (
    "PRE-LEVERAGE MODE: context.execution.max_safe_notional_usd is the MARGIN ceiling (bot multiplies by max_leverage internally). "
    "Contract multipliers are handled automatically. "
)

_SEC_DIRECTION_RULE_BASE = (
    "CRITICAL DIRECTION RULE: for a BUY, stop_loss MUST be strictly below entry price and take_profit MUST be strictly above entry price. "
    "For a SELL, stop_loss MUST be strictly above entry price and take_profit MUST be strictly below entry price. "
    "A take_profit or stop_loss on the wrong side of entry will be rejected by the execution layer. "
)

# Conditional: only included when require_reward_risk_ratio is True.
_SEC_DIRECTION_RR_RULES = (
    "The take-profit distance from entry must be at least context.guardrails.min_reward_risk_ratio times the stop-loss distance from entry. "
    "A trade where potential loss exceeds potential gain will be hard-blocked by the execution layer. "
)

_SEC_HOLD_GUIDANCE = (
    "Choose HOLD whenever capital constraints, fee/credit depletion, missing TP/SL, poor reward-to-risk, "
    "or low confidence (< 0.45) prevent a high-quality entry — and describe the specific blocker. "
    "HTF contradiction alone is NOT a reason to HOLD if LTF signals are strong enough to survive the confidence penalty. "
    "CREDIT CONSERVATION: if context.pre_computed_modifiers.credit_conservation is true, "
    "add context.pre_computed_modifiers.credit_risk_add to risk_score. Note 'credit conservation mode' in rationale. "
    "SPIKE RECENCY CHECK: if context.pre_computed_modifiers.spike_recency is true, "
    "increase risk_score by context.pre_computed_modifiers.spike_risk_add and prefer a limit order at the nearest support. Note the spike recency adjustment in rationale. "
    "REGIME PRECEDENCE: regime rules adjust bias and apply confidence penalties, but they do NOT override the confidence-based sizing system. "
    "If the regime says 'prefer HOLD' but your computed confidence for a directional trade exceeds 0.55, the trade is valid — document the regime tension in rationale. "
)

_SEC_STEP_STRATEGY = (
    "STRATEGY ARCHETYPES — prefer one of the following when signals are ambiguous: "
    "(A) Trend-continuation after consolidation: enter after a spike-and-base pattern when price holds above VWAP, "
    "ADX stays elevated (> 25), and DI+ dominates. Confirm with OBV rising or CVD net positive. "
    "Use a limit order at the nearest LTF support for better fill. "
    "(B) Mean-reversion in bullish HTF context: use an uptrending HTF as macro backdrop; wait for RSI to pull back to "
    "50–55 on the LTF before entering long — this avoids chasing overbought conditions and improves R:R. "
    "(C) Funding-rate fade: when context.pre_computed_modifiers.archetype_c_eligible is true (funding < −0.05%) and the HTF is bullish, "
    "lean long on any LTF pullback — the overcrowded-shorts squeeze is a well-documented crypto-perps edge. "
    "Also check context.session_context.hours_until_funding — if funding settlement is imminent (< 2 hours), the squeeze is more acute. "
    "State which archetype you are using in your rationale. "
)

_SEC_STEP_REGIME = (
    "MARKET REGIME RULES — use context.market_signals.market_regime to adjust behaviour: "
    "REGIME CONFIDENCE: if context.market_signals.market_regime_confidence is 'weakening', "
    "the regime label may be stale — a sharp reversal candle on high volume has occurred. "
    "Check context.market_signals.regime_note for details. Add +0.10 to risk_score and consider "
    "the regime one tier less bullish/bearish than labelled (e.g. treat 'trending_up' as 'post_spike_consolidation'). "
    "trending_up: prefer trend-continuation entries (Archetype A); RSI/Stoch RSI overbought reduces size but does NOT veto "
    "a trade if ADX > 30 and DI+ dominates — shift instead to a limit order at the nearest support. "
    "trending_down: prefer SELL or HOLD; no BUY unless strong HTF bullish divergence with high volume. "
    "ranging: prefer mean-reversion (Archetype B); apply a −0.10 confidence penalty by default; tighten stops to ATR×1.0. "
    "  RANGING PENALTY OFFSET: if a dominant sub-signal is present in the trade direction, reduce the penalty to −0.05: "
    "    − funding rate < −0.15% (shorts overcrowded, squeeze risk), OR "
    "    − obv_trend diverges in the trade direction (e.g. diverging_bullish for a BUY). "
    "  Both conditions together do NOT stack below −0.05; the floor for the offset is −0.05. "
    "  State whether the offset applies and why in rationale. "
    "post_spike_consolidation: prefer HOLD or a reduced-size BUY only on a confirmed support hold; wait for ADX to "
    "re-expand above 25 before sizing up. "
    "breakdown: strong SELL bias or HOLD; do not BUY unless HTF shows compelling bullish divergence. "
    "  BREAKDOWN STALENESS: if context.pre_computed_modifiers.regime_reclassification is set, "
    "use that regime instead of 'breakdown' and note the override in rationale. "
    "unknown: treat as ranging and apply the −0.10 penalty (offset rules above still apply). "
)

# ---------------------------------------------------------------------------
# PROMPT_SECTIONS — ordered list of all logical blocks that make up the
# decision prompt.  Each entry has a stable ``key``, a human-readable
# ``label``, and the module-level ``default`` text constant.  The optional
# ``alt_default`` is used for the sizing_rule section in pre-leverage mode.
# ---------------------------------------------------------------------------

PROMPT_SECTIONS: list[dict[str, Any]] = [
    {"key": "intro",            "label": "Introduction / Role",         "default": _SEC_INTRO},
    {"key": "step1_preflight",  "label": "Step 1: Pre-flight checks",   "default": _SEC_STEP1_PREFLIGHT},
    {"key": "step2_htf",        "label": "Step 2: HTF trend filter",    "default": _SEC_STEP2_HTF},
    {"key": "step3_divergence", "label": "Step 3: Divergence check",    "default": _SEC_STEP3_DIVERGENCE},
    {"key": "step4_signals",        "label": "Step 4: Signal hierarchy",    "default": _SEC_STEP4_SIGNALS},
    {"key": "step4_footprint",      "label": "Step 4: Footprint chart",     "default": _SEC_STEP4_FOOTPRINT},
    {"key": "strategy_archetypes",  "label": "Strategy archetypes",         "default": _SEC_STEP_STRATEGY},
    {"key": "regime_rules",         "label": "Market regime rules",         "default": _SEC_STEP_REGIME},
    {"key": "step5_sizing",         "label": "Step 5: Confidence & sizing", "default": _SEC_STEP5_SIZING},
    {"key": "step6_tp",         "label": "Step 6: TP/SL methodology",  "default": _SEC_STEP6_TP_BASE},
    {"key": "step6_tp_rr",      "label": "Step 6: R:R TP rule",         "default": _SEC_STEP6_TP_RR_RULE},
    {"key": "step6_tp_close",   "label": "Step 6: TP close guidance",   "default": _SEC_STEP6_TP_CLOSE},
    {
        "key": "sizing_rule",
        "label": "Sizing rule",
        "default": _SEC_SIZING_RULE_POST,
        "alt_default": _SEC_SIZING_RULE_PRE,
    },
    {"key": "direction_rule",   "label": "Direction rule",              "default": _SEC_DIRECTION_RULE_BASE},
    {"key": "direction_rr",     "label": "Direction R:R rules",         "default": _SEC_DIRECTION_RR_RULES},
    {"key": "hold_guidance",    "label": "HOLD guidance",               "default": _SEC_HOLD_GUIDANCE},
]


def default_prompt_sections(
    *, require_rr: bool = False, pre_leverage: bool = False
) -> "dict[str, dict]":
    """Return the default prompt_sections config dict.

    All sections enabled with no text overrides.  The R:R sections
    (step6_tp_rr, direction_rr) default to the current ``require_rr``
    guardrail state so the editor opens in a sensible state.
    """
    rr_keys = {"step6_tp_rr", "direction_rr"}
    _off_by_default = {"direction_rule"}
    return {
        sec["key"]: {
            "enabled": (require_rr if sec["key"] in rr_keys else sec["key"] not in _off_by_default),
            "override": None,
        }
        for sec in PROMPT_SECTIONS
    }


def assemble_decision_prompt(
    *,
    require_rr: bool = False,
    pre_leverage: bool = False,
    sections_config: "dict[str, dict] | None" = None,
    footprint_cfg: "dict[str, Any] | None" = None,
) -> str:
    """Assemble the decision prompt from canonical section constants.

    When ``sections_config`` is supplied (the per-section enable/override dict
    stored in ``runtime_config["prompt_sections"]``), each section's enabled
    flag and optional text override are respected.  Otherwise the legacy
    ``require_rr`` / ``pre_leverage`` flags drive conditional section inclusion.
    """
    if sections_config is not None:
        parts: list[str] = []
        for sec in PROMPT_SECTIONS:
            key = sec["key"]
            cfg = sections_config.get(key, {})
            if not cfg.get("enabled", True):
                continue
            override = (cfg.get("override") or "").strip()
            if override:
                parts.append(override + " ")
            else:
                # step4_footprint uses a dynamic paragraph when footprint_cfg is present
                if key == "step4_footprint" and footprint_cfg is not None:
                    parts.append(_build_footprint_prompt_section(footprint_cfg))
                # sizing_rule uses alt_default when pre_leverage is active
                elif key == "sizing_rule" and pre_leverage and "alt_default" in sec:
                    parts.append(sec["alt_default"])
                else:
                    parts.append(sec["default"])
        return "".join(parts)

    # Legacy path (no sections_config): assemble from guardrail flags.
    parts: list[str] = [
        _SEC_INTRO,
        _SEC_STEP1_PREFLIGHT,
        _SEC_STEP2_HTF,
        _SEC_STEP3_DIVERGENCE,
        _SEC_STEP4_SIGNALS,
        _build_footprint_prompt_section(footprint_cfg),
        _SEC_STEP_STRATEGY,
        _SEC_STEP_REGIME,
        _SEC_STEP5_SIZING,
        _SEC_STEP6_TP_BASE,
    ]
    if require_rr:
        parts.append(_SEC_STEP6_TP_RR_RULE)
    parts.append(_SEC_STEP6_TP_CLOSE)
    parts.append(_SEC_SIZING_RULE_PRE if pre_leverage else _SEC_SIZING_RULE_POST)
    if require_rr:
        parts.append(_SEC_DIRECTION_RR_RULES)
    parts.append(_SEC_HOLD_GUIDANCE)
    return "".join(parts)


# Baseline display constant (no R:R enforcement, post-leverage sizing).
# Exported for the CFG page and legacy references.  The actual prompt sent
# to the LLM is always freshly assembled in PromptBuilder.build() via
# assemble_decision_prompt(), so guardrail state is always reflected.
DEFAULT_DECISION_PROMPT = assemble_decision_prompt(require_rr=False, pre_leverage=False)

DEFAULT_EXECUTION_FEEDBACK_TTL_SECONDS = 600

RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    # OpenAI strict mode requires every property to appear in 'required'.
    # Optional fields use anyOf with null so the model can omit them by
    # returning null without violating the schema.
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
            "description": "Directional conviction (0–1): how strongly the signal set supports the chosen direction, after applying all timeframe-alignment penalties. The execution layer uses this to scale position size.",
        },
        "rationale": {
            "type": "string",
            "description": "Short explanation citing the strongest signals",
        },
        "risk_score": {
            "type": "number",
            "minimum": 0,
            "maximum": 1,
            "description": "Setup risk (0–1): reflects volatility (ATR %), spread width, and proximity to key support/resistance. Higher = riskier = smaller position. The execution layer uses this to scale position size.",
        },
        "stop_loss": {
            "anyOf": [{"type": "number"}, {"type": "null"}],
            "description": "Stop-loss price: for BUY must be BELOW entry price; for SELL must be ABOVE entry price",
        },
        "take_profit": {
            "anyOf": [{"type": "number"}, {"type": "null"}],
            "description": "Take-profit price: for BUY must be ABOVE entry price; for SELL must be BELOW entry price",
        },
        "timeframe_alignment": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "description": "How the decision aligns with provided timeframe",
        },
        "notes": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "description": "Additional implementation notes or cautionary flags",
        },
        "tags": {
            "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}],
        },
    },
    "required": [
        "action",
        "confidence",
        "rationale",
        "risk_score",
        "stop_loss",
        "take_profit",
        "timeframe_alignment",
        "notes",
        "tags",
    ],
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
    max_candles: int = 50
    max_htf_candles: int = 25
    _cache_symbol: Optional[str] = field(init=False, default=None)

    def build(self, *, symbol: str | None = None, timeframe: str | None = None) -> dict[str, Any]:
        snapshot = self.snapshot or {}
        runtime_meta = self.metadata or {}
        risk_locks = runtime_meta.get("risk_locks") or {}
        resolved_symbol = self._resolve_symbol(symbol)
        market_block = self._select_market(resolved_symbol)
        indicators = market_block.get("indicators") or {}
        ticker = market_block.get("ticker") or snapshot.get("ticker") or {}
        funding = market_block.get("funding_rate") or snapshot.get("funding_rate") or {}
        open_interest = market_block.get("open_interest") or snapshot.get("open_interest") or {}
        custom_metrics = market_block.get("custom_metrics") or snapshot.get("custom_metrics") or {}
        risk_metrics = market_block.get("risk_metrics") or snapshot.get("risk_metrics") or {}
        order_book = market_block.get("order_book") or snapshot.get("order_book") or {}
        liquidations = market_block.get("liquidations") or snapshot.get("liquidations") or []

        live_section = self._build_live_section(ticker, funding, open_interest, custom_metrics, order_book)
        # Extract LTF candles for internal computations (not sent to LLM — all indicators pre-computed)
        _ohlcv_raw = indicators.get("ohlcv") or []
        ltf_candles: list[dict[str, Any]] = _ohlcv_raw[-self.max_candles:]
        history_section = self._build_history_section(indicators)
        _last_price_for_htf = _to_float(live_section.get("last_price"))
        indicator_section = self._build_indicator_section(indicators, last_price=_last_price_for_htf)
        # 2.1 — change_24h_pct fallback: compute from candle data when the ticker doesn't supply it.
        if live_section.get("change_24h_pct") is None and len(ltf_candles) >= 2:
            _first_open = _to_float(ltf_candles[0].get("open"))
            _last_close = _to_float(ltf_candles[-1].get("close"))
            if _first_open and _first_open > 0 and _last_close is not None:
                live_section["change_24h_pct"] = round((_last_close - _first_open) / _first_open * 100, 2)
        market_signals = self._build_market_signals(indicators, live_section, ltf_candles)
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
        execution_settings: dict[str, Any] = {}
        execution_settings["enabled"] = bool(runtime_meta.get("execution_enabled", False))
        execution_settings["trade_mode"] = runtime_meta.get("execution_trade_mode") or "isolated"
        execution_settings["order_type"] = runtime_meta.get("execution_order_type") or "market"
        if price_hint is not None:
            execution_settings["price_reference"] = price_hint
        # 6.3 — Always include symbol_rules so the model can reference
        # tick_size for TP/SL level placement.  Fall back to min_size when the
        # full instrument spec isn't available.
        if instrument_spec:
            execution_settings["symbol_rules"] = instrument_spec
        else:
            _fallback_rules: dict[str, Any] = {}
            if min_size_value is not None:
                _fallback_rules["min_size"] = min_size_value
            execution_settings["symbol_rules"] = _fallback_rules
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
        equity_cap_usd = None
        symbol_equity_cap_usd = None
        if account_equity_usd is not None and max_position_pct:
            equity_cap_usd = account_equity_usd * max_position_pct
            execution_settings["max_equity_allocation_usd"] = equity_cap_usd
        if max_position_pct is not None:
            execution_settings["max_position_pct"] = max_position_pct
        if symbol_cap_pct is not None:
            execution_settings["symbol_max_position_pct"] = symbol_cap_pct
        if account_equity_usd is not None and symbol_cap_pct:
            symbol_equity_cap_usd = account_equity_usd * symbol_cap_pct
            execution_settings["symbol_max_equity_allocation_usd"] = symbol_equity_cap_usd
        if margin_cap_usd is not None:
            execution_settings["margin_max_position_value_usd"] = margin_cap_usd
        if tier_cap_usd is not None:
            execution_settings["tier_max_position_value_usd"] = tier_cap_usd
        effective_candidates = [
            value
            for value in (margin_cap_usd, tier_cap_usd)
            if value and value > 0
        ]
        if effective_candidates:
            effective_cap = min(effective_candidates)
            execution_settings["effective_max_position_value_usd"] = effective_cap
        effective_max_leverage = guardrail_max_leverage if guardrail_max_leverage and guardrail_max_leverage > 0 else None
        if effective_max_leverage is not None:
            execution_settings["max_leverage"] = effective_max_leverage
        # Pre-compute the authoritative notional ceiling so the LLM never has to
        # guess: max_safe_notional_usd = available_margin_usd × max_leverage, then
        # capped by whichever position/tier/equity limit is tightest.
        llm_notional_mode = (guardrails.get("llm_notional_mode") or "post_leverage").lower()
        if available_margin_usd is not None and effective_max_leverage is not None and effective_max_leverage > 0:
            max_safe_notional_usd: Optional[float] = available_margin_usd * effective_max_leverage
            if llm_notional_mode == "pre_leverage":
                # In pre-leverage mode the LLM commits margin; the bot multiplies by
                # max_leverage internally.  The ceiling shown to the LLM is therefore
                # available_margin_usd (= position ceiling / leverage).
                # Only OKX-sourced hard limits (margin_cap_usd, tier_cap_usd) are
                # applied here — they represent real exchange constraints that must
                # not be breached.  Equity-percentage caps (equity_cap_usd,
                # symbol_equity_cap_usd) are enforced by the execution layer on the
                # resulting position notional, so they must NOT be divided by
                # leverage here (that would collapse the margin ceiling to near-zero
                # and force the LLM to output trivially small margin commitments).
                hard_cap_candidates = [
                    v for v in (margin_cap_usd, tier_cap_usd)
                    if v is not None and v > 0
                ]
                if hard_cap_candidates:
                    max_safe_notional_usd = min(max_safe_notional_usd, min(hard_cap_candidates))
                max_safe_notional_usd = max_safe_notional_usd / effective_max_leverage
            else:
                cap_candidates = [
                    v for v in (margin_cap_usd, tier_cap_usd, equity_cap_usd, symbol_equity_cap_usd)
                    if v is not None and v > 0
                ]
                if cap_candidates:
                    max_safe_notional_usd = min(max_safe_notional_usd, min(cap_candidates))
            execution_settings["max_safe_notional_usd"] = round(max_safe_notional_usd, 2)
        # Minimum notional required by OKX to open a new isolated-margin position.
        # Even for cross-margin this acts as a useful sanity floor.
        # In pre-leverage mode the LLM expresses size as margin, so scale the
        # minimum down by max_leverage (e.g. $5 position / 5× = $1.00 margin floor).
        _min_position_notional = 5.0
        if (
            llm_notional_mode == "pre_leverage"
            and effective_max_leverage is not None
            and effective_max_leverage > 0
        ):
            _min_notional_usd = round(_min_position_notional / effective_max_leverage, 2)
        else:
            _min_notional_usd = _min_position_notional
        execution_settings["min_notional_usd"] = _min_notional_usd
        if tier_imr is not None:
            execution_settings["tier_initial_margin_ratio"] = tier_imr
        if tier_source:
            execution_settings["tier_source"] = tier_source
        # Pass updated_at temporarily so _build_margin_health_section can assess data freshness
        _exec_updated_at = execution_limits.get("updated_at")
        if _exec_updated_at:
            execution_settings["updated_at"] = _exec_updated_at
        margin_health = self._build_margin_health_section(execution_settings)
        if margin_health and margin_health.get("summary"):
            execution_settings["margin_summary"] = margin_health["summary"]
        schema_overrides = runtime_meta.get("llm_response_schemas") or {}
        timeframe_value = timeframe or runtime_meta.get("ta_timeframe") or snapshot.get("timeframe") or "4H"
        # trend_section intentionally omitted from context; adx/EMA already in indicators
        liquidity_section = self._build_liquidity_profile(live_section, ticker)
        derivatives_section = self._build_derivatives_posture(funding, custom_metrics, liquidations)
        fee_window_summary = self._build_fee_window_summary(account_section)
        credit_availability = self._build_credit_availability()
        execution_feedback = self._format_execution_feedback(
            snapshot.get("execution_feedback"),
            symbol=resolved_symbol,
        )
        feedback_digest = self._build_execution_feedback_digest(execution_feedback)

        # Session context — helps the model weigh liquidity/volatility expectations
        _utc_now = datetime.now(timezone.utc)
        _utc_hour = _utc_now.hour
        if _utc_hour < 8:
            _active_session = "Asia"
        elif _utc_hour < 16:
            _active_session = "EU"
        else:
            _active_session = "US"
        # 2.12 — Enrich session context with hours_until_funding.
        # OKX perpetual funding settlements are at 00:00, 08:00, 16:00 UTC.
        _funding_hours = [0, 8, 16]
        _hours_until_funding: float | None = None
        _current_fractional_hour = _utc_hour + _utc_now.minute / 60.0
        for fh in _funding_hours:
            diff = fh - _current_fractional_hour
            if diff <= 0:
                diff += 24
            if _hours_until_funding is None or diff < _hours_until_funding:
                _hours_until_funding = round(diff, 1)

        # Pre-compute deterministic flags so the LLM reads results, not instructions
        # on how to calculate them.  Requires ltf_candles — computed above internally.
        pre_computed_modifiers = self._build_pre_computed_modifiers(
            indicators, indicator_section, market_signals, execution_settings,
            execution_feedback, ltf_candles, credit_availability, live_section,
            derivatives_section,
            positions_section=positions_section,
            pending_orders=pending_orders,
            resolved_symbol=resolved_symbol,
        )
        # Flag portfolio over-exposure so the LLM can weight concentration risk.
        _net_pct = _to_float(exposure_section.get("net_pct_of_equity"))
        if _net_pct is not None and abs(_net_pct) > 100:
            pre_computed_modifiers["portfolio_overexposed"] = True
            pre_computed_modifiers["portfolio_overexposure_pct"] = round(_net_pct, 1)

        # Deduplication: strip market fields already present in liquidity_context
        live_section.pop("spread", None)
        live_section.pop("spread_pct", None)
        live_section.pop("funding_rate", None)  # duplicated in derivatives_posture.funding.current
        _order_flow_mut = live_section.get("order_flow")
        if isinstance(_order_flow_mut, dict):
            _order_flow_mut.pop("bid_depth", None)
            _order_flow_mut.pop("ask_depth", None)
            _order_flow_mut.pop("order_book_note", None)
        # Strip funding_archetype_c_eligible from market_signals (now in pre_computed_modifiers)
        market_signals.pop("funding_archetype_c_eligible", None)
        # Slim long_short_ratio: keep only value (series/metadata already consumed by ls_signal);
        # omit the key entirely when value is absent to avoid an empty {} confusing the LLM.
        _lsr = derivatives_section.get("long_short_ratio")
        if isinstance(_lsr, dict):
            _lsr_value = _lsr.get("value")
            if _lsr_value is not None:
                derivatives_section["long_short_ratio"] = {"value": _lsr_value}
            else:
                derivatives_section.pop("long_short_ratio", None)
        # Slim down execution block: remove internal cap fields not needed by the LLM
        for _k in (
            "available_margin_usd", "account_equity_usd",
            "max_equity_allocation_usd", "max_position_pct",
            "symbol_max_position_pct", "symbol_max_equity_allocation_usd",
            "margin_max_position_value_usd", "tier_max_position_value_usd",
            "effective_max_position_value_usd", "tier_initial_margin_ratio", "tier_source",
            "updated_at",
        ):
            execution_settings.pop(_k, None)

        _symbol_positions = [p for p in positions_section if p.get("symbol") == resolved_symbol]
        context = {
            "symbol": resolved_symbol,
            "timeframe": timeframe_value,
            "session_context": {
                "utc_hour": _utc_hour,
                "active_session": _active_session,
                "hours_until_funding": _hours_until_funding,
            },
            "market": live_section,
            "history": history_section,
            "indicators": indicator_section,
            "portfolio_exposure": {k: v for k, v in exposure_section.items() if k != "heatmap"},
            # strip execution-layer-only guardrail keys before sending to LLM
            "guardrails": self._strip_guardrails_for_llm(guardrails),
            "liquidity_context": liquidity_section,
            "derivatives_posture": derivatives_section,
            "pending_orders": pending_orders,
            "execution": execution_settings,
            "market_signals": market_signals,
            "credit_availability": credit_availability,
            "pre_computed_modifiers": pre_computed_modifiers,
        }
        _llm_notes = runtime_meta.get("llm_notes")
        if _llm_notes:
            context["notes"] = _llm_notes
        if _symbol_positions:
            context["positions"] = _symbol_positions
        if risk_locks:
            _dl_lock = risk_locks.get("daily_loss") or {}
            if bool(_dl_lock.get("active", False)):
                context["risk_locks"] = {"daily_loss_active": True}
        if execution_feedback:
            context["execution_feedback"] = execution_feedback
            execution_settings["recent_feedback"] = execution_feedback
        if feedback_digest:
            context["execution_feedback_digest"] = feedback_digest
            execution_settings["feedback_digest"] = feedback_digest
        # Round all float values to contextually appropriate precision to save tokens
        context = self._round_floats(context)
        system_prompt = sanitize_prompt_text(runtime_meta.get("llm_system_prompt") or DEFAULT_SYSTEM_PROMPT)

        # Build the decision prompt.  If the user has saved a truly custom prompt,
        # use it as-is (sanitized).  Otherwise assemble fresh from section constants
        # so the current guardrail state is always reflected — no string-patching.
        _require_rr = bool(guardrails.get("require_reward_risk_ratio"))
        _pre_leverage = llm_notional_mode == "pre_leverage"
        sections_config = runtime_meta.get("prompt_sections")
        custom_prompt = runtime_meta.get("llm_decision_prompt")
        _footprint_cfg = guardrails.get("footprint") if isinstance(guardrails.get("footprint"), dict) else None
        if sections_config:
            decision_prompt = sanitize_prompt_text(
                assemble_decision_prompt(
                    sections_config=sections_config,
                    pre_leverage=_pre_leverage,
                    footprint_cfg=_footprint_cfg,
                )
            )
        elif custom_prompt and custom_prompt.strip():
            decision_prompt = sanitize_prompt_text(custom_prompt)
        else:
            decision_prompt = sanitize_prompt_text(
                assemble_decision_prompt(
                    require_rr=_require_rr,
                    pre_leverage=_pre_leverage,
                    footprint_cfg=_footprint_cfg,
                )
            )
        response_schema = self._response_schema(model_id, schema_overrides)
        prompt_block = {
            "system": (system_prompt or DEFAULT_SYSTEM_PROMPT).strip(),
            "task": (decision_prompt or DEFAULT_DECISION_PROMPT).strip(),
            "model": model_id,
            "response_schema": response_schema,
        }
        # 2.5 — response_schema already lives inside prompt_block.response_schema;
        # avoid duplicating it at the top level to save ~500 tokens per call.
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
        logger.warning("PromptBuilder: could not resolve symbol from snapshot; no symbol will be used")
        return "UNKNOWN"

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
        change_24h_pct = _to_float(ticker.get("changeRate") or ticker.get("change24h"))
        volume_24h = _to_float(ticker.get("volCcy24h") or ticker.get("vol24h"))
        funding_rate = _to_float(funding.get("fundingRate"))
        next_funding = funding.get("nextFundingTime") or funding.get("nextFundingRate")
        oi_value = _to_float(open_interest.get("oi"))
        oi_value_ccy = _to_float(open_interest.get("oiCcy"))
        bid_depth = sum(_to_float(level[1]) or 0.0 for level in (order_book.get("bids") or [])[:5])
        ask_depth = sum(_to_float(level[1]) or 0.0 for level in (order_book.get("asks") or [])[:5])
        # 8.1 — Detect degraded order-book data.  Zero on *either* side is stale;
        # near-zero on *both* sides (< 50 contracts) means the entire depth
        # snapshot is too thin to reason from.  Both flags are surfaced.
        _MIN_DEPTH_CONTRACTS = 50
        order_book_stale = bid_depth == 0.0 or ask_depth == 0.0
        order_book_thin = bid_depth < _MIN_DEPTH_CONTRACTS and ask_depth < _MIN_DEPTH_CONTRACTS
        ofi = custom_metrics.get("order_flow_imbalance")
        cvd = custom_metrics.get("cumulative_volume_delta")
        _ofi_ratio = custom_metrics.get("ofi_ratio_series")
        _ofi_ratio_trimmed = (
            _ofi_ratio[-5:]
            if isinstance(_ofi_ratio, list) and len(_ofi_ratio) >= 3
            else None
        )
        _cvd_series_raw = custom_metrics.get("cvd_series")
        _cvd_series_trimmed = (
            _cvd_series_raw[-5:]
            if isinstance(_cvd_series_raw, list) and len(_cvd_series_raw) > 5
            else _cvd_series_raw
        )
        order_flow: dict[str, Any] = {
            "imbalance": ofi,
            "cvd": cvd,
            "bid_depth": None if order_book_stale else bid_depth,
            "ask_depth": None if order_book_stale else ask_depth,
            "cvd_series": _cvd_series_trimmed,
        }
        # Only include ofi_ratio_series when there are enough data points to show a
        # trend.  Omit the key entirely (rather than null) when unavailable so the
        # model doesn't reserve attention on an absent field.
        if _ofi_ratio_trimmed is not None:
            order_flow["ofi_ratio_series"] = _ofi_ratio_trimmed
        if order_book_stale:
            _note_parts = []
            if bid_depth == 0.0:
                _note_parts.append("bid_depth was zero at snapshot time")
            if ask_depth == 0.0:
                _note_parts.append("ask_depth was zero at snapshot time")
            if order_book_thin and not (bid_depth == 0.0 and ask_depth == 0.0):
                _note_parts.append(f"near-zero depth (bid={bid_depth:.0f}, ask={ask_depth:.0f})")
            order_flow["order_book_note"] = "stale: " + "; ".join(_note_parts)
        elif order_book_thin:
            order_flow["order_book_note"] = (
                f"thin: bid_depth={bid_depth:.0f}, ask_depth={ask_depth:.0f} "
                f"(both < {_MIN_DEPTH_CONTRACTS} contracts); order flow unreliable"
            )
        _footprint = custom_metrics.get("footprint")
        result: dict[str, Any] = {
            "last_price": last_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "change_24h_pct": change_24h_pct,
            "volume_24h": volume_24h,
            "funding_rate": funding_rate,
            "order_flow": order_flow,
        }
        if _footprint:
            result["footprint"] = _footprint
        return result

    @staticmethod
    def _trim_series(series: Any, n: int) -> Any:
        """Return the last *n* elements of *series* as a plain list.

        Handles lists, numpy arrays, pandas Series, and other sliceable
        sequence types.  Returns None when the input is None or not sliceable.
        """
        if series is None:
            return None
        try:
            return list(series[-n:])
        except (TypeError, KeyError):
            return None

    @staticmethod
    def _normalize_htf_candles(raw: list[Any]) -> list[dict[str, Any]]:
        """Convert raw OKX candlestick rows (list-of-lists) to named-field dicts.

        OKX returns candles newest-first; we sort ascending (oldest first) so the
        LLM receives the same chronological order as context.history.candles.
        """
        result = []
        for row in raw:
            if len(row) < 6:
                continue
            try:
                result.append({
                    "ts": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                })
            except (ValueError, TypeError):
                continue
        result.sort(key=lambda c: c["ts"])
        return result

    # ------------------------------------------------------------------
    # Pre-computed signal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_swing_pivots(
        candles: list[dict[str, Any]],
        n_pivots: int = 3,
        window: int = 5,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Identify the last n_pivots swing highs and lows from OHLCV candles.

        A swing high at index i: high[i] > all highs in [i-window, i) and (i, i+window].
        A swing low  at index i: low[i]  < all lows  in [i-window, i) and (i, i+window].
        Returns (swing_highs, swing_lows) — last n_pivots of each, newest last.
        """
        highs_out: list[dict[str, Any]] = []
        lows_out: list[dict[str, Any]] = []
        n = len(candles)
        for i in range(window, n - window):
            try:
                h = float(candles[i].get("high") or candles[i].get("close") or 0)
                l = float(candles[i].get("low") or candles[i].get("close") or 0)
            except (ValueError, TypeError):
                continue
            pre_h = [float(candles[j].get("high") or candles[j].get("close") or 0)
                     for j in range(i - window, i)]
            post_h = [float(candles[j].get("high") or candles[j].get("close") or 0)
                      for j in range(i + 1, i + window + 1)]
            if pre_h and post_h and h > max(pre_h) and h > max(post_h):
                highs_out.append({"price": h, "bar_index": i})
            pre_l = [float(candles[j].get("low") or candles[j].get("close") or h)
                     for j in range(i - window, i)]
            post_l = [float(candles[j].get("low") or candles[j].get("close") or h)
                      for j in range(i + 1, i + window + 1)]
            if pre_l and post_l and l < min(pre_l) and l < min(post_l):
                lows_out.append({"price": l, "bar_index": i})
        return highs_out[-n_pivots:], lows_out[-n_pivots:]

    @staticmethod
    def _compute_obv_trend(
        obv_series: list[Any] | None,
        candles: list[dict[str, Any]],
        n: int = 10,
    ) -> str:
        """Classify OBV trend relative to price direction.

        Returns: "rising" | "falling" | "diverging_bearish" | "diverging_bullish" | "unknown".
        diverging_bearish = price rising but OBV falling (distribution).
        diverging_bullish = price falling but OBV rising (accumulation).
        """
        if not obv_series or len(obv_series) < 2:
            return "unknown"
        tail_obv = obv_series[-n:]
        try:
            obv_rising = float(tail_obv[-1]) > float(tail_obv[0])
        except (ValueError, TypeError):
            return "unknown"
        # Short-term sanity check: verify the last 5 values agree with the n-bar
        # direction. If not, the trend has recently reversed — prefer the recent
        # direction so the label matches what the LLM sees in the trimmed series.
        _recent_n = min(5, len(tail_obv))
        if _recent_n >= 2:
            try:
                _recent_rising = float(tail_obv[-1]) > float(tail_obv[-_recent_n])
                if _recent_rising != obv_rising:
                    obv_rising = _recent_rising
            except (ValueError, TypeError):
                pass
        price_rising: bool | None = None
        if candles and len(candles) >= 2:
            closes = [c.get("close") for c in candles[-n:] if c.get("close") is not None]
            if len(closes) >= 2:
                try:
                    price_rising = float(closes[-1]) > float(closes[0])
                except (ValueError, TypeError):
                    pass
            # Short-term sanity check for price direction: if the last 5 closes
            # contradict the n-bar direction, prefer the recent direction so the
            # divergence label matches what the LLM sees in the trimmed series.
            if price_rising is not None and len(closes) >= 5:
                try:
                    _recent_price_rising = float(closes[-1]) > float(closes[-5])
                    if _recent_price_rising != price_rising:
                        price_rising = _recent_price_rising
                except (ValueError, TypeError):
                    pass
        if price_rising is None:
            return "rising" if obv_rising else "falling"
        if obv_rising and price_rising:
            return "rising"
        if not obv_rising and not price_rising:
            return "falling"
        if price_rising and not obv_rising:
            return "diverging_bearish"
        return "diverging_bullish"

    @staticmethod
    def _label_confidence(series: list[Any] | None, label: str, n: int = 5) -> str:
        """Rate how well the **last *n* values** of *series* support *label*.

        Returns ``"strong"``  — last *n* values monotonically support the label.
        Returns ``"moderate"`` — net direction supports the label but recent bars are noisy.
        Returns ``"weak"``    — recent values contradict or are flat relative to the label.
        Returns ``"unknown"`` — insufficient data to judge.
        """
        if not series or len(series) < 3:
            return "unknown"
        try:
            tail = [float(v) for v in series[-n:] if v is not None]
        except (ValueError, TypeError):
            return "unknown"
        if len(tail) < 3:
            return "unknown"

        net_change = tail[-1] - tail[0]
        # Determine expected direction from the label.
        rising_labels = {"rising", "diverging_bullish", "net_positive_rising", "net_negative_recovering"}
        falling_labels = {"falling", "diverging_bearish", "net_positive_declining", "net_negative_declining"}
        # 6.2 — "flat_stable" means the data exists but shows no directional
        # movement.  Return "weak" (not "unknown") so the model knows the series
        # *was* evaluated but doesn't carry conviction.
        flat_labels = {"flat_stable"}
        if label in flat_labels:
            return "weak"
        if label in rising_labels:
            expected_rising = True
        elif label in falling_labels:
            expected_rising = False
        else:
            return "unknown"

        net_supports = (net_change > 0) == expected_rising
        if not net_supports:
            return "weak"

        # Check monotonicity: how many consecutive bars move in the expected direction?
        supporting = sum(
            1 for i in range(1, len(tail))
            if (tail[i] > tail[i - 1]) == expected_rising
        )
        ratio = supporting / (len(tail) - 1)
        if ratio >= 0.8:
            return "strong"
        if ratio >= 0.5:
            return "moderate"
        return "weak"

    @staticmethod
    def _trim_volume_block(volume: dict[str, Any] | None, max_len: int = 50) -> dict[str, Any] | None:
        """Return a copy of the volume indicator block with its series trimmed.

        The full series from the indicator engine can have ~200 values, but the
        candle data already provides per-bar volume.  Keep at most *max_len*
        values for Volume RSI spot-checks and save tokens.
        """
        if not volume or not isinstance(volume, dict):
            return volume
        out = dict(volume)
        raw = out.get("series")
        if isinstance(raw, list) and len(raw) > max_len:
            out["series"] = raw[-max_len:]
        return out

    @staticmethod
    def _strip_volume_series(volume: dict[str, Any] | None) -> dict[str, Any] | None:
        """Return volume block without the ``series`` key.

        Candle data already contains per-bar volume and Volume RSI lives in
        ``history.volume_rsi_series``, so the raw series is redundant.  Keeping
        only ``last`` and ``average`` saves ~200 tokens per call.
        """
        if not volume or not isinstance(volume, dict):
            return volume
        return {k: v for k, v in volume.items() if k != "series"}

    @staticmethod
    def _compute_cvd_trend(cvd_series: list[Any] | None, n: int = 10) -> str:
        """Summarize recent CVD direction and momentum slope.

        Uses the last *n* values (default 10) to determine the directional
        qualifier.  A wider recent window (10-15) avoids false "recovering"
        labels when a brief mid-series bounce is followed by resumed decline.

        Returns compound labels that describe both the net direction and whether
        it is accelerating or reversing, e.g.:
          net_positive_rising    — CVD positive and still climbing (bullish & accelerating)
          net_positive_declining — CVD positive but losing momentum (potential topping)
          net_negative_declining — CVD negative and deepening (bearish & worsening)
          net_negative_recovering— CVD negative but last period reversed (bearish easing)
          flat_stable            — no meaningful net movement
          unknown                — insufficient data
        """
        if not cvd_series or len(cvd_series) < 2:
            return "unknown"
        try:
            tail = [float(v) for v in cvd_series[-n:] if v is not None]
        except (ValueError, TypeError):
            return "unknown"
        if len(tail) < 2:
            return "unknown"
        net_change = tail[-1] - tail[0]
        # Recent slope: average of last 3 periods to smooth single-bar noise.
        if len(tail) >= 4:
            recent_slope = sum(tail[-i] - tail[-i - 1] for i in range(1, min(4, len(tail)))) / min(3, len(tail) - 1)
        else:
            recent_slope = tail[-1] - tail[-2]
        # Noise floor: require at least 1% of the first-period absolute value to avoid
        # labelling de-minimis moves.
        abs_threshold = max(abs(tail[0]) * 0.01, 1e-9)
        if abs(net_change) <= abs_threshold:
            return "flat_stable"
        if net_change > 0:
            # Net buying over the window — determine if still rising or starting to fall.
            return "net_positive_rising" if recent_slope >= 0 else "net_positive_declining"
        else:
            # Net selling over the window — determine if deepening or recovering.
            return "net_negative_declining" if recent_slope <= 0 else "net_negative_recovering"

    @staticmethod
    def _compute_price_vs_vwap(last_price: float | None, vwap: float | None) -> str:
        """Return 'above', 'below', 'at', or 'unknown'."""
        if last_price is None or vwap is None:
            return "unknown"
        if last_price > vwap:
            return "above"
        if last_price < vwap:
            return "below"
        return "at"

    @staticmethod
    def _compute_rsi_zone(
        rsi: float | None,
        overbought: float = 70.0,
        oversold: float = 30.0,
    ) -> str:
        """Classify RSI into 'overbought', 'oversold', or 'neutral'."""
        if rsi is None:
            return "unknown"
        if rsi >= overbought:
            return "overbought"
        if rsi <= oversold:
            return "oversold"
        return "neutral"

    @staticmethod
    def _compute_market_regime(
        adx_value: float | None,
        di_plus: float | None,
        di_minus: float | None,
        price: float | None,
        vwap: float | None,
        obv_series: list[Any] | None,
        n: int = 10,
    ) -> str:
        """Classify the current market regime.

        trending_up          ADX > 25 AND DI+ > DI-
        trending_down        ADX > 25 AND DI- > DI+  (gentle bear)
        breakdown            ADX > 20 AND DI- > DI+ AND price < VWAP AND OBV falling
        ranging              ADX < 20
        post_spike_consolidation  20 <= ADX <= 25 AND |DI+ - DI-| <= 5
        """
        if adx_value is None:
            return "unknown"
        if adx_value < 20:
            return "ranging"
        if 20 <= adx_value <= 25:
            if di_plus is not None and di_minus is not None and abs(di_plus - di_minus) <= 5:
                return "post_spike_consolidation"
        if adx_value > 20 and di_plus is not None and di_minus is not None:
            if di_plus > di_minus:
                return "trending_up"
            # DI- dominates — check for true breakdown vs mild downtrend
            obv_falling = False
            if obv_series and len(obv_series) >= 2:
                tail = obv_series[-n:]
                try:
                    obv_falling = float(tail[-1]) < float(tail[0])
                except (ValueError, TypeError):
                    pass
            price_below_vwap = price is not None and vwap is not None and price < vwap
            if obv_falling and price_below_vwap:
                return "breakdown"
            return "trending_down"
        return "unknown"

    def _build_market_signals(
        self,
        indicators: dict[str, Any],
        live_section: dict[str, Any],
        candles: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Pre-compute key signals so the LLM receives labelled summaries
        rather than raw series it must interpret from scratch.

        Fields injected into ``context.market_signals``:
          swing_high_ltf   — last 3 significant pivot highs (price + ts)
          swing_low_ltf    — last 3 significant pivot lows  (price + ts)
          swing_high_htf   — last 3 significant HTF pivot highs (price + ts)
          swing_low_htf    — last 3 significant HTF pivot lows  (price + ts)
          obv_trend        — rising / falling / diverging_bearish / diverging_bullish
          cvd_trend        — net_positive_rising / net_positive_declining /
                             net_negative_declining / net_negative_recovering / flat_stable
          price_vs_vwap    — above / below / at
          rsi_zone         — overbought / oversold / neutral
          market_regime    — trending_up / trending_down / ranging /
                             post_spike_consolidation / breakdown
        """
        obv_block = indicators.get("obv") or {}
        adx_block = indicators.get("adx") or {}
        order_flow = live_section.get("order_flow") or {}

        last_price = _to_float(live_section.get("last_price"))
        vwap = _to_float(indicators.get("vwap"))
        rsi = _to_float(indicators.get("rsi"))
        adx_value = _to_float(adx_block.get("value"))
        di_plus = _to_float(adx_block.get("di_plus"))
        di_minus = _to_float(adx_block.get("di_minus"))
        obv_series: list[Any] = obv_block.get("series") or []

        # CVD series — list of scalars or list of {value: ...} dicts
        cvd_series_raw: Any = order_flow.get("cvd_series")
        cvd_series: list[Any] | None = None
        if isinstance(cvd_series_raw, list):
            cvd_series = [
                (v.get("value") if isinstance(v, dict) else v)
                for v in cvd_series_raw
            ]

        # HTF candles for swing pivot computation
        htf_candles_raw = indicators.get("ohlcv_htf") or []
        htf_candles: list[dict[str, Any]] = self._normalize_htf_candles(htf_candles_raw) if htf_candles_raw else []

        swing_highs, swing_lows = self._compute_swing_pivots(candles)
        swing_highs_htf, swing_lows_htf = self._compute_swing_pivots(htf_candles, n_pivots=3, window=3)
        obv_trend = self._compute_obv_trend(obv_series, candles)
        # 2.2 — Add label_confidence so the model doesn't have to parse 15 numbers.
        obv_trend_confidence = self._label_confidence(obv_series, obv_trend, n=5)
        cvd_trend = self._compute_cvd_trend(cvd_series)
        cvd_trend_confidence = self._label_confidence(
            cvd_series, cvd_trend, n=10
        ) if cvd_series else "unknown"
        price_vs_vwap = self._compute_price_vs_vwap(last_price, vwap)
        rsi_zone = self._compute_rsi_zone(rsi)
        market_regime = self._compute_market_regime(
            adx_value, di_plus, di_minus, last_price, vwap, obv_series
        )

        # 8.8 — Regime confidence: detect when the last completed candle
        # contradicts the regime label (e.g. a −4% candle while regime says
        # "trending_up").  Computes the percentage move of the penultimate
        # candle and its volume relative to the average.
        regime_confidence: str | None = None
        regime_note: str | None = None
        if len(candles) >= 3:
            # Use the penultimate candle (last *completed*); the final candle
            # may still be forming.
            _rc_candle = candles[-2]
            _rc_open = _to_float(_rc_candle.get("open"))
            _rc_close = _to_float(_rc_candle.get("close"))
            _rc_vol = _to_float(_rc_candle.get("volume"))
            # Average volume across earlier candles
            _rc_vols = [_to_float(c.get("volume")) for c in candles[:-1] if _to_float(c.get("volume")) is not None and _to_float(c.get("volume")) > 0]
            _rc_avg_vol = sum(_rc_vols) / len(_rc_vols) if _rc_vols else None
            if _rc_open and _rc_open > 0 and _rc_close is not None:
                _rc_pct = (_rc_close - _rc_open) / _rc_open * 100
                _rc_vol_ratio = (_rc_vol / _rc_avg_vol) if (_rc_vol and _rc_avg_vol and _rc_avg_vol > 0) else None
                # A sharp candle (> 3%) on elevated volume (> 2× avg) that
                # opposes the regime label signals weakening confidence.
                _regime_bullish = market_regime in ("trending_up",)
                _regime_bearish = market_regime in ("trending_down", "breakdown")
                if (
                    abs(_rc_pct) > 3.0
                    and _rc_vol_ratio is not None
                    and _rc_vol_ratio > 2.0
                    and (
                        (_regime_bullish and _rc_pct < -3.0)
                        or (_regime_bearish and _rc_pct > 3.0)
                    )
                ):
                    regime_confidence = "weakening"
                    regime_note = (
                        f"last completed candle {_rc_pct:+.1f}% on "
                        f"{_rc_vol_ratio:.1f}x avg volume; trend integrity questionable"
                    )

        # 2.10 — Pre-flag Archetype C eligibility so the model doesn't have to
        # compare funding rate against the −0.05% threshold itself.
        funding_rate = _to_float(live_section.get("funding_rate"))
        funding_archetype_c = False
        if funding_rate is not None and funding_rate < -0.0005:  # −0.05%
            funding_archetype_c = True

        # 6.4 — Swing exhaustion: flag when current price has exceeded most
        # available swing references, leaving the model with limited TP anchors.
        swing_high_exhaustion: str | None = None
        swing_low_exhaustion: str | None = None
        if last_price is not None:
            # 9.5-struct — Use structured format for swing exhaustion so
            # the model can easily access remaining reference levels.
            all_highs = (swing_highs or []) + (swing_highs_htf or [])
            highs_above = [s for s in all_highs if _to_float(s.get("price")) is not None and _to_float(s["price"]) > last_price]
            if all_highs and len(highs_above) <= 1:
                _remaining = [_to_float(s["price"]) for s in highs_above]
                if not highs_above:
                    swing_high_exhaustion = {
                        "active": True,
                        "remaining_above": [],
                        "note": "price above ALL recent swing highs; no upside reference levels available",
                    }
                else:
                    swing_high_exhaustion = {
                        "active": True,
                        "remaining_above": _remaining,
                        "note": f"price above most swing highs; only {_remaining[0]} remains above",
                    }

            all_lows = (swing_lows or []) + (swing_lows_htf or [])
            lows_below = [s for s in all_lows if _to_float(s.get("price")) is not None and _to_float(s["price"]) < last_price]
            if all_lows and len(lows_below) <= 1:
                _remaining_below = [_to_float(s["price"]) for s in lows_below]
                if not lows_below:
                    swing_low_exhaustion = {
                        "active": True,
                        "remaining_below": [],
                        "note": "price below ALL recent swing lows; no downside reference levels available",
                    }
                else:
                    swing_low_exhaustion = {
                        "active": True,
                        "remaining_below": _remaining_below,
                        "note": f"price below most swing lows; only {_remaining_below[0]} remains below",
                    }

        result: dict[str, Any] = {
            "swing_high_ltf": swing_highs or None,
            "swing_low_ltf": swing_lows or None,
            "swing_high_htf": swing_highs_htf or None,
            "swing_low_htf": swing_lows_htf or None,
            "obv_trend": obv_trend,
            "obv_trend_confidence": obv_trend_confidence,
            "cvd_trend": cvd_trend,
            "cvd_trend_confidence": cvd_trend_confidence,
            "price_vs_vwap": price_vs_vwap,
            "rsi_zone": rsi_zone,
            "market_regime": market_regime,
            "funding_archetype_c_eligible": funding_archetype_c,
        }
        # 9.6 — Always include regime_confidence and regime_note so the
        # model can check the fields without null-handling.
        result["market_regime_confidence"] = regime_confidence or "stable"
        result["regime_note"] = regime_note
        if swing_high_exhaustion:
            result["swing_high_exhaustion"] = swing_high_exhaustion
        if swing_low_exhaustion:
            result["swing_low_exhaustion"] = swing_low_exhaustion
        return result

    def _build_history_section(self, indicators: dict[str, Any]) -> dict[str, Any]:
        # Raw LTF/HTF candle arrays are NOT sent to the LLM — all indicators are
        # pre-computed.  vwap_series and volume_rsi_series are dropped; their
        # directional conclusions are captured in market_signals labels.
        section: dict[str, Any] = {}
        htf_bar = indicators.get("ohlcv_htf_bar")
        if htf_bar:
            section["timeframe_htf"] = htf_bar
        return section

    def _build_indicator_section(
        self, indicators: dict[str, Any], *, last_price: float | None = None,
    ) -> dict[str, Any]:
        macd = indicators.get("macd") or {}
        rsi = indicators.get("rsi")
        stoch = indicators.get("stoch_rsi") or {}
        bb = indicators.get("bollinger_bands") or {}
        ma = indicators.get("moving_averages") or {}
        adx = indicators.get("adx") or {}
        obv = indicators.get("obv") or {}
        cmf = indicators.get("cmf") or {}
        _series_tail = 5  # retain only a short tail; full series are pre-summarised in market_signals
        raw_obv_series = obv.get("series")
        return {
            "rsi": rsi,
            "stoch_rsi": stoch,
            "macd": {
                "value": macd.get("value"),
                "signal": macd.get("signal"),
                "hist": macd.get("hist"),
                # Series dropped — scalar value/signal/hist are sufficient for decisions
            },
            "bollinger_bands": bb,
            "moving_averages": ma,
            "adx": {
                "value": adx.get("value"),
                "di_plus": adx.get("di_plus"),
                "di_minus": adx.get("di_minus"),
                # Series dropped — adx scalar + DI values are sufficient
            },
            "obv": {
                "value": obv.get("value"),
                # Keep a short tail so the model can confirm obv_trend direction if needed
                "series": self._trim_series(raw_obv_series, _series_tail),
            },
            "cmf": {
                "value": cmf.get("value"),
                # Series dropped — CMF direction is captured in market_signals labels
            },
            "vwap": indicators.get("vwap"),
            "atr": indicators.get("atr"),
            "atr_pct": indicators.get("atr_pct"),
            # volume.series removed — candle data already contains per-bar
            # volume and Volume RSI is in history.volume_rsi_series.  Keep only
            # the scalar summaries (last, average) to save ~200 tokens.
            "volume": self._strip_volume_series(indicators.get("volume")),
            "htf": self._build_htf_indicator_section(indicators, last_price=last_price),
        }

    def _build_htf_indicator_section(
        self,
        indicators: dict[str, Any],
        *,
        last_price: float | None = None,
    ) -> dict[str, Any]:
        """Extract key HTF indicators from pre-computed htf_indicators bundle."""
        htf = indicators.get("htf_indicators") or {}
        if not htf:
            return {}
        adx = htf.get("adx") or {}
        ma = htf.get("moving_averages") or {}
        bb = htf.get("bollinger_bands") or {}
        ema_50 = _to_float(ma.get("ema_50"))
        ema_200 = _to_float(ma.get("ema_200"))
        ma_section: dict[str, Any] = {
            "ema_50": ema_50,
            "ema_200": ema_200,
        }
        # 9.5 — EMA convergence indicator: when EMAs are in a bearish cross
        # but converging, the "bearish" bias label is technically correct but
        # increasingly misleading.  Surface the gap and direction so the model
        # can contextualise lagging EMA signals.
        if ema_50 is not None and ema_200 is not None and ema_200 > 0:
            _gap_pct = round(abs(ema_50 - ema_200) / ema_200 * 100, 2)
            # Infer narrowing: if price sits on the side of the faster EMA
            # that pulls it toward the slower one, the gap will close.
            _narrowing: bool | None = None
            if last_price is not None:
                if ema_50 < ema_200:
                    # Bearish cross — price above EMA50 pulls it up → narrowing
                    _narrowing = last_price > ema_50
                else:
                    # Bullish cross — price below EMA50 pulls it down → narrowing
                    _narrowing = last_price < ema_50
            _conv: dict[str, Any] = {
                "gap_pct": _gap_pct,
                "narrowing": _narrowing,
            }
            if _narrowing and _gap_pct < 15:
                _bias_label = "bearish" if ema_50 < ema_200 else "bullish"
                _conv["note"] = (
                    f"EMA50 rapidly approaching EMA200; {_bias_label} cross may reverse soon"
                )
            ma_section["ema_convergence"] = _conv
        return {
            "adx": {
                "value": adx.get("value"),
                "di_plus": adx.get("di_plus"),
                "di_minus": adx.get("di_minus"),
            },
            "moving_averages": ma_section,
            "bollinger_bands": bb,
            "rsi": htf.get("rsi"),
            "vwap": htf.get("vwap"),
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
        # Flag LTF/HTF directional conflict when short-term momentum (DI direction
        # or price vs EMA50) diverges from longer-term EMA structure.
        _di_bullish = (di_state == "+DI dominance")
        _di_bearish = (di_state == "-DI dominance")
        _ltf_bullish = _di_bullish or price_vs_ema == "above"
        _ltf_bearish = _di_bearish or price_vs_ema == "below"
        if (_ltf_bullish and ema_bias == "bearish") or (_ltf_bearish and ema_bias == "bullish"):
            summary_bits.append("LTF/HTF directional conflict")
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
        ticker: dict[str, Any],
    ) -> dict[str, Any]:
        order_flow = market.get("order_flow") or {}
        spread = _to_float(market.get("spread"))
        spread_pct = _to_float(market.get("spread_pct"))
        bid_depth = _to_float(order_flow.get("bid_depth"))
        ask_depth = _to_float(order_flow.get("ask_depth"))
        _MIN_DEPTH = 50  # minimum contracts for a meaningful bias claim
        last_price = _to_float(market.get("last_price") or ticker.get("last") or ticker.get("px"))
        depth_floor = None
        if bid_depth is not None and ask_depth is not None:
            depth_floor = min(bid_depth, ask_depth)

        target_usd = 100000.0
        target_units = (target_usd / last_price) if last_price else None
        slippage_bps = None
        slippage_note: str | None = None
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
        # 8.3 — When depth is degraded (stale or thin) and no slippage could be
        # computed, provide a conservative floor so the model has a numeric
        # risk input instead of null (which triggers the heuristic spread_pct×2
        # fallback that massively underestimates real slippage in thin books).
        _ob_note = order_flow.get("order_book_note")
        if slippage_bps is None and _ob_note:
            slippage_bps = 50.0
            slippage_note = "elevated estimate due to thin/stale order book"

        imbalance_raw = order_flow.get("imbalance")
        if isinstance(imbalance_raw, dict):
            imbalance_value = _to_float(imbalance_raw.get("net"))
        else:
            imbalance_value = _to_float(imbalance_raw)
        # 6.1 — Derive liquidity_bias from raw depth, not OFI momentum.
        # OFI measures order-flow *change*; depth measures standing liquidity.
        # 8.2 — Add minimum depth threshold: when both sides have trivially few
        # contracts, no directional bias claim is meaningful.
        if bid_depth is None or ask_depth is None:
            liquidity_bias = "unreliable"
        elif bid_depth < _MIN_DEPTH and ask_depth < _MIN_DEPTH:
            liquidity_bias = "thin"
        elif bid_depth > ask_depth * 1.2:
            liquidity_bias = "bid-supported"
        elif ask_depth > bid_depth * 1.2:
            liquidity_bias = "ask-heavy"
        else:
            liquidity_bias = "balanced"

        summary_bits: list[str] = []
        if spread_pct is not None:
            summary_bits.append(f"spread {spread_pct:.3f}%")
        if slippage_bps is not None:
            summary_bits.append(f"~{slippage_bps:.1f} bps est. slippage for $100k")
        summary_bits.append(liquidity_bias)

        # 7.3 — volume last/average already lives in indicators.volume;
        # OBV/CMF values are in indicators.obv/cmf.  Keep only the
        # liquidity-specific fields here to avoid duplication.
        result: dict[str, Any] = {
            "spread": spread,
            "spread_pct": spread_pct,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "estimated_slippage_bps": round(slippage_bps, 2) if slippage_bps is not None else None,
            "liquidity_bias": liquidity_bias,
            "summary": ", ".join(summary_bits),
        }
        if slippage_note:
            result["slippage_note"] = slippage_note
        return result

    def _build_derivatives_posture(
        self,
        funding: dict[str, Any],
        custom_metrics: dict[str, Any],
        liquidations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_rate = _to_float(funding.get("fundingRate") or funding.get("fundRate"))
        next_rate = _to_float(funding.get("nextFundingRate"))
        previous_rate = _to_float(funding.get("prevFundingRate") or funding.get("fundingRatePrev"))
        # 2.10 — Pre-flag Archetype C eligibility (funding < −0.05%).
        funding_archetype_c = bool(current_rate is not None and current_rate < -0.0005)
        funding_delta = None
        if current_rate is not None and next_rate is not None:
            funding_delta = next_rate - current_rate
        elif current_rate is not None and previous_rate is not None:
            funding_delta = current_rate - previous_rate

        long_short_raw = custom_metrics.get("market_long_short_ratio") or {}
        # Trim the L/S ratio series to the last 20 values — enough to determine
        # trend direction (rising/declining for 10+ periods) without wasting tokens
        # on the full 200-candle history.  Also align timestamps to the same tail
        # length so the model can correlate data points.
        _ls_series = long_short_raw.get("series")
        _ls_timestamps = long_short_raw.get("timestamps")
        long_short: dict[str, Any] = {
            k: v for k, v in long_short_raw.items() if k not in ("series", "timestamps")
        }
        _ls_n = 20
        if isinstance(_ls_series, list) and _ls_series:
            long_short["series"] = _ls_series[-_ls_n:]
            if isinstance(_ls_timestamps, list) and _ls_timestamps:
                long_short["timestamps"] = _ls_timestamps[-_ls_n:]
        elif _ls_series is not None:
            try:
                _ls_as_list = list(_ls_series[-_ls_n:])
                long_short["series"] = _ls_as_list
                if isinstance(_ls_timestamps, list) and _ls_timestamps:
                    long_short["timestamps"] = _ls_timestamps[-_ls_n:]
            except (TypeError, KeyError):
                pass  # drop unsliceable series entirely
        # 6.5 — L/S series staleness detection: flag when the most recent
        # timestamp in the series is more than 6 hours old so the model
        # knows the trend direction may not reflect current positioning.
        _ls_ts_list = long_short.get("timestamps")
        if isinstance(_ls_ts_list, list) and _ls_ts_list:
            try:
                _ls_latest_ts = max(int(t) for t in _ls_ts_list if t is not None)
                _now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                _ls_age_hours = (_now_ms - _ls_latest_ts) / (3600 * 1000)
                if _ls_age_hours > 6:
                    long_short["series_stale"] = True
                    long_short["series_age_hours"] = round(_ls_age_hours, 1)
                    # 8.7 — If the series is extremely stale (> 24h), drop it
                    # entirely to save ~150 tokens and prevent the model from
                    # accidentally reasoning from 2-day-old positioning data.
                    if _ls_age_hours > 24:
                        long_short.pop("series", None)
                        long_short.pop("timestamps", None)
                        long_short["note"] = "series omitted: >24h old"
            except (ValueError, TypeError):
                pass
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

        # 9.7 — Slim funding block: only include non-null values; remove metadata timestamps.
        _funding_result: dict[str, Any] = {"current": current_rate}
        if next_rate is not None:
            _funding_result["next"] = next_rate
        if previous_rate is not None:
            _funding_result["previous"] = previous_rate
        if funding_delta is not None:
            _funding_result["delta"] = funding_delta
        result: dict[str, Any] = {
            "funding": _funding_result,
            "long_short_ratio": long_short,
            "summary": ", ".join(summary_bits),
        }
        if liquidation_clusters:
            result["liquidation_clusters"] = liquidation_clusters
        return result

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
            "long_notional": long_notional,
            "short_notional": short_notional,
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
        result: dict[str, Any] = {
            "remaining": remaining,
            "granted": granted,
        }
        # Flag when credit is running low so the model can apply credit conservation.
        if remaining is not None and granted is not None and granted > 0:
            result["low_credit_warning"] = remaining < (granted * 0.10)
        return result

    def _build_margin_health_section(self, execution_settings: dict[str, Any]) -> dict[str, Any] | None:
        if not execution_settings:
            return None
        available_margin = _to_float(execution_settings.get("available_margin_usd"))
        account_equity = _to_float(execution_settings.get("account_equity_usd"))
        equity_cap = _to_float(execution_settings.get("max_equity_allocation_usd"))
        margin_cap = _to_float(execution_settings.get("margin_max_position_value_usd"))
        tier_cap = _to_float(execution_settings.get("tier_max_position_value_usd"))
        effective_cap = _to_float(execution_settings.get("effective_max_position_value_usd"))
        symbol_equity_cap = _to_float(execution_settings.get("symbol_max_equity_allocation_usd"))
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
        if equity_cap is not None:
            summary_bits.append(f"equity cap ${equity_cap:,.0f}")
        if symbol_equity_cap is not None and symbol_cap_pct is not None:
            summary_bits.append(f"symbol cap {symbol_cap_pct * 100:.1f}% (${symbol_equity_cap:,.0f})")
        if tier_imr is not None:
            summary_bits.append(f"tier IMR {tier_imr * 100:.2f}%")
        if freshness_seconds is not None:
            summary_bits.append(f"snapshot age {freshness_seconds}s")
        return {
            "available_margin_usd": available_margin,
            "account_equity_usd": account_equity,
            "effective_cap_usd": limiting_value,
            "limiting_factor": limiting_factor,
            "equity_cap_usd": equity_cap,
            "symbol_equity_cap_usd": symbol_equity_cap,
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

    def _resolve_feedback_ttl_seconds(self) -> int:
        guardrails: dict[str, Any] | None = None
        if isinstance(self.metadata, dict):
            candidate = self.metadata.get("guardrails")
            if isinstance(candidate, dict):
                guardrails = candidate
        if guardrails is None and isinstance(self.snapshot, dict):
            candidate = self.snapshot.get("guardrails")
            if isinstance(candidate, dict):
                guardrails = candidate
        ttl_value: Any | None = None
        if guardrails:
            ttl_value = guardrails.get("execution_feedback_ttl_seconds")
        if ttl_value is None:
            return DEFAULT_EXECUTION_FEEDBACK_TTL_SECONDS
        try:
            numeric = int(float(ttl_value))
        except (TypeError, ValueError):
            return DEFAULT_EXECUTION_FEEDBACK_TTL_SECONDS
        return max(0, numeric)

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
        ttl_seconds = self._resolve_feedback_ttl_seconds()
        cutoff: datetime | None = None
        if ttl_seconds > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)
        selected: list[dict[str, Any]] = []
        for entry in reversed(feedback):
            if not isinstance(entry, dict):
                continue
            if cutoff is not None:
                timestamp = entry.get("timestamp")
                parsed_ts = self._parse_timestamp(timestamp)
                if parsed_ts and parsed_ts < cutoff:
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
                    "recommendation": entry.get("recommendation"),
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
            return {"total": 0}
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

    # ------------------------------------------------------------------
    # New helper methods: pre-computed modifiers, guardrail stripping,
    # numeric rounding
    # ------------------------------------------------------------------

    def _build_pre_computed_modifiers(
        self,
        indicators: dict[str, Any],
        indicator_section: dict[str, Any],
        market_signals: dict[str, Any],
        execution_settings: dict[str, Any],
        execution_feedback: list[dict[str, Any]] | None,
        ltf_candles: list[dict[str, Any]],
        credit_availability: dict[str, Any] | None,
        live_section: dict[str, Any],
        derivatives_section: dict[str, Any],
        *,
        positions_section: list[dict[str, Any]] | None = None,
        pending_orders: dict[str, Any] | None = None,
        resolved_symbol: str | None = None,
    ) -> dict[str, Any]:
        """Pre-compute deterministic flags/penalties so the LLM reads results,
        not instructions on how to calculate them."""

        # HTF state
        htf = indicator_section.get("htf") or {}
        htf_adx_block = htf.get("adx") or {}
        htf_ma_block = htf.get("moving_averages") or {}
        htf_adx_val = _to_float(htf_adx_block.get("value"))
        htf_ema50 = _to_float(htf_ma_block.get("ema_50"))
        htf_ema200 = _to_float(htf_ma_block.get("ema_200"))
        htf_di_plus = _to_float(htf_adx_block.get("di_plus"))
        htf_di_minus = _to_float(htf_adx_block.get("di_minus"))
        htf_available = bool(htf and htf_adx_val is not None)
        htf_strong = htf_adx_val is not None and htf_adx_val > 25
        _htf_di_clear = (
            htf_di_plus is not None
            and htf_di_minus is not None
            and abs(htf_di_plus - htf_di_minus) > 5
        )
        _dmi_bullish = _htf_di_clear and (htf_di_plus or 0) > (htf_di_minus or 0)
        _dmi_bearish = _htf_di_clear and (htf_di_minus or 0) > (htf_di_plus or 0)

        # EMA gap and narrowing direction — used to resolve EMA/DMI conflicts.
        # Narrowing is inferred from price position relative to EMA50:
        #   EMA50 < EMA200 (bearish cross): price above EMA50 pulls it up → gap closes
        #   EMA50 > EMA200 (bullish cross): price below EMA50 pulls it down → gap closes
        _ema_gap_pct: float | None = (
            abs(htf_ema50 - htf_ema200) / htf_ema200 * 100
            if htf_ema50 is not None and htf_ema200 is not None and htf_ema200 != 0
            else None
        )
        _last_price_htf = _to_float(live_section.get("last_price"))
        _ema_narrowing: bool | None = None
        if htf_ema50 is not None and htf_ema200 is not None and _last_price_htf is not None:
            if htf_ema50 < htf_ema200:
                _ema_narrowing = _last_price_htf > htf_ema50
            else:
                _ema_narrowing = _last_price_htf < htf_ema50

        # Unified EMA + DMI classification:
        #   ADX < 25 → neutral regardless of EMA ordering
        #   EMA agrees with DMI (or DMI ambiguous) → follow EMA
        #   EMA contradicts DMI AND gap < 2% AND narrowing → DMI wins (EMA is lagging)
        #   EMA contradicts DMI AND (gap ≥ 2% OR widening) → neutral (genuine conflict)
        #   EMA200 missing → DMI fallback if strong + clear; else unknown
        if htf_ema50 is not None and htf_ema200 is not None:
            if not htf_strong:
                htf_trend_direction = "neutral"
            elif htf_ema50 > htf_ema200:
                if not _dmi_bearish:
                    htf_trend_direction = "bullish"
                else:
                    _gap_small = _ema_gap_pct is not None and _ema_gap_pct < 2.0
                    if _gap_small and bool(_ema_narrowing):
                        htf_trend_direction = "bearish"  # EMA lagging; DMI has flipped
                    else:
                        htf_trend_direction = "neutral"  # genuine conflict, wide gap
            elif htf_ema50 < htf_ema200:
                if not _dmi_bullish:
                    htf_trend_direction = "bearish"
                else:
                    _gap_small = _ema_gap_pct is not None and _ema_gap_pct < 2.0
                    if _gap_small and bool(_ema_narrowing):
                        htf_trend_direction = "bullish"  # EMA lagging; DMI has flipped
                    else:
                        htf_trend_direction = "neutral"  # genuine conflict, wide gap
            else:
                htf_trend_direction = "neutral"  # EMA50 == EMA200
        elif htf_strong and _htf_di_clear:
            # DMI fallback: EMA200 missing but ADX is strong and DI clearly indicates direction.
            htf_trend_direction = "bullish" if _dmi_bullish else "bearish"
        else:
            htf_trend_direction = "unknown"

        # HTF alignment class and pre-computed penalty values
        if not htf_available:
            htf_alignment_class = "unavailable"
            htf_align_penalty: float = -0.20
            htf_contradict_additive: float = -0.20
            htf_contradict_multiplicative: float = 1.0
            htf_contradict_bracket = "no HTF data"
        elif htf_trend_direction in ("neutral", "unknown"):
            htf_alignment_class = "neutral"
            htf_align_penalty = -0.15
            htf_contradict_additive = -0.15
            htf_contradict_multiplicative = 1.0
            htf_contradict_bracket = "weak/neutral ADX" if not htf_strong else "EMA/DMI conflict or insufficient data"
        else:
            htf_alignment_class = htf_trend_direction  # "bullish" or "bearish"
            htf_align_penalty = 0.0
            if htf_adx_val > 30:
                htf_contradict_additive = -0.30
                htf_contradict_multiplicative = 0.7
                htf_contradict_bracket = "strong (ADX > 30)"
            elif htf_adx_val >= 20:
                htf_contradict_additive = -0.20
                htf_contradict_multiplicative = 0.8
                htf_contradict_bracket = "moderate (ADX 20–30)"
            else:
                htf_contradict_additive = -0.10
                htf_contradict_multiplicative = 1.0
                htf_contradict_bracket = "weak (ADX < 20)"

        # OBV/CVD effective weights (map confidence labels to numeric weights)
        _weight_map = {"strong": 1.0, "moderate": 0.75, "weak": 0.5, "unknown": 0.5}
        obv_effective_weight = _weight_map.get(
            str(market_signals.get("obv_trend_confidence") or "unknown"), 0.5
        )
        cvd_effective_weight = _weight_map.get(
            str(market_signals.get("cvd_trend_confidence") or "unknown"), 0.5
        )

        # Order-flow reliability (thin or stale order book)
        order_flow = live_section.get("order_flow") or {}
        ob_note = str(order_flow.get("order_book_note") or "")
        order_flow_reliable = not (ob_note.startswith("thin:") or ob_note.startswith("stale:"))

        # Order-flow penalty (pre-computed from order_book_note)
        order_flow_penalty: float = 0.0
        if not order_flow_reliable:
            order_flow_penalty = -0.10 if ob_note.startswith("thin:") else -0.05

        # Credit conservation
        credit_conservation = False
        credit_risk_add = 0.0
        if credit_availability and isinstance(credit_availability, dict):
            remaining = _to_float(credit_availability.get("remaining"))
            granted = _to_float(credit_availability.get("granted"))
            if remaining is not None and granted is not None and granted > 0:
                if remaining < granted * 0.10:
                    credit_conservation = True
                    credit_risk_add = 0.10

        # Capital sufficiency
        max_safe = _to_float(execution_settings.get("max_safe_notional_usd"))
        min_notional = _to_float(execution_settings.get("min_notional_usd"))
        if max_safe is not None and min_notional is not None:
            capital_sufficient = max_safe >= min_notional
        else:
            capital_sufficient = True  # optimistic when we can't determine

        # Pre-flight flags (visible to LLM as bool; execution layer does the hard checks)
        pre_flight_passed = capital_sufficient
        execution_feedback_blocks = False  # The execution layer checks timing; always False here
        _positions = positions_section or []
        _pending = pending_orders or {}
        has_existing_position = (
            any(
                p.get("symbol") == resolved_symbol and float(p.get("size") or 0) != 0
                for p in _positions
            )
            if resolved_symbol else False
        )
        has_pending_order = bool(
            _pending.get("total", 0) > 0
            and resolved_symbol is not None
            and any(o.get("symbol") == resolved_symbol for o in (_pending.get("open") or []))
        )

        # Spike recency (computed from LTF candles before they are stripped from context)
        spike_recency = False
        spike_risk_add = 0.0
        atr_val = _to_float(indicators.get("atr"))
        if ltf_candles and len(ltf_candles) >= 3 and atr_val and atr_val > 0:
            last_3 = ltf_candles[-3:]
            moves: list[float] = []
            vols: list[float] = []
            for c in last_3:
                open_ = _to_float(c.get("open"))
                close_ = _to_float(c.get("close"))
                vol_ = _to_float(c.get("volume"))
                if open_ is not None and close_ is not None:
                    moves.append(abs(close_ - open_))
                if vol_ is not None:
                    vols.append(vol_)
            if sum(moves) > 2 * atr_val and len(vols) >= 2 and vols[-1] < vols[-2]:
                spike_recency = True
                spike_risk_add = 0.15

        # Regime reclassification: breakdown with diminishing momentum → post_spike_consolidation
        regime_reclassification: str | None = None
        market_regime = market_signals.get("market_regime")
        if market_regime == "breakdown" and ltf_candles and len(ltf_candles) >= 10:
            last_10 = ltf_candles[-10:]
            lows = [v for v in (_to_float(c.get("low")) for c in last_10) if v is not None]
            if len(lows) >= 4:
                higher_low_count = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i - 1])
                ranges_early = [
                    abs((_to_float(c.get("high")) or 0.0) - (_to_float(c.get("low")) or 0.0))
                    for c in last_10[:5]
                ]
                ranges_late = [
                    abs((_to_float(c.get("high")) or 0.0) - (_to_float(c.get("low")) or 0.0))
                    for c in last_10[5:]
                ]
                avg_early = sum(ranges_early) / len(ranges_early) if ranges_early else 0.0
                avg_late = sum(ranges_late) / len(ranges_late) if ranges_late else 0.0
                if (
                    higher_low_count >= len(lows) // 2
                    and avg_early > 0
                    and avg_late < avg_early * 0.8
                ):
                    regime_reclassification = "post_spike_consolidation"

        # L/S ratio signal pre-computation
        ls = derivatives_section.get("long_short_ratio") or {}
        ls_value = _to_float(ls.get("value"))
        ls_series_raw = ls.get("series") if not ls.get("series_stale") else None
        ls_confidence_add: float = 0.0
        if ls_value is None:
            ls_signal = "absent"
        elif not isinstance(ls_series_raw, list) or len(ls_series_raw) < 10:
            ls_signal = "stale_series" if ls.get("series_stale") else "insufficient_data"
        else:
            _ls_tail = [_to_float(v) for v in ls_series_raw[-10:] if _to_float(v) is not None]
            if len(_ls_tail) >= 2 and _ls_tail[-1] < _ls_tail[0]:
                ls_signal = "squeeze_candidate"
                ls_confidence_add = 0.05
            elif len(_ls_tail) >= 2:
                ls_signal = "pressure_candidate"
                ls_confidence_add = 0.05
            else:
                ls_signal = "insufficient_data"
        # Funding fallback for absent L/S: funding < -0.15% → partial Archetype C
        if ls_signal == "absent":
            _funding_rate_live = _to_float(live_section.get("funding_rate"))
            if _funding_rate_live is not None and _funding_rate_live < -0.0015:
                ls_confidence_add = 0.03

        # Archetype C eligibility (funding < -0.05%)
        archetype_c_eligible = bool(market_signals.get("funding_archetype_c_eligible"))

        result: dict[str, Any] = {
            "htf_available": htf_available,
            "htf_adx": htf_adx_val,
            "htf_alignment_class": htf_alignment_class,
            "htf_align_penalty": htf_align_penalty,
            "htf_contradict_additive": htf_contradict_additive,
            "htf_contradict_multiplicative": htf_contradict_multiplicative,
            "htf_contradict_bracket": htf_contradict_bracket,
            "obv_effective_weight": obv_effective_weight,
            "cvd_effective_weight": cvd_effective_weight,
            "order_flow_reliable": order_flow_reliable,
            "order_flow_penalty": order_flow_penalty,
            "credit_conservation": credit_conservation,
            "credit_risk_add": credit_risk_add,
            "capital_sufficient": capital_sufficient,
            "pre_flight_passed": pre_flight_passed,
            "execution_feedback_blocks": execution_feedback_blocks,
            "has_existing_position": has_existing_position,
            "has_pending_order": has_pending_order,
            "spike_recency": spike_recency,
            "spike_risk_add": spike_risk_add,
            "ls_signal": ls_signal,
            "ls_current_value": ls_value,
            "ls_confidence_add": ls_confidence_add,
            "archetype_c_eligible": archetype_c_eligible,
        }
        if regime_reclassification:
            result["regime_reclassification"] = regime_reclassification
        return result

    @staticmethod
    def _strip_guardrails_for_llm(guardrails: dict[str, Any]) -> dict[str, Any]:
        """Return only guardrail fields the LLM needs; strip execution-layer internals."""
        _LLM_GUARDRAIL_KEYS = {
            "max_leverage",
            "min_leverage",
            "min_leverage_confidence_gate",
            "max_position_pct",
            "symbol_position_caps",
            "min_hold_seconds",
            "min_reward_risk_ratio",
            "require_reward_risk_ratio",
            "max_trades_per_hour",
            "daily_loss_limit_pct",
            "risk_model",
        }
        return {k: v for k, v in guardrails.items() if k in _LLM_GUARDRAIL_KEYS}

    @staticmethod
    def _round_floats(obj: Any, *, _depth: int = 0, _key: str = "") -> Any:
        """Recursively round float values to contextually appropriate precision.

        Timestamp-like keys (ts, timestamp, generated_at, …) are skipped.
        Stops at depth 8 to guard against pathological nesting.
        """
        if _depth > 8 or _key in _ROUND_FLOAT_SKIP_KEYS:
            return obj
        if isinstance(obj, float):
            ax = abs(obj)
            if ax >= 10_000:
                return round(obj, 2)
            if ax >= 100:
                return round(obj, 3)
            if ax >= 1:
                return round(obj, 4)
            if ax >= 0.001:
                return round(obj, 6)
            return round(obj, 8)
        if isinstance(obj, dict):
            return {
                k: PromptBuilder._round_floats(v, _depth=_depth + 1, _key=k)
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [
                PromptBuilder._round_floats(item, _depth=_depth + 1, _key=_key)
                for item in obj
            ]
        return obj

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
            "daily_loss_limit_pct": 0.03,
            "risk_model": "ATR based stops x1.5",
            "min_hold_seconds": 180,
            "max_trades_per_hour": 2,
            "trade_window_seconds": 3600,
            "require_position_alignment": True,
            "snapshot_max_age_seconds": 900,
            "wait_for_tp_sl": False,
            "require_protection": True,
            "fallback_orders_enabled": True,
            "min_leverage_confidence_gate": 0.5,
            "execution_feedback_ttl_seconds": DEFAULT_EXECUTION_FEEDBACK_TTL_SECONDS,
            "isolated_margin_seed_usd": None,
            "isolated_margin_symbol_seeds_usd": {},
            "isolated_margin_max_transfer_usd": None,
            "isolated_wallet_bootstrap_pct": None,
            "min_reward_risk_ratio": None,
            "require_reward_risk_ratio": False,
            "adjust_invalid_tp": False,
            "adjust_invalid_tp_pct": 0.10,
            "footprint": {
                "poc_risk_delta": 0.05,
                "net_delta_confidence_delta": 0.02,
                "imbalance_zone_confidence_delta": 0.03,
                "imbalance_zone_proximity_pct": 0.3,
                "bucket_pct": 0.1,
            },
        }

    @staticmethod
    def _response_schema(model_id: str | None, overrides: dict[str, Any] | None) -> dict[str, Any]:
        if model_id and isinstance(overrides, dict):
            override = overrides.get(model_id)
            if isinstance(override, dict):
                return copy.deepcopy(override)
        return copy.deepcopy(RESPONSE_SCHEMA)