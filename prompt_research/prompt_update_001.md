```markdown
# Comprehensive Prompt Analysis & Improvement Recommendations

This is a well-structured, deeply detailed trading prompt. However, there are several areas where wording, logic, data coherence, and missing information could be improved to help the model produce more accurate and consistent recommendations.

---

## 1. Contradicting / Conflicting Signals in the Context Data

These are the most critical issues because they can confuse the model and lead to unreliable outputs.

### 1.1 — CVD Trend Label vs. Actual CVD Data (MAJOR CONTRADICTION)

**The problem:**
- `context.market_signals.cvd_trend` is labelled `"net_positive_rising"`
- But `context.market.order_flow.cvd` = **-80,464** (deeply negative)
- The `cvd_series` shows a clear collapse from ~+8,400 down to ~-95,000, with a partial recovery to -80,464 — this is **net negative and was recently falling hard**, not "net positive rising."

**Impact:** The model is told to trust pre-computed labels, yet the raw data blatantly contradicts the label. This will either:
- Mislead the model if it trusts the label, or
- Create confusion if it inspects the raw series (which the prompt only instructs for OBV, not CVD).

**Recommendation:**
1. Fix the CVD labelling logic upstream to accurately reflect the series.
2. Add a **CVD LABEL VERIFICATION** rule analogous to the existing OBV LABEL VERIFICATION rule:
   > *"Before relying on cvd_trend, inspect the last 20-30 values of the CVD series. If the recent slope contradicts the label, treat the label as stale and reduce its weight by half."*
3. Alternatively, provide a `cvd_slope` or `cvd_direction` field computed over a defined lookback window.

---

### 1.2 — OBV Trend Label vs. OBV Series (POTENTIAL INCONSISTENCY)

**The problem:**
- `obv_trend` = `"diverging_bearish"` (meaning price rising while OBV falls)
- But price has been **falling** (from ~0.001700 → 0.001623), not rising.
- Looking at the OBV series: the last 5 values are `[-1676827, -1570976, -1485939, -1686833]` — OBV rose then dropped back.

The label "diverging_bearish" implies price↑ + OBV↓, but price is actually **down**. This is more accurately described as **confirming bearish** (price falling, OBV falling together), not a divergence.

**Recommendation:**
- Clarify in the prompt what "diverging_bearish" means when price is also declining. The current Step 3 definition says *"price rising, OBV falling"*, but the data shows price falling too. The model will notice this mismatch.
- Add a rule: *"If the divergence label does not match the observed price direction, reclassify the OBV signal as 'confirming' rather than 'diverging' and note the mismatch."*

---

### 1.3 — Market Regime "breakdown" vs. Recent Price Action

**The problem:**
- `market_regime` = `"breakdown"` — this triggers a "strong SELL bias or HOLD; do not BUY" rule.
- However, the last ~12 candles show price stabilising and gently rising (0.001603 → 0.001631 → 0.001624), suggesting the breakdown may be transitioning into a base/consolidation.

**Recommendation:**
- The regime classification should include a **staleness/freshness indicator** — e.g., "breakdown_active" vs. "breakdown_stabilising."
- Add guidance: *"If the market_regime is 'breakdown' but the last N candles show diminishing downward momentum (higher lows, declining ATR, or volume contraction), consider reclassifying to 'post_spike_consolidation' and note the override in rationale."*

---

### 1.4 — Liquidity Bias "bid-supported" in a Breakdown Regime

**The problem:**
- `liquidity_bias` = `"bid-supported"` with `bid_depth` = 33,876 and `ask_depth` = 0.
- A `ask_depth` of exactly **0** is almost certainly a data error or snapshot artefact — there is always some ask-side liquidity.
- Labelling this as "bid-supported" when ask depth is zero makes the imbalance ratio undefined/infinite, which is not a meaningful signal.

**Recommendation:**
1. Validate `ask_depth` upstream — if zero, flag as `"depth_data_incomplete"` rather than computing a bias.
2. Add a prompt rule: *"If bid_depth or ask_depth is 0, treat order_flow.imbalance as unreliable and apply −0.05 to any order-flow-derived confidence."*
3. Note that `imbalance.ratio` is already `null` — this should be explicitly handled.

---

## 2. Prompt Wording & Phrasing Issues

### 2.1 — Overly Long and Dense Task Block

The `task` field is ~2,800 words of continuous instruction packed into a single string. This creates several risks:
- **Instruction dilution**: later rules may receive less attention from the model.
- **Ambiguous precedence**: when rules interact (e.g., regime rules vs. archetype rules vs. signal hierarchy), it's unclear which wins.

**Recommendation:**
- Break the task into **numbered, clearly delineated sections** with explicit headers (even in the JSON string, use `\n\n### STEP 1 — ...`).
- Add an **explicit precedence clause** at the top:
  > *"In case of conflict between rules, precedence is: Pre-flight checks > Signal Hierarchy > Regime Rules > Archetype preference > Confidence penalties."*
- Consider moving the response_schema description out of the task and into its own `output_instructions` field.

---

### 2.2 — Ambiguous Penalty Arithmetic

Step 2 describes penalties as both **additive** and **multiplicative** without clear sequencing:

> *"HTF ADX > 30 and contradicts: −0.30 penalty AND reduce confidence by ×0.7"*

Does this mean:
- $(confidence - 0.30) \times 0.7$, or
- $confidence \times 0.7 - 0.30$, or
- Apply whichever is larger?

**Recommendation:**
Specify the order explicitly:
> *"First apply the additive penalty to the base confidence, then apply the multiplicative scaling: $final = (base - 0.30) \times 0.7$."*

Similarly, clarify whether the ranging penalty (−0.10) stacks with HTF penalties or is applied independently.

---

### 2.3 — "flip_llm_decision" Guardrail is Unexplained

`guardrails.flip_llm_decision` = `true` — this is an extraordinary flag that could mean the execution layer **reverses** whatever the model recommends. Yet the prompt never mentions it, and the model has no instruction on how to account for it.

**Recommendation:**
- Either explain this in the prompt: *"Note: the execution layer may flip your recommended direction. Ensure your TP/SL and rationale are internally consistent with YOUR stated direction — the flip is handled downstream."*
- Or remove it from the context if the model shouldn't reason about it (to avoid confusion).
- **If the model reasons about this flag**, it may start gaming its own output (e.g., recommending BUY when it means SELL), which would be catastrophic.

---

### 2.4 — Vague "estimated_slippage_bps: null"

Slippage is referenced conceptually in risk_score computation, but the actual field is `null`. The prompt doesn't tell the model what to do when slippage data is missing.

**Recommendation:**
Add a fallback rule:
> *"If estimated_slippage_bps is null, estimate slippage from spread_pct × 2 as a conservative proxy, and note this assumption."*

---

## 3. Elements That Do Not Add Value / Add Noise

### 3.1 — Excessive Raw Series Data

The prompt includes:
- **200 CVD data points** in `cvd_series`
- **200+ volume data points** in `volume.series`
- **200 L/S ratio timestamps** (but only 20 actual values — see issue 4.2 below)
- Full `vwap_series` of 50 points

Most models won't meaningfully process 200 sequential numbers in a JSON blob — they'll at best sample a few or derive a vague trend sense.

**Recommendation:**
- Replace raw series with **pre-computed summaries**:
  ```json
  "cvd_summary": {
    "current": -80464,
    "slope_20": -1250.5,
    "direction": "falling",
    "recent_reversal": true,
    "reversal_from": -95023
  }
  ```
- Keep only the last 10-15 points of any series the model is explicitly asked to inspect (e.g., OBV last 5 per the verification rule).
- This will **reduce token count by ~40%** and improve reasoning quality.

---

### 3.2 — Duplicated Information

Several fields appear in multiple places:
- `account.available_equity` ≈ `execution.available_margin_usd` ≈ `margin_health.available_margin_usd`
- `positions` data appears in both `positions[]` and `portfolio_exposure.heatmap[]`
- `response_schema` appears **twice** — once inside `prompt` and once at the top level of `payload`
- `atr` and `atr_pct` appear in both `indicators` and `risk_metrics`
- `volume.last` and `volume.average` duplicate what's derivable from the series

**Recommendation:**
- Deduplicate aggressively. Keep the most complete version and reference it once.
- The duplicated `response_schema` is particularly wasteful — it consumes ~500 tokens for zero additional value.

---

### 3.3 — Volume RSI Series Without Context

`volume_rsi_series` is provided (15 values) but **never referenced** in any prompt rule. The model has no instruction on how to use it.

**Recommendation:**
Either:
- Add a rule in the prompt for how volume RSI should influence decisions, or
- Remove it to reduce noise.

---

## 4. Missing Information That Would Improve Accuracy

### 4.1 — No Execution Feedback / Previous Trade Context

The prompt references `context.execution_feedback` in the staleness rule (Step 1d), but **no execution_feedback field exists** in the context. The model is told to check for a hard-block from prior rejections but has no data to evaluate.

**Recommendation:**
- Always include the field, even if empty:
  ```json
  "execution_feedback": null
  ```
  or
  ```json
  "execution_feedback": {
    "last_rejection": null,
    "last_rejection_ts": null,
    "digest": null
  }
  ```
- This prevents the model from guessing or hallucinating about prior rejections.

---

### 4.2 — L/S Ratio: 200 Timestamps but Only 20 Values

`long_short_ratio.timestamps` has **200 entries** but `long_short_ratio.series` has only **20 entries**. There's no way to align these, and the prompt asks the model to assess "10+ period" trends from the series.

**Recommendation:**
- Ensure timestamps and values arrays are the **same length** and aligned.
- Or provide a simpler structure:
  ```json
  "long_short_ratio": {
    "current": 2.18,
    "trend_direction": "declining",
    "trend_periods": 14,
    "series_last_20": [2.26, 2.23, ..., 2.18]
  }
  ```

---

### 4.3 — Missing `change_24h`

`market.change_24h` is `null`. This is a basic but useful contextualization metric. The model has no quick reference for how far price has moved in 24 hours.

**Recommendation:**
- Compute this upstream (it's trivial from candle data).
- If unavailable, provide a note: `"change_24h": null, "change_24h_note": "data unavailable from exchange"`

---

### 4.4 — No Recent Trade History / Win Rate Context

The prompt mentions fee availability and credit usage but provides no information about:
- Recent trade outcomes on this symbol
- Overall strategy win rate
- Whether the model has been over-trading or under-trading

**Recommendation:**
Add a `recent_performance` block:
```json
"recent_performance": {
  "trades_last_24h": 3,
  "win_rate_last_20": 0.55,
  "avg_rr_achieved": 1.2,
  "last_trade_symbol": "PUMP-USDT-SWAP",
  "last_trade_result": "stop_loss_hit",
  "last_trade_pnl_pct": -0.8
}
```
This helps the model avoid repeating recent mistakes.

---

### 4.5 — No Broader Market / Correlation Context

Crypto altcoins (especially a micro-cap like PUMP) are heavily correlated with BTC/ETH. There is no reference to:
- BTC trend or current price action
- Overall market sentiment
- Correlation coefficient

**Recommendation:**
Add:
```json
"macro_context": {
  "btc_trend": "ranging",
  "btc_change_4h": -0.3,
  "eth_trend": "bearish",
  "crypto_fear_greed": 42,
  "correlation_to_btc_30d": 0.72
}
```

---

### 4.6 — No Time-of-Day / Session Context

The snapshot is from `08:08 UTC` — which session is active matters for volume expectations and typical volatility patterns.

**Recommendation:**
Add:
```json
"session_context": {
  "utc_hour": 8,
  "active_session": "EU_open",
  "expected_volume_profile": "increasing"
}
```

---

## 5. Structural / Logic Issues

### 5.1 — Confidence Floor Logic is Contradictory with Regime Rules

The prompt says:
- **Breakdown regime**: "strong SELL bias or HOLD; do not BUY unless HTF shows compelling bullish divergence"
- **Confidence floor**: default 0.50 to trade, lowerable to 0.45 only under specific Archetype C conditions

But it never specifies whether the **regime rule is a hard block or a soft penalty**. If the model computes a SELL with 0.60 confidence in a breakdown regime, is that automatically valid? What if it wants to HOLD with 0.70 confidence because breakdown says "HOLD"?

**Recommendation:**
Clarify:
> *"Regime rules adjust bias and may apply penalties, but they do NOT override the confidence-based sizing system. If the regime says 'prefer HOLD' but your computed confidence for a SELL exceeds 0.55, the SELL is valid. Document the regime tension in rationale."*

---

### 5.2 — Isolated Margin Seed ($500) vs. Account Equity ($100)

`guardrails.isolated_margin_seed_usd` = 500, but the total account is only ~$100. The model might reference this as a constraint but it's nonsensical — you can't seed $500 from a $100 account.

**Recommendation:**
- Either cap the seed at account equity, or explain that this is a **maximum** that will be clamped:
  > *"isolated_margin_seed_usd is capped at available_margin_usd if the account has insufficient funds."*

---

### 5.3 — The "min_hold_seconds: 600" Has No Prompt Rule

The guardrails specify a 10-minute minimum hold time, but the prompt never instructs the model to consider this when timing entries (e.g., don't enter right before expected high-volatility if you can't exit for 10 minutes).

**Recommendation:**
Add to Step 1 or Step 5:
> *"Note that positions must be held for a minimum of context.guardrails.min_hold_seconds. If near-term volatility (e.g., funding settlement in < min_hold_seconds) could trigger a stop before the hold period expires, prefer HOLD."*

---

## 6. Summary of Priority Recommendations

| Priority | Issue | Type | Recommendation |
|----------|-------|------|----------------|
| 🔴 Critical | CVD label contradicts CVD data | Data integrity | Fix labelling logic; add CVD verification rule |
| 🔴 Critical | OBV "diverging_bearish" when price also falling | Data integrity | Reclassify as "confirming_bearish" or add mismatch handling |
| 🔴 Critical | `flip_llm_decision: true` unexplained | Prompt logic | Explain or hide from model |
| 🟠 High | Penalty arithmetic order ambiguous | Wording | Specify exact formula with order of operations |
| 🟠 High | `execution_feedback` referenced but absent | Missing data | Always include field, even if null |
| 🟠 High | L/S ratio timestamp/series mismatch | Data integrity | Align arrays or summarise |
| 🟡 Medium | 200-point raw series bloat | Efficiency | Replace with pre-computed summaries |
| 🟡 Medium | `ask_depth = 0` treated as valid | Data integrity | Add validation/fallback rule |
| 🟡 Medium | Duplicated fields (~500 wasted tokens) | Efficiency | Deduplicate |
| 🟡 Medium | No macro/BTC context | Missing data | Add correlation & BTC trend |
| 🟢 Low | No session/time-of-day context | Missing data | Add session info |
| 🟢 Low | Volume RSI unused | Noise | Add rule or remove |
| 🟢 Low | Regime rule hard/soft ambiguity | Wording | Clarify precedence |

---

## 7. Suggested Prompt Addendum

Here is a **concise block** you could insert at the end of the task to address the most critical gaps without rewriting the entire prompt:

```
DATA QUALITY SAFEGUARDS:
(a) CVD LABEL VERIFICATION: analogous to OBV verification — inspect the
    last 20+ values of cvd_series. If the slope contradicts the cvd_trend
    label, treat the label as stale and reduce weight by half. State the
    observed slope vs. label in rationale.
(b) ZERO DEPTH FALLBACK: if bid_depth or ask_depth is 0, treat
    order_flow imbalance as unreliable; apply −0.05 confidence penalty
    and note "incomplete depth data."
(c) DIVERGENCE LABEL SANITY CHECK: if obv_trend says "diverging_X" but
    the actual price direction does not match the divergence definition
    (e.g. "diverging_bearish" requires price RISING + OBV FALLING),
    reclassify the signal as "confirming" and note the mismatch.
(d) EXECUTION FEEDBACK ABSENT: if context.execution_feedback is missing
    or null, note "no prior rejection feedback" and proceed — do not
    infer any rejection.
(e) IGNORE guardrails.flip_llm_decision — this is handled by the
    execution layer post-response. Reason only about your intended
    direction.
```

This single block resolves the top 5 critical issues identified above.
```

Here is your downloadable markdown file. It contains the complete analysis with all sections, tables, code blocks, and recommendations preserved in proper markdown format.