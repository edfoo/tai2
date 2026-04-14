````markdown
# Review of Updated Prompt

## Overall Assessment

This is a **meaningful improvement**. The data payload is substantially leaner. However, the instruction text (the `task` field) has barely changed and still contains ~4,000+ tokens of logic that either duplicates the new `pre_computed_modifiers` or belongs in the execution layer. The biggest remaining wins are on the instruction side.

---

## What Was Addressed Well

| Recommendation | Status | Notes |
|---|---|---|
| Remove raw candles | ✅ Done | Both `candles` and `candles_htf` arrays are completely removed. This is the single biggest token savings. |
| Truncate `vwap_series` | ✅ Done | Reduced from ~95 values to 5. |
| Truncate `volume_rsi_series` | ✅ Done | Reduced from ~15 to 5 values. |
| Remove `trend_confirmation` duplicate | ✅ Done | Entire block removed; indicators live in one place now. |
| Remove `risk_metrics` duplicate | ✅ Done | ATR values now only in `indicators`. |
| Flatten `account` object | ✅ Done | Reduced from ~15 fields to just `{"quote_currency":"USDT"}`. |
| Slim `guardrails` | ✅ Done | Execution-only fields like `adjust_invalid_tp`, `fallback_orders_enabled`, `isolated_margin_seed_usd`, `flip_llm_decision`, etc. all removed. |
| Remove `IGNORE flip_llm_decision` instruction | ✅ Done | |
| Simplify `risk_locks` | ✅ Done | Now just `{"daily_loss_active": false}`. |
| Round numeric precision | ✅ Done | Numbers like `0.039588` instead of `0.03897116134060636`. |
| Add `pre_computed_modifiers` block | ✅ Done | Includes `htf_available`, `htf_adx`, `htf_trend_direction`, `credit_conservation`, `spike_recency`, `capital_sufficient`, etc. |
| Use `capital_sufficient` flag | ✅ Done | Prompt references the flag instead of comparing two raw values. |
| Use `credit_conservation` flag | ✅ Done | Prompt reads the flag and adds the pre-computed `credit_risk_add`. |
| Use `spike_recency` flag | ✅ Done | Prompt reads the flag and adds `spike_risk_add`. |
| Use `regime_reclassification` for breakdown staleness | ✅ Done | Prompt references `pre_computed_modifiers.regime_reclassification`. |
| Remove other-symbol positions from `positions` array | ✅ Done | Now `[]` since no TRIA position exists. |

**Estimated tokens saved so far: ~6,000–7,000** (mostly from candle and data removal).

---

## What Still Needs Attention

### Priority 1: The `task` instructions are still ~4,000+ tokens and largely unchanged

The `pre_computed_modifiers` block was added to the data, but the instructions still tell the LLM **how to compute the same things from scratch**. This means you are paying for both the pre-computed result AND the instructions explaining the computation. Specific examples:

**HTF alignment penalties are still fully described as computation steps:**

The prompt still says:
> *"Score the HTF alignment with your proposed direction: Strong alignment (ema_50/ema_200 agree AND adx.value > 25 with correct DI): +0.0 penalty... Weak/neutral HTF... HTF contradicts direction - apply an ADX-scaled penalty: HTF ADX > 30 and contradicts: −0.30 penalty AND reduce confidence by ×0.7..."*

But `pre_computed_modifiers` already provides `htf_adx` and `htf_trend_direction`. The missing piece is pre-computing the penalty values themselves. Add these to the modifiers block:

```json
{
  "htf_alignment": "weak_neutral",
  "htf_penalty_additive": -0.15,
  "htf_penalty_multiplicative": 1.0,
  "htf_contradiction_additive": -0.20,
  "htf_contradiction_multiplicative": 0.8,
  "htf_contradiction_bracket": "moderate (ADX 20-30)"
}
```

Then collapse the entire STEP 2 section to roughly:

> *"STEP 2 - HTF TREND FILTER: pre_computed_modifiers contains the HTF alignment classification and applicable penalty values. If your proposed direction aligns with htf_trend_direction, apply htf_penalty_additive only. If it contradicts, apply htf_contradiction_additive then multiply by htf_contradiction_multiplicative. A counter-trend trade is allowed only if net confidence after all penalties ≥ 0.45. State the alignment in rationale."*

This replaces ~400 tokens of branching rules with ~80 tokens.

**OBV/CVD weight calculation instructions are redundant with `obv_effective_weight` / `cvd_effective_weight`:**

The prompt still contains the full weight mapping explanation:
> *"If obv_trend_confidence is 'strong', the signal carries full weight. If 'moderate', reduce weight by one-quarter. If 'weak' or 'unknown', reduce weight by half."*

This is already computed as `obv_effective_weight: 0.5` and `cvd_effective_weight: 1.0`. Replace the weight mapping paragraphs with:

> *"Use pre_computed_modifiers.obv_effective_weight and cvd_effective_weight to scale the OBV and CVD signal strength."*

**HTF DI AMBIGUITY paragraph is redundant:**

This 50-token paragraph describes a check that should produce a single flag value. Pre-compute it as `htf_alignment: "weak_neutral_di_ambiguity"` and remove the paragraph entirely.

**THIN BOOK RULE, ZERO DEPTH FALLBACK are redundant with `order_flow_reliable`:**

`pre_computed_modifiers.order_flow_reliable` already captures this. Add an `order_flow_penalty` value to the modifiers and collapse two paragraphs into one sentence.

---

### Priority 2: Remaining data duplication

| Value | Still appears in | Action |
|---|---|---|
| Available margin ($78.39) | `execution.available_margin_usd`, `execution.live_margin_snapshot.available_margin_usd`, `execution.live_margin_snapshot.quote_available_usd` | Keep one: `execution.available_margin_usd`. Drop `live_margin_snapshot` sub-fields that repeat it. |
| Account equity ($91.81) | `execution.account_equity_usd`, `execution.live_margin_snapshot.account_equity_usd` | Keep one: `execution.account_equity_usd`. |
| Spread / spread_pct | `market.spread`, `market.spread_pct`, `liquidity_context.spread`, `liquidity_context.spread_pct` | Keep one location. Since `liquidity_context` has the summary, put it there only and remove from `market`. |
| `funding_archetype_c_eligible` | `market_signals.funding_archetype_c_eligible` and `pre_computed_modifiers.archetype_c_eligible` | Keep only in `pre_computed_modifiers`. |
| Bid/ask depth | `market.order_flow.bid_depth`, `market.order_flow.ask_depth`, `liquidity_context.bid_depth`, `liquidity_context.ask_depth` | Keep one location. |
| `margin_health` sub-object | `execution.margin_health` repeats caps/equity already in `execution` top-level | Flatten: keep `margin_health.summary` string, drop the rest. The LLM does not compute from individual cap fields. |

**Estimated savings: ~400–600 tokens.**

---

### Priority 3: Series data still longer than needed

| Series | Current length | Recommended | Reason |
|---|---|---|---|
| `cvd_series` | 30 values | 5 values | Prompt says "cross-check against last 20+ values" but with `cvd_trend_confidence: "strong"` already provided, 5 recent values suffice for slope verification. Update the prompt instruction to say "last 5 values" instead of "last 20+". |
| `ofi_ratio_series` | 20 values | 5 values | Prompt says "sustained run of 3+ periods" — 5 values is sufficient to check this. |
| `obv.series` | 15 values | 5 values | Prompt says "cross-check last 3-5 values". Already matches recommendation for OBV, but 15 are still sent. |
| `cmf.series` | 15 values | 5 values | No prompt rule references more than recent trend. |

**Estimated savings: ~400–500 tokens.**

---

### Priority 4: Step 1 pre-flight checks are still in the prompt

The entire STEP 1 block (~500 tokens) still describes checks that should be execution-layer hard-blocks. In this snapshot:

- `positions` is `[]` for TRIA — no conflict. This check is trivial code.
- `pending_orders.total` is `0` — no conflict. Trivial code.
- `execution_feedback` is `null` — no block. Trivial code.
- `risk_locks.daily_loss_active` is `false` — no block. Trivial code.
- `capital_sufficient` is `true` — already pre-computed.

If any of these failed, the LLM should never be called. Since you have already added `capital_sufficient` to `pre_computed_modifiers`, extend the pattern:

```json
{
  "pre_flight_passed": true,
  "execution_feedback_blocks": false,
  "has_existing_position": false,
  "has_pending_order": false
}
```

Then reduce STEP 1 to:

> *"STEP 1 - PRE-FLIGHT: pre_computed_modifiers.pre_flight_passed confirms all hard-block checks have been passed by the execution layer. If execution_feedback_blocks is true, prefer HOLD. If has_existing_position or has_pending_order is true in the same direction, prefer HOLD or close/reverse. Cite pre-flight status in rationale."*

This replaces ~500 tokens with ~60 tokens.

---

### Priority 5: Post-validation rules still in the prompt

These rules are still present and should be enforced by the execution layer after receiving the response, not by instructing the LLM:

- **"CRITICAL DIRECTION RULE"** (~100 tokens): `stop_loss` direction validation is a trivial post-check.
- **"confidence < 0.45 → MUST HOLD"**: Override downstream if violated.
- **"require_reward_risk_ratio"**: This guardrail is sent but never referenced by a prompt rule. Either the execution layer checks R:R after receiving TP/SL, or it should not be sent.

Removing these saves ~150 tokens of instruction and makes enforcement deterministic.

---

### Priority 6: Remaining structural items

**Timestamps are still millisecond integers:**

Swing highs/lows like `{"price":0.02614,"ts":1775993400000,"bar_index":86}` — the `ts` field is 13 digits. Since `bar_index` provides ordering and the LLM does not compute time deltas, consider dropping `ts` entirely from swing arrays and saving ~10 tokens per entry (~80 tokens across all swing arrays). If the timestamp is needed, use truncated ISO: `"2026-04-12T11:30"`.

**`fee_availability` still present with no rule:**

```json
"fee_availability":{"window_hours":24.0,"total_fee":0.203218,"pct_of_equity":0.221339,"note":"Fees gathered from recent OKX fills"}
```

No prompt instruction references this data. Remove it (~40 tokens) or add a rule that uses it.

**`response_schema` still in the prompt body:**

If the model API supports a structured output / JSON schema parameter, pass the schema there instead of embedding it in the prompt (~200 tokens).

**`portfolio_exposure.heatmap` still lists other symbols:**

The LLM is deciding on TRIA. The heatmap for HMSTR and RAVE is ~80 tokens. The `summary` string (`"Long $83, Short $5, Net 84.2% of equity"`) already captures what the LLM needs. Drop `heatmap` or keep only the TRIA entry if one exists.

**`execution.live_margin_snapshot` contains redundant metadata:**

Fields like `source: "balance-snapshot"`, `quote_currency: "USDT"`, `updated_at` are metadata the LLM does not use. The `margin_health.summary` string already provides the human-readable version. Consider keeping only:

```json
"execution": {
  "max_safe_notional_usd": 78.39,
  "min_notional_usd": 0.5,
  "max_leverage": 10.0,
  "price_reference": 0.02526,
  "symbol_rules": {"min_size": 0.001},
  "margin_summary": "$78 free margin, equity cap $46, snapshot age 0s"
}
```

---

## Summary of Remaining Savings

| Optimization | Est. Tokens | Difficulty |
|---|---|---|
| Shorten `task` instructions using pre-computed values | 1,500–2,000 | Medium |
| Remaining data deduplication | 400–600 | Low |
| Truncate series to 5 values each | 400–500 | Low |
| Move STEP 1 pre-flight to execution layer | 400–500 | Medium |
| Remove post-validation instructions | ~150 | Low |
| Structural cleanup (timestamps, fee_availability, heatmap, margin metadata, response_schema) | 300–400 | Low |
| **Total remaining** | **~3,200–4,200** | — |

Combined with the ~6,000–7,000 tokens already saved, total savings from the original prompt would be **~9,000–11,000 tokens**, approaching the 65% reduction target.

---

## Quick-Reference Checklist

- [ ] Pre-compute HTF penalty values and add to `pre_computed_modifiers`; shorten STEP 2 instructions
- [ ] Replace OBV/CVD weight paragraphs with reference to pre-computed weights
- [ ] Remove HTF DI AMBIGUITY paragraph (pre-compute the classification)
- [ ] Remove THIN BOOK / ZERO DEPTH paragraphs (pre-compute as `order_flow_reliable` + `order_flow_penalty`)
- [ ] Add `pre_flight_passed`, `execution_feedback_blocks`, `has_existing_position`, `has_pending_order` to modifiers; collapse STEP 1
- [ ] Remove duplicate margin/equity/spread/depth fields
- [ ] Truncate `cvd_series`, `ofi_ratio_series`, `obv.series`, `cmf.series` to 5 values each
- [ ] Update prompt to say "last 5 values" instead of "last 20+ values" for CVD cross-check
- [ ] Remove "CRITICAL DIRECTION RULE" and confidence-floor enforcement from prompt; enforce downstream
- [ ] Remove `fee_availability` or add a rule
- [ ] Remove `portfolio_exposure.heatmap` (keep `summary` only)
- [ ] Flatten `execution` object; drop `live_margin_snapshot` sub-fields
- [ ] Drop `ts` from swing arrays or convert to short ISO format
- [ ] Move `response_schema` to model API parameter if supported
````