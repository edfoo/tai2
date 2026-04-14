````markdown
# Review of Updated Prompt (v3)

## Overall Assessment

This is a **strong iteration**. The data payload is now well-optimised and the `pre_computed_modifiers` block is comprehensive. The `task` instructions have been meaningfully shortened in STEP 1 and STEP 2. The remaining improvements are mostly incremental — you are close to the practical optimum without a full prompt rewrite.

---

## What Was Addressed Since Last Review

| Recommendation | Status | Notes |
|---|---|---|
| Pre-compute HTF penalty values (`htf_align_penalty`, `htf_contradict_additive`, etc.) | ✅ Done | Full penalty values now in `pre_computed_modifiers`. |
| Shorten STEP 2 instructions to reference pre-computed values | ✅ Done | STEP 2 is now ~60% shorter. The LLM reads values instead of computing them. |
| Replace OBV/CVD weight paragraphs with reference to pre-computed weights | ✅ Done | Prompt now says "Use pre_computed_modifiers.obv_effective_weight... to scale". |
| Remove HTF DI AMBIGUITY paragraph | ✅ Done | Now handled by `htf_alignment_class`. |
| Remove THIN BOOK / ZERO DEPTH paragraphs, use `order_flow_reliable` + `order_flow_penalty` | ✅ Done | Single sentence now: "if order_flow_reliable is false, apply order_flow_penalty". |
| Add `pre_flight_passed`, `execution_feedback_blocks`, `has_existing_position`, `has_pending_order` | ✅ Done | All present in `pre_computed_modifiers`. |
| Collapse STEP 1 to short paragraph | ✅ Done | STEP 1 is now ~80 tokens instead of ~500. |
| Remove duplicate spread/depth from `market` (now only in `liquidity_context`) | ✅ Done | `market.spread` and `market.spread_pct` are removed. |
| Remove duplicate `funding_archetype_c_eligible` from `market_signals` | ✅ Done | Only in `pre_computed_modifiers` now. |
| Remove `fee_availability` | ✅ Done | No longer present. |
| Remove `portfolio_exposure.heatmap` | ✅ Done | Only `summary` remains. |
| Flatten `execution.margin_health` | ✅ Done | Now just `{"summary": "..."}`. |
| Remove timestamps from swing arrays | ✅ Done | Only `price` and `bar_index` remain. |
| Truncate `cvd_series` to 5 values | ✅ Done | |
| Truncate `ofi_ratio_series` to 5 values | ✅ Done | |
| Truncate `obv.series` to 5 values | ✅ Done | |
| Truncate `cmf.series` to 5 values | ✅ Done | |
| Update CVD cross-check instruction to "last 5 values" | ✅ Done | |

---

## Remaining Items

### 1. `response_schema` still embedded in the prompt

The `response_schema` object is ~200 tokens. If your model API supports a `response_format` or `json_schema` parameter (OpenAI, many OpenRouter-hosted models), pass the schema there instead of in the prompt body. The LLM still knows to output JSON from the system message instruction. If the model you are using (`deepseek-v3.1-nex-n1`) does not support a schema parameter, this is unavoidable — keep it as-is.

**Savings if applicable: ~200 tokens.**

---

### 2. CRITICAL DIRECTION RULE is still in the prompt

This passage is still present:

> *"CRITICAL DIRECTION RULE: for a BUY, stop_loss MUST be strictly below entry price and take_profit MUST be strictly above entry price. For a SELL, stop_loss MUST be strictly above entry price and take_profit MUST be strictly below entry price. A take_profit or stop_loss on the wrong side of entry will be rejected by the execution layer."*

This is pure post-validation. The execution layer already rejects wrong-side TP/SL (the prompt itself says so). Enforcing it in code is trivial and 100% reliable:

```python
if action == "BUY" and (stop_loss >= entry or take_profit <= entry):
    reject()
if action == "SELL" and (stop_loss <= entry or take_profit >= entry):
    reject()
```

However, there is a pragmatic argument for keeping it: it reduces the frequency of rejected responses, saving you a retry call. Whether to keep it depends on how often the LLM gets this wrong without the instruction. If rejection rate is low (< 5%), remove it. If it is higher, keep it as a cheap insurance (~80 tokens).

**Recommendation:** Test removal. If rejection rate stays low, remove. **Potential savings: ~80 tokens.**

---

### 3. `capital_sufficient` HOLD instruction appears twice

The exact same rule appears in both STEP 5 and the SIZING CONTEXT paragraph:

> *"If context.pre_computed_modifiers.capital_sufficient is false (insufficient free capital), you MUST choose HOLD."*

Remove the second occurrence in SIZING CONTEXT. One mention is sufficient.

**Savings: ~25 tokens.**

---

### 4. Remaining data-level duplication

| Value | Appears in | Recommendation |
|---|---|---|
| `execution.available_margin_usd` (78.39) | `execution.available_margin_usd` AND `execution.live_margin_snapshot.quote_available_usd` | Drop `live_margin_snapshot.quote_available_usd`. |
| `execution.account_equity_usd` (90.61) | Top-level field, while `margin_health.summary` already says "$78 free margin, equity cap $45" | The top-level field is fine for the LLM to reference. But `live_margin_snapshot` as a whole now adds little over `margin_health.summary`. |
| `live_margin_snapshot` sub-object | Contains `quote_available_usd`, `quote_cash_usd`, `quote_currency`, `source`, `updated_at` | The LLM does not use `source` or `updated_at` or `quote_cash_usd`. The `margin_health.summary` already provides a human-readable version. Consider removing `live_margin_snapshot` entirely — the LLM has `max_safe_notional_usd`, `min_notional_usd`, and `margin_health.summary`. |

Proposed slimmed `execution` block:

```json
{
  "execution": {
    "enabled": true,
    "trade_mode": "isolated",
    "order_type": "market",
    "min_size": 0.001,
    "price_reference": 0.02559,
    "symbol_rules": {"min_size": 0.001},
    "max_leverage": 10.0,
    "max_safe_notional_usd": 78.39,
    "min_notional_usd": 0.5,
    "margin_summary": "$78 free margin, equity cap $45, snapshot age 0s"
  }
}
```

Fields removed from `execution`:
- `available_margin_usd` — the LLM uses `max_safe_notional_usd` for sizing decisions, not raw margin.
- `account_equity_usd` — already captured in `margin_summary` and `portfolio_exposure`.
- `max_equity_allocation_usd` — execution layer concern, not referenced by any prompt rule.
- `max_position_pct` — already in `guardrails`.
- `live_margin_snapshot` — redundant with `margin_summary`.
- `margin_health` sub-object — replaced by `margin_summary` string.

**Savings: ~120 tokens.**

---

### 5. `execution.min_size` appears twice

Present as both `execution.min_size` and `execution.symbol_rules.min_size` with the same value (0.001). Keep one.

**Savings: ~15 tokens.**

---

### 6. `order_book_note` is a new field but prompt doesn't reference it

The `market.order_flow.order_book_note` field (`"stale: ask_depth was zero at snapshot time"`) is present in the data but no prompt rule reads this specific field. Previously the prompt had the "THIN BOOK RULE" that checked for `order_book_note` starting with `"thin:"` — but that rule was replaced by `pre_computed_modifiers.order_flow_reliable`.

This field is now only useful for the pre-computation layer. You can either:
- **Remove it from the LLM payload** (the pre-computed flag is all the LLM needs), or
- **Keep it** so the LLM can optionally cite the reason in rationale (minor benefit).

If you remove it: **~15 tokens saved.** Minimal but clean.

---

### 7. Fields the LLM receives but no prompt rule uses

| Field | Used by any rule? | Recommendation |
|---|---|---|
| `market.next_funding` (timestamp) | Not directly — `session_context.hours_until_funding` is the usable version | Remove. Save ~15 tokens. |
| `derivatives_posture.funding.observed_at` | No rule references this | Remove. Save ~15 tokens. |
| `derivatives_posture.funding.next_settlement` | No rule — `hours_until_funding` covers it | Remove. Save ~15 tokens. |
| `derivatives_posture.funding.next`, `.previous`, `.delta` (all null) | No — and null fields carry no information | Remove null funding sub-fields. Save ~20 tokens. |
| `open_interest.contracts` AND `open_interest.base_tokens` | Not referenced by any prompt rule | Either add a rule that uses OI or remove the block. Save ~30 tokens. |
| `market.order_flow.imbalance.weighted` | No rule references weighted imbalance specifically | Could remove, but it's small (~10 tokens). Low priority. |
| `credit_availability.used`, `credit_availability.currency`, `credit_availability.resets_at` | No rule references these specific sub-fields | The credit-conservation flag is pre-computed. The LLM only needs `remaining` and `granted` if you want it to cite the numbers in rationale. Remove `used`, `currency`, `resets_at`. Save ~25 tokens. |
| `execution.enabled` | Execution layer concern | Remove. Save ~5 tokens. |
| `execution.trade_mode` | Execution layer concern | Remove. Save ~5 tokens. |
| `execution.order_type` | The prompt mentions "shift to a limit order" — but this field says "market". The LLM doesn't set order type in its response schema. | Execution layer concern. Remove. Save ~5 tokens. |
| `prompt_version_id` / `prompt_version_name` | Metadata, not used by LLM | Remove from LLM payload (keep in your logging layer). Save ~30 tokens. |
| `notes: null` | Carries no information | Remove when null. Save ~5 tokens. |

**Combined savings: ~180–200 tokens.** Individually small, but they add up and reduce cognitive noise for the LLM.

---

### 8. The L/S RATIO section is still ~200 tokens for a rarely-actionable signal

The L/S ratio instructions cover three scenarios (declining 10+ periods, data absent with negative funding, series stale) across ~200 tokens. In this snapshot — and likely most snapshots — `series_stale: true` with `series_age_hours: 46.4` makes the entire series unusable. The current value alone (0.82) tells the LLM "short-heavy" but the prompt already has this via `derivatives_posture.summary`.

**Recommendation:** Consider pre-computing an `ls_signal` flag in `pre_computed_modifiers`:

```json
{
  "ls_signal": "stale_series",
  "ls_current_value": 0.82,
  "ls_confidence_add": 0.0
}
```

Then collapse the three L/S paragraphs to:

> *"L/S RATIO: if pre_computed_modifiers.ls_signal indicates a squeeze or capitulation setup, add ls_confidence_add. If 'stale_series' or 'absent', rely on ls_current_value for directional bias only. Note staleness in rationale."*

**Savings: ~150 tokens.**

---

### 9. Minor instruction-text tightening

A few passages could be shortened without losing meaning:

**SIZING CONTEXT paragraph** (~120 tokens) says mostly the same thing as the last two sentences of STEP 5. Consider merging:

Current (STEP 5 ending + SIZING CONTEXT):
> *"The execution layer derives position size from your confidence and risk_score: notional = max_safe_notional_usd × confidence × (1 − risk_score). Focus exclusively on direction and signal quality - do NOT output a dollar amount. If context.pre_computed_modifiers.capital_sufficient is false (insufficient free capital), you MUST choose HOLD. SIZING CONTEXT - PRE-LEVERAGE MODE (execution layer owns all arithmetic): context.execution.max_safe_notional_usd is the ceiling for the MARGIN you can commit; context.execution.min_notional_usd is the minimum margin required. These are provided for reference only - the execution layer computes the final margin commitment from your confidence and risk_score; you do NOT output a dollar amount. If context.pre_computed_modifiers.capital_sufficient is false (insufficient free capital), you MUST choose HOLD."*

Proposed replacement (merge into STEP 5):
> *"The execution layer sizes positions from your confidence and risk_score. Do NOT output a dollar amount. context.execution.max_safe_notional_usd and min_notional_usd are reference only."*

**Savings: ~80 tokens.**

---

## Summary of Remaining Savings

| Item | Est. Tokens | Priority |
|---|---|---|
| Move `response_schema` to API parameter (if supported) | ~200 | Medium |
| Remove unreferenced data fields (funding nulls, OI, credit sub-fields, metadata, etc.) | ~200 | Low |
| Flatten `execution` further, remove `live_margin_snapshot` | ~120 | Low |
| Collapse L/S ratio instructions via pre-computed flag | ~150 | Medium |
| Merge/deduplicate SIZING CONTEXT with STEP 5 | ~80 | Low |
| Remove CRITICAL DIRECTION RULE (test first) | ~80 | Low |
| Remove duplicate `capital_sufficient` check | ~25 | Low |
| Remove `min_size` duplication | ~15 | Low |
| **Total** | **~870** | — |

---

## What's Working Well — Keep As-Is

These elements are well-designed and should not be changed:

- **`pre_computed_modifiers` structure** — clean, comprehensive, and properly referenced by the prompt instructions.
- **STEP 2 rewrite** — concise, correctly references the pre-computed penalty values without re-explaining the logic.
- **STEP 1 collapse** — minimal and effective.
- **Series truncation to 5 values** — appropriate for the cross-check purpose.
- **`portfolio_exposure.summary`** — gives the LLM aggregate exposure context without per-symbol noise.
- **Swing arrays without timestamps** — `bar_index` is sufficient for recency, `price` is what the LLM needs for TP/SL.
- **`margin_health.summary`** — human-readable summary is ideal for LLM consumption.
- **Signal hierarchy (STEP 4)** — still clear and well-structured. This is the core of what the LLM should reason about.
- **Archetype framework** — gives the LLM structured templates for ambiguous situations, which is a good use of LLM judgment.
- **Regime rules** — appropriately scoped with clear mappings.

---

## Final Verdict

The prompt has gone from an estimated ~14,000 tokens to roughly ~7,000–8,000 tokens — approximately a **45–50% reduction**. The remaining ~870 tokens of savings are available but represent diminishing returns. The most impactful remaining change is collapsing the L/S ratio instructions via a pre-computed flag (~150 tokens) and moving the `response_schema` to an API parameter (~200 tokens) if your model supports it.

The architecture is now sound: deterministic logic lives in the execution layer, the LLM receives pre-computed flags and focuses on qualitative judgment, and the data payload is lean with minimal duplication. Further optimisation would require a full rewrite of the instruction text into a more compressed format, which risks reducing LLM comprehension and output quality.
````