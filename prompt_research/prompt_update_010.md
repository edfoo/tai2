````markdown
# Review of Updated Prompt (v4)

## Overall Assessment

This is a **well-optimised prompt**. The data payload is lean, the instructions properly reference pre-computed values, and the remaining token budget is spent on things that matter (signal interpretation, archetype selection, qualitative judgment). You are at the point of diminishing returns — remaining items are small and individually optional.

---

## Changes Addressed Since Last Review

| Recommendation | Status |
|---|---|
| Collapse L/S ratio instructions via pre-computed `ls_signal` / `ls_confidence_add` | ✅ Done — three paragraphs replaced by one concise block |
| Flatten `execution` object, remove `live_margin_snapshot` | ✅ Done — now just 6 fields + `margin_summary` |
| Remove `min_size` duplication | ✅ Done — only in `symbol_rules` now |
| Remove unreferenced funding sub-fields (`next`, `previous`, `delta`, `observed_at`, `next_settlement`) | ✅ Done — only `current` remains |
| Remove `credit_availability.used`, `.currency`, `.resets_at` | ✅ Done |
| Remove `open_interest` block | ✅ Done |
| Remove `prompt_version_id` / `prompt_version_name` | ✅ Done |
| Remove `notes: null` top-level | ✅ Done |
| Remove `execution.enabled`, `.trade_mode`, `.order_type` | ✅ Done |
| Merge SIZING CONTEXT with STEP 5 | ✅ Done — single paragraph now |
| Remove duplicate `capital_sufficient` check | ✅ Done — appears once in STEP 5 |
| Keep CRITICAL DIRECTION RULE | ✅ Kept (confirmed needed for cheaper models) |
| Keep `response_schema` in prompt body | ✅ Kept (model lacks structured schema support) |

---

## Remaining Items (Diminishing Returns)

### 1. `htf_trend_direction` is redundant with `htf_alignment_class`

In `pre_computed_modifiers`, both fields exist:

```json
"htf_trend_direction": "bearish",
"htf_alignment_class": "bearish"
```

The prompt instructions only reference `htf_alignment_class`. The `htf_trend_direction` field is never mentioned. Remove it.

**Savings: ~10 tokens.**

---

### 2. `execution_feedback` and `execution_feedback_digest` are sent as null

```json
"execution_feedback": null,
"execution_feedback_digest": null
```

When null, these carry zero information. The pre-computed flag `execution_feedback_blocks: false` already tells the LLM there is no blocking feedback. Omit both fields when null.

**Savings: ~15 tokens.**

---

### 3. `pending_orders` can be simplified when empty

Currently:
```json
"pending_orders": {"total": 0, "by_side": {}, "open": []}
```

The pre-computed flag `has_pending_order: false` already communicates this. You could either omit the block entirely when total is 0, or reduce to:
```json
"pending_orders": {"total": 0}
```

**Savings: ~15 tokens.**

---

### 4. `liquidation_clusters: []` — always empty, no prompt rule

This field appears in every snapshot as an empty array and no prompt rule references it. Either remove it until you add a rule that uses liquidation cluster data, or keep it as a placeholder for future use.

**Savings: ~10 tokens.**

---

### 5. `derivatives_posture.long_short_ratio` partially overlaps with `pre_computed_modifiers`

The raw object is still present:
```json
"long_short_ratio": {
  "value": 0.82,
  "period": "5m",
  "series_stale": true,
  "series_age_hours": 46.4,
  "note": "series omitted: >24h old"
}
```

Meanwhile `pre_computed_modifiers` already has:
```json
"ls_signal": "stale_series",
"ls_current_value": 0.82,
"ls_confidence_add": 0.0
```

The prompt instructions now reference `pre_computed_modifiers.ls_signal` and `ls_current_value`. The raw `long_short_ratio` sub-fields (`period`, `series_stale`, `series_age_hours`, `note`) are not referenced by any instruction. You could reduce to:

```json
"long_short_ratio": {"value": 0.82}
```

Or remove it entirely since `ls_current_value` is in `pre_computed_modifiers`.

**Savings: ~40 tokens.**

---

### 6. `derivatives_posture.summary` vs raw fields

The `summary` string (`"funding 0.028%, L/S short-heavy"`) duplicates what the LLM can read from `funding.current` and `long_short_ratio.value`. However, the summary is cheap (~15 tokens) and helps the LLM quickly contextualise without parsing numbers. This is a reasonable trade-off — keep it unless you want maximum compression.

---

### 7. `risk_locks` when inactive

```json
"risk_locks": {"daily_loss_active": false}
```

When false, this is inert. The pre-flight check should prevent the LLM call if it were true. You could omit the block when inactive. But at only ~10 tokens, this is barely worth the conditional logic to exclude it.

---

### 8. One minor instruction observation: Archetype C reference location

The prompt says:

> *"when context.market_signals.funding_archetype_c_eligible is true"*

But this field was moved to `pre_computed_modifiers.archetype_c_eligible` and is no longer in `market_signals`. The data correctly has it in `pre_computed_modifiers`, but the instruction text still points to `market_signals`. Update the reference:

> *"when context.pre_computed_modifiers.archetype_c_eligible is true"*

This is a **correctness issue**, not a token issue — the LLM might look for the field in the wrong location.

---

### 9. `market_signals` could drop `funding_archetype_c_eligible` if still present in schema

Verify that your code no longer emits `market_signals.funding_archetype_c_eligible`. In this snapshot it appears to be correctly absent from `market_signals` — good. Just ensure the instruction text matches (see item 8 above).

---

## Summary

| Item | Tokens | Priority |
|---|---|---|
| Fix Archetype C field reference in instructions (`market_signals` → `pre_computed_modifiers`) | ~0 (text swap) | **High (correctness)** |
| Remove `htf_trend_direction` from modifiers | ~10 | Low |
| Omit null `execution_feedback` / `execution_feedback_digest` | ~15 | Low |
| Simplify empty `pending_orders` | ~15 | Low |
| Remove `liquidation_clusters: []` | ~10 | Low |
| Slim `long_short_ratio` (redundant with `ls_*` modifiers) | ~40 | Low |
| Remove inactive `risk_locks` | ~10 | Low |
| **Total** | **~100** | — |

---

## Verdict

The prompt is in good shape. The only action item I would call important is **item 8** — the Archetype C field reference pointing to the wrong location. Everything else is sub-50-token polish. The architecture is clean: deterministic logic is pre-computed, the LLM focuses on qualitative judgment, and the data payload is minimal with no significant duplication remaining.

The total prompt is now likely in the **~6,000–7,000 token** range, down from an original ~14,000 — roughly a **50–55% reduction**.
````