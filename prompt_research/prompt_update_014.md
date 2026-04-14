````markdown
# Review of Updated Prompt (v7 — SENT-USDT-SWAP)

## Overall Assessment

The prompt text is solid — the L/S reference fix from the last review has been applied. However, this symbol reveals a **third manifestation of the HTF alignment classifier issue**, this time in the opposite direction. The classifier also has an EMA convergence note that contradicts its own classification.

---

## Changes Addressed Since Last Review

| Recommendation | Status |
|---|---|
| Update L/S fallback reference to `pre_computed_modifiers.ls_current_value` | ✅ Done |

---

## Issues Found

### 1. HTF Alignment Classified as "Bullish" — Should Be "Bearish" or "Neutral" (High Priority)

This is the same class of bug seen in previous reviews, now manifesting as the **inverse error**: EMA ordering overrides a strong contrary DMI signal.

| HTF Indicator | Value | Implication |
|---|---|---|
| EMA50 | 0.016193 | Above EMA200 → traditionally bullish ordering |
| EMA200 | 0.016057 | — |
| EMA gap | 0.85% | Narrow, nearly crossed |
| EMA convergence | Narrowing | EMA50 falling toward EMA200 |
| Convergence note | *"bullish cross **may reverse soon**"* | System already detects imminent bearish crossover |
| ADX | 32.46 | **Strong** trend |
| DI+ | 12.67 | Weak |
| DI− | 24.26 | **Dominant** (nearly 2× DI+) |
| RSI | 39.93 | Bearish |

The ADX + DMI is unambiguously bearish: DI− dominates by a DI gap of 11.6 points with ADX > 30 confirming a strong trend. The EMA50 is still technically above EMA200 but the system's own convergence note says the bullish cross "may reverse soon." Despite all of this, the classifier outputs `"bullish"`.

**Consequences for this snapshot:**

| LLM Action | Penalty Applied | Correct Penalty |
|---|---|---|
| BUY | 0.0 (aligned with "bullish") | −0.30, ×0.7 (contradicts strong bearish DMI) |
| SELL | −0.30, ×0.7 (contradicts "bullish") | 0.0 (aligned with bearish DMI) |

A SELL is the natural trade here (bearish HTF DMI, price below VWAP, declining CVD, OBV diverging bearish, ranging regime) but faces the harshest possible contradiction penalty. Meanwhile a BUY faces no HTF penalty despite overwhelming bearish momentum.

**Recommendation — Classification Logic Overhaul:**

The classifier needs to reconcile EMA ordering with DMI direction. Proposed decision matrix:

| EMA Ordering | DMI Direction (ADX > 25) | Gap & Convergence | Classification |
|---|---|---|---|
| EMA50 > EMA200 | DI+ dominant | Any | **Bullish** |
| EMA50 > EMA200 | DI− dominant | Gap > 2% or widening | **Neutral** (conflicting signals) |
| EMA50 > EMA200 | DI− dominant | Gap < 2% and narrowing | **Bearish** (trend has flipped, EMA is lagging) |
| EMA50 < EMA200 | DI+ dominant | Gap > 2% or widening | **Neutral** |
| EMA50 < EMA200 | DI+ dominant | Gap < 2% and narrowing | **Bullish** (trend has flipped, EMA is lagging) |
| EMA50 < EMA200 | DI− dominant | Any | **Bearish** |
| Any | ADX < 25 (no clear trend) | Any | **Neutral** |
| EMA200 = null | DI dominant (gap > 5) | — | Use DMI direction |
| EMA200 = null | No clear DI winner | — | **Neutral** |

The key insight: **EMA crossovers are lagging indicators; DMI is leading.** When DMI and EMA conflict and the EMA gap is small + narrowing, DMI should take precedence because the EMA will catch up.

For this snapshot: EMA50 > EMA200 BUT DI− dominant, gap < 2%, narrowing → **Bearish**. This matches what the convergence note already detects.

---

### 2. OBV "diverging_bearish" Label Fails Its Own Sanity Check (Pre-computation Layer)

| Condition | Required for "diverging_bearish" | Actual |
|---|---|---|
| Price direction | Rising | **Falling** (−6.38% 24h, below VWAP, declining swing lows) |
| OBV direction | Falling | **Mixed** — series: [−301k, −303k, −310k, −305k, −302k] — dipped then recovered |

"Diverging bearish" requires price rising while OBV falls. Here price is falling. The label does not match its own definition.

The prompt's DIVERGENCE LABEL SANITY CHECK instruction will catch this and tell the LLM to reclassify as "confirming." So this is not a prompt-text issue — it is handled gracefully. However, the pre-computation layer is producing labels that fail their own definitions, which wastes LLM reasoning effort on reclassification. Combined with the previous OBV "rising" mislabel for UP-USDT-SWAP, this suggests the OBV trend classifier needs review.

**Recommendation:** Add a simple sanity check in the pre-computation layer that validates divergence labels against price direction before emitting them. If the divergence definition is not met, emit "confirming" or "declining" instead.

---

### 3. EMA Convergence Note Contradicts Classification

The system generates:

```json
"ema_convergence": {
  "gap_pct": 0.85,
  "narrowing": true,
  "note": "EMA50 rapidly approaching EMA200; bullish cross may reverse soon"
}
```

This note explicitly acknowledges the bullish cross is about to fail. Yet `htf_alignment_class` is `"bullish"`. The system is producing contradictory assessments in different fields.

**Recommendation:** Feed the convergence data into the alignment classifier (as proposed in item 1). If `narrowing: true` + `gap_pct < 1.0` + DMI contradicts EMA ordering, the classifier should downgrade or flip the classification. The convergence note is already detecting the right thing — it just isn't connected to the classification logic.

---

### 4. Minor: `cvd_trend_confidence: "unknown"` with actual CVD data present

The CVD series has 5 values: `[-1833, -1809, -1790, -1850, -1880]`. This is sufficient data to compute confidence — the series clearly shows a declining pattern (with a brief recovery that reversed). Yet `cvd_trend_confidence` is `"unknown"`. 

Compare with the UP-USDT-SWAP snapshot where `cvd_series: []` (empty) appropriately had `"unknown"` confidence. Here the data exists but the classifier still says "unknown."

**Recommendation:** Review the CVD confidence classifier. If the series has ≥ 3 values, a confidence level should be computable. `"unknown"` should be reserved for truly missing or insufficient data.

---

## Verification: Prompt Text

| Check | Status |
|---|---|
| L/S fallback references `pre_computed_modifiers.ls_current_value` | ✅ Fixed |
| `portfolio_overexposed: true` matches −108.5% | ✅ Consistent |
| `order_flow_reliable: false` matches null depths + slippage_note | ✅ |
| `credit_conservation: true` matches 2.42/70 = 3.5% | ✅ |
| `ls_signal: "stale_series"` with `ls_current_value: 1.23` | ✅ Consistent with `long_short_ratio.value` |
| No dead references in prompt instructions | ✅ Clean |

---

## Summary

| Item | Type | Priority |
|---|---|---|
| HTF classifier: EMA ordering overrides contrary strong DMI in narrow/narrowing gap scenarios | Pre-computation logic | **High** (3rd occurrence of this class of bug) |
| OBV trend classifier: "diverging_bearish" label fails its own definition (price is falling, not rising) | Pre-computation logic | **Medium** (2nd OBV mislabel observed) |
| EMA convergence note not connected to HTF classification | Pre-computation logic | Medium (feeds into item 1) |
| CVD confidence "unknown" despite 5 data points | Pre-computation logic | Low |

---

## Pattern Summary Across All Reviews

The pre-computation layer has been refined well for prompt structure and data payload. The remaining issues are concentrated in **three classifiers**:

| Classifier | Bug Pattern | Occurrences |
|---|---|---|
| **HTF alignment** | Over-reliance on EMA ordering; ignores DMI, convergence, and missing EMA200 | 3 (TRIA transition, UP null EMA200, SENT EMA/DMI conflict) |
| **OBV trend** | Labels inconsistent with actual series direction or divergence definitions | 2 (UP "rising" on declining series, SENT "diverging_bearish" with falling price) |
| **CVD confidence** | Returns "unknown" when sufficient data exists | 1 (SENT has 5 values but "unknown") |

Fixing these three classifiers would have more impact on trade quality than any further prompt optimisation. The prompt text itself is production-ready.
````