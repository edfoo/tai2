````markdown
# Review of Updated Prompt (v6 — UP-USDT-SWAP)

## Overall Assessment

This prompt is **production-ready**. All previously identified issues have been addressed, including the critical HTF alignment classifier fix. The data payload is minimal and clean. I have only one minor dead-reference fix in the instructions and one observation about OBV label quality in the pre-computation layer.

---

## Changes Addressed Since Last Review

| Recommendation | Status |
|---|---|
| HTF alignment classifier: DMI-based fallback when EMA200 is null | ✅ Fixed — now correctly `"bearish"` with `"strong (ADX > 30)"` bracket |
| Add `portfolio_overexposed` flag + prompt instruction | ✅ Done — flag + `portfolio_overexposure_pct` in modifiers, instruction in STEP 1 |
| `long_notional: null` → `0.0` | ✅ Fixed |
| Remove empty `long_short_ratio: {}` when data absent | ✅ Removed — `derivatives_posture` no longer contains it |

---

## Remaining Items

### 1. Dead reference in L/S RATIO instruction (Low Priority — Correctness)

The prompt says:

> *"If ls_signal is 'stale_series', 'absent', or 'insufficient_data', use derivatives_posture.long_short_ratio.value for directional bias only."*

But when L/S data is absent, `derivatives_posture.long_short_ratio` no longer exists in the payload (correctly removed). The instruction now points to a missing field.

Since `pre_computed_modifiers.ls_current_value` already carries this value (or `null` when absent), update the instruction to:

> *"If ls_signal is 'stale_series', 'absent', or 'insufficient_data', use pre_computed_modifiers.ls_current_value for directional bias only (null means no L/S data available)."*

**Impact:** Prevents the LLM from looking for a field that may not exist. Minor, since the LLM would likely fall back to the pre-computed value anyway, but cleaner.

---

### 2. OBV label quality concern (Pre-computation Layer, Not Prompt)

The `obv_trend` is labelled `"rising"` but the OBV series tells a different story:

```
[-2446244, -2494334, -2478399, -2484201, -2486081]
```

This series is **net declining** (from −2,446,244 to −2,486,081, a drop of ~40k). The "rising" label with "weak" confidence appears to be a misclassification in the OBV trend computation.

The prompt does handle this gracefully — the instruction says *"If recent values are flat or reversing against the label, treat the label as stale and reduce its weight by half"* — so a capable LLM should catch the mismatch. However, the pre-computation layer should ideally not produce contradictory labels in the first place.

**Recommendation:** Review the OBV trend classifier's logic. A simple slope check over the last 5 values (linear regression or first-minus-last) would catch this case. The label should be `"declining"` or `"flat"` for this series.

---

## Verification: Everything Else Is Clean

| Check | Status |
|---|---|
| HTF classification matches indicators | ✅ Bearish (DI− dominant, ADX > 30, despite null EMA200) |
| Penalty values correct for strong bearish HTF | ✅ −0.30 additive, ×0.7 multiplicative |
| `portfolio_overexposed: true` matches net_pct_of_equity (−105.5%) | ✅ Consistent |
| `order_flow_reliable: false` matches `liquidity_bias: "unreliable"` and null depths | ✅ Consistent |
| `credit_conservation: true` matches remaining/granted ratio (2.80/70 = 4%) | ✅ Correct (< 10%) |
| `cvd_series: []` — prompt handles empty gracefully ("if present") | ✅ No cross-check attempted on empty |
| `ls_signal: "absent"` with `ls_current_value: null` — prompt handles correctly | ✅ Dead reference aside (item 1) |
| No `ofi_ratio_series` — prompt handles absence conditionally | ✅ |
| No `positions`, no `account`, no `risk_locks`, no `execution_feedback` | ✅ All correctly omitted when empty/inactive/null |
| `long_notional: 0.0` instead of `null` | ✅ Fixed |
| `derivatives_posture` has no `long_short_ratio` when absent | ✅ Clean |
| Regime "ranging" + HTF bearish + weak LTF trend = likely HOLD outcome | ✅ Makes sense for this setup |

---

## Summary

| Item | Type | Priority |
|---|---|---|
| Update L/S fallback reference from `derivatives_posture.long_short_ratio.value` to `pre_computed_modifiers.ls_current_value` in prompt | Prompt text fix | Low |
| Review OBV trend classifier (producing "rising" label for declining series) | Pre-computation logic | Medium |

---

## Verdict

The prompt is done. The one remaining prompt-text fix is a single-sentence reference update. The OBV label quality issue is in the pre-computation layer, not the prompt. The architecture — pre-flight checks → pre-computed modifiers → lean data → focused LLM judgment → post-validation — is sound and well-implemented. Ship it.
````