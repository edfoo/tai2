```markdown
# Remaining Issues — RAVE-USDT-SWAP (Corrected Second Snapshot)

## 1 — New Change: System Prompt JSON Instruction

**Added sentence:**
```
Respond strictly as JSON matching 'response_schema'.
```

**Assessment:** ✅ Good addition. Reinforces output format compliance for non-OpenAI models that may drift into conversational output or add markdown wrappers.

**Minor suggestion:** Could be strengthened slightly:
> *"Respond with ONLY a valid JSON object matching 'response_schema'. Do not include any text, markdown, or explanation outside the JSON."*

This guards against models that prepend "Here is my analysis:" or wrap the JSON in ` ```json ` fences.

---

## 2 — Duplicated `response_schema` Still Present (Recurring ×4)

The `response_schema` still appears **twice**:
1. Inside `prompt.response_schema`
2. At the top level of `payload.response_schema`

They are identical. ~500 tokens wasted per call.

**Recommendation:** Remove one instance. Keep whichever one the execution layer actually reads. This has been flagged in every review since the first.

---

## 3 — `change_24h: 4.31` Units Unclear (Recurring ×3)

```json
"change_24h": 4.31
```

Is this +4.31% or something else? Given the massive downtrend from ~$0.27 to ~$0.228 followed by a recovery to $0.249, a +4.31% 24h change is plausible if the comparison point was ~$0.239 yesterday — but the units are never defined.

**Recommendation:**
1. Rename to `change_24h_pct` or add a companion note:
   ```json
   "change_24h_pct": 4.31,
   "change_24h_note": "percentage vs 24h ago"
   ```
2. Verify the computation is correct.

---

## 4 — CVD Label "net_negative_recovering" Contradicts Recent Series

```json
"cvd_trend": "net_negative_recovering"
```

CVD series (last 30 values):
```
[-927, -924, -913, -874, -857, -854, -850, -844, -842, -837,
 -832, -827, -825, -824, -823, -803, -806, -809, -789, -794,
 -798, -821, -822, -864, -863, -867, -874, -895, -900, -897]
```

**Analysis:**
- All values are **negative** ✅ (net negative confirmed)
- The series starts at −927, recovers to −789 (mid-series), then **declines again** to −900, with a tiny uptick to −897
- The last ~10 values show: `[-821, -822, -864, -863, -867, -874, -895, -900, -897]` — this is **declining**, not recovering

**Assessment:** The label `"net_negative_recovering"` is **misleading for the recent window**. The recovery phase ended around value −789; the series has since resumed declining. A more accurate label would be `"net_negative_declining"` or `"net_negative_stalled"`.

The prompt's CVD LABEL VERIFICATION rule should catch this, but the upstream labelling logic needs improvement.

**Recommendation:** Fix the CVD labelling logic to use a recent window (last 10-15 values) for the directional qualifier rather than the full series. Also add a `cvd_trend_confidence` field:
```json
"cvd_trend": "net_negative_declining",
"cvd_trend_confidence": "moderate"
```

---

## 5 — No `cvd_trend_confidence` Field

OBV has `obv_trend_confidence: "moderate"` but CVD has no equivalent. The model must manually inspect the 30-value CVD series to assess label quality.

**Recommendation:** Add `cvd_trend_confidence` analogous to `obv_trend_confidence`:
```json
"cvd_trend_confidence": "weak"
```

This pre-computes the label quality and reduces the model's analytical burden.

---

## 6 — `portfolio_heatmap` Duplicates `portfolio_exposure.heatmap` (Recurring ×3)

```json
"portfolio_exposure": {
  "heatmap": [...]
},
"portfolio_heatmap": [...]  // identical
```

**Recommendation:** Remove `portfolio_heatmap` top-level field. Reference via `portfolio_exposure.heatmap` only.

---

## 7 — `volume.series` Duplicates Candle Volume Data (Recurring ×3)

`indicators.volume.series` (50 values) is identical to the volume column in `history.candles` (50 candles). The model can extract per-bar volume from candles if needed.

**Recommendation:** Remove `indicators.volume.series` entirely. Keep `volume.last` and `volume.average` as the summary. Savings: ~200 tokens.

---

## 8 — Duplicated Financial Fields (Recurring ×4)

Still present in multiple locations:

| Field | Approximate Copies |
|---|---|
| `available_margin_usd` | 4 locations |
| `account_equity_usd` | 5 locations |
| ATR / ATR% | 2 locations (`indicators` + `risk_metrics`) |
| Volume last/average | 2 locations (`indicators.volume` + `liquidity_context.volume`) |

**Recommendation:** Consolidate. Keep `execution.margin_health` as the single source for capital fields, `indicators` for ATR, and `liquidity_context` for volume summary.

---

## 9 — HTF DI Ambiguity Rule Missing From Prompt (Recurring ×2)

HTF indicators show `di_plus: 26.8` vs `di_minus: 20.4` — spread of 6.4, which clears the suggested |DI+ − DI−| < 5 threshold. So not triggered for this snapshot, but the rule should exist for future cases.

**Recommendation:** Add to Step 2:
```
HTF DI AMBIGUITY: if HTF ADX > 25 but |di_plus − di_minus| < 5, treat the
HTF as weak/neutral regardless of ADX value — the trend exists but has no
clear directional bias. Apply the −0.15 weak/neutral penalty.
```

---

## 10 — Credit Availability Running Low (Recurring ×2)

```json
"credit_availability": {
  "remaining": 3.60,
  "granted": 60.0,
  "used": 56.40
}
```

Only **$3.60 of $60** remaining (~6%). The prompt mentions "fee/credit depletion" as a reason to HOLD but doesn't define a threshold.

**Recommendation:** Add a prompt rule:
```
CREDIT CONSERVATION: if credit_availability.remaining < 10% of granted,
add +0.10 to risk_score to reduce position sizing and conserve remaining
credit. Note 'credit conservation mode' in rationale.
```

Or add context enrichment:
```json
"credit_availability": {
  "remaining": 3.60,
  "granted": 60.0,
  "used": 56.40,
  "low_credit_warning": true
}
```

---

## 11 — Spike Recency Check Missing From Prompt (Recurring ×2)

The last 8 candles show a ~8.5% rally from $0.229 → $0.249. No prompt rule addresses entering after a sharp recent move on declining volume.

**Recommendation:** Add to the prompt:
```
SPIKE RECENCY CHECK: if the last 3 candles show a cumulative move > 2× ATR
on declining volume in the most recent bar, increase risk_score by +0.15 and
prefer a limit order at the nearest support rather than a market order. Note
the spike recency adjustment in rationale.
```

---

## 12 — No Macro/BTC Context (Recurring ×5)

Still absent. For a high-beta altcoin like RAVE with 1.3% ATR on 15m candles, BTC correlation matters significantly.

**Recommendation:**
```json
"macro_context": {
  "btc_trend": "ranging",
  "btc_change_1h": -0.15,
  "btc_change_4h": -0.3,
  "correlation_to_btc_30d": null,
  "note": "RAVE too new for 30d correlation; assume high beta"
}
```

---

## 13 — `isolated_margin_seed_usd: 500` vs. Account Equity ~$100 (Recurring ×4)

The seed exceeds the account by 5×.

**Recommendation:** Add clarifying note or cap the value:
```json
"isolated_margin_seed_usd": 500.0,
"isolated_margin_seed_note": "capped at available_margin_usd if account insufficient"
```

---

## 14 — `trend_confirmation.summary` Doesn't Flag LTF/HTF Conflict (Recurring ×2)

```
"ADX 37.3 (trending), +DI dominance, EMA stack bearish, price above EMA50"
```

This is technically accurate but doesn't explicitly flag the directional conflict between short-term momentum (bullish) and long-term structure (bearish).

**Recommendation:** Enhance to:
```
"ADX 37.3 (trending), +DI dominance, EMA stack bearish BUT price above EMA50 — LTF/HTF directional conflict"
```

---

## Priority Actions Summary

| Priority | Item | Effort | Times Flagged |
|---|---|---|---|
| 🟠 High | Remove duplicated `response_schema` | Trivial | 4 |
| 🟠 High | Fix CVD labelling logic (label contradicts recent series) | Small code fix | **New** |
| 🟠 High | Add `cvd_trend_confidence` field | Small enrichment | 2 |
| 🟠 High | Clarify `change_24h` units | Trivial | 3 |
| 🟡 Medium | Remove `portfolio_heatmap` duplicate | Trivial | 3 |
| 🟡 Medium | Remove `indicators.volume.series` duplicate | Trivial | 3 |
| 🟡 Medium | Add HTF DI ambiguity rule to prompt | One line | 2 |
| 🟡 Medium | Add credit conservation rule | Small prompt addition | 2 |
| 🟡 Medium | Add spike recency check rule | Small prompt addition | 2 |
| 🟡 Medium | Deduplicate financial fields | Moderate refactor | 4 |
| 🟢 Low | Add macro/BTC context | Moderate (new data source) | 5 |
| 🟢 Low | Cap `isolated_margin_seed_usd` or add note | Trivial | 4 |
| 🟢 Low | Enhance `trend_confirmation.summary` for conflicts | Small enrichment | 2 |
| 🟢 Low | Strengthen system prompt JSON instruction | Trivial | **New** |
```