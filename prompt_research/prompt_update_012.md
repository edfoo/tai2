````markdown
# Review of Updated Prompt (v5 — UP-USDT-SWAP)

## Overall Assessment

The prompt structure and data payload remain well-optimised. This different token reveals a few **pre-computation logic issues** that would not have been visible with the TRIA snapshots. The prompt text itself is unchanged and needs no further modification.

---

## Changes Since Last Review (Cleanup Applied)

| Recommendation | Status |
|---|---|
| Remove `market.funding_rate` (duplicate of `derivatives_posture.funding.current`) | ✅ Removed |
| Remove `account.quote_currency` | ✅ Removed (entire `account` block absent) |
| Remove `positions: []` when empty | ✅ Removed entirely |
| Remove `market.bid` / `market.ask` | ✅ Removed |
| Remove `generated_at` | ✅ Removed |

---

## Issues Revealed by This Token

### 1. HTF Alignment Classification Is Wrong (High Priority — Trade Quality)

This is the same class of issue flagged in the TRIA review, manifesting differently.

| HTF Indicator | Value | Implication |
|---|---|---|
| ADX | 33.82 | **Strong** trend |
| DI+ | 13.89 | Weak |
| DI− | 23.12 | **Dominant** |
| DI gap | 9.24 | Meaningful bearish bias |
| EMA50 | 0.21156 | Present |
| EMA200 | `null` | **Missing** |
| RSI | 36.14 | Bearish |

The `htf_alignment_class` is set to `"neutral"` with `htf_contradict_bracket: "weak/neutral ADX"`. This is incorrect. The HTF is showing a **strong bearish trend** — ADX > 30 with DI− clearly dominating. The classification has likely fallen back to "neutral" because `ema_200` is `null`, preventing the EMA crossover check.

**Consequences:**

- A BUY receives only the −0.15 neutral penalty instead of the full −0.30 additive / ×0.7 multiplicative contradiction penalty it should face against a strong bearish HTF.
- A SELL receives a −0.15 penalty instead of 0.0 (aligned with bearish HTF).
- The LLM is being told the HTF is neutral when the DMI/ADX clearly says otherwise.

**Recommendation:** The classification logic needs a fallback for missing EMA200:

- If EMA200 is `null` **but** ADX > 25 with a clear DI imbalance (|DI+ − DI−| > 5), classify based on DMI alone:
  - DI+ dominates → `"bullish"`
  - DI− dominates → `"bearish"`
- Only fall back to `"neutral"` when **both** EMA ordering is ambiguous **and** DMI is inconclusive.
- If EMA200 is `null` and ADX < 20, `"neutral"` / `"unavailable"` is appropriate.

For this snapshot the correct classification would be:

```json
{
  "htf_alignment_class": "bearish",
  "htf_align_penalty": 0.0,
  "htf_contradict_additive": -0.30,
  "htf_contradict_multiplicative": 0.7,
  "htf_contradict_bracket": "strong (ADX > 30)"
}
```

---

### 2. `cvd_series` has only 1 value

```json
"cvd_series": [59.0]
```

The prompt instructs the LLM to *"cross-check against the last 5 values"*. With only 1 value, the LLM cannot assess slope or trend direction. This is not a prompt bug — it is correctly handled by the conditional language ("if present"). However, the `cvd_trend` is `"unknown"` and `cvd_trend_confidence` is `"unknown"`, which is the correct classification for insufficient data.

**No action needed** — the prompt handles this gracefully. But consider adding a `cvd_data_sufficient: false` flag to `pre_computed_modifiers` if you want to save the LLM from attempting to interpret a single-value series. Very low priority.

---

### 3. `long_short_ratio` is an empty object `{}`

```json
"long_short_ratio": {}
```

The pre-computed modifiers correctly set `ls_signal: "absent"` and `ls_current_value: null`. The prompt handles "absent" correctly. However, the empty object `{}` means `derivatives_posture.long_short_ratio.value` (referenced by the prompt for directional bias fallback) will be `undefined`/missing. The prompt says:

> *"If ls_signal is 'stale_series', 'absent', or 'insufficient_data', use derivatives_posture.long_short_ratio.value for directional bias only."*

Since `.value` doesn't exist in `{}`, the LLM might hallucinate a value or throw a rationale error. Two options:

- **Option A:** When L/S data is absent, omit `long_short_ratio` entirely from `derivatives_posture` (the pre-computed flag handles everything).
- **Option B:** Send `"long_short_ratio": {"value": null}` so the LLM sees an explicit null and knows not to use it.

**Recommendation:** Option A is cleaner — remove the key entirely when absent. Saves ~10 tokens and avoids ambiguity.

---

### 4. `portfolio_exposure.long_notional` is `null` instead of 0

```json
"long_notional": null,
"short_notional": 93.8408,
"net_exposure": -93.8408,
"net_pct_of_equity": -106.25,
"summary": "Short $94, Net -106.3% of equity"
```

The `null` carries no information — this should be `0` or omitted. The `summary` string already captures the exposure accurately. Minor, but `null` can confuse cheaper models (they might interpret it as "unknown" rather than "zero").

**Recommendation:** Send `0` instead of `null` for zero-valued notional fields.

---

### 5. `ofi_ratio_series` is absent

The prompt says *"if context.market.order_flow.ofi_ratio_series is present"* — correctly conditional. No issue here — just confirming the prompt handles missing OFI data gracefully for lower-liquidity tokens.

---

### 6. Net portfolio exposure exceeds 100% of equity

```json
"net_pct_of_equity": -106.25
```

This means the account is short ~106% of equity. No prompt rule currently flags over-exposure as a risk factor. Given that `max_position_pct` is 0.5 (50%), the existing portfolio is already well beyond this cap. Adding a new TRIA/UP position would push total exposure further.

This is not a prompt-text issue, but you might consider:

- Adding a `portfolio_exposure_pct` check to the pre-flight layer: if `abs(net_pct_of_equity)` exceeds some threshold (e.g., 120%), either block the call or add a `portfolio_overexposed: true` flag with an associated `risk_score` add.
- Alternatively, add a brief prompt instruction: *"If portfolio_exposure.net_pct_of_equity exceeds ±100%, add +0.10 to risk_score and note portfolio concentration risk."*

**Priority:** Medium. This is a risk management gap rather than a prompt optimisation issue.

---

### 7. HTF `ema_200: null` — prompt should account for partial HTF data

The prompt says:

> *"FALLBACK: if context.pre_computed_modifiers.htf_available is false... apply a flat −0.20 penalty and skip further HTF analysis."*

But here `htf_available` is `true` because some HTF data exists (ADX, EMA50, Bollinger, RSI, VWAP are all present — only EMA200 is null). The issue is the classification logic (item 1 above), not the prompt text. However, you might consider adding a `htf_data_partial: true` flag when key HTF fields are null, so the LLM can note data limitations in its rationale.

**Priority:** Low — fixing item 1 (the classification logic) resolves the material impact.

---

## Summary

| Item | Type | Priority |
|---|---|---|
| HTF alignment classification wrong when EMA200 is null but DMI is clear | Pre-computation logic | **High** |
| Empty `long_short_ratio: {}` leaves `.value` undefined | Data shape | Medium |
| `long_notional: null` instead of `0` | Data quality | Low |
| Portfolio over-exposure not flagged | Risk management gap | Medium |
| `cvd_series` with 1 value | Gracefully handled | None needed |
| Missing `ofi_ratio_series` | Gracefully handled | None needed |

---

## Verdict

The prompt text is stable and needs no further changes. The issues are all in the **pre-computation layer** — specifically the HTF alignment classifier, which needs a DMI-based fallback when EMA200 is unavailable. This is the second time this class of bug has appeared (TRIA had an EMA crossover transition issue; UP has a missing EMA200 issue), confirming that the classifier's EMA dependency is too rigid. Fixing this one piece of logic will meaningfully improve trade quality across tokens with incomplete HTF indicator coverage.
````