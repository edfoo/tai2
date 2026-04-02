```markdown
# Remaining Issues & New Observations — FLOKI-USDT-SWAP Prompt Review

## 2.1 — `change_24h` Still Null

```json
"change_24h": null
```

This was flagged previously. It's a trivial computation from candle data and gives the model quick context. Still missing.

**Recommendation:** Compute upstream. From the candles, price went from roughly $0.0000281 → $0.0000266, which is approximately **−5.3%** in the visible window.

---

## 2.2 — OBV Trend "diverging_bullish" — Verify the Label

The label says `obv_trend: "diverging_bullish"`, which by definition means **price falling + OBV rising**.

Let's check:
- **Price:** clearly falling (0.00002809 → 0.00002660) ✅ price is falling
- **OBV series (last 15):** `[-79518, -77515, -75361, -75361, -71752, -69536, -55998, -59869, -65627, -68416, -73388, -58777, -62593, -58268, -58634]`

The OBV series is **noisy and not clearly rising**. It rose from −79,518 to −55,998 (mid-series), then fell back to −73,388, then bounced to −58,268, then dipped slightly to −58,634. The overall trajectory from the start of the series is upward (−79,518 → −58,634), but the recent 5 values show: `[-73388, -58777, -62593, -58268, -58634]` — a zigzag pattern, not a clean rise.

**Assessment:** The label is *plausible* but not strongly confirmed by the last 3-5 values. The model's OBV LABEL VERIFICATION step should catch this and reduce weight by half. The new prompt rules handle this correctly — **the labelling system just needs to be more conservative upstream.**

**Recommendation:** Consider adding a `label_confidence` field to pre-computed signals:
```json
"obv_trend": "diverging_bullish",
"obv_trend_confidence": "weak"
```
This gives the model a head start rather than requiring it to parse 15 numbers.

---

## 2.3 — CVD Label "net_positive_rising" — Looks Correct This Time

- `cvd` = 4,419 (positive) ✅
- `cvd_series` last 30 values: starts around 2,464, rises to ~4,564, dips to ~4,272, returns to 4,419 ✅

The label is defensible here. The series is net positive and has a general upward trend, though with mild consolidation at the end. **Good improvement over the previous prompt.**

---

## 2.4 — Liquidity Bias "ask-heavy" With Null Depths

```json
"bid_depth": null,
"ask_depth": null,
"liquidity_bias": "ask-heavy"
```

The `order_book_note` correctly flags: `"stale: bid_depth was zero at snapshot time"`. However, the system still computed a `liquidity_bias` of `"ask-heavy"` from unreliable data. The prompt's ZERO DEPTH FALLBACK rule should catch this, but ideally:

**Recommendation:** When depth data is stale/null, set `liquidity_bias` to `"unknown"` or `"unreliable"` upstream rather than computing a potentially misleading label. The model now has the rule to discount it, but a clean label avoids any ambiguity:
```json
"liquidity_bias": "unreliable",
"liquidity_bias_reason": "depth data stale at snapshot"
```

---

## 2.5 — Duplicated `response_schema` Still Present

The `response_schema` still appears **twice**:
1. Inside `prompt.response_schema`
2. At the top level of `payload.response_schema`

They are identical. This wastes ~500 tokens per call.

**Recommendation:** Remove one. Keep whichever one the execution layer actually reads.

---

## 2.6 — Duplicated Financial Fields Still Present

These still appear in multiple places:

| Field | Locations |
|---|---|
| `available_margin_usd` | `account.available_equity`, `execution.available_margin_usd`, `margin_health.available_margin_usd`, `margin_health.live_snapshot.available_margin_usd` |
| `account_equity_usd` | `account.account_equity`, `account.total_eq_usd`, `execution.account_equity_usd`, `margin_health.account_equity_usd`, `margin_health.live_snapshot.account_equity_usd` |
| ATR values | `indicators.atr`, `indicators.atr_pct`, `risk_metrics.atr`, `risk_metrics.atr_pct` |

**Recommendation:** This is lower priority since it's mostly a token-efficiency issue, but on a per-call basis across many snapshots, deduplication would meaningfully reduce costs and reduce the chance of subtle inconsistencies between copies.

---

## 2.7 — Volume Series Still Very Long (~200 Values)

`indicators.volume.series` still contains **~200 data points**. The model is not instructed to inspect this series directly (Volume RSI is the derived signal). The candle-level volumes in `history.candles` already provide 50 bars of volume.

**Recommendation:** Trim `volume.series` to match the candle window (50 bars), or remove it entirely since the candle data already contains per-bar volume. Savings: ~150 tokens.

---

## 2.8 — `isolated_margin_seed_usd: 500` vs. Account Equity ~$100

This mismatch persists from the previous prompt. The seed is 5× the account equity.

**Recommendation:** Low priority since the model is told not to compute dollar amounts, but adding a note helps:
```json
"isolated_margin_seed_usd": 500.0,
"isolated_margin_seed_note": "capped at available_margin_usd if account insufficient"
```

---

## 2.9 — No Macro/BTC Context

Still absent. For an altcoin like FLOKI, BTC correlation is very high.

**Recommendation (unchanged):**
```json
"macro_context": {
  "btc_trend": "ranging",
  "btc_change_4h": -0.3,
  "correlation_to_btc_30d": 0.82
}
```

This is medium-priority — not every model will use it well, but for a hedge-fund-trader persona, ignoring macro is unrealistic.

---

## 2.10 — Derivatives Summary Says "funding −0.028%" but Actual is −0.0277%

```json
"funding_rate": -0.0002773876984522,
"summary": "funding -0.028%, L/S long-heavy"
```

The summary rounds −0.02774% to −0.028%, which is fine for display, but note that the prompt's Archetype C threshold is `< −0.05%`. The actual funding of **−0.0277%** is below −0.05%, which **qualifies for Archetype C**. The summary doesn't highlight this proximity to a decision threshold.

**Recommendation:** Add a flag in the derivatives posture:
```json
"funding_archetype_c_eligible": true,
"funding_note": "funding −0.028% qualifies for Archetype C (threshold: −0.05%)"
```

This reduces the model's arithmetic burden for a critical decision gate.

---

## 2.11 — New: `ofi_ratio_series` Undocumented

```json
"ofi_ratio_series": [1.573, 2.348, 0.0, 0.0, 0.0, 1.344, 1.851, 0.0]
```

This field is new but **never referenced in the prompt rules**. The model has no instruction on what OFI (Order Flow Imbalance) ratio means or how to use it.

**Recommendation:** Either:
- Add a rule: *"ofi_ratio_series reflects order-flow imbalance momentum. Values > 1.5 suggest aggressive buying; values near 0 suggest inactive or balanced flow. Use within the Order Flow tier of the signal hierarchy."*
- Or remove it to avoid noise.

---

## 2.12 — Session Context Could Be Richer

The current addition is good but minimal:
```json
"session_context": {"utc_hour": 9, "active_session": "EU"}
```

**Recommendation:** Optionally enrich with:
```json
"session_context": {
  "utc_hour": 9,
  "active_session": "EU_open",
  "expected_volume_profile": "increasing",
  "hours_until_funding": 8.2,
  "hours_until_us_open": 4.5
}
```

The `hours_until_funding` is particularly relevant given the deeply negative funding rate and Archetype C logic.

---

## Signal Coherence Check for This Specific Snapshot

Verification of whether the data tells a consistent story for the model:

| Signal | Value | Direction |
|---|---|---|
| LTF ADX | 34.2, DI− dominant | Bearish ✅ |
| LTF EMAs | Price below EMA50 below EMA200 | Bearish ✅ |
| HTF ADX | 26.7, DI− dominant | Bearish ✅ |
| HTF EMAs | EMA50 < EMA200 | Bearish ✅ |
| Market regime | `trending_down` | Bearish ✅ |
| RSI (LTF) | 35.7 | Oversold-leaning |
| RSI (HTF) | 29.4 | Oversold |
| OBV trend | `diverging_bullish` | Bullish counter-signal ⚠️ |
| CVD trend | `net_positive_rising` | Bullish counter-signal ⚠️ |
| Funding | −0.028% (deeply negative) | Squeeze risk → Bullish ⚠️ |
| L/S ratio | 1.22, mild oscillation | Neutral/long-heavy |
| CMF | −0.03 (near zero) | Slight sell pressure |
| Price vs VWAP | Below | Bearish ✅ |

**Assessment:** This is a much cleaner snapshot than the previous PUMP token. The dominant signal is bearish (trend, EMAs, ADX, regime), with counter-signals from volume accumulation (OBV, CVD) and deeply negative funding suggesting a potential squeeze setup. This creates a genuine analytical tension that the model should navigate well given the prompt's Archetype C and signal hierarchy rules.

The model should likely conclude: **HOLD or cautious BUY under Archetype C**, depending on how it weighs the HTF contradiction penalties against the funding/OBV squeeze signals. This is a reasonable test of the prompt's decision framework.

---

## Summary Scorecard

| Category | Previous Prompt | Current Prompt | Notes |
|---|---|---|---|
| Structural clarity | 6/10 | 8.5/10 | Precedence clause, penalty arithmetic, regime clarification |
| Data integrity | 4/10 | 7.5/10 | CVD verification added, OBV sanity check, depth fallback |
| Signal coherence | 5/10 | 8/10 | Labels are more defensible; CVD matches data this time |
| Token efficiency | 5/10 | 5.5/10 | Still duplicated schema, long volume series, duplicate fields |
| Missing context | 5/10 | 6.5/10 | Session added; still missing macro, change_24h, funding timing |
| Edge case handling | 6/10 | 9/10 | Null execution feedback, zero depth, slippage absent, breakdown staleness |

**Overall: strong improvement from ~5.2/10 to ~7.5/10.** The remaining gaps are mostly optimization (token efficiency, richer context) rather than correctness issues.

---

## Priority Remaining Actions

| Priority | Item | Effort |
|---|---|---|
| 🟠 High | Remove duplicated `response_schema` | Trivial |
| 🟠 High | Set `liquidity_bias` to `"unreliable"` when depth is null upstream | Small code change |
| 🟠 High | Add OFI ratio rule or remove the field | One line in prompt or data layer |
| 🟡 Medium | Compute `change_24h` upstream | Trivial |
| 🟡 Medium | Add `label_confidence` to pre-computed signals | Small enrichment |
| 🟡 Medium | Add `hours_until_funding` to session context | Small enrichment |
| 🟡 Medium | Trim `volume.series` to 50 bars | Trivial |
| 🟢 Low | Add macro/BTC context | Moderate (new data source) |
| 🟢 Low | Deduplicate financial fields | Moderate refactor |
| 🟢 Low | Add `funding_archetype_c_eligible` flag | Trivial |
```

Here is the raw markdown file containing all the remaining issues and new observations from the FLOKI-USDT-SWAP prompt review.