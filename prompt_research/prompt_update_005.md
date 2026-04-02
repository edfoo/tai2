````
```markdown
# Analysis — RAVE-USDT-SWAP (Snapshot 4, ~45 Minutes Later)

This is a genuinely new snapshot. Price has pulled back from ~$0.271 to ~$0.265 after spiking to $0.273. Several previously recommended improvements have been implemented, but new data integrity issues have appeared.

---

## ✅ New Improvements Successfully Implemented

| Recommendation | Status | Notes |
|---|---|---|
| L/S ratio staleness detection | ✅ Added | `"series_stale": true, "series_age_hours": 46.4` |
| L/S SERIES STALE rule in prompt | ✅ Added | New paragraph handles stale L/S correctly |
| SWING EXHAUSTION rule in prompt | ✅ Added | New TP methodology for exhausted swing levels |
| `symbol_rules` partially restored | ⚠️ Partial | Present but only contains `min_size` — missing `lot_size` and `tick_size` |
| Credit granted increased | ✅ Changed | $60 → $70 granted; remaining $10.55 (15.1%) — above 10% threshold now |
| `low_credit_warning` responsive | ✅ Working | Correctly set to `false` since remaining > 10% |
| OBV/CMF/volume removed from `liquidity_context` | ✅ Partially cleaned | These duplicate fields are no longer present |

---

## 🔴 Critical Issues

### 8.1 — `ask_depth: 0` and `cvd_series: []` — Severely Degraded Order Flow Data

```json
"bid_depth": 7.0,
"ask_depth": 0,
"cvd_series": [],
"cvd": 0.0,
"imbalance": {"net": 7.0, "ratio": null, "weighted": 1.81108}
```

This is the worst order flow data quality seen across all snapshots:
- `ask_depth` is 0 — triggers ZERO DEPTH FALLBACK (−0.05 confidence)
- `cvd_series` is completely empty — no CVD verification possible
- `cvd` is 0.0 which is almost certainly wrong given the massive volume
- `imbalance.ratio` is null
- `bid_depth` is only 7 contracts — essentially no meaningful depth on either side

Combined with:
```json
"cvd_trend": "unknown",
"cvd_trend_confidence": "unknown"
```

This is internally consistent (both unknown when no data) — an improvement over last snapshot's `flat_stable` + `unknown` inconsistency. But the complete absence of order flow data severely limits the model's analysis.

**Impact:** The model loses the entire Order Flow tier (tier 4) and most of the CVD component of the Volume/Participation tier (tier 2). Combined penalties: −0.05 (zero depth) + −0.05 (CVD unknown) = −0.10 confidence.

**Recommendation:**
1. Add an `order_book_note` when depth data is this degraded:
   ```json
   "order_book_note": "stale: ask_depth was zero at snapshot time, bid_depth near-zero (7 contracts)"
   ```
2. Consider adding a circuit breaker: if both bid_depth < 50 AND ask_depth < 50, flag the entire order flow block as unreliable rather than letting individual rules catch pieces.

### 8.2 — `liquidity_bias: "bid-supported"` with ask_depth = 0 (Recurring ×4)

```json
"bid_depth": 7.0,
"ask_depth": 0,
"liquidity_bias": "bid-supported"
```

With 7 contracts on the bid and 0 on the ask, there is no meaningful liquidity on either side. Labelling this as "bid-supported" is technically not wrong (bids > asks) but is deeply misleading — 7 contracts is effectively zero liquidity.

This is the **4th time** the `liquidity_bias` labelling has been flagged. The logic clearly needs a minimum-depth threshold before making any bias claim:

```python
MIN_DEPTH_THRESHOLD = 50  # contracts

if bid_depth < MIN_DEPTH_THRESHOLD and ask_depth < MIN_DEPTH_THRESHOLD:
    liquidity_bias = "thin"
elif bid_depth > ask_depth * 1.2:
    liquidity_bias = "bid-supported"
elif ask_depth > bid_depth * 1.2:
    liquidity_bias = "ask-heavy"
else:
    liquidity_bias = "balanced"
```

### 8.3 — `estimated_slippage_bps: null` Again

```json
"estimated_slippage_bps": null
```

This was fixed in the previous snapshot (12.04 bps, then 16.61 bps) but has regressed. With zero ask depth, the system can't compute slippage — which is understandable — but the SLIPPAGE ABSENT RULE will force the model to estimate from `spread_pct × 2 = 0.083 bps`, which massively underestimates actual slippage in a zero-depth book.

**Recommendation:** When depth data is degraded, provide a conservative floor estimate:
```json
"estimated_slippage_bps": null,
"slippage_note": "cannot estimate: ask_depth is 0; assume elevated slippage risk"
```

Or better yet, set a high conservative value rather than null:
```json
"estimated_slippage_bps": 50.0,
"slippage_note": "elevated estimate due to thin order book"
```

---

## 🟠 High Priority Issues

### 8.4 — `symbol_rules` Still Incomplete

```json
"symbol_rules": {"min_size": 0.001}
```

Missing `lot_size` and `tick_size`. The model needs `tick_size` to set valid TP/SL levels. Last snapshot that had it showed `tick_size: 1e-05`.

**Recommendation:**
```json
"symbol_rules": {
  "min_size": 0.001,
  "lot_size": 0.001,
  "tick_size": 0.00001
}
```

### 8.5 — OBV Trend "rising" with "weak" Confidence — Verify

```json
"obv_trend": "rising",
"obv_trend_confidence": "weak"
```

OBV series (last 15):
```
[-51290, -18724, 21698, 42151, 65312, 121950, 204715, 260672,
 316591, 421897, 522928, 375440, 460926, 274427, 333972]
```

**Analysis:**
- Overall trend from −51,290 → +333,972 is clearly rising ✅
- However, there's a sharp drop from 522,928 → 375,440 (index 11) and another from 460,926 → 274,427 (index 13)
- The last two values show partial recovery: 274,427 → 333,972

The "weak" confidence is justified — the OBV is rising on the macro scale but has two sharp pullbacks in the recent window, suggesting distribution mixed with accumulation. This is a **good** label.

### 8.6 — Swing High/Low Exhaustion Not Flagged in Data

The prompt now has the SWING EXHAUSTION rule, but there's no `swing_high_exhaustion` or `swing_low_exhaustion` field in the data to trigger it.

Current price: $0.26509. Swing highs available above: only 0.29166 (7.6% away).

**Recommendation:** Add the enrichment field the prompt rule expects:
```json
"swing_high_exhaustion": true,
"swing_high_exhaustion_note": "price above all recent swing highs except 0.29166 (7.6% above current)"
```

Without this field, the model must infer exhaustion by comparing price to all swing levels — doable but error-prone.

### 8.7 — L/S Ratio Series Now 46.4 Hours Stale

```json
"series_stale": true,
"series_age_hours": 46.4
```

The staleness detection is working correctly ✅. However, 46.4 hours is extreme — the series is from nearly 2 days ago. The `value: 1.08` may be current but the 20-point series is useless for trend analysis.

**Recommendation:** If the series can't be refreshed, consider not sending it at all when `series_age_hours > 24`:
```json
"long_short_ratio": {
  "value": 1.08,
  "series": null,
  "series_stale": true,
  "series_age_hours": 46.4,
  "note": "series omitted: >24h old"
}
```

This saves ~150 tokens and prevents any model from accidentally using the stale data despite the flag.

---

## 🟡 Medium Priority Issues

### 8.8 — Large Price Drop in Penultimate Candle Not Reflected in Regime

Candle at `ts: 1775138400000`:
```json
{"open": 0.27226, "high": 0.27321, "low": 0.26012, "close": 0.26031, "volume": 186499}
```

This is a **−4.4% candle** on the highest volume in the entire series (186,499 — 3× average). The market regime is still `"trending_up"` but this candle represents a potential trend break or at least a sharp reversal.

The current candle recovered to 0.26509, which is a partial bounce, but the regime labelling may be stale.

**Recommendation:** Add a regime confidence or staleness indicator:
```json
"market_regime": "trending_up",
"market_regime_confidence": "weakening",
"regime_note": "last completed candle was -4.4% on 3x avg volume; trend integrity questionable"
```

### 8.9 — `portfolio_heatmap` Duplicate Still Present (Recurring ×5)

Not present in this snapshot! ✅ **Finally removed.**

Wait — checking again... The top-level `portfolio_heatmap` field is indeed absent. Only `portfolio_exposure.heatmap` remains. **This is fixed.**

### 8.10 — `volume.series` Still Duplicates Candle Volumes (Recurring ×5)

The `indicators.volume` block still contains a `series` field with values identical to candle volumes. However, checking this snapshot... the `indicators.volume` block doesn't contain a `series` field. It only has `last` and `average`. ✅ **This is fixed.**

### 8.11 — Duplicated Financial Fields (Recurring ×6)

`available_margin_usd` still appears in 4 locations, `account_equity_usd` in 5 locations. Unchanged.

---

## 🟢 Low Priority / Recurring

### 8.12 — No Macro/BTC Context (Recurring ×7)

RAVE had a sharp −4.4% candle on huge volume. Was this RAVE-specific or part of a broader market move? No way to tell.

### 8.13 — `isolated_margin_seed_usd: 500` vs. ~$99 Account (Recurring ×6)

Unchanged.

---

## Signal Coherence Check

| Signal | Value | Direction | Notes |
|---|---|---|---|
| LTF ADX | **47.6**, DI+ dominant (29.5 vs 16.2) | **Bullish** ✅ | Strong trend, but DI+ declining from 39.9 |
| LTF Price vs EMA50 | Above ($0.265 vs $0.248) | **Bullish** ✅ | |
| LTF EMA stack | EMA50 < EMA200 | **Bearish** ⚠️ | Legacy |
| HTF ADX | **33.9**, DI+ dominant (35.8 vs 15.9) | **Bullish** ✅ | Good alignment |
| HTF EMAs | EMA50 < EMA200 | **Bearish** ⚠️ | EMAs still lagging |
| HTF RSI | 64.2 | **Neutral/Bullish** | Not overbought anymore |
| Market regime | `trending_up` | **Bullish** ✅ | But questionable after −4.4% candle |
| OBV trend | `rising` (weak) | **Weak Bullish** ⚠️ | Distribution visible in series |
| CVD trend | `unknown` (unknown) | **No signal** ⚠️ | Complete data absence |
| CMF | +0.112 (declining from 0.470) | **Weakening** ⚠️ | Was strongly bullish, now barely positive |
| RSI (LTF) | 64.6 | **Neutral** ✅ | No longer overbought — good |
| Stoch RSI | K=33.9, D=57.9 | **Neutral/Cooling** | Coming off overbought |
| MACD | Positive but histogram shrinking | **Weakening Bull** ⚠️ | |
| Price vs VWAP | Above | **Bullish** ✅ | |
| Liquidity bias | `"bid-supported"` (**MISLEADING**) | Unreliable ❌ | 7 bid / 0 ask — no real depth |
| Volume | 59,545 (below 61,380 avg) | **Neutral** | |
| Funding | +0.009% | Neutral | |
| Credit | 15.1% remaining | **OK** | Above 10% threshold |

**Key observation:** This snapshot shows a market that has **cooled significantly** from the previous one. RSI dropped from 87 → 64.6, Stoch RSI from 100/100 → 34/58, CMF from 0.354 → 0.112, and there was a sharp −4.4% reversal candle. The trend is intact (ADX still very high) but conviction is weakening.

**Expected model behaviour:** This is a difficult call. The trend is technically bullish but the sharp reversal candle, weakening OBV confidence, absent CVD data, and degraded order book all argue for caution. A reasonable output would be:
- **HOLD** (most conservative — citing degraded order flow data, weakening momentum, distribution in OBV)
- Or **BUY** with low confidence (~0.50-0.55) and high risk_score (~0.60-0.70) — citing strong ADX but noting all the warnings

---

## Summary Scorecard

| Category | Previous | Current | Notes |
|---|---|---|---|
| Structural clarity | 9/10 | **9.5/10** | L/S staleness rule, swing exhaustion rule added |
| Data integrity | 7/10 | **5.5/10** | Severe: ask_depth=0, cvd_series empty, slippage null |
| Signal coherence | 7.5/10 | **6/10** | Degraded order flow, weakening CMF, stale regime label |
| Token efficiency | 6/10 | **7/10** | portfolio_heatmap removed, volume.series removed |
| Missing context | 7/10 | **7/10** | Unchanged |
| Edge case handling | 9.5/10 | **9.5/10** | Swing exhaustion rule good but data field missing |

**Overall: 7.4/10** — prompt quality continues to improve but data integrity has degraded significantly in this snapshot, likely due to thin order book conditions.

---

## Priority Actions

| Priority | Item | Effort | New/Recurring |
|---|---|---|---|
| 🔴 Critical | **Fix `liquidity_bias` labelling — add minimum depth threshold** | Small code fix | Recurring ×4 |
| 🔴 Critical | **Add `order_book_note` when depth is near-zero on either side** | Small enrichment | Recurring ×3 |
| 🔴 Critical | **Provide conservative slippage estimate when depth is degraded instead of null** | Small code fix | Recurring ×2 |
| 🟠 High | Add `swing_high_exhaustion` / `swing_low_exhaustion` data fields to match new prompt rule | Small enrichment | **New** |
| 🟠 High | Complete `symbol_rules` with `lot_size` and `tick_size` | Trivial | Recurring ×3 |
| 🟠 High | Add regime confidence/staleness when sharp reversal candles appear | Small enrichment | **New** |
| 🟠 High | Omit L/S series entirely when `series_age_hours > 24` (save tokens, prevent misuse) | Small logic | **New** |
| 🟡 Medium | Deduplicate financial fields (`available_margin_usd` ×4, `account_equity_usd` ×5) | Moderate refactor | Recurring ×6 |
| 🟢 Low | Add macro/BTC context | Moderate (new data source) | Recurring ×7 |
| 🟢 Low | Cap `isolated_margin_seed_usd` or add note | Trivial | Recurring ×6 |
```
````