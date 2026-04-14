````markdown
# Analysis of Crypto Trading LLM Prompt

This is a thorough analysis. Findings are grouped into actionable categories, with the highest-impact items first.

---

## 1. Massive Token Waste: Raw Candle Data

**This is by far the biggest single win.** The `candles` array has ~95 LTF candles and ~48 HTF candles, each with 6 fields. Meanwhile, every indicator the LLM actually needs (RSI, MACD, ADX, Bollinger, EMA, OBV, VWAP, ATR…) is **already pre-computed** in `context.indicators`.

| Data | Approx. Tokens | LLM Actually Uses It For |
|---|---|---|
| `candles` (95 LTF) | ~3,000 | Nothing — indicators are pre-computed |
| `candles_htf` (48 HTF) | ~1,500 | Nothing — HTF indicators are pre-computed |
| `vwap_series` (95 values) | ~800 | Maybe last 3-5; trend is summarisable |
| `cvd_series` (30 values) | ~250 | "Cross-check" against pre-computed label |
| `ofi_ratio_series` (20 values) | ~200 | Modest confirmation signal |
| `volume_rsi_series` (15 values) | ~150 | Modifier only |

**Recommendation:** Drop all raw candles. The prompt says *"you may still cross-check against the last 3-5 values"* for OBV/CVD — so at most send the **last 5 values** of any series the LLM is told to verify. For VWAP, a single value + slope direction suffices. **Estimated savings: ~5,000–6,000 tokens per call.**

---

## 2. Redundant / Duplicated Fields

The same values appear in multiple places. Every duplicate costs tokens for the key name + the value:

| Value | Appears In | Keep |
|---|---|---|
| Available margin (~$74.72) | `account.available_equity`, `account.available_eq_usd`, `account.available_balances.USDT.available`, `account.available_balances.USDT.available_usd`, `account.quote_available`, `account.quote_available_usd`, `execution.available_margin_usd`, `execution.margin_health.available_margin_usd`, `execution.margin_health.live_snapshot.available_margin_usd`, `execution.live_margin_snapshot.available_margin_usd` | **1 field** |
| Account equity (~$94.24) | `account.account_equity`, `account.total_eq_usd`, `execution.account_equity_usd`, `execution.margin_health.account_equity_usd`, `execution.margin_health.live_snapshot.account_equity_usd`, `execution.live_margin_snapshot.account_equity_usd` | **1 field** |
| ADX/DI values | `indicators.adx` and `trend_confirmation.adx` | **1 location** |
| EMA 50/200 | `indicators.moving_averages` and `trend_confirmation.moving_averages` | **1 location** |
| ATR / ATR% | `indicators.atr`, `risk_metrics.atr`, `risk_metrics.atr_pct`, `indicators.atr_pct` | **1 field** |
| Spread | `market.spread`, `market.spread_pct`, `liquidity_context.spread`, `liquidity_context.spread_pct` | **1 location** |
| `funding_archetype_c_eligible` | `derivatives_posture` and `market_signals` | **1 field** |
| `live_margin_snapshot` | `execution.live_margin_snapshot` is identical to `execution.margin_health.live_snapshot` | **1 copy** |

**Recommendation:** Flatten `account` + `execution` + `margin_health` into one concise block. Merge `indicators` and `trend_confirmation`. **Estimated savings: ~1,500–2,000 tokens.**

---

## 3. Deterministic Decisions → Execution Layer

Many "instructions to the LLM" are actually **pure if/then checks on the data**. They should not be LLM decisions at all. Move them upstream and either (a) do not call the LLM, or (b) inject the result as a pre-computed flag.

### 3a. Hard-blocks that should prevent the LLM call entirely

The following are all deterministic. Check them in the execution layer BEFORE calling the LLM:

- **Capital sufficiency:** if `max_safe_notional_usd < min_notional_usd`, return HOLD. Do not call the LLM at all.
- **Existing position in same direction:** if the symbol already has an open position in the same direction, return HOLD.
- **Execution feedback staleness:** if feedback exists and its age is less than 2 × candle period, return HOLD.
- **Min hold time violation:** if the position age is less than `min_hold_seconds`, return HOLD.
- **Daily loss lock active:** if `risk_locks.daily_loss.active` is true, return HOLD.

**Impact:** Every blocked call saves the **entire prompt cost** (~10k+ tokens in, ~500 tokens out).

### 3b. Deterministic penalties/flags to pre-compute

Instead of burning tokens teaching the LLM arithmetic, compute in the execution layer and pass as flags:

| Rule in prompt | Deterministic? | Pre-compute as |
|---|---|---|
| HTF alignment bracket (±penalty) | Yes — compare EMA50 vs EMA200, ADX thresholds, DI gap | `htf_alignment: "strong" / "weak" / "contradicts_strong"`, `htf_confidence_penalty: -0.15` |
| OBV/CVD weight adjustment from confidence label | Yes — map "strong"→1.0, "moderate"→0.75, etc. | `obv_weight: 0.5`, `cvd_weight: 0.5` |
| Ranging penalty offset eligible? | Yes — check funding < -0.15% OR obv diverges | `ranging_offset_eligible: true` |
| Credit conservation mode | Yes — `remaining / granted < 0.10` | `credit_conservation: true` |
| Thin book check | Yes — startsWith('thin:') | `order_flow_reliable: false` |
| Zero depth fallback | Yes — bid_depth == 0 or null | Already handled by thin book |
| Swing exhaustion | Already pre-computed! | Already a flag — just remove the instructions explaining how to detect it |
| L/S series stale | Already flagged | Just pass the flag, remove the paragraph explaining what stale means |
| Spike recency check | Yes — compare last 3 candle moves to 2×ATR | `spike_recency_flag: true, risk_score_add: 0.15` |
| Breakdown staleness reclassification | Yes — check last 10 candles for higher lows + declining ATR | `regime_reclassified: "post_spike_consolidation"` |
| `IGNORE guardrails.flip_llm_decision` | Why send it just to say ignore it? | Do not send it |

**Recommendation:** Create a `pre_computed_modifiers` block:

```json
{
  "pre_computed_modifiers": {
    "htf_alignment": "weak_neutral",
    "htf_penalty_additive": -0.15,
    "htf_penalty_multiplicative": 1.0,
    "htf_adx": 24.84,
    "obv_effective_weight": 0.5,
    "cvd_effective_weight": 0.5,
    "regime": "trending_up",
    "regime_confidence": "stable",
    "regime_penalty": 0.0,
    "ranging_offset_applied": false,
    "credit_conservation": true,
    "credit_risk_add": 0.10,
    "order_flow_reliable": true,
    "spike_recency": false,
    "swing_high_exhaustion": true,
    "swing_exhaustion_risk_add": 0.10,
    "ls_series_usable": false,
    "archetype_c_eligible": false,
    "capital_sufficient": true,
    "execution_feedback_blocks": false
  }
}
```

---

## 4. The System/Task Prompt Is ~4,500 Tokens of Rules

The prompt instructions are extremely detailed and legalistic. While precision is valuable, much of this is:

- **Rules for edge cases that do not apply to this snapshot** (e.g., Archetype C paragraphs when `funding_archetype_c_eligible: false`)
- **Arithmetic worked examples** the LLM should not be doing
- **Defensive instructions** against LLM failure modes (divergence sanity checks, label verification)

### 4a. Conditional prompt assembly

Build the prompt dynamically. Only include rule sections that are relevant:

- Always include `CORE_FRAMEWORK`.
- If HTF data exists, include `HTF_RULES`. Otherwise the pre-computed note "No HTF data, -0.20 penalty applied" suffices.
- If regime is "ranging", include `RANGING_RULES`. If "breakdown", include `BREAKDOWN_RULES`. And so on.
- If `archetype_c_eligible` is true, include `ARCHETYPE_C_RULES`.
- If `execution_feedback` exists, include `FEEDBACK_STALENESS_RULES`.

**Estimated savings:** 30–50% of prompt instruction tokens on any given call (~1,500–2,000 tokens).

### 4b. Replace "how to calculate" with "here is the result"

**Before (in prompt — ~120 tokens):**

> *"HTF DI AMBIGUITY: if HTF ADX > 25 but |di_plus − di_minus| < 5, treat the HTF as weak/neutral regardless of ADX value — the trend exists but has no clear directional bias. Apply the −0.15 weak/neutral penalty."*

**After (in pre-computed data — ~10 tokens):**

```json
"htf_alignment": "weak_neutral_di_ambiguity"
```

The LLM just needs to **know** the HTF alignment, not **calculate** it.

---

## 5. Numeric Precision Waste

Many numbers have 15+ decimal places:

```
0.11978074033971713   →  0.1198
9.999999999999593e-06 →  0.00001
0.02426890256972577   →  0.02427
83.01431004273446     →  83.01
```

**Recommendation:** Round all floats contextually:

- **Prices:** to tick_size (`1e-05` → 5 decimal places)
- **Percentages:** 2 decimal places
- **Ratios:** 2–4 decimal places

**Estimated savings:** ~500–800 tokens across all numeric fields.

---

## 6. Post-validation That Should Be Execution Layer

The prompt contains rules the execution layer should enforce **after** receiving the LLM response:

| Rule | Why it is post-validation |
|---|---|
| "stop_loss MUST be below entry for BUY" | Trivially validated: `if action == "BUY" and stop_loss >= entry: reject` |
| "confidence < 0.45 → MUST HOLD" | `if response.confidence < 0.45 and response.action != "HOLD": override` |
| "take_profit on wrong side → rejected" | Simple arithmetic check |
| Regime-adjusted floor logic | Apply after getting raw confidence |
| `adjust_invalid_tp` | Already a guardrail flag — execution layer handles it |

**Do not teach the LLM to police itself — police it downstream.** This saves instruction tokens and is more reliable.

---

## 7. Structural / Miscellaneous

### 7a. The `positions` array includes unrelated symbols

The prompt is for `TRIA-USDT-SWAP` but includes positions for `HUMA`, `RAVE`, `HMSTR`. The LLM only needs to know:

- Whether there is an existing TRIA position (there is not)
- Aggregate portfolio exposure (already in `portfolio_exposure.summary`)

Send only the TRIA position (if any) + the summary string. Do not send the full heatmap of other positions.

### 7b. Timestamps as millisecond integers

`1775993400000` is ~13 tokens and semantically opaque to an LLM. If timestamps are needed (e.g., for swing highs), use ISO-8601 truncated: `"2026-04-12T11:30"` — shorter and interpretable.

### 7c. `response_schema` in the prompt body

If the model API supports structured outputs or JSON mode, pass the schema via the API parameter rather than in the prompt text. Saves ~200 tokens.

### 7d. `guardrails` object

Many guardrails are for the execution layer, not the LLM: `adjust_invalid_tp`, `fallback_orders_enabled`, `isolated_margin_seed_usd`, `snapshot_max_age_seconds`, `require_reward_risk_ratio`, `isolated_wallet_bootstrap_pct`, etc. The LLM is told to ignore `flip_llm_decision` — which means sending it wastes tokens. Strip guardrails to only what the LLM needs: `max_leverage`, `max_position_pct`, `min_hold_seconds`.

### 7e. `risk_locks` object

If `daily_loss.active` is `false`, the entire block is irrelevant. Do not send it, or send just `"risk_locks": {"daily_loss_active": false}`.

### 7f. `fee_availability`

No prompt rule references fee data. Either add a rule or remove the field.

---

## 8. Summary: Estimated Token Savings

| Optimization | Tokens Saved (est.) | Difficulty |
|---|---|---|
| Remove raw candles, truncate series | 5,000–6,000 | Low |
| Deduplicate fields | 1,500–2,000 | Low |
| Pre-compute penalties/flags, shorten instructions | 1,500–2,000 | Medium |
| Conditional prompt assembly | 1,500–2,000 | Medium |
| Round numeric precision | 500–800 | Low |
| Strip irrelevant guardrails, positions, risk_locks | 500–700 | Low |
| Move post-validation rules out of prompt | 300–500 | Low |
| **Total** | **~6,000–10,000** | — |

Given that the current prompt is likely **~12,000–15,000 tokens**, this represents a **40–65% reduction**.

---

## 9. Proposed Lean Architecture

```
┌──────────────────────────────────────────┐
│           EXECUTION LAYER (pre)          │
│                                          │
│  1. Hard-block checks → skip LLM if fail │
│  2. Compute all penalties / flags        │
│  3. Flatten & deduplicate context        │
│  4. Select relevant prompt sections      │
│  5. Round all numerics                   │
│  6. Strip raw candles, keep last 5 of    │
│     series the LLM is asked to verify    │
└──────────────────┬───────────────────────┘
                   │ lean prompt (~5k tokens)
                   ▼
┌──────────────────────────────────────────┐
│                 LLM                      │
│                                          │
│  Evaluate direction, confidence,         │
│  risk_score, TP/SL levels                │
│  (qualitative judgment only)             │
└──────────────────┬───────────────────────┘
                   │ JSON response
                   ▼
┌──────────────────────────────────────────┐
│          EXECUTION LAYER (post)          │
│                                          │
│  1. Validate TP/SL direction             │
│  2. Enforce confidence floor → HOLD      │
│  3. Apply flip_llm_decision if needed    │
│  4. Compute notional from confidence     │
│     and risk_score                       │
│  5. Check reward:risk ratio              │
│  6. Submit or reject order               │
└──────────────────────────────────────────┘
```
````