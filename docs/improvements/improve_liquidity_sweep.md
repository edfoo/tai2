# Improve Liquidity Sweep — Findings & Implementation Handoff

> **Status:** Analysis-complete, no code changed.
> **Author:** Senior Quantitative review (2026-08-10)
> **Scope:** `LiquiditySweepStrategy` — 15m timeframe (default `analysis_timeframe: "15m"`), volatile altcoin markets.
> **Audience:** A senior engineer who will implement the fixes.
> **Reference:** `.github/skills/launcher-strategy-reference.md` (§ Liquidity Sweep)

---

## 1. TL;DR

The Liquidity Sweep strategy's *conceptual framing* is correct for volatile 15m alts
(sweep is a chop-regime microstructure pattern; `htf_regime_preference: "chop"`,
`max_adx: 28`, BB-bandwidth percentile ≤ 60 gate are all right; the per-strategy 15m
cadence from `per_strategy_timeframe` was the correct enablement). The failures come
from **entry-logic and confirmation, not the regime framing**. The dominant flaws:

1. **The reclaim check does not require a reclaim of the swept level (real logic gap, HIGH).**
   `long_reclaim_ok = close_pos >= reclaim_ratio` only checks the close's position
   *within its own candle body*; it never compares `curr_close` to `swing_low`. A candle
   that pierces support and closes in the upper half of its body — but **entirely below
   the swept level** — satisfies the check. That is a breakdown, not a reclaimed stop-run,
   and it feeds the primary "real breakout mistaken for a sweep" failure mode.
2. **The "swing" is a raw 20-bar min/max, not real structure (HIGH).** `swing_low = min(prior_lows)`
   / `swing_high = max(prior_highs)` is a trailing range edge inside the noise, not a
   defended, visible level that stop-hunters actually cluster at. Consequently the TP
   (opposite swing extreme) is also economically meaningless and often unreachable →
   poor geometry → downstream R:R guardrail blocks.
3. **`sweep_buffer_pct = 0.1%` is trivially tight for volatile alts (MEDIUM-HIGH).**
   A 15m volatile-alt wick routinely breaches an edge by >0.1% as ordinary oscillation,
   so at default the buffer is nearly a no-op and all discrimination falls on the weak
   reclaim check. Penetration should be ATR-scaled (or a larger constant).
4. **The on-thesis confirmation gates are opt-in and off (MEDIUM-HIGH).**
   `require_close_in_va: False`, `require_macro_sl: False`, and there is **no
   order-book-imbalance gate wired into this strategy at all** (`liquidity_helpers.order_book_imbalance`
   exists but the sweep does not consume it). The fade has no institutional-flow/absorption
   confirmation before committing.
5. **A dead/duplicate TP/SL sizing path remains in the file (LOW-MEDIUM).**
   Lines ~445–515 compute `_effective_tp/_effective_sl/_sizing_source` and set
   `_sizing_source = "structural(...)"`, but the final return discards all of it in favor of
   `compute_tp_sl_pct(...)` + rationale `"...[trade_mgmt]"`. The `_sizing_source` reporting is stale.

Recommended fix priority: **#1 → #2 → #3 → #5 (correctness/cleanup), then #4 (confirmation)
as the second wave**, then tuning only after OOS/walk-forward backtesting.

---

## 2. Relevant Files / Line References

| Concern | Location |
|---|---|
| LS strategy logic / signal & sizing | `app/services/strategies/liquidity_sweep.py` |
| LS canonical defaults | `app/services/strategies/defaults.py` (`DEFAULT_LIQUIDITY_SWEEP`, lines ~120–170) |
| Shared liquidity helpers | `app/services/strategies/liquidity_helpers.py` (`order_book_imbalance`, `funding_is_blocked`, `oi_confirms_momentum`) |
| Unified TP/SL manager | `app/services/trade_management.py` (`OrderContext`, `calculate`, `compute_tp_sl_pct`, `_ensure_rr` ≥ 1.8) |
| HTF regime gate | `app/services/indicator_service.py` (`is_trending`, `htf_regime_allows`) |
| Analysis-block resolution | `app/services/strategies/__init__.py` `resolve_analysis_block()` |
| Per-strategy timeframe design | `docs/objectives/per_strategy_timeframe.md` (implemented 2026-08-10) |
| Strategy reference | `.github/skills/launcher-strategy-reference.md` (§ Liquidity Sweep) |

---

## 3. Diagnostic Findings (root causes)

### F1 — Reclaim check does not require a reclaim of the level (HIGH severity, correctness)

`app/services/strategies/liquidity_sweep.py`:
```python
close_pos = (curr_close - curr_low) / candle_range  # 0=at low, 1=at high
long_reclaim_ok  = long_sweep_pierced  and close_pos >= reclaim_ratio
short_reclaim_ok = short_sweep_pierced and close_pos <= (1.0 - reclaim_ratio)
```
The docstring promises "*closes back inside the range*," but nothing compares `curr_close`
to `swing_low` / `swing_high`. Pathological long case (valid BUY today):
`swing_low = 98`, candle `open 97, high 97.1, low 90, close 95.5` → pierced (`90 < 98*(1-0.001)`),
`close_pos = (95.5-90)/7.1 ≈ 0.77 ≥ 0.5` → fires, but price closed **below former support**.
That is a breakdown continuation, the exact opposite of the thesis.

### F2 — Raw min/max "swing" is not real structure (HIGH severity)

```python
swing_low  = min(prior_lows)
swing_high = max(prior_highs)
```
A trailing 20-bar extreme has no economic meaning as a stop-hunt target:
- Real stop-hunts target **visible, freshly-respected** pivots (local extremes with *N* bars
  on each side), which trailing min/max cannot express.
- The **TP mirrors this** via `tp_target = swing_high` (long) / `swing_low` (short), so an unreachable
  range edge yields poor TP geometry and downstream `require_reward_risk_ratio` blocks.

### F3 — `sweep_buffer_pct = 0.1%` trivially tight on volatile 15m alts (MEDIUM-HIGH severity)

```python
sweep_buffer = sweep_buffer_pct / 100.0   # 0.1 → 0.001 = 0.1%
long_sweep_pierced  = curr_low  < swing_low  * (1.0 - sweep_buffer)
short_sweep_pierced = curr_high > swing_high * (1.0 + sweep_buffer)
```
0.1% is inside ordinary 15m-alt oscillation, so the "penetration" half of the definition is
nearly degenerate; all discrimination falls onto the (weak, per F1) reclaim. Should scale with
each symbol's volatility (ATR-based) or be a larger constant.

### F4 — On-thesis confirmation is off / absent (MEDIUM-HIGH severity)

- `require_close_in_va: False` — the single most on-thesis §3 filter (stop-run absorbed *back
  inside value*) — off by default.
- `require_macro_sl: False` — recessed SL to survive the post-sweep shakeout — off.
- **No book-imbalance gate at all** in liquidity_sweep: `order_book_imbalance()` exists in
  `liquidity_helpers.py` but the sweep never calls it. A long sweep fading into a stop-run wants
  the book **bid-supported after the reclaim** so the fade has somewhere to go.

### F5 — Dead/duplicate sizing path + stale `_sizing_source` reporting (LOW-MEDIUM severity)

The early block (lines ~445–515) fully computes `_effective_tp/_effective_sl/_sizing_source`
(the `clamp TP/SL` structural-ATR math), then the final return path calls
`compute_tp_sl_pct(...)` and labels everything `"[trade_mgmt]"`. Consequences:
- Every symbol pays the cost of running both sizing paths.
- `_sizing_source` (which would tell you *why* TP/SL were chosen) is never surfaced in the final
  signal rationale — the audit trail is lost.

---

## 4. Implementation Steps (for the engineer)

### Step 1 — Fix the reclaim logic to require an actual reclaim of the swept level (F1)

**Target:** `app/services/strategies/liquidity_sweep.py` (reclaim check block).

Change the reclaim predicates from *body-position only* to *body-position AND reclaimed-the-level*:

```python
# Existing body-position filter (keep, as-is):
close_pos = (curr_close - curr_low) / candle_range
body_ok_long  = close_pos >= reclaim_ratio
body_ok_short = close_pos <= (1.0 - reclaim_ratio)

# NEW: the close must actually reclaim the swept level by a margin.
# Use the same sweep buffer so symmetry is preserved (penetrate below, close back above).
reclaim_margin = sweep_buffer  # reuse config; or a dedicated `reclaim_buffer_pct`
level_ok_long  = curr_close > swing_low  * (1.0 + reclaim_margin)
level_ok_short = curr_close < swing_high * (1.0 - reclaim_margin)

long_reclaim_ok  = long_sweep_pierced  and body_ok_long  and level_ok_long
short_reclaim_ok = short_sweep_pierced and body_ok_short and level_ok_short
```

- Add a dedicated config key `reclaim_buffer_pct` (default equal to `sweep_buffer_pct`) rather
  than silently reusing `sweep_buffer`, so the two can be tuned independently.
- Emit a `helpers.emit_debug` when swept-but-`level_ok_*` fails naming `curr_close` vs
  `swing_low/high` so `/debug` distinguishes "no reclaim of level" from "weak body position".

**Verify:** add unit tests: (a) pierced + body-upper-half but **close below `swing_low`** → **no** BUY;
(b) pierced + body-upper-half + close above `swing_low*(1+buffer)` → BUY; (c) mirror for shorts.

### Step 2 — Replace raw min/max swing with fractal pivot swing (F2)

**Target:** `app/services/strategies/liquidity_sweep.py` (swing detection).

1. Add a helper (e.g. `_fractal_swings(ohlcv, lookback)` or a util in `strategies/__init__.py`)
   that identifies local pivots: candle `i` is a pivot low if its low is the min of bars
   `[i-n, i+n]` for a configurable `pivot_bars` (default e.g. 3 on 15m). 
2. Define `swing_low = min(pivot_lows)` / `swing_high = max(pivot_highs)` from the pivots found
   in the prior `lookback` bars.
3. **Fallback:** if fewer than `pivot_bars*2+1` candles or no pivots are found, fall back to the
   current trailing `min/max` and log it. This keeps legacy behavior when history is short.
4. Keep the TP/SL anchoring logic unchanged (it already reads `swing_high/swing_low` once computed).

**Verify:** unit test that a pivot-based swing is picked correctly on synthetic data, and that the
trailing-min/max fallback fires when pivots are unavailable.

### Step 3 — ATR-scaled sweep penetration (F3)

**Target:** `app/services/strategies/liquidity_sweep.py` (sweep detection) + `defaults.py`.

- Make the penetration threshold volatility-scaled. Replace the flat `sweep_buffer` with:
  ```python
  atr_price = (indicators.get("atr_pct") / 100.0) * swing_low      # per symbol
  pen_atr = cfg.get("sweep_buffer_atr", 0.25)                      # NEW key, default 0.25×ATR
  long_sweep_pierced  = curr_low  < swing_low  - pen_atr * atr_price
  short_sweep_pierced = curr_high > swing_high + pen_atr * atr_price
  ```
- Preserve `sweep_buffer_pct` (percentage-based) as an **alternative** mode selected by a toggle
  (e.g. `sweep_penetration_mode: "atr" | "pct"`, default `"atr"`), so backtests can compare both.
- Do **not** quietly change semantics on live before the backtest comparison (see Test Plan #5).

### Step 4 — Remove the dead/duplicate sizing path (F5)

**Target:** `app/services/strategies/liquidity_sweep.py` (lines ~445–515).

Delete the block that computes `_static_tp/_effective_tp/_effective_sl/_sizing_source` and the
earlier `raw_tp_dist/raw_sl_dist` structural-ATR clamping — it is fully superseded by the
`compute_tp_sl_pct` path at the return. After removal:
- Ensure `atr_pct` for `use_adaptive_atr` scaling is applied to the `OrderContext.atr_tf_pct`
  passed into `compute_tp_sl_pct`, so adaptive-ATR behavior is preserved in the surviving path
  (otherwise `use_adaptive_atr` silently dies with the dead block).
- Preserve any fields the removed block was the only producer of (verify `_sizing_source` is not
  referenced downstream; if it is, repopulate it from the `compute_tp_sl_pct` return value, which
  currently only reports `"[trade_mgmt]"`).

### Step 5 — Wire the on-thesis confirmation gates (F4, second wave)

**Target:** `app/services/strategies/liquidity_sweep.py` + `defaults.py` + `pages.py`.

1. **`require_close_in_va`** — currently read (`close_in_va`) but gated off by default. Keep off
   but expose it prominently; it is the highest-value confirmation.
2. **`require_book_imbalance`** (NEW key, default `False`): call
   `order_book_imbalance(sym_data.get("order_book"))` after the reclaim passes. For a **long**
   sweep require `imbalance >= imbalance_min_for_long` (bid-supported, default e.g. `1.0`); for a
   **short** sweep require `imbalance <= imbalance_max_for_short` (default `1.0`). Emit the computed
   ratio in the debug line when the gate is enabled.
3. **`require_macro_sl`** — the code path already exists; a shorter `macro_sl_lookback` (e.g. 25
   for 15m) is worth evaluating in backtest, but keep default `False` until validated.
4. Mirror all new keys in `pages.py` "Set Recommended Defaults" for LS so the button and canonical
   defaults agree (same requirement as the MR handoff).

---

## 5. Config Defaults Summary (before → after, proposed)

| Key | Before | After | Rationale |
|---|---|---|---|
| `reclaim_buffer_pct` (new) | n/a | `= sweep_buffer_pct` | close must reclaim the level, not just sit high in its body |
| swing detection | raw min/max | fractal pivot (+ min/max fallback) | swept level = real defended structure |
| `sweep_penetration_mode` (new) | — | `"atr"` | volatility-scaled penetration for volatile alts |
| `sweep_buffer_atr` (new) | — | `0.25` | penetration in ATR units |
| `sweep_buffer_pct` | 0.1 | keep (used in `"pct"` mode) | backtest comparison mode |
| `require_close_in_va` | False | keep **False** (opt-in, surfaced) | enable only after OOS backtest |
| `require_macro_sl` | False | keep **False** (opt-in) | validate `macro_sl_lookback` ~25 on 15m first |
| `require_book_imbalance` (new) | n/a | `False` (opt-in) | fade into bid-supported (long) / ask-heavy (short) book |

(Steps 1–4 are correctness/cleanup; Step 5 is confirmation that should follow only after
backtest/OOS validation. Do not flip any opt-in gate on before measuring its OOS effect.)

---

## 6. Test Plan

1. **Regression:** `poetry run pytest tests/ -q` (all 69 must pass).
2. **Reclaim logic (F1):** unit tests in `tests/test_strategies.py` for the three cases in Step 1
   (no-true-reclaim → no signal; reclaimed-level + body-position → signal; short mirror).
3. **Swing detection (F2):** synthetic data test for fractal pivot + min/max fallback.
4. **Penetration (F3):** test ATR-mode and pct-mode both fire/cancel correctly.
5. **Backtest comparison:** add a config-switch backtest comparing `sweep_penetration_mode: "pct"`
   (legacy, 0.1%) vs `"atr"` (0.25×ATR) and the fractal-swing vs raw-minmax, with strict
   **in-sample train / out-of-sample validate** splits and walk-forward windows, before adopting
   any new default.
6. **Confirmation gates (F5):** test `require_book_imbalance` directionally (bid-supported long,
   ask-heavy short) and that it degrades gracefully when `order_book` is absent.

---

## 7. Recommended Sequencing for the Implementer

1. **Step 1 + Step 2 + Step 3** are the core correctness fix (reclaim-of-level, real structure,
   ATR penetration) — do these together, then re-run the backtest comparison first.
2. **Step 4** (remove dead sizing path) alongside 1–3 so the adaptive-ATR behavior is preserved
   under the single surviving path.
3. **Step 5** (confirmation gates + book imbalance) second wave, and only after OOS/walk-forward
   validation of the new defaults.