# Improve Mean Reversion — Findings & Implementation Handoff

> **Status:** Analysis-complete, no code changed.
> **Author:** Senior Quantitative review (2026-08-10)
> **Scope:** `MeanReversionStrategy` — 15m timeframe, volatile altcoin markets.
> **Audience:** A senior engineer who will implement the fixes.
> **Objective doc:** `/home/fisheagle/projects/tai2/docs/objectives/gemini_objective.md`

---

## 1. TL;DR

The Mean Reversion strategy gets stopped out on volatile 15m altcoins primarily because of **exit/TP-SL geometry and the R:R pipeline**, not entry-timing. The entry filters (closed-candle evaluation, ADX/BB-bandwidth/regime gates, candle rejection) are already reasonably robust. The dominant flaws are:

1. **1:1 ATR TP:SL default** → a mathematically guaranteed losing bias when win-rate < 50 %.
2. **`compute_tp_sl_pct()` silently swallows the R:R `ValueError`** from `calculate()` and falls back to ATR/static exits, so positions execute without ever satisfying the intended reward-to-risk.
3. **Structural sizing is `False` by default** and even when enabled, the **SL anchors to the forming (not closed) candle wick** with a **0.15×ATR buffer** — inside the noise band, an active stop-hunt target.
4. **No regime-breakdown exit**: once in a position, a chop→trend transition rides the widening noise and is never systematically flattened by the strategy-level regime gate (only the global trade-management time/breakeven/partial rules apply pre-close).

Recommended fix priority: **#1 → #2 → #3 → #4**, then apply the entry-quality tightening only if R:R guardrail blocks persist.

---

## 1.5 Timeframe Rationale (15m vs 1H vs 4H)

**Recommended: stay on 15m.** It is the only analysis timeframe that is internally
consistent with the strategy's design and this bot's runtime defaults. The layout
below is what an implementer should preserve (and what to change if 1H is ever pursued).

| Choice | Recommended? | Why |
|---|---|---|
| **15m** | ✅ **Best** | Existing default. HTF gate (via `_HTF_MAP` 15m→**1H**) is meaningful and passable — an alt can genuinely chop on 1H. Reversion thesis (TP = **15m BB middle**) completes in minutes–few bars, which fits the default 45-min time-stop. |
| **1H** | ⚠️ Workable alt | Higher per-signal quality (real overextension instead of a 15m wick), but requires (a) raising `time_stop_seconds` to ~4–8 h, (b) a wider `structural_sl_buffer_atr`, and (c) accepting far fewer signals. Re-validate the HTF (1H→**4H**) chop-gate cadence before committing. |
| **4H** | ❌ Avoid | HTF = **1D** chop gate blocks nearly every entry on volatile alts (most are "trending" daily) → gate effectively dead. And the default **45-min time-stop** defeats a multi-day 4H reversion thesis. |

**Why 15m noise is not an argument for a higher TF.** The premature stop-outs come
from the *exit-geometry* problems (F1/F2/F3), not from 15m being too fast:

- Entry evaluation already runs on the **previous closed candle**
  (`_launcher_evaluate_signals` → `_build_closed_candle_snapshot`), which removes the
  intra-bar RSI/BB transient bias that most naturally bites on a fast bar.
- The noise expresses itself through **SL anchored to the forming wick with a 0.15×ATR
  buffer** (F3). Fixing the anchor (closed candle + swing/VA + buffer ≥ 1.0×ATR) recovers
  15m as a viable scalp timeframe. Jumping to 1H to "escape noise" is the wrong lever and
  buys the time-stop + HTF-gate problems above.

**Rule for the implementer:** do not relax the analysis timeframe to cover up exit-geometry
bugs. Keep 15m, fix F1–F3, backtest, and only revisit 1H afterward if 15m still grades as
too noisy on out-of-sample evaluation.

---

## 2. Relevant Files / Line References

| Concern | Location |
|---|---|
| MR strategy logic / signal & sizing | `app/services/strategies/mean_reversion.py` |
| MR canonical defaults | `app/services/strategies/defaults.py` (DEFAULT_MEAN_REVERSION, lines ~20–78) |
| Unified TP/SL manager | `app/services/trade_management.py` (`OrderContext`, `calculate`, `compute_tp_sl_pct`) |
| Launcher closed-candle evaluation | `app/services/market_service.py` `_launcher_evaluate_signals()`, `_build_closed_candle_snapshot()` (~line 3059) |
| Launcher R:R guardrail | `app/services/market_service.py` `handle_llm_decision()` (~lines 9710–9785) |
| Trade management / risk exits | `app/services/market_service.py` `_check_trade_management()` (line 2223) |
| HTF regime gate | `app/services/indicator_service.py` (`is_trending`, `htf_regime_allows`) |
| Analysis-block resolution | `app/services/strategies/__init__.py` `resolve_analysis_block()` |

---

## 3. Diagnostic Findings (root causes)

### F1 — 1:1 ATR TP:SL = structural negative-EV (HIGH severity)

`app/services/strategies/defaults.py`:
```python
"use_atr_sizing": True,
"use_structural_sizing": False,
"atr_tp_multiplier": 1.0,
"atr_sl_multiplier": 1.0,
```
With `atr_tp_multiplier == atr_sl_multiplier == 1.0` and MR naturally having a win-rate < 50 %, the strategy **needs > 50 %+fees** win rate to break even. The docstring at `mean_reversion.py` line 76 already documents this hazard. **Live config confirms this is active**: `logs/` show `use_atr_sizing: False` but the ATR multipliers are exercised whenever `use_atr_sizing` is `True` in prod. This is the highest-leverage fix.

### F2 — R:R guardrail silently bypassed (HIGH severity, likely bug)

In `trade_management.compute_tp_sl_pct()`:
```python
except ValueError:
    return static_tp_pct, static_sl_pct
```
This **catches** the `ValueError` raised by `calculate()` when reward-to-risk < 1.8 and silently falls back. The launcher-side R:R guardrail at `market_service.py:9713` only runs when `require_reward_risk_ratio` is true AND it computes R:R from the **final** TP/SL prices placed on the order. If dynamic structural exits degrade to static/ATR, the guardrail is evaluating an *already-degraded* geometry, so the 1.8× intent is never honored. Result: trades execute at 1:1 or worse.

### F3 — Structural SL anchors to the forming candle wick with 0.15×ATR buffer (HIGH severity on 15m alts)

`mean_reversion.py` (structural sizing block):
```python
_curr = ohlcv_compact[-1] if ohlcv_compact and isinstance(ohlcv_compact[-1], dict) else {}
curr_low  = helpers.extract_float(_curr.get("low"))
curr_high = helpers.extract_float(_curr.get("high"))
...
sl_level = curr_low - structural_sl_buffer_atr * atr_price   # long
```
Two compounding problems:
- `_compute_indicators`/`ohlcv_compact` here is the **full snapshot**, not trimmed to closed candles. `_launcher_evaluate_signals()` evaluates inputs on the **previous closed candle**, but the SL anchor reads `_curr[-1]` = the **live, forming bar** whose low/high still extends for up to 15 minutes. So the stop sits under a moving floor that is not yet validated → premature "secondary wick" stops.
- `structural_sl_buffer_atr: 0.15` is a **0.15×ATR buffer** — well inside normal noise on an altcoin. The objective asks for 2.0–3.0× ATR anchored to macro swing / VA, not a local wick.

### F4 — No regime-breakdown exit for open positions (MEDIUM-HIGH severity)

`_check_trade_management()` (line 2223) implements breakeven, partial-TP, and time-stop only. There is **no** strategy-level "regime flipped to trend → flatten" exit. The entry-side filters (`htf_regime_allows`, `max_adx=28`) prevent *new* entries during trends but do nothing for *open* MR positions. Result: the classic story — MR enters into chop, the alt breaks out, ADX climbs past the gate, and the bot holds the losing fade to the time-stop instead of flattening on invalidation.

### F5 — Structural TP/SL availability degrades silently (MEDIUM severity)

Both the in-strategy block and the unified manager require exact keys (`bb_middle`, `curr_low/high`, `vpoc`, `value_area_width`, `swing_high/low`). If any is missing (e.g. short history, degenerate VA), the code falls through ATR/static — no debug/feedback records that the thesis-specific exit was discarded. Hard to audit in logs.

### F6 — `use_adaptive_atr` exists but is off & only scales the raw ATR% (LOW severity)

When `config.get("use_adaptive_atr")` is `True`, it multiplies `atr_pct` (1.20/1.80/2.50 by regime) before computing ATR-based TP/SL. Good idea, but (a) not in defaults, (b) does not scale structural anchors, (c) gives no regime anchoring for entry scaling.

### F7 — Entry quality knobs available but soft (env-dependent)

Defaults: `max_adx: 28`, `rsi_oversold: 30 / overbought: 70`, `bb_proximity_pct: 0.5`, `candle_rejection_pct: 30`. These are reasonable. The liquidity gates (`require_price_in_va`, `require_no_extreme_funding`, `require_balanced_book`) are all **opt-in and off** — for volatile alts they directly target the "falling knife / institutional flow" failure and should be considered in the tuning pass, *not* the geometry fix.

---

## 4. Implementation Steps (for the engineer)

### Step 1 — Fix the exit math bias (F1)

**Target:** `app/services/strategies/defaults.py`
- Change canonical MR defaults to a real R:R:
  - `"atr_tp_multiplier": 1.0 → 1.8`
  - `"atr_sl_multiplier": 1.0 → 1.0` (wider SL to survive noise, modest TP to bank snapback), OR the symmetric `2.0 / 1.0`.
  - Keep `atr_tp_multiplier >= atr_sl_multiplier` invariant.
- Mirror in `app/ui/pages.py` "Set Recommended Defaults" for MR (currently `or 2.0` / `or 1.5` fallbacks at ~lines 3336/3343 — update to match the canonical default so the button and canonical agree).

**Verify:** `tests/test_strategies.py` MR TP/SL assertions and `tests/test_backtest.py` parameter expectations must still pass or be updated to the new multipliers.

### Step 2 — Stop silently swallowing the R:R rejection (F2)

**Target:** `app/services/trade_management.py::compute_tp_sl_pct`

Decision required — implement **one** of these:

- **Option A (recommended):** Do not catch-and-fallback when structure exists but R:R fails. Re-raise `ValueError` (or return a sentinel `(None, None)` plus a reason flag) so the **launcher guardrail** is the single owner of R:R. Update `compute_tp_sl_pct` signature to return the exit source / failure reason in a way the caller logs.
- **Option B (safer, less invasive):** Keep fallback but emit a `warn` log + structured feedback (via `helpers.emit_*` / `_record_execution_feedback`) whenever a dynamic exit is discarded. Add the underlying reason (e.g. `structural_sl_missing`, `rr_below_1.8`) to the emitted metadata so it surfaces in `/debug` and HISTORY.

Also add an assertion / defensive check in `_launcher_evaluate_signals()` (or the executor) that **logs when an MR signal ends up with effective R:R < 1.0**, since that signals the fallback path fired.

### Step 3 — Anchor structural SL to validated structure, not the forming wick (F3)

**Target:** `app/services/strategies/mean_reversion.py` structural sizing block.

1. **Evaluate on the same closed-candle set**: use the entry candle that produced `buy_signal`/`sell_signal` (the *previous closed* candle), not `ohlcv_compact[-1]` (the live bar). The cleanest fix: anchor SL to the **swing low/high** or **VA low/high** (`indicators["swing_low"]` / `indicators["value_area_low"]`) rather than the current candle wick at all.
2. **Raise the buffer**: set `structural_sl_buffer_atr` default from `0.15 → 1.0` (and expose in CFG/`defaults.py`). The checkout spec is 2.0–3.0× ATR; 1.0 is a defensible minimum that still survives wicks without being unreachable.
3. **Anchor to macro levels**: prefer (in order) `swing_low`/`swing_high` (minus/plus buffer), then VA edge, then closed-candle wick + buffer. Only fall back to the forming wick when nothing else exists, and log that fallback.
4. **Carry the closed-candle structure through** — if you reuse `_build_closed_candle_snapshot`, ensure the structural levels (`swing_low`, `value_area_low`, `ohlcv`) use the *trimmed* payload. Currently `_build_closed_candle_snapshot` trims only the target symbol's OHLCV/indicators; confirm the structure/swing values it propagates are consistent (see F5).

### Step 4 — Regime-breakdown exit for open MR positions (F4)

**Target:** `app/services/market_service.py::_check_trade_management` or a new companion method `_check_strategy_regime_exits()`.

- For each open launcher-tracked position opened by `mean_reversion`, re-evaluate the strategy's **HTF regime gate** (`htf_regime_allows` with `htf_regime_preference="chop"`). When the gate flips from pass→block (HTF became trending) while the position is open and losing (below breakeven), close it (reduce-only) and log reason `mean_reversion_regime_breakdown`.
- Respect `reentry_cooldown_seconds` and the existing launcher/trade-mgmt state (`_launcher_in_position`, `_seed_trade_mgmt_state`) so it doesn't conflict with breakeven/partial handling — flatten **after** those, or only when none apply.
- Gate behind `config["launcher"]["strategies"]["mean_reversion"]["exit_on_regime_breakdown"]` (new key, default `False` to preserve live behavior until tuned), or reuse `use_structural_sizing`.

### Step 5 — Surface structural-exit degradation (F5)

- In `mean_reversion.py`, when the thesis-specific `tp_target`/`sl_level` could not be materialized (missing `bb_middle`, `curr_low/high`, VA, etc.), `helpers.emit_debug` a message naming the missing field and the fallback used. This makes `/debug` actionable.
- Optionally add a per-strategy counter surfaced in the `/debug` or STRATEGY page.

### Step 6 — Wire `use_adaptive_atr` into structural geometry (F6, optional)

- Extend `use_adaptive_atr` to scale the **structural** SL buffer / clamp bounds, not just the raw `atr_pct`. Keep default behavior unchanged (off).

### Step 7 — Entry-quality tuning (only after 1–5, if R:R guardrail blocks persist)

Per the repo strategy-tuning guidance: treat R:R guardrail blocks as a signal to tighten **entry**, not to loosen guardrails. Candidate knobs for volatile altcoins (default→proposed):
- `max_adx`: 28 → 24
- `candle_rejection_pct`: 30 → 40 (requires stronger exhaustion wick)
- `bb_proximity_pct`: 0.5 → 0.3 (requires deeper band penetration)
- `rsi_oversold`: 30 → 25 / `rsi_overbought`: 70 → 75
- Enable opt-in liquidity gates: `require_price_in_va: True`, `require_no_extreme_funding: True`, `require_balanced_book: True` (with `funding_max_abs_rate`, `imbalance_min/max` defaults)

---

## 5. Config Defaults Summary (before → after, proposed)

| Key | Before | After | Rationale |
|---|---|---|---|
| `atr_tp_multiplier` | 1.0 | 1.8 | real R:R vs <50 % win rate |
| `atr_sl_multiplier` | 1.0 | 1.0 | wider SL to survive wicks |
| `use_structural_sizing` | False | True | restore thesis-specific exits |
| `structural_sl_buffer_atr` | 0.15 | 1.0 | outside the noise band |
| `exit_on_regime_breakdown` | (n/a) | False, opt-in | flatten MR on chop→trend flip |

(Steps 1–6 are code/defaults; Step 7 is tuning that should follow only after backtest/OOS validation.)

---

## 6. Test Plan

1. **Regression:** `poetry run pytest tests/ -q` (all 69 must pass).
2. **MR suite:** `tests/test_strategies.py` — verify new TP/SL multipliers, structural SL anchoring on closed-candle vs forming-wick, and that a missing `bb_middle` logs the fallback.
3. **Trade-management:** add/adjust a test for the regime-breakdown exit (open MR position + HTF trending → flatten, respects cooldown).
4. **Unified sizing:** `tests/test_backtest.py` — confirm `compute_tp_sl_pct` no longer (or explicitly) swallows R:R failures; assert exit source metadata.
5. **Backtest validation:** re-run the MR parameter sweep with strict **in-sample train / out-of-sample validate** splits and walk-forward windows (per the objective's §4) **before** applying Step 7 defaults, to avoid over-fitting the new number choices.

---

## 7. Recommended Sequencing for the Implementer

1. **Step 1 + Step 2 + Step 3** are the core geometry fix — do these together and re-run the backtest first.
2. **Step 5** (logging) alongside 2–3 so degradation is visible from the start.
3. **Step 4** (regime-breakdown exit) second wave.
4. **Step 6** optional.
5. **Step 7** (entry tightening + liquidity gates) last, and only after OOS/walk-forward validation.