# Improve VWAP Reversion — Findings & Implementation Handoff

> **Status:** Analysis-complete, no code changed.
> **Author:** Senior Quantitative review (2026-08-10)
> **Scope:** `VWAPReversionStrategy` — 15m timeframe (`analysis_timeframe: "15m"`), volatile altcoin markets.
> **Audience:** A senior engineer who will implement the fixes.
> **Reference:** `.github/skills/launcher-strategy-reference.md` (§ VWAP Reversion)

---

## 1. TL;DR

The VWAP Reversion strategy's *conceptual framing* is correct for volatile 15m alts
(price stretched ≥ `vwap_min_distance_atr` from VWAP should revert toward it; the extended →
closing-back-toward-VWAP → not-a-strong-trend gate stack is the right fade-the-knife shape,
not a naive dip-buyer). The thesis is sound **only in the regime it gates itself into**, and the
`htf_regime_preference: "chop"`, `max_adx`, and BB-bandwidth gates are all appropriate.

The failures are concentrated in **the VWAP anchor, the timing of the distance gate, and the
asymmetric structural TP/SL geometry** — not the regime framing. Most live trades are stopped out
(confirmed by the operator; `use_adaptive_atr` enabled, strategy on 15m). The dominant flaws:

1. **The "VWAP" is NOT session-anchored (HIGH severity, code-level).**
   `ta.vwap(high, low, close, volume)` in `market_service.py:6091` computes a **cumulative VWAP
   anchored at the first bar of the loaded dataframe** — a rolling ~200-bar 15m window (~50h).
   That is not the intraday magnet institutions/programs actually target (the **day's**
   volume-weighted average). Consequences:
   - It re-anchors as old bars roll out, so the anchor drifts deterministically with the window —
     a large old-volume bar dropping off steps VWAP with no relation to current trading.
   - The "distance from VWAP" that triggers entries is therefore often spurious or decaying
     right after entry → stop-outs within the first few candles.
   - The same has applied to every backtest (`w200` cache files confirm the same rolling window).

2. **The distance gate reads live `last_price`, but the closeback/HTF gates read closed bars
   (MEDIUM-HIGH severity, correctness).**
   `distance = last_price - vwap_value` uses the live ticker, while `require_closeback`
   (`ohlcv_compact[-1]` vs `[-2]`) and HTF trend (`ema_50/ema_200`) use the previous **closed**
   15m candle. On volatile alts a live print can be far from the last close, so the distance
   arm-bandits the entry (triggers a fade on a live whipsaw print the closed-bar logic already
   contradicts). All entry gates should share the same reference price.

3. **Structural TP-at-VWAP vs. ATR-clamped SL gives structurally low R:R → guardrail blocks and
   tight-stop wick-outs (MEDIUM-HIGH severity).**
   Entry happens `2–3 ATR` from VWAP (so TP-at-VWAP is that same ~2–3 ATR away), but the SL sits
   **beyond the extension candle's extreme** (`curr_low/high ± 0.15 ATR`) which is *farther* from
   entry than the TP because you entered on an extended candle. That is an inherently
   low-R:R geometry: the structural SL is almost always the wider side. It is then clamped to
   `atr_max_sl_mult = 3.0`, and when the clamp pulls it tight toward entry you get a stop that a
   volatile 15m wick can hit immediately → stop-out churn. `require_reward_risk_ratio` may also
   block valid fades for the same reason.

4. **Triple regime filters intersect too rarely (MEDIUM severity).**
   The strategy gates on **HTF regime** (`htf_regime_preference: "chop"` via `htf_regime_allows`),
   **BB-bandwidth percentile** (`require_regime`, `max_bb_bandwidth_percentile = 55`), **and**
   `max_adx = 25` — three independent "not-trending" filters. On volatile alts where chop and
   micro-trend oscillate, the intersection fires rarely → long dry spells; and they can disagree
   because they read different timeframes (HTF = 1H derived from the strategy's 15m; BB/ADX on
   LTF 15m). Pick one primary regime filter.

5. **`use_adaptive_atr` is a hidden behavior change (MEDIUM severity).**
   `vwap_reversion.py:184` scales `atr_pct` by up to **2.5×** at high vol *before* computing the
   distance gate and sizing. That silently widens the SL and makes the extension threshold harder
   to hit in exactly the volatile regime you want to trade — it fights the already-bad §3 geometry.

Recommended fix priority: **#1 (VWAP anchor) → #2 (consistent reference price) → #3 (TP/SL
geometry) → #4 (regime gate consolidation)**, then **#5 (adaptive ATR)** only if it survives
backtest under the new geometry. Each must be validated with OOS/walk-forward backtesting before
being adopted live.

---

## 2. Relevant Files / Line References

| Concern | Location |
|---|---|
| VWAP strategy logic / signal & sizing | `app/services/strategies/vwap_reversion.py` |
| VWAP canonical defaults | `app/services/strategies/defaults.py` (`DEFAULT_VWAP_REVERSION`, lines ~158–190) |
| VWAP indicator computation | `app/services/market_service.py:6091` (`ta.vwap(...)`), assembled ~6187–6188 |
| Unified TP/SL manager | `app/services/trade_management.py` (`OrderContext`, `calculate`, `compute_tp_sl_pct`, `_ensure_rr` ≥ 1.8) |
| Adaptive ATR scaling | `app/services/strategies/vwap_reversion.py:184` (read from `cfg`/config) |
| HTF regime gate | `app/services/indicator_service.py` (`htf_regime_allows`) |
| Analysis-block resolution | `app/services/strategies/__init__.py` `resolve_analysis_block()` |
| BB-bandwidth percentile | `app/services/strategies/__init__.py` `compute_bb_bandwidth_percentile()` |
| Per-strategy timeframe design | `docs/objectives/per_strategy_timeframe.md` (implemented 2026-08-10) |
| Strategy reference | `.github/skills/launcher-strategy-reference.md` (§ VWAP Reversion) |

---

## 3. Diagnostic Findings (root causes)

### F1 — VWAP is NOT session-anchored (HIGH severity, code-level)

```python
# market_service.py:6091 — inside _compute_indicators()
vwap_series = ta.vwap(high=df["high"], low=df["low"], close=df["close"], volume=df["volume"])
# ... then vwap = vwap_series.iloc[-1]
```
`pandas_ta.vwap` is **cumulative from the start of the passed dataframe**. The strategy's analysis
block is a rolling ~200-bar 15m window (`w200`), so the "VWAP" is anchored ~50 hours back and
slides with the window. It is **not** the current session's volume-weighted average.

Why this feeds stop-outs:
- Every new closed bar drops the oldest bar out of the window. A high-volume old bar exiting
  causes a step in VWAP unrelated to current flows — the `distance_atr` that triggered entry
  decays or inverts as the anchor rolls, but the stop already sits tight to it.
- The magnet you're trading toward is a blended multi-session average, not today's defended
  level, so price frequently reverts to the *blend* and then keeps going, stopping the trade.

### F2 — Distance gate uses live `last_price`, closeback/HTF use closed bars (MEDIUM-HIGH)

`vwap_reversion.py`:
```python
last_price = helpers.get_last_price(symbol)          # LIVE ticker
...
distance = last_price - vwap_value                    # LIVE print
distance_atr = abs(distance) / atr_price              # gates entry
```
but:
```python
_prev_close = ... ohlcv_compact[-2].get("close")      # previous CLOSED candle
_curr_close = ... ohlcv_compact[-1].get("close")      # previous CLOSED candle
# closeback_long_ok = distance < 0 and _curr_close > _prev_close   (uses closed bars)
# HTF trend from ema_50/ema_200 on closed bars
```
The entry gates mix a live reference (distance) with closed-bar references (closeback, HTF).
On a fast 15m alt, `last_price` can be far from the last close → distance fires on a live whipsaw
print, or fails to fire when the closed-close is legitimately extended. Inconsistent snapshot in a
single decision.

### F3 — Structural TP-at-VWAP vs. clamped wide SL → low R:R / tight stop wick-outs

```python
# vwap_reversion.py, structural sizing
if side == "long":
    tp_target = vwap_value
    sl_level = curr_low - structural_sl_buffer_atr * atr_price    # below extension low
# (short mirrors with curr_high + buffer)
```
Because you enter only when price is `>= 2.0 ATR` extended from VWAP, `tp_target = vwap_value`
is only ~2–3 ATR away, while the SL is beyond the *extension* candle (≥ entry minus the full
extended move) — i.e. generally the **wider** of the two. `trade_management._ensure_rr` requires
`RR >= 1.8`, and the ATR clamps (`atr_min_sl_mult=0.3` / `atr_max_sl_mult=3.0`) can:
- Leave a wide SL → `require_reward_risk_ratio` blocks the fade (guardrail blocks), or
- Clamp it tight toward entry → a volatile 15m wick stops the trade immediately (stop-out churn).

This is the exact structural-sizing failure surface documented in the strategy reference and
tuning playbook. On this strategy the TP-at-VWAP is the thesis (not negotiable), so the fix must
come from entry geometry, not R:R gymnastics.

### F4 — Triple redundant regime filters reduce signal frequency (MEDIUM)

- `htf_regime_allows(adx_htf, chop_htf, "chop")` — block when HTF (1H) is trending.
- `max_adx = 25` on LTF 15m ADX — block strong trends on the analysis timeframe.
- `require_regime` + `max_bb_bandwidth_percentile = 55` on LTF — block volatile expansion.

These read **different timeframes** (HTF 1H vs LTF 15m) and can disagree / both fire rarely on
volatile alts that oscillate between chop and micro-trend. Triple intersection → long dry spells.
They should be consolidated (see Step 4).

### F5 — `use_adaptive_atr` silently warps both the gate and the sizing (MEDIUM)

```python
# vwap_reversion.py:184
if atr_pct is not None and cfg.get("use_adaptive_atr", False):
    if atr_pct < 1.5:      atr_pct *= 1.20
    elif atr_pct < 3.0:    atr_pct *= 1.80
    else:                  atr_pct *= 2.50
```
`atr_pct` is scaled **before** both (a) `atr_price = (atr_pct/100)*last_price` → the distance
gate (`distance_atr = |distance|/atr_price` gets *smaller*, so the `>= 2.0 ATR` entry threshold is
harder to reach), and (b) the SL clamp math (handed down as `atr_tf_pct`, so the SL *widens*).
At high vol it simultaneously makes entries rarer and stops wider — diametrically opposed to
what a volatile-alt 15m reversion needs. Confirmed enabled in the operator's live config.

---

## 4. Implementation Steps (for the engineer)

### Step 1 — Session-anchor the VWAP used by the reversion strategy (F1)

**Target:** `app/services/strategies/vwap_reversion.py` (and/or `market_service.py`/snapshot builder).

The cleanest fix is to make VWAP a **per-session cumulative** rather than a rolling-window
cumulative. Options in increasing level of effort:

1. **(Preferred) Session-block cumulative VWAP.** When computing the VWAP series, reset the
   cumulative sum at each UTC session boundary (e.g. UTC midnight for daily, matching OKX funding
   day). Compute `vwap` as the latest session's volume-weighted average (not the whole window).
   Implement a small helper (e.g. `session_vwap(high, low, close, volume, ts, session="day")`)
   in `app/services/indicator_service.py` or `market_service.py`, and consume its last value in
   both the live path (`_compute_indicators`) and the backtest snapshot
   (`app/services/backtest/snapshot_builder.py`). Keep the raw `vwap_series` for display but make
   the **strategy-facing `vwap`** session-anchored.
2. **(Minimal diff, still an improvement)** Re-center the window to a fixed lookback anchored at
   session start rather than a free-rolling 200-bar tail, so at least the anchor is stable within
   a session.

**Verify:** unit test that (a) two sessions in the window produce a VWAP equal to the *current*
session's cumulative, (b) a large-volume bar from an older session does **not** step the current
session VWAP, (c) the rolling (legacy) series remains available for display.

### Step 2 — Use ONE reference price for all entry gates (F2)

**Target:** `app/services/strategies/vwap_reversion.py`.

Replace the live `last_price` in the **gate** computations with the previous **closed** candle's
close, and reserve `last_price` (live/ticker) only for **execution sizing / order construction**:

```python
# Gate reference: previous closed 15m close
ref_price = helpers.extract_float(ohlcv_compact[-1].get("close"))
# use ref_price for: distance = ref_price - vwap, distance_atr, closeback, and any gate-level
# ATR-price. Keep last_price only for OrderContext.last_price / entry_price at execution.
```
- Move the `ohlcv_compact` read above the distance gate (it is currently fetched later, inside the
  closeback block).
- If `ohlcv_compact[-1]` is unavailable (short history), fall back to `last_price` and emit a debug
  line so the path is transparent.

**Verify:** unit test that a live `last_price` far from the last close no longer flips the distance
gate; gate decisions agree with the closed-close references for closeback and HTF.

### Step 3 — Fix the structural TP/SL geometry (F3)

**Target:** `app/services/strategies/vwap_reversion.py` + `defaults.py`.

Entry geometry is the lever; TP-at-VWAP stays. Preferred changes:

1. **Raise the minimum extension** so the TP-hop to VWAP is meaningful relative to the structural
   SL. If `vwap_min_distance_atr` moves up (e.g. `2.5–3.0`), entry sits farther from VWAP → the TP
   distance grows while the SL (beyond the extension extreme) stays bounded → better R:R at the
   structural level and fewer guardrail blocks. Expose this as a tunable default, do NOT hard-code.
2. **Keep `atr_max_sl_mult` from clamping to a wick-able stop.** Consider a per-strategy
   `atr_min_sl_mult` floor high enough that the stop survives an ordinary 15m wick (e.g. `0.5–0.7`
   on volatile alts) — but only after backtesting; a too-high floor re-creates the wide-SL problem.
3. **Gate R:R at the strategy layer consistently** — currently `compute_tp_sl_pct` returns
   `(static_tp_pct, static_sl_pct)` on `ValueError` from the R:R check, silently dropping the
   structural geometry and falling back to ATR/static. Keep the launcher guardrail
   (`require_reward_risk_ratio`) as the decision owner, but **emit a debug line naming the
   structural vs fallback sizing source** so `/debug` shows *why* a structurally-sized trade was
   downgraded (mirrors the stale `_sizing_source` audit issue found in the LS handoff).
4. Consider `dynamic_tp` / fractional TP (already available) if backtest shows full-VWAP targets
   rarely get hit on volatile alts.

**Verify:** unit test that with default entry parameters the structural R:R is computed, and that
the fallback-to-static path emits an auditable debug line instead of silently re-sizing.

### Step 4 — Consolidate the regime gates (F4)

**Target:** `app/services/strategies/vwap_reversion.py` + `defaults.py`.

Keep **one** primary "not-trending" filter; demote the others to rationale-only. Recommended
primary: the BB-bandwidth percentile gate (`require_regime`), because it reads the analysis TF
(15m) and directly encodes "chop where VWAP reversion is reliable". Options:

- If the HTF regime gate (`htf_regime_preference: "chop"`) is kept, **drop** the LTF `max_adx`
  gate for VWAP (or raise it), since the two read the trend on different timeframes and double
  penalty when they disagree.
- Or keep `max_adx` as the LTF trend guard and treat the BB gate as a *soft* filter (log but do
  not block), validating the choice in backtest.
- Do **not** silently drop a gate on live before the backtest comparison (see Test Plan #5).

**Verify:** unit test that a name passing LTF-BB-chop but failing HTF-trend (or vice versa)
behaves per the chosen primary gate, and that the debug rationale reports the secondary filter
status.

### Step 5 — Review/reconcile `use_adaptive_atr` (F5)

**Target:** `app/services/strategies/vwap_reversion.py` + config.

After Steps 1–3, `use_adaptive_atr` (confirmed enabled) needs a decision, not silent behavior:

1. **Apply the adaptive scaling to sizing ATR only, NOT to the distance gate** — compute the entry
   distance with the *unscaled* ATR so the extension threshold is stable, and apply adaptive
   scaling only to `atr_tf_pct` used in `OrderContext` / SL clamp math. This decouples "how far is
   the entry" from "how wide is the risk".
2. Re-run the backtest with adaptive ATR on vs off under the new geometry; if the 2.5× widening at
   high vol re-creates the wide-SL/stop-out problem (§3), set the live default to off.

**Verify:** unit test that `use_adaptive_atr` no longer shifts the distance gate, and that the
backtest comparison (Test #5) covers on vs off.

---

## 5. Config Defaults Summary (before → after, proposed)

| Key | Before | After (proposed) | Rationale |
|---|---|---|---|
| VWAP anchor | rolling 200-bar cumulative (`ta.vwap`) | session-anchored cumulative (current session) | fade today's defended level, not a multi-session blend |
| gate reference price | live `last_price` | previous closed 15m close (± live for sizing) | consistent snapshot; kill live-whipsaw arm-banding |
| `vwap_min_distance_atr` | 2.0 | 2.5–3.0 (tune in backtest) | TP-hop to VWAP meaningful vs structural SL → better R:R |
| `atr_min_sl_mult` | 0.3 | 0.5–0.7 (backtest first) | stop survives a 15m wick; do not over-tighten |
| structural R:R fallback | silent downgrade to static | emit audit debug line | know when structural geometry was dropped |
| `max_adx` | 25 | keep only if it's the primary LTF guard | consolidate triple regime filters |
| `require_regime` (BB pct) | True, 55 | keep as primary "chop" filter | most on-thesis on the analysis TF |
| `htf_regime_preference` | chop | keep (if chosen primary) | only one primary trend filter |
| `use_adaptive_atr` | enabled, 2.5× cap | off, OR apply to sizing-only (not the gate) | decouple entry distance from risk width |

(Steps 1–2 are correctness; Steps 3–4 + 5 are geometry/robustness. Validate everything with the
OOS/walk-forward backtest below before adopting new defaults live.)

---

## 6. Test Plan

1. **Regression:** `poetry run pytest tests/ -q` (all 69 must pass).
2. **Session VWAP (F1):** unit tests for two-session windows (current-session VWAP, old-bar
   step immunity), plus live/backtest snapshot parity.
3. **Reference price (F2):** unit test that a live `last_price` far from the last close no longer
   flips the distance gate.
4. **TP/SL geometry (F3):** assert structural R:R improves with higher `vwap_min_distance_atr`;
   assert the `[trade_mgmt]` → fallback path emits an audit debug line.
5. **Backtest comparison:** config-switch backtest comparing (a) rolling vs session-anchored VWAP,
   (b) gate-on-live vs gate-on-closed-close, (c) `vwap_min_distance_atr` 2.0 vs 2.5/3.0,
   (d) adaptive-ATR on vs off — using strict **in-sample train / out-of-sample validate** splits
   and walk-forward windows before adopting any new default.
6. **Regime consolidation (F4):** verify one primary gate behaves correctly on synthetic
   chop-vs-micro-trend and that the debug rationale surfaces the secondary filter status.

---

## 7. Recommended Sequencing for the Implementer

1. **Step 1 (session-anchor VWAP)** and **Step 2 (consistent reference price)** first — both are
   correctness fixes and compound directly on the "most live trades stopped out" symptom. Do them
   together, then re-run the backtest comparison before touching anything else.
2. **Step 3 (TP/SL geometry)** next — raise `vwap_min_distance_atr`, add the audit debug line.
3. **Step 4 (regime consolidation)** — pick one primary trend filter, demote the others.
4. **Step 5 (adaptive ATR)** last, and only in the direction the backtest supports (sizing-only or
   off). Do **not** flip any gate/default live before OOS/walk-forward validation.