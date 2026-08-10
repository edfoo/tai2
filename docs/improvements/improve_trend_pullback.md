# Improve Trend Pullback Strategy

**Strategy file:** `app/services/strategies/trend_pullback.py`
**Canonical defaults:** `app/services/strategies/defaults.py` (`DEFAULT_TREND_PULLBACK`)
**Shared exit model:** `app/services/trade_management.py` (`calculate`, `compute_tp_sl_pct`, `_ensure_rr`)
**Launcher R:R guardrail:** `app/services/market_service.py` (~lines 9710–9785)
**UI panel:** `app/ui/pages.py` (~lines 4374–4625, save-persist ~5663–5685)
**Tests:** `tests/test_strategies.py` (`TestTrendPullbackStrategy`, `_make_tp_snapshot`, `_tp_bare`, `_make_pullback_ohlcv`), `tests/test_liquidity_gates.py` (`TestTrendPullbackLiquidity`)

---

## Goal

Fix the Trend Pullback (TP) strategy so it performs as an actual trend-aligned pullback
entry on **volatile altcoin symbols on the 15m timeframe** — it must stop producing
structurally sub-1.5 R:R exits that get silently degraded instead of rejected, and stop
using a fixed-% proximity band and a 1H signal/1H-ATR sizing mismatch that fight the
15m microstructure it actually trades.

The following fixes are listed in **priority order**. Implement all of them. Do not
"cherry-pick" — the first two are correctness, the rest are necessary for the strategy
to actually function on its target market.

---

## Background / Current behaviour

TP joins the main trend after price pulls back to value (EMA21 or VWAP) in an established
HTF trend, confirmed by a rejection candle off the level. It fills the gap between SC
breakouts (too late) and MR extremes (wrong in a trend).

Diagnosed problems (verify each against the code before changing):

1. Structural TP/SL geometry produces sub-1.5 R:R on 15m volatile alts, and
   `compute_tp_sl_pct` **swallows** the `calculate()` R:R rejection and silently degrades
   to poor ATR/static fallbacks (1.2:1 and 1.33:1). The launcher's `min_reward_risk_ratio`
   default of 1.0 lets all of them through.
2. `pullback_proximity_pct = 0.3%` is a fixed percentage band on a volatility-scaled
   market — too tight to trigger cleanly, and when it does trigger it catches the knife.
3. Timeframe mismatch: `analysis_timeframe = "1H"` means the pullback level, candle
   confirmation, and ATR% are all **1H** values, while the launcher executes at **15m**
   granularity. 1H ATR% is larger than 15m ATR%, so ATR-fallback stops are even wider
   relative to 15m noise.
4. ADX band `[20, 28]` is narrow and lagging for volatile alts — starves entries or
   admits late ones.
5. `use_adaptive_atr` scales ATR% *before* both the `min_atr_pct` gate and sizing,
   compounding the volatility mismatch.
6. Structural SL is anchored to the pullback candle's wick (`curr_low`), which on 15m
   volatile alts is a noisy extreme → wide stop or tight-clamp churn.

---

## Fix 1 — Unify exit model and raise R:R (PRIORITY, correctness)

### Problem

The structural geometry (TP at nearest swing high, SL beyond pullback candle low) is
asymmetric in the wrong direction on 15m volatile alts: TP is typically close (price just
pulled back *from* that high), SL is typically wide (wick-anchored). This yields
sub-1.5 R:R.

The shared exit model enforces `_ensure_rr(...) >= 1.8` inside `calculate()`, but
`compute_tp_sl_pct` catches the `ValueError` and returns degraded fallbacks instead of
rejecting:

```python
try:
    tp_price, sl_price = calculate(entry, side, ctx)
    ...
except ValueError:
    return static_tp_pct, static_sl_pct
```

The fallbacks are themselves poor:

| Source | `atr_tp_multiplier` | `atr_sl_multiplier` | R:R |
|---|---|---|---|
| Class docstring in `trend_pullback.py` | 2.0 | 1.5 | 1.33 |
| Inline code fallback in `evaluate()` | 2.0 | 1.5 | 1.33 |
| **`DEFAULT_TREND_PULLBACK` (canonical, used via `merged_config`)** | **1.2** | **1.0** | **1.2** |
| Static fallback (`tp_pct`/`sl_pct`) | 4.0 | 3.0 | 1.33 |

The launcher's `require_reward_risk_ratio` guardrail (`market_service.py:9728`) defaults
`min_rr = 1.0` and only reads a per-strategy override if `min_reward_risk_ratio` is set in
the strategy config. So all of the above pass.

### Requirements

- In **all three places** (class docstring, inline `evaluate()` fallbacks,
  `DEFAULT_TREND_PULLBACK`) set the **same** values:
  - `atr_tp_multiplier = 3.0`
  - `atr_sl_multiplier = 2.0`
  - This yields ≥ 1.5 R:R when ATR sizing is active.
- In `DEFAULT_TREND_PULLBACK`, set `min_reward_risk_ratio = 1.5`.
- If `use_atr_sizing` is disabled the static fallbacks (`tp_pct: 4.0`, `sl_pct: 3.0`)
  give 1.33 R:R — raise to keep the same floor (e.g. `tp_pct: 6.0`, `sl_pct: 4.0`, or keep
  ATR sizing on by default; the merged default `use_atr_sizing: True` should remain).
- **Acceptance check:** there must be no path where the effective TP/SL is below ~1.5 R:R.
  Document the single source of truth for these values and make the other two reference it
  instead of literals, so they cannot drift again.

---

## Fix 2 — ATR-normalize the pullback proximity gate (correctness)

### Problem

`pullback_proximity_pct = 0.3` is a fixed percentage band:

```python
def _near(level):
    return level is not None and level > 0 and abs(last_price - level) / level <= proximity
```

On 15m volatile alts where ATR% runs 1–3%, a 0.3% band is far too tight: price rarely sits
within it cleanly, and when it does it is usually mid-wick (knife-catch). This is the same
lesson as the VWAP reversion findings.

### Requirements

- Add a volatility-normalised proximity: `pullback_proximity_atr` (default ~0.5) such that
  a level is "touched" when `abs(last_price - level) / level <= pullback_proximity_atr * atr_pct / 100`.
- Keep `pullback_proximity_pct` as a **hard floor** (e.g. 0.3%) so the band never collapses
  to zero on dead coins, but the effective band is `max(pullback_proximity_pct, pullback_proximity_atr * atr_pct)`.
- Update `DEFAULT_TREND_PULLBACK`, the class docstring, the inline fallback, the UI panel
  (`pages.py` ~line 4410 `tp_proximity_input`), the save-persist block (`pages.py` ~line 5670),
  and `_set_tp_defaults()` (`pages.py` ~line 4600).
- **Acceptance check:** on a 2% ATR name the effective band is ~1% (not 0.3%); on a 0.5%
  ATR name it floors at 0.3%. A clean pullback to value triggers; a mid-wick touch does not.

---

## Fix 3 — Resolve the 1H-signal / 15m-execution mismatch

### Problem

`analysis_timeframe = "1H"` means `resolve_analysis_block` returns the **1H** indicator
block. The pullback level (EMA21/VWAP), the candle-rejection confirmation, and ATR% are all
1H values, while the launcher polls and executes at 15m. Consequences:

- The "pullback to value" is a 1H event, but the fill happens at whatever 15m price is live
  when the 1H candle closes — often not the best intra-candle price.
- The entry candle confirmation (`close > prev close`, lower wick ≥ 25%) is evaluated on a
  **1H candle**, so the 15m microstructure where the fill happens is invisible to the signal.
- ATR% is 1H ATR%, which is *larger* than 15m ATR% — so ATR-fallback stops are even wider
  relative to 15m noise.

### Requirements

- Decide the intended execution timeframe. For a 15m volatile-alt scalp, set
  `analysis_timeframe = "15m"` (the global LTF) so the pullback level, candle confirmation,
  and ATR% are all 15m values, and the HTF trend gate (`htf_indicators`) still reads the
  strategy's own HTF (15m→1H via `_HTF_MAP`).
- If a 1H signal is genuinely desired, then **size the exits to 15m noise**, not 1H ATR:
  pass the 15m `atr_pct` into `OrderContext.atr_tf_pct` for the ATR fallback and clamps,
  and keep the 1H ATR only for the HTF volatility multiplier.
- Update `DEFAULT_TREND_PULLBACK.analysis_timeframe`, the UI default
  (`pages.py` `_set_tp_defaults` sets `"1H"`), and the save-persist default.
- **Acceptance check:** the strategy's effective ATR% used for sizing matches the timeframe
  on which the fill occurs. No path where a 1H ATR% sizes a 15m stop.

---

## Fix 4 — Widen / de-lag the ADX band

### Problem

`min_adx = 20` / `max_adx_for_entry = 28`. On volatile altcoins ADX ramps fast and is a
lagging measure: by the time ADX confirms a trend at 20+, the pullback is often already
deep, and by 28+ the trend is frequently extended (the exact "late entry" failure mode TP
must avoid). The 8-point band is a thin window that either starves entries or admits late
ones.

### Requirements

- Widen the band, e.g. `min_adx = 18`, `max_adx_for_entry = 40` (or 0 = disabled).
- Prefer replacing the upper bound with a **volatility-normalised extension check** (price
  not more than `max_pullback_extension_atr × ATR%` past the pullback level) rather than a
  raw ADX cap, since ADX is lagging.
- Update `DEFAULT_TREND_PULLBACK`, the class docstring, the inline fallbacks, the UI panel
  (`pages.py` `tp_min_adx_input` / `tp_max_adx_entry_input`), the save-persist block, and
  `_set_tp_defaults()`.
- **Acceptance check:** clean pullbacks in a genuine trend are not excluded by a premature
  ADX cap; late entries are still blocked by the extension check.

---

## Fix 5 — Make adaptive ATR sizing-only (or off)

### Problem

```python
if atr_pct is not None and cfg.get("use_adaptive_atr", False):
    if atr_pct < 1.5: atr_pct *= 1.20
    elif atr_pct < 3.0: atr_pct *= 1.80
    else: atr_pct *= 2.50
```

This inflates `atr_pct` **before** the `min_atr_pct` gate **and** before sizing. At high
volatility it simultaneously makes entries rarer (higher effective ATR% floor) and stops
wider (larger ATR% in the fallback). Same anti-pattern as VWAP reversion.

### Requirements

- Apply the adaptive scaling **only to sizing** (the ATR fallback / clamps), not to the
  `min_atr_pct` gate.
- Or set `use_adaptive_atr = False` by default and rely on the unified exit model (Fix 1).
- Update `DEFAULT_TREND_PULLBACK`, the class docstring, the inline fallback, and the UI
  default (`pages.py` `_set_tp_defaults`).
- **Acceptance check:** high-volatility regimes do not simultaneously starve entries and
  widen stops.

---

## Fix 6 — Re-anchor the structural SL off the wick

### Problem

```python
if curr_low is not None:
    sl_level = curr_low - structural_sl_buffer_atr * atr_price   # longs
```

Anchoring the stop to the pullback candle's low on a 15m volatile alt means the stop sits at
the extreme of a noisy wick. Combined with `atr_max_sl_mult = 3.0`, this either produces a
very wide stop (poor R:R, blocked or degraded) or gets clamped tight (15m wick stop-out
churn).

### Requirements

- Anchor the structural SL to a **structural invalidation** rather than the single candle's
  wick — e.g. below the swing low that defines the pullback structure (longs), or below the
  pullback level minus a volatility buffer.
- Keep the ATR clamps (`atr_min_sl_mult` / `atr_max_sl_mult`) as a sanity bound, but the
  *anchor* should be structural.
- Emit an audit debug line when the structural SL falls back to the wick anchor or gets
  clamped, so the geometry is observable.
- **Acceptance check:** the SL is not placed at the extreme of a single 15m wick; stop-outs
  are driven by structural invalidation, not noise.

---

## Files to touch (complete list)

| File | Change |
|---|---|
| `app/services/strategies/trend_pullback.py` | Fixes 1, 2, 3, 4, 5, 6 (inline fallbacks, ATR-normalised proximity, sizing-timeframe, ADX band, adaptive-ATR scope, structural SL anchor) |
| `app/services/strategies/defaults.py` | `DEFAULT_TREND_PULLBACK`: ATR multipliers 3.0/2.0, `min_reward_risk_ratio` 1.5, static `tp_pct`/`sl_pct` floor, `pullback_proximity_atr`, `analysis_timeframe` 15m, ADX band, `use_adaptive_atr` |
| `app/ui/pages.py` | TP panel (~4374–4625): new `pullback_proximity_atr` input, updated defaults in `_set_tp_defaults` (~4595), save-persist block (~5663–5685) |
| `tests/test_strategies.py` | `TestTrendPullbackStrategy`: update `_tp_bare` defaults; add tests for ATR-normalised proximity, R:R floor, sizing-timeframe, ADX band |
| `tests/test_liquidity_gates.py` | `TestTrendPullbackLiquidity`: keep POC gate tests passing with new proximity logic |

---

## Validation

- Run the full suite: `poetry run pytest tests/ -q` (currently 69 tests; failures beyond the
  0 pre-existing ones are regressions).
- Backtest on a comparable multi-day 15m volatile-alt universe (see
  `backtest_cache/cli/20260809_090606_15m_results.json` for the reference 17-trade run).
- **Acceptance check:** TP trade count is meaningful (not starved to near-zero), win rate
  stays ≥ 55%, and loses are bounded (max drawdown comparable to or better than the
  reference). No path where the effective TP/SL is below ~1.5 R:R.
