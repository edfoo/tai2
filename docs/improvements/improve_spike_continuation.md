# Improve Spike Continuation Strategy

**Strategy file:** `app/services/strategies/spike_continuation.py`
**Canonical defaults:** `app/services/strategies/defaults.py` (`DEFAULT_SPIKE_CONTINUATION`)
**Shared exit model:** `app/services/trade_management.py` (`compute_tp_sl_pct`)
**OI momentum helper:** `app/services/strategies/liquidity_helpers.py` (`oi_confirms_momentum`)
**Backtest reference:** `backtest_cache/cli/20260809_090606_15m_results.json` (17 trades across 5 strategies)

---

## Goal

Fix the Spike Continuation (SC) strategy so it performs as an actual momentum scalp on
**volatile altcoin symbols on the 15m timeframe** — it must stop being filtered into
near-zero trades and stop using exits that cannot survive its own whipsaw propensity.

The following fixes are listed in **priority order**. Implement all of them. Do not
"cherry-pick" — the first three are correctness, the rest are necessary for the strategy
to actually function on its target market.

---

## Background / Current behaviour

SC rides volume-driven momentum spikes for a short move (intended 3–5%) before they revert.
It enters **with** the spike, not against it. The critical design rule is "don't enter at the
top of a spike" — momentum must still be accelerating, not peaking.

Diagnosed problems (verify each against the code before changing):

1. Inconsistent ATR exit multipliers → 1.2:1 R:R that cannot survive whipsaw.
2. Spike-origin is anchored to a trailing window low, which miscounts extension on straight-line impulses.
3. `require_momentum_acceleration` and `max_spike_extension_pct` conflict and collapse the entry window.
4. Genuine bug: OI gate direction is evaluated once as `"long"` and reused for shorts.
5. Conjunctive filter stack + `rsi_max` band starves the strategy on the very names it targets.

---

## Fix 1 — Unify exit model and raise R:R (PRIORITY, correctness)

### Problem

Three sources disagree on the ATR multipliers:

| Source | `atr_tp_multiplier` | `atr_sl_multiplier` | R:R |
|---|---|---|---|
| Class docstring in `spike_continuation.py` | 2.2 | 2.0 | 1.1 |
| Inline code fallback in `evaluate()` | 2.2 | 2.0 | 1.1 |
| **`DEFAULT_SPIKE_CONTINUATION` (canonical, used via `merged_config`)** | **1.2** | **1.0** | **1.2** |

`compute_tp_sl_pct` branch #2 returns `(atr_tp_multiplier·atr_pct, atr_sl_multiplier·atr_pct)`
verbatim, so the launcher's `require_reward_risk_ratio` guardrail evaluates the degraded 1.2:1
geometry, sees `1.2 ≥ min_reward_risk_ratio(1.0)`, and passes it. A 1.0×ATR stop is a *wick stop*
on 15m volatile alts, not a volatility buffer.

### Requirements

- In **all three places** (class docstring, inline `evaluate()` fallbacks, `DEFAULT_SPIKE_CONTINUATION`)
  set the **same** values:
  - `atr_tp_multiplier = 3.0`
  - `atr_sl_multiplier = 2.0`
  - This yields ≥ 1.5 R:R when ATR sizing is active.
- In `DEFAULT_SPIKE_CONTINUATION`, set `min_reward_risk_ratio = 1.5`.
- If `use_atr_sizing` is disabled the static fallbacks (`tp_pct: 4.0`, `sl_pct: 3.0`)
  give 1.33 R:R — raise to keep the same floor (e.g. `tp_pct: 6.0`, `sl_pct: 4.0`, or keep
  ATR sizing on by default; the merged default `use_atr_sizing: True` should remain).
- **Acceptance check:** there must be no path where the effective TP/SL is below ~1.5 R:R.
  Document the single source of truth for these values and make the other two reference it
  instead of literals, so they cannot drift again.

---

## Fix 2 — Fix OI gate direction mismatch on shorts (correctness bug)

### Problem

In `spike_continuation.py`, `oi_confirms_momentum` is called **once** with
`direction="long"` and the result is reused for both buy and sell signals:

```python
oi_ok, oi_info = oi_confirms_momentum(open_interest, direction="long", ...)
...
sell_signal = ( ... and (not require_oi_confirmation or oi_ok) )
```

In `liquidity_helpers.oi_confirms_momentum` the z-score branch returns:
`ok = oi_zscore > min_zscore` for longs, but `ok = oi_zscore < -min_zscore` for shorts.
So a short currently requires **rising** OI (long confirmation) — the inverse of correct
momentum confirmation for a short.

### Requirements

- Compute OI confirmation **separately for each direction**:
  - `oi_ok_buy = oi_confirms_momentum(open_interest, direction="long", ...)`
  - `oi_ok_sell = oi_confirms_momentum(open_interest, direction="short", ...)`
- Wire `oi_ok_buy` into `buy_signal` and `oi_ok_sell` into `sell_signal` (only when
  `require_oi_confirmation` is true).
- Add a regression test `TestSpikeContinuationLiquidity` (in `tests/test_liquidity_gates.py`)
  that asserts: a short is **blocked** when `oi_zscore` is strongly positive, and **allowed**
  when `oi_zscore` is strongly negative (mirror of the existing long tests).
- **Acceptance check:** with `require_oi_confirmation` on, a long requires `oi_zscore > +min_zscore`
  and a short requires `oi_zscore < -min_zscore`. No shared `oi_ok` reuse.

---

## Fix 3 — Re-anchor spike origin to the volume-expansion candle

### Problem

Current logic uses the trailing window low as the spike origin:

```python
spike_origin_low = min(lows)   # min low over last spike_lookback candles (excl current)
spike_extension_buy = (current_close - spike_origin_low) / spike_origin_low * 100
```

For a straight-line 15m impulse the "origin" is the pre-move consolidation low many candles
back, so state-of-the-move is inflated and SC wrongly blocks early entries. On a chop-then-up
pattern the origin is close and a late 6% leg reads as "not extended." The metric measures the
wrong anchor.

### Requirements

- Anchor the spike origin to the candle where **volume expansion began**, not a trailing min/max.
- Concretely: set the origin as the **low of the first candle in the lookback window whose
  volume-RSI exceeded `volume_rsi_min`** (the candle that started the spike). For shorts use the
  corresponding **high**.
- Keep the existing `spike_lookback` window for the search, but the origin is event-anchored,
  not a window extreme.
- If no qualifying volume-expansion candle exists in the window, treat extension as **not
  confirmed** (block) so we never enter with a meaningless origin.
- Update the debug output to log the anchor candle timestamp/index used.
- **Acceptance check:** a fresh impulse that started one candle ago and is still accelerating
  is measured as *low* extension (allowed), while a move 6% off the volume-expansion candle on
  the current leg is measured as *high* extension (blocked).

---

## Fix 4 — De-conflict acceleration vs. extension filters

### Problem

- `require_momentum_acceleration` demands current body ≥ `acceleration_min_ratio` (1.3×) the
  average of the last `acceleration_lookback` (3) candles.
- `max_spike_extension_pct` (3.5%) demands price not be more than 3.5% past the origin.

On a fast alt rack, a 1.3× accelerating body almost always travels > 3.5% from the
consolidation low in one candle. Together the gates produce an almost-empty passing set and are
the main reason SC contributes so few of the 17 backtest trades.

### Requirements

- Keep **one** anti-late-entry filter, the ATR-anchored one.
- Replace the raw-`%` `max_spike_extension_pct` gate with a **volatility-normalised** check:
  price may be at most `max_spike_extension_atr` × ATR`% from the (re-anchored, Fix 3) origin
  per candle travelled. Default `max_spike_extension_atr` ≈ 2.0.
- Either drop `require_momentum_acceleration` (the body-ratio check) or make it opt-in and
  default-off. It encodes the same "not too late" intent as the extension gate, so it must not
  compound it.
- Update `DEFAULT_SPIKE_CONTINUATION`, the class docstring, the inline fallbacks, and the UI
  panel in `app/ui/pages.py` (SC section, ~lines 3519–3640) to reflect the renamed key
  (`max_spike_extension_atr` replacing `max_spike_extension_pct`).
- **Acceptance check:** a clearly accelerating fresh breakout is allowed; a slow drift that has
  already travelled > 2×ATR from the volume-expansion origin is blocked. The two filters should
  no longer be able to veto the same trade for opposite reasons.

---

## Fix 5 — Loosen the entry window and de-stack the conjunctive filters

### Problem

The full entry requires (simultaneously): RSI in `55–72`, vol RSI ≥ 72, vol RSI **rising**,
BB breakout (`price ≥ BB upper`), candle close in top 25% of range (`candle_strength_pct: 70`),
body acceleration, RSI rising, spike extension OK, BB-bandwidth percentile ≥ 55, and ATR% ≥ 1.0.
On 15m volatile alts the cleanest impulses push RSI ≥ 75 within a candle or two, so the
`rsi_max: 72` band systematically excludes the best momentum, and the volume-RSI
both-conditions filter is noisy on thin books.

### Requirements

- Raise `rsi_max` to ~80 (and mirror the sell band to `100 - rsi_max`), so the strongest
  impulses are not excluded. Keep `rsi_min` low enough to require genuine momentum (e.g. 55).
- Make `require_volume_rsi_rising` (rising vol RSI) **optional/default-off**; keep
  `volume_rsi_min` as the primary volume gate.
- Relax `candle_strength_pct` from 70 to ~60 (close within top 40% of range) so candle-strength
  does not fight the acceleration gate.
- Review the BB-bandwidth-percentile regime gate default (currently 55) and lower to ~50 so it
  no longer starves entries in moderate expansions.
- **Acceptance check:** backtest trade count for SC is meaningfully above ~5 in a comparable
  multi-day 15m alt universe, while win rate stays ≥ 55% and loses are bounded (max drawdown
  comparable to or better than the 2.12% reference).

---

## Files to touch (complete list)

| File | Change |
|---|---|
| `app/services/strategies/spike_continuation.py` | Fixes 1, 2, 3, 4, 5 (inline fallbacks, oi_ok_buy/oi_ok_sell, origin re-anchor, gate priority) |
| `app/services/strategies/defaults.py` | Unify ATR exits, R:R, new `max_spike_extension_atr`, looser RSI/volume gates |
| `app/ui/pages.py` | SC config panel: rename keys, update hints/defaults to match |
| `tests/test_liquidity_gates.py` | Add short-direction OI regression test |
| `tests/test_strategies.py` | Update SC tests + `_make_spike_snapshot` for re-anchored origin & renamed config keys |

## Guardrails / invariants you MUST preserve

- Never touch `compute_tp_sl_pct`'s function signature or its branch priority; the strategy
  supplies the multipliers via `OrderContext`/params as today.
- Keep `min_reward_risk_ratio` override flow intact (blank inherits global) in the UI.
- SC must still require **trending** HTF by default (`htf_regime_preference: "trend"`); do not
  flip the regime gate to favour choppy markets — that is Mean Reversion's job.
- Keep `analysis_timeframe: "15m"` for SC.
- Preserve graceful degradation (no OI data → gate passes) for all OI/volume gates.
- Document the single source of truth for the ATR multipliers so the docstring, inline fallback,
  and canonical defaults cannot diverge again.

## How to verify

```bash
poetry run pytest tests/test_strategies.py tests/test_liquidity_gates.py tests/test_screener_dual_universe.py -q
```

Then run a comparable multi-day 15m backtest on the same volatile-alt universe
(`AEON,BEAT,BICO,EDEN,HMSTR`-style names) with SC enabled and confirm:
- meaningful trade count (was ~17 across 5 strategies),
- the OI short gate behaves directionally correctly,
- effective R:R is ≥ 1.5 on executed trades (check backtest output / logs, not just config),
- max drawdown does not exceed the 2.12% reference.

---

*If any requirement in this document is ambiguous, ask the requester (or inspect
`DEFAULT_SPIKE_CONTINUATION` and `merge_config`/`merged_config` in `app/services/strategies/`
for the canonical config flow) before guessing.*