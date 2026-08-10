# Design: Per-Strategy Analysis Timeframe

**Status:** Implemented (2026-08-10)
**Date:** 2026-08-10
**Scope:** Analysis + execution (the strategy's timeframe drives both signal
generation and TP/SL/sizing), with live/backtest alignment.

> **Implementation status:** All 7 implementation steps are complete and the
> test suite passes. See §8 for what was implemented and any notes.

---

## 1. Motivation

Today the bot runs every launcher strategy on a **single global OHLC timeframe**
(`MarketService._ohlc_bar`). The effective default is set on the **CFG page**
("Analysis Timeframe" selector, `pages.py:7112`), which loads from
`config["ta_timeframe"]` — persisted in Postgres via `load_ta_timeframe`
(`main.py:364`) and currently `1H`. (`MarketService.DEFAULT_TIMEFRAME = "4H"`
is only the code-level fallback when no stored value exists.) All five
strategies read the *same* `indicators` bundle computed on that one bar.

But the strategies have fundamentally different natural cadences:

| Strategy | Best regime | Natural analysis cadence |
|---|---|---|
| Mean Reversion | chop | fast (15m) — fade overextension |
| Liquidity Sweep | chop | fast (15m) — microstructure stop-runs |
| VWAP Reversion | chop / controlled | medium (15m–1H) |
| Trend Pullback | established HTF trend | slow (1H) — join the trend |
| Spike Continuation | expansion | fast (15m) — ride the impulse |

Forcing all of them onto one timeframe means some are always on the wrong
cadence. The goal is to let each strategy declare its **own analysis timeframe**
so it sees indicators computed on the candles it actually trades.

---

## 2. Current architecture (what exists today)

```
MarketService._ohlc_bar (global, e.g. "1H" — set from CFG page)
   ├─ _fetch_ohlcv(symbol)      → bar = self._ohlc_bar
   │     └─ _compute_indicators(ohlcv) → indicators["ohlcv"], rsi, adx, bb, vwap, ...
   └─ _fetch_ohlcv_htf(symbol)  → bar = _HTF_MAP[self._ohlc_bar]  (e.g. "4H")
         └─ indicators["htf_indicators"], adx_htf, choppiness_htf
```

- `_HTF_MAP` (`market_service.py:148`): `15m→1H`, `1H→4H`, `4H→1D`, `1D→""`.
- `Strategy.evaluate(symbol, snapshot, strat_cfg, helpers)` reads
  `snapshot["market_data"][symbol]["indicators"]` — one bundle for all strategies.
- Backtest `SnapshotBuilder.build()` mirrors this: computes LTF indicators on
  `self._ltf_candles` and HTF indicators on `htf_for(ltf_timeframe)`.
- `_build_closed_candle_snapshot()` recomputes indicators on closed candles for
  live evaluation (aligns live with backtest — see
  `/memories/repo/backtest_live_divergence.md`).

### Key constraint
The `Strategy.evaluate()` protocol receives a **pre-built snapshot**. A strategy
cannot request different candles at evaluation time. Any per-strategy timeframe
must be resolved **upstream** (in the snapshot builder) and exposed in the
snapshot.

---

## 3. Design

### 3.1 Config: `analysis_timeframe` per strategy

Add a `analysis_timeframe` key to each strategy's config (in `defaults.py`). The
defaults below are **now the active defaults** (2026-08-10). Set a strategy's
`analysis_timeframe` to `None` (UI `·`) to fall back to the global LTF:

| Strategy | Default `analysis_timeframe` |
|---|---|
| mean_reversion | `"15m"` |
| liquidity_sweep | `"15m"` |
| vwap_reversion | `"15m"` |
| trend_pullback | `"1H"` |
| spike_continuation | `"15m"` |

> The mechanism is timeframe-agnostic; these defaults are the starting point
> and are best validated/tuned by a backtest sweep.

### 3.2 Snapshot carries a per-timeframe indicator map

Instead of a single `indicators` bundle, the snapshot exposes a map keyed by
timeframe. Each strategy reads its own timeframe's block.

```
snapshot["market_data"][symbol]["indicators"]            # global LTF (unchanged, for TP/SL + fallback)
snapshot["market_data"][symbol]["timeframes"]["15m"]     # indicators computed on 15m
snapshot["market_data"][symbol]["timeframes"]["1H"]      # indicators computed on 1H
snapshot["market_data"][symbol]["timeframes"]["1H"]["htf_indicators"]  # HTF of 1H = 4H
```

Each `timeframes[<tf>]` block is the **same shape** as today's `indicators`
(produced by `_compute_indicators`), plus its own `htf_indicators` /
`adx_htf` / `choppiness_htf` derived from `_HTF_MAP[<tf>]`.

### 3.3 Which timeframes get computed?

Only the **distinct set** of timeframes requested by any *enabled* strategy is
fetched and computed. If all strategies use `"15m"`, only `15m` is computed —
no extra cost. The global LTF is always kept (execution/TP-SL + fallback).

### 3.4 Strategy reads its own timeframe

Each strategy's `evaluate()` resolves its analysis block:

```python
tf = cfg.get("analysis_timeframe", "15m")
block = (sym_data.get("timeframes") or {}).get(tf) or indicators  # fallback to global
```

Then it reads `rsi`, `adx`, `bollinger_bands`, `ohlcv`, `adx_htf`, etc. from
`block` instead of `indicators`. The `htf_regime_preference` gate (added
2026-08-10) continues to work — it reads `adx_htf`/`choppiness_htf` from the
strategy's own block, so the HTF is now the HTF **of the strategy's timeframe**.

### 3.5 Execution follows the strategy timeframe

Because the user wants analysis **and** execution on the strategy timeframe:

- **TP/SL placement** uses the strategy block's `atr_pct`, `bollinger_bands`,
  `structure` (swing levels), and `ohlcv` (wick extremes) — not the global LTF.
- **Order sizing** (notional) uses the strategy block's `atr_pct` for
  volatility-adjusted sizing.
- The **entry price** still comes from the real-time ticker (`last_price`),
  unchanged — only the *analysis* inputs to TP/SL/sizing change.

### 3.6 Backtest alignment

`SnapshotBuilder` must reproduce the same per-timeframe map:

- Fetch candles for each distinct requested timeframe (in addition to the
  global LTF and its HTF).
- At each step, compute `timeframes[<tf>]` from the candles of that timeframe
  closed at or before the current eval candle.
- The HTF of each `timeframes[<tf>]` block = `htf_for(<tf>)`.
- The finer-LTF evaluation loop (`eval_mode == "finer_ltf"`) must bucket the
  eval candles into each strategy's timeframe the same way it buckets the LTF.

This keeps live and backtest aligned (per `/memories/repo/backtest_live_divergence.md`).

---

## 4. Files to change

| File | Change |
|---|---|
| `app/services/strategies/defaults.py` | Add `analysis_timeframe` to each strategy default |
| `app/services/market_service.py` | `_build_snapshot()`: fetch + compute `timeframes[<tf>]` for the distinct requested set; expose map |
| `app/services/market_service.py` | `_build_closed_candle_snapshot()`: recompute each strategy's timeframe on closed candles |
| `app/services/strategies/*.py` (5) | `evaluate()` reads its own `timeframes[<tf>]` block |
| `app/services/backtest/snapshot_builder.py` | Build `timeframes[<tf>]` map from per-tf candles |
| `app/services/backtest/engine.py` | Fetch per-tf candles; finer-LTF bucketing per strategy tf |
| `app/services/backtest/data_fetcher.py` | `htf_for()` already generic; add multi-tf fetch helper |
| `app/ui/pages.py` | Per-strategy `analysis_timeframe` selector + save-persist + defaults |
| `tests/test_liquidity_gates.py`, `tests/test_strategies.py` | Tests for per-tf resolution + fallback |

---

## 5. Risks & mitigations

| Risk | Mitigation |
|---|---|
| **Extra REST calls** (one per distinct tf per symbol) | Only compute the distinct set of *enabled* strategies' tfs; reuse the existing candle pool + cache (`_latest_ohlcv`). |
| **Live/backtest divergence** | Update `SnapshotBuilder` in the same change; reuse `_compute_indicators`/`_compute_structure` for both. |
| **Closed-candle bias** | `_build_closed_candle_snapshot` recomputes each tf on closed candles, matching backtest. |
| **TP/SL on a different tf than entry** | Execution reads the strategy block's ATR/structure; entry price stays ticker-based. |
| **Config drift** | `analysis_timeframe` defaults preserve current behaviour; UI selector + defaults setter keep them in sync. |
| **HTF of a strategy tf may be "" (e.g. 1D)** | `htf_regime_allows` already treats missing HTF as neutral (never blocks). |

---

## 6. Open questions

1. **Default timeframes** — are the proposed defaults (15m for MR/sweep/VWAP/SC,
   1H for TP) the right starting point, or should they be validated by a backtest
   sweep first?
2. **Global LTF role** — should the global LTF remain the execution/TP-SL
   fallback, or should each strategy's tf fully own execution? (Design assumes
   the former.)
3. **Screener / dual-universe** — **RESOLVED** — see §6.1 below. Per-strategy tf
   does **not** couple to screener universe selection; the interaction is
   resolved by the `htf_regime_preference` gate on the strategy's own HTF.
4. **UI** — one selector per strategy card, or a single global "analysis
   timeframe" with per-strategy overrides?

### 6.1 Screener / dual-universe interaction (decision)

**Decision: keep the screener and per-strategy timeframe decoupled.**

The screener (`run_screener_if_due`, `market_service.py:4615`) scores symbols
from **24-hour ticker data** (`get_tickers("SWAP")` → `open24h`/`high24h`/
`low24h`/`volCcy24h`), not from OHLCV candles. It has **no timeframe at all** —
it is a coarse "which coins are worth watching" universe filter. The per-strategy
`analysis_timeframe` is a fine "how do I trade this coin" signal filter. These
are orthogonal axes and should stay that way.

The strategy→universe routing (`_strategy_allowed_on_symbol`) is **regime-based,
not timeframe-based**, and is already correct:
- SC + Trend Pullback → SC (trending) universe
- MR + Sweep + VWAP → MR (chop) universe

**The real risk — cadence mismatch.** The screener's MR universe selects for
"chop" on a 24h window, but a strategy's intraday TF may disagree (e.g. a coin
choppy on 24h but trending on 15m → MR on 15m gets stopped out). This is the
"catching falling knives" failure mode.

**Resolution:** rely on the `htf_regime_preference` gate (already built) as the
regime-consistency check. The HTF of the strategy's *own* TF is the natural
"does the higher context agree with my universe label" filter:
- MR on 15m with `htf_regime_preference="chop"` → HTF is 1H. If 1H is trending,
  the gate blocks the 15m MR entry — directly resolving the cadence mismatch.
- Trend Pullback on 1H with `htf_regime_preference="trend"` → HTF is 4H. The 4H
  trend confirms the pullback is in a real trend, matching the SC universe label.

**Explicitly rejected:** making the screener run per-TF scoring. It would
multiply cost across the whole SWAP universe, duplicate the regime logic the
`htf_regime_preference` gate already provides, and create a circular dependency
(screener needs TF → TF needs universe → universe needs screener).

**Optional follow-up (diagnostic only, not a coupling):** add a per-strategy TF
regime field to the screener's debug output (e.g. "MR universe, but 15m is
trending — gate will block") so operators can see when the gate is doing work.

---

## 7. Suggested implementation order

1. Add `analysis_timeframe` to `defaults.py` (defaults preserve behaviour).
2. Extend `_compute_indicators` usage: build `timeframes[<tf>]` map in
   `_build_snapshot` for the distinct requested set.
3. Update the 5 strategies to read their own block (with fallback to global).
4. Update `_build_closed_candle_snapshot` for closed-candle alignment.
5. Update `SnapshotBuilder` + `engine.py` for backtest parity.
6. Add UI selector + save-persist + defaults.
7. Add tests; run full suite.

---

## 8. What was implemented (2026-08-10)

All 7 steps are complete. The `analysis_timeframe` defaults are **15m for
MR/SC/sweep/VWAP and 1H for trend pullback** (active, matching the "Set
Recommended Defaults" buttons). Set a strategy's `analysis_timeframe` to `None`
(UI `·`) to fall back to the global LTF.

- `defaults.py`: `analysis_timeframe` on all 5 strategies (15m/15m/15m/1H/15m).
- `strategies/__init__.py`: new `resolve_analysis_block(sym_data, cfg)` helper —
  returns `sym_data["timeframes"][<tf>]` when the strategy sets
  `analysis_timeframe` and the block exists, else falls back to
  `sym_data["indicators"]`.
- `market_service.py`:
  - Refactored `_fetch_ohlcv`/`_fetch_ohlcv_htf` into a shared
    `_fetch_ohlcv_bar(symbol, bar, cache)`; added per-tf cache
    `_latest_ohlcv_tf` (keyed by (symbol, bar)).
  - Added `_requested_analysis_timeframes()` (distinct tfs from enabled
    strategies) and `_build_timeframes_block(symbol, bar)` (computes an
    indicator block + its own HTF layer).
  - `_build_snapshot`: fetches the requested tfs + their HTFs and exposes
    `entry["timeframes"][<tf>]` per symbol. Clears the per-tf cache on symbol
    removal.
- All 5 strategies: read their analysis block via `resolve_analysis_block`.
- `backtest/snapshot_builder.py`: `__init__` accepts `tf_candles`; `build` and
  `build_with_incomplete_ltf` expose `timeframes[<tf>]` via a shared
  `_build_block`.
- `backtest/engine.py`: `_requested_tfs()` fetches per-tf candles and passes
  them to the `SnapshotBuilder`; `_compute_tp_sl` resolves the strategy's
  analysis block for dynamic TP.
- `pages.py`: per-strategy "Analysis timeframe" selector (`·`/15m/1H/4H/1D)
  with save-persist + defaults setter.
- Tests: `TestResolveAnalysisBlock` + per-strategy timeframe read/fallback
  tests. Full suite passes.

### Notes
- Only the distinct set of tfs requested by *enabled* strategies is fetched,
  plus each one's HTF (for the regime gate). No extra cost when all strategies
  use `analysis_timeframe=None` (global LTF).
- The `·` UI value maps to `None` (use global LTF).
- Live/backtest aligned: both resolve the same `timeframes` map and read the
  strategy's analysis block.
