# Design: Hourly Wick Rejection Short (`wick_rejection`)

**Status:** Planned (redesigned 2026-11-18 from the manual-trade analysis — §1.1; vol screener §3.5)
**Date:** 2026-10-28 (revised 2026-11-18)
**Scope:** New launcher strategy with a clock-aligned entry trigger, forming-candle
exposure, strategy-owned exits (TP + emergency stop + time stop), per-strategy
leverage and margin sizing, and a dedicated `vol` screener universe, with
live/backtest parity.

---

## 1. Motivation

Fast-moving alt-coins that have printed one or more **large green 1H bodies** in
the last few hours tend to **dip for a few seconds to minutes right after each
new hour opens**. The user traded this manually for two weeks with a profit:

- find a coin with one or more large green 1H bodies in the last 2–4 hours;
- near the end of the hour, if price is close to the top of the forming candle,
  **short in the last 0–2 minutes** (as close to the hour as possible);
- take a **small, close TP**. It usually hits within 5–10 minutes, even after the
  trade goes against you first (−20% on margin was seen).

The strategy is deliberately narrow:

- **Universe:** fast-moving coins only, from a dedicated `vol` screener list
  ranked on *speed* (§3.5).
- **Setup:** a green 1H body ≥ `min_body_ratio` × the average body of the
  prior 8 candles, within the last `setup_lookback_candles` hours (§3.1).
- **Trigger:** the clock. Entry fires `entry_seconds_before_close` before the
  hour (default 40 s) **when** price sits in the top slice of the forming
  candle's range (§3.1, §3.6).
- **Direction:** SHORT only.
- **Exit:** a small static TP (default 0.5% price ≈ 2.5% on margin at 5×), a
  wide **emergency stop** and a **time stop** (§3.2). No breakeven, partial TP
  or trailing.

> **R:R is deliberately not a constraint.** The TP is ~0.5% against an emergency
> stop of ~7%. The strategy is judged on win rate and expectancy, so it sets
> `skip_rr_guard = True` (§3.3).

### 1.1 Evidence: the user's manual trades

`scripts/analyze_manual_trades.py` (read-only) pulled the user's OKX position and
fill history for 2026-10-24 19:00 → 2026-11-09 19:00 SAST (17:00 UTC). Bot
trades were excluded by `clOrdId` prefix. **33 shorts entered in the last
3 minutes of the hour** match the strategy (the other 80 manual trades were a
different strategy).

| Metric | Value |
|---|---|
| Win rate | **85%** (28 / 33) |
| Net PnL | **+35.72 USDT**; worst trade −7.47 USDT |
| Leverage | 5× (all but one) |
| Entry timing | median **39 s** before the hour; p10 11 s, p90 76 s |
| Hold time | median **2.9 min**; p75 11 min; p90 30 min; max 81 min |
| Exit move (price) | median **+0.49%**, p75 0.63% (median **+2.15% on margin**) |
| Max adverse excursion (margin) | median 3.5%, p75 7.0%, p90 13.1%, **max 29.7%** |
| Best move after the hour (price) | 1 min **0.32%**, 5 min **0.61%**, 10 min **1.11%** (medians) |
| High offset at entry `(H−P)/(H−L)` | median **18%**; p25 12%, p75 28%; only 5/33 ≤ 10%, 20/33 ≤ 20% |
| Forming 1H range | median 5.6%; p10 1.9% |
| Largest green body, last 4 closed 1H (× avg prior 8) | median **3.0×**; 16/33 ≥ 3×, only 6/33 ≥ 5× |
| Forming body (× avg prior 8) | median 2.4× |
| Trades in the same hour | never more than 1 (a manual limit: one browser, not a rule of the strategy) |
| Funding at the hour | ≈ 0 (not the driver) |

What this means for the defaults:

- **The stated 5× body rule and the 5–10% high offset are tighter than what was
  traded.** A 5× rule keeps 6 of 33 trades; a 10% offset keeps 5. The defaults
  below start from the data (3×, 20%), and the backtest sweeps both down to
  the stated values.
- Losers were mostly entered far from the high (3 of 5 at 30–45% offset).
- **Outside the window it fails hard:** a later INIT trade (21:59 SAST on Nov 9)
  went 85% against the position on margin and was closed at −78% (−73 USDT),
  twice the window's total profit. An emergency stop at 35% on margin would
  have closed none of the 33 in-window trades and capped INIT. This is why the
  emergency stop is on by default (§3.2).

---

## 2. Current architecture (what exists today)

### Strategy protocol
`app/services/strategies/__init__.py` defines:

```python
class Strategy(Protocol):
    name: str
    def evaluate(self, symbol, snapshot, config, helpers) -> StrategySignal | None: ...
```

`StrategySignal(direction, strategy_name, tp_pct, sl_pct, rationale)`.

Helpers: `resolve_analysis_block(sym_data, cfg)`, `merged_config(config, name)`,
`compute_bb_bandwidth_percentile`, `fractal_swings`.

### Registries
| Location | Purpose |
|---|---|
| `MarketService._strategies` (`market_service.py`) | live evaluation |
| `_AVAILABLE_STRATEGIES` (`backtest/engine.py`) | backtest evaluation |
| `DEFAULT_STRATEGIES` (`backtest/runner.py`) | CLI defaults |
| `DEFAULT_STRATEGY_CONFIG` (`strategies/defaults.py`) | canonical defaults |
| `STRATEGY_SWEEP_PARAMS` (`backtest/sweep_catalog.py`) | parameter sweeps |

### TP/SL resolution (shared live + backtest)
`launcher_tp_sl.resolve_launcher_tp_sl` returns `LauncherTpSl(action, tp_price,
sl_price, disable_protection, skip_rr_guard, flipped, ...)`. Both
`MarketService.build_launcher_decisions` and `BacktestEngine._compute_tp_sl`
call it.

### Guardrails that block entries
| Guard | Bypass |
|---|---|
| `stop_loss_required` / `require_protection` | `decision["_disable_protection"]` (strips **both** TP and SL; MR-only today) |
| R:R ratio | `decision["_skip_rr_guard"]` |
| min leverage | `leverage_override_reason` |
| `max_position_pct` | none — caps **notional** at `equity × max_position_pct` |

### Leverage and sizing
`target_leverage` is derived in `_execute_decision` from notional / equity, then
clamped to `[min_leverage, max_leverage]` and the tier cap. There is **no
per-strategy leverage override**. Launcher sizing is a fixed
`launcher.notional_usd` for every strategy.

### Trade management
`_check_trade_management` (live) and the simulator apply **one global**
`strategy.trade_management` config to every launcher position: software stop,
breakeven, partial TP, trailing, R-based time stop. All of these are measured in
R (multiples of the SL distance). WR needs none of them except a plain time stop
and a stop, and its R is meaningless (0.5% TP vs 7% stop).

### Scheduler cadence
`PromptScheduler` ticks every `interval_seconds` (≥ 30, default 300), not
aligned to the clock. A WR entry at H:59:20 cannot ride this tick.

### Forming candle
Live evaluates the **last closed** candle: `_build_closed_candle_snapshot`
trims the forming candle. The backtest's `finer_ltf` mode
(`SnapshotBuilder.build_with_incomplete_ltf`) builds snapshots with an
in-progress last candle; `evaluation_timeframe` defaults to `1m`.

### Screener
`screener.py`: `build_candidates`, `compute_recent_activity`, `score_universe`,
`universe_for_strategy`. Routing: `MarketService._strategy_allowed_on_symbol`
(live), `UniverseSchedule.universe_at` (backtest).

---

## 3. Design

### 3.1 Strategy funnel

`WickRejectionStrategy.evaluate()` runs an ordered funnel. Each stage vetoes
before the next and emits a debug line with the measured value.

| # | Stage | Gate | Default |
|---|---|---|---|
| 0 | Enabled | `cfg["enabled"]` | `False` |
| 1 | Forming candle | `indicators["forming_candle"]` present | — |
| 2 | Entry window | `min_seconds_to_close <= seconds_to_close <= max_seconds_to_close` | `10` / `90` |
| 3 | Forming green | `close > open` (opt-out `require_forming_green`) | `True` |
| 4 | Range | `(H − L) / L × 100 >= min_candle_range_pct` | `1.5` |
| 5 | High offset | `(H − P) / (H − L) × 100 <= max_high_offset_pct` | `20.0` |
| 6 | Big-body setup | count of candles with `body_ratio >= min_body_ratio` in the setup window `>= min_big_bodies` | `3.0`, `1` |
| 7 | HTF regime (opt-in) | `htf_regime_allows(...)` | `"off"` |

Definitions:

- `seconds_to_close = (candle_ts + bar_ms − now_ms) / 1000`. Live uses wall-clock
  `now`; the backtest uses the eval step's timestamp. With 1m eval, the step that
  sees the forming candle at H:59:00 gets `seconds_to_close = 60`.
- `P` is the forming candle's current close (live: the latest price at trigger
  time).
- `body_ratio(i) = (C_i − O_i) / mean(|C_j − O_j| for the body_avg_window candles before i)`,
  `0` for a red candle. This matches the analysis script's `_body_ratio`.
- The **setup window** is the last `setup_lookback_candles` (default 4) closed
  1H candles, plus the forming candle when `setup_include_forming=True`
  (default `True`; the forming body is measured to `P`).
- Zero-range candle (`H == L`) or zero average body → reject, never divide.

On success:

```python
StrategySignal(
    direction="sell",
    strategy_name="wick_rejection",
    tp_pct=<resolved>, sl_pct=<resolved>,
    rationale="WickRejection: 38s to close, offset 14% of 6.2% range, body 4.1x (2h ago)",
)
```

The signal also carries a `rank_score` for §3.6 (`body_ratio_max × (1 − offset/100)`).

**Why the high offset is range-relative:** a price-relative gate `(H − P) / P`
can never exceed the candle's own range, so on a typical 4% candle it passes
every price including the low. `(H − P) / (H − L)` scales with volatility.

### 3.2 Exits — strategy-owned

WR positions **opt out of global trade management**
(`use_global_trade_management = False`, default). The R-based rules (breakeven
at 0.3R, partial TP at 0.8R, trailing) would fire almost at once on a 0.5% TP
vs a 7% stop. WR has three exits:

| Exit | Where | Default |
|---|---|---|
| **TP** | exchange algo order | `tp_pct = 0.5`, `tp_unit = "price"` |
| **Emergency stop** | exchange algo order (`emergency_stop_mode = "exchange"`) | `emergency_stop_pct = 35.0`, `emergency_stop_unit = "margin"` |
| **Time stop** | software, market close | `max_hold_minutes = 60` |

- **Units.** `"price"` is % move of the entry price; `"margin"` is % PnL on
  margin, converted with the strategy leverage: `price_pct = margin_pct / leverage`.
  So 35% on margin at 5× is a stop 7% above entry, and a TP of 2.5% on margin is
  0.5% price.
- **Emergency stop modes.** `"exchange"`: an OKX SL algo order. It still
  protects the position when the bot is down, and it satisfies
  `stop_loss_required` without bypassing guardrails. `"software"`: no SL order;
  the monitor closes at market when the level is crossed. This addresses the
  worry that a visible stop gets hunted, at the cost of protection while the
  app is offline. `"off"`: no stop at all; needs `disable_protection = True`
  (TP-only, §3.3) and is not recommended (see INIT, §1.1).
- **Time stop** closes at market after `max_hold_minutes`, win or lose. No R
  condition. Data: p90 hold was 30 min, max 81 min, so 60 is the starting
  point and the sweep covers 15/30/60/120.
- **Re-entry:** at most one WR entry per symbol per hour
  (`reentry_cooldown_minutes`, default 55).

The rule lives in **one** pure function,
`trade_management_rules.compute_strategy_exit(position, now, price, high, low, params)`
→ `{close: bool, reason: "time_stop" | "emergency_stop"}`. Live
(`_check_trade_management`) and the simulator both call it for positions whose
strategy opts out, and skip the global rules for them.

### 3.3 Guardrail changes

- `skip_rr_guard = True` from strategy config (generic: `strat_cfg.get("skip_rr_guard")`,
  OR'd with the existing flip condition).
- **TP-only protection.** Today `_disable_protection` strips both TP and SL. Add
  `_protection = "tp_only"`: place the TP algo order, skip
  `stop_loss_required`, keep `require_protection` for the TP. It is used when
  `emergency_stop_mode` is `"software"` or `"off"`.
- `disable_protection` (strip both) becomes generic too, keeping the MR condition
  as an OR so MR is unchanged.
- **`max_position_pct` stays authoritative.** At 50% margin × 5× the notional is
  2.5× equity; if `max_position_pct` is below that, the order is **capped** and
  a debug line says so. The STRATEGY card shows the effective cap.

### 3.4 Per-strategy leverage and margin sizing

`LauncherTpSl` gains `leverage_override` and `notional_override`, both resolved
in `launcher_tp_sl.py` so the backtest gets the same numbers.

```
leverage = strat_cfg["leverage"]                         # default 5
margin   = equity × margin_pct / 100   if margin_mode == "pct_equity"
         = margin_usdt                  if margin_mode == "fixed_usdt"
notional = margin × leverage
```

- `build_launcher_decisions` sets `decision["notional_usd"] = notional` (instead of
  `launcher.notional_usd`) and `decision["_leverage_override"] = leverage`.
- `_execute_decision` uses the override as `target_leverage`, clamped to
  `[min_leverage, max_leverage]` and the tier cap, and sets
  `leverage_override_reason`. **If the clamp changes the leverage**, notional is
  recomputed from margin so the margin at risk stays as configured, and the
  change is logged.
- `equity` is the account equity already fetched for sizing (live) and the
  simulator's equity (backtest).
- Defaults: `margin_mode = "fixed_usdt"`, `margin_usdt = 20`. The user traded
  50% of the portfolio (`margin_mode = "pct_equity"`, `margin_pct = 50`).
  That is available, but at that size one INIT-type trade with a 35% emergency
  stop costs ~17.5% of the account, and with no stop it cost ~39%.
- **Multiple positions.** Several symbols can fire on the same trigger, and the
  bot opens all of them up to `max_concurrent_positions` (default `3`), ranked by
  `rank_score`. Open WR positions count against the limit. The user's one trade
  per hour was a manual limit, not part of the strategy.
- **Total margin cap.** `margin_pct` and `margin_usdt` are **per position**, so
  3 × 50% of equity would ask for 150%. `max_total_margin_pct` (default `100`)
  caps the summed margin of open plus new WR positions as a % of equity. A
  signal that would exceed it is sized down to the remaining room, or skipped if
  the room is below the exchange minimum (logged either way). Execution stays
  sequential, so each entry sees the balance left by the one before
  (`_pending_notional`), the same as the scheduler's execute phase.
- Each position keeps its own TP, emergency stop and time stop.

### 3.5 Dedicated `vol` universe

A third screener list, ranked on *speed* rather than 24h aggregates. It decides
which fast-moving coins get live market data at all; the WR funnel (§3.1) still
checks the candles.

#### Speed

```
closed_speed = recent_range_pct / hours_covered            # closed candles, last activity_lookback_hours
fast_speed   = |fast_move_pct| / (fast_move_window_minutes / 60)
speed        = max(closed_speed, fast_speed)                # % per hour
```

- `closed_speed` uses **only** `compute_recent_activity()` features, divided by
  `hours_covered` (not the configured lookback). It must **never** fall back to
  the 24h `hl_range_pct` (that inflates speed ~6×). No activity data → undefined.
- `fast_speed` comes from `_fast_move_from_history()` over the screener's
  rolling price samples. It is the only term that sees the current hour, which
  lets a coin that spikes this hour reach the list before the H:59 trigger.
  No price history → undefined.
- A symbol with neither term defined cannot enter the vol list.

#### Score, gates, cap

```
vol_score = speed_norm × 0.50 + vol_ratio_norm × 0.30 + decay_norm × 0.20
```

- Normalisers are the max over the vol pool. `vol_ratio` = `recent_vol_ratio`;
  `decay` = `decay_ratio` (missing → contributes `0`).
- Gates: `speed >= vol_min_speed_pct_per_hour` (`1.0`) and
  `vol_ratio >= vol_min_vol_ratio` (`0.5`). Missing `vol_ratio` **fails**.
- Own gates only: the SC/MR `activity_min_ratio` gate does **not** apply.
- Cap `vol_max_symbols` (`8`). Built in both dual and legacy (`dual_universe=False`) mode.

#### Data fetching

- The fast move is computed for **every** candidate, independent of
  `fast_lane_enabled` (live already records price samples on every tick).
- `vol` is appended to `selected` (after SC and MR) so vol-only symbols are
  fetched live (`update_symbols(selected)`) and in the backtest
  (`UniverseSchedule.all_symbols`).

#### Routing

`universe_for_strategy` routes `wick_rejection` → `vol_symbols`, falling back to
`selected`, then `fallback_symbols`.

#### Parity limitation

Live re-runs the screener on every tick, so before H:59 `fast_speed` covers the
current hour. The backtest rebuilds the universe only at hour boundaries from 1H
candles, so a coin spiking during hour H enters the backtest's vol list at H+1.
**Accepted:** the backtest misses some fresh-spike entries live would take; it
undercounts rather than invents. Recorded in the schedule provenance.

### 3.6 Clock-aligned entry trigger (live)

A dedicated task, `WickRejectionTrigger`, started by the scheduler when WR is
enabled and `launcher.mode == "launcher_only"`:

1. Sleep until `H:00:00 − entry_seconds_before_close` (default 40 s), computed
   from wall-clock UTC each cycle (no drift accumulation).
2. For each symbol in the current vol list: fetch the last
   `body_avg_window + setup_lookback_candles + 2` 1H candles (REST, one call per
   symbol, run concurrently; 8 symbols is well inside OKX's candle rate limit)
   and the latest ticker price. Build the per-symbol snapshot through the normal
   snapshot path, with `forming_candle` set and its close = the latest price.
3. Run `build_launcher_decisions(symbol, strategies=["wick_rejection"])`. All
   existing guards apply: in-flight, already tracked, `_launcher_symbol_conflict`,
   re-entry cooldown, daily-loss lock.
4. Rank the signals by `rank_score`, keep `max_concurrent_positions` minus open
   WR positions, apply `max_total_margin_pct`, and execute them **sequentially**
   through `handle_llm_decision` (fast enough: each entry is one order plus its
   algo orders, so 3 entries fit well inside the window).
5. Log one summary line per trigger: symbols checked, the funnel stage that
   rejected each one, entries.

Rules:

- The regular scheduler tick **does not** evaluate WR (its random timing would
  fire outside the window or race the trigger).
- Execution from the trigger and from the tick share one `asyncio.Lock` around
  the execute phase, so balance sequencing and `_pending_notional` stay correct.
- If step 2 has not finished by `min_seconds_to_close`, the stage-2 gate rejects
  the late symbols and the miss is logged. A late entry is worse than none.
- `launcher.schedule = "on_close"` does not block the trigger (WR manages its own
  timing).

### 3.7 Backtest

- Requires `evaluation_mode="finer_ltf"` with `evaluation_timeframe="1m"`. The
  entry step is H:59:00 (`seconds_to_close = 60`, inside the default 10–90 s
  window); the entry price is that step's close. In `closed` mode, or with an
  eval timeframe ≥ 5m that never lands inside the window, WR produces **zero
  trades**, and the engine logs why.
- TP, emergency stop and time stop are simulated on 1m candles. When the TP and
  the emergency stop are both inside the same 1m candle, the **stop fills first**
  (conservative).
- Margin sizing and leverage come from `resolve_launcher_tp_sl`, so they match
  live.
- **Parity gap:** live enters ~40 s before the close; the backtest enters 60 s
  before. Accepted (1m is the finest OKX history), and recorded in provenance.
- Validation: the 1m backtest over the user's window, run with the user's
  observed settings, should broadly reproduce §1.1 (win rate, median hold, MAE
  distribution). If it does not, fix the simulation before tuning.

---

## 4. Implementation steps

### Phase 1 — Forming-candle exposure

1. **`market_service._build_closed_candle_snapshot`** — before trimming, copy the
   forming candle into `new_indicators["forming_candle"]` as
   `{ts, open, high, low, close, volume}`; `None` on the untrimmed path so the
   key always exists. Existing strategies ignore it.
2. **`backtest/snapshot_builder.py`** — `build_with_incomplete_ltf` sets
   `indicators["forming_candle"]` from `incomplete_candle`; `build()` sets `None`.

### Phase 2 — Strategy *(parallel with 3, 4)*

3. **New `strategies/wick_rejection.py`** — the §3.1 funnel, `body_ratio` helper,
   `rank_score`.
4. **`strategies/defaults.py`** — `DEFAULT_WICK_REJECTION` (§5), registered in
   `DEFAULT_STRATEGY_CONFIG`.
5. Register in `market_service._strategies`, `backtest/engine._AVAILABLE_STRATEGIES`,
   `backtest/runner.DEFAULT_STRATEGIES`.

### Phase 3 — TP/SL, sizing and guardrail plumbing *(parallel with 2, 4)*

6. **`launcher_tp_sl.py`**
   - WR TP/stop resolution with `tp_unit` / `emergency_stop_unit` conversion (§3.2).
   - Generic `disable_protection` (MR condition kept as OR), `skip_rr_guard`,
     new `protection = "tp_only"`.
   - `leverage_override` and `notional_override` (§3.4).
7. **`market_service.build_launcher_decisions`** — forward `notional_usd`
   override, `_leverage_override`, `_protection`; accept a `strategies=` filter.
8. **`market_service._execute_decision` / `handle_llm_decision`** — apply the
   leverage override (with margin-preserving re-clamp), the `tp_only` protection
   path, and the logged `max_position_pct` cap.

### Phase 4 — Strategy-owned exits *(parallel with 2, 3)*

9. **`trade_management_rules.py`** — `StrategyExitParams` +
   `compute_strategy_exit()` (time stop, software emergency stop).
10. **`market_service._check_trade_management`** — for positions whose strategy
    has `use_global_trade_management = False`, call `compute_strategy_exit`
    and skip the global rules. Software emergency stop is also checked from the
    positions-refresh loop (not only the scheduler tick).
11. **`backtest/simulator.py`** — the same split per position; 1m fill-order
    rule from §3.7.

### Phase 5 — Dedicated `vol` universe *(parallel with 2–4)*

12. **`screener.py`** — `score_universe` takes optional `price_history` and
    `now_ts`; computes `closed_speed`, `fast_speed`, `speed` for every candidate;
    builds `vol_pool` with its own gates in dual and legacy mode; returns `"vol"`
    and `"vol_pool_size"`, plus a diagnostic line per selected symbol; appends
    vol to `selected`. `universe_for_strategy` accepts `vol_symbols` and routes WR.
13. **`strategies/defaults.py`** — `DEFAULT_SCREENER` gains `vol_enabled`
    (`False`), `vol_max_symbols`, `vol_min_speed_pct_per_hour`, `vol_min_vol_ratio`.
14. **`backtest/universe.py`** — build `price_history` when `vol_enabled` **or**
    `fast_lane_enabled`; pass to `score_universe`; `UniverseInterval.vol`,
    `universe_at`, `to_dict`; record the §3.5 parity limitation in provenance.
15. **`market_service`** — pass `_screener_price_history` to `score_universe`;
    `_screener_vol_symbols`; `run_screener_if_due` stores `result["vol"]`;
    `_strategy_allowed_on_symbol` and `get_screener_universe` route WR.
16. **`prompt_scheduler`** — expose `runtime_config["_screener_vol_symbols"]`.

### Phase 6 — Clock trigger *(depends on 1–5)*

17. **New `app/services/wick_rejection_trigger.py`** — the §3.6 loop, started and
    stopped with the scheduler; shared execute lock with `PromptScheduler`.
18. **`prompt_scheduler._collect_launcher_decisions`** — exclude WR.
19. **`backtest/engine.py`** — WR zero-trade diagnostics for unsupported eval
    modes; provenance for the 60 s vs 40 s entry gap.

### Phase 7 — UI *(depends on 2–6)*

20. **`pages.py` STRATEGY page** — "Wick Rejection" card with
    `_set_wr_defaults()`. Groups: setup (body ratio, lookback, avg window, min
    big bodies, include forming); entry (seconds before close, window, high
    offset, range, require green); exits (TP + unit, emergency stop + mode +
    unit, max hold); risk (leverage, margin mode/pct/USDT, max concurrent,
    effective `max_position_pct` cap). `save_strategy_settings` entry.
21. **CFG page screener section** — `vol_*` inputs.
22. **LIVE page** — show "next WR trigger in mm:ss" and the last trigger's summary.
23. **`backtest/sweep_catalog.py`** — `"wick_rejection": "WR"` and a sweep block
    (§6).

### Phase 8 — Docs + tests *(depends on all)*

24. `.github/skills/launcher-strategy-reference/SKILL.md` — WR section.
25. `CLAUDE.md` — `forming_candle`, the vol universe, the WR trigger,
    strategy-owned exits, per-strategy sizing.
26. Tests:
    - `test_strategies.py` — fires on the §1.1 median shape; rejects outside the
      entry window, on a red forming candle, small range, offset above the max,
      no big body in the setup window, zero range / zero average body;
      `body_ratio` matches the script on a fixture; `rank_score` ordering.
    - New `test_launcher_tp_sl.py` — price/margin unit conversion; margin modes →
      notional; `tp_only`; generic `disable_protection`; `skip_rr_guard`;
      leverage override pass-through.
    - `test_trade_management_rules.py` — `compute_strategy_exit` time stop and
      software stop; global rules not applied to opted-out positions (live and
      simulator).
    - `test_okx.py` — leverage override clamp keeps margin constant; `tp_only`
      places TP, no SL, no `stop_loss_required` block; `max_position_pct` cap
      logged.
    - Trigger — fires at H:00 − 40 s (frozen clock); evaluates only vol
      symbols and only WR; opens several positions up to `max_concurrent_positions` by `rank_score`;
      `max_total_margin_pct` sizes down / skips the last entry;
      the regular tick never evaluates WR.
    - Vol screener — all cases from the previous revision (`hours_covered`
      divisor, no 24h fallback, quiet-then-spiking coin via `fast_speed`,
      neither term → excluded, missing `vol_ratio` fails, missing decay scores 0,
      `activity_min_ratio` ignored, legacy mode, appended to `selected`,
      `vol_enabled=False` → empty).
    - `test_backtest_universe.py`, `test_sweep_catalog.py`,
      `test_live_backtest_parity.py` — WR signal and exit parity on a 1m fixture;
      same-candle TP + stop → stop.

---

## 5. Config reference

```python
DEFAULT_WICK_REJECTION = {
    "enabled": False,
    "analysis_timeframe": "1H",
    "htf_regime_preference": "off",
    # Setup: big green bodies in the recent hours
    "min_body_ratio": 3.0,            # green body ÷ avg |body| of the prior window (user's rule: 5)
    "body_avg_window": 8,
    "setup_lookback_candles": 4,      # closed 1H candles searched for a big body
    "setup_include_forming": True,    # the forming candle (to current price) also counts
    "min_big_bodies": 1,
    # Entry
    "entry_seconds_before_close": 40, # live trigger time (H:00 − this)
    "min_seconds_to_close": 10,       # entry window gate (live + backtest)
    "max_seconds_to_close": 90,
    "require_forming_green": True,
    "min_candle_range_pct": 1.5,
    "max_high_offset_pct": 20.0,      # (H − P)/(H − L) × 100 (user's rule: 5–10)
    # Exits (strategy-owned)
    "use_global_trade_management": False,
    "tp_pct": 0.5,
    "tp_unit": "price",               # price | margin
    "emergency_stop_mode": "exchange",# exchange | software | off
    "emergency_stop_pct": 35.0,
    "emergency_stop_unit": "margin",  # price | margin
    "max_hold_minutes": 60,           # 0 = off
    "reentry_cooldown_minutes": 55,
    # Risk / sizing
    "leverage": 5,
    "margin_mode": "fixed_usdt",      # fixed_usdt | pct_equity
    "margin_usdt": 20.0,
    "margin_pct": 50.0,               # used when margin_mode == "pct_equity"
    "max_concurrent_positions": 3,    # open WR positions at once, ranked by rank_score
    "max_total_margin_pct": 100.0,    # summed WR margin cap, % of equity
    "skip_rr_guard": True,
    "disable_protection": False,      # required only for emergency_stop_mode == "off"
}
```

Screener additions (`DEFAULT_SCREENER`, §3.5):

```python
    "vol_enabled": False,
    "vol_max_symbols": 8,
    "vol_min_speed_pct_per_hour": 1.0,
    "vol_min_vol_ratio": 0.5,
    # fast_speed reuses fast_move_window_minutes (default 60)
```

---

## 6. Verification

1. `.venv/bin/python -m pytest tests/ -q` — all pass.
2. **Reproduce the manual window.** Backtest 2026-10-24 17:00 → 2026-11-09 17:00
   UTC, `finer_ltf` / 1m, `universe_mode="screener"` with `vol_enabled=True`,
   WR only, user-like settings (5×, TP 0.5% price, emergency stop off,
   `max_hold_minutes=0`). Compare with §1.1: trade count order of magnitude, win
   rate, hold and MAE distributions. Then check how many of the user's 33
   trades the screener + funnel would have found (symbol-hour overlap); misses
   point at the screener or the gates.
3. **Sweep** (same window, then a longer window outside it to avoid fitting to
   it): `min_body_ratio` 2/3/4/5; `max_high_offset_pct` 10/15/20/30;
   `tp_pct` 0.3/0.5/0.75/1.0; `emergency_stop_pct` (margin) 20/35/50/off;
   `max_hold_minutes` 15/30/60/120; `max_concurrent_positions` 1/3/5. Rank by expectancy and worst-trade loss,
   not win rate alone.
4. `scripts/list_active_symbols.py --hours 3 --min-speed 1.0 --top 20` — the vol
   ranking surfaces genuinely fast movers.
5. `closed` eval mode and 15m eval → zero WR trades, with the reason logged.
6. **Live dry run** (only once the live position currently open has been dealt with, since app restarts
   affect it): `vol_enabled=True`, WR enabled, `margin_usdt` small. Confirm the
   trigger fires at H:59:20 ± 2 s, the funnel log per symbol, entries only
   inside the window, TP + emergency SL algo orders on the exchange, the time
   stop closes at 60 min, and global breakeven/trailing never touch the WR
   position.
7. `emergency_stop_mode="software"` → TP algo only, no SL order, no
   `stop_loss_required` block; the monitor closes past the stop.

---

## 7. Risks and open items

| Risk | Mitigation |
|---|---|
| **Small sample** — 33 trades over 16 days in one market regime | Backtest a longer, out-of-sample window before any real-size live use; start live with `fixed_usdt` margin |
| **Runaway pump** (INIT: −85% MAE on margin) | Emergency stop on by default (35% margin, exchange-side); `max_total_margin_pct` caps combined exposure; modest default margin |
| **50% portfolio margin** — one bad trade costs a large share of the account | Configurable but not the default; the STRATEGY card shows margin at risk and the emergency-stop loss in USDT |
| **Exchange stop gets hunted** | Wide (7% price at 5×); `software` mode as the alternative, with its offline-risk trade-off documented |
| **Trigger latency** — REST fetches for 8 symbols must finish inside ~30 s | Concurrent fetches; the entry-window gate drops late symbols rather than entering late; logged |
| **Clock skew** | Use exchange server time (`/public/time`) offset, refreshed hourly, for the trigger |
| **Entry-time parity** — live 40 s vs backtest 60 s before close | Accepted, recorded in provenance; sweep `entry_seconds_before_close` live-only via the dry run |
| **Same-candle TP + stop in 1m simulation** | Stop fills first (conservative) |
| **`max_position_pct` caps WR notional** at high margin × leverage | Cap is kept and logged; UI shows the effective cap |
| **Leverage clamp** changes margin at risk | Recompute notional from margin after clamping; logged |
| **Defaults looser than the stated rules** (3× vs 5×, 20% vs 5–10%) | Chosen from the traded data (§1.1); the sweep covers the stated values |
| **VOL/SC overlap** — SC long and WR short on one symbol | Live: `_launcher_symbol_conflict` (`f833fca`) blocks it; backtest: `allow_concurrent_strategies_per_symbol=False` |
| **Forming-mode screener parity** — live sees the current hour's spike, the backtest universe lags an hour | Accepted (§3.5): the backtest undercounts; recorded in provenance |
| **Fetch cost** — vol adds up to 8 symbols per poll plus 8 REST calls per trigger | `vol_enabled` defaults to `False`; cap at 8 |
| **Correlated losses** — several WR shorts at the same hour can all run against the position in a market-wide pump | `max_total_margin_pct` bounds the combined loss; the sweep compares `max_concurrent_positions` 1/3/5 on worst-hour loss, not only total PnL |
| **App restart** wipes WR tracking (`_launcher_in_position`) | `_rehydrate_open_positions` must restore WR positions with their entry time so the time stop survives restarts; add a test |
