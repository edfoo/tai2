# Design: Hourly Wick Rejection Short (`wick_rejection`)

**Status:** In progress — Phase 1 done 2026-11-19 (closed-candle alignment + `forming_candle` + `as_of_ts`, live and backtest); Phase 2 done 2026-11-20 (strategy funnel, defaults, registration; disabled by default); Phase 3 done 2026-11-20 (TP/stop units, protection modes, per-strategy leverage + margin sizing, live and backtest); Phase 4 done 2026-11-20 (strategy-owned exits: time stop, software emergency stop, own re-entry cooldown, live and backtest); Phase 5 done 2026-11-20 (`vol` screener universe, live and backtest; also fixed a 1H look-ahead in the backtest universe); Phase 6 done 2026-11-20 (clock trigger at H:59:20, WR removed from the scheduler tick, backtest entry-timing diagnostics; restart re-hydration restores WR positions). Redesigned 2026-11-18 from the manual-trade analysis (§1.1); vol screener §3.5; optional momentum entry mode §3.8 / Phase 9.
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

### 1.2 Evidence: tick-level momentum at entry

`scripts/analyze_manual_trades_tape.py` pulled the public trade tape for the
10 minutes before each of the 33 entries (plus the two after the window).
Measured at the moment of entry:

| Measure (33 in-window trades) | p25 | median | p75 |
|---|---|---|---|
| Price change, last 30 s | −0.19% | −0.02% | +0.25% |
| Price change, last 60 s | −0.22% | −0.05% | +0.46% |
| Price change, last 5 min | −0.40% | +0.18% | +0.68% |
| Seconds since the 10-min high | 108 | **243** | 382 |
| % below the 10-min high | 0.39 | **0.74** | 1.16 |
| Buy share of traded size, last 60 s | 0.41 | 0.48 | 0.54 |
| Trade rate, last 60 s ÷ 10-min average | 0.65 | 0.89 | 1.30 |

- **Entries came after the push had stalled, not into it.** The median entry
  was flat over the last 30–60 s, about 4 minutes after the last 10-min high
  and 0.7% below it.
- **Entering into a strong push did not hurt:** the 6 entries with a ≥ +1%
  rise in the last 60 s were all winners. A "veto while still rising" rule is
  **not** supported by this data.
- **Losers didn't look like a runaway at entry.** The clearest odd one was BEAT
  (Oct 27): −2.3% in the last 60 s at 4.6× the usual trade rate, i.e. entered
  after a sharp drop had already happened, 43% off the hour high.
- **INIT looked normal at entry:** flat 60 s, last high 77 s earlier. No
  momentum measure at entry would have flagged it, so the emergency stop
  (§3.2), not a momentum veto, is what protects against runaways.
- Buy/sell share and trade rate showed no difference between winners and
  losers.
- With 5 losers this is descriptive, not a validated rule.

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

- `seconds_to_close = (candle_ts + bar_ms − now_ms) / 1000`, with
  `now_ms = snapshot["as_of_ts"]` — the moment the snapshot describes, set by
  both live (`_build_closed_candle_snapshot`) and the backtest
  (`current_ts + eval_ms`, the close of the eval candle). With 1m eval, the step
  whose close is H:59:00 (the eval candle opened at H:58) gets
  `seconds_to_close = 60`.
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
enabled and `launcher.mode == "launcher_only"`. It is built as a **watch loop**
(sleep until the watch start, then evaluate on a cadence until the window
closes) with `entry_mode="clock"` evaluating exactly once. That lets the
optional momentum mode (§3.8) slot in without rework.

1. Sleep until `H:00:00 − entry_seconds_before_close` (default 40 s), computed
   from wall-clock UTC each cycle (no drift accumulation).
2. For each symbol in the current vol list, gather the data (see *Data sources*
   below): REST 1H candles, then the websocket price. Build the per-symbol
   snapshot through the normal snapshot path, with `forming_candle` set and its
   close = the current price.
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

#### Data sources

| Data | Source | Why |
|---|---|---|
| Current price `P` (high-offset gate, forming close) | **Public websocket `tickers` channel** — `_latest_ticker[symbol]["last"]`, used when its `ts` is at most `ws_price_max_age_seconds` (default `5`) old; otherwise a REST ticker call (`/market/ticker`) | Freshest price with no request cost. Vol symbols are in `selected`, so `update_symbols` already subscribes them. The fallback covers a websocket gap or reconnect |
| Forming 1H candle (open, high, low) | **REST** `/market/candles` at trigger time, then `high = max(high, P)`, `low = min(low, P)` | The tickers channel carries only 24h high/low, and the bot has no candle websocket. Folding `P` in covers moves after the fetch |
| Closed 1H candles (body-ratio setup) | REST, in the same call: the last `body_avg_window + setup_lookback_candles + 2` candles | They don't change during the hour |
| TP and emergency stop | Exchange algo orders | Nothing to poll. `software` stop mode uses mark price from the private websocket `positions` channel; the time stop runs from the positions-refresh loop |

- One REST call per symbol, run concurrently: 8 symbols take well under a
  second, inside OKX's candle rate limit and the ~30 s window.
- Each trigger logs, per symbol, which price source was used and its age.
- The screener's `fast_speed` keeps using the all-SWAP REST ticker poll the
  screener already does (§3.5). The websocket only covers selected symbols, so
  it can't discover new ones.
- **Later option, only if the live dry run shows the trigger is tight:**
  subscribe to `candle1H` on OKX's business websocket
  (`wss://ws.okx.com:8443/ws/v5/business`), so the forming candle is always
  current in memory and the trigger makes no REST calls. This costs a third
  websocket connection with its own reconnect, ping and resubscribe handling.

### 3.7 Backtest

- Requires `evaluation_mode="finer_ltf"` with `evaluation_timeframe="1m"`. The
  entry step is the 1m candle opened at H:58, whose close is the price at
  H:59:00 (`as_of_ts = H:59:00`, `seconds_to_close = 60`, inside the default
  10–90 s window); the entry price is that close. The next step (the H:59
  candle) closes exactly on the hour, so its 1H bar has closed and
  `forming_candle` is `None` — no entry can land after the hour. In `closed` mode, or with an
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

### 3.8 Momentum entry mode (optional, Phase 9)

Replaces the fixed 40 s entry with continuous watching in the last minutes of
the hour, like the user watching the chart. Off by default
(`entry_mode = "clock"`).

| `entry_mode` | Behaviour |
|---|---|
| `"clock"` (default) | §3.6: one evaluation at `H:00 − entry_seconds_before_close` |
| `"momentum"` | Watch from `watch_seconds_before_close`; enter the first moment all momentum conditions pass inside the momentum entry window; no entry otherwise |
| `"momentum_or_clock"` | As `"momentum"`, but if nothing has triggered by the clock time, evaluate the clock entry as usual |

**Watching vs entering.** Entries should stay close to the end of the hour (the
thesis is the dip at the hour open; the user's entries were a median 39 s
before). So the window is split:

- **Watch** from `watch_seconds_before_close` (default `300`): fetch the 1H
  candles once, run the setup and range gates (§3.1 stages 3–6) to get a
  shortlist, then compute momentum for the shortlist every
  `momentum_eval_interval_seconds` (default `2`).
- **Enter** only inside `momentum_entry_max_seconds_to_close` (default `120`) to
  `min_seconds_to_close` (`10`). Earlier signals are logged but not traded.

**Data.** All in memory from the public websocket `trades` channel (already
subscribed for selected symbols): a per-symbol rolling tape of
`{ts, px, sz, side}` for the last `watch_seconds_before_close + 120` seconds.
The existing footprint buffer keeps price and time but is sized for the
footprint, so add a dedicated `_wr_tape[symbol]` deque. No REST calls after the
watch start. If a symbol's tape has a gap (websocket reconnect), it is dropped
from momentum evaluation for that hour and logged; with `"momentum_or_clock"`
it can still enter on the clock.

**Momentum measures** (one pure function, `compute_momentum(tape, now_ms)`,
shared by live and the backtest):

| Measure | Definition |
|---|---|
| `ret_30s`, `ret_60s`, `ret_120s` | % change of last price vs the price at `now − N s` |
| `accel_60s` | `ret_60s` minus the change over the 60 s before that |
| `secs_since_high` | seconds since the highest trade in the watch window |
| `below_high_pct` | % below that high |
| `buy_share_60s` | buy size ÷ total size, last 60 s |
| `rate_ratio_60s` | trades in the last 60 s ÷ average per 60 s over the watch window |

**Entry conditions** (all must pass; starting values from §1.2):

| Condition | Default | Why |
|---|---|---|
| `secs_since_high >= stall_min_seconds` | `60` | "Stopped making new highs" — 75% of the user's entries were ≥ 108 s after the high |
| `below_high_pct <= max_below_high_pct` | `1.5` | Still near the top — p75 was 1.16%, max 2.6% |
| `ret_60s >= min_ret_60s` | `-1.0` | Skip entries after a sharp drop already happened (the BEAT loser: −2.3%) |
| `ret_60s <= max_ret_60s` | `null` (off) | A "still pushing up" veto is available but **off**: the 6 entries after a ≥ 1% rise all won |
| `buy_share_60s <= max_buy_share_60s` | `null` (off) | No difference seen in the data; available for sweeps |

The §3.1 high-offset gate (range-relative, vs the 1H high) still applies.
When several symbols trigger at different moments, each enters when it
triggers, subject to `max_concurrent_positions` and `max_total_margin_pct`.
`rank_score` only orders symbols that trigger on the same evaluation.

**Backtest.** Two levels:

1. **1m approximation** (default for Phase 9): the 1m candles give
   minute-resolution measures (`ret_60s` = the last 1m candle's change,
   `secs_since_high` rounded to minutes, no buy share). Entries can land on
   the steps whose close is H:58:00 or H:59:00 only. Cheap and enough to see whether momentum
   timing beats the clock at all.
2. **Tape replay** (`wr_momentum_tape=True`, only if level 1 looks promising):
   fetch the trade tape for the watch window of each hour where a vol symbol
   passed the setup gates, and replay `compute_momentum` second by second.
   Uses `data_fetcher.fetch_trade_tape` after its pagination bug is fixed
   (`okx_metrics.fetch_trade_history_records` sends a timestamp as `after`
   without `type=2`, so OKX reads it as a trade ID, and it keeps only one trade
   per millisecond). OKX keeps about 3 months of tape, so tape replay is
   limited to recent windows; cached per symbol-hour.

---

## 4. Implementation steps

### Phase 1 — Forming-candle exposure ✅ *(done 2026-11-19)*

1. **`market_service._build_closed_candle_snapshot`** — splits each timeframe
   on its own timestamps (`now < ts + bar_ms` → forming): base bar, every
   `timeframes[<tf>]` block, and each HTF layer. Closed candles feed the
   indicators; the forming candle is exposed as `forming_candle`
   (`{ts, open, high, low, close, volume}` or `None`) on the base indicators
   and each tf block. Sets `snapshot["as_of_ts"]`.
2. **`backtest/snapshot_builder.py`** — tf blocks and HTF layers hold only
   candles closed by the snapshot's moment; in finer_ltf mode each tf's
   `forming_candle` is aggregated from the eval candles seen so far; closed
   mode sets `None`. Sets `as_of_ts`.

Found and fixed while doing this (they blocked Phase 1):

- **Backtest look-ahead.** Tf blocks and HTF layers selected candles by
  `ts <= now`, and OKX `ts` is the bar's open, so the in-progress bar was
  visible with its final high/low/close. With 1m eval, the five strategies'
  15m blocks could see up to 14 minutes ahead, and HTF regime layers up to a
  full HTF bar. Backtest results before this fix are optimistic.
- **Live per-timeframe cache never populated.** `_fetch_ohlcv_bar` stored
  per-tf candles under `symbol` but they were read as `(symbol, bar)`, so live
  `timeframes[<tf>]` blocks were never built and strategies silently fell back
  to the global `ta_timeframe` indicators. Fixed with a `cache_key` argument.
- **Live forming bar treated as closed in its last 10%.** The old forming check
  (`now < close − 10% of bar`) let strategies read the in-progress candle as
  closed during the last 10% of each bar. Now strict.
- **Live HTF layers included the forming HTF bar.** Now trimmed, matching the
  backtest.
- **Backtest history capped at 1,440 candles.** The fetcher only paged OKX
  `/market/candles`, which serves the newest 1,440 candles, so 1m stepping
  covered about one day whatever range was requested. It now continues on
  `/market/history-candles`, and truncated cache files are refetched. Needed
  for the 16-day 1m validation in §6.

Tests: `tests/test_forming_candle.py` (no look-ahead in either mode, forming
candle built from minutes so far, last-minute boundary, live trimming per
timeframe, cache key regression, live ↔ backtest parity at the same moment).

### Phase 2 — Strategy ✅ *(done 2026-11-20)*

3. **New `strategies/wick_rejection.py`** — the §3.1 funnel, `body_ratio` helper,
   `rank_score`.
4. **`strategies/defaults.py`** — `DEFAULT_WICK_REJECTION` (§5), registered in
   `DEFAULT_STRATEGY_CONFIG`.
5. Register in `market_service._strategies`, `backtest/engine._AVAILABLE_STRATEGIES`,
   `backtest/runner.DEFAULT_STRATEGIES`.

As built:

- `StrategySignal` gained an optional `rank_score` (other strategies leave it
  `None`).
- `P` is the forming candle's close. `H`/`L` are widened by `P` so a price
  above the recorded high counts as the new high. The trigger (Phase 6) will
  patch the live price into the forming candle before evaluating.
- `exit_price_pcts(cfg)` converts `tp_unit` / `emergency_stop_unit` to price
  percent (default 0.5% TP, 35% margin ÷ 5× = 7% stop) and fills the signal's
  `tp_pct` / `sl_pct`. Phase 3 still owns the TP/SL resolution in
  `launcher_tp_sl` (which today rescales signal percentages by guard leverage).
- Stages 1–2 (no forming candle, outside the entry window) veto silently,
  since they veto on every tick; stages 3–7 log the measured value.
- `STRATEGY_SWEEP_PARAMS["wick_rejection"]` is added (the catalogue test
  requires every engine strategy). Live-trigger timing keys
  (`entry_seconds_before_close`, `ws_price_max_age_seconds`,
  `momentum_eval_interval_seconds`) are excluded from sweeps.
- **Deferred:** `backtest/runner.DEFAULT_STRATEGIES` (unused in code; it seeds
  "all strategies enabled") gets WR in Phase 4, once exits exist.
- Universe routing is unchanged until Phase 5: WR falls back to the combined
  screener list.
- Not yet safe to enable live: the regular tick would evaluate it at random
  times and the global trade management would manage it (Phases 3, 4, 6).

Tests: `tests/test_wick_rejection.py` (32): every funnel stage and its
boundaries, `body_ratio`, exit unit conversion, registries, and a finer_ltf
backtest snapshot that fires at minute 58 without seeing the minute-59 dump
and stays silent mid-hour.

### Phase 3 — TP/SL, sizing and guardrail plumbing ✅ *(done 2026-11-20)*

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

As built:

- **Resolver (`launcher_tp_sl.py`).** A strategy has *strategy-owned exits*
  when `emergency_stop_mode` is in its defaults (only WR). For those,
  `resolve_launcher_tp_sl` takes TP and the stop from config via
  `exit_price_pcts`. Margin units convert at the **clamped** leverage (35%
  margin at a clamped 3× is an 11.7% stop, not 7%). It ignores the launcher's
  PnL% mode. `software`/`off` stop → `protection="tp_only"`, no SL price.
  `disable_protection` (any strategy, OR'd with the MR rule) →
  `protection="none"`. `skip_rr_guard` is read from any strategy's config.
- **Sizing (`resolve_strategy_sizing`).** Leverage is clamped to the guardrail
  `[min_leverage, max_leverage]` (read like `handle_llm_decision`), and margin
  comes from `fixed_usdt` or `pct_equity` × equity. Notional is
  `margin × clamped leverage`. `pct_equity` with no equity → `sizing_failed`,
  and the entry is skipped (never a silent fallback to `launcher.notional_usd`).
  `LauncherTpSl` gained `protection`, `leverage_override`, `margin_usd`,
  `notional_override` and `sizing_failed`.
- **`build_launcher_decisions(symbol, strategies=None)`.** Equity comes from
  `snapshot["account"]`. Decisions carry `notional_usd` (override or
  `launcher.notional_usd`), `_leverage_override`, `_margin_usd`, `_protection`
  and `_rank_score`. `launcher.notional_usd` is now checked per signal, so WR
  works without it.
- **`handle_llm_decision`.**
  - A launcher decision with `_leverage_override` + `_margin_usd` sizes as
    `margin × leverage`, bypassing both the exec-layer formula and
    `llm_notional_mode` (`pre_leverage` would have multiplied by `max_leverage`
    again).
  - OKX leverage = the strategy leverage (re-clamped; tier cap applied with the
    notional shrunk so margin stays as configured).
  - `leverage_override_reason = "strategy leverage Nx"` so the min-leverage
    guard (notional ÷ equity) doesn't block.
  - `max_position_pct` and the margin caps still clip, with a log line.
  - `_protection="tp_only"` strips the SL and requires only the TP
    (`require_stop`).
- **Isolated-margin limits are margin budgets for strategy-sized entries.** A
  new isolated position goes through the wallet-bootstrap cap
  (`isolated_wallet_bootstrap_pct`, default 25% of the USDT balance), which the
  code applies to **notional**. For WR that would have turned 20 USDT × 5 into
  a ~29 USDT position on a 117 USDT account. For strategy-sized entries the
  bootstrap budget, the balance cap and the fallback seed are converted to
  notional with the strategy leverage. Other entries are unchanged.
- **Backtest.** The engine passes the simulator's equity, skips
  `sizing_failed`, and passes `notional`/`leverage`/`margin` to
  `Simulator.open_position`. That replaces `notional_per_trade` and the
  guardrail `max_leverage`, and keeps margin when an instrument tier caps
  leverage. `_passes_protection_guard` accepts `tp_only` with a TP only.
- **Config to check before going live.** `max_leverage` must be ≥ the WR
  `leverage` (otherwise it is clamped, margin kept). `max_position_pct` (a
  fraction of equity, applied to notional) must be ≥ `margin × leverage ÷
  equity`, otherwise every WR entry is cut.
- Not in this phase: WR's own `reentry_cooldown_minutes` (Phase 4, with the
  exits), and `max_concurrent_positions` / `max_total_margin_pct` (Phase 6,
  in the trigger).

Tests: `tests/test_wick_rejection_execution.py` (38): resolver, sizing and
clamping, protection modes, simulator sizing including the tier cap,
`build_launcher_decisions` forwarding and the filter, and execution in cross
and isolated modes (size, OKX leverage, attached TP/SL algos, caps).

### Phase 4 — Strategy-owned exits ✅ *(done 2026-11-20)*

9. **`trade_management_rules.py`** — `StrategyExitParams` +
   `compute_strategy_exit()` (time stop, software emergency stop).
10. **`market_service._check_trade_management`** — for positions whose strategy
    has `use_global_trade_management = False`, call `compute_strategy_exit`
    and skip the global rules. Software emergency stop is also checked from the
    positions-refresh loop (not only the scheduler tick).
11. **`backtest/simulator.py`** — the same split per position; 1m fill-order
    rule from §3.7.

**As built.**

- **One rule, parameters fixed at entry.** `resolve_launcher_tp_sl` returns
  `strategy_exit: StrategyExitParams` for strategies with owned exits
  (`max_hold_seconds`, `software_stop_pct`, `reentry_cooldown_seconds`,
  `use_global_trade_management`). The software stop is a price % converted at
  the clamped leverage (35% margin at 3× = 11.67%). Live carries it on the
  decision as `_strategy_exit` into `_seed_trade_mgmt_state`; the backtest
  passes it to `Simulator.open_position`. Both call
  `compute_strategy_exit(params, is_long, entry_price, held_seconds, high, low)`.
  Later config edits do not change an open position's exits.
- **Live.** Owned-exit positions are seeded even when global trade
  management is off, and `_check_trade_management` (positions-refresh loop,
  every 10 s) runs for them in that case. It closes at market on the software
  stop or the time stop, logs `TradeMgmt emergency_stop|time_stop: SYMBOL
  [wick_rejection] …`, and skips the global rules unless
  `use_global_trade_management` is set. Untracked positions are not
  auto-seeded while global management is off.
- **Backtest.** `Simulator._apply_exits`: software stop first (filled at the
  stop, or the open on a gap), then the TP/SL algos (exchange stop already
  fills before the TP), then the time stop at the candle close. Hold time is
  `candle.ts − entry_ts`, so a 60-minute hold entered on the H:58 1m candle
  closes on the (H+1):58 candle, 60 minutes after entry. Close reasons
  `emergency_stop` / `time_stop`.
- **Re-entry cooldown.** When an owned-exit position closes, its own
  `reentry_cooldown_minutes` (default 55) replaces the global
  `reentry_cooldown_seconds`, live (`_clear_trade_mgmt_state`) and backtest
  (`can_enter`). Measured from the close, per symbol.
- `wick_rejection` added to `runner.DEFAULT_STRATEGIES`.
- **Restart gap (fixed 2026-11-20, after Phase 6).** `_rehydrate_open_positions`
  now reads the `strategy` column of the most recent unreconciled entry
  (`FETCH_UNRECONCILED_SQL` selects it). For a strategy that owns its exits
  (wick_rejection) it restores the `strategy:SYMBOL` key with the position's
  margin and the exits via `launcher_tp_sl.strategy_exit_params()` (current config, software
  stop at the position's `lever`), with `entry_ts` = the DB entry time so the
  time stop counts from the real entry. Re-hydration still needs Postgres;
  without it a WR position open across a restart stays unmanaged (as for
  every launcher position). Other strategies keep the symbol-only key on
  purpose: restoring `mean_reversion:SYMBOL` would re-enable MR's regime-breakdown
  close (and SC's rollover close), which have never applied after a restart.

Tests: `tests/test_wick_rejection_exits.py` (25): the rule, the resolver
params, simulator fill order, time stop, global rules skipped/opt-in,
cooldown, and the live split with global management on and off.

### Phase 5 — Dedicated `vol` universe ✅ *(done 2026-11-20)*

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

**As built.**

- `screener.compute_speed()` gives `closed_speed`, `fast_speed`, `speed` for
  every candidate (attached to the candidate dicts, so the diagnostics show
  both terms). `score_universe(..., price_history=, now_ts=)` builds the vol
  pool with its own gates, score and cap in dual and legacy mode, returns
  `vol` / `vol_pool_size`, logs `Screener VOL selected (n/pool …)` with speed,
  closed/fast terms, volume ratio and decay per symbol, and appends vol to
  `selected`.
- **Routing.** `universe_for_strategy(..., vol_symbols=)` sends WR to the vol
  list (fallback: selected, then the configured symbols). Symbols that are
  **only** on the vol list are removed from every other strategy's fallback,
  so adding vol to the fetch list never widens SC/MR strategies, including in
  legacy mode. Live `_strategy_allowed_on_symbol` applies the same rules
  (before the dual-mode check), and `get_screener_universe` passes the list.
- **Live data.** `run_screener_if_due` passes `_screener_price_history` and
  stores `_screener_vol_symbols`; the scheduler exposes
  `runtime_config["_screener_vol_symbols"]`. Note: the vol gate needs
  `recent_vol_ratio`, which live only fetches for the
  `activity_prefilter_top_n` volume-ranked candidates plus fast-lane symbols.
  A fast coin outside both cannot reach the vol list; enable the fast lane
  (or raise the prefilter) if this misses movers.
- **Backtest.** `UniverseInterval.vol`, `universe_at` and `to_dict` carry the
  list; the price deque is built when `vol_enabled` or `fast_lane_enabled`;
  the §3.5 parity limitation is in the provenance (`vol_universe`).
- **Look-ahead fix (all screener backtests).** The backtest universe read the
  1H candle *opening* at each interval boundary (`ts <= boundary`), i.e. the
  hour that had not happened yet: tickers, activity features and fast-lane
  samples all knew that hour's high/low/close. It now uses only candles closed
  by the boundary (open `<= boundary − 1h`), and price samples are stamped
  with the candle's close time. Recorded as `candle_visibility` in the
  provenance. Universe-mode backtests from before this fix are optimistic.
- Defaults: `vol_enabled=False`, `vol_max_symbols=8`,
  `vol_min_speed_pct_per_hour=1.0`, `vol_min_vol_ratio=0.5`. No UI yet (Phase 7).

Tests: `tests/test_screener_vol.py` (19).

### Phase 6 — Clock trigger ✅ *(done 2026-11-20)*

17. **New `app/services/wick_rejection_trigger.py`** — the §3.6 loop, started and
    stopped with the scheduler; shared execute lock with `PromptScheduler`.
18. **`prompt_scheduler._collect_launcher_decisions`** — exclude WR.
19. **`backtest/engine.py`** — WR zero-trade diagnostics for unsupported eval
    modes; provenance for the 60 s vs 40 s entry gap.

**As built.**

- **`app/services/wick_rejection_trigger.py`.** `WickRejectionTrigger` is
  owned by `PromptScheduler`: started and stopped with it, so it only runs
  while the scheduler is on (`auto_prompt_enabled`), and each cycle also
  checks `launcher.mode == "launcher_only"`, WR `enabled` and the scheduler's
  enabled flag (the daily-loss lock clears it). It sleeps to
  `next_trigger_ts(now, entry_seconds_before_close)` (wall-clock UTC), then
  `run_once()`:
  1. symbols = `get_screener_universe("wick_rejection")` (the vol list);
  2. per symbol, concurrently: `refresh_timeframe_candles(sym, "1H")` (one
     REST call, refreshes the shared `_latest_ohlcv_tf` cache) and
     `fetch_trigger_price` (websocket ticker if its `ts` is at most
     `ws_price_max_age_seconds` old, else one REST `/market/ticker`); logs
     `WR trigger: prices SYM=P (ws|rest, age)`;
  3. if fewer than `min_seconds_to_close` seconds remain → `late`, no entries;
  4. `build_launcher_decisions(sym, strategies=["wick_rejection"], price=P)`;
  5. rank by `_rank_score`; slots = `max_concurrent_positions` − open WR
     positions (`_launcher_in_position`); `max_total_margin_pct` × equity caps
     the summed `_margin_usd` (open WR margins are now stored on
     `_launcher_in_position[...]["margin_usd"]`);
  6. execute sequentially through `handle_llm_decision` with
     `launcher_execution_context` (shared with the scheduler), re-checking the
     time left before each order; waiting for the execute lock is bounded by
     the window;
  7. one summary line: `WR trigger: ok | checked n | signals … | entries … |
     dropped SYM: reason`. Per-symbol funnel vetoes are WR's own debug lines.
- **Snapshot path.** `build_launcher_decisions(..., price=)` →
  `_launcher_evaluate_signals(..., forming_price=)` →
  `_build_closed_candle_snapshot(..., forming_price=, ensure_timeframes=)`:
  each forming candle gets `close = P`, `high = max(high, P)`,
  `low = min(low, P)`, and a missing `timeframes["1H"]` block is built from the
  cache. `P` is also the decision's last price. The regular path is unchanged.
- **Scheduler.** `_collect_launcher_decisions` evaluates every strategy except
  `TRIGGER_ONLY_STRATEGIES` (`wick_rejection`). `execute_lock` is held around
  each order (not the whole execute phase), so a tick cannot block the trigger
  for long. `launcher.schedule = "on_close"` does not affect the trigger.
- **Backtest.** `wick_rejection.backtest_entry_timing()` finds the eval steps
  inside the entry window. The engine logs a warning when there are none
  (closed mode, 5m eval, a window narrower than the step) and adds a
  `wick_rejection_entry_timing` entry to `data_provenance`, including the
  60 s (backtest) vs 40 s (live) gap.
- The restart gap noted in Phase 4 is fixed (see Phase 4 *As built*).

Tests: `tests/test_wick_rejection_trigger.py` (22).

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
      `max_total_margin_pct` sizes down / skips the last entry; uses the
      websocket price when fresh and falls back to REST when stale or missing;
      the forming high/low are widened by `P`;
      the regular tick never evaluates WR.
    - Vol screener — all cases from the previous revision (`hours_covered`
      divisor, no 24h fallback, quiet-then-spiking coin via `fast_speed`,
      neither term → excluded, missing `vol_ratio` fails, missing decay scores 0,
      `activity_min_ratio` ignored, legacy mode, appended to `selected`,
      `vol_enabled=False` → empty).
    - `test_backtest_universe.py`, `test_sweep_catalog.py`,
      `test_live_backtest_parity.py` — WR signal and exit parity on a 1m fixture;
      same-candle TP + stop → stop.

### Phase 9 — Momentum entry mode *(optional; after the clock mode has run live)*

27. **New `app/services/strategies/wr_momentum.py`** — pure
    `compute_momentum(tape, now_ms, window_s)` and
    `momentum_entry_ok(measures, cfg)` (§3.8).
28. **`market_service`** — `_wr_tape[symbol]` rolling deque fed from
    `_handle_ws_message` (`trades` channel) for vol symbols only; gap detection
    on reconnect.
29. **`wick_rejection_trigger.py`** — `entry_mode` handling: watch start,
    shortlist from the setup gates, evaluation every
    `momentum_eval_interval_seconds`, entry window, `momentum_or_clock`
    fallback, per-symbol log of the measures at each decision.
30. **`okx_metrics.fetch_trade_history_records`** — fix pagination (`type=2`)
    and keep every trade (dedupe by `tradeId`, not timestamp). Add a test with a
    recorded page. This also fixes the backtest spread estimate.
31. **Backtest** — 1m-approximation momentum in the engine's finer_ltf loop;
    optional tape replay behind `wr_momentum_tape`.
32. **UI** — entry mode selector and the momentum thresholds on the WR card;
    LIVE page shows the shortlist and current measures during the watch window.
33. **Tests** — `compute_momentum` on synthetic tapes (stall, push, drop, gap);
    `momentum_entry_ok` per condition, `null` thresholds disabled; trigger
    enters inside the entry window only, falls back to clock in
    `momentum_or_clock`, never enters in `momentum` mode without a trigger;
    tape-gap symbols excluded; live/backtest parity of the measures on the same
    1m-derived tape.

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
    "ws_price_max_age_seconds": 5,    # use the websocket price if fresher than this, else REST
    # Momentum entry mode (§3.8, Phase 9) — off by default
    "entry_mode": "clock",            # clock | momentum | momentum_or_clock
    "watch_seconds_before_close": 300,
    "momentum_entry_max_seconds_to_close": 120,
    "momentum_eval_interval_seconds": 2,
    "stall_min_seconds": 60,
    "max_below_high_pct": 1.5,
    "min_ret_60s": -1.0,
    "max_ret_60s": None,              # "still pushing up" veto, off
    "max_buy_share_60s": None,        # off
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
8. **Momentum mode (Phase 9):** backtest `entry_mode` clock vs
   `momentum_or_clock` on the same window with the 1m approximation; adopt
   momentum only if expectancy and worst-hour loss are at least as good. Live:
   run `momentum_or_clock` with small margin and compare per-hour logs of the
   measures with what the clock would have done.

---

## 7. Risks and open items

| Risk | Mitigation |
|---|---|
| **Small sample** — 33 trades over 16 days in one market regime | Backtest a longer, out-of-sample window before any real-size live use; start live with `fixed_usdt` margin |
| **Runaway pump** (INIT: −85% MAE on margin) | Emergency stop on by default (35% margin, exchange-side); `max_total_margin_pct` caps combined exposure; modest default margin |
| **50% portfolio margin** — one bad trade costs a large share of the account | Configurable but not the default; the STRATEGY card shows margin at risk and the emergency-stop loss in USDT |
| **Exchange stop gets hunted** | Wide (7% price at 5×); `software` mode as the alternative, with its offline-risk trade-off documented |
| **Trigger latency** — REST candle fetches for 8 symbols must finish inside ~30 s | Concurrent fetches; price from the websocket; the entry-window gate drops late symbols rather than entering late; logged. Fallback: `candle1H` business websocket (§3.6) |
| **Stale websocket price** — reconnect gap or unsubscribed symbol | `ws_price_max_age_seconds` check on the ticker `ts`; REST ticker fallback; source and age logged |
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
| **Momentum mode overfits** — thresholds come from 33 trades with 5 losers | Off by default; 1m backtest on windows outside the user's period before enabling; vetoes the data doesn't support (`max_ret_60s`, `max_buy_share_60s`) stay off |
| **Momentum parity** — live uses second-level tape, the backtest 1m candles | Accepted for level 1 and recorded in provenance; tape replay closes the gap for recent windows |
| **Websocket trade gaps** during the watch window | Gap detection drops the symbol from momentum for that hour; `momentum_or_clock` still allows the clock entry |
| **App restart** wipes WR tracking (`_launcher_in_position`) | `_rehydrate_open_positions` must restore WR positions with their entry time so the time stop survives restarts; add a test |
