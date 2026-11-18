# Design: Hourly Wick Rejection Short (`wick_rejection`)

**Status:** Planned (screener design revised 2026-11-18 — see §3.5)
**Date:** 2026-10-28
**Scope:** New launcher strategy + forming-candle exposure + per-strategy
leverage/guardrail overrides + a dedicated `vol` screener universe, with
live/backtest parity.

---

## 1. Motivation

The existing five launcher strategies all fade or follow *closed* candles. None
of them exploits the most reliable intra-hour price-action pattern on volatile
alt-coins: **a candle that runs up into its high and stalls there**. When a
fast-moving coin prints a large 1H range and then closes the hour pinned near
its high, the next hour frequently mean-reverts — the move was a liquidity grab,
not a trend.

The strategy is deliberately narrow:

- **Universe:** only fast-moving (volatile) coins — a dedicated `vol` screener
  list ranked on *speed* (range % per hour), not the 24h aggregates.
- **Trigger:** the 1H candle sits in the top slice of its own range —
  `(H − C) / (H − L) × 100 <= max_high_offset_pct` (default 10). Which candle is
  measured is selected by `candle_source` (§3.1.1): `"forming"` (default — the
  in-progress bar, entered in the last few minutes of the hour) or `"closed"`
  (the finished bar, entered just after the hour).
- **Direction:** SHORT only.
- **Exit:** a shallow retracement into the **previous candle's body** — 25% of
  the body height below the body top (`tp_body_fraction = 0.25`). The thesis is
  that price dips into the prior body before continuing up, so the target is
  that dip, not a volatility multiple.

> **R:R is deliberately not a constraint here.** These are volatile coins: an
> adverse excursion of 50% is possible against a 3% target. The strategy is
> sized and judged on win rate and expectancy, not on reward-to-risk geometry,
> so it sets `skip_rr_guard = True`. See §3.3.

This is the mirror image of Spike Continuation: SC enters *with* an accelerating
impulse and holds for a wide target; WR enters *against* a stalled impulse and
takes a small, fast profit.

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

Helpers available: `resolve_analysis_block(sym_data, cfg)` (resolves the
strategy's `analysis_timeframe` block), `merged_config(config, name)` (merges
caller config over canonical defaults), `compute_bb_bandwidth_percentile`,
`fractal_swings`.

### Registries
| Location | Purpose |
|---|---|
| `MarketService._strategies` (`market_service.py` ~L481) | live evaluation |
| `_AVAILABLE_STRATEGIES` (`backtest/engine.py` ~L57) | backtest evaluation |
| `DEFAULT_STRATEGIES` (`backtest/runner.py` ~L32) | CLI defaults |
| `DEFAULT_STRATEGY_CONFIG` (`strategies/defaults.py`) | canonical defaults |
| `STRATEGY_SWEEP_PARAMS` (`backtest/sweep_catalog.py`) | parameter sweeps |

### Config location
`config["launcher"]["strategies"][<strategy_name>]`. Canonical defaults live in
`app/services/strategies/defaults.py`; `main.py` bootstraps from there and the
CFG/STRATEGY pages merge saved config over it.

### TP/SL resolution (shared live + backtest)
`app/services/launcher_tp_sl.py::resolve_launcher_tp_sl` returns a
`LauncherTpSl(action, tp_price, sl_price, disable_protection, skip_rr_guard,
flipped, flip_tp_sl_active, dynamic_tp_source, debug_lines)`. Both
`MarketService.build_launcher_decisions` and `BacktestEngine._compute_tp_sl`
call it, so TP/SL logic cannot drift.

### Guardrails that block entries
| Guard | Location | Bypass |
|---|---|---|
| `stop_loss_required` | `market_service.py` ~L10270 | `decision["_disable_protection"]` |
| `require_protection` (TP required) | `market_service.py` ~L10285 | `decision["_disable_protection"]` |
| R:R ratio | `market_service.py` ~L10300 | `decision["_skip_rr_guard"]` |
| min leverage | `market_service.py` ~L10783 | `leverage_override_reason` |

The `_launcher_no_protection_entry` path already exists and strips TP/SL from
the entry order when `_disable_protection` is set — but it is currently only
reachable for `mean_reversion` (hardcoded in `launcher_tp_sl.py`).

### Leverage
`target_leverage` is derived in `_execute_decision` (~L10780–10830) from
`raw_size × last_price / account_equity`, then clamped to
`[min_leverage, max_leverage]` and the instrument tier cap.
`_ensure_isolated_leverage_setting` (~L12142) pushes it to OKX via
`set_leverage`. There is **no per-strategy leverage override** today.

### Screener
`app/services/screener.py`:
- `build_candidates(tickers, cfg, ...)` — liquidity prefilter
- `compute_recent_activity(candles, ...)` — short-window features
  (`recent_vol_ratio`, `recent_range_pct`, `recent_mom_pct`, `decay_ratio`)
- `score_universe(...)` — returns `selected`, `sc`, `mr`, `candidates`, …
- `universe_for_strategy(name, sc_symbols, mr_symbols, selected_symbols,
  fallback_symbols, dual_universe)` — routes a strategy to its list

Live routing: `MarketService._strategy_allowed_on_symbol` (~L3608).
Backtest routing: `UniverseSchedule.universe_at(ts, strategy_name)`
(`backtest/universe.py`), built by `build_universe_schedule`.

### The forming-candle problem
Live deliberately evaluates the **last closed** candle:
`MarketService._build_closed_candle_snapshot` (~L3760) trims the forming candle
and recomputes indicators, so live matches the backtest's closed-candle
semantics (see `/memories/repo/backtest_live_divergence.md`).

The forming candle's running high **is** available live (it is the last row of
`_latest_ohlcv_tf`), but it is discarded before strategies run. WR needs it, so
Phase 1 exposes it under a new key without changing any existing behaviour.

The backtest already has an equivalent in `finer_ltf` eval mode:
`SnapshotBuilder.build_with_incomplete_ltf(closed_ltf_window, incomplete_candle,
current_ts)` builds a snapshot whose last LTF candle is in progress. In `closed`
eval mode there is no forming candle at all.

---

## 3. Design

### 3.1 Strategy funnel

`WickRejectionStrategy.evaluate()` runs an ordered funnel; each stage vetoes
before the next runs, and every rejection emits a debug line with the measured
value (matching `SpikeContinuationStrategy`'s style).

| # | Stage | Gate | Default |
|---|---|---|---|
| 0 | Enabled | `cfg["enabled"]` | `False` |
| 1 | Candle source | resolve the measured candle per `candle_source` (§3.1.1) | `"forming"` |
| 2 | Elapsed | `elapsed_pct >= min_candle_elapsed_pct` | `90.0` |
| 3 | Range | `(high - low) / low * 100 >= min_candle_range_pct` | `2.0` |
| 4 | Volatility | `atr_pct >= min_atr_pct` | `1.0` |
| 5 | High offset | `(high - close) / (high - low) * 100 <= max_high_offset_pct` | `10.0` |
| 6 | Volume (opt-in) | `volume_participation_ok(...)` | `False` |
| 7 | HTF regime (opt-in) | `htf_regime_allows(adx_htf, chop_htf, pref)` | `"off"` |

`elapsed_pct = (now_ms - candle_ts) / bar_ms * 100`, where `bar_ms` comes from
`screener.timeframe_to_ms(analysis_timeframe)`. Live uses `time.time()`; the
backtest uses the eval candle's timestamp, so both agree without a special case.

#### 3.1.1 Candle source (`candle_source`)

The strategy supports two measurement modes behind one config key:

| `candle_source` | Candle measured | Entry moment | Needs Phase 1 |
|---|---|---|---|
| `"forming"` (default) | `indicators["forming_candle"]` — the in-progress bar | last ~6 min of the hour | **yes** |
| `"closed"` | `indicators["ohlcv"][-1]` — the finished bar | ~1 min after the hour | no |

`"forming"` is the original intent: enter 0–6 minutes before the candle closes,
while the running high is still the reference. `"closed"` is the fallback that
needs no new infrastructure and works in both backtest eval modes.

Resolution logic:

```python
if candle_source == "forming":
    candle = indicators.get("forming_candle")
    if candle is None:
        # closed eval mode / no forming data — reject with a debug line
        return None
else:  # "closed"
    ohlcv = indicators.get("ohlcv") or []
    candle = ohlcv[-1] if ohlcv else None
```

In `"closed"` mode the elapsed gate is inverted into an **age** gate: the last
closed candle must be no older than `max_candle_age_minutes` (default `10`), so
the strategy does not fire on a stale bar hours after the fact. In `"forming"`
mode the elapsed gate applies as written.

> **Backtest note:** `"forming"` requires `evaluation_mode="finer_ltf"`. In
> `closed` eval mode there is no forming candle, so `"forming"` produces zero
> trades — expected, and logged. `"closed"` works in both modes.

#### Stage 5 — why the gate is range-relative

The high-offset gate measures **where the close sits inside the candle's own
range**, not its distance from price:

$$
\text{high\_offset} = \frac{H - C}{H - L} \times 100
$$

This is the complement of the close-position metric used by
`spike_continuation` (`candle_strength_pct`):

$$
\frac{H - C}{H - L} \times 100 \;\equiv\; 100 - \frac{C - L}{H - L} \times 100
$$

A price-relative gate such as `(H − C) / C` is **mathematically a no-op** here:
its maximum possible value is the candle's own range, so a 5% gate only rejects
candles whose range exceeds 5% — and even then only those closing in the bottom
portion. On a typical 4% range candle every close passes, including one at the
low. The range-relative form self-scales with volatility, which is exactly what
the strategy wants.

| Candle range | `high_offset <= 10` allows | Effective price proximity |
|---|---|---|
| 2% | 0.2% | 0.2% |
| 4% | 0.4% | 0.4% |
| 6% | 0.6% | 0.6% |
| 10% | 1.0% | 1.0% |

Guard against a zero-range candle (`H == L`) — treat it as a rejection, not a
division by zero.

On success the strategy returns:

```python
StrategySignal(
    direction="sell",
    strategy_name="wick_rejection",
    tp_pct=<resolved>,
    sl_pct=<resolved>,
    rationale="WickRejection: 1H close in top 10% of range (offset 4.1%) on 4.2% range",
)
```

### 3.2 Exit model

**TP** (`tp_mode`) — default `"prev_body_retrace"`:

Let the previous candle's body be
`body_top = max(prev_open, prev_close)`, `body_bottom = min(prev_open, prev_close)`,
`body_height = body_top - body_bottom`. Then

$$
\text{target} = \text{body\_top} - f \times \text{body\_height}, \qquad f = \texttt{tp\_body\_fraction} = 0.25
$$

$$
\text{tp\_pct} = \frac{\text{entry} - \text{target}}{\text{entry}} \times 100
$$

`body_top` is `max(O, C)`, not `C` — on a bearish previous candle the top is the
*open*, which is the level the thesis aims at.

Worked example — previous candle `O=100, C=104`, entry `106`:
`body_top=104`, `body_height=4`, `target = 104 − 0.25×4 = 103`,
`tp_pct = (106−103)/106×100 = 2.83%`.

Other modes:
- `"prev_open"`: target = previous candle's open (the full body retracement).
- `"prev_body_mid"`: target = `(body_top + body_bottom) / 2`.
- `"atr"`: `atr_tp_multiplier (0.75) × atr_pct` — volatility-scaled, thesis-agnostic.
- `"static"`: `tp_pct (1.5)`.

Degenerate cases (all modes):
- `body_height ≈ 0` (doji) → target ≈ `body_top`; guard the zero-height body.
- `target >= entry` (current candle gapped above the body top) → invalid short
  target; **skip the trade** rather than fall back, since the setup no longer
  matches the thesis.

**SL** (`sl_mode`):
- `"structural"` (default): the candle high plus an ATR buffer —
  `sl_pct = (high × (1 + sl_buffer_atr × atr_pct / 100) - entry) / entry × 100`
  with `sl_buffer_atr = 0.25`. If price reclaims the high, the thesis is wrong.
- `"atr"`: `sl_atr_multiplier (1.0) × atr_pct`
- `"static"`: `sl_pct (3.0)`
- `"none"`: `sl_pct = None` — requires `disable_protection = True`

### 3.3 Guardrail overrides

**R:R is not a meaningful constraint for this strategy.** The universe is
volatile by construction: a coin can run 50% against the position while the
target is a few percent. Judging the setup on reward-to-risk geometry would
reject every trade the thesis actually wants. WR therefore sets
`skip_rr_guard = True` by default — not as a workaround for a tight TP, but
because R:R is the wrong metric for a high-win-rate, deep-adverse-excursion
strategy. Expectancy and win rate are the evaluation criteria.

`disable_protection` is generalized from the `mean_reversion`-only special case
to any strategy that sets the key. `sl_mode="none"` is opt-in only — the default
`structural` stop keeps short-squeeze risk bounded.

### 3.4 Per-strategy leverage

`LauncherTpSl` gains `leverage_override: float | None`, read from
`strat_cfg["leverage"]`. `build_launcher_decisions` forwards it as
`decision["_leverage_override"]`; `_execute_decision` clamps it to
`[min_leverage, max_leverage]` and the instrument tier cap, assigns it to
`target_leverage`, and sets `leverage_override_reason` so the min-leverage block
is bypassed.

> **Semantics note:** launcher `notional_usd` is a fixed position notional, so
> leverage changes the **required margin**, not the position size. A
> `notional_scale_with_leverage` flag is out of scope for this phase.

### 3.5 Dedicated `vol` universe

A third screener list, ranked on *speed* rather than 24h aggregates. Its job is
to decide which fast-moving coins get live market data at all (and therefore
can be evaluated by WR); the WR funnel (§3.1) still checks the candle itself.

#### Speed

Speed blends two windows and takes the faster, so a coin qualifies either by
being volatile over the last few closed hours **or** by spiking right now:

```
closed_speed = recent_range_pct / hours_covered            # closed candles, last activity_lookback_hours
fast_speed   = |fast_move_pct| / (fast_move_window_minutes / 60)
speed        = max(closed_speed, fast_speed)                # % per hour
```

- `closed_speed` uses **only** the recent-activity features from
  `compute_recent_activity()`, divided by the hours actually covered
  (`hours_covered`), not the configured `activity_lookback_hours`. It must
  **never** fall back to the 24h `hl_range_pct`: dividing a 24h range by a 4h
  window inflates speed ~6x. No activity data → `closed_speed` is undefined.
- `fast_speed` comes from `_fast_move_from_history()` over the screener's
  rolling price samples. The current hour is only visible through this term —
  the activity window is closed-candle only — which is what lets a coin that
  was quiet for hours and spikes this hour reach the list in time for a
  forming-mode entry at ~:54. No usable price history → `fast_speed` is
  undefined.
- A symbol with neither term defined cannot enter the vol list.

#### Score, gates, cap

```
vol_score = speed_norm × 0.50 + vol_ratio_norm × 0.30 + decay_norm × 0.20
```

- Normalisers are the max over the vol pool (same style as SC/MR).
- `vol_ratio` = `recent_vol_ratio` (recent volume vs the symbol's own 24h
  average). `decay` = `decay_ratio`. A missing `decay_ratio` contributes `0` to
  the score (it does not exclude the symbol).
- Gates: `speed >= vol_min_speed_pct_per_hour` (default `1.0`) and
  `vol_ratio >= vol_min_vol_ratio` (default `0.5`, drops moves on unusually low
  participation). A missing `vol_ratio` **fails** the gate — WR needs evidence
  of participation.
- The vol list uses **only** its own gates. It does **not** apply the SC/MR
  `activity_min_ratio` gate (default `1.0`), which would make the looser
  `vol_min_vol_ratio` meaningless.
- Cap: `vol_max_symbols` (default `8`).
- The vol list is built in **both** dual-universe and legacy single-list mode
  (`dual_universe=False`); it is independent of the SC/MR split.

#### Data fetching

- The fast move must be computed for **every** candidate, not just fast-lane
  admissions above `fast_move_threshold_pct`. Live already records price
  samples on every screener tick regardless of `fast_lane_enabled`.
- `vol` is added to the union `selected` list (after SC and MR). Live only
  builds snapshots for `selected` (`update_symbols(selected)`), and the
  backtest only fetches candles for symbols in `UniverseSchedule.all_symbols`;
  without this, vol-only symbols would never be evaluated. Cost: up to
  `vol_max_symbols` extra symbols per snapshot poll.

#### Routing

`universe_for_strategy` routes `wick_rejection` → `vol_symbols`, falling back to
`selected` then `fallback_symbols` — the same fallback chain SC/MR use.

#### Parity limitation (forming mode)

Live re-runs the screener on every scheduler tick, so at ~:54 `fast_speed`
covers the current hour. The backtest rebuilds the universe only at interval
boundaries from 1H candles, so at H:00 the fast move is the *previous* hour's
close-to-close move: a coin spiking during hour H only enters the backtest's
vol list at H+1. Closing this would need sub-hour candles for the entire SWAP
universe (the engine's finer candles exist only for symbols already in the
universe). **Accepted:** the backtest misses fresh-spike forming-mode entries
live would take — it undercounts WR trades rather than inventing them.
Recorded in the schedule provenance. `candle_source="closed"` is unaffected:
both sides decide at the hour boundary.

---

## 4. Implementation steps

### Phase 1 — Forming-candle exposure *(required only for `candle_source="forming"`)*

This phase can be deferred: `candle_source="closed"` works without it. Ship it
first if you want the forming mode available from day one.

1. **`app/services/market_service.py`** — in `_build_closed_candle_snapshot`,
   before trimming, copy the forming candle into
   `new_indicators["forming_candle"]` as `{ts, open, high, low, close, volume}`.
   Set `forming_candle = None` on the untrimmed (already-closed) path so the key
   always exists.
2. **`app/services/backtest/snapshot_builder.py`** — in
   `build_with_incomplete_ltf`, set `indicators["forming_candle"]` from
   `incomplete_candle`. In `build()` (closed mode), set it to `None`.

> **Parity note:** `closed` eval mode has no forming candle, so
> `candle_source="forming"` produces zero trades there. Document in the strategy
> docstring and in `BACKTEST.md`; use `finer_ltf` for forming-mode backtests.

### Phase 2 — Strategy *(parallel with 3, 4)*

3. **New `app/services/strategies/wick_rejection.py`** — `WickRejectionStrategy`
   implementing the §3.1 funnel and §3.2 exit model.
4. **`app/services/strategies/defaults.py`** — add `DEFAULT_WICK_REJECTION` and
   register it in `DEFAULT_STRATEGY_CONFIG`.
5. Register the strategy in `market_service._strategies`,
   `backtest/engine._AVAILABLE_STRATEGIES`, and
   `backtest/runner.DEFAULT_STRATEGIES`.

### Phase 3 — TP/SL + guardrail plumbing *(parallel with 2, 4)*

6. **`app/services/launcher_tp_sl.py`**
   - Generalize `disable_protection`: read `strat_cfg.get("disable_protection")`
     for any strategy, keeping the existing MR condition as an OR so MR
     behaviour is unchanged.
   - Add `skip_rr_guard` from `strat_cfg.get("skip_rr_guard", False)`, OR'd with
     the existing `flip_tp_sl_active`.
   - Add `leverage_override` to `LauncherTpSl`, read from `strat_cfg["leverage"]`.
7. **`market_service.build_launcher_decisions`** — forward
   `"_leverage_override": resolved.leverage_override`.
8. **`market_service._execute_decision`** — apply the override (§3.4).

### Phase 4 — Dedicated `vol` universe *(parallel with 2, 3)*

9. **`app/services/screener.py`**
   - `score_universe`: accept an optional `price_history` mapping and
     `now_ts`; compute `closed_speed`, `fast_speed` and `speed` per candidate
     (§3.5) — `fast_speed` via `_fast_move_from_history()` for **every**
     candidate, independent of `fast_lane_enabled`.
   - Build a `vol_pool` with its own gates only (no `activity_min_ratio`), in
     both dual and legacy mode. Return `"vol"` and `"vol_pool_size"` in the
     result dict, and add a diagnostic line showing `speed` (with which term
     won), `vol_ratio` and `decay` per selected symbol.
   - Append `vol` to `selected` (after SC, then MR) so vol-only symbols are
     fetched.
   - `universe_for_strategy`: accept `vol_symbols` (default `[]`) and route
     `wick_rejection` to it.
10. **`strategies/defaults.py`** — `DEFAULT_SCREENER` gains `vol_enabled`
    (default `False`, so the list and its fetch cost only exist when wanted),
    `vol_max_symbols`, `vol_min_speed_pct_per_hour`, `vol_min_vol_ratio`.
11. **`backtest/universe.py`** — build `price_history` via
    `_price_history_at()` when `vol_enabled` **or** `fast_lane_enabled` (today
    only the latter), and pass it to `score_universe`. `UniverseInterval.vol`,
    `universe_at` passes it, `to_dict` includes it, `all_symbols` unions it
    (already true once vol is in `selected`). Record the forming-mode parity
    limitation (§3.5) in the schedule provenance.
12. **`market_service`** — pass `self._screener_price_history` to
    `score_universe`; `_screener_vol_symbols` attribute; `run_screener_if_due`
    stores `result["vol"]`; `_strategy_allowed_on_symbol` routes
    `wick_rejection`; `get_screener_universe` passes `vol_symbols`.
13. **`prompt_scheduler`** — expose `runtime_config["_screener_vol_symbols"]`
    alongside the SC/MR lists (~L180).

### Phase 5 — UI *(depends on 2, 4)*

14. **`app/ui/pages.py`**
    - New `ui.expansion("Wick Rejection")` card on the STRATEGY page (after
      Trend Pullback, ~L5130) with a `_set_wr_defaults()` button reading
      `strategy_defaults("wick_rejection")`. Stages: timeframe + HTF regime;
      volatility; entry; exits; risk.
    - Add `_strategies_cfg["wick_rejection"] = {...}` in
      `save_strategy_settings` (~L6259).
    - CFG page screener section: add the `vol_*` inputs (`vol_enabled`,
      `vol_max_symbols`, `vol_min_speed_pct_per_hour`, `vol_min_vol_ratio`).
15. **`backtest/sweep_catalog.py`** — `"wick_rejection": "WR"` in
    `STRATEGY_DISPLAY_NAMES` plus a `STRATEGY_SWEEP_PARAMS` block.

### Phase 6 — Docs + tests *(depends on all)*

16. `.github/skills/launcher-strategy-reference/SKILL.md` — Wick Rejection
    section (regime, core idea, TP/SL, failure modes, key params,
    tighten/loosen).
17. `CLAUDE.md` — note the `forming_candle` indicator key and the `vol` universe.
18. Tests:
    - `tests/test_strategies.py` — fires on a forming candle closing in the top
      10% of its range late in the hour; rejects on early elapsed / small range /
      low ATR / close too far below the high / zero-range candle;
      `sl_mode` variants produce the expected `sl_pct`;
      `sl_mode="none"` → `sl_pct is None`.
    - `tests/test_screener_dual_universe.py` — `wick_rejection` routes to the vol
      list; falls back to `selected` when vol is empty.
    - Vol screener (`tests/test_screener_activity.py` or a new
      `tests/test_screener_vol.py`): `closed_speed` divides by `hours_covered`
      and never uses the 24h range; a quiet-then-spiking coin qualifies via
      `fast_speed`; a symbol with neither term is excluded; missing
      `vol_ratio` fails the gate; missing `decay_ratio` scores 0;
      `activity_min_ratio` does not affect the vol list; vol is built with
      `dual_universe=False`; vol symbols are appended to `selected`;
      `vol_enabled=False` produces an empty list and no extra symbols.
    - `tests/test_backtest_universe.py` — `universe_at` returns the vol list;
      `to_dict` includes `vol`.
    - `tests/test_sweep_catalog.py` — WR has params and dotted keys.
    - New `tests/test_launcher_tp_sl.py` — generic `disable_protection`,
      `skip_rr_guard`, `leverage_override` pass-through.

---

## 5. Config reference

```python
DEFAULT_WICK_REJECTION = {
    "enabled": False,
    "analysis_timeframe": "1H",
    "htf_regime_preference": "off",
    # Entry
    "candle_source": "forming",       # forming | closed
    "max_high_offset_pct": 10.0,      # (H-C)/(H-L)*100 <= this → close in top 10% of range
    "min_candle_range_pct": 2.0,
    "min_atr_pct": 1.0,
    "min_candle_elapsed_pct": 90.0,   # forming mode: how far into the bar
    "max_candle_age_minutes": 10.0,   # closed mode: how stale the last bar may be
    # Exits
    "tp_mode": "prev_body_retrace",   # prev_body_retrace | prev_open | prev_body_mid | atr | static
    "tp_body_fraction": 0.25,         # target = body_top - f * body_height
    "atr_tp_multiplier": 0.75,        # used when tp_mode == "atr"
    "tp_pct": 1.5,                    # used when tp_mode == "static"
    "sl_mode": "structural",
    "sl_atr_multiplier": 1.0,
    "sl_pct": 3.0,
    "sl_buffer_atr": 0.25,
    # Risk / guardrails
    "leverage": None,
    "disable_protection": False,
    "skip_rr_guard": True,
    "min_reward_risk_ratio": 0.5,
    # Volume gate (opt-in)
    "require_min_volume": False,
    "min_volume_ratio": 0.7,
    "volume_lookback": 20,
    "flip_launcher_direction": None,
}
```

Screener additions (`DEFAULT_SCREENER`, §3.5):

```python
    "vol_enabled": False,               # build the vol list (adds up to vol_max_symbols to fetches)
    "vol_max_symbols": 8,
    "vol_min_speed_pct_per_hour": 1.0,  # max(closed_speed, fast_speed) gate
    "vol_min_vol_ratio": 0.5,           # own gate; activity_min_ratio does NOT apply
    # fast_speed reuses the existing fast_move_window_minutes (default 60)
```

---

## 6. Verification

1. `.venv/bin/python -m pytest tests/ -q` — all pass, no new failures.
2. `.venv/bin/python scripts/list_active_symbols.py --hours 3 --min-speed 1.0 --top 20`
   — confirm the vol ranking surfaces genuinely fast movers.
3. Backtest smoke (`candle_source="forming"`): `POST /backtest/run` with
   `strategy_names=["wick_rejection"]`, `evaluation_mode="finer_ltf"`,
   `evaluation_timeframe="15m"`, `universe_mode="screener"` — trades generated,
   `forming_candle` populated.
4. Backtest smoke (`candle_source="closed"`): same run with
   `evaluation_mode="closed"` — trades generated, no `forming_candle` needed.
5. Backtest `candle_source="forming"` in `closed` eval mode → **zero** WR trades
   (expected; documents the parity limitation).
6. Live dry-run (`candle_source="forming"`, `enabled=True`, `leverage=3`,
   `sl_mode="structural"`) — watch DEBUG logs for the funnel breakdown; confirm
   entries fire only in the last ~6 minutes of the hour.
7. Live dry-run (`candle_source="closed"`) — confirm entries fire within
   `max_candle_age_minutes` of the hour boundary and not later.
8. `disable_protection=True` + `sl_mode="none"` → entry with no TP/SL algo
   orders and no `stop_loss_required` block.

---

## 7. Risks and open items

| Risk | Mitigation |
|---|---|
| **Forming-candle parity** — `candle_source="forming"` cannot be reproduced in `closed` eval mode | `candle_source="closed"` works in both modes; document the `finer_ltf` requirement for forming |
| **Elapsed gate vs eval timeframe** — 15m eval caps elapsed at 75%, so `min_candle_elapsed_pct=90` never fires in forming-mode backtests | Default to 70, or use 5m/1m eval; also confirm the live scheduler interval is ≤ 300s |
| **High-offset gate on a zero-range candle** — `H == L` divides by zero | Treat as a rejection; covered by `min_candle_range_pct` upstream, but guard explicitly |
| **`max_high_offset_pct` too strict** — 10 (top 10% of range) may starve signals on noisy 1H alts | Sweep 10 / 15 / 20 / 25; loosen if trade count is too low |
| **TP target above entry** — a gap up leaves the body-retrace target invalid for a short | Skip the trade (no ATR fallback); log the reason |
| **`tp_body_fraction` too deep** — 0.25 may be unreachable on small bodies | Sweep 0.15 / 0.25 / 0.35 / 0.5; measure hit rate per fraction |
| **Leverage semantics** — `notional_usd` is fixed, so `leverage` only reduces required margin | Implement margin-only; add `notional_scale_with_leverage` later if needed |
| **`sl_mode="none"`** — unbounded short-squeeze exposure | Default is `structural`; `none` requires explicit opt-in |
| **VOL/SC overlap** — a symbol can appear in both lists, so SC could be long while WR signals a short | Live: `MarketService._launcher_symbol_conflict` (added 2026-11-18, `f833fca`) blocks a launcher entry when the symbol has an open position or another launcher strategy tracks it — before that fix, WR's SELL would have been executed as a `reduce_only` close of SC's long. Backtest: `allow_concurrent_strategies_per_symbol=False` (default) |
| **Forming-mode screener parity** — live sees the current hour's spike via `fast_speed`; the backtest universe is rebuilt only at the hour | Accepted (§3.5): the backtest undercounts fresh-spike WR entries rather than inventing them; recorded in provenance. `candle_source="closed"` unaffected |
| **Fetch cost** — vol adds up to `vol_max_symbols` symbols to every snapshot poll | `vol_enabled` defaults to `False`; cap at 8 |
