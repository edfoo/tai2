---
name: launcher-strategy-reference
description: 'Reference guide for tai2 launcher strategies, their market regimes, structural TP/SL logic, failure modes, and the main parameters to tighten or loosen without rereading strategy code.'
user-invocable: false
---

# Launcher Strategy Reference

Use this skill when:
- you need a fast reference for a launcher strategy
- you want to know which parameters matter for a given strategy
- you want to understand expected regime, failure mode, and TP/SL logic
- you need to compare strategies without rereading code

## Config location

Launcher strategy config lives under:
- `config["launcher"]["strategies"][<strategy_name>]`

Implementations live in:
- `app/services/strategies/mean_reversion.py`
- `app/services/strategies/vwap_reversion.py`
- `app/services/strategies/trend_pullback.py`
- `app/services/strategies/liquidity_sweep.py`
- `app/services/strategies/spike_continuation.py`
- `app/services/strategies/wick_rejection.py` (entry timing: `app/services/wick_rejection_trigger.py`)

## Mean Reversion

### Best regime
- chop
- low to moderate ADX
- BB extremes with visible exhaustion

### Core idea
Fade overextension back toward the mean.

### Structural TP/SL
- TP: BB middle band
- SL: beyond entry candle wick plus ATR buffer
- ATR fallback and clamp available

### Main failure mode
- fading a move that is still accelerating
- TP too close relative to wick-based SL, causing R:R blocks

### Most important parameters
- `rsi_oversold`, `rsi_overbought`
- `max_adx`
- `require_bb_position`
- `candle_rejection_pct`
- `use_structural_sizing`

### Tighten when
- many stop-outs happen immediately after entry
- many R:R guardrail blocks appear
- entries happen in strong directional candles

### Loosen when
- no signals for long periods in otherwise choppy markets

## VWAP Reversion

### Best regime
- chop or controlled reversion after extension
- not a strong directional trend

### Core idea
Price stretched far from VWAP should revert back toward VWAP.

### Structural TP/SL
- TP: VWAP
- SL: beyond extension candle low/high plus ATR buffer
- ATR fallback and clamp available

### Main failure mode
- catching a falling knife or shorting a squeeze in a strong trend

### Most important parameters
- `vwap_min_distance_atr`
- `vwap_max_distance_atr`
- `max_adx`
- `require_closeback`
- `require_regime`
- `use_structural_sizing`

### Tighten when
- trades lose in strong trends
- entries happen at 5+ ATR from VWAP
- price keeps extending after entry

### Loosen when
- good-looking reversion setups are skipped because they are only slightly beyond the current caps

## Trend Pullback

### Best regime
- established HTF trend
- orderly pullback to EMA or VWAP
- not too early, not too late in the trend

### Core idea
Join the main trend after price pulls back to value.

### Structural TP/SL
- TP: nearest swing high/low
- SL: beyond pullback candle low/high plus ATR buffer
- ATR fallback and clamp available

### Main failure mode
- entering late in a mature trend
- entering on a pullback that is too deep and is actually a reversal

### Most important parameters
- `pullback_proximity_pct`
- `min_adx`
- `max_adx_for_entry`
- `require_bullish_candle`
- `use_vwap_as_level`
- `use_structural_sizing`

### Tighten when
- trend-following trades reverse quickly after entry
- entries happen at very high ADX
- price overshoots the pullback level deeply before stopping out

### Loosen when
- clean pullbacks are being missed and trend quality remains good

## Liquidity Sweep

### Best regime
- chop/range
- obvious stop-hunt wick with reclaim
- not a strong breakout trend

### Core idea
Fade a stop-run after price sweeps a prior swing and reclaims the range.

### Structural TP/SL
- TP: opposite swing extreme
- SL: beyond sweep wick plus ATR buffer
- ATR fallback and clamp available

### Main failure mode
- real breakout mistaken for a sweep
- narrow range with a very deep wick produces poor geometry

### Most important parameters
- `sweep_buffer_pct`
- `reclaim_ratio`
- `require_volume_spike`
- `max_adx`
- `use_structural_sizing`

### Tighten when
- sweeps fail into continuation
- many losses happen on strong-trend names

### Loosen when
- obvious reclaim setups are not triggering

## Spike Continuation

### Best regime
- momentum expansion
- rising volume and still-building RSI/volume RSI

### Core idea
Ride a fresh spike before it exhausts.

### TP/SL
- currently ATR/static-based, depending on config
- no structural TP/SL rollout assumptions should be made unless code changes confirm it

### Main failure mode
- entering at the top after the move has already extended too far

### Most important parameters
- `volume_rsi_min`
- `rsi_min`, `rsi_max`
- `acceleration_min_ratio`
- `max_spike_extension_atr`
- `max_adx_for_entry`

### Tighten when
- entries happen after most of the move is already gone
- candles are large but momentum is already fading

### Loosen when
- too few breakout entries occur in clearly expanding volatility regimes

## Wick Rejection

Differs from every other strategy: it is **trigger-only**, owns its **exits**
and its **sizing**, and trades only the screener's **vol** universe. Plan and
as-built notes: `docs/objectives/wick_rejection_plan.md`.

### Best regime
- fast, high-volume coins (vol universe, ranked on % per hour; needs
  `screener.vol_enabled`, otherwise it falls back to the combined list)
- a strong green 1H candle trading at or near its high in the final minute
- recent hours already printed a big green body (blow-off, not a fresh trend)

### Core idea
Short seconds before the 1H close, betting the candle closes below the
current price (the wick forms as late buyers stop). The scheduler tick never
evaluates it (`TRIGGER_ONLY_STRATEGIES`); `WickRejectionTrigger` wakes at
H:00 − `entry_seconds_before_close`, takes the websocket price (REST if stale),
patches the forming candle with it, ranks signals by `rank_score` and enters
up to `max_concurrent_positions` / `max_total_margin_pct`.

### Funnel (vetoes in order)
1. forming 1H candle present and `as_of_ts` known
2. seconds to close inside `min_seconds_to_close`..`max_seconds_to_close`
3. forming candle green (`require_forming_green`)
4. range `(H − L)/L ≥ min_candle_range_pct` (zero range vetoes)
5. high offset `(H − P)/(H − L) ≤ max_high_offset_pct` (price above the recorded high counts as the new high)
6. at least `min_big_bodies` green bodies ≥ `min_body_ratio` × avg |body| of the prior `body_avg_window`, within `setup_lookback_candles` (+ the forming candle if `setup_include_forming`)
7. optional `htf_regime_preference` (default off)

### TP/SL and exits (strategy-owned)
- TP `tp_pct` in `price` or `margin` units (margin % ÷ leverage actually used)
- emergency stop `exchange` (OKX SL order), `software` (bot closes at market,
  unprotected while the app is down) or `off` (needs `disable_protection`)
- time stop `max_hold_minutes` closes **win or lose**; own `reentry_cooldown_minutes`
- global breakeven/partial/trailing are skipped unless `use_global_trade_management`
- R:R guard skipped (`skip_rr_guard`) — a tiny TP against a wide stop is by design
- survives restarts via `_rehydrate_open_positions` (needs Postgres)

### Sizing
`margin_usdt` (or `margin_pct` of equity) × `leverage`, independent of
`launcher.notional_usd`. Leverage is clamped to guardrail min/max with the
margin kept; `max_position_pct` still caps the position. The STRATEGY card
shows the resulting USDT risk.

### Main failure mode
- the candle keeps squeezing through the close (continuation, not rejection)
  → the emergency stop or the time stop at a loss
- late entries: price fetched too close to the close, or slippage on thin books
- low hit rate cannot pay for the wide stop: the edge relies on many small TPs

### Most important parameters
- `max_high_offset_pct`, `min_candle_range_pct`
- `min_body_ratio`, `setup_lookback_candles`
- `tp_pct` / `emergency_stop_pct` (and their units)
- `max_hold_minutes`
- `leverage`, `margin_usdt`, `max_concurrent_positions`

### Tighten when
- stops hit on candles that were still accelerating → lower `max_high_offset_pct`, raise `min_body_ratio`
- many trades on quiet coins → raise `vol_min_speed_pct_per_hour` / `min_candle_range_pct`

### Loosen when
- few signals on clearly exhausted candles → raise `max_high_offset_pct` or lower `min_body_ratio`

### Momentum entry mode (optional, off)
`entry_mode`: `clock` (default) | `momentum` | `momentum_or_clock`. Watches the
trade tape from `watch_seconds_before_close` and enters inside
`momentum_entry_max_seconds_to_close` once the price has stalled
(`stall_min_seconds` since the high, within `max_below_high_pct`, `ret_60s >=
min_ret_60s`). Thresholds came from 33 trades — sweep clock vs
`momentum_or_clock` in the backtest (1m approximation) before using it live.

### Backtest note
Needs `evaluation_mode="finer_ltf"` with 1m eval candles: it enters at the
close of the 1m step 60 s before the hour (live: 40 s). Closed-candle mode is
unsupported and recorded in `data_provenance`.

## Shared guidance

### Structural sizing note
Structural TP/SL can improve realism, but it can also produce more R:R guardrail blocks when the target is too close or the invalidation is too wide.

### Guardrail note
If a structurally-sized trade is blocked by R:R:
1. inspect whether the setup is weak
2. tighten entry quality first
3. only loosen the guardrail last

### Timeframe note
If changing `ta_timeframe`, all launcher strategies are affected. Prefer strategy parameter tuning first unless noise is obviously the dominant issue.
