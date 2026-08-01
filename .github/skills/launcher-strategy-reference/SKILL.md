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
- `max_spike_extension_pct`
- `max_adx_for_entry`

### Tighten when
- entries happen after most of the move is already gone
- candles are large but momentum is already fading

### Loosen when
- too few breakout entries occur in clearly expanding volatility regimes

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
