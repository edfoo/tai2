---
name: log-diagnostics-playbook
description: 'Inspect tai2 runtime logs to diagnose losing strategies, stop-out causes, reward-to-risk blocks, churn, trade management issues, and which parameter families should be adjusted next.'
user-invocable: false
---

# Log Diagnostics Playbook

Use this skill when:
- the bot is losing money and you need evidence from logs
- a strategy seems to overtrade or get stopped out repeatedly
- you want to know whether losses come from entry quality, exit placement, or churn
- you need fast grep patterns and interpretation rules

## Primary log sources
- `logs/app.log`
- `logs/app.log.1`
- `logs/app.log.2`
- `logs/app.log.3`

## First commands to run

### 1. Recent realized PnL

```bash
grep -hE "Reconciled PnL" logs/app.log logs/app.log.1 logs/app.log.2 2>/dev/null | tail -100
```

Use this to see:
- recent winners and losers
- symbol concentration
- whether the system is net losing or just noisy

### 2. Recent launcher entries

```bash
grep -hE "Launcher signal" logs/app.log 2>/dev/null | tail -50
```

Use this to inspect:
- which strategies are firing
- TP/SL distances
- whether structural sizing is active (look for `[structural(...)]` in rationale when available)
- repeated entries on the same symbol

### 3. Guardrail and management events

```bash
grep -hE "Blocked: reward-to-risk ratio|TradeMgmt:|re-entry cooldown|Skimming" logs/app.log 2>/dev/null | tail -80
```

Use this to detect:
- R:R blocks
- churn controls
- breakeven / partials / time stop behavior
- whether skimming is interfering with natural trade development

## High-value grep patterns

### Strategy-specific diagnostics

```bash
grep -hE "MeanReversion:|VWAPReversion:|TrendPullback:|LiquiditySweep:|SpikeContinuation:" logs/app.log 2>/dev/null | tail -200
```

### R:R blocks

```bash
grep -hE "Blocked: reward-to-risk ratio" logs/app.log 2>/dev/null | tail -50
```

Interpretation:
- structural target too close
- invalidation too wide
- or entry quality poor enough that the strategy has no room to move

### Strong-trend rejection

```bash
grep -hE "ADX=.*> max=" logs/app.log 2>/dev/null | tail -80
```

Interpretation:
- confirms the ADX filter is actually doing work
- if there are still many losing trades, the threshold may still be too loose

### Distance cap rejection

```bash
grep -hE "distance=.*ATR > max=" logs/app.log 2>/dev/null | tail -80
```

Interpretation:
- confirms distance caps are blocking extreme moves
- if many bad trades still happen just below the cap, cap may still be too wide

### Missing confirmation

```bash
grep -hE "closeback\(|candle\(long=|no sweep|no pullback level touched" logs/app.log 2>/dev/null | tail -120
```

Interpretation:
- tells you whether confirmation filters are blocking enough bad setups

## Strategy attribution from logs

PnL lines do not include strategy names.

To estimate which strategy produced a trade:
1. find a `Reconciled PnL for SYMBOL`
2. pair it with the most recent prior `Launcher signal: SYMBOL ... [strategy_name]`
3. use that as heuristic attribution

This is usually accurate enough for ranking which strategy is hurting the system.

## How to interpret common patterns

### Pattern: repeated entries on the same symbol every few minutes
Likely causes:
- `trade_management.enabled = False`
- re-entry cooldown not seeding
- strategy filters too permissive

Action:
- check trade management config first
- then inspect entry strictness

### Pattern: many stop-outs with almost no R:R blocks
Likely cause:
- entry quality problem, not exit geometry

Action:
- tighten ADX, confirmation, and distance/proximity filters before touching guardrails

### Pattern: many R:R blocks after structural sizing rollout
Likely cause:
- structural TP too close or structural SL too wide

Action:
- tighten setup quality first
- inspect clamp ranges second
- lower guardrail only as last resort

### Pattern: win rate improves but pnl still negative
Likely cause:
- profits are clipped too early
- skimming too aggressive
- TP target too conservative

Action:
- inspect skimming threshold
- inspect structural TP clamp max
- inspect partial TP behavior

### Pattern: very few trades after a change
Likely cause:
- overshot tightening

Action order:
1. loosen ADX cap slightly
2. loosen distance/proximity slightly
3. relax confirmation slightly
4. revert timeframe experiment if needed

## Parameter family mapping from logs

### If logs show strong-trend losses
Change first:
- `max_adx`
- `max_adx_for_entry`
- `vwap_max_distance_atr`

### If logs show false reclaim / fake reversal losses
Change first:
- `require_closeback`
- `reclaim_ratio`
- `require_volume_spike`
- `candle_rejection_pct`

### If logs show late-trend entries
Change first:
- `max_adx_for_entry`
- `pullback_proximity_pct`
- `max_spike_extension_pct`

### If logs show structural R:R blocks
Change first:
- setup quality
- then structural clamp ranges
- then guardrail threshold only if justified

## Minimum reporting format

When using logs to recommend a change, always report:
1. the strategy involved
2. the observed pattern from logs
3. the likely failure mode
4. the smallest parameter change to test next
5. the expected tradeoff

Example:
- "VWAP Reversion is still entering trend-continuation moves just below the distance cap. Lower `vwap_max_distance_atr` from 3.0 to 2.5. Expect fewer signals and fewer knife-catch losses."
