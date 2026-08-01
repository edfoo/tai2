---
name: strategy-tuning-playbook
description: 'Diagnose tai2 trading performance, inspect launcher logs, identify stop-out causes, reward-to-risk blocks, churn, and strategy-specific failure modes, then recommend launcher, strategy, guardrail, trade-management, and timeframe parameter changes to improve profitability without rereading the codebase.'
user-invocable: false
---

# Strategy Tuning Playbook

Use this skill when:
- trades are mostly stopped out
- a strategy is consistently losing money
- reward-to-risk guardrail blocks are appearing
- trade count is too high or too low
- the bot keeps re-entering the same symbol
- structural TP/SL was added and needs interpretation
- you need parameter recommendations without remapping the code

## Goal

Turn recent runtime behavior into concrete parameter changes.

## Companion Skills

Use these when you need more focused detail instead of the full workflow:
- `launcher-strategy-reference` — per-strategy regime, failure modes, TP/SL logic, and the main parameter levers
- `log-diagnostics-playbook` — exact grep patterns, log interpretation, and how to map observed runtime behavior to parameter families

The workflow is:
1. Identify which strategy is losing.
2. Decide whether the problem is entry quality, exit placement, churn, or guardrails.
3. Map the observed failure mode to the smallest safe config change.
4. Prefer tightening entry quality before loosening guardrails.

## Repo map

### Runtime config
- Main runtime config object: `app.state.runtime_config`
- Initialized in: `app/main.py`
- Launcher strategy config path: `config["launcher"]["strategies"][<strategy_name>]`
- Position-management config path: `config["strategy"]`
- Guardrails config path: `config["guardrails"]`
- Analysis timeframe config path: `config["ta_timeframe"]`

### Persistence
- CFG / STRATEGY page values are persisted via: `app/db/postgres.py`
- Important implication: code defaults do not automatically affect existing live setups. If the DB already has old values, the user must click "Set Recommended Defaults" and Save on the STRATEGY page.

### Core execution path
- Strategy implementations: `app/services/strategies/*.py`
- Launcher signal construction: `app/services/market_service.py` in `build_launcher_decisions()`
- Strategy evaluation entrypoint: `app/services/market_service.py` in `_launcher_evaluate_signals()`
- R:R guardrail block: `app/services/market_service.py` in `handle_llm_decision()` under `require_reward_risk_ratio`
- Trade management logic: `app/services/market_service.py` in `_check_trade_management()`
- Skimming logic: `app/services/market_service.py` in `_check_skimming()`
- Closed-candle live alignment: `app/services/market_service.py` in `_build_closed_candle_snapshot()`

## Current strategy design summary

### Mean Reversion
- Best regime: chop, low-to-moderate ADX
- Typical failure mode: fading a move that is still accelerating; structural TP at BB middle can be too close and get R:R-blocked
- Structural sizing: TP at BB middle band, SL beyond entry wick, ATR-clamped
- Highest risk of R:R blocks among the strategies

### VWAP Reversion
- Best regime: chop with large but not extreme VWAP deviation
- Typical failure mode: catching a falling knife in a strong trend
- Structural sizing: TP at VWAP, SL beyond extension candle, ATR-clamped
- Most important controls: `max_adx`, `vwap_min_distance_atr`, `vwap_max_distance_atr`, `require_closeback`

### Trend Pullback
- Best regime: established trend, orderly pullback, not too mature
- Typical failure mode: entering late in a mature trend or on a pullback that is too deep
- Structural sizing: TP at nearest swing high or low, SL beyond pullback candle, ATR-clamped
- Most important controls: `min_adx`, `max_adx_for_entry`, `pullback_proximity_pct`, `require_bullish_candle`

### Liquidity Sweep
- Best regime: range/chop with visible stop-hunt wick and reclaim
- Typical failure mode: a real breakout mistaken for a sweep, or a very deep wick in a narrow range
- Structural sizing: TP at opposite swing extreme, SL beyond sweep wick, ATR-clamped
- Most important controls: `max_adx`, `require_volume_spike`, `reclaim_ratio`, `sweep_buffer_pct`

### Spike Continuation
- Best regime: volatility expansion with still-building momentum
- Typical failure mode: entering at the top after extension is already exhausted
- Most important controls: `max_spike_extension_pct`, `acceleration_min_ratio`, `require_rsi_rising`, `require_volume_rsi_rising`, `max_adx_for_entry`

## Known repo-specific lessons

These are already validated in this repo and should be assumed unless new evidence contradicts them:

1. Bad performance was not fixed by flipping trade direction.
- Mirroring TP/SL preserves bad geometry.
- If entries are wrong because of regime, flipping usually produces the opposite side of the same low-edge process.

2. The live path previously diverged from backtest because it evaluated on the forming candle.
- Live now evaluates launcher strategies on the previous closed candle.
- If signal frequency or quality changes unexpectedly, confirm that this behavior still exists.

3. `trade_management.enabled = False` causes churn.
- No breakeven
- No partial TP
- No time stop
- No re-entry cooldown
- If the same symbol keeps getting traded every few minutes, check this first.

4. Structural TP/SL can increase R:R guardrail blocks.
- This is often correct behavior.
- It means the structural target is too close or the invalidation is too wide.
- Prefer tightening the entry setup first.

5. VWAP Reversion needed both a max ADX filter and a max distance cap.
- Strong-trend reversion entries were a major failure mode.

## First-pass log workflow

Use recent logs first. Do not start by rereading strategy code.

### 1. Identify which symbols and strategies are losing

Useful patterns:
- `Reconciled PnL for`
- `Launcher signal:`
- `Blocked: reward-to-risk ratio`
- `TradeMgmt:`
- strategy-specific `no signal` diagnostics like `VWAPReversion:` or `TrendPullback:`

Suggested shell commands:

```bash
grep -hE "Reconciled PnL" logs/app.log logs/app.log.1 logs/app.log.2 2>/dev/null | tail -100
```

```bash
grep -hE "Launcher signal" logs/app.log 2>/dev/null | tail -50
```

```bash
grep -hE "Blocked: reward-to-risk ratio|TradeMgmt:|re-entry cooldown" logs/app.log 2>/dev/null | tail -50
```

### 2. Pair PnL with the prior launcher signal

The PnL reconciliation lines do not include strategy names.
Use the most recent prior `Launcher signal:` for the same symbol as a heuristic strategy attribution.

If needed, rank strategy performance by:
- total pnl
- win count / loss count
- average win
- average loss
- recent 24h only

### 3. Inspect block reasons

Common grep targets:

```bash
grep -hE "no signal|no entry signal|Blocked:" logs/app.log 2>/dev/null | tail -200
```

Watch specifically for:
- `ADX=... > max=...`
- `distance=... ATR > max=...`
- `closeback(long=blocked, short=blocked)`
- `reward-to-risk ratio ... below minimum ...`
- `re-entry cooldown`

## Decision tree

### Case A: Many losses, few or no R:R blocks

Interpretation:
- Entries are poor, not exits

What to do:
1. Tighten entry filters before touching guardrails.
2. Prefer ADX tightening, distance caps, and stronger confirmation.
3. Only then consider higher-level changes like timeframe.

### Case B: Many R:R blocks after structural sizing rollout

Interpretation:
- The structural target is too close or the structural invalidation is too wide.

What to do first:
1. Tighten entry quality so the setup has more room to travel.
2. Inspect whether the strategy is in the wrong regime.
3. Only lower `min_reward_risk_ratio` if the blocked trades are otherwise high-quality.

Do not immediately disable the guardrail.

### Case C: Same symbol re-enters repeatedly

Interpretation:
- Churn problem

Check:
1. `trade_management.enabled`
2. `reentry_cooldown_seconds`
3. whether the strategy itself is too permissive
4. scheduler interval / launcher entry interval

### Case D: Win rate rises but pnl remains negative

Interpretation:
- TP is too small, skimming too aggressive, or good trades are clipped too early

Check:
1. Skimming threshold
2. Partial TP settings
3. Structural TP clamp minimum and maximum
4. Whether TP is being clamped below a reasonable structural target

### Case E: Few trades after tightening

Interpretation:
- Filters too strict or timeframe too slow

Relax in this order:
1. raise ADX max slightly
2. loosen proximity or distance thresholds slightly
3. relax confirmation thresholds slightly
4. lower timeframe only if noise reduction experiment clearly overshot

## Parameter playbook by strategy

### Mean Reversion

If stop-outs remain frequent:
- decrease `rsi_oversold` and increase `rsi_overbought` for deeper entries
- require stricter candle rejection
- keep `require_vwap_reversion` off unless testing shows it improves quality
- if structural R:R blocks are frequent, the BB middle may be too close relative to the wick; tighten entries before lowering the guardrail

If too few signals:
- loosen RSI thresholds gradually
- relax BB proximity slightly
- lower `min_bb_bandwidth` slightly

### VWAP Reversion

If it still fades strong trends:
- lower `max_adx`
- lower `vwap_max_distance_atr`
- keep `require_closeback = True`

If it misses good reversions:
- raise `vwap_max_distance_atr` slightly
- raise `max_adx` slightly, but only after checking that strong-trend losses are under control

### Trend Pullback

If it enters too late:
- lower `max_adx_for_entry`
- tighten `pullback_proximity_pct`
- keep candle confirmation on

If it misses clean pullbacks:
- loosen `pullback_proximity_pct` slightly
- lower `min_adx` slightly only if it is filtering too much real trend

### Liquidity Sweep

If sweep trades fail into continuation:
- lower `max_adx`
- require volume spike
- increase `reclaim_ratio`
- increase `sweep_buffer_pct`

If TP is rarely reached:
- inspect whether the opposite swing target is too far and getting clamped
- consider lowering `atr_max_tp_mult` only if targets are consistently unrealistic

### Spike Continuation

If it buys tops or sells bottoms:
- lower `max_spike_extension_pct`
- increase `acceleration_min_ratio`
- keep RSI and volume-rising requirements on
- lower `max_adx_for_entry` if the trend is already too mature when entries happen

## Guardrail handling

### Reward-to-risk guardrail

Current expectation:
- some structural-sized trades will be blocked
- Mean Reversion is the most likely strategy to be blocked
- VWAP Reversion is the least likely among the structural strategies

If blocks become too frequent, consider in this order:
1. tighten the entry setup
2. inspect structural TP/SL clamp ranges
3. inspect whether TP target choice is too conservative
4. only then consider lowering `min_reward_risk_ratio`

### Skimming

Skimming is a position-management overlay, not an entry-quality fix.
Use it when trades often move into profit and then give it back.
Do not use it as the first response to bad entries.

## Timeframe guidance

The analysis timeframe affects indicator smoothness and signal count.

Rules of thumb:
- moving from `15m` to `1H` reduces noise and usually reduces trade count
- if trades are too sparse after moving to `1H`, revert to `15m`
- timeframe changes are broad changes affecting all launcher strategies, so prefer parameter tuning first

## Safe tuning order

Use this order unless the logs strongly indicate otherwise:

1. Confirm live config actually matches intended defaults.
2. Check whether churn controls are on (`trade_management`).
3. Tighten entry filters.
4. Validate structural TP/SL behavior and R:R blocks.
5. Adjust position-management overlays like skimming only after entry quality is acceptable.
6. Loosen guardrails last.

## Output style for agents using this skill

When recommending changes:
- identify the exact strategy first
- state the observed failure mode from logs
- recommend the smallest parameter change that addresses that failure mode
- explain the expected tradeoff
- separate "change now" from "watch and revisit"

## Example recommendations

Bad:
- "Try changing a few thresholds."

Good:
- "VWAP Reversion is buying into strong-trend extensions. Lower `max_adx` from 25 to 22 and `vwap_max_distance_atr` from 3.0 to 2.5. Expect fewer signals and fewer knife-catch entries."

- "Mean Reversion is getting R:R-blocked because TP at the BB middle is too close relative to the entry wick. Tighten the entry by requiring deeper RSI and stronger rejection before lowering the guardrail."
