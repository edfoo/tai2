# tai2

`tai2` is a cryptocurrency trading assistant that combines FastAPI, NiceGUI, OKX market data, Redis state caching, a TimescaleDB-compatible PostgreSQL backend, and an LLM reasoning engine.

## Getting Started

1. **Clone & enter the repo**
   ```bash
   git clone <repo-url>
   cd tai2
   ```
2. **Activate the project environment**
   ```bash
   va  # helper script that sources .venv/bin/activate
   ```
3. **Install dependencies**
   ```bash
   uv sync
   ```

## Environment Variables

`tai2` reads its configuration via `pydantic-settings`. Create a `.env` file or export these variables:

- `OKX_API_KEY`, `OKX_SECRET_KEY`, `OKX_PASSPHRASE`
- `OPENROUTER_API_KEY`
- `DATABASE_URL` (e.g. `postgresql://user:pass@host:5432/tai2`)
- `REDIS_URL` (e.g. `redis://localhost:6379/0`)
- `POLL_INTERVAL` (seconds, defaults to 180)
- `TELEGRAM_BOT_TOKEN` (optional — bot token from @BotFather; enables Telegram alerts)
- `TELEGRAM_CHAT_ID` (optional — target chat or group ID; required when bot token is set)

## Running the App

```bash
uv run uvicorn app.main:app --reload
```
Visit http://localhost:8000/ to see the NiceGUI landing page.

## Testing

```bash
uv run pytest
```

## Project Structure

```
app/
  core/            # config and security
  db/              # PostgreSQL/Timescale access
  models/          # Pydantic schemas (e.g., ExecutedTrade)
  services/        # Redis state + OKX/LLM logic
    strategies/    # Pluggable Launcher strategies (Mean Reversion, …)
    backtest/      # Backtesting engine (data fetcher, simulator, metrics)
  ui/              # NiceGUI components and pages
  main.py          # FastAPI entry point

tests/             # pytest suites
```

## Roadmap (Phases)

1. **Initialization** – skeleton app, config, NiceGUI landing page, smoke tests.
2. **Data Layer** – TimescaleDB schema, asyncpg pool, Redis `StateService`.
3. **Market Engine** – OKX REST/WebSocket, indicators, Redis snapshots.
4. **Reasoning Engine** – OpenRouter-based LLM decisions and trade execution.
5. **Frontend** – NiceGUI pages for LIVE/TA/STRATEGY/BACKTEST/HISTORY/DEBUG/PROMPT/CFG.
6. **Integration** – FastAPI startup orchestration, global error surface, final docs/tests.
7. **Backtesting** – Historical strategy backtesting with simulated broker and metrics.

## Notes

- Use `uv run` to execute any Python commands inside the managed environment.
- Switching to Python 3.12+ is recommended because `pandas-ta` only supports 3.12 or newer.

## Prompt Scheduler

Enable "Auto Prompt Scheduler" on the CFG page to have the backend iterate every tracked symbol automatically. The FastAPI lifespan wires those values into the `PromptScheduler`, which reuses the same logic as the `/llm/prompt` and `/llm/execute` endpoints. Disable the toggle to keep prompts purely manual.

### Trigger mode

The scheduler has two trigger modes, configured on the CFG page:

- **Scheduled** — the default. After each tick completes, the scheduler sleeps for a fixed interval (minimum 30 seconds) before starting the next one.
- **Consecutive** — after each tick completes, the scheduler polls the open-position snapshot every 10 seconds and starts the next tick as soon as all positions are flat. Useful for high-frequency Launcher-only strategies that open and close on every cycle.

### Decision source (Launcher mode)

Each tick can source its entry decisions from the LLM, from the Launcher rule engine, or from both:

- **Disabled** — Launcher is inactive. The scheduler calls the LLM for every symbol concurrently and acts on whatever it returns.
- **Launcher only** — no LLM calls are made. The Launcher evaluates rule-based technical signals for each symbol synchronously and emits BUY/SELL/skip decisions directly. Supports its own scheduling sub-mode (`timer`: check every N seconds; `on_close`: fire when all positions clear).
- **LLM + Launcher filter** (`llm_with_filter`) — the LLM is called for all symbols as normal, but before any order is placed the Launcher evaluates the same symbol independently. If the Launcher's indicator signal conflicts with the LLM's direction, the trade is vetoed. If they agree, the Launcher may amend the trade's notional size, take-profit, and stop-loss with its own configured values before execution.

**CFG page control states** — the Scheduler toggle is the master on/off; secondary controls are automatically enabled or disabled to prevent conflicting settings:

| Scheduler toggle | Launcher mode | Trigger | → Scheduler Trigger field | → Prompt Interval field |
|---|---|---|---|---|
| **OFF** | any | any | disabled | disabled |
| ON | **Launcher only** | any | disabled | disabled |
| ON | LLM / filter / disabled | **Consecutive** | enabled | disabled |
| ON | LLM / filter / disabled | Scheduled | enabled | **enabled** |

### Execution ordering by confidence

Each scheduler tick runs in four phases to ensure the highest-quality setups get first access to available capital:

1. **Collect decisions.** In LLM mode all symbols are queried concurrently (pure I/O). In Launcher-only mode signals are evaluated synchronously, which is fast.
2. **Sort.** BUY/SELL decisions are ranked by `confidence` descending, then `risk_score` ascending, so the most compelling setup executes first.
3. **Execute BUY/SELL — sequential.** Trades fire one at a time in ranked order. Each call to `handle_llm_decision` fetches a live balance, so the second trade automatically sees the reduced available equity left after the first trade committed funds. Size is clipped accordingly; stop-loss and take-profit levels are preserved unless Launcher filter mode overrides them.
4. **Record HOLDs — concurrent.** HOLD decisions do not place orders and carry no budget impact, so they are persisted in parallel. (Launcher-only mode skips this phase — signals are binary.)

This design prevents a lower-confidence trade from racing a higher-confidence one to the same USDT pool and avoids the OKX `51008` "insufficient balance" error that occurs when two isolated-margin bootstraps each size to 50 % of equity simultaneously.

If the daily loss guard trips, the scheduler logs an execution alert, automatically pauses itself, and surfaces a "Reset Lock & Resume" control on the LIVE page. Once equity recovers above the configured drawdown cap, hit that button to re-enable auto prompts without digging through the CFG page.

## Configuration Controls

Open the `CFG` page in the NiceGUI UI to tune the runtime behavior. Key controls and their impact:

- **Execution Guardrails** – Max leverage, max position pct, daily loss cap, cooldown/hold period, hourly trade limit, and alignment switch are enforced before any OKX order is attempted. They prevent the LLM from overtrading or flipping sides without closing a position.
- **ATR Risk Per Trade %** – Caps position size so that a full stop-out (measured as ATR × 1.5 from entry) loses at most this percentage of equity. Implements the 1% risk model: `max_notional = (equity × risk%) / (ATR_stop / price)`. Leave blank to disable.
- **CVD Guard** – Blocks BUY/SELL entries when Cumulative Volume Delta momentum conflicts with the trade direction. Configurable lookback window and minimum slope threshold; neutral CVD never blocks.
- **OB Wall Guard** – Blocks BUY/SELL entries when a dominant resting limit-order wall on the opposing side sits within a configurable % of current price. A level qualifies as a wall when its size exceeds N× the average level size across the full book depth.
- **Snapshot Max Age** – Blocks prompt generation and trading decisions if the cached market snapshot is older than the threshold, forcing fresh data before acting.
- **WS Update Interval** – Sets the background poller cadence (`POLL_INTERVAL`) in seconds. The poller runs continuously regardless of the scheduler and is the sole refresh source when the scheduler is off. It also runs fill reconciliation against the DB every other tick. When the scheduler is enabled, each tick forces its own full snapshot rebuild before evaluating signals, so indicators (RSI, ADX, BB, etc.) are at most one scheduler-interval old — not one poller-interval old. The poller then acts as a fallback safety net and keeps data fresh for the UI and strategy loops between ticks.
- **Live Websocket Stream** – Toggles the high-frequency OKX websocket listener. Disable it to rely solely on the poller (quieter logs, lower network use) while keeping periodic snapshots.
- **Auto Prompt Scheduler + Interval** – Enables periodic, automatic prompt execution for every enabled symbol. Interval must stay above 30 seconds to avoid rate limits.
- **Model Select + Response Schema** – Choose the OpenRouter model and optionally override the response schema JSON used to parse structured reasoning outputs.
- **Trading Pairs** – Defines which perpetual instruments the engine tracks. Changing this updates Redis snapshots, indicators, and scheduler coverage.
- **Live Execution Switch** – Master toggle for automated OKX order placement. Trade mode (cross/isolated) and default min order size live here as well.
- **Per-Symbol Min Sizes** – Optional overrides to enforce instrument-specific minimum contract sizes. The helper converts USDT budgets into contract sizes using the latest price snapshot.
- **Fee Window Hours** – Controls how many trailing hours of OKX fees are aggregated in the LIVE view.
- **OKX Sub-Account + Routing** – Lets you target a specific OKX sub-account and declare whether the API key belongs to the master account (so `subAcct` is appended to requests).
- **OKX Environment Flag** – Chooses between live trading (`0`) and the OKX demo environment (`1`). Clients are rebuilt automatically when the flag changes.
- **Prompt Versions** – Load prior prompt templates, clone them, or save new immutable versions for A/B testing. The preview pane updates in real time.

Open the `STRATEGY` page to configure autonomous position-management strategies that run independently of LLM decisions on a fast refresh loop:

- **Mean Reversion Scalping** – Launcher entry strategy: rule-based RSI mean-reversion entries with optional CMF, HTF trend, ADX, BB position, and footprint-delta filters. Has its own TP/SL and Dynamic TP (BB bandwidth) settings. Enable/disable independently; the Launcher must also be set to `launcher_only` or `llm_with_filter` mode on the CFG page.
- **Skimming** – Closes any position whose unrealised PnL ratio crosses a configurable threshold.
- **Protector** – Locks in a portion of profit by ratcheting the stop-loss upward as the position gains.
- **Commutator** – Reverses a losing position once after a configurable drawdown.
- **Alternator** – Oscillates between long and short on profit/loss thresholds. Supports dynamic thresholds (based on average candle amplitude), trailing-reverse mode, trailing-close, candle-position filter, footprint-delta filter, continuous LLM supervision, and **OB Wall Suppression** (blocks flips when a dominant opposing order-book wall is detected within proximity of the current price).
- **OB Wall Dynamic Stop-Loss** – Independent of Alternator; anchors stop-losses to the nearest dominant supporting limit-order wall (bid wall for LONGs, ask wall for SHORTs). The stop only ever moves in the profit direction; it is never loosened. Configurable proximity, wall-ratio threshold, minimum improvement gate, and buffer behind the wall.

When you click **Save**, the app persists the configuration (PostgreSQL for guardrails/prompt versions/execution settings, Redis for runtime snapshot state) and rehydrates all services in-place: MarketService gets new symbols, websocket or poll intervals, and OKX credentials; the scheduler updates its cadence; the LLM service swaps models; and the UI log buffers announce the change.


## Launcher strategies

The Launcher iterates through registered strategies on each scheduler tick; the first strategy to fire a signal wins. Strategies are pluggable — each implements a `Strategy` protocol with an `evaluate()` method that returns `"buy"`, `"sell"`, or `None`. Strategy configs are namespaced under `config["launcher"]["strategies"][<name>]`.

To add a new strategy:
1. Create `app/services/strategies/my_strategy.py` implementing the `Strategy` protocol
2. Register it in `MarketService._strategies`
3. Add a card on the STRATEGY page with its own enable/disable switch and config fields

### Mean Reversion Scalping

The built-in Mean Reversion strategy uses these indicators, all of which must agree simultaneously:

| Indicator | Buy | Sell | Configurable |
|---|---|---|---|
| RSI (required) | < RSI Oversold (default 35) | > RSI Overbought (default 65) | Yes |
| CMF (optional) | CMF > 0 | CMF < 0 | Toggle `require_cmf` |
| HTF EMA trend (optional) | EMA50 > EMA200 | EMA50 < EMA200 | Toggle `require_htf_trend` |
| ADX min (optional) | ADX ≥ min_adx | ADX ≥ min_adx | `min_adx` (0 = disabled) |
| ADX max (optional) | ADX ≤ max_adx | ADX ≤ max_adx | `max_adx` (0 = disabled) |
| BB position (optional) | price ≤ lower band × (1 + proximity%) | price ≥ upper band × (1 − proximity%) | Toggle `require_bb_position` |
| BB bandwidth min (optional) | bandwidth > min_bb_bandwidth | bandwidth > min_bb_bandwidth | `min_bb_bandwidth` % (0 = disabled) |
| BB bandwidth max (optional) | bandwidth < max_bb_bandwidth | bandwidth < max_bb_bandwidth | `max_bb_bandwidth` % (0 = disabled) |

BB bandwidth is calculated as `(upper − lower) / middle × 100`.  
All data comes from the LTF snapshot (except EMA trend which uses `htf_indicators`).

### Mean-reversion scalping configuration

The Launcher is well suited to mean-reversion scalping on 15m candles when combined with the Skimming strategy (small TP, wider SL). The recommended settings below filter for ranging conditions where price is at a statistical extreme:

| Setting | Recommended value | Rationale |
|---|---|---|
| `rsi_oversold` | `25`–`28` | Only enter when RSI is at a genuine extreme, not mid-range noise |
| `rsi_overbought` | `72`–`75` | Symmetric with above |
| `require_htf_trend` | `false` | Ranging markets have no clean HTF trend; this filter blocks most signals |
| `require_cmf` | `false` | CMF is a trend-confirmation signal; mean-reversion entries oppose prevailing flow |
| `require_cmf_cross` | `false` | Zero-line cross is a trend-initiation signal — entry would be too late |
| `require_cmf_no_divergence` | `false` | CMF divergence at extremes is actually a reversion signal, not a risk |
| `require_footprint_delta` | `false` | 15-min net delta confirms trend direction, not reversion |
| `require_bb_position` | `true` | Core filter: only enter when price is at or near a BB extreme |
| `bb_proximity_pct` | `0.5` | Allows entry up to 0.5% inside the band to compensate for polling latency on 15m candles |
| `min_bb_bandwidth` | `2.0` | Blocks entries during bandwidth squeezes that often precede breakouts |
| `max_bb_bandwidth` | `0` (off) | Optionally cap at ~10–15% to avoid entering during extreme volatility events |
| `max_adx` | `20` | Only trade when ADX confirms a ranging market; raise to `25` if too few signals |
| `min_adx` | `0` (off) | Leave off for mean-reversion; a minimum trend strength is not required |

**Mean Reversion TP/SL settings** (STRATEGY page → Mean Reversion Scalping):

| Setting | Recommended value | Rationale |
|---|---|---|
| `tp_pct` | `2`–`3` | Take-profit as % price move; tightened further by Dynamic TP in low-bandwidth conditions |
| `sl_pct` | `5`–`15` | Stop-loss as % price move; sized to survive normal noise without hitting a full trend leg |
| `dynamic_tp` | `true` | Tightens TP in low-bandwidth conditions to exit before a squeeze breakout |
| `dynamic_tp_fraction` | `0.7` | Target 70% reversion toward the BB midline before exiting |

**Revised recommendation** — deeper entries for fewer stop-outs:

| Setting | Current | Recommended | Why |
|---|---|---|---|
| `rsi_oversold` | 25 | 20 | Enter deeper — less adverse excursion left |
| `rsi_overbought` | 75 | 80 | Symmetric |
| `bb_proximity_pct` | 0.5 | 0.0 | Must touch the band, not be near it |
| `min_bb_bandwidth` | 2.0 | 3.0 | Only enter when bands are wide enough to matter |
| `tp_pct` | 7 | 7 (keep) | Don't change |
| `sl_pct` | 11 | 15 | Small bump only — survives normal noise |

**Spike exhaustion filters** — prevent entering mid-spike when price keeps pumping after RSI > 80:

| Setting | Recommended value | Rationale |
|---|---|---|
| `require_candle_rejection` | `true` | Require upper wick for shorts / lower wick for longs — candle must show rejection, not a clean close at the extreme |
| `candle_rejection_pct` | `30` | Wick must be at least 30% of the candle's total range (shooting star / pin bar pattern) |
| `require_vwap_reversion` | `true` | Require price extended from VWAP AND closing back toward it — confirms reversion has started, not just that price is extended |
| `vwap_min_distance_pct` | `1.0` | Price must be at least 1% from VWAP to qualify as "extended" |
| `require_volume_cooling` | `true` | Block entry while volume RSI is still high — the spike is being driven by heavy volume and may continue |
| `volume_rsi_max` | `70` | Only enter when volume RSI drops below 70, signaling buying pressure is fading |

These three filters together prevent the pattern where RSI > 80 triggers a short, but price keeps pumping another 50–100% before reverting. Instead, the strategy waits for the spike to show exhaustion (rejection wick + volume cooling + closing back toward VWAP) before entering.

**Skimming settings** (STRATEGY page) — optional auto-close on PnL threshold:

| Setting | Recommended value | Rationale |
|---|---|---|
| `threshold_pct` | `7`–`10` (at 3× leverage) | TP ceiling as % of margin; at 3× leverage this equals ~2.3–3.3% price move |
| `stop_loss_pct` | `15`–`20` | SL as % of margin; fallback if algo TP/SL not attached |

**Signal frequency tuning** — if the Launcher fires too few signals, adjust in this order:

1. Raise `max_adx` from `20` → `25` → `30`
2. Lower `min_bb_bandwidth` from `2.0` → `1.5` → `1.0`
3. Relax RSI thresholds from `25/75` → `28/72`
4. Increase `bb_proximity_pct` from `0.5` → `1.0` as a last resort

All configurable from the STRATEGY page → Mean Reversion Scalping section.

### Spike Continuation (Momentum Scalping)

The Spike Continuation strategy rides momentum spikes for 3-5% gains before reversion. It complements Mean Reversion by catching the spike that MR fades.

**Entry conditions** (all must agree):

| Indicator | Buy | Sell | Configurable |
|---|---|---|
| Volume RSI (required) | > volume_rsi_min (default 80) | > volume_rsi_min (default 80) | Yes |
| Volume RSI max (optional) | < volume_rsi_max (default 95) | < volume_rsi_max (default 95) | Yes |
| BB position (optional) | price ≥ upper band × (1 − proximity%) | price ≤ lower band × (1 + proximity%) | Toggle `require_bb_position` |
| Candle shape (optional) | Close near high (≥ 70% of range) | Close near low (≤ 30% of range) | Toggle `require_candle_shape` |

**Exit conditions** (any triggers exit):
- Fixed TP at `tp_pct` (default 3-5%)
- Fixed SL at `sl_pct` (default 2-3%)
- Candle shows rejection (opposite wick ≥ 30% of range)
- Volume RSI drops below `volume_rsi_min`

**Spike Continuation recommended settings** (STRATEGY page → Spike Continuation):

| Setting | Recommended value | Rationale |
|---|---|---|
| `volume_rsi_min` | `80` | Only enter when volume momentum is strong — confirms spike is real |
| `volume_rsi_max` | `95` | Avoid entering when volume RSI is already exhausted — may be too late |
| `require_bb_position` | `true` | Core filter: only enter when price is at or beyond a BB extreme |
| `bb_proximity_pct` | `0.0` | Must be at or beyond the band — no tolerance for being "near" |
| `require_candle_shape` | `true` | Require candle to close in direction of spike — confirms momentum |
| `tp_pct` | `3`–`5` | Take-profit as % price move — ride the spike for 3-5% before reversion |
| `sl_pct` | `2`–`3` | Stop-loss as % price move — tight SL since we're entering late in the move |

**Momentum acceleration filters** — prevent entering at the TOP of a spike (critical):

| Setting | Recommended value | Rationale |
|---|---|---|
| `require_momentum_acceleration` | `true` | Current candle body must be larger than recent average — spike is still accelerating, not peaking |
| `acceleration_lookback` | `3` | Compare current body to average of last 3 candles |
| `acceleration_min_ratio` | `1.5` | Current body must be at least 1.5× the recent average (50% larger) |
| `require_rsi_rising` | `true` | RSI must be moving in the spike direction (bullish candle for buys, bearish for sells) — momentum still building, not fading |
| `require_volume_rsi_rising` | `true` | Volume RSI must be rising vs previous candle — volume momentum still building |
| `max_spike_extension_pct` | `3.0` | Block entry if price already moved more than 3% from spike origin — prevents entering at the top |
| `spike_lookback` | `5` | Candles to look back to find the spike origin (lowest low for buys, highest high for sells) |

These filters together prevent the pattern where the strategy enters a long at the top of a spike right before price reverts. Instead, it only enters when the spike is still accelerating with building volume momentum and hasn't extended too far from its origin.

**When to use Spike Continuation vs Mean Reversion:**
- Use **Spike Continuation** when you see a strong momentum spike with high volume RSI and want to ride it for 3-5%
- Use **Mean Reversion** when you see price at BB extremes with RSI oversold/overbought and want to fade the move
- Both can run concurrently — they target different market conditions

### Liquidity Sweep (Stop-Hunt Reversal)

The Liquidity Sweep strategy detects stop-hunt wicks: price pierces a recent swing low/high (triggering stops) then closes back inside the range. It enters in the opposite direction of the sweep, expecting a rapid reversal as the stop-run exhausts.

**Entry conditions** (all must agree):

| Indicator | Buy (long sweep) | Sell (short sweep) | Configurable |
|---|---|---|---|
| Sweep (required) | Wick below swing low × (1 − buffer%) | Wick above swing high × (1 + buffer%) | `sweep_buffer_pct` |
| Reclaim (required) | Close in upper `reclaim_ratio` of candle range | Close in lower `reclaim_ratio` of candle range | `reclaim_ratio` |
| HTF trend (optional) | EMA50 > EMA200 | EMA50 < EMA200 | Toggle `require_htf_trend` |
| Volume spike (optional) | Volume ≥ avg × `volume_spike_ratio` | Volume ≥ avg × `volume_spike_ratio` | Toggle `require_volume_spike` |
| ADX max (optional) | ADX ≤ `max_adx` | ADX ≤ `max_adx` | `max_adx` (0 = disabled) |
| Regime (optional) | BB bandwidth percentile ≤ `max_bb_bandwidth_percentile` | same | Toggle `require_regime` |

**Structural TP/SL sizing** (default on):

The strategy uses **structural levels** for TP/SL instead of pure ATR distances:

- **TP** targets the **opposite swing extreme** — for a long sweep (wick below swing low), TP is at the swing high; for a short sweep, TP is at the swing low. This is where price reversed from before, so it's a natural target.
- **SL** sits just **beyond the sweep wick** — for a long, SL is below the sweep candle's low plus a small ATR buffer (`structural_sl_buffer_atr`, default 0.15 ATR). This is the structural invalidation: if price goes below the sweep wick, the sweep failed.
- **ATR clamps both** to a sane range so you don't get absurdly tight or wide levels:
  - TP clamped to `[atr_min_tp_mult × ATR, atr_max_tp_mult × ATR]` from entry (default `[0.5, 4.0]`)
  - SL clamped to `[atr_min_sl_mult × ATR, atr_max_sl_mult × ATR]` from entry (default `[0.3, 3.0]`)

When structural levels are unavailable, the strategy falls back to ATR sizing (`atr_tp_multiplier × atr_pct` / `atr_sl_multiplier × atr_pct`).

| Setting | Default | Purpose |
|---|---|---|
| `use_structural_sizing` | `True` | Master switch for structural TP/SL |
| `structural_sl_buffer_atr` | `0.15` | SL placed this many ATR beyond the sweep wick |
| `atr_min_tp_mult` | `0.5` | Min TP distance in ATR units (clamp) |
| `atr_max_tp_mult` | `4.0` | Max TP distance in ATR units (clamp) |
| `atr_min_sl_mult` | `0.3` | Min SL distance in ATR units (clamp) |
| `atr_max_sl_mult` | `3.0` | Max SL distance in ATR units (clamp) |
| `use_atr_sizing` | `True` | ATR fallback when structural levels unavailable |
| `atr_tp_multiplier` | `1.5` | ATR fallback TP multiplier |
| `atr_sl_multiplier` | `1.2` | ATR fallback SL multiplier |

The debug log shows `[structural(tp=1.01→1.01%, sl=1.80→1.80%)]` in the signal rationale when structural sizing is active, so you can verify it's working.

### Trend Pullback

The Trend Pullback strategy enters on pullbacks to value (EMA21 or VWAP) in an established HTF trend, then prints a bullish/bearish candle off the level. It fills the gap between Spike Continuation (breakouts, too late) and Mean Reversion (extremes, wrong in a trend).

**Entry conditions** (all must agree):

| Indicator | Buy | Sell | Configurable |
|---|---|---|---|
| HTF trend (required) | EMA50 > EMA200 | EMA50 < EMA200 | Toggle `require_htf_trend` |
| Pullback level (required) | Price near EMA21 or VWAP | Price near EMA21 or VWAP | `pullback_proximity_pct`, `use_vwap_as_level` |
| Candle confirmation (optional) | Close > prev close + lower wick ≥ `candle_rejection_pct` | Close < prev close + upper wick ≥ `candle_rejection_pct` | Toggle `require_bullish_candle` |
| ADX min (optional) | ADX ≥ `min_adx` | ADX ≥ `min_adx` | `min_adx` (0 = disabled) |
| ADX max (optional) | ADX ≤ `max_adx_for_entry` | ADX ≤ `max_adx_for_entry` | `max_adx_for_entry` (0 = disabled) |

**Structural TP/SL sizing** (default on):

The strategy uses **structural levels** for TP/SL instead of pure ATR distances:

- **TP** targets the **nearest swing high** (for longs) or **swing low** (for shorts) from `indicators["structure"]["swing_highs"]` / `["swing_lows"]`. These are levels where price reversed before, so they're natural targets.
- **SL** sits just **beyond the pullback candle's low** (for longs) or **high** (for shorts) plus a small ATR buffer (`structural_sl_buffer_atr`, default 0.15 ATR). This is the structural invalidation: if price goes below the pullback candle, the pullback failed.
- **ATR clamps both** to a sane range (same config keys and defaults as Liquidity Sweep).

When structural levels are unavailable, the strategy falls back to ATR sizing.

### VWAP Reversion

The VWAP Reversion strategy enters when price is extended >N ATR from VWAP and the current candle closes back toward VWAP. VWAP is a strong magnet on alt-coins; intraday deviations mean-revert hard.

**Entry conditions** (all must agree):

| Indicator | Buy | Sell | Configurable |
|---|---|---|---|
| VWAP distance (required) | Price > `vwap_min_distance_atr` ATR below VWAP | Price > `vwap_min_distance_atr` ATR above VWAP | `vwap_min_distance_atr`, `vwap_max_distance_atr` |
| Closeback (optional) | Current candle closes up (toward VWAP) | Current candle closes down (toward VWAP) | Toggle `require_closeback` |
| ADX max (optional) | ADX ≤ `max_adx` | ADX ≤ `max_adx` | `max_adx` (0 = disabled) |
| HTF trend (optional) | EMA50 > EMA200 | EMA50 < EMA200 | Toggle `require_htf_trend` |
| Regime (optional) | BB bandwidth percentile ≤ `max_bb_bandwidth_percentile` | same | Toggle `require_regime` |

**Structural TP/SL sizing** (default on):

- **TP** targets **VWAP** — the magnet price is expected to revert to. This is the natural target for a reversion trade.
- **SL** sits just **beyond the extension candle's extreme** (low for longs, high for shorts) plus a small ATR buffer (`structural_sl_buffer_atr`, default 0.15 ATR). If price extends further, the reversion thesis is wrong.
- **ATR clamps both** to a sane range (same config keys and defaults as Liquidity Sweep / Trend Pullback).

When VWAP or candle data is unavailable, the strategy falls back to ATR sizing.

### Mean Reversion Structural TP/SL

Mean Reversion also supports structural TP/SL sizing (default on):

- **TP** targets the **BB middle band** (the 20-period SMA mean) — the natural reversion target for a mean-reversion trade.
- **SL** sits just **beyond the entry candle's wick** (low for longs, high for shorts) plus a small ATR buffer. If price extends beyond the exhaustion candle, the reversion thesis is wrong.
- **ATR clamps both** to a sane range (same config keys and defaults as the other strategies).

When BB or candle data is unavailable, the strategy falls back to ATR sizing. The `dynamic_tp` feature (BB bandwidth-scaled TP) is automatically disabled when `use_atr_sizing` is on, and structural sizing takes priority over both.

## Future Strategies

Here are some strategies that could be implemented using existing indicators, ranked by implementation effort:

| Priority | Strategy | Why | Effort |
|---|---|---|---|
| 1 | **Liquidity Sweep Reversal** | Highest win-rate signal, detection already built | Low (existing indicators) |
| 2 | **VWAP Fade** | Stronger magnet than BB, institutional level | Low (existing indicators) |
| 3 | **CVD Divergence** | Catches exhaustion before RSI does | Medium (needs series comparison) |
| 4 | **Stoch RSI Snapback** | Faster signals, more trades | Low (existing indicators) |

## Configuration Controls

Open the `CFG` page in the NiceGUI UI to tune the runtime behavior. Key controls and their impact:

- **Execution Guardrails** – Max leverage, max position pct, daily loss cap, cooldown/hold period, hourly trade limit, and alignment switch are enforced before any OKX order is attempted. They prevent the LLM from overtrading or flipping sides without closing a position.
- **ATR Risk Per Trade %** – Caps position size so that a full stop-out (measured as ATR × 1.5 from entry) loses at most this percentage of equity. Implements the 1% risk model: `max_notional = (equity × risk%) / (ATR_stop / price)`. Leave blank to disable.
- **CVD Guard** – Blocks BUY/SELL entries when Cumulative Volume Delta momentum conflicts with the trade direction. Configurable lookback window and minimum slope threshold; neutral CVD never blocks.
- **OB Wall Guard** – Blocks BUY/SELL entries when a dominant resting limit-order wall on the opposing side sits within a configurable % of current price. A level qualifies as a wall when its size exceeds N× the average level size across the full book depth.
- **Snapshot Max Age** – Blocks prompt generation and trading decisions if the cached market snapshot is older than the threshold, forcing fresh data before acting.
- **WS Update Interval** – Sets the background poller cadence (`POLL_INTERVAL`) in seconds. The poller runs continuously regardless of the scheduler and is the sole refresh source when the scheduler is off. It also runs fill reconciliation against the DB every other tick. When the scheduler is enabled, each tick forces its own full snapshot rebuild before evaluating signals, so indicators (RSI, ADX, BB, etc.) are at most one scheduler-interval old — not one poller-interval old. The poller then acts as a fallback safety net and keeps data fresh for the UI and strategy loops between ticks.
- **Live Websocket Stream** – Toggles the high-frequency OKX websocket listener. Disable it to rely solely on the poller (quieter logs, lower network use) while keeping periodic snapshots.
- **Auto Prompt Scheduler + Interval** – Enables periodic, automatic prompt execution for every enabled symbol. Interval must stay above 30 seconds to avoid rate limits.
- **Model Select + Response Schema** – Choose the OpenRouter model and optionally override the response schema JSON used to parse structured reasoning outputs.
- **Trading Pairs** – Defines which perpetual instruments the engine tracks. Changing this updates Redis snapshots, indicators, and scheduler coverage.
- **Live Execution Switch** – Master toggle for automated OKX order placement. Trade mode (cross/isolated) and default min order size live here as well.
- **Per-Symbol Min Sizes** – Optional overrides to enforce instrument-specific minimum contract sizes. The helper converts USDT budgets into contract sizes using the latest price snapshot.
- **Fee Window Hours** – Controls how many trailing hours of OKX fees are aggregated in the LIVE view.
- **OKX Sub-Account + Routing** – Lets you target a specific OKX sub-account and declare whether the API key belongs to the master account (so `subAcct` is appended to requests).
- **OKX Environment Flag** – Chooses between live trading (`0`) and the OKX demo environment (`1`). Clients are rebuilt automatically when the flag changes.
- **Prompt Versions** – Load prior prompt templates, clone them, or save new immutable versions for A/B testing. The preview pane updates in real time.

Open the `STRATEGY` page to configure autonomous position-management strategies that run independently of LLM decisions on a fast refresh loop:

- **Mean Reversion Scalping** – Launcher entry strategy: rule-based RSI mean-reversion entries with optional CMF, HTF trend, ADX, BB position, and footprint-delta filters. Has its own TP/SL and Dynamic TP (BB bandwidth) settings. Enable/disable independently; the Launcher must also be set to `launcher_only` or `llm_with_filter` mode on the CFG page.
- **Skimming** – Closes any position whose unrealised PnL ratio crosses a configurable threshold.
- **Protector** – Locks in a portion of profit by ratcheting the stop-loss upward as the position gains.
- **Commutator** – Reverses a losing position once after a configurable drawdown.
- **Alternator** – Oscillates between long and short on profit/loss thresholds. Supports dynamic thresholds (based on average candle amplitude), trailing-reverse mode, trailing-close, candle-position filter, footprint-delta filter, continuous LLM supervision, and **OB Wall Suppression** (blocks flips when a dominant opposing order-book wall is detected within proximity of the current price).
- **OB Wall Dynamic Stop-Loss** – Independent of Alternator; anchors stop-losses to the nearest dominant supporting limit-order wall (bid wall for LONGs, ask wall for SHORTs). The stop only ever moves in the profit direction; it is never loosened. Configurable proximity, wall-ratio threshold, minimum improvement gate, and buffer behind the wall.

When you click **Save**, the app persists the configuration (PostgreSQL for guardrails/prompt versions/execution settings, Redis for runtime snapshot state) and rehydrates all services in-place: MarketService gets new symbols, websocket or poll intervals, and OKX credentials; the scheduler updates its cadence; the LLM service swaps models; and the UI log buffers announce the change.

If you get zero signals for several days, relax in this order:
```
RSI 20 → 22 → 25
BB proximity 0.0 → 0.2 → 0.5
Min BB bandwidth 3.0 → 2.5 → 2.0
```

If SL still gets hit occasionally:
```
Don't widen SL further. Instead
Drop RSI to 18 (even deeper entries)
Or enable OB Wall Dynamic Stop-Loss on the STRATEGY page — it places the SL at the nearest order-book wall, which adapts to market structure rather than a fixed %
```

## Backtesting

The **BACKTEST** page (`/backtest`) lets you run historical backtests of your strategies using the current config values from the STRATEGY and CFG pages. Data is fetched from OKX and cached locally for instant re-runs.

### How it works

1. **Data fetching** — Historical OHLCV candles (LTF + HTF) are fetched from OKX using paginated REST calls (max 300 candles/request, walking backward from the end date). Fetched data is cached to `backtest_cache/` as JSON, keyed by `symbol_timeframe_start_end`, so re-running a backtest with different strategy parameters is instant.
2. **Indicator computation** — The engine reuses the same `MarketService._compute_indicators()` and `_compute_structure()` static methods used in live trading, ensuring indicators are calculated identically.
3. **Strategy evaluation** — The engine reuses the live `Strategy.evaluate()` protocol. Each selected strategy is evaluated against a synthetic snapshot built from the historical candle window, exactly matching the snapshot shape strategies expect.
4. **Simulated broker** — The simulator replicates OKX algo-order TP/SL behaviour: when a candle's high/low crosses the TP or SL price, the position closes at that price. If both TP and SL are hit within the same candle, SL is assumed to trigger first (pessimistic assumption).
5. **Metrics** — After the run, the engine computes: net profit, total return %, win rate, profit factor, max drawdown, Sharpe ratio per candle, average win/loss, expectancy, win/loss streaks, and a per-strategy breakdown.

### Configuration

On the BACKTEST page:
- **Symbol** — multi-select from current trading pairs
- **Timeframe** — 15m, 1H, 4H, or 1D (matches the live `ta_timeframe` options)
- **Initial Capital** — starting account equity in USDT
- **Lookback (days)** — backtest period = last N days from now (200 warmup candles are automatically added before the start for indicator stabilisation)
- **Strategies** — toggle which strategies to backtest (uses current config values from the STRATEGY page)

### Results

- **Summary metrics** — cards showing net profit, total trades, win rate, profit factor, max drawdown, final equity, Sharpe/candle, average win/loss, and expectancy
- **Per-strategy breakdown** — table with trades, win rate, net profit, and profit factor per strategy
- **Equity curve** — line chart of account equity over the backtest period
- **Trade table** — detailed list of all closed trades with entry/exit prices, close reason, PnL, and PnL %

### Limitations

- **CVD / Footprint / OFI** — These metrics derive from live WebSocket tick-level trade data and are not available in backtest. Strategies with `require_footprint_delta` or `require_cmf_no_divergence` filters should disable those filters for backtesting.
- **Position-management strategies** — The current phase simulates the OKX algo-order TP/SL close mechanism. Position-management strategies (Skimming, Protector, Commutator, Alternator) are not yet simulated but the simulator includes extension hooks (`on_entry` and per-candle `check`) for a future phase.
- **Order book** — Historical L2 order book data is not available, so OB Wall Guard and OB Wall Stops are not simulated.

### Architecture

```
app/services/backtest/
  __init__.py
  models.py          # Candle, SimPosition, EquityPoint, BacktestConfig, BacktestResult
  data_fetcher.py    # Paginated OKX historical OHLCV fetcher with file cache
  snapshot_builder.py# Builds synthetic snapshots from historical candles
  simulator.py       # Simulated broker (TP/SL close, equity tracking, PM hooks)
  metrics.py         # Performance metrics (Sharpe, max DD, win rate, etc.)
  engine.py          # Orchestrator: fetch → window-slide → evaluate → simulate → metrics
```

The engine runs entirely in Python memory — no Redis or PostgreSQL involvement. It does not interfere with live trading since it uses its own data fetcher and simulated broker, sharing only the read-only strategy config values.