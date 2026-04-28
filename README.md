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
  services/        # Redis state + upcoming OKX/LLM logic
  ui/              # NiceGUI components and pages
  main.py          # FastAPI entry point

tests/             # pytest suites
```

## Roadmap (Phases)

1. **Initialization** – skeleton app, config, NiceGUI landing page, smoke tests.
2. **Data Layer** – TimescaleDB schema, asyncpg pool, Redis `StateService`.
3. **Market Engine** – OKX REST/WebSocket, indicators, Redis snapshots.
4. **Reasoning Engine** – OpenRouter-based LLM decisions and trade execution.
5. **Frontend** – NiceGUI pages for LIVE/TA/STRATEGY/HISTORY/DEBUG/PROMPT/CFG.
6. **Integration** – FastAPI startup orchestration, global error surface, final docs/tests.

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
- **WS Update Interval** – Sets the REST/poller cadence in seconds. Lower values refresh Redis snapshots more often but increase OKX REST usage.
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

- **Skimming** – Closes any position whose unrealised PnL ratio crosses a configurable threshold.
- **Protector** – Locks in a portion of profit by ratcheting the stop-loss upward as the position gains.
- **Commutator** – Reverses a losing position once after a configurable drawdown.
- **Alternator** – Oscillates between long and short on profit/loss thresholds. Supports dynamic thresholds (based on average candle amplitude), trailing-reverse mode, trailing-close, candle-position filter, footprint-delta filter, continuous LLM supervision, and **OB Wall Suppression** (blocks flips when a dominant opposing order-book wall is detected within proximity of the current price).
- **OB Wall Dynamic Stop-Loss** – Independent of Alternator; anchors stop-losses to the nearest dominant supporting limit-order wall (bid wall for LONGs, ask wall for SHORTs). The stop only ever moves in the profit direction; it is never loosened. Configurable proximity, wall-ratio threshold, minimum improvement gate, and buffer behind the wall.

When you click **Save**, the app persists the configuration (PostgreSQL for guardrails/prompt versions/execution settings, Redis for runtime snapshot state) and rehydrates all services in-place: MarketService gets new symbols, websocket or poll intervals, and OKX credentials; the scheduler updates its cadence; the LLM service swaps models; and the UI log buffers announce the change.


## Launcher decision
The Launcher uses these indicators, all of which must agree simultaneously:

|Indicator|Buy|Sell|Configurable|
|-|-|-|-|
|RSI (required)|< RSI Oversold (default 35)|> RSI Overbought (default 65)|	Yes|
|CMF (optional)|CMF > 0|CMF < 0|Toggle require_cmf|
|HTF EMA trend (optional)|EMA50 > EMA200|EMA50 < EMA200|Toggle require_htf_trend|
|ADX (optional)|ADX ≥ min_adx|ADX ≥ min_adx|min_adx (0 = disabled)|

All data comes from the LTF snapshot (except EMA trend which uses indicators_htf).

All signals firing "no entry" means RSI is neither oversold nor overbought on any of those symbols — i.e. the market is in a neutral RSI range. The HTF EMA trend and CMF filters compound this. If you're seeing it consistently fire nothing, you could:

- Loosen the RSI thresholds (e.g. 40/60 instead of 35/65)
- Disable require_htf_trend and/or require_cmf
- Lower min_adx or set it to 0
- All configurable from the CFG page → Launcher section.