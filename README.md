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
- `WS_UPDATE_INTERVAL` (seconds, defaults to 180)

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
5. **Frontend** – NiceGUI pages for LIVE/TA/ENGINE/HISTORY/DEBUG/CFG.
6. **Integration** – FastAPI startup orchestration, global error surface, final docs/tests.

## Notes

- Use `uv run` to execute any Python commands inside the managed environment.
- Switching to Python 3.12+ is recommended because `pandas-ta` only supports 3.12 or newer.

## Prompt Scheduler

Enable "Auto Prompt Scheduler" on the CFG page to have the backend iterate every tracked symbol and send an LLM prompt automatically. You can configure the interval (minimum 30 seconds) there as well; the FastAPI lifespan wires those values into the new `PromptScheduler`, which reuses the same logic as the `/llm/prompt` and `/llm/execute` endpoints. Disable the toggle to keep prompts purely manual.

## Configuration Controls

Open the `CFG` page in the NiceGUI UI to tune the runtime behavior. Key controls and their impact:

- **Execution Guardrails** – Max leverage, max position pct, daily loss cap, cooldown/hold period, hourly trade limit, and alignment switch are enforced before any OKX order is attempted. They prevent the LLM from overtrading or flipping sides without closing a position.
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

When you click **Save**, the app persists the configuration (PostgreSQL for guardrails/prompt versions/execution settings, Redis for runtime snapshot state) and rehydrates all services in-place: MarketService gets new symbols, websocket or poll intervals, and OKX credentials; the scheduler updates its cadence; the LLM service swaps models; and the UI log buffers announce the change.
