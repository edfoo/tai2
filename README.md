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
