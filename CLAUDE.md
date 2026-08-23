# CLAUDE.md — tai2 Project Reference

## What this project is
**tai2** is a cryptocurrency trading automation bot that:
- Pulls live market data from **OKX** (REST + WebSocket)
- Builds a structured market snapshot and feeds it to an **LLM via OpenRouter**
- Executes trades on OKX based on LLM decisions, subject to a guardrail layer
- Serves a **NiceGUI + FastAPI** web UI for monitoring and configuration

---

## Stack
| Layer | Tech |
|---|---|
| Web framework | FastAPI + NiceGUI (served together via `app/main.py`) |
| Exchange | OKX via `python-okx` + `okx-sdk` (adapter in `app/services/okx_sdk_adapter.py`) |
| LLM | OpenRouter API (`app/services/llm_service.py`, `app/services/openrouter_service.py`) |
| State cache | Redis (`app/services/state_service.py`) |
| Persistence | PostgreSQL/asyncpg (`app/db/postgres.py`) — optional, feature-degraded without it |
| Indicators | `pandas-ta` inside `MarketService` |
| Config | `pydantic-settings` from `.env` (`app/core/config.py`) |
| Package manager | `uv` / `pyproject.toml` |
| Python | ≥ 3.12 |

---

## Key environment variables (`.env`)
```
OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
OKX_API_FLAG          # "0" = live, "1" = paper/demo
OKX_SUB_ACCOUNT       # optional
OPENROUTER_API_KEY
DATABASE_URL          # optional postgres URL
REDIS_URL             # optional redis URL
TRADING_PAIRS         # comma-separated, e.g. BTC-USDT-SWAP,ETH-USDT-SWAP
POLL_INTERVAL         # seconds between market data polls (default 180)
SNAPSHOT_MAX_AGE_SECONDS  # max staleness before LLM calls are blocked (default 900)
```

---

## Running tests
```bash
poetry run pytest tests/ -q
# or directly via the venv
.venv/bin/python -m pytest tests/ -q
```
All tests must pass. Failures beyond the 0 pre-existing ones are regressions.

---

## Project structure
```
app/
  main.py               # FastAPI app factory, lifespan, log setup, API routes
  core/config.py        # Settings (pydantic-settings)
  db/postgres.py        # asyncpg pool, trade/settings persistence
  models/trade.py       # Pydantic trade model
  services/
    market_service.py   # Core engine: data fetch, guardrails, order execution
    llm_service.py      # LLM payload prep, compact_context trimming, dispatch
    prompt_builder.py   # Builds the JSON context + prompt sent to the LLM
    prompt_runner.py    # Staleness gate, guards, payload assembly, execution
    prompt_scheduler.py # Auto-prompt timer: 4-phase (LLM → sort → execute → HOLD)
    state_service.py    # Redis snapshot store + pub/sub
    okx_sdk_adapter.py  # Wraps OKX SDK for sub-account routing
    openrouter_service.py
    prompt_utils.py
  ui/
    pages.py            # All NiceGUI page renderers (LIVE, TA, STRATEGY, HISTORY, DEBUG, PROMPT, CFG)
    components.py
tests/
  test_app.py, test_okx.py, test_okx_adapter.py, test_db.py, test_prompt_runner.py
logs/
  app.log               # Rotating log file (5 MB × 5 backups), created at runtime
```

---

## Critical invariants — never break these

### OKX contract sizing
- OKX `sz` field is in **contracts**, not base tokens
- `ct_val` = how many base-token units = 1 contract (from OKX instruments API, stored in `_instrument_specs[symbol]["ct_val"]`, default 1.0)
- Always: `okx_sz = raw_size_in_base_tokens / ct_val`
- Always: `reference_price = ct_val * last_price` (notional per contract)
- Bootstrap `minSz` floor: `_t1_min_sz * ct_val * last_price`

### LLM sizing interface
- LLM outputs **`notional_usd`** (dollar value, e.g. `150.0`) — never contracts or base tokens
- Bot converts: `raw_size = notional_usd / last_price`, then `okx_sz = raw_size / ct_val`
- Legacy fallback fields: `position_size` (base tokens) → `equity_pct` → leverage-scaled

### Snapshot staleness
- Enforced **by the bot** (`prompt_runner._snapshot_is_stale`), never delegated to the LLM
- Stale → 503 returned before prompt builder runs; LLM never called
- `snapshot_health` is NOT included in the LLM context payload
- UI shows stale banner independently via `update_snapshot_health()` on a 5s timer

### Prompt / LLM context
- `compact_context` (reasoning models): trims LTF candles to 20, caps volume_series at 60, trims macd/adx/obv/cmf series to 20 — all scalars preserved
- `indicators.htf` and `history.candles_htf` are always fully preserved
- OBV/CMF series kept (trimmed to 20) for divergence detection (prompt Step 3)

### Guardrails
- Stop-loss is always required for entry orders
- `require_reward_risk_ratio` gates on TP/SL R:R
- `require_protection` blocks if TP/SL algo placement fails
- `max_position_pct` and per-symbol caps enforced before sizing
- `daily_loss_limit_pct` locks execution when threshold breached

### Prompt versioning
- `insert_prompt_version` only called when user explicitly types a name in "Save As New Version"
- Loading a version from the dropdown clears the name input (prevents auto-duplicate on save)

### Isolated margin / bootstrap
- `OKX_ISOLATED_BOOT_MIN_NOTIONAL_USD = 5.0` USDT floor
- Bootstrap orders: max 1 retry; always blocklists on 51008 error regardless of retry count
- `_pending_notional[symbol]` tracks committed capital across concurrent orders

---

## Logging
- All log lines → `app.state.log_lines` deque (5000 entries, always current)
- All log lines → `logs/app.log` (RotatingFileHandler, 5 MB × 5 backups)
- Debug page reads from `log_lines` deque; preloaded on every page render
- `GET /api/logs?lines=500&filter=text` — JSON endpoint for log tail

---

## UI pages
| Path | Name | Purpose |
|---|---|---|
| `/` | LIVE | Real-time positions, balances, ticker, LLM Insight Feed |
| `/ta` | TA | Indicators, funding, CVD, order book |
| `/history` | HISTORY | Executed trades |
| `/debug` | DEBUG | Application logs (live, filterable), WebSocket events |
| `/cfg` | CFG | All runtime config: prompts, guardrails, trading pairs, model |

---

## Scheduler flow (4-phase)
1. **Refresh snapshot** — single pass covering all symbols
2. **LLM phase** — all symbols queried concurrently (pure I/O)
3. **Sort** — BUY/SELL by confidence desc, risk_score asc; HOLD last
4. **Execute** — BUY/SELL sequentially (so each trade sees remaining balance); HOLD processed after

---

## Things to watch out for
- `pages.py` is large (~4600 lines); always read surrounding context before editing
- NiceGUI UI mutations must happen in the NiceGUI slot context — avoid creating UI elements inside background tasks
- `get_settings()` is `@lru_cache`; changes to env vars after import have no effect
- Postgres and Redis are both optional; all DB calls must degrade gracefully
- The `_flipped` flag on LLM decisions means the guardrail layer reversed the action; track `_effective_action` not `action` for display

## Testing
- Tests can be run with `cd /home/eduard/projects/tai2 && poetry run pytest`
