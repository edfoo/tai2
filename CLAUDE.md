# CLAUDE.md — tai2 Project Reference

## What this project is
**tai2** is a cryptocurrency trading automation bot for OKX USDT perpetual swaps that:
- Pulls live market data from **OKX** (REST + WebSocket) and builds a per-symbol market snapshot (indicators via `pandas-ta`)
- Selects symbols with a **screener** (dual SC/MR universes)
- Generates entries with the **Launcher**: deterministic, rule-based strategies (the actively developed path)
- Executes on OKX through a shared guardrail + sizing + TP/SL layer, then manages open positions (breakeven, partial TP, trailing, time stop)
- Ships a **backtest engine** that reuses the live strategy/TP-SL/screener code for parity
- Serves a **NiceGUI + FastAPI** web UI for monitoring and configuration

It started as an LLM (OpenRouter) decision bot. The **LLM path is dormant but must stay working** — don't remove it. Much of the legacy docs (`Instructions.md`, parts of `README.md`, `GUARDRAILS.md`, older `ChangeLog.md` entries) describe the LLM era.

---

## Stack
| Layer | Tech |
|---|---|
| Web framework | FastAPI + NiceGUI (served together via `app/main.py`) |
| Exchange | OKX via `python-okx` + `okx-sdk` (adapter in `app/services/okx_sdk_adapter.py`) |
| LLM (dormant) | OpenRouter API (`app/services/llm_service.py`, `app/services/openrouter_service.py`) |
| State cache | Redis (`app/services/state_service.py`) |
| Persistence | PostgreSQL/TimescaleDB via asyncpg (`app/db/postgres.py`) — optional, feature-degraded without it |
| Indicators | `pandas-ta` inside `MarketService` |
| Alerts | Telegram (`app/services/alert_service.py`) — optional |
| Config | `pydantic-settings` from `.env` (`app/core/config.py`) + runtime config persisted in Postgres |
| Package manager | `uv` / `pyproject.toml` |
| Python | ≥ 3.12 |

`docker-compose.yml` runs TimescaleDB (pg14) and Redis 7 locally.

---

## Key environment variables (`.env`)
```
OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE
OKX_API_FLAG                 # "0" = live, "1" = paper/demo
OKX_SUB_ACCOUNT              # optional
OKX_SUB_ACCOUNT_USE_MASTER   # optional bool (default false)
OPENROUTER_API_KEY           # only needed for the LLM path
ALLOW_FALLBACK_ORDERS        # default true
DATABASE_URL                 # optional postgres URL
REDIS_URL                    # optional redis URL
TRADING_PAIRS                # comma-separated bootstrap list, e.g. BTC-USDT-SWAP,ETH-USDT-SWAP
POLL_INTERVAL                # seconds between market data polls (default 180)
SNAPSHOT_MAX_AGE_SECONDS     # max staleness before LLM calls are blocked (default 900)
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   # optional trade alerts
BACKTEST_WORKERS             # optional; backtest process-pool size (default os.cpu_count())
```
`.env` holds credentials and bootstrap values only. Most behaviour lives in `app.state.runtime_config` (built in `main.py` lifespan, overlaid with values saved in Postgres, edited from the CFG/STRATEGY pages).

---

## Running
```bash
uv sync
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```
- Bind `0.0.0.0` (not a single IP) so `localhost` works — the backtest CLI scripts default to `http://localhost:8000`.
- Do **not** use `--reload` outside local dev: each restart wipes in-memory launcher/trade-management state (`_launcher_in_position`, `_trade_mgmt_state`), which is only partly re-hydrated by `_rehydrate_open_positions`.
- Background services are disabled automatically under pytest (`PYTEST_CURRENT_TEST`).

## Running tests
```bash
uv run pytest
# or directly via the venv
.venv/bin/python -m pytest tests/ -q
```
- `pytest-xdist` runs with `-n auto` by default (see `pyproject.toml`); the suite takes ~15s.
- Tests marked `network` hit live services and are skipped by default.
- All tests must pass (802 passing, 0 pre-existing failures as of 2026-11-17). Any failure is a regression.

---

## Project structure
```
app/
  main.py                  # FastAPI app factory, lifespan, runtime_config bootstrap, log setup, API routes
  core/config.py           # Settings (pydantic-settings, @lru_cache get_settings)
  db/postgres.py           # asyncpg pool, trade/settings/prompt-version persistence
  models/trade.py          # Pydantic trade model
  lib/                     # volatility.py, volume_profile.py (pure helpers)
  services/
    market_service.py      # Core engine (~13.7k lines): data fetch, snapshot, screener runner,
                           #   launcher, handle_llm_decision (guardrails + sizing + orders),
                           #   trade management, fill reconciliation, legacy position strategies
    prompt_scheduler.py    # Scheduler tick: screener → snapshot → decisions → sort → execute
    launcher_tp_sl.py      # resolve_launcher_tp_sl — shared live + backtest TP/SL resolution
    trade_management_rules.py  # Pure breakeven/trailing/partial-TP rules (shared with simulator)
    trade_management.py    # TP/SL calculation helpers (compute_tp_sl_pct, R:R checks)
    screener.py            # Shared symbol-screener scoring core (live + backtest)
    strategies/            # Pluggable Launcher strategies
      __init__.py          # Strategy protocol, StrategySignal, StrategyHelpers, shared helpers
      defaults.py          # Canonical defaults: strategies, trade management, screener
      mean_reversion.py, spike_continuation.py, trend_pullback.py,
      liquidity_sweep.py, vwap_reversion.py, liquidity_helpers.py
    indicator_service.py, okx_metrics.py, alert_service.py
    state_service.py       # Redis snapshot store + pub/sub
    okx_sdk_adapter.py     # Wraps OKX SDK for sub-account routing
    llm_service.py         # (LLM) payload prep, compact_context trimming, dispatch
    prompt_builder.py      # (LLM) JSON context + prompt; also _default_guardrails()
    prompt_runner.py       # (LLM) staleness gate, guards, payload assembly, execution
    openrouter_service.py, prompt_utils.py
    backtest/              # Backtest engine + REST API
      engine.py            # Orchestrator: fetch → snapshot → evaluate → simulate → metrics
      snapshot_builder.py  # Builds live-shaped snapshots from historical candles
      indicator_cache.py   # Caches indicator computation across steps
      simulator.py         # Simulated broker (TP/SL, trade mgmt, isolated/cross liquidation)
      costs.py, spread.py  # Fees / slippage / funding; Corwin-Schultz & Roll spread estimators
      sizing.py            # Deterministic sizing (ct_val/lot_size/max_position_pct)
      metrics.py           # Sharpe / max DD / win rate / MAE-MFE / R-multiples
      data_fetcher.py      # Cached historical candles / trade tape
      universe.py          # Historical screener-universe reconstruction (live parity)
      runner.py            # Single build_backtest_config constructor (CLI + UI + API)
      grid.py              # BacktestGrid parameter sweep (parallel per combination)
      sweep_analysis.py, sweep_report.py, sweep_catalog.py, summaries.py,
      recommendations.py, research.py   # Sweep ranking, reports, attribution, research bundles
      persistence.py       # JSON/CSV result serialisation (result_to_dict/from_dict)
      models.py, progress.py
      api.py, api_models.py  # FastAPI router: /backtest/run|grid|status|result
      job_manager.py       # Job queue + ProcessPoolExecutor (app.state.backtest_jobs)
      client.py            # Shared REST client helpers for CLI scripts
  ui/
    pages.py               # All NiceGUI page renderers (~11.2k lines)
    components.py
scripts/                   # Backtest/sweep CLIs, diagnostics (list_active_symbols.py, performance_summary.py, …)
tests/                     # ~35 pytest modules (backtest, screener, strategies, parity, okx, db, …)
docs/objectives/           # Design/plan docs for features (status line at the top of each)
.github/skills/            # Domain playbooks: launcher-strategy-reference,
                           #   strategy-tuning-playbook, log-diagnostics-playbook
backtest_cache/            # Cached historical data (runtime)
logs/app.log               # Rotating log file (5 MB × 5 backups), created at runtime
```

---

## Decision paths
`runtime_config["launcher"]["mode"]` selects the path:

| Mode | Behaviour |
|---|---|
| `launcher_only` | **Active path.** Rule-based strategies, no LLM calls |
| `llm_with_filter` | LLM decides; the decision is vetoed unless a launcher strategy agrees on direction |
| `disabled` (default) | Pure LLM path |

### Launcher flow (`launcher_only`)
1. `PromptScheduler._tick()` runs `run_screener_if_due()`, refreshes the snapshot, then calls `_collect_launcher_decisions()`.
2. `MarketService.build_launcher_decisions(symbol)` → `_launcher_evaluate_signals()`:
   - Rebuilds the snapshot on **closed candles only** (`_build_closed_candle_snapshot`), including per-strategy `timeframes[<tf>]` blocks, so live sees the same indicator values as the backtest.
   - Runs every enabled strategy whose screener universe contains the symbol (`_strategy_allowed_on_symbol`).
3. Per signal: skip if the `strategy:symbol` key is in-flight (`_launcher_entering`), already tracked (`_launcher_in_position`), or in re-entry cooldown; then `resolve_launcher_tp_sl()` produces action/TP/SL (static, ATR, structural, dynamic TP, direction flip).
4. The decision dict (`notional_usd` from `launcher.notional_usd`, `_decision_origin: "launcher"`, `_strategy_name`) is passed straight to `handle_llm_decision()`, so **all guardrails, sizing and order/TP-SL placement are shared with the LLM path**.
5. After entry: `_check_trade_management` (software stop, breakeven, partial TP, trailing, time stop, cooldown), `_check_strategy_regime_exits` (MR), and `_check_spike_continuation_rollover_exits` (SC) manage the position.

With `launcher.schedule="on_close"`, the scheduler waits for all positions to close before the next tick.

### LLM flow (dormant)
`prompt_builder` → `llm_service` / OpenRouter → `prompt_runner.apply_llm_decision` → `handle_llm_decision()`.

### Scheduler phases
1. **Collect decisions**: launcher signals, or LLM queried concurrently for all symbols
2. **Sort**: BUY/SELL by confidence desc, risk_score asc; HOLD last
3. **Execute**: BUY/SELL sequentially, so each trade sees the remaining balance
4. **HOLD**: recorded concurrently (LLM path only; the launcher has no HOLDs)

---

## Launcher strategies
| Strategy | Universe | Idea |
|---|---|---|
| `mean_reversion` | MR (chop) | Fade overextension at BB/RSI extremes in ranging HTF |
| `spike_continuation` | SC (trending) | Follow volume/momentum breakouts |
| `trend_pullback` | SC (trending) | Enter pullbacks in established trends |
| `liquidity_sweep` | MR (chop) | Fade stop-hunt sweeps of fractal swing levels |
| `vwap_reversion` | MR (chop) | Revert intraday deviations back to VWAP |

- Config lives under `runtime_config["launcher"]["strategies"][<name>]`; defaults in `strategies/defaults.py` (`strategy_defaults()` / `merged_config()`).
- Each strategy may set `analysis_timeframe`; the snapshot then exposes `sym_data["timeframes"][<tf>]` (read via `resolve_analysis_block`).
- Multiple strategies can fire on one symbol; each holds its own position keyed `strategy:SYMBOL`.

### Adding a strategy — every place that must change
1. New module in `app/services/strategies/` implementing the `Strategy` protocol; defaults in `defaults.py`.
2. Register in **both** `MarketService._strategies` (`market_service.py`) and `_AVAILABLE_STRATEGIES` (`backtest/engine.py`).
3. Universe routing in **both** `MarketService._strategy_allowed_on_symbol` and `screener.universe_for_strategy`.
4. A card on the STRATEGY page (`pages.py`) with enable switch, config fields, and "Set Recommended Defaults".
5. Tests in `tests/test_strategies.py` and the parity tests.

---

## Live ↔ backtest parity
Parity is a core design rule: logic is written **once** and shared, never mirrored by hand.
- Strategy evaluation: the same strategy classes, evaluated on closed candles in both.
- TP/SL resolution: `launcher_tp_sl.resolve_launcher_tp_sl`.
- Trade management: `trade_management_rules.py`.
- Screener: `screener.py` (see below).
- Guardrails: the backtest receives `guardrails_config` separately from `launcher_config` / `strategy_config`, matching the live `runtime_config` split.

Covered by `tests/test_live_backtest_parity.py` — extend it when touching any of the above.

---

## Critical invariants — never break these

### OKX contract sizing
- OKX `sz` field is in **contracts**, not base tokens
- `ct_val` = how many base-token units = 1 contract (from OKX instruments API, stored in `_instrument_specs[symbol]["ct_val"]`, default 1.0)
- Always: `okx_sz = raw_size_in_base_tokens / ct_val`
- Always: `reference_price = ct_val * last_price` (notional per contract)
- Bootstrap `minSz` floor: `_t1_min_sz * ct_val * last_price`

### Decision sizing interface (launcher + LLM)
- Decisions carry **`notional_usd`** (dollar value, e.g. `150.0`) — never contracts or base tokens. The launcher takes it from `launcher.notional_usd`.
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
- `require_reward_risk_ratio` gates on TP/SL R:R (launcher decisions may set `_skip_rr_guard`)
- `require_protection` blocks if TP/SL algo placement fails
- `max_position_pct` and per-symbol caps enforced before sizing
- `daily_loss_limit_pct` locks execution when threshold breached
- One launcher position per symbol: `_launcher_symbol_conflict` blocks a launcher entry when the symbol has an open position or another launcher strategy tracks it (build time + execution time). Without it, an opposite-direction launcher signal becomes a `reduce_only` close of the other strategy's position. Matches backtest `allow_concurrent_strategies_per_symbol=False`.

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
- Launcher activity is logged with the `Launcher:` / `Launcher signal:` prefixes via `_emit_debug`

## Backtest REST API
- `POST /backtest/run` / `POST /backtest/grid` → `202 {job_id}` (in-process FIFO job queue)
- `GET /backtest/status/{job_id}` → queued/running/completed/failed
- `GET /backtest/result/{job_id}` → full `result_to_dict` payload (409 until completed)
- Job registry lives on `app.state.backtest_jobs` (`BacktestJobManager`), started in lifespan, cancelled on shutdown
- Jobs execute **concurrently** across a `ProcessPoolExecutor` (true multi-core
  parallelism — thread-level measured no speedup because pandas-ta + pure-Python
  strategy eval hold the GIL).  Default pool size = `os.cpu_count()`, overridable
  via `BACKTEST_WORKERS` env var.  Only the picklable `BacktestConfig`/`GridConfig`
  crosses the process boundary; the engine/strategies are constructed in-worker.
- `BacktestGrid` additionally parallelises its own Cartesian-product combinations
  in-subprocess (each parameter combination = one process), so `POST /backtest/grid`
  and the UI Parameter Sweep use all cores — not just the job-level pool.
- Request body mirrors `BacktestConfig`; guardrails passed via `guardrails_config` (separate from `launcher_config`/`strategy_config`, matching live `runtime_config` split)
- CLI scripts (`backtest_client.py`, `backtest_research.py`, `run_gate_ab_sweep.py`, `run_trend_pullback_ab.py`, `run_vwap_ab_sweep.py`) are thin REST clients using `app/services/backtest/client.py` and need the app running. **Exceptions:** `run_strategy_geometry_sweep.py` and `run_vwap_geometry_sweep.py` also construct `BacktestEngine` directly.
- See `BACKTEST.md` for the full config reference, cost models and research workflow.

## Screener universe parity (backtest ↔ live)
- Screener scoring lives in `app/services/screener.py` (`score_universe`, `universe_for_strategy`) — **one** implementation shared by live `MarketService` and the backtest. Never re-implement scoring in the backtest.
- `universe_mode="screener"` reconstructs per-interval tickers from 1H candles (`app/services/backtest/universe.py`) and gates each strategy per step via `BacktestEngine._symbol_in_universe` (SC strategies → SC list, MR strategies → MR list). `symbols` becomes the pre-first-interval fallback.
- **Recent-activity window:** 24h ticker fields are aggregates, so a coin that pumped hours ago and has since gone quiet still ranks highly on them. When `screener.activity_lookback_hours > 0` (default 4), SC/MR gates and scores use `compute_recent_activity()` features (`recent_vol_ratio`, `recent_range_pct`, `recent_mom_pct`, `decay_ratio`) derived from the last N hours of candles instead. Live fetches those candles for the volume-ranked top `activity_prefilter_top_n` candidates (cached `activity_cache_seconds`); the backtest derives them from the 1H candles it already fetched, so parity costs nothing. `activity_lookback_hours=0` restores pure 24h ranking.
- `Candle.quote_volume` (OKX `volCcyQuote`) is required for the volume filter; legacy caches fall back to `volume × close`.
- **Volume units:** OKX's raw ticker `volCcy24h` is **base-currency** volume, not USDT. `build_candidates()` converts it via `volCcy24h × last` unless `vol_ccy_is_quote=True`. Live passes `False`; the backtest passes `True` because `_ticker_at()` already sums `volCcyQuote`. Never compare raw `volCcy24h` against `min_volume_usd` — it silently excludes high-priced symbols and admits low-priced ones on token counts.
- The live `max_spread_pct` filter is **skipped** historically (no bid/ask in OHLCV) and recorded in the schedule provenance.
- **Screener defaults live in `app/services/strategies/defaults.py`** (`DEFAULT_SCREENER` / `screener_defaults()`), not inline in `main.py`. `main.py` bootstraps `runtime_config["screener"]` from it and the CFG page merges saved config over it — change defaults there, never in the UI or lifespan.
- **Fast lane (`fast_lane_enabled`, default off):** `build_candidates()` gates on trailing-24h volume, which is a *lagging* indicator — a quiet-baseline coin cannot reach `min_volume_usd` until the pump's candles have printed (SHELL +72% in 4h, gate crossed 4h late). `fast_lane_candidates()` (screener.py) admits symbols on a short-window price move with a much lower `fast_lane_min_volume_usd` floor. It is a **discovery widening only** — admitted symbols still go through `compute_recent_activity` and SC/MR scoring, so they must win to be selected. It costs **no extra REST calls**: the move comes from `_record_screener_price_samples()` on the all-SWAP ticker poll the screener already does. `_fetch_recent_activity` exempts `_fast_lane` symbols from the `activity_prefilter_top_n` volume-ranked cap (bounding them by `fast_lane_max_symbols` instead), otherwise the lane could never fetch the low-volume symbols it exists to find. Backtest parity via `_price_history_at()` in `universe.py`; the `sodUtc0` cold-start fallback is live-only and recorded in provenance.

---

## UI pages
| Path | Name | Purpose |
|---|---|---|
| `/`, `/live` | LIVE | Real-time positions, balances, ticker, decision/insight feed, risk-lock card |
| `/ta` | TA | Indicators, funding, CVD, order book |
| `/strategy` | STRATEGY | Launcher strategy cards (enable + params), trade management, legacy position strategies |
| `/history` | HISTORY | Executed trades |
| `/performance` | PERFORMANCE | Performance summary |
| `/backtest` | BACKTEST | Single runs and parameter sweeps, results/reports |
| `/debug` | DEBUG | Application logs (live, filterable), WebSocket events |
| `/prompt` | PROMPT | LLM prompt editing/preview (LLM path) |
| `/cfg` | CFG | Runtime config: launcher mode, screener, guardrails, execution, trading pairs, model |

---

## Legacy position strategies
`runtime_config["strategy"]` (separate from the launcher) holds the older position-oscillation and supervision features in `MarketService`: Skimming, Shotgun, Protector, Commutator, and Alternator (`_check_*` methods, run from the positions refresh loop). They're still wired up but not the focus of development.

---

## Things to watch out for
- `market_service.py` (~13.7k lines) and `pages.py` (~11.2k lines) are very large; `handle_llm_decision` alone is ~2.5k lines. Always read surrounding context before editing, and locate code with `grep -n` first.
- Despite its name, `handle_llm_decision` is the shared execution entry point for **launcher** decisions too.
- NiceGUI UI mutations must happen in the NiceGUI slot context — avoid creating UI elements inside background tasks
- `get_settings()` is `@lru_cache`; changes to env vars after import have no effect
- Postgres and Redis are both optional; all DB calls must degrade gracefully
- The `_flipped` flag on decisions means the guardrail layer reversed the action; track `_effective_action` not `action` for display
- Feature work is usually specified in `docs/objectives/*.md`; check the status line before assuming a plan is pending or done.
- Record notable changes in `ChangeLog.md` (newest first).
