# Backtesting (headless REST client + UI persistence)

This document explains how to run **deterministic, headless** backtests and
parse the results, outside of the NiceGUI UI. The UI is fine for a quick look,
but its results live on `app.state` and are **lost on refresh**. The REST API
and its thin clients below persist everything to disk so results survive and
can be diffed across runs.

> **Note on the UI**: the BACKTEST page now **persists every completed result
> to disk automatically** and has a **Saved Runs** browser, so results also
> survive app restarts (not just page refreshes). See
> [UI persistence](#ui-persistence-saved-runs) below.

---

## Overview

| Interest | You want |
|---|---|
| Run backtests / compare timeframes (REST client) | [`scripts/backtest_client.py`](#1-run-backtests) |
| Parse / compare results (CLI) | [`scripts/parse_backtest_results.py`](#2-parse-results) |
| Run / browse / load results (UI) | BACKTEST page → Saved Runs |

The headless path is now a **thin REST client** that submits jobs to the
running tai2 server (`POST /backtest/run` / `POST /backtest/grid`) and polls
for the result — the same single interface the UI uses. The server persists
results via `app/services/backtest/persistence.py`, so a run produced by
either path is viewable by the other.

**Parallelism**: the server executes jobs concurrently across a
`ProcessPoolExecutor` (default `os.cpu_count()`; override with the
`BACKTEST_WORKERS` env var), and the clients submit multiple variants at once
(``--workers`` flag, default 8). A/B sweeps therefore run their variants in
parallel rather than one-after-another. Process-based parallelism is required
here — threads give no speedup because the indicator computation and strategy
evaluation hold the GIL.

---

## 0. UI persistence (Saved Runs)

The BACKTEST page persists every **completed, trade-producing** backtest to
disk automatically and exposes a **Saved Runs** browser:

- **Saved Runs list** — each persisted result shows its time, LTF, strategies,
  net PnL, win %, and trade count, with **Load** (renders the full result into
  the results area) and **Delete** buttons.
- **Saved Runs Comparison** — a sortable table built from the cumulative
  `comparison.csv` showing every run side-by-side.
- Results survive **app restarts**, not just page refreshes (plain files on
  disk, no DB).

Saved runs live in the same place as the CLI output: `backtest_cache/cli/`.
A run created in the UI is visible to the CLI parser and vice-versa.

---

## 1. Run backtests

> The server must be running (e.g. `uv run uvicorn app.main:app --host 0.0.0.0 --port 8000`).
> The client submits a job and polls it to completion.

### Basic usage

```bash
.venv/bin/python scripts/backtest_client.py run \
    --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \
    --timeframe 15m \
    --strategies mean_reversion,liquidity_sweep,trend_pullback,vwap_reversion,spike_continuation \
    --days 60 \
    --capital 1000 \
    --base-url http://localhost:8000
```

To compare multiple timeframes, run once per LTF (each maps to its own HTF
automatically):

```bash
for ltf in 15m 1H; do
  .venv/bin/python scripts/backtest_client.py run --timeframe $ltf --days 60
done
```

### Options (shared `run` / `grid` subcommands)

| Flag | Default | Meaning |
|---|---|---|
| `--symbols` | `BTC-USDT-SWAP` | Comma-separated OKX symbols |
| `--timeframe` | `15m` | LTF to backtest (maps to its own HTF automatically) |
| `--strategies` | all 5 | Comma-separated strategy names to enable |
| `--days` | `30` | Trailing window in days, ending now |
| `--capital` | `1000` | Initial capital / per-trade notional |
| `--warmup` | `200` | Warmup candles before `start_ts` for indicator stabilisation |
| `--base-url` | `http://localhost:8000` | Server address |
| `--evaluation-mode` / `--evaluation-timeframe` | `finer_ltf` / `1m` | Evaluation stepping |
| `--workers` | `8` (A/B clients) | Max concurrent submissions to the server |
| `--validation-folds` | `0` | Chronological validation folds; 0 scores the full requested interval |
| `--validation-train-ratio` | `0.7` | Unscored prefix fraction of the interval before any final holdout; it does not fit parameters |
| `--final-holdout-fraction` | `0` | Reserve a terminal fraction for one evaluation of the selected validation candidate; requires validation folds |
| `--search-mode` | `exhaustive` | `exhaustive` or seeded `random` candidate sampling |
| `--combination-budget` | `0` | Maximum random candidates; 0 means use all combinations |
| `--random-seed` | `42` | Seed for reproducible random sampling |

### Parameter sweep (`grid` subcommand)

```bash
.venv/bin/python scripts/backtest_client.py grid \
    --symbols BTC-USDT-SWAP --timeframe 15m --strategies mean_reversion \
    --days 60 --capital 1000 \
    --params strategies.mean_reversion.rsi_oversold=25,30,35 \
    --params strategies.mean_reversion.max_adx=20,25,30 \
    --rank-by net_profit_after_cost_pct \
    --validation-folds 4 --validation-train-ratio 0.6 \
    --final-holdout-fraction 0.15 \
    --search-mode random --combination-budget 64 --random-seed 7
```

  The initial prefix is excluded from candidate scoring; it is **not** a training
  period. Validation folds are non-overlapping, and a requested final holdout is
  reserved at the end of the date range. The best eligible candidate is chosen
  using validation scores first, then evaluated once on the holdout. Holdout
  results are reported separately and never reorder the validation ranking. Keep
  that holdout untouched when making parameter choices.

### Strategy-specific A/B sweeps

Thin clients (also REST-driven) that keep their strategy-specific catalog
client-side and submit one single-strategy run per variant:

```bash
.venv/bin/python scripts/run_gate_ab_sweep.py --strategy liquidity_sweep --gate all
.venv/bin/python scripts/run_trend_pullback_ab.py --symbols AEON-USDT-SWAP
.venv/bin/python scripts/run_vwap_ab_sweep.py --symbols BTC-USDT-SWAP
```

### Timeframe → higher-timeframe mapping

The engine resolves the HTF automatically via `htf_for()`:

| LTF | HTF |
|---|---|
| 15m | 1H |
| 1H  | 4H |
| 4H  | 1D |

So `--timeframe 15m` compares **15m/1H** against `--timeframe 1H` → **1H/4H** regimes.

### Notes on data

- Existing candles are **cached** in `backtest_cache/*.json` and reused, so
  repeat runs are fast and need no OKX API keys for those pairs.
- The run uses `finer_ltf` evaluation (steps on 1m) by default, matching live
  intra-candle behaviour.

---

## 2. Parse results

### What gets written when you run a backtest

All output is persisted under `backtest_cache/cli/`:

| Path | Content |
|---|---|
| `<timestamp>_<ltf>_results.json` | Full result/config, cost assumptions, data-source fingerprints, metrics, every trade with fees/funding/slippage, and grid fold/holdout evidence when applicable |
| `<timestamp>_<ltf>_per_strategy.json` | Per-strategy breakdown |
| `comparison.csv` | **One row per run**, cumulative across runs — easy to diff in a spreadsheet |
| `overview.json` | Machine-readable list of all run summaries |

### Read the results as a table

```bash
.venv/bin/python scripts/parse_backtest_results.py
```

Prints a side-by-side risk/return table (LTF, HTF, Trades, Win%, PF, NetPnL,
Return%, MaxDD%, Sharpe, Expectancy, AvgTrade) plus a per-strategy breakdown
for the first run.

### Filters / control

```bash
# Only 1H runs, sorted by win rate, top 5
.venv/bin/python scripts/parse_backtest_results.py --ltf 1H --sort-by win_rate --top 5

# Machine-readable JSON for downstream tooling
.venv/bin/python scripts/parse_backtest_results.py --json
```

| Flag | Default | Meaning |
|---|---|---|
| `--source` | `auto` | `csv` (comparison.csv), `json` (*_results.json files), or `auto` (json, then csv) |
| `--ltf` | — | Only show runs for a given LTF (e.g. `15m` or `1H`) |
| `--sort-by` | `m_sharpe_per_candle` | Sort key (metric column) |
| `--json` | off | Emit JSON instead of a table |
| `--top` | `0` | Show only the top N rows (0 = all) |

> The metric columns are exposed both in the raw `metrics` dict and flattened
> as `m_*` keys. Use the `m_`-prefixed name when sorting.

---

## 3. Recommended workflow (15m vs 1H)

These strategies generally scale well to a 1H analysis timeframe:

- **Liquidity Sweep** and **Trend Pullback** tend to *improve* on 1H (cleaner
  structure / HTF trend).
- **Mean Reversion** and **Spike Continuation** are scalping-oriented and
  usually degrade on 1H.

To confirm with data on your symbols:

```bash
# 1. Run the comparison (once per timeframe)
.venv/bin/python scripts/backtest_client.py run \
    --strategies liquidity_sweep,trend_pullback \
    --timeframe 15m --days 60 --capital 1000
.venv/bin/python scripts/backtest_client.py run \
    --strategies liquidity_sweep,trend_pullback \
    --timeframe 1H --days 60 --capital 1000

# 2. Inspect results
.venv/bin/python scripts/parse_backtest_results.py
```

Repeat for all 5 strategies if you want the full picture:

```bash
.venv/bin/python scripts/backtest_client.py run --timeframe 15m --days 60
.venv/bin/python scripts/backtest_client.py run --timeframe 1H --days 60
```

---

## Troubleshooting

- **`No comparison.csv found` / `No matching runs found`** — you haven't run a
  backtest yet, or filtered to an LTF with no results. Run
  `backtest_client.py` first, then check `backtest_cache/cli/`.
- **`Unsupported timeframe 'XYZ'`** — pass a supported LTF: `1m, 5m, 15m, 1H, 4H, 1D`.
- **`request failed` / connection refused** — the tai2 server isn't running at
  `--base-url`; start it first.
- **Unknown strategies** — the server validates against
  `available_strategy_names()` and prints the valid list.
- **No data fetched** — the (read-only) OKX `MarketData` client needs a network
  connection for pairs not already in the local cache.