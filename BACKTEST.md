# Backtesting (headless CLI + UI persistence)

This document explains how to run **deterministic, headless** backtests and
parse the results, outside of the NiceGUI UI. The UI is fine for a quick look,
but its results live on `app.state` and are **lost on refresh**. The CLI tools
below persist everything to disk so results survive and can be diffed across
runs.

> **Note on the UI**: the BACKTEST page now **persists every completed result
> to disk automatically** and has a **Saved Runs** browser, so results also
> survive app restarts (not just page refreshes). See
> [UI persistence](#ui-persistence-saved-runs) below.

---

## Overview

| Interest | You want |
|---|---|
| Run backtests / compare timeframes (CLI) | [`scripts/run_backtest_cli.py`](#1-run-backtests) |
| Parse / compare results (CLI) | [`scripts/parse_backtest_results.py`](#2-parse-results) |
| Run / browse / load results (UI) | BACKTEST page → Saved Runs |

Both scripts live in `scripts/` and use the project venv. Run them from the
**repo root**. The UI and CLI share the same persistence format
(`app/services/backtest/persistence.py`), so a run produced by either is
viewable by the other.

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

### Basic usage

```bash
.venv/bin/python scripts/run_backtest_cli.py \
    --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \
    --timeframes 15m,1H \
    --strategies mean_reversion,liquidity_sweep,trend_pullback,vwap_reversion,spike_continuation \
    --days 60 \
    --capital 1000
```

### Options

| Flag | Default | Meaning |
|---|---|---|
| `--symbols` | `BTC-USDT-SWAP` | Comma-separated OKX symbols |
| `--timeframes` | `15m,1H` | Comma-separated LTFs to backtest (each maps to its own HTF automatically) |
| `--strategies` | all 5 | Comma-separated strategy names to enable |
| `--days` | `60` | Trailing window in days, ending now |
| `--capital` | `1000` | Initial capital / per-trade notional |
| `--warmup` | `200` | Warmup candles before `start_ts` for indicator stabilisation |
| `--rank-by` | `m_sharpe_per_candle` | Metric used to sort the printed comparison |

### Timeframe → higher-timeframe mapping

The engine resolves the HTF automatically via `htf_for()`:

| LTF | HTF |
|---|---|
| 15m | 1H |
| 1H  | 4H |
| 4H  | 1D |

So `--timeframes 15m,1H` compares **15m/1H** against **1H/4H** regimes.

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
| `<timestamp>_<ltf>_results.json` | Full result: metrics + every trade (symbol, direction, entry/TP/SL, close reason, PnL) |
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
# 1. Run the comparison
.venv/bin/python scripts/run_backtest_cli.py \
    --strategies liquidity_sweep,trend_pullback \
    --timeframes 15m,1H \
    --days 60 --capital 1000

# 2. Inspect results
.venv/bin/python scripts/parse_backtest_results.py
```

Repeat for all 5 strategies if you want the full picture:

```bash
.venv/bin/python scripts/run_backtest_cli.py \
    --timeframes 15m,1H --days 60
```

---

## Troubleshooting

- **`No comparison.csv found` / `No matching runs found`** — you haven't run a
  backtest yet, or filtered to an LTF with no results. Run
  `run_backtest_cli.py` first, then check `backtest_cache/cli/`.
- **`Unsupported timeframe 'XYZ'`** — pass a supported LTF: `1m, 5m, 15m, 1H, 4H, 1D`.
- **Unknown strategies** — the runner validates against
  `available_strategy_names()` and prints the valid list.
- **No data fetched** — the (read-only) OKX `MarketData` client needs a network
  connection for pairs not already in the local cache.