# Backtest Research and Parameter-Selection Plan

**Status:** P0, P1, P2, P3, and P4 implemented; cross-margin, trade-tape spread estimation, and historical screener-universe reconstruction are supported; only true order-book depth/impact calibration is data-limited  
**Date:** 2026-09-26  
**Goal:** Produce reproducible backtest and grid-sweep results that support evidence-based parameter recommendations. No backtest can guarantee profitability; the objective is to reduce simulation bias, quantify uncertainty, and make overfitting visible.

## Current assessment

The engine reuses live strategy evaluation and TP/SL resolution, models fees/slippage/funding, records equity and trade excursions, supports finer-timeframe stepping, and persists run results. P0 prevents entry-candle look-ahead and adds optional validation folds plus seeded bounded search. P1 adds reproducible API/persistence/UI handoff, data fingerprints, position-level diagnostics, and an isolated final holdout. P2 adds timestamped funding accrual into equity, three slippage modes (OHLCV proxy, trade-tape/OHLCV spread, fixed) with an adverse-scenario stress multiplier, gap-aware OHLC fills, tick-quantized protection prices, OKX contract/tier sizing, both isolated and cross-margin liquidation, and configurable same-symbol concurrency. P3 codifies the expert decision workflow as a bundle builder plus a CLI that runs baseline → sweep → stress → bundle and emits a screening verdict. P4 reconstructs the live dual-universe screener from historical candles so a backtest trades the same universe live would have selected, instead of a hand-picked symbol list. The only remaining P2 item is data-limited: true order-book depth/impact calibration, since no historical depth archive exists (spread can be estimated from the trade tape). Validation candidates can still overfit validation folds; spread estimates do not reproduce order-book depth, and liquidation remains an approximation.

The simulator's single-candle API remains available to direct callers; the engine now skips same-timestamp post-entry management, preserving the standalone simulator contract while preventing pre-fill OHLC look-ahead.

## Principles

1. Correct event ordering before optimizing metrics or adding search methods.
2. Keep the live strategy and shared TP/SL/trade-management implementations as the source of truth where possible.
3. Separate calibration from validation. Never present the best in-sample parameter set as a recommendation.
4. Model costs conservatively and state which market data and live-only gates are absent.
5. Preserve full configuration, assumptions, and per-fold outcomes so an expert can reproduce conclusions.
6. Prefer stable parameter plateaus across symbols, regimes, and time periods over a single unusually strong combination.

## P0: Correct event ordering and add out-of-sample sweep scoring

### P0.1 Exclude pre-fill movement on the entry candle

Signals are evaluated using the current candle close, then opened at that close, but the simulator is immediately given that same candle's high/low. This can trigger TP/SL using price movement that occurred before the close fill. In finer-LTF mode the same ordering applies to the evaluation candle.

**Implemented:** positions opened at a step are skipped for TP/SL, trade management, MAE/MFE, and held-candle increments on that candle, and strategy exits also skip them. The first later timestamped candle is evaluated normally; pessimistic stop-first handling remains. Regression tests cover entry-candle excursions and a TP on the following candle.

### P0.2 Make grid selection use forward validation windows

**Implemented:** opt-in validation folds exclude an initial prefix and split the remaining range into contiguous validation windows. Each window is run with the configured candle warmup; candidates rank by the mean fold score, and fold metrics are retained in the grid result. Invalid fold ranges are rejected. This is candidate scoring, not parameter fitting: the prefix is not used to train or choose parameters, and these folds are not an untouched final holdout.
The fold count defaults to zero to preserve legacy single-window behavior. The REST API and BACKTEST UI expose the fold controls. Because candidates are compared on these folds, the winning score remains subject to multiple-comparison selection bias; retain a separate untouched final holdout for any recommendation.

### P0.3 Bound search size and reduce exhaustive-search bias

**Implemented:** exhaustive search remains available; seeded random sampling can cap the number of combinations. Grid results record the full combination count, attempted count, mode, and seed in memory and persisted/API output. Do not claim a sampled top result is globally optimal. OAT/coordinate refinement can follow later.

### P0 acceptance checks

- Entry-step OHLC excursions cannot close or manage a newly filled position.
- Existing next-step TP/SL behavior and stop-first tie handling remain unchanged.
- Forward validation fold timestamps are non-overlapping, chronological, and in-range.
- OOS scoring is stable and deterministic for identical config, seed, and input data.
- Legacy API clients that omit validation/search options still work.
- Grid output identifies the evaluation mode and carries per-fold evidence.

## P1 Result Handoff

**Implemented:** a grid run is auditable from execution through API serialization, JSON persistence, reload, and the BACKTEST saved-sweep view. An analyst can recover settings and assumptions and inspect validation folds without relying on in-memory job state.

### Required output

- Full `BacktestConfig`: symbols, requested date range, strategies, launcher/strategy configuration, guardrails, costs, timeframe/evaluation mode, and warmup.
- Grid configuration: swept parameter definitions and values, ranking metric, minimum trades, validation fold count/prefix fraction, search mode, total/attempted combinations, and random seed.
- Per-fold outcomes: start/end timestamps, trade count, rank metric, net PnL/return, and drawdown; retain aggregate ranking and mark incomplete/failed folds clearly.
- Per-position/per-leg trade records with size, fees, funding, slippage, excursions, and management flags; equity curve; assumptions and unavailable live-only inputs.
- Source provenance per normalized candle series: symbol/timeframe, requested and returned ranges, cache/source metadata, candle count, cache key, and content fingerprint.
- A separately stored final-holdout candidate/result, with the holdout score excluded from ranking and sensitivity analysis.

### Acceptance checks

- Grid API output includes full config, search metadata, aggregate ranking, assumptions, and fold outcomes.
- Saved grid JSON is JSON-safe and preserves configuration, fold evidence, ranking, and assumptions.
- Saved/API results preserve returned-data fingerprints and complete per-trade cost attribution.
- Older single-backtest result files still load using `BacktestConfig` defaults for fields they do not contain.
- Tests cover grid job execution through serialization, actual JSON persistence, reload, multiple folds, and failed-fold visibility.
- The BACKTEST UI displays per-fold outcomes and simulation assumptions.

### P1 Completion

**Complete.** Data/cache provenance is attached to each normalized fetched candle series and persisted as source/cache metadata plus a deterministic SHA-256 content digest. The optional final holdout reserves a terminal time fraction, evaluates only the top eligible validation candidate once, stores it separately, and does not affect ranking. Fold executions use half-open endpoints so boundary candles cannot be scored in adjacent folds or in both validation and holdout.

## P1: Make results interpretable and reproducible

1. **Implemented:** persist complete `BacktestConfig`, grid/search settings, validation-fold evidence, per-trade costs, and data/cache provenance with JSON schema versioning.
2. **Implemented:** report execution/cost assumptions, unavailable historical features (footprint, order-book imbalance, open interest, and historical funding), and configured gates whose inputs are unavailable. Never fabricate those values.
3. **Implemented:** report position-level after-cost expectancy, standard deviation, standard error and a 95% normal-approximation interval (only when at least two positions exist), trade-return dispersion, time-in-market, concurrency, and maximum underwater duration. Partial-close legs share a stable position ID and are aggregated as one statistical observation. Mark Sharpe as mark-to-market evaluation-bar returns and emit the annualization factor and serial-correlation caveat.
4. **Implemented:** retain legacy `net_profit` as after-slippage/pre-fee-and-funding PnL, while adding explicit gross-before-costs, fee, funding, positive slippage-cost, and net-after-all-modeled-costs fields. Partial-close legs receive proportional entry-fee, funding, and slippage attribution. Sweep defaults rank by net return after modeled costs.
5. **Implemented:** benchmark the requested multi-symbol portfolio with synchronized equal-weight buy-and-hold closes, excluding warmup candles. The benchmark names its symbols/allocation and states that no trading costs are included; it is a spot-style reference, not a leveraged/cost-matched replica.

## P2: Improve crypto microstructure realism

1. **Implemented:** fetch/cache timestamped OKX historical funding events and accrue signed charges into cash/equity at settlement timestamps, including partial exits. `historical` is the default; missing history uses the configured constant-rate fallback. `constant` and `off` are explicit alternatives exposed in API/UI/CLI.
2. **Implemented (proxy + spread + stress):** three slippage modes. `ohlcv_liquidity` estimates adverse slippage from median prior completed-bar range and contract-adjusted quote turnover with configurable impact coefficient, range fraction, base bps, and cap. `tape_spread` estimates the **effective bid-ask spread** from OHLCV (Corwin-Schultz) or the public trade tape (Roll) and adds it to the base bps. `fixed` uses a constant. All modes exclude the execution candle and are scaled by `slippage_stress_multiplier` for adverse-scenario stress tests. **Data-limited:** the spread estimators capture spread cost but not order-book depth or market impact; OKX exposes no historical depth, so true impact calibration remains impossible. The spread estimate is a genuine improvement over a constant because it is derived from observed market data and varies over time.
3. **Implemented:** gap-through stops fill at the worse opening price; marketable TP limits fill at the favorable opening price. Same-candle stop-first behavior remains. OHLC bars still cannot resolve intrabar path/order beyond the conservative tie rule.
4. **Implemented (isolated + cross):** fetch/cache OKX `ctVal`, `lotSz`, `minSz`, max order size, and isolated position tiers; enforce lot/min/max size and cap leverage by the selected tier. TP/SL trigger prices are rounded conservatively to `tickSz`. In **isolated** mode the simulator reserves per-position initial margin and estimates liquidation from tier IMR/MMR/deduction via a single validated helper (`compute_isolated_liquidation_price`), filling adverse gaps at the open and charging an optional `liquidation_fee_bps`. In **cross** mode there is no per-position reservation: account equity backs all positions, new entries are capped by `equity × leverage − deployed notional`, and liquidation is account-level (all positions close when equity ≤ the aggregate maintenance requirement). `margin_mode` is selectable via API/UI/CLI; isolated remains the live default. **Data-limited:** funding accrued in the liquidation equation and exchange-specific risk adjustments are omitted; cross-margin does not model partial de-risking, auto-deleveraging, or insurance-fund mechanics.
5. **Implemented:** default to one open position per symbol across launcher strategies, matching live selection behavior; the `allow_concurrent_strategies_per_symbol` option permits comparison runs. Per-symbol capital remains bounded by portfolio sizing rules.

P2 controls and assumptions round-trip through the API, saved results, BACKTEST UI, and REST CLI. Instrument/funding cache source and fingerprints are included in result provenance.

### P2 Completion

**Complete.** All five P2 items are implemented and covered by tests: timestamped funding accrual, three slippage modes (OHLCV proxy, trade-tape/OHLCV spread, fixed) with a stress multiplier, gap-aware fills, tick-quantized protection with OKX contract/tier sizing and both isolated and cross-margin liquidation, and same-symbol concurrency control. The cost/margin knobs (`slippage_mode`, `spread_estimator`, `spread_window`, `slippage_stress_multiplier`, `liquidation_fee_bps`, `margin_mode`) round-trip through `BacktestConfig`, the engine `CostModel`/`Simulator`, the runner builders, the REST API, the job manager, the BACKTEST UI, and the REST CLI, and are reported in the simulation assumptions.

One item remains genuinely data-limited:

- **Order-book depth/impact calibration is data-limited.** Fitting market-impact coefficients requires historical order-book depth, which OKX does not expose and the project does not archive. The `tape_spread` mode estimates the effective *spread* (Corwin-Schultz/Roll) from OHLCV or the public trade tape, which is a real improvement over a constant, but it captures spread cost, not depth/impact. The stress multiplier remains the supported way to test adverse impact assumptions.

### P2 open items (data-limited)

P2 is complete for everything the available data supports. The remaining item is **not** unfinished implementation; it is bounded by data availability:

- **Data-limited (not implementable now):** true order-book depth/impact calibration requires a historical depth archive that OKX does not provide and the project does not store. Spread can be estimated from the trade tape (`tape_spread` mode); depth/impact cannot. The `slippage_stress_multiplier` is the supported way to test adverse execution assumptions until such data exists.

## P3: Decision workflow for experts

**Implemented:** the workflow is codified in `app/services/backtest/research.py` (bundle builder + screening) and driven end-to-end by `scripts/backtest_research.py`, which runs baseline → sweep → stress → bundle against the REST API and writes a JSON + Markdown analysis bundle.

1. **Implemented:** the CLI establishes a baseline with frozen canonical defaults (`build_single_strategy_launcher`), realistic costs, and the intended live evaluation timeframe before any tuning.
2. **Implemented:** the sweep is restricted to one strategy at a time and to the strategy's catalogue parameters (`grid_param_defs`); `--params` narrows it further. Inert/unavailable-data gates are not swept by default.
3. **Implemented:** the sweep uses forward-validation folds plus an untouched final holdout (`--validation-folds`, `--final-holdout-fraction`); the bundle reports per-fold net PnL and fold consistency.
4. **Implemented:** the bundle flags single-point optima (via `analyze_sweep` robustness), low fold consistency, and profit concentration in one symbol or one trade, and prefers plateaus.
5. **Implemented:** the CLI re-runs the best validation candidate under adverse presets (2× fees, 2× slippage, adverse funding, combined) and the bundle reports whether the edge survives each.
6. **Implemented:** the bundle contains full configuration, assumptions, aggregate and per-fold metrics, per-trade records, equity path, data provenance, and explicit caveats, with a screening verdict (`candidate` / `inconclusive` / `reject`).

The verdict is a screening aid, not a profitability guarantee: it encodes the plan's principles so a human can review the evidence quickly, and it never claims future profitability.

### P3 Completion

**Complete.** `app/services/backtest/research.py` provides the pure bundle builder (`build_research_bundle`, `fold_consistency`, `concentration`, `stress_comparison`, `render_bundle_markdown`) and `scripts/backtest_research.py` drives the full workflow over REST. The bundle is JSON-safe, round-trips through `json.dumps`, and renders to Markdown. Screening thresholds (`max_symbol_share`, `max_trade_share`, `min_fold_consistency`) are configurable. Tests cover fold consistency, concentration, stress deltas, the candidate/inconclusive/reject verdicts, and JSON/Markdown rendering.

## P4: Live-parity universe selection

**Implemented:** `universe_mode="screener"` reconstructs the live dual-universe screener from historical candles so a backtest trades the same universe live would have selected, rather than a hand-picked symbol list.

1. **Implemented:** the screener scoring core is extracted to `app/services/screener.py` (`score_universe`, `universe_for_strategy`). Live `MarketService.run_screener_if_due` / `get_screener_universe` and the backtest both call it, so the two cannot drift.
2. **Implemented:** `app/services/backtest/universe.py` reconstructs per-interval tickers from 1H candles (`last`/`open24h`/`high24h`/`low24h`/`volCcy24h`) and builds a time-indexed `UniverseSchedule` (SC/MR/union per interval). `Candle` now carries `quote_volume` (OKX `volCcyQuote`) so the volume filter matches live's `volCcy24h`; legacy caches fall back to `volume × close`.
3. **Implemented:** the engine resolves the union of every selected symbol as its fetch list and gates each strategy per step via `_symbol_in_universe` (SC strategies → SC list, MR strategies → MR list, soft overlap). Before the first interval the configured `symbols` act as the fallback list.
4. **Implemented:** `universe_mode`, `screener_config`, and `universe_candidate_symbols` round-trip through `BacktestConfig`, the runner builders, the REST API, the job manager, the BACKTEST UI, and the REST CLI. The reconstructed schedule is persisted on the result and rendered in the UI.
5. **Data-limited:** the live `max_spread_pct` filter reads live bid/ask, which OHLCV cannot reproduce. When enabled it is skipped and recorded in the schedule provenance; the reconstructed universe may be slightly larger than live's.

### P4 Completion

**Complete.** Tests cover scoring-core parity, strategy→universe routing, historical ticker reconstruction (including the quote-volume fallback), interval-boundary alignment, `UniverseSchedule` lookup, `build_universe_schedule` end-to-end, engine gating (explicit/screener/fallback), persistence round-trip, and API-model validation.

## Not a profitability guarantee

Backtests can establish whether a strategy survived a specified historical simulation, not whether it will be profitable in the future. Parameter recommendations remain hypotheses until they survive untouched out-of-sample data, realistic costs, paper trading, and ongoing live monitoring.