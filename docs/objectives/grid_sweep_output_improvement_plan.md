# Grid Sweep Output Improvement Plan

**Status:** ✅ All phases implemented
**Date:** 2026-09-26
**Author:** Senior Python Quantitative Engineer (crypto microstructure)
**Scope:** `app/services/backtest/grid.py`, `sweep_analysis.py`, `research.py`, `models.py`, `persistence.py`, `api_models.py`, `app/ui/pages.py`, `scripts/backtest_research.py`
**Goal:** Turn the parameter-sweep output from a wall of numbers into (a) a decision-first report a human can read in 30 seconds, and (b) a compact, self-describing artifact an expert/LLM can consume to make defensible parameter-change recommendations.

---

## Implementation status

| Phase | Status | Notes |
|---|---|---|
| 1 — Correctness & ranking integrity | ✅ Complete | See §9.1 |
| 2 — Per-run evidence | ✅ Complete | See §9.2 |
| 3 — Analysis upgrades | ✅ Complete | See §9.3 |
| 4 — Recommendation layer | ✅ Complete | See §9.4 |
| 5 — Handoff artifact | ✅ Complete | See §9.5 |
| 6 — UI readability | ✅ Complete | See §9.6 |

---


## 1. What the grid sweep produces today (audit)

### 1.1 Data model (`models.py`)

```
GridConfig(base_config, params[], rank_by, min_trades, validation_folds,
           validation_train_ratio, final_holdout_fraction, search_mode,
           combination_budget, random_seed)

GridResult(config, runs[], ranked[], final_holdout, assumptions,
           data_provenance, total/attempted_combinations, search_mode,
           random_seed, error)

GridRunResult(params, result: BacktestResult|None, rank_score: float|None,
              below_min_trades: bool, fold_metrics: list[dict])
```

### 1.2 Ranking (`grid.py`)

- Each combination runs the full engine per validation fold (or once for the full window).
- `rank_score` = **mean of the `rank_by` metric across folds**, and only if *every* fold completed.
- Sort key: `(1, rank_score)` for eligible runs, `(0, 0.0)` for `None`/`below_min_trades` → eligible runs first, then everything else in arbitrary order.
- Default `rank_by = "net_profit_after_cost_pct"`.
- `_extract_metric()` falls back to the **best per-strategy value** when the aggregate metric is missing or zero.

### 1.3 Analysis (`sweep_analysis.py`)

`analyze_sweep(entries, rank_by, min_trades)` returns:
- `best` — the single highest-scoring combination + 13 headline metrics.
- `robustness` — `within_5pct` count, `plateau` bool, `single_point_optimum` bool (best beats all 1-knob neighbours by >5%).
- `sensitivity` — per parameter, the **average** `rank_by` across *all* combinations that used each value.

### 1.4 Research bundle (`research.py`)

`build_research_bundle()` adds `fold_consistency`, `concentration`, `stress_comparison`, and a `candidate`/`inconclusive`/`reject` verdict. Rendered to Markdown by `render_bundle_markdown()`.

### 1.5 Surfaces

- **UI** (`pages.py::_render_sweep_results` + `_render_sweep_analysis`): ranked table (params + 8 metrics), fold-details table, holdout card, best-combination card, sensitivity table, assumptions/provenance expansions.
- **CLI** (`scripts/backtest_research.py`): baseline → sweep → stress → bundle, writes `research_bundle.json` + `.md`.

---

## 2. Diagnosis — why the output is not informative

### D1. A single scalar hides the trade-off surface
`rank_by` collapses return, risk, sample size, and consistency into one number. A 3-trade +40% combination outranks a 200-trade +12% one. `min_trades` mitigates but does not fix it, and the ranked table shows no risk-adjusted or significance columns by default.

### D2. Uncertainty is computed but never surfaced
`compute_metrics` already emits `net_expectancy_t_stat`, `net_expectancy_ci95_low/high_normal_approx`, `net_trade_pnl_stddev`, `net_expectancy_standard_error`. The grid output ignores all of them. A top combination with t-stat 0.4 is visually indistinguishable from one with t-stat 3.0.

### D3. No baseline or benchmark comparison
The engine computes an equal-weight buy-and-hold benchmark (`compute_equal_weight_buy_and_hold`) and the research CLI runs a frozen-defaults baseline — but the grid output never answers "did the winner beat holding?" or "did it beat the current defaults?". The user sees absolute numbers with no reference point.

### D4. Sensitivity is confounded and dispersion-blind
`sensitivity` averages each value across the *entire* grid, so a value's score is contaminated by whatever other parameters happened to co-occur with it. It is not a marginal effect. It also reports only the mean — a value with a high mean and huge variance is presented as "best" with no warning.

### D5. Robustness is one coarse heuristic
`single_point_optimum` uses a single 5% threshold on one metric. There is no per-parameter plateau detection, no **edge-of-range** detection (best value at the boundary ⇒ the true optimum is outside the swept range), no interaction detection, and no stability across symbols.

### D6. Fold evidence is collected but not aggregated
`fold_metrics` holds per-fold net PnL, trades, and drawdown, but nothing computes fold dispersion, worst fold, or sign consistency. `fold_consistency` (in the bundle only) counts positive folds but ignores magnitude and is not shown in the UI.

### D7. **Defect: the single-trade concentration screen never fires**
`research.concentration()` reads `t.get("net_pnl")` from serialized trades:

```python
gross_profit = sum(_num(t.get("net_pnl")) for t in trade_rows if _num(t.get("net_pnl")) > 0)
largest_win = max((_num(t.get("net_pnl")) for t in trade_rows), default=0.0)
```

But `persistence._trade_to_dict()` emits `pnl` (after-slippage) and **no `net_pnl` key**. Therefore `gross_profit == 0` and `largest_win == 0` always, so `max_trade_share` is always `None` and the "Profit is concentrated in one trade" reason can never trigger. The screen is silently dead.

### D8. **Defect: rank scope can disagree with displayed metrics**
`_extract_metric()` falls back to the best per-strategy value when the aggregate is missing/zero. A combination can therefore be ranked on a per-strategy number while the table displays aggregate metrics — the ranking and the evidence disagree, with no indication.

### D9. The handoff artifact is not handoff-shaped
`grid_result_to_dict()` embeds **every** run's full trade list and equity curve. A 1,000-combination sweep produces a multi-hundred-MB JSON that is impractical to give an analyst or an LLM, and it has no decision-oriented summary at the top.

### D10. UI is a table, not an analysis
No charts (the codebase already uses `ui.echart` elsewhere), no highlighting of the best row, no drill-down from a row to that combination's trades/equity, no baseline row, no export.

### D11. No recommendation layer
Nothing translates the sweep into "change `rsi_oversold` 30→35, expected effect +X%, confidence medium, because it sits in a plateau of 4 values". The user must reverse-engineer this from the sensitivity table.

---

## 3. Design goals

1. **Decision-first readability** — verdict, recommended changes, and confidence before raw data.
2. **Statistical honesty** — surface uncertainty, sample size, multiple-comparison risk, and OOS discipline.
3. **Expert/LLM handoff** — a compact, schema-versioned, bounded-size artifact with a stable contract and explicit caveats.
4. **Reproducibility preserved** — seed, provenance, and full config remain intact.
5. **No live-parity regressions** — screener universe, sizing, and guardrail semantics untouched.

---

## 4. Proposed improvements (phased)

### Phase 1 — Correctness & ranking integrity *(small, high value)*

| # | Change | File |
|---|---|---|
| 1.1 | Fix `concentration()` to read `pnl` (or add `net_pnl` to `_trade_to_dict`). Add a regression test with a single-trade-dominated run. | `research.py` / `persistence.py` |
| 1.2 | Remove the silent per-strategy fallback in `_extract_metric`. Record `rank_scope` (`"aggregate"` or `"per_strategy:<name>"`) on `GridRunResult`; if the aggregate metric is absent, set `rank_score=None` rather than substituting a different scope. | `grid.py`, `models.py` |
| 1.3 | Add a composite `rank_by="robust_score"` option: weighted blend of net return after cost, expectancy t-stat, fold consistency, and a drawdown penalty. Keep all single-metric rankings available. | `grid.py`, `sweep_analysis.py` |
| 1.4 | Add `min_expectancy_t_stat` (and/or min trades *per fold*) as an eligibility gate; ineligible runs are reported but flagged, not ranked. | `models.py`, `grid.py` |

### Phase 2 — Richer per-run evidence *(data model)*

Extend `GridRunResult` with compact, serializable summaries (no full trade lists except top-N):

- `summary` — trades, net after cost, net after cost %, net PF, expectancy, **t-stat**, **CI95 low/high**, max DD %, Sharpe/candle, net win %, avg R, time-in-market, exit-reason mix.
- `fold_summary` — per-fold net/PF/trades/DD plus `std`, `worst`, `positive_count`, `sign_consistency`.
- `symbol_summary` — per-symbol net share + trades (from `per_symbol`).
- `stability` — per-parameter plateau membership and `edge_of_range` flag.
- `baseline_delta` — vs frozen-default baseline and vs buy-and-hold (baseline supplied to the grid or computed in the bundle).
- `rank_scope`, `rank_score`, `rank_score_ci`.
- `detail_retained: bool` — full trades/equity kept only for the top `top_n_detail` runs (default 10); others carry `trades_omitted=True`.

### Phase 3 — Analysis layer upgrades (`sweep_analysis.py`)

| # | Change |
|---|---|
| 3.1 | **Marginal (OAT) sensitivity** — for each parameter, hold the others at the best combination's values and vary this one. Report the true marginal curve, slope, monotonicity, best value, and edge-of-range flag. |
| 3.2 | **Interaction detection** — for each parameter pair, measure how much A's effect varies across B's levels; flag dominant interactions (which invalidate naive per-parameter reads). |
| 3.3 | **Per-parameter plateau detection** — contiguous value ranges whose mean score is within X% of the best ⇒ a "safe range" recommendation instead of a single point. |
| 3.4 | **Robustness score (0–1)** — combine plateau width, fold consistency, symbol breadth, t-stat, and an edge-of-range penalty. Optionally rank by it. |
| 3.5 | **Multiple-comparison caveat** — report combinations tested and a best-of-N haircut / deflated expectation so a top score from 5,000 combinations is not read as significant. |

### Phase 4 — Recommendation layer *(new module `recommendations.py`)*

Deterministic, data-only recommendation object:

- `recommended_params` — per parameter: value + rationale (plateau centre / marginal best / keep default).
- `expected_effect` — baseline → recommended delta on key metrics, explicitly labelled **in-sample**.
- `confidence` — low/medium/high from robustness score + fold consistency + t-stat + holdout.
- `risks` — edge optimum, single-symbol concentration, low trades, dominant interaction, stress failure.
- `next_experiments` — narrower range around the plateau, untested parameters, different regime window.
- `caveats` — standard non-guarantee language.

### Phase 5 — Expert/LLM handoff artifact

- **5.1** Define a compact `sweep_report` schema: `schema_version`, config, provenance, baseline, **top-N runs (summary only)**, marginal sensitivity, interactions, plateaus, robustness, recommendation, caveats. Bounded size (no full trades/equity except optionally top-1).
- **5.2** `render_sweep_markdown(report)` for humans; JSON for machines.
- **5.3** UI "Copy for analysis" / download button; CLI `--emit-report`.
- **5.4** Optional deterministic prompt template wrapping the report for the existing LLM infra (data-only, no free-form generation).

**Target shape of the handoff artifact (illustrative):**

```json
{
  "schema_version": 1,
  "report_type": "grid_sweep",
  "workflow": {"strategy": "mean_reversion", "symbols": ["BTC-USDT-SWAP"],
               "timeframe": "15m", "days": 90, "rank_by": "robust_score"},
  "baseline": {"net_profit_after_cost_pct": 3.1, "trades": 84, "t_stat": 1.2},
  "benchmark": {"buy_and_hold_return_pct": 6.4},
  "top_runs": [
    {"params": {"rsi_oversold": 35, "max_adx": 25},
     "net_profit_after_cost_pct": 11.8, "trades": 96, "t_stat": 2.4,
     "ci95": [4.1, 19.5], "max_dd_pct": 7.2, "fold_consistency": 0.75,
     "robust_score": 0.71, "edge_of_range": false}
  ],
  "marginal_sensitivity": [
    {"key": "strategies.mean_reversion.rsi_oversold",
     "curve": [{"value": 25, "mean": 4.2}, {"value": 30, "mean": 8.9},
               {"value": 35, "mean": 11.1}, {"value": 40, "mean": 9.8}],
     "best": 35, "plateau": [30, 35, 40], "edge_of_range": false}
  ],
  "interactions": [{"a": "rsi_oversold", "b": "max_adx", "strength": 0.62}],
  "recommendation": {
    "changes": [{"key": "strategies.mean_reversion.rsi_oversold",
                 "from": 30, "to": 35, "rationale": "plateau centre"}],
    "confidence": "medium",
    "risks": ["profit concentrated in 1 symbol (78%)"],
    "next_experiments": ["sweep rsi_oversold 32-38 at 1-step resolution"]
  },
  "caveats": ["In-sample hypothesis; confirm on untouched OOS and paper trading."]
}
```

### Phase 6 — UI readability

- **6.1** Decision-first layout: verdict banner + recommended changes + confidence at top; evidence next; raw table last.
- **6.2** Charts via existing `ui.echart`:
  - parameter heatmap (2 params vs rank metric) or parallel-coordinates for >2 params;
  - marginal-effect line per parameter (mean ± band);
  - per-fold net bar chart for the selected combination;
  - equity curve of best vs baseline vs buy-and-hold.
- **6.3** Ranked table: highlight the best row, add t-stat / CI / DD columns, colour-code sign, row click → drill-down (trades, equity, per-symbol).
- **6.4** Pinned baseline comparison row.
- **6.5** Export buttons (JSON report, Markdown, ranked-table CSV).

---

## 5. Deliverables / file map

| File | Change |
|---|---|
| `models.py` | Extend `GridRunResult` (summaries, `rank_scope`, `detail_retained`); extend `GridConfig` (`rank_by` options, `top_n_detail`, `min_expectancy_t_stat`). |
| `grid.py` | Compute summaries, `rank_scope`, top-N detail retention, baseline delta; fix `_extract_metric`. |
| `sweep_analysis.py` | Marginal sensitivity, interactions, plateaus, robustness score, multiple-comparison note. |
| `recommendations.py` *(new)* | Recommendation object + Markdown renderer. |
| `research.py` | Fix `concentration`; consume new analysis; extend bundle. |
| `persistence.py` | Serialize new fields; compact `sweep_report` writer. |
| `api_models.py` / `api.py` | Expose new options. |
| `pages.py` | Decision-first UI, charts, drill-down, export. |
| `scripts/backtest_research.py` | `--emit-report`. |
| `tests/test_backtest_grid.py` + new tests | Cover all of the above. |

---

## 6. Acceptance criteria

- Ranking never silently uses a different scope than the displayed metrics (`rank_scope` recorded and shown).
- The single-trade concentration screen fires on a synthetic single-trade-dominated run.
- The compact report JSON is bounded (< ~200 KB for a 1,000-combination sweep) and round-trips through `json.dumps`.
- Marginal sensitivity matches a hand-computed OAT example.
- Edge-of-range and plateau flags are correct on synthetic data.
- The UI shows verdict + recommendation + at least one chart; row drill-down works.
- All existing tests pass; no live-parity regressions.

---

## 7. Sequencing & effort

| Phase | Effort |
|---|---|
| 1 — Correctness & ranking integrity | 0.5–1 day |
| 2 — Per-run evidence | 1–2 days |
| 3 — Analysis upgrades | 2–3 days |
| 4 — Recommendation layer | 1–2 days |
| 5 — Handoff artifact | 1 day |
| 6 — UI readability | 2–3 days |

Phase 1 alone removes two silent defects and materially improves trust in the ranking. Phases 3–5 are what make the output genuinely useful to an expert.

---

## 8. Risks & non-goals

- **Not a profitability guarantee.** Recommendations are in-sample hypotheses until they survive untouched OOS data, realistic costs, paper trading, and live monitoring.
- **Multiple-comparison correction is a caveat, not proof.** A best-of-N haircut reduces overconfidence; it does not validate an edge.
- **Keep live-parity invariants intact** — screener universe reconstruction, contract sizing, and guardrail semantics must not change.
- **Bounded artifacts** — retaining full trades/equity for every combination is the main size risk; top-N retention is the mitigation.

---

## 9. Implementation log

### 9.1 Phase 1 — Correctness & ranking integrity ✅

**Completed 2026-09-26.**

| Item | Change | Files |
|---|---|---|
| 1.1 | Fixed the dead single-trade concentration screen. `persistence._trade_to_dict` now emits `net_pnl` (the `SimPosition.net_pnl` property), and `research.concentration` reads `net_pnl` with a `pnl` fallback for legacy payloads. | `persistence.py`, `research.py` |
| 1.2 | Removed the silent rank-scope substitution. `_extract_metric_scoped` returns `(value, scope)`; `GridRunResult.rank_scope` records `"aggregate"` or `"per_strategy:<name>"`. The scope is serialized and round-trips. | `grid.py`, `models.py`, `persistence.py` |
| 1.3 | Added `rank_by="robust_score"` — a composite 0–1 score blending after-cost return (soft-capped), expectancy t-stat significance, fold consistency, and a drawdown penalty. Works for per-fold and holdout scoring via `_score_result`. | `grid.py` |
| 1.4 | Added `min_expectancy_t_stat` eligibility gate. `GridRunResult.below_min_t_stat` flags runs whose after-cost expectancy t-stat is below the threshold (or unmeasurable when the gate is enabled); flagged runs are excluded from the ranked head and from holdout selection. Exposed via `GridConfig`, the REST API (`BacktestGridRequest`), and the job manager. | `models.py`, `grid.py`, `api_models.py`, `job_manager.py` |

**Tests added:** `test_extract_metric_scoped_reports_scope`, `test_below_min_t_stat_gate`, `test_robust_score_prefers_significant_consistent_returns`, `test_grid_records_rank_scope_and_t_stat_gate`, `test_grid_robust_score_ranking`, `test_trade_share_uses_serialized_pnl_field`, `test_trade_share_prefers_net_pnl_when_present`.

**Verification:** `tests/test_backtest_grid.py` + `tests/test_backtest_research.py` → 31 passed.

**Effect:** Two silent defects removed (dead concentration screen; ranking/display scope mismatch), and the ranking can now be made significance-aware and risk-aware instead of a single raw-return scalar.

### 9.2 Phase 2 — Per-run evidence ✅

**Completed 2026-09-26.**

New module `app/services/backtest/summaries.py` (pure, JSON-safe) distils each run into a compact evidence block:

- `run_summary` — headline metrics including significance (`net_expectancy_t_stat`, CI95 bounds, PnL stddev), risk (max DD + duration, Sharpe/Sortino/Calmar), R-multiples, time-in-market, concurrency, and cost attribution.
- `fold_summary` — per-fold after-cost net PnL, mean/stddev, worst/best fold, and sign consistency.
- `symbol_summary` — per-symbol net contribution and share of total net profit (concentration risk).
- `benchmark_delta` — strategy return vs equal-weight buy-and-hold, with `beat_benchmark`.

`GridRunResult` gained `evidence` and `detail_retained`; `GridConfig` gained `top_n_detail` (default 10). `_apply_detail_retention` drops full trades/equity from all but the top-N ranked runs, keeping a large sweep's payload bounded while every run keeps its evidence summary. The holdout run also carries evidence. All fields round-trip through persistence and are exposed via the REST API and job manager.

**Tests added:** `tests/test_backtest_summaries.py` (6 tests) plus `test_grid_builds_evidence_and_retains_top_n_detail`.

**Verification:** grid + summaries + research tests → 38 passed.

**Effect:** Every combination now carries the evidence (significance, fold dispersion, symbol concentration, benchmark delta) needed to judge it, without shipping hundreds of trade lists.

### 9.3 Phase 3 — Analysis layer upgrades ✅

**Completed 2026-09-26.** `analyze_sweep` now returns four additional analyses alongside the existing best/robustness/sensitivity:

- **`marginal_sensitivity`** — true one-at-a-time (OAT) effect: holds every other parameter at the best combination's value and varies one parameter, reporting the marginal curve, slope, monotonicity, and an `edge_of_range` flag (best value at a swept boundary ⇒ the optimum may lie outside the range). This replaces the confounded whole-grid average as the primary per-parameter read.
- **`interactions`** — for each parameter pair, removes the additive main effects and reports the largest residual as interaction strength. A high strength means a parameter's best value depends on another, so a naive per-parameter read is unsafe.
- **`plateaus`** — contiguous value ranges whose marginal score is within 5% of best, i.e. a *safe range* rather than a single point.
- **`robustness_score`** — composite 0–1 score blending plateau fraction, expectancy t-stat significance, and an edge-of-range penalty.
- **`multiple_comparison`** — reports combinations tested and a `1/√N` deflation factor so a best-of-N spike is not read as significant.

**Tests added:** `test_marginal_sensitivity_is_oat_not_confounded`, `test_marginal_sensitivity_flags_edge_of_range`, `test_parameter_plateaus_detects_safe_range`, `test_detect_interactions_ranks_pairs`, `test_robustness_score_and_multiple_comparison`.

**Verification:** `tests/test_sweep_analysis.py` → 7 passed.

**Effect:** The sweep now answers *which parameter settings are robust* (plateaus, marginal curves, interactions) rather than only *which combination scored highest*.

### 9.4 Phase 4 — Recommendation layer ✅

**Completed 2026-09-26.** New module `app/services/backtest/recommendations.py` (deterministic, data-only) turns the sweep analysis into an actionable recommendation:

- **`changes`** — per parameter: baseline value → recommended value, with a rationale (plateau centre / marginal best / edge-of-range) and `changed`/`in_plateau`/`edge_of_range` flags.
- **`expected_effect`** — baseline → recommended delta on net return, trades, and drawdown, explicitly labelled `in_sample_validation`.
- **`confidence`** — `low`/`medium`/`high` from robustness score, fold consistency, expectancy t-stat, holdout survival, and stress failures, with the inputs recorded.
- **`risks`** — edge optimum, single-symbol/single-trade concentration, low trade count, strong interactions, stress failure.
- **`next_experiments`** — plateau refinement, range extension, joint 2-D sweeps.
- **`caveats`** — standard non-guarantee language.

`build_research_bundle` now attaches `recommendation` and the new Phase 3 analyses (`marginal_sensitivity`, `interactions`, `plateaus`, `robustness_score`, `multiple_comparison`) to the bundle, and `render_bundle_markdown` renders the recommendation section.

**Tests added:** `tests/test_backtest_recommendations.py` (4 tests).

**Verification:** recommendations + research tests → 18 passed.

**Effect:** The sweep now emits a concrete, auditable "change X from A to B, confidence medium, because it sits in a plateau of 3 values, but watch these risks" recommendation instead of leaving the user to reverse-engineer it.

### 9.5 Phase 5 — Expert/LLM handoff artifact ✅

**Completed 2026-09-26.** New module `app/services/backtest/sweep_report.py` builds a **bounded, schema-versioned** report from a serialised grid payload:

- `build_sweep_report(...)` — config + provenance + assumptions, baseline, buy-and-hold benchmark, **top-N runs (summary only, no trade lists or equity curves)**, marginal sensitivity, interactions, plateaus, robustness, multiple-comparison note, fold consistency, concentration, holdout, stress, and the Phase 4 recommendation.
- `render_sweep_markdown(...)` — human-readable Markdown.

Surfaces:
- **CLI** — `scripts/backtest_research.py --emit-report [--report-top-n N]` writes `sweep_report.json` + `sweep_report.md`.
- **UI** — a "Copy for analysis" button on the sweep results card builds the report and copies the Markdown to the clipboard.

**Tests added:** `tests/test_backtest_sweep_report.py` (4 tests), including a bounded-artifact assertion that no trade lists or equity curves are embedded.

**Verification:** sweep-report tests → 4 passed.

**Effect:** An expert or LLM can now be handed a compact, self-describing artifact (config, evidence, recommendation, caveats) instead of a multi-hundred-MB dump of every run's trades.

### 9.6 Phase 6 — UI readability ✅

**Completed 2026-09-26.** The BACKTEST sweep view is now decision-first:

- **Decision banner** (`_render_sweep_decision_banner`) at the top of the results: recommended parameter changes (from → to + rationale), a confidence badge colour-coded by level, the in-sample expected effect, and the top risks. It reuses the same `build_sweep_report`/recommendation layer as the CLI, so the UI, CLI, and handoff report never disagree.
- **Marginal-effect charts** (`_render_sweep_charts`) — one `ui.echart` line per swept parameter showing the OAT marginal curve with the best point marked.
- **Ranked table** gained `t-stat` and `CI95 (net expectancy)` columns, plus a `†` marker for runs excluded by the t-stat gate (alongside the existing `*` for below-min-trades).
- **"Copy for analysis"** button builds the compact sweep report and copies its Markdown to the clipboard.

**Verification:** full suite → 686 passed.

**Effect:** A user can read the verdict, the recommended changes, and the confidence in seconds, then drill into the evidence — instead of scanning a wall of numbers.

---

## 10. Final verification

- **Full test suite:** 686 passed (0 failures).
- **New tests:** `test_backtest_summaries.py` (6), `test_backtest_recommendations.py` (4), `test_backtest_sweep_report.py` (4), plus 7 new grid tests and 5 new sweep-analysis tests.
- **New modules:** `summaries.py`, `recommendations.py`, `sweep_report.py`.
- **No live-parity regressions:** screener universe reconstruction, contract sizing, and guardrail semantics are untouched.
- **Backward compatibility:** legacy grid payloads load with defaults for the new fields (`rank_scope="aggregate"`, `detail_retained=True`, `evidence={}`, `top_n_detail=10`, `min_expectancy_t_stat=0.0`).

### What changed for the user

| Before | After |
|---|---|
| A single scalar ranking | Significance-aware ranking (`robust_score`, t-stat gate) with recorded scope |
| No uncertainty shown | t-stat, CI95, fold dispersion, symbol concentration per run |
| Confounded per-parameter averages | True OAT marginal curves, plateaus, interactions |
| No recommendation | Deterministic change list with rationale, confidence, risks, next experiments |
| Multi-hundred-MB JSON dump | Bounded, schema-versioned sweep report (JSON + Markdown) |
| A table | Decision banner + charts + drill-down + copy-for-analysis |

