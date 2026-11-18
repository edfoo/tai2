# Grid Sweep Output — Strategy Attribution Plan

**Status:** ✅ All phases implemented (2026-09-27)
**Date:** 2026-09-27
**Author:** Senior Python Quantitative Engineer (crypto market microstructure)
**Depends on:** `docs/objectives/grid_sweep_output_improvement_plan.md` (phases 1–6, ✅ complete)
**Scope:** `app/services/backtest/summaries.py`, `grid.py`, `sweep_analysis.py`, `sweep_report.py`, `recommendations.py`, `persistence.py`, `app/ui/pages.py`, `scripts/parse_backtest_results.py`, `scripts/backtest_research.py`
**Goal:** Make every grid-sweep artifact state **which strategy produced each trade and each metric** — so a human (and an expert/LLM) can tell whether the swept parameter's edge comes from the strategy being tuned or from a co-enabled strategy, and can attribute results, exposure and exits per strategy.

---

## 1. Problem statement

The previous plan turned the grid sweep into a decision-first, expert-ready artifact. One
dimension is still missing from *every* grid surface: **strategy attribution**.

The grid sweep is fully **multi-strategy capable**:

- `BacktestConfig.strategy_names` is a **list** (`models.py`), and `BacktestRunRequest.strategy_names`
  defaults to *all* available strategies (`api_models.py`, `job_manager.py:193`).
- Each combination enables all requested strategies **simultaneously**; the engine evaluates every
  enabled strategy at each step and opens positions tagged with `strategy_name`
  (`engine.py:616`, `engine.py:815`).
- Decision order in the engine is `PRIMARY_DIRECTION_RANK` (SC → MR → LS → TP → VW) — **only the
  first strategy to signal on a symbol can open**, which materially shapes attribution.
- `compute_per_strategy_metrics` (`metrics.py:521`) already computes a full per-strategy metrics
  block, and `BacktestResult.per_strategy` is populated (`engine.py:498`) — including for the
  multi-fold aggregate (`grid.py:660`).

…and yet **none of that reaches the grid output**:

| Grid artifact | Strategy attribution today |
|---|---|
| `GridRunResult.evidence` | ❌ Only `summary`, `fold_summary`, `symbol_summary`, `benchmark_delta` — no strategy block |
| `sweep_report.top_runs` | ❌ per-run row has params + headline metrics only |
| `sweep_report` markdown | ❌ no strategy section |
| Sweep ranked table (UI) | ❌ one row per combination; no strategy column or breakdown |
| Sweep analysis / sensitivity | ⚠️ `params` are dotted `strategies.<name>.<param>`, but labels are stripped to `key.split('.')[-1]` → the owning strategy is invisible |
| Recommendation | ⚠️ changes name a bare param (`_short(key)`), never the strategy it belongs to |
| Grid trade drilldown (UI) | ❌ grid results have **no** trade drilldown at all (only single-run `_render_results` does) |
| `parse_backtest_results.py` | ✅ prints per-strategy for the first *single-run* result (grid results: N/A) |

### Why this is a defect, not a nicety

1. **Blended ranking.** With >1 strategy enabled, `rank_by` (e.g. `net_profit_after_cost_pct`) is a
   **portfolio** metric. A sweep of `mean_reversion.rsi_oversold` can "win" because a *different*
   enabled strategy (e.g. `spike_continuation`) carried the PnL that window. The user cannot see this
   — the strategy that actually earned the money is never named.
2. **Wrong tuning sign.** Without attribution the user may raise/lower a swept parameter based on
   another strategy's contribution, i.e. **tune the wrong knob**.
3. **`rank_scope` already hints at this** (`grid.py` falls back to the best *per-strategy* value when
   the aggregate is absent) — the machinery exists to reason per strategy, it is simply not surfaced.
4. **Expert/LLM handoff is blind.** `build_sweep_report` is explicitly built for expert/LLM handoff;
   an expert cannot make a defensible recommendation without knowing which strategy each number is
   attributed to, nor the per-strategy trade counts, win rates, exit mixes and R-multiples.

---

## 2. Design goals

1. **Attribute everywhere** — every grid surface (evidence, report JSON, report markdown, UI table,
   UI cards, recommendation) names the strategy behind each trade block and metric.
2. **Bounded** — no full trade lists shipped; strategy attribution is *aggregate per strategy*, not
   trade-by-trade dumps (except an opt-in top-N drilldown).
3. **Reproducible & consistent** — one shared summariser (like `symbol_summary`) used by UI, CLI, and
   the handoff report, so they cannot drift.
4. **Backward compatible** — legacy grid payloads load with an empty strategy block; nothing breaks.
5. **No live-parity regressions** — no change to sizing, guardrails, screener universe, or the engine
   decision order.

---

## 3. Proposed changes (phased)

### Phase 1 — Strategy attribution in the per-run evidence (data layer)

**`app/services/backtest/summaries.py`**

- Add `strategy_summary(per_strategy: dict) -> dict`, mirroring `symbol_summary`:
  - `total_net_profit_after_cost`, `strategies: [ {strategy, trades, net_profit_after_cost,
    share, win_rate, net_win_rate_after_cost_pct, net_profit_factor_after_cost,
    net_expectancy_after_cost, avg_r_multiple, avg_candles_held, exit_reasons}, … ]`.
  - `max_strategy_share` (concentration analogue to `max_symbol_share`) — flags a run whose PnL is
    dominated by a strategy the sweep is **not** tuning.
  - `n_strategies` (count of strategies that actually traded) vs `n_strategies_enabled`.
- Extend `build_run_evidence(...)` with `per_strategy=` and add the block under key `"strategy_summary"`.

**`app/services/backtest/grid.py`**

- `build_run_evidence(...)` calls (validation aggregate **and** holdout) pass
  `per_strategy=aggregate.per_strategy` / `holdout_result.per_strategy`.
- `_aggregate_fold_results` already recomputes `per_strategy` (`grid.py:660`) — no change needed.

**`app/services/backtest/persistence.py`**

- Verify `evidence` round-trips generically (it does — `evidence=dict(row.get("evidence") or {})`);
  add a legacy default `strategy_summary` in `build_run_evidence` so old payloads are consistent.

**Tests:** extend `tests/test_backtest_summaries.py` — `strategy_summary` shares, empty case,
single-strategy concentration. Extend `tests/test_backtest_grid.py` — evidence carries
`strategy_summary` and it survives `grid_result_to_dict` → `grid_result_from_dict`.

---

### Phase 2 — Strategy in the sweep analysis (name the owning strategy)

**`app/services/backtest/sweep_analysis.py`**

- Add a helper `strategy_of_key(key) -> str | None` that parses `strategies.<name>.<param>`;
  returns `None` for launcher-level keys.
- Annotate outputs with the owning strategy so labels are never orphaned:
  - `sensitivity[].strategy`, `marginal_sensitivity[].strategy`,
    `plateaus[].strategy`, `interactions[].a_strategy` / `.b_strategy`.
  - `best` gains `"strategies_traded"` (per-strategy net share from the best run, when supplied).
- Add a top-level `strategy_scope` block:
  ```json
  "strategy_scope": {
    "enabled": ["mean_reversion","spike_continuation"],
    "tuned":   ["mean_reversion"],
    "params_by_strategy": {"mean_reversion": ["rsi_oversold","max_adx"]}
  }
  ```
  This makes explicit **which strategies were tuned vs merely enabled** — the crux of the defect.

**Tests:** `tests/test_sweep_analysis.py` — `strategy_of_key` parsing; `strategy_scope` correct for a
single-strategy sweep and for a mixed sweep.

---

### Phase 3 — Strategy attribution in the handoff report (JSON + Markdown)

**`app/services/backtest/sweep_report.py`**

- `_top_runs`: add `strategy_breakdown` per top run (from its `evidence.strategy_summary`) and
  `max_strategy_share`.
- New top-level `best_run_strategy_breakdown` (attribution for the top-ranked combination) and
  `strategy_scope` (from Phase 2).
- `render_sweep_markdown`:
  - Add a **"Strategy attribution (best run)"** table: strategy · trades · net after cost · share ·
    win % · net PF · exit mix.
  - Add a **per-top-run strategy share** column.
  - Prefix parameter labels with their owning strategy where it disambiguates
    (`mean_reversion.rsi_oversold=35` instead of `rsi_oversold=35`).
- Keep the artifact **bounded** — attribution is per-strategy aggregates only.

**Tests:** `tests/test_backtest_sweep_report.py` — top-runs carry `strategy_breakdown`; markdown
contains the attribution section; bounded-artifact assertion still passes (no trade lists).

---

### Phase 4 — Strategy-aware recommendation

**`app/services/backtest/recommendations.py`**

- Each `change` gains `strategy` (owner of the dotted key) and a disambiguated `param` label.
- Add a `risks` entry + rationale when **another strategy dominates the best run's PnL**
  (`max_strategy_share` high but the dominant strategy is not the tuned one): *"Edge is not
  attributable to `<tuned strategy>`; `<other>` contributed N% of net — sweep `<other>` instead."*
- Add `attribution` block to the recommendation:
  `{"tuned": [...], "best_run_net_by_strategy": {name: pct|abs}, "dominant_strategy": name,
    "dominant_share": float, "edge_attributable_to_tuned": bool}`.
- `render_recommendation_markdown`: render the attribution block.

**Tests:** `tests/test_backtest_recommendations.py` — change carries owner strategy; mis-attribution
risk fires when a co-enabled strategy dominates.

---

### Phase 5 — UI: strategy column, breakdown cards, drilldown

**`app/ui/pages.py`** (`_render_sweep_results` / `_render_sweep_analysis`)

- **Header**: show the enabled strategy set (from `result.config.base_config.strategy_names`) and the
  tuned strategy set (from `strategy_scope`), so the user sees the blend up front.
- **Ranked table**: add a compact `strategies` column (traded strategies + each one's net share), e.g.
  `mean_reversion 62% · spike_continuation 38%`, and colour/flag rows whose dominant strategy is not
  tuned.
- **New: "Strategy attribution — best combination" card**: per-strategy table (trades, net after cost,
  share, win %, net PF, expectancy, avg R, exit mix) reusing the shared `strategy_summary` data (from
  `run.evidence`), so it matches the CLI/report exactly.
- **Row drilldown (top-N runs only)**: reuse the existing `_render_strategy_card` → `_render_symbol_card`
  → `_render_trade_table` components (already used for single runs) to let the user expand a retained
  top-N run into per-strategy → per-symbol → per-trade tables. Gate on `run.detail_retained` (only
  top-N keep trades) with an explanatory note for the rest.

**Tests:** UI is not unit-tested here; covered indirectly by the shared summariser tests. Manual
verification via the BACKTEST page.

---

### Phase 6 — CLI / parser surfacing

- **`scripts/parse_backtest_results.py`**: when a results file is a **grid** result, print a
  per-strategy breakdown for the best run (currently it only handles single-run `per_strategy`).
- **`scripts/backtest_research.py`**: `--emit-report` already writes `sweep_report.{json,md}`; the
  attribution additions flow through automatically. Add `strategy_scope` to the console summary.

**Tests:** extend `tests/test_backtest_client.py` / parser tests if present; otherwise a parser smoke test.

---

### Phase 7 — Docs & memory

- Update `BACKTEST.md` "What gets written" + a short "Strategy attribution" subsection describing the
  new `strategy_summary` / `strategy_scope` fields in the handoff report.
- Note in `.github/skills/launcher-strategy-reference/SKILL.md` (or the diagnostics memory) that grid
  attribution now exists, so future tuning reads it first.

---

## 4. Deliverables / file map

| File | Change |
|---|---|
| `summaries.py` | + `strategy_summary`, extend `build_run_evidence` |
| `grid.py` | pass `per_strategy` into evidence (folds + holdout) |
| `sweep_analysis.py` | + `strategy_of_key`, per-key `strategy` field, `strategy_scope` |
| `sweep_report.py` | + per-run `strategy_breakdown`, `strategy_scope`, markdown section |
| `recommendations.py` | + owner strategy per change, attribution block, mis-attribution risk |
| `persistence.py` | legacy default for `strategy_summary` (round-trip already generic) |
| `app/ui/pages.py` | strategy column, attribution card, top-N row drilldown |
| `scripts/parse_backtest_results.py` | grid best-run strategy breakdown |
| `scripts/backtest_research.py` | console `strategy_scope` |
| `BACKTEST.md` | document new fields |
| `tests/test_backtest_summaries.py`, `test_backtest_grid.py`, `test_sweep_analysis.py`, `test_backtest_sweep_report.py`, `test_backtest_recommendations.py` | new coverage |

---

## 5. Target shape of the added fields

```json
// GridRunResult.evidence
"strategy_summary": {
  "total_net_profit_after_cost": 18.4,
  "n_strategies": 2,
  "max_strategy_share": 0.62,
  "strategies": [
    {"strategy": "mean_reversion", "trades": 41, "net_profit_after_cost": 11.5,
     "share": 0.62, "net_win_rate_after_cost_pct": 58.5,
     "net_profit_factor_after_cost": 1.8, "net_expectancy_after_cost": 0.28,
     "avg_r_multiple": 0.42, "avg_candles_held": 6.1,
     "exit_reasons": {"tp": 22, "sl": 17, "time_stop": 2}},
    {"strategy": "spike_continuation", "trades": 19, "net_profit_after_cost": 6.9, "share": 0.38, "...": "..."}
  ]
}

// sweep_report
"strategy_scope": {"enabled": ["mean_reversion","spike_continuation"], "tuned": ["mean_reversion"],
                   "params_by_strategy": {"mean_reversion": ["rsi_oversold"]}},
"top_runs": [{"params": {"strategies.mean_reversion.rsi_oversold": 35},
              "strategy_breakdown": {"max_strategy_share": 0.62, "strategies": ["..."]}, "...": "..."}],
"best_run_strategy_breakdown": {"max_strategy_share": 0.62, "strategies": ["..."]},

// recommendation
"attribution": {"tuned": ["mean_reversion"], "dominant_strategy": "mean_reversion",
                "dominant_share": 0.62, "edge_attributable_to_tuned": true,
                "best_run_net_by_strategy": {"mean_reversion": 11.5, "spike_continuation": 6.9}}
```

---

## 6. Acceptance criteria

- Every top-N grid run's `evidence` carries a `strategy_summary` with per-strategy trades, net PnL,
  share, win %, net PF, expectancy, avg R and exit mix.
- `sweep_report` JSON + Markdown name the strategy for each tuned parameter and each top run's
  attribution; the bounded-size test still passes (no embedded trades/equity).
- The recommendation flags mis-attribution when a co-enabled (untuned) strategy dominates the best
  run's net PnL, and every `change` names its owning strategy.
- The UI shows the enabled-vs-tuned strategy sets, a per-strategy attribution table for the best
  combination, and a top-N row drilldown into per-strategy trades using the existing components.
- Legacy grid payloads load with an empty `strategy_summary` (no crash, no reconciliation step).
- Full test suite passes; no live-parity regressions.

---

## 7. Sequencing & effort

| Phase | Effort |
|---|---|
| 1 — Evidence data layer | 0.5 day |
| 2 — Sweep analysis owning strategy | 0.5 day |
| 3 — Handoff report JSON + Markdown | 0.5–1 day |
| 4 — Strategy-aware recommendation | 0.5 day |
| 5 — UI column / card / drilldown | 1–1.5 days |
| 6 — CLI / parser surfacing | 0.5 day |
| 7 — Docs | 0.25 day |

Phases 1–3 alone make the handoff artifact expert-correct. Phases 4–6 make it usable interactively.

---

## 8. Risks & non-goals

- **Not a profitability guarantee** — attribution is descriptive; it does not validate an edge.
- **Do not reorder strategy evaluation** — `PRIMARY_DIRECTION_RANK` is a live-parity invariant; the
  fix *reports* attribution, it does not change which strategy wins a symbol.
- **Bounded artifacts** — attribution is per-strategy aggregates; only top-N runs keep full trades.
- **Multi-strategy sweeps are legitimately blended** — the goal is to make the blend *visible*, not to
  forbid it. Where a sweep tunes one strategy while others are only enabled, the recommendation should
  surface the caveat rather than rank a blended number as if it were the tuned strategy's edge.
- **Non-goal:** per-trade strategy re-attribution or changing `trade_id`/sizing semantics.

---

## 9. Test plan (summary)

| Area | Test |
|---|---|
| `summaries.py` | `strategy_summary` shares, empty payload, concentration flag, exit-mix passthrough |
| `grid.py` | evidence carries `strategy_summary` for folds + holdout; survives persistence round-trip |
| `sweep_analysis.py` | `strategy_of_key` parsing; `strategy_scope` for single/mixed sweeps |
| `sweep_report.py` | top-run `strategy_breakdown`; markdown section; bounded artifact unchanged |
| `recommendations.py` | change owner strategy; mis-attribution risk + `attribution` block |
| parser | grid best-run strategy breakdown prints |
| regression | full suite green; legacy payload backward-compat |

---

## 10. Implementation log (2026-09-27)

All phases implemented. **696 tests pass.**

| Phase | Files | What landed |
|---|---|---|
| 1 | `summaries.py`, `grid.py` | `strategy_summary()` (trades, net, share, win %, net PF, expectancy, avg R, exit mix, `dominant_strategy`, `max_strategy_share`, `n_strategies`); `build_run_evidence(per_strategy=...)`; grid passes `per_strategy` for folds + holdout |
| 2 | `sweep_analysis.py` | `strategy_of_key()`; per-key `strategy`, `a_strategy`/`b_strategy`; top-level `strategy_scope` (`tuned` + `params_by_strategy`) |
| 3 | `sweep_report.py` | top-run `strategy_breakdown`/`dominant_strategy`/`max_strategy_share`; `strategy_scope` + `best_run_strategy_breakdown`; "Strategy scope" + "Strategy attribution (best run)" markdown; `_param_label` disambiguation |
| 4 | `recommendations.py`, `research.py` | `changes[].strategy`; `attribution` block; mis-attribution risk; research bundle `strategy_scope` + `strategy_attribution` + markdown |
| 5 | `app/ui/pages.py` | enabled-vs-tuned header + multi-strategy badge; "Strategy mix" ranked column; attribution card for best combination; top-N trade drilldown |
| 6 | `parse_backtest_results.py`, `backtest_research.py` | grid `_grid_summary_row` + attribution print; research CLI `tuned`/`attribution` console lines |
| 7 | `BACKTEST.md`, tests | §4 "Strategy attribution" docs; new tests in summaries/sweep_analysis/sweep_report/recommendations/grid |

**New tests:** `test_strategy_summary_shares_and_telemetry`, `test_strategy_summary_no_shares_when_unprofitable`, `test_strategy_summary_empty`, `test_strategy_of_key_parses_owner`, `test_strategy_scope_splits_tuned_vs_enabled`, `test_strategy_scope_mixed_strategies`, `test_report_carries_strategy_attribution`, `test_recommendation_names_owning_strategy`, `test_recommendation_flags_misattribution`, `test_recommendation_attribution_ok_when_dominant_is_tuned`, plus grid evidence assertion.

**Backward compatibility:** legacy grid payloads (verified against a real
`backtest_cache/cli/*_results.json`) load with an empty `strategy_summary` —
no crash, no migration step.

