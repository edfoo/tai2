# Change Log

## 2026-01-24
- Ensured `handle_llm_decision()` seeds `price_hints` and margin context before invoking open-position calculations, and added a regression test so future refactors cannot reintroduce the crash when snapshots lack prior prices.
- Reworked the free-equity guard to prioritize OKX-reported available margin (and added detailed notional breakdown logging plus a regression test) so trades are no longer blocked while real margin headroom remains.
- Let the per-symbol `guardrail_notional_cap` follow available margin * max leverage * max position %, ensuring we only clip sizes when margin headroom is actually exhausted and capturing the behavior in `test_guardrail_notional_cap_tracks_available_margin`.
- Made isolated seeding tier-aware by pulling OKX IMR data before adjustments and introduced `isolated_margin_seed_pct` (+ per-symbol overrides) so each bucket can only consume a bounded share of equity; covered by the new tier-seeding and seed-cap regression tests.
- Tightened the auto-seed percentage guardrail so `_resolve_isolated_seed_limit()` clamps transfers using both the global and per-symbol equity caps, and locked the behavior in `test_isolated_margin_seed_pct_caps_transfer` plus the new `test_isolated_margin_symbol_seed_pct_override_wins` regression.
- Reworked the tier-margin guard so it only enforces OKX tier size caps (instead of clipping by the currently free margin), which lets the isolated auto-seed flow top up collateral for symbols like SENT, GPS, and PIPPIN before submitting orders.
- Documented every remaining helper inside `app/services/market_service.py`, adding detailed docstrings so future maintainers and LLM agents understand leverage prep, TP/SL management, and execution feedback flows.
- Added a UML sequence diagram to `GUARDRAILS.md` that illustrates how MarketService funnels prompt output through guardrails, margin prep, OKX submission, and protection syncing, giving operators a visual map of the safety layers.
- Extended `app/services/market_service.py` equity guardrails to record free-equity availability plus requested/clipped notionals, and taught `app/ui/pages.py` to display the extra telemetry (including an "Equity clip" flag) so operators immediately see when exposure was scaled or blocked.

## 2026-01-23
- Added funding auto-seed retry logic to `MarketService`, ensuring isolated margin top-ups can pull from funding wallets when allowed by guardrails.
- Restored missing execution-context parsing inside `handle_llm_decision()` and removed duplicated margin code paths.
- Introduced `test_handle_llm_auto_seeds_isolated_margin` to cover the new funding transfer flow.
- Attached structured margin recommendations to execution-feedback entries when OKX rejects orders for insufficient isolated collateral, plus regression coverage for the new path.
- Surfaced a real-time Execution Alerts panel on the LIVE page so operators immediately see guardrail warnings and recommended configuration tweaks.
- Added a fallback margin recommendation for OKX 51008 errors so Execution Alerts always show an actionable hint, and captured the scenario in `test_submit_order_attaches_fallback_recommendation_without_guidance`.
- Implemented auto-downsizing when isolated margin caps block funding transfers, including UI guidance snapshots and regression coverage via `test_isolated_margin_buffer_auto_downsizes_to_seed_cap`.

## 2026-01-22
- Updated `_normalize_account_balances()` so OKX snapshots preserve `None` for unknown margin fields instead of defaulting to zero.
- Tightened `handle_llm_decision()` guardrails to re-fetch balances once and block execution when margin availability remains unknown; added regression coverage for the new guard.

## 2026-01-01
- Rolled out the enhanced guardrail suite: trade window/cooldown enforcement, TP/SL wait logic, and order-cap controls.
- Added configurable UI sections for leverage caps, symbol-specific overrides, and guardrail previews to keep operator inputs in sync with prompt payloads.
