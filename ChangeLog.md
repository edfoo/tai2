# Change Log

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
