# Improve Trade Management

**Trade-management loop:** `app/services/market_service.py` → `_check_trade_management()` (~lines 2240–2620), `_seed_trade_mgmt_state()`, `_clear_trade_mgmt_state()`, `_trade_mgmt_move_sl()`, `_trade_mgmt_partial_close()`, `_trade_mgmt_full_close()`
**Shared exit model:** `app/services/trade_management.py` (`calculate`, `compute_tp_sl_pct`, `_ensure_rr`)
**Skimming:** `app/services/market_service.py` → `_check_skimming()` (~line 1598), `_skim_close_position()`
**Protector:** `app/services/market_service.py` → `_check_protector()` (~line 1948), `_protector_update_sl()`
**Commutator:** `app/services/market_service.py` → `_check_commutator()` (~line 2765), `_commutator_flip()`
**Alternator:** `app/services/market_service.py` → `_check_alternator()` (~line 3701), `_alternator_flip()`
**Shotgun:** `app/services/market_service.py` → `_check_shotgun()` (~line 1818), `_shotgun_close_position()`
**Config defaults:** `app/ui/pages.py` → `render_strategy_page()` (~lines 2931–3010)
**Performance tool:** `scripts/performance_summary.py`
**Tests:** `tests/test_performance_summary.py`, `tests/test_trade_management.py` (if present), `tests/test_strategies.py`

---

## Goal

Make trade management **asymmetric** so that winners run and losers are cut small, instead
of the current symmetric profile where a fixed TP caps winners at a small amount while the
SL lets losers run to full size. The measured problem is an **inverted reward-to-risk**:
average win (+0.2652) is *smaller* than average loss (−0.3555), i.e. a 0.75:1 ratio, so the
bot loses money at a 52.5% win rate (break-even needs ~0.905:1).

The fixes below are listed in **priority order**. Implement all of them. Do not
"cherry-pick" — the first two are the highest-leverage changes and the rest harden them.

---

## Background / Current behaviour

### Measured performance (from `scripts/performance_summary.py`, 2026-08-09 → 2026-08-12)

```
Trades: 59   Wins: 31   Losses: 28   Win rate: 52.5%
Net PnL: -1.73 USDT
Avg win:  +0.2652
Avg loss: -0.3555
Break-even win rate: 57.3%
```

By strategy (heuristic attribution):

| strategy | trades | W | L | win% | net PnL | avg |
|---|---|---|---|---|---|---|
| trend_pullback | 36 | 15 | 21 | 41.7% | −2.33 | −0.0647 |
| liquidity_sweep | 6 | 1 | 5 | 16.7% | −1.61 | −0.2679 |
| vwap_reversion | 17 | 15 | 2 | 88.2% | +2.21 | +0.1298 |

### Diagnosed problems

1. **Inverted reward-to-risk.** Avg loss > avg win. The fixed TP (Skimming threshold, or
   the strategy's static/ATR TP) caps winners small while the SL lets losers run to full
   size. This is the core problem.
2. **The losing strategies are the entry problem, not the exit problem.** `trend_pullback`
   and `liquidity_sweep` are the bleed. Trade management is a *multiplier* on entry quality
   — it cannot rescue a strategy entering into the wrong regime. Fixing their entries is a
   prerequisite for any trade-management change to show up in PnL.
3. **Skimming is a fixed TP/SL, not trade management.** It closes at a fixed `threshold_pct`
   (default 2.0%) and optionally a fixed `stop_loss_pct`. This is exactly the symmetric
   profile that produces avg win < avg loss. It should be replaced by (or subordinated to)
   an asymmetric exit.
4. **The core `TradeMgmt` loop already has breakeven + partial TP + time-stop**, but the
   *remainder* after partial TP still has a fixed TP. There is no trailing component, so
   winners are still capped.
5. **Commutator and Alternator are churn/martingale machines.** Commutator reverses a
   losing position (doubles exposure into the losing side). Alternator oscillates long/short
   on thresholds (overtrades). Both fight the data and should be left disabled. Shotgun is a
   portfolio-level blunt instrument (closes *all* positions on account-equity thresholds) and
   cannot distinguish good from bad positions. Protector is conceptually sound (ratchet SL
   into profit) but only activates after a large profit % and can give back a big peak.

### Note on the "stops get hit" hypothesis

The user's hypothesis is that adding a stop-loss makes it more likely to be hit (resting
stops are a harvestable liquidity pool in the order book). This is a real microstructure
phenomenon, but the correct response is **not** to remove the stop — it is to make the stop
**structural and non-obvious** (beyond a real invalidation level, not a round ATR multiple)
and to make the exit **asymmetric** (breakeven + trailing). Removing the SL converts a
bounded loss into unbounded tail risk. The `calculate()` function in `trade_management.py`
already does structural placement; the launcher must feed it good structural levels.

### Software stops (monitoring-based exits) — the hybrid approach

The user's follow-up idea is to **track PnL and market-close a trade without placing a
stop-loss in the book**, so the SL level is never exposed. This is a legitimate, well-known
technique called a **software stop** (a.k.a. shadow/monitoring stop). It is worth doing —
but as the *normal* exit mechanism, **not** as a replacement for the SL.

**Precision on the order-book mechanics.** An OKX stop-loss *algo* order is a *trigger*
order, not resting liquidity — it does not sit in the visible book as a bid/ask. What *is*
exposed is the **trigger level**: clustered stop triggers just below support / above
resistance are a harvestable liquidity pool that gets swept. The real benefit of a software
stop is therefore not "the book isn't skewed" — it is that **your stop level never exists
anywhere except in the bot's memory**, so nobody can see or target it. That is a genuine
edge against stop-hunting.

**The critical safety flaw — why the SL cannot be fully removed:**

1. **Latency.** A resting SL is executed by the exchange instantly on touch. A software
   stop depends on the monitoring loop + network round-trip. On a fast altcoin, a flash
   crash or gap can fill far worse than the intended stop.
2. **Process death / disconnect.** If the bot crashes, loses its WebSocket, or the machine
   dies, there is **no stop in the book** — the position is completely unprotected. A
   resting SL survives bot failure; a software stop does not.
3. **Market-order slippage.** A software stop is *always* a market close, so it is exposed
   to the thin-book slippage that `performance_summary.py` already detects.

**Current monitoring frequency (verified in code).** The exchange is polled every
**10 seconds** via `_positions_refresh_loop` (`_positions_refresh_interval = 10` in
`market_service.py`). `_check_trade_management()`, `_check_skimming()`, `_check_protector()`,
`_check_commutator()`, and `_check_alternator()` all run inside that same loop on the same
10s cadence. This is the loop Skimming uses to track PnL. 10s is far better than the 180s
full poll, but it is **not** sub-second — on 15m volatile alts a 10s gap during a flash move
can still be material. A software stop is only as safe as how often you look.

**Recommended hybrid architecture:**

| Layer | Mechanism | Why |
|---|---|---|
| **Wide disaster SL** (in the book) | A resting algo stop placed *far* from price — beyond the structural invalidation, not at an obvious round level | Protects against process death, disconnect, and flash crashes. Because it is wide and structural, it is not an attractive hunt target. |
| **Software stops** (in the bot) | Monitor PnL and market-close on breakeven, trailing, time-stop, and the *normal* loss threshold | Hides your real exit levels from the book; lets you trail and manage without re-placing algo orders (rate-limited and expensive). |

This is where the codebase is already heading:
- `_check_trade_management()` already tracks `r_multiple`, `pnl_pct`, `upl_ratio`, `upl_usd`
  from the snapshot — the PnL tracking needed for software stops is **already present**.
- The **time-stop** and **partial TP** are already software stops (market closes based on
  monitored state).
- `_check_skimming()`'s SL path is already a software stop (it watches `uplRatio` and
  market-closes at `stop_loss_pct`).

**Concrete implementation (see Fix 2 and Fix 4):** stop attaching the *normal* SL algo
order, keep a wide disaster SL, and let the `TradeMgmt` loop market-close when
`pnl_pct <= -sl_pct` (reusing `_trade_mgmt_full_close`). The monitoring frequency must be
high — ideally WebSocket-driven position updates rather than the 10s poll — before relying
on the software stop as the primary loss protection.

---

## Fix 1 — Fix the losing strategies' entries first (PREREQUISITE)

### Problem

`trend_pullback` (41.7% win, −2.33) and `liquidity_sweep` (16.7% win, −1.61) are the bleed.
No trade-management change can make a 16.7% win-rate strategy profitable.

### Requirements

- Apply the existing per-strategy improvement docs first:
  - `docs/improvements/improve_trend_pullback.md`
  - `docs/improvements/improve_liquidity_sweep.md`
- Tighten entry filters (ADX bands, distance caps, confirmation candles) before touching
  exits. Prefer tightening entry quality over loosening guardrails.
- **Acceptance check:** after these fixes, re-run `scripts/performance_summary.py` and
  confirm `trend_pullback` and `liquidity_sweep` win rates rise above ~45% before
  attributing any remaining loss to exits.

---

## Fix 2 — Make the exit asymmetric: breakeven + trailing on the remainder (PRIORITY)

### Problem

The core `TradeMgmt` loop moves SL to breakeven at `breakeven_at_r` (0.7R) and takes a
partial at `partial_tp_at_r` (0.8R), but the remaining position still has a **fixed TP**.
This caps winners at the fixed TP while the SL is the only downside protection. The result
is avg win < avg loss.

### Requirements

Add a **trailing stop** to the `TradeMgmt` loop that applies to the *remainder* after the
partial TP, replacing the fixed-TP cap on that remainder. This is the single most important
change.

1. **New config keys** under `strategy.trade_management` (add defaults in
   `app/ui/pages.py` `render_strategy_page()`):
   - `trailing_enabled` – bool (default `True`)
   - `trailing_activate_r` – float, start trailing after this R (default `1.0`)
   - `trailing_distance_atr` – float, trail the SL this many ATR% behind price
     (default `1.5`). Use the trade-timeframe ATR% (`atr_tf_pct`) scaled to price.
   - `trailing_floor_r` – float, never let the trailing SL go below this R (default
     `0.5`). This guarantees the trailing stop never gives back more than the partial
     already banked plus a small buffer.
   - `trailing_step_r` – float, only re-place the SL when the improvement exceeds this
     many R (default `0.2`). Prevents re-placing the algo order on every tick (rate
     limiting / churn).

2. **Logic** in `_check_trade_management()`, after the partial-TP block:
   - Compute `r_multiple` as already done.
   - If `trailing_enabled` and `r_multiple >= trailing_activate_r` and the symbol is not
     already in a trailing-update set:
     - Compute the trailing SL price: for a long,
       `trail_sl = last_price * (1 - trailing_distance_atr * atr_tf_pct / 100)`; for a
       short, `trail_sl = last_price * (1 + trailing_distance_atr * atr_tf_pct / 100)`.
     - Compute the floor SL price from `trailing_floor_r`:
       `floor_sl = entry * (1 - trailing_floor_r * risk_pct / 100)` for a long (mirror
       for short).
     - Take `new_sl = max(trail_sl, floor_sl)` for a long (the more protective, i.e.
       higher), `min(...)` for a short.
     - **Ratchet:** only update if `new_sl` is strictly better than the current SL by at
       least `trailing_step_r * risk_pct` (i.e. `new_sl - current_sl >= step` for a long).
     - Reuse `_trade_mgmt_move_sl()` to cancel the old algo and re-place with the new SL,
       preserving the current TP (or with TP removed if the fixed TP is being replaced).
   - Add a `_trade_mgmt_trailing_updating` set (mirror `_trade_mgmt_be_updating`) to
     prevent concurrent updates for the same symbol.

3. **Remove the fixed-TP cap on the remainder.** When `trailing_enabled` is on, do not
   attach a fixed TP to the remainder after the partial. The trailing stop becomes the
   profit-taking mechanism. (Keep the fixed TP only when trailing is disabled, for
   backward compatibility.)

4. **Emit debug lines** in the same style as the existing BE/partial/time-stop lines, e.g.
   `TradeMgmt trailing: {symbol} R={r:.2f} → SL {old:.6f} → {new:.6f} (floor {floor:.6f})`.

5. **Acceptance check:** with trailing enabled, a trade that reaches 1R and then runs
   should close at a profit *above* the old fixed-TP cap, and a trade that peaks then
   reverses should close at no worse than `trailing_floor_r` (never back to breakeven
   minus the full SL). Re-run `performance_summary.py` and confirm avg win rises relative
   to avg loss (ratio trending toward ≥ 0.9:1).

6. **Software-stop integration (see Fix 4):** the trailing SL and the normal-loss exit
   should be implemented as **software stops** — the bot tracks PnL and market-closes via
   `_trade_mgmt_full_close()` rather than re-placing a visible SL algo order. This hides
   the real exit levels from the book. Keep only a wide structural disaster SL in the book
   (see Fix 4).

---

## Fix 3 — Replace Skimming's fixed TP/SL with the asymmetric exit (or subordinate it)

### Problem

Skimming closes at a fixed `threshold_pct` (default 2.0%) and optionally a fixed
`stop_loss_pct`. This is the symmetric profile that caps winners small.

### Requirements

- **Preferred:** disable Skimming (`enabled: False`) and rely on the asymmetric
  `TradeMgmt` exit from Fix 2. Skimming's fixed threshold directly conflicts with "let
  winners run."
- **Alternative (if Skimming must stay):** raise `threshold_pct` well above the trailing
  activation point so Skimming only fires as a *final* profit cap far out, and let the
  trailing stop do the normal profit-taking. Do **not** set a fixed `stop_loss_pct` — the
  structural SL + breakeven + trailing already handle the downside.
- **Acceptance check:** no position should be closed at a fixed small profit while the
  trailing stop would have captured more.

---

## Fix 4 — Implement software stops with a wide structural disaster SL (addresses the "stops get hit" concern)

### Problem

Stops placed at round ATR multiples or obvious levels are harvestable liquidity pools in
the order book. The user's proposal is to track PnL and market-close without placing a
visible SL, so the level is never exposed. This is sound as the *normal* exit mechanism,
but the SL cannot be fully removed (latency, process death, slippage — see the
"Software stops" section above).

### Requirements

1. **Wide structural disaster SL (in the book).** Keep a resting algo SL placed *far* from
   price — beyond the structural invalidation level, not at a round ATR multiple. This
   protects against process death, disconnect, and flash crashes. Because it is wide and
   structural, it is not an attractive hunt target. Ensure the launcher feeds `calculate()`
   real structural levels (`swing_low`/`swing_high`, `vpoc`, `value_area_width`, and
   strategy-provided `sl_level`) rather than falling back to ATR. Audit the `sizing=...`
   lines in `/debug` and the log file — if most trades show `sizing=atr` or
   `sizing=fallback`, the structural levels are not reaching the exit model.

2. **Software stops for the normal exits.** Do **not** attach the normal SL algo order.
   Instead, let the `TradeMgmt` loop market-close when the monitored PnL crosses the exit
   threshold:
   - **Normal loss stop:** in `_check_trade_management()`, when `pnl_pct <= -sl_pct`
     (or `r_multiple <= -1.0`), call `_trade_mgmt_full_close()` instead of relying on a
     resting SL. This is the software-stop loss exit.
   - **Breakeven / trailing / time-stop / partial TP:** already software stops (market
     closes based on monitored state). Keep them.
   - Reuse `_trade_mgmt_full_close()` for all software-stop closes.

3. **Monitoring frequency.** A software stop is only as safe as how often the bot looks.
   The current 10s `_positions_refresh_loop` cadence is workable but not sub-second. Before
   relying on the software stop as the primary loss protection, raise the refresh frequency
   and/or drive position updates from the OKX private WebSocket feed (sub-second) rather
   than the 10s poll. Document the chosen cadence.

4. **Keep the ATR clamp bounds** in `calculate()` (`atr_min_sl_mult`/`atr_max_sl_mult`) but
   prefer structural anchors when available.

- **Acceptance check:** the majority of seeded trades should show `sizing=structural` in
  the audit lines; the disaster SL sits beyond a real invalidation level; and normal exits
  (loss, breakeven, trailing, time-stop) are software stops that market-close via
  `_trade_mgmt_full_close()`.

---

## Fix 5 — Leave Commutator, Alternator, and Shotgun disabled (do not re-enable)

### Problem

These are churn/martingale machines that fight the data:
- **Commutator** reverses a losing position (doubles exposure into the losing side).
- **Alternator** oscillates long/short on thresholds (overtrades; converts a directional
  thesis into churn).
- **Shotgun** closes *all* positions on account-equity thresholds (cannot distinguish good
  from bad positions).

### Requirements

- Keep `enabled: False` for `commutator`, `alternator`, and `shotgun` in the strategy
  config.
- Do not invest further implementation effort in these unless a specific, data-backed
  thesis emerges. The `TradeMgmt` asymmetric exit (Fix 2) supersedes their profit-taking
  role.
- **Acceptance check:** none of these appear in the logs as active (only `TradeMgmt` and
  optionally `Protector`).

---

## Fix 6 — Harden Protector (optional, after Fix 2 is proven)

### Problem

Protector ratchets the SL into profit in steps, but only activates after a large profit %
(`activate_pct` default 10%) and can give back a big peak before the next step locks in.

### Requirements

- Only consider this after Fix 2 is validated. If Protector is re-enabled, lower
  `activate_pct` to align with the trailing activation point and reduce `step_pct` so the
  ratchet locks in profit more frequently.
- Ensure Protector and the `TradeMgmt` trailing stop do not fight each other (they both
  move the SL). Pick one as the owner of the trailing SL — prefer the `TradeMgmt` trailing
  stop from Fix 2.
- **Acceptance check:** no symbol should have both Protector and `TradeMgmt` trailing
  updating its SL concurrently.

---

## Implementation checklist

- [ ] Fix 1: apply `improve_trend_pullback.md` and `improve_liquidity_sweep.md`; re-run
      `performance_summary.py` and confirm the losing strategies' win rates rise.
- [ ] Fix 2: add trailing-stop config keys + logic to `_check_trade_management()`; add
      `_trade_mgmt_trailing_updating` set; remove the fixed-TP cap on the remainder when
      trailing is enabled; add debug lines.
- [ ] Fix 3: disable Skimming (or raise its threshold and drop its fixed SL).
- [ ] Fix 4: implement software stops — keep a wide structural disaster SL in the book,
      market-close normal exits (loss/breakeven/trailing/time-stop) via
      `_trade_mgmt_full_close()`; audit `sizing=` lines so structural levels reach
      `calculate()`; raise monitoring frequency (WebSocket-driven) before relying on the
      software stop as primary loss protection.
- [ ] Fix 5: confirm Commutator/Alternator/Shotgun remain disabled.
- [ ] Fix 6 (optional): align Protector with the trailing stop or leave it disabled.
- [ ] Add/update tests in `tests/` for the trailing-stop logic (ratchet, floor, step gate,
      no-concurrent-update) and the software-stop loss exit (`pnl_pct <= -sl_pct` →
      `_trade_mgmt_full_close`).
- [ ] Re-run the full test suite: `poetry run pytest tests/ -q` (all must pass).
- [ ] Re-run `scripts/performance_summary.py` and confirm avg win / avg loss ratio trends
      toward ≥ 0.9:1 and net PnL improves.

---

## Key invariants — never break these

- **Stop-loss is always required** for entry orders (guardrail). Never remove the SL.
- **Breakeven + partial TP + time-stop** are the sound core of `TradeMgmt`; the trailing
  stop extends them, it does not replace them.
- **The launcher R:R guardrail** (`require_reward_risk_ratio`) is the single owner of the
  R:R decision. Do not bypass it.
- **Ratchet only:** the trailing SL (and Protector) must only ever move in the profitable
  direction, never loosened.
- **No concurrent SL updates** for the same symbol (use the in-flight sets).
- **Trade management is a multiplier on entry quality** — fix entries (Fix 1) before
  expecting exits to show up in PnL.
