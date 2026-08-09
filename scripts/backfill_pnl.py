#!/usr/bin/env python3
"""One-off back-fill of realized PnL for trades that were missed by the reconciler.

Background
----------
The fill reconciler (``MarketService._reconcile_fills``) back-fills realized PnL
onto locally recorded entry rows by matching OKX closing fills (``pnl != 0``) to
the most recent unreconciled entry for the same symbol. It treats
``okx_fill_id IS NULL`` as the "not yet reconciled" marker.

A previous bug made the *entry-fee* pass stamp ``okx_fill_id`` onto entry rows
*before* the position closed. Once set, those rows were permanently excluded from
PnL reconciliation (``FETCH_UNRECONCILED_SQL`` filters on ``okx_fill_id IS NULL``),
so their ``pnl`` stayed NULL even after the position closed.

This script repairs those rows by:
  1. Loading every ``executed_trades`` row where ``pnl IS NULL`` (the affected set).
  2. Paginating OKX ``fills-history`` (max 90-day lookback) to collect closing fills.
  3. For each closing fill, matching the most recent unmatched entry row for the
     same symbol with the opposite side and an earlier timestamp, then back-filling
     ``pnl``, ``fee`` and ``okx_fill_id``.

Usage::

    python scripts/backfill_pnl.py --dry-run     # preview what would change
    python scripts/backfill_pnl.py               # apply the back-fill
    python scripts/backfill_pnl.py --max-pages 50

Notes
-----
- Read-only against the DB except for the final ``UPDATE`` on matched rows.
- ``--dry-run`` performs no writes.
- Requires OKX credentials in ``.env`` (same as the bot) and a reachable DB.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.config import get_settings  # noqa: E402
from app.db.postgres import (  # noqa: E402
    close_postgres_pool,
    get_postgres_pool,
    init_postgres_pool,
)
from app.services.okx_sdk_adapter import OkxTradeAdapter  # noqa: E402

try:  # noqa: E402
    import okx.Trade as OkxTrade
except Exception:  # pragma: no cover - SDK optional
    OkxTrade = None

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("backfill_pnl")

FETCH_AFFECTED_SQL = """
    SELECT id, timestamp, symbol, side
    FROM executed_trades
    WHERE pnl IS NULL
    ORDER BY timestamp ASC
"""

UPDATE_PNL_SQL = """
    UPDATE executed_trades
    SET pnl = $1,
        fee = COALESCE($2, fee),
        okx_fill_id = $3,
        fee_paid_at = COALESCE(fee_paid_at, NOW())
    WHERE id = $4
"""


def _safe_data(response: Any) -> list[Any]:
    """Normalize OKX responses into list form (mirrors MarketService._safe_data)."""
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, list):
            return data
    if isinstance(response, list):
        return response
    return []


def _fill_ts_ms(fill: dict[str, Any]) -> int:
    """Return the fill timestamp in epoch milliseconds (0 if unknown)."""
    raw = fill.get("ts") or fill.get("fillTime") or 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _build_trade_api() -> OkxTradeAdapter | None:
    """Build the same TradeAPI adapter the bot uses (with sub-account routing)."""
    if OkxTrade is None:
        logger.error("okx SDK not installed; cannot fetch fills")
        return None
    settings = get_settings()
    if not (settings.okx_api_key and settings.okx_secret_key and settings.okx_passphrase):
        logger.error("OKX credentials missing in .env")
        return None
    flag = "1" if str(settings.okx_api_flag).strip() == "1" else "0"
    raw_api = OkxTrade.TradeAPI(
        api_key=settings.okx_api_key,
        api_secret_key=settings.okx_secret_key,
        passphrase=settings.okx_passphrase,
        flag=flag,
    )
    return OkxTradeAdapter(raw_api)


async def fetch_closing_fills(
    trade_api: OkxTradeAdapter,
    sub_acct: str | None,
    oldest_ts_ms: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    """Paginate fills-history backwards, collecting closing fills (pnl != 0).

    Stops when a page is empty, pagination stalls, or we've gone back before
    ``oldest_ts_ms`` (the oldest affected entry row).
    """
    closing: list[dict[str, Any]] = []
    after = ""
    for _ in range(max_pages):
        resp = await asyncio.to_thread(
            trade_api.get_fills_history,
            inst_type="SWAP",
            after=after,
            limit=100,
            sub_acct=sub_acct,
        )
        fills = _safe_data(resp)
        if not fills:
            break

        page_oldest = _fill_ts_ms(fills[-1])
        for fill in fills:
            raw_pnl = fill.get("pnl") or fill.get("fillPnl") or "0"
            try:
                pnl = float(raw_pnl)
            except (TypeError, ValueError):
                continue
            if pnl == 0.0:
                # Entry fills carry no realized PnL; skip.
                continue
            closing.append(fill)

        # Advance the cursor to the oldest fill of this page (older fills).
        next_after = str(fills[-1].get("billId") or fills[-1].get("fillId") or "")
        if not next_after or next_after == after:
            break
        after = next_after

        # Stop once we've gone back before the oldest affected entry row.
        if oldest_ts_ms and page_oldest and page_oldest < oldest_ts_ms:
            break

    return closing


async def run(dry_run: bool, max_pages: int) -> None:
    settings = get_settings()
    if not settings.database_url:
        logger.error("DATABASE_URL not set; aborting")
        return

    await init_postgres_pool()
    pool = await get_postgres_pool()

    # 1. Load all rows missing PnL.
    rows = await pool.fetch(FETCH_AFFECTED_SQL)
    if not rows:
        logger.info("No rows with pnl IS NULL — nothing to do.")
        await close_postgres_pool()
        return
    logger.info("Found %d row(s) with pnl IS NULL", len(rows))

    # Group affected entries by (symbol, side) for matching.
    # side here is the *entry* side.
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]).upper(), []).append(dict(row))
    for entries in by_symbol.values():
        entries.sort(key=lambda e: e["timestamp"])

    oldest_ts_ms = int(min(r["timestamp"].timestamp() * 1000 for r in rows))

    # 2. Build the OKX trade API and fetch closing fills.
    trade_api = _build_trade_api()
    if trade_api is None:
        await close_postgres_pool()
        return
    sub_acct = settings.okx_sub_account if settings.okx_sub_account_use_master else None
    logger.info("Fetching closing fills from OKX (sub_acct=%s) ...", sub_acct or "master")
    closing_fills = await fetch_closing_fills(trade_api, sub_acct, oldest_ts_ms, max_pages)
    logger.info("Collected %d closing fill(s) with pnl != 0", len(closing_fills))

    # Sort oldest-first so each closing fill matches the earliest open entry.
    closing_fills.sort(key=_fill_ts_ms)

    # 3. Match closing fills to affected entry rows.
    matched = 0
    skipped_no_match = 0
    for fill in closing_fills:
        inst_id = str(fill.get("instId") or "").upper()
        fill_side = str(fill.get("side") or "").lower()  # closing side
        if not inst_id or not fill_side:
            continue
        # Entry side is the opposite of the closing side.
        entry_side = "sell" if fill_side == "buy" else "buy"
        fill_ts_ms = _fill_ts_ms(fill)
        fill_dt = datetime.fromtimestamp(fill_ts_ms / 1000, tz=timezone.utc) if fill_ts_ms else None

        entries = by_symbol.get(inst_id, [])
        # Find the most recent unmatched entry with the opposite side and an
        # earlier timestamp than this closing fill.
        target = None
        for entry in entries:
            if entry.get("_matched"):
                continue
            if entry["side"] != entry_side:
                continue
            if fill_dt is not None and entry["timestamp"] >= fill_dt:
                continue
            target = entry  # entries sorted ascending → last valid is most recent
        if target is None:
            skipped_no_match += 1
            continue

        raw_pnl = fill.get("pnl") or fill.get("fillPnl") or "0"
        try:
            pnl_value = float(raw_pnl)
        except (TypeError, ValueError):
            continue
        raw_fee = fill.get("fee") or fill.get("fillFee") or None
        try:
            fee_value = abs(float(raw_fee)) if raw_fee is not None else None
        except (TypeError, ValueError):
            fee_value = None
        fill_id = str(fill.get("fillId") or fill.get("tradeId") or "")

        target["_matched"] = True
        matched += 1
        if dry_run:
            logger.info(
                "[dry-run] would set pnl=%+.4f fee=%s fill=%s on trade %s (%s)",
                pnl_value,
                f"{fee_value:.4f}" if fee_value is not None else "None",
                fill_id,
                target["id"],
                inst_id,
            )
            continue

        await pool.execute(
            UPDATE_PNL_SQL,
            pnl_value,
            fee_value,
            fill_id or None,
            target["id"],
        )
        logger.info(
            "Back-filled pnl=%+.4f fee=%s fill=%s on trade %s (%s)",
            pnl_value,
            f"{fee_value:.4f}" if fee_value is not None else "None",
            fill_id,
            target["id"],
            inst_id,
        )

    logger.info(
        "Done: matched %d row(s), %d closing fill(s) had no matching entry.",
        matched,
        skipped_no_match,
    )
    await close_postgres_pool()


def main() -> None:
    parser = argparse.ArgumentParser(description="Back-fill realized PnL from OKX fills.")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing.")
    parser.add_argument("--max-pages", type=int, default=100, help="Max fills-history pages to fetch.")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, max_pages=args.max_pages))


if __name__ == "__main__":
    main()
