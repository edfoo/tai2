from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

import asyncpg

from app.core.config import get_settings
from app.models.trade import ExecutedTrade

logger = logging.getLogger(__name__)

_POOL: Optional[asyncpg.Pool] = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS executed_trades (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    price NUMERIC NOT NULL,
    amount NUMERIC NOT NULL,
    llm_reasoning TEXT,
    pnl NUMERIC,
    fee NUMERIC
);

CREATE TABLE IF NOT EXISTS prompt_runs (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol TEXT NOT NULL,
    timeframe TEXT,
    model_id TEXT,
    guardrails JSONB,
    payload JSONB NOT NULL,
    decision JSONB,
    notes TEXT,
    prompt_version_id UUID
);

CREATE TABLE IF NOT EXISTS equity_history (
    id BIGSERIAL,
    observed_at TIMESTAMPTZ NOT NULL,
    account_equity NUMERIC,
    total_account_value NUMERIC,
    total_eq_usd NUMERIC,
    PRIMARY KEY (observed_at, id)
);

CREATE TABLE IF NOT EXISTS prompt_versions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    decision_prompt TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

INSERT_SQL = (
    "INSERT INTO executed_trades (id, timestamp, symbol, side, price, amount, llm_reasoning, pnl, fee) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
)

FETCH_RECENT_SQL = (
    "SELECT id, timestamp, symbol, side, price, amount, llm_reasoning, pnl, fee "
    "FROM executed_trades ORDER BY timestamp DESC LIMIT $1"
)

INSERT_PROMPT_SQL = (
    """
    INSERT INTO prompt_runs (
        id,
        symbol,
        timeframe,
        model_id,
        guardrails,
        payload,
        decision,
        notes,
        prompt_version_id
    )
    VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8, $9)
    """
)

INSERT_EQUITY_SQL = (
    """
    INSERT INTO equity_history (observed_at, account_equity, total_account_value, total_eq_usd)
    VALUES ($1, $2, $3, $4)
    """
)

FETCH_EQUITY_SQL = (
    """
    SELECT observed_at, account_equity, total_account_value, total_eq_usd
    FROM equity_history
    ORDER BY observed_at DESC
    LIMIT $1
    """
)

FETCH_PAIRS_SQL = (
    "SELECT symbol, display_name, enabled, source "
    "FROM trading_pairs {where_clause} ORDER BY symbol"
)

FETCH_PROMPTS_SQL = (
    """
    SELECT
        pr.id,
        pr.created_at,
        pr.symbol,
        pr.timeframe,
        pr.model_id,
        pr.guardrails,
        pr.payload,
        pr.decision,
        pr.notes,
        pr.prompt_version_id,
        pv.name AS prompt_version_name
    FROM prompt_runs pr
    LEFT JOIN prompt_versions pv ON pv.id = pr.prompt_version_id
    ORDER BY pr.created_at DESC
    LIMIT $1
    """
)

FETCH_TOTAL_FEES_SQL = "SELECT COALESCE(SUM(fee), 0) AS total_fee FROM executed_trades"
FETCH_WINDOW_FEES_SQL = (
    "SELECT COALESCE(SUM(fee), 0) AS total_fee "
    "FROM executed_trades WHERE timestamp >= NOW() - ($1::double precision * INTERVAL '1 hour')"
)

UPSERT_PAIR_SQL = (
    "INSERT INTO trading_pairs (symbol, enabled, display_name, source, updated_at) "
    "VALUES ($1, TRUE, $2, $3, NOW()) "
    "ON CONFLICT(symbol) DO UPDATE SET enabled=EXCLUDED.enabled, display_name=COALESCE(EXCLUDED.display_name, trading_pairs.display_name), "
    "source=EXCLUDED.source, updated_at=NOW()"
)


async def init_postgres_pool(*, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    global _POOL
    if _POOL:
        return _POOL

    settings = get_settings()
    if not settings.database_url:
        msg = "DATABASE_URL environment variable is required for PostgreSQL"
        raise RuntimeError(msg)

    _POOL = await asyncpg.create_pool(dsn=settings.database_url, min_size=min_size, max_size=max_size)
    await _ensure_timescale_extension(_POOL)
    await _ensure_schema(_POOL)
    await _POOL.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_pairs (
            symbol TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            display_name TEXT,
            source TEXT DEFAULT 'OKX',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    await _POOL.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_settings (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    logger.info("PostgreSQL pool initialized")
    return _POOL


async def get_postgres_pool() -> asyncpg.Pool:
    if _POOL is None:
        await init_postgres_pool()
    assert _POOL is not None  # for type-checkers
    return _POOL


async def close_postgres_pool() -> None:
    global _POOL
    if _POOL is not None:
        await _POOL.close()
        _POOL = None
        logger.info("PostgreSQL pool closed")


async def insert_executed_trade(trade: ExecutedTrade) -> None:
    pool = await get_postgres_pool()
    await pool.execute(
        INSERT_SQL,
        trade.id,
        trade.timestamp,
        trade.symbol,
        trade.side,
        trade.price,
        trade.amount,
        trade.llm_reasoning,
        trade.pnl,
        trade.fee,
    )


async def fetch_recent_trades(limit: int = 200) -> list[dict[str, Any]]:
    pool = await get_postgres_pool()
    records = await pool.fetch(FETCH_RECENT_SQL, limit)
    results: list[dict[str, Any]] = []
    for row in records:
        results.append(
            {
                "id": str(row["id"]),
                "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                "symbol": row["symbol"],
                "side": row["side"],
                "price": float(row["price"]),
                "amount": float(row["amount"]),
                "pnl": float(row["pnl"]) if row["pnl"] is not None else None,
                "llm_reasoning": row["llm_reasoning"],
                "fee": float(row["fee"]) if row["fee"] is not None else None,
            }
        )
    return results


async def fetch_total_okx_fees() -> float:
    pool = await get_postgres_pool()
    row = await pool.fetchrow(FETCH_TOTAL_FEES_SQL)
    if not row:
        return 0.0
    value = row.get("total_fee") if isinstance(row, dict) else row[0]
    try:
        return abs(float(value)) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


async def fetch_okx_fees_window(hours: float = 24.0) -> float:
    pool = await get_postgres_pool()
    window = max(1.0, float(hours or 0.0))
    row = await pool.fetchrow(FETCH_WINDOW_FEES_SQL, window)
    if not row:
        return 0.0
    value = row.get("total_fee") if isinstance(row, dict) else row[0]
    try:
        return abs(float(value)) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


async def fetch_trading_pairs(*, enabled_only: bool = True) -> list[dict[str, Any]]:
    pool = await get_postgres_pool()
    where_clause = "WHERE enabled = TRUE" if enabled_only else ""
    query = FETCH_PAIRS_SQL.format(where_clause=where_clause)
    records = await pool.fetch(query)
    return [
        {
            "symbol": row["symbol"],
            "display_name": row["display_name"] or row["symbol"],
            "enabled": row["enabled"],
            "source": row["source"],
        }
        for row in records
    ]


async def set_enabled_trading_pairs(symbols: list[str], *, source: str = "OKX") -> None:
    pool = await get_postgres_pool()
    normalized = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    async with pool.acquire() as conn:
        async with conn.transaction():
            if normalized:
                args = [(symbol, symbol, source) for symbol in normalized]
                await conn.executemany(UPSERT_PAIR_SQL, args)
                await conn.execute(
                    "UPDATE trading_pairs SET enabled=FALSE, updated_at=NOW() WHERE symbol <> ALL($1::text[])",
                    normalized,
                )
            else:
                await conn.execute("UPDATE trading_pairs SET enabled=FALSE, updated_at=NOW()")


def _coerce_numeric(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _serialize_json(value: Any) -> str:
    try:
        return json.dumps(value or {})
    except (TypeError, ValueError):
        return json.dumps({"error": "serialization_failed"})


async def insert_prompt_run(
    *,
    symbol: str,
    timeframe: str | None,
    model_id: str | None,
    guardrails: dict[str, Any] | None,
    payload: dict[str, Any],
    notes: str | None = None,
    decision: dict[str, Any] | None = None,
    prompt_version_id: str | None = None,
) -> str:
    pool = await get_postgres_pool()
    record_id = uuid4()
    await pool.execute(
        INSERT_PROMPT_SQL,
        record_id,
        symbol,
        timeframe,
        model_id,
        _serialize_json(guardrails),
        _serialize_json(payload),
        _serialize_json(decision),
        notes,
        prompt_version_id,
    )
    return str(record_id)


async def insert_equity_point(
    *,
    observed_at: datetime | None = None,
    account_equity: Any = None,
    total_account_value: Any = None,
    total_eq_usd: Any = None,
) -> None:
    pool = await get_postgres_pool()
    await pool.execute(
        INSERT_EQUITY_SQL,
        (observed_at or datetime.now(timezone.utc)),
        _coerce_numeric(account_equity),
        _coerce_numeric(total_account_value),
        _coerce_numeric(total_eq_usd),
    )


async def fetch_equity_history(limit: int = 200) -> list[dict[str, Any]]:
    pool = await get_postgres_pool()
    records = await pool.fetch(FETCH_EQUITY_SQL, limit)
    items: list[dict[str, Any]] = []
    for row in records:
        observed = row["observed_at"]
        items.append(
            {
                "observed_at": observed.isoformat() if observed else None,
                "account_equity": float(row["account_equity"]) if row["account_equity"] is not None else None,
                "total_account_value": float(row["total_account_value"]) if row["total_account_value"] is not None else None,
                "total_eq_usd": float(row["total_eq_usd"]) if row["total_eq_usd"] is not None else None,
            }
        )
    return list(reversed(items))


async def fetch_prompt_runs(limit: int = 100) -> list[dict[str, Any]]:
    pool = await get_postgres_pool()
    records = await pool.fetch(FETCH_PROMPTS_SQL, limit)
    results: list[dict[str, Any]] = []
    for row in records:
        guardrails = json.loads(row["guardrails"] or "{}")
        payload = json.loads(row["payload"] or "{}")
        decision = json.loads(row["decision"] or "{}")
        created = row["created_at"]
        results.append(
            {
                "id": str(row["id"]),
                "created_at": created.isoformat() if created else None,
                "symbol": row["symbol"],
                "timeframe": row["timeframe"],
                "model_id": row["model_id"],
                "guardrails": guardrails,
                "payload": payload,
                "decision": decision,
                "notes": row["notes"],
                "prompt_version_id": str(row["prompt_version_id"]) if row["prompt_version_id"] else None,
                "prompt_version_name": row["prompt_version_name"],
            }
        )
    return results


async def insert_prompt_version(
    *,
    name: str,
    system_prompt: str,
    decision_prompt: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    pool = await get_postgres_pool()
    record_id = uuid4()
    await pool.execute(
        """
        INSERT INTO prompt_versions (id, name, system_prompt, decision_prompt, metadata)
        VALUES ($1, $2, $3, $4, $5::jsonb)
        """,
        record_id,
        name,
        system_prompt,
        decision_prompt,
        _serialize_json(metadata),
    )
    return str(record_id)


async def fetch_prompt_versions(limit: int = 50) -> list[dict[str, Any]]:
    pool = await get_postgres_pool()
    records = await pool.fetch(
        """
        SELECT id, name, system_prompt, decision_prompt, metadata, created_at
        FROM prompt_versions
        ORDER BY created_at DESC
        LIMIT $1
        """,
        limit,
    )
    results: list[dict[str, Any]] = []
    for row in records:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        results.append(
            {
                "id": str(row["id"]),
                "name": row["name"],
                "system_prompt": row["system_prompt"],
                "decision_prompt": row["decision_prompt"],
                "metadata": metadata or {},
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
        )
    return results


async def save_guardrails(config: dict[str, Any]) -> None:
    pool = await get_postgres_pool()
    await pool.execute(
        """
        INSERT INTO runtime_settings (key, value, updated_at)
        VALUES ('guardrails', $1::jsonb, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        json.dumps(config or {}),
    )


async def load_guardrails() -> dict[str, Any]:
    pool = await get_postgres_pool()
    row = await pool.fetchrow("SELECT value FROM runtime_settings WHERE key = 'guardrails'")
    if not row:
        return {}
    value = row["value"]
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    if isinstance(value, dict):
        return value
    return {}


async def save_llm_model(model_id: str | None) -> None:
    pool = await get_postgres_pool()
    payload = json.dumps({"model_id": model_id} if model_id else {})
    await pool.execute(
        """
        INSERT INTO runtime_settings (key, value, updated_at)
        VALUES ('llm_model', $1::jsonb, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        payload,
    )


async def load_llm_model(default: str | None = None) -> str | None:
    pool = await get_postgres_pool()
    row = await pool.fetchrow("SELECT value FROM runtime_settings WHERE key = 'llm_model'")
    if not row:
        return default
    value = row["value"]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value or default
    else:
        parsed = value
    if isinstance(parsed, dict):
        candidate = parsed.get("model_id") or parsed.get("value") or parsed.get("id")
        if candidate:
            return str(candidate)
    if isinstance(parsed, str) and parsed:
        return parsed
    return default


async def save_okx_sub_account(
    sub_account: str | None,
    use_master: bool = False,
    api_flag: str | None = None,
) -> None:
    pool = await get_postgres_pool()
    normalized_flag = str(api_flag).strip() if api_flag is not None else None
    if normalized_flag not in {"0", "1"}:
        normalized_flag = None
    payload = json.dumps(
        {
            "sub_account": sub_account,
            "use_master": bool(use_master),
            "api_flag": normalized_flag,
        }
    )
    await pool.execute(
        """
        INSERT INTO runtime_settings (key, value, updated_at)
        VALUES ('okx_sub_account', $1::jsonb, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        payload,
    )


async def load_okx_sub_account(
    default: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    pool = await get_postgres_pool()
    row = await pool.fetchrow("SELECT value FROM runtime_settings WHERE key = 'okx_sub_account'")
    base: dict[str, Any] = {
        "sub_account": None,
        "use_master": False,
        "api_flag": "0",
    }
    if isinstance(default, dict):
        base["sub_account"] = (default.get("sub_account") or default.get("value") or "").strip() or None
        base["use_master"] = bool(default.get("use_master"))
        candidate_flag = str(default.get("api_flag")) if default.get("api_flag") is not None else None
        if candidate_flag in {"0", "1"}:
            base["api_flag"] = candidate_flag
    elif isinstance(default, str):
        base["sub_account"] = default.strip() or None
    if not row:
        return base
    value = row["value"]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = {"sub_account": value}
    else:
        parsed = value
    if isinstance(parsed, dict):
        candidate = parsed.get("sub_account") or parsed.get("value")
        if candidate is not None:
            base["sub_account"] = str(candidate).strip() or None
        if "use_master" in parsed:
            base["use_master"] = bool(parsed.get("use_master"))
        if "api_flag" in parsed:
            candidate_flag = str(parsed.get("api_flag") or "").strip()
            if candidate_flag in {"0", "1"}:
                base["api_flag"] = candidate_flag
    elif isinstance(parsed, str) and parsed:
        base["sub_account"] = parsed.strip() or base["sub_account"]
    return base


async def save_execution_settings(config: dict[str, Any]) -> None:
    pool = await get_postgres_pool()
    min_sizes = config.get("min_sizes")
    if isinstance(min_sizes, dict):
        normalized_min_sizes = {}
        for key, value in min_sizes.items():
            try:
                normalized_value = float(value)
            except (TypeError, ValueError):
                continue
            if normalized_value <= 0:
                continue
            normalized_min_sizes[str(key).upper()] = normalized_value
    else:
        normalized_min_sizes = None
    payload = json.dumps(
        {
            "enabled": bool(config.get("enabled")),
            "trade_mode": (config.get("trade_mode") or "cross").lower(),
            "order_type": (config.get("order_type") or "market").lower(),
            "min_size": float(config.get("min_size") or 0.0),
            "min_sizes": normalized_min_sizes,
        }
    )
    await pool.execute(
        """
        INSERT INTO runtime_settings (key, value, updated_at)
        VALUES ('execution', $1::jsonb, NOW())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        payload,
    )


async def load_execution_settings() -> dict[str, Any]:
    pool = await get_postgres_pool()
    row = await pool.fetchrow("SELECT value FROM runtime_settings WHERE key = 'execution'")
    if not row:
        return {}
    value = row["value"]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
    else:
        parsed = value or {}
    if not isinstance(parsed, dict):
        return {}
    result: dict[str, Any] = {}
    if "enabled" in parsed:
        result["enabled"] = bool(parsed["enabled"])
    if "trade_mode" in parsed:
        result["trade_mode"] = str(parsed["trade_mode"] or "cross")
    if "order_type" in parsed:
        result["order_type"] = str(parsed["order_type"] or "market")
    if "min_size" in parsed:
        try:
            result["min_size"] = float(parsed["min_size"])
        except (TypeError, ValueError):
            pass
    min_sizes = parsed.get("min_sizes")
    if isinstance(min_sizes, dict):
        normalized: dict[str, float] = {}
        for key, value in min_sizes.items():
            try:
                normalized_value = float(value)
            except (TypeError, ValueError):
                continue
            if normalized_value <= 0:
                continue
            normalized[str(key).upper()] = normalized_value
        if normalized:
            result["min_sizes"] = normalized
    return result


async def get_prompt_version(version_id: str) -> dict[str, Any] | None:
    pool = await get_postgres_pool()
    row = await pool.fetchrow(
        """
        SELECT id, name, system_prompt, decision_prompt, metadata, created_at
        FROM prompt_versions
        WHERE id = $1
        """,
        version_id,
    )
    if not row:
        return None
    metadata = row["metadata"]
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "system_prompt": row["system_prompt"],
        "decision_prompt": row["decision_prompt"],
        "metadata": metadata or {},
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }


async def _ensure_schema(pool: asyncpg.Pool) -> None:
    await pool.execute(SCHEMA_SQL)
    await pool.execute(
        """
        ALTER TABLE executed_trades
        ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS symbol TEXT,
        ADD COLUMN IF NOT EXISTS side TEXT,
        ADD COLUMN IF NOT EXISTS price NUMERIC,
        ADD COLUMN IF NOT EXISTS amount NUMERIC,
        ADD COLUMN IF NOT EXISTS llm_reasoning TEXT,
        ADD COLUMN IF NOT EXISTS pnl NUMERIC,
        ADD COLUMN IF NOT EXISTS fee NUMERIC
        """
    )
    await _ensure_equity_primary_key(pool)
    await _ensure_equity_hypertable(pool)
    await pool.execute(
        "ALTER TABLE prompt_runs ADD COLUMN IF NOT EXISTS prompt_version_id UUID"
    )


async def _ensure_timescale_extension(pool: asyncpg.Pool) -> None:
    try:
        await pool.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    except asyncpg.PostgresError as exc:  # pragma: no cover - depends on DB perms
        logger.warning("TimescaleDB extension unavailable: %s", exc)


async def _ensure_equity_hypertable(pool: asyncpg.Pool) -> None:
    try:
        await pool.execute(
            "SELECT create_hypertable('equity_history', 'observed_at', if_not_exists => TRUE);"
        )
    except asyncpg.PostgresError as exc:  # pragma: no cover - depends on extension support
        logger.warning("Unable to convert equity_history to hypertable: %s", exc)


async def _ensure_equity_primary_key(pool: asyncpg.Pool) -> None:
    try:
        await pool.execute("ALTER TABLE equity_history DROP CONSTRAINT IF EXISTS equity_history_pkey;")
        await pool.execute("ALTER TABLE equity_history ADD PRIMARY KEY (observed_at, id);")
    except asyncpg.PostgresError as exc:  # pragma: no cover - depends on perms/data
        logger.warning("Unable to enforce equity_history primary key: %s", exc)


__all__ = [
    "close_postgres_pool",
    "fetch_prompt_runs",
    "fetch_prompt_versions",
    "fetch_equity_history",
    "fetch_total_okx_fees",
    "fetch_okx_fees_window",
    "fetch_recent_trades",
    "fetch_trading_pairs",
    "get_postgres_pool",
    "get_prompt_version",
    "init_postgres_pool",
    "load_guardrails",
    "load_llm_model",
    "load_okx_sub_account",
    "insert_equity_point",
    "insert_executed_trade",
    "insert_prompt_run",
    "insert_prompt_version",
    "save_okx_sub_account",
    "save_llm_model",
    "save_guardrails",
    "set_enabled_trading_pairs",
]
