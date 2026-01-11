from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class ExecutedTrade(BaseModel):
    """Canonical representation for persisted trades."""

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    symbol: str
    instrument: str | None = None
    size: Decimal | None = None
    side: Literal["buy", "sell"]
    price: Decimal
    amount: Decimal
    llm_reasoning: Optional[str] = None
    pnl: Optional[Decimal] = None
    fee: Optional[Decimal] = None

    @field_validator("symbol", mode="before")
    def normalize_symbol(cls, value: str) -> str:  # noqa: N805
        if not isinstance(value, str) or not value.strip():
            msg = "Symbol must be provided"
            raise ValueError(msg)
        return value.strip().upper()

    @field_validator("instrument", mode="before")
    def normalize_instrument(cls, value: str | None, info):  # noqa: N805
        candidate = value
        if candidate is None:
            candidate = info.data.get("symbol")
        if candidate is None:
            return None
        candidate = str(candidate).strip().upper()
        return candidate or None

    @field_validator("size", mode="before")
    def default_size(cls, value: Decimal | None, info):  # noqa: N805
        if value is not None:
            return value
        amount = info.data.get("amount")
        return amount


__all__ = ["ExecutedTrade"]
