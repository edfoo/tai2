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


__all__ = ["ExecutedTrade"]
