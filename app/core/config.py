from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    okx_api_key: Optional[str] = Field(default=None, alias="OKX_API_KEY")
    okx_secret_key: Optional[str] = Field(default=None, alias="OKX_SECRET_KEY")
    okx_passphrase: Optional[str] = Field(default=None, alias="OKX_PASSPHRASE")
    openrouter_api_key: Optional[str] = Field(default=None, alias="OPENROUTER_API_KEY")
    database_url: Optional[str] = Field(default=None, alias="DATABASE_URL")
    redis_url: Optional[str] = Field(default=None, alias="REDIS_URL")
    ws_update_interval: int = Field(default=180, alias="WS_UPDATE_INTERVAL", ge=1)
    trading_pairs_raw: str = Field(default="BTC-USDT-SWAP", alias="TRADING_PAIRS")

    @property
    def trading_pairs(self) -> list[str]:
        raw = self.trading_pairs_raw
        if isinstance(raw, str):
            pairs = [item.strip().upper() for item in raw.split(",") if item.strip()]
        elif isinstance(raw, (list, tuple)):
            pairs = [str(item).strip().upper() for item in raw if str(item).strip()]
        else:
            pairs = []
        return pairs or ["BTC-USDT-SWAP"]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
