"""
Configuration management for the backend runtime.

This module belongs to the infrastructure/config layer and exposes a
``Settings`` object backed by environment variables for database connections,
API keys, and runtime flags. ``get_settings`` is imported by API modules,
ingestion jobs, and database utilities to ensure consistent configuration.
"""

import os
from functools import lru_cache
from typing import Optional

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """Pydantic model capturing environment-driven configuration values."""
    app_name: str = Field("Economy Analytics API", env="APP_NAME")
    env: str = Field("dev", env="ENV")
    debug: bool = Field(False, env="DEBUG")

    database_url: str = Field(..., env="DATABASE_URL")
    redis_url: Optional[str] = Field(None, env="REDIS_URL")

    fred_api_key: str = Field(..., env="FRED_API_KEY")
    eia_api_key: str = Field(..., env="EIA_API_KEY")
    comtrade_api_key: str = Field(..., env="COMTRADE_API_KEY")
    aisstream_api_key: str = Field(..., env="AISSTREAM_API_KEY")

    @validator("env")
    def validate_env(cls, value: str) -> str:
        allowed = {"dev", "staging", "prod"}
        if value not in allowed:
            raise ValueError(f"ENV must be one of {allowed}")
        return value

    @validator("database_url", pre=True)
    def validate_database_url(cls, value: Optional[str]) -> str:
        if value:
            return value

        legacy = os.getenv("POSTGRES_URL")
        if legacy:
            return legacy

        raise ValueError("DATABASE_URL must be configured for database access.")

    @property
    def postgres_url(self) -> str:
        """Backward-compatible alias for the primary database URL."""

        return self.database_url

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
