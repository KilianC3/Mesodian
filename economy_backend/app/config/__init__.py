from functools import lru_cache
from typing import Optional

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    app_name: str = Field("Economy Analytics API", env="APP_NAME")
    env: str = Field("dev", env="ENV")
    debug: bool = Field(False, env="DEBUG")

    postgres_url: str = Field(..., env="POSTGRES_URL")
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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
