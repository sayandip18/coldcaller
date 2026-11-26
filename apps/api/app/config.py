from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://coldcaller:coldcaller@localhost:5432/coldcaller"
    redis_url: str = "redis://localhost:6379/0"
    session_ttl_hours: int = 168
    session_cookie_name: str = "cc_session"
    cookie_secure: bool = False
    default_timezone: str = "Asia/Kolkata"


@lru_cache
def get_settings() -> Settings:
    return Settings()
