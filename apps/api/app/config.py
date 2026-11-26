from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://coldcaller:coldcaller@localhost:5432/coldcaller"
    redis_url: str = "redis://localhost:6380/0"
    session_ttl_hours: int = 168
    session_cookie_name: str = "cc_session"
    cookie_secure: bool = False
    default_timezone: str = "Asia/Kolkata"

    # --- dialing limits (carrier-wide) ---
    carrier_cps: int = 5  # new calls per second across all campaigns
    max_global_channels: int = 20  # concurrent calls across all campaigns
    max_call_seconds: int = 600  # hard cap; also the channel lease length
    dial_max_attempts: int = 3

    # --- worker ---
    dialer: str = "fake"  # fake | livekit (Phase 3)
    worker_sweep_interval: float = 15.0
    worker_max_inflight: int = 200
    default_call_window_start: str = "09:00"
    default_call_window_end: str = "21:00"

    # --- fake dialer simulation ---
    fake_ring_min: float = 1.0
    fake_ring_max: float = 3.0
    fake_talk_min: float = 3.0
    fake_talk_max: float = 10.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
