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

    # --- LiveKit (Phase 3) ---
    livekit_url: str = "ws://localhost:7880"
    livekit_api_key: str = "devkey"
    livekit_api_secret: str = "devsecret-devsecret-devsecret-devsecret"
    livekit_sip_trunk_id: str = ""
    livekit_agent_name: str = "coldcaller-agent"
    livekit_ring_timeout: int = 40  # seconds the customer's phone may ring
    # Shared secret the voice agent presents when calling the API back. Override outside dev.
    agent_shared_secret: str = "dev-agent-secret-change-me"
    # Where the agent reaches the API (it runs in a container, so not localhost)
    agent_api_url: str = "http://host.docker.internal:8000/api"
    agent_stub_seconds: float = 5.0  # Phase 3 stub agent: how long to "talk"

    # --- fake dialer simulation ---
    fake_ring_min: float = 1.0
    fake_ring_max: float = 3.0
    fake_talk_min: float = 3.0
    fake_talk_max: float = 10.0


    @property
    def livekit_http_url(self) -> str:
        return self.livekit_url.replace("wss://", "https://").replace("ws://", "http://")


@lru_cache
def get_settings() -> Settings:
    return Settings()
