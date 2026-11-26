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
    # Promotional calls must not reach numbers on the national do-not-disturb register.
    ncpr_enforce: bool = True

    # --- dialing limits (carrier-wide) ---
    carrier_cps: int = 5  # new calls per second across all campaigns
    max_global_channels: int = 20  # concurrent calls across all campaigns
    max_call_seconds: int = 600  # hard cap; also the channel lease length
    dial_max_attempts: int = 3

    # --- outbound webhooks ---
    webhook_signing_secret: str = "dev-webhook-secret-change-me"
    webhook_allow_private: bool = False  # True only in dev: lets webhooks target localhost/LAN
    webhook_max_attempts: int = 8
    webhook_timeout_s: float = 10.0

    # --- alerts ---
    sns_topic_arn: str = ""  # empty = log campaign-finished alerts instead of publishing

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
    # After the customer answers, how long to wait for the agent to join before hanging up.
    agent_join_timeout: float = 10.0
    # Shared secret the voice agent presents when calling the API back. Override outside dev.
    agent_shared_secret: str = "dev-agent-secret-change-me"
    # Where the agent reaches the API (it runs in a container, so not localhost)
    agent_api_url: str = "http://host.docker.internal:8000/api"
    agent_stub_seconds: float = 5.0  # stub mode: how long to "talk"

    # --- voice agent (Phase 4). Provider API keys are read from the environment by the
    # LiveKit plugins: DEEPGRAM_API_KEY, CARTESIA_API_KEY, ANTHROPIC_API_KEY / OPENAI_API_KEY.
    agent_mode: str = "auto"  # auto (live if keys present, else stub) | live | stub
    agent_llm_provider: str = "openai"  # openai | anthropic
    agent_llm_model: str = ""  # empty = provider default below
    agent_stt_model: str = "nova-3"
    agent_stt_language: str = "en-IN"
    agent_tts_model: str = "sonic-3"
    agent_tts_voice: str = ""  # Cartesia voice id; empty = plugin default
    agent_max_call_seconds: int = 300  # polite hard stop (must be < max_call_seconds)
    agent_user_away_timeout: float = 12.0  # silence before "are you still there?"

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
