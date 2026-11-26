"""Builds the STT -> LLM -> TTS session from settings."""

import logging
import os
from collections.abc import Mapping

from livekit.agents import AgentSession, TurnHandlingOptions, llm

from app.config import Settings

log = logging.getLogger("agent")

DEFAULT_LLM_MODEL = {"anthropic": "claude-haiku-5-5", "openai": "gpt-4o-mini"}
LLM_KEY_ENV = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}


def required_keys(settings: Settings) -> list[str]:
    llm_key = LLM_KEY_ENV.get(settings.agent_llm_provider)
    if llm_key is None:
        raise ValueError(f"AGENT_LLM_PROVIDER must be one of {sorted(LLM_KEY_ENV)}")
    return ["DEEPGRAM_API_KEY", "CARTESIA_API_KEY", llm_key]


def missing_keys(settings: Settings, env: Mapping[str, str] | None = None) -> list[str]:
    env = os.environ if env is None else env
    return [k for k in required_keys(settings) if not env.get(k)]


def resolve_mode(settings: Settings, env: Mapping[str, str] | None = None) -> str:
    """'live' runs the real conversation, 'stub' the Phase 3 placeholder.
    'auto' goes live only when every provider key is present."""
    mode = settings.agent_mode
    if mode not in ("auto", "live", "stub"):
        raise ValueError("AGENT_MODE must be auto, live or stub")
    if mode != "auto":
        return mode
    absent = missing_keys(settings, env)
    if absent:
        log.warning("running the STUB agent: missing %s (set AGENT_MODE=live to make this an error)", absent)
        return "stub"
    return "live"


def build_llm(settings: Settings) -> llm.LLM:
    model = settings.agent_llm_model or DEFAULT_LLM_MODEL[settings.agent_llm_provider]
    if settings.agent_llm_provider == "anthropic":
        from livekit.plugins import anthropic

        return anthropic.LLM(model=model)
    from livekit.plugins import openai

    return openai.LLM(model=model)


def build_session(settings: Settings, vad) -> AgentSession:
    from livekit.plugins import cartesia, deepgram

    tts_kwargs = {"model": settings.agent_tts_model}
    if settings.agent_tts_voice:
        tts_kwargs["voice"] = settings.agent_tts_voice
    return AgentSession(
        stt=deepgram.STT(model=settings.agent_stt_model, language=settings.agent_stt_language),
        llm=build_llm(settings),
        tts=cartesia.TTS(**tts_kwargs),
        vad=vad,
        turn_handling=TurnHandlingOptions(turn_detection="vad"),
        user_away_timeout=settings.agent_user_away_timeout,
    )
