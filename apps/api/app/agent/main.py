"""Voice agent worker.

Phase 3 ships a *stub* conversation: the agent joins the call's room, fetches the call
context from the API, "talks" for a few seconds (or until the customer hangs up), reports an
outcome and ends the call. That proves the whole dial -> answer -> dispatch -> hang-up loop.
Phase 4 replaces `converse()` with the real STT -> LLM -> TTS pipeline.

Run:  python -m app.agent.main dev    (or `start` in a container)
"""

import asyncio
import json
import logging
import os

from livekit import agents

from app.agent.api_client import AgentApi
from app.config import get_settings

log = logging.getLogger("agent")
settings = get_settings()
server = agents.AgentServer()


async def converse(ctx: agents.JobContext, call: dict, customer_left: asyncio.Event) -> tuple[str, str]:
    """Return (result, transcript). Stub for Phase 3."""
    try:
        await asyncio.wait_for(customer_left.wait(), timeout=settings.agent_stub_seconds)
        return "customer_hung_up", "[stub conversation: customer hung up]"
    except TimeoutError:
        return "stub_completed", f"[stub conversation with {call.get('contact_name') or 'customer'}]"


@server.rtc_session(agent_name=settings.livekit_agent_name)
async def entrypoint(ctx: agents.JobContext) -> None:
    call_id = json.loads(ctx.job.metadata or "{}").get("call_id")
    if not call_id:
        log.error("job without call_id metadata; leaving")
        return
    api = AgentApi()
    try:
        await ctx.connect()
        call = await api.get_call(call_id)
        log.info("agent joined %s for call %s (%s)", ctx.room.name, call_id, call["to_number"])

        customer_left = asyncio.Event()

        @ctx.room.on("participant_disconnected")
        def _on_left(participant) -> None:
            if participant.identity.startswith("sip-"):
                customer_left.set()

        result, transcript = await converse(ctx, call, customer_left)
        await api.report_outcome(call_id, result=result, transcript=transcript)
        await ctx.delete_room()  # hangs up the SIP leg if the customer is still on the line
    finally:
        await api.aclose()


def main() -> None:
    # The LiveKit CLI reads its connection settings from the environment.
    os.environ.setdefault("LIVEKIT_URL", settings.livekit_url)
    os.environ.setdefault("LIVEKIT_API_KEY", settings.livekit_api_key)
    os.environ.setdefault("LIVEKIT_API_SECRET", settings.livekit_api_secret)
    agents.cli.run_app(server)


if __name__ == "__main__":
    main()
