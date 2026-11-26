"""Voice agent worker.

Two modes (see AGENT_MODE):
  live  the real conversation: Deepgram STT -> LLM -> Cartesia TTS
  stub  joins, waits a few seconds, reports an outcome, hangs up (no provider keys needed)
'auto' (default) uses live when every provider key is present, else stub.

Run:  python -m app.agent.main dev    (or `start` in a container)
"""

import asyncio
import json
import logging
import os

from livekit import agents
from livekit.agents import JobProcess
from livekit.agents.voice.room_io import RoomOptions

from app.agent.api_client import AgentApi
from app.agent.caller import CallerAgent, format_transcript
from app.agent.controller import CallController
from app.agent.factory import build_session, missing_keys, resolve_mode
from app.agent.prompt import build_instructions
from app.config import get_settings

log = logging.getLogger("agent")
settings = get_settings()

GREETING = (
    "Greet the customer now, following your opening rules: say hello, say you are an automated "
    "AI assistant, name the company and the purpose of the call, and ask if it is a good time."
)


def prewarm(proc: JobProcess) -> None:
    if resolve_mode(settings) != "live":
        return  # the stub never listens, so don't spend CPU and memory loading the voice model
    from livekit.plugins import silero

    proc.userdata["vad"] = silero.VAD.load()


server = agents.AgentServer(setup_fnc=prewarm, num_idle_processes=settings.agent_idle_processes)


async def run_stub(ctx: agents.JobContext, call_id: str, api: AgentApi, call: dict) -> None:
    customer_left = asyncio.Event()

    @ctx.room.on("participant_disconnected")
    def _on_left(participant) -> None:
        if participant.identity.startswith("sip-"):
            customer_left.set()

    try:
        await asyncio.wait_for(customer_left.wait(), timeout=settings.agent_stub_seconds)
        result, transcript = "customer_hung_up", "[stub conversation: customer hung up]"
    except TimeoutError:
        result = "stub_completed"
        transcript = f"[stub conversation with {call.get('contact_name') or 'customer'}]"
    await api.report_outcome(call_id, result=result, transcript=transcript)
    await ctx.delete_room()


async def run_live(ctx: agents.JobContext, call_id: str, api: AgentApi, call: dict) -> None:
    """Start the real conversation. Owns `api` from here on: it is closed in the shutdown
    callback, and any failure ends the call with an `error` outcome instead of raising."""
    ctl: CallController | None = None
    try:
        session = build_session(settings, vad=ctx.proc.userdata["vad"])

        async def speak(text: str) -> None:
            await session.say(text, allow_interruptions=False).wait_for_playout()

        async def end() -> None:
            ctx.shutdown(reason="call ended")

        ctl = CallController(
            call_id=call_id,
            report=api.report_outcome,
            speak=speak,
            end=end,
            get_transcript=lambda: format_transcript(session.history),
            max_seconds=settings.agent_max_call_seconds,
        )
        agent = CallerAgent(
            instructions=build_instructions(
                call["script_prompt"], call["contact_name"], call["custom_fields"]
            ),
            controller=ctl,
        )

        async def on_shutdown() -> None:
            try:
                await ctl.finish()
            finally:
                try:
                    await ctx.delete_room()  # drops the SIP leg if the customer is still on
                finally:
                    await api.aclose()

        ctx.add_shutdown_callback(on_shutdown)

        @ctx.room.on("participant_disconnected")
        def _on_left(participant) -> None:
            if participant.identity.startswith("sip-"):
                ctl.on_customer_left()
                ctx.shutdown(reason="customer hung up")

        @session.on("user_state_changed")
        def _on_user_state(ev) -> None:
            if ev.new_state == "away":
                asyncio.create_task(ctl.on_user_away())

        await session.start(
            agent=agent,
            room=ctx.room,
            room_options=RoomOptions(participant_identity=f"sip-{call_id}"),
        )
        ctl.start_timer()
        session.generate_reply(instructions=GREETING)
    except Exception as exc:
        log.exception("could not run the live conversation for %s", call_id)
        if ctl is not None:  # the shutdown callback will report it and clean up
            ctl.set_outcome("error", notes=f"agent error: {exc}"[:1000])
            ctx.shutdown(reason="agent error")
        else:
            await _fail_call(ctx, api, call_id, exc)
            await api.aclose()


async def _fail_call(ctx: agents.JobContext, api: AgentApi, call_id: str, exc: Exception) -> None:
    try:
        await api.report_outcome(call_id, result="error", notes=f"agent error: {exc}"[:1000])
        await ctx.delete_room()
    except Exception:
        log.exception("could not clean up call %s", call_id)


@server.rtc_session(agent_name=settings.livekit_agent_name)
async def entrypoint(ctx: agents.JobContext) -> None:
    call_id = json.loads(ctx.job.metadata or "{}").get("call_id")
    if not call_id:
        log.error("job without call_id metadata; leaving")
        return
    api = AgentApi()
    handed_off = False  # run_live takes over closing the API client
    try:
        await ctx.connect()
        call = await api.get_call(call_id)
        mode = resolve_mode(settings)
        log.info("call %s (%s): %s mode", call_id, call["to_number"], mode)
        if mode == "live":
            absent = missing_keys(settings)
            if absent:
                raise RuntimeError(f"AGENT_MODE=live but missing {absent}")
            handed_off = True
            await run_live(ctx, call_id, api, call)
        else:
            await run_stub(ctx, call_id, api, call)
    except Exception as exc:
        log.exception("agent failed on call %s", call_id)
        if not handed_off:
            await _fail_call(ctx, api, call_id, exc)
    finally:
        if not handed_off:
            await api.aclose()


def main() -> None:
    # The LiveKit CLI reads its connection settings from the environment.
    os.environ.setdefault("LIVEKIT_URL", settings.livekit_url)
    os.environ.setdefault("LIVEKIT_API_KEY", settings.livekit_api_key)
    os.environ.setdefault("LIVEKIT_API_SECRET", settings.livekit_api_secret)
    agents.cli.run_app(server)


if __name__ == "__main__":
    main()
