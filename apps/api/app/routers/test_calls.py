"""Browser test calls: talk to the voice agent from your laptop, with no phone or carrier.

1. POST /api/test-calls            creates a test call and returns a LiveKit token for the browser
2. (browser joins the room with the token, mic on)
3. POST /api/test-calls/{id}/agent asks LiveKit to dispatch the agent into that room

The agent starts only after the browser has joined, so you never miss the greeting.
Test calls skip dialing, queues, CPS and channel limits, DNC/NCPR checks and webhooks.
"""

import asyncio
import json
import logging
from datetime import timedelta

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import func, select

from app.auth.deps import DB, CurrentUser
from app.config import get_settings
from app.db.models import Call, CallEvent, CallStatus, utcnow
from app.livekit_client import LiveKitClient, customer_token
from app.schemas.api import TestCallCreate, TestCallStarted
from app.services.lifecycle import ACTIVE, mark

log = logging.getLogger(__name__)
router = APIRouter(prefix="/test-calls", tags=["test-calls"])

MAX_PER_HOUR = 20  # a runaway page must not rack up STT/LLM/TTS bills
TEST_NUMBER = "browser-test"
TOKEN_TTL_S = 600


async def _dispatch(room: str, agent_name: str, metadata: str) -> None:
    client = LiveKitClient()
    try:
        await client.dispatch_agent(room, agent_name, metadata)
    finally:
        await client.aclose()


def _owned_test_call(db, user_id: str, call_id: str) -> Call:
    call = db.scalar(
        select(Call).where(Call.id == call_id, Call.owner_id == user_id, Call.is_test.is_(True))
    )
    if call is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Test call not found")
    return call


@router.post("", response_model=TestCallStarted, status_code=status.HTTP_201_CREATED)
def start_test_call(body: TestCallCreate, user: CurrentUser, db: DB) -> TestCallStarted:
    recent = db.scalar(
        select(func.count())
        .select_from(Call)
        .where(
            Call.owner_id == user.id,
            Call.is_test.is_(True),
            Call.created_at > utcnow() - timedelta(hours=1),
        )
    )
    if (recent or 0) >= MAX_PER_HOUR:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS, f"At most {MAX_PER_HOUR} test calls per hour"
        )

    call = Call(
        owner_id=user.id,
        to_number=TEST_NUMBER,
        contact_name=body.contact_name,
        custom_fields=body.custom_fields,
        script_prompt=body.script_prompt,
        is_test=True,
        status=CallStatus.answered.value,  # the "customer" picks up by joining the room
        dialed_at=utcnow(),
        answered_at=utcnow(),
        attempt=1,
    )
    db.add(call)
    db.flush()
    call.livekit_room = room = f"call-{call.id}"
    db.add(CallEvent(call_id=call.id, type="test_call_started", payload={}))
    db.commit()

    identity = f"sip-{call.id}"  # the agent treats this identity as the customer
    settings = get_settings()
    return TestCallStarted(
        call_id=call.id,
        room=room,
        identity=identity,
        token=customer_token(room, identity, settings, TOKEN_TTL_S),
        url=settings.livekit_browser_url,
    )


@router.post("/{call_id}/agent")
def start_agent(call_id: str, user: CurrentUser, db: DB) -> dict[str, str]:
    call = _owned_test_call(db, user.id, call_id)
    if call.status != CallStatus.answered.value:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Call is {call.status}")
    already = db.scalar(
        select(CallEvent.id).where(CallEvent.call_id == call.id, CallEvent.type == "agent_dispatched")
    )
    if already:  # a double click must not put two agents in one room
        return {"status": "already_dispatched"}
    db.add(CallEvent(call_id=call.id, type="agent_dispatched", payload={}))
    db.commit()
    try:
        asyncio.run(
            _dispatch(call.livekit_room or f"call-{call.id}", get_settings().livekit_agent_name, json.dumps({"call_id": call.id}))
        )
    except Exception as exc:
        log.exception("could not dispatch the agent for test call %s", call_id)
        mark(
            db, call.id, ACTIVE, CallStatus.failed,
            event="failed", failure_reason=f"could not start the agent: {exc}"[:255],
        )
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "Could not reach LiveKit to start the agent. Is the LiveKit stack running (pnpm infra:livekit)?",
        ) from exc
    return {"status": "dispatched"}
