"""Apply LiveKit webhook events to calls. Pure DB logic so it can be tested without LiveKit."""

from sqlalchemy import select

from app.db.models import Call, CallEvent, CallStatus
from app.db.session import SessionLocal
from app.services.lifecycle import mark

ROOM_PREFIX = "call-"
SIP_IDENTITY_PREFIX = "sip-"
AGENT_KIND = 4  # livekit ParticipantInfo.Kind.AGENT


def call_id_from_room(room_name: str) -> str | None:
    return room_name[len(ROOM_PREFIX):] if room_name.startswith(ROOM_PREFIX) else None


def _note(db, call_id: str, event_id: str, type_: str) -> bool:
    """Record an informational event once per LiveKit event id (webhooks are retried)."""
    seen = db.scalar(
        select(CallEvent.id).where(
            CallEvent.call_id == call_id,
            CallEvent.type == type_,
            CallEvent.payload["event_id"].as_string() == event_id,
        )
    )
    if seen:
        return False
    db.add(CallEvent(call_id=call_id, type=type_, payload={"event_id": event_id}))
    db.commit()
    return True


def apply_event(
    event: str, event_id: str, room_name: str, identity: str = "", kind: int = 0
) -> tuple[str, str | None] | None:
    """Update the call for a webhook event. Returns (call_id, campaign_id) when the call's
    concurrency slot should be released, else None."""
    call_id = call_id_from_room(room_name)
    if call_id is None:
        return None
    with SessionLocal() as db:
        call = db.get(Call, call_id)
        if call is None:
            return None
        campaign_id = call.campaign_id

        if event == "participant_joined" and (kind == AGENT_KIND or not identity.startswith(SIP_IDENTITY_PREFIX)):
            mark(db, call_id, [CallStatus.answered], CallStatus.in_progress, event="agent_joined")
        elif event == "participant_left" and identity.startswith(SIP_IDENTITY_PREFIX):
            _note(db, call_id, event_id, "customer_hangup")
        elif event == "room_finished":
            mark(
                db,
                call_id,
                [CallStatus.answered, CallStatus.in_progress],
                CallStatus.completed,
                event="room_finished",
            )
            return call_id, campaign_id
    return None
