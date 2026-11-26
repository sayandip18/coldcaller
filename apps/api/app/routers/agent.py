"""Endpoints used by the voice agent (authenticated with a shared secret, not a user session)."""

import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.auth.deps import DB
from app.config import get_settings
from app.db.models import Call, CallEvent, CallStatus, Campaign, DncNumber
from app.services.lifecycle import ACTIVE, mark

router = APIRouter(prefix="/agent", tags=["agent"])


def require_agent(x_agent_secret: Annotated[str | None, Header()] = None) -> None:
    expected = get_settings().agent_shared_secret
    if not x_agent_secret or not secrets.compare_digest(x_agent_secret, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid agent secret")


AgentAuth = Depends(require_agent)


class AgentCallContext(BaseModel):
    call_id: str
    to_number: str
    contact_name: str
    custom_fields: dict[str, str]
    script_prompt: str
    status: str


class OutcomeIn(BaseModel):
    result: str = Field(min_length=1, max_length=64)
    notes: str = Field(default="", max_length=4000)
    follow_up: str | None = Field(default=None, max_length=1000)
    do_not_call: bool = False
    transcript: str | None = Field(default=None, max_length=200_000)


@router.get("/calls/{call_id}", response_model=AgentCallContext, dependencies=[AgentAuth])
def get_call_context(call_id: str, db: DB) -> AgentCallContext:
    call = db.get(Call, call_id)
    if call is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Call not found")
    prompt = call.script_prompt
    if prompt is None and call.campaign_id:
        campaign = db.get(Campaign, call.campaign_id)
        prompt = campaign.script_prompt if campaign else None
    return AgentCallContext(
        call_id=call.id,
        to_number=call.to_number,
        contact_name=call.contact_name,
        custom_fields=call.custom_fields or {},
        script_prompt=prompt or "",
        status=call.status,
    )


@router.patch("/calls/{call_id}/outcome", dependencies=[AgentAuth])
def report_outcome(call_id: str, body: OutcomeIn, db: DB) -> dict[str, str]:
    call = db.get(Call, call_id)
    if call is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Call not found")
    outcome = {
        "result": body.result,
        "notes": body.notes,
        "follow_up": body.follow_up,
        "do_not_call": body.do_not_call,
    }
    fields = {"outcome": outcome, "transcript": body.transcript}

    # If the call is still live this is the end of it. If the hang-up webhook won the race the
    # call is already completed, in which case we only attach the outcome and transcript.
    if not mark(db, call_id, ACTIVE, CallStatus.completed, event="outcome_reported", **fields):
        db.execute(update(Call).where(Call.id == call_id).values(**fields))
        db.add(CallEvent(call_id=call_id, type="outcome_reported", payload={"result": body.result}))
        db.commit()

    if body.do_not_call and not call.is_test:  # a browser test has no real number to block
        db.execute(
            pg_insert(DncNumber)
            .values(owner_id=call.owner_id, number=call.to_number, source="user_request")
            .on_conflict_do_nothing(constraint="uq_dnc_owner_number")
        )
        db.commit()
    return {"status": db.scalar(select(Call.status).where(Call.id == call_id)) or ""}
