from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.auth.deps import DB, CurrentUser
from app.db.models import Call, CallEvent
from app.queue.producer import enqueue_calls
from app.schemas.api import CallCreate, CallDetail, CallOut, CallPage
from app.services.call_service import add_event, queue_call
from app.services.phone import normalize_in_number

router = APIRouter(prefix="/calls", tags=["calls"])


@router.post("", response_model=CallOut, status_code=status.HTTP_201_CREATED)
def create_call(
    body: CallCreate,
    response: Response,
    user: CurrentUser,
    db: DB,
    idempotency_key: Annotated[str | None, Header(max_length=128)] = None,
) -> Call:
    if idempotency_key:
        existing = db.scalar(
            select(Call).where(Call.owner_id == user.id, Call.idempotency_key == idempotency_key)
        )
        if existing:
            response.status_code = status.HTTP_200_OK
            return existing
    try:
        number = normalize_in_number(body.to_number)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc

    call = Call(
        owner_id=user.id,
        to_number=number,
        contact_name=body.contact_name,
        custom_fields=body.custom_fields,
        script_prompt=body.script_prompt,
        idempotency_key=idempotency_key,
    )
    db.add(call)
    try:
        db.flush()
    except IntegrityError:  # concurrent request with the same idempotency key
        db.rollback()
        existing = db.scalar(
            select(Call).where(Call.owner_id == user.id, Call.idempotency_key == idempotency_key)
        )
        if existing:
            response.status_code = status.HTTP_200_OK
            return existing
        raise
    add_event(db, call, "created")
    queued = queue_call(db, call)
    db.commit()
    if queued:
        enqueue_calls([call.id])
    return call


@router.get("", response_model=CallPage)
def list_calls(
    user: CurrentUser,
    db: DB,
    campaign_id: str | None = None,
    status_: Annotated[str | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CallPage:
    q = select(Call).where(Call.owner_id == user.id)
    if campaign_id:
        q = q.where(Call.campaign_id == campaign_id)
    if status_:
        q = q.where(Call.status == status_)
    total = db.scalar(select(func.count()).select_from(q.subquery())) or 0
    items = db.scalars(q.order_by(Call.created_at.desc(), Call.id).limit(limit).offset(offset))
    return CallPage(items=[CallOut.model_validate(c) for c in items], total=total)


@router.get("/{call_id}", response_model=CallDetail)
def get_call(call_id: str, user: CurrentUser, db: DB) -> CallDetail:
    call = db.scalar(select(Call).where(Call.id == call_id, Call.owner_id == user.id))
    if call is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Call not found")
    events = db.scalars(select(CallEvent).where(CallEvent.call_id == call.id).order_by(CallEvent.id))
    detail = CallDetail.model_validate(call)
    detail.events = [e for e in events]  # type: ignore[misc]
    return detail
