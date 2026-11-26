from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import (
    Call,
    CallEvent,
    CallStatus,
    Campaign,
    CampaignStatus,
    DncNumber,
    utcnow,
)


def add_event(db: Session, call: Call, type_: str, **payload) -> None:
    db.add(CallEvent(call_id=call.id, type=type_, payload=payload))


def is_dnc(db: Session, owner_id: str, number: str) -> bool:
    return (
        db.scalar(select(DncNumber.id).where(DncNumber.owner_id == owner_id, DncNumber.number == number))
        is not None
    )


def queue_call(db: Session, call: Call) -> bool:
    """Move a draft call to queued, or skip it if the number is on the owner's DNC list.

    Returns True if the call is now queued. The caller must commit, then pass the ids of
    queued calls to `queue.producer.enqueue_calls` (after commit, so a worker never sees a
    call id whose row isn't visible yet).
    """
    if is_dnc(db, call.owner_id, call.to_number):
        call.status = CallStatus.skipped_dnc.value
        call.ended_at = utcnow()
        add_event(db, call, "skipped_dnc")
        return False
    call.status = CallStatus.queued.value
    call.queued_at = utcnow()
    add_event(db, call, "queued")
    return True


def start_campaign(db: Session, campaign: Campaign) -> list[str]:
    """Queue every draft call. Returns the ids to enqueue once the transaction is committed."""
    campaign.status = CampaignStatus.running.value
    drafts = db.scalars(
        select(Call).where(Call.campaign_id == campaign.id, Call.status == CallStatus.draft.value)
    )
    return [call.id for call in drafts if queue_call(db, call)]


def campaign_counts(db: Session, campaign_ids: list[str]) -> dict[str, dict[str, int]]:
    if not campaign_ids:
        return {}
    rows = db.execute(
        select(Call.campaign_id, Call.status, func.count())
        .where(Call.campaign_id.in_(campaign_ids))
        .group_by(Call.campaign_id, Call.status)
    )
    out: dict[str, dict[str, int]] = {cid: {} for cid in campaign_ids}
    for cid, status, n in rows:
        out[cid][status] = n
    return out
