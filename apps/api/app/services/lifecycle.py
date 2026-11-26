"""Call state transitions. Shared by the worker and (Phase 3) the LiveKit webhook handler.

Every transition is a conditional UPDATE, so duplicate or out-of-order events are no-ops.
"""

from collections.abc import Iterable
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import (
    TERMINAL_CALL_STATUSES,
    Call,
    CallEvent,
    CallStatus,
    Campaign,
    CampaignStatus,
    utcnow,
)

ACTIVE = (CallStatus.dialing, CallStatus.ringing, CallStatus.answered, CallStatus.in_progress)


def mark(
    db: Session,
    call_id: str,
    from_statuses: Iterable[CallStatus],
    to: CallStatus,
    *,
    event: str | None = None,
    payload: dict | None = None,
    **fields,
) -> bool:
    values = {"status": to.value, **fields}
    if to in TERMINAL_CALL_STATUSES:
        values.setdefault("ended_at", utcnow())
    res = db.execute(
        update(Call)
        .where(Call.id == call_id, Call.status.in_([s.value for s in from_statuses]))
        .values(**values)
    )
    if res.rowcount != 1:  # type: ignore[attr-defined]
        db.rollback()
        return False
    db.add(CallEvent(call_id=call_id, type=event or to.value, payload=payload or {}))
    db.commit()
    return True


def reap_stuck_calls(db: Session, older_than_seconds: int) -> list[tuple[str, str | None]]:
    """Fail calls that have been in flight longer than any call can legitimately last."""
    cutoff = utcnow() - timedelta(seconds=older_than_seconds)
    rows = db.execute(
        select(Call.id, Call.campaign_id).where(
            Call.status.in_([s.value for s in ACTIVE]), Call.dialed_at < cutoff
        )
    ).all()
    reaped = []
    for call_id, campaign_id in rows:
        if mark(
            db, call_id, ACTIVE, CallStatus.failed, event="timeout", failure_reason="timeout"
        ):
            reaped.append((call_id, campaign_id))
    return reaped


def finalize_campaigns(db: Session) -> list[str]:
    """Mark running campaigns done once every call is in a terminal state."""
    terminal = [s.value for s in TERMINAL_CALL_STATUSES]
    open_calls = (
        select(Call.id)
        .where(Call.campaign_id == Campaign.id, Call.status.not_in(terminal))
        .exists()
    )
    ids = list(
        db.scalars(
            select(Campaign.id).where(Campaign.status == CampaignStatus.running.value, ~open_calls)
        )
    )
    for cid in ids:
        db.execute(
            update(Campaign)
            .where(Campaign.id == cid, Campaign.status == CampaignStatus.running.value)
            .values(status=CampaignStatus.done.value, updated_at=utcnow())
        )
    db.commit()
    return ids
