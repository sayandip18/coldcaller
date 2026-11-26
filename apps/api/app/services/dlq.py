"""Dead-letter queue tooling. Calls land in the DLQ when dialing keeps failing for infrastructure
reasons (LiveKit down, trunk misconfigured). After fixing the cause, replay them."""

from sqlalchemy import update

from app.db.models import Call, CallEvent, CallStatus, utcnow
from app.db.session import SessionLocal
from app.queue import keys
from app.queue.producer import enqueue_calls, redis_client


def list_dlq(limit: int = 100) -> list[dict]:
    out = []
    with SessionLocal() as db:
        for entry_id, fields in redis_client().xrange(keys.DLQ, count=limit):
            call = db.get(Call, fields["call_id"])
            out.append(
                {
                    "entry": entry_id,
                    "call_id": fields["call_id"],
                    "reason": fields.get("reason", ""),
                    "status": call.status if call else "missing",
                    "to_number": call.to_number if call else "",
                }
            )
    return out


def replay_dlq(limit: int = 100, call_id: str | None = None, dry_run: bool = False) -> list[str]:
    """Put dead-lettered calls back in the queue with a fresh attempt count. Returns the call ids
    replayed. Entries whose call is no longer `failed` are dropped from the DLQ without replay."""
    r = redis_client()
    replayed: list[str] = []
    with SessionLocal() as db:
        for entry_id, fields in r.xrange(keys.DLQ, count=limit if not call_id else None):
            cid = fields["call_id"]
            if call_id and cid != call_id:
                continue
            if dry_run:
                replayed.append(cid)
                continue
            res = db.execute(
                update(Call)
                .where(Call.id == cid, Call.status == CallStatus.failed.value)
                .values(
                    status=CallStatus.queued.value,
                    attempt=0,
                    failure_reason=None,
                    ended_at=None,
                    queued_at=utcnow(),
                )
            )
            if res.rowcount == 1:  # type: ignore[attr-defined]
                db.add(CallEvent(call_id=cid, type="replayed", payload={"from_dlq": entry_id}))
                replayed.append(cid)
            db.commit()
            r.xdel(keys.DLQ, entry_id)
            if call_id:
                break
    if replayed and not dry_run:
        enqueue_calls(replayed)
    return replayed
