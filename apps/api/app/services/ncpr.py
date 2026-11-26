"""National Customer Preference Register (India, TRAI TCCCPR).

Promotional calls must not go to numbers registered on the NCPR. The official register is
obtained through a registered telemarketer's access provider, so the app does not call it
directly. Instead an admin imports the scrub list (one number per line) and every call is
checked against that table, both when it is queued and again just before it is dialled.
Swapping in a live provider later only means replacing `ncpr_registered`.
"""

from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db.models import NcprNumber
from app.services.phone import normalize_in_number

CHUNK = 5000


def ncpr_registered(db: Session, number: str) -> bool:
    return db.scalar(select(NcprNumber.number).where(NcprNumber.number == number)) is not None


def import_numbers(db: Session, raw_numbers: Iterable[str]) -> tuple[int, int]:
    """Add numbers to the register. Returns (added, rejected); invalid numbers are skipped."""
    valid: list[str] = []
    rejected = 0
    for raw in raw_numbers:
        raw = raw.strip()
        if not raw:
            continue
        try:
            valid.append(normalize_in_number(raw))
        except ValueError:
            rejected += 1
    added = 0
    for i in range(0, len(valid), CHUNK):
        chunk = [{"number": n} for n in set(valid[i : i + CHUNK])]
        res = db.execute(
            pg_insert(NcprNumber)
            .values(chunk)
            .on_conflict_do_nothing()
            .returning(NcprNumber.number)  # rowcount is -1 for multi-row inserts, so count rows
        )
        added += len(res.all())
    db.commit()
    return added, rejected
