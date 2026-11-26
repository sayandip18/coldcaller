from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.auth.deps import DB, CurrentUser
from app.db.models import DncNumber
from app.schemas.api import DncCreate, DncOut
from app.services.phone import normalize_in_number

router = APIRouter(prefix="/dnc", tags=["dnc"])


@router.get("", response_model=list[DncOut])
def list_dnc(user: CurrentUser, db: DB) -> list[DncNumber]:
    return list(
        db.scalars(
            select(DncNumber).where(DncNumber.owner_id == user.id).order_by(DncNumber.added_at.desc())
        )
    )


@router.post("", response_model=DncOut, status_code=status.HTTP_201_CREATED)
def add_dnc(body: DncCreate, user: CurrentUser, db: DB) -> DncNumber:
    try:
        number = normalize_in_number(body.number)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    row = DncNumber(owner_id=user.id, number=number, source="manual")
    db.add(row)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "Number already on the list") from exc
    return row


@router.delete("/{dnc_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_dnc(dnc_id: str, user: CurrentUser, db: DB) -> None:
    row = db.scalar(select(DncNumber).where(DncNumber.id == dnc_id, DncNumber.owner_id == user.id))
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not found")
    db.delete(row)
    db.commit()
