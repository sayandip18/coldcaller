from typing import Annotated

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from sqlalchemy import func, select

from app.auth.deps import DB, AdminUser
from app.db.models import NcprNumber
from app.services.ncpr import import_numbers

router = APIRouter(prefix="/ncpr", tags=["ncpr"])

MAX_BYTES = 50 * 1024 * 1024


@router.get("")
def ncpr_status(_: AdminUser, db: DB) -> dict[str, int]:
    return {"count": db.scalar(select(func.count()).select_from(NcprNumber)) or 0}


@router.post("/import")
async def import_ncpr(_: AdminUser, db: DB, file: Annotated[UploadFile, File()]) -> dict[str, int]:
    """Admin only. Upload the NCPR scrub list as text, one number per line (extra columns ignored)."""
    data = await file.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "File larger than 50 MB")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "File must be UTF-8") from exc
    numbers = (line.split(",")[0] for line in text.splitlines())
    added, rejected = import_numbers(db, numbers)
    return {"added": added, "rejected": rejected}
