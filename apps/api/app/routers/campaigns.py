from datetime import time
from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select

from app.auth.deps import DB, CurrentUser
from app.db.models import Call, Campaign, CampaignStatus
from app.queue.producer import enqueue_calls
from app.schemas.api import CampaignCreated, CampaignOut, RejectedRow
from app.services.call_service import campaign_counts, start_campaign
from app.services.caller_id import caller_id_error
from app.services.caller_id import normalize as normalize_caller_id
from app.services.csv_import import CsvError, parse_contacts_csv
from app.services.webhooks import validate_webhook_url

router = APIRouter(prefix="/campaigns", tags=["campaigns"])

MAX_UPLOAD_BYTES = 5 * 1024 * 1024


def _out(db: DB, campaigns: list[Campaign]) -> list[CampaignOut]:
    counts = campaign_counts(db, [c.id for c in campaigns])
    outs = []
    for c in campaigns:
        out = CampaignOut.model_validate(c)
        out.counts = counts.get(c.id, {})
        out.total = sum(out.counts.values())
        outs.append(out)
    return outs


def _get_owned(db: DB, user_id: str, campaign_id: str) -> Campaign:
    campaign = db.scalar(
        select(Campaign).where(Campaign.id == campaign_id, Campaign.owner_id == user_id)
    )
    if campaign is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Campaign not found")
    return campaign


def _parse_hhmm(value: str, field: str) -> time:
    try:
        h, m = value.split(":")
        return time(int(h), int(m))
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, f"{field} must be HH:MM"
        ) from exc


@router.post("", response_model=CampaignCreated, status_code=status.HTTP_201_CREATED)
async def create_campaign(
    user: CurrentUser,
    db: DB,
    file: Annotated[UploadFile, File()],
    name: Annotated[str, Form(min_length=1, max_length=200)],
    script_prompt: Annotated[str, Form(min_length=1)],
    caller_id: Annotated[str, Form(max_length=32)] = "",
    max_concurrency: Annotated[int, Form(ge=1, le=100)] = 5,
    cps: Annotated[int, Form(ge=1, le=50)] = 1,
    timezone: Annotated[str, Form()] = "Asia/Kolkata",
    call_window_start: Annotated[str, Form()] = "09:00",
    call_window_end: Annotated[str, Form()] = "21:00",
    webhook_url: Annotated[str | None, Form(max_length=2048)] = None,
) -> CampaignCreated:
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Unknown timezone") from exc
    start, end = _parse_hhmm(call_window_start, "call_window_start"), _parse_hhmm(
        call_window_end, "call_window_end"
    )
    if start >= end:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Window start must precede end")
    # TRAI: promotional calls only between 09:00 and 21:00.
    if start < time(9, 0) or end > time(21, 0):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "Calling window must be within 09:00-21:00 for promotional calls",
        )

    caller_error = caller_id_error(caller_id)
    if caller_error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, caller_error)
    if webhook_url:
        try:
            await run_in_threadpool(validate_webhook_url, webhook_url)
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc

    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "CSV larger than 5 MB")
    try:
        parsed = parse_contacts_csv(data)
    except CsvError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    if not parsed.rows:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "No valid phone numbers in CSV")

    campaign = Campaign(
        owner_id=user.id,
        name=name,
        script_prompt=script_prompt,
        caller_id=normalize_caller_id(caller_id),
        max_concurrency=max_concurrency,
        cps=cps,
        timezone=timezone,
        call_window_start=start,
        call_window_end=end,
        webhook_url=webhook_url,
    )
    db.add(campaign)
    db.flush()
    db.add_all(
        Call(
            owner_id=user.id,
            campaign_id=campaign.id,
            to_number=r.to_number,
            contact_name=r.contact_name,
            custom_fields=r.custom_fields,
        )
        for r in parsed.rows
    )
    db.commit()
    return CampaignCreated(
        campaign=_out(db, [campaign])[0],
        accepted=len(parsed.rows),
        rejected=[RejectedRow(**r) for r in parsed.rejected],
    )


@router.get("", response_model=list[CampaignOut])
def list_campaigns(user: CurrentUser, db: DB) -> list[CampaignOut]:
    campaigns = list(
        db.scalars(
            select(Campaign).where(Campaign.owner_id == user.id).order_by(Campaign.created_at.desc())
        )
    )
    return _out(db, campaigns)


@router.get("/{campaign_id}", response_model=CampaignOut)
def get_campaign(campaign_id: str, user: CurrentUser, db: DB) -> CampaignOut:
    return _out(db, [_get_owned(db, user.id, campaign_id)])[0]


@router.post("/{campaign_id}/start", response_model=CampaignOut)
def start(campaign_id: str, user: CurrentUser, db: DB) -> CampaignOut:
    campaign = _get_owned(db, user.id, campaign_id)
    if campaign.status != CampaignStatus.draft.value:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Cannot start a {campaign.status} campaign")
    queued_ids = start_campaign(db, campaign)
    db.commit()
    enqueue_calls(queued_ids)
    return _out(db, [campaign])[0]


@router.post("/{campaign_id}/pause", response_model=CampaignOut)
def pause(campaign_id: str, user: CurrentUser, db: DB) -> CampaignOut:
    campaign = _get_owned(db, user.id, campaign_id)
    if campaign.status != CampaignStatus.running.value:
        raise HTTPException(status.HTTP_409_CONFLICT, "Only running campaigns can be paused")
    campaign.status = CampaignStatus.paused.value
    db.commit()
    return _out(db, [campaign])[0]


@router.post("/{campaign_id}/resume", response_model=CampaignOut)
def resume(campaign_id: str, user: CurrentUser, db: DB) -> CampaignOut:
    campaign = _get_owned(db, user.id, campaign_id)
    if campaign.status != CampaignStatus.paused.value:
        raise HTTPException(status.HTTP_409_CONFLICT, "Only paused campaigns can be resumed")
    campaign.status = CampaignStatus.running.value
    db.commit()
    return _out(db, [campaign])[0]


@router.delete("/{campaign_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_campaign(campaign_id: str, user: CurrentUser, db: DB) -> None:
    campaign = _get_owned(db, user.id, campaign_id)
    if campaign.status != CampaignStatus.draft.value:
        raise HTTPException(status.HTTP_409_CONFLICT, "Only draft campaigns can be deleted")
    for call in db.scalars(select(Call).where(Call.campaign_id == campaign.id)):
        db.delete(call)
    db.delete(campaign)
    db.commit()
