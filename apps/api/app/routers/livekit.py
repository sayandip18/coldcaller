import logging
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from livekit import api

from app.config import get_settings
from app.queue.producer import release_channels
from app.services.livekit_events import apply_event

log = logging.getLogger(__name__)
router = APIRouter(tags=["livekit"])


def _receiver() -> api.WebhookReceiver:
    s = get_settings()
    return api.WebhookReceiver(api.TokenVerifier(s.livekit_api_key, s.livekit_api_secret))


@router.post("/livekit/webhook")
async def livekit_webhook(
    request: Request, authorization: Annotated[str | None, Header()] = None
) -> dict[str, bool]:
    body = (await request.body()).decode()
    try:
        event = _receiver().receive(body, authorization or "")
    except Exception as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid webhook signature") from exc

    def work() -> None:
        release = apply_event(
            event.event,
            event.id,
            event.room.name,
            event.participant.identity,
            int(event.participant.kind),
        )
        if release:
            release_channels(*release)

    await run_in_threadpool(work)
    return {"ok": True}
