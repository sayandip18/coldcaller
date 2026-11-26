from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from app.auth.deps import DB, CurrentUser
from app.db.models import WebhookDelivery

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    campaign_id: str | None
    call_id: str | None
    event: str
    url: str
    status: str
    attempts: int
    last_status_code: int | None
    last_error: str | None
    created_at: object
    delivered_at: object | None


@router.get("/deliveries", response_model=list[DeliveryOut])
def list_deliveries(
    user: CurrentUser,
    db: DB,
    campaign_id: str | None = None,
    status: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[WebhookDelivery]:
    q = select(WebhookDelivery).where(WebhookDelivery.owner_id == user.id)
    if campaign_id:
        q = q.where(WebhookDelivery.campaign_id == campaign_id)
    if status:
        q = q.where(WebhookDelivery.status == status)
    return list(db.scalars(q.order_by(WebhookDelivery.created_at.desc()).limit(limit)))
