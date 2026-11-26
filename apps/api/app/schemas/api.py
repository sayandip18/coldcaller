from datetime import datetime, time
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class ORM(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ---- auth / users ----
class LoginIn(BaseModel):
    email: EmailStr
    password: str


class UserOut(ORM):
    id: str
    email: str
    name: str
    role: str
    is_active: bool
    created_at: datetime


class UserCreate(BaseModel):
    email: EmailStr
    name: str = ""
    password: str = Field(min_length=10, max_length=128)
    role: str = Field(default="member", pattern="^(admin|member)$")


class UserUpdate(BaseModel):
    name: str | None = None
    role: str | None = Field(default=None, pattern="^(admin|member)$")
    is_active: bool | None = None
    password: str | None = Field(default=None, min_length=10, max_length=128)


# ---- calls ----
class CallCreate(BaseModel):
    to_number: str
    contact_name: str = ""
    custom_fields: dict[str, str] = Field(default_factory=dict)
    script_prompt: str = Field(min_length=1)


class CallEventOut(ORM):
    type: str
    payload: dict[str, Any]
    at: datetime


class CallOut(ORM):
    id: str
    campaign_id: str | None
    to_number: str
    contact_name: str
    custom_fields: dict[str, Any]
    script_prompt: str | None
    status: str
    attempt: int
    outcome: dict[str, Any] | None
    transcript: str | None
    failure_reason: str | None
    created_at: datetime
    queued_at: datetime | None
    dialed_at: datetime | None
    answered_at: datetime | None
    ended_at: datetime | None


class CallDetail(CallOut):
    events: list[CallEventOut] = []


class CallPage(BaseModel):
    items: list[CallOut]
    total: int


# ---- campaigns ----
class CampaignOut(ORM):
    id: str
    name: str
    status: str
    script_prompt: str
    caller_id: str
    max_concurrency: int
    cps: int
    timezone: str
    call_window_start: time
    call_window_end: time
    webhook_url: str | None
    created_at: datetime
    total: int = 0
    counts: dict[str, int] = {}


class RejectedRow(BaseModel):
    line: int
    phone: str
    reason: str


class CampaignCreated(BaseModel):
    campaign: CampaignOut
    accepted: int
    rejected: list[RejectedRow]


# ---- dnc ----
class DncCreate(BaseModel):
    number: str


class DncOut(ORM):
    id: str
    number: str
    source: str
    added_at: datetime
