import enum
import uuid
from datetime import UTC, datetime, time

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
    false,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


class Role(enum.StrEnum):
    admin = "admin"
    member = "member"


class CampaignStatus(enum.StrEnum):
    draft = "draft"
    running = "running"
    paused = "paused"
    done = "done"
    failed = "failed"


class CallStatus(enum.StrEnum):
    draft = "draft"
    queued = "queued"
    dialing = "dialing"
    ringing = "ringing"
    answered = "answered"
    in_progress = "in_progress"
    completed = "completed"
    no_answer = "no_answer"
    busy = "busy"
    failed = "failed"
    skipped_dnc = "skipped_dnc"
    skipped_window = "skipped_window"


TERMINAL_CALL_STATUSES = {
    CallStatus.completed,
    CallStatus.no_answer,
    CallStatus.busy,
    CallStatus.failed,
    CallStatus.skipped_dnc,
    CallStatus.skipped_window,
}


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), default=Role.member.value)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AuthSession(Base):
    __tablename__ = "sessions"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    status: Mapped[str] = mapped_column(String(16), default=CampaignStatus.draft.value)
    script_prompt: Mapped[str] = mapped_column(Text)
    caller_id: Mapped[str] = mapped_column(String(32), default="")
    max_concurrency: Mapped[int] = mapped_column(Integer, default=5)
    cps: Mapped[int] = mapped_column(Integer, default=1)
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Kolkata")
    call_window_start: Mapped[time] = mapped_column(Time, default=time(9, 0))
    call_window_end: Mapped[time] = mapped_column(Time, default=time(21, 0))
    webhook_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    calls: Mapped[list[Call]] = relationship(back_populates="campaign")


class Call(Base):
    __tablename__ = "calls"
    __table_args__ = (
        UniqueConstraint("owner_id", "idempotency_key", name="uq_calls_owner_idem"),
        Index("ix_calls_campaign_status", "campaign_id", "status"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    campaign_id: Mapped[str | None] = mapped_column(
        ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=True
    )
    to_number: Mapped[str] = mapped_column(String(20))
    contact_name: Mapped[str] = mapped_column(String(200), default="")
    custom_fields: Mapped[dict] = mapped_column(JSONType, default=dict)
    script_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default=CallStatus.draft.value, index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    livekit_room: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sip_call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    outcome: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    transcript: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # A call placed from the browser to try the agent. Never dialled, never queued, never
    # added to the DNC list or sent to webhooks.
    is_test: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dialed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    campaign: Mapped[Campaign | None] = relationship(back_populates="calls")


class CallEvent(Base):
    __tablename__ = "call_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[str] = mapped_column(ForeignKey("calls.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSONType, default=dict)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DncNumber(Base):
    __tablename__ = "dnc_numbers"
    __table_args__ = (UniqueConstraint("owner_id", "number", name="uq_dnc_owner_number"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    number: Mapped[str] = mapped_column(String(20), index=True)
    source: Mapped[str] = mapped_column(String(32), default="import")
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NcprNumber(Base):
    """National Customer Preference Register: numbers that must not receive promotional calls.
    Global (not per user). Loaded by an admin import; see app/services/ncpr.py."""

    __tablename__ = "ncpr_numbers"

    number: Mapped[str] = mapped_column(String(20), primary_key=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WebhookDelivery(Base):
    """A pending/finished HTTP notification to a campaign's webhook URL."""

    __tablename__ = "webhook_deliveries"
    __table_args__ = (Index("ix_webhook_due", "status", "next_attempt_at"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    campaign_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    call_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event: Mapped[str] = mapped_column(String(32))
    url: Mapped[str] = mapped_column(String(2048))
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending|delivered|failed
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
