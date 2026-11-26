"""Outbound webhooks: tell a campaign's webhook URL when a call ends and when the campaign is done.

Delivery is durable and at-least-once. Events are rows in `webhook_deliveries` written in the
same transaction as the state change that caused them; the worker sends them with exponential
backoff. Receivers should de-duplicate on the `X-Coldcaller-Delivery` id and verify
`X-Coldcaller-Signature` (HMAC-SHA256 of "<timestamp>.<body>" with WEBHOOK_SIGNING_SECRET).
"""

import hashlib
import hmac
import ipaddress
import json
import logging
import socket
from collections.abc import Callable
from datetime import timedelta
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.models import Call, Campaign, WebhookDelivery, utcnow
from app.db.session import SessionLocal

log = logging.getLogger(__name__)

CALL_COMPLETED = "call.completed"
CAMPAIGN_DONE = "campaign.done"
# Give the voice agent a moment to attach its outcome before a completed call is announced.
COMPLETED_GRACE = timedelta(seconds=3)
RETRY_STATUSES = {408, 425, 429}  # other 4xx responses are the receiver saying "never"
LEASE = timedelta(seconds=60)  # how long a worker owns a delivery it is sending


# --------------------------------------------------------------------- URL safety
def validate_webhook_url(
    url: str, *, allow_private: bool | None = None, resolve: Callable | None = None
) -> None:
    """Raise ValueError unless the URL is safe for the server to call (SSRF guard).

    Webhook URLs are supplied by users and requested by this server, so without this a user
    could aim it at cloud metadata endpoints or internal services. Every address the host
    resolves to must be public. This is checked when the campaign is saved and again before
    each delivery. (A DNS answer can change between check and request; for a hostile
    environment also egress-filter the worker.)"""
    if allow_private is None:
        allow_private = get_settings().webhook_allow_private
    resolve = resolve or socket.getaddrinfo
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("Webhook URL must be an http(s) URL")
    if parts.scheme == "http" and not allow_private:
        raise ValueError("Webhook URL must use https")
    if parts.username or parts.password:
        raise ValueError("Webhook URL must not contain credentials")
    try:
        infos = resolve(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80))
    except OSError as exc:
        raise ValueError(f"Cannot resolve webhook host: {parts.hostname}") from exc
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if getattr(ip, "ipv4_mapped", None):
            ip = ip.ipv4_mapped
        if not allow_private and not ip.is_global:
            raise ValueError("Webhook URL must point to a public address")


# ----------------------------------------------------------------------- emitting
def _campaign_target(db: Session, campaign_id: str) -> tuple[str, str] | None:
    row = db.execute(
        select(Campaign.webhook_url, Campaign.owner_id).where(Campaign.id == campaign_id)
    ).first()
    if row is None or not row[0]:
        return None
    return row[0], row[1]


def enqueue_call_event(db: Session, call_id: str, status: str) -> None:
    """Queue a `call.completed` webhook (add to the caller's open transaction)."""
    campaign_id = db.scalar(select(Call.campaign_id).where(Call.id == call_id))
    if not campaign_id:
        return
    target = _campaign_target(db, campaign_id)
    if target is None:
        return
    url, owner_id = target
    delay = COMPLETED_GRACE if status == "completed" else timedelta(0)
    db.add(
        WebhookDelivery(
            owner_id=owner_id,
            campaign_id=campaign_id,
            call_id=call_id,
            event=CALL_COMPLETED,
            url=url,
            next_attempt_at=utcnow() + delay,
        )
    )


def enqueue_campaign_done(db: Session, campaign_id: str) -> None:
    target = _campaign_target(db, campaign_id)
    if target is None:
        return
    url, owner_id = target
    # Announced after the per-call events (which wait out COMPLETED_GRACE) so receivers normally
    # see the calls first. Delivery is at-least-once and unordered in general (retries reorder).
    db.add(
        WebhookDelivery(
            owner_id=owner_id,
            campaign_id=campaign_id,
            event=CAMPAIGN_DONE,
            url=url,
            next_attempt_at=utcnow() + COMPLETED_GRACE + timedelta(seconds=2),
        )
    )


# ---------------------------------------------------------------------- payloads
def _call_data(call: Call) -> dict:
    iso = lambda d: d.isoformat() if d else None
    return {
        "id": call.id,
        "campaign_id": call.campaign_id,
        "to_number": call.to_number,
        "contact_name": call.contact_name,
        "custom_fields": call.custom_fields or {},
        "status": call.status,
        "outcome": call.outcome,
        "transcript": call.transcript,
        "failure_reason": call.failure_reason,
        "answered_at": iso(call.answered_at),
        "ended_at": iso(call.ended_at),
    }


def build_payload(db: Session, d: WebhookDelivery) -> dict | None:
    """Built when sending, not when queued, so a late outcome is included."""
    data: dict = {}
    if d.event == CALL_COMPLETED:
        call = db.get(Call, d.call_id) if d.call_id else None
        if call is None:
            return None
        data["call"] = _call_data(call)
    elif d.event == CAMPAIGN_DONE:
        campaign = db.get(Campaign, d.campaign_id) if d.campaign_id else None
        if campaign is None:
            return None
        counts = dict(
            db.execute(
                select(Call.status, func.count()).where(Call.campaign_id == campaign.id).group_by(Call.status)
            ).all()
        )
        data["campaign"] = {
            "id": campaign.id,
            "name": campaign.name,
            "status": campaign.status,
            "total": sum(counts.values()),
            "counts": counts,
        }
    return {
        "event": d.event,
        "delivery_id": d.id,
        "created_at": d.created_at.isoformat(),
        "data": data,
    }


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return "v1=" + mac.hexdigest()


def backoff(attempts: int) -> timedelta:
    return timedelta(seconds=min(3600, 10 * 2 ** (attempts - 1)))


# ---------------------------------------------------------------------- delivery
class WebhookSender:
    def __init__(
        self,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
        resolve: Callable | None = None,
    ):
        self.settings = settings or get_settings()
        self.client = client or httpx.AsyncClient(
            timeout=self.settings.webhook_timeout_s, follow_redirects=False
        )
        self._resolve = resolve

    @staticmethod
    def claim_due(limit: int = 20) -> list[tuple[str, str, dict | None, int]]:
        """Reserve due deliveries (safe with several workers). Returns (id, url, payload, attempts)."""
        out = []
        with SessionLocal() as db:
            rows = db.scalars(
                select(WebhookDelivery)
                .where(WebhookDelivery.status == "pending", WebhookDelivery.next_attempt_at <= utcnow())
                .order_by(WebhookDelivery.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).all()
            for d in rows:
                d.next_attempt_at = utcnow() + LEASE  # if this worker dies, it is retried later
                out.append((d.id, d.url, build_payload(db, d), d.attempts))
            db.commit()
        return out

    @staticmethod
    def record(delivery_id: str, *, ok: bool, status_code: int | None, error: str | None, permanent: bool, max_attempts: int) -> None:
        with SessionLocal() as db:
            d = db.get(WebhookDelivery, delivery_id)
            if d is None:
                return
            d.attempts += 1
            d.last_status_code, d.last_error = status_code, (error or None)
            if ok:
                d.status, d.delivered_at, d.last_error = "delivered", utcnow(), None
            elif permanent or d.attempts >= max_attempts:
                d.status = "failed"
            else:
                d.next_attempt_at = utcnow() + backoff(d.attempts)
            db.commit()

    async def send_one(self, delivery_id: str, url: str, payload: dict | None, attempts: int) -> None:
        import asyncio

        s = self.settings
        if payload is None:  # the call/campaign no longer exists
            await asyncio.to_thread(self.record, delivery_id, ok=False, status_code=None, error="subject no longer exists", permanent=True, max_attempts=s.webhook_max_attempts)
            return
        try:
            validate_webhook_url(url, allow_private=s.webhook_allow_private, resolve=self._resolve)
        except ValueError as exc:
            await asyncio.to_thread(self.record, delivery_id, ok=False, status_code=None, error=str(exc), permanent=True, max_attempts=s.webhook_max_attempts)
            return
        body = json.dumps(payload, separators=(",", ":")).encode()
        ts = str(int(utcnow().timestamp()))
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "coldcaller-webhooks/1",
            "X-Coldcaller-Event": payload["event"],
            "X-Coldcaller-Delivery": delivery_id,
            "X-Coldcaller-Timestamp": ts,
            "X-Coldcaller-Signature": sign(s.webhook_signing_secret, ts, body),
        }
        status_code, error, permanent = None, None, False
        try:
            res = await self.client.post(url, content=body, headers=headers)
            status_code = res.status_code
            ok = 200 <= status_code < 300
            if not ok:
                error = f"HTTP {status_code}"
                permanent = 300 <= status_code < 500 and status_code not in RETRY_STATUSES
        except httpx.HTTPError as exc:
            ok, error = False, f"{type(exc).__name__}: {exc}"[:500]
        await asyncio.to_thread(self.record, delivery_id, ok=ok, status_code=status_code, error=error, permanent=permanent, max_attempts=s.webhook_max_attempts)

    async def deliver_due(self, limit: int = 20) -> int:
        import asyncio

        due = await asyncio.to_thread(self.claim_due, limit)
        if due:
            await asyncio.gather(*(self.send_one(*item) for item in due))
        return len(due)

    async def aclose(self) -> None:
        await self.client.aclose()
