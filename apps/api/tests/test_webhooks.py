import asyncio
import hmac
import json
import socket
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings, get_settings
from app.db.models import Call, WebhookDelivery, utcnow
from app.db.session import SessionLocal
from app.services.webhooks import (
    WebhookSender,
    backoff,
    build_payload,
    sign,
    validate_webhook_url,
)

from .test_campaigns import create
from .test_worker import fast_dialer, numbers_csv, run_worker, wait_for

HOOK = "https://hooks.example.test/coldcaller"
PUBLIC = [(socket.AF_INET, 1, 6, "", ("93.184.216.34", 0))]


def fake_dns(mapping: dict[str, str] | None = None):
    mapping = mapping or {}

    def resolve(host, port, *a, **k):
        if host in mapping:
            return [(socket.AF_INET, 1, 6, "", (mapping[host], 0))]
        if host.endswith(".example.test"):
            return PUBLIC
        raise socket.gaierror("unknown host")

    return resolve


@pytest.fixture
def public_dns(monkeypatch):
    """Webhook hosts under .example.test resolve to a public IP; everything else is untouched."""
    real = socket.getaddrinfo

    def patched(host, *a, **k):
        if isinstance(host, str) and host.endswith(".example.test"):
            return PUBLIC
        return real(host, *a, **k)

    monkeypatch.setattr(socket, "getaddrinfo", patched)


# ------------------------------------------------------------------ SSRF guard
@pytest.mark.parametrize(
    "url,host_ip",
    [
        ("https://internal.example.test/x", "10.0.0.5"),
        ("https://metadata.example.test/x", "169.254.169.254"),  # cloud metadata
        ("https://loop.example.test/x", "127.0.0.1"),
        ("https://lan.example.test/x", "192.168.1.10"),
        ("https://v6.example.test/x", "::1"),
        ("https://mapped.example.test/x", "::ffff:10.0.0.1"),
    ],
)
def test_private_addresses_are_rejected(url, host_ip):
    host = url.split("/")[2]
    with pytest.raises(ValueError, match="public"):
        validate_webhook_url(url, allow_private=False, resolve=fake_dns({host: host_ip}))


@pytest.mark.parametrize(
    "url,msg",
    [
        ("http://hooks.example.test/x", "https"),
        ("ftp://hooks.example.test/x", "http"),
        ("https://user:pw@hooks.example.test/x", "credentials"),
        ("https:///nohost", "http"),
        ("https://does-not-resolve.invalid/x", "resolve"),
    ],
)
def test_malformed_or_unsafe_urls_are_rejected(url, msg):
    with pytest.raises(ValueError, match=msg):
        validate_webhook_url(url, allow_private=False, resolve=fake_dns())


def test_public_https_url_is_accepted_and_dev_mode_allows_private():
    validate_webhook_url(HOOK, allow_private=False, resolve=fake_dns())
    validate_webhook_url("http://localhost:9000/hook", allow_private=True, resolve=fake_dns({"localhost": "127.0.0.1"}))


def test_campaign_creation_rejects_an_unsafe_webhook(member, public_dns):
    assert create(member, webhook_url="http://169.254.169.254/latest/meta-data").status_code == 422
    assert create(member, webhook_url="https://localhost/hook").status_code == 422
    assert create(member, webhook_url=HOOK).status_code == 201


# ------------------------------------------------------------------- signature
def test_signature_is_verifiable_and_binds_the_timestamp():
    body = b'{"a":1}'
    sig = sign("secret", "1700000000", body)
    expected = hmac.new(b"secret", b"1700000000." + body, "sha256").hexdigest()
    assert sig == f"v1={expected}"
    assert sign("secret", "1700000001", body) != sig  # replaying with a new timestamp fails


def test_backoff_grows_and_is_capped():
    assert [backoff(n).total_seconds() for n in (1, 2, 3, 4)] == [10, 20, 40, 80]
    assert backoff(30).total_seconds() == 3600


# ------------------------------------------------------------- delivery & retry
class Receiver:
    """Stands in for the customer's server."""

    def __init__(self, *statuses: int):
        self.statuses = list(statuses) or [200]
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return httpx.Response(status)


def sender_for(receiver: Receiver, **settings) -> WebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver))
    return WebhookSender(Settings(webhook_allow_private=False, **settings), client=client, resolve=fake_dns())


def make_delivery(member_email="m@test.dev", url=HOOK, event="call.completed") -> str:
    from .conftest import make_user

    user = make_user(member_email)
    with SessionLocal() as db:
        call = Call(owner_id=user.id, to_number="+919876500001", status="completed",
                    outcome={"result": "interested"}, transcript="Agent: hi")
        db.add(call)
        db.flush()
        d = WebhookDelivery(owner_id=user.id, call_id=call.id, event=event, url=url)
        db.add(d)
        db.commit()
        return d.id


def state(delivery_id: str) -> WebhookDelivery:
    with SessionLocal() as db:
        return db.get(WebhookDelivery, delivery_id)


def make_due(delivery_id: str) -> None:
    with SessionLocal() as db:
        db.get(WebhookDelivery, delivery_id).next_attempt_at = utcnow() - timedelta(seconds=1)
        db.commit()


def test_successful_delivery_is_signed_and_carries_the_current_call_state():
    did = make_delivery()
    rx = Receiver(200)
    assert asyncio.run(sender_for(rx).deliver_due()) == 1
    req = rx.requests[0]
    body = req.content
    payload = json.loads(body)
    assert payload["event"] == "call.completed" and payload["delivery_id"] == did
    assert payload["data"]["call"]["outcome"] == {"result": "interested"}
    ts = req.headers["X-Coldcaller-Timestamp"]
    assert req.headers["X-Coldcaller-Signature"] == sign(get_settings().webhook_signing_secret, ts, body)
    assert req.headers["X-Coldcaller-Delivery"] == did
    d = state(did)
    assert d.status == "delivered" and d.attempts == 1 and d.delivered_at is not None


def test_server_errors_are_retried_with_backoff_then_delivered():
    did = make_delivery()
    rx = Receiver(500, 503, 200)
    s = sender_for(rx)
    asyncio.run(s.deliver_due())
    d = state(did)
    assert d.status == "pending" and d.attempts == 1 and d.last_status_code == 500
    assert d.next_attempt_at > utcnow() + timedelta(seconds=5)  # backed off, not retried at once
    assert asyncio.run(s.deliver_due()) == 0  # nothing due yet
    make_due(did)
    asyncio.run(s.deliver_due())
    make_due(did)
    asyncio.run(s.deliver_due())
    d = state(did)
    assert d.status == "delivered" and d.attempts == 3 and len(rx.requests) == 3


def test_permanent_client_errors_are_not_retried_but_throttling_is():
    gone = make_delivery("a@test.dev")
    asyncio.run(sender_for(Receiver(404)).deliver_due())
    assert state(gone).status == "failed" and state(gone).attempts == 1

    busy = make_delivery("b@test.dev")
    asyncio.run(sender_for(Receiver(429, 200)).deliver_due())
    assert state(busy).status == "pending"  # 429 = try again later


def test_redirects_are_not_followed():
    did = make_delivery()
    asyncio.run(sender_for(Receiver(302)).deliver_due())
    assert state(did).status == "failed"


def test_gives_up_after_max_attempts():
    did = make_delivery()
    s = sender_for(Receiver(500), webhook_max_attempts=3)
    for _ in range(3):
        make_due(did)
        asyncio.run(s.deliver_due())
    d = state(did)
    assert d.status == "failed" and d.attempts == 3 and d.last_error == "HTTP 500"


def test_network_errors_are_retried():
    did = make_delivery()

    def boom(request):
        raise httpx.ConnectError("refused")

    client = httpx.AsyncClient(transport=httpx.MockTransport(boom))
    s = WebhookSender(Settings(), client=client, resolve=fake_dns())
    asyncio.run(s.deliver_due())
    d = state(did)
    assert d.status == "pending" and "ConnectError" in d.last_error


def test_url_that_turns_private_after_queueing_is_refused_at_delivery():
    did = make_delivery(url="https://rebind.example.test/x")
    rx = Receiver(200)
    client = httpx.AsyncClient(transport=httpx.MockTransport(rx))
    s = WebhookSender(Settings(), client=client, resolve=fake_dns({"rebind.example.test": "10.0.0.9"}))
    asyncio.run(s.deliver_due())
    assert rx.requests == [] and state(did).status == "failed"


def test_two_workers_never_send_the_same_delivery():
    did = make_delivery()
    first = WebhookSender.claim_due()
    second = WebhookSender.claim_due()  # the lease hides it from a concurrent worker
    assert [x[0] for x in first] == [did] and second == []


def test_delivery_for_a_deleted_call_is_dropped():
    did = make_delivery()
    with SessionLocal() as db:
        d = db.get(WebhookDelivery, did)
        call = db.get(Call, d.call_id)
        d.call_id = "gone"
        db.delete(call)
        db.commit()
    rx = Receiver(200)
    asyncio.run(sender_for(rx).deliver_due())
    assert rx.requests == [] and state(did).status == "failed"


def test_payload_reflects_an_outcome_that_arrived_after_queueing():
    did = make_delivery()
    with SessionLocal() as db:
        d = db.get(WebhookDelivery, did)
        db.get(Call, d.call_id).outcome = {"result": "callback_requested"}
        db.commit()
        assert build_payload(db, d)["data"]["call"]["outcome"]["result"] == "callback_requested"


# --------------------------------------------------------- end to end via worker
def test_campaign_sends_a_webhook_per_call_and_one_when_done(member, public_dns):
    cid = create(member, csv=numbers_csv(4), webhook_url=HOOK, cps=50).json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    rx = Receiver(200)

    async def scenario(r, worker):
        def all_sent():
            with SessionLocal() as db:
                rows = list(db.scalars(select(WebhookDelivery).where(WebhookDelivery.campaign_id == cid)))
            return len(rows) == 5 and all(d.status == "delivered" for d in rows)

        assert await wait_for(all_sent, timeout=25), "webhooks were not all delivered"

    run_worker(
        scenario,
        dialer_factory=fast_dialer(),
        carrier_cps=100,
        webhook_sender=sender_for(rx),
    )
    events = [json.loads(r.content)["event"] for r in rx.requests]
    assert events.count("call.completed") == 4 and events.count("campaign.done") == 1
    done = next(json.loads(r.content) for r in rx.requests if b"campaign.done" in r.content)
    assert done["data"]["campaign"]["total"] == 4
    assert events[-1] == "campaign.done"  # announced after the per-call events
    # the owner can see delivery status through the API
    listed = member.get("/api/webhooks/deliveries", params={"campaign_id": cid}).json()
    assert len(listed) == 5 and {d["status"] for d in listed} == {"delivered"}


def test_deliveries_endpoint_is_scoped_to_the_owner(member):
    make_delivery("someone@else.dev")
    assert member.get("/api/webhooks/deliveries").json() == []
