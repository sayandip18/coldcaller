import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.config import get_settings
from app.db.models import Call, CallEvent, CallStatus, NcprNumber, User
from app.db.session import SessionLocal
from app.main import app
from app.queue import keys
from app.queue.producer import redis_client
from app.services.alerts import LogSink, SnsSink, alert_campaign_finished, get_sink
from app.services.dlq import list_dlq, replay_dlq
from app.services.lifecycle import mark
from app.worker.worker import plan_call

from .conftest import PASSWORD, make_user
from .test_campaigns import create
from .test_worker import (
    TERMINAL,
    _status,
    fast_dialer,
    numbers_csv,
    run_worker,
    wait_for,
)

# ----------------------------------------------------------------------- NCPR


def import_ncpr(client, text: str):
    return client.post("/api/ncpr/import", files={"file": ("ncpr.csv", text.encode(), "text/csv")})


def test_ncpr_import_is_admin_only_and_reports_counts(admin, member):
    r = import_ncpr(admin, "9876500001\n9876500002,extra,columns\nnot-a-number\n\n9876500001\n+14155552671\n")
    assert r.status_code == 200 and r.json() == {"added": 2, "rejected": 2}
    assert admin.get("/api/ncpr").json() == {"count": 2}
    assert import_ncpr(member, "9876500003").status_code == 403
    assert member.get("/api/ncpr").status_code == 403
    assert TestClient(app).post("/api/ncpr/import", files={"file": ("x", b"1")}).status_code == 401
    # importing again adds nothing new
    assert import_ncpr(admin, "9876500001").json()["added"] == 0


def add_ncpr(*numbers: str) -> None:
    with SessionLocal() as db:
        db.add_all(NcprNumber(number=n) for n in numbers)
        db.commit()


def reason_of(call_id: str) -> str | None:
    with SessionLocal() as db:
        ev = db.scalar(select(CallEvent).where(CallEvent.call_id == call_id, CallEvent.type == "skipped_dnc"))
        return ev.payload.get("reason") if ev else None


def test_single_call_to_an_ncpr_number_is_not_dialled(member):
    add_ncpr("+919876500001")
    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()
    assert call["status"] == "skipped_dnc" and reason_of(call["id"]) == "ncpr"


def test_own_dnc_is_reported_separately_from_ncpr(member):
    member.post("/api/dnc", json={"number": "9876500001"})
    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()
    assert reason_of(call["id"]) == "dnc"


def test_campaign_start_skips_ncpr_numbers(member):
    add_ncpr("+919876500002")
    cid = create(member, csv="phone\n9876500001\n9876500002\n9876500003\n").json()["campaign"]["id"]
    body = member.post(f"/api/campaigns/{cid}/start").json()
    assert body["counts"] == {"queued": 2, "skipped_dnc": 1}


def test_number_registered_after_queueing_is_refused_at_dial_time(member):
    cid = create(member, csv="phone\n9876500001\n").json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    with SessionLocal() as db:
        call = db.scalar(select(Call).where(Call.campaign_id == cid))
    add_ncpr(call.to_number)
    noon = datetime(2026, 1, 5, 6, 30, tzinfo=UTC)
    assert plan_call(call.id, get_settings(), 5, now=noon).action == "drop"
    assert _status(call.id) == "skipped_dnc" and reason_of(call.id) == "ncpr"


def test_ncpr_check_can_be_switched_off(member, monkeypatch):
    monkeypatch.setattr(get_settings(), "ncpr_enforce", False)
    add_ncpr("+919876500001")
    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()
    assert call["status"] == "queued"


# ------------------------------------------------------------------ SNS alerts
class FakeSns:
    def __init__(self, fail=False):
        self.fail, self.published = fail, []

    def publish(self, **kw):
        if self.fail:
            raise RuntimeError("AccessDenied")
        self.published.append(kw)


def test_sns_sink_publishes_a_json_message_with_an_event_attribute():
    fake = FakeSns()
    SnsSink("arn:aws:sns:ap-south-1:1:topic", fake).publish("x" * 300, {"event": "campaign.done", "total": 3})
    sent = fake.published[0]
    assert sent["TopicArn"].endswith(":topic") and len(sent["Subject"]) == 100
    assert json.loads(sent["Message"]) == {"event": "campaign.done", "total": 3}
    assert sent["MessageAttributes"]["event"]["StringValue"] == "campaign.done"


def test_sink_is_chosen_from_settings():
    from app.config import Settings

    assert isinstance(get_sink(Settings(sns_topic_arn="")), LogSink)


def finished_campaign(member) -> str:
    cid = create(member, csv=numbers_csv(2)).json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    return cid


def test_alert_message_summarises_the_campaign(member):
    cid = finished_campaign(member)

    class Capture:
        def publish(self, subject, message):
            self.subject, self.message = subject, message

    sink = Capture()
    with SessionLocal() as db:
        assert alert_campaign_finished(db, cid, sink) is True
    assert sink.subject == "Campaign finished: Q4 outreach"
    assert sink.message["total"] == 2 and sink.message["counts"] == {"queued": 2}


def test_a_failing_alert_sink_never_raises(member):
    cid = finished_campaign(member)
    with SessionLocal() as db:
        assert alert_campaign_finished(db, cid, SnsSink("arn", FakeSns(fail=True))) is False


def test_worker_publishes_one_alert_when_a_campaign_finishes(member):
    cid = create(member, csv=numbers_csv(3), cps=50).json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    published = []

    class Capture:
        def publish(self, subject, message):
            published.append(message)

    async def scenario(r, worker):
        from .test_worker import campaign_status

        assert await wait_for(lambda: campaign_status(cid) == "done", timeout=20)
        import asyncio

        await asyncio.sleep(0.8)  # extra sweeps must not alert again

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100, alert_sink=Capture())
    assert len(published) == 1 and published[0]["campaign_id"] == cid and published[0]["total"] == 3


# ---------------------------------------------------------------- login limits
def attempt(client, email, password="wrong-password"):
    return client.post("/api/auth/login", json={"email": email, "password": password})


def test_five_failures_lock_that_email_and_ip_even_for_the_right_password():
    make_user("victim@test.dev")
    c = TestClient(app)
    assert [attempt(c, "victim@test.dev").status_code for _ in range(5)] == [401] * 5
    locked = attempt(c, "victim@test.dev", PASSWORD)  # correct password, still refused
    assert locked.status_code == 429 and int(locked.headers["Retry-After"]) > 0


def test_lock_applies_to_unknown_emails_too_so_it_leaks_nothing():
    c = TestClient(app)
    for _ in range(5):
        attempt(c, "nobody@test.dev")
    assert attempt(c, "nobody@test.dev").status_code == 429


def test_success_clears_the_failure_count():
    make_user("u@test.dev")
    c = TestClient(app)
    for _ in range(4):
        attempt(c, "u@test.dev")
    assert attempt(c, "u@test.dev", PASSWORD).status_code == 200
    for _ in range(4):  # a fresh allowance of four more failures
        assert attempt(c, "u@test.dev").status_code == 401


def test_one_ip_cannot_lock_a_user_out_from_a_different_ip():
    make_user("owner@test.dev")
    attacker = TestClient(app, client=("203.0.113.9", 5000))
    for _ in range(5):
        attempt(attacker, "owner@test.dev")
    assert attempt(attacker, "owner@test.dev", PASSWORD).status_code == 429
    owner = TestClient(app, client=("198.51.100.7", 5000))
    assert attempt(owner, "owner@test.dev", PASSWORD).status_code == 200


def test_one_ip_trying_many_accounts_is_stopped():
    c = TestClient(app, client=("203.0.113.50", 1))
    codes = [attempt(c, f"user{i}@test.dev").status_code for i in range(22)]
    assert codes[:20] == [401] * 20 and codes[20] == 429


def test_many_ips_guessing_one_account_are_stopped_at_the_email_limit():
    make_user("target@test.dev")
    for i in range(30):
        attempt(TestClient(app, client=(f"10.1.0.{i}", 1)), "target@test.dev")
    fresh = TestClient(app, client=("192.0.2.77", 1))
    assert attempt(fresh, "target@test.dev", PASSWORD).status_code == 429


def test_login_still_works_when_the_limiter_is_unavailable(monkeypatch):
    import redis

    from app.auth import ratelimit

    make_user("ok@test.dev")

    def down():
        raise redis.ConnectionError("redis is down")

    monkeypatch.setattr(ratelimit, "redis_client", down)
    assert attempt(TestClient(app), "ok@test.dev", PASSWORD).status_code == 200


# ------------------------------------------------------------------ DLQ replay
def dead_letter(email="m@test.dev", status=CallStatus.failed) -> str:
    user = make_user(email) if not _user_exists(email) else None
    with SessionLocal() as db:
        owner = user.id if user else db.scalar(select(User.id).where(User.email == email))
        call = Call(owner_id=owner, to_number="+919876500001", script_prompt="hi", status="dialing", attempt=3)
        db.add(call)
        db.commit()
        mark(db, call.id, [CallStatus.dialing], status, failure_reason="dial failed after 3 attempts: x")
    redis_client().xadd(keys.DLQ, {"call_id": call.id, "reason": "livekit down"})
    return call.id


def _user_exists(email: str) -> bool:
    with SessionLocal() as db:
        return db.scalar(select(User.id).where(User.email == email)) is not None


def test_replay_requeues_failed_calls_with_a_fresh_attempt_count():
    cid = dead_letter()
    listed = list_dlq()
    assert [(x["call_id"], x["status"], x["reason"]) for x in listed] == [(cid, "failed", "livekit down")]
    assert replay_dlq(dry_run=True) == [cid] and redis_client().xlen(keys.DLQ) == 1  # dry run changes nothing

    assert replay_dlq() == [cid]
    with SessionLocal() as db:
        call = db.get(Call, cid)
        events = list(db.scalars(select(CallEvent.type).where(CallEvent.call_id == cid)))
    assert call.status == "queued" and call.attempt == 0 and call.failure_reason is None
    assert call.ended_at is None and "replayed" in events
    assert redis_client().xlen(keys.DLQ) == 0
    assert [f["call_id"] for _, f in redis_client().xrange(keys.STREAM)] == [cid]  # back on the queue


def test_replay_ignores_entries_whose_call_is_no_longer_failed():
    cid = dead_letter(status=CallStatus.completed)
    assert replay_dlq() == []
    assert redis_client().xlen(keys.DLQ) == 0  # entry cleared, call untouched
    assert _status(cid) == "completed"


def test_replay_can_target_one_call():
    a, b = dead_letter("a@test.dev"), dead_letter("b@test.dev")
    assert replay_dlq(call_id=b) == [b]
    assert _status(a) == "failed" and _status(b) == "queued"
    assert [x["call_id"] for x in list_dlq()] == [a]


def test_calls_that_were_dead_lettered_complete_after_the_cause_is_fixed(member):
    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()

    async def broken(r, worker):
        assert await wait_for(lambda: _status(call["id"]) == "failed", timeout=20)

    run_worker(broken, dialer_factory=fast_dialer(dial_error_rate=1.0), carrier_cps=100)
    assert replay_dlq() == [call["id"]]

    async def fixed(r, worker):
        assert await wait_for(lambda: _status(call["id"]) in TERMINAL, timeout=20)

    run_worker(fixed, dialer_factory=fast_dialer(p_no_answer=0, p_busy=0, p_fail=0), carrier_cps=100)
    assert _status(call["id"]) == "completed"
