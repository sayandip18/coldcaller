import asyncio
import hashlib
import json
import os

import pytest
from livekit import api
from redis.asyncio import Redis
from sqlalchemy import select

from app.config import get_settings
from app.db.models import Call, CallEvent, CallStatus, DncNumber
from app.db.session import SessionLocal
from app.livekit_client import classify_twirp_error
from app.queue import keys
from app.queue.primitives import Channels
from app.services.lifecycle import mark
from app.worker.dialer import DialRequest, LiveKitDialer
from app.worker.worker import ended_leases

from .conftest import make_user

SECRET = get_settings().agent_shared_secret
AGENT = {"X-Agent-Secret": SECRET}


def twirp(
    code: str, sip: int | None = None, message: str = "", status: int = 500
) -> api.TwirpError:
    meta = {"sip_status_code": str(sip)} if sip else {}
    return api.TwirpError(code, message, status=status, metadata=meta)


# ---------------------------------------------------------------- error mapping
@pytest.mark.parametrize(
    "exc,kind",
    [
        (twirp("unknown", 486, "Busy Here"), "busy"),
        (twirp("unknown", 603, "Decline"), "busy"),
        (twirp("unknown", 480), "no_answer"),
        (twirp("unknown", 408), "no_answer"),
        (twirp("deadline_exceeded"), "no_answer"),
        # shapes captured from a real livekit-sip run
        (twirp("canceled", message="sip request timed out", status=408), "no_answer"),
        (twirp("failed_precondition", 486, "INVITE failed: sip status: 486: Busy Here", 412), "busy"),
        (twirp("permission_denied", 603, "unexpected status from INVITE response", 403), "busy"),
        (twirp("unknown", 404), "failed"),
    ],
)
def test_sip_errors_are_classified(exc, kind):
    assert classify_twirp_error(exc).kind == kind


@pytest.mark.parametrize("sip", [401, 403, 407, 500, 502, 503, 504])
def test_carrier_refusals_and_errors_are_infrastructure_not_the_customers_fault(sip):
    """The carrier refusing us, erroring, or being unreachable (the bridge answers 503) must be
    retried and dead-lettered, never recorded as a failed call to that customer."""
    assert classify_twirp_error(twirp("failed_precondition", sip, status=412)) is None


def test_error_without_sip_status_is_treated_as_infrastructure():
    assert classify_twirp_error(twirp("unavailable", message="connection refused")) is None
    # unknown trunk id is a config problem, not an unanswered call
    missing = twirp("not_found", message="requested sip trunk does not exist", status=404)
    assert classify_twirp_error(missing) is None


# ------------------------------------------------------------------ dialer logic
class FakeLiveKit:
    def __init__(
        self,
        dial_error: Exception | None = None,
        dispatch_error: Exception | None = None,
        agent_joins: bool = True,
        dispatch_failures: int = 0,
    ):
        self.dial_error, self.dispatch_error = dial_error, dispatch_error
        self.dispatch_failures = dispatch_failures  # first N dispatches fail, then succeed
        self.agent_joins = agent_joins
        self.calls: list = []

    async def dial(self, **kw):
        self.calls.append(("dial", kw))
        if self.dial_error:
            raise self.dial_error

    async def dispatch_agent(self, room, agent_name, metadata):
        self.calls.append(("dispatch", room, agent_name, json.loads(metadata)))
        if self.dispatch_failures > 0:
            self.dispatch_failures -= 1
            raise RuntimeError("could not find any available nodes")
        if self.dispatch_error:
            raise self.dispatch_error
        if self.agent_joins:  # what the participant_joined webhook does when the agent connects
            call_id = json.loads(metadata)["call_id"]
            with SessionLocal() as db:
                mark(db, call_id, [CallStatus.answered], CallStatus.in_progress, event="agent_joined")

    async def delete_room(self, room):
        self.calls.append(("delete_room", room))

    async def aclose(self):
        pass


def make_dialing_call(member_email="m@test.dev") -> Call:
    user = make_user(member_email)
    with SessionLocal() as db:
        call = Call(owner_id=user.id, to_number="+919876500001", script_prompt="hi", status="queued")
        db.add(call)
        db.commit()
        assert mark(db, call.id, [CallStatus.queued], CallStatus.dialing, dialed_at=None)
        return call


def run_dialer(client: FakeLiveKit, call: Call, join_timeout: float = 2.0):
    released, errors = [], []

    async def release(cid, camp):
        released.append(cid)

    async def on_error(req, msg):
        errors.append(msg)

    async def main():
        d = LiveKitDialer(
            release, on_error, client, trunk_id="ST_x", agent_name="coldcaller-agent",
            ring_timeout_s=30, max_call_s=600, agent_join_timeout_s=join_timeout,
            dispatch_retry_delays=(0.0, 0.0),
        )
        req = DialRequest(call.id, call.to_number, "+911111111111", None, f"call-{call.id}")
        await d.dial(req)
        await asyncio.gather(*d._tasks)

    asyncio.run(main())
    return released, errors


def status_of(call_id: str) -> str:
    with SessionLocal() as db:
        return db.get(Call, call_id).status


def test_answered_call_dispatches_agent_and_keeps_its_slot():
    call = make_dialing_call()
    lk = FakeLiveKit()
    released, errors = run_dialer(lk, call)
    assert status_of(call.id) == "in_progress"  # the agent joined
    assert released == [] and errors == []  # slot held until the call ends
    kinds = [c[0] for c in lk.calls]
    assert kinds == ["dial", "dispatch"]
    assert lk.calls[0][1]["identity"] == f"sip-{call.id}"
    assert lk.calls[1][3] == {"call_id": call.id}


@pytest.mark.parametrize(
    "exc,expected", [(twirp("unknown", 486), "busy"), (twirp("unknown", 480), "no_answer"), (twirp("unknown", 404), "failed")]
)
def test_carrier_refusals_end_the_call_and_free_the_slot(exc, expected):
    call = make_dialing_call()
    released, errors = run_dialer(FakeLiveKit(dial_error=exc), call)
    assert status_of(call.id) == expected
    assert released == [call.id] and errors == []


def test_livekit_outage_is_reported_for_retry_not_marked_failed():
    call = make_dialing_call()
    _released, errors = run_dialer(FakeLiveKit(dial_error=ConnectionError("refused")), call)
    assert status_of(call.id) == "dialing"  # the worker's retry path decides what happens next
    assert len(errors) == 1 and "unreachable" in errors[0]


def test_agent_dispatch_failure_hangs_up_and_fails_the_call():
    call = make_dialing_call()
    lk = FakeLiveKit(dispatch_error=RuntimeError("no agents"))
    released, _ = run_dialer(lk, call)
    assert status_of(call.id) == "failed"
    assert ("delete_room", f"call-{call.id}") in lk.calls and released == [call.id]


def test_a_momentary_dispatch_refusal_is_retried_and_the_call_goes_ahead():
    """'No available nodes' while every agent worker is busy starting one clears in a moment."""
    call = make_dialing_call()
    lk = FakeLiveKit(dispatch_failures=2)
    released, errors = run_dialer(lk, call)
    assert status_of(call.id) == "in_progress"  # the third attempt got through and the agent joined
    assert [c[0] for c in lk.calls].count("dispatch") == 3 and ("delete_room", f"call-{call.id}") not in lk.calls
    assert released == [] and errors == []


def test_agent_that_never_joins_gets_the_customer_hung_up_on_quickly():
    """LiveKit accepts a dispatch when no agent is running, then drops it."""
    call = make_dialing_call()
    lk = FakeLiveKit(agent_joins=False)
    released, _ = run_dialer(lk, call, join_timeout=0.5)
    assert status_of(call.id) == "failed"
    assert ("delete_room", f"call-{call.id}") in lk.calls  # the line is dropped, not left silent
    assert released == [call.id]
    with SessionLocal() as db:
        row = db.get(Call, call.id)
    assert row.failure_reason == "agent did not join the call in time"


def test_a_call_that_already_ended_is_not_failed_by_the_watchdog():
    """Customer hangs up (room_finished completes the call) before the join check runs."""
    call = make_dialing_call()

    class Hangup(FakeLiveKit):
        async def dispatch_agent(self, room, agent_name, metadata):
            await super().dispatch_agent(room, agent_name, metadata)
            with SessionLocal() as db:
                mark(db, call.id, [CallStatus.answered, CallStatus.in_progress], CallStatus.completed)

    released, _ = run_dialer(Hangup(agent_joins=False), call, join_timeout=0.5)
    assert status_of(call.id) == "completed" and released == []


# --------------------------------------------------------------------- webhooks
def signed(body: dict) -> tuple[bytes, dict]:
    s = get_settings()
    raw = json.dumps(body)
    sha = hashlib.sha256(raw.encode()).digest()
    import base64

    token = (
        api.AccessToken(s.livekit_api_key, s.livekit_api_secret)
        .with_grants(api.VideoGrants())
        .with_sha256(base64.b64encode(sha).decode())
        .to_jwt()
    )
    return raw.encode(), {"Authorization": token, "Content-Type": "application/webhook+json"}


def post_event(client, call_id, event, identity="", kind="STANDARD", eid=None):
    body = {
        "event": event,
        "id": eid or f"EV_{event}_{identity}",
        "room": {"name": f"call-{call_id}"},
        "participant": {"identity": identity, "kind": kind},
    }
    raw, headers = signed(body)
    return client.post("/api/livekit/webhook", content=raw, headers=headers)


def active_call(member_email="m@test.dev", status=CallStatus.answered, lease=True) -> Call:
    call = make_dialing_call(member_email)
    with SessionLocal() as db:
        mark(db, call.id, [CallStatus.dialing], status)
    if lease:
        import redis as sync

        sync.Redis.from_url(os.environ["REDIS_URL"]).zadd(keys.GLOBAL_CHANNELS, {call.id: 9_999_999_999_999})
    return call


def lease_count() -> int:
    import redis as sync

    return sync.Redis.from_url(os.environ["REDIS_URL"]).zcard(keys.GLOBAL_CHANNELS)


def test_webhook_rejects_bad_signature(member):
    raw, _ = signed({"event": "room_finished", "room": {"name": "call-x"}})
    from fastapi.testclient import TestClient

    from app.main import app

    c = TestClient(app)
    assert c.post("/api/livekit/webhook", content=raw, headers={"Authorization": "nope"}).status_code == 401
    assert c.post("/api/livekit/webhook", content=raw).status_code == 401


def test_agent_joining_marks_in_progress_and_room_finish_completes_and_frees_slot():
    from fastapi.testclient import TestClient

    from app.main import app

    c = TestClient(app)
    call = active_call()
    assert lease_count() == 1
    assert post_event(c, call.id, "participant_joined", "agent-abc", "AGENT").status_code == 200
    assert status_of(call.id) == "in_progress"
    post_event(c, call.id, "participant_left", f"sip-{call.id}", "SIP", eid="EV_left")
    post_event(c, call.id, "participant_left", f"sip-{call.id}", "SIP", eid="EV_left")  # LiveKit retry
    post_event(c, call.id, "room_finished")
    assert status_of(call.id) == "completed"
    assert lease_count() == 0
    with SessionLocal() as db:
        types = list(db.scalars(select(CallEvent.type).where(CallEvent.call_id == call.id).order_by(CallEvent.id)))
    assert types.count("customer_hangup") == 1  # duplicate delivery recorded once
    assert types[-1] == "room_finished"


def test_room_finished_does_not_overwrite_a_call_that_already_failed():
    from fastapi.testclient import TestClient

    from app.main import app

    call = active_call(status=CallStatus.answered)
    with SessionLocal() as db:
        mark(db, call.id, [CallStatus.answered], CallStatus.failed, failure_reason="x")
    post_event(TestClient(app), call.id, "room_finished")
    assert status_of(call.id) == "failed" and lease_count() == 0


# ------------------------------------------------------------------ agent callbacks
def test_agent_endpoints_need_the_shared_secret(member):
    call = active_call()
    assert member.get(f"/api/agent/calls/{call.id}").status_code == 401
    assert member.get(f"/api/agent/calls/{call.id}", headers={"X-Agent-Secret": "wrong"}).status_code == 401
    ok = member.get(f"/api/agent/calls/{call.id}", headers=AGENT)
    assert ok.status_code == 200 and ok.json()["script_prompt"] == "hi"


def test_outcome_completes_a_live_call():
    from fastapi.testclient import TestClient

    from app.main import app

    c = TestClient(app)
    call = active_call(status=CallStatus.in_progress)
    r = c.patch(
        f"/api/agent/calls/{call.id}/outcome",
        headers=AGENT,
        json={"result": "interested", "notes": "wants demo", "transcript": "A: hi\nB: hello"},
    )
    assert r.status_code == 200 and r.json()["status"] == "completed"
    with SessionLocal() as db:
        row = db.get(Call, call.id)
        assert row.outcome["result"] == "interested" and row.transcript.startswith("A: hi")
        assert row.ended_at is not None


def test_outcome_after_hangup_webhook_still_attaches_to_the_completed_call():
    from fastapi.testclient import TestClient

    from app.main import app

    c = TestClient(app)
    call = active_call(status=CallStatus.in_progress)
    post_event(c, call.id, "room_finished")
    assert status_of(call.id) == "completed"
    r = c.patch(f"/api/agent/calls/{call.id}/outcome", headers=AGENT, json={"result": "callback"})
    assert r.status_code == 200
    with SessionLocal() as db:
        assert db.get(Call, call.id).outcome["result"] == "callback"


def test_do_not_call_outcome_adds_number_to_the_owners_dnc_list():
    from fastapi.testclient import TestClient

    from app.main import app

    c = TestClient(app)
    call = active_call()
    body = {"result": "not_interested", "do_not_call": True}
    assert c.patch(f"/api/agent/calls/{call.id}/outcome", headers=AGENT, json=body).status_code == 200
    assert c.patch(f"/api/agent/calls/{call.id}/outcome", headers=AGENT, json=body).status_code == 200  # idempotent
    with SessionLocal() as db:
        rows = list(db.scalars(select(DncNumber)))
    assert len(rows) == 1 and rows[0].number == "+919876500001" and rows[0].source == "user_request"


def test_outcome_for_unknown_call_is_404():
    from fastapi.testclient import TestClient

    from app.main import app

    r = TestClient(app).patch("/api/agent/calls/nope/outcome", headers=AGENT, json={"result": "x"})
    assert r.status_code == 404


# ------------------------------------------------------------ lease reconciliation
def test_leases_of_finished_or_unknown_calls_are_found():
    live = active_call("a@test.dev", lease=False)
    done = active_call("b@test.dev", lease=False)
    with SessionLocal() as db:
        mark(db, done.id, [CallStatus.answered], CallStatus.completed)
    ended = dict(ended_leases([live.id, done.id, "ghost"]))
    assert set(ended) == {done.id, "ghost"}


def test_sweeper_releases_a_lease_whose_hangup_webhook_was_lost():
    from app.worker.worker import Worker

    call = active_call(lease=False)
    with SessionLocal() as db:
        mark(db, call.id, [CallStatus.answered], CallStatus.completed)

    async def scenario():
        r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        try:
            ch = Channels(r, 10, 600)
            assert await ch.acquire(call.id, None, None)
            w = Worker(r, dialer_factory=lambda release, on_error: None)
            await w.q.ensure_group()
            await w.sweep_once()
            return await r.zcard(keys.GLOBAL_CHANNELS)
        finally:
            await r.aclose()

    assert asyncio.run(scenario()) == 0
