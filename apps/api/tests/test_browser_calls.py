import jwt
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.config import get_settings
from app.db.models import Call, DncNumber
from app.db.session import SessionLocal
from app.main import app
from app.queue import keys
from app.queue.producer import redis_client
from app.routers import test_calls

from .conftest import login, make_user
from .test_livekit import AGENT, active_call, post_event, status_of

BODY = {"script_prompt": "Hi {name}, selling {product}", "contact_name": "Asha", "custom_fields": {"product": "solar"}}


def start(client, **overrides):
    return client.post("/api/test-calls", json=BODY | overrides)


class Dispatches:
    def __init__(self, fail: Exception | None = None):
        self.sent, self.fail = [], fail

    async def __call__(self, room, agent_name, metadata):
        if self.fail:
            raise self.fail
        self.sent.append((room, agent_name, metadata))


def test_requires_login():
    c = TestClient(app)
    assert c.post("/api/test-calls", json=BODY).status_code == 401
    assert c.post("/api/test-calls/x/agent").status_code == 401


def test_start_returns_a_token_that_joins_exactly_one_room_as_the_customer(member):
    r = start(member)
    assert r.status_code == 201, r.text
    body = r.json()
    s = get_settings()
    claims = jwt.decode(body["token"], s.livekit_api_secret, algorithms=["HS256"])
    assert claims["sub"] == body["identity"] == f"sip-{body['call_id']}"  # the agent treats sip-* as the customer
    assert claims["video"]["room"] == body["room"] == f"call-{body['call_id']}"
    assert claims["video"]["roomJoin"] and claims["video"]["canPublish"]
    assert not claims["video"].get("roomAdmin") and not claims["video"].get("roomCreate")
    assert claims["exp"] - claims["nbf"] <= 600
    assert body["url"] == s.livekit_browser_url


def test_test_call_is_recorded_but_never_queued_or_dialled(member):
    call_id = start(member).json()["call_id"]
    with SessionLocal() as db:
        call = db.get(Call, call_id)
    assert call.is_test and call.status == "answered" and call.to_number == "browser-test"
    assert call.script_prompt == BODY["script_prompt"] and call.custom_fields == {"product": "solar"}
    r = redis_client()
    assert r.xlen(keys.STREAM) == 0 and r.scard(keys.ACTIVE) == 0 and r.zcard(keys.GLOBAL_CHANNELS) == 0


def test_test_calls_can_be_filtered_in_and_out_of_call_lists(member):
    member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "x"})
    start(member)
    tests = member.get("/api/calls", params={"is_test": "true"}).json()
    real = member.get("/api/calls", params={"is_test": "false"}).json()
    assert tests["total"] == 1 and tests["items"][0]["is_test"] is True
    assert real["total"] == 1 and real["items"][0]["is_test"] is False
    assert member.get("/api/calls").json()["total"] == 2


def test_agent_is_dispatched_once_into_the_callers_room(member, monkeypatch):
    sent = Dispatches()
    monkeypatch.setattr(test_calls, "_dispatch", sent)
    call_id = start(member).json()["call_id"]
    assert member.post(f"/api/test-calls/{call_id}/agent").json() == {"status": "dispatched"}
    assert member.post(f"/api/test-calls/{call_id}/agent").json() == {"status": "already_dispatched"}
    assert len(sent.sent) == 1  # a double click must not put two agents on the line
    room, agent_name, metadata = sent.sent[0]
    assert room == f"call-{call_id}" and agent_name == get_settings().livekit_agent_name
    assert '"call_id"' in metadata and call_id in metadata


def test_dispatch_failure_fails_the_call_and_explains(member, monkeypatch):
    monkeypatch.setattr(test_calls, "_dispatch", Dispatches(fail=ConnectionError("refused")))
    call_id = start(member).json()["call_id"]
    r = member.post(f"/api/test-calls/{call_id}/agent")
    assert r.status_code == 502 and "pnpm infra:livekit" in r.json()["detail"]
    assert status_of(call_id) == "failed"


def test_only_the_owner_can_use_a_test_call_and_real_calls_are_not_test_calls(member, monkeypatch):
    monkeypatch.setattr(test_calls, "_dispatch", Dispatches())
    call_id = start(member).json()["call_id"]
    make_user("other@test.dev")
    other = login("other@test.dev")
    assert other.post(f"/api/test-calls/{call_id}/agent").status_code == 404
    real = active_call("owner@test.dev")  # a real (non-test) call
    owner = login("owner@test.dev")
    assert owner.post(f"/api/test-calls/{real.id}/agent").status_code == 404


def test_agent_cannot_be_dispatched_into_a_finished_call(member, monkeypatch):
    sent = Dispatches()
    monkeypatch.setattr(test_calls, "_dispatch", sent)
    call_id = start(member).json()["call_id"]
    post_event(TestClient(app), call_id, "room_finished")  # the browser left before the agent started
    assert status_of(call_id) == "completed"
    assert member.post(f"/api/test-calls/{call_id}/agent").status_code == 409
    assert sent.sent == []


def test_twenty_test_calls_an_hour_is_the_limit(member):
    assert all(start(member).status_code == 201 for _ in range(20))
    assert start(member).status_code == 429


def test_webhooks_drive_a_test_call_like_any_other(member):
    c = TestClient(app)
    call_id = start(member).json()["call_id"]
    post_event(c, call_id, "participant_joined", "agent-1", "AGENT")
    assert status_of(call_id) == "in_progress"
    post_event(c, call_id, "participant_left", f"sip-{call_id}", "STANDARD", eid="left1")
    post_event(c, call_id, "room_finished")
    assert status_of(call_id) == "completed"


def test_a_do_not_call_outcome_on_a_test_call_does_not_block_anything(member):
    call_id = start(member).json()["call_id"]
    r = TestClient(app).patch(
        f"/api/agent/calls/{call_id}/outcome",
        headers=AGENT,
        json={"result": "do_not_call", "do_not_call": True, "transcript": "Customer: stop calling"},
    )
    assert r.status_code == 200
    with SessionLocal() as db:
        assert list(db.scalars(select(DncNumber))) == []
        assert db.get(Call, call_id).outcome["do_not_call"] is True  # still recorded on the call
