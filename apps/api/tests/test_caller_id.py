from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from app.config import Settings, get_settings
from app.db.models import Call, CallEvent
from app.db.session import SessionLocal
from app.services.caller_id import caller_id_error, effective, normalize
from app.worker.worker import plan_call

from .test_campaigns import create
from .test_worker import _status


def cfg(**kw) -> Settings:
    return Settings(**kw)


def test_normalize_keeps_digits_and_a_leading_plus():
    assert normalize("+91 140-0000001") == "+911400000001"
    assert normalize("(0140) 000 0001") == "01400000001"
    assert normalize("") == "" and normalize("  ") == ""


def test_without_an_allow_list_anything_goes():
    assert caller_id_error("", cfg()) is None
    assert caller_id_error("+15551234567", cfg()) is None


@pytest.mark.parametrize(
    "caller_id,ok",
    [
        ("+911400000001", True),
        ("+91 1400000001", True),  # spaces are ignored: same number as the exact entry
        ("+9114", False),  # shorter than the +91140 prefix
        ("+911409999999", True),  # matches the +91140* prefix
        ("+919876543210", False),  # a personal mobile must never be presented
        ("", True),  # falls back to DEFAULT_CALLER_ID, which is allowed
    ],
)
def test_allow_list_exact_and_prefix(caller_id, ok):
    s = cfg(allowed_caller_ids="+911400000001, +91140*", default_caller_id="+911400000001")
    assert (caller_id_error(caller_id, s) is None) is ok


def test_missing_caller_id_is_an_error_when_enforced_and_no_default():
    s = cfg(allowed_caller_ids="+91140*")
    assert "required" in caller_id_error("", s)
    assert effective("", cfg(default_caller_id="+911400000001")) == "+911400000001"


def test_campaign_creation_enforces_the_allow_list(member, monkeypatch):
    monkeypatch.setattr(get_settings(), "allowed_caller_ids", "+91140*")
    bad = create(member, caller_id="+919876543210")
    assert bad.status_code == 422 and "not on this deployment's allowed list" in bad.json()["detail"]
    assert create(member).status_code == 422  # none given, no default
    ok = create(member, caller_id="+91 140 0000002")
    assert ok.status_code == 201 and ok.json()["campaign"]["caller_id"] == "+911400000002"


def test_default_caller_id_satisfies_the_requirement(member, monkeypatch):
    monkeypatch.setattr(get_settings(), "allowed_caller_ids", "+91140*")
    monkeypatch.setattr(get_settings(), "default_caller_id", "+911400000009")
    assert create(member).status_code == 201


def test_dialling_is_refused_if_the_allow_list_changed_after_the_campaign_was_made(member, monkeypatch):
    cid = create(member, csv="phone\n9876500001\n", caller_id="+919876543210").json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    with SessionLocal() as db:
        call = db.scalar(select(Call).where(Call.campaign_id == cid))
    monkeypatch.setattr(get_settings(), "allowed_caller_ids", "+91140*")  # tightened later
    noon = datetime(2026, 1, 5, 6, 30, tzinfo=UTC)
    assert plan_call(call.id, get_settings(), 5, now=noon).action == "drop"
    assert _status(call.id) == "failed"
    with SessionLocal() as db:
        ev = db.scalar(select(CallEvent).where(CallEvent.call_id == call.id, CallEvent.type == "caller_id_rejected"))
    assert ev is not None and "allowed list" in ev.payload["reason"]


def test_single_calls_use_the_default_caller_id(member, monkeypatch):
    monkeypatch.setattr(get_settings(), "default_caller_id", "+911400000009")
    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()
    noon = datetime(2026, 1, 5, 6, 30, tzinfo=UTC)
    plan = plan_call(call["id"], get_settings(), 5, now=noon)
    assert plan.action == "dial" and plan.call.caller_id == "+911400000009"
