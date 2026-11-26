import pytest

from app.services.phone import normalize_in_number


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("9876543210", "+919876543210"),
        ("+91 98765-43210", "+919876543210"),
        ("09876543210", "+919876543210"),
        ("919876543210", "+919876543210"),
    ],
)
def test_normalize_ok(raw, expected):
    assert normalize_in_number(raw) == expected


@pytest.mark.parametrize("raw", ["", "abc", "12345", "+14155552671", "0000000000"])
def test_normalize_rejects(raw):
    with pytest.raises(ValueError):
        normalize_in_number(raw)


BODY = {"to_number": "98765 43210", "contact_name": "Asha", "script_prompt": "Hello"}


def test_single_call_is_queued_with_events(member):
    r = member.post("/api/calls", json=BODY)
    assert r.status_code == 201, r.text
    call = r.json()
    assert call["status"] == "queued" and call["to_number"] == "+919876543210"
    detail = member.get(f"/api/calls/{call['id']}").json()
    assert [e["type"] for e in detail["events"]] == ["created", "queued"]


def test_single_call_invalid_number(member):
    assert member.post("/api/calls", json=BODY | {"to_number": "123"}).status_code == 422


def test_idempotency_key_returns_same_call(member):
    h = {"Idempotency-Key": "abc-1"}
    first = member.post("/api/calls", json=BODY, headers=h)
    second = member.post("/api/calls", json=BODY, headers=h)
    assert first.status_code == 201 and second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert member.get("/api/calls").json()["total"] == 1


def test_single_call_to_dnc_number_is_skipped(member):
    member.post("/api/dnc", json={"number": "9876543210"})
    call = member.post("/api/calls", json=BODY).json()
    assert call["status"] == "skipped_dnc"


def test_dnc_crud(member):
    r = member.post("/api/dnc", json={"number": "9876543210"})
    assert r.status_code == 201
    assert member.post("/api/dnc", json={"number": "+91 98765 43210"}).status_code == 409
    assert member.post("/api/dnc", json={"number": "bad"}).status_code == 422
    assert len(member.get("/api/dnc").json()) == 1
    assert member.delete(f"/api/dnc/{r.json()['id']}").status_code == 204
    assert member.get("/api/dnc").json() == []
