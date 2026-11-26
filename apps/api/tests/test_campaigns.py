from .conftest import login, make_user

CSV = (
    "name,phone,city\n"
    "Asha,98765 43210,Pune\n"
    "Ravi,+91 91234 56789,Delhi\n"
    "Dup,9876543210,Pune\n"
    "Bad,12345,Nowhere\n"
    "Foreign,+14155552671,SF\n"
    ",,\n"
)


def create(client, csv=CSV, **overrides):
    data = {"name": "Q4 outreach", "script_prompt": "Hi {name} from {city}"} | overrides
    return client.post(
        "/api/campaigns", data=data, files={"file": ("c.csv", csv.encode(), "text/csv")}
    )


def test_create_campaign_parses_csv(member):
    r = create(member)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["accepted"] == 2
    reasons = {x["phone"]: x["reason"] for x in body["rejected"]}
    assert reasons["9876543210"] == "duplicate"
    assert "12345" in reasons and "+14155552671" in reasons
    assert body["campaign"]["status"] == "draft"
    assert body["campaign"]["counts"] == {"draft": 2}

    calls = member.get("/api/calls", params={"campaign_id": body["campaign"]["id"]}).json()
    assert calls["total"] == 2
    asha = next(c for c in calls["items"] if c["contact_name"] == "Asha")
    assert asha["to_number"] == "+919876543210"
    assert asha["custom_fields"] == {"city": "Pune"}


def test_csv_without_phone_column(member):
    assert create(member, csv="name\nA\n").status_code == 422


def test_csv_with_no_valid_rows(member):
    assert create(member, csv="phone\n123\n").status_code == 422


def test_window_must_be_within_trai_hours(member):
    assert create(member, call_window_start="08:00").status_code == 422
    assert create(member, call_window_end="22:00").status_code == 422
    assert create(member, call_window_start="10:00", call_window_end="09:30").status_code == 422
    assert create(member, call_window_start="10:00", call_window_end="18:00").status_code == 201


def test_bad_timezone(member):
    assert create(member, timezone="Mars/Base").status_code == 422


def test_start_queues_calls_and_skips_dnc(member):
    cid = create(member).json()["campaign"]["id"]
    assert member.post("/api/dnc", json={"number": "91234 56789"}).status_code == 201
    started = member.post(f"/api/campaigns/{cid}/start")
    assert started.status_code == 200
    body = started.json()
    assert body["status"] == "running"
    assert body["counts"] == {"queued": 1, "skipped_dnc": 1}
    assert member.post(f"/api/campaigns/{cid}/start").status_code == 409


def test_pause_resume_and_delete_rules(member):
    cid = create(member).json()["campaign"]["id"]
    assert member.post(f"/api/campaigns/{cid}/pause").status_code == 409
    member.post(f"/api/campaigns/{cid}/start")
    assert member.post(f"/api/campaigns/{cid}/pause").json()["status"] == "paused"
    assert member.post(f"/api/campaigns/{cid}/resume").json()["status"] == "running"
    assert member.delete(f"/api/campaigns/{cid}").status_code == 409

    other = create(member).json()["campaign"]["id"]
    assert member.delete(f"/api/campaigns/{other}").status_code == 204
    assert member.get(f"/api/campaigns/{other}").status_code == 404


def test_data_is_scoped_to_owner(member):
    cid = create(member).json()["campaign"]["id"]
    call_id = member.get("/api/calls").json()["items"][0]["id"]
    make_user("other@test.dev")
    other = login("other@test.dev")
    assert other.get("/api/campaigns").json() == []
    assert other.get(f"/api/campaigns/{cid}").status_code == 404
    assert other.post(f"/api/campaigns/{cid}/start").status_code == 404
    assert other.get(f"/api/calls/{call_id}").status_code == 404
    assert other.get("/api/calls").json()["total"] == 0
