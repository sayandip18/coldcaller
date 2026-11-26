from fastapi.testclient import TestClient

from app.main import app

from .conftest import PASSWORD, login, make_user


def test_unauthenticated_is_rejected():
    c = TestClient(app)
    assert c.get("/api/auth/me").status_code == 401
    assert c.get("/api/campaigns").status_code == 401
    assert c.get("/api/calls").status_code == 401


def test_login_me_logout(admin):
    me = admin.get("/api/auth/me")
    assert me.status_code == 200 and me.json()["email"] == "admin@test.dev"
    assert "password" not in me.text
    assert admin.post("/api/auth/logout").status_code == 204
    assert admin.get("/api/auth/me").status_code == 401


def test_bad_credentials():
    make_user("u@test.dev")
    c = TestClient(app)
    assert c.post("/api/auth/login", json={"email": "u@test.dev", "password": "nope"}).status_code == 401
    assert c.post("/api/auth/login", json={"email": "x@test.dev", "password": PASSWORD}).status_code == 401


def test_no_public_signup():
    c = TestClient(app)
    r = c.post("/api/users", json={"email": "a@b.dev", "password": "longenoughpw1"})
    assert r.status_code == 401


def test_member_cannot_manage_users(member):
    assert member.get("/api/users").status_code == 403
    r = member.post("/api/users", json={"email": "a@b.dev", "password": "longenoughpw1"})
    assert r.status_code == 403


def test_admin_creates_user_who_can_login(admin):
    r = admin.post(
        "/api/users", json={"email": "New@Test.dev", "password": "longenoughpw1", "name": "N"}
    )
    assert r.status_code == 201 and r.json()["email"] == "new@test.dev"
    c = TestClient(app)
    assert c.post(
        "/api/auth/login", json={"email": "new@test.dev", "password": "longenoughpw1"}
    ).status_code == 200
    dup = admin.post("/api/users", json={"email": "new@test.dev", "password": "longenoughpw1"})
    assert dup.status_code == 409


def test_short_password_rejected(admin):
    r = admin.post("/api/users", json={"email": "s@test.dev", "password": "short"})
    assert r.status_code == 422


def test_disabling_user_kills_sessions(admin):
    user = make_user("victim@test.dev")
    victim = login("victim@test.dev")
    assert admin.patch(f"/api/users/{user.id}", json={"is_active": False}).status_code == 200
    assert victim.get("/api/auth/me").status_code == 401


def test_admin_cannot_disable_self(admin):
    me = admin.get("/api/auth/me").json()
    assert admin.patch(f"/api/users/{me['id']}", json={"is_active": False}).status_code == 400
