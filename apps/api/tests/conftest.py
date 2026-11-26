import os

# Must be set before the app (and its engine) is imported.
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://coldcaller:coldcaller@localhost:5432/coldcaller_test",
)

os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://localhost:6380/15")

import pytest
import redis as redis_sync
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.auth.security import hash_password
from app.db.models import Base, Role, User
from app.db.session import SessionLocal, engine
from app.main import app

PASSWORD = "correct-horse-battery"


@pytest.fixture(scope="session", autouse=True)
def _schema():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture(autouse=True)
def _clean():
    redis_sync.Redis.from_url(os.environ["REDIS_URL"]).flushdb()
    yield
    redis_sync.Redis.from_url(os.environ["REDIS_URL"]).flushdb()
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE call_events, calls, campaigns, dnc_numbers, sessions, users CASCADE"
            )
        )


def make_user(email: str, role: str = Role.member.value) -> User:
    with SessionLocal() as db:
        user = User(email=email, password_hash=hash_password(PASSWORD), role=role)
        db.add(user)
        db.commit()
        return user


def login(email: str) -> TestClient:
    client = TestClient(app)
    r = client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return client


@pytest.fixture
def admin() -> TestClient:
    make_user("admin@test.dev", Role.admin.value)
    return login("admin@test.dev")


@pytest.fixture
def member() -> TestClient:
    make_user("member@test.dev")
    return login("member@test.dev")
