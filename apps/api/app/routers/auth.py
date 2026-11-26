from fastapi import APIRouter, HTTPException, Request, Response, status
from sqlalchemy import select

from app.auth import ratelimit
from app.auth.deps import DB, AdminUser, CurrentUser
from app.auth.security import (
    create_session,
    hash_password,
    revoke_all_sessions,
    revoke_session,
    verify_password,
)
from app.config import get_settings
from app.db.models import Role, User
from app.schemas.api import LoginIn, UserCreate, UserOut, UserUpdate

router = APIRouter(tags=["auth"])


@router.post("/auth/login", response_model=UserOut)
def login(body: LoginIn, request: Request, response: Response, db: DB) -> User:
    email = body.email.lower()
    ip = request.client.host if request.client else "unknown"
    wait = ratelimit.locked_for(email, ip)
    if wait:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "Too many failed attempts. Try again later.",
            headers={"Retry-After": str(wait)},
        )
    user = db.scalar(select(User).where(User.email == email))
    ok = verify_password(body.password, user.password_hash if user else None)
    if not ok or user is None or not user.is_active:
        ratelimit.record_failure(email, ip)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid email or password")
    ratelimit.record_success(email, ip)
    settings = get_settings()
    token = create_session(db, user)
    response.set_cookie(
        settings.session_cookie_name,
        token,
        max_age=settings.session_ttl_hours * 3600,
        httponly=True,
        samesite="lax",
        secure=settings.cookie_secure,
        path="/",
    )
    return user


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(request: Request, response: Response, db: DB) -> None:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if token:
        revoke_session(db, token)
    response.delete_cookie(settings.session_cookie_name, path="/")


@router.get("/auth/me", response_model=UserOut)
def me(user: CurrentUser) -> User:
    return user


# ---- admin-only user management (there is no public signup) ----
@router.get("/users", response_model=list[UserOut])
def list_users(_: AdminUser, db: DB) -> list[User]:
    return list(db.scalars(select(User).order_by(User.created_at)))


@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(body: UserCreate, _: AdminUser, db: DB) -> User:
    email = body.email.lower()
    if db.scalar(select(User.id).where(User.email == email)):
        raise HTTPException(status.HTTP_409_CONFLICT, "Email already in use")
    user = User(
        email=email, name=body.name, password_hash=hash_password(body.password), role=body.role
    )
    db.add(user)
    db.commit()
    return user


@router.patch("/users/{user_id}", response_model=UserOut)
def update_user(user_id: str, body: UserUpdate, admin: AdminUser, db: DB) -> User:
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if user.id == admin.id and (body.is_active is False or body.role == Role.member.value):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "You cannot demote or disable yourself")
    if body.name is not None:
        user.name = body.name
    if body.role is not None:
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active
    if body.password is not None:
        user.password_hash = hash_password(body.password)
    db.commit()
    if body.is_active is False or body.password is not None:
        revoke_all_sessions(db, user.id)
    return user
