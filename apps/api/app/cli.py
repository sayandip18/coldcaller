"""Admin CLI. There is no public signup, so the first admin is created here.

    uv run python -m app.cli create-admin --email you@example.com [--password ...]

If --password is omitted a strong random one is generated and printed once.
Existing users are left untouched unless --reset-password is passed.
"""

import argparse
import secrets
import sys

from sqlalchemy import select

from app.auth.security import MIN_PASSWORD_LENGTH, hash_password
from app.db.models import Role, User
from app.db.session import SessionLocal


def create_admin(email: str, password: str | None, name: str, reset: bool) -> int:
    email = email.lower()
    generated = password is None
    password = password or secrets.token_urlsafe(18)
    if len(password) < MIN_PASSWORD_LENGTH:
        print(f"Password must be at least {MIN_PASSWORD_LENGTH} characters", file=sys.stderr)
        return 1
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.email == email))
        if user and not reset:
            print(f"User {email} already exists (use --reset-password to change it)")
            return 0
        if user:
            user.password_hash = hash_password(password)
            user.role, user.is_active = Role.admin.value, True
        else:
            db.add(
                User(
                    email=email,
                    name=name,
                    password_hash=hash_password(password),
                    role=Role.admin.value,
                )
            )
        db.commit()
    print(f"Admin ready: {email}")
    if generated:
        print(f"Password: {password}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="app.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("create-admin")
    p.add_argument("--email", required=True)
    p.add_argument("--password")
    p.add_argument("--name", default="Admin")
    p.add_argument("--reset-password", action="store_true")
    args = parser.parse_args()
    return create_admin(args.email, args.password, args.name, args.reset_password)


if __name__ == "__main__":
    raise SystemExit(main())
