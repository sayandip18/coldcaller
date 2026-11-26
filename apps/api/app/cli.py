"""Admin CLI. There is no public signup, so the first admin is created here.

    uv run python -m app.cli create-admin --email you@example.com [--password ...]

If --password is omitted a strong random one is generated and printed once.
Existing users are left untouched unless --reset-password is passed.
"""

import argparse
import asyncio
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


def livekit_trunk(args: argparse.Namespace) -> int:
    """Create (or list) the LiveKit SIP outbound trunk the worker dials through."""
    from app.livekit_client import LiveKitClient

    async def run() -> int:
        client = LiveKitClient()
        try:
            if args.action == "list":
                for trunk_id, name, address in await client.list_outbound_trunks():
                    print(f"{trunk_id}  {name}  {address}")
                return 0
            trunk_id = await client.create_outbound_trunk(
                name=args.name,
                address=args.address,
                numbers=args.number,
                username=args.username or "",
                password=args.password or "",
            )
            print(f"Created outbound trunk: {trunk_id}")
            print(f"Set LIVEKIT_SIP_TRUNK_ID={trunk_id} and DIALER=livekit in apps/api/.env")
            return 0
        finally:
            await client.aclose()

    return asyncio.run(run())


def dlq_command(args: argparse.Namespace) -> int:
    from app.services.dlq import list_dlq, replay_dlq

    if args.action == "list":
        rows = list_dlq(args.limit)
        for row in rows:
            print(f"{row['call_id']}  {row['status']:<8} {row['to_number']}  {row['reason'][:80]}")
        print(f"{len(rows)} dead-lettered call(s)")
        return 0
    ids = replay_dlq(args.limit, args.call_id, args.dry_run)
    verb = "would replay" if args.dry_run else "replayed"
    print(f"{verb} {len(ids)} call(s)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="app.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("create-admin")
    p.add_argument("--email", required=True)
    p.add_argument("--password")
    p.add_argument("--name", default="Admin")
    p.add_argument("--reset-password", action="store_true")
    t = sub.add_parser("livekit-trunk", help="manage the LiveKit SIP outbound trunk")
    t.add_argument("action", choices=["create", "list"])
    t.add_argument("--name", default="coldcaller-outbound")
    t.add_argument("--address", help="carrier / SIP bridge host[:port], e.g. asterisk.internal:5060")
    t.add_argument("--number", action="append", default=[], help="caller-ID number(s); repeatable")
    t.add_argument("--username")
    t.add_argument("--password")
    d = sub.add_parser("dlq", help="inspect or replay calls the worker gave up on")
    d.add_argument("action", choices=["list", "replay"])
    d.add_argument("--limit", type=int, default=100)
    d.add_argument("--call-id")
    d.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.cmd == "dlq":
        return dlq_command(args)
    if args.cmd == "livekit-trunk":
        if args.action == "create" and (not args.address or not args.number):
            parser.error("create needs --address and at least one --number")
        return livekit_trunk(args)
    return create_admin(args.email, args.password, args.name, args.reset_password)


if __name__ == "__main__":
    raise SystemExit(main())
