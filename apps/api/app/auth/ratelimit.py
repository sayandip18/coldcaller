"""Brute-force protection for login.

Failed attempts are counted three ways, so one attacker can neither guess a password quickly
nor lock a real user out of their own account:

  * per (email, IP)  - 5 failures:   slows guessing from one source
  * per IP           - 20 failures:  stops one source trying many accounts
  * per email        - 30 failures:  stops a distributed guess at one account, high enough that
                                     a single attacker cannot lock the owner out

Counters live in Redis for 15 minutes. A locked pair/IP is refused even with the right password,
so the lock cannot be used to confirm a guess. If Redis is unavailable login still works (fail
open) and the problem is logged. Behind a reverse proxy, start uvicorn with
`--proxy-headers --forwarded-allow-ips=<proxy ip>` so `request.client.host` is the real client.
"""

import hashlib
import logging

import redis

from app.queue.producer import redis_client

log = logging.getLogger(__name__)

WINDOW_S = 15 * 60
PAIR_LIMIT = 5
IP_LIMIT = 20
EMAIL_LIMIT = 30


def _h(value: str) -> str:
    return hashlib.sha256(value.lower().encode()).hexdigest()[:20]  # never store raw emails


def _keys(email: str, ip: str) -> dict[str, tuple[str, int]]:
    return {
        "pair": (f"login:fail:pair:{_h(email)}:{ip}", PAIR_LIMIT),
        "ip": (f"login:fail:ip:{ip}", IP_LIMIT),
        "email": (f"login:fail:email:{_h(email)}", EMAIL_LIMIT),
    }


def locked_for(email: str, ip: str) -> int:
    """Seconds until the caller may try again, or 0 if they may try now."""
    try:
        r = redis_client()
        wait = 0
        for key, limit in _keys(email, ip).values():
            count = r.get(key)
            if count is not None and int(count) >= limit:
                wait = max(wait, r.ttl(key))
        return max(wait, 0)
    except redis.RedisError:
        log.exception("login rate limiter unavailable; allowing the attempt")
        return 0


def record_failure(email: str, ip: str) -> None:
    try:
        pipe = redis_client().pipeline()
        for key, _ in _keys(email, ip).values():
            pipe.incr(key)
            pipe.expire(key, WINDOW_S, nx=True)  # the window starts at the first failure
        pipe.execute()
    except redis.RedisError:
        log.exception("could not record a failed login")


def record_success(email: str, ip: str) -> None:
    """A correct password clears that email/IP pair's failures (not the per-IP count)."""
    try:
        redis_client().delete(_keys(email, ip)["pair"][0])
    except redis.RedisError:
        log.exception("could not reset the login counter")
