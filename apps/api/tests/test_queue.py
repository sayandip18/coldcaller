import asyncio
import os
from datetime import UTC, datetime, time

from redis.asyncio import Redis

from app.queue import keys
from app.queue.primitives import CallQueue, Channels, RateLimiter
from app.services.compliance import next_allowed_time


def run_with_redis(fn):
    async def main():
        r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        try:
            return await fn(r)
        finally:
            await r.aclose()

    return asyncio.run(main())


def test_rate_limiter_burst_then_wait():
    async def scenario(r):
        rl = RateLimiter(r, carrier_cps=3)
        results = [await rl.take(None, None) for _ in range(4)]
        assert results[:3] == [0, 0, 0]
        assert 1 <= results[3] <= 400  # one token per ~333ms
        await asyncio.sleep(0.4)
        assert await rl.take(None, None) == 0

    run_with_redis(scenario)


def test_rate_limiter_campaign_bucket_binds_and_consumes_nothing_on_wait():
    async def scenario(r):
        rl = RateLimiter(r, carrier_cps=100)
        assert await rl.take("c1", 1) == 0
        assert await rl.take("c1", 1) > 0  # campaign bucket empty
        # the refused attempt must not have drained the carrier bucket
        tokens = float(await r.hget(keys.GLOBAL_CPS, "tokens"))
        assert tokens > 97
        assert await rl.take("c2", 1) == 0  # other campaign unaffected

    run_with_redis(scenario)


def test_channels_enforce_campaign_and_global_limits():
    async def scenario(r):
        ch = Channels(r, global_max=3, lease_seconds=60)
        assert await ch.acquire("a", "c1", 2)
        assert await ch.acquire("b", "c1", 2)
        assert not await ch.acquire("c", "c1", 2)  # campaign full
        assert await ch.acquire("d", "c2", 2)  # global now 3/3
        assert not await ch.acquire("e", "c2", 2)  # global full
        await ch.release("a", "c1")
        assert await ch.acquire("e", "c2", 2)
        assert await ch.acquire("e", "c2", 2)  # re-acquire by the same call is idempotent
        assert await r.zcard(keys.GLOBAL_CHANNELS) == 3

    run_with_redis(scenario)


def test_expired_lease_is_reclaimed():
    async def scenario(r):
        ch = Channels(r, global_max=1, lease_seconds=1)
        assert await ch.acquire("a", None, None)
        assert not await ch.acquire("b", None, None)
        await asyncio.sleep(1.2)
        assert await ch.acquire("b", None, None)

    run_with_redis(scenario)


def test_delayed_items_move_to_stream_when_due():
    async def scenario(r):
        q = CallQueue(r)
        await q.ensure_group()
        await q.delay("late", 5)
        await q.delay("soon", 0.1)
        assert await q.move_due() == 0
        await asyncio.sleep(0.2)
        assert await q.move_due() == 1
        msgs = await r.xrange(keys.STREAM)
        assert [m[1]["call_id"] for m in msgs] == ["soon"]
        assert await r.zscore(keys.DELAYED, "late") is not None

    run_with_redis(scenario)


def test_next_allowed_time():
    start, end = time(9, 0), time(21, 0)
    ist = "Asia/Kolkata"
    inside = datetime(2026, 1, 5, 6, 0, tzinfo=UTC)  # 11:30 IST
    assert next_allowed_time(inside, ist, start, end) == inside
    early = datetime(2026, 1, 5, 2, 0, tzinfo=UTC)  # 07:30 IST -> 09:00 IST same day
    assert next_allowed_time(early, ist, start, end) == datetime(2026, 1, 5, 3, 30, tzinfo=UTC)
    late = datetime(2026, 1, 5, 16, 0, tzinfo=UTC)  # 21:30 IST -> 09:00 IST next day
    assert next_allowed_time(late, ist, start, end) == datetime(2026, 1, 6, 3, 30, tzinfo=UTC)
    boundary = datetime(2026, 1, 5, 15, 30, tzinfo=UTC)  # exactly 21:00 IST is outside
    assert next_allowed_time(boundary, ist, start, end) > boundary
