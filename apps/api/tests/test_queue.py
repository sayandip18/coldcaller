import asyncio
import os
from datetime import UTC, datetime, time

from redis.asyncio import Redis

from app.queue import keys
from app.queue.primitives import (
    CAMPAIGN_FULL,
    CLAIMED,
    EMPTY,
    GLOBAL_FULL,
    RATE_LIMITED,
    CallQueue,
    CampaignQueues,
    Channels,
)
from app.services.compliance import next_allowed_time


def run_with_redis(fn):
    async def main():
        r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        try:
            return await fn(r)
        finally:
            await r.aclose()

    return asyncio.run(main())


def make_queues(r, cps=100_000, global_max=100):
    return CampaignQueues(r, carrier_cps=cps, global_max=global_max, lease_seconds=60)


def test_claim_pops_in_order_and_reserves_a_slot():
    async def scenario(r):
        cq = make_queues(r)
        await cq.push("c1", ["a", "b"])
        assert await cq.active() == ["c1"]
        assert await cq.claim_next("c1", 5, None) == (CLAIMED, "a")
        assert await r.zscore(keys.GLOBAL_CHANNELS, "a") is not None
        assert await r.zscore(keys.campaign_channels("c1"), "a") is not None
        assert await r.zscore(keys.HANDOFF, "a") is not None  # popped, not yet claimed in the DB
        assert await cq.claim_next("c1", 5, None) == (CLAIMED, "b")

    run_with_redis(scenario)


def test_empty_queue_is_removed_from_the_active_set():
    async def scenario(r):
        cq = make_queues(r)
        await cq.push("c1", ["a"])
        await cq.claim_next("c1", 5, None)
        code, _ = await cq.claim_next("c1", 5, None)
        assert code == EMPTY and await cq.active() == []

    run_with_redis(scenario)


def test_campaign_channel_cap_blocks_without_touching_the_queue():
    async def scenario(r):
        cq = make_queues(r)
        await cq.push("c1", ["a", "b", "c"])
        assert (await cq.claim_next("c1", 2, None))[0] == CLAIMED
        assert (await cq.claim_next("c1", 2, None))[0] == CLAIMED
        before = await r.lrange(keys.ready_queue("c1"), 0, -1)
        assert (await cq.claim_next("c1", 2, None))[0] == CAMPAIGN_FULL
        assert await r.lrange(keys.ready_queue("c1"), 0, -1) == before == ["c"]
        await Channels(r, 100, 60).release("a", "c1")
        assert await cq.claim_next("c1", 2, None) == (CLAIMED, "c")

    run_with_redis(scenario)


def test_global_cap_is_shared_between_campaigns():
    async def scenario(r):
        cq = make_queues(r, global_max=2)
        await cq.push("c1", ["a"])
        await cq.push("c2", ["b", "c"])
        assert (await cq.claim_next("c1", 5, None))[0] == CLAIMED
        assert (await cq.claim_next("c2", 5, None))[0] == CLAIMED
        assert (await cq.claim_next("c2", 5, None))[0] == GLOBAL_FULL

    run_with_redis(scenario)


def test_rate_limit_paces_calls_with_no_burst_and_consumes_nothing_when_refused():
    async def scenario(r):
        cq = make_queues(r)
        await cq.push("c1", ["a", "b", "c"])
        assert (await cq.claim_next("c1", 50, 5))[0] == CLAIMED  # campaign cps = 5
        code, wait = await cq.claim_next("c1", 50, 5)
        assert code == RATE_LIMITED and 1 <= wait <= 200  # one token per 200 ms, no burst
        assert await cq.depth("c1") == 2  # the refused attempt popped nothing
        await asyncio.sleep(wait / 1000 + 0.05)
        assert (await cq.claim_next("c1", 50, 5))[0] == CLAIMED

    run_with_redis(scenario)


def test_carrier_cps_is_shared_by_all_campaigns():
    async def scenario(r):
        cq = make_queues(r, cps=2)
        await cq.push("c1", ["a"])
        await cq.push("c2", ["b"])
        assert (await cq.claim_next("c1", 5, None))[0] == CLAIMED
        assert (await cq.claim_next("c2", 5, None))[0] == RATE_LIMITED  # carrier bucket is empty

    run_with_redis(scenario)


def test_single_calls_ignore_campaign_limits_but_obey_global_ones():
    async def scenario(r):
        cq = make_queues(r, global_max=1)
        await cq.push(keys.SINGLE, ["a", "b"])
        assert (await cq.claim_next(keys.SINGLE, None, None))[0] == CLAIMED
        assert (await cq.claim_next(keys.SINGLE, None, None))[0] == GLOBAL_FULL

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
