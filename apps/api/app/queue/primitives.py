"""Async Redis building blocks used by the worker."""

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.queue import keys, lua


class CallQueue:
    def __init__(self, r: Redis):
        self.r = r
        self._move_due = r.register_script(lua.MOVE_DUE)

    async def ensure_group(self) -> None:
        try:
            await self.r.xgroup_create(keys.STREAM, keys.GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def now_ms(self) -> int:
        sec, usec = await self.r.time()
        return int(sec) * 1000 + int(usec) // 1000

    async def enqueue(self, call_ids: list[str]) -> None:
        if not call_ids:
            return
        pipe = self.r.pipeline()
        for cid in call_ids:
            pipe.sadd(keys.ENQUEUED, cid)
            pipe.xadd(keys.STREAM, {"call_id": cid})
        await pipe.execute()

    async def delay(self, call_id: str, seconds: float) -> None:
        due = await self.now_ms() + int(max(seconds, 0.0) * 1000)
        pipe = self.r.pipeline()
        pipe.sadd(keys.ENQUEUED, call_id)
        pipe.zadd(keys.DELAYED, {call_id: due})
        await pipe.execute()

    async def move_due(self, limit: int = 500) -> int:
        return int(await self._move_due(keys=[keys.DELAYED, keys.STREAM], args=[limit]))

    async def forget(self, call_id: str) -> None:
        await self.r.srem(keys.ENQUEUED, call_id)

    async def known(self, call_ids: list[str]) -> list[bool]:
        if not call_ids:
            return []
        return [bool(x) for x in await self.r.smismember(keys.ENQUEUED, call_ids)]

    async def dead_letter(self, call_id: str, reason: str) -> None:
        pipe = self.r.pipeline()
        pipe.xadd(keys.DLQ, {"call_id": call_id, "reason": reason})
        pipe.srem(keys.ENQUEUED, call_id)
        pipe.hdel(keys.HANDLER_ERRORS, call_id)
        await pipe.execute()


class Channels:
    """Concurrency limits as leases. A lease is released when the call ends, or expires."""

    def __init__(self, r: Redis, global_max: int, lease_seconds: int):
        self.r = r
        self.global_max = global_max
        self.lease_ms = lease_seconds * 1000
        self._acquire = r.register_script(lua.ACQUIRE_CHANNEL)

    async def acquire(self, call_id: str, campaign_id: str | None, campaign_max: int | None) -> bool:
        lease_keys = [keys.GLOBAL_CHANNELS]
        limits = [self.global_max]
        if campaign_id and campaign_max:
            lease_keys.append(keys.campaign_channels(campaign_id))
            limits.append(campaign_max)
        ok = await self._acquire(keys=lease_keys, args=[call_id, self.lease_ms, *limits])
        return bool(ok)

    async def release(self, call_id: str, campaign_id: str | None) -> None:
        pipe = self.r.pipeline()
        pipe.zrem(keys.GLOBAL_CHANNELS, call_id)
        if campaign_id:
            pipe.zrem(keys.campaign_channels(campaign_id), call_id)
        await pipe.execute()


CLAIMED, CAMPAIGN_FULL, GLOBAL_FULL, RATE_LIMITED, EMPTY = 1, 2, 3, 4, 5


class CampaignQueues:
    """One ready list per campaign. A call is popped only when its campaign may start it, so
    calls waiting behind a limit are never touched, and a slow campaign cannot hold up others."""

    def __init__(self, r: Redis, *, carrier_cps: int, global_max: int, lease_seconds: int):
        self.r = r
        self.carrier_cps, self.global_max = carrier_cps, global_max
        self.lease_ms = lease_seconds * 1000
        self.handoff_ms = 60_000
        self._claim = r.register_script(lua.CLAIM_NEXT)

    async def push(self, queue_id: str, call_ids: list[str]) -> None:
        pipe = self.r.pipeline(transaction=True)
        pipe.rpush(keys.ready_queue(queue_id), *call_ids)
        pipe.sadd(keys.ACTIVE, queue_id)
        await pipe.execute()

    async def push_front(self, queue_id: str, call_id: str) -> None:
        pipe = self.r.pipeline(transaction=True)
        pipe.lpush(keys.ready_queue(queue_id), call_id)
        pipe.sadd(keys.ACTIVE, queue_id)
        await pipe.execute()

    async def active(self) -> list[str]:
        return sorted(await self.r.smembers(keys.ACTIVE))

    async def depth(self, queue_id: str) -> int:
        return int(await self.r.llen(keys.ready_queue(queue_id)))

    async def claim_next(
        self, queue_id: str, campaign_max: int | None, campaign_cps: int | None
    ) -> tuple[int, str | int]:
        is_campaign = queue_id != keys.SINGLE
        res = await self._claim(
            keys=[
                keys.ready_queue(queue_id),
                keys.ACTIVE,
                keys.GLOBAL_CHANNELS,
                keys.campaign_channels(queue_id),
                keys.GLOBAL_CPS,
                keys.campaign_cps(queue_id),
                keys.HANDOFF,
            ],
            args=[
                queue_id,
                self.lease_ms,
                self.global_max,
                (campaign_max or 0) if is_campaign else 0,
                self.carrier_cps,
                (campaign_cps or 0) if is_campaign else 0,
                self.handoff_ms,
            ],
        )
        return int(res[0]), res[1]

    async def ack_handoff(self, call_id: str) -> None:
        await self.r.zrem(keys.HANDOFF, call_id)

    async def expired_handoffs(self, limit: int = 100) -> list[str]:
        now = int((await self.r.time())[0]) * 1000
        return list(await self.r.zrangebyscore(keys.HANDOFF, "-inf", now, start=0, num=limit))
