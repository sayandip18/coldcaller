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


class RateLimiter:
    """Token buckets: carrier-wide plus (optionally) per campaign, taken atomically."""

    def __init__(self, r: Redis, carrier_cps: int):
        self.carrier_cps = carrier_cps
        self._take = r.register_script(lua.TAKE_TOKENS)

    async def take(self, campaign_id: str | None, campaign_cps: int | None) -> int:
        """Return 0 if a call may start now, else milliseconds to wait."""
        bucket_keys = [keys.GLOBAL_CPS]
        rates = [self.carrier_cps]
        if campaign_id and campaign_cps:
            bucket_keys.append(keys.campaign_cps(campaign_id))
            rates.append(campaign_cps)
        return int(await self._take(keys=bucket_keys, args=rates))


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
