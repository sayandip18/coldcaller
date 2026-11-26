import asyncio
import logging
import os
import random
import socket
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from redis.asyncio import Redis
from sqlalchemy import select

from app.config import Settings, get_settings
from app.db.models import Call, CallStatus, Campaign, CampaignStatus
from app.db.session import SessionLocal
from app.livekit_client import LiveKitClient
from app.queue import keys
from app.queue.primitives import CallQueue, Channels, RateLimiter
from app.services.call_service import is_dnc
from app.services.compliance import next_allowed_time
from app.services.lifecycle import finalize_campaigns, mark, reap_stuck_calls
from app.worker.dialer import Dialer, DialRequest, FakeDialer, LiveKitDialer

log = logging.getLogger(__name__)

DialerFactory = Callable[[Callable, Callable], Dialer]


@dataclass
class CallSnapshot:
    id: str
    to_number: str
    campaign_id: str | None
    max_concurrency: int | None
    cps: int | None
    caller_id: str


@dataclass
class Plan:
    action: str  # drop | delay | dial
    delay_s: float = 0.0
    call: CallSnapshot | None = None


def _hhmm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


def plan_call(call_id: str, settings: Settings, pause_delay_s: float, now: datetime | None = None) -> Plan:
    """Decide (using the DB only) whether a queued call can be attempted now."""
    now = now or datetime.now(UTC)
    with SessionLocal() as db:
        call = db.get(Call, call_id)
        if call is None or call.status != CallStatus.queued.value:
            return Plan("drop")

        campaign = db.get(Campaign, call.campaign_id) if call.campaign_id else None
        if campaign is not None:
            if campaign.status == CampaignStatus.paused.value:
                return Plan("delay", pause_delay_s)
            if campaign.status != CampaignStatus.running.value:
                return Plan("drop")

        if is_dnc(db, call.owner_id, call.to_number):
            mark(db, call.id, [CallStatus.queued], CallStatus.skipped_dnc, event="skipped_dnc")
            return Plan("drop")

        if campaign is not None:
            tz, start, end = campaign.timezone, campaign.call_window_start, campaign.call_window_end
        else:
            tz = settings.default_timezone
            start, end = _hhmm(settings.default_call_window_start), _hhmm(settings.default_call_window_end)
        allowed = next_allowed_time(now, tz, start, end)
        if allowed > now:
            return Plan("delay", (allowed - now).total_seconds())

        return Plan(
            "dial",
            call=CallSnapshot(
                id=call.id,
                to_number=call.to_number,
                campaign_id=campaign.id if campaign else None,
                max_concurrency=campaign.max_concurrency if campaign else None,
                cps=campaign.cps if campaign else None,
                caller_id=campaign.caller_id if campaign else "",
            ),
        )


def claim_call(call_id: str) -> bool:
    from sqlalchemy import func

    with SessionLocal() as db:
        return mark(
            db,
            call_id,
            [CallStatus.queued],
            CallStatus.dialing,
            event="dialing",
            dialed_at=func.now(),
            attempt=Call.attempt + 1,
            livekit_room=f"call-{call_id}",
        )


def fail_dial(call_id: str, error: str, max_attempts: int) -> tuple[str, int]:
    """Dial attempt raised. Returns ('retry'|'failed', attempt)."""
    with SessionLocal() as db:
        attempt = db.scalar(select(Call.attempt).where(Call.id == call_id)) or 0
        payload = {"error": error, "attempt": attempt}
        if attempt < max_attempts:
            mark(db, call_id, [CallStatus.dialing], CallStatus.queued, event="dial_error", payload=payload)
            return "retry", attempt
        mark(
            db,
            call_id,
            [CallStatus.dialing],
            CallStatus.failed,
            event="dial_error",
            payload=payload,
            failure_reason=f"dial failed after {attempt} attempts: {error}"[:255],
        )
        return "failed", attempt


def fail_call(call_id: str, reason: str) -> None:
    with SessionLocal() as db:
        mark(
            db,
            call_id,
            [CallStatus.queued, CallStatus.dialing],
            CallStatus.failed,
            event="worker_error",
            payload={"reason": reason},
            failure_reason=reason[:255],
        )


def find_orphans(limit: int = 500) -> list[str]:
    """Queued calls older than a minute (so not just-created) that a running campaign or a
    single call should be attempting."""
    from sqlalchemy import or_

    from app.db.models import utcnow

    cutoff = utcnow() - timedelta(seconds=60)
    with SessionLocal() as db:
        q = (
            select(Call.id)
            .outerjoin(Campaign, Call.campaign_id == Campaign.id)
            .where(
                Call.status == CallStatus.queued.value,
                Call.queued_at < cutoff,
                or_(Call.campaign_id.is_(None), Campaign.status == CampaignStatus.running.value),
            )
            .limit(limit)
        )
        return list(db.scalars(q))


def ended_leases(call_ids: list[str]) -> list[tuple[str, str | None]]:
    """Of the calls holding a channel lease, those that are already finished (or unknown)."""
    from app.db.models import TERMINAL_CALL_STATUSES

    terminal = {s.value for s in TERMINAL_CALL_STATUSES}
    with SessionLocal() as db:
        rows = db.execute(
            select(Call.id, Call.campaign_id, Call.status).where(Call.id.in_(call_ids))
        ).all()
    known = {r[0] for r in rows}
    ended = [(cid, camp) for cid, camp, st in rows if st in terminal]
    ended += [(cid, None) for cid in call_ids if cid not in known]
    return ended


class Worker:
    def __init__(
        self,
        r: Redis,
        *,
        settings: Settings | None = None,
        dialer_factory: DialerFactory | None = None,
        carrier_cps: int | None = None,
        max_global_channels: int | None = None,
        sweep_interval: float | None = None,
        pause_delay_s: float = 5.0,
        retry_base_s: float = 5.0,
        consumer: str | None = None,
    ):
        self.settings = s = settings or get_settings()
        self.r = r
        self.q = CallQueue(r)
        self.limiter = RateLimiter(r, carrier_cps or s.carrier_cps)
        self.channels = Channels(
            r, max_global_channels or s.max_global_channels, s.max_call_seconds + 60
        )
        self.sweep_interval = sweep_interval if sweep_interval is not None else s.worker_sweep_interval
        self.pause_delay_s = pause_delay_s
        self.retry_base_s = retry_base_s
        self.consumer = consumer or f"{socket.gethostname()}-{os.getpid()}"
        factory = dialer_factory or self._default_dialer
        self.dialer: Dialer = factory(self.release_call, self.handle_dial_error)
        self._inflight: set[asyncio.Task] = set()

    def _default_dialer(self, release, on_error) -> Dialer:
        s = self.settings
        if s.dialer == "fake":
            return FakeDialer(
                release,
                ring_s=(s.fake_ring_min, s.fake_ring_max),
                talk_s=(s.fake_talk_min, s.fake_talk_max),
            )
        if s.dialer == "livekit":
            if not s.livekit_sip_trunk_id:
                raise RuntimeError("DIALER=livekit needs LIVEKIT_SIP_TRUNK_ID (see `cli livekit-trunk`)")
            return LiveKitDialer(
                release,
                on_error,
                LiveKitClient(s),
                trunk_id=s.livekit_sip_trunk_id,
                agent_name=s.livekit_agent_name,
                ring_timeout_s=s.livekit_ring_timeout,
                max_call_s=s.max_call_seconds,
            )
        raise RuntimeError(f"unknown DIALER '{s.dialer}'")

    async def release_call(self, call_id: str, campaign_id: str | None) -> None:
        await self.channels.release(call_id, campaign_id)

    # ---------------------------------------------------------------- per-call
    async def process(self, call_id: str) -> None:
        plan = await asyncio.to_thread(plan_call, call_id, self.settings, self.pause_delay_s)
        log.debug("plan %s -> %s (delay %.2fs)", call_id, plan.action, plan.delay_s)
        if plan.action == "drop":
            await self.q.forget(call_id)
            return
        if plan.action == "delay":
            await self.q.delay(call_id, plan.delay_s)
            return

        c = plan.call
        assert c is not None
        if not await self.channels.acquire(c.id, c.campaign_id, c.max_concurrency):
            log.debug("no channel for %s, retrying shortly", call_id)
            await self.q.delay(call_id, 1.0 + random.random())
            return
        wait_ms = await self.limiter.take(c.campaign_id, c.cps)
        if wait_ms:
            log.debug("rate limited %s for %dms", call_id, wait_ms)
            await self.channels.release(c.id, c.campaign_id)
            await self.q.delay(call_id, wait_ms / 1000 + random.random() * 0.05)
            return
        if not await asyncio.to_thread(claim_call, c.id):
            await self.channels.release(c.id, c.campaign_id)
            await self.q.forget(call_id)
            return

        await self.q.forget(call_id)
        try:
            await self.dialer.dial(
                DialRequest(c.id, c.to_number, c.caller_id, c.campaign_id, f"call-{c.id}")
            )
        except Exception as exc:  # noqa: BLE001 - any dialer error is a retryable dial failure
            log.warning("dial failed for %s: %s", c.id, exc)
            await self.handle_dial_error(DialRequest(c.id, "", "", c.campaign_id, ""), str(exc))

    async def handle_dial_error(self, req: DialRequest, error: str) -> None:
        """A dial attempt failed for infrastructure reasons: free the slot, then retry with
        backoff or, after too many attempts, fail the call and dead-letter it."""
        await self.channels.release(req.call_id, req.campaign_id)
        outcome, attempt = await asyncio.to_thread(
            fail_dial, req.call_id, error, self.settings.dial_max_attempts
        )
        if outcome == "retry":
            await self.q.delay(req.call_id, min(300.0, self.retry_base_s * 2 ** (attempt - 1)))
        else:
            await self.q.dead_letter(req.call_id, error)

    async def _handle(self, msg_id: str, call_id: str) -> None:
        try:
            await self.process(call_id)
        except Exception as exc:
            log.exception("unexpected error handling %s", call_id)
            await self._on_unexpected_error(call_id, exc)
        finally:
            await self.r.xack(keys.STREAM, keys.GROUP, msg_id)

    async def _on_unexpected_error(self, call_id: str, exc: Exception) -> None:
        try:
            n = int(await self.r.hincrby(keys.HANDLER_ERRORS, call_id, 1))
            if n >= self.settings.dial_max_attempts:
                await asyncio.to_thread(fail_call, call_id, f"worker error: {exc}")
                await self.q.dead_letter(call_id, str(exc))
            else:
                await self.q.delay(call_id, min(300.0, self.retry_base_s * 2 ** (n - 1)))
        except Exception:
            log.exception("could not reschedule %s after error", call_id)

    def _spawn(self, msg_id: str, call_id: str) -> None:
        task = asyncio.create_task(self._handle(msg_id, call_id))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    # ------------------------------------------------------------------ loops
    async def _consume(self) -> None:
        while True:
            if len(self._inflight) >= self.settings.worker_max_inflight:
                await asyncio.sleep(0.05)
                continue
            resp = await self.r.xreadgroup(
                keys.GROUP, self.consumer, {keys.STREAM: ">"}, count=20, block=1000
            )
            for _, messages in resp or []:
                for msg_id, fields in messages:
                    self._spawn(msg_id, fields["call_id"])

    async def _delayed_loop(self) -> None:
        while True:
            moved = await self.q.move_due()
            await asyncio.sleep(0.05 if moved >= 500 else 0.2)

    async def sweep_once(self) -> None:
        """Recover from crashes: reclaim stale messages, re-enqueue lost calls, fail stuck
        calls, and close finished campaigns."""
        res = await self.r.xautoclaim(
            keys.STREAM, keys.GROUP, self.consumer, min_idle_time=60_000, start_id="0-0", count=50
        )
        for msg_id, fields in res[1]:
            if fields:
                self._spawn(msg_id, fields["call_id"])

        orphans = await asyncio.to_thread(find_orphans)
        if orphans:
            known = await self.q.known(orphans)
            lost = [cid for cid, k in zip(orphans, known, strict=True) if not k]
            if lost:
                log.warning("re-enqueueing %d orphaned calls", len(lost))
                await self.q.enqueue(lost)

        # A lease whose call has already ended (e.g. a hang-up webhook was lost) is dead weight.
        held = await self.r.zrange(keys.GLOBAL_CHANNELS, 0, -1)
        if held:
            for call_id, campaign_id in await asyncio.to_thread(ended_leases, held):
                log.warning("releasing stale channel lease for ended call %s", call_id)
                await self.channels.release(call_id, campaign_id)

        def reap():
            with SessionLocal() as db:
                return reap_stuck_calls(db, self.settings.max_call_seconds + 60)

        for call_id, campaign_id in await asyncio.to_thread(reap):
            log.warning("call %s timed out", call_id)
            await self.channels.release(call_id, campaign_id)

        def finalize():
            with SessionLocal() as db:
                return finalize_campaigns(db)

        for cid in await asyncio.to_thread(finalize):
            log.info("campaign %s done", cid)

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await self.sweep_once()
            except Exception:
                log.exception("sweep failed")
            await asyncio.sleep(self.sweep_interval)

    async def run(self) -> None:
        await self.q.ensure_group()
        log.info("worker %s started", self.consumer)
        tasks = [
            asyncio.create_task(self._consume()),
            asyncio.create_task(self._delayed_loop()),
            asyncio.create_task(self._sweep_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.dialer.aclose()
            for t in list(self._inflight):
                t.cancel()
            await asyncio.gather(*self._inflight, return_exceptions=True)
