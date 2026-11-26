import asyncio
import logging
import os
import socket
import time as _time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import or_, select

from app.config import Settings, get_settings
from app.db.models import (
    TERMINAL_CALL_STATUSES,
    Call,
    CallStatus,
    Campaign,
    CampaignStatus,
    utcnow,
)
from app.db.session import SessionLocal
from app.livekit_client import LiveKitClient
from app.queue import keys
from app.queue.primitives import (
    CAMPAIGN_FULL,
    CLAIMED,
    GLOBAL_FULL,
    RATE_LIMITED,
    CallQueue,
    CampaignQueues,
    Channels,
)
from app.services.alerts import AlertSink, alert_campaign_finished, get_sink
from app.services.call_service import is_blocked
from app.services.caller_id import caller_id_error
from app.services.caller_id import effective as effective_caller_id
from app.services.compliance import next_allowed_time
from app.services.lifecycle import finalize_campaigns, mark, reap_stuck_calls
from app.services.webhooks import WebhookSender
from app.worker.dialer import Dialer, DialRequest, FakeDialer, LiveKitDialer

log = logging.getLogger(__name__)

DialerFactory = Callable[[Callable, Callable], Dialer]

SCHEDULER_TICK_S = 0.05
CONFIG_TTL_S = 2.0
CHANNEL_FULL_BACKOFF_S = 0.1
BURST_PER_CAMPAIGN = 50
MAX_WINDOW_SKIP_S = 30.0


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
    """Final DB check before dialing a call the scheduler has just popped."""
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

        reason = is_blocked(db, call.owner_id, call.to_number)
        if reason:
            mark(
                db,
                call.id,
                [CallStatus.queued],
                CallStatus.skipped_dnc,
                event="skipped_dnc",
                payload={"reason": reason},
            )
            return Plan("drop")

        # Last line of defence: never present a caller ID this deployment is not allowed to use,
        # even if the allow-list changed after the campaign was created.
        caller_id = effective_caller_id(campaign.caller_id if campaign else "", settings)
        bad_caller_id = caller_id_error(caller_id, settings)
        if bad_caller_id:
            mark(
                db,
                call.id,
                [CallStatus.queued],
                CallStatus.failed,
                event="caller_id_rejected",
                payload={"reason": bad_caller_id},
                failure_reason=bad_caller_id[:255],
            )
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
                caller_id=caller_id,
            ),
        )


def claim_call(call_id: str) -> bool:
    with SessionLocal() as db:
        return mark(
            db,
            call_id,
            [CallStatus.queued],
            CallStatus.dialing,
            event="dialing",
            dialed_at=utcnow(),  # same clock as answered_at/ended_at, so durations are consistent
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
    terminal = {s.value for s in TERMINAL_CALL_STATUSES}
    with SessionLocal() as db:
        rows = db.execute(
            select(Call.id, Call.campaign_id, Call.status).where(Call.id.in_(call_ids))
        ).all()
    known = {r[0] for r in rows}
    ended = [(cid, camp) for cid, camp, st in rows if st in terminal]
    ended += [(cid, None) for cid in call_ids if cid not in known]
    return ended


def call_states(call_ids: list[str]) -> dict[str, tuple[str, str | None]]:
    with SessionLocal() as db:
        rows = db.execute(
            select(Call.id, Call.status, Call.campaign_id).where(Call.id.in_(call_ids))
        ).all()
    return {cid: (status, camp) for cid, status, camp in rows}


def route_many(call_ids: list[str]) -> dict[str, str | None]:
    """Which ready queue each call belongs in (None = should not be queued at all)."""
    with SessionLocal() as db:
        rows = db.execute(
            select(Call.id, Call.status, Call.campaign_id).where(Call.id.in_(call_ids))
        ).all()
    routes: dict[str, str | None] = dict.fromkeys(call_ids)
    for call_id, status, campaign_id in rows:
        if status == CallStatus.queued.value:
            routes[call_id] = campaign_id or keys.SINGLE
    return routes


@dataclass
class QueueConfig:
    """What the scheduler needs to know about a queue, cached for a couple of seconds."""

    status: str
    max_concurrency: int | None
    cps: int | None
    timezone: str
    start: time
    end: time


def load_queue_configs(queue_ids: list[str], settings: Settings) -> dict[str, QueueConfig]:
    configs: dict[str, QueueConfig] = {}
    real = [q for q in queue_ids if q != keys.SINGLE]
    if keys.SINGLE in queue_ids:
        configs[keys.SINGLE] = QueueConfig(
            CampaignStatus.running.value,
            None,
            None,
            settings.default_timezone,
            _hhmm(settings.default_call_window_start),
            _hhmm(settings.default_call_window_end),
        )
    if real:
        with SessionLocal() as db:
            for c in db.scalars(select(Campaign).where(Campaign.id.in_(real))):
                configs[c.id] = QueueConfig(
                    c.status, c.max_concurrency, c.cps, c.timezone, c.call_window_start, c.call_window_end
                )
    return configs


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
        webhook_sender: WebhookSender | None = None,
        alert_sink: AlertSink | None = None,
    ):
        self.settings = s = settings or get_settings()
        self.r = r
        self.q = CallQueue(r)
        lease_seconds = s.max_call_seconds + 60
        self.channels = Channels(r, max_global_channels or s.max_global_channels, lease_seconds)
        self.queues = CampaignQueues(
            r,
            carrier_cps=carrier_cps or s.carrier_cps,
            global_max=max_global_channels or s.max_global_channels,
            lease_seconds=lease_seconds,
        )
        self.sweep_interval = sweep_interval if sweep_interval is not None else s.worker_sweep_interval
        self.pause_delay_s = pause_delay_s
        self.retry_base_s = retry_base_s
        self.consumer = consumer or f"{socket.gethostname()}-{os.getpid()}"
        factory = dialer_factory or self._default_dialer
        self.dialer: Dialer = factory(self.release_call, self.handle_dial_error)
        self.webhooks = webhook_sender or WebhookSender(s)
        self.alerts = alert_sink or get_sink(s)
        self._inflight: set[asyncio.Task] = set()
        self._skip_until: dict[str, float] = {}  # queue id -> monotonic time to look again
        self._global_block_until = 0.0
        self._next_wake = 0.0  # monotonic time of the earliest queue we are waiting on
        self._configs: dict[str, QueueConfig] = {}
        self._configs_at = 0.0
        self._rotation = 0

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
                agent_join_timeout_s=s.agent_join_timeout,
            )
        raise RuntimeError(f"unknown DIALER '{s.dialer}'")

    async def release_call(self, call_id: str, campaign_id: str | None) -> None:
        await self.channels.release(call_id, campaign_id)

    # ----------------------------------------------------------- ingress (stream)
    async def ingest(self, batch: list[tuple[str, str]]) -> None:
        """Move calls from the stream into their campaigns' ready queues.
        `batch` is [(stream message id, call id)]. One DB query, one push per queue, one ack."""
        call_ids = [call_id for _, call_id in batch]
        try:
            routes = await asyncio.to_thread(route_many, call_ids)
            by_queue: dict[str, list[str]] = {}
            dropped: list[str] = []
            for call_id in call_ids:
                queue_id = routes.get(call_id)
                if queue_id is None:
                    dropped.append(call_id)
                else:
                    by_queue.setdefault(queue_id, []).append(call_id)
            for queue_id, ids in by_queue.items():
                await self.queues.push(queue_id, ids)
            if dropped:
                await self.r.srem(keys.ENQUEUED, *dropped)
        except Exception as exc:
            log.exception("unexpected error routing a batch of %d calls", len(call_ids))
            for call_id in call_ids:
                await self._on_unexpected_error(call_id, exc)
        finally:
            await self.r.xack(keys.STREAM, keys.GROUP, *[msg_id for msg_id, _ in batch])

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

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    # ------------------------------------------------------------- scheduling
    async def _load_configs(self, queue_ids: list[str]) -> dict[str, QueueConfig]:
        now = _time.monotonic()
        missing = [q for q in queue_ids if q not in self._configs]
        if missing or now - self._configs_at > CONFIG_TTL_S:
            self._configs = await asyncio.to_thread(load_queue_configs, queue_ids, self.settings)
            self._configs_at = now
        return self._configs

    async def schedule_once(self) -> int:
        """One pass over the campaigns with waiting calls; returns how many calls it started.
        Cost is proportional to the number of campaigns, not the number of waiting calls."""
        self._next_wake = 0.0
        active = await self.queues.active()
        if not active:
            return 0
        now = _time.monotonic()
        if now < self._global_block_until:
            self._next_wake = self._global_block_until
            return 0
        configs = await self._load_configs(active)
        self._rotation += 1
        order = active[self._rotation % len(active):] + active[: self._rotation % len(active)]
        started = 0
        for queue_id in order:
            if self._skip_until.get(queue_id, 0.0) > now:
                continue
            cfg = configs.get(queue_id)
            if cfg is None:  # campaign vanished; leave its calls for the orphan sweeper
                self._skip_until[queue_id] = now + 5
                continue
            if cfg.status != CampaignStatus.running.value:
                self._skip_until[queue_id] = now + 1.0  # paused (or not started): wait for it
                continue
            allowed = next_allowed_time(datetime.now(UTC), cfg.timezone, cfg.start, cfg.end)
            if allowed > datetime.now(UTC):
                wait = (allowed - datetime.now(UTC)).total_seconds()
                self._skip_until[queue_id] = now + min(wait, MAX_WINDOW_SKIP_S)
                continue

            for _ in range(BURST_PER_CAMPAIGN):
                code, value = await self.queues.claim_next(queue_id, cfg.max_concurrency, cfg.cps)
                if code == CLAIMED:
                    started += 1
                    self._spawn(self._dial_popped(str(value), queue_id))
                    continue
                if code == RATE_LIMITED:
                    self._skip_until[queue_id] = now + int(value) / 1000
                elif code == CAMPAIGN_FULL:
                    self._skip_until[queue_id] = now + CHANNEL_FULL_BACKOFF_S
                elif code == GLOBAL_FULL:
                    self._global_block_until = now + CHANNEL_FULL_BACKOFF_S
                break  # EMPTY needs no skip: the script already removed it from the active set
            if self._global_block_until > now:
                break
        # When to look again: the soonest moment any waiting queue may be able to start a call.
        waits = [t for t in self._skip_until.values() if t > now]
        if self._global_block_until > now:
            waits.append(self._global_block_until)
        self._next_wake = min(waits) if waits else 0.0
        return started

    async def _dial_popped(self, call_id: str, queue_id: str) -> None:
        """A call has been popped with its channel and rate-limit slot already reserved."""
        campaign_id = None if queue_id == keys.SINGLE else queue_id
        try:
            plan = await asyncio.to_thread(plan_call, call_id, self.settings, self.pause_delay_s)
            log.debug("plan %s -> %s", call_id, plan.action)
            if plan.action == "drop":
                await self.channels.release(call_id, campaign_id)
                await self.q.forget(call_id)
                return
            if plan.action == "delay":  # paused or out of window in the instant since the check
                await self.channels.release(call_id, campaign_id)
                await self.queues.push_front(queue_id, call_id)
                return

            c = plan.call
            assert c is not None
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
        except Exception:
            # Leave the handoff record in place: the sweeper will requeue the call and free its slot.
            log.exception("could not start call %s", call_id)
            return
        await self.queues.ack_handoff(call_id)

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

    # ------------------------------------------------------------------ loops
    async def _consume(self) -> None:
        while True:
            try:
                resp = await self.r.xreadgroup(
                    keys.GROUP, self.consumer, {keys.STREAM: ">"}, count=200, block=1000
                )
                for _, messages in resp or []:
                    # sequential on purpose: it is the backpressure on a huge campaign start
                    await self.ingest([(msg_id, f["call_id"]) for msg_id, f in messages])
            except RedisError as exc:
                # Redis restarted or was flushed (the stream and its consumer group are gone), or
                # the connection dropped. Recreate the group and carry on; calls that were queued
                # in the database are re-enqueued by the orphan sweeper.
                log.warning("stream read failed (%s); recovering", exc)
                await asyncio.sleep(1.0)
                try:
                    await self.q.ensure_group()
                except RedisError:
                    log.exception("could not recreate the consumer group yet")

    async def _delayed_loop(self) -> None:
        while True:
            moved = 0
            try:
                moved = await self.q.move_due()
            except RedisError as exc:
                log.warning("delayed-set mover failed (%s); retrying", exc)
                await asyncio.sleep(1.0)
            await asyncio.sleep(0.05 if moved >= 500 else 0.2)

    async def _schedule_loop(self) -> None:
        while True:
            try:
                started = await self.schedule_once()
            except Exception:
                log.exception("scheduler pass failed")
                started = 0
            if started:
                await asyncio.sleep(0)
                continue
            # Sleep exactly until the next slot opens (not a fixed tick, which would cap the
            # start rate of a campaign at 1/tick calls per second whatever its CPS limit).
            until = self._next_wake - _time.monotonic() if self._next_wake else SCHEDULER_TICK_S
            await asyncio.sleep(min(SCHEDULER_TICK_S, max(0.001, until)))

    async def _webhook_loop(self) -> None:
        while True:
            try:
                sent = await self.webhooks.deliver_due()
            except Exception:
                log.exception("webhook delivery pass failed")
                sent = 0
            await asyncio.sleep(0.1 if sent else 1.0)

    async def recover_handoffs(self) -> None:
        """A worker died between popping a call and marking it dialing: put the call back."""
        ids = await self.queues.expired_handoffs()
        if not ids:
            return
        states = await asyncio.to_thread(call_states, ids)
        terminal = {s.value for s in TERMINAL_CALL_STATUSES}
        for call_id in ids:
            status, campaign_id = states.get(call_id, (None, None))
            if status in (None, CallStatus.queued.value) or status in terminal:
                await self.channels.release(call_id, campaign_id)  # the slot reserved at pop time
            if status == CallStatus.queued.value:
                log.warning("re-queueing call %s lost between pop and dial", call_id)
                await self.q.enqueue([call_id])
            await self.queues.ack_handoff(call_id)

    async def sweep_once(self) -> None:
        """Recover from crashes: reclaim stale messages, re-enqueue lost calls, fail stuck
        calls, and close finished campaigns."""
        res = await self.r.xautoclaim(
            keys.STREAM, keys.GROUP, self.consumer, min_idle_time=60_000, start_id="0-0", count=50
        )
        stale = [(msg_id, f["call_id"]) for msg_id, f in res[1] if f]
        if stale:
            await self.ingest(stale)

        await self.recover_handoffs()

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

        def alert(campaign_id: str) -> None:
            with SessionLocal() as db:
                alert_campaign_finished(db, campaign_id, self.alerts)

        for cid in await asyncio.to_thread(finalize):
            log.info("campaign %s done", cid)
            await asyncio.to_thread(alert, cid)

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
            asyncio.create_task(self._schedule_loop()),
            asyncio.create_task(self._sweep_loop()),
            asyncio.create_task(self._webhook_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.dialer.aclose()
            await self.webhooks.aclose()
            for t in list(self._inflight):
                t.cancel()
            await asyncio.gather(*self._inflight, return_exceptions=True)
