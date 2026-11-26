import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from livekit import api as lk_api
from sqlalchemy import select

from app.db.models import Call, CallStatus, utcnow
from app.db.session import SessionLocal
from app.livekit_client import LiveKitClient, classify_twirp_error
from app.services.lifecycle import ACTIVE, mark

log = logging.getLogger(__name__)

ReleaseFn = Callable[[str, str | None], Awaitable[None]]
# Called when a dial attempt failed for infrastructure reasons and should be retried.
ErrorFn = Callable[["DialRequest", str], Awaitable[None]]


@dataclass
class DialRequest:
    call_id: str
    to_number: str
    caller_id: str
    campaign_id: str | None
    room: str


class Dialer:
    """Starts a call and returns immediately. The call's later progress is reported through
    the lifecycle (fake: a simulation task; LiveKit: webhooks), which also releases the channel."""

    async def dial(self, req: DialRequest) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        pass


class FakeDialer(Dialer):
    """Simulates ringing, answer, conversation and hang-up so the pipeline runs without a carrier."""

    def __init__(
        self,
        release: ReleaseFn,
        on_error: ErrorFn | None = None,
        *,
        ring_s: tuple[float, float] = (1.0, 3.0),
        talk_s: tuple[float, float] = (3.0, 10.0),
        p_no_answer: float = 0.15,
        p_busy: float = 0.05,
        p_fail: float = 0.03,
        dial_error_rate: float = 0.0,
        seed: int | None = None,
    ):
        self.release = release
        self.ring_s, self.talk_s = ring_s, talk_s
        self.p_no_answer, self.p_busy, self.p_fail = p_no_answer, p_busy, p_fail
        self.dial_error_rate = dial_error_rate
        self.rng = random.Random(seed)
        self._tasks: set[asyncio.Task] = set()

    async def dial(self, req: DialRequest) -> None:
        if self.rng.random() < self.dial_error_rate:
            raise ConnectionError("simulated carrier error")
        task = asyncio.create_task(self._simulate(req))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _simulate(self, req: DialRequest) -> None:
        try:
            await asyncio.sleep(self.rng.uniform(*self.ring_s))
            if not await self._step(req, ACTIVE, CallStatus.ringing):
                return
            roll = self.rng.random()
            if roll < self.p_no_answer:
                await self._step(req, ACTIVE, CallStatus.no_answer)
            elif roll < self.p_no_answer + self.p_busy:
                await self._step(req, ACTIVE, CallStatus.busy)
            elif roll < self.p_no_answer + self.p_busy + self.p_fail:
                await self._step(req, ACTIVE, CallStatus.failed, failure_reason="simulated failure")
            else:
                await self._step(req, ACTIVE, CallStatus.answered, answered_at=utcnow())
                await self._step(req, ACTIVE, CallStatus.in_progress)
                await asyncio.sleep(self.rng.uniform(*self.talk_s))
                await self._step(
                    req,
                    ACTIVE,
                    CallStatus.completed,
                    outcome={"result": self.rng.choice(["interested", "not_interested", "callback"]),
                             "simulated": True},
                    transcript="[simulated call]",
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("fake call %s crashed", req.call_id)
        finally:
            await self.release(req.call_id, req.campaign_id)

    async def _step(self, req: DialRequest, from_statuses, to: CallStatus, **fields) -> bool:
        def run() -> bool:
            with SessionLocal() as db:
                return mark(db, req.call_id, from_statuses, to, **fields)

        return await asyncio.to_thread(run)

    async def aclose(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)


class LiveKitDialer(Dialer):
    """Places real calls through LiveKit SIP.

    `dial()` returns immediately; a background task waits for the callee to answer (LiveKit
    blocks the CreateSIPParticipant request until then), marks the call answered and
    dispatches the voice agent into the room. The channel lease stays held for the whole
    conversation and is released by the LiveKit `room_finished` webhook (or, if a webhook is
    lost, by the worker's lease reconciliation).
    """

    def __init__(
        self,
        release: ReleaseFn,
        on_error: ErrorFn,
        client: LiveKitClient,
        *,
        trunk_id: str,
        agent_name: str,
        ring_timeout_s: int,
        max_call_s: int,
        agent_join_timeout_s: float = 10.0,
        dispatch_retry_delays: tuple[float, ...] = (1.0, 2.0),
    ):
        self.agent_join_timeout_s = agent_join_timeout_s
        self.dispatch_retry_delays = dispatch_retry_delays
        self.release, self.on_error, self.client = release, on_error, client
        self.trunk_id, self.agent_name = trunk_id, agent_name
        self.ring_timeout_s, self.max_call_s = ring_timeout_s, max_call_s
        self._tasks: dict[asyncio.Task, DialRequest] = {}

    async def dial(self, req: DialRequest) -> None:
        task = asyncio.create_task(self._run(req))
        self._tasks[task] = req
        task.add_done_callback(lambda t: self._tasks.pop(t, None))

    async def _run(self, req: DialRequest) -> None:
        try:
            await self.client.dial(
                trunk_id=self.trunk_id,
                to_number=req.to_number,
                from_number=req.caller_id,
                room=req.room,
                identity=f"sip-{req.call_id}",
                attributes={"call_id": req.call_id},
                ring_timeout_s=self.ring_timeout_s,
                max_call_s=self.max_call_s,
            )
        except asyncio.CancelledError:
            raise
        except lk_api.TwirpError as exc:
            failure = classify_twirp_error(exc)
            if failure is None:
                log.warning("LiveKit error dialing %s: %s", req.call_id, exc.message)
                await self.on_error(req, f"livekit: {exc.message}")
                return
            await self._finish(req, CallStatus(failure.kind), failure.reason)
            return
        except Exception as exc:  # noqa: BLE001 - network/auth problems are retried
            log.warning("could not reach LiveKit to dial %s: %s", req.call_id, exc)
            await self.on_error(req, f"livekit unreachable: {exc}")
            return

        # The callee answered.
        if not await self._step(req, ACTIVE, CallStatus.answered, answered_at=utcnow()):
            # The call was failed/reaped while ringing; don't leave a live call behind.
            await self._hang_up(req)
            return
        # The customer has already answered, so a refused dispatch is a silent call. "No available
        # nodes" is usually momentary (every agent worker is busy starting one), so try again a
        # couple of times before giving up on the call.
        metadata = json.dumps({"call_id": req.call_id})
        for attempt, delay in enumerate((*self.dispatch_retry_delays, None), start=1):
            try:
                await self.client.dispatch_agent(req.room, self.agent_name, metadata)
                break
            except Exception as exc:
                if delay is None:
                    log.exception("agent dispatch failed for %s after %d attempts", req.call_id, attempt)
                    await self._hang_up(req)
                    await self._finish(req, CallStatus.failed, f"agent dispatch failed: {exc}")
                    return
                log.warning("agent dispatch for %s failed (%s); retrying in %.1fs", req.call_id, exc, delay)
                await asyncio.sleep(delay)

        # LiveKit accepts a dispatch even when no agent is running (e.g. mid-deploy) and then
        # drops it, which would leave the customer on a silent line. Insist the agent shows up.
        if not await self._agent_joined(req):
            log.warning("agent never joined %s; hanging up", req.call_id)
            await self._hang_up(req)
            await self._finish(req, CallStatus.failed, "agent did not join the call in time")

    async def _agent_joined(self, req: DialRequest) -> bool:
        """True once the call has moved past `answered` (the webhook marks `in_progress`)."""

        def still_waiting() -> bool:
            with SessionLocal() as db:
                return db.scalar(select(Call.status).where(Call.id == req.call_id)) == CallStatus.answered.value

        deadline = asyncio.get_running_loop().time() + self.agent_join_timeout_s
        while asyncio.get_running_loop().time() < deadline:
            if not await asyncio.to_thread(still_waiting):
                return True
            await asyncio.sleep(0.25)
        return not await asyncio.to_thread(still_waiting)

    async def _finish(self, req: DialRequest, status: CallStatus, reason: str) -> None:
        await self._step(
            req,
            ACTIVE,
            status,
            event=status.value,
            payload={"reason": reason},
            failure_reason=reason[:255],
        )
        await self.release(req.call_id, req.campaign_id)

    async def _hang_up(self, req: DialRequest) -> None:
        try:
            await self.client.delete_room(req.room)
        except Exception:  # noqa: BLE001
            log.warning("could not delete room %s", req.room)

    async def _step(self, req: DialRequest, from_statuses, to: CallStatus, **fields) -> bool:
        def run() -> bool:
            with SessionLocal() as db:
                return mark(db, req.call_id, from_statuses, to, **fields)

        return await asyncio.to_thread(run)

    async def aclose(self) -> None:
        # Worker shutting down: calls still ringing are abandoned, so hang them up.
        pending = dict(self._tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for req in pending.values():
            await self._hang_up(req)
        await self.client.aclose()
