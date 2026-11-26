import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.db.models import CallStatus, utcnow
from app.db.session import SessionLocal
from app.services.lifecycle import ACTIVE, mark

log = logging.getLogger(__name__)

ReleaseFn = Callable[[str, str | None], Awaitable[None]]


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
