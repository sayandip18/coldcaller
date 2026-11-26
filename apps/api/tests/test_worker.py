import asyncio
import os
import time as _time
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import select

from app.config import get_settings
from app.db.models import Call, Campaign, DncNumber, User
from app.db.session import SessionLocal
from app.queue import keys
from app.services.lifecycle import mark
from app.worker.dialer import FakeDialer
from app.worker.worker import Worker, plan_call

from .test_campaigns import create

TERMINAL = {"completed", "no_answer", "busy", "failed", "skipped_dnc"}


def fast_dialer(**kw):
    def factory(release, on_error=None):
        opts = {"ring_s": (0.05, 0.1), "talk_s": (0.2, 0.4), "seed": 1} | kw
        return FakeDialer(release, **opts)

    return factory


def numbers_csv(n: int) -> str:
    return "name,phone\n" + "\n".join(f"C{i},98765{i:05d}" for i in range(n)) + "\n"


def call_rows(campaign_id: str):
    with SessionLocal() as db:
        return list(db.scalars(select(Call).where(Call.campaign_id == campaign_id)))


def campaign_status(cid: str) -> str:
    with SessionLocal() as db:
        return db.get(Campaign, cid).status


async def wait_for(pred, timeout=30.0, step=0.05, probe=None):
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if probe:
            await probe()
        if await asyncio.to_thread(pred):
            return True
        await asyncio.sleep(step)
    return False


def run_worker(scenario, **worker_kw):
    async def main():
        r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        worker = Worker(r, sweep_interval=0.2, pause_delay_s=0.2, retry_base_s=0.05, **worker_kw)
        task = asyncio.create_task(worker.run())
        try:
            return await scenario(r, worker)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await r.aclose()

    return asyncio.run(main())


def test_campaign_respects_cps_and_concurrency(member):
    csv = numbers_csv(30)
    made = create(member, csv=csv, cps=10, max_concurrency=4).json()
    assert made["accepted"] == 30
    cid = made["campaign"]["id"]
    assert member.post(f"/api/campaigns/{cid}/start").status_code == 200

    peak = 0

    async def scenario(r, worker):
        nonlocal peak

        async def probe():
            nonlocal peak
            peak = max(peak, await r.zcard(keys.campaign_channels(cid)))

        done = await wait_for(lambda: campaign_status(cid) == "done", probe=probe)
        assert done, "campaign never finished"

    run_worker(
        scenario,
        dialer_factory=fast_dialer(),
        carrier_cps=100,
        max_global_channels=100,
    )

    rows = call_rows(cid)
    assert len(rows) == 30 and all(c.status in TERMINAL for c in rows)
    assert 2 <= peak <= 4  # parallel, but never above the campaign cap
    stamps = sorted(c.dialed_at.timestamp() for c in rows)
    # cps=10: no more than 10 calls may start inside any one-second window
    assert all(stamps[i + 10] - stamps[i] >= 0.8 for i in range(len(stamps) - 10))
    assert member.get(f"/api/campaigns/{cid}").json()["status"] == "done"


def test_global_channel_cap_applies_across_campaigns(member):
    ids = []
    for _ in range(2):
        made = create(member, csv=numbers_csv(8), cps=50, max_concurrency=50).json()
        cid = made["campaign"]["id"]
        member.post(f"/api/campaigns/{cid}/start")
        ids.append(cid)

    peak = 0

    async def scenario(r, worker):
        nonlocal peak

        async def probe():
            nonlocal peak
            peak = max(peak, await r.zcard(keys.GLOBAL_CHANNELS))

        ok = await wait_for(lambda: all(campaign_status(c) == "done" for c in ids), probe=probe)
        assert ok

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100, max_global_channels=3)
    assert 2 <= peak <= 3


def test_paused_campaign_is_not_dialed_until_resumed(member):
    cid = create(member, csv=numbers_csv(3)).json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    member.post(f"/api/campaigns/{cid}/pause")

    async def scenario(r, worker):
        await asyncio.sleep(1.0)
        assert {c.status for c in call_rows(cid)} == {"queued"}
        member.post(f"/api/campaigns/{cid}/resume")
        assert await wait_for(lambda: campaign_status(cid) == "done")

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100)


def test_dnc_added_after_queueing_is_honoured_at_dial_time(member):
    cid = create(member, csv="phone\n9876500001\n9876500002\n").json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    member.post("/api/dnc", json={"number": "9876500002"})

    async def scenario(r, worker):
        assert await wait_for(lambda: campaign_status(cid) == "done")

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100)
    by_number = {c.to_number: c for c in call_rows(cid)}
    assert by_number["+919876500002"].status == "skipped_dnc"
    assert by_number["+919876500002"].dialed_at is None
    assert by_number["+919876500001"].status in TERMINAL - {"skipped_dnc"}


def test_dial_errors_retry_then_dead_letter(member):
    call = member.post(
        "/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}
    ).json()

    async def scenario(r, worker):
        assert await wait_for(lambda: _status(call["id"]) == "failed")
        assert await r.xlen(keys.DLQ) == 1

    run_worker(scenario, dialer_factory=fast_dialer(dial_error_rate=1.0), carrier_cps=100)
    with SessionLocal() as db:
        row = db.get(Call, call["id"])
    assert row.attempt == get_settings().dial_max_attempts
    assert "dial failed" in row.failure_reason


def _status(call_id: str) -> str:
    with SessionLocal() as db:
        return db.get(Call, call_id).status


def test_single_call_runs_to_completion_and_frees_its_slot(member):
    call = member.post(
        "/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}
    ).json()

    async def scenario(r, worker):
        assert await wait_for(lambda: _status(call["id"]) in TERMINAL)
        await asyncio.sleep(0.1)
        assert await r.zcard(keys.GLOBAL_CHANNELS) == 0

    run_worker(scenario, dialer_factory=fast_dialer(p_no_answer=0, p_busy=0, p_fail=0), carrier_cps=100)
    detail = member.get(f"/api/calls/{call['id']}").json()
    assert detail["status"] == "completed" and detail["outcome"]["simulated"] is True
    assert [e["type"] for e in detail["events"]] == [
        "created", "queued", "dialing", "ringing", "answered", "in_progress", "completed",
    ]


def test_lost_enqueue_is_recovered_by_sweeper(member):
    """API committed the row but Redis never got the message."""
    from datetime import timedelta

    from app.db.models import utcnow

    call = member.post(
        "/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}
    ).json()

    async def scenario(r, worker):
        await r.flushdb()  # simulate the lost message
        with SessionLocal() as db:  # and age the row past the orphan threshold
            row = db.get(Call, call["id"])
            row.queued_at = utcnow() - timedelta(minutes=5)
            db.commit()
        assert await wait_for(lambda: _status(call["id"]) in TERMINAL)

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100)


def test_stuck_call_is_failed_and_slot_released(member):
    from app.services.lifecycle import reap_stuck_calls

    call = member.post(
        "/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}
    ).json()
    with SessionLocal() as db:
        from app.db.models import CallStatus

        assert mark(db, call["id"], [CallStatus.queued], CallStatus.dialing, dialed_at=datetime(2020, 1, 1, tzinfo=UTC))
        reaped = reap_stuck_calls(db, 600)
    assert [c for c, _ in reaped] == [call["id"]]
    assert _status(call["id"]) == "failed"


def test_plan_call_defers_outside_window_and_skips_dnc(member):
    from app.config import get_settings

    cid = create(member, csv="phone\n9876500001\n9876500002\n").json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    calls = {c.to_number: c.id for c in call_rows(cid)}
    settings = get_settings()

    # 08:30 IST -> wait until 09:00 IST (30 minutes)
    early = datetime(2026, 1, 5, 3, 0, tzinfo=UTC)
    plan = plan_call(calls["+919876500001"], settings, 5, now=early)
    assert plan.action == "delay" and abs(plan.delay_s - 1800) < 1

    noon = datetime(2026, 1, 5, 6, 30, tzinfo=UTC)
    assert plan_call(calls["+919876500001"], settings, 5, now=noon).action == "dial"

    with SessionLocal() as db:
        owner = db.scalar(select(User.id))
        db.add(DncNumber(owner_id=owner, number="+919876500002"))
        db.commit()
    assert plan_call(calls["+919876500002"], settings, 5, now=noon).action == "drop"
    assert _status(calls["+919876500002"]) == "skipped_dnc"


# ------------------------------------------------------ per-campaign queues (Phase 5)
def evalsha_calls(info: dict) -> int:
    return int(info.get("cmdstat_evalsha", {}).get("calls", 0))


def test_calls_waiting_behind_a_limit_cost_almost_nothing(member):
    """150 calls wait behind max_concurrency=1. They must not be polled one by one."""
    made = create(member, csv=numbers_csv(150), cps=50, max_concurrency=1).json()
    cid = made["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    used = {}

    async def scenario(r, worker):
        await r.config_resetstat()
        await asyncio.sleep(1.5)  # one slow call occupies the only slot the whole time
        used["evals"] = evalsha_calls(await r.info("commandstats"))
        used["depth"] = await worker.queues.depth(cid)

    run_worker(
        scenario,
        dialer_factory=fast_dialer(ring_s=(2.0, 2.0), talk_s=(2.0, 2.0)),
        carrier_cps=100,
    )
    assert used["depth"] >= 148  # nothing but the first call has left the queue
    # one scheduler pass per ~100 ms plus the delayed-set mover; the old design re-tried every call
    assert used["evals"] < 60, used


def test_a_slow_campaign_does_not_hold_up_another(member):
    slow = create(member, csv=numbers_csv(30), name="slow", cps=1, max_concurrency=5).json()
    quick = create(member, csv=numbers_csv(5).replace("98765", "98764"), name="quick", cps=50, max_concurrency=5).json()
    slow_id, quick_id = slow["campaign"]["id"], quick["campaign"]["id"]
    member.post(f"/api/campaigns/{slow_id}/start")
    member.post(f"/api/campaigns/{quick_id}/start")
    seen = {}

    async def scenario(r, worker):
        assert await wait_for(lambda: campaign_status(quick_id) == "done", timeout=10)
        seen["slow_still_waiting"] = await worker.queues.depth(slow_id)

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100, max_global_channels=50)
    assert seen["slow_still_waiting"] >= 20  # slow campaign is crawling at 1 call/second
    assert campaign_status(slow_id) == "running"


def test_call_lost_between_pop_and_dial_is_recovered(member):
    """Worker popped a call (slot reserved) and died before marking it dialing."""
    from app.queue.primitives import CampaignQueues

    call = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()

    async def crash():
        # Build the crashed state *before* any worker runs, so nothing races with it.
        r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        try:
            await r.delete(keys.STREAM)  # drop the normal enqueue
            cq = CampaignQueues(r, carrier_cps=100_000, global_max=100, lease_seconds=60)
            await cq.push(keys.SINGLE, [call["id"]])
            _code, popped = await cq.claim_next(keys.SINGLE, None, None)
            assert popped == call["id"] and await r.zcard(keys.GLOBAL_CHANNELS) == 1
            await r.zadd(keys.HANDOFF, {call["id"]: 0})  # its handoff deadline has passed
        finally:
            await r.aclose()

    asyncio.run(crash())
    assert _status(call["id"]) == "queued"  # popped from Redis, never dialled

    async def scenario(r, worker):
        # the worker's sweep notices the stale handoff, requeues the call and dials it normally
        assert await wait_for(lambda: _status(call["id"]) in TERMINAL, timeout=15)
        await asyncio.sleep(0.2)
        assert await r.zcard(keys.GLOBAL_CHANNELS) == 0
        assert await r.zcard(keys.HANDOFF) == 0

    run_worker(scenario, dialer_factory=fast_dialer(p_no_answer=0, p_busy=0, p_fail=0), carrier_cps=100)
    assert _status(call["id"]) == "completed"


def test_paused_campaign_keeps_its_calls_queued_and_resumes(member):
    cid = create(member, csv=numbers_csv(4)).json()["campaign"]["id"]
    member.post(f"/api/campaigns/{cid}/start")
    member.post(f"/api/campaigns/{cid}/pause")

    async def scenario(r, worker):
        await asyncio.sleep(1.2)
        assert await worker.queues.depth(cid) == 4  # untouched while paused
        member.post(f"/api/campaigns/{cid}/resume")
        assert await wait_for(lambda: campaign_status(cid) == "done", timeout=15)

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100)


def test_worker_survives_redis_losing_its_stream_and_consumer_group(member):
    """Redis restarted or was flushed under a running worker: it must not crash, and calls queued
    afterwards (and ones that were queued in the database before) still get dialled."""
    first = member.post("/api/calls", json={"to_number": "9876500001", "script_prompt": "hi"}).json()

    async def scenario(r, worker):
        assert await wait_for(lambda: _status(first["id"]) in TERMINAL, timeout=15)
        await r.flushall()  # the stream, the consumer group and everything else vanish
        await asyncio.sleep(1.5)  # the worker's stream read fails and it recovers
        second = member.post("/api/calls", json={"to_number": "9876500002", "script_prompt": "hi"}).json()
        assert await wait_for(lambda: _status(second["id"]) in TERMINAL, timeout=15)

    run_worker(scenario, dialer_factory=fast_dialer(), carrier_cps=100)
