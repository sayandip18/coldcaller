"""Load test: push a campaign through the real system and check that the limits held.

    uv run python -m app.loadtest --calls 500 --concurrency 50 --cps 20 \\
        --email you@example.com --password '...'

It talks to the running API (so the API and worker must be up), creates a campaign of N
synthetic Indian numbers, starts it, samples Redis while it runs, and when the campaign ends
checks the database for what really happened:

  * concurrency never exceeded the campaign's limit (peak overlap of calls, computed exactly)
  * call starts never exceeded the CPS limit in any one-second window (small jitter allowed)
  * every call reached a final state, and every channel slot was given back
  * nothing was dead-lettered

What it exercises depends on how the worker runs: with DIALER=fake it measures the queue and
scheduler; with DIALER=livekit it measures the whole chain (LiveKit, bridge, agent).
Exit code 1 if any check fails.
"""

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime

import httpx
import redis
from sqlalchemy import delete, select

from app.config import get_settings
from app.db.models import (
    TERMINAL_CALL_STATUSES,
    Call,
    CallEvent,
    Campaign,
    WebhookDelivery,
)
from app.db.session import SessionLocal
from app.queue import keys

CPS_JITTER = 0.15  # call start times are recorded a few ms after the rate-limit slot is taken


def make_numbers(n: int, prefix: str = "9878") -> list[str]:
    """n distinct valid Indian mobile numbers (10 digits)."""
    return [f"{prefix}{i:06d}" for i in range(n)]


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, round(p / 100 * (len(s) - 1)))]


def peak_concurrency(intervals: list[tuple[float, float]]) -> int:
    """Largest number of calls in flight at once. Intervals are (start, end) in seconds."""
    events = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals], key=lambda x: (x[0], x[1]))
    cur = peak = 0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)
    return peak


def max_starts_in_a_second(starts: list[float]) -> int:
    s = sorted(starts)
    best, j = 0, 0
    for i in range(len(s)):
        while s[i] - s[j] >= 1.0:
            j += 1
        best = max(best, i - j + 1)
    return best


def analyse(rows: list[dict], *, concurrency_limit: int, cps_limit: int, global_limit: int) -> dict:
    """Pure function over call rows ({queued_at, dialed_at, answered_at, ended_at, status}) in
    epoch seconds, so the checks can be unit-tested without running anything."""
    terminal = {s.value for s in TERMINAL_CALL_STATUSES}
    dialed = [r for r in rows if r["dialed_at"] is not None]
    now = time.time()
    intervals = [(r["dialed_at"], r["ended_at"] or now) for r in dialed]
    starts = [r["dialed_at"] for r in dialed]
    waits = [r["dialed_at"] - r["queued_at"] for r in dialed if r["queued_at"]]
    answers = [r["answered_at"] - r["dialed_at"] for r in dialed if r["answered_at"]]
    talk = [r["ended_at"] - r["answered_at"] for r in dialed if r["answered_at"] and r["ended_at"]]
    statuses: dict[str, int] = {}
    for r in rows:
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    peak = peak_concurrency(intervals)
    per_second = max_starts_in_a_second(starts)
    window = max(starts) - min(starts) if len(starts) > 1 else 0.0
    gaps = sorted((b - a) * 1000 for a, b in zip(sorted(starts), sorted(starts)[1:], strict=False))
    cps_allowed = cps_limit + max(1, int(cps_limit * CPS_JITTER))
    return {
        "calls": len(rows),
        "statuses": statuses,
        "not_finished": sum(1 for r in rows if r["status"] not in terminal),
        "peak_concurrency": peak,
        "concurrency_limit": min(concurrency_limit, global_limit),
        "max_starts_in_1s": per_second,
        "cps_limit": cps_limit,
        "cps_allowed_with_jitter": cps_allowed,
        "dial_phase_seconds": round(window, 2),
        "start_gap_ms": {"p50": round(percentile(gaps, 50)), "p90": round(percentile(gaps, 90)),
                         "p99": round(percentile(gaps, 99)), "max": round(max(gaps)) if gaps else 0},
        "achieved_cps": round(len(starts) / window, 2) if window else None,
        "queue_wait_s": {"p50": round(percentile(waits, 50), 2), "p95": round(percentile(waits, 95), 2),
                         "max": round(max(waits), 2) if waits else 0},
        "time_to_answer_s": {"p50": round(percentile(answers, 50), 2), "p95": round(percentile(answers, 95), 2)},
        "talk_time_s": {"p50": round(percentile(talk, 50), 2), "mean": round(statistics.fmean(talk), 2) if talk else 0},
    }


def verdict(a: dict, *, leaked_leases: int, dlq: int) -> list[tuple[str, bool, str]]:
    return [
        ("every call reached a final state", a["not_finished"] == 0, f"{a['not_finished']} not finished"),
        ("concurrency stayed within the limit", a["peak_concurrency"] <= a["concurrency_limit"],
         f"peak {a['peak_concurrency']} / limit {a['concurrency_limit']}"),
        ("call starts stayed within CPS", a["max_starts_in_1s"] <= a["cps_allowed_with_jitter"],
         f"max {a['max_starts_in_1s']} in any 1 s / limit {a['cps_limit']} (+jitter {a['cps_allowed_with_jitter']})"),
        ("all channel slots were returned", leaked_leases == 0, f"{leaked_leases} still held"),
        ("nothing was dead-lettered", dlq == 0, f"{dlq} in the DLQ"),
    ]


def run(args: argparse.Namespace) -> int:
    settings = get_settings()
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    dlq_before = r.xlen(keys.DLQ)
    csv_body = "name,phone\n" + "\n".join(f"L{i},{n}" for i, n in enumerate(make_numbers(args.calls))) + "\n"

    with httpx.Client(base_url=args.api, timeout=30.0) as http:
        res = http.post("/auth/login", json={"email": args.email, "password": args.password})
        if res.status_code != 200:
            print(f"login failed: {res.status_code} {res.text}", file=sys.stderr)
            return 2
        res = http.post(
            "/campaigns",
            data={"name": f"loadtest {datetime.now():%H:%M:%S}", "script_prompt": "Hi {name}",
                  "max_concurrency": args.concurrency, "cps": args.cps, "caller_id": args.caller_id},
            files={"file": ("load.csv", csv_body.encode(), "text/csv")},
        )
        if res.status_code != 201:
            print(f"could not create the campaign: {res.status_code} {res.text}", file=sys.stderr)
            return 2
        created = res.json()
        cid = created["campaign"]["id"]
        print(f"campaign {cid}: {created['accepted']} calls, concurrency {args.concurrency}, cps {args.cps}, "
          f"global limit {args.global_limit or settings.max_global_channels}")
        http.post(f"/campaigns/{cid}/start").raise_for_status()

        t0 = time.monotonic()
        peak_leases, peak_depth, last_print = 0, 0, 0.0
        status = "running"
        while time.monotonic() - t0 < args.timeout:
            peak_leases = max(peak_leases, r.zcard(keys.campaign_channels(cid)))
            peak_depth = max(peak_depth, r.llen(keys.ready_queue(cid)))
            if time.monotonic() - last_print > 2:
                last_print = time.monotonic()
                c = http.get(f"/campaigns/{cid}").json()
                status = c["status"]
                print(f"  {time.monotonic() - t0:5.0f}s  {status:8} {c['counts']}")
                if status == "done":
                    break
            time.sleep(0.2)
        elapsed = time.monotonic() - t0
        if status != "done":
            print(f"TIMED OUT after {args.timeout}s", file=sys.stderr)

    time.sleep(1.0)  # let the last releases land
    with SessionLocal() as db:
        rows = [
            {
                "status": c.status,
                "queued_at": c.queued_at.timestamp() if c.queued_at else None,
                "dialed_at": c.dialed_at.timestamp() if c.dialed_at else None,
                "answered_at": c.answered_at.timestamp() if c.answered_at else None,
                "ended_at": c.ended_at.timestamp() if c.ended_at else None,
            }
            for c in db.scalars(select(Call).where(Call.campaign_id == cid))
        ]
    global_limit = args.global_limit or settings.max_global_channels
    a = analyse(rows, concurrency_limit=args.concurrency, cps_limit=args.cps, global_limit=global_limit)
    a["elapsed_s"] = round(elapsed, 1)
    a["calls_per_minute"] = round(len(rows) / elapsed * 60, 1) if elapsed else None
    a["peak_ready_queue_depth"] = peak_depth
    a["peak_leases_sampled"] = peak_leases
    leaked = r.zcard(keys.GLOBAL_CHANNELS)
    dlq = r.xlen(keys.DLQ) - dlq_before
    checks = verdict(a, leaked_leases=leaked, dlq=dlq)

    print("\n=== results ===")
    for k in ("calls", "elapsed_s", "calls_per_minute", "achieved_cps", "peak_concurrency", "max_starts_in_1s",
              "start_gap_ms", "peak_ready_queue_depth", "queue_wait_s", "time_to_answer_s", "talk_time_s", "statuses"):
        print(f"  {k:24} {a[k]}")
    print("\n=== checks ===")
    for name, ok, detail in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    report = {"settings": {k: getattr(args, k) for k in ("calls", "concurrency", "cps")},
              "dialer": settings.dialer, "results": a,
              "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks]}
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\nreport written to {args.report}")

    if args.cleanup:
        with SessionLocal() as db:
            ids = list(db.scalars(select(Call.id).where(Call.campaign_id == cid)))
            db.execute(delete(CallEvent).where(CallEvent.call_id.in_(ids)))
            db.execute(delete(WebhookDelivery).where(WebhookDelivery.campaign_id == cid))
            db.execute(delete(Call).where(Call.campaign_id == cid))
            db.execute(delete(Campaign).where(Campaign.id == cid))
            db.commit()
        print("test data removed")
    return 0 if all(ok for _, ok, _ in checks) else 1


def main() -> int:
    p = argparse.ArgumentParser(prog="app.loadtest", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--calls", type=int, default=200)
    p.add_argument("--concurrency", type=int, default=20, help="the campaign's max concurrent calls")
    p.add_argument("--cps", type=int, default=10, help="the campaign's calls per second")
    p.add_argument("--global-limit", type=int, default=0,
                   help="the WORKER's MAX_GLOBAL_CHANNELS (default: this process's own setting, which "
                        "must match the worker's or the concurrency check will be wrong)")
    p.add_argument("--caller-id", default=get_settings().default_caller_id)
    p.add_argument("--api", default="http://localhost:8000/api")
    p.add_argument("--email", default=os.environ.get("LOADTEST_EMAIL", ""))
    p.add_argument("--password", default=os.environ.get("LOADTEST_PASSWORD", ""))
    p.add_argument("--timeout", type=float, default=900.0, help="give up after this many seconds")
    p.add_argument("--report", help="write a JSON report to this path")
    p.add_argument("--cleanup", action="store_true", help="delete the campaign and its calls afterwards")
    args = p.parse_args()
    if not args.email or not args.password:
        p.error("--email and --password are required (or LOADTEST_EMAIL / LOADTEST_PASSWORD)")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
