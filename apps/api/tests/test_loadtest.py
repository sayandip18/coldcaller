from app.loadtest import (
    analyse,
    make_numbers,
    max_starts_in_a_second,
    peak_concurrency,
    percentile,
    verdict,
)
from app.services.phone import normalize_in_number


def test_synthetic_numbers_are_valid_distinct_indian_numbers():
    nums = make_numbers(2000)
    assert len(set(nums)) == 2000
    assert all(normalize_in_number(n) == f"+91{n}" for n in nums[:200] + nums[-200:])


def test_peak_concurrency_counts_overlap_exactly():
    assert peak_concurrency([(0, 5), (1, 6), (2, 7), (10, 11)]) == 3
    assert peak_concurrency([(0, 1), (1, 2), (2, 3)]) == 1  # back-to-back is not overlap
    assert peak_concurrency([]) == 0


def test_starts_per_second_uses_a_sliding_window():
    assert max_starts_in_a_second([0, 0.2, 0.4, 0.6, 0.8, 1.0]) == 5  # [0,1) holds five
    assert max_starts_in_a_second([0, 1, 2, 3]) == 1
    assert max_starts_in_a_second([0, 0.5, 1.4, 1.6]) == 2  # no one-second window holds three
    assert max_starts_in_a_second([0, 0.4, 0.8, 1.2]) == 3  # 0, 0.4, 0.8 (and 0.4, 0.8, 1.2)


def test_percentile():
    assert percentile([], 95) == 0
    assert percentile([1, 2, 3, 4, 5], 50) == 3
    assert percentile(list(range(1, 101)), 95) in (95, 96)


def rows(n, gap, hold, status="completed"):
    return [
        {"status": status, "queued_at": 0.0, "dialed_at": i * gap, "answered_at": i * gap + 0.5,
         "ended_at": i * gap + hold}
        for i in range(n)
    ]


def checks(a, leaked=0, dlq=0):
    return {name: ok for name, ok, _ in verdict(a, leaked_leases=leaked, dlq=dlq)}


def test_a_well_behaved_run_passes_every_check():
    a = analyse(rows(40, gap=0.1, hold=0.3), concurrency_limit=5, cps_limit=10, global_limit=100)
    assert a["peak_concurrency"] <= 5 and a["max_starts_in_1s"] <= 11
    assert all(checks(a).values())


def test_a_concurrency_overrun_is_caught():
    a = analyse(rows(20, gap=0.1, hold=5.0), concurrency_limit=5, cps_limit=10, global_limit=100)
    assert a["peak_concurrency"] == 20
    assert checks(a)["concurrency stayed within the limit"] is False


def test_the_global_cap_counts_when_it_is_lower_than_the_campaigns():
    a = analyse(rows(20, gap=0.1, hold=5.0), concurrency_limit=50, cps_limit=10, global_limit=8)
    assert a["concurrency_limit"] == 8 and checks(a)["concurrency stayed within the limit"] is False


def test_a_cps_burst_is_caught_but_jitter_is_tolerated():
    burst = analyse(rows(30, gap=0.01, hold=0.1), concurrency_limit=100, cps_limit=10, global_limit=100)
    assert checks(burst)["call starts stayed within CPS"] is False
    jitter = analyse(rows(30, gap=0.095, hold=0.1), concurrency_limit=100, cps_limit=10, global_limit=100)
    assert checks(jitter)["call starts stayed within CPS"] is True  # 11 in one second is within 15% + 1


def test_unfinished_calls_leaked_slots_and_dead_letters_fail_the_run():
    r = rows(5, 0.1, 0.2) + [{"status": "dialing", "queued_at": 0.0, "dialed_at": 1.0, "answered_at": None, "ended_at": None}]
    a = analyse(r, concurrency_limit=50, cps_limit=50, global_limit=100)
    assert a["not_finished"] == 1 and checks(a)["every call reached a final state"] is False
    ok = analyse(rows(5, 0.1, 0.2), concurrency_limit=50, cps_limit=50, global_limit=100)
    assert checks(ok, leaked=2)["all channel slots were returned"] is False
    assert checks(ok, dlq=1)["nothing was dead-lettered"] is False
