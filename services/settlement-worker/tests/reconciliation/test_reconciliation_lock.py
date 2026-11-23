"""The reconciliation lock proof (D-144, D-115, D-116; docs/design/cache-invalidation.md §1.3, §5 rows 2d and 4d, §7).

Several real workers (LEDGER_TEST_LEASE_WORKERS, default 3) share one Postgres and one Redis (lock_rs), each with
REDIS_URL, RECONCILE_INTERVAL_MS=200, RECONCILE_LOCK_TTL_MS=3000 and DB timeouts at the bound (2 x 1000 + 1000). Each
worker has its own fresh consumer group (D-123; no records flow). Every case starts by deleting the lease key, so
nothing bleeds between cases, and stops its workers (exit 0), except the holder case (3) kills.

(1) contention the test creates: one completion within 3 s of the test deleting its own lease, every other worker
skipping. (2) spacing: consecutive runs' as_of (the DB clock) are a lease period apart, give or take ε = 300 ms, and
never far more (someone always picks the lease up). (3) a dead holder: its lease still blocks the survivors until it
expires, then a survivor runs. (4) Redis stopped: every worker runs (fail open), with the 1 s bypass between real
attempts; after Redis returns, runs are spaced again. The config bound (5) is T12's test_an_invalid_lock_ttl_exits_2.

Clocks: spacing uses as_of (the DB's clock); windows use the workers' own log timestamps (the host's clock), compared
only with host time, never with as_of.

Lease-holding vs fail-open runs: a run is lease-holding iff its worker logged no reconciliation_lock_unavailable for
that run_id. A lease SET that times out (the 300 ms bound; seen on a loaded machine) makes that tick fail open and run
(D-144), and the 1 s bypass makes the next ticks run too: designed behaviour, reported but not held to the lease's
claims. Cases (1)-(3) assert exclusivity over lease-holding runs only. The lower bound on their spacing stays exact even
when a timed-out SET executed late on the server: that late SET holds the key, so every other worker's attempt is
"held" until its server-side PX ends, and the lease-holding runs stay a PX apart.
"""

import contextlib
import os
import re
import statistics
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

import docker

from support import cache_kit
from support.failure_kit import fresh_group
from support.worker_process import start_worker

WORKERS = int(os.environ.get("LEDGER_TEST_LEASE_WORKERS", "3"))
PERIODS = int(os.environ.get("LEDGER_TEST_LEASE_PERIODS", "5"))
LEASE_KEY = "ledger:v1:reconcile:lease"
TTL_MS = 3000
INTERVAL_MS = 200
EPSILON_MS = 300
SLACK_MS = 1000
ENV = {
    "RECONCILE_INTERVAL_MS": str(INTERVAL_MS),
    "RECONCILE_LOCK_TTL_MS": str(TTL_MS),
    "WORKER_LOCK_TIMEOUT_MS": "500",
    "WORKER_DB_COMMAND_TIMEOUT_MS": "1000",
    "RECONCILE_STATEMENT_TIMEOUT_MS": "1000",
}
UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ms(delta: timedelta) -> float:
    return delta.total_seconds() * 1000


def _as_of(event: dict) -> datetime:
    return datetime.fromisoformat(event["as_of"])


def _redis(rs, body):
    return cache_kit.raw_redis(rs.redis.url, body)


def _del_lease(rs) -> None:
    _redis(rs, lambda client: client.delete(LEASE_KEY))


def _lease(rs) -> tuple[str | None, int]:
    async def body(client):
        return await client.get(LEASE_KEY), await client.pttl(LEASE_KEY)

    return _redis(rs, body)


def _start_workers(rs, label: str, stack: contextlib.ExitStack) -> list:
    workers = []
    for index in range(WORKERS):
        group = fresh_group(rs, f"lock-{label}-{index}-{uuid.uuid4().hex[:6]}")
        worker = stack.enter_context(
            start_worker(
                database=rs.database,
                bootstrap_servers=rs.kafka.bootstrap_servers,
                group=group,
                log_dir=rs.log_dir,
                extra_env={"REDIS_URL": rs.redis.url, **ENV},
            )
        )
        workers.append(worker)
    for worker in workers:
        started = worker.wait_for("worker_started")
        assert (started["reconcile_lease"], started["reconcile_lock_ttl_ms"]) == ("on", TTL_MS), worker.diagnostics()
    return workers


def _events(workers, name: str, start: datetime | None = None, end: datetime | None = None) -> list[tuple[int, dict]]:
    out = []
    for index, worker in enumerate(workers):
        for event in worker.events(name):
            at = cache_kit.timestamp(event)
            if (start is None or at >= start) and (end is None or at <= end):
                out.append((index, event))
    return sorted(out, key=lambda pair: cache_kit.timestamp(pair[1]))


def _runs(workers, start: datetime | None = None, end: datetime | None = None) -> list[tuple[int, dict, bool]]:
    """(worker index, reconciliation_completed, lease-holding?) in log order. Lease-holding iff that worker logged no
    reconciliation_lock_unavailable for the run's run_id (run_ids are per worker)."""
    out = []
    for index, worker in enumerate(workers):
        failed_open = {e["run_id"] for e in worker.events("reconciliation_lock_unavailable")}
        for event in worker.events("reconciliation_completed"):
            at = cache_kit.timestamp(event)
            if (start is None or at >= start) and (end is None or at <= end):
                out.append((index, event, event["run_id"] not in failed_open))
    return sorted(out, key=lambda run: cache_kit.timestamp(run[1]))


def _fail_open_report(workers, runs) -> list[tuple[int, int, str]]:
    """(worker, run_id, error_class) for each fail-open run: report-only; a non-bypass class is a real Redis stall."""
    classes = {
        (index, e["run_id"]): e["error_class"]
        for index, worker in enumerate(workers)
        for e in worker.events("reconciliation_lock_unavailable")
    }
    return [(index, e["run_id"], classes[(index, e["run_id"])]) for index, e, holding in runs if not holding]


def _wait(condition, timeout: float, what: str, workers):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"{what} within {timeout} s:\n" + "\n".join(w.diagnostics() for w in workers))


def _answers(rs, timeout: float = 30) -> None:
    """The same REDIS_URL answers a PING. Errors while the restarted container comes up are retried, not raised."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if _redis(rs, lambda client: client.ping()):
                return
        except Exception as ex:  # noqa: BLE001 - not up yet (e.g. "Connection closed by server" mid-start)
            last = ex
        time.sleep(0.2)
    raise AssertionError(f"{rs.redis.url} didn't answer within {timeout} s: {last!r}")


def _stop_all(workers) -> None:
    for worker in workers:
        if worker.process.poll() is None:
            assert worker.stop() == 0, worker.diagnostics()


def _clean(workers) -> None:
    for worker in workers:
        for name in ("reconciliation_failed", "processing_failed", "drift_detected"):
            assert worker.events(name) == [], f"{name}:\n{worker.diagnostics()}"


def _kill_hard(worker) -> None:
    """No graceful stop. On Windows the venv's python.exe is a launcher that spawns the real interpreter, so the
    whole tree is terminated (taskkill /F /T: TerminateProcess); elsewhere SIGKILL (Popen.kill)."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(worker.process.pid)], capture_output=True, check=False)
    else:
        worker.process.kill()
    worker.process.wait(timeout=30)


def _max_duration_ms(workers) -> int:
    return max((e["duration_ms"] for _i, e in _events(workers, "reconciliation_completed")), default=0)


# (1)
def test_contention_has_exactly_one_winner(lock_rs):
    rs = lock_rs
    _del_lease(rs)
    _redis(rs, lambda client: client.set(LEASE_KEY, f"test:0:{uuid.uuid4()}", px=60_000))
    with contextlib.ExitStack() as stack:
        workers = _start_workers(rs, "contention", stack)
        _wait(lambda: all(w.events("reconciliation_skipped") for w in workers), 60, "every worker skipping", workers)
        assert _events(workers, "reconciliation_completed") == [], "nobody ran while the test held the lease"

        t0 = _now()
        _del_lease(rs)
        end = t0 + timedelta(milliseconds=TTL_MS)
        _wait(lambda: any(cache_kit.timestamp(e) > end for w in workers for e in w.lines()), 30, "logs past the window",
              workers)
        runs = _runs(workers, t0, end)
        holding = [(i, e) for i, e, h in runs if h]
        print(f"RECONCILE_LOCK contention leaseHolding={[(i, cache_kit.timestamp(e).isoformat()) for i, e in holding]} "
              f"failOpen={_fail_open_report(workers, runs)}")
        assert len(holding) == 1, runs
        winner = holding[0][0]
        for index in range(len(workers)):
            if index != winner:
                assert _events([workers[index]], "reconciliation_skipped", t0, end), f"worker {index} didn't skip"
        _stop_all(workers)
    _clean(workers)


# (2)
def test_runs_are_a_lease_period_apart(lock_rs):
    rs = lock_rs
    _del_lease(rs)
    with contextlib.ExitStack() as stack:
        workers = _start_workers(rs, "spacing", stack)
        _wait(lambda: sum(1 for _i, _e, h in _runs(workers) if h) >= PERIODS + 1,
              (PERIODS + 3) * (TTL_MS + INTERVAL_MS + SLACK_MS) / 1000 + 60, f"{PERIODS + 1} lease-holding runs",
              workers)
        runs = sorted(_runs(workers), key=lambda run: _as_of(run[1]))
        holding = [run for run in runs if run[2]][: PERIODS + 1]
        span = [run for run in runs if _as_of(run[1]) <= _as_of(holding[-1][1])]
        gaps = [_ms(_as_of(b[1]) - _as_of(a[1])) for a, b in zip(holding, holding[1:])]
        any_gaps = [_ms(_as_of(b[1]) - _as_of(a[1])) for a, b in zip(span, span[1:])]
        upper = TTL_MS + INTERVAL_MS + _max_duration_ms(workers) + SLACK_MS
        print(f"RECONCILE_LOCK spacing periods={PERIODS} workers={WORKERS} gapsMs={[round(g) for g in gaps]} "
              f"min={min(gaps):.0f} median={statistics.median(gaps):.0f} max={max(gaps):.0f} upperMs={upper} "
              f"ranBy={[i for i, _e, _h in holding]} failOpen={_fail_open_report(workers, span)} "
              f"cpu_count={os.cpu_count()}")
        # Lower bound: between lease-holding runs only. Upper bound: across runs of any kind (someone always runs).
        assert min(gaps) >= TTL_MS - EPSILON_MS, gaps
        assert max(any_gaps) <= upper, any_gaps
        for index, worker in enumerate(workers):
            assert worker.events("reconciliation_skipped"), f"worker {index} never skipped"
        _stop_all(workers)
    _clean(workers)


# (3)
def test_a_dead_holders_lease_blocks_until_it_expires(lock_rs):
    rs = lock_rs
    _del_lease(rs)
    with contextlib.ExitStack() as stack:
        workers = _start_workers(rs, "dead-holder", stack)
        after_start = _now()
        first = _wait(lambda: [r for r in _runs(workers, after_start) if r[2]], 60, "a lease-holding run", workers)
        holder_index, _completed, _holding = first[0]
        value, pttl = _lease(rs)
        read_at = _now()
        assert value is not None and 0 < pttl <= TTL_MS, (value, pttl)
        assert re.fullmatch(rf"[^:]+:\d+:{UUID}", value), value
        if os.name != "nt":  # on Windows the lease carries the launcher's child pid (see _kill_hard)
            assert int(value.split(":")[1]) == workers[holder_index].process.pid, (value, holder_index)
        expiry = read_at + timedelta(milliseconds=pttl)

        holder = workers[holder_index]
        _kill_hard(holder)
        killed_at = _now()
        survivors = [w for i, w in enumerate(workers) if i != holder_index]
        upper = expiry + timedelta(milliseconds=INTERVAL_MS + _max_duration_ms(workers) + SLACK_MS)
        run = _wait(lambda: [r for r in _runs(survivors, killed_at) if r[2]],
                    (pttl + INTERVAL_MS + SLACK_MS) / 1000 + 30, "a survivor's lease-holding run after the holder died",
                    workers)
        first_run_at = cache_kit.timestamp(run[0][1])
        delay = _ms(first_run_at - expiry)
        skipped_before_expiry = _events(survivors, "reconciliation_skipped", killed_at, expiry)
        print(f"RECONCILE_LOCK dead-holder value={value.split(':', 1)[1]} pttlMs={pttl} "
              f"expiryToFirstSurvivorRunMs={delay:.0f} survivorSkipsBeforeExpiry={len(skipped_before_expiry)} "
              f"failOpen={_fail_open_report(survivors, _runs(survivors, killed_at, first_run_at))}")
        assert skipped_before_expiry, "the dead holder's lease must still block the survivors"
        assert first_run_at >= expiry - timedelta(milliseconds=EPSILON_MS), (first_run_at, expiry)
        assert first_run_at <= upper, (first_run_at, upper)
        assert holder.process.poll() is not None
        assert not [e for e in holder.lines() if cache_kit.timestamp(e) > killed_at + timedelta(seconds=1)]
        _stop_all(survivors)
    _clean(survivors)


# (4)
def test_with_redis_stopped_every_worker_runs_then_spacing_returns(lock_rs):
    rs = lock_rs
    _del_lease(rs)
    container = docker.from_env().containers.get(rs.redis.container_id)
    with contextlib.ExitStack() as stack:
        workers = _start_workers(rs, "redis-down", stack)
        _wait(lambda: _events(workers, "reconciliation_completed"), 60, "a first run", workers)
        container.stop(timeout=0)
        stopped_at = _now()
        try:
            time.sleep(4)
        finally:
            ended_at = _now()
            container.start()
        _answers(rs)
        window_s = (ended_at - stopped_at).total_seconds()

        report = []
        for index, worker in enumerate(workers):
            unavailable = _events([worker], "reconciliation_lock_unavailable", stopped_at, ended_at)
            bypass = [e for _i, e in unavailable if e["error_class"] == "bypass"]
            real = [e for _i, e in unavailable if e["error_class"] != "bypass"]
            runs = _events([worker], "reconciliation_completed", stopped_at, ended_at)
            report.append((index, len(real), len(bypass), len(runs)))
            assert runs, f"worker {index} didn't run with Redis stopped (fail open)"
            assert unavailable, f"worker {index} logged no reconciliation_lock_unavailable"
            assert bypass, f"worker {index}: no bypassed attempt (the 1 s bypass)"
            assert len(real) <= window_s + 2, f"worker {index}: {len(real)} real attempts in {window_s:.1f} s"
            for _i, run in runs:
                assert (run["drifted_accounts"], run["global_sum"], run["escrow_drift"]) == (0, "0.0000", "0.0000")
        print(f"RECONCILE_LOCK redis-down windowS={window_s:.1f} perWorker(index, realAttempts, bypassed, runs)={report}")

        # Spaced again: past 2 lease periods (and the bypass), consecutive runs are a period apart.
        settle = _now() + timedelta(milliseconds=2 * TTL_MS + 1000)
        after = _wait(lambda: (lambda c: c if len(c) >= 3 else None)(
            sorted(_events(workers, "reconciliation_completed", settle), key=lambda pair: _as_of(pair[1]))),
            40, "three runs after Redis returned", workers)
        gaps = [_ms(_as_of(b[1]) - _as_of(a[1])) for a, b in zip(after, after[1:])]
        print(f"RECONCILE_LOCK redis-back gapsMs={[round(g) for g in gaps]}")
        assert min(gaps) >= TTL_MS - EPSILON_MS, gaps
        _stop_all(workers)
    _clean(workers)
