"""Reconciliation reports no false drift under load: the D-019 proof (D-114, D-118).

On this module's own stack (relay on), one worker in a fresh group settles while it reconciles every
200 ms, the only reconciling instance (D-115). Seeded through the API (D-103): 1,600 CUSTOMER and 400
MERCHANT accounts, the first 200 customers funded with 100,000.00 each. Then 8 threads send real
HTTP requests, from a seeded RNG (RECONCILE_SNAPSHOT_SEED): 40% transfers from a working customer to
any other CUSTOMER or MERCHANT, 20% deposits to any of the 2,000, 40% authorizes from a working
customer to any merchant, one in four declined (cents 13, D-073); amounts 0.01-9.99, never cents 91
or 92 (reserved for the processor failure proofs). No ordering can 422 (D-087's pre-funding): before
each transfer or authorize, its amount is added to the sender's running outflow, inflows never
counted, and a request that would take it past its funding fails the test instead of being sent.

The load runs until RECONCILE_RUNS runs (default 20, CI; the soak sets more) have as_of later than
T_start, read just before the load. When it stops, before any drain, the fixture records and prints
the load-window runs' clean check, the in-flight tripwire and the summary line; then the worker
drains and one more run is observed. The fixture raises on neither the checks nor the drain, so a
slow drain can't hide the proof: each test asserts one of them. Every run, before, during and after
the load, must report 0 on all three checks. The snapshot is one statement, so it has no seam a test
could hold open: the proof is load-based, and its tripwire shows the load reached the snapshots
(D-067, D-106).
"""

import asyncio
import json
import os
import random
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pytest

from support import hardware
from support.failure_kit import fresh_group, worker
from support.worker_process import group_lag

CUSTOMERS = 1600
MERCHANTS = 400
WORKING = 200
FUND = Decimal("100000.00")
THREADS = 8
SEED_THREADS = 8
RUN_TIMEOUT_S = 30
# UNCALIBRATED (D-106): the floor of runs with a write in flight at the snapshot starts at 2 until
# it is calibrated from this test's CI history plus 10 fresh runs.
RECONCILE_INFLIGHT_FLOOR = 2


def _positive_int(name: str, default: str) -> int:
    raw = os.environ.get(name, default)
    if not raw.isdigit() or int(raw) < 1:
        raise pytest.UsageError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


RECONCILE_RUNS = _positive_int("RECONCILE_RUNS", "20")
_seed = os.environ.get("RECONCILE_SNAPSHOT_SEED", "")
if _seed and not _seed.isdigit():
    raise pytest.UsageError(f"RECONCILE_SNAPSHOT_SEED must be a non-negative integer, got {_seed!r}")
SEED = int(_seed) if _seed else random.SystemRandom().randrange(1 << 40)

# The load stops after RECONCILE_RUNS runs; a run under load takes well under a second, so 2 s a run
# is ample.
LOAD_TIMEOUT_S = 60 + 2 * RECONCILE_RUNS


def drain_timeout_s(backlog: int) -> float:
    """120 s plus the load's payments still AUTHORIZED at load stop at 10 a second: half the idle
    settle rate measured on the local soak (~22-25/s, one worker)."""
    return 120 + backlog / 10


def run(coro):
    return asyncio.run(coro)


async def _fetch(db, query, *args):
    conn = await db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def _app(rs, query, *args):
    return run(_fetch(rs.app_db, query, *args))


def _clock(rs) -> datetime:
    return _app(rs, "SELECT clock_timestamp() AS now")[0]["now"]


def _time(text: str) -> datetime:
    """A D-097 time (UTC, six fractional digits, Z)."""
    return datetime.fromisoformat(text)


def _nearest_rank(values: list[int], percent: int):
    ordered = sorted(values)
    return ordered[max(0, (percent * len(ordered) + 99) // 100 - 1)] if ordered else "na"


def _amount(rng: random.Random, *, declined: bool = False) -> Decimal:
    """0.01-9.99; declined is N.13, approved never has cents 13, 91 or 92."""
    if declined:
        return Decimal(f"{rng.randrange(10)}.13")
    while True:
        cents = rng.randrange(1, 1000)
        if cents % 100 not in (13, 91, 92):
            return (Decimal(cents) / 100).quantize(Decimal("0.01"))


class Load:
    """THREADS threads of real HTTP requests until stopped; every outcome is recorded."""

    def __init__(self, api, working: list[int], everyone: list[int], merchants: list[int]) -> None:
        self._api = api
        self._working = working
        self._everyone = everyone
        self._merchants = merchants
        self._lock = threading.Lock()
        self._outflow = {customer: Decimal(0) for customer in working}
        self.stop = threading.Event()
        self.statuses: Counter = Counter()
        self.failures: list = []
        self.payments: list[uuid.UUID] = []
        self.exhausted: str | None = None
        self._threads = [threading.Thread(target=self._run, args=(i,), daemon=True) for i in range(THREADS)]

    def start(self) -> None:
        for t in self._threads:
            t.start()

    def join(self, timeout: float = 120) -> None:
        self.stop.set()
        for t in self._threads:
            t.join(timeout)
        assert not any(t.is_alive() for t in self._threads), f"load threads still running after {timeout} s"

    @property
    def requests(self) -> int:
        return sum(self.statuses.values())

    def alive(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def _reserve(self, customer: int, amount: Decimal) -> bool:
        """Adds amount to the customer's outflow, unless that would take it past FUND."""
        with self._lock:
            if self._outflow[customer] + amount > FUND:
                self.exhausted = f"funding exhausted: customer {customer} at {self._outflow[customer]} + {amount}"
                return False
            self._outflow[customer] += amount
            return True

    def _request(self, rng: random.Random) -> tuple[str, str, dict] | None:
        """(kind, path, body) of the next request, or None if it would overdraw its sender."""
        choice = rng.random()
        if choice < 0.2:
            return "deposit", "/v1/deposits", {"account_id": rng.choice(self._everyone), "amount": str(_amount(rng))}
        source = rng.choice(self._working)
        if choice < 0.6:
            target = source
            while target == source:
                target = rng.choice(self._everyone)
            amount = _amount(rng)
            kind, path, body = "transfer", "/v1/transfers", {"from_account_id": source, "to_account_id": target}
        else:
            amount = _amount(rng, declined=rng.random() < 0.25)
            merchant = rng.choice(self._merchants)
            kind, path, body = "authorize", "/v1/payments", {"customer_account_id": source, "merchant_account_id": merchant}
        if not self._reserve(source, amount):
            return None
        return kind, path, {**body, "amount": str(amount)}

    def _run(self, index: int) -> None:
        rng = random.Random(f"{SEED}-{index}")
        while not self.stop.is_set():
            planned = self._request(rng)
            if planned is None:
                self.stop.set()
                return
            kind, path, body = planned
            try:
                status, response = self._api.try_post(path, body)
            except Exception as ex:  # noqa: BLE001 - recorded and asserted on
                status, response = -1, repr(ex)
            with self._lock:
                self.statuses[(kind, status)] += 1
                if status != 201:
                    self.failures.append((kind, status, str(response)[:300]))
                elif kind == "authorize":
                    self.payments.append(uuid.UUID(response["payment_id"]))


@dataclass(frozen=True)
class Run:
    completed: dict
    drifts: list  # the drift_detected lines emitted just before it


class LogTail:
    """The worker's complete JSON lines, read incrementally from where the last read stopped, with
    each reconciliation_completed paired with the drift_detected lines just before it (one reconciler
    task emits them; drift_detected carries no run_id)."""

    def __init__(self, path) -> None:
        self._path = path
        self._offset = 0
        self._pending: list = []
        self.retries: list[dict] = []

    def new_runs(self) -> list[Run]:
        with open(self._path, "rb") as f:
            f.seek(self._offset)
            data = f.read()
        complete = data[: data.rfind(b"\n") + 1]
        self._offset += len(complete)
        runs = []
        for raw in complete.decode("utf-8").splitlines():
            if not raw.strip():
                continue
            line = json.loads(raw)
            if line["event"] == "drift_detected":
                self._pending.append(line)
            elif line["event"] == "reconciliation_completed":
                runs.append(Run(line, self._pending))
                self._pending = []
            elif line["event"] == "reconciliation_failed":
                self._pending = []
            elif line["event"] == "retry_scheduled":
                self.retries.append(line)
        return runs


def _clean(run_: Run) -> bool:
    c = run_.completed
    return (c["drifted_accounts"], c["global_sum"], c["escrow_drift"], run_.drifts) == (0, "0.0000", "0.0000", [])


def _breakdown(runs: list[Run]) -> str:
    drifted = [r for r in runs if r.completed["drifted_accounts"] != 0]
    global_off = [r for r in runs if r.completed["global_sum"] != "0.0000"]
    escrow_off = [r for r in runs if r.completed["escrow_drift"] != "0.0000"]
    lines = [d for r in runs for d in r.drifts]
    first = next((r for r in runs if not _clean(r)), None)
    return (
        f"of {len(runs)} runs: {sum(1 for r in runs if not _clean(r))} not clean ({len(drifted)} with drifted "
        f"accounts, {len(global_off)} with global_sum != 0, {len(escrow_off)} with escrow drift); "
        f"{len(lines)} drift_detected lines over {len({d['account_id'] for d in lines})} accounts; "
        f"first: {(first.completed, first.drifts[:3]) if first else None}"
    )


@dataclass
class Loaded:
    worker: object = None
    load: Load | None = None
    t_start: datetime | None = None
    load_runs: list[Run] = field(default_factory=list)  # the runs counted towards RECONCILE_RUNS
    load_breakdown: str = ""  # the load-window runs' clean check, recorded at load stop
    in_flight: dict[int, int] = field(default_factory=dict)  # run_id -> transactions in flight
    backlog: int | None = None  # the load's payments still AUTHORIZED at load stop
    drain_timeout_s: float | None = None
    drained: bool = False
    drain_s: float | None = None
    drain_last: tuple | None = None
    t_drained: datetime | None = None
    post_drain: dict | None = None
    max_transaction_id: int | None = None
    lines: list[dict] = field(default_factory=list)  # the worker's whole log, after it stopped
    exit_code: int | None = None

    def events(self, name: str) -> list[dict]:
        return [line for line in self.lines if line["event"] == name]


def _seed(api) -> tuple[list[int], list[int], list[int]]:
    with ThreadPoolExecutor(SEED_THREADS) as pool:
        customers = list(pool.map(lambda _: api.create_account("CUSTOMER"), range(CUSTOMERS)))
        merchants = list(pool.map(lambda _: api.create_account("MERCHANT"), range(MERCHANTS)))
        working = customers[:WORKING]
        list(pool.map(lambda customer: api.deposit(customer, str(FUND)), working))
    return customers, merchants, working


_AUTHORIZED_SQL = "SELECT count(*) FROM payments WHERE id = ANY($1::uuid[]) AND status = 'AUTHORIZED'"


async def _drain(rs, group: str, payment_ids: list[uuid.UUID], w, timeout: float) -> tuple[bool, float, tuple]:
    """Polls until, twice in a row: none of the load's payments is AUTHORIZED, no outbox row is
    unpublished, and the group's lag is 0 on every partition. Returns (drained, seconds, last poll);
    never raises on a timeout or a worker exit, so the checks recorded at load stop stand."""
    conn = await rs.app_db.connect()
    started = time.monotonic()
    try:
        consecutive, last = 0, None
        while time.monotonic() - started < timeout:
            if w.process.poll() is not None:
                return False, time.monotonic() - started, ("worker exited", w.process.returncode, last)
            authorized = await conn.fetchval(_AUTHORIZED_SQL, payment_ids)
            unpublished = await conn.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
            lag = await group_lag(rs.kafka.bootstrap_servers, group)
            last = (authorized, unpublished, lag)
            quiet = authorized == 0 and unpublished == 0 and set(lag) == {0, 1, 2} and all(v == 0 for v in lag.values())
            consecutive = consecutive + 1 if quiet else 0
            if consecutive == 2:
                return True, time.monotonic() - started, last
            await asyncio.sleep(0.5)
        return False, time.monotonic() - started, last
    finally:
        await conn.close()


# Per run: the transactions that started before the run's transaction (created_at is the writer's
# now(), which no role can set, V8) and are newer than every entry the run saw (entries commit with
# their transaction, invariant 1, so such a transaction was not in the snapshot). Each one was in
# flight at the snapshot: D-019's seam. A lower bound: ids come from a sequence, not in commit order,
# so a lower id that committed after the snapshot isn't counted.
IN_FLIGHT_SQL = """
SELECT r.run_id,
       (SELECT count(*) FROM transactions t WHERE t.id > r.max_tx AND t.created_at < r.as_of) AS in_flight
FROM unnest($1::int[], $2::bigint[], $3::timestamptz[]) AS r(run_id, max_tx, as_of)
"""


def _summary(state: Loaded, retries_55p03: int) -> str:
    durations = [r.completed["duration_ms"] for r in state.load_runs]
    counts = list(state.in_flight.values())
    return (
        f"RECONCILE_SNAPSHOT runs={len(state.load_runs)} in_flight_runs={sum(1 for n in counts if n >= 1)} "
        f"in_flight_total={sum(counts)} max_in_flight={max(counts) if counts else 'na'} "
        f"duration_ms_p50={_nearest_rank(durations, 50)} duration_ms_max={max(durations) if durations else 'na'} "
        f"requests={state.load.requests} retries_55P03={retries_55p03} "
        f"drifted_runs={sum(1 for r in state.load_runs if not _clean(r))} "
        f"drift_lines={sum(len(r.drifts) for r in state.load_runs)} seed={SEED} {hardware.summary_fields()}"
    )


def _write(request, line: str) -> None:
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line)
    else:
        print(line)


@pytest.fixture(scope="module")
def loaded(relay_rs, request):
    rs = relay_rs
    state = Loaded()
    customers, merchants, working = _seed(rs.api)
    group = fresh_group(rs, "reconciliation-snapshot")
    w = state.worker = worker(rs, group, RECONCILE_INTERVAL_MS=200)
    try:
        w.wait_for("partitions_assigned")
        w.wait_for("reconciliation_completed")
        tail = LogTail(w.stdout_path)

        # The load, until RECONCILE_RUNS runs have as_of later than T_start.
        state.t_start = _clock(rs)
        state.load = Load(rs.api, working, customers + merchants, merchants)
        state.load.start()
        deadline = time.monotonic() + LOAD_TIMEOUT_S
        while len(state.load_runs) < RECONCILE_RUNS:
            assert time.monotonic() < deadline, f"{len(state.load_runs)} runs within {LOAD_TIMEOUT_S} s"
            assert w.process.poll() is None, w.diagnostics()
            assert state.load.exhausted is None, state.load.exhausted
            assert state.load.alive(), "the load threads stopped on their own"
            for r in tail.new_runs():
                if _time(r.completed["as_of"]) > state.t_start and len(state.load_runs) < RECONCILE_RUNS:
                    state.load_runs.append(r)
            time.sleep(0.05)
        state.load.join()

        # At load stop, before any drain: the clean check, the tripwire (as ledger_api_app) and the
        # summary line, recorded and printed whatever the drain does next.
        state.load_breakdown = _breakdown(state.load_runs)
        rows = _app(
            rs,
            IN_FLIGHT_SQL,
            [r.completed["run_id"] for r in state.load_runs],
            [r.completed["max_transaction_id"] for r in state.load_runs],
            [_time(r.completed["as_of"]) for r in state.load_runs],
        )
        state.in_flight = {row["run_id"]: row["in_flight"] for row in rows}
        tail.new_runs()
        _write(request, _summary(state, sum(1 for e in tail.retries if e.get("sqlstate") == "55P03")))

        # The drain, then one more run.
        state.backlog = _app(rs, _AUTHORIZED_SQL, state.load.payments)[0][0]
        state.drain_timeout_s = drain_timeout_s(state.backlog)
        state.drained, state.drain_s, state.drain_last = run(
            _drain(rs, group, state.load.payments, w, state.drain_timeout_s)
        )
        _write(
            request,
            f"RECONCILE_SNAPSHOT_DRAIN backlog_at_stop={state.backlog} timeout_s={state.drain_timeout_s:.0f} "
            f"drained={state.drained} drain_s={state.drain_s:.1f} last={state.drain_last}",
        )
        if state.drained:
            state.t_drained = _clock(rs)
            deadline = time.monotonic() + RUN_TIMEOUT_S
            while state.post_drain is None and time.monotonic() < deadline and w.process.poll() is None:
                for r in tail.new_runs():
                    if state.post_drain is None and _time(r.completed["as_of"]) > state.t_drained:
                        state.post_drain = r.completed
                time.sleep(0.05)
            state.max_transaction_id = _app(rs, "SELECT max(id) AS id FROM transactions")[0]["id"]

        state.exit_code = w.stop()
        state.lines = w.lines()
        yield state
    finally:
        w.__exit__(None, None, None)
        if state.load is not None and state.load.alive():
            state.load.join()


def _all_runs(lines: list[dict]) -> list[Run]:
    runs, pending = [], []
    for line in lines:
        if line["event"] == "drift_detected":
            pending.append(line)
        elif line["event"] == "reconciliation_completed":
            runs.append(Run(line, pending))
            pending = []
        elif line["event"] == "reconciliation_failed":
            pending = []
    return runs


def test_every_run_reports_no_drift_under_load(loaded):
    """Every run in the log (before, during and after the load) is clean; the message also gives the
    load-window breakdown recorded at load stop."""
    runs = _all_runs(loaded.lines)
    assert len(loaded.load_runs) == RECONCILE_RUNS, len(loaded.load_runs)
    assert all(_clean(r) for r in runs), f"whole log {_breakdown(runs)}; load window {loaded.load_breakdown}"
    assert loaded.events("drift_detected") == [], loaded.events("drift_detected")[:3]
    assert loaded.events("reconciliation_failed") == [], loaded.events("reconciliation_failed")[:3]


def test_the_load_was_accepted_and_the_worker_settled_without_failures(loaded):
    load = loaded.load
    assert load.exhausted is None, load.exhausted
    assert load.failures == [], (dict(load.statuses), load.failures[:5])
    assert {status for _, status in load.statuses} == {201}, dict(load.statuses)
    assert loaded.exit_code == 0, loaded.worker.diagnostics()
    assert loaded.events("processing_failed") == [], loaded.worker.diagnostics()
    assert loaded.events("message_dead_lettered") == [], loaded.worker.diagnostics()
    # Lock waits under load may time out and retry in place (D-108); reported, never asserted zero.
    other_retries = [e for e in loaded.events("retry_scheduled") if e.get("sqlstate") != "55P03"]
    assert other_retries == [], other_retries[:3]
    assert loaded.events("db_reconnected") == [], loaded.worker.diagnostics()  # tripwire (D-109)
    assert loaded.events("offset_commit_failed") == [], loaded.worker.diagnostics()  # tripwire (D-109)


def test_writes_were_in_flight_at_the_snapshots(loaded):
    in_flight_runs = sum(1 for n in loaded.in_flight.values() if n >= 1)
    assert in_flight_runs >= RECONCILE_INFLIGHT_FLOOR, (
        f"in_flight_runs={in_flight_runs} < floor {RECONCILE_INFLIGHT_FLOOR} over {len(loaded.in_flight)} runs: "
        f"the load never reached a snapshot; {loaded.in_flight}"
    )


def test_the_worker_drained_within_its_timeout(loaded):
    assert loaded.drained, (
        f"not drained within {loaded.drain_timeout_s:.0f} s (backlog at stop {loaded.backlog}): "
        f"(authorized, unpublished, lag) = {loaded.drain_last}"
    )


def test_the_run_after_the_drain_saw_every_transaction(loaded):
    post = loaded.post_drain
    assert post is not None, f"no run after the drain (drained={loaded.drained}, t_drained={loaded.t_drained})"
    assert _time(post["as_of"]) > loaded.t_drained, post
    assert (post["drifted_accounts"], post["global_sum"], post["escrow_drift"]) == (0, "0.0000", "0.0000"), post
    assert post["max_transaction_id"] == loaded.max_transaction_id, (post, loaded.max_transaction_id)
