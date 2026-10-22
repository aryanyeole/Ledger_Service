"""Retry in place (D-107, D-108, D-125) on real worker processes, on the failure_modes stack.

(a) 55P03: the test holds ESCROW FOR UPDATE as ledger_api_app (a granted lock; no trigger, schema
    or role touched) and the worker's settle times out on it: retried with the exact backoff, then
    applied exactly once when the holder lets go; the counter on /metrics matches.
(b) Unclassified (RAISE_BEFORE_COMMIT on P): exactly WORKER_MAX_ATTEMPTS attempts on the exact
    schedule, each rolled back (no dedup row, no posting, P AUTHORIZED); then exit 1, P's offset
    uncommitted; a worker without the fault settles P once.
(c) Other partitions keep moving: Q, published on another partition after P's first retry, settles
    while P is still retrying.
(d) Head of line holds: R (and any other payment behind P on P's partition) stays AUTHORIZED
    through every attempt; the follow-up worker settles P, then R.
(e) A stop during a backoff exits 0 at once, P uncommitted; the next worker in the group settles it.
(b)-(d) are one scenario: they share P's fault run and the follow-up worker.

Timing is read from the worker's own log timestamps (structlog, UTC ISO with microseconds): each
attempt after the first starts no earlier than its retry_scheduled timestamp + delay_ms, less a
5 ms allowance for the wall clock against the monotonic clock the worker schedules with.
"""

import asyncio
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from support.stack import TOPIC
from support.worker_process import committed_offsets, start_worker

CLOCK_ALLOWANCE = timedelta(milliseconds=5)


def run(coro):
    return asyncio.run(coro)


async def _fetch(db, query, *args):
    conn = await db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def _app(fm, query, *args):
    return run(_fetch(fm.app_db, query, *args))


def _worker_role(fm, query, *args):
    """processed_messages is readable only by settlement_worker (V12 grants)."""
    return run(_fetch(fm.worker_db, query, *args))


def _status(fm, payment_id) -> str:
    return _app(fm, "SELECT status FROM payments WHERE id = $1", payment_id)[0]["status"]


def _postings(fm, payment_id) -> int:
    rows = _app(
        fm,
        "SELECT count(*) AS n FROM transactions WHERE reference = $1 AND kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE')",
        str(payment_id),
    )
    return rows[0]["n"]


def _results(fm, payment_id) -> int:
    rows = _app(
        fm,
        "SELECT count(*) AS n FROM outbox WHERE message_key = $1 AND event_type IN ('payment.settled', 'payment.failed')",
        str(payment_id),
    )
    return rows[0]["n"]


def _event_id(fm, payment_id) -> uuid.UUID:
    rows = _app(
        fm, "SELECT event_id FROM outbox WHERE message_key = $1 AND event_type = 'payment.authorized'", str(payment_id)
    )
    return rows[0]["event_id"]


def _dedup_rows(fm, group, payment_id) -> int:
    rows = _worker_role(
        fm,
        "SELECT count(*) AS n FROM processed_messages WHERE consumer_group = $1 AND event_id = $2",
        group,
        _event_id(fm, payment_id),
    )
    return rows[0]["n"]




def _committed(fm, group, partition) -> int | None:
    return run(committed_offsets(fm.kafka.bootstrap_servers, group)).get(partition)


def _ts(event) -> datetime:
    return datetime.fromisoformat(event["timestamp"])


def _funded_pair(fm) -> tuple[int, int]:
    customer = fm.api.create_account("CUSTOMER")
    merchant = fm.api.create_account("MERCHANT")
    fm.api.deposit(customer, "10000.00")
    return customer, merchant


def _worker(fm, group, **env):
    return start_worker(
        database=fm.database,
        bootstrap_servers=fm.kafka.bootstrap_servers,
        group=group,
        log_dir=fm.log_dir,
        extra_env={k: str(v) for k, v in env.items()},
    )


def _retries_metric(port: int, error_class: str, sqlstate: str) -> Decimal:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as response:
        body = response.read().decode("utf-8")
    prefix = f'settlement_retries_total{{error_class="{error_class}",sqlstate="{sqlstate}"}} '
    lines = [line for line in body.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, body
    return Decimal(lines[0][len(prefix) :])


class EscrowHolder:
    """Holds system:escrow FOR UPDATE as ledger_api_app on its own thread and event loop until
    released (a granted row lock: ledger_api_app has UPDATE(balance) on accounts, D-034)."""

    def __init__(self, fm) -> None:
        self._fm = fm
        self._locked = threading.Event()
        self._release = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def hold():
            conn = await self._fm.app_db.connect()
            try:
                tx = conn.transaction()
                await tx.start()
                await conn.fetch("SELECT id FROM accounts WHERE name = 'system:escrow' FOR UPDATE")
                self._locked.set()
                await asyncio.to_thread(self._release.wait)
                await tx.rollback()
            finally:
                await conn.close()

        try:
            asyncio.run(hold())
        except BaseException as ex:  # noqa: BLE001 - surfaced by release()
            self._error = ex
            self._locked.set()

    def __enter__(self) -> "EscrowHolder":
        self._thread.start()
        assert self._locked.wait(30), "the escrow lock was not taken"
        assert self._error is None, self._error
        return self

    def release(self) -> None:
        self._release.set()
        self._thread.join(30)
        assert self._error is None, self._error

    def __exit__(self, *exc) -> None:
        self.release()


def _wait(worker, predicate, timeout: float, what: str):
    """Polls predicate until it holds; fails at once if the worker exits first."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        if worker.process.poll() is not None:
            raise AssertionError(
                f"worker exited {worker.process.returncode} waiting for {what}:\n{worker.diagnostics()}"
            )
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}:\n{worker.diagnostics()}")


# (a)
def test_lock_timeout_retries_in_place_then_applies_once(fm):
    group = "retry-55p03"
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with EscrowHolder(fm) as holder:
        with _worker(fm, group, WORKER_LOCK_TIMEOUT_MS=300) as worker:
            mine = {"partition": partition, "offset": offset}
            _wait(worker, lambda: len(worker.events("retry_scheduled", **mine)) >= 2, 60, "two retries of P")
            holder.release()
            worker.wait_for("payment_settled", payment_id=str(payment))
            retries = worker.events("retry_scheduled")
            port = worker.metrics_port()
            counted = _retries_metric(port, "Transient", "55P03")
            assert worker.stop() == 0, worker.diagnostics()

    assert retries, worker.diagnostics()
    assert all(r["partition"] == partition and r["offset"] == offset for r in retries), retries
    assert all(r["error_class"] == "Transient" and r["sqlstate"] == "55P03" for r in retries), retries
    assert [r["attempt"] for r in retries] == list(range(1, len(retries) + 1))
    assert [r["delay_ms"] for r in retries[:2]] == [50, 100]
    assert all(r["level"] == "warning" for r in retries)
    assert counted == len(retries)

    calls = worker.events("processor_called", payment_id=str(payment))
    assert len(calls) == len(retries) + 1, "one processor call per attempt"
    assert len({c["decision"] for c in calls}) == 1
    assert _postings(fm, payment) == 1
    assert _dedup_rows(fm, group, payment) == 1
    assert _results(fm, payment) == 1
    assert _status(fm, payment) == "SETTLED"
    assert (_committed(fm, group, partition) or 0) > offset
    assert worker.events("processing_failed") == []


def _publish_until(fm, group, customer, merchant, predicate, limit: int = 12):
    """Authorizes and publishes candidates one at a time, reading each one's partition back from
    the topic (D-105), until predicate((partition, offset)) holds. Returns (chosen, others), where
    others maps each other candidate to its position."""
    others = {}
    for _ in range(limit):
        candidate = fm.api.authorize(customer, merchant, "10.00")
        fm.publish(candidate)
        position = fm.position(candidate)
        if predicate(position):
            return candidate, others
        others[candidate] = position
    raise AssertionError(f"no candidate matched within {limit} publishes: {others}")


# (b), (c), (d)
def test_unclassified_retries_in_place_without_blocking_other_partitions(fm):
    group = "retry-unclassified"
    backoff = {"WORKER_RETRY_BASE_MS": 500, "WORKER_RETRY_MAX_MS": 2000}
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    fm.publish(payment)
    p_partition, p_offset = fm.position(payment)
    # R: behind P on P's partition, published before the worker starts (same fetch as P).
    r, _ = _publish_until(fm, group, customer, merchant, lambda pos: pos[0] == p_partition and pos[1] > p_offset)
    witnesses = [r]
    q = None
    checks = []

    with _worker(fm, group, FAULT_POINT="RAISE_BEFORE_COMMIT", FAULT_PAYMENT_ID=payment, **backoff) as worker:
        mine = {"partition": p_partition, "offset": p_offset}
        checked = 0
        while worker.process.poll() is None:
            retries = worker.events("retry_scheduled", **mine)
            for _ in retries[checked:]:
                checks.append(
                    (_dedup_rows(fm, group, payment), _postings(fm, payment), _status(fm, payment))
                    + tuple(_status(fm, w) for w in witnesses)
                )
            checked = len(retries)
            if retries and q is None:
                # (c): published after P's first retry, so it can only settle if P's backoff lets
                # the loop keep fetching other partitions.
                q, behind = _publish_until(fm, group, customer, merchant, lambda pos: pos[0] != p_partition)
                witnesses += [w for w, pos in behind.items() if pos[0] == p_partition]
            time.sleep(0.05)
        assert worker.wait_exit(30) == 1, worker.diagnostics()

    # (b)
    retries = worker.events("retry_scheduled", **mine)
    assert [r["attempt"] for r in retries] == [1, 2, 3, 4], worker.diagnostics()
    assert [r["delay_ms"] for r in retries] == [500, 1000, 2000, 2000]
    assert all(r["error_class"] == "Unclassified" and r["sqlstate"] is None for r in retries), retries
    assert all("RAISE_BEFORE_COMMIT" in r["error"] for r in retries)
    assert len(worker.events("fault_injected", point="RAISE_BEFORE_COMMIT", payment_id=str(payment))) == 5
    calls = worker.events("processor_called", payment_id=str(payment))
    assert len(calls) == 5 and len({c["decision"] for c in calls}) == 1
    for retry, next_call in zip(retries, calls[1:]):
        assert _ts(next_call) >= _ts(retry) + timedelta(milliseconds=retry["delay_ms"]) - CLOCK_ALLOWANCE, (
            retry,
            next_call,
        )
    assert len(checks) == 4, checks
    assert all(c[:3] == (0, 0, "AUTHORIZED") for c in checks), checks
    failed = worker.events("processing_failed")
    assert [(f["partition"], f["offset"]) for f in failed] == [(p_partition, p_offset)]
    assert _dedup_rows(fm, group, payment) == 0 and _postings(fm, payment) == 0
    assert _status(fm, payment) == "AUTHORIZED"
    assert (_committed(fm, group, p_partition) or 0) <= p_offset

    # (c)
    assert q is not None
    q_settled = worker.events("payment_settled", payment_id=str(q))
    assert len(q_settled) == 1, worker.diagnostics()
    assert _ts(q_settled[0]) < _ts(retries[-1]), (q_settled, retries[-1])

    # (d): in every check and after the exit.
    assert all(c[3:] == tuple("AUTHORIZED" for _ in c[3:]) for c in checks), checks
    for w in witnesses:
        assert _status(fm, w) == "AUTHORIZED", w
        assert worker.events("payment_settled", payment_id=str(w)) == []

    with _worker(fm, group) as recovery:
        for pid in [payment, *witnesses]:
            recovery.wait_for("payment_settled", payment_id=str(pid))
        assert recovery.stop() == 0, recovery.diagnostics()
    settled = [e["payment_id"] for e in recovery.events("payment_settled")]
    assert settled.index(str(payment)) < settled.index(str(r)), settled
    assert recovery.events("retry_scheduled") == []
    for pid in [payment, *witnesses]:
        assert _postings(fm, pid) == 1, pid
        assert _dedup_rows(fm, group, pid) == 1, pid
        assert _status(fm, pid) == "SETTLED", pid


# (e)
def test_stop_during_a_backoff_exits_0_uncommitted(fm):
    group = "retry-stop-in-backoff"
    backoff = {"WORKER_RETRY_BASE_MS": 2000, "WORKER_RETRY_MAX_MS": 2000, "WORKER_LOCK_TIMEOUT_MS": 300}
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with EscrowHolder(fm):
        with _worker(fm, group, **backoff) as worker:
            first = worker.wait_for("retry_scheduled", partition=partition, offset=offset)
            assert first["delay_ms"] == 2000
            stopped_at = time.monotonic()
            code = worker.stop()
            stop_ms = (time.monotonic() - stopped_at) * 1000
        assert code == 0, worker.diagnostics()
        print(f"STOP_IN_BACKOFF stop_ms={stop_ms:.0f} backoff_ms=2000")
        assert stop_ms < 2000, stop_ms
        assert len(worker.events("retry_scheduled")) == 1
        assert _dedup_rows(fm, group, payment) == 0 and _postings(fm, payment) == 0
        assert _status(fm, payment) == "AUTHORIZED"
        assert (_committed(fm, group, partition) or 0) <= offset

    with _worker(fm, group) as again:
        again.wait_for("payment_settled", payment_id=str(payment))
        assert again.stop() == 0, again.diagnostics()
    assert again.events("retry_scheduled") == []
    assert _postings(fm, payment) == 1 and _dedup_rows(fm, group, payment) == 1
    assert _status(fm, payment) == "SETTLED"
