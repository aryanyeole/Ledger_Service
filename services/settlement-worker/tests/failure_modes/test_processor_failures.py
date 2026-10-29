"""Processor failures retry to one effect (D-107, D-108, D-112) on real worker processes, on the
failure_modes stack. Every case starts its own consumer group at the topic's end offsets (fresh_group,
D-123) and publishes its payments from their outbox rows (D-103). Backoffs are the harness's: no
jitter, 50, 100, 200, 400, 400 ... ms. The fake processor fails by amount (processor.py): cents 91
is unavailable on a record's first two runs, cents 92 answers too late on them; this file is the
only one allowed those amounts (tests/unit/test_reserved_amounts.py).

(1) Unavailable: two Transient retries, no processor_called until the third run, then one effect.
(2) Timeout is not a decline: the processor decides on every run, the deadline expires on the first
    two, and the payment settles with that decision; no release, never FAILED. A row-lock probe
    shows no lock on P's row while the worker waits for the processor (invariant 5).
(3) Outage: nothing is called, written, committed or dead-lettered however long it lasts; the
    backoff doubles to the cap and stays there. A normal worker then settles everything once.
(4) A restart resets the attempt count (characterization of D-108's in-memory count).
(5) Transient failures don't count toward Unclassified exhaustion (D-107, D-112). The one case that
    expects a dead letter.
Every case asserts processing_failed == []; (1)-(4) also assert message_dead_lettered == [].
"""

import time

from support.failure_kit import RowLockProbe, fresh_group, run
from support.failure_kit import app as _app
from support.failure_kit import assert_not_past as _assert_not_past
from support.failure_kit import committed as _committed
from support.failure_kit import dead_letters_of as _dead_letters_of
from support.failure_kit import dedup_rows as _dedup_rows
from support.failure_kit import event_id as _event_id
from support.failure_kit import funded_pair as _funded_pair
from support.failure_kit import postings as _postings
from support.failure_kit import results as _results
from support.failure_kit import status as _status
from support.failure_kit import ts as _ts
from support.failure_kit import wait as _wait
from support.failure_kit import worker as _worker
from support.stack import TOPIC, topic_partitions

UNAVAILABLE = "10.91"  # cents 91: ProcessorUnavailable on runs 1-2
SLOW = "10.92"  # cents 92: answers after 2 x WORKER_PROCESSOR_TIMEOUT_MS on runs 1-2


def _schedule(attempts, base_ms: int = 50, max_ms: int = 400) -> list[int]:
    """The harness's backoff (D-123): base, doubling, capped; no jitter."""
    return [min(base_ms * 2 ** (a - 1), max_ms) for a in attempts]


def _posting_kinds(fm, payment_id) -> list[str]:
    rows = _app(
        fm,
        "SELECT kind FROM transactions WHERE reference = $1 AND kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE') "
        "ORDER BY id",
        str(payment_id),
    )
    return [r["kind"] for r in rows]


def _settled_once(fm, group, payment_id, partition: int, offset: int) -> None:
    assert _posting_kinds(fm, payment_id) == ["PAYMENT_SETTLE"], payment_id
    assert _dedup_rows(fm, group, payment_id) == 1, payment_id
    assert _results(fm, payment_id) == 1, payment_id
    assert _status(fm, payment_id) == "SETTLED", payment_id
    assert (_committed(fm, group, partition) or 0) > offset, payment_id


def _no_failures_or_dead_letters(worker) -> None:
    assert worker.events("processing_failed") == [], worker.diagnostics()
    assert worker.events("message_dead_lettered") == [], worker.diagnostics()


# (1)
def test_an_unavailable_processor_is_retried_then_called_once(fm):
    group = fresh_group(fm, "processor-unavailable")
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, UNAVAILABLE)
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with _worker(fm, group) as worker:
        worker.wait_for("payment_settled", payment_id=str(payment))
        assert worker.stop() == 0, worker.diagnostics()

    retries = worker.events("retry_scheduled", partition=partition, offset=offset)
    assert [r["attempt"] for r in retries] == [1, 2], worker.diagnostics()
    assert [r["delay_ms"] for r in retries] == [50, 100]
    assert all(r["error_class"] == "Transient" and r["sqlstate"] is None for r in retries), retries
    assert all(r["error"].startswith("ProcessorUnavailable: ") for r in retries), [r["error"] for r in retries]
    assert worker.events("processor_timeout") == []
    # The processor was never reached on runs 1-2, so it logged nothing for them.
    calls = worker.events("processor_called", payment_id=str(payment))
    assert [(c["attempt"], c["decision"]) for c in calls] == [(3, "APPROVED")], calls
    _settled_once(fm, group, payment, partition, offset)
    _no_failures_or_dead_letters(worker)


def _at_most_one_locked_in_a_row(samples) -> None:
    """A processor wait inside the settle transaction would hold P's row for the whole timed-out
    wait (400 ms, about four probes in a row). Two in a row already fails."""
    longest, current = 0, 0
    for _, locked in samples:
        current = current + 1 if locked else 0
        longest = max(longest, current)
    assert longest < 2, [s for s in samples if s[1]]


# (2)
def test_a_processor_timeout_is_retried_never_a_decline(fm):
    group = fresh_group(fm, "processor-timeout")
    # 400 ms, not less: each timed-out wait must hold at least two probes (one every 100 ms).
    timeout_ms = 400
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, SLOW)
    fm.publish(payment)
    partition, offset = fm.position(payment)
    # The worker logs UTC wall-clock time and the probe records time.monotonic(); this converts one
    # to the other (the same host clock, read once).
    wall_minus_monotonic = time.time() - time.monotonic()

    # The probe watches P's row. A processor call inside the transaction but before any row lock
    # holds no row lock, so it couldn't see that one; mutation 4 (T10) calls the processor after
    # P's row is locked, which is where a check-then-decide settle would.
    with RowLockProbe(fm, payment) as probe:
        with _worker(fm, group, WORKER_PROCESSOR_TIMEOUT_MS=timeout_ms) as worker:
            mine = {"payment_id": str(payment)}
            _wait(
                worker,
                lambda: worker.events("payment_settled", **mine) or worker.events("payment_released", **mine),
                60,
                "P's outcome",
            )
            assert worker.stop() == 0, worker.diagnostics()
    samples = probe.samples

    # The outcome first: a timeout may hide a decision already made, so it is never a decline.
    assert _status(fm, payment) == "SETTLED", worker.diagnostics()
    assert worker.events("payment_released") == []
    _settled_once(fm, group, payment, partition, offset)

    timeouts = worker.events("processor_timeout", **mine)
    assert [t["attempt"] for t in timeouts] == [1, 2], worker.diagnostics()
    assert all(t["timeout_ms"] == timeout_ms for t in timeouts), timeouts
    calls = worker.events("processor_called", **mine)
    assert [c["attempt"] for c in calls] == [1, 2, 3], calls
    assert {c["decision"] for c in calls} == {"APPROVED"}, calls
    retries = worker.events("retry_scheduled", partition=partition, offset=offset)
    assert [r["attempt"] for r in retries] == [1, 2], retries
    assert [r["delay_ms"] for r in retries] == [50, 100]
    assert all(r["error_class"] == "Transient" and r["sqlstate"] is None for r in retries), retries
    assert all(r["error"].startswith("ProcessorTimeout: ") for r in retries), [r["error"] for r in retries]
    _no_failures_or_dead_letters(worker)

    # Invariant 5: no lock on P's row while the worker waits for the processor.
    windows = [(_ts(call).timestamp(), _ts(timeout).timestamp()) for call, timeout in zip(calls, timeouts)]
    for start, end in windows:
        inside = [s for s in samples if start <= s[0] + wall_minus_monotonic <= end]
        assert len(inside) >= 2, (start, end, samples)
    locked = sum(1 for _, held in samples if held)
    print(f"PROCESSOR_TIMEOUT_PROBE samples={len(samples)} locked={locked} timeout_ms={timeout_ms}")
    _at_most_one_locked_in_a_row(samples)


def _one_per_partition(fm, customer, merchant, limit: int = 15) -> dict:
    """Authorizes and publishes payments of 10.00 one at a time until every partition has one, reading
    each one's position back from the topic (D-105). Returns {payment_id: (partition, offset)}."""
    partitions = set(run(topic_partitions(fm.kafka.bootstrap_servers, TOPIC)) or [])
    assert partitions, f"{TOPIC} has no partitions"
    published = {}
    for _ in range(limit):
        payment = fm.api.authorize(customer, merchant, "10.00")
        fm.publish(payment)
        published[payment] = fm.position(payment)
        if {p for p, _ in published.values()} == partitions:
            return published
    raise AssertionError(f"not every partition of {sorted(partitions)} got a payment in {limit}: {published}")


# (3)
def test_an_outage_holds_every_partition_without_dead_letters_then_settles_once(fm):
    group = fresh_group(fm, "processor-outage")
    max_attempts = 5
    customer, merchant = _funded_pair(fm)
    published = _one_per_partition(fm, customer, merchant)
    heads = {}  # partition -> the offset of its first payment, which every run retries
    for partition, offset in published.values():
        heads[partition] = min(offset, heads.get(partition, offset))

    with _worker(fm, group, FAKE_PROCESSOR_OUTAGE=1, WORKER_MAX_ATTEMPTS=max_attempts) as worker:
        started = worker.wait_for("worker_started")

        def past_the_budget():
            # Ends early on any dead letter, so a mutation fails on the assertion below, not on a wait.
            if worker.events("message_dead_lettered"):
                return True
            return [
                p for p, o in heads.items()
                if any(r["attempt"] > max_attempts + 1 for r in worker.events("retry_scheduled", partition=p, offset=o))
            ]

        reached = _wait(worker, past_the_budget, 60, f"a run count past WORKER_MAX_ATTEMPTS ({max_attempts})")
        assert worker.events("message_dead_lettered") == [], worker.diagnostics()
        assert worker.stop() == 0, worker.diagnostics()

    assert (started["processor_outage"], started["level"]) == (True, "warning"), started
    assert worker.events("processor_called") == [], worker.diagnostics()
    _no_failures_or_dead_letters(worker)
    for payment, (partition, offset) in published.items():
        assert _dedup_rows(fm, group, payment) == 0, payment
        assert _postings(fm, payment) == 0, payment
        assert _status(fm, payment) == "AUTHORIZED", payment
        assert _dead_letters_of(fm, partition, offset) == [], payment
        _assert_not_past(_committed(fm, group, partition), offset, f"after the outage, {payment}")
    # The backoff doubles to the cap and stays there, however many runs (D-108).
    partition = reached[0]
    retries = worker.events("retry_scheduled", partition=partition, offset=heads[partition])
    assert len(retries) >= max_attempts + 2, retries
    assert [r["attempt"] for r in retries] == list(range(1, len(retries) + 1)), retries
    assert [r["delay_ms"] for r in retries] == _schedule(range(1, len(retries) + 1)), retries
    assert all(r["error_class"] == "Transient" and r["error"].startswith("ProcessorUnavailable: ") for r in retries), [
        (r["error_class"], r["error"]) for r in retries
    ]

    # The outage is over: a normal worker in the same group, with every attempt count back at 0.
    with _worker(fm, group) as after:
        for payment in published:
            after.wait_for("payment_settled", payment_id=str(payment))
        assert after.stop() == 0, after.diagnostics()

    assert after.wait_for("worker_started")["processor_outage"] is False
    assert after.events("retry_scheduled") == [], after.diagnostics()
    for payment, (partition, offset) in published.items():
        calls = after.events("processor_called", payment_id=str(payment))
        assert [c["attempt"] for c in calls] == [1], (payment, calls)
        _settled_once(fm, group, payment, partition, offset)
    _no_failures_or_dead_letters(after)


# (4)
def test_a_restart_resets_the_attempt_count(fm):
    # Characterization, not a requirement: D-108's attempt count is in memory, so a new worker starts
    # P's count again at 0 and the fake processor (which fails by run number) is unavailable for two
    # more runs. P fails three times in all before its single processor call.
    group = fresh_group(fm, "processor-restart")
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, UNAVAILABLE)
    fm.publish(payment)
    partition, offset = fm.position(payment)
    mine = {"partition": partition, "offset": offset}

    # A 5 s backoff: the first worker is still waiting when it is stopped, so it has exactly one retry.
    with _worker(fm, group, WORKER_RETRY_BASE_MS=5000, WORKER_RETRY_MAX_MS=5000) as first:
        first.wait_for("retry_scheduled", **mine)
        assert first.stop() == 0, first.diagnostics()

    retries = first.events("retry_scheduled", **mine)
    assert [(r["attempt"], r["error"].split(":")[0]) for r in retries] == [(1, "ProcessorUnavailable")], retries
    assert first.events("processor_called") == []
    assert _status(fm, payment) == "AUTHORIZED" and _dedup_rows(fm, group, payment) == 0
    _assert_not_past(_committed(fm, group, partition), offset, "after the first worker")
    _no_failures_or_dead_letters(first)

    with _worker(fm, group) as second:
        second.wait_for("payment_settled", payment_id=str(payment))
        assert second.stop() == 0, second.diagnostics()

    retries = second.events("retry_scheduled", **mine)
    assert [(r["attempt"], r["error"].split(":")[0]) for r in retries] == [
        (1, "ProcessorUnavailable"),
        (2, "ProcessorUnavailable"),
    ], second.diagnostics()
    calls = second.events("processor_called", payment_id=str(payment))
    assert [c["attempt"] for c in calls] == [3], calls
    _settled_once(fm, group, payment, partition, offset)
    _no_failures_or_dead_letters(second)


# (5)
def test_transient_failures_do_not_count_toward_unclassified_exhaustion(fm):
    # The one case that expects a dead letter. WORKER_MAX_ATTEMPTS=2 and two transient runs first:
    # if those counted, P would be dead-lettered on its first Unclassified failure (run 3).
    group = fresh_group(fm, "processor-exhaustion")
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, UNAVAILABLE)
    [(_, original)] = fm.read_events(payment)
    fm.publish(payment)
    partition, offset = fm.position(payment)
    mine = {"partition": partition, "offset": offset}
    first_past = None  # P's DLQ records, read when P's committed offset was first seen past P

    env = {"FAULT_POINT": "RAISE_BEFORE_COMMIT", "FAULT_PAYMENT_ID": payment, "WORKER_MAX_ATTEMPTS": 2}
    with _worker(fm, group, **env) as worker:
        while not worker.events("message_dead_lettered", **mine):
            assert worker.process.poll() is None, worker.diagnostics()
            _assert_not_past(_committed(fm, group, partition), offset, "before P's dead letter")
            time.sleep(0.05)

        def past() -> bool:
            nonlocal first_past
            if (_committed(fm, group, partition) or 0) > offset:
                first_past = _dead_letters_of(fm, partition, offset)
                return True
            return False

        _wait(worker, past, 30, "P's offset committed")
        assert worker.stop() == 0, worker.diagnostics()

    [letter] = _dead_letters_of(fm, partition, offset)
    assert letter.header("dlq.attempts") == "4", worker.diagnostics()
    assert letter.header("dlq.error_class") == "Unclassified"
    assert letter.key == str(payment).encode() and letter.value == original, "byte-identical"
    [dead] = worker.events("message_dead_lettered", **mine)
    assert (dead["error_class"], dead["attempts"]) == ("Unclassified", 4), dead
    assert dead["event_id"] == str(_event_id(fm, payment))
    assert (letter.partition, letter.offset) == (dead["dlq_partition"], dead["dlq_offset"])

    retries = worker.events("retry_scheduled", **mine)
    assert [(r["attempt"], r["error_class"]) for r in retries] == [
        (1, "Transient"),
        (2, "Transient"),
        (3, "Unclassified"),
    ], retries
    assert all(r["error"].startswith("ProcessorUnavailable: ") for r in retries[:2]), [r["error"] for r in retries]
    assert "RAISE_BEFORE_COMMIT" in retries[2]["error"], [r["error"] for r in retries]
    calls = worker.events("processor_called", payment_id=str(payment))
    assert [c["attempt"] for c in calls] == [3, 4] and len({c["decision"] for c in calls}) == 1, calls
    assert len(worker.events("fault_injected", point="RAISE_BEFORE_COMMIT", payment_id=str(payment))) == 2

    # Committed past P only after the DLQ ack: the record was there when the commit was first seen.
    assert first_past is not None and len(first_past) == 1, first_past
    assert _dedup_rows(fm, group, payment) == 0 and _postings(fm, payment) == 0
    assert _status(fm, payment) == "AUTHORIZED"
    assert worker.events("processing_failed") == []
