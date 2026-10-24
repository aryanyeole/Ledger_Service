"""Retry in place (D-107, D-108, D-125) and dead letters (D-111) on real worker processes, on the
failure_modes stack. Every case starts its own consumer group at the topic's end offsets (fresh_group,
D-123), so it sees only the records it publishes and case order doesn't matter.

(a) 55P03: the test holds ESCROW FOR UPDATE as ledger_api_app (a granted lock; no trigger, schema
    or role touched) and the worker's settle times out on it: retried with the exact backoff, then
    applied exactly once when the holder lets go; the counter on /metrics matches.
(b) Unclassified (RAISE_BEFORE_COMMIT on P): exactly WORKER_MAX_ATTEMPTS attempts on the exact
    schedule, each rolled back (no dedup row, no posting, P AUTHORIZED); then P is dead-lettered
    (one DLQ record, key and value byte-identical, dlq.attempts=5) and its offset is committed, never
    before the DLQ ack. No exit; P stays AUTHORIZED with no dedup row.
(c) Other partitions keep moving: Q, published on another partition after P's first retry, settles
    while P is still retrying.
(d) Head of line holds: R (and any other payment behind P on P's partition) stays AUTHORIZED
    through every attempt, then settles in the same worker after P's message_dead_lettered.
(e) A stop during a backoff exits 0 at once, P uncommitted; the next worker in the group settles it.
(f) DLQ unavailable (InvalidEvent): WORKER_DLQ_TOPIC names a topic that doesn't exist (auto-create
    is off). Every produce fails as DlqUnavailable, a transient: retried on the schedule, offset not
    committed, no message_dead_lettered. Once the harness creates the topic: exactly one DLQ record
    (dlq.attempts = the processings so far), then the offset is committed past the record.
(g) DLQ unavailable, in-transaction poison (EventMismatch, raised after the payment row's UPDATE):
    while the produces fail, a FOR UPDATE NOWAIT probe on P's row as settlement_worker, every
    100 ms, never finds it held across a produce, and no settlement_worker transaction stays open
    longer than 500 ms. So the produce runs after the rollback, outside any DB transaction
    (invariant 5).
(b)-(d) are one scenario: they share P's fault run.

Timing is read from the worker's own log timestamps (structlog, UTC ISO with microseconds): each
attempt after the first starts no earlier than its retry_scheduled timestamp + delay_ms, less a
5 ms allowance for the wall clock against the monotonic clock the worker schedules with.
"""

import json
import time
import urllib.request
import uuid
from datetime import timedelta
from decimal import Decimal

from support.failure_kit import CLOCK_ALLOWANCE, EscrowHolder, RowLockProbe, create_dlq_topic, fresh_group
from support.failure_kit import assert_not_past as _assert_not_past
from support.failure_kit import committed as _committed
from support.failure_kit import dead_letters_of as _dead_letters_of
from support.failure_kit import dedup_rows as _dedup_rows
from support.failure_kit import dlq_records as _dlq_records
from support.failure_kit import event_id as _event_id
from support.failure_kit import funded_pair as _funded_pair
from support.failure_kit import postings as _postings
from support.failure_kit import results as _results
from support.failure_kit import status as _status
from support.failure_kit import ts as _ts
from support.failure_kit import wait as _wait
from support.failure_kit import worker as _worker
from support.failure_kit import worker_backends as _worker_backends
from support.stack import TOPIC

NOT_JSON = b"\xff\xfe not json\n"
DLQ_TIMEOUT = {"WORKER_DLQ_TIMEOUT_MS": 1000}


def _sample(port: int, prefix: str) -> Decimal:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as response:
        body = response.read().decode("utf-8")
    lines = [line for line in body.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, body
    return Decimal(lines[0][len(prefix) :])


def _retries_metric(port: int, error_class: str, sqlstate: str) -> Decimal:
    return _sample(port, f'settlement_retries_total{{error_class="{error_class}",sqlstate="{sqlstate}"}} ')


def _dead_letters_metric(port: int, error_class: str) -> Decimal:
    return _sample(port, f'settlement_dead_letters_total{{error_class="{error_class}"}} ')


def _missing_dlq_topic() -> str:
    return f"ledger.payments.dlq.missing.{uuid.uuid4().hex[:8]}"


def _schedule(attempts, base_ms: int = 50, max_ms: int = 400) -> list[int]:
    """The harness's backoff (D-123): base, doubling, capped; no jitter."""
    return [min(base_ms * 2 ** (a - 1), max_ms) for a in attempts]


# (a)
def test_lock_timeout_retries_in_place_then_applies_once(fm):
    group = fresh_group(fm, "retry-55p03")
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
    group = fresh_group(fm, "retry-unclassified")
    backoff = {"WORKER_RETRY_BASE_MS": 500, "WORKER_RETRY_MAX_MS": 2000}
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    [(_, original)] = fm.read_events(payment)
    fm.publish(payment)
    p_partition, p_offset = fm.position(payment)
    # R: behind P on P's partition, published before the worker starts (same fetch as P).
    r, _ = _publish_until(fm, group, customer, merchant, lambda pos: pos[0] == p_partition and pos[1] > p_offset)
    witnesses = [r]
    q = None
    checks = []
    first_past = None  # P's DLQ records, read when P's committed offset was first seen past P

    with _worker(fm, group, FAULT_POINT="RAISE_BEFORE_COMMIT", FAULT_PAYMENT_ID=payment, **backoff) as worker:
        mine = {"partition": p_partition, "offset": p_offset}
        checked = 0
        while not worker.events("message_dead_lettered", **mine):
            assert worker.process.poll() is None, worker.diagnostics()
            retries = worker.events("retry_scheduled", **mine)
            for retry in retries[checked:]:
                checks.append(
                    (_dedup_rows(fm, group, payment), _postings(fm, payment), _status(fm, payment))
                    + tuple(_status(fm, w) for w in witnesses)
                )
                _assert_not_past(
                    _committed(fm, group, p_partition), p_offset, f"after P's retry_scheduled attempt {retry['attempt']}"
                )
            checked = len(retries)
            if retries and q is None:
                # (c): published after P's first retry, so it can only settle if P's backoff lets
                # the loop keep fetching other partitions.
                q, behind = _publish_until(fm, group, customer, merchant, lambda pos: pos[0] != p_partition)
                witnesses += [w for w, pos in behind.items() if pos[0] == p_partition]
            time.sleep(0.05)

        def past() -> bool:
            nonlocal first_past
            if (_committed(fm, group, p_partition) or 0) > p_offset:
                first_past = _dead_letters_of(fm, p_partition, p_offset)
                return True
            return False

        _wait(worker, past, 30, "P's offset committed")
        for pid in [q, *witnesses]:
            worker.wait_for("payment_settled", payment_id=str(pid))
        dead_lettered = _dead_letters_metric(worker.metrics_port(), "Unclassified")
        assert worker.stop() == 0, worker.diagnostics()

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
    assert worker.events("processing_failed") == []

    [dead] = worker.events("message_dead_lettered", **mine)
    assert (dead["error_class"], dead["attempts"], dead["level"]) == ("Unclassified", 5, "error"), dead
    assert dead["event_id"] == str(_event_id(fm, payment))
    assert "RAISE_BEFORE_COMMIT" in dead["error"]
    assert _ts(dead) > _ts(retries[-1])
    [letter] = _dead_letters_of(fm, p_partition, p_offset)
    assert (letter.partition, letter.offset) == (dead["dlq_partition"], dead["dlq_offset"])
    assert letter.key == str(payment).encode() and letter.value == original, "byte-identical"
    assert (letter.header("dlq.error_class"), letter.header("dlq.attempts")) == ("Unclassified", "5")
    assert letter.header("dlq.consumer_group") == group
    assert letter.header("dlq.event_id") == dead["event_id"]
    assert dead_lettered == 1
    # Committed past P only after the DLQ ack: the record was there when the commit was first seen.
    assert first_past is not None and len(first_past) == 1, first_past
    assert _dedup_rows(fm, group, payment) == 0 and _postings(fm, payment) == 0
    assert _status(fm, payment) == "AUTHORIZED"

    # (c)
    assert q is not None
    q_settled = worker.events("payment_settled", payment_id=str(q))
    assert len(q_settled) == 1, worker.diagnostics()
    assert _ts(q_settled[0]) < _ts(retries[-1]), (q_settled, retries[-1])

    # (d): AUTHORIZED in every check; nothing behind P was applied before P's dead letter, and each
    # settled once, in this worker, after it (by log order).
    assert all(c[3:] == tuple("AUTHORIZED" for _ in c[3:]) for c in checks), checks
    lines = worker.lines()
    dead_at = next(
        i for i, e in enumerate(lines)
        if e["event"] == "message_dead_lettered" and (e["partition"], e["offset"]) == (p_partition, p_offset)
    )
    for w in witnesses:
        settled_at = [i for i, e in enumerate(lines) if e["event"] == "payment_settled" and e["payment_id"] == str(w)]
        assert len(settled_at) == 1 and settled_at[0] > dead_at, (w, settled_at, dead_at)
    for pid in [q, *witnesses]:
        assert _postings(fm, pid) == 1, pid
        assert _dedup_rows(fm, group, pid) == 1, pid
        assert _status(fm, pid) == "SETTLED", pid


# (e)
def test_stop_during_a_backoff_exits_0_uncommitted(fm):
    group = fresh_group(fm, "retry-stop-in-backoff")
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


def _dlq_unavailable_retries(worker, mine, at_least: int, sample=None) -> None:
    """Waits for at_least retries of the record; sample(), if given, runs on every poll."""
    deadline = time.monotonic() + 60
    while len(worker.events("retry_scheduled", **mine)) < at_least:
        assert worker.process.poll() is None, worker.diagnostics()
        assert time.monotonic() < deadline, f"timed out waiting for {at_least} retries:\n{worker.diagnostics()}"
        if sample is not None:
            sample()
        time.sleep(0.1)


def _assert_dlq_unavailable_retries(retries) -> None:
    assert retries, retries
    assert [r["attempt"] for r in retries] == list(range(1, len(retries) + 1)), retries
    assert [r["delay_ms"] for r in retries] == _schedule(r["attempt"] for r in retries), retries
    assert all(r["error_class"] == "Transient" and r["sqlstate"] is None for r in retries), retries
    assert all(r["error"].startswith("DlqUnavailable: ") for r in retries), retries


# (f)
def test_an_unavailable_dlq_is_retried_and_the_offset_waits_for_the_ack(fm):
    group = fresh_group(fm, "dlq-unavailable")
    missing = _missing_dlq_topic()
    key = uuid.uuid4()
    fm.produce([(key, NOT_JSON)])
    partition, offset = fm.position(key)

    with _worker(fm, group, WORKER_DLQ_TOPIC=missing, **DLQ_TIMEOUT) as worker:
        mine = {"partition": partition, "offset": offset}
        _dlq_unavailable_retries(worker, mine, 3)
        _assert_not_past(_committed(fm, group, partition), offset, "while the DLQ topic is missing")
        assert worker.events("message_dead_lettered") == [], worker.diagnostics()
        create_dlq_topic(fm, missing)
        dead = worker.wait_for("message_dead_lettered", timeout=60, **mine)
        _wait(worker, lambda: (_committed(fm, group, partition) or 0) > offset, 30, "the offset committed")
        retries = worker.events("retry_scheduled", **mine)
        assert worker.stop() == 0, worker.diagnostics()

    print(f"DLQ_UNAVAILABLE retries={len(retries)} attempts={dead['attempts']}")
    _assert_dlq_unavailable_retries(retries)
    assert (dead["error_class"], dead["event_id"]) == ("InvalidEvent", None), dead
    assert dead["attempts"] == len(retries) + 1, (dead, len(retries))
    [letter] = _dlq_records(fm, missing)
    assert letter.original() == (TOPIC, partition, offset)
    assert (letter.partition, letter.offset) == (dead["dlq_partition"], dead["dlq_offset"])
    assert letter.key == str(key).encode() and letter.value == NOT_JSON, "byte-identical"
    assert letter.header("dlq.error_class") == "InvalidEvent"
    assert letter.header("dlq.attempts") == str(len(retries) + 1)
    assert letter.header("dlq.event_id") is None
    assert _dead_letters_of(fm, partition, offset) == [], "nothing on the module's real DLQ"
    assert worker.events("processing_failed") == []


def _no_lock_held_across_a_produce(samples) -> None:
    """The settle transaction lasts milliseconds; a produce from inside it would hold P's row for up
    to WORKER_DLQ_TIMEOUT_MS (1 s), about ten probes in a row. Two in a row already fails."""
    longest, current = 0, 0
    for _, locked in samples:
        current = current + 1 if locked else 0
        longest = max(longest, current)
    assert longest < 2, [s for s in samples if s[1]]


# (g)
def test_an_in_transaction_poison_holds_no_lock_while_the_dlq_produce_blocks(fm):
    group = fresh_group(fm, "dlq-unavailable-mismatch")
    missing = _missing_dlq_topic()
    customer, merchant = _funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    [(_, original)] = fm.read_events(payment)
    document = json.loads(original)
    assert document["data"]["amount"] == "10.00"  # a JSON string, never a float (invariant 3)
    document["data"]["amount"] = "11.00"  # same event_id; disagrees with the payment row
    mismatched = json.dumps(document, separators=(",", ":")).encode("utf-8")
    fm.produce([(payment, mismatched)])
    partition, offset = fm.position(payment)
    long_transactions = []

    def sample_transactions() -> None:
        # Secondary: a settlement_worker transaction open for more than 500 ms.
        long_transactions.extend(
            _worker_backends(fm, "xact_start IS NOT NULL AND clock_timestamp() - xact_start > interval '500 milliseconds'")
        )

    with _worker(fm, group, WORKER_DLQ_TOPIC=missing, **DLQ_TIMEOUT) as worker:
        mine = {"partition": partition, "offset": offset}
        with RowLockProbe(fm, payment) as probe:
            _dlq_unavailable_retries(worker, mine, 3, sample=sample_transactions)
        samples = probe.samples
        assert worker.events("message_dead_lettered") == [], worker.diagnostics()
        create_dlq_topic(fm, missing)
        dead = worker.wait_for("message_dead_lettered", timeout=60, **mine)
        _wait(worker, lambda: (_committed(fm, group, partition) or 0) > offset, 30, "the offset committed")
        retries = worker.events("retry_scheduled", **mine)
        assert worker.stop() == 0, worker.diagnostics()

    locked = sum(1 for _, held in samples if held)
    print(f"DLQ_IN_TX_PROBE samples={len(samples)} locked={locked} long_transactions={len(long_transactions)}")
    assert len(samples) >= 20, samples
    _no_lock_held_across_a_produce(samples)
    assert long_transactions == [], long_transactions
    _assert_dlq_unavailable_retries(retries)
    assert (dead["error_class"], dead["event_id"]) == ("EventMismatch", str(_event_id(fm, payment))), dead
    assert dead["attempts"] == len(retries) + 1, (dead, len(retries))
    [letter] = _dlq_records(fm, missing)
    assert letter.original() == (TOPIC, partition, offset)
    assert letter.key == str(payment).encode() and letter.value == mismatched, "byte-identical"
    assert letter.header("dlq.error_class") == "EventMismatch"
    assert letter.header("dlq.event_id") == dead["event_id"]
    # The rollback took the dedup row with it (D-111: a dead-lettered event never has one).
    assert _dedup_rows(fm, group, payment) == 0 and _postings(fm, payment) == 0
    assert _status(fm, payment) == "AUTHORIZED"
    assert worker.events("processing_failed") == []
