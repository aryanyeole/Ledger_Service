"""Poison messages are dead-lettered, never retried or lost (D-107, D-111, D-125): real worker
processes on this module's own failure_modes stack (D-123). Relay off: the harness publishes each
event from its outbox row (D-103) unless a case fabricates one. Every case starts its own consumer
group at the topic's end offsets (fresh_group), and every DLQ check reads the topic back with a
separate client. No case may log processing_failed; the poison cases log no retry_scheduled either.

(1) InvalidEvent, six shapes in one worker: a value that isn't UTF-8; a JSON-number amount; an
    unknown event_type; a record key that isn't data.payment_id; a null value with a key; neither key
    nor value. Each: exactly one DLQ record with the original key and value bytes (the last one: an
    empty value and dlq.original_value_null: true), InvalidEvent, dlq.attempts=1, the original
    topic/partition/offset; dlq.event_id and the log's correlation_id only for the mis-keyed record,
    whose envelope parses (BUG-007). No processor_called, no dedup row, every offset committed past
    its record; settlement_dead_letters_total{error_class="InvalidEvent"} = 6.
(2) UnknownPayment: a real envelope with a fresh payment_id, record key and event_id. One DLQ record;
    the dedup row rolled back with the transaction; no posting; processor_called once.
(3) EventMismatch, then the genuine event: P's event with only the amount changed, same event_id, is
    dead-lettered and P stays AUTHORIZED with no dedup row; P's genuine event right behind it then
    settles exactly once. A dead letter never uses up the dedup slot.
(4) Head of line: a poison record keyed by P's id, then P's real event on the same partition. The
    poison's message_dead_lettered comes before P's payment_settled; P settles once; when the
    committed offset is first seen past the poison record, its DLQ record is already there.
(5) Replay (D-125, D-111): RAISE_BEFORE_COMMIT on P, 5 attempts, one DLQ record (Unclassified,
    dlq.attempts=5). A worker without the fault, in the same group, then gets the DLQ record replayed
    twice by support/dlq_replay: 1 posting, 1 dedup row, 1 result row, 1 payment_settled,
    1 duplicate_skipped, both copies committed; the copies carry the original header and no dlq.*.
"""

import json
import urllib.request
import uuid
from decimal import Decimal

from support.dlq_replay import replay
from support.failure_kit import (
    committed,
    dead_letters_of,
    dedup_rows,
    event_id,
    fresh_group,
    funded_pair,
    positions,
    postings,
    produce_null_record,
    results,
    status,
    topic_records,
    wait,
    wait_idle,
    worker,
    worker_role,
)
from support.outbox_events import with_changes
from support.stack import TOPIC

NOT_UTF8 = b"\xff\xfe not UTF-8"
NOT_JSON = b"\xff not json"
ORIGIN_HEADER = ("x-test-origin", b"t9-replay")


def _dead_letters_metric(port: int, error_class: str) -> Decimal:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10) as response:
        body = response.read().decode("utf-8")
    prefix = f'settlement_dead_letters_total{{error_class="{error_class}"}} '
    lines = [line for line in body.splitlines() if line.startswith(prefix)]
    assert len(lines) == 1, body
    return Decimal(lines[0][len(prefix) :])


def _envelope(fm, payment) -> bytes:
    [(_, raw)] = fm.read_events(payment)
    return raw


def _replaced(raw: bytes, old: bytes, new: bytes) -> bytes:
    assert raw.count(old) == 1, (old, raw)
    return raw.replace(old, new)


def _dead(w, partition: int, offset: int) -> dict:
    [entry] = w.events("message_dead_lettered", partition=partition, offset=offset)
    return entry


def _letter(fm, partition: int, offset: int):
    [letter] = dead_letters_of(fm, partition, offset)
    return letter


def _line_of(w, name: str, **match) -> int:
    [index] = [i for i, e in enumerate(w.lines()) if e["event"] == name and all(e.get(k) == v for k, v in match.items())]
    return index


def _dedup_by_event_id(fm, group: str, eid: uuid.UUID) -> int:
    rows = worker_role(
        fm, "SELECT count(*) AS n FROM processed_messages WHERE consumer_group = $1 AND event_id = $2", group, eid
    )
    return rows[0]["n"]


def _committed_past(fm, w, group: str, records: list[tuple[int, int]], what: str) -> None:
    last = {}
    for partition, offset in records:
        last[partition] = max(offset, last.get(partition, -1))
    wait(w, lambda: all((committed(fm, group, p) or 0) > o for p, o in last.items()), 30, what)


def _no_retries_or_failures(w) -> None:
    assert w.events("retry_scheduled") == [], w.diagnostics()
    assert w.events("processing_failed") == [], w.diagnostics()


# (1)
def test_invalid_events_are_dead_lettered_once_each(fm):
    group = fresh_group(fm, "dlq-invalid")
    customer, merchant = funded_pair(fm)
    number_amount, unknown_type, mis_keyed = (
        fm.api.authorize(customer, merchant, "10.00", correlation_id=f"c-t9-{name}")
        for name in ("number", "type", "mis-keyed")
    )
    keys = {name: uuid.uuid4() for name in ("not-utf8", "mis-keyed", "null-value")}
    cases = {
        "not-utf8": (keys["not-utf8"], NOT_UTF8),
        "number-amount": (number_amount, _replaced(_envelope(fm, number_amount), b'"amount":"10.00"', b'"amount":10.00')),
        "unknown-type": (unknown_type, with_changes(_envelope(fm, unknown_type), event_type="payment.refunded")),
        "mis-keyed": (keys["mis-keyed"], _envelope(fm, mis_keyed)),
        "null-value": (keys["null-value"], None),
    }
    fm.produce(list(cases.values()))
    where = {name: positions(fm, str(key).encode())[0] for name, (key, _) in cases.items()}
    where["neither"] = produce_null_record(fm)

    with worker(fm, group) as w:
        wait(w, lambda: len(w.events("message_dead_lettered")) == 6, 60, "six dead letters")
        _committed_past(fm, w, group, list(where.values()), "every poison offset committed")
        dead_lettered = _dead_letters_metric(w.metrics_port(), "InvalidEvent")
        assert w.stop() == 0, w.diagnostics()

    print(f"DLQ_INVALID records={len(where)} dead_letters_total={dead_lettered}")
    _no_retries_or_failures(w)
    assert w.events("processor_called") == [], "no processor call for a record that never parsed into a payment"
    assert dead_lettered == 6
    expected = {name: (str(key).encode(), value) for name, (key, value) in cases.items()}
    expected["neither"] = (None, b"")
    parsed = {"mis-keyed": (str(event_id(fm, mis_keyed)), "c-t9-mis-keyed")}
    for name, (partition, offset) in where.items():
        letter = _letter(fm, partition, offset)
        dead = _dead(w, partition, offset)
        assert (letter.key, letter.value) == expected[name], name
        assert letter.original() == (TOPIC, partition, offset), name
        assert (letter.header("dlq.error_class"), letter.header("dlq.attempts")) == ("InvalidEvent", "1"), name
        assert letter.header("dlq.consumer_group") == group, name
        assert letter.header("dlq.original_value_null") == ("true" if name == "neither" else None), name
        event, correlation = parsed.get(name, (None, None))
        assert letter.header("dlq.event_id") == event, name
        assert (dead["error_class"], dead["attempts"], dead["event_id"], dead["correlation_id"]) == (
            "InvalidEvent", 1, event, correlation
        ), name
        assert (letter.partition, letter.offset) == (dead["dlq_partition"], dead["dlq_offset"]), name
    for payment in (number_amount, unknown_type, mis_keyed):
        assert dedup_rows(fm, group, payment) == 0 and postings(fm, payment) == 0, payment
        assert status(fm, payment) == "AUTHORIZED", payment


# (2)
def test_an_unknown_payment_is_dead_lettered_and_its_dedup_row_rolled_back(fm):
    group = fresh_group(fm, "dlq-unknown-payment")
    customer, merchant = funded_pair(fm)
    template = fm.api.authorize(customer, merchant, "10.00")
    ghost, ghost_event = uuid.uuid4(), uuid.uuid4()
    value = with_changes(_envelope(fm, template), event_id=str(ghost_event), data={"payment_id": str(ghost)})
    fm.produce([(ghost, value)])
    [(partition, offset)] = positions(fm, str(ghost).encode())

    with worker(fm, group) as w:
        dead = w.wait_for("message_dead_lettered", partition=partition, offset=offset)
        _committed_past(fm, w, group, [(partition, offset)], "the offset committed")
        assert w.stop() == 0, w.diagnostics()

    _no_retries_or_failures(w)
    letter = _letter(fm, partition, offset)
    assert (letter.key, letter.value) == (str(ghost).encode(), value)
    assert (letter.header("dlq.error_class"), letter.header("dlq.attempts")) == ("UnknownPayment", "1")
    assert letter.header("dlq.event_id") == str(ghost_event) == dead["event_id"]
    assert dead["error_class"] == "UnknownPayment"
    assert len(w.events("processor_called", payment_id=str(ghost))) == 1
    assert _dedup_by_event_id(fm, group, ghost_event) == 0, "rolled back with the transaction"
    assert postings(fm, ghost) == 0
    assert status(fm, template) == "AUTHORIZED" and postings(fm, template) == 0


# (3)
def test_a_mismatched_event_is_dead_lettered_and_the_genuine_one_still_settles_once(fm):
    group = fresh_group(fm, "dlq-mismatch-then-genuine")
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    genuine = _envelope(fm, payment)
    mismatched = with_changes(genuine, data={"amount": "11.00"})  # a valid D-047 string; same event_id
    fm.produce([(payment, mismatched), (payment, genuine)])
    first, second = positions(fm, str(payment).encode())
    assert first[0] == second[0] and first[1] < second[1]

    with worker(fm, group) as w:
        w.wait_for("payment_settled", payment_id=str(payment))
        _committed_past(fm, w, group, [second], "both offsets committed")
        assert w.stop() == 0, w.diagnostics()

    _no_retries_or_failures(w)
    letter = _letter(fm, *first)
    dead = _dead(w, *first)
    assert (letter.key, letter.value) == (str(payment).encode(), mismatched)
    assert (letter.header("dlq.error_class"), letter.header("dlq.attempts")) == ("EventMismatch", "1")
    assert letter.header("dlq.event_id") == str(event_id(fm, payment)) == dead["event_id"]
    assert dead_letters_of(fm, *second) == []
    assert _line_of(w, "message_dead_lettered", partition=first[0], offset=first[1]) < _line_of(
        w, "payment_settled", payment_id=str(payment)
    )
    assert len(w.events("processor_called", payment_id=str(payment))) == 2
    assert postings(fm, payment) == 1
    assert dedup_rows(fm, group, payment) == 1
    assert results(fm, payment) == 1
    assert status(fm, payment) == "SETTLED"


# (4)
def test_a_poison_record_ahead_of_its_payment_is_dead_lettered_first(fm):
    group = fresh_group(fm, "dlq-head-of-line")
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    fm.produce([(payment, NOT_JSON), (payment, _envelope(fm, payment))])
    poison, real = positions(fm, str(payment).encode())
    assert poison[0] == real[0] and poison[1] < real[1]
    first_past = None  # the poison's DLQ records, read when its offset was first seen committed

    with worker(fm, group) as w:
        def sample() -> bool:
            nonlocal first_past
            seen = committed(fm, group, poison[0])
            if (seen or 0) > poison[1] and first_past is None:
                first_past = dead_letters_of(fm, *poison)
            return (seen or 0) > real[1]

        wait(w, sample, 60, "P's offset committed")
        w.wait_for("payment_settled", payment_id=str(payment))
        assert w.stop() == 0, w.diagnostics()

    _no_retries_or_failures(w)
    # The offset passed the poison record only with its DLQ record already on the topic (D-111).
    assert first_past is not None and len(first_past) == 1, first_past
    letter = _letter(fm, *poison)
    assert (letter.key, letter.value) == (str(payment).encode(), NOT_JSON)
    assert (letter.header("dlq.error_class"), letter.header("dlq.event_id")) == ("InvalidEvent", None)
    assert _line_of(w, "message_dead_lettered", partition=poison[0], offset=poison[1]) < _line_of(
        w, "payment_settled", payment_id=str(payment)
    )
    assert len(w.events("payment_settled", payment_id=str(payment))) == 1
    assert postings(fm, payment) == 1 and dedup_rows(fm, group, payment) == 1 and results(fm, payment) == 1


# (5)
def test_a_replayed_dead_letter_settles_once_and_a_second_copy_is_deduplicated(fm):
    group = fresh_group(fm, "dlq-replay")
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    original = _envelope(fm, payment)
    fm.produce([(payment, original)], headers=[ORIGIN_HEADER])
    [(partition, offset)] = positions(fm, str(payment).encode())

    with worker(fm, group, FAULT_POINT="RAISE_BEFORE_COMMIT", FAULT_PAYMENT_ID=payment) as faulty:
        faulty.wait_for("message_dead_lettered", partition=partition, offset=offset)
        _committed_past(fm, faulty, group, [(partition, offset)], "P's offset committed after its dead letter")
        assert faulty.stop() == 0, faulty.diagnostics()
    assert len(faulty.events("retry_scheduled")) == 4
    assert faulty.events("processing_failed") == []
    letter = _letter(fm, partition, offset)
    assert (letter.header("dlq.error_class"), letter.header("dlq.attempts")) == ("Unclassified", "5")
    assert letter.headers[0] == ORIGIN_HEADER, letter.headers
    assert postings(fm, payment) == 0 and dedup_rows(fm, group, payment) == 0

    with worker(fm, group) as w:
        wait_idle(fm, w, group)
        copies = replay(fm.kafka.bootstrap_servers, letter, times=2)
        w.wait_for("payment_settled", payment_id=str(payment))
        w.wait_for("duplicate_skipped", payment_id=str(payment))
        _committed_past(fm, w, group, copies, "both replayed copies committed")
        assert w.stop() == 0, w.diagnostics()

    print(f"DLQ_REPLAY copies={copies}")
    _no_retries_or_failures(w)
    assert [c[0] for c in copies] == [partition, partition], "keyed by P, so P's partition"
    assert len(w.events("processor_called", payment_id=str(payment))) == 2
    assert len(w.events("payment_settled", payment_id=str(payment))) == 1
    assert len(w.events("duplicate_skipped", payment_id=str(payment))) == 1
    assert postings(fm, payment) == 1
    assert dedup_rows(fm, group, payment) == 1
    assert results(fm, payment) == 1
    assert status(fm, payment) == "SETTLED"
    # Last, so a helper that kept the dlq.* headers fails only here, after every processing check.
    replayed = {(r.partition, r.offset): r for r in topic_records(fm)}
    for position in copies:
        record = replayed[position]
        assert (record.key, record.value) == (str(payment).encode(), original)
        assert record.headers == [ORIGIN_HEADER], "the original header, and no dlq.* header"
