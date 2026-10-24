"""The per-record step (D-081, D-107, D-108, D-109, D-111).

It rejects a bad record before touching the DB or the broker. The _step tests below drive the
retry / dead-letter / pending-commit state machine with fake Kafka objects (consumer and DLQ
producer) and a raising stand-in for the processing step. Ledger, dedup and idempotency logic are
never faked here (CLAUDE.md): those run against real Postgres in the integration and failure_modes
tests.
"""

import asyncio
import json
import random
from dataclasses import dataclass

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import KafkaConnectionError
from structlog.testing import capture_logs

from settlement_worker import consumer
from settlement_worker.config import Config
from settlement_worker.envelope import InvalidEvent
from settlement_worker.dlq import DlqUnavailable
from settlement_worker.retry import RetryState

PAYMENT = "5b01a9e5-32ce-4c82-90b0-ac8c5143460c"
OTHER = "0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp: int = 1_792_000_000_000
    headers: tuple = ()


class Untouchable:
    """Any use of the DB connection or the consumer fails the test."""

    def __init__(self, name):
        object.__setattr__(self, "_name", name)

    def __getattr__(self, attribute):
        raise AssertionError(f"{self._name}.{attribute} was used")


def _value() -> bytes:
    return json.dumps(
        {
            "event_id": OTHER,
            "event_type": "payment.authorized",
            "version": 1,
            "occurred_at": "2026-10-19T06:22:05.120000Z",
            "correlation_id": None,
            "data": {
                "payment_id": PAYMENT,
                "customer_account_id": 12,
                "merchant_account_id": 13,
                "amount": "100.00",
                "fee": "2.90",
            },
        }
    ).encode()


@pytest.mark.parametrize("key", [OTHER.encode(), None, b"not-a-uuid", b"\xff"], ids=["other id", "none", "text", "bytes"])
def test_key_must_equal_the_payment_id(key):
    record = Record("ledger.payments.v1", 0, 7, key, _value())
    config = Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x"})
    with pytest.raises(InvalidEvent):
        asyncio.run(
            consumer.process_record(
                record,
                session=Untouchable("session"),
                consumer=Untouchable("consumer"),
                config=config,
                commits=Untouchable("commits"),
            )
        )


class FailingCommit:
    """A consumer whose offset commit fails; seeking or pausing (a retry) fails the test."""

    def __init__(self):
        self.commits = 0

    async def commit(self, offsets):
        self.commits += 1
        raise KafkaConnectionError("broker gone")

    def seek(self, *args):
        raise AssertionError("a failed offset commit must not seek (no re-run, D-107)")

    def pause(self, *args):
        raise AssertionError("a failed offset commit must not pause (no re-run, D-107)")


def _own_result() -> bytes:
    return json.dumps(
        {
            "event_id": OTHER,
            "event_type": "payment.settled",
            "version": 1,
            "occurred_at": "2026-10-19T06:22:05.120000Z",
            "correlation_id": None,
            "data": {"payment_id": PAYMENT},
        }
    ).encode()


class LiveSession:
    """A DB session that is connected; dropping it is recorded (D-109)."""

    def __init__(self):
        self.dropped = 0

    broken = False

    def closed(self):
        return False

    def drop(self):
        self.dropped += 1


class Recording:
    """A consumer that records seeks and pauses."""

    def __init__(self):
        self.calls = []

    def seek(self, tp, offset):
        self.calls.append(("seek", tp.partition, offset))

    def pause(self, *tps):
        self.calls.append(("pause", *[tp.partition for tp in tps]))


# D-107/D-109: the worker's own result event is committed without a DB write, so a failing commit
# here is exactly "the effect is done, the offset commit failed". It is never classified or re-run:
# no retry_scheduled, no seek or pause, no attempt counted, no exit; offset_commit_failed and the
# offset stays pending for the loop to recommit.
def test_failed_offset_commit_is_pending_not_a_retry():
    record = Record("ledger.payments.v1", 1, 9, PAYMENT.encode(), _own_result())
    tp = TopicPartition(record.topic, record.partition)
    config = Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x", "WORKER_RETRY_JITTER": "0"})
    state = RetryState()
    fake = FailingCommit()
    commits = consumer.Commits(fake, config, random.Random(0))
    with capture_logs() as logs:
        result = asyncio.run(
            consumer._step(
                record, tp, session=LiveSession(), consumer=fake, config=config, state=state, commits=commits,
                dlq=Untouchable("dlq"), rng=random.Random(0),
            )
        )
    assert result is consumer.Step.OK
    assert fake.commits == 1
    assert [entry["event"] for entry in logs] == ["offset_commit_failed"]
    assert (logs[0]["offset"], logs[0]["attempt"], logs[0]["delay_ms"]) == (9, 1, 200)
    assert "OffsetCommitFailed" in logs[0]["error"]
    assert commits.pending.offset(tp) == 9
    assert state.attempts(tp, record.offset) == 0


# D-109: only DB call sites turn a timeout into DbTimeout. A TimeoutError raised outside a DB call
# (here, by the processor) is a plain transient: retried in place, the DB connection left alone.
def test_timeout_outside_a_db_call_is_not_db_timeout(monkeypatch):
    def slow_processor(*args, **kwargs):
        raise TimeoutError("processor took too long")

    monkeypatch.setattr(consumer.processor, "decide", slow_processor)
    record = Record("ledger.payments.v1", 2, 4, PAYMENT.encode(), _value())
    tp = TopicPartition(record.topic, record.partition)
    config = Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x", "WORKER_RETRY_JITTER": "0"})
    session = LiveSession()
    fake = Recording()
    with capture_logs() as logs:
        result = asyncio.run(
            consumer._step(
                record, tp, session=session, consumer=fake, config=config, state=RetryState(),
                commits=Untouchable("commits"), dlq=Untouchable("dlq"), rng=random.Random(0),
            )
        )
    assert result is consumer.Step.RETRY
    retries = [entry for entry in logs if entry["event"] == "retry_scheduled"]
    assert len(retries) == 1
    assert retries[0]["error"].startswith("TimeoutError: ")
    assert "DbTimeout" not in retries[0]["error"]
    assert (retries[0]["error_class"], retries[0]["sqlstate"]) == ("Transient", None)
    assert session.dropped == 0, "a non-DB timeout must not reconnect the DB"
    assert fake.calls == [("seek", 2, 4), ("pause", 2)]


def test_matching_key_is_accepted_case_insensitively():
    event = consumer.classify(Record("t", 0, 0, PAYMENT.upper().encode(), _value()))
    assert str(event.payment_id) == PAYMENT


def test_a_null_value_is_invalid():
    with pytest.raises(InvalidEvent, match="record has no value"):
        consumer.classify(Record("ledger.payments.v1", 0, 1, PAYMENT.encode(), None))


NOT_JSON = bytes([0xFF]) + b" not json"  # not UTF-8 either


class Sequence:
    """One ordered log shared by the fake consumer and the fake DLQ producer."""

    def __init__(self):
        self.calls = []


class FakeConsumer:
    """Commits succeed; seeks and pauses are recorded."""

    def __init__(self, seq):
        self.seq = seq

    async def commit(self, offsets):
        self.seq.calls.append(("commit", {tp.partition: off for tp, off in offsets.items()}))

    def seek(self, tp, offset):
        self.seq.calls.append(("seek", tp.partition, offset))

    def pause(self, *tps):
        self.seq.calls.append(("pause", *[tp.partition for tp in tps]))


class FakeDlq:
    """A DLQ producer that fails the first `failures` produces with DlqUnavailable."""

    def __init__(self, seq, failures=0):
        self.seq = seq
        self.failures = failures
        self.letters = []

    async def produce(self, letter):
        if self.failures > 0:
            self.failures -= 1
            self.seq.calls.append(("produce-failed",))
            raise DlqUnavailable("dead letter to ledger.payments.dlq.v1 not acknowledged: KafkaTimeoutError")
        self.letters.append(letter)
        self.seq.calls.append(("produce", len(self.letters) - 1))
        return 0, len(self.letters) - 1


def _run_step(record, *, config, state, commits, dlq, fake):
    return asyncio.run(
        consumer._step(
            record, TopicPartition(record.topic, record.partition), session=LiveSession(), consumer=fake,
            config=config, state=state, commits=commits, dlq=dlq, rng=random.Random(0),
        )
    )


def _config(**extra):
    return Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x", "WORKER_RETRY_JITTER": "0", **extra})


# D-111: poison is dead-lettered on its first failure: produce (ack) -> message_dead_lettered ->
# commit offset+1, in that order; attempts=1; the original bytes go out unchanged.
def test_poison_is_dead_lettered_on_its_first_failure_then_committed():
    seq = Sequence()
    fake, dlq = FakeConsumer(seq), FakeDlq(seq)
    record = Record("ledger.payments.v1", 1, 12, b"some-key", NOT_JSON)
    config, state = _config(), RetryState()
    commits = consumer.Commits(fake, config, random.Random(0))
    with capture_logs() as logs:
        result = _run_step(record, config=config, state=state, commits=commits, dlq=dlq, fake=fake)
    assert result is consumer.Step.OK
    assert seq.calls == [("produce", 0), ("commit", {1: 13})]
    letter = dlq.letters[0]
    assert (letter.key, letter.value) == (b"some-key", NOT_JSON)
    headers = dict(letter.headers)
    assert (headers["dlq.error_class"], headers["dlq.attempts"]) == (b"InvalidEvent", b"1")
    assert "dlq.event_id" not in headers
    assert [entry["event"] for entry in logs] == ["message_dead_lettered"]
    assert logs[0]["log_level"] == "error"
    assert (logs[0]["error_class"], logs[0]["attempts"], logs[0]["dlq_offset"]) == ("InvalidEvent", 1, 0)
    assert state.attempts(TopicPartition(record.topic, record.partition), 12) == 0


# D-111: a DLQ produce that fails is transient: the record is re-run on the D-108 schedule, nothing
# is committed, and no message_dead_lettered is emitted.
def test_a_failed_dlq_produce_is_retried_and_nothing_is_committed():
    seq = Sequence()
    fake, dlq = FakeConsumer(seq), FakeDlq(seq, failures=1)
    record = Record("ledger.payments.v1", 1, 12, None, b"not json")
    config, state = _config(), RetryState()
    commits = consumer.Commits(fake, config, random.Random(0))
    with capture_logs() as logs:
        result = _run_step(record, config=config, state=state, commits=commits, dlq=dlq, fake=fake)
    assert result is consumer.Step.RETRY
    assert seq.calls == [("produce-failed",), ("seek", 1, 12), ("pause", 1)]
    assert [entry["event"] for entry in logs] == ["retry_scheduled"]
    assert logs[0]["error"].startswith("DlqUnavailable: ")
    assert (logs[0]["error_class"], logs[0]["attempt"]) == ("Transient", 1)


# D-111: the attempt count survives DlqUnavailable. An Unclassified record exhausted at attempt 5
# whose dead letter fails once is dead-lettered on its 6th run (dlq.attempts=6), not given 5 more.
def test_an_exhausted_record_hit_by_dlq_unavailable_is_dead_lettered_on_its_next_run(monkeypatch):
    def broken_processor(*args, **kwargs):
        raise RuntimeError("bug in the processing step")

    monkeypatch.setattr(consumer.processor, "decide", broken_processor)
    seq = Sequence()
    fake, dlq = FakeConsumer(seq), FakeDlq(seq, failures=1)
    record = Record("ledger.payments.v1", 0, 3, PAYMENT.encode(), _value())
    config, state = _config(), RetryState()
    commits = consumer.Commits(fake, config, random.Random(0))
    results = [_run_step(record, config=config, state=state, commits=commits, dlq=dlq, fake=fake) for _ in range(6)]
    assert results == [consumer.Step.RETRY] * 5 + [consumer.Step.OK]
    assert [c[0] for c in seq.calls if c[0].startswith("produce")] == ["produce-failed", "produce"]
    headers = dict(dlq.letters[0].headers)
    assert (headers["dlq.error_class"], headers["dlq.attempts"]) == (b"Unclassified", b"6")
    assert headers["dlq.event_id"] == OTHER.encode()
    assert seq.calls[-1] == ("commit", {0: 4})
