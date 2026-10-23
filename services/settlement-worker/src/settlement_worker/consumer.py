"""The consume loop: offsets after the DB commit, retry in place, fault points (D-014, D-081, D-085,
D-107, D-108, D-125).

Records are processed one at a time, in partition order. For each record:
  1. envelope.parse (D-102) and, for payment.authorized, the record key must equal
     data.payment_id. Both happen before the DB or the broker is touched;
  2. the worker's own payment.settled / payment.failed: commit offset+1, no DB write;
  3. payment.authorized: processor.decide on every delivery, before the dedup check (its
     idempotency stays visible to the crash test) -> settlement.handle, which commits its own
     transaction (D-082) -> commit offset+1. Committing the offset only after the DB commit means
     a crash between them redelivers the record, and the dedup row absorbs it (invariant 6).
A failure is classified (failures.classify, D-107). Retried in place now (D-108): an Unclassified
error, up to WORKER_MAX_ATTEMPTS attempts, and 55P03 / 40P01 / 40001 on a live connection,
indefinitely. A retry is a redelivery to self: seek the partition back to the record, pause it,
drop the rest of its fetched records, resume it after the backoff; other partitions keep moving.
Everything else, an exhausted Unclassified included, -> processing_failed and exit 1 without
committing (connection loss: T5b; poison and exhausted Unclassified to the DLQ: T7). A failed
offset commit is not a record failure (the effect committed): it is never classified or re-run,
and exits 1 until T5b (D-109). Records fetched but not processed stay uncommitted and are
redelivered. A stop request is honoured between records and during a backoff (within one getmany
wait), never between the DB commit and the offset commit.
Faults (tests only), for FAULT_PAYMENT_ID only: at AFTER_PROCESSOR_BEFORE_DB / AFTER_DB_BEFORE_OFFSET
log fault_injected, flush stdout and os._exit(86): no finally blocks, no LeaveGroup, no offset
commit, exactly like a crash. RAISE_BEFORE_COMMIT logs fault_injected and raises a RuntimeError
inside the settle transaction on every attempt (D-125).
"""

import asyncio
import enum
import os
import random
import sys
import time
import uuid

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import KafkaError

from settlement_worker import db, envelope, events, failures, metrics, processor, settlement
from settlement_worker.config import AFTER_DB_BEFORE_OFFSET, AFTER_PROCESSOR_BEFORE_DB, RAISE_BEFORE_COMMIT, Config
from settlement_worker.envelope import AuthorizedEvent, InvalidEvent, OwnResultEvent
from settlement_worker.failures import ErrorClass
from settlement_worker.retry import RetryState, backoff_ms

EXIT_STOPPED = 0
EXIT_PROCESSING_ERROR = 1
EXIT_CONFIG = 2
EXIT_FAULT = 86

GETMANY_TIMEOUT_MS = 500


def classify(record) -> AuthorizedEvent | OwnResultEvent:
    """Parse a record and check its key, touching neither the DB nor the broker."""
    event = envelope.parse(record.value)
    if isinstance(event, AuthorizedEvent):
        try:
            key = record.key.decode("utf-8") if record.key is not None else None
        except UnicodeDecodeError as ex:
            raise InvalidEvent("record key is not UTF-8") from ex
        if key is None or not envelope.CANONICAL_UUID.fullmatch(key) or uuid.UUID(key) != event.payment_id:
            raise InvalidEvent(f"record key {key!r} does not equal data.payment_id {event.payment_id}")
    return event


def _fault(config: Config, point: str, event: AuthorizedEvent) -> None:
    if config.fault_point == point and event.payment_id == config.fault_payment_id:
        events.emit(
            structlog.get_logger(),
            events.FAULT_INJECTED,
            point=point,
            payment_id=str(event.payment_id),
            correlation_id=event.correlation_id,
        )
        sys.stdout.flush()
        os._exit(EXIT_FAULT)


class OffsetCommitFailed(Exception):
    """The record's effect committed but its offset commit failed: not a record failure (D-107)."""


async def _commit(consumer, tp: TopicPartition, offset: int) -> None:
    try:
        await consumer.commit({tp: offset + 1})
    except KafkaError as ex:
        raise OffsetCommitFailed(f"offset commit failed for {tp.topic}[{tp.partition}]@{offset}: {ex!r}") from ex


def _raise_before_commit(config: Config, event: AuthorizedEvent):
    """The RAISE_BEFORE_COMMIT fault for FAULT_PAYMENT_ID, else None (D-125)."""
    if config.fault_point != RAISE_BEFORE_COMMIT or event.payment_id != config.fault_payment_id:
        return None

    def fault() -> None:
        events.emit(
            structlog.get_logger(),
            events.FAULT_INJECTED,
            point=RAISE_BEFORE_COMMIT,
            payment_id=str(event.payment_id),
            correlation_id=event.correlation_id,
        )
        raise RuntimeError(f"fault {RAISE_BEFORE_COMMIT} for payment {event.payment_id}")

    return fault


async def process_record(record, *, conn: asyncpg.Connection, consumer, config: Config) -> None:
    event = classify(record)
    tp = TopicPartition(record.topic, record.partition)
    if isinstance(event, OwnResultEvent):
        await _commit(consumer, tp, record.offset)
        return
    decision = processor.decide(event.payment_id, event.amount, correlation_id=event.correlation_id)
    _fault(config, AFTER_PROCESSOR_BEFORE_DB, event)
    await settlement.handle(
        conn,
        event,
        decision,
        consumer_group=config.consumer_group,
        topic=config.topic,
        lock_timeout=config.lock_timeout,
        before_commit=_raise_before_commit(config, event),
    )
    _fault(config, AFTER_DB_BEFORE_OFFSET, event)
    await _commit(consumer, tp, record.offset)


def _retry_in_place(failure: failures.Failure) -> bool:
    """What T5a retries (D-107 staging): Unclassified, and the lock SQLSTATEs on a live connection."""
    if failure.error_class is ErrorClass.UNCLASSIFIED:
        return True
    return (
        failure.error_class is ErrorClass.TRANSIENT
        and not failure.connection
        and failure.sqlstate in failures.LOCK_SQLSTATES
    )


class Step(enum.Enum):
    OK = "ok"
    RETRY = "retry"
    FAILED = "failed"


async def _step(record, tp: TopicPartition, *, conn, consumer, config: Config, state: RetryState, rng) -> Step:
    """One record: process it; on failure, classify it and either schedule a retry in place or fail."""
    log = structlog.get_logger()
    try:
        await process_record(record, conn=conn, consumer=consumer, config=config)
    except OffsetCommitFailed as ex:
        # Not a record failure: the effect committed, so it is never classified or re-run (D-107).
        # Exit 1 until T5b's pending recommit (D-109).
        _processing_failed(log, record, ex)
        return Step.FAILED
    except Exception as ex:  # noqa: BLE001 - classified below (D-107)
        # settlement.handle's transaction context has exited by now and rolled back, so no row or
        # account lock is held during the backoff that may follow (D-108).
        failure = failures.classify(ex)
        attempt = state.failed(tp, record.offset)
        exhausted = failure.error_class is ErrorClass.UNCLASSIFIED and attempt >= config.max_attempts
        if not _retry_in_place(failure) or exhausted:
            _processing_failed(log, record, ex)
            return Step.FAILED
        delay = backoff_ms(
            attempt, base_ms=config.retry_base_ms, max_ms=config.retry_max_ms, jitter=config.retry_jitter, rng=rng
        )
        events.emit(
            log,
            events.RETRY_SCHEDULED,
            level="error" if failure.sqlstate == "40P01" else "warning",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            attempt=attempt,
            delay_ms=delay,
            error_class=failure.error_class.value,
            sqlstate=failure.sqlstate,
            error=f"{type(ex).__name__}: {ex}",
            correlation_id=_correlation_id(record),
        )
        metrics.RETRIES.labels(error_class=failure.error_class.value, sqlstate=failure.sqlstate or "").inc()
        # Redelivery to self: back to this record, paused until the backoff ends.
        consumer.seek(tp, record.offset)
        consumer.pause(tp)
        state.schedule(tp, time.monotonic_ns() + delay * 1_000_000)
        return Step.RETRY
    state.succeeded(tp, record.offset)
    return Step.OK


def _processing_failed(log, record, ex: BaseException) -> None:
    events.emit(
        log,
        events.PROCESSING_FAILED,
        level="error",
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        error=f"{type(ex).__name__}: {ex}",
        correlation_id=None,
    )


def _correlation_id(record) -> str | None:
    try:
        return classify(record).correlation_id
    except InvalidEvent:
        return None


def _wait_ms(state: RetryState, now_ns: int) -> int:
    """getmany's wait: at most GETMANY_TIMEOUT_MS, shortened to the next resume deadline."""
    next_due = state.next_due_ns()
    if next_due is None:
        return GETMANY_TIMEOUT_MS
    return max(0, min(GETMANY_TIMEOUT_MS, (next_due - now_ns + 999_999) // 1_000_000))


def announce_start(log, config: Config) -> None:
    """worker_started; WARNING when a fault is armed (D-085)."""
    events.emit(
        log,
        events.WORKER_STARTED,
        level="warning" if config.fault_armed else "info",
        consumer_group=config.consumer_group,
        fault_point=config.fault_point,
        correlation_id=None,
    )


class _AssignmentLogger(ConsumerRebalanceListener):
    """partitions_assigned: the tests' readiness signal (D-090). A revoke forgets the revoked
    partitions' attempt counts and pending resumes (D-108)."""

    def __init__(self, state: RetryState | None = None) -> None:
        self._state = state

    def on_partitions_revoked(self, revoked):
        if self._state is not None:
            self._state.revoked(revoked)

    def on_partitions_assigned(self, assigned):
        events.emit(
            structlog.get_logger(),
            events.PARTITIONS_ASSIGNED,
            partitions=sorted(tp.partition for tp in assigned),
            correlation_id=None,
        )


async def run(config: Config, stop: asyncio.Event, stop_reason: dict) -> int:
    """Startup order (D-113): the role check, then /metrics, then the consumer. A worker that
    refuses its role opens no port; a port it can't bind is invalid configuration (exit 2). The
    metrics server is closed before worker_stopped, so a stop still exits 0."""
    log = structlog.get_logger()
    try:
        conn = await db.connect(**config.db_params())
    except db.WorkerRoleError as ex:
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return EXIT_CONFIG
    except (OSError, asyncio.TimeoutError, asyncpg.PostgresError) as ex:
        events.emit(log, events.WORKER_STOPPED, level="error", reason=f"db_unavailable: {ex}", correlation_id=None)
        return EXIT_PROCESSING_ERROR

    try:
        server = metrics.start(config.metrics_host, config.metrics_port)
    except metrics.MetricsBindError as ex:
        await conn.close()
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return EXIT_CONFIG
    events.emit(log, events.METRICS_LISTENING, host=server.host, port=server.port, correlation_id=None)

    try:
        code, reason = await _consume(config, stop, conn)
    finally:
        await asyncio.to_thread(server.close)
    events.emit(
        log,
        events.WORKER_STOPPED,
        level="error" if code else "info",
        reason=reason or stop_reason.get("why", "stopped"),
        correlation_id=None,
    )
    return code


async def _consume(config: Config, stop: asyncio.Event, conn: asyncpg.Connection) -> tuple[int, str | None]:
    """Runs the consumer until a stop request or a processing error; closes the DB connection.
    Returns the exit code and the reason (None for a requested stop)."""
    log = structlog.get_logger()
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=config.session_timeout_ms,
        heartbeat_interval_ms=config.heartbeat_interval_ms,
    )
    try:
        await consumer.start()
    except KafkaError as ex:
        await conn.close()
        return EXIT_PROCESSING_ERROR, f"broker_unavailable: {ex}"

    code, reason = EXIT_STOPPED, None
    state = RetryState()
    rng = random.Random()
    try:
        consumer.subscribe([config.topic], listener=_AssignmentLogger(state))
        announce_start(log, config)
        while not stop.is_set() and reason is None:
            now = time.monotonic_ns()
            due = state.due(now)
            if due:
                assigned = consumer.assignment()
                resumable = [tp for tp in due if tp in assigned]
                if resumable:
                    consumer.resume(*resumable)
            batches = await consumer.getmany(timeout_ms=_wait_ms(state, now))
            for tp, records in batches.items():
                for record in records:
                    if stop.is_set():
                        break
                    result = await _step(record, tp, conn=conn, consumer=consumer, config=config, state=state, rng=rng)
                    if result is Step.FAILED:
                        code, reason = EXIT_PROCESSING_ERROR, "processing_failed"
                        break
                    if result is Step.RETRY:
                        # The partition's other fetched records are dropped; they come again later.
                        break
                if reason is not None or stop.is_set():
                    break
    finally:
        # With auto-commit off, stop() commits nothing; it leaves the group so others take over fast.
        await consumer.stop()
        await conn.close()
    return code, reason
