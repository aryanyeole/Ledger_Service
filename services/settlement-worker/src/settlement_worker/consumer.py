"""The consume loop: offsets after the DB commit, retry in place, reconnect, pending offset commits,
fault points (D-014, D-081, D-085, D-107, D-108, D-109, D-125, D-129).

Records are processed one at a time, in partition order. For each record:
  0. if the DB connection was lost, reconnect first (the role check runs again; a refusal exits 2);
  1. envelope.parse (D-102) and, for payment.authorized, the record key must equal
     data.payment_id. Both happen before the DB or the broker is touched;
  2. the worker's own payment.settled / payment.failed: commit offset+1, no DB write;
  3. payment.authorized: processor.decide on every delivery, before the dedup check (its
     idempotency stays visible to the crash test) -> settlement.handle, which commits its own
     transaction (D-082) -> commit offset+1. Committing the offset only after the DB commit means
     a crash between them redelivers the record, and the dedup row absorbs it (invariant 6).
A failure is classified (failures.classify, D-107). Transient (indefinitely) and Unclassified (up
to WORKER_MAX_ATTEMPTS) are retried in place (D-108): a redelivery to self that seeks the
partition back to the record, pauses it, drops the rest of its fetched records and resumes it after
the backoff, while other partitions keep moving. A lost DB connection is terminated first, and
each re-run reconnects (D-109); a timeout or loss during COMMIT leaves the outcome unknown, and the
re-run's dedup decides. Poison, Fatal and an exhausted Unclassified -> processing_failed and exit 1
without committing (the DLQ replaces it in T7). A failed offset commit is not a record failure (the
effect committed): never classified, never re-run; the offset stays pending, every assigned
partition is paused, and the worker keeps polling while it recommits on the D-108 schedule.
Records fetched but not processed stay uncommitted and are redelivered. A stop request is honoured
between records, during a backoff and while a commit is pending (within one getmany wait), never
between the DB commit and the offset commit.
Faults (tests only), for FAULT_PAYMENT_ID only: at AFTER_PROCESSOR_BEFORE_DB / AFTER_DB_BEFORE_OFFSET
log fault_injected, flush stdout and os._exit(86): no finally blocks, no LeaveGroup, no offset
commit, exactly like a crash. RAISE_BEFORE_COMMIT logs fault_injected and raises a RuntimeError
inside the settle transaction on every attempt (D-125). FAIL_OFFSET_COMMIT logs fault_injected and
fails the first two offset commits of the payment's record (D-129).
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
from settlement_worker.config import (
    AFTER_DB_BEFORE_OFFSET,
    AFTER_PROCESSOR_BEFORE_DB,
    FAIL_OFFSET_COMMIT,
    RAISE_BEFORE_COMMIT,
    Config,
)
from settlement_worker.envelope import AuthorizedEvent, InvalidEvent, OwnResultEvent
from settlement_worker.failures import ErrorClass
from settlement_worker.retry import PendingCommits, RetryState, backoff_ms

EXIT_STOPPED = 0
EXIT_PROCESSING_ERROR = 1
EXIT_CONFIG = 2
EXIT_FAULT = 86

GETMANY_TIMEOUT_MS = 500

# FAIL_OFFSET_COMMIT fails this many commits of the payment's record, then commits are real (D-129).
FAILED_COMMITS = 2


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


class Commits:
    """Offset commits and the pending ones (D-109): commit(), and on failure failed(), which
    schedules a recommit on the D-108 backoff; recommit_due() runs the due ones. Carries the
    FAIL_OFFSET_COMMIT target, so a recommit goes through the same fault check as the first try."""

    def __init__(self, consumer, config: Config, rng: random.Random) -> None:
        self._consumer = consumer
        self._config = config
        self._rng = rng
        self.pending = PendingCommits()
        self._fault_target: tuple[TopicPartition, int] | None = None
        self._fault_left = 0
        self._fault_payment: AuthorizedEvent | None = None

    def arm(self, tp: TopicPartition, offset: int, event: AuthorizedEvent) -> None:
        """FAIL_OFFSET_COMMIT: this record's commits are the ones that fail, once armed."""
        if self._config.fault_point != FAIL_OFFSET_COMMIT or event.payment_id != self._config.fault_payment_id:
            return
        if self._fault_target != (tp, offset):
            self._fault_target, self._fault_left = (tp, offset), FAILED_COMMITS
        self._fault_payment = event

    async def commit(self, tp: TopicPartition, offset: int) -> None:
        if self._fault_target == (tp, offset) and self._fault_left > 0:
            self._fault_left -= 1
            event = self._fault_payment
            events.emit(
                structlog.get_logger(),
                events.FAULT_INJECTED,
                point=FAIL_OFFSET_COMMIT,
                payment_id=str(event.payment_id),
                correlation_id=event.correlation_id,
            )
            raise OffsetCommitFailed(f"fault {FAIL_OFFSET_COMMIT} for {tp.topic}[{tp.partition}]@{offset}")
        try:
            await self._consumer.commit({tp: offset + 1})
        except KafkaError as ex:
            raise OffsetCommitFailed(f"offset commit failed for {tp.topic}[{tp.partition}]@{offset}: {ex!r}") from ex
        self.pending.committed(tp, offset)

    def failed(self, tp: TopicPartition, offset: int, ex: BaseException) -> None:
        attempt = self.pending.failed(tp, offset)
        delay = backoff_ms(
            attempt,
            base_ms=self._config.retry_base_ms,
            max_ms=self._config.retry_max_ms,
            jitter=self._config.retry_jitter,
            rng=self._rng,
        )
        self.pending.schedule(tp, time.monotonic_ns() + delay * 1_000_000)
        events.emit(
            structlog.get_logger(),
            events.OFFSET_COMMIT_FAILED,
            level="warning",
            topic=tp.topic,
            partition=tp.partition,
            offset=self.pending.offset(tp),
            attempt=attempt,
            delay_ms=delay,
            error=f"{type(ex).__name__}: {ex}",
            correlation_id=None,
        )

    async def recommit_due(self, now_ns: int) -> None:
        assigned = self._consumer.assignment()
        for tp, offset in self.pending.due(now_ns):
            if tp not in assigned:
                self.pending.revoked([tp])  # the new owner's redelivery goes through dedup
                continue
            try:
                await self.commit(tp, offset)
            except OffsetCommitFailed as ex:
                self.failed(tp, offset, ex)


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


async def process_record(record, *, session: db.DbSession, consumer, config: Config, commits: Commits) -> None:
    event = classify(record)
    tp = TopicPartition(record.topic, record.partition)
    if isinstance(event, OwnResultEvent):
        await commits.commit(tp, record.offset)
        return
    decision = processor.decide(event.payment_id, event.amount, correlation_id=event.correlation_id)
    _fault(config, AFTER_PROCESSOR_BEFORE_DB, event)
    try:
        await settlement.handle(
            session.conn,
            event,
            decision,
            consumer_group=config.consumer_group,
            topic=config.topic,
            lock_timeout=config.lock_timeout,
            before_commit=_raise_before_commit(config, event),
        )
    except Exception as ex:  # noqa: BLE001 - only DB call sites wrap their timeouts (D-109)
        failure = db.as_db_failure(ex)
        if failure is ex:
            raise
        raise failure from ex
    _fault(config, AFTER_DB_BEFORE_OFFSET, event)
    commits.arm(tp, record.offset, event)
    await commits.commit(tp, record.offset)


class Step(enum.Enum):
    OK = "ok"
    RETRY = "retry"
    FAILED = "failed"
    REFUSED = "refused"


async def _step(
    record, tp: TopicPartition, *, session: db.DbSession, consumer, config: Config, state: RetryState, commits, rng
) -> Step:
    """One record: reconnect if the connection was lost, process it; on failure, classify it and
    either schedule a retry in place or fail."""
    log = structlog.get_logger()
    try:
        if session.broken:
            await session.reconnect()
        await process_record(record, session=session, consumer=consumer, config=config, commits=commits)
    except db.WorkerRoleError as ex:
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return Step.REFUSED
    except OffsetCommitFailed as ex:
        # Not a record failure: the effect committed, so it is never classified or re-run (D-107).
        # The offset stays pending; the loop pauses and recommits it (D-109).
        commits.failed(tp, record.offset, ex)
        state.succeeded(tp, record.offset)
        return Step.OK
    except Exception as ex:  # noqa: BLE001 - classified below (D-107)
        # settlement.handle's transaction context has exited by now (rolled back, or its outcome
        # unknown if the COMMIT itself failed), so no row or account lock is held during the backoff
        # that may follow (D-108). A lost connection is terminated before anything else (D-109).
        failure = failures.classify(ex, connection_closed=session.closed())
        if failure.connection:
            session.drop()
        attempt = state.failed(tp, record.offset)
        exhausted = failure.error_class is ErrorClass.UNCLASSIFIED and attempt >= config.max_attempts
        if failure.error_class in (ErrorClass.POISON, ErrorClass.FATAL) or exhausted:
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


def _wait_ms(now_ns: int, *deadlines: int | None) -> int:
    """getmany's wait: at most GETMANY_TIMEOUT_MS, shortened to the next deadline (a retry's resume
    or a pending recommit)."""
    upcoming = [d for d in deadlines if d is not None]
    if not upcoming:
        return GETMANY_TIMEOUT_MS
    return max(0, min(GETMANY_TIMEOUT_MS, (min(upcoming) - now_ns + 999_999) // 1_000_000))


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
    partitions' attempt counts, pending resumes (D-108) and pending commits (D-109)."""

    def __init__(self, state: RetryState | None = None, pending: PendingCommits | None = None) -> None:
        self._state = state
        self._pending = pending

    def on_partitions_revoked(self, revoked):
        if self._state is not None:
            self._state.revoked(revoked)
        if self._pending is not None:
            self._pending.revoked(revoked)

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
    metrics server is closed before worker_stopped, so a stop still exits 0. An unreachable DB at
    startup still exits 1 (D-109)."""
    log = structlog.get_logger()
    session = db.DbSession(config)
    try:
        await session.open()
    except db.WorkerRoleError as ex:
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return EXIT_CONFIG
    except (OSError, asyncio.TimeoutError, asyncpg.PostgresError) as ex:
        reason = f"db_unavailable: {type(ex).__name__}: {ex}"
        events.emit(log, events.WORKER_STOPPED, level="error", reason=reason, correlation_id=None)
        return EXIT_PROCESSING_ERROR

    try:
        server = metrics.start(config.metrics_host, config.metrics_port)
    except metrics.MetricsBindError as ex:
        await session.close()
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return EXIT_CONFIG
    events.emit(log, events.METRICS_LISTENING, host=server.host, port=server.port, correlation_id=None)

    try:
        code, reason = await _consume(config, stop, session)
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


async def _consume(config: Config, stop: asyncio.Event, session: db.DbSession) -> tuple[int, str | None]:
    """Runs the consumer until a stop request, a processing error or a role refusal; closes the DB
    connection (bounded). Returns the exit code and the reason (None for a requested stop)."""
    log = structlog.get_logger()
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=config.session_timeout_ms,
        heartbeat_interval_ms=config.heartbeat_interval_ms,
        max_poll_interval_ms=config.max_poll_interval_ms,
    )
    try:
        await consumer.start()
    except KafkaError as ex:
        await session.close()
        return EXIT_PROCESSING_ERROR, f"broker_unavailable: {ex}"

    code, reason = EXIT_STOPPED, None
    state = RetryState()
    rng = random.Random()
    commits = Commits(consumer, config, rng)
    paused_for_commit = False
    try:
        consumer.subscribe([config.topic], listener=_AssignmentLogger(state, commits.pending))
        announce_start(log, config)
        while not stop.is_set() and reason is None:
            now = time.monotonic_ns()
            if commits.pending:
                await commits.recommit_due(now)
            if commits.pending:
                # A commit is pending (D-109): no new records until it lands, but keep polling,
                # paused, so the member stays in the group (getmany is what max.poll.interval counts).
                assigned = consumer.assignment()
                if assigned:
                    consumer.pause(*assigned)
                paused_for_commit = True
            else:
                if paused_for_commit:
                    in_backoff = state.scheduled()
                    resumable = [tp for tp in consumer.assignment() if tp not in in_backoff]
                    if resumable:
                        consumer.resume(*resumable)
                    paused_for_commit = False
                due = state.due(now)
                if due:
                    assigned = consumer.assignment()
                    resumable = [tp for tp in due if tp in assigned]
                    if resumable:
                        consumer.resume(*resumable)
            wait = _wait_ms(now, commits.pending.next_due_ns(), None if commits.pending else state.next_due_ns())
            batches = await consumer.getmany(timeout_ms=wait)
            for tp, records in batches.items():
                for record in records:
                    if stop.is_set():
                        break
                    result = await _step(
                        record, tp, session=session, consumer=consumer, config=config, state=state, commits=commits,
                        rng=rng,
                    )
                    if result is Step.FAILED:
                        code, reason = EXIT_PROCESSING_ERROR, "processing_failed"
                        break
                    if result is Step.REFUSED:
                        code, reason = EXIT_CONFIG, "config_invalid"
                        break
                    if result is Step.RETRY:
                        # The partition's other fetched records are dropped; they come again later.
                        break
                if reason is not None or stop.is_set():
                    break
    finally:
        # With auto-commit off, stop() commits nothing; it leaves the group so others take over fast.
        await consumer.stop()
        await session.close()
    return code, reason
