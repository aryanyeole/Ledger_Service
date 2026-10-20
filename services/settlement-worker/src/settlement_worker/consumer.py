"""The consume loop: offsets after the DB commit, fault points (D-014, D-081, D-085).

Records are processed one at a time, in partition order. For each record:
  1. envelope.parse (D-102) and, for payment.authorized, the record key must equal
     data.payment_id. Both happen before the DB or the broker is touched;
  2. the worker's own payment.settled / payment.failed: commit offset+1, no DB write;
  3. payment.authorized: processor.decide on every delivery, before the dedup check (its
     idempotency stays visible to the crash test) -> settlement.handle, which commits its own
     transaction (D-082) -> commit offset+1. Committing the offset only after the DB commit means
     a crash between them redelivers the record, and the dedup row absorbs it (invariant 6).
Any invalid event or error -> processing_failed and exit 1 without committing (retry/DLQ: M4);
records fetched but not processed stay uncommitted and are redelivered. A stop request is only
honoured between records, never between the DB commit and the offset commit.
Faults (tests only): at a point, for FAULT_PAYMENT_ID only, log fault_injected, flush stdout and
os._exit(86): no finally blocks, no LeaveGroup, no offset commit, exactly like a crash.
"""

import asyncio
import os
import sys
import uuid

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import KafkaError

from settlement_worker import db, envelope, events, processor, settlement
from settlement_worker.config import AFTER_DB_BEFORE_OFFSET, AFTER_PROCESSOR_BEFORE_DB, Config
from settlement_worker.envelope import AuthorizedEvent, InvalidEvent, OwnResultEvent

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


async def process_record(record, *, conn: asyncpg.Connection, consumer, config: Config) -> None:
    event = classify(record)
    tp = TopicPartition(record.topic, record.partition)
    if isinstance(event, OwnResultEvent):
        await consumer.commit({tp: record.offset + 1})
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
    )
    _fault(config, AFTER_DB_BEFORE_OFFSET, event)
    await consumer.commit({tp: record.offset + 1})


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
    """partitions_assigned: the tests' readiness signal (D-090)."""

    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        events.emit(
            structlog.get_logger(),
            events.PARTITIONS_ASSIGNED,
            partitions=sorted(tp.partition for tp in assigned),
            correlation_id=None,
        )


async def run(config: Config, stop: asyncio.Event, stop_reason: dict) -> int:
    log = structlog.get_logger()
    try:
        conn = await db.connect(**config.db_params())
    except db.WorkerRoleError as ex:
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return EXIT_CONFIG
    except (OSError, asyncio.TimeoutError, asyncpg.PostgresError) as ex:
        events.emit(log, events.WORKER_STOPPED, level="error", reason=f"db_unavailable: {ex}", correlation_id=None)
        return EXIT_PROCESSING_ERROR

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
        events.emit(log, events.WORKER_STOPPED, level="error", reason=f"broker_unavailable: {ex}", correlation_id=None)
        return EXIT_PROCESSING_ERROR

    code, reason = EXIT_STOPPED, None
    try:
        consumer.subscribe([config.topic], listener=_AssignmentLogger())
        announce_start(log, config)
        while not stop.is_set() and reason is None:
            batches = await consumer.getmany(timeout_ms=GETMANY_TIMEOUT_MS)
            for records in batches.values():
                for record in records:
                    if stop.is_set():
                        break
                    try:
                        await process_record(record, conn=conn, consumer=consumer, config=config)
                    except Exception as ex:  # noqa: BLE001 - every failure stops the worker uncommitted
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
                        code, reason = EXIT_PROCESSING_ERROR, "processing_failed"
                        break
                if reason is not None or stop.is_set():
                    break
    finally:
        # With auto-commit off, stop() commits nothing; it leaves the group so others take over fast.
        await consumer.stop()
        await conn.close()
    events.emit(
        log,
        events.WORKER_STOPPED,
        level="error" if code else "info",
        reason=reason or stop_reason.get("why", "stopped"),
        correlation_id=None,
    )
    return code
