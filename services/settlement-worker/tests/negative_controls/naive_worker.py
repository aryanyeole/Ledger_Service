"""D-017 / D-086 negative control: "effect, then mark", the separate-transaction dedup bug.

TEST SOURCE ONLY. It deliberately violates invariant 6 and lives only in tests/negative_controls/;
src/settlement_worker never imports it (tests/unit/test_negative_control_isolation.py). It reuses
main's pure helpers (config parsing, logging, classify, processor.call, the leg builder, money,
events) and main's role check, and keeps invariants 1-5: each posting balances (the deferred
trigger checks it), entries are only inserted, money is Decimal, accounts are locked in ascending
id order, and nothing but Postgres is awaited inside a transaction.

Per payment.authorized record:
  1. pre-check payments.status: not AUTHORIZED -> commit offset+1;
  2. the ballast (NAIVE_SKIP_PAYMENT_ID) -> commit offset+1, so its escrow covers the duplicates;
  3. processor.call, as attempt 1 and without the consumer's deadline (D-112);
  4. transaction 1: lock the legs' accounts, post PAYMENT_SETTLE / PAYMENT_RELEASE, append the
     result outbox row. Commit;
  5. fault AFTER_EFFECT_BEFORE_MARK for NAIVE_FAULT_PAYMENT_ID: log, flush, os._exit(86);
  6. transaction 2: the dedup row and the status transition. Commit;
  7. commit offset+1.
A crash at step 5 leaves the effect committed, but no dedup row and the status still AUTHORIZED, so
the redelivered record passes the pre-check and posts again. That is the bug the test demonstrates.

Launched by path: python tests/negative_controls/naive_worker.py (tests/ is not a package).
"""

import asyncio
import json
import os
import sys
import threading
import time
import uuid
from datetime import timedelta

import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import KafkaError

from settlement_worker import consumer, db, events, logging_setup, processor, settlement
from settlement_worker.config import Config, ConfigError
from settlement_worker.envelope import CANONICAL_UUID, FAILED, SETTLED, AuthorizedEvent, OwnResultEvent
from settlement_worker.money import to_api_string
from settlement_worker.processor import Decision

FAULT_POINT = "AFTER_EFFECT_BEFORE_MARK"
FAULT_ENV = "NAIVE_FAULT_PAYMENT_ID"
SKIP_ENV = "NAIVE_SKIP_PAYMENT_ID"


def _payment_id(name: str) -> uuid.UUID | None:
    raw = os.environ.get(name, "")
    if raw == "":
        return None
    if not CANONICAL_UUID.fullmatch(raw):
        raise ConfigError(f"{name} must be a canonical UUID")
    return uuid.UUID(raw)


def _lock_timeout(value: timedelta) -> str:
    return f"{int(value / timedelta(milliseconds=1))}ms"


class _AssignmentLogger(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        events.emit(
            structlog.get_logger(),
            events.PARTITIONS_ASSIGNED,
            partitions=sorted(tp.partition for tp in assigned),
            correlation_id=None,
        )


async def _effect(conn, config: Config, event: AuthorizedEvent, decision: Decision, row) -> tuple[int, int]:
    """Transaction 1: the posting and its result event, without the dedup row or the transition."""
    payment = settlement.PaymentRow(
        customer_account_id=row["customer_account_id"],
        merchant_account_id=row["merchant_account_id"],
        amount=row["amount"],
        fee=row["fee"],
    )
    async with conn.transaction(isolation="read_committed"):
        await conn.execute("SELECT set_config('lock_timeout', $1, true)", _lock_timeout(config.lock_timeout))
        escrow_id = await conn.fetchval("SELECT id FROM accounts WHERE name = $1", settlement.ESCROW_NAME)
        fees_id = await conn.fetchval("SELECT id FROM accounts WHERE name = $1", settlement.PLATFORM_FEES_NAME)
        posting = sorted(settlement.legs(decision, payment, escrow_id, fees_id))
        account_ids = [account for account, _ in posting]
        started = time.perf_counter_ns()
        locked = await conn.fetch(
            "SELECT id FROM accounts WHERE id = ANY($1::bigint[]) ORDER BY id FOR UPDATE", account_ids
        )
        lock_wait_ms = (time.perf_counter_ns() - started) // 1_000_000
        if [r["id"] for r in locked] != account_ids:
            raise RuntimeError(f"accounts missing: {sorted(set(account_ids) - {r['id'] for r in locked})}")
        kind = "PAYMENT_SETTLE" if decision is Decision.APPROVED else "PAYMENT_RELEASE"
        transaction_id = await conn.fetchval(
            "INSERT INTO transactions (kind, reference, correlation_id) VALUES ($1, $2, $3) RETURNING id",
            kind,
            str(event.payment_id),
            event.correlation_id,
        )
        for account, delta in posting:
            await conn.execute(
                "INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)",
                transaction_id,
                account,
                delta,
            )
        for account, delta in posting:
            await conn.execute("UPDATE accounts SET balance = balance + $2 WHERE id = $1", account, delta)
        payload = {
            "payment_id": str(event.payment_id),
            "customer_account_id": payment.customer_account_id,
            "merchant_account_id": payment.merchant_account_id,
            "amount": to_api_string(payment.amount),
            "fee": to_api_string(payment.fee),
            "transaction_id": transaction_id,
        }
        if decision is Decision.APPROVED:
            payload["merchant_amount"] = to_api_string(payment.amount - payment.fee)
        await conn.execute(
            "INSERT INTO outbox (topic, message_key, event_type, payload, headers) "
            "VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)",
            config.topic,
            str(event.payment_id),
            SETTLED if decision is Decision.APPROVED else FAILED,
            json.dumps(payload, separators=(",", ":")),
            json.dumps({"correlation_id": event.correlation_id}, separators=(",", ":")),
        )
    return transaction_id, lock_wait_ms


async def _mark(conn, config: Config, event: AuthorizedEvent, decision: Decision) -> None:
    """Transaction 2: the dedup row and the status transition, committed after the effect."""
    async with conn.transaction(isolation="read_committed"):
        await conn.execute("SELECT set_config('lock_timeout', $1, true)", _lock_timeout(config.lock_timeout))
        await conn.execute(
            "INSERT INTO processed_messages (consumer_group, event_id) VALUES ($1, $2) "
            "ON CONFLICT (consumer_group, event_id) DO NOTHING",
            config.consumer_group,
            event.event_id,
        )
        await conn.execute(
            "UPDATE payments SET status = $2 WHERE id = $1 AND status = 'AUTHORIZED'",
            event.payment_id,
            "SETTLED" if decision is Decision.APPROVED else "FAILED",
        )


async def _process(record, *, conn, kafka, config: Config, fault_id, skip_id) -> None:
    log = structlog.get_logger()
    event = consumer.classify(record)
    tp = TopicPartition(record.topic, record.partition)
    if isinstance(event, OwnResultEvent):
        await kafka.commit({tp: record.offset + 1})
        return
    row = await conn.fetchrow(
        "SELECT status, customer_account_id, merchant_account_id, amount, fee FROM payments WHERE id = $1",
        event.payment_id,
    )
    if row is None:
        raise RuntimeError(f"no payment {event.payment_id}")
    if row["status"] != "AUTHORIZED" or event.payment_id == skip_id:
        await kafka.commit({tp: record.offset + 1})
        return
    decision = await processor.call(
        event.payment_id,
        event.amount,
        1,
        timeout_ms=config.processor_timeout_ms,
        outage=config.processor_outage,
        correlation_id=event.correlation_id,
    )
    transaction_id, lock_wait_ms = await _effect(conn, config, event, decision, row)
    name = events.PAYMENT_SETTLED if decision is Decision.APPROVED else events.PAYMENT_RELEASED
    events.emit(
        log,
        name,
        event_id=str(event.event_id),
        payment_id=str(event.payment_id),
        transaction_id=transaction_id,
        lock_wait_ms=lock_wait_ms,
        correlation_id=event.correlation_id,
    )
    if event.payment_id == fault_id:
        events.emit(
            log,
            events.FAULT_INJECTED,
            point=FAULT_POINT,
            payment_id=str(event.payment_id),
            correlation_id=event.correlation_id,
        )
        sys.stdout.flush()
        os._exit(consumer.EXIT_FAULT)
    await _mark(conn, config, event, decision)
    await kafka.commit({tp: record.offset + 1})


async def _run(config: Config, fault_id, skip_id) -> int:
    log = structlog.get_logger()
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    if config.stop_on_stdin_eof:

        def wait_for_eof():
            sys.stdin.read()
            try:
                loop.call_soon_threadsafe(stop.set)
            except RuntimeError:
                pass

        threading.Thread(target=wait_for_eof, daemon=True).start()
    try:
        conn = await db.connect(**config.db_params())
    except db.WorkerRoleError as ex:
        events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return consumer.EXIT_CONFIG
    kafka = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=config.session_timeout_ms,
        heartbeat_interval_ms=config.heartbeat_interval_ms,
    )
    try:
        await kafka.start()
    except KafkaError as ex:
        await conn.close()
        events.emit(log, events.WORKER_STOPPED, level="error", reason=f"broker_unavailable: {ex}", correlation_id=None)
        return consumer.EXIT_PROCESSING_ERROR
    code, reason = consumer.EXIT_STOPPED, "stdin_eof"
    try:
        kafka.subscribe([config.topic], listener=_AssignmentLogger())
        events.emit(
            log,
            events.WORKER_STARTED,
            level="warning" if fault_id or config.processor_outage else "info",
            consumer_group=config.consumer_group,
            fault_point=FAULT_POINT if fault_id else None,
            processor_outage=config.processor_outage,
            correlation_id=None,
        )
        while not stop.is_set() and code == consumer.EXIT_STOPPED:
            batches = await kafka.getmany(timeout_ms=consumer.GETMANY_TIMEOUT_MS)
            for records in batches.values():
                for record in records:
                    if stop.is_set():
                        break
                    try:
                        await _process(record, conn=conn, kafka=kafka, config=config, fault_id=fault_id, skip_id=skip_id)
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
                        code, reason = consumer.EXIT_PROCESSING_ERROR, "processing_failed"
                        break
                if code != consumer.EXIT_STOPPED or stop.is_set():
                    break
    finally:
        await kafka.stop()
        await conn.close()
    events.emit(log, events.WORKER_STOPPED, level="error" if code else "info", reason=reason, correlation_id=None)
    return code


def main() -> int:
    logging_setup.configure()
    try:
        config = Config.from_env()
        # It honours none of main's fault points; silently ignoring an armed one would look like it
        # ran the scenario (D-125).
        if config.fault_armed:
            raise ConfigError(
                f"the naive worker honours only {FAULT_ENV}, not FAULT_POINT={config.fault_point}"
            )
        fault_id = _payment_id(FAULT_ENV)
        skip_id = _payment_id(SKIP_ENV)
    except ConfigError as ex:
        events.emit(structlog.get_logger(), events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return consumer.EXIT_CONFIG
    return asyncio.run(_run(config, fault_id, skip_id))


if __name__ == "__main__":
    raise SystemExit(main())
