"""The failure_modes stack (D-104, D-123): a fresh migrated Postgres, its own Kafka and the ledger-api
jar in server mode with the relay off, one per test module.

Failure tests inject poison, faults and held locks, so they never share the e2e topic, where every
fresh group reads from the earliest offset. With the relay off, the harness plays the topic owner
(both topics, D-080, D-110) and publishes each payment.authorized itself, from its outbox row as
the relay would build it (D-097, D-103), keyed by payment_id with the real partitioner. Every case
uses its own consumer group.
"""

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from support import outbox_events, stack
from support.api import ApiClient


@dataclass(frozen=True)
class FailureModesStack:
    database: stack.Database
    kafka: stack.Kafka
    api: ApiClient
    app_db: stack.AppDb
    worker_db: stack.WorkerDb
    log_dir: Path

    def publish(self, *payment_ids) -> None:
        self.produce(self.read_events(*payment_ids))

    def read_events(self, *payment_ids) -> list[tuple]:
        """(payment_id, record value) from the outbox, read before anything that takes the DB away."""
        return asyncio.run(_read_events(self, payment_ids))

    def produce(self, values: list[tuple], headers: list[tuple[str, bytes]] | None = None) -> None:
        """(payment_id, value) pairs, keyed by payment_id; headers, if given, go on every record."""
        asyncio.run(_produce(self.kafka.bootstrap_servers, values, headers))

    def position(self, payment_id) -> tuple[int, int]:
        """(partition, offset) of the payment's payment.authorized record, read from the topic
        (D-105). From offset 0 and without a group: a worker in the case's group may already have
        committed past the record, and a group that has never committed has no coordinator yet."""
        return asyncio.run(_position(self.kafka.bootstrap_servers, payment_id))


async def _position(bootstrap: str, payment_id) -> tuple[int, int]:
    partitions = sorted(await stack.topic_partitions(bootstrap, stack.TOPIC) or [])
    reader = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    await reader.start()
    try:
        tps = [TopicPartition(stack.TOPIC, p) for p in partitions]
        reader.assign(tps)
        ends = await reader.end_offsets(tps)
        for tp in tps:
            reader.seek(tp, 0)
        remaining = {tp for tp in tps if ends[tp] > 0}
        deadline = time.monotonic() + 30
        while remaining:
            assert time.monotonic() < deadline, f"could not read {stack.TOPIC} up to {ends}"
            for tp, records in (await reader.getmany(timeout_ms=500)).items():
                for record in records:
                    if record.offset < ends[tp] and record.key == str(payment_id).encode():
                        return tp.partition, record.offset
            remaining = {tp for tp in remaining if await reader.position(tp) < ends[tp]}
        seen = {tp.partition: await reader.position(tp) for tp in tps}
    finally:
        await reader.stop()
    raise AssertionError(f"no record for {payment_id} on {stack.TOPIC}: ends {ends}, read up to {seen}")


async def _read_events(fm: FailureModesStack, payment_ids) -> list[tuple]:
    conn = await fm.app_db.connect()  # the outbox is readable by ledger_api_app only (D-077)
    try:
        return [(pid, await outbox_events.authorized_event(conn, pid)) for pid in payment_ids]
    finally:
        await conn.close()


async def _produce(bootstrap: str, values: list[tuple], headers: list[tuple[str, bytes]] | None = None) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=True)
    await producer.start()
    try:
        for pid, value in values:
            await producer.send_and_wait(stack.TOPIC, value, key=str(pid).encode(), headers=headers)
    finally:
        await producer.stop()


@pytest.fixture(scope="module")
def fm(tmp_path_factory) -> FailureModesStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("failure-modes")
    with stack.postgres() as database, stack.kafka() as broker:
        stack.migrate(database, jar)
        asyncio.run(
            stack.create_topics(
                broker.bootstrap_servers,
                {stack.TOPIC: (3, {}), stack.DLQ_TOPIC: (1, {"retention.ms": "-1"})},
            )
        )
        with stack.ledger_api(database, jar, log_dir) as api:
            yield FailureModesStack(database, broker, api, stack.AppDb(database), stack.WorkerDb(database), log_dir)
