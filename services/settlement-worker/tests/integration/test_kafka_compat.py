"""aiokafka against apache/kafka:4.3.1 (D-084).

Kafka 4.0 removed old protocol API versions (KIP-896), so compatibility is tested, not assumed:
create a topic, produce, consume in a group, commit offset+1, and read the committed offset back
from a fresh consumer in the same group, which resumes right after it. The records all go to
partition 0 so there is a next record to resume on.
"""

import asyncio
import time
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

DEADLINE_SECONDS = 30


async def _records_from(consumer: AIOKafkaConsumer, tp: TopicPartition, count: int) -> list:
    records = []
    deadline = time.monotonic() + DEADLINE_SECONDS
    while len(records) < count:
        assert time.monotonic() < deadline, f"read {len(records)} of {count} records from {tp}"
        batch = await consumer.getmany(tp, timeout_ms=500)
        records.extend(batch.get(tp, []))
    return records


def test_aiokafka_produces_consumes_and_commits_against_the_broker(kafka):
    topic = f"t6-compat-{uuid.uuid4().hex}"
    group = f"t6-group-{uuid.uuid4().hex}"
    tp = TopicPartition(topic, 0)

    async def scenario():
        admin = AIOKafkaAdminClient(bootstrap_servers=kafka.bootstrap_servers)
        await admin.start()
        try:
            await admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        finally:
            await admin.close()

        producer = AIOKafkaProducer(bootstrap_servers=kafka.bootstrap_servers, acks="all", enable_idempotence=True)
        await producer.start()
        try:
            sent = [
                await producer.send_and_wait(topic, key=f"key-{i}".encode(), value=f"value-{i}".encode(), partition=0)
                for i in range(3)
            ]
        finally:
            await producer.stop()
        assert [m.partition for m in sent] == [0, 0, 0]

        first_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka.bootstrap_servers,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await first_consumer.start()
        try:
            assert first_consumer.partitions_for_topic(topic) == {0, 1, 2}
            records = await _records_from(first_consumer, tp, 3)
            assert [r.key for r in records] == [b"key-0", b"key-1", b"key-2"]
            first = records[0]
            await first_consumer.commit({tp: first.offset + 1})
        finally:
            await first_consumer.stop()

        second_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=kafka.bootstrap_servers,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await second_consumer.start()
        try:
            assert await second_consumer.committed(tp) == first.offset + 1
            resumed = await _records_from(second_consumer, tp, 1)
            assert resumed[0].offset == first.offset + 1
            assert resumed[0].key == b"key-1"
        finally:
            await second_consumer.stop()

    asyncio.run(scenario())
