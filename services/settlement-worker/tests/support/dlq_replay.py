"""Replaying a dead letter (D-111, D-125): the reference implementation for the operator replay tool.

D-111's replay rule: a dead-lettered event never has a dedup row, so a replay is re-producing the
DLQ record's key, its value (null again when dlq.original_value_null is set; the empty value stood in
only because Kafka clients refuse a record with neither key nor value) and its original headers
(every dlq.* header removed) to ledger.payments.v1. The worker then processes it like any other
delivery, and its dedup row absorbs a second copy.

Delivery to the DLQ is at-least-once, so whoever replays dedups by (dlq.original_topic,
dlq.original_partition, dlq.original_offset). This helper deliberately doesn't: T9's replay case
sends the same record twice to show the worker's dedup absorbs the second copy.
"""

import asyncio

from aiokafka import AIOKafkaProducer

from support.stack import TOPIC

DLQ_PREFIX = "dlq."


def replay_record(letter) -> tuple[bytes | None, bytes | None, list[tuple[str, bytes]]]:
    """(key, value, headers) to re-produce, from a DLQ record (failure_kit.DlqRecord). Pure."""
    restore_null = any(k == "dlq.original_value_null" and v == b"true" for k, v in letter.headers)
    value = None if restore_null else letter.value
    headers = [(k, v) for k, v in letter.headers if not k.startswith(DLQ_PREFIX)]
    return letter.key, value, headers


def replay(bootstrap_servers: str, letter, *, times: int = 1, topic: str = TOPIC) -> list[tuple[int, int]]:
    """Re-produces the DLQ record to the topic `times` times (acks=all, idempotent producer);
    returns each copy's (partition, offset)."""
    return asyncio.run(_replay(bootstrap_servers, letter, times, topic))


async def _replay(bootstrap_servers: str, letter, times: int, topic: str) -> list[tuple[int, int]]:
    key, value, headers = replay_record(letter)
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, acks="all", enable_idempotence=True)
    await producer.start()
    try:
        sent = []
        for _ in range(times):
            metadata = await producer.send_and_wait(topic, value, key=key, headers=headers)
            sent.append((metadata.partition, metadata.offset))
        return sent
    finally:
        await producer.stop()
