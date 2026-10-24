"""The worker's dead-letter producer (D-111).

aiokafka's producer with acks=all and the idempotent producer, no serializers (the record's own
bytes go out unchanged). One deadline, WORKER_DLQ_TIMEOUT_MS, covers send() (topic metadata, then
the append) and the ack; anything that fails or runs past it is DlqUnavailable, a transient
failure: the record is re-run on the D-108 schedule and its offset stays uncommitted. A missing
topic (auto-create is off) fails in send()'s metadata wait, before anything is buffered. An ack
that times out after the append may still be delivered: dead letters are at-least-once.
Started after the metrics server and before the consumer; stopped with the same bound.
Never called inside a DB transaction (CLAUDE.md invariant 5, D-111).
"""

import asyncio
import time

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from settlement_worker.dead_letter import DeadLetter


class DlqUnavailable(Exception):
    """The dead letter couldn't be produced and acknowledged in time (D-107: transient)."""


class DlqProducer:
    def __init__(self, *, bootstrap_servers: str, topic: str, timeout_ms: int) -> None:
        self.topic = topic
        self._timeout_ns = timeout_ms * 1_000_000
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id="settlement-worker-dlq",
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=timeout_ms,
            linger_ms=0,
        )

    async def start(self) -> None:
        """Raises KafkaError if the broker can't be reached (the caller exits 1)."""
        await self._producer.start()

    async def stop(self) -> None:
        try:
            await asyncio.wait_for(self._producer.stop(), self._timeout_ns / 1_000_000_000)
        except (KafkaError, TimeoutError):
            pass  # a producer that can't flush within the bound is abandoned at exit

    async def produce(self, letter: DeadLetter) -> tuple[int, int]:
        """Produces and awaits the ack; returns (partition, offset)."""
        deadline = time.monotonic_ns() + self._timeout_ns

        def left() -> float:
            return max(0, deadline - time.monotonic_ns()) / 1_000_000_000

        try:
            future = await asyncio.wait_for(
                self._producer.send(self.topic, value=letter.value, key=letter.key, headers=letter.headers), left()
            )
            metadata = await asyncio.wait_for(future, left())
        except (KafkaError, TimeoutError) as ex:
            raise DlqUnavailable(f"dead letter to {self.topic} not acknowledged: {type(ex).__name__}: {ex}") from ex
        return metadata.partition, metadata.offset
