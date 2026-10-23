"""Retry in place: the backoff schedule and the per-record attempt counts (D-108). Pure: no Kafka.

delay(attempt) = min(cap, base * 2^(attempt - 1)) milliseconds; with jitter, a uniform integer in
[0, delay]. Integers throughout (D-093). Attempts are counted per (topic, partition, offset) and
forgotten on success and on revoke; a revoke also cancels the partition's pending resume.
Deadlines are time.monotonic_ns() values.
"""

import random
from collections.abc import Iterable

from aiokafka import TopicPartition


def backoff_ms(attempt: int, *, base_ms: int, max_ms: int, jitter: bool, rng: random.Random) -> int:
    if attempt < 1:
        raise ValueError(f"attempt must be at least 1, was {attempt}")
    delay = min(max_ms, base_ms * 2 ** (attempt - 1))
    return rng.randint(0, delay) if jitter else delay


class RetryState:
    def __init__(self) -> None:
        self._attempts: dict[tuple[str, int, int], int] = {}
        self._resume_at: dict[TopicPartition, int] = {}

    def failed(self, tp: TopicPartition, offset: int) -> int:
        """Counts a failed attempt at this record; returns how many it has had."""
        key = (tp.topic, tp.partition, offset)
        self._attempts[key] = self._attempts.get(key, 0) + 1
        return self._attempts[key]

    def succeeded(self, tp: TopicPartition, offset: int) -> None:
        self._attempts.pop((tp.topic, tp.partition, offset), None)

    def attempts(self, tp: TopicPartition, offset: int) -> int:
        return self._attempts.get((tp.topic, tp.partition, offset), 0)

    def revoked(self, tps: Iterable[TopicPartition]) -> None:
        gone = set(tps)
        self._attempts = {k: v for k, v in self._attempts.items() if TopicPartition(k[0], k[1]) not in gone}
        for tp in gone:
            self._resume_at.pop(tp, None)

    def schedule(self, tp: TopicPartition, resume_at_ns: int) -> None:
        self._resume_at[tp] = resume_at_ns

    def due(self, now_ns: int) -> list[TopicPartition]:
        """The partitions whose backoff has ended, removed from the schedule."""
        ready = [tp for tp, at in self._resume_at.items() if at <= now_ns]
        for tp in ready:
            del self._resume_at[tp]
        return ready

    def next_due_ns(self) -> int | None:
        return min(self._resume_at.values(), default=None)
