"""Retry in place: the backoff schedule, the per-record attempt counts (D-108) and the pending
offset commits (D-109). Pure: no Kafka.

delay(attempt) = min(cap, base * 2^(attempt - 1)) milliseconds; with jitter, a uniform integer in
[0, delay]. Integers throughout (D-093). Two counts per (topic, partition, offset), both forgotten
on success and on revoke (a revoke also cancels the partition's pending resume), and both in memory,
so a restart starts them again (D-108):
  attempts      every failed run: the backoff, retry_scheduled.attempt, the attempt the processor
                is called with (this run's number = attempts + 1) and dlq.attempts;
  unclassified  Unclassified failures only: exhaustion at WORKER_MAX_ATTEMPTS (D-112).
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
        self._unclassified: dict[tuple[str, int, int], int] = {}
        self._resume_at: dict[TopicPartition, int] = {}

    def failed(self, tp: TopicPartition, offset: int) -> int:
        """Counts a failed attempt at this record; returns how many it has had."""
        key = (tp.topic, tp.partition, offset)
        self._attempts[key] = self._attempts.get(key, 0) + 1
        return self._attempts[key]

    def failed_unclassified(self, tp: TopicPartition, offset: int) -> int:
        """Counts an Unclassified failure of this record (besides failed()); returns how many it has had."""
        key = (tp.topic, tp.partition, offset)
        self._unclassified[key] = self._unclassified.get(key, 0) + 1
        return self._unclassified[key]

    def succeeded(self, tp: TopicPartition, offset: int) -> None:
        self._attempts.pop((tp.topic, tp.partition, offset), None)
        self._unclassified.pop((tp.topic, tp.partition, offset), None)

    def attempts(self, tp: TopicPartition, offset: int) -> int:
        return self._attempts.get((tp.topic, tp.partition, offset), 0)

    def revoked(self, tps: Iterable[TopicPartition]) -> None:
        gone = set(tps)
        self._attempts = {k: v for k, v in self._attempts.items() if TopicPartition(k[0], k[1]) not in gone}
        self._unclassified = {k: v for k, v in self._unclassified.items() if TopicPartition(k[0], k[1]) not in gone}
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

    def scheduled(self) -> set[TopicPartition]:
        """The partitions still in a retry backoff (paused until their resume)."""
        return set(self._resume_at)


class PendingCommits:
    """Offsets whose commit failed after the record's effect committed (D-109). One per partition,
    the highest record offset; a failure at a higher offset replaces a lower one and keeps the
    partition's attempt count. Cleared by any successful commit at or above it, and never
    committed below what the worker already committed (no rewinding). Dropped on revoke."""

    def __init__(self) -> None:
        self._pending: dict[TopicPartition, list[int]] = {}  # tp -> [offset, attempts, due_ns]
        self._committed: dict[TopicPartition, int] = {}

    def __bool__(self) -> bool:
        return bool(self._pending)

    def failed(self, tp: TopicPartition, offset: int) -> int:
        """Counts a failed commit of this record's offset; returns the partition's attempt count."""
        entry = self._pending.get(tp)
        if entry is None:
            entry = self._pending[tp] = [offset, 0, 0]
        entry[0] = max(entry[0], offset)
        entry[1] += 1
        return entry[1]

    def schedule(self, tp: TopicPartition, due_ns: int) -> None:
        self._pending[tp][2] = due_ns

    def offset(self, tp: TopicPartition) -> int | None:
        entry = self._pending.get(tp)
        return entry[0] if entry else None

    def committed(self, tp: TopicPartition, offset: int) -> None:
        """A commit of this record's offset succeeded: clears a pending offset at or below it."""
        self._committed[tp] = max(self._committed.get(tp, -1), offset)
        entry = self._pending.get(tp)
        if entry is not None and entry[0] <= offset:
            del self._pending[tp]

    def due(self, now_ns: int) -> list[tuple[TopicPartition, int]]:
        """(tp, offset) to recommit now. A pending offset already covered by a later commit is
        dropped here instead of being committed (that would rewind the group's offset)."""
        ready = []
        for tp, (offset, _attempts, due_ns) in list(self._pending.items()):
            if self._committed.get(tp, -1) >= offset:
                del self._pending[tp]
            elif due_ns <= now_ns:
                ready.append((tp, offset))
        return ready

    def revoked(self, tps: Iterable[TopicPartition]) -> None:
        for tp in tps:
            self._pending.pop(tp, None)
            self._committed.pop(tp, None)

    def next_due_ns(self) -> int | None:
        return min((entry[2] for entry in self._pending.values()), default=None)
