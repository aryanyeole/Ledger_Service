"""D-108: the backoff schedule and the per-record attempt counts."""

import random

import pytest
from aiokafka import TopicPartition

from settlement_worker.retry import PendingCommits, RetryState, backoff_ms

TP = TopicPartition("ledger.payments.v1", 1)
OTHER = TopicPartition("ledger.payments.v1", 2)


def test_schedule_without_jitter_is_exact_and_capped():
    delays = [backoff_ms(a, base_ms=200, max_ms=30_000, jitter=False, rng=random.Random(0)) for a in range(1, 10)]
    assert delays == [200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000]
    assert [backoff_ms(a, base_ms=50, max_ms=400, jitter=False, rng=random.Random(0)) for a in range(1, 6)] == [
        50,
        100,
        200,
        400,
        400,
    ]


def test_jitter_stays_within_zero_and_the_delay():
    rng = random.Random(1234)
    for attempt in range(1, 8):
        cap = min(30_000, 200 * 2 ** (attempt - 1))
        draws = [backoff_ms(attempt, base_ms=200, max_ms=30_000, jitter=True, rng=rng) for _ in range(500)]
        assert all(isinstance(d, int) and 0 <= d <= cap for d in draws), attempt
        assert len(set(draws)) > 1, attempt


def test_attempt_must_be_positive():
    with pytest.raises(ValueError):
        backoff_ms(0, base_ms=200, max_ms=30_000, jitter=False, rng=random.Random(0))


def test_attempts_count_per_record_and_clear_on_success():
    state = RetryState()
    assert [state.failed(TP, 7) for _ in range(3)] == [1, 2, 3]
    assert state.failed(TP, 8) == 1
    state.succeeded(TP, 7)
    assert state.attempts(TP, 7) == 0
    # A later record at the same (topic, partition, offset) key starts again from 1.
    assert state.failed(TP, 7) == 1
    assert state.attempts(TP, 8) == 1


def test_revoke_forgets_attempts_and_cancels_the_pending_resume():
    state = RetryState()
    state.failed(TP, 7)
    state.failed(OTHER, 3)
    state.schedule(TP, 1_000)
    state.schedule(OTHER, 2_000)
    state.revoked([TP])
    assert state.attempts(TP, 7) == 0
    assert state.attempts(OTHER, 3) == 1
    assert state.next_due_ns() == 2_000
    assert state.due(10_000) == [OTHER]


def test_due_returns_only_ended_backoffs_once():
    state = RetryState()
    state.schedule(TP, 100)
    state.schedule(OTHER, 300)
    assert state.due(99) == []
    assert state.due(200) == [TP]
    assert state.due(200) == []
    assert state.next_due_ns() == 300


def test_pending_commit_attempts_due_and_success():
    pending = PendingCommits()
    assert not pending
    assert pending.failed(TP, 7) == 1
    pending.schedule(TP, 1_000)
    assert pending and pending.offset(TP) == 7
    assert pending.due(999) == []
    assert pending.due(1_000) == [(TP, 7)]
    assert pending.failed(TP, 7) == 2
    pending.committed(TP, 7)
    assert not pending


def test_pending_commit_keeps_the_highest_offset_and_the_attempt_count():
    pending = PendingCommits()
    pending.failed(TP, 7)
    assert pending.failed(TP, 9) == 2, "a higher offset replaces the lower one, attempts carry on"
    assert pending.offset(TP) == 9
    assert pending.failed(TP, 8) == 3
    assert pending.offset(TP) == 9, "never lowered"


def test_a_later_commit_supersedes_and_nothing_is_rewound():
    pending = PendingCommits()
    pending.failed(TP, 7)
    pending.schedule(TP, 0)
    pending.committed(TP, 6)
    assert pending.offset(TP) == 7, "a lower commit doesn't clear it"
    pending.committed(TP, 8)
    assert not pending
    # A failure reported after a higher offset already committed is dropped when due, not committed.
    pending.failed(TP, 7)
    pending.schedule(TP, 0)
    assert pending.due(1) == []
    assert not pending


def test_revoke_drops_pending_commits():
    pending = PendingCommits()
    pending.failed(TP, 7)
    pending.failed(OTHER, 3)
    pending.schedule(TP, 5)
    pending.schedule(OTHER, 9)
    pending.revoked([TP])
    assert pending.offset(TP) is None
    assert pending.next_due_ns() == 9


def test_scheduled_lists_partitions_in_backoff():
    state = RetryState()
    state.schedule(TP, 100)
    assert state.scheduled() == {TP}
    state.due(100)
    assert state.scheduled() == set()
