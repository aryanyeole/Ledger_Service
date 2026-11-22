"""balance_cache against fake clients (D-142, D-152): key literals, invalidate()'s outcomes and its bound, and
BalanceInvalidator's event and counter. invalidate() never raises; after_commit never raises either, even when
its own logging fails. The real-Redis behaviour is tests/integration/test_balance_invalidate_redis.py.
"""

import asyncio
import re
import time
from dataclasses import dataclass

import pytest
import redis
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from settlement_worker import balance_cache, events

CANONICAL_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
PAYMENT = "5b01a9e5-32ce-4c82-90b0-ac8c5143460c"


class RecordingClient:
    def __init__(self):
        self.calls = []

    async def eval(self, script, numkeys, *args):
        self.calls.append((script, numkeys, args))
        return numkeys


class RaisingClient:
    def __init__(self, error):
        self.error = error

    async def eval(self, *args):
        raise self.error


class HangingClient:
    async def eval(self, *args):
        await asyncio.sleep(10)


@dataclass(frozen=True)
class Event:
    payment_id: str = PAYMENT
    correlation_id: str | None = "corr-1"
    customer_account_id: int = 12
    merchant_account_id: int = 13


def _count(result: str) -> float:
    return REGISTRY.get_sample_value("settlement_balance_invalidations_total", {"result": result}) or 0


def test_keys_are_pairs_for_distinct_ids_in_ascending_order():
    # The same case as ledger-api's BalanceCacheUnitTest: [30, 3, 12, 3].
    assert balance_cache.keys([30, 3, 12, 3]) == [
        "ledger:v1:bal:3",
        "ledger:v1:bal-gen:3",
        "ledger:v1:bal:12",
        "ledger:v1:bal-gen:12",
        "ledger:v1:bal:30",
        "ledger:v1:bal-gen:30",
    ]
    assert balance_cache.keys([7]) == ["ledger:v1:bal:7", "ledger:v1:bal-gen:7"]


def test_the_bound_is_connect_plus_command_in_integer_milliseconds():
    assert balance_cache.INVALIDATION_BOUND_MS == 300
    assert isinstance(balance_cache.INVALIDATION_BOUND_MS, int)


def test_sent_is_one_eval_with_a_fresh_canonical_token():
    client = RecordingClient()
    first = asyncio.run(balance_cache.invalidate(client, [13, 12, 2, 3]))
    second = asyncio.run(balance_cache.invalidate(client, [13]))
    assert first == balance_cache.Invalidation("sent") and second.result == "sent"
    script, numkeys, args = client.calls[0]
    assert script == balance_cache.INVALIDATE
    assert numkeys == 8
    assert list(args[:8]) == balance_cache.keys([2, 3, 12, 13])
    assert len(args) == 9 and CANONICAL_UUID.fullmatch(args[8])
    assert client.calls[1][2][-1] != args[8], "a fresh token per call"


@pytest.mark.parametrize(
    "error, name",
    [
        (redis.exceptions.ConnectionError("refused"), "ConnectionError"),
        (redis.exceptions.TimeoutError("slow"), "TimeoutError"),
        (OSError("unreachable"), "OSError"),
        (TimeoutError("socket"), "TimeoutError"),
        (RuntimeError("anything"), "RuntimeError"),
    ],
)
def test_a_raising_client_is_failed_without_raising(error, name):
    assert asyncio.run(balance_cache.invalidate(RaisingClient(error), [1, 2])) == balance_cache.Invalidation(
        "failed", name
    )


def test_a_hanging_client_is_failed_at_the_bound():
    started = time.monotonic_ns()
    outcome = asyncio.run(balance_cache.invalidate(HangingClient(), [1, 2]))
    elapsed_ms = (time.monotonic_ns() - started) // 1_000_000
    print(f"INVALIDATION_HANG elapsed_ms={elapsed_ms} bound_ms={balance_cache.INVALIDATION_BOUND_MS}")
    assert outcome == balance_cache.Invalidation("failed", "TimeoutError")
    assert balance_cache.INVALIDATION_BOUND_MS <= elapsed_ms < balance_cache.INVALIDATION_BOUND_MS + 800


def test_after_commit_invalidates_the_four_accounts_and_counts_sent():
    client = RecordingClient()
    invalidator = balance_cache.BalanceInvalidator(client, 2, 3)
    sent = _count("sent")
    with capture_logs() as logs:
        asyncio.run(invalidator.after_commit(Event()))
    assert list(client.calls[0][2][:-1]) == balance_cache.keys([2, 3, 12, 13])
    assert _count("sent") - sent == 1
    assert logs == []


def test_after_commit_failed_is_counted_and_logged_without_keys_or_ids():
    invalidator = balance_cache.BalanceInvalidator(RaisingClient(OSError("down")), 2, 3)
    failed = _count("failed")
    with capture_logs() as logs:
        asyncio.run(invalidator.after_commit(Event()))
    assert _count("failed") - failed == 1
    assert logs == [
        {
            "event": "cache_invalidation_failed",
            "log_level": "warning",
            "payment_id": PAYMENT,
            "error_class": "OSError",
            "correlation_id": "corr-1",
        }
    ]


def test_after_commit_never_raises_even_when_its_own_logging_fails(monkeypatch):
    def broken_emit(*args, **kwargs):
        raise RuntimeError("logging is broken")

    monkeypatch.setattr(events, "emit", broken_emit)
    invalidator = balance_cache.BalanceInvalidator(RaisingClient(OSError("down")), 2, 3)
    failed = _count("failed")
    asyncio.run(invalidator.after_commit(Event()))
    assert _count("failed") - failed == 1, "counted once, not again by the fallback"


def test_after_commit_never_raises_when_the_counter_fails(monkeypatch):
    class BrokenCounter:
        def labels(self, **kwargs):
            raise RuntimeError("metrics are broken")

    monkeypatch.setattr(balance_cache.metrics, "BALANCE_INVALIDATIONS", BrokenCounter())
    invalidator = balance_cache.BalanceInvalidator(RecordingClient(), 2, 3)
    with capture_logs() as logs:
        asyncio.run(invalidator.after_commit(Event()))
    assert [(log["event"], log["error_class"]) for log in logs] == [("cache_invalidation_failed", "RuntimeError")]
