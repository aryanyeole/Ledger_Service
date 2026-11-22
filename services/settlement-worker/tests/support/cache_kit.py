"""Helpers for the M5 cache proofs in tests/cache (D-142, D-152): a raw redis-py client on a stack's Redis (never
through the worker), the worker's invalidation counter from its /metrics, a payment's terminal status through the
API, and the worker's log timestamps. Moved from tests/cache/test_balance_invalidation.py (T9) in T11, unchanged in
behaviour, so both cache modules share one copy.
"""

import asyncio
import time
import urllib.request
import uuid
from datetime import datetime
from decimal import Decimal

from settlement_worker import redis_client

TERMINAL = ("SETTLED", "FAILED")


def raw_redis(url: str, body):
    """A raw redis-py client on this Redis, closed afterwards; body(client) is a coroutine function."""

    async def main():
        client = redis_client.create(url)
        try:
            return await body(client)
        finally:
            await client.aclose()

    return asyncio.run(main())


def invalidations(worker, result: str) -> Decimal:
    """settlement_balance_invalidations_total{result} from the worker's /metrics, 0 before its first increment."""
    with urllib.request.urlopen(f"http://127.0.0.1:{worker.metrics_port()}/metrics", timeout=10) as response:
        text = response.read().decode()
    prefix = f'settlement_balance_invalidations_total{{result="{result}"}} '
    for line in text.splitlines():
        if line.startswith(prefix):
            return Decimal(line[len(prefix):])
    return Decimal(0)


def wait_terminal(api, payment_id: uuid.UUID, timeout: float = 60) -> str:
    """Polls GET /v1/payments/{id} (a DB read) until SETTLED or FAILED; returns it, or the last status on timeout."""
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = api.payment_status(payment_id)
        if status in TERMINAL:
            return status
        time.sleep(0.2)
    return status


def wait_status(api, payment_id: uuid.UUID, expected: str, timeout: float = 60) -> None:
    status = wait_terminal(api, payment_id, timeout)
    assert status == expected, f"payment {payment_id} is {status}, expected {expected}"


def timestamp(event: dict) -> datetime:
    """A worker log line's time: structlog's ISO timestamp, UTC, microseconds (D-090)."""
    return datetime.fromisoformat(event["timestamp"])
