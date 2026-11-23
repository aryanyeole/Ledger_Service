"""The reconciliation lease (D-144, docs/design/cache-invalidation.md §1.3, §2.1, §3.8, §7).

Before each tick, before any DB call for that run, the reconciler tries SET ledger:v1:reconcile:lease
"{host}:{pid}:{uuid}" NX PX RECONCILE_LOCK_TTL_MS: acquired → run; held → skip the tick with no DB call; unavailable →
run anyway (fail open). The value is for diagnosis only (a fresh uuid per attempt); nobody compares it. The lease is
never released: it expires after its PX, so at most one instance runs per period while Redis is up, and every instance
runs when it's down, which is harmless (D-115: reconciliation is read-only). No fencing token: nothing is written
(§7.1).

The SET is bounded by LEASE_BOUND_MS (connect + command, integer ms divided at the call site, D-093) and never raises:
every failure is "unavailable". After an unavailable attempt, the worker's 1 s bypass (D-137) makes further attempts
"unavailable" without a Redis call until it ends. The bypass is the lease's alone: balance invalidations never consult
it (D-142, D-152).
"""

import asyncio
import os
import socket
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from settlement_worker import redis_client

LEASE_KEY = "ledger:v1:reconcile:lease"
LEASE_BOUND_MS = redis_client.CONNECT_TIMEOUT_MS + redis_client.COMMAND_TIMEOUT_MS
BYPASS_MS = 1000

ACQUIRED = "acquired"
HELD = "held"
UNAVAILABLE = "unavailable"
BYPASS = "bypass"


@dataclass(frozen=True)
class Attempt:
    """result is ACQUIRED, HELD or UNAVAILABLE; error_class names the exception, or BYPASS, for UNAVAILABLE."""

    result: str
    error_class: str | None = None


def lease_value() -> str:
    """{host}:{pid}:{uuid}, a fresh uuid per attempt (diagnosis only)."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"


class Lease:
    """try_acquire() never raises. clock: monotonic nanoseconds (injectable for tests)."""

    def __init__(self, client, ttl_ms: int, *, clock: Callable[[], int] = time.monotonic_ns) -> None:
        self._client = client
        self._ttl_ms = ttl_ms
        self._clock = clock
        self._bypass_until: int | None = None

    @property
    def ttl_ms(self) -> int:
        return self._ttl_ms

    async def try_acquire(self) -> Attempt:
        now = self._clock()
        if self._bypass_until is not None and now < self._bypass_until:
            return Attempt(UNAVAILABLE, BYPASS)
        try:
            async with asyncio.timeout(LEASE_BOUND_MS / 1000):
                written = await self._client.set(LEASE_KEY, lease_value(), nx=True, px=self._ttl_ms)
        except Exception as ex:  # noqa: BLE001 - every failure is "unavailable": the tick runs (fail open, D-144)
            self._bypass_until = self._clock() + BYPASS_MS * 1_000_000
            return Attempt(UNAVAILABLE, type(ex).__name__)
        self._bypass_until = None
        return Attempt(ACQUIRED) if written else Attempt(HELD)
