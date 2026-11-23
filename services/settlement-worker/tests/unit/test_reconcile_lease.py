"""The reconciliation lease (D-144) against fake clients and a fake clock, and the reconciler's use of it.

try_acquire(): acquired, held, unavailable (raising and hanging clients, never raising itself), the SET's arguments,
and the 1 s bypass after an unavailable attempt (no Redis call inside it, a call after it). The reconciler: held skips
the tick with no DB call at all (connect and the snapshot are stand-ins that fail the test if reached) and emits
reconciliation_skipped with the skipped counter; unavailable runs after reconciliation_lock_unavailable (WARNING);
acquired and no lease run. Real Redis: tests/integration/test_reconcile_lease_redis.py; multi-worker: T13.
"""

import asyncio
import os
import re
import socket
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry
from structlog.testing import capture_logs

from settlement_worker import reconcile, reconcile_lease
from settlement_worker.config import Config
from settlement_worker.metrics import ReconcileMetrics

UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


class Clock:
    def __init__(self):
        self.now = 1_000_000_000

    def __call__(self):
        return self.now

    def advance_ms(self, ms: int) -> None:
        self.now += ms * 1_000_000


class FakeRedis:
    """SET NX PX against one key; raises `error` or hangs if told to. Records every call."""

    def __init__(self, *, error=None, hang=False):
        self.calls = []
        self.value = None
        self.error = error
        self.hang = hang

    async def set(self, key, value, *, nx, px):
        self.calls.append((key, value, nx, px))
        if self.hang:
            await asyncio.sleep(10)
        if self.error is not None:
            raise self.error
        if self.value is not None:
            return None
        self.value = value
        return True


def _acquire(lease):
    return asyncio.run(lease.try_acquire())


def test_the_bound_is_connect_plus_command():
    assert reconcile_lease.LEASE_BOUND_MS == 300
    assert reconcile_lease.LEASE_KEY == "ledger:v1:reconcile:lease"


def test_acquired_then_held_with_the_contract_set():
    client = FakeRedis()
    lease = reconcile_lease.Lease(client, 3000, clock=Clock())
    assert _acquire(lease) == reconcile_lease.Attempt("acquired")
    assert _acquire(lease) == reconcile_lease.Attempt("held")
    (key, value, nx, px), (_k, second_value, _nx, _px) = client.calls
    assert (key, nx, px) == ("ledger:v1:reconcile:lease", True, 3000)
    assert re.fullmatch(rf"{re.escape(socket.gethostname())}:{os.getpid()}:{UUID}", value), value
    assert second_value != value, "a fresh uuid per attempt"


@pytest.mark.parametrize("error", [ConnectionError("refused"), OSError("unreachable"), TimeoutError("slow")])
def test_a_raising_client_is_unavailable_without_raising(error):
    lease = reconcile_lease.Lease(FakeRedis(error=error), 3000, clock=Clock())
    assert _acquire(lease) == reconcile_lease.Attempt("unavailable", type(error).__name__)


def test_a_hanging_client_is_unavailable_at_the_bound():
    lease = reconcile_lease.Lease(FakeRedis(hang=True), 3000)
    started = time.monotonic_ns()
    attempt = _acquire(lease)
    elapsed_ms = (time.monotonic_ns() - started) // 1_000_000
    assert attempt == reconcile_lease.Attempt("unavailable", "TimeoutError")
    assert reconcile_lease.LEASE_BOUND_MS <= elapsed_ms < reconcile_lease.LEASE_BOUND_MS + 800, elapsed_ms


def test_the_bypass_skips_redis_for_one_second_after_unavailable():
    client, clock = FakeRedis(error=ConnectionError("down")), Clock()
    lease = reconcile_lease.Lease(client, 3000, clock=clock)
    assert _acquire(lease).error_class == "ConnectionError"
    assert len(client.calls) == 1

    clock.advance_ms(999)
    assert _acquire(lease) == reconcile_lease.Attempt("unavailable", "bypass")
    assert len(client.calls) == 1, "no Redis call inside the bypass"

    client.error = None
    clock.advance_ms(1)
    assert _acquire(lease) == reconcile_lease.Attempt("acquired")
    assert len(client.calls) == 2, "a call once the bypass ends"


# --- the reconciler ---

D = Decimal
AS_OF = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
CLEAN_ROWS = [
    {"id": i, "type": t, "balance": b, "entry_sum": b, "last_entry_id": i, "global_sum": D("0.0000"),
     "max_transaction_id": 3, "authorized_sum": D("0.0000"), "as_of": AS_OF}
    for i, t, b in [(1, "EXTERNAL", D("-10.0000")), (2, "ESCROW", D("0.0000")), (3, "CUSTOMER", D("10.0000"))]
]


class StubLease:
    def __init__(self, attempt):
        self.attempt = attempt
        self.calls = 0
        self.ttl_ms = 3000

    async def try_acquire(self):
        self.calls += 1
        return self.attempt


def _reconciler(monkeypatch, lease, *, db_allowed: bool):
    config = Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x", "RECONCILE_INTERVAL_MS": "60000"})
    registry = CollectorRegistry()
    reconciler = reconcile.Reconciler(config, ReconcileMetrics(registry), lease=lease)
    touched = []

    async def connect():
        touched.append("connect")
        if not db_allowed:
            raise AssertionError("a skipped tick must not connect")
        return object()

    async def run_once(conn, statement_timeout_ms):
        touched.append("snapshot")
        if not db_allowed:
            raise AssertionError("a skipped tick must not run the snapshot")
        return CLEAN_ROWS

    monkeypatch.setattr(reconciler, "connect", connect)
    monkeypatch.setattr(reconcile, "run_once", run_once)
    return reconciler, registry, touched


def _runs(registry, result):
    return registry.get_sample_value("ledger_reconciliation_runs_total", {"result": result}) or 0


def test_held_skips_the_tick_with_no_db_call(monkeypatch):
    lease = StubLease(reconcile_lease.Attempt("held"))
    reconciler, registry, touched = _reconciler(monkeypatch, lease, db_allowed=False)
    with capture_logs() as logs:
        asyncio.run(reconciler.run_one())
    assert touched == [], "no DB session used"
    assert logs == [
        {"event": "reconciliation_skipped", "log_level": "info", "run_id": 1, "lease_ttl_ms": 3000,
         "correlation_id": None}
    ]
    assert _runs(registry, "skipped") == 1


def test_unavailable_runs_after_a_warning(monkeypatch):
    lease = StubLease(reconcile_lease.Attempt("unavailable", "ConnectionError"))
    reconciler, registry, touched = _reconciler(monkeypatch, lease, db_allowed=True)
    with capture_logs() as logs:
        asyncio.run(reconciler.run_one())
    assert [(e["event"], e["log_level"]) for e in logs] == [
        ("reconciliation_lock_unavailable", "warning"),
        ("reconciliation_completed", "info"),
    ]
    assert (logs[0]["run_id"], logs[0]["error_class"]) == (1, "ConnectionError")
    assert touched == ["connect", "snapshot"]
    assert _runs(registry, "skipped") == 0


@pytest.mark.parametrize("lease", [StubLease(reconcile_lease.Attempt("acquired")), None], ids=["acquired", "no-lease"])
def test_acquired_or_no_lease_runs(monkeypatch, lease):
    reconciler, registry, touched = _reconciler(monkeypatch, lease, db_allowed=True)
    with capture_logs() as logs:
        asyncio.run(reconciler.run_one())
    assert [e["event"] for e in logs] == ["reconciliation_completed"]
    assert touched == ["connect", "snapshot"]
    if lease is not None:
        assert lease.calls == 1
