"""Shared helpers for the failure_modes tests (D-123): DB reads as the runtime roles, a held escrow
lock, worker launches, waits, and the container-level outages (D-129).

DB outages use the real container: docker pause for the hang path, pg_terminate_backend through
docker exec as the container's superuser for the reset path. Both are admin actions on the server;
neither touches a trigger, the schema or a role.
"""

import asyncio
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta

from aiokafka.errors import GroupCoordinatorNotAvailableError

from support.worker_process import committed_offsets, group_lag, start_worker

CLOCK_ALLOWANCE = timedelta(milliseconds=5)


def run(coro):
    return asyncio.run(coro)


async def _fetch(db, query, *args):
    conn = await db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def app(fm, query, *args):
    return run(_fetch(fm.app_db, query, *args))


def worker_role(fm, query, *args):
    """processed_messages is readable only by settlement_worker (V12 grants)."""
    return run(_fetch(fm.worker_db, query, *args))


def status(fm, payment_id) -> str:
    return app(fm, "SELECT status FROM payments WHERE id = $1", payment_id)[0]["status"]


def postings(fm, payment_id) -> int:
    rows = app(
        fm,
        "SELECT count(*) AS n FROM transactions WHERE reference = $1 AND kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE')",
        str(payment_id),
    )
    return rows[0]["n"]


def results(fm, payment_id) -> int:
    rows = app(
        fm,
        "SELECT count(*) AS n FROM outbox WHERE message_key = $1 AND event_type IN ('payment.settled', 'payment.failed')",
        str(payment_id),
    )
    return rows[0]["n"]


def event_id(fm, payment_id) -> uuid.UUID:
    rows = app(
        fm, "SELECT event_id FROM outbox WHERE message_key = $1 AND event_type = 'payment.authorized'", str(payment_id)
    )
    return rows[0]["event_id"]


def dedup_rows(fm, group, payment_id) -> int:
    rows = worker_role(
        fm,
        "SELECT count(*) AS n FROM processed_messages WHERE consumer_group = $1 AND event_id = $2",
        group,
        event_id(fm, payment_id),
    )
    return rows[0]["n"]


def committed(fm, group, partition) -> int | None:
    """The group's committed offset, read with a separate admin client that never commits."""
    return run(committed_offsets(fm.kafka.bootstrap_servers, group)).get(partition)


def assert_not_past(offset_committed: int | None, offset: int, when: str) -> None:
    """A committed offset past the record means it would never be redelivered."""
    assert offset_committed is None or offset_committed <= offset, (
        f"{when}: committed offset {offset_committed} > P's offset {offset}"
    )


def ts(event) -> datetime:
    return datetime.fromisoformat(event["timestamp"])


def funded_pair(fm) -> tuple[int, int]:
    customer = fm.api.create_account("CUSTOMER")
    merchant = fm.api.create_account("MERCHANT")
    fm.api.deposit(customer, "10000.00")
    return customer, merchant


def worker(fm, group, *, command=None, **env):
    return start_worker(
        database=fm.database,
        bootstrap_servers=fm.kafka.bootstrap_servers,
        group=group,
        log_dir=fm.log_dir,
        command=command,
        extra_env={k: str(v) for k, v in env.items()},
    )


def wait(process_worker, predicate, timeout: float, what: str):
    """Polls predicate until it holds; fails at once if the worker exits first."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        if process_worker.process.poll() is not None:
            raise AssertionError(
                f"worker exited {process_worker.process.returncode} waiting for {what}:\n{process_worker.diagnostics()}"
            )
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}:\n{process_worker.diagnostics()}")


def wait_idle(fm, process_worker, group, timeout: float = 60) -> None:
    """Until the worker has started and joined, and the group has consumed everything on the topic
    (lag 0 on every partition). Joined first: on an empty topic the lag is 0 before the worker has
    even connected. On a fresh broker the group coordinator appears only after the first group
    exists; until then the group isn't idle yet."""
    process_worker.wait_for("partitions_assigned", timeout=timeout)

    def idle() -> bool:
        try:
            return all(lag == 0 for lag in run(group_lag(fm.kafka.bootstrap_servers, group)).values())
        except GroupCoordinatorNotAvailableError:
            return False

    wait(process_worker, idle, timeout, f"group {group} to catch up")


class EscrowHolder:
    """Holds system:escrow FOR UPDATE as ledger_api_app on its own thread and event loop until
    released (a granted row lock: ledger_api_app has UPDATE(balance) on accounts, D-034)."""

    def __init__(self, fm) -> None:
        self._fm = fm
        self._locked = threading.Event()
        self._release = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def hold():
            conn = await self._fm.app_db.connect()
            try:
                tx = conn.transaction()
                await tx.start()
                await conn.fetch("SELECT id FROM accounts WHERE name = 'system:escrow' FOR UPDATE")
                self._locked.set()
                await asyncio.to_thread(self._release.wait)
                await tx.rollback()
            finally:
                await conn.close()

        try:
            asyncio.run(hold())
        except BaseException as ex:  # noqa: BLE001 - surfaced by release()
            self._error = ex
            self._locked.set()

    def __enter__(self) -> "EscrowHolder":
        self._thread.start()
        assert self._locked.wait(30), "the escrow lock was not taken"
        assert self._error is None, self._error
        return self

    def release(self) -> None:
        self._release.set()
        self._thread.join(30)
        assert self._error is None, self._error

    def __exit__(self, *exc) -> None:
        self.release()


def docker(*args: str) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 0, f"docker {' '.join(args)} failed: {result.stderr}"
    return result.stdout


class Paused:
    """docker pause on the Postgres container for the duration of the block; unpaused in any case."""

    def __init__(self, fm) -> None:
        self._id = fm.database.container_id
        assert self._id, "the stack's Database carries no container id"

    def __enter__(self) -> "Paused":
        docker("pause", self._id)
        return self

    def __exit__(self, *exc) -> None:
        docker("unpause", self._id)


def superuser_sql(fm, sql: str) -> str:
    """One statement as the container's superuser over the local socket (the image trusts it)."""
    return docker("exec", fm.database.container_id, "psql", "-U", "ledger", "-d", "ledger", "-tAc", sql).strip()


def worker_backends(fm, where: str = "true") -> list[int]:
    out = superuser_sql(fm, f"SELECT pid FROM pg_stat_activity WHERE usename = 'settlement_worker' AND ({where})")
    return [int(line) for line in out.splitlines() if line.strip()]


def terminate_worker_backends(fm, where: str = "true") -> int:
    """pg_terminate_backend for settlement_worker's backends matching where; returns how many."""
    out = superuser_sql(
        fm,
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
        f"WHERE usename = 'settlement_worker' AND ({where})",
    )
    return int(out)
