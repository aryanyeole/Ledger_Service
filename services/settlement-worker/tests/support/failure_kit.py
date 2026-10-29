"""Shared helpers for the failure_modes tests (D-123): DB reads as the runtime roles, a held escrow
lock, a row-lock probe, fresh consumer groups, worker launches, waits, DLQ topics and their records
(D-111), and the container-level outages (D-129).

DB outages use the real container: docker pause for the hang path, pg_terminate_backend through
docker exec as the container's superuser for the reset path. Both are admin actions on the server;
neither touches a trigger, the schema or a role.
"""

import asyncio
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import CoordinatorNotAvailableError, GroupCoordinatorNotAvailableError, NotCoordinatorError

from support.stack import DLQ_TOPIC, TOPIC, create_topics, topic_partitions
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


def fresh_group(fm, group: str) -> str:
    """D-123: commits the case's new consumer group at the topic's current end offset on every
    partition, before its worker starts, so the case sees only the records it publishes itself:
    records earlier cases left (dead-lettered poison included) never reach it, and case order
    doesn't matter. A short-lived consumer in that group commits explicitly and closes."""
    run(_seed_group(fm.kafka.bootstrap_servers, group))
    return group


async def _seed_group(bootstrap: str, group: str) -> None:
    partitions = sorted(await topic_partitions(bootstrap, TOPIC) or [])
    assert partitions, f"{TOPIC} has no partitions"
    seeder = AIOKafkaConsumer(bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False)
    await seeder.start()
    try:
        tps = [TopicPartition(TOPIC, p) for p in partitions]
        seeder.assign(tps)
        ends = await seeder.end_offsets(tps)
        deadline = time.monotonic() + 60
        while True:
            try:
                await seeder.commit(ends)
                break
            except (GroupCoordinatorNotAvailableError, CoordinatorNotAvailableError, NotCoordinatorError):
                # A fresh broker creates the group coordinator on first use.
                assert time.monotonic() < deadline, f"could not commit group {group}"
                await asyncio.sleep(0.25)
    finally:
        await seeder.stop()
    committed = await committed_offsets(bootstrap, group)
    assert committed == {tp.partition: end for tp, end in ends.items()}, (committed, ends)


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


@dataclass(frozen=True)
class DlqRecord:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]

    def header(self, name: str) -> str | None:
        values = [v for k, v in self.headers if k == name]
        assert len(values) <= 1, (name, self.headers)
        return values[0].decode("utf-8") if values else None

    def original(self) -> tuple[str | None, int, int]:
        return (
            self.header("dlq.original_topic"),
            int(self.header("dlq.original_partition")),
            int(self.header("dlq.original_offset")),
        )


def dlq_records(fm, topic: str = DLQ_TOPIC) -> list[DlqRecord]:
    """Every record on the DLQ topic, from offset 0, read without a group."""
    return run(_read_all(fm.kafka.bootstrap_servers, topic))


def topic_records(fm, topic: str = TOPIC) -> list[DlqRecord]:
    """Every record on a topic (ledger.payments.v1 by default), from offset 0, read without a group."""
    return run(_read_all(fm.kafka.bootstrap_servers, topic))


def positions(fm, key: bytes) -> list[tuple[int, int]]:
    """(partition, offset) of every record on ledger.payments.v1 with this key, in offset order."""
    return sorted((r.partition, r.offset) for r in topic_records(fm) if r.key == key)


def produce_null_record(fm) -> tuple[int, int]:
    """Writes one record with neither key nor value to ledger.payments.v1 and returns its (partition,
    offset). aiokafka refuses such a record (producer.send asserts a key or a value), so the broker
    image's own console producer writes it, through docker exec: with parse.key=true and
    null.marker=NULL, the line "NULL<TAB>NULL" is a record whose key and value are both null."""
    assert fm.kafka.container_id, "the stack's Kafka carries no container id"
    before = {(r.partition, r.offset) for r in topic_records(fm)}
    result = subprocess.run(
        [
            "docker", "exec", "-i", fm.kafka.container_id,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", fm.kafka.bootstrap_servers,
            "--topic", TOPIC,
            "--reader-property", "parse.key=true",
            "--reader-property", "null.marker=NULL",
        ],
        input="NULL\tNULL\n",
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, f"console producer failed: {result.stdout}{result.stderr}"
    new = [r for r in topic_records(fm) if (r.partition, r.offset) not in before]
    assert len(new) == 1 and new[0].key is None and new[0].value is None, new
    return new[0].partition, new[0].offset


def dead_letters_of(fm, partition: int, offset: int, topic: str = DLQ_TOPIC) -> list[DlqRecord]:
    """The DLQ records whose dlq.original_* headers name this record of ledger.payments.v1."""
    return [r for r in dlq_records(fm, topic) if r.original() == (TOPIC, partition, offset)]


async def _read_all(bootstrap: str, topic: str) -> list[DlqRecord]:
    partitions = sorted(await topic_partitions(bootstrap, topic) or [])
    assert partitions, f"{topic} does not exist"
    reader = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    await reader.start()
    found: list[DlqRecord] = []
    try:
        tps = [TopicPartition(topic, p) for p in partitions]
        reader.assign(tps)
        ends = await reader.end_offsets(tps)
        for tp in tps:
            reader.seek(tp, 0)
        remaining = {tp for tp in tps if ends[tp] > 0}
        deadline = time.monotonic() + 30
        while remaining:
            assert time.monotonic() < deadline, f"could not read {topic} up to {ends}"
            for tp, records in (await reader.getmany(timeout_ms=500)).items():
                for r in records:
                    if r.offset < ends[tp]:
                        found.append(DlqRecord(r.partition, r.offset, r.key, r.value, list(r.headers)))
            remaining = {tp for tp in remaining if await reader.position(tp) < ends[tp]}
    finally:
        await reader.stop()
    return found


def create_dlq_topic(fm, name: str) -> None:
    """Creates a DLQ topic as its owner would: 1 partition, retention.ms=-1 (DlqTopicGuard, D-110)."""
    run(create_topics(fm.kafka.bootstrap_servers, {name: (1, {"retention.ms": "-1"})}))


class RowLockProbe:
    """Tries SELECT ... FOR UPDATE NOWAIT on one payments row as settlement_worker every interval,
    on its own thread and event loop, until stopped. Each try is its own statement (autocommit), so
    the probe holds the lock for no longer than that statement. Records (monotonic_s, locked)."""

    def __init__(self, fm, payment_id, interval_s: float = 0.1) -> None:
        self._fm = fm
        self._payment_id = payment_id
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self.samples: list[tuple[float, bool]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def probe():
            conn = await self._fm.worker_db.connect()
            try:
                self._ready.set()
                while not self._stop.is_set():
                    try:
                        await conn.fetch("SELECT id FROM payments WHERE id = $1 FOR UPDATE NOWAIT", self._payment_id)
                        locked = False
                    except asyncpg.LockNotAvailableError:
                        locked = True
                    self.samples.append((time.monotonic(), locked))
                    await asyncio.sleep(self._interval_s)
            finally:
                await conn.close()

        try:
            asyncio.run(probe())
        except BaseException as ex:  # noqa: BLE001 - surfaced by stop()
            self._error = ex
            self._ready.set()

    def __enter__(self) -> "RowLockProbe":
        self._thread.start()
        assert self._ready.wait(30), "the probe did not connect"
        assert self._error is None, self._error
        return self

    def stop(self) -> list[tuple[float, bool]]:
        self._stop.set()
        self._thread.join(30)
        assert self._error is None, self._error
        return self.samples

    def __exit__(self, *exc) -> None:
        self.stop()


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
