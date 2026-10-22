"""Running the real worker as a subprocess, and watching it from outside (D-085, D-091, D-104).

The worker runs as python -m settlement_worker with stdin on a pipe and WORKER_STOP_ON_STDIN_EOF=1,
so closing stdin stops it the same way on Windows and Linux. Its stdout (JSON lines) goes to a
file the tests parse; PYTHONUNBUFFERED is removed so the worker's own per-line flushing is what
makes lines visible. Quiescence is judged from outside: the payments are terminal, their outbox
rows are published, and the group's committed offset equals the end offset on every partition.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError

from support.stack import TOPIC, WORKER_PASSWORD, Database, topic_partitions

from settlement_worker import envelope

SESSION_TIMEOUT_MS = "3000"  # the test broker's group.min.session.timeout.ms is 1000 (D-085)
HEARTBEAT_INTERVAL_MS = "1000"


class WorkerProcess:
    def __init__(self, process: subprocess.Popen, stdout_path: Path, stderr_path: Path, group: str) -> None:
        self.process = process
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.group = group

    def lines(self) -> list[dict]:
        text = self.stdout_path.read_text(encoding="utf-8", errors="replace")
        complete = text[: text.rfind("\n") + 1]
        return [json.loads(line) for line in complete.splitlines() if line.strip()]

    def events(self, name: str, **match) -> list[dict]:
        return [e for e in self.lines() if e["event"] == name and all(e.get(k) == v for k, v in match.items())]

    def wait_for(self, name: str, *, timeout: float = 60, **match) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = self.events(name, **match)
            if found:
                return found[0]
            if self.process.poll() is not None:
                break
            time.sleep(0.1)
        raise AssertionError(f"no {name} {match} in the worker log (exit {self.process.poll()}):\n{self.diagnostics()}")

    def metrics_port(self, timeout: float = 60) -> int:
        """The port the worker's /metrics server bound, from its metrics_listening event (D-113)."""
        return self.wait_for("metrics_listening", timeout=timeout)["port"]

    def wait_exit(self, timeout: float) -> int:
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"worker did not exit within {timeout} s:\n{self.diagnostics()}") from None

    def stop(self, timeout: float = 15) -> int:
        """Close stdin (D-091) and wait for the exit code."""
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        return self.wait_exit(timeout)

    def diagnostics(self) -> str:
        out = self.stdout_path.read_text(encoding="utf-8", errors="replace")[-3000:]
        err = self.stderr_path.read_text(encoding="utf-8", errors="replace")[-3000:]
        return f"--- stdout ---\n{out}\n--- stderr ---\n{err}"

    def __enter__(self) -> "WorkerProcess":
        return self

    def __exit__(self, *exc) -> None:
        if self.process.poll() is None:
            try:
                self.stop()
            except AssertionError:
                self.process.kill()
                self.process.wait(timeout=30)


def start_worker(
    *,
    database: Database,
    bootstrap_servers: str,
    group: str,
    log_dir: Path,
    fault_point: str | None = None,
    fault_payment_id: uuid.UUID | None = None,
    command: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
) -> WorkerProcess:
    """command defaults to the real worker; the negative control passes its naive entrypoint (D-086)."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k != "PYTHONUNBUFFERED" and not k.startswith(("FAULT_", "NAIVE_"))
    }
    env.update(
        POSTGRES_HOST=database.host,
        POSTGRES_PORT=str(database.port),
        POSTGRES_DB=database.name,
        SETTLEMENT_WORKER_PASSWORD=WORKER_PASSWORD,
        KAFKA_BOOTSTRAP_SERVERS=bootstrap_servers,
        LEDGER_PAYMENTS_TOPIC=TOPIC,
        WORKER_CONSUMER_GROUP=group,
        WORKER_SESSION_TIMEOUT_MS=SESSION_TIMEOUT_MS,
        WORKER_HEARTBEAT_INTERVAL_MS=HEARTBEAT_INTERVAL_MS,
        WORKER_STOP_ON_STDIN_EOF="1",
        # Any free port: several workers share one host (D-087, D-113); metrics_port() reads it back.
        WORKER_METRICS_PORT="0",
        # Exact, short backoffs (D-123): 50, 100, 200, 400 ms. Tests that measure timing override them.
        WORKER_RETRY_JITTER="0",
        WORKER_RETRY_BASE_MS="50",
        WORKER_RETRY_MAX_MS="400",
    )
    if fault_point is not None:
        env.update(FAULT_POINT=fault_point, FAULT_PAYMENT_ID=str(fault_payment_id))
    env.update(extra_env or {})
    name = f"{group}-{uuid.uuid4().hex[:6]}"
    stdout_path = log_dir / f"{name}.out.jsonl"
    stderr_path = log_dir / f"{name}.err.txt"
    with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err:
        process = subprocess.Popen(
            command or [sys.executable, "-m", "settlement_worker"],
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            env=env,
        )
    return WorkerProcess(process, stdout_path, stderr_path, group)


async def committed_offsets(bootstrap_servers: str, group: str, topic: str = TOPIC) -> dict[int, int | None]:
    """Per partition: the group's committed offset, or None if it has none."""
    partitions = sorted(await topic_partitions(bootstrap_servers, topic) or [])
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        committed = await admin.list_consumer_group_offsets(group)
    finally:
        await admin.close()
    result: dict[int, int | None] = {}
    for p in partitions:
        meta = committed.get(TopicPartition(topic, p))
        result[p] = meta.offset if meta is not None and meta.offset >= 0 else None
    return result


async def end_offsets(bootstrap_servers: str, partitions: list[int], topic: str = TOPIC) -> dict[int, int]:
    probe = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await probe.start()
    try:
        ends = await probe.end_offsets([TopicPartition(topic, p) for p in partitions])
    finally:
        await probe.stop()
    return {tp.partition: offset for tp, offset in ends.items()}


async def group_lag(bootstrap_servers: str, group: str, topic: str = TOPIC) -> dict[int, int]:
    """Per partition: end offset minus the group's committed offset (0 if it has none)."""
    committed = await committed_offsets(bootstrap_servers, group, topic)
    ends = await end_offsets(bootstrap_servers, sorted(committed), topic)
    return {p: ends[p] - (committed[p] or 0) for p in sorted(committed)}


async def authorized_positions(
    bootstrap_servers: str, group: str, payment_ids: list[uuid.UUID], topic: str = TOPIC
) -> dict[uuid.UUID, tuple[int, int]]:
    """(partition, offset) of each payment's payment.authorized record, read from the topic between
    the group's committed offsets and the end offsets: the real partitioner decides, not a
    reimplementation of it (D-105)."""
    committed = await committed_offsets(bootstrap_servers, group, topic)
    ends = await end_offsets(bootstrap_servers, sorted(committed), topic)
    wanted = set(payment_ids)
    found: dict[uuid.UUID, tuple[int, int]] = {}
    reader = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
    await reader.start()
    try:
        assigned = [TopicPartition(topic, p) for p in sorted(committed) if ends[p] > (committed[p] or 0)]
        reader.assign(assigned)
        for tp in assigned:
            reader.seek(tp, committed[tp.partition] or 0)
        remaining = {tp for tp in assigned}
        deadline = time.monotonic() + 30
        while remaining:
            if time.monotonic() > deadline:
                raise AssertionError(f"could not read {topic} up to {ends}: stuck on {remaining}")
            batches = await reader.getmany(timeout_ms=500)
            for tp, records in batches.items():
                for record in records:
                    if record.offset >= ends[tp.partition]:
                        continue
                    event = envelope.parse(record.value)
                    if isinstance(event, envelope.AuthorizedEvent) and event.payment_id in wanted:
                        found[event.payment_id] = (record.partition, record.offset)
            remaining = {tp for tp in remaining if await reader.position(tp) < ends[tp.partition]}
    finally:
        await reader.stop()
    missing = wanted - set(found)
    assert not missing, f"payment.authorized not found on {topic} for {sorted(map(str, missing))}"
    return found


async def group_state(bootstrap_servers: str, group: str) -> tuple[str, int]:
    """(state, member count) of a consumer group, e.g. ("Stable", 2), from the admin client.

    describe_consumer_groups returns DescribeGroupsResponse objects whose groups are tuples of
    (error_code, group_id, state, protocol_type, protocol, members) (checked on apache/kafka:4.3.1).
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        responses = await admin.describe_consumer_groups([group])
    finally:
        await admin.close()
    for response in responses:
        for error_code, group_id, state, _protocol_type, _protocol, members in response.groups:
            if group_id == group:
                return (state, len(members)) if error_code == 0 else (f"error {error_code}", 0)
    return ("unknown", 0)


async def wait_quiescent(
    *,
    app_conn,
    bootstrap_servers: str,
    group: str,
    payment_ids: list[uuid.UUID],
    timeout: float = 60,
    require_terminal: bool = True,
    check=None,
) -> dict[int, int]:
    """Payments terminal, their outbox rows published, and lag 0 on every partition, twice in a row.

    require_terminal=False drops the first condition (D-105): lag 0 alone means every record was
    processed and committed, so a payment still AUTHORIZED afterwards was skipped, and the caller
    can count it as lost instead of timing out here. check, if given, runs on every poll and returns
    a failure message (e.g. a worker exited) to fail at once instead of waiting out the deadline.
    """
    keys = [str(p) for p in payment_ids]
    deadline = time.monotonic() + timeout
    consecutive = 0
    last = None
    while time.monotonic() < deadline:
        failure = check() if check else None
        if failure:
            raise AssertionError(failure)
        authorized = await app_conn.fetchval(
            "SELECT count(*) FROM payments WHERE id = ANY($1::uuid[]) AND status = 'AUTHORIZED'", payment_ids
        )
        unpublished = await app_conn.fetchval(
            "SELECT count(*) FROM outbox WHERE message_key = ANY($1::text[]) AND published_at IS NULL", keys
        )
        try:
            lag = await group_lag(bootstrap_servers, group)
        except KafkaError as ex:
            if not ex.retriable:
                raise
            # e.g. the first group on a fresh broker: no coordinator until __consumer_offsets exists.
            lag = {"retriable": type(ex).__name__}
        last = (authorized, unpublished, lag)
        quiet = (authorized == 0 or not require_terminal) and unpublished == 0 and set(lag) == {0, 1, 2} and all(v == 0 for v in lag.values())
        consecutive = consecutive + 1 if quiet else 0
        if consecutive == 2:
            return lag
        await asyncio.sleep(0.5)
    raise AssertionError(f"not quiescent within {timeout} s: (authorized, unpublished, lag) = {last}")
