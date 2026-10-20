"""Builders for the worker's test stacks (D-083, D-084, D-103, D-104).

Postgres gets its schema from the real ledger-api jar in migrate-only mode; Kafka is a generic
apache/kafka container (testcontainers' Kafka module only knows the Confluent image); ledger-api
runs as the real jar in server mode. Each builder is a context manager, so the integration session
(tests/conftest.py) and the e2e package (tests/e2e/conftest.py) build separate stacks the same way.
"""

import asyncio
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import pytest
from aiokafka.admin import AIOKafkaAdminClient
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from support.api import ApiClient

REPO = Path(__file__).resolve().parents[4]
LEDGER_API = REPO / "services" / "ledger-api"
ROLES_SCRIPT = REPO / "infra" / "postgres" / "init" / "01-roles.sh"
MIGRATIONS = LEDGER_API / "src" / "main" / "resources" / "db" / "migration"

POSTGRES_IMAGE = "postgres:17.11"  # D-027
KAFKA_IMAGE = "apache/kafka:4.3.1"  # D-027
TOPIC = "ledger.payments.v1"

# Test-only credentials for throwaway containers, as in ledger-api's TestcontainersConfiguration.
SUPERUSER_PASSWORD = "test-superuser-password"
OWNER_PASSWORD = "test-owner-password"
APP_PASSWORD = "test-app-password"
WORKER_PASSWORD = "test-worker-password"
PASSWORDS = {"settlement_worker": WORKER_PASSWORD, "ledger_api_app": APP_PASSWORD, "ledger_api_owner": OWNER_PASSWORD}

PACKAGE_HINT = r"run .\mvnw.cmd -q -DskipTests package in services\ledger-api"
MIGRATE_TIMEOUT_SECONDS = 120


@dataclass(frozen=True)
class Database:
    host: str
    port: int
    name: str = "ledger"

    def params(self, role: str) -> dict:
        """asyncpg.connect keyword arguments for a role."""
        return {"host": self.host, "port": self.port, "database": self.name, "user": role, "password": PASSWORDS[role]}


async def _connect_as(database: Database, role: str) -> asyncpg.Connection:
    conn = await asyncpg.connect(**database.params(role))
    try:
        row = await conn.fetchrow("SELECT current_user AS who, rolsuper FROM pg_roles WHERE rolname = current_user")
        assert row["who"] == role, f"current_user is {row['who']}, expected {role}"
        assert row["rolsuper"] is False, f"{role} must not be a superuser"
    except BaseException:
        await conn.close()
        raise
    return conn


@dataclass(frozen=True)
class WorkerDb:
    """Connections as settlement_worker, checked to be that role and not a superuser (D-036)."""

    database: Database

    async def connect(self) -> asyncpg.Connection:
        return await _connect_as(self.database, "settlement_worker")


@dataclass(frozen=True)
class AppDb:
    """Connections as ledger_api_app (D-036), for reading what the API and the worker wrote."""

    database: Database

    async def connect(self) -> asyncpg.Connection:
        return await _connect_as(self.database, "ledger_api_app")


@dataclass(frozen=True)
class Kafka:
    bootstrap_servers: str


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _host(container) -> str:
    # Windows resolves localhost to ::1 first; the published ports are IPv4.
    host = container.get_container_host_ip()
    return "127.0.0.1" if host == "localhost" else host


def find_jar() -> Path:
    """The ledger-api jar, refused if missing or older than its sources (D-083)."""
    jars = list((LEDGER_API / "target").glob("ledger-api-*.jar"))
    if len(jars) != 1:
        pytest.fail(f"expected one ledger-api jar in {LEDGER_API / 'target'}, found {jars}: {PACKAGE_HINT}")
    jar = jars[0]
    sources = [p for p in (LEDGER_API / "src" / "main").rglob("*") if p.is_file()] + [LEDGER_API / "pom.xml"]
    newest = max(sources, key=lambda p: p.stat().st_mtime)
    if jar.stat().st_mtime < newest.stat().st_mtime:
        pytest.fail(f"{jar.name} is older than {newest.relative_to(REPO)}: {PACKAGE_HINT}")
    return jar


def java() -> str:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("java")
    if not found:
        pytest.fail("java not found (set JAVA_HOME or put Java 21 on PATH)")
    return found


@contextmanager
def postgres():
    """postgres:17.11 bootstrapped by the same roles script as Compose and ledger-api's tests."""
    container = (
        PostgresContainer(POSTGRES_IMAGE, username="ledger", password=SUPERUSER_PASSWORD, dbname="ledger", driver=None)
        .with_env("LEDGER_API_OWNER_PASSWORD", OWNER_PASSWORD)
        .with_env("LEDGER_API_APP_PASSWORD", APP_PASSWORD)
        .with_env("SETTLEMENT_WORKER_PASSWORD", WORKER_PASSWORD)
        .with_volume_mapping(str(ROLES_SCRIPT), "/docker-entrypoint-initdb.d/01-roles.sh", "ro")
    )
    container.start()
    try:
        yield Database(host=_host(container), port=int(container.get_exposed_port(5432)))
    finally:
        container.stop()


def _jdbc_url(database: Database) -> str:
    return f"jdbc:postgresql://{database.host}:{database.port}/{database.name}"


def _jar_env() -> dict:
    return dict(os.environ, LEDGER_API_APP_PASSWORD=APP_PASSWORD, LEDGER_API_OWNER_PASSWORD=OWNER_PASSWORD)


def _highest_migration() -> int:
    versions = [int(m.group(1)) for p in MIGRATIONS.glob("V*.sql") if (m := re.match(r"V(\d+)__", p.name))]
    assert versions, f"no V*.sql files under {MIGRATIONS}"
    return max(versions)


async def _latest_applied(database: Database) -> str | None:
    conn = await asyncpg.connect(**database.params("ledger_api_owner"))
    try:
        return await conn.fetchval(
            "SELECT version FROM flyway_schema_history WHERE success AND version IS NOT NULL "
            "ORDER BY installed_rank DESC LIMIT 1"
        )
    finally:
        await conn.close()


def migrate(database: Database, jar: Path) -> None:
    """The Flyway-built schema: the jar in migrate-only mode must exit 0 (D-083)."""
    command = [java(), "-jar", str(jar), "--ledger.migrate-only=true", f"--spring.datasource.url={_jdbc_url(database)}"]
    try:
        result = subprocess.run(
            command, env=_jar_env(), capture_output=True, text=True, timeout=MIGRATE_TIMEOUT_SECONDS, check=False
        )
    except subprocess.TimeoutExpired as ex:
        pytest.fail(f"migrate-only run did not exit within {MIGRATE_TIMEOUT_SECONDS} s:\n{(ex.stdout or '')[-4000:]}")
    if result.returncode != 0:
        pytest.fail(f"migrate-only run exited {result.returncode}:\n{(result.stdout + result.stderr)[-4000:]}")
    latest = asyncio.run(_latest_applied(database))
    expected = _highest_migration()
    assert latest == str(expected), f"flyway_schema_history is at {latest}, highest migration file is V{expected}"


async def _wait_for_broker(bootstrap: str, deadline: float) -> None:
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        try:
            await admin.start()
            await admin.describe_cluster()
            return
        except Exception as ex:  # noqa: BLE001 - retried until the deadline
            last_error = ex
            await asyncio.sleep(0.5)
        finally:
            await admin.close()
    raise TimeoutError(f"Kafka at {bootstrap} not ready: {last_error!r}")


async def topic_partitions(bootstrap: str, topic: str) -> set[int] | None:
    """The topic's partition numbers from the broker's metadata, or None if it doesn't exist.

    The admin client, not a consumer: AIOKafkaConsumer.topics() fetches metadata without updating
    the client's cluster view, so partitions_for_topic() stays None for an unsubscribed topic.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        described = await admin.describe_topics([topic])
    finally:
        await admin.close()
    entry = next((t for t in described if t["topic"] == topic), None)
    if entry is None or entry["error_code"] != 0:
        return None
    return {p["partition"] for p in entry["partitions"]}


@contextmanager
def kafka():
    """apache/kafka:4.3.1, single-node KRaft, auto-create off, mirroring Compose (D-080, D-084).

    The broker listens on the same port number inside the container as it is published on
    (127.0.0.1 only), so the advertised address 127.0.0.1:P works from the host and inside the
    container alike, without rewriting a start script.
    """
    port = free_port()
    container = DockerContainer(KAFKA_IMAGE)
    for key, value in {
        "CLUSTER_ID": "LpxVmGQZTpuBR9TtWL2B8Q",
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_LISTENERS": f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://127.0.0.1:{port}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",  # every group's first join, T8-T11 (D-085)
        "KAFKA_NUM_PARTITIONS": "1",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
    }.items():
        container.with_env(key, value)
    # with_bind_ports only takes a host port (binding 0.0.0.0); docker-py accepts (ip, port).
    container.ports[f"{port}/tcp"] = ("127.0.0.1", port)
    container.waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(90))
    container.start()
    try:
        bootstrap = f"127.0.0.1:{port}"
        asyncio.run(_wait_for_broker(bootstrap, time.monotonic() + 60))
        yield Kafka(bootstrap_servers=bootstrap)
    finally:
        container.stop()


def _wait_for_health(base_url: str, process: subprocess.Popen, log_path: Path, *, deadline: float) -> None:
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"ledger-api exited {process.returncode}:\n{log_path.read_text(errors='replace')[-4000:]}")
        try:
            with urllib.request.urlopen(base_url + "/actuator/health", timeout=2) as response:
                if json.loads(response.read()).get("status") == "UP":
                    return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.25)
    pytest.fail(f"ledger-api not healthy at {base_url}:\n{log_path.read_text(errors='replace')[-4000:]}")


@contextmanager
def ledger_api(database: Database, jar: Path, log_dir: Path, *, relay_bootstrap: str | None = None):
    """The jar in server mode on 127.0.0.1 with one test API key (D-103).

    relay_bootstrap None: the outbox relay is off. Otherwise it runs on its 200 ms schedule against
    that broker (D-104). Only the key's SHA-256 reaches the jar (D-049); the key itself stays in
    this process and is never printed. The jar's output goes to a file, shown only on failure.
    """
    port = free_port()
    api_key = secrets.token_urlsafe(24)
    key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    log_path = log_dir / "ledger-api.log"
    command = [
        java(),
        "-jar",
        str(jar),
        "--server.address=127.0.0.1",
        f"--server.port={port}",
        f"--spring.datasource.url={_jdbc_url(database)}",
        f"--ledger.api-keys.{key_hash}=worker-tests",
    ]
    if relay_bootstrap is None:
        command += ["--ledger.outbox.relay.enabled=false", "--ledger.outbox.relay.scheduled=false"]
    else:
        command += [
            "--ledger.outbox.relay.enabled=true",
            "--ledger.outbox.relay.scheduled=true",
            "--ledger.outbox.relay.poll-interval=200ms",
            f"--ledger.outbox.relay.bootstrap-servers={relay_bootstrap}",
        ]
    with open(log_path, "wb") as log:
        process = subprocess.Popen(command, env=_jar_env(), stdout=log, stderr=subprocess.STDOUT)
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_health(base_url, process, log_path, deadline=time.monotonic() + 120)
        yield ApiClient(base_url, api_key)
    finally:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
