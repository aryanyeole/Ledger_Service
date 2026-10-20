"""Session-scoped test infrastructure for the settlement worker (D-083, D-084).

Postgres gets its schema from the real ledger-api jar in migrate-only mode, so the worker is
tested against exactly the Flyway-built schema, roles and grants. Kafka is a generic
apache/kafka container: testcontainers' Kafka module only knows the Confluent image.
Fixtures are created on first use, so tests that need neither start no containers.
Tests are synchronous and drive coroutines with asyncio.run (no pytest-asyncio).
"""

import asyncio
import os
import re
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import pytest
from aiokafka.admin import AIOKafkaAdminClient
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

REPO = Path(__file__).resolve().parents[3]
LEDGER_API = REPO / "services" / "ledger-api"
ROLES_SCRIPT = REPO / "infra" / "postgres" / "init" / "01-roles.sh"
MIGRATIONS = LEDGER_API / "src" / "main" / "resources" / "db" / "migration"

POSTGRES_IMAGE = "postgres:17.11"  # D-027
KAFKA_IMAGE = "apache/kafka:4.3.1"  # D-027

# Test-only credentials for throwaway containers, as in ledger-api's TestcontainersConfiguration.
SUPERUSER_PASSWORD = "test-superuser-password"
OWNER_PASSWORD = "test-owner-password"
APP_PASSWORD = "test-app-password"
WORKER_PASSWORD = "test-worker-password"

PACKAGE_HINT = r"run .\mvnw.cmd -q -DskipTests package in services\ledger-api"
MIGRATE_TIMEOUT_SECONDS = 120


@dataclass(frozen=True)
class Database:
    host: str
    port: int
    name: str = "ledger"


@dataclass(frozen=True)
class WorkerDb:
    """Connections as settlement_worker, checked to be that role and not a superuser (D-036)."""

    database: Database

    async def connect(self) -> asyncpg.Connection:
        conn = await asyncpg.connect(
            host=self.database.host,
            port=self.database.port,
            database=self.database.name,
            user="settlement_worker",
            password=WORKER_PASSWORD,
        )
        try:
            row = await conn.fetchrow(
                "SELECT current_user AS who, rolsuper FROM pg_roles WHERE rolname = current_user"
            )
            assert row["who"] == "settlement_worker", f"current_user is {row['who']}"
            assert row["rolsuper"] is False, "settlement_worker must not be a superuser"
        except BaseException:
            await conn.close()
            raise
        return conn


@dataclass(frozen=True)
class Kafka:
    bootstrap_servers: str


def _host(container) -> str:
    # Windows resolves localhost to ::1 first; the published ports are IPv4.
    host = container.get_container_host_ip()
    return "127.0.0.1" if host == "localhost" else host


@pytest.fixture(scope="session")
def ledger_api_jar() -> Path:
    """The ledger-api jar, refused if missing or older than its sources (D-083)."""
    jars = [p for p in (LEDGER_API / "target").glob("ledger-api-*.jar")]
    if len(jars) != 1:
        pytest.fail(f"expected one ledger-api jar in {LEDGER_API / 'target'}, found {jars}: {PACKAGE_HINT}")
    jar = jars[0]
    sources = [p for p in (LEDGER_API / "src" / "main").rglob("*") if p.is_file()] + [LEDGER_API / "pom.xml"]
    newest = max(sources, key=lambda p: p.stat().st_mtime)
    if jar.stat().st_mtime < newest.stat().st_mtime:
        pytest.fail(f"{jar.name} is older than {newest.relative_to(REPO)}: {PACKAGE_HINT}")
    return jar


@pytest.fixture(scope="session")
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


def _java() -> str:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("java")
    if not found:
        pytest.fail("java not found (set JAVA_HOME or put Java 21 on PATH)")
    return found


def _highest_migration() -> int:
    versions = [int(m.group(1)) for p in MIGRATIONS.glob("V*.sql") if (m := re.match(r"V(\d+)__", p.name))]
    assert versions, f"no V*.sql files under {MIGRATIONS}"
    return max(versions)


async def _latest_applied(database: Database) -> str | None:
    conn = await asyncpg.connect(
        host=database.host, port=database.port, database=database.name, user="ledger_api_owner", password=OWNER_PASSWORD
    )
    try:
        return await conn.fetchval(
            "SELECT version FROM flyway_schema_history WHERE success AND version IS NOT NULL "
            "ORDER BY installed_rank DESC LIMIT 1"
        )
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def migrated_db(postgres: Database, ledger_api_jar: Path) -> Database:
    """The Flyway-built schema: the jar in migrate-only mode must exit 0 (D-083)."""
    env = dict(os.environ, LEDGER_API_APP_PASSWORD=APP_PASSWORD, LEDGER_API_OWNER_PASSWORD=OWNER_PASSWORD)
    command = [
        _java(),
        "-jar",
        str(ledger_api_jar),
        "--ledger.migrate-only=true",
        f"--spring.datasource.url=jdbc:postgresql://{postgres.host}:{postgres.port}/{postgres.name}",
    ]
    try:
        result = subprocess.run(
            command, env=env, capture_output=True, text=True, timeout=MIGRATE_TIMEOUT_SECONDS, check=False
        )
    except subprocess.TimeoutExpired as ex:
        pytest.fail(f"migrate-only run did not exit within {MIGRATE_TIMEOUT_SECONDS} s:\n{(ex.stdout or '')[-4000:]}")
    if result.returncode != 0:
        pytest.fail(f"migrate-only run exited {result.returncode}:\n{(result.stdout + result.stderr)[-4000:]}")
    latest = asyncio.run(_latest_applied(postgres))
    expected = _highest_migration()
    assert latest == str(expected), f"flyway_schema_history is at {latest}, highest migration file is V{expected}"
    return postgres


@pytest.fixture(scope="session")
def worker_db(migrated_db: Database) -> WorkerDb:
    return WorkerDb(migrated_db)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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


@pytest.fixture(scope="session")
def kafka():
    """apache/kafka:4.3.1, single-node KRaft, auto-create off, mirroring Compose (D-080, D-084).

    The broker listens on the same port number inside the container as it is published on
    (127.0.0.1 only), so the advertised address 127.0.0.1:P works from the host and inside the
    container alike, without rewriting a start script.
    """
    port = _free_port()
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
