"""The e2e package's own stack (D-104): a fresh migrated Postgres, its own Kafka, and the ledger-api
jar in server mode with the outbox relay on its 200 ms schedule.

Separate from the integration session's database and broker: those hold authorized payments and
unpublished outbox rows an e2e relay would publish, and every e2e worker reads the topic from the
earliest offset in a fresh group, so a shared broker would hand it events for payments this
database doesn't have.
"""

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from support import stack
from support.api import ApiClient


@dataclass(frozen=True)
class E2EStack:
    database: stack.Database
    kafka: stack.Kafka
    api: ApiClient
    app_db: stack.AppDb
    worker_db: stack.WorkerDb
    log_dir: Path


def _wait_for_topic(bootstrap: str, topic: str, timeout: float = 60) -> set[int]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        partitions = asyncio.run(stack.topic_partitions(bootstrap, topic))
        if partitions:
            return partitions
        time.sleep(0.25)
    raise AssertionError(f"the relay did not create {topic} within {timeout} s")


@pytest.fixture(scope="package")
def e2e(tmp_path_factory) -> E2EStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("e2e")
    with stack.postgres() as database, stack.kafka() as broker:
        stack.migrate(database, jar)
        with stack.ledger_api(database, jar, log_dir, relay_bootstrap=broker.bootstrap_servers) as api:
            # The relay owns the topic (D-080): it creates it before its first batch.
            assert _wait_for_topic(broker.bootstrap_servers, stack.TOPIC) == {0, 1, 2}
            yield E2EStack(database, broker, api, stack.AppDb(database), stack.WorkerDb(database), log_dir)
