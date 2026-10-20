"""The negative control's own stack (D-086, D-104): a fresh migrated Postgres, its own Kafka, and the
ledger-api jar with the outbox relay on, built per module and shared with nothing else, so its
deliberate duplicates can't leak into another test's ledger and the stack-wide escrow check is exact.
"""

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from support import stack
from support.api import ApiClient


@dataclass(frozen=True)
class ControlStack:
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


@pytest.fixture(scope="module")
def control(tmp_path_factory) -> ControlStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("negative-control")
    with stack.postgres() as database, stack.kafka() as broker:
        stack.migrate(database, jar)
        with stack.ledger_api(database, jar, log_dir, relay_bootstrap=broker.bootstrap_servers) as api:
            assert _wait_for_topic(broker.bootstrap_servers, stack.TOPIC) == {0, 1, 2}
            yield ControlStack(database, broker, api, stack.AppDb(database), stack.WorkerDb(database), log_dir)
