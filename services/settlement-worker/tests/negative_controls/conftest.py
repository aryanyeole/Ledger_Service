"""The negative controls' own stacks, built per module and shared with nothing else.

control (D-086, D-104): a fresh migrated Postgres, its own Kafka, and the ledger-api jar with the
outbox relay on, so the dedup control's deliberate duplicates can't leak into another test's ledger
and the stack-wide escrow check is exact.

two_query_stack (D-118): a fresh migrated Postgres and the jar with the relay off. No Kafka: nothing
is published or consumed, and the jar starts without a broker when its relay is off.
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


@dataclass(frozen=True)
class TwoQueryStack:
    database: stack.Database
    api: ApiClient
    app_db: stack.AppDb
    worker_db: stack.WorkerDb


@pytest.fixture(scope="module")
def two_query_stack(tmp_path_factory) -> TwoQueryStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("two-query-control")
    with stack.postgres() as database:
        stack.migrate(database, jar)
        with stack.ledger_api(database, jar, log_dir) as api:
            yield TwoQueryStack(database, api, stack.AppDb(database), stack.WorkerDb(database))
