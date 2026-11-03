"""The reconciliation proofs' stacks, one per test module that asks for one: a fresh migrated Postgres,
its own Kafka and the ledger-api jar in server mode.

rs (D-117): the relay off. Drift is injected into this database, so it never shares a stack: the e2e
stack asserts stack-wide invariants (D-105). With the relay off nothing is published, so the worker
under test consumes nothing; it only reconciles. The topics are created as their owner would (D-080,
D-110), because the worker subscribes to ledger.payments.v1 and produces dead letters, and
auto-create is off.

relay_rs (D-118): the relay on its 200 ms schedule, so the worker under test settles while it
reconciles. The relay is the topics' owner and creates both itself: ledger.payments.v1 before its
first batch (D-080), the DLQ after every tick until it is verified, an empty one included (D-110).
"""

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from support import stack
from support.api import ApiClient


@dataclass(frozen=True)
class ReconciliationStack:
    database: stack.Database
    kafka: stack.Kafka
    api: ApiClient
    app_db: stack.AppDb
    worker_db: stack.WorkerDb
    log_dir: Path


@pytest.fixture(scope="module")
def rs(tmp_path_factory) -> ReconciliationStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("reconciliation")
    with stack.postgres() as database, stack.kafka() as broker:
        stack.migrate(database, jar)
        asyncio.run(
            stack.create_topics(
                broker.bootstrap_servers,
                {stack.TOPIC: (3, {}), stack.DLQ_TOPIC: (1, {"retention.ms": "-1"})},
            )
        )
        with stack.ledger_api(database, jar, log_dir) as api:
            yield ReconciliationStack(database, broker, api, stack.AppDb(database), stack.WorkerDb(database), log_dir)


def _wait_for_topics(bootstrap: str, expected: dict[str, set[int]], timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    found: dict = {}
    while time.monotonic() < deadline:
        found = {topic: asyncio.run(stack.topic_partitions(bootstrap, topic)) for topic in expected}
        if found == expected:
            return
        time.sleep(0.25)
    raise AssertionError(f"the relay did not create {expected} within {timeout} s: {found}")


@pytest.fixture(scope="module")
def relay_rs(tmp_path_factory) -> ReconciliationStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("reconciliation-relay")
    with stack.postgres() as database, stack.kafka() as broker:
        stack.migrate(database, jar)
        with stack.ledger_api(database, jar, log_dir, relay_bootstrap=broker.bootstrap_servers) as api:
            _wait_for_topics(broker.bootstrap_servers, {stack.TOPIC: {0, 1, 2}, stack.DLQ_TOPIC: {0}})
            yield ReconciliationStack(database, broker, api, stack.AppDb(database), stack.WorkerDb(database), log_dir)
