"""The reconciliation proof's stack (D-117): a fresh migrated Postgres, its own Kafka and the ledger-api
jar in server mode with the relay off, one per test module.

Drift is injected into this database, so it never shares a stack: the e2e stack asserts stack-wide
invariants (D-105). With the relay off nothing is published, so the worker under test consumes
nothing; it only reconciles. The topics are created as their owner would (D-080, D-110), because the
worker subscribes to ledger.payments.v1 and produces dead letters, and auto-create is off.
"""

import asyncio
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
