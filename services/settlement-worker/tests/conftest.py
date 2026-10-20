"""Session-scoped test infrastructure for the settlement worker's integration tests (D-083, D-084, D-103).

The builders live in support/stack.py; the e2e package builds its own stack with them
(tests/e2e/conftest.py, D-104). Fixtures are created on first use, so tests that need neither
Postgres nor Kafka start no containers. Tests are synchronous and drive coroutines with
asyncio.run (no pytest-asyncio).
"""

from pathlib import Path

import pytest

from support import stack
from support.api import ApiClient


@pytest.fixture(scope="session")
def ledger_api_jar() -> Path:
    return stack.find_jar()


@pytest.fixture(scope="session")
def postgres():
    with stack.postgres() as database:
        yield database


@pytest.fixture(scope="session")
def migrated_db(postgres: stack.Database, ledger_api_jar: Path) -> stack.Database:
    stack.migrate(postgres, ledger_api_jar)
    return postgres


@pytest.fixture(scope="session")
def worker_db(migrated_db: stack.Database) -> stack.WorkerDb:
    return stack.WorkerDb(migrated_db)


@pytest.fixture(scope="session")
def app_db(migrated_db: stack.Database) -> stack.AppDb:
    return stack.AppDb(migrated_db)


@pytest.fixture(scope="session")
def connect_params(migrated_db: stack.Database):
    """asyncpg.connect keyword arguments for a role, for tests that check role handling."""
    return migrated_db.params


@pytest.fixture(scope="session")
def ledger_api(migrated_db: stack.Database, ledger_api_jar: Path, tmp_path_factory) -> ApiClient:
    """The jar in server mode, relay off (D-103)."""
    with stack.ledger_api(migrated_db, ledger_api_jar, tmp_path_factory.mktemp("ledger-api")) as api:
        yield api


@pytest.fixture(scope="session")
def kafka():
    with stack.kafka() as broker:
        yield broker
