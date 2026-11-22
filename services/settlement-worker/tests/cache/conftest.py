"""The cross-language balance invalidation proof's stack (D-123, D-145): one per test module that asks for one.

A fresh migrated Postgres, its own Kafka, its own Redis (stack.redis(): DBSIZE = 0, D-138) and the ledger-api jar
in server mode with the outbox relay on its 200 ms schedule (the relay creates both topics, D-080, D-110) and both
caches on against that Redis, with ledger.cache.balance.ttl=10m: no cached value can expire during a case, so a
fresh read after a settle can only come from an invalidation. Workers get this stack's REDIS_URL explicitly (the
harness strips REDIS_ from the caller's environment, D-138).
"""

from dataclasses import dataclass
from pathlib import Path

import pytest

from support import stack
from support.api import ApiClient

BALANCE_TTL_ARG = "--ledger.cache.balance.ttl=10m"


@dataclass(frozen=True)
class CacheStack:
    database: stack.Database
    kafka: stack.Kafka
    redis: stack.Redis
    api: ApiClient
    app_db: stack.AppDb
    log_dir: Path


@pytest.fixture(scope="module")
def cs(tmp_path_factory) -> CacheStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("cache")
    with stack.postgres() as database, stack.kafka() as broker, stack.redis() as redis:
        stack.migrate(database, jar)
        with stack.ledger_api(
            database,
            jar,
            log_dir,
            relay_bootstrap=broker.bootstrap_servers,
            redis_url=redis.url,
            extra_args=[BALANCE_TTL_ARG],
        ) as api:
            stack.wait_for_topics(broker.bootstrap_servers, {stack.TOPIC: {0, 1, 2}, stack.DLQ_TOPIC: {0}})
            yield CacheStack(database, broker, redis, api, stack.AppDb(database), log_dir)


# The Redis outage proof's stack (T11): the same shape, its own Redis (stack.redis() binds an explicit free host
# port, so a stop/start keeps REDIS_URL valid), and a 3 s balance TTL, so (b2) can show that values a lost worker
# invalidation left stale live at most the TTL (§5 row 5).
OUTAGE_BALANCE_TTL_ARG = "--ledger.cache.balance.ttl=3s"


@pytest.fixture(scope="module")
def outage_cs(tmp_path_factory) -> CacheStack:
    jar = stack.find_jar()
    log_dir = tmp_path_factory.mktemp("cache-outage")
    with stack.postgres() as database, stack.kafka() as broker, stack.redis() as redis:
        stack.migrate(database, jar)
        with stack.ledger_api(
            database,
            jar,
            log_dir,
            relay_bootstrap=broker.bootstrap_servers,
            redis_url=redis.url,
            extra_args=[OUTAGE_BALANCE_TTL_ARG],
        ) as api:
            stack.wait_for_topics(broker.bootstrap_servers, {stack.TOPIC: {0, 1, 2}, stack.DLQ_TOPIC: {0}})
            yield CacheStack(database, broker, redis, api, stack.AppDb(database), log_dir)
