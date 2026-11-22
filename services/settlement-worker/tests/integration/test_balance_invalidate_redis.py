"""balance_cache.invalidate() against a real redis:7.4.11 (D-142, D-152), through the worker's own client factory.

(1) Four accounts with cached values and old tokens: one call deletes every value and sets every token to the
call's UUID with PX 300000. (2) Redis stopped: the call is "failed" within INVALIDATION_BOUND_MS plus slack and
raises nothing. Each case has its own empty Redis (stack.redis() asserts DBSIZE = 0, D-138). The end-to-end proof
through the consumer is T9's tests/cache/test_balance_invalidation.py.
"""

import asyncio
import time
import uuid

import docker

from settlement_worker import balance_cache, redis_client
from support import stack

ACCOUNTS = [41, 7, 1003, 2]


def test_one_call_deletes_every_value_and_sets_every_token(monkeypatch):
    token = uuid.uuid4()
    monkeypatch.setattr(balance_cache.uuid, "uuid4", lambda: token)

    async def main(url):
        client = redis_client.create(url)
        try:
            for account in ACCOUNTS:
                await client.set(f"ledger:v1:bal:{account}", "1|100.0000|2026-11-16T00:00:00.000000Z", px=60_000)
                await client.set(f"ledger:v1:bal-gen:{account}", str(uuid.uuid4()), px=300_000)
            outcome = await balance_cache.invalidate(client, ACCOUNTS)
            after = [
                (
                    await client.get(f"ledger:v1:bal:{account}"),
                    await client.get(f"ledger:v1:bal-gen:{account}"),
                    await client.pttl(f"ledger:v1:bal-gen:{account}"),
                )
                for account in ACCOUNTS
            ]
            return outcome, after, await client.dbsize()
        finally:
            await client.aclose()

    with stack.redis() as server:
        outcome, after, keys = asyncio.run(main(server.url))
    assert outcome == balance_cache.Invalidation("sent")
    for value, account_token, pttl in after:
        assert value is None
        assert account_token == str(token)
        assert 299_000 < pttl <= 300_000
    assert keys == len(ACCOUNTS), "only the four tokens remain"


def test_redis_stopped_is_failed_within_the_bound_and_raises_nothing():
    async def main(url):
        client = redis_client.create(url)
        try:
            started = time.monotonic_ns()
            outcome = await balance_cache.invalidate(client, ACCOUNTS)
            return outcome, (time.monotonic_ns() - started) // 1_000_000
        finally:
            await client.aclose()

    with stack.redis() as server:
        docker.from_env().containers.get(server.container_id).stop(timeout=0)
        outcome, elapsed_ms = asyncio.run(main(server.url))
    print(f"INVALIDATION_REDIS_STOPPED outcome={outcome} elapsed_ms={elapsed_ms}")
    assert outcome.result == "failed"
    assert outcome.error_class is not None
    assert elapsed_ms < balance_cache.INVALIDATION_BOUND_MS + 800, elapsed_ms
