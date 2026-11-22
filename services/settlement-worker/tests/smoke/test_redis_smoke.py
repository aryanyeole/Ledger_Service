"""redis-py 8.1.0 against redis:7.4.11 (D-136), through the worker's own client factory (D-137).

The same assertions as ledger-api's RedisClientSmokeTest, under redis-py's default protocol: the server is the
D-027 pin, SET NX PX writes once, PTTL is in range, MGET returns None for an absent key, and the two scripts from
docs/design/cache-invalidation.md §2.1 (copied verbatim; T8 moves the invalidation script into main code with the
D-143 literal-string test) behave as the doc says. Compatibility is tested, not assumed: a failure here is a
stop-and-decide, never a server downgrade (D-084). Last, the client never retries: against a closed port it fails
within its connect timeout.
"""

import asyncio
import time
import uuid

import pytest
import redis.exceptions

from settlement_worker import balance_cache, redis_client
from support import stack

# The worker's own copy since T8, pinned against the design doc by tests/unit/test_balance_cache_script.py (D-143).
INVALIDATE = balance_cache.INVALIDATE

# ledger-api's populate script (the worker never populates), verbatim from the design doc §2.1.
POPULATE = """-- ledger:v1 balance populate (D-142). KEYS[1]: ledger:v1:bal:{id}; KEYS[2]: ledger:v1:bal-gen:{id}.
-- ARGV[1]: token seen by the MGET ('' if absent); ARGV[2]: the value; ARGV[3]: the value TTL in ms.
local token = redis.call('GET', KEYS[2])
if token == false then token = '' end
if token ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"""


@pytest.fixture(scope="module")
def redis_url():
    with stack.redis() as server:
        yield server.url


def run(url, body):
    """One client per test, closed afterwards; body(client) is a coroutine function."""

    async def main():
        client = redis_client.create(url)
        try:
            return await body(client)
        finally:
            await client.aclose()

    return asyncio.run(main())


def _id() -> str:
    return str(uuid.uuid4())


def test_server_is_the_d027_pin_and_the_default_protocol_is_resp3(redis_url):
    async def body(client):
        return await client.info("server"), await client.client_info()

    server, info = run(redis_url, body)
    assert server["redis_version"] == "7.4.11"
    assert str(info["resp"]) == "3", info


def test_set_nx_px_writes_once_and_pttl_is_in_range(redis_url):
    key = f"smoke:{_id()}"

    async def body(client):
        first = await client.set(key, "first", nx=True, px=60_000)
        second = await client.set(key, "second", nx=True, px=60_000)
        return first, second, await client.get(key), await client.pttl(key)

    first, second, value, pttl = run(redis_url, body)
    assert first is True
    assert second is None
    assert value == "first"
    assert 59_000 < pttl <= 60_000


def test_mget_returns_none_for_an_absent_key(redis_url):
    present, absent = f"smoke:{_id()}", f"smoke:{_id()}"

    async def body(client):
        await client.set(present, "v", px=60_000)
        return await client.mget(present, absent)

    assert run(redis_url, body) == ["v", None]


@pytest.mark.parametrize("accounts", [1, 3])
def test_invalidation_script(redis_url, accounts):
    ids = [_id() for _ in range(accounts)]
    keys = [k for i in ids for k in (f"ledger:v1:bal:{i}", f"ledger:v1:bal-gen:{i}")]
    fresh = _id()

    async def body(client):
        for i in ids:
            await client.set(f"ledger:v1:bal:{i}", "1|100.0000|2026-11-16T00:00:00.000000Z", px=60_000)
            await client.set(f"ledger:v1:bal-gen:{i}", _id(), px=300_000)
        reply = await client.eval(INVALIDATE, len(keys), *keys, fresh)
        after = [(await client.get(f"ledger:v1:bal:{i}"), await client.get(f"ledger:v1:bal-gen:{i}"),
                  await client.pttl(f"ledger:v1:bal-gen:{i}")) for i in ids]
        return reply, after

    reply, after = run(redis_url, body)
    assert reply == 2 * accounts
    for value, token, pttl in after:
        assert value is None
        assert token == fresh
        assert 299_000 < pttl <= 300_000


def test_populate_script_writes_only_while_the_token_is_unchanged(redis_url):
    entry = "1|42.0000|2026-11-16T00:00:00.000000Z"

    async def populate(client, account, seen):
        return await client.eval(POPULATE, 2, f"ledger:v1:bal:{account}", f"ledger:v1:bal-gen:{account}", seen, entry, "60000")

    async def body(client):
        results = {}
        a, token_a = _id(), _id()
        await client.set(f"ledger:v1:bal-gen:{a}", token_a, px=300_000)
        results["unchanged"] = (await populate(client, a, token_a), await client.get(f"ledger:v1:bal:{a}"),
                                await client.pttl(f"ledger:v1:bal:{a}"))
        b = _id()
        await client.set(f"ledger:v1:bal-gen:{b}", _id(), px=300_000)
        results["replaced"] = (await populate(client, b, _id()), await client.get(f"ledger:v1:bal:{b}"))
        c = _id()
        results["absent_then_absent"] = (await populate(client, c, ""), await client.get(f"ledger:v1:bal:{c}"))
        d = _id()
        await client.set(f"ledger:v1:bal-gen:{d}", _id(), px=300_000)
        results["absent_then_present"] = (await populate(client, d, ""), await client.get(f"ledger:v1:bal:{d}"))
        return results

    results = run(redis_url, body)
    written, value, pttl = results["unchanged"]
    assert (written, value) == (1, entry)
    assert 59_000 < pttl <= 60_000
    assert results["replaced"] == (0, None)
    assert results["absent_then_absent"] == (1, entry)
    assert results["absent_then_present"] == (0, None)


def test_a_closed_port_fails_within_the_connect_timeout_with_no_retries():
    url = f"redis://127.0.0.1:{stack.free_port()}"

    async def attempt():
        client = redis_client.create(url)
        started = time.monotonic_ns()
        try:
            # Linux refuses at once (ConnectionError); Windows retries the SYN until the connect timeout
            # (TimeoutError). Either way the client gives up after one attempt.
            with pytest.raises((redis.exceptions.ConnectionError, redis.exceptions.TimeoutError)):
                await client.ping()
            return (time.monotonic_ns() - started) // 1_000_000
        finally:
            await client.aclose()

    async def main():
        # Twice: the first includes one-time start-up.
        return [await attempt(), await attempt()]

    elapsed = asyncio.run(main())
    print(f"REDIS_CLOSED_PORT ping to a closed port failed after {elapsed} ms")
    assert max(elapsed) < redis_client.CONNECT_TIMEOUT_MS + 800, elapsed
