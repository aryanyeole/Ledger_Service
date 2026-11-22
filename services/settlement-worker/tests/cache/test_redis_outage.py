"""A Redis outage never retries, dead-letters or stops settlement (D-135, D-137, D-152; docs/design/cache-invalidation.md
§1.4, §5 rows 1, 2, 2c, 5).

Its own stack (conftest outage_cs: Postgres, Kafka, Redis, the jar with the relay on, both caches on, balance TTL 3 s).
stack.redis() binds an explicit free host port, so a stop/start keeps REDIS_URL valid: each restart asserts that the
same URL answers. Pause/unpause and stop/start go through the Docker API, never a restart. Every case has a fresh
consumer group committed at the topic's end (D-123) and stops its worker at the end (exit 0).

(a) Redis stopped while a worker settles: every payment settles, every invalidation fails (one cache_invalidation_failed
per payment, the tripwire that Redis really was down for each), and nothing is retried or dead-lettered; once Redis is
back on the same port, the next settle's invalidation is sent without a worker restart. (b1) the same with Redis paused,
plus each invalidation's duration from the worker's own log timestamps (payment_settled/payment_released to
cache_invalidation_failed: the invalidation is the only thing between them) under INVALIDATION_BOUND_MS + 800 ms; Redis
is flushed first and no balance is read during the outage, so after it every value present was populated afterwards and
must equal the DB. (b2) primed balances and a pause: the worker's redis-py pool sends at most the first invalidation
(later ones open a new connection whose HELLO hangs), so values stay stale, and live at most the 3 s TTL (§5 row 5). (c)
a worker started while Redis is stopped reports balance_invalidation "on" (the client doesn't connect at startup) and
settles as in (a).
"""

import asyncio
import time
import uuid
from decimal import Decimal

import docker

from support import cache_kit
from support.failure_kit import fresh_group
from support.worker_process import start_worker, wait_quiescent

APPROVED = ("10.00", "11.00", "12.00", "13.00", "14.00")
DECLINED = "10.13"  # cents 13: the fake processor declines (CENTS_13 lists it)
AMOUNTS = APPROVED + (DECLINED,) * 5
BOUND_SLACK_MS = 800
INVALIDATION_BOUND_MS = 300  # balance_cache.INVALIDATION_BOUND_MS, asserted below
FORBIDDEN = ("retry_scheduled", "message_dead_lettered", "processing_failed")
ROLES = ("customer", "merchant", "escrow", "fees")
SETTLE_EVENTS = ("payment_settled", "payment_released")


def run(coroutine):
    return asyncio.run(coroutine)


def _container(cs):
    return docker.from_env().containers.get(cs.redis.container_id)


def _answers(cs, timeout: float = 30) -> None:
    """The same REDIS_URL answers a PING (after a restart: same port, D-138's empty start)."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if cache_kit.raw_redis(cs.redis.url, lambda client: client.ping()):
                return
        except Exception as ex:  # noqa: BLE001 - not up yet
            last = ex
        time.sleep(0.2)
    raise AssertionError(f"{cs.redis.url} didn't answer within {timeout} s: {last!r}")


def _ensure_running(cs) -> None:
    container = _container(cs)
    container.reload()
    if container.status == "paused":
        container.unpause()
    elif container.status != "running":
        container.start()
    _answers(cs)


def _start(cs, group: str):
    worker = start_worker(
        database=cs.database,
        bootstrap_servers=cs.kafka.bootstrap_servers,
        group=group,
        log_dir=cs.log_dir,
        extra_env={"REDIS_URL": cs.redis.url},
    )
    started = worker.wait_for("worker_started")
    assert started["balance_invalidation"] == "on", worker.diagnostics()
    return worker


def _pair(cs) -> tuple[int, int]:
    customer = cs.api.create_account("CUSTOMER")
    cs.api.deposit(customer, "10000.00")
    return customer, cs.api.create_account("MERCHANT")


def _authorize(cs, customer: int, merchant: int, amounts) -> dict[uuid.UUID, str]:
    """payment id -> the status its amount should end in."""
    return {
        cs.api.authorize(customer, merchant, amount): ("FAILED" if amount == DECLINED else "SETTLED")
        for amount in amounts
    }


def _all_terminal(cs, payments: dict[uuid.UUID, str]) -> None:
    for payment, expected in payments.items():
        cache_kit.wait_status(cs.api, payment, expected)


def _wait_count(worker, result: str, n: int, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and cache_kit.invalidations(worker, result) < n:
        time.sleep(0.1)
    assert cache_kit.invalidations(worker, result) == n, (
        f"invalidations{{{result}}} = {cache_kit.invalidations(worker, result)}, expected {n}"
    )


def _quiescent(cs, group: str, payments) -> None:
    async def wait():
        conn = await cs.app_db.connect()
        try:
            return await wait_quiescent(
                app_conn=conn, bootstrap_servers=cs.kafka.bootstrap_servers, group=group, payment_ids=list(payments)
            )
        finally:
            await conn.close()

    lag = run(wait())
    assert all(v == 0 for v in lag.values()), lag


def _every_payment_failed_once(worker, payments) -> None:
    for payment in payments:
        events = worker.events("cache_invalidation_failed", payment_id=str(payment))
        assert len(events) == 1, f"{payment}: {len(events)} cache_invalidation_failed\n{worker.diagnostics()}"


def _clean(*workers) -> None:
    for worker in workers:
        for name in FORBIDDEN:
            assert worker.events(name) == [], f"{name}:\n{worker.diagnostics()}"


async def _system_id(cs, name: str) -> int:
    conn = await cs.app_db.connect()
    try:
        return await conn.fetchval("SELECT id FROM accounts WHERE name = $1", name)
    finally:
        await conn.close()


async def _db_balance(cs, account_id: int) -> Decimal:
    conn = await cs.app_db.connect()
    try:
        return await conn.fetchval("SELECT balance FROM accounts WHERE id = $1", account_id)
    finally:
        await conn.close()


def _accounts(cs, customer: int, merchant: int) -> dict[str, int]:
    return {
        "customer": customer,
        "merchant": merchant,
        "escrow": run(_system_id(cs, "system:escrow")),
        "fees": run(_system_id(cs, "system:platform_fees")),
    }


def _raw_values(cs, accounts: dict[str, int]) -> dict[str, tuple[str | None, str | None]]:
    async def body(client):
        return {
            role: (await client.get(f"ledger:v1:bal:{account}"), await client.get(f"ledger:v1:bal-gen:{account}"))
            for role, account in accounts.items()
        }

    return cache_kit.raw_redis(cs.redis.url, body)


def _reads_equal_the_db(cs, accounts: dict[str, int], label: str) -> int:
    """Every touched account's GET equals the DB; every present raw value equals the DB. Returns the present count."""
    for role, account in accounts.items():
        balance, cache = cs.api.balance(account)
        assert Decimal(balance) == run(_db_balance(cs, account)), f"{label}: {role} read {cache} {balance}"
    present = 0
    for role, (value, _token) in _raw_values(cs, accounts).items():
        if value is None:
            continue
        present += 1
        assert Decimal(value.split("|")[1]) == run(_db_balance(cs, accounts[role])), f"{label}: {role} cached {value}"
    return present


def test_the_bound_is_the_workers():
    from settlement_worker import balance_cache

    assert balance_cache.INVALIDATION_BOUND_MS == INVALIDATION_BOUND_MS


# (a)
def test_redis_stopped_while_settling(outage_cs):
    cs = outage_cs
    _ensure_running(cs)
    customer, merchant = _pair(cs)
    group = fresh_group(cs, f"outage-stopped-{uuid.uuid4().hex[:8]}")
    worker = _start(cs, group)
    with worker:
        _container(cs).stop(timeout=0)
        try:
            payments = _authorize(cs, customer, merchant, AMOUNTS)
            _all_terminal(cs, payments)
            _wait_count(worker, "failed", len(payments))
            _quiescent(cs, group, payments)
            assert cache_kit.invalidations(worker, "sent") == 0
            _every_payment_failed_once(worker, payments)
        finally:
            _container(cs).start()
        _answers(cs)

        # The client recovers without a worker restart: the next settle's invalidation is sent.
        recovered = _authorize(cs, customer, merchant, ("15.00",))
        _all_terminal(cs, recovered)
        _wait_count(worker, "sent", 1)
        assert worker.stop() == 0
    _clean(worker)


# (b1)
def test_redis_paused_while_settling(outage_cs):
    cs = outage_cs
    _ensure_running(cs)
    cache_kit.raw_redis(cs.redis.url, lambda client: client.flushall())
    customer, merchant = _pair(cs)
    accounts = _accounts(cs, customer, merchant)
    group = fresh_group(cs, f"outage-paused-{uuid.uuid4().hex[:8]}")
    worker = _start(cs, group)
    with worker:
        _container(cs).pause()
        try:
            payments = _authorize(cs, customer, merchant, AMOUNTS)
            _all_terminal(cs, payments)
            _wait_count(worker, "failed", len(payments))
            _quiescent(cs, group, payments)
        finally:
            _container(cs).unpause()
        _answers(cs)
        assert cache_kit.invalidations(worker, "sent") == 0
        _every_payment_failed_once(worker, payments)

        durations, settles = [], []
        for payment in payments:
            settled = next(e for name in SETTLE_EVENTS for e in worker.events(name, payment_id=str(payment)))
            failed = worker.events("cache_invalidation_failed", payment_id=str(payment))[0]
            called = worker.events("processor_called", payment_id=str(payment))[0]
            durations.append((cache_kit.timestamp(failed) - cache_kit.timestamp(settled)).total_seconds() * 1000)
            settles.append((cache_kit.timestamp(failed) - cache_kit.timestamp(called)).total_seconds() * 1000)
        print(f"REDIS_OUTAGE_WORKER paused invalidationMs={[round(d) for d in durations]} "
              f"settleMs={[round(s) for s in settles]} errorClasses="
              f"{sorted({e['error_class'] for e in worker.events('cache_invalidation_failed')})}")
        bound = INVALIDATION_BOUND_MS + BOUND_SLACK_MS
        assert max(durations) < bound, f"an invalidation took {max(durations):.0f} ms (bound {bound} ms): {durations}"

        # Flushed first, nothing read during the outage: every value present now was populated after it.
        present = _reads_equal_the_db(cs, accounts, "after the pause")
        print(f"REDIS_OUTAGE_WORKER paused presentBalances={present}/{len(accounts)}")
        assert worker.stop() == 0
    _clean(worker)


# (b2) §5 rows 2c and 5: lost worker invalidations leave primed values stale, for at most the TTL (3 s here).
def test_a_pause_leaves_primed_balances_stale_for_at_most_the_ttl(outage_cs):
    cs = outage_cs
    _ensure_running(cs)
    customer, merchant = _pair(cs)
    accounts = _accounts(cs, customer, merchant)
    group = fresh_group(cs, f"outage-stale-{uuid.uuid4().hex[:8]}")
    worker = _start(cs, group)
    with worker:
        worker.wait_for("partitions_assigned")
        for role, account in accounts.items():
            cs.api.balance(account)
            _balance, cache = cs.api.balance(account)
            assert cache == "hit", f"priming: {role}'s second read must be a hit (it was {cache})"
        primed = _raw_values(cs, accounts)
        _container(cs).pause()
        paused_at = time.monotonic()
        try:
            payments = _authorize(cs, customer, merchant, APPROVED[:3] + (DECLINED,) * 2)
            _all_terminal(cs, payments)
            _wait_count(worker, "failed", len(payments))
        finally:
            _container(cs).unpause()
        outage_s = time.monotonic() - paused_at
        _answers(cs)

        after = _raw_values(cs, accounts)
        stale = [
            role
            for role, (value, _token) in after.items()
            if value is not None and Decimal(value.split("|")[1]) != run(_db_balance(cs, accounts[role]))
        ]
        delivered = [role for role in ROLES if after[role][1] != primed[role][1]]
        print(f"REDIS_OUTAGE_WORKER stale-window outageS={outage_s:.1f} staleRightAfterUnpause={stale} "
              f"tokensChanged={delivered} (the API's own invalidations execute in order on resume; at most the "
              f"worker's first EVAL reaches the server)")

        time.sleep(3 + 2)  # past the 3 s TTL, plus slack
        present = _reads_equal_the_db(cs, accounts, "past the TTL")
        print(f"REDIS_OUTAGE_WORKER stale-window presentAfterTtl={present}/{len(accounts)}")
        _quiescent(cs, group, payments)
        assert worker.stop() == 0
    _clean(worker)


# (c)
def test_a_worker_started_while_redis_is_stopped(outage_cs):
    cs = outage_cs
    _ensure_running(cs)
    customer, merchant = _pair(cs)
    group = fresh_group(cs, f"outage-cold-{uuid.uuid4().hex[:8]}")
    _container(cs).stop(timeout=0)
    try:
        worker = _start(cs, group)  # asserts balance_invalidation "on": the client doesn't connect at startup
        with worker:
            payments = _authorize(cs, customer, merchant, AMOUNTS)
            _all_terminal(cs, payments)
            _wait_count(worker, "failed", len(payments))
            _quiescent(cs, group, payments)
            assert cache_kit.invalidations(worker, "sent") == 0
            _every_payment_failed_once(worker, payments)
            assert worker.stop() == 0
        _clean(worker)
    finally:
        _container(cs).start()
    _answers(cs)
