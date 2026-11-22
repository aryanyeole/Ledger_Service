"""Cross-language balance invalidation (D-142, D-143, D-148, D-152; docs/design/cache-invalidation.md §2.1, §3.7,
§5 rows 7 and 14): the Python worker invalidates the balances ledger-api caches, under the key names ledger-api
reads.

Priming (every case): the API's authorize invalidates customer and ESCROW itself, so no worker runs during the
authorize. Then each of customer, merchant, ESCROW and PLATFORM_FEES is read twice; the second read must be
X-Cache: hit (the tripwire that the values really are cached). Then the worker starts, the payment reaches its
terminal status (GET /v1/payments/{id}, a DB read), and the worker's settlement_balance_invalidations_total
{result="sent"} reaches exactly 1: the status is visible from the DB commit on, before the worker's EVAL, so the
status alone would race. The case's own payment.settled/payment.failed is committed without settlement.handle
(D-081) and never invalidates, so 1 is exact. The balance TTL is 10 min (conftest), so no fresh read can come from
expiry.

(a) an approved settle: all four reads are misses equal to the DB; every value key gone, every token new.
(b) a declined release: likewise, all four invalidated. (c) the D-148 crash window: the worker dies after the commit
and before the invalidation; the merchant's stale hit is observed; a restarted worker in the same group gets the
redelivery, duplicate_skipped invalidates, and all four read fresh. (d) the keys the worker touched are exactly the
ones the design doc's §2.1 key table names (the table ledger-api's BalanceCacheScripts follows), read from the doc,
not a third copy. Each case has a fresh consumer group committed at the topic's end (D-123) and stops its worker
(exit 0), so no worker outlives its case and touches ESCROW or fees in the next one.
"""

import asyncio
import re
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from support import cache_kit
from support.failure_kit import fresh_group
from support.worker_process import start_worker

DOC = Path(__file__).resolve().parents[4] / "docs" / "design" / "cache-invalidation.md"
CANONICAL_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
ROLES = ("customer", "merchant", "escrow", "fees")
SENT_TIMEOUT_SECONDS = 30
FORBIDDEN = ("retry_scheduled", "message_dead_lettered", "processing_failed")


def _key_templates() -> tuple[str, str]:
    """(value key, token key) templates from the §2.1 key table, e.g. ledger:v1:bal:{id}."""
    doc = DOC.read_text(encoding="utf-8").replace("\r\n", "\n")
    # The table's balance rows: | `ledger:v1:bal:{id}` | ... and | `ledger:v1:bal-gen:{id}` | ...
    names = re.findall(r"^\| `(ledger:v\d+:bal(?:-gen)?:\{id\})` \|", doc, re.MULTILINE)
    values = [name for name in names if ":bal:" in name]
    tokens = [name for name in names if ":bal-gen:" in name]
    assert len(values) == 1 and len(tokens) == 1, f"§2.1 key table balance rows: {names}"
    return values[0], tokens[0]


VALUE_KEY, TOKEN_KEY = _key_templates()


@dataclass
class Accounts:
    ids: dict[str, int]

    def __getitem__(self, role: str) -> int:
        return self.ids[role]


@dataclass
class Primed:
    balances: dict[str, Decimal]
    tokens: dict[str, str | None]


def run(coroutine):
    return asyncio.run(coroutine)


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


async def _payment(cs, payment_id: uuid.UUID) -> tuple[Decimal, Decimal]:
    conn = await cs.app_db.connect()
    try:
        row = await conn.fetchrow("SELECT amount, fee FROM payments WHERE id = $1", payment_id)
        return row["amount"], row["fee"]
    finally:
        await conn.close()


def _redis(cs, body):
    """A raw redis-py client on this stack's Redis (never through the worker); body(client) is a coroutine fn."""
    return cache_kit.raw_redis(cs.redis.url, body)


def _raw(cs, accounts: Accounts) -> dict[str, tuple[str | None, str | None, int]]:
    """Per role: (value, token, token PTTL)."""

    async def body(client):
        out = {}
        for role in ROLES:
            account = accounts[role]
            out[role] = (
                await client.get(VALUE_KEY.format(id=account)),
                await client.get(TOKEN_KEY.format(id=account)),
                await client.pttl(TOKEN_KEY.format(id=account)),
            )
        return out

    return _redis(cs, body)


def _accounts(cs) -> Accounts:
    customer = cs.api.create_account("CUSTOMER")
    cs.api.deposit(customer, "1000.00")
    merchant = cs.api.create_account("MERCHANT")
    return Accounts(
        {
            "customer": customer,
            "merchant": merchant,
            "escrow": run(_system_id(cs, "system:escrow")),
            "fees": run(_system_id(cs, "system:platform_fees")),
        }
    )


def _prime(cs, accounts: Accounts) -> Primed:
    balances = {}
    for role in ROLES:
        cs.api.balance(accounts[role])
        balance, cache = cs.api.balance(accounts[role])
        assert cache == "hit", f"priming: {role}'s second read must be a hit (it was {cache})"
        balances[role] = Decimal(balance)
    tokens = {role: token for role, (_value, token, _pttl) in _raw(cs, accounts).items()}
    return Primed(balances, tokens)


def _reads(cs, accounts: Accounts) -> dict[str, tuple[Decimal, str | None]]:
    out = {}
    for role in ROLES:
        balance, cache = cs.api.balance(accounts[role])
        out[role] = (Decimal(balance), cache)
    return out


def _start(cs, group: str, **fault):
    worker = start_worker(
        database=cs.database,
        bootstrap_servers=cs.kafka.bootstrap_servers,
        group=group,
        log_dir=cs.log_dir,
        extra_env={"REDIS_URL": cs.redis.url},
        **fault,
    )
    started = worker.wait_for("worker_started")
    assert started["balance_invalidation"] == "on", worker.diagnostics()
    return worker


def _wait_status(cs, payment_id: uuid.UUID, expected: str, timeout: float = 60) -> None:
    cache_kit.wait_status(cs.api, payment_id, expected, timeout)


def _counter(worker, result: str) -> Decimal:
    """settlement_balance_invalidations_total{result} from the worker's /metrics, 0 before its first increment."""
    return cache_kit.invalidations(worker, result)


def _wait_sent(cs, worker, accounts: Accounts) -> None:
    """Exactly one invalidation sent and none failed, within SENT_TIMEOUT_SECONDS of the terminal status. On
    timeout the four reads go into the message, so a missing invalidation shows its stale reads directly."""
    deadline = time.monotonic() + SENT_TIMEOUT_SECONDS
    sent = Decimal(0)
    while time.monotonic() < deadline:
        sent = _counter(worker, "sent")
        if sent >= 1:
            break
        time.sleep(0.1)
    if sent < 1:
        reads = ", ".join(f"{role} {cache} {balance}" for role, (balance, cache) in _reads(cs, accounts).items())
        raise AssertionError(f"no invalidation counted within {SENT_TIMEOUT_SECONDS} s; reads: {reads}")
    assert sent == 1, f"exactly one invalidation per settle, got {sent}"
    assert _counter(worker, "failed") == 0, worker.diagnostics()


def _clean(*workers) -> None:
    for worker in workers:
        for name in FORBIDDEN:
            assert worker.events(name) == [], f"{name}:\n{worker.diagnostics()}"
        assert worker.events("cache_invalidation_failed") == [], worker.diagnostics()


def _assert_fresh(cs, accounts: Accounts, primed: Primed, expected: dict[str, Decimal]) -> None:
    """Every read is a miss equal to the DB and to primed + the expected delta; every token changed."""
    raw = _raw(cs, accounts)
    for role in ROLES:
        value, token, pttl = raw[role]
        assert value is None, f"{role}'s value key survived the invalidation"
        assert token is not None and CANONICAL_UUID.fullmatch(token), f"{role}'s token: {token!r}"
        assert token != primed.tokens[role], f"{role}'s token didn't change"
        assert 0 < pttl <= 300_000, f"{role}'s token PTTL {pttl}"
    reads = _reads(cs, accounts)
    for role in ROLES:
        balance, cache = reads[role]
        assert cache == "miss", f"{role} read {cache} {balance} after the settle (primed {primed.balances[role]})"
        assert balance == run(_db_balance(cs, accounts[role])), f"{role} = DB"
        assert balance - primed.balances[role] == expected[role], f"{role} moved {balance - primed.balances[role]}"


# (a)
def test_an_approved_settle_invalidates_all_four_balances(cs):
    accounts = _accounts(cs)
    group = fresh_group(cs, f"cache-approved-{uuid.uuid4().hex[:8]}")
    payment = cs.api.authorize(accounts["customer"], accounts["merchant"], "10.00")
    primed = _prime(cs, accounts)
    amount, fee = run(_payment(cs, payment))

    worker = _start(cs, group)
    with worker:
        _wait_status(cs, payment, "SETTLED")
        _wait_sent(cs, worker, accounts)
        _assert_fresh(
            cs, accounts, primed, {"customer": Decimal(0), "merchant": amount - fee, "escrow": -amount, "fees": fee}
        )
        assert worker.stop() == 0
    _clean(worker)


# (b)
def test_a_declined_release_invalidates_all_four_balances(cs):
    accounts = _accounts(cs)
    group = fresh_group(cs, f"cache-declined-{uuid.uuid4().hex[:8]}")
    payment = cs.api.authorize(accounts["customer"], accounts["merchant"], "10.13")  # cents 13: declined
    primed = _prime(cs, accounts)
    amount, _fee = run(_payment(cs, payment))

    worker = _start(cs, group)
    with worker:
        _wait_status(cs, payment, "FAILED")
        _wait_sent(cs, worker, accounts)
        # Merchant and fees don't move on a release, but the worker invalidates all four (D-142).
        _assert_fresh(
            cs, accounts, primed, {"customer": amount, "merchant": Decimal(0), "escrow": -amount, "fees": Decimal(0)}
        )
        assert worker.stop() == 0
    _clean(worker)


# (c) D-148
def test_a_crash_before_the_invalidation_is_healed_by_duplicate_skipped(cs):
    accounts = _accounts(cs)
    group = fresh_group(cs, f"cache-crash-{uuid.uuid4().hex[:8]}")
    payment = cs.api.authorize(accounts["customer"], accounts["merchant"], "10.00")
    primed = _prime(cs, accounts)
    amount, fee = run(_payment(cs, payment))

    faulted = _start(cs, group, fault_point="AFTER_DB_BEFORE_INVALIDATION", fault_payment_id=payment)
    with faulted:
        assert faulted.wait_exit(60) == 86, faulted.diagnostics()
    injected = faulted.events("fault_injected")
    assert [(e["point"], e["payment_id"]) for e in injected] == [("AFTER_DB_BEFORE_INVALIDATION", str(payment))]

    # Mid-crash: the commit landed, the invalidation never ran (§5 row 14, observed).
    assert cs.api.payment_status(payment) == "SETTLED"
    merchant_balance, merchant_cache = cs.api.balance(accounts["merchant"])
    assert (Decimal(merchant_balance), merchant_cache) == (primed.balances["merchant"], "hit"), "the stale window"
    assert _raw(cs, accounts)["merchant"][1] == primed.tokens["merchant"], "merchant's token unchanged mid-crash"

    # The same group: the record was never committed, so it is redelivered; dedup absorbs it and invalidates.
    restarted = _start(cs, group)
    with restarted:
        restarted.wait_for("duplicate_skipped", payment_id=str(payment))
        _wait_sent(cs, restarted, accounts)
        _assert_fresh(
            cs, accounts, primed, {"customer": Decimal(0), "merchant": amount - fee, "escrow": -amount, "fees": fee}
        )
        assert restarted.stop() == 0
    _clean(faulted, restarted)


# (d)
def test_the_worker_touches_exactly_the_keys_the_contract_names(cs):
    accounts = _accounts(cs)
    group = fresh_group(cs, f"cache-keys-{uuid.uuid4().hex[:8]}")
    payment = cs.api.authorize(accounts["customer"], accounts["merchant"], "10.00")
    _prime(cs, accounts)

    worker = _start(cs, group)
    with worker:
        _wait_status(cs, payment, "SETTLED")
        _wait_sent(cs, worker, accounts)

        async def scan(client):
            # Idempotency entries (the API runs with both caches on here) end in an Idempotency-Key, so a suffix
            # match on them would be safe only by luck: skip them. Any other prefix, ledger:v2:... included, stays.
            return [key async for key in client.scan_iter(match="ledger:*") if not key.startswith("ledger:v1:idem:")]

        keys = _redis(cs, scan)
        for role in ROLES:
            account = accounts[role]
            touched = sorted(key for key in keys if key.endswith(f":{account}"))
            assert touched == [TOKEN_KEY.format(id=account)], f"{role} ({account}): {touched}"
        assert worker.stop() == 0
    _clean(worker)
