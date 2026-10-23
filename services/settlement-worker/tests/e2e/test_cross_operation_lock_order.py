"""API and worker postings never deadlock: the cross-operation lock-order proof (invariant 4; D-007,
D-075, D-087; closes the D-068 open issue).

On the D-104 e2e stack, the real API jar and two real workers in one group write to the same
accounts at once. Accounts are created so C1.id < M.id < C2.id; ESCROW and PLATFORM_FEES are seeded
first, so they have the lowest ids. Per round, behind one start gate and shuffled by a logged seed:
  - 40 authorizes, alternating C1->M and C2->M, 10.00 (approved; fee 0.29) and 10.13 (declined, so
    the worker releases it back to the customer);
  - 20 transfers of 1.00: C1->M, M->C1, C2->M, M->C2, 5 each;
  - 10 deposits of 1.00: 4 into C1, 3 into C2, 3 into M.
The two workers settle and release as the relay publishes. Every writer locks all its accounts in one
ascending FOR UPDATE, so no ordering of the load can deadlock. lock_wait_ms on the result events is
report-only (D-087): CI gates correctness only.
"""

import asyncio
import os
import random
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from support import hardware
from support.worker_process import group_state, start_worker, wait_quiescent

GROUP = "lock-order"
DEADLOCK_MARKERS = ("deadlock detected", "40P01", "55P03")

# Pre-funding, before the gate. Worst case per customer, every outflow before any inflow:
# 10 x 10.00 + 10 x 10.13 authorized + 5 x 1.00 transferred = 206.30 < 300.00. Worst case for M:
# 10 x 1.00 transferred out < 50.00. So no ordering can produce a 422.
FUND_CUSTOMER = "300.00"
FUND_MERCHANT = "50.00"

# Exact deltas per round, from the baseline taken just before the gate:
#   C1: -10 x 10.00 settled; declines released (net 0); transfers net 0; +4 deposits  = -96.00
#   C2: the same with +3 deposits                                                    = -97.00
#   M:  +20 x 9.71 settled; transfers net 0; +3 deposits                              = +197.20
#   ESCROW 0 (every authorize settled or released); PLATFORM_FEES +20 x 0.29 = +5.80;
#   EXTERNAL -10.00 (the deposits). They sum to 0.
EXPECTED = {
    "C1": Decimal("-96.00"),
    "C2": Decimal("-97.00"),
    "M": Decimal("197.20"),
    "ESCROW": Decimal("0"),
    "FEES": Decimal("5.80"),
    "EXTERNAL": Decimal("-10.00"),
}
EXPECTED_KINDS = {
    "PAYMENT_AUTHORIZE": 40,
    "PAYMENT_SETTLE": 20,
    "PAYMENT_RELEASE": 20,
    "TRANSFER": 20,
    "DEPOSIT": 10,
}


def _positive_int(name: str, default: str) -> int:
    raw = os.environ.get(name, default)
    if not raw.isdigit() or int(raw) < 1:
        raise pytest.UsageError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


ROUNDS = _positive_int("LOCK_ORDER_ROUNDS", "1")
_seed = os.environ.get("LOCK_ORDER_SEED", "")
if _seed and not _seed.isdigit():
    raise pytest.UsageError(f"LOCK_ORDER_SEED must be a non-negative integer, got {_seed!r}")
BASE_SEED = int(_seed) if _seed else random.SystemRandom().randrange(1 << 40)


def run(coro):
    return asyncio.run(coro)


async def _fetch(db, query, *args):
    conn = await db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def _app(e2e, query, *args):
    return run(_fetch(e2e.app_db, query, *args))


def _worker(e2e):
    return start_worker(
        database=e2e.database, bootstrap_servers=e2e.kafka.bootstrap_servers, group=GROUP, log_dir=e2e.log_dir
    )


def _nearest_rank(values: list[int], percent: int):
    ordered = sorted(values)
    return ordered[max(0, (percent * len(ordered) + 99) // 100 - 1)] if ordered else "na"


@dataclass
class Report:
    settles: int = 0
    releases: int = 0
    lock_waits: list[int] = field(default_factory=list)
    rounds: int = 0


@pytest.fixture(scope="module")
def report():
    return Report()


@pytest.fixture(scope="module")
def workers(e2e, report, request):
    """Prime the group, then two workers; the gate waits until the group is Stable with both in it."""
    everything = [r["id"] for r in _app(e2e, "SELECT id FROM payments")]
    with _worker(e2e) as primer:
        run(_quiescent(e2e, everything))
        assert primer.stop() == 0, primer.diagnostics()
    assert primer.events("retry_scheduled") == [], primer.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)

    pair = (_worker(e2e), _worker(e2e))
    try:
        _wait_for_stable_pair(e2e, pair)
        yield pair
        alive = [w.process.poll() is None for w in pair]
        codes = [w.stop() for w in pair]
    finally:
        for w in pair:
            w.__exit__(None, None, None)
    line = (
        f"LOCK_ORDER rounds={report.rounds} seed={BASE_SEED} settles={report.settles} releases={report.releases} "
        f"lock_wait_ms_p50={_nearest_rank(report.lock_waits, 50)} p95={_nearest_rank(report.lock_waits, 95)} "
        f"max={max(report.lock_waits) if report.lock_waits else 'na'} {hardware.summary_fields()}"
    )
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line)
    else:
        print(line)
    assert alive == [True, True], f"a worker had exited before the end: {[w.diagnostics() for w in pair]}"
    assert codes == [0, 0], [w.diagnostics() for w in pair]


def _wait_for_stable_pair(e2e, pair, timeout: float = 60) -> None:
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        assigned = []
        for w in pair:
            events = w.events("partitions_assigned")
            assigned.append(set(events[-1]["partitions"]) if events else None)
            if w.process.poll() is not None:
                raise AssertionError(f"a worker exited before the gate: {w.diagnostics()}")
        state = run(group_state(e2e.kafka.bootstrap_servers, GROUP))
        last = (assigned, state)
        if (
            all(a is not None for a in assigned)
            and not (assigned[0] & assigned[1])
            and assigned[0] | assigned[1] == {0, 1, 2}
            and state == ("Stable", 2)
        ):
            return
        time.sleep(0.25)
    raise AssertionError(f"group {GROUP} not Stable with both workers within {timeout} s: {last}")


async def _quiescent(e2e, payment_ids, *, check=None, timeout: float = 60):
    conn = await e2e.app_db.connect()
    try:
        return await wait_quiescent(
            app_conn=conn,
            bootstrap_servers=e2e.kafka.bootstrap_servers,
            group=GROUP,
            payment_ids=payment_ids,
            timeout=timeout,
            check=check,
        )
    finally:
        await conn.close()


def _requests(c1: int, m: int, c2: int) -> list[tuple[str, str, dict]]:
    """(kind, path, body) for the 70 requests of one round, unshuffled."""
    out = []
    for i in range(40):
        customer = c1 if i % 2 == 0 else c2
        amount = "10.00" if (i // 2) % 2 == 0 else "10.13"
        out.append(
            ("authorize", "/v1/payments", {"customer_account_id": customer, "merchant_account_id": m, "amount": amount})
        )
    for source, target in ((c1, m), (m, c1), (c2, m), (m, c2)):
        out += [
            ("transfer", "/v1/transfers", {"from_account_id": source, "to_account_id": target, "amount": "1.00"})
        ] * 5
    for account, count in ((c1, 4), (c2, 3), (m, 3)):
        out += [("deposit", "/v1/deposits", {"account_id": account, "amount": "1.00"})] * count
    return out


def _fire(api, planned) -> list[tuple[str, int, object]]:
    """One thread per request, all parked on one gate; the gate opens once every thread is ready."""
    ready = threading.Barrier(len(planned) + 1)
    gate = threading.Event()
    results: list = [None] * len(planned)

    def send(index, kind, path, body):
        ready.wait()
        gate.wait()
        try:
            status, response = api.try_post(path, body)
        except Exception as ex:  # noqa: BLE001 - recorded and asserted on below
            status, response = -1, repr(ex)
        results[index] = (kind, status, response)

    threads = [
        threading.Thread(target=send, args=(i, kind, path, body), daemon=True)
        for i, (kind, path, body) in enumerate(planned)
    ]
    for t in threads:
        t.start()
    ready.wait()
    gate.set()
    for t in threads:
        t.join(timeout=120)
    assert all(not t.is_alive() for t in threads), "requests still running after 120 s"
    return results


def _balances(e2e, ids: dict[str, int]):
    """One statement, so one snapshot (D-019): each account's balance and its entry sum."""
    rows = _app(
        e2e,
        "SELECT a.id, a.balance, (SELECT coalesce(sum(e.amount), 0) FROM entries e WHERE e.account_id = a.id) "
        "AS entry_sum FROM accounts a WHERE a.id = ANY($1::bigint[])",
        list(ids.values()),
    )
    by_id = {r["id"]: r for r in rows}
    return {name: by_id[account_id] for name, account_id in ids.items()}


def _log_text(path, offset: int = 0) -> str:
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read().decode("utf-8", errors="replace")


@pytest.mark.parametrize("round_", range(ROUNDS), ids=[f"lock_order-{r:03d}" for r in range(ROUNDS)])
def test_api_and_worker_postings_never_deadlock(e2e, workers, report, round_):
    api = e2e.api
    seed = BASE_SEED + round_
    jar_log = e2e.log_dir / "ledger-api.log"

    # Accounts in this order, so C1.id < M.id < C2.id; system accounts were seeded first.
    c1 = api.create_account("CUSTOMER")
    m = api.create_account("MERCHANT")
    c2 = api.create_account("CUSTOMER")
    system = {
        r["name"]: r["id"]
        for r in _app(
            e2e,
            "SELECT name, id FROM accounts WHERE name IN ('system:escrow', 'system:platform_fees', 'system:external')",
        )
    }
    ids = {
        "C1": c1,
        "M": m,
        "C2": c2,
        "ESCROW": system["system:escrow"],
        "FEES": system["system:platform_fees"],
        "EXTERNAL": system["system:external"],
    }
    assert c1 < m < c2, ids
    assert ids["ESCROW"] < c1 and ids["FEES"] < c1, ids
    api.deposit(c1, FUND_CUSTOMER)
    api.deposit(c2, FUND_CUSTOMER)
    api.deposit(m, FUND_MERCHANT)

    before = _balances(e2e, ids)
    first_new_tx = _app(e2e, "SELECT coalesce(max(id), 0) + 1 FROM transactions")[0][0]
    jar_offset = jar_log.stat().st_size

    planned = _requests(c1, m, c2)
    random.Random(seed).shuffle(planned)
    results = _fire(api, planned)
    statuses = Counter((kind, status) for kind, status, _ in results)
    failures = [(kind, status, body) for kind, status, body in results if status != 201][:5]
    assert set(status for _, status, _ in results) == {201}, f"seed {seed}: {dict(statuses)}; first: {failures}"
    payment_ids = [uuid.UUID(body["payment_id"]) for kind, _, body in results if kind == "authorize"]
    assert len(payment_ids) == 40

    def alive():
        dead = [w for w in workers if w.process.poll() is not None]
        if dead:
            return f"seed {seed}: worker exited {dead[0].process.returncode}: {dead[0].events('processing_failed')}"
        return None

    run(_quiescent(e2e, payment_ids, check=alive, timeout=120))

    for w in workers:
        assert w.process.poll() is None, w.diagnostics()
        assert w.events("processing_failed") == [], w.diagnostics()
        # Stricter than the deadlock markers below: nothing in this test may retry at all, so a
        # deadlock the worker retried (40P01, D-108) fails here too (D-123).
        assert w.events("retry_scheduled") == [], w.diagnostics()
        text = _log_text(w.stdout_path) + _log_text(w.stderr_path)
        assert [marker for marker in DEADLOCK_MARKERS if marker in text] == [], w.diagnostics()
    jar_text = _log_text(jar_log, jar_offset)
    assert [marker for marker in DEADLOCK_MARKERS if marker in jar_text] == [], jar_text[-4000:]

    # Every payment terminal with exactly one terminal posting.
    statuses_by_id = {
        r["id"]: r["status"] for r in _app(e2e, "SELECT id, status FROM payments WHERE id = ANY($1::uuid[])", payment_ids)
    }
    assert Counter(statuses_by_id.values()) == {"SETTLED": 20, "FAILED": 20}, Counter(statuses_by_id.values())
    terminal = Counter(
        r["reference"]
        for r in _app(
            e2e,
            "SELECT reference FROM transactions WHERE kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE') "
            "AND reference = ANY($1::text[])",
            [str(p) for p in payment_ids],
        )
    )
    assert sorted(terminal.values()) == [1] * 40, terminal

    # Exact counts of what this round wrote on these accounts.
    kinds = Counter(
        r["kind"]
        for r in _app(
            e2e,
            "SELECT DISTINCT t.id, t.kind FROM transactions t JOIN entries e ON e.transaction_id = t.id "
            "WHERE t.id >= $1 AND e.account_id = ANY($2::bigint[])",
            first_new_tx,
            [c1, m, c2],
        )
    )
    assert kinds == EXPECTED_KINDS, kinds

    # One snapshot: balances equal entry sums, and the deltas are exact.
    after = _balances(e2e, ids)
    for name, row in after.items():
        assert row["balance"] == row["entry_sum"], (name, row)
    deltas = {name: after[name]["balance"] - before[name]["balance"] for name in ids}
    assert deltas == EXPECTED, deltas

    # Report-only lock waits from both workers' result events for this round's payments.
    wanted = {str(p) for p in payment_ids}
    for w in workers:
        for name in ("payment_settled", "payment_released"):
            for event in w.events(name):
                if event["payment_id"] in wanted:
                    report.lock_waits.append(event["lock_wait_ms"])
                    if name == "payment_settled":
                        report.settles += 1
                    else:
                        report.releases += 1
    report.rounds += 1
