"""P3: crash recovery at both fault points (spec §2, §9; invariant 6; D-014, D-085, D-105).

On the D-104 e2e stack, for fault point F x iteration 0..N-1 (N = CRASH_ITERATIONS, default 5):
  1. authorize a target T, then neighbours 3 at a time until one lands on T's partition behind it
     (positions read from the topic, never computed);
  2. W1 in the crash-recovery group, armed with F for T, must exit 86;
  3. the mid-crash state is what F implies, and T's offset is not committed;
  4. W2 recovers: lag 0 on every partition with all of the iteration's outbox rows published, then
     a payment that isn't terminal counts as lost;
  5-9. T has exactly one dedup row, one posting and one result event; W2's log shows which dedup
     layer caught the redelivery; the neighbours are settled; stack-wide invariants hold in one
     snapshot (D-019).
A priming worker drains the group first; a clean-restart control closes the module. A summary line
per fault point records duplicates, lost, iteration time, guard_runs (must be 0) and the hardware.
"""

import asyncio
import os
import statistics
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from support import hardware
from support.worker_process import authorized_positions, committed_offsets, group_lag, start_worker, wait_quiescent

from settlement_worker.config import AFTER_DB_BEFORE_OFFSET, AFTER_PROCESSOR_BEFORE_DB

CRASH_GROUP = "crash-recovery"
POINTS = (AFTER_PROCESSOR_BEFORE_DB, AFTER_DB_BEFORE_OFFSET)


def _iterations() -> int:
    raw = os.environ.get("CRASH_ITERATIONS", "5")
    if not raw.isdigit() or int(raw) < 1:
        raise pytest.UsageError(f"CRASH_ITERATIONS must be a positive integer, got {raw!r}")
    return int(raw)


N = _iterations()
PARAMS = [(point, i) for point in POINTS for i in range(N)]
IDS = [f"{point}-{i:03d}" for point, i in PARAMS]

# Approved on even iterations, declined (cents 13, D-073) on odd ones. 0.17 x 0.029 rounds to a
# fee of 0.00, so CI's N = 5 covers the posting without a fee leg.
APPROVED_AMOUNTS = ("100.00", "0.17", "42.50")
DECLINED_AMOUNTS = ("10.13", "0.13", "113.13")
NEIGHBOUR_AMOUNT = "1.00"
NEIGHBOUR_BATCH = 3
NEIGHBOUR_CAP = 30  # (2/3)^30 ~ 5e-6 per iteration without a neighbour behind T
DEPOSIT = "500.00"
TERMINAL = ("SETTLED", "FAILED")


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


def _worker_role(e2e, query, *args):
    """processed_messages is readable only by settlement_worker (V12 grants)."""
    return run(_fetch(e2e.worker_db, query, *args))


def _worker(e2e, **fault):
    return start_worker(
        database=e2e.database,
        bootstrap_servers=e2e.kafka.bootstrap_servers,
        group=CRASH_GROUP,
        log_dir=e2e.log_dir,
        **fault,
    )


async def _quiescent(e2e, payment_ids, *, require_terminal=True):
    conn = await e2e.app_db.connect()
    try:
        return await wait_quiescent(
            app_conn=conn,
            bootstrap_servers=e2e.kafka.bootstrap_servers,
            group=CRASH_GROUP,
            payment_ids=payment_ids,
            require_terminal=require_terminal,
        )
    finally:
        await conn.close()


def _drain(e2e) -> None:
    """A normal worker in the group, to full quiescence over every payment the stack holds."""
    everything = [r["id"] for r in _app(e2e, "SELECT id FROM payments")]
    with _worker(e2e) as worker:
        run(_quiescent(e2e, everything))
        assert worker.stop() == 0, worker.diagnostics()
    assert worker.events("processing_failed") == [], worker.diagnostics()
    assert worker.events("retry_scheduled") == [], worker.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert worker.events("db_reconnected") == [], worker.diagnostics()  # tripwire (D-109)
    assert worker.events("offset_commit_failed") == [], worker.diagnostics()  # tripwire (D-109)


def _group_is_quiet(e2e) -> bool:
    lag = run(group_lag(e2e.kafka.bootstrap_servers, CRASH_GROUP))
    pending = _app(
        e2e,
        "SELECT (SELECT count(*) FROM payments WHERE status = 'AUTHORIZED') + "
        "(SELECT count(*) FROM outbox WHERE published_at IS NULL)",
    )[0][0]
    return set(lag) == {0, 1, 2} and all(v == 0 for v in lag.values()) and pending == 0


@dataclass
class Collector:
    results: dict[str, list[dict]] = field(default_factory=lambda: {point: [] for point in POINTS})
    guard_runs: int = 0

    def summary(self, point: str) -> str:
        entries = self.results[point]
        seconds = [e["seconds"] for e in entries if e["seconds"] is not None]
        p50 = f"{statistics.median(seconds):.1f}" if seconds else "na"
        top = f"{max(seconds):.1f}" if seconds else "na"
        return (
            f"CRASH_RECOVERY point={point} iterations={len(entries)} "
            f"duplicates={sum(e['duplicates'] for e in entries)} lost={sum(e['lost'] for e in entries)} "
            f"iteration_s_p50={p50} iteration_s_max={top} guard_runs={self.guard_runs} {hardware.summary_fields()}"
        )


@pytest.fixture(scope="module")
def collector(request):
    collected = Collector()
    yield collected
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    lines = [collected.summary(point) for point in POINTS if collected.results[point]]
    for line in lines:
        # The terminal reporter isn't captured, so the lines show in CI logs without -s.
        if reporter is not None:
            reporter.write_line(line)
        else:
            print(line)
    # A drain inside a green run would hide whatever left the group busy (D-105).
    assert collected.guard_runs == 0, f"the cascade guard drained the group {collected.guard_runs} time(s)"


@pytest.fixture(scope="module")
def crash_group(e2e, collector) -> str:
    """Prime the group over whatever the stack already holds: every iteration starts quiet."""
    _drain(e2e)
    assert _group_is_quiet(e2e)
    return CRASH_GROUP


def _guard(e2e, collector: Collector) -> None:
    """Only after an earlier iteration failed: drain, so one failure doesn't fail all that follow."""
    if not _group_is_quiet(e2e):
        collector.guard_runs += 1
        _drain(e2e)


def _wait_published(e2e, payment_ids, timeout: float = 30) -> None:
    keys = [str(p) for p in payment_ids]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = _app(
            e2e,
            "SELECT message_key, published_at IS NOT NULL AS published FROM outbox "
            "WHERE message_key = ANY($1::text[]) AND event_type = 'payment.authorized'",
            keys,
        )
        if len(rows) == len(keys) and all(r["published"] for r in rows):
            return
        time.sleep(0.2)
    raise AssertionError(f"payment.authorized rows not all published within {timeout} s")


def _event_ids(e2e, payment_ids) -> dict[uuid.UUID, uuid.UUID]:
    rows = _app(
        e2e,
        "SELECT message_key, event_id FROM outbox "
        "WHERE message_key = ANY($1::text[]) AND event_type = 'payment.authorized'",
        [str(p) for p in payment_ids],
    )
    return {uuid.UUID(r["message_key"]): r["event_id"] for r in rows}


def _dedup_rows(e2e, event_id) -> int:
    return _worker_role(
        e2e,
        "SELECT count(*) FROM processed_messages WHERE consumer_group = $1 AND event_id = $2",
        CRASH_GROUP,
        event_id,
    )[0][0]


def _postings(e2e, payment_id) -> dict[str, int]:
    rows = _app(
        e2e,
        "SELECT kind, count(*) AS n FROM transactions WHERE reference = $1 "
        "AND kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE') GROUP BY kind",
        str(payment_id),
    )
    counts = {"PAYMENT_SETTLE": 0, "PAYMENT_RELEASE": 0}
    counts.update({r["kind"]: r["n"] for r in rows})
    return counts


def _results(e2e, payment_id) -> dict[str, int]:
    rows = _app(
        e2e,
        "SELECT event_type, count(*) AS n FROM outbox WHERE message_key = $1 "
        "AND event_type IN ('payment.settled', 'payment.failed') GROUP BY event_type",
        str(payment_id),
    )
    counts = {"payment.settled": 0, "payment.failed": 0}
    counts.update({r["event_type"]: r["n"] for r in rows})
    return counts


def _statuses(e2e, payment_ids) -> dict[uuid.UUID, str]:
    rows = _app(e2e, "SELECT id, status FROM payments WHERE id = ANY($1::uuid[])", list(payment_ids))
    return {r["id"]: r["status"] for r in rows}


def _entries(e2e, payment_id, kind) -> dict[int, Decimal]:
    rows = _app(
        e2e,
        "SELECT e.account_id, e.amount FROM transactions t JOIN entries e ON e.transaction_id = t.id "
        "WHERE t.reference = $1 AND t.kind = $2",
        str(payment_id),
        kind,
    )
    legs = {r["account_id"]: r["amount"] for r in rows}
    assert len(legs) == len(rows), f"two legs on one account in {kind} for {payment_id}"
    return legs


def _stack_wide(e2e):
    """One statement, so one snapshot (D-019)."""
    return _app(
        e2e,
        "SELECT "
        "(SELECT coalesce(sum(amount), 0) FROM entries) AS entry_total, "
        "(SELECT count(*) FROM accounts) AS accounts, "
        "(SELECT count(*) FROM accounts a WHERE a.balance <> "
        "  (SELECT coalesce(sum(e.amount), 0) FROM entries e WHERE e.account_id = a.id)) AS drifted, "
        "(SELECT balance FROM accounts WHERE name = 'system:escrow') AS escrow, "
        "(SELECT coalesce(sum(amount), 0) FROM payments WHERE status = 'AUTHORIZED') AS authorized",
    )[0]


def _authorize_with_neighbour_behind(e2e, customer, merchant, amount):
    """T first, then neighbours NEIGHBOUR_BATCH at a time until one sits behind T on its partition."""
    api = e2e.api
    target = api.authorize(customer, merchant, amount)
    neighbours: list[uuid.UUID] = []
    while len(neighbours) < NEIGHBOUR_CAP:
        neighbours += [api.authorize(customer, merchant, NEIGHBOUR_AMOUNT) for _ in range(NEIGHBOUR_BATCH)]
        _wait_published(e2e, [target, *neighbours])
        positions = run(authorized_positions(e2e.kafka.bootstrap_servers, CRASH_GROUP, [target, *neighbours]))
        partition, offset = positions[target]
        behind = [n for n in neighbours if positions[n][0] == partition and positions[n][1] > offset]
        if behind:
            return target, neighbours, positions, behind
    pytest.fail(f"none of {NEIGHBOUR_CAP} neighbours landed on partition {partition} behind {target}")


@pytest.mark.parametrize(("point", "iteration"), PARAMS, ids=IDS)
def test_crash_recovery(e2e, crash_group, collector, point, iteration):
    started = time.monotonic()
    _guard(e2e, collector)
    api = e2e.api
    approve = iteration % 2 == 0
    amounts = APPROVED_AMOUNTS if approve else DECLINED_AMOUNTS
    amount = amounts[(iteration // 2) % len(amounts)]
    expected_decision = "APPROVED" if approve else "DECLINED"

    # 1. Payments, with at least one neighbour behind T on T's partition.
    customer = api.create_account("CUSTOMER")
    api.deposit(customer, DEPOSIT)
    merchant = api.create_account("MERCHANT")
    target, neighbours, positions, behind = _authorize_with_neighbour_behind(e2e, customer, merchant, amount)
    payments = [target, *neighbours]
    t_partition, t_offset = positions[target]
    event_ids = _event_ids(e2e, payments)
    t_event = event_ids[target]

    # 2. Crash.
    with _worker(e2e, fault_point=point, fault_payment_id=target) as w1:
        assert w1.wait_exit(60) == 86, w1.diagnostics()
    assert w1.events("retry_scheduled") == [], w1.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert w1.events("db_reconnected") == [], w1.diagnostics()  # tripwire (D-109)
    assert w1.events("offset_commit_failed") == [], w1.diagnostics()  # tripwire (D-109)
    assert len(w1.events("fault_injected", point=point, payment_id=str(target))) == 1, w1.diagnostics()
    w1_calls = w1.events("processor_called", payment_id=str(target))
    assert len(w1_calls) == 1, w1.diagnostics()
    decision = w1_calls[0]["decision"]
    assert decision == expected_decision

    # 3. Mid-state.
    status = _statuses(e2e, [target])[target]
    if point == AFTER_PROCESSOR_BEFORE_DB:
        assert status == "AUTHORIZED"
        assert _dedup_rows(e2e, t_event) == 0
        assert _postings(e2e, target) == {"PAYMENT_SETTLE": 0, "PAYMENT_RELEASE": 0}
        assert _results(e2e, target) == {"payment.settled": 0, "payment.failed": 0}
    else:
        assert status in TERMINAL
        assert _dedup_rows(e2e, t_event) == 1
        assert sum(_postings(e2e, target).values()) == 1
        assert sum(_results(e2e, target).values()) == 1
    committed = run(committed_offsets(e2e.kafka.bootstrap_servers, CRASH_GROUP))[t_partition]
    assert (committed or 0) <= t_offset, f"committed {committed} > T's offset {t_offset} on partition {t_partition}"
    assert set(_statuses(e2e, behind).values()) == {"AUTHORIZED"}, "a record behind T was processed"

    # 4. Recover: lag 0 with every outbox row published; then whatever isn't terminal was lost.
    with _worker(e2e) as w2:
        run(_quiescent(e2e, payments, require_terminal=False))
        assert w2.stop() == 0, w2.diagnostics()
    statuses = _statuses(e2e, payments)
    postings = {p: _postings(e2e, p) for p in payments}
    entry = {
        "duplicates": sum(sum(c.values()) - 1 for c in postings.values() if sum(c.values()) > 1),
        "lost": sum(1 for p in payments if statuses[p] not in TERMINAL),
        "seconds": None,
    }
    collector.results[point].append(entry)
    assert entry["lost"] == 0, f"not terminal after recovery: { {str(p): s for p, s in statuses.items()} }"
    assert entry["duplicates"] == 0, f"postings: { {str(p): c for p, c in postings.items()} }"

    # 5. Final state of T.
    assert _dedup_rows(e2e, t_event) == 1
    kind, other = ("PAYMENT_SETTLE", "PAYMENT_RELEASE") if approve else ("PAYMENT_RELEASE", "PAYMENT_SETTLE")
    assert postings[target] == {kind: 1, other: 0}
    assert statuses[target] == ("SETTLED" if approve else "FAILED")
    row = _app(e2e, "SELECT amount, fee FROM payments WHERE id = $1", target)[0]
    a, f = row["amount"], row["fee"]
    escrow, fees = (
        r["id"]
        for r in _app(
            e2e,
            "SELECT id FROM accounts WHERE name IN ('system:escrow', 'system:platform_fees') ORDER BY name",
        )
    )
    if approve:
        expected = {escrow: -a, merchant: a - f, fees: f}
        if amount == "0.17":
            assert f == 0
    else:
        expected = {escrow: -a, customer: a}
    expected = {account: delta for account, delta in expected.items() if delta != 0}
    assert _entries(e2e, target, kind) == expected
    result, other_result = ("payment.settled", "payment.failed") if approve else ("payment.failed", "payment.settled")
    assert _results(e2e, target) == {result: 1, other_result: 0}

    # 6. Which layer caught the redelivery, from W2's log.
    w2_calls = w2.events("processor_called", payment_id=str(target))
    assert [c["decision"] for c in w2_calls] == [decision], w2.diagnostics()
    assert w2.events("processing_failed") == [], w2.diagnostics()
    assert w2.events("retry_scheduled") == [], w2.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert w2.events("db_reconnected") == [], w2.diagnostics()  # tripwire (D-109)
    assert w2.events("offset_commit_failed") == [], w2.diagnostics()  # tripwire (D-109)
    effect = "payment_settled" if approve else "payment_released"
    if point == AFTER_DB_BEFORE_OFFSET:
        assert len(w2.events("duplicate_skipped", event_id=str(t_event))) == 1, w2.diagnostics()
        assert w2.events("payment_not_authorized", payment_id=str(target)) == [], w2.diagnostics()
        assert w2.events(effect, payment_id=str(target)) == []
    else:
        # Two processor calls, one effect.
        assert w2.events("duplicate_skipped", payment_id=str(target)) == [], w2.diagnostics()
        assert len(w2.events(effect, payment_id=str(target))) == 1, w2.diagnostics()

    # 7. Neighbours.
    for n in neighbours:
        assert _dedup_rows(e2e, event_ids[n]) == 1, n
        assert sum(postings[n].values()) == 1, n
        assert statuses[n] == "SETTLED", n

    # 8. Stack-wide, one snapshot (D-019); exact because the D-104 stack holds only API-created data.
    snapshot = _stack_wide(e2e)
    assert snapshot["accounts"] > 0
    assert snapshot["entry_total"] == 0
    assert snapshot["drifted"] == 0
    assert snapshot["escrow"] == snapshot["authorized"]

    # 9. Iteration time for the summary.
    entry["seconds"] = time.monotonic() - started


def test_clean_restart_after_the_crashes_redelivers_nothing(e2e, crash_group):
    with _worker(e2e) as worker:
        worker.wait_for("partitions_assigned", partitions=[0, 1, 2])
        run(_quiescent(e2e, []))
        assert worker.stop() == 0, worker.diagnostics()
    assert run(group_lag(e2e.kafka.bootstrap_servers, CRASH_GROUP)) == {0: 0, 1: 0, 2: 0}
    assert worker.events("processor_called") == []
    assert worker.events("duplicate_skipped") == []
    assert worker.events("processing_failed") == []
    assert worker.events("retry_scheduled") == []  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert worker.events("db_reconnected") == []  # tripwire (D-109)
    assert worker.events("offset_commit_failed") == []  # tripwire (D-109)
