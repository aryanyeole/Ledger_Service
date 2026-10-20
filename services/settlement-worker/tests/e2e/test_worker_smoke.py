"""The real pipeline end to end (D-014, D-081, D-085, D-091, D-104):
API authorize -> outbox -> relay -> Kafka -> worker subprocess -> settlement -> result outbox row.

Each test uses its own consumer groups and its own payments; every assertion is scoped to them.
A fresh group reads the topic from the earliest offset, so it also meets earlier tests' events:
those payments are already terminal and only count where a test says so.
"""

import asyncio
import uuid
from decimal import Decimal

from support.worker_process import group_lag, start_worker, wait_quiescent

from settlement_worker.config import AFTER_DB_BEFORE_OFFSET


def run(coro):
    return asyncio.run(coro)


def _group(name: str) -> str:
    return f"e2e-{name}-{uuid.uuid4().hex[:8]}"


def _worker(e2e, group, **fault):
    return start_worker(
        database=e2e.database,
        bootstrap_servers=e2e.kafka.bootstrap_servers,
        group=group,
        log_dir=e2e.log_dir,
        **fault,
    )


async def _quiescent(e2e, group, payment_ids):
    conn = await e2e.app_db.connect()
    try:
        return await wait_quiescent(
            app_conn=conn, bootstrap_servers=e2e.kafka.bootstrap_servers, group=group, payment_ids=payment_ids
        )
    finally:
        await conn.close()


async def _ledger(e2e, query, *args):
    conn = await e2e.app_db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def _posting(e2e, payment_id, kind):
    rows = run(
        _ledger(
            e2e,
            "SELECT e.account_id, e.amount FROM transactions t JOIN entries e ON e.transaction_id = t.id "
            "WHERE t.reference = $1 AND t.kind = $2 ORDER BY e.id",
            str(payment_id),
            kind,
        )
    )
    return {r["account_id"]: r["amount"] for r in rows}, len({r["account_id"] for r in rows})


def _count(e2e, query, *args) -> int:
    return run(_ledger(e2e, query, *args))[0][0]


def _system_ids(e2e):
    rows = run(_ledger(e2e, "SELECT name, id FROM accounts WHERE name IN ('system:escrow', 'system:platform_fees')"))
    ids = {r["name"]: r["id"] for r in rows}
    return ids["system:escrow"], ids["system:platform_fees"]


def _authorized_event_id(e2e, payment_id) -> str:
    rows = run(
        _ledger(
            e2e,
            "SELECT event_id FROM outbox WHERE message_key = $1 AND event_type = 'payment.authorized'",
            str(payment_id),
        )
    )
    return str(rows[0]["event_id"])


def test_settle_and_release_through_the_whole_pipeline(e2e):
    api = e2e.api
    customer = api.create_account("CUSTOMER")
    api.deposit(customer, "200.00")
    merchant = api.create_account("MERCHANT")
    settle = api.authorize(customer, merchant, "100.00")
    release = api.authorize(customer, merchant, "10.13")
    group = _group("smoke")

    with _worker(e2e, group) as worker:
        run(_quiescent(e2e, group, [settle, release]))
        assert worker.stop() == 0, worker.diagnostics()

    assert worker.events("worker_stopped", reason="stdin_eof"), worker.diagnostics()
    assert worker.events("processing_failed") == []
    statuses = dict(
        (r["id"], r["status"])
        for r in run(_ledger(e2e, "SELECT id, status FROM payments WHERE id = ANY($1::uuid[])", [settle, release]))
    )
    assert statuses == {settle: "SETTLED", release: "FAILED"}
    escrow, fees = _system_ids(e2e)
    assert _posting(e2e, settle, "PAYMENT_SETTLE")[0] == {
        escrow: Decimal("-100.0000"),
        merchant: Decimal("97.1000"),
        fees: Decimal("2.9000"),
    }
    assert _posting(e2e, release, "PAYMENT_RELEASE")[0] == {escrow: Decimal("-10.1300"), customer: Decimal("10.1300")}
    results = run(
        _ledger(
            e2e,
            "SELECT message_key, event_type, published_at FROM outbox "
            "WHERE message_key = ANY($1::text[]) AND event_type <> 'payment.authorized'",
            [str(settle), str(release)],
        )
    )
    assert {(r["message_key"], r["event_type"]) for r in results} == {
        (str(settle), "payment.settled"),
        (str(release), "payment.failed"),
    }
    assert all(r["published_at"] is not None for r in results)
    assert len(worker.events("payment_settled", payment_id=str(settle))) == 1
    assert len(worker.events("payment_released", payment_id=str(release))) == 1


def test_clean_restart_redelivers_nothing(e2e):
    """offset+1, not offset: a restarted group member has nothing to process. Lag decides it: a
    redelivered own-result event would be committed without any log line."""
    group = _group("restart")
    with _worker(e2e, group) as first:
        run(_quiescent(e2e, group, []))
        assert first.stop() == 0, first.diagnostics()

    with _worker(e2e, group) as second:
        second.wait_for("partitions_assigned", partitions=[0, 1, 2])
        run(_quiescent(e2e, group, []))
        assert second.stop() == 0, second.diagnostics()

    assert run(group_lag(e2e.kafka.bootstrap_servers, group)) == {0: 0, 1: 0, 2: 0}
    assert second.events("processor_called") == []
    assert second.events("duplicate_skipped") == []
    assert second.events("processing_failed") == []


def test_crash_after_the_db_commit_is_absorbed_by_dedup(e2e):
    api = e2e.api
    customer = api.create_account("CUSTOMER")
    api.deposit(customer, "60.00")
    merchant = api.create_account("MERCHANT")
    payment = api.authorize(customer, merchant, "50.00")
    event_id = _authorized_event_id(e2e, payment)
    group = _group("fault")

    with _worker(e2e, group, fault_point=AFTER_DB_BEFORE_OFFSET, fault_payment_id=payment) as faulty:
        assert faulty.wait_exit(60) == 86, faulty.diagnostics()
    assert len(faulty.events("fault_injected", payment_id=str(payment), point=AFTER_DB_BEFORE_OFFSET)) == 1
    assert len(faulty.events("processor_called", payment_id=str(payment))) == 1
    settles_after_crash = _count(
        e2e, "SELECT count(*) FROM transactions WHERE reference = $1 AND kind = 'PAYMENT_SETTLE'", str(payment)
    )
    assert settles_after_crash == 1, "the effect committed before the crash"

    with _worker(e2e, group) as recovery:
        run(_quiescent(e2e, group, [payment]))
        assert recovery.stop() == 0, recovery.diagnostics()

    assert len(recovery.events("duplicate_skipped", event_id=event_id)) == 1
    assert recovery.events("processing_failed") == []
    assert _count(
        e2e, "SELECT count(*) FROM transactions WHERE reference = $1 AND kind = 'PAYMENT_SETTLE'", str(payment)
    ) == 1
