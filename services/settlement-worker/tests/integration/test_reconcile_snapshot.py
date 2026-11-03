"""Reconciliation against the real Flyway-built schema (D-114), read-only, with no drift injected.

The ledger is built through the real API (deposits, a transfer, a payment left AUTHORIZED) and a
payment settled by settlement.handle, so ESCROW holds exactly what production would. One run of the
real Reconciler, on a fresh metrics registry, must find every account clean. The session database is
shared with the other integration tests; none of them leaves a drift, so the checks are global.
The drift-injection proof is T12; the snapshot proofs are T13/T14.
"""

import asyncio
import re
from datetime import datetime, timedelta

import asyncpg
import pytest
from prometheus_client import CollectorRegistry
from structlog.testing import capture_logs
from support import stack
from support.outbox_events import authorized_event

from settlement_worker import envelope, processor, reconcile, settlement
from settlement_worker.config import Config
from settlement_worker.metrics import ReconcileMetrics

D097 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")


def run(coro):
    return asyncio.run(coro)


def _config(database: stack.Database) -> Config:
    return Config.from_env(
        {
            "POSTGRES_HOST": database.host,
            "POSTGRES_PORT": str(database.port),
            "POSTGRES_DB": database.name,
            "SETTLEMENT_WORKER_PASSWORD": stack.WORKER_PASSWORD,
            "RECONCILE_INTERVAL_MS": "60000",
        }
    )


def _seed(api) -> tuple:
    customer = api.create_account("CUSTOMER")
    other = api.create_account("CUSTOMER")
    merchant = api.create_account("MERCHANT")
    api.deposit(customer, "500.00")
    api.deposit(other, "5.00")
    api.transfer(customer, other, "20.00")
    held = api.authorize(customer, merchant, "30.00")  # stays AUTHORIZED: ESCROW holds it
    settled = api.authorize(customer, merchant, "12.34")
    return held, settled


async def _settle(app, worker, payment_id) -> None:
    event = envelope.parse(await authorized_event(app, payment_id))
    await settlement.handle(
        worker, event, processor.Decision.APPROVED, consumer_group="t11-reconcile",
        topic=stack.TOPIC, lock_timeout=timedelta(seconds=5),
    )


def test_one_run_on_a_consistent_ledger_finds_no_drift(ledger_api, migrated_db, app_db, worker_db):
    held, settled = _seed(ledger_api)
    registry = CollectorRegistry()
    reconciler = reconcile.Reconciler(_config(migrated_db), ReconcileMetrics(registry))

    async def scenario():
        app = await app_db.connect()
        worker = await worker_db.connect()
        try:
            await _settle(app, worker, settled)
            assert await app.fetchval("SELECT status FROM payments WHERE id = $1", held) == "AUTHORIZED"
            before = await app.fetchval("SELECT clock_timestamp()")
            with capture_logs() as logs:
                await reconciler.run_one()
            after = await app.fetchval("SELECT clock_timestamp()")
            accounts = await app.fetchval("SELECT count(*) FROM accounts")
            max_transaction_id = await app.fetchval("SELECT max(id) FROM transactions")  # as ledger_api_app
            return logs, before, after, accounts, max_transaction_id
        finally:
            reconciler.close()
            await app.close()
            await worker.close()

    logs, before, after, accounts, max_transaction_id = run(scenario())
    assert [entry["event"] for entry in logs] == ["reconciliation_completed"], logs
    [completed] = logs
    assert completed["log_level"] == "info"
    assert (completed["run_id"], completed["accounts"], completed["drifted_accounts"]) == (1, accounts, 0)
    assert (completed["global_sum"], completed["escrow_drift"]) == ("0.0000", "0.0000")
    assert completed["max_transaction_id"] == max_transaction_id
    assert completed["correlation_id"] is None and isinstance(completed["duration_ms"], int)
    # as_of is the DB clock (now() of the snapshot), in D-097's format.
    assert D097.fullmatch(completed["as_of"]), completed["as_of"]
    assert before <= datetime.fromisoformat(completed["as_of"]) <= after, (before, completed["as_of"], after)
    assert registry.get_sample_value("ledger_reconciliation_runs_total", {"result": "ok"}) == 1
    assert registry.get_sample_value("ledger_drift_accounts") == 0
    assert registry.get_sample_value("ledger_escrow_drift_amount") == 0


def test_the_reconciler_connects_as_settlement_worker_and_its_snapshot_is_read_only(migrated_db):
    reconciler = reconcile.Reconciler(_config(migrated_db), ReconcileMetrics(CollectorRegistry()))

    async def scenario():
        conn = await reconciler.connect()
        try:
            assert await conn.fetchval("SELECT current_user") == "settlement_worker"
            assert await conn.fetchval("SELECT current_setting('application_name')") == reconcile.APPLICATION_NAME
            async with reconcile.snapshot(conn, 30000):
                assert await conn.fetchval("SHOW transaction_read_only") == "on"
                assert await conn.fetchval("SHOW statement_timeout") == "30s"
                # settlement_worker may INSERT these columns (V8); READ ONLY refuses it all the same.
                with pytest.raises(asyncpg.ReadOnlySQLTransactionError) as refused:
                    await conn.execute(
                        "INSERT INTO transactions (kind, reference, correlation_id) VALUES ('DEPOSIT', 't11', NULL)"
                    )
                raise _Rollback  # the failed statement aborted the transaction; leave it through an exception
        except _Rollback:
            pass
        finally:
            conn.terminate()
        return refused.value

    refusal = run(scenario())
    assert refusal.sqlstate == "25006", refusal


class _Rollback(Exception):
    pass
