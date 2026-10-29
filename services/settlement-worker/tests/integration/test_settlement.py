"""settlement.handle against the real Flyway-built schema (D-074, D-075, D-082, D-102, D-103).

Payments are authorized through the real API, so ESCROW holds their amounts exactly as in
production, and each event is built from its outbox row the way the relay builds it. ESCROW and
PLATFORM_FEES are shared singletons: they are checked by deltas measured inside each test;
everything else is scoped to the test's own payment, accounts and event ids.
"""

import asyncio
import json
import re
import uuid
from datetime import timedelta
from decimal import Decimal

import pytest
from structlog.testing import capture_logs
from support.outbox_events import authorized_event, with_changes

from settlement_worker import db, envelope, events, processor, settlement

GROUP = "settlement-worker"
TOPIC = "ledger.payments.v1"
LOCK_TIMEOUT = timedelta(seconds=5)


def run(coro):
    return asyncio.run(coro)


class Ledger:
    """Reads, as ledger_api_app (and the worker for its dedup rows)."""

    def __init__(self, app, worker):
        self.app = app
        self.worker = worker

    async def system_ids(self):
        rows = await self.app.fetch(
            "SELECT name, id FROM accounts WHERE name IN ('system:escrow', 'system:platform_fees')"
        )
        ids = {r["name"]: r["id"] for r in rows}
        return ids["system:escrow"], ids["system:platform_fees"]

    async def totals(self, account_id):
        row = await self.app.fetchrow(
            "SELECT a.balance, (SELECT COALESCE(SUM(e.amount), 0) FROM entries e WHERE e.account_id = a.id) AS sum "
            "FROM accounts a WHERE a.id = $1",
            account_id,
        )
        return row["balance"], row["sum"]

    async def status(self, payment_id):
        return await self.app.fetchval("SELECT status FROM payments WHERE id = $1", payment_id)

    async def postings(self, payment_id, kind):
        return [
            r["id"]
            for r in await self.app.fetch(
                "SELECT id FROM transactions WHERE reference = $1 AND kind = $2 ORDER BY id", str(payment_id), kind
            )
        ]

    async def transaction(self, transaction_id):
        return await self.app.fetchrow(
            "SELECT kind, reference, correlation_id FROM transactions WHERE id = $1", transaction_id
        )

    async def entries(self, transaction_id):
        return [
            (r["account_id"], r["amount"])
            for r in await self.app.fetch(
                "SELECT account_id, amount FROM entries WHERE transaction_id = $1 ORDER BY id", transaction_id
            )
        ]

    async def results(self, payment_id):
        return await self.app.fetch(
            "SELECT topic, event_type, payload::text AS payload, headers::text AS headers FROM outbox "
            "WHERE message_key = $1 AND event_type <> 'payment.authorized' ORDER BY id",
            str(payment_id),
        )

    async def dedup_rows(self, event_id):
        return await self.worker.fetchval(
            "SELECT count(*) FROM processed_messages WHERE consumer_group = $1 AND event_id = $2", GROUP, event_id
        )


async def _open(app_db, worker_db):
    return Ledger(await app_db.connect(), await worker_db.connect())


async def _close(ledger):
    await ledger.app.close()
    await ledger.worker.close()


def _authorize(api, amount, *, correlation_id=None):
    customer = api.create_account("CUSTOMER")
    api.deposit(customer, "200.00")
    merchant = api.create_account("MERCHANT")
    payment_id = api.authorize(customer, merchant, amount, correlation_id=correlation_id)
    return customer, merchant, payment_id


async def _handle(conn, raw):
    event = envelope.parse(raw)
    decision = processor.decide(event.amount)  # the pure decision (D-112); these cases test settlement.handle
    return await settlement.handle(conn, event, decision, consumer_group=GROUP, topic=TOPIC, lock_timeout=LOCK_TIMEOUT)


def _events(logs, name):
    return [entry for entry in logs if entry["event"] == name]


def test_settle_posts_once_in_one_transaction(ledger_api, app_db, worker_db):
    correlation_id = "t7-" + uuid.uuid4().hex
    customer, merchant, payment_id = _authorize(ledger_api, "100.00", correlation_id=correlation_id)

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            event_id = envelope.parse(raw).event_id
            escrow, fees = await ledger.system_ids()
            before = {a: await ledger.totals(a) for a in (escrow, merchant, fees, customer)}
            with capture_logs() as logs:
                outcome = await _handle(ledger.worker, raw)

            assert outcome is settlement.Outcome.SETTLED
            assert await ledger.status(payment_id) == "SETTLED"
            settles = await ledger.postings(payment_id, "PAYMENT_SETTLE")
            assert len(settles) == 1
            row = await ledger.transaction(settles[0])
            assert (row["kind"], row["reference"], row["correlation_id"]) == (
                "PAYMENT_SETTLE",
                str(payment_id),
                correlation_id,
            )
            expected = {escrow: Decimal("-100.00"), merchant: Decimal("97.10"), fees: Decimal("2.90")}
            entries = await ledger.entries(settles[0])
            assert [a for a, _ in entries] == sorted(expected), "entries inserted in account-id order"
            assert dict(entries) == expected
            for account, delta in {**expected, customer: Decimal("0")}.items():
                balance, entry_sum = await ledger.totals(account)
                assert balance - before[account][0] == delta, f"balance delta of {account}"
                assert entry_sum - before[account][1] == delta, f"entry-sum delta of {account}"
                assert balance == entry_sum, f"balance of {account} equals its entry sum"
            assert await ledger.dedup_rows(event_id) == 1
            results = await ledger.results(payment_id)
            assert len(results) == 1
            assert (results[0]["topic"], results[0]["event_type"]) == (TOPIC, "payment.settled")
            assert json.loads(results[0]["payload"]) == {
                "payment_id": str(payment_id),
                "customer_account_id": customer,
                "merchant_account_id": merchant,
                "amount": "100.00",
                "fee": "2.90",
                "merchant_amount": "97.10",
                "transaction_id": settles[0],
            }
            assert json.loads(results[0]["headers"]) == {"correlation_id": correlation_id}
            assert [(e["payment_id"], e["transaction_id"]) for e in _events(logs, events.PAYMENT_SETTLED)] == [
                (str(payment_id), settles[0])
            ]
        finally:
            await _close(ledger)

    run(scenario())


def test_declined_payment_is_released_to_the_customer(ledger_api, app_db, worker_db):
    customer, merchant, payment_id = _authorize(ledger_api, "10.13")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            escrow, fees = await ledger.system_ids()
            before = {a: await ledger.totals(a) for a in (escrow, customer, merchant, fees)}
            with capture_logs() as logs:
                outcome = await _handle(ledger.worker, raw)

            assert outcome is settlement.Outcome.RELEASED
            assert await ledger.status(payment_id) == "FAILED"
            assert await ledger.postings(payment_id, "PAYMENT_SETTLE") == []
            releases = await ledger.postings(payment_id, "PAYMENT_RELEASE")
            assert len(releases) == 1
            expected = {escrow: Decimal("-10.13"), customer: Decimal("10.13")}
            assert dict(await ledger.entries(releases[0])) == expected
            for account, delta in {**expected, merchant: Decimal("0"), fees: Decimal("0")}.items():
                balance, entry_sum = await ledger.totals(account)
                assert balance - before[account][0] == delta, f"balance delta of {account}"
                assert balance == entry_sum
            results = await ledger.results(payment_id)
            assert [r["event_type"] for r in results] == ["payment.failed"]
            assert json.loads(results[0]["payload"]) == {
                "payment_id": str(payment_id),
                "customer_account_id": customer,
                "merchant_account_id": merchant,
                "amount": "10.13",
                "fee": "0.29",
                "transaction_id": releases[0],
            }
            assert len(_events(logs, events.PAYMENT_RELEASED)) == 1
        finally:
            await _close(ledger)

    run(scenario())


def test_zero_fee_settles_without_a_fee_leg(ledger_api, app_db, worker_db):
    _, merchant, payment_id = _authorize(ledger_api, "0.01")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            escrow, _ = await ledger.system_ids()
            assert await _handle(ledger.worker, raw) is settlement.Outcome.SETTLED
            settles = await ledger.postings(payment_id, "PAYMENT_SETTLE")
            assert dict(await ledger.entries(settles[0])) == {escrow: Decimal("-0.01"), merchant: Decimal("0.01")}
            payload = json.loads((await ledger.results(payment_id))[0]["payload"])
            assert (payload["fee"], payload["merchant_amount"]) == ("0.00", "0.01")
        finally:
            await _close(ledger)

    run(scenario())


def test_redelivery_is_skipped_by_the_dedup_row(ledger_api, app_db, worker_db):
    customer, merchant, payment_id = _authorize(ledger_api, "20.00")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            event_id = envelope.parse(raw).event_id
            assert await _handle(ledger.worker, raw) is settlement.Outcome.SETTLED
            escrow, fees = await ledger.system_ids()
            accounts = (escrow, fees, customer, merchant)
            before = ([await ledger.totals(a) for a in accounts], await ledger.results(payment_id))

            with capture_logs() as logs:
                outcome = await _handle(ledger.worker, raw)

            assert outcome is settlement.Outcome.DUPLICATE_SKIPPED
            assert ([await ledger.totals(a) for a in accounts], await ledger.results(payment_id)) == before
            assert len(await ledger.postings(payment_id, "PAYMENT_SETTLE")) == 1
            assert await ledger.dedup_rows(event_id) == 1
            skipped = _events(logs, events.DUPLICATE_SKIPPED)
            assert [(e["event_id"], e["consumer_group"]) for e in skipped] == [(str(event_id), GROUP)]
            assert _events(logs, events.PAYMENT_NOT_AUTHORIZED) == []
        finally:
            await _close(ledger)

    run(scenario())


def test_new_event_for_a_handled_payment_is_stopped_by_the_status_layer(ledger_api, app_db, worker_db):
    _, _, payment_id = _authorize(ledger_api, "30.00")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            assert await _handle(ledger.worker, raw) is settlement.Outcome.SETTLED
            second_id = uuid.uuid4()
            second = with_changes(raw, event_id=str(second_id))

            with capture_logs() as logs:
                outcome = await _handle(ledger.worker, second)

            assert outcome is settlement.Outcome.PAYMENT_NOT_AUTHORIZED
            assert await ledger.dedup_rows(second_id) == 1, "the dedup row commits"
            assert len(await ledger.postings(payment_id, "PAYMENT_SETTLE")) == 1
            assert len(await ledger.results(payment_id)) == 1
            assert [(e["event_id"], e["status"]) for e in _events(logs, events.PAYMENT_NOT_AUTHORIZED)] == [
                (str(second_id), "SETTLED")
            ]
        finally:
            await _close(ledger)

    run(scenario())


def test_event_for_an_unknown_payment_rolls_back(ledger_api, app_db, worker_db):
    _, _, payment_id = _authorize(ledger_api, "15.00")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = with_changes(await authorized_event(ledger.app, payment_id), data={"payment_id": str(uuid.uuid4())})
            with pytest.raises(settlement.UnknownPayment):
                await _handle(ledger.worker, raw)
            assert await ledger.dedup_rows(envelope.parse(raw).event_id) == 0
            assert await ledger.status(payment_id) == "AUTHORIZED"
        finally:
            await _close(ledger)

    run(scenario())


def test_dedup_row_and_effect_commit_together(ledger_api, app_db, worker_db, monkeypatch):
    """Invariant 6: a failure after the outbox insert, before the commit, leaves nothing behind."""
    _, _, payment_id = _authorize(ledger_api, "40.00")
    original = settlement._append_result

    async def failing(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("injected failure after the outbox insert, before the commit")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            event_id = envelope.parse(raw).event_id
            monkeypatch.setattr(settlement, "_append_result", failing)
            with pytest.raises(RuntimeError, match="injected failure"):
                await _handle(ledger.worker, raw)

            assert await ledger.dedup_rows(event_id) == 0
            assert await ledger.postings(payment_id, "PAYMENT_SETTLE") == []
            assert await ledger.results(payment_id) == []
            assert await ledger.status(payment_id) == "AUTHORIZED"

            monkeypatch.undo()
            assert await _handle(ledger.worker, raw) is settlement.Outcome.SETTLED
            assert await ledger.dedup_rows(event_id) == 1
        finally:
            await _close(ledger)

    run(scenario())


@pytest.mark.parametrize("field, value", [("amount", "41.00"), ("fee", "1.17")])
def test_event_disagreeing_with_the_payment_rolls_back(ledger_api, app_db, worker_db, field, value):
    _, _, payment_id = _authorize(ledger_api, "40.00")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = with_changes(await authorized_event(ledger.app, payment_id), data={field: value})
            with pytest.raises(settlement.EventMismatch):
                await _handle(ledger.worker, raw)
            assert await ledger.dedup_rows(envelope.parse(raw).event_id) == 0
            assert await ledger.status(payment_id) == "AUTHORIZED"
            assert await ledger.postings(payment_id, "PAYMENT_SETTLE") == []
            assert await ledger.results(payment_id) == []
        finally:
            await _close(ledger)

    run(scenario())


_LOCKS_ACCOUNTS = re.compile(r"UPDATE\s+accounts|INSERT\s+INTO\s+entries|FROM\s+accounts.*\bFOR\s+(UPDATE|NO KEY|SHARE|KEY)", re.I | re.S)


def test_accounts_are_locked_in_one_ascending_statement(ledger_api, app_db, worker_db):
    """D-075: one FOR UPDATE over exactly the legs' accounts, sorted, before anything else locks one."""
    _, merchant, payment_id = _authorize(ledger_api, "50.00")

    async def scenario():
        ledger = await _open(app_db, worker_db)
        try:
            raw = await authorized_event(ledger.app, payment_id)
            escrow, fees = await ledger.system_ids()
            queries = []
            ledger.worker.add_query_logger(lambda q: queries.append((q.query, q.args)))
            assert await _handle(ledger.worker, raw) is settlement.Outcome.SETTLED

            locking = [i for i, (sql, _) in enumerate(queries) if re.search(r"\bFOR\s+UPDATE\b", sql, re.I)]
            assert len(locking) == 1, [queries[i][0] for i in locking]
            sql, args = queries[locking[0]]
            assert re.search(r"FROM\s+accounts", sql, re.I)
            assert list(args[0]) == sorted([escrow, merchant, fees])
            earlier = [s for s, _ in queries[: locking[0]] if _LOCKS_ACCOUNTS.search(s)]
            assert earlier == [], f"statements locking accounts rows before the lock: {earlier}"
        finally:
            await _close(ledger)

    run(scenario())


def test_worker_refuses_any_role_but_settlement_worker(connect_params):
    async def scenario():
        with pytest.raises(db.WorkerRoleError):
            await db.connect(**connect_params("ledger_api_app"))
        conn = await db.connect(**connect_params("settlement_worker"))
        await conn.close()

    run(scenario())
