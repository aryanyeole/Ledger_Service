"""Settle or release one authorized payment, exactly once (D-009, D-074, D-075, D-082).

One Postgres transaction per event, READ COMMITTED (D-061: a waiting duplicate then does nothing
instead of raising 40001), lock_timeout first:
  1. the dedup row: INSERT ... ON CONFLICT (consumer_group, event_id) DO NOTHING. 0 rows means the
     event was already applied: nothing else is written (invariant 6, D-078);
  2. the conditional status change, the second dedup layer (spec §4). No row: an existing payment
     that isn't AUTHORIZED any more was already handled (the dedup row commits, no posting); no
     payment at all can't come from normal operation (D-077 commits both together), so it raises
     and the whole transaction, dedup row included, rolls back;
  3. the event must agree with the payment row on customer, merchant, amount and fee; the posting
     always uses the row's values, never the event's;
  4. one ascending SELECT ... FOR UPDATE over exactly the accounts of the legs (invariant 4,
     D-075). Nothing before it locks an accounts row: the dedup insert and the payment update
     touch no account, and the system account ids are read without a lock;
  5. the PAYMENT_SETTLE or PAYMENT_RELEASE transaction, its entries in account-id order and the
     balances; the deferred trigger checks they sum to 0 at commit (invariant 1); entries are
     only ever inserted (invariant 2);
  6. the result event in the outbox (invariant 5): the relay publishes it.
The processor's decision is an input: nothing inside the transaction awaits anything but Postgres.
Money is Decimal throughout (invariant 3). Log events go out after the commit.
"""

import enum
import json
import time
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

import asyncpg
import structlog

from settlement_worker import events
from settlement_worker.envelope import FAILED, SETTLED, AuthorizedEvent
from settlement_worker.money import to_api_string
from settlement_worker.processor import Decision

ESCROW_NAME = "system:escrow"
PLATFORM_FEES_NAME = "system:platform_fees"

_DEDUP = (
    "INSERT INTO processed_messages (consumer_group, event_id) VALUES ($1, $2) "
    "ON CONFLICT (consumer_group, event_id) DO NOTHING"
)
_TRANSITION = (
    "UPDATE payments SET status = $2 WHERE id = $1 AND status = 'AUTHORIZED' "
    "RETURNING customer_account_id, merchant_account_id, amount, fee"
)
_LOCK_ACCOUNTS = "SELECT id, balance FROM accounts WHERE id = ANY($1::bigint[]) ORDER BY id FOR UPDATE"


class Outcome(enum.StrEnum):
    SETTLED = "settled"
    RELEASED = "released"
    DUPLICATE_SKIPPED = "duplicate_skipped"
    PAYMENT_NOT_AUTHORIZED = "payment_not_authorized"


class SettlementError(Exception):
    """A processing error: the transaction rolled back and the event must not be acknowledged."""


class UnknownPayment(SettlementError):
    pass


class EventMismatch(SettlementError):
    pass


class EscrowShortfall(SettlementError):
    pass


@dataclass(frozen=True)
class PaymentRow:
    customer_account_id: int
    merchant_account_id: int
    amount: Decimal
    fee: Decimal


def legs(decision: Decision, payment: PaymentRow, escrow_id: int, fees_id: int) -> list[tuple[int, Decimal]]:
    """The posting (spec §4, D-074): amounts from the payment row; the merchant's share is always
    amount - fee, never computed on its own; zero legs are omitted (entries.amount <> 0)."""
    amount, fee = payment.amount, payment.fee
    if decision is Decision.APPROVED:
        candidates = [(escrow_id, -amount), (payment.merchant_account_id, amount - fee), (fees_id, fee)]
    else:
        candidates = [(escrow_id, -amount), (payment.customer_account_id, amount)]
    return [(account, delta) for account, delta in candidates if delta != 0]


def _lock_timeout(value: timedelta) -> str:
    millis = value / timedelta(milliseconds=1)
    if millis <= 0 or millis != int(millis):
        raise ValueError(f"lock_timeout must be a positive whole number of milliseconds: {value}")
    return f"{int(millis)}ms"


async def handle(
    conn: asyncpg.Connection,
    event: AuthorizedEvent,
    decision: Decision,
    *,
    consumer_group: str,
    topic: str,
    lock_timeout: timedelta,
) -> Outcome:
    log = structlog.get_logger()
    status = "SETTLED" if decision is Decision.APPROVED else "FAILED"
    not_authorized_status = None
    transaction_id = None
    lock_wait_ms = None
    async with conn.transaction(isolation="read_committed"):
        # SET takes no bind parameters; set_config(..., true) is SET LOCAL.
        await conn.execute("SELECT set_config('lock_timeout', $1, true)", _lock_timeout(lock_timeout))

        if await conn.execute(_DEDUP, consumer_group, event.event_id) == "INSERT 0 0":
            outcome = Outcome.DUPLICATE_SKIPPED
        else:
            row = await conn.fetchrow(_TRANSITION, event.payment_id, status)
            if row is None:
                not_authorized_status = await conn.fetchval(
                    "SELECT status FROM payments WHERE id = $1", event.payment_id
                )
                if not_authorized_status is None:
                    raise UnknownPayment(f"no payment {event.payment_id} for event {event.event_id}")
                outcome = Outcome.PAYMENT_NOT_AUTHORIZED
            else:
                payment = PaymentRow(**dict(row))
                _check_matches(event, payment)
                transaction_id, lock_wait_ms = await _post(conn, event, decision, payment)
                await _append_result(conn, topic, event, decision, payment, transaction_id)
                outcome = Outcome.SETTLED if decision is Decision.APPROVED else Outcome.RELEASED

    # Committed.
    base = {"event_id": str(event.event_id), "payment_id": str(event.payment_id), "correlation_id": event.correlation_id}
    if outcome is Outcome.DUPLICATE_SKIPPED:
        events.emit(log, events.DUPLICATE_SKIPPED, consumer_group=consumer_group, **base)
    elif outcome is Outcome.PAYMENT_NOT_AUTHORIZED:
        events.emit(log, events.PAYMENT_NOT_AUTHORIZED, status=not_authorized_status, **base)
    else:
        name = events.PAYMENT_SETTLED if outcome is Outcome.SETTLED else events.PAYMENT_RELEASED
        events.emit(log, name, transaction_id=transaction_id, lock_wait_ms=lock_wait_ms, **base)
    return outcome


def _check_matches(event: AuthorizedEvent, payment: PaymentRow) -> None:
    expected = (event.customer_account_id, event.merchant_account_id, event.amount, event.fee)
    actual = (payment.customer_account_id, payment.merchant_account_id, payment.amount, payment.fee)
    if expected != actual:
        raise EventMismatch(
            f"event {event.event_id} disagrees with payment {event.payment_id}: "
            f"(customer, merchant, amount, fee) event={expected} row={actual}"
        )


async def _system_account(conn: asyncpg.Connection, name: str) -> int:
    account_id = await conn.fetchval("SELECT id FROM accounts WHERE name = $1", name)
    if account_id is None:
        raise SettlementError(f"{name} account is missing")
    return account_id


async def _post(
    conn: asyncpg.Connection, event: AuthorizedEvent, decision: Decision, payment: PaymentRow
) -> tuple[int, int]:
    """The posting; returns its transaction id and the whole milliseconds spent in the lock statement
    (report-only, D-087; integer nanoseconds, so no float is involved)."""
    escrow_id = await _system_account(conn, ESCROW_NAME)
    fees_id = await _system_account(conn, PLATFORM_FEES_NAME)
    posting = sorted(legs(decision, payment, escrow_id, fees_id))
    account_ids = [account for account, _ in posting]

    started = time.perf_counter_ns()
    locked = {r["id"]: r["balance"] for r in await conn.fetch(_LOCK_ACCOUNTS, account_ids)}
    lock_wait_ms = (time.perf_counter_ns() - started) // 1_000_000
    if set(locked) != set(account_ids):
        raise SettlementError(f"accounts missing: {sorted(set(account_ids) - set(locked))}")
    # Under T10's negative control, its ballast payment (D-086) is what keeps a double-applied settle from tripping this.
    if locked[escrow_id] < payment.amount:
        raise EscrowShortfall(f"escrow balance {locked[escrow_id]} < {payment.amount} for {event.payment_id}")

    kind = "PAYMENT_SETTLE" if decision is Decision.APPROVED else "PAYMENT_RELEASE"
    transaction_id = await conn.fetchval(
        "INSERT INTO transactions (kind, reference, correlation_id) VALUES ($1, $2, $3) RETURNING id",
        kind,
        str(event.payment_id),
        event.correlation_id,
    )
    for account, delta in posting:
        await conn.execute(
            "INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)",
            transaction_id,
            account,
            delta,
        )
    for account, delta in posting:
        await conn.execute("UPDATE accounts SET balance = balance + $2 WHERE id = $1", account, delta)
    return transaction_id, lock_wait_ms


async def _append_result(
    conn: asyncpg.Connection,
    topic: str,
    event: AuthorizedEvent,
    decision: Decision,
    payment: PaymentRow,
    transaction_id: int,
) -> None:
    """The result event (D-102): literal keys, money as strings at scale 2, written by the plain
    json module so the format can't drift with any serializer config (D-095 reasoning)."""
    payload = {
        "payment_id": str(event.payment_id),
        "customer_account_id": payment.customer_account_id,
        "merchant_account_id": payment.merchant_account_id,
        "amount": to_api_string(payment.amount),
        "fee": to_api_string(payment.fee),
        "transaction_id": transaction_id,
    }
    if decision is Decision.APPROVED:
        payload["merchant_amount"] = to_api_string(payment.amount - payment.fee)
    await conn.execute(
        "INSERT INTO outbox (topic, message_key, event_type, payload, headers) "
        "VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)",
        topic,
        str(event.payment_id),
        SETTLED if decision is Decision.APPROVED else FAILED,
        json.dumps(payload, separators=(",", ":")),
        json.dumps({"correlation_id": event.correlation_id}, separators=(",", ":")),
    )
