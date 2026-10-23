"""D-017 / D-086 negative control: separate-transaction dedup double-applies.

The naive worker (naive_worker.py) commits the effect in one transaction and the dedup row plus the
status transition in a later one. Crashing between them (AFTER_EFFECT_BEFORE_MARK) leaves the status
AUTHORIZED, so the redelivered record passes the pre-check and posts again. This test asserts that
the naive variant FAILS: every iteration ends with two balanced postings, two result events and one
dedup row, and escrow short by exactly the duplicated amounts. The balance trigger can't see it (each
posting balances); only escrow against the AUTHORIZED total can. One ballast payment, never settled
by the naive worker, keeps escrow covering the duplicates, so they commit instead of tripping
no_overdraft. It backs a "demonstrated" claim, never a "debugged" one (D-016).
"""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import pytest

from support import hardware
from support.stack import WORKER_PASSWORD
from support.worker_process import start_worker, wait_quiescent

from settlement_worker.config import FAULT_POINTS

GROUP = "negative-control-separate-tx"
FAULT_POINT = "AFTER_EFFECT_BEFORE_MARK"
NAIVE = [sys.executable, str(Path(__file__).with_name("naive_worker.py"))]

# Iteration i: approve 100.00, decline 10.13, approve 42.50, decline 113.13 (cents 13 decline, D-073).
AMOUNTS = ("100.00", "10.13", "42.50", "113.13")
APPROVED = {"100.00", "42.50"}
TERMINAL = ("SETTLED", "FAILED")


def _iterations() -> int:
    raw = os.environ.get("NEGATIVE_CONTROL_ITERATIONS", "3")
    if not raw.isdigit() or int(raw) < 1:
        raise pytest.UsageError(f"NEGATIVE_CONTROL_ITERATIONS must be a positive integer, got {raw!r}")
    return int(raw)


N = _iterations()
BALLAST = Decimal(N) * max(Decimal(a) for a in AMOUNTS)


def run(coro):
    return asyncio.run(coro)


async def _fetch(db, query, *args):
    conn = await db.connect()
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


def _app(control, query, *args):
    return run(_fetch(control.app_db, query, *args))


def _naive(control, **extra):
    return start_worker(
        database=control.database,
        bootstrap_servers=control.kafka.bootstrap_servers,
        group=GROUP,
        log_dir=control.log_dir,
        command=NAIVE,
        extra_env={k: str(v) for k, v in extra.items()},
    )


async def _quiescent(control, payment_ids):
    conn = await control.app_db.connect()
    try:
        return await wait_quiescent(
            app_conn=conn,
            bootstrap_servers=control.kafka.bootstrap_servers,
            group=GROUP,
            payment_ids=payment_ids,
            require_terminal=False,  # the ballast stays AUTHORIZED by design (D-105 definition)
        )
    finally:
        await conn.close()


def _wait_published(control, payment_id, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = _app(
            control,
            "SELECT published_at IS NOT NULL AS published FROM outbox "
            "WHERE message_key = $1 AND event_type = 'payment.authorized'",
            str(payment_id),
        )
        if rows and rows[0]["published"]:
            return
        time.sleep(0.2)
    raise AssertionError(f"payment.authorized for {payment_id} not published within {timeout} s")


def _postings(control, payment_id, kind) -> list[Decimal]:
    """Each posting of that kind for the payment, as the sum of its entries."""
    rows = _app(
        control,
        "SELECT t.id, sum(e.amount) AS total FROM transactions t JOIN entries e ON e.transaction_id = t.id "
        "WHERE t.reference = $1 AND t.kind = $2 GROUP BY t.id ORDER BY t.id",
        str(payment_id),
        kind,
    )
    return [r["total"] for r in rows]


def _results(control, payment_id) -> int:
    return _app(
        control,
        "SELECT count(*) FROM outbox WHERE message_key = $1 AND event_type IN ('payment.settled', 'payment.failed')",
        str(payment_id),
    )[0][0]


def _dedup_rows(control, event_id) -> int:
    """processed_messages is readable only by settlement_worker (V12 grants)."""
    return run(
        _fetch(
            control.worker_db,
            "SELECT count(*) FROM processed_messages WHERE consumer_group = $1 AND event_id = $2",
            GROUP,
            event_id,
        )
    )[0][0]


def _status(control, payment_id) -> str:
    return _app(control, "SELECT status FROM payments WHERE id = $1", payment_id)[0][0]


def _event_id(control, payment_id):
    return _app(
        control,
        "SELECT event_id FROM outbox WHERE message_key = $1 AND event_type = 'payment.authorized'",
        str(payment_id),
    )[0][0]


def _snapshot(control):
    """One statement, so one snapshot (D-019), including the duplicated amounts themselves."""
    return _app(
        control,
        "SELECT "
        "(SELECT coalesce(sum(amount), 0) FROM entries) AS entry_total, "
        "(SELECT count(*) FROM accounts a WHERE a.balance <> "
        "  (SELECT coalesce(sum(e.amount), 0) FROM entries e WHERE e.account_id = a.id)) AS drifted, "
        "(SELECT balance FROM accounts WHERE name = 'system:escrow') AS escrow, "
        "(SELECT coalesce(sum(amount), 0) FROM payments WHERE status = 'AUTHORIZED') AS authorized, "
        "(SELECT coalesce(sum(p.amount * (d.n - 1)), 0) FROM payments p JOIN "
        "  (SELECT reference, count(*) AS n FROM transactions "
        "   WHERE kind IN ('PAYMENT_SETTLE', 'PAYMENT_RELEASE') GROUP BY reference) d "
        "  ON d.reference = p.id::text WHERE d.n > 1) AS duplicated",
    )[0]


@dataclass
class Tally:
    duplicated: int = 0
    duplicated_amount: Decimal = Decimal("0")
    iterations: list[str] = field(default_factory=list)


@pytest.fixture(scope="module")
def tally(request):
    counted = Tally()
    yield counted
    if counted.iterations:
        line = (
            f"NEGATIVE_CONTROL separate_tx_dedup iterations={len(counted.iterations)} "
            f"duplicated={counted.duplicated} {hardware.summary_fields()}"
        )
        reporter = request.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(line)
        else:
            print(line)
        # The naive variant must FAIL every time (D-017): one duplicate per iteration.
        assert counted.duplicated == len(counted.iterations) == N, line


@pytest.fixture(scope="module")
def ballast(control, tally):
    """Authorized first and skipped by the naive worker, so escrow covers every duplicate (D-086)."""
    assert BALLAST >= N * max(Decimal(a) for a in AMOUNTS)
    customer = control.api.create_account("CUSTOMER")
    control.api.deposit(customer, str(BALLAST))
    merchant = control.api.create_account("MERCHANT")
    payment = control.api.authorize(customer, merchant, str(BALLAST))
    _wait_published(control, payment)
    with _naive(control, NAIVE_SKIP_PAYMENT_ID=payment) as primer:
        run(_quiescent(control, [payment]))
        assert primer.stop() == 0, primer.diagnostics()
    assert primer.events("processing_failed") == [], primer.diagnostics()
    assert primer.events("retry_scheduled") == [], primer.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert primer.events("db_reconnected") == [], primer.diagnostics()  # tripwire (D-109)
    assert primer.events("offset_commit_failed") == [], primer.diagnostics()  # tripwire (D-109)
    assert _status(control, payment) == "AUTHORIZED"
    return payment


@pytest.mark.parametrize("iteration", range(N), ids=[f"separate_tx_dedup-{i:03d}" for i in range(N)])
def test_separate_transaction_dedup_double_applies(control, ballast, tally, iteration):
    api = control.api
    amount = AMOUNTS[iteration % len(AMOUNTS)]
    approve = amount in APPROVED
    kind, other = ("PAYMENT_SETTLE", "PAYMENT_RELEASE") if approve else ("PAYMENT_RELEASE", "PAYMENT_SETTLE")

    # 1. The target.
    customer = api.create_account("CUSTOMER")
    api.deposit(customer, "200.00")
    merchant = api.create_account("MERCHANT")
    target = api.authorize(customer, merchant, amount)
    _wait_published(control, target)
    event_id = _event_id(control, target)

    # 2. Crash between the effect and the mark.
    with _naive(control, NAIVE_FAULT_PAYMENT_ID=target, NAIVE_SKIP_PAYMENT_ID=ballast) as w1:
        assert w1.wait_exit(60) == 86, w1.diagnostics()
    assert w1.events("retry_scheduled") == [], w1.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert w1.events("db_reconnected") == [], w1.diagnostics()  # tripwire (D-109)
    assert w1.events("offset_commit_failed") == [], w1.diagnostics()  # tripwire (D-109)
    assert len(w1.events("fault_injected", point=FAULT_POINT, payment_id=str(target))) == 1, w1.diagnostics()
    w1_calls = w1.events("processor_called", payment_id=str(target))
    assert len(w1_calls) == 1, w1.diagnostics()
    decision = w1_calls[0]["decision"]
    assert decision == ("APPROVED" if approve else "DECLINED")

    # 3. Mid-state: the effect committed, the mark didn't.
    assert len(_postings(control, target, kind)) == 1
    assert _results(control, target) == 1
    assert _status(control, target) == "AUTHORIZED"
    assert _dedup_rows(control, event_id) == 0

    # 4. The restarted naive worker sees AUTHORIZED and posts again.
    with _naive(control, NAIVE_SKIP_PAYMENT_ID=ballast) as w2:
        run(_quiescent(control, [ballast, target]))
        assert w2.stop() == 0, w2.diagnostics()
    assert w2.events("processing_failed") == [], w2.diagnostics()
    assert w2.events("retry_scheduled") == [], w2.diagnostics()  # tripwire: nothing in this proof retries in place (D-108, D-123)
    assert w2.events("db_reconnected") == [], w2.diagnostics()  # tripwire (D-109)
    assert w2.events("offset_commit_failed") == [], w2.diagnostics()  # tripwire (D-109)

    # 5. The naive variant FAILS: two balanced postings, two result events, one dedup row.
    postings = _postings(control, target, kind)
    tally.iterations.append(str(target))
    if len(postings) > 1:
        tally.duplicated += len(postings) - 1
        tally.duplicated_amount += Decimal(amount) * (len(postings) - 1)
    assert len(postings) == 2, f"{kind} postings for {target}: {postings}"
    assert all(total == 0 for total in postings), postings
    assert _postings(control, target, other) == []
    assert _results(control, target) == 2
    assert _dedup_rows(control, event_id) == 1
    assert _status(control, target) == ("SETTLED" if approve else "FAILED")
    # The processor was called on both deliveries, with the same decision (D-010).
    assert [c["decision"] for c in w2.events("processor_called", payment_id=str(target))] == [decision]

    # Every posting balances, so the ledger-wide checks hold; only escrow shows the duplicates.
    snapshot = _snapshot(control)
    assert snapshot["entry_total"] == 0
    assert snapshot["drifted"] == 0
    assert snapshot["duplicated"] == tally.duplicated_amount
    assert snapshot["escrow"] == snapshot["authorized"] - snapshot["duplicated"]


def test_main_worker_rejects_the_naive_fault_point():
    """AFTER_EFFECT_BEFORE_MARK exists only in the naive entrypoint (D-086)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "NAIVE_", "WORKER_"))}
    env.update(
        SETTLEMENT_WORKER_PASSWORD=WORKER_PASSWORD,
        FAULT_POINT=FAULT_POINT,
        FAULT_PAYMENT_ID=str(uuid.uuid4()),
        WORKER_METRICS_PORT="0",
    )
    result = subprocess.run(
        [sys.executable, "-m", "settlement_worker"], env=env, capture_output=True, text=True, timeout=60, check=False
    )
    assert result.returncode == 2, result
    lines = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    assert FAULT_POINT in lines[0]["error"]
    assert result.stderr == ""


@pytest.mark.parametrize("main_point", FAULT_POINTS)
def test_naive_worker_rejects_main_fault_points(main_point):
    """The naive entrypoint honours none of main's fault points, so it refuses an armed one (D-125)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FAULT_", "NAIVE_", "WORKER_"))}
    env.update(
        SETTLEMENT_WORKER_PASSWORD=WORKER_PASSWORD,
        FAULT_POINT=main_point,
        FAULT_PAYMENT_ID=str(uuid.uuid4()),
        WORKER_METRICS_PORT="0",
    )
    result = subprocess.run(NAIVE, env=env, capture_output=True, text=True, timeout=60, check=False)
    assert result.returncode == 2, result
    lines = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert [line["event"] for line in lines] == ["config_invalid"], lines
    assert main_point in lines[0]["error"]
    assert result.stderr == ""
