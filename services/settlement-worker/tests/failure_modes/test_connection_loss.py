"""DB connection loss and failed offset commits (D-109, D-129) on real worker processes, on its own
failure_modes stack. Cases run in order; each restores the container before the next.

(a) Hang: the Postgres container is paused (docker pause) while P is being settled. Every DB call
    and every reconnect ends at the command timeout as DbTimeout and is retried; the worker never
    exits. After the unpause it reconnects (db_reconnected) and settles P exactly once.
(b) Reset in the transaction: the worker blocks on ESCROW (held by the test as ledger_api_app)
    inside its settle transaction, and the test terminates its backend (pg_terminate_backend, as the
    container superuser). Connection-class retry, reconnect, then P settles once.
(c) Reconnect refusal: role_swap_worker reconnects as ledger_api_app; the repeated role check
    refuses it: config_invalid, exit 2, P uncommitted.
(d) Failed offset commit (FAIL_OFFSET_COMMIT on P): two offset_commit_failed on the backoff, then
    the commit lands. The record is never re-run, and nothing new is fetched while the commit is
    pending: R, published behind P meanwhile, is processed only after P's commit.
(e) Stop while a commit is pending: exit 0, offset uncommitted; the next worker's redelivery is
    absorbed by dedup.
(f) A commit pending longer than max.poll.interval: the worker keeps polling with every partition
    paused, so it stays in the group (one partitions_assigned) and the commit lands.
"""

import sys
import time
from datetime import timedelta
from pathlib import Path

from support import stack
from support.failure_kit import (
    CLOCK_ALLOWANCE,
    EscrowHolder,
    Paused,
    assert_not_past,
    committed,
    dedup_rows,
    funded_pair,
    postings,
    results,
    status,
    terminate_worker_backends,
    ts,
    wait,
    wait_idle,
    worker,
    worker_backends,
)

TESTS = Path(__file__).resolve().parents[1]


def _settled_once(fm, group, payment) -> None:
    assert postings(fm, payment) == 1, payment
    assert dedup_rows(fm, group, payment) == 1, payment
    assert results(fm, payment) == 1, payment
    assert status(fm, payment) == "SETTLED", payment


# (a)
def test_a_paused_database_times_out_retries_and_reconnects(fm):
    group = "conn-hang"
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")
    values = fm.read_events(payment)  # before the pause
    timeouts = {"WORKER_LOCK_TIMEOUT_MS": 300, "WORKER_DB_COMMAND_TIMEOUT_MS": 1000}

    with worker(fm, group, **timeouts) as w:
        wait_idle(fm, w, group)
        with Paused(fm):
            fm.produce(values)
            paused_at = time.monotonic()
            first = wait(
                w,
                lambda: [r for r in w.events("retry_scheduled") if r["error"].startswith("DbTimeout")],
                30,
                "a DbTimeout retry while paused",
            )
            while time.monotonic() - paused_at < 3.5:  # >= 3 x the command timeout
                assert w.process.poll() is None, w.diagnostics()
                time.sleep(0.1)
            assert w.process.poll() is None, w.diagnostics()
            while_paused = w.events("retry_scheduled")
        reconnected = w.wait_for("db_reconnected", timeout=60)
        w.wait_for("payment_settled", payment_id=str(payment), timeout=60)
        partition, offset = fm.position(payment)
        wait(w, lambda: (committed(fm, group, partition) or 0) > offset, 30, "P's offset committed")
        assert w.stop() == 0, w.diagnostics()

    print(f"HANG retries_while_paused={len(while_paused)} first_error={first[0]['error'][:60]!r}")
    assert while_paused and all(r["error_class"] == "Transient" for r in while_paused), while_paused
    assert all(r["error"].startswith("DbTimeout") for r in while_paused), while_paused
    assert reconnected["current_user"] == "settlement_worker"
    assert reconnected["level"] == "info"
    assert len(w.events("payment_settled", payment_id=str(payment))) == 1
    _settled_once(fm, group, payment)
    assert w.events("processing_failed") == []


# (b)
def test_a_backend_killed_inside_the_transaction_reconnects(fm):
    group = "conn-reset"
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(fm, group) as w:
        wait_idle(fm, w, group)
        with EscrowHolder(fm) as holder:
            fm.publish(payment)
            wait(w, lambda: worker_backends(fm, "wait_event_type = 'Lock'"), 30, "the worker blocked on ESCROW")
            assert terminate_worker_backends(fm, "wait_event_type = 'Lock'") == 1
            retry = w.wait_for("retry_scheduled", timeout=30)
            reconnected = w.wait_for("db_reconnected", timeout=30)
            holder.release()
        w.wait_for("payment_settled", payment_id=str(payment), timeout=60)
        assert w.stop() == 0, w.diagnostics()

    print(f"RESET retry sqlstate={retry['sqlstate']} error={retry['error'][:80]!r}")
    assert retry["error_class"] == "Transient", retry
    assert ts(reconnected) > ts(retry)
    assert reconnected["current_user"] == "settlement_worker"
    _settled_once(fm, group, payment)
    assert w.events("processing_failed") == []


# (c)
def test_a_reconnect_with_the_wrong_role_exits_2(fm):
    group = "conn-role-swap"
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(
        fm,
        group,
        command=[sys.executable, "-m", "support.role_swap_worker"],
        PYTHONPATH=TESTS,
        ROLE_SWAP_PASSWORD=stack.APP_PASSWORD,
    ) as w:
        wait_idle(fm, w, group)
        assert terminate_worker_backends(fm) == 1
        fm.publish(payment)
        assert w.wait_exit(60) == 2, w.diagnostics()

    refusals = w.events("config_invalid")
    assert len(refusals) == 1, w.diagnostics()
    assert "connected as ledger_api_app, expected settlement_worker" in refusals[0]["error"]
    assert w.events("db_reconnected") == []
    partition, offset = fm.position(payment)
    assert_not_past(committed(fm, group, partition), offset, "after the refusal")
    assert status(fm, payment) == "AUTHORIZED" and postings(fm, payment) == 0


def _publish_until(fm, customer, merchant, predicate, limit: int = 12):
    others = {}
    for _ in range(limit):
        candidate = fm.api.authorize(customer, merchant, "10.00")
        fm.publish(candidate)
        position = fm.position(candidate)
        if predicate(position):
            return candidate, others
        others[candidate] = position
    raise AssertionError(f"no candidate matched within {limit} publishes: {others}")


# (d)
def test_a_failed_offset_commit_is_recommitted_not_rerun(fm):
    group = "commit-fail"
    backoff = {"WORKER_RETRY_BASE_MS": 1000, "WORKER_RETRY_MAX_MS": 4000}
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(fm, group, **backoff) as primer:
        wait_idle(fm, primer, group)
        assert primer.stop() == 0
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with worker(fm, group, FAULT_POINT="FAIL_OFFSET_COMMIT", FAULT_PAYMENT_ID=payment, **backoff) as w:
        mine = {"partition": partition, "offset": offset}
        w.wait_for("offset_commit_failed", timeout=60, **mine)
        # Published while P's commit is pending: must not be fetched until the commit lands.
        r, _ = _publish_until(fm, customer, merchant, lambda pos: pos[0] == partition and pos[1] > offset)
        wait(w, lambda: (committed(fm, group, partition) or 0) > offset, 30, "P's offset committed")
        w.wait_for("payment_settled", payment_id=str(r), timeout=60)
        assert w.process.poll() is None, w.diagnostics()
        assert w.stop() == 0, w.diagnostics()

    failures = w.events("offset_commit_failed", **mine)
    assert [f["attempt"] for f in failures] == [1, 2], w.diagnostics()
    assert [f["delay_ms"] for f in failures] == [1000, 2000]
    assert all(f["level"] == "warning" for f in failures)
    assert len(w.events("fault_injected", point="FAIL_OFFSET_COMMIT", payment_id=str(payment))) == 2
    assert len(w.events("processor_called", payment_id=str(payment))) == 1, "never re-run"
    assert w.events("retry_scheduled") == []
    assert w.events("duplicate_skipped") == []
    assert w.events("processing_failed") == []
    _settled_once(fm, group, payment)
    # The fetch gate: R's first attempt comes only after P's last recommit was due.
    r_call = w.events("processor_called", payment_id=str(r))[0]
    gate = ts(failures[-1]) + timedelta(milliseconds=failures[-1]["delay_ms"]) - CLOCK_ALLOWANCE
    assert ts(r_call) >= gate, (r_call, failures[-1])
    _settled_once(fm, group, r)


# (e)
def test_a_stop_with_a_commit_pending_exits_0_and_dedup_absorbs_the_redelivery(fm):
    group = "commit-fail-stop"
    backoff = {"WORKER_RETRY_BASE_MS": 2000, "WORKER_RETRY_MAX_MS": 2000}
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(fm, group) as primer:
        wait_idle(fm, primer, group)
        assert primer.stop() == 0
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with worker(fm, group, FAULT_POINT="FAIL_OFFSET_COMMIT", FAULT_PAYMENT_ID=payment, **backoff) as w:
        w.wait_for("offset_commit_failed", timeout=60, partition=partition, offset=offset)
        stopped_at = time.monotonic()
        code = w.stop()
        stop_ms = (time.monotonic() - stopped_at) * 1000
    print(f"STOP_WITH_PENDING_COMMIT stop_ms={stop_ms:.0f} backoff_ms=2000")
    assert code == 0, w.diagnostics()
    assert stop_ms < 2000, stop_ms
    assert len(w.events("offset_commit_failed")) == 1
    assert_not_past(committed(fm, group, partition), offset, "after the stop")
    assert postings(fm, payment) == 1, "the effect had committed"

    with worker(fm, group) as again:
        again.wait_for("duplicate_skipped", payment_id=str(payment), timeout=60)
        wait(again, lambda: (committed(fm, group, partition) or 0) > offset, 30, "P's offset committed")
        assert again.stop() == 0, again.diagnostics()
    assert again.events("processor_called", payment_id=str(payment)), "redelivered"
    _settled_once(fm, group, payment)


# (f)
def test_a_long_pending_commit_keeps_polling_and_stays_in_the_group(fm):
    group = "commit-fail-poll"
    settings = {"WORKER_MAX_POLL_INTERVAL_MS": 2000, "WORKER_RETRY_BASE_MS": 1500, "WORKER_RETRY_MAX_MS": 3000}
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(fm, group) as primer:
        wait_idle(fm, primer, group)
        assert primer.stop() == 0
    fm.publish(payment)
    partition, offset = fm.position(payment)

    with worker(fm, group, FAULT_POINT="FAIL_OFFSET_COMMIT", FAULT_PAYMENT_ID=payment, **settings) as w:
        w.wait_for("offset_commit_failed", timeout=60, partition=partition, offset=offset)
        wait(w, lambda: (committed(fm, group, partition) or 0) > offset, 30, "P's offset committed")
        failures = w.events("offset_commit_failed")
        assert w.stop() == 0, w.diagnostics()

    pending_ms = sum(f["delay_ms"] for f in failures)
    print(f"LONG_PENDING_COMMIT pending_ms={pending_ms} max_poll_interval_ms=2000")
    assert [f["delay_ms"] for f in failures] == [1500, 3000]
    assert pending_ms > 2000
    assert len(w.events("partitions_assigned")) == 1, w.diagnostics()
    assert len(w.events("processor_called", payment_id=str(payment))) == 1
    _settled_once(fm, group, payment)
