"""The reconciler's own connection (D-114) on real worker processes, on the failure_modes stack.

(a) Lost between runs: its backend is terminated (pg_terminate_backend through docker exec, as T5b),
    found by application_name. The next run discovers the loss on use: exactly one
    reconciliation_failed, then a completed run on a new backend. The consumer is untouched: no
    retry_scheduled, and a payment published afterwards settles.
(b) Refused its role: support.role_swap_worker connects as ledger_api_app from its second connect on,
    and the reconciler's first connect is the worker's second. The worker exits 2 with config_invalid
    naming the role, as a refusal on the consumer's own connection does (D-109); no run completes.
"""

import sys
from pathlib import Path

from support import stack
from support.failure_kit import (
    fresh_group,
    funded_pair,
    terminate_worker_backends,
    wait,
    worker,
    worker_backends,
)

from settlement_worker.reconcile import APPLICATION_NAME

TESTS = Path(__file__).resolve().parents[1]
RECONCILER = f"application_name = '{APPLICATION_NAME}'"


# (a)
def test_a_lost_reconciler_connection_fails_one_run_and_the_next_reconnects(fm):
    group = fresh_group(fm, "reconcile-conn-loss")
    customer, merchant = funded_pair(fm)
    payment = fm.api.authorize(customer, merchant, "10.00")

    with worker(fm, group, RECONCILE_INTERVAL_MS=1000) as w:
        w.wait_for("reconciliation_completed", timeout=60)
        [before] = wait(w, lambda: worker_backends(fm, RECONCILER), 30, "the reconciler's backend")
        assert terminate_worker_backends(fm, RECONCILER) == 1
        failed = w.wait_for("reconciliation_failed", timeout=30)
        later = wait(
            w,
            lambda: [e for e in w.events("reconciliation_completed") if e["run_id"] > failed["run_id"]],
            30,
            "a completed run after the failure",
        )
        [after] = wait(w, lambda: worker_backends(fm, RECONCILER), 30, "the reconciler's new backend")
        fm.publish(payment)  # the consumer still works
        w.wait_for("payment_settled", payment_id=str(payment), timeout=60)
        assert w.stop() == 0, w.diagnostics()

    assert w.wait_for("worker_started")["reconcile_interval_ms"] == 1000
    failures = w.events("reconciliation_failed")
    assert len(failures) == 1, w.diagnostics()
    assert failures[0]["level"] == "warning" and failures[0]["error_class"] == "Transient", failures
    assert later[0]["run_id"] == failed["run_id"] + 1, (failed, later)
    assert (later[0]["drifted_accounts"], later[0]["escrow_drift"]) == (0, "0.0000"), later
    assert after != before, (before, after)
    assert w.events("retry_scheduled") == [], w.diagnostics()
    assert w.events("processing_failed") == [], w.diagnostics()
    assert w.events("db_reconnected") == [], "the consumer's own connection was never lost"


# (b)
def test_a_reconciler_refused_its_role_exits_2(fm):
    group = fresh_group(fm, "reconcile-role-swap")

    with worker(
        fm,
        group,
        command=[sys.executable, "-m", "support.role_swap_worker"],
        PYTHONPATH=TESTS,
        ROLE_SWAP_PASSWORD=stack.APP_PASSWORD,
        RECONCILE_INTERVAL_MS=1000,
    ) as w:
        assert w.wait_exit(30) == 2, w.diagnostics()

    refusals = w.events("config_invalid")
    assert len(refusals) == 1, w.diagnostics()
    assert "connected as ledger_api_app, expected settlement_worker" in refusals[0]["error"], refusals
    assert [e["reason"] for e in w.events("worker_stopped")] == ["config_invalid"], w.diagnostics()
    assert w.events("reconciliation_completed") == [], w.diagnostics()
    assert w.events("reconciliation_failed") == [], "a refusal is configuration, not a failed run"
