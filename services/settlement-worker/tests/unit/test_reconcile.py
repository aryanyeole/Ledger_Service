"""Reconciliation's pure parts (D-114, D-116): the evaluator, the drift windows, the scale-4 money
formatter and the gauges' lifecycle on a fresh registry. No DB: rows are what SNAPSHOT_SQL returns."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry

from settlement_worker.metrics import ReconcileMetrics
from settlement_worker.money import InvalidAmount
from settlement_worker.reconcile import DriftWindows, ReconcileError, Window, evaluate, money4

T0 = datetime(2026, 11, 2, 12, 0, 0, 123456, tzinfo=timezone.utc)
D = Decimal


def _at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _rows(accounts, *, authorized_sum=D("40.0000"), global_sum=D("0.0000"), max_transaction_id=5, as_of=T0):
    """accounts: (id, type, balance, entry_sum, last_entry_id); the window columns repeat on each row."""
    return [
        {
            "id": account_id,
            "type": account_type,
            "balance": balance,
            "entry_sum": entry_sum,
            "last_entry_id": last_entry_id,
            "global_sum": global_sum,
            "max_transaction_id": max_transaction_id,
            "authorized_sum": authorized_sum,
            "as_of": as_of,
        }
        for account_id, account_type, balance, entry_sum, last_entry_id in accounts
    ]


CLEAN = [
    (1, "EXTERNAL", D("-1000.0000"), D("-1000.0000"), 1),
    (2, "ESCROW", D("40.0000"), D("40.0000"), 9),
    (3, "PLATFORM_FEES", D("0.2900"), D("0.2900"), 10),
    (4, "CUSTOMER", D("925.0000"), D("925.0000"), 8),
    (5, "MERCHANT", D("9.7100"), D("9.7100"), 11),
    (6, "CUSTOMER", D("0.0000"), D("0"), None),  # never funded: SUM over no entries is COALESCE's 0
]


def _replace(accounts, account_id, **changes):
    fields = ("id", "type", "balance", "entry_sum", "last_entry_id")
    out = []
    for account in accounts:
        if account[0] == account_id:
            values = dict(zip(fields, account)) | changes
            account = tuple(values[f] for f in fields)
        out.append(account)
    return out


# The evaluator


def test_a_clean_snapshot():
    evaluation = evaluate(_rows(CLEAN))
    assert evaluation.drifted == ()
    assert (evaluation.global_sum, evaluation.escrow_drift, evaluation.max_transaction_id) == (D("0"), D("0"), 5)
    assert evaluation.account_ids == (1, 2, 3, 4, 5, 6)
    assert (evaluation.as_of, evaluation.result) == (T0, "ok")


def test_one_drifted_account_drifts_by_balance_minus_entry_sum():
    evaluation = evaluate(_rows(_replace(CLEAN, 4, balance=D("935.0000"))))
    [drift] = evaluation.drifted
    assert (drift.account_id, drift.account_type, drift.balance, drift.entry_sum, drift.last_entry_id) == (
        4, "CUSTOMER", D("935.0000"), D("925.0000"), 8
    )
    assert drift.drift == D("10.0000")  # balance above its entries: positive
    assert money4(drift.drift) == "10.0000"
    assert evaluation.result == "drift"


def test_a_never_funded_account_drifting():
    evaluation = evaluate(_rows(_replace(CLEAN, 6, balance=D("0.0100"))))
    [drift] = evaluation.drifted
    assert (drift.account_id, drift.entry_sum, drift.last_entry_id) == (6, D("0"), None)
    assert money4(drift.drift) == "0.0100"
    assert money4(drift.entry_sum) == "0.0000"


def test_escrow_only_drift():
    evaluation = evaluate(_rows(CLEAN, authorized_sum=D("30.0000")))
    assert evaluation.drifted == ()
    assert evaluation.escrow_drift == D("10.0000")  # ESCROW's entries above what AUTHORIZED payments hold
    assert evaluation.result == "drift"
    assert money4(evaluate(_rows(CLEAN, authorized_sum=D("40.0100"))).escrow_drift) == "-0.0100"


def test_a_global_sum_other_than_zero_is_reported_as_drift():
    evaluation = evaluate(_rows(CLEAN, global_sum=D("-0.0001")))
    assert evaluation.drifted == () and evaluation.escrow_drift == 0
    assert money4(evaluation.global_sum) == "-0.0001"
    assert evaluation.result == "drift"


@pytest.mark.parametrize(
    "rows, message",
    [
        ([], "no rows"),
        (_rows([a for a in CLEAN if a[1] != "ESCROW"]), "exactly one ESCROW account, found 0"),
        (_rows(CLEAN + [(7, "ESCROW", D("0.0000"), D("0"), None)]), "exactly one ESCROW account, found 2"),
    ],
    ids=["no rows", "no ESCROW", "two ESCROW"],
)
def test_an_unusable_snapshot_raises(rows, message):
    with pytest.raises(ReconcileError, match=message):
        evaluate(rows)


# The drift windows


def test_windows_across_clean_drift_drift_repair_and_drift_again():
    windows = DriftWindows()
    ids = (2, 4)
    assert windows.update(_at(0), ids, []) == {}
    assert windows.window(4) == Window(last_clean_as_of=_at(0), first_drift_as_of=None)

    assert windows.update(_at(60), ids, [4]) == {4: Window(_at(0), _at(60))}
    assert windows.update(_at(120), ids, [4]) == {4: Window(_at(0), _at(60))}  # both kept while it drifts

    assert windows.update(_at(180), ids, []) == {}  # repaired
    assert windows.window(4) == Window(last_clean_as_of=_at(180), first_drift_as_of=None)

    assert windows.update(_at(240), ids, [4]) == {4: Window(_at(180), _at(240))}  # a new drift, a new window
    assert windows.window(2) == Window(_at(240), None)


def test_a_drift_on_the_first_run_has_no_clean_time():
    assert DriftWindows().update(_at(0), (4,), [4]) == {4: Window(last_clean_as_of=None, first_drift_as_of=_at(0))}


# The scale-4 formatter


@pytest.mark.parametrize(
    "value, text",
    [
        (D("0.01"), "0.0100"),
        (D("-0.0001"), "-0.0001"),
        (D("0"), "0.0000"),
        (D("0.0000"), "0.0000"),
        (D("-0.0000"), "0.0000"),
        (D("-1000.0000"), "-1000.0000"),
        (D("123456789012345.6789"), "123456789012345.6789"),
        (D("-999999999999999.9999"), "-999999999999999.9999"),
    ],
)
def test_money4_is_exact(value, text):
    assert money4(value) == text


@pytest.mark.parametrize("value", [D("0.00001"), D("1.23456"), "0.01", 1])
def test_money4_refuses_more_than_4_dp_and_non_decimals(value):
    with pytest.raises(InvalidAmount):
        money4(value)


# The gauges (D-116)


def _sample(registry, name, labels=None):
    return registry.get_sample_value(name, labels or {})


def _drift_series(registry) -> dict[str, object]:
    [family] = [f for f in registry.collect() if f.name == "ledger_drift_amount"]
    return {sample.labels["account_id"]: sample.value for sample in family.samples}


def test_a_drift_series_exists_only_after_a_drift_and_goes_to_zero_on_repair():
    registry = CollectorRegistry()
    reconcile_metrics = ReconcileMetrics(registry)

    reconcile_metrics.completed({}, D("0.0000"), D("0.0000"), "ok")
    assert _drift_series(registry) == {}  # no per-account series before a drift
    assert _sample(registry, "ledger_drift_accounts") == 0
    assert _sample(registry, "ledger_reconciliation_runs_total", {"result": "ok"}) == 1
    assert _sample(registry, "ledger_reconciliation_last_completed_timestamp_seconds") > 0

    # Samples are stored as binary floats by the library; -0.5 is exact in binary, so == stays exact.
    reconcile_metrics.completed({4: D("10.0000"), 6: D("-0.5000")}, D("0.0000"), D("10.0000"), "drift")
    assert _drift_series(registry) == {"4": 10, "6": D("-0.5")}
    assert _sample(registry, "ledger_drift_accounts") == 2
    assert _sample(registry, "ledger_escrow_drift_amount") == 10
    assert _sample(registry, "ledger_reconciliation_runs_total", {"result": "drift"}) == 1

    reconcile_metrics.completed({6: D("-0.5000")}, D("0.0000"), D("0.0000"), "drift")  # 4 repaired
    assert _drift_series(registry) == {"4": 0, "6": D("-0.5")}

    reconcile_metrics.completed({}, D("0.0000"), D("0.0000"), "ok")  # both repaired: still there, at 0
    assert _drift_series(registry) == {"4": 0, "6": 0}
    assert _sample(registry, "ledger_drift_accounts") == 0
    assert _sample(registry, "ledger_escrow_drift_amount") == 0
    assert _sample(registry, "ledger_entries_global_sum") == 0

    reconcile_metrics.failed()
    assert _sample(registry, "ledger_reconciliation_runs_total", {"result": "error"}) == 1
    assert _sample(registry, "ledger_reconciliation_runs_total", {"result": "ok"}) == 2
