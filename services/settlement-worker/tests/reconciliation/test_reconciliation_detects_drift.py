"""Reconciliation detects injected drift (spec §9, D-114, D-116, D-117), on real worker processes.

One worker reconciles every second (RECONCILE_INTERVAL_MS=1000) on this module's own stack. Drift is
injected only as settlement_worker, through grants it already holds (D-034, D-076): a balance
changed without an entry, and a payment moved AUTHORIZED -> FAILED without a posting. These are the
bug classes the balance trigger can't see; no trigger, schema or role is touched (invariant 1).
Global SUM != 0 can't be injected without bypassing P1, so it's left to the evaluator's unit test.

Each injection reads clock_timestamp() before (T_before) and after its commit (T_after), and every
case waits for the first reconciliation_completed whose as_of is later than T_after: never a fixed
sleep. drift_detected lines carry no run_id, so they belong to the run whose reconciliation_completed
follows them in the log (one reconciler task emits them just before it). Gauges come from a real
/metrics scrape, read as Decimal from the exposition text.

The cases run in order on the one stack:
  Baseline  nothing drifted, no per-account series yet.
  A         +0.01 on never-funded C: a drift with no entries at all (the snapshot's LEFT JOIN).
  B         -0.0001 on M: a negative, sub-cent drift; C's window is unchanged.
  C         both balances restored exactly: series kept at 0, a clean run.
  C2        both drifts again: the windows were reset by the repair, then restored again.
  D         P AUTHORIZED -> FAILED with no posting: only the escrow check sees it (last: P stays FAILED
            with its escrow held).
"""

import asyncio
import re
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from support.failure_kit import fresh_group, worker

RUN_TIMEOUT_S = 30
_SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)$")
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')


def run(coro):
    return asyncio.run(coro)


def _time(text: str) -> datetime:
    """A D-097 time (UTC, six fractional digits, Z)."""
    return datetime.fromisoformat(text)


@dataclass
class Scene:
    worker: object
    customer: int  # C: never funded
    merchant: int  # M: funded by a deposit
    payment: object  # P: AUTHORIZED, never settled (the relay is off)
    balances: dict  # the balances to restore
    saved: dict = field(default_factory=dict)  # values one case hands to the next


async def _clock(rs) -> datetime:
    conn = await rs.app_db.connect()
    try:
        return await conn.fetchval("SELECT clock_timestamp()")
    finally:
        await conn.close()


async def _inject_async(rs, statements) -> tuple[datetime, datetime]:
    """Runs the UPDATEs in one transaction as settlement_worker (D-036), each changing exactly one row.
    Returns (T_before, T_after): clock_timestamp() just before, and after the commit."""
    conn = await rs.worker_db.connect()
    try:
        assert await conn.fetchval("SELECT current_user") == "settlement_worker"
        before = await conn.fetchval("SELECT clock_timestamp()")
        async with conn.transaction():
            for sql, *args in statements:
                assert await conn.execute(sql, *args) == "UPDATE 1", sql
        after = await conn.fetchval("SELECT clock_timestamp()")
        return before, after
    finally:
        await conn.close()


def _inject(rs, *statements) -> tuple[datetime, datetime]:
    return run(_inject_async(rs, statements))


def _set_balance(account_id: int, balance: Decimal):
    return ("UPDATE accounts SET balance = $2 WHERE id = $1", account_id, balance)


def _add_balance(account_id: int, delta: Decimal):
    return ("UPDATE accounts SET balance = balance + $2 WHERE id = $1", account_id, delta)


@dataclass(frozen=True)
class Run:
    completed: dict
    drifts: dict  # account_id -> its drift_detected line in this run

    @property
    def as_of(self) -> datetime:
        return _time(self.completed["as_of"])


def _runs(w) -> list[Run]:
    """Every completed run in log order, each with the drift_detected lines emitted just before it."""
    runs, pending = [], {}
    for line in w.lines():
        if line["event"] == "drift_detected":
            pending[line["account_id"]] = line
        elif line["event"] == "reconciliation_completed":
            runs.append(Run(line, pending))
            pending = {}
        elif line["event"] == "reconciliation_failed":
            pending = {}
    return runs


def _run_after(w, t_after: datetime) -> Run:
    """The first completed run whose as_of is later than t_after."""
    deadline = time.monotonic() + RUN_TIMEOUT_S
    while time.monotonic() < deadline:
        for completed in _runs(w):
            if completed.as_of > t_after:
                return completed
        assert w.process.poll() is None, w.diagnostics()
        time.sleep(0.1)
    raise AssertionError(f"no reconciliation_completed with as_of > {t_after} within {RUN_TIMEOUT_S} s:\n{w.diagnostics()}")


def _scrape(w) -> dict[tuple, Decimal]:
    """/metrics as {(name, frozenset of labels): Decimal(value text)}."""
    with urllib.request.urlopen(f"http://127.0.0.1:{w.metrics_port()}/metrics", timeout=10) as response:
        body = response.read().decode("utf-8")
    samples = {}
    for line in body.splitlines():
        match = _SAMPLE.match(line)
        if match and not line.startswith("#"):
            labels = frozenset(_LABEL.findall(match.group(2) or ""))
            samples[(match.group(1), labels)] = Decimal(match.group(3))
    return samples


def _drift_series(metrics) -> dict[int, Decimal]:
    return {int(dict(labels)["account_id"]): v for (name, labels), v in metrics.items() if name == "ledger_drift_amount"}


def _gauge(metrics, name: str) -> Decimal:
    return metrics[(name, frozenset())]


def _runs_total(metrics, result: str) -> Decimal:
    return metrics.get(("ledger_reconciliation_runs_total", frozenset({("result", result)})), Decimal(0))


def _no_failures(w) -> None:
    assert w.events("reconciliation_failed") == [], w.diagnostics()
    assert w.events("processing_failed") == [], w.diagnostics()


@pytest.fixture(scope="module")
def scene(rs):
    customer = rs.api.create_account("CUSTOMER")  # C: never funded
    merchant = rs.api.create_account("MERCHANT")
    rs.api.deposit(merchant, "100.00")  # D-062: deposits may credit a MERCHANT
    payer = rs.api.create_account("CUSTOMER")
    rs.api.deposit(payer, "100.00")
    payment = rs.api.authorize(payer, merchant, "42.50")  # P: stays AUTHORIZED, the relay is off
    balances = {customer: Decimal("0.0000"), merchant: Decimal("100.0000")}
    group = fresh_group(rs, "reconciliation-drift")
    with worker(rs, group, RECONCILE_INTERVAL_MS=1000) as w:
        yield Scene(w, customer, merchant, payment, balances)
        assert w.stop() == 0, w.diagnostics()
    _no_failures(w)


def test_baseline_nothing_has_drifted(rs, scene):
    w = scene.worker
    started = w.wait_for("worker_started")
    assert started["reconcile_interval_ms"] == 1000
    baseline = _run_after(w, run(_clock(rs)))
    assert (baseline.completed["drifted_accounts"], baseline.drifts) == (0, {})
    assert (baseline.completed["global_sum"], baseline.completed["escrow_drift"]) == ("0.0000", "0.0000")
    metrics = _scrape(w)
    assert _drift_series(metrics) == {}, "no per-account series before a drift (D-116)"
    assert _runs_total(metrics, "ok") >= 1
    _no_failures(w)


def test_a_a_balance_changed_without_an_entry_is_a_drift(rs, scene):
    w, c = scene.worker, scene.customer
    t_before, t_after = _inject(rs, _add_balance(c, Decimal("0.01")))
    seen = _run_after(w, t_after)

    # C has no entries at all: only a LEFT JOIN keeps it in the snapshot.
    assert c in seen.drifts, ("no drift_detected for C", c, seen.completed, seen.drifts)
    drift = seen.drifts[c]
    assert (drift["account_type"], drift["balance"], drift["entry_sum"], drift["drift"]) == (
        "CUSTOMER", "0.0100", "0.0000", "0.0100"
    )
    assert drift["last_entry_id"] is None and drift["level"] == "error"
    assert seen.completed["drifted_accounts"] == 1
    assert (seen.completed["global_sum"], seen.completed["escrow_drift"]) == ("0.0000", "0.0000")
    metrics = _scrape(w)
    assert _drift_series(metrics) == {c: Decimal("0.01")}
    assert _gauge(metrics, "ledger_drift_accounts") == 1

    # The window brackets the injection. The run that first saw it may have started just before
    # T_after (as_of is its transaction's start), but it ended after T_before.
    assert _time(drift["last_clean_as_of"]) < t_after, (drift, t_after)
    [first] = [r for r in _runs(w) if r.completed["as_of"] == drift["first_drift_as_of"]]
    assert first.as_of + timedelta(milliseconds=first.completed["duration_ms"]) > t_before, (first, t_before)
    scene.saved["c_first_drift_as_of"] = drift["first_drift_as_of"]
    _no_failures(w)


def test_b_a_negative_sub_cent_drift_survives(rs, scene):
    w, c, m = scene.worker, scene.customer, scene.merchant
    _, t_after = _inject(rs, _add_balance(m, Decimal("-0.0001")))
    seen = _run_after(w, t_after)

    assert m in seen.drifts, ("no drift_detected for M", m, seen.completed, seen.drifts)
    assert seen.drifts[m]["drift"] == "-0.0001"  # sub-cent survives: scale 4, never the scale-2 formatter
    assert (seen.drifts[m]["balance"], seen.drifts[m]["entry_sum"]) == ("99.9999", "100.0000")
    assert seen.completed["drifted_accounts"] == 2
    assert seen.drifts[c]["first_drift_as_of"] == scene.saved["c_first_drift_as_of"], "C's window is unchanged"
    series = _drift_series(_scrape(w))
    assert series[m] < 0 and series[m] == Decimal("-0.0001"), series
    assert series[c] == Decimal("0.01")
    scene.saved["m_first_drift_as_of"] = seen.drifts[m]["first_drift_as_of"]
    _no_failures(w)


def test_c_a_repair_keeps_the_series_at_zero(rs, scene):
    w, c, m = scene.worker, scene.customer, scene.merchant
    ok_before = _runs_total(_scrape(w), "ok")
    _, t_after = _inject(rs, _set_balance(c, scene.balances[c]), _set_balance(m, scene.balances[m]))
    clean = _run_after(w, t_after)

    assert (clean.completed["drifted_accounts"], clean.drifts) == (0, {})
    assert (clean.completed["global_sum"], clean.completed["escrow_drift"]) == ("0.0000", "0.0000")
    metrics = _scrape(w)
    assert _drift_series(metrics) == {c: 0, m: 0}, "both series stay, at 0 (D-116)"
    assert _gauge(metrics, "ledger_drift_accounts") == 0
    assert _runs_total(metrics, "ok") > ok_before, "a run with result=ok after the repair"
    scene.saved["clean_as_of"] = clean.as_of
    _no_failures(w)


def test_c2_a_new_drift_after_the_repair_starts_a_new_window(rs, scene):
    w, c, m = scene.worker, scene.customer, scene.merchant
    clean_as_of = scene.saved["clean_as_of"]
    _, t_after = _inject(rs, _add_balance(c, Decimal("0.01")), _add_balance(m, Decimal("-0.0001")))
    seen = _run_after(w, t_after)
    assert set(seen.drifts) == {c, m}, seen.drifts

    later = [r for r in _runs(w) if r.as_of > clean_as_of]
    for account, earlier in ((c, scene.saved["c_first_drift_as_of"]), (m, scene.saved["m_first_drift_as_of"])):
        drift = seen.drifts[account]
        # The run that first saw the new drift: the first one after the clean run with a line for it.
        first = next(r for r in later if account in r.drifts)
        assert drift["first_drift_as_of"] == first.completed["as_of"], (account, drift, first.completed)
        assert drift["first_drift_as_of"] != earlier, "the repair cleared the old window"
        assert _time(drift["last_clean_as_of"]) >= clean_as_of, (account, drift, clean_as_of)

    # Restored again, so case D starts with every account clean.
    _, t_after = _inject(rs, _set_balance(c, scene.balances[c]), _set_balance(m, scene.balances[m]))
    clean = _run_after(w, t_after)
    assert (clean.completed["drifted_accounts"], clean.drifts) == (0, {})
    _no_failures(w)


def test_d_a_failed_payment_without_a_posting_is_seen_only_by_the_escrow_check(rs, scene):
    w, c, m = scene.worker, scene.customer, scene.merchant
    drift_runs_before = _runs_total(_scrape(w), "drift")
    _, t_after = _inject(rs, ("UPDATE payments SET status = 'FAILED' WHERE id = $1", scene.payment))
    seen = _run_after(w, t_after)

    # ESCROW still holds P's 42.50, but P is no longer AUTHORIZED.
    assert seen.completed["escrow_drift"] == "42.5000"
    assert (seen.completed["drifted_accounts"], seen.drifts) == (0, {}), "no account drifts"
    assert seen.completed["global_sum"] == "0.0000"
    metrics = _scrape(w)
    assert _gauge(metrics, "ledger_escrow_drift_amount") == Decimal("42.5")
    assert _drift_series(metrics) == {c: 0, m: 0}
    assert _gauge(metrics, "ledger_drift_accounts") == 0
    assert _runs_total(metrics, "drift") > drift_runs_before, "result=drift"
    _no_failures(w)
