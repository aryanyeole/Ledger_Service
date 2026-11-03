"""Two-query reconciliation reports false drift: the D-019 negative control (D-017, D-118).

The naive reconciler (two_query_reconciler.py) reads balances in statement 1 and entry sums and the
rest in statement 2, as settlement_worker, and is judged by main's own evaluate(). On this module's
own stack (relay off, no Kafka), with fixtures through the API (D-103):
  A  CUSTOMER, deposit 1,000.00: sends the transfers
  B  MERCHANT, deposit 100.00: receives them (D-066)
  C  CUSTOMER, never funded (the LEFT JOIN)
  K  CUSTOMER, deposit 100.00, with P = authorize(K, B, "42.50") left AUTHORIZED, so the escrow check
     compares a held escrow with the AUTHORIZED sum.

Case 1 (asserted red, every run): READ COMMITTED with a barrier between the statements, where the
test commits a transfer A -> B of 7.25 through the API: drift is exactly A +7.25, B -7.25.
Case 2 (the control): the same two statements and barrier under REPEATABLE READ: no drift; a fresh
run then shows the transfer, so it committed and the control didn't just miss it.
Case 3 (report-only): the naive READ COMMITTED reconciler under concurrent transfers, no barrier.
"""

import asyncio
import os
import random
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal

import pytest
import two_query_reconciler as naive

from support import hardware
from settlement_worker.reconcile import money4

X = "7.25"
LOAD_THREADS = 4
LOAD_CUSTOMERS = 10
LOAD_MERCHANTS = 10
LOAD_FUND = Decimal("100000.00")


def _positive_int(name: str, default: str) -> int:
    raw = os.environ.get(name, default)
    if not raw.isdigit() or int(raw) < 1:
        raise pytest.UsageError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


NAIVE_RECON_RUNS = _positive_int("NAIVE_RECON_RUNS", "50")


def run(coro):
    return asyncio.run(coro)


@dataclass(frozen=True)
class Scene:
    a: int
    b: int
    c: int
    k: int


@dataclass
class Report:
    barrier_drift: str = "na"
    control_clean: object = "na"
    drift_runs: object = "na"
    requests: object = "na"


@pytest.fixture(scope="module")
def report(request):
    found = Report()
    yield found
    line = (
        f"TWO_QUERY_CONTROL barrier_drift={found.barrier_drift} control_clean={found.control_clean} "
        f"under_load_drift_runs={found.drift_runs}/{NAIVE_RECON_RUNS} requests={found.requests} "
        f"{hardware.summary_fields()}"
    )
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(line)
    else:
        print(line)


@pytest.fixture(scope="module")
def scene(two_query_stack) -> Scene:
    api = two_query_stack.api
    a = api.create_account("CUSTOMER")
    b = api.create_account("MERCHANT")
    c = api.create_account("CUSTOMER")  # never funded
    k = api.create_account("CUSTOMER")
    api.deposit(a, "1000.00")
    api.deposit(b, "100.00")
    api.deposit(k, "100.00")
    api.authorize(k, b, "42.50")  # P: stays AUTHORIZED, the relay is off
    return Scene(a, b, c, k)


async def _reconcile(ts, isolation: str, barrier=None):
    conn = await naive.connect(ts.worker_db)
    try:
        return await naive.reconcile(conn, isolation=isolation, barrier=barrier)
    finally:
        await conn.close()


def _transfer(ts, scene: Scene):
    """The barrier: commits a transfer A -> B of X through the API (201), on a worker thread so the
    reconciler's event loop isn't blocked while the API locks and commits."""

    async def commit() -> None:
        body = {"from_account_id": scene.a, "to_account_id": scene.b, "amount": X}
        status, response = await asyncio.to_thread(ts.api.try_post, "/v1/transfers", body)
        assert status == 201, (status, response)

    return commit


def _drift(evaluation) -> dict[int, str]:
    return {d.account_id: money4(d.drift) for d in evaluation.drifted}


def _signed(text: str) -> str:
    return text if text.startswith("-") else f"+{text}"


def test_two_queries_under_read_committed_report_false_drift(two_query_stack, scene, report):
    ts = two_query_stack
    transfer = _transfer(ts, scene)
    evaluation, _ = run(_reconcile(ts, "read_committed", barrier=transfer))
    drift = _drift(evaluation)
    report.barrier_drift = ",".join(
        f"{name}:{_signed(drift[account])}" for name, account in (("A", scene.a), ("B", scene.b)) if account in drift
    ) or "none"
    assert drift == {scene.a: "7.2500", scene.b: "-7.2500"}, drift
    # The global sum and the escrow check come from statement 2 alone, so they stay consistent.
    assert money4(evaluation.global_sum) == "0.0000", evaluation
    assert money4(evaluation.escrow_drift) == "0.0000", evaluation

    # The drift was false: a fresh run sees the ledger consistent.
    after, _ = run(_reconcile(ts, "read_committed"))
    assert (_drift(after), money4(after.global_sum), money4(after.escrow_drift)) == ({}, "0.0000", "0.0000"), after


def test_the_same_two_queries_under_repeatable_read_report_none(two_query_stack, scene, report):
    ts = two_query_stack
    transfer = _transfer(ts, scene)
    control, before = run(_reconcile(ts, "repeatable_read", barrier=transfer))
    assert (_drift(control), money4(control.global_sum), money4(control.escrow_drift)) == ({}, "0.0000", "0.0000"), control

    # The transfer committed: a fresh run sees it, and the control's snapshot didn't.
    after, rows = run(_reconcile(ts, "repeatable_read"))
    assert after.max_transaction_id > control.max_transaction_id, (after, control)
    assert rows[scene.a]["entry_sum"] == before[scene.a]["entry_sum"] - Decimal(X), (rows[scene.a], before[scene.a])
    assert rows[scene.b]["entry_sum"] == before[scene.b]["entry_sum"] + Decimal(X), (rows[scene.b], before[scene.b])
    assert (_drift(after), money4(after.global_sum), money4(after.escrow_drift)) == ({}, "0.0000", "0.0000"), after
    report.control_clean = True


class TransferLoad:
    """LOAD_THREADS threads of API transfers between random distinct accounts until stopped. Before
    each request its amount is added to the sender's outflow (inflows never counted); one that would
    take it past LOAD_FUND fails the test instead of being sent, so none can 422 (D-087)."""

    def __init__(self, api, accounts: list[int]) -> None:
        self._api = api
        self._accounts = accounts
        self._lock = threading.Lock()
        self._outflow = {account: Decimal(0) for account in accounts}
        self.stop = threading.Event()
        self.statuses: dict[int, int] = {}
        self.failures: list = []
        self.exhausted: str | None = None
        self._threads = [threading.Thread(target=self._run, args=(i,), daemon=True) for i in range(LOAD_THREADS)]

    @property
    def requests(self) -> int:
        return sum(self.statuses.values())

    def start(self) -> None:
        for t in self._threads:
            t.start()

    def join(self, timeout: float = 120) -> None:
        self.stop.set()
        for t in self._threads:
            t.join(timeout)
        assert not any(t.is_alive() for t in self._threads), f"load threads still running after {timeout} s"

    def _run(self, index: int) -> None:
        rng = random.Random()
        while not self.stop.is_set():
            source, target = rng.sample(self._accounts, 2)
            amount = (Decimal(rng.randrange(1, 1000)) / 100).quantize(Decimal("0.01"))
            with self._lock:
                if self._outflow[source] + amount > LOAD_FUND:
                    self.exhausted = f"funding exhausted: account {source} at {self._outflow[source]} + {amount}"
                    self.stop.set()
                    return
                self._outflow[source] += amount
            body = {"from_account_id": source, "to_account_id": target, "amount": str(amount)}
            try:
                status, response = self._api.try_post("/v1/transfers", body)
            except Exception as ex:  # noqa: BLE001 - recorded and asserted on
                status, response = -1, repr(ex)
            with self._lock:
                self.statuses[status] = self.statuses.get(status, 0) + 1
                if status != 201:
                    self.failures.append((status, str(response)[:300]))


def test_two_queries_under_load_report_only(two_query_stack, scene, report):
    """No barrier: the naive READ COMMITTED reconciler runs NAIVE_RECON_RUNS times back to back while
    API transfers commit concurrently. The load is transfers only, so only per-account drift can
    appear. How many runs drift is reported, never asserted (D-118)."""
    ts = two_query_stack
    api = ts.api
    accounts = [api.create_account("CUSTOMER") for _ in range(LOAD_CUSTOMERS)]
    accounts += [api.create_account("MERCHANT") for _ in range(LOAD_MERCHANTS)]
    for account in accounts:
        api.deposit(account, str(LOAD_FUND))

    load = TransferLoad(api, accounts)
    load.start()
    try:
        deadline = time.monotonic() + 30
        while load.requests < LOAD_THREADS:
            assert time.monotonic() < deadline and not load.stop.is_set(), (load.statuses, load.exhausted)
            time.sleep(0.01)

        async def runs() -> int:
            conn = await naive.connect(ts.worker_db)
            try:
                drifted = 0
                for _ in range(NAIVE_RECON_RUNS):
                    evaluation, _ = await naive.reconcile(conn, isolation="read_committed")
                    drifted += evaluation.result != "ok"
                return drifted
            finally:
                await conn.close()

        report.drift_runs = run(runs())
    finally:
        load.join()
    report.requests = load.requests
    assert load.exhausted is None, load.exhausted
    assert load.failures == [] and set(load.statuses) == {201}, (load.statuses, load.failures[:5])
