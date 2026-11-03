"""Snapshot reconciliation (D-114, D-115, D-116). Read-only: it never writes.

Each run is one READ ONLY transaction: statement_timeout set first, then SNAPSHOT_SQL, one statement
and so one snapshot under READ COMMITTED. It returns one row per account (never-funded ones included)
with its balance, COALESCE(SUM(entries.amount), 0) and last entry id; the global SUM and
max(entries.transaction_id) as window aggregates over those rows (entries.account_id is a NOT NULL
foreign key, so they equal the aggregates over entries, from one scan); the sum of AUTHORIZED
payment amounts; and now() as as_of. ESCROW's entry sum is its own row's.

evaluate() is pure: per-account drift = balance - entry_sum, the global sum, and escrow drift = ESCROW's
entry sum - sum of AUTHORIZED amounts. DriftWindows keeps, per account and in memory only (null after
a restart), when it was last clean and when its current drift was first seen.

The Reconciler is an asyncio task with its own connection (application_name APPLICATION_NAME, role
checked as settlement_worker, never the consumer's). Its first run is at start, then each run starts
RECONCILE_INTERVAL_MS after the last one ended, so runs never overlap. A failed run logs
reconciliation_failed and never affects the consumer; a run that finds its connection closed drops it
and the next run reconnects. A role refusal on its connection is configuration: config_invalid, and the
worker exits 2 (D-109). On stop the task is cancelled and awaited for at most STOP_TIMEOUT_SECONDS;
its connection is terminated, never closed.
"""

import asyncio
import time
from collections.abc import Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg
import structlog

from settlement_worker import db, events, failures, metrics, timestamps
from settlement_worker.money import InvalidAmount

APPLICATION_NAME = "settlement-worker-reconcile"
STOP_TIMEOUT_SECONDS = 5

SNAPSHOT_SQL = """
WITH per_account AS (
  SELECT a.id, a.type, a.balance,
         COALESCE(SUM(e.amount), 0) AS entry_sum,
         MAX(e.id)                  AS last_entry_id,
         MAX(e.transaction_id)      AS last_transaction_id
  FROM accounts a
  LEFT JOIN entries e ON e.account_id = a.id
  GROUP BY a.id
), authorized AS (
  SELECT COALESCE(SUM(amount), 0) AS authorized_sum FROM payments WHERE status = 'AUTHORIZED'
)
SELECT p.id, p.type, p.balance, p.entry_sum, p.last_entry_id,
       SUM(p.entry_sum)           OVER () AS global_sum,
       MAX(p.last_transaction_id) OVER () AS max_transaction_id,
       au.authorized_sum, now() AS as_of
FROM per_account p CROSS JOIN authorized au
ORDER BY p.id
"""

_SCALE4 = Decimal("0.0001")


def money4(value: Decimal) -> str:
    """A Decimal as an exact scale-4 string: Decimal('0.01') -> '0.0100', zero (signed or not) ->
    '0.0000'. Never rounds: NUMERIC(19,4) and its sums have no digits past 4 dp."""
    if not isinstance(value, Decimal):
        raise InvalidAmount(f"not a Decimal: {value!r}")
    quantized = value.quantize(_SCALE4)
    if quantized != value:
        raise InvalidAmount(f"non-zero digits past 4 dp: {value}")
    if quantized == 0:
        quantized = abs(quantized)
    return format(quantized, "f")


class ReconcileError(Exception):
    """A snapshot that can't be evaluated: no rows, or not exactly one ESCROW account."""


@dataclass(frozen=True)
class Drift:
    account_id: int
    account_type: str
    balance: Decimal
    entry_sum: Decimal
    drift: Decimal
    last_entry_id: int | None


@dataclass(frozen=True)
class Evaluation:
    as_of: datetime
    account_ids: tuple[int, ...]
    drifted: tuple[Drift, ...]
    global_sum: Decimal
    escrow_drift: Decimal
    max_transaction_id: int | None

    @property
    def result(self) -> str:
        """ledger_reconciliation_runs_total's result: drift if anything is off, else ok."""
        return "drift" if self.drifted or self.escrow_drift != 0 or self.global_sum != 0 else "ok"


def evaluate(rows: Sequence[Mapping]) -> Evaluation:
    """SNAPSHOT_SQL's rows -> the run's findings. The window aggregates are the same on every row."""
    if not rows:
        raise ReconcileError("the snapshot returned no rows")
    escrow = [row for row in rows if row["type"] == "ESCROW"]
    if len(escrow) != 1:
        raise ReconcileError(f"expected exactly one ESCROW account, found {len(escrow)}")
    first = rows[0]
    drifted = tuple(
        Drift(row["id"], row["type"], row["balance"], row["entry_sum"], row["balance"] - row["entry_sum"],
              row["last_entry_id"])
        for row in rows
        if row["balance"] != row["entry_sum"]
    )
    return Evaluation(
        as_of=first["as_of"],
        account_ids=tuple(row["id"] for row in rows),
        drifted=drifted,
        global_sum=first["global_sum"],
        escrow_drift=escrow[0]["entry_sum"] - first["authorized_sum"],
        max_transaction_id=first["max_transaction_id"],
    )


@dataclass(frozen=True)
class Window:
    last_clean_as_of: datetime | None
    first_drift_as_of: datetime | None


class DriftWindows:
    """Per account, in memory: the as_of of its last clean run, and of the first run of its current
    drift (cleared when it reconciles)."""

    def __init__(self) -> None:
        self._last_clean: dict[int, datetime] = {}
        self._first_drift: dict[int, datetime] = {}

    def update(self, as_of: datetime, account_ids: Iterable[int], drifted: Iterable[int]) -> dict[int, Window]:
        """Records one run; returns the window of each drifted account."""
        drifted = set(drifted)
        for account_id in account_ids:
            if account_id in drifted:
                self._first_drift.setdefault(account_id, as_of)
            else:
                self._last_clean[account_id] = as_of
                self._first_drift.pop(account_id, None)
        return {account_id: self.window(account_id) for account_id in drifted}

    def window(self, account_id: int) -> Window:
        return Window(self._last_clean.get(account_id), self._first_drift.get(account_id))


@asynccontextmanager
async def snapshot(conn: asyncpg.Connection, statement_timeout_ms: int):
    """One READ ONLY transaction with statement_timeout set first (SET LOCAL takes no parameters;
    set_config(..., true) is SET LOCAL)."""
    async with conn.transaction(readonly=True):
        await conn.execute("SELECT set_config('statement_timeout', $1, true)", f"{statement_timeout_ms}ms")
        yield conn


async def run_once(conn: asyncpg.Connection, statement_timeout_ms: int) -> list:
    async with snapshot(conn, statement_timeout_ms):
        return await conn.fetch(SNAPSHOT_SQL)


def _time(moment: datetime | None) -> str | None:
    return timestamps.utc_text(moment) if moment is not None else None


class Reconciler:
    """The reconciliation task (D-114). refusal is set, with the error, if its connection is refused
    the role; the consume loop then exits 2."""

    def __init__(self, config, reconcile_metrics: metrics.ReconcileMetrics | None = None) -> None:
        self._config = config
        self._metrics = reconcile_metrics or metrics.RECONCILE
        self._conn: asyncpg.Connection | None = None
        self._windows = DriftWindows()
        self._run_id = 0
        self.refusal: str | None = None

    async def run_forever(self) -> None:
        try:
            while True:
                await self.run_one()
                if self.refusal is not None:
                    return
                await asyncio.sleep(self._config.reconcile_interval_ms / 1000)
        finally:
            self.close()

    def close(self) -> None:
        """Terminates the connection, never waiting on the server."""
        if self._conn is not None:
            self._conn.terminate()
            self._conn = None

    async def connect(self) -> asyncpg.Connection:
        """A new connection as the reconciler opens it: role checked, application_name set, connect
        bounded by WORKER_DB_COMMAND_TIMEOUT_MS and commands by the statement timeout plus that."""
        return await db.connect(
            timeout_ms=self._config.db_command_timeout_ms,
            command_timeout_ms=self._config.reconcile_statement_timeout_ms + self._config.db_command_timeout_ms,
            server_settings={"application_name": APPLICATION_NAME},
            **self._config.db_params(),
        )

    async def run_one(self) -> None:
        self._run_id += 1
        run_id = self._run_id
        log = structlog.get_logger()
        started = time.monotonic_ns()
        try:
            if self._conn is None:
                self._conn = await self.connect()
            evaluation = evaluate(await run_once(self._conn, self._config.reconcile_statement_timeout_ms))
        except db.WorkerRoleError as ex:
            self.refusal = str(ex)
            events.emit(log, events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
            return
        except Exception as ex:  # noqa: BLE001 - a failed run never affects the consumer (D-114)
            closed = self._conn is None or self._conn.is_closed()
            failure = failures.classify(db.as_db_failure(ex), connection_closed=closed)
            if self._conn is not None and (closed or failure.connection):
                self._conn.terminate()
                self._conn = None
            events.emit(
                log,
                events.RECONCILIATION_FAILED,
                level="warning",
                run_id=run_id,
                error_class=failure.error_class.value,
                error=f"{type(ex).__name__}: {ex}",
                correlation_id=None,
            )
            self._metrics.failed()
            return
        self._report(log, run_id, evaluation, (time.monotonic_ns() - started) // 1_000_000)

    def _report(self, log, run_id: int, evaluation: Evaluation, duration_ms: int) -> None:
        windows = self._windows.update(
            evaluation.as_of, evaluation.account_ids, (d.account_id for d in evaluation.drifted)
        )
        for drift in evaluation.drifted:
            window = windows[drift.account_id]
            events.emit(
                log,
                events.DRIFT_DETECTED,
                level="error",
                account_id=drift.account_id,
                account_type=drift.account_type,
                balance=money4(drift.balance),
                entry_sum=money4(drift.entry_sum),
                drift=money4(drift.drift),
                last_entry_id=drift.last_entry_id,
                last_clean_as_of=_time(window.last_clean_as_of),
                first_drift_as_of=_time(window.first_drift_as_of),
                correlation_id=None,
            )
        events.emit(
            log,
            events.RECONCILIATION_COMPLETED,
            run_id=run_id,
            as_of=timestamps.utc_text(evaluation.as_of),
            accounts=len(evaluation.account_ids),
            drifted_accounts=len(evaluation.drifted),
            global_sum=money4(evaluation.global_sum),
            escrow_drift=money4(evaluation.escrow_drift),
            max_transaction_id=evaluation.max_transaction_id,
            duration_ms=duration_ms,
            correlation_id=None,
        )
        self._metrics.completed(
            {d.account_id: d.drift for d in evaluation.drifted},
            evaluation.global_sum,
            evaluation.escrow_drift,
            evaluation.result,
        )


async def stop(task: asyncio.Task) -> None:
    """Cancels the task and waits at most STOP_TIMEOUT_SECONDS; asyncio.wait never raises the task's
    CancelledError, so nothing here catches it."""
    task.cancel()
    await asyncio.wait({task}, timeout=STOP_TIMEOUT_SECONDS)
