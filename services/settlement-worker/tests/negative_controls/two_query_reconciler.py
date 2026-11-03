"""D-017 / D-118 negative control: the two-query reconciler, the D-019 bug.

TEST SOURCE ONLY. It lives only in tests/negative_controls/; src/settlement_worker never imports it
(tests/unit/test_negative_control_isolation.py). It violates no CLAUDE.md invariant (D-019 is a log
rule): it only reads, as settlement_worker, in one READ ONLY transaction.

It differs from main's reconciler (settlement_worker.reconcile) only in query shape. Main reads
everything in one statement, so one snapshot. This reads balances in statement 1 and everything else
in statement 2: per-account entry sums and last entry id, the global sum and max transaction id, the
AUTHORIZED sum and now(). The rows are merged by id into main's row shape and judged by main's own
evaluate(). Under READ COMMITTED each statement takes its own snapshot, so a transfer committed
between them shows as drift on both of its accounts; under REPEATABLE READ both statements share
the transaction's snapshot and it can't.
"""

from collections.abc import Awaitable, Callable

import asyncpg

from settlement_worker.reconcile import Evaluation, evaluate

BALANCES_SQL = "SELECT id, type, balance FROM accounts ORDER BY id"

REST_SQL = """
WITH per_account AS (
  SELECT a.id,
         COALESCE(SUM(e.amount), 0) AS entry_sum,
         MAX(e.id)                  AS last_entry_id,
         MAX(e.transaction_id)      AS last_transaction_id
  FROM accounts a
  LEFT JOIN entries e ON e.account_id = a.id
  GROUP BY a.id
), authorized AS (
  SELECT COALESCE(SUM(amount), 0) AS authorized_sum FROM payments WHERE status = 'AUTHORIZED'
)
SELECT p.id, p.entry_sum, p.last_entry_id,
       SUM(p.entry_sum)           OVER () AS global_sum,
       MAX(p.last_transaction_id) OVER () AS max_transaction_id,
       au.authorized_sum, now() AS as_of
FROM per_account p CROSS JOIN authorized au
ORDER BY p.id
"""


async def connect(worker_db) -> asyncpg.Connection:
    """A connection as settlement_worker, the role main's reconciler runs as (D-036)."""
    conn = await worker_db.connect()
    assert await conn.fetchval("SELECT current_user") == "settlement_worker"
    return conn


async def reconcile(
    conn: asyncpg.Connection, *, isolation: str, barrier: Callable[[], Awaitable[None]] | None = None
) -> tuple[Evaluation, dict[int, dict]]:
    """One run: statement 1, the barrier (if any), statement 2, in one READ ONLY transaction at
    isolation ('read_committed' or 'repeatable_read'). Returns main's evaluation and the merged rows
    by account id."""
    async with conn.transaction(isolation=isolation, readonly=True):
        # Statement 1 is the transaction's first statement: unlike main, nothing (no set_config) runs
        # before it, and BEGIN takes no snapshot. Under REPEATABLE READ it fixes the transaction's
        # snapshot, so the barrier below sits after the snapshot is fixed.
        balances = await conn.fetch(BALANCES_SQL)
        if barrier is not None:
            await barrier()
        rest = await conn.fetch(REST_SQL)
    by_id = {row["id"]: dict(row) for row in rest}
    if set(by_id) != {row["id"] for row in balances}:
        raise AssertionError(f"the two statements saw different accounts: {sorted(set(by_id) ^ {r['id'] for r in balances})}")
    rows = {row["id"]: {**by_id[row["id"]], "type": row["type"], "balance": row["balance"]} for row in balances}
    return evaluate(list(rows.values())), rows
