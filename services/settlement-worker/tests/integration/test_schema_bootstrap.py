"""The worker's view of the Flyway-built schema (D-020, D-077, D-078, D-083)."""

import asyncio

import asyncpg
import pytest

TABLES = ["accounts", "transactions", "entries", "payments", "outbox", "processed_messages"]


def test_worker_role_sees_the_schema_it_needs_and_nothing_more(worker_db):
    async def scenario():
        conn = await worker_db.connect()  # asserts current_user and not superuser
        try:
            present = await conn.fetch(
                "SELECT t, to_regclass('public.' || t) IS NOT NULL AS present FROM unnest($1::text[]) AS t", TABLES
            )
            assert {r["t"]: r["present"] for r in present} == {t: True for t in TABLES}

            # D-020: no access to idempotency keys.
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.fetch("SELECT 1 FROM idempotency_keys")
            # D-077: the worker writes the outbox but never reads it.
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.fetch("SELECT event_id FROM outbox")
            # D-078: SELECT on the dedup key columns, which ON CONFLICT (...) needs.
            await conn.fetch("SELECT consumer_group, event_id FROM processed_messages LIMIT 1")
        finally:
            await conn.close()

    asyncio.run(scenario())
