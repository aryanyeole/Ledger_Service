-- Idempotency key purge (D-020, D-043, D-119): the index and the grant V5 deferred to M4.
-- The ledger-api purge job (T2) deletes rows past expires_at in batches ordered by expires_at,
-- so it needs an index on that column and DELETE for the app. Plain CREATE INDEX, not
-- CONCURRENTLY: Flyway runs each migration in a transaction, and the table is small.
-- Which rows may be deleted is V15's guard, separate so it can be seen failing first (D-038).
-- No other grant changes: no TRUNCATE for any role, no UPDATE(expires_at) for any role (the
-- TTL comes from the database clock, D-043), and nothing for settlement_worker (D-020).

CREATE INDEX idempotency_keys_expires_at ON public.idempotency_keys (expires_at);

GRANT DELETE ON public.idempotency_keys TO ledger_api_app;
