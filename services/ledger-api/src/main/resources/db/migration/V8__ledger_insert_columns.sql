-- Ledger timestamps come only from the database (D-071). V1 gave both runtime roles
-- table-level INSERT on transactions and entries, so either could set created_at and
-- backdate a posting. The listing order (D-053) and any audit timeline trust created_at.
-- Runtime roles now insert through column-level grants that exclude it: created_at comes
-- only from DEFAULT now(), and id is GENERATED ALWAYS (no sequence privilege needed).
-- Same pattern as accounts (D-034) and idempotency_keys (D-043). SELECT is unchanged, so
-- RETURNING id, created_at still works. Runs as ledger_api_owner; no other privilege changes.

REVOKE INSERT ON public.transactions FROM ledger_api_app, settlement_worker;
REVOKE INSERT ON public.entries      FROM ledger_api_app, settlement_worker;

GRANT INSERT (kind, reference, correlation_id)    ON public.transactions TO ledger_api_app, settlement_worker;
GRANT INSERT (transaction_id, account_id, amount) ON public.entries      TO ledger_api_app, settlement_worker;
