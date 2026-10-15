-- Keyset pagination for GET /v1/transactions (D-053): one index serves both the cursor filter
-- (created_at, id) < (?, ?) and the ORDER BY created_at DESC, id DESC, so a page is an index
-- range scan with no sort, however deep the cursor. Runs as ledger_api_owner; no grant
-- changes (an index needs none).

CREATE INDEX transactions_created_at_id_desc ON public.transactions (created_at DESC, id DESC);
