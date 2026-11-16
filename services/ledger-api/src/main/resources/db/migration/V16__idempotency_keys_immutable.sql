-- A completed idempotency key row never changes what a replay returns (D-140). Replays, and from
-- M5 the Redis fast path, serve the stored status and response_body::text verbatim (D-044,
-- D-139); the cache is only valid while those bytes equal the DB's, so "cached bytes = DB bytes"
-- becomes a database guarantee, like the completeness trigger (D-059) and the purge guard (D-119),
-- for every role including the owner.
--
-- idempotency_keys_immutable: row-level BEFORE UPDATE. Once status_code is set (the row is
-- complete), changing client_id, key, request_hash, status_code or the text of response_body
-- raises check_violation (23514) naming idempotency_keys_immutable, distinct from 42501 (grants),
-- 23001 (append-only), 23502 (completeness) and idempotency_keys_purge's 23514.
--  - response_body is compared as ::text, not as jsonb: jsonb equality is by value, so
--    '{"a": 1.0}' = '{"a": 1.00}', but replays and the cache return the text, and an update that
--    keeps the value and changes the text would change the bytes a client receives.
--  - expires_at is not checked: the D-120 fixture backdates completed rows as ledger_api_owner,
--    and no runtime role has UPDATE(expires_at) (D-043), so only the owner can move it at all.
--  - With V6's deferred completeness trigger: the owner's first store sees OLD.status_code NULL
--    (the row was inserted in the same transaction without a response), so it passes here and V6
--    checks completeness at COMMIT. Clearing a completed response now fails here, at the UPDATE,
--    before V6 could see it at COMMIT.
-- The message carries no client id, key, request hash or body. The function reads no tables;
-- search_path is pinned per D-040.

CREATE FUNCTION public.idempotency_keys_immutable_guard() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
  IF OLD.status_code IS NULL THEN
    RETURN NEW;
  END IF;
  IF NEW.client_id IS DISTINCT FROM OLD.client_id
     OR NEW.key IS DISTINCT FROM OLD.key
     OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
     OR NEW.status_code IS DISTINCT FROM OLD.status_code
     OR NEW.response_body::text IS DISTINCT FROM OLD.response_body::text THEN
    RAISE EXCEPTION 'a completed idempotency key row is immutable'
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'idempotency_keys_immutable',
            SCHEMA = 'public',
            TABLE = 'idempotency_keys';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER idempotency_keys_immutable
  BEFORE UPDATE ON public.idempotency_keys
  FOR EACH ROW EXECUTE FUNCTION public.idempotency_keys_immutable_guard();
