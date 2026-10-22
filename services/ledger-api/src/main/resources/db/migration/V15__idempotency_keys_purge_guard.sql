-- Only expired idempotency keys may be deleted (D-020, D-119). A key replays its stored
-- response until it is purged, so deleting a live key would let a retried request run its
-- effect a second time; this makes "a purge can't delete a live key" a database guarantee,
-- like the completeness trigger (D-059), for every role including the owner.
--
-- idempotency_keys_purge: row-level BEFORE DELETE. A row whose expires_at is not yet past
-- raises check_violation (23514) naming idempotency_keys_purge, distinct from 42501 (grants),
-- 23001 (append-only) and 23502 (completeness), and the whole statement fails, so one live
-- row stops a batch. It compares with now(), not clock_timestamp(): the purge's select (T2)
-- uses now() too, so both see the same instant within one transaction. The message carries
-- no client id, key or request hash. The function reads no tables; search_path is pinned
-- per D-040.
--
-- idempotency_keys_no_truncate: TRUNCATE skips row triggers, so it is rejected by the
-- statement-level forbid_mutation() from V3 (23001, D-041). No runtime role has TRUNCATE;
-- this is what stops the owner.

CREATE FUNCTION public.idempotency_keys_purge_guard() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
  IF OLD.expires_at < now() THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'only expired idempotency keys may be deleted'
    USING ERRCODE = 'check_violation',
          CONSTRAINT = 'idempotency_keys_purge',
          SCHEMA = 'public',
          TABLE = 'idempotency_keys';
END $$;

CREATE TRIGGER idempotency_keys_purge
  BEFORE DELETE ON public.idempotency_keys
  FOR EACH ROW EXECUTE FUNCTION public.idempotency_keys_purge_guard();

CREATE TRIGGER idempotency_keys_no_truncate
  BEFORE TRUNCATE ON public.idempotency_keys
  FOR EACH STATEMENT EXECUTE FUNCTION public.forbid_mutation();
