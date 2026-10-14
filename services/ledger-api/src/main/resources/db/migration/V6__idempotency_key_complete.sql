-- An idempotency key commits with its response (invariant 7, D-059). The key row is
-- inserted with a null response and completed later in the same transaction, so a CHECK
-- constraint cannot express this: a deferred constraint trigger re-reads the row at COMMIT.
-- It uses NEW only to find the row by primary key, never for the values it checks, so it
-- sees the row as the transaction leaves it, including any later UPDATE. A row that no
-- longer exists is skipped. not_null_violation (23502) with the constraint name is distinct
-- from 42501 (grants), 23001 (append-only) and 23514 (balance).
-- Same hardening as V2 (D-040): schema-qualified names, pinned search_path, invoker rights.

CREATE FUNCTION public.assert_idempotency_key_complete() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE s INT; b JSONB;
BEGIN
  SELECT k.status_code, k.response_body INTO s, b
  FROM public.idempotency_keys k
  WHERE k.client_id = NEW.client_id AND k.key = NEW.key;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  IF s IS NULL OR b IS NULL THEN
    RAISE EXCEPTION 'idempotency key %/% has no stored response', NEW.client_id, NEW.key
      USING ERRCODE = 'not_null_violation',
            CONSTRAINT = 'idempotency_keys_complete',
            TABLE = 'idempotency_keys';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER idempotency_keys_complete
  AFTER INSERT OR UPDATE ON public.idempotency_keys
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION public.assert_idempotency_key_complete();
