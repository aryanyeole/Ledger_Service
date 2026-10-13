-- Append-only ledger (invariant 2): corrections are new reversing transactions.
-- Statement-level BEFORE triggers reject the statement itself, so zero-row UPDATE/DELETE
-- and TRUNCATE (which row triggers never see) fail too, with restrict_violation (23001),
-- distinct from 42501 (grants) and 23514 (balance) (D-041).
-- Runtime roles have no UPDATE/DELETE/TRUNCATE grants on these tables and are stopped
-- with 42501 before any trigger runs; these triggers are what stop the owner.
-- transactions is append-only too: its kind and reference are part of the record (D-042).
-- The function reads no tables; search_path is pinned for consistency with V2 (D-040).

CREATE FUNCTION public.forbid_mutation() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
  RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
    USING ERRCODE = 'restrict_violation';
  RETURN NULL;
END $$;

CREATE TRIGGER entries_append_only
  BEFORE UPDATE OR DELETE OR TRUNCATE ON public.entries
  FOR EACH STATEMENT EXECUTE FUNCTION public.forbid_mutation();

CREATE TRIGGER transactions_append_only
  BEFORE UPDATE OR DELETE OR TRUNCATE ON public.transactions
  FOR EACH STATEMENT EXECUTE FUNCTION public.forbid_mutation();
