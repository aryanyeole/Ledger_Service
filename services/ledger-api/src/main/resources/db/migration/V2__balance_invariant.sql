-- P1: every transaction's entries sum to 0, checked by the database (invariant 1).
-- Constraint triggers are DEFERRABLE INITIALLY DEFERRED: entries go in one row at a time,
-- so the sum is legitimately non-zero mid-transaction; each queued check runs at COMMIT
-- and re-sums the whole transaction, including entries committed earlier.
-- The functions run as the inserting role (no SECURITY DEFINER), which has SELECT on
-- entries. Tables are schema-qualified and search_path is pinned, so a temp table named
-- entries cannot shadow the real one inside the check (D-040).

CREATE FUNCTION public.assert_transaction_balanced() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE s NUMERIC; n INT;
BEGIN
  SELECT COALESCE(SUM(e.amount), 0), COUNT(*) INTO s, n
  FROM public.entries e WHERE e.transaction_id = NEW.transaction_id;
  IF s <> 0 OR n < 2 THEN
    RAISE EXCEPTION 'unbalanced transaction %: sum=% entries=%', NEW.transaction_id, s, n
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER entries_balanced
  AFTER INSERT ON public.entries
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION public.assert_transaction_balanced();

-- entries_balanced never fires for a transaction with no entries; this one does.
CREATE FUNCTION public.assert_transaction_has_entries() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE n INT;
BEGIN
  SELECT COUNT(*) INTO n FROM public.entries e WHERE e.transaction_id = NEW.id;
  IF n = 0 THEN
    RAISE EXCEPTION 'transaction % has no entries', NEW.id
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER transactions_have_entries
  AFTER INSERT ON public.transactions
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION public.assert_transaction_has_entries();
