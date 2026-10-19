-- Payment guards (D-076). The status transition is the second dedup layer of spec §4: the
-- worker's conditional UPDATE ... WHERE status = 'AUTHORIZED' matches nothing for a payment
-- already handled. This trigger makes the transition rule a database guarantee too:
--  - status may only move AUTHORIZED -> SETTLED or AUTHORIZED -> FAILED;
--  - no other column may change in the same UPDATE. The check compares the whole row as
--    jsonb minus status and updated_at, so a column added later is covered without editing
--    this function;
--  - updated_at always comes from the database clock (D-071): the trigger sets it, and no
--    runtime role has UPDATE on it.
-- A violation raises check_violation (23514) naming the constraint payments_status_transition,
-- distinct from 42501 (grants) and 23001 (append-only). Row-level BEFORE, so it fails the
-- statement itself, and a conditional UPDATE matching no row never fires it.
-- The function reads no tables; search_path is pinned per D-040, invoker rights.
-- The row is the audit record of the authorize (D-042): DELETE and TRUNCATE are rejected by
-- the statement-level forbid_mutation() trigger from V3 (23001), zero-row DELETE included.

CREATE FUNCTION public.payments_status_transition() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
  IF NOT (OLD.status = 'AUTHORIZED' AND NEW.status IN ('SETTLED', 'FAILED')) THEN
    RAISE EXCEPTION 'payment %: status % -> % is not allowed', OLD.id, OLD.status, NEW.status
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'payments_status_transition',
            SCHEMA = 'public',
            TABLE = 'payments';
  END IF;
  IF (to_jsonb(NEW) - 'status' - 'updated_at') IS DISTINCT FROM (to_jsonb(OLD) - 'status' - 'updated_at') THEN
    RAISE EXCEPTION 'payment %: only status may change', OLD.id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'payments_status_transition',
            SCHEMA = 'public',
            TABLE = 'payments';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

CREATE TRIGGER payments_status_transition
  BEFORE UPDATE ON public.payments
  FOR EACH ROW EXECUTE FUNCTION public.payments_status_transition();

CREATE TRIGGER payments_no_delete
  BEFORE DELETE OR TRUNCATE ON public.payments
  FOR EACH STATEMENT EXECUTE FUNCTION public.forbid_mutation();
