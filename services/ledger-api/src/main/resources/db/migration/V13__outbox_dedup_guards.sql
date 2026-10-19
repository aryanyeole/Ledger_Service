-- Outbox and dedup guards (D-077, D-078).
--
-- outbox_publish_once: a row is marked published exactly once and nothing else in it ever
-- changes. published_at may only go NULL -> set; any other change (the event's content, its
-- id, event_id or created_at) is rejected. The check compares the whole row as jsonb minus
-- published_at, so a column added later is covered without editing this function. The value
-- comes from clock_timestamp(), not now(): now() is the relay transaction's start, which is
-- before it sends the batch and waits for the acks, and published_at should record when the
-- row was marked. A violation raises check_violation (23514) naming outbox_publish_once,
-- distinct from 42501 (grants) and 23001 (append-only). Row-level BEFORE, so it fails the
-- statement itself. The function reads no tables; search_path is pinned per D-040.
--
-- processed_messages is append-only (D-041): the statement-level forbid_mutation() trigger
-- from V3 rejects UPDATE, DELETE (zero-row included) and TRUNCATE with 23001, so even the
-- owner can't remove a dedup row and readmit a redelivered message.

CREATE FUNCTION public.outbox_publish_once() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
  IF OLD.published_at IS NOT NULL OR NEW.published_at IS NULL THEN
    RAISE EXCEPTION 'outbox row %: published_at may only be set once', OLD.id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'outbox_publish_once',
            SCHEMA = 'public',
            TABLE = 'outbox';
  END IF;
  IF (to_jsonb(NEW) - 'published_at') IS DISTINCT FROM (to_jsonb(OLD) - 'published_at') THEN
    RAISE EXCEPTION 'outbox row %: only published_at may change', OLD.id
      USING ERRCODE = 'check_violation',
            CONSTRAINT = 'outbox_publish_once',
            SCHEMA = 'public',
            TABLE = 'outbox';
  END IF;
  NEW.published_at := clock_timestamp();
  RETURN NEW;
END $$;

CREATE TRIGGER outbox_publish_once
  BEFORE UPDATE ON public.outbox
  FOR EACH ROW EXECUTE FUNCTION public.outbox_publish_once();

CREATE TRIGGER processed_messages_append_only
  BEFORE UPDATE OR DELETE OR TRUNCATE ON public.processed_messages
  FOR EACH STATEMENT EXECUTE FUNCTION public.forbid_mutation();
