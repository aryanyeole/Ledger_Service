-- Consumer dedup (spec §5, §6.4, D-009, D-078). The settlement worker inserts one row per
-- consumed event in the same transaction as the event's effect (invariant 6), so the effect
-- is applied iff the row exists; a redelivered event conflicts on the primary key and is
-- skipped. A concurrent duplicate blocks on the uncommitted key until the first commits, then
-- does nothing: the same mechanism as idempotency keys. Runs as ledger_api_owner. The
-- append-only trigger is V13, so it can be seen failing first (D-038). processed_at comes
-- from now() (D-071). Rows are never purged in M3: a purged row would readmit its message.
--
-- The worker's dedup statement is the targeted form:
--   INSERT INTO processed_messages (consumer_group, event_id) VALUES (...)
--   ON CONFLICT (consumer_group, event_id) DO NOTHING
-- and it reads the outcome from the row count (1 = first delivery, 0 = already processed).
-- A conflict target needs SELECT on its columns, hence SELECT (consumer_group, event_id).
-- The untargeted ON CONFLICT DO NOTHING needs only INSERT, but it would treat a conflict on
-- any unique index as "already processed" and skip the effect silently; the targeted form
-- raises 23505 on any other unique violation.

CREATE TABLE public.processed_messages (
  consumer_group  TEXT NOT NULL,
  event_id        UUID NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id),
  CONSTRAINT processed_messages_consumer_group_nonempty CHECK (consumer_group <> '')
);

-- Grants (D-034, D-071, D-078): column-level only; processed_at is in neither grant.
-- ledger_api_app is not a consumer and gets nothing.
GRANT SELECT (consumer_group, event_id) ON public.processed_messages TO settlement_worker;
GRANT INSERT (consumer_group, event_id) ON public.processed_messages TO settlement_worker;
