-- Transactional outbox (spec §5, §6.2-§6.3, D-008, D-077). An event row is written in the same
-- transaction as the state change it announces, so the event exists iff the change committed;
-- the relay in ledger-api publishes unpublished rows to Kafka later and marks them. Runs as
-- ledger_api_owner. The publish-once guard is V13, so it can be seen failing first (D-038).
-- id is an identity column like transactions and entries: inserts need no sequence privilege.
-- event_id comes from gen_random_uuid() and created_at from now(): neither writer can choose
-- them (D-071). payload holds only the event's data; the relay assembles the envelope from the
-- columns, with occurred_at = created_at, so redelivered copies are byte-identical.

CREATE TABLE public.outbox (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id      UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  topic         TEXT NOT NULL,
  message_key   TEXT NOT NULL,            -- partition key (payment id) -> per-payment ordering
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  headers       JSONB NOT NULL DEFAULT '{}'::jsonb,   -- correlation id travels here
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ,
  CONSTRAINT outbox_event_type_known
    CHECK (event_type IN ('payment.authorized', 'payment.settled', 'payment.failed')),
  CONSTRAINT outbox_payload_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT outbox_headers_object CHECK (jsonb_typeof(headers) = 'object'),
  CONSTRAINT outbox_topic_nonempty CHECK (topic <> ''),
  CONSTRAINT outbox_message_key_nonempty CHECK (message_key <> '')
);

-- The relay's poll: WHERE published_at IS NULL ORDER BY id (D-008), never a high-water mark.
CREATE INDEX outbox_unpublished ON public.outbox (id) WHERE published_at IS NULL;

-- Grants (D-034, D-071, D-077): both writers insert only the event's content. The app's relay
-- reads rows and marks them published; the worker never reads the outbox. No DELETE or
-- TRUNCATE for any runtime role. Outbox purge is an open issue, so for now that is enforced by
-- grants alone; there is no DELETE/TRUNCATE trigger yet.
GRANT SELECT                                                ON public.outbox TO ledger_api_app;
GRANT INSERT (topic, message_key, event_type, payload, headers) ON public.outbox TO ledger_api_app;
GRANT UPDATE (published_at)                                 ON public.outbox TO ledger_api_app;
GRANT INSERT (topic, message_key, event_type, payload, headers) ON public.outbox TO settlement_worker;
