-- Idempotency keys (spec §5, D-004, D-043). One row per (client, key): the first request
-- inserts it, and the same transaction stores the response (invariant 7; V6 enforces it).
-- request_hash is SHA-256 of method + path + canonical body: exactly 32 bytes.
-- expires_at comes from the database clock, never the app (D-043). Runs as ledger_api_owner.
-- No expires_at index and no DELETE grant yet: both arrive with the M4 purge job (D-020).

CREATE TABLE public.idempotency_keys (
  client_id      TEXT NOT NULL,
  key            TEXT NOT NULL,
  request_hash   BYTEA NOT NULL,
  status_code    INT,
  response_body  JSONB,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at     TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (client_id, key),
  CONSTRAINT idempotency_keys_request_hash_sha256 CHECK (octet_length(request_hash) = 32),
  CONSTRAINT idempotency_keys_status_code_range CHECK (status_code BETWEEN 100 AND 599)
);

-- Grants (D-034, D-043): the app inserts a key with its fingerprint and later stores the
-- response; it can never rewrite the key, the fingerprint or the TTL, and cannot delete.
-- settlement_worker gets nothing (D-020).
GRANT SELECT                                ON public.idempotency_keys TO ledger_api_app;
GRANT INSERT (client_id, key, request_hash) ON public.idempotency_keys TO ledger_api_app;
GRANT UPDATE (status_code, response_body)   ON public.idempotency_keys TO ledger_api_app;
