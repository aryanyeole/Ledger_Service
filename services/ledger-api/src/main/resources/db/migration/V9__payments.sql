-- Payments (spec §4, §5, D-076). One row per authorize: the API inserts it AUTHORIZED in the
-- same transaction as the PAYMENT_AUTHORIZE posting; the settlement worker later moves it to
-- SETTLED or FAILED. Runs as ledger_api_owner. The transition guard and the DELETE/TRUNCATE
-- trigger are V10, so they can be seen failing first (D-038).
-- id comes from gen_random_uuid() (built in since Postgres 13) and status, created_at and
-- updated_at from their defaults: no runtime role can choose them (D-071).
-- Money is NUMERIC(19,4), never floating point.

CREATE TABLE public.payments (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_account_id  BIGINT NOT NULL REFERENCES public.accounts(id),
  merchant_account_id  BIGINT NOT NULL REFERENCES public.accounts(id),
  amount               NUMERIC(19,4) NOT NULL CHECK (amount > 0),
  fee                  NUMERIC(19,4) NOT NULL CHECK (fee >= 0),
  status               TEXT NOT NULL DEFAULT 'AUTHORIZED' CHECK (status IN ('AUTHORIZED','SETTLED','FAILED')),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT payments_fee_le_amount CHECK (fee <= amount),
  CONSTRAINT payments_distinct_accounts CHECK (customer_account_id <> merchant_account_id)
);

-- Grants (D-034, D-071, D-076): the API inserts the parties and the money, nothing else, and
-- never updates. The worker changes only status; UPDATE (status) is also what its
-- SELECT ... FOR UPDATE needs. No DELETE, TRUNCATE, REFERENCES, TRIGGER or MAINTAIN for any
-- runtime role.
GRANT SELECT                                                       ON public.payments TO ledger_api_app;
GRANT INSERT (customer_account_id, merchant_account_id, amount, fee) ON public.payments TO ledger_api_app;
GRANT SELECT                                                       ON public.payments TO settlement_worker;
GRANT UPDATE (status)                                              ON public.payments TO settlement_worker;
