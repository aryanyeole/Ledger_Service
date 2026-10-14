-- System accounts (D-048): exactly one EXTERNAL, ESCROW and PLATFORM_FEES account, seeded
-- here as ledger_api_owner. The partial unique index makes each a singleton in the database,
-- whoever inserts; the API only creates CUSTOMER and MERCHANT accounts. allow_negative
-- follows D-039: true for EXTERNAL only. Balances start at the column default, 0.
-- No grant changes: the index and rows are covered by V1's grants on accounts.

CREATE UNIQUE INDEX accounts_one_per_system_type ON public.accounts (type)
  WHERE type IN ('EXTERNAL', 'ESCROW', 'PLATFORM_FEES');

INSERT INTO public.accounts (name, type, allow_negative) VALUES
  ('system:external',      'EXTERNAL',      TRUE),
  ('system:escrow',        'ESCROW',        FALSE),
  ('system:platform_fees', 'PLATFORM_FEES', FALSE);
