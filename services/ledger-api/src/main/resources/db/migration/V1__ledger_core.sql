-- Ledger core: accounts, transactions, entries (spec §5). Runs as ledger_api_owner,
-- which therefore owns every object here. The balance and append-only triggers are
-- V2 and V3. Money is NUMERIC(19,4), never floating point.

CREATE TABLE accounts (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name           TEXT NOT NULL UNIQUE,
  type           TEXT NOT NULL CHECK (type IN ('CUSTOMER','MERCHANT','ESCROW','PLATFORM_FEES','EXTERNAL')),
  currency       CHAR(3) NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  balance        NUMERIC(19,4) NOT NULL DEFAULT 0,   -- materialized; reconciled against entries
  allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT no_overdraft CHECK (allow_negative OR balance >= 0),
  -- Spec §4: only EXTERNAL may go negative (D-039).
  CONSTRAINT external_only_negative CHECK (allow_negative = (type = 'EXTERNAL'))
);

CREATE TABLE transactions (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind            TEXT NOT NULL CHECK (kind IN ('DEPOSIT','TRANSFER','PAYMENT_AUTHORIZE','PAYMENT_SETTLE','PAYMENT_RELEASE')),
  reference       TEXT,          -- e.g. payment id
  correlation_id  TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE entries (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  transaction_id  BIGINT NOT NULL REFERENCES transactions(id),
  account_id      BIGINT NOT NULL REFERENCES accounts(id),
  amount          NUMERIC(19,4) NOT NULL CHECK (amount <> 0),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX entries_by_txn     ON entries (transaction_id);
CREATE INDEX entries_by_account ON entries (account_id, id);

-- Grants (D-034): explicit per table, in the migration that creates the table.
-- No UPDATE on transactions/entries, and no DELETE, TRUNCATE, REFERENCES or TRIGGER
-- for any app role. UPDATE (balance) is also what SELECT ... FOR UPDATE needs.
-- Account inserts leave balance, currency and the audit columns to their defaults.
GRANT SELECT                                ON accounts     TO ledger_api_app;
GRANT INSERT (name, type, allow_negative)   ON accounts     TO ledger_api_app;
GRANT UPDATE (balance)                      ON accounts     TO ledger_api_app;
GRANT SELECT, INSERT                        ON transactions TO ledger_api_app;
GRANT SELECT, INSERT                        ON entries      TO ledger_api_app;

GRANT SELECT                                ON accounts     TO settlement_worker;
GRANT UPDATE (balance)                      ON accounts     TO settlement_worker;
GRANT SELECT, INSERT                        ON transactions TO settlement_worker;
GRANT SELECT, INSERT                        ON entries      TO settlement_worker;
