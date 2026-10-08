# Ledger Service — Canonical Spec

> Source of truth for WHAT gets built and WHY. If anything else conflicts with this file, the Decision Log in `PROGRESS.md` wins, then this file.
> Owner: Aryan Yeole · Repo: `ledger-service` (GitHub: aryanyeole) · Local: `C:\Users\Aryan\Projects\ledger-service`

---

## 1. Thesis

A payments ledger for a small marketplace: customers pay merchants, the platform takes a fee, money moves through a message queue to an async settlement worker. Three independently deployable services, one Postgres ledger, correctness guarantees that hold under **concurrency, crashes, and retries**.

Money is the one domain where "mostly correct" is a failure. That forces every property a backend interviewer probes: idempotency, exactly-once effects over at-least-once delivery, transactional boundaries, lock ordering, reconciliation, and what happens when a consumer dies mid-batch.

**The project is done when every number in the README can be defended from memory, live, under questioning — not when it runs.**

Two artifacts carry the whole project in interviews:
1. **The concurrency test** — 100 threads fire the identical request simultaneously; exactly one transaction exists afterward and all 100 got byte-identical responses.
2. **The postmortem** — the system broken deliberately three ways, diagnosed from its own dashboards, written up with a real timeline.

Everything else is scaffolding that makes those two possible.

---

## 2. The four non-negotiable properties

| # | Property | How it is guaranteed | Proof artifact |
|---|---|---|---|
| P1 | **Double-entry invariant enforced by the database** | Deferred constraint trigger: every transaction's entries sum to exactly 0 at COMMIT, or the commit fails. Entries are append-only (UPDATE/DELETE raise). | `UnbalancedTransactionRejectedTest` — raw SQL insert of an unbalanced transaction, asserts the DB rejects it at commit |
| P2 | **Real idempotency** | Same key + same payload → identical stored response, no second effect. Same key + different payload → **422**. Concurrent in-flight duplicate that exceeds lock wait → **409**. Fingerprint = SHA-256 of method + path + canonical body. TTL 24h. | `ConcurrentIdempotentTransferTest` (100 threads), `IdempotencyPayloadMismatchTest` |
| P3 | **Exactly-once effects over at-least-once delivery** | Transactional outbox on the write side; `processed_messages` dedup row inserted in the **same DB transaction** as the effect on the consume side; Kafka offsets committed only after DB commit. | `test_crash_recovery.py` — deterministic crash at each fault point, restart, assert no entry applied twice and none lost |
| P4 | **No floating point touches money** | `NUMERIC(19,4)` in Postgres, `BigDecimal` in Java, `Decimal` in Python, **strings** in JSON and TypeScript. | README section with the `0.1 + 0.2` failure shown in all three languages; a lint/test that fails on `double`/`float` in money paths |

> Note on P2 status codes: the original Project Specs said 409 for a key reused with a different payload. This spec follows the IETF `Idempotency-Key` header draft instead: **422** for payload mismatch, **409** for "original request still in progress." Knowing why is part of the interview answer. (Decision D-004.)

---

## 3. Architecture

```
                         ┌──────────────────────────────┐
  HTTP (X-Api-Key,       │  ledger-api  (Spring Boot,    │
  Idempotency-Key) ────► │  Java 21)                     │
                         │  • REST API                   │
                         │  • idempotency                │
                         │  • posts transfers/authorize  │
                         │  • outbox relay (poller)      │──┐ publishes
                         └──────────────┬───────────────┘  │
                                        │ JDBC (owner role) │
                                        ▼                   ▼
                         ┌──────────────────────┐   ┌──────────────┐
                         │ PostgreSQL           │   │ Kafka (KRaft)│
                         │ ledger schema        │   │ ledger.      │
                         │ (Flyway, owned by    │   │ payments.v1  │
                         │  ledger-api)         │   └──────┬───────┘
                         └──────────▲───────────┘          │ consumes
                                    │ asyncpg (worker role) │
                         ┌──────────┴──────────────────────▼─┐
                         │ settlement-worker (Python 3.13)    │
                         │ • settles/release payments         │
                         │ • writes ledger + dedup + outbox   │
                         │   in ONE transaction               │
                         │ • reconciliation job (drift gauge) │
                         └────────────────────────────────────┘
   Redis: idempotency fast path · balance cache · reconciliation lock
   Prometheus + Grafana + kafka-exporter: metrics, lag, drift, SLOs
   dashboard (React + TS): balances, live transactions, lag, health
```

**Exactly three services**: `ledger-api`, `settlement-worker`, `dashboard`. Kafka, Postgres, Redis, Prometheus, Grafana, kafka-exporter are infrastructure, not services.

### Why two writers to the ledger is fine (and is the point)
Both the Java API and the Python worker post ledger transactions. The balance invariant lives in the one place both must go through — the database. An application-level check would have to be implemented twice, in two languages, and kept in sync forever. A constraint can't drift. This is the strongest single argument for P1.

---

## 4. Domain model

### Accounts
| Type | Purpose | `allow_negative` |
|---|---|---|
| `CUSTOMER` | Customer wallets | false |
| `MERCHANT` | Merchant wallets | false |
| `ESCROW` | Funds held between authorize and settle | false |
| `PLATFORM_FEES` | Platform revenue | false |
| `EXTERNAL` | The outside world; source of deposits | true |

**Sign convention (D-003):** every entry has a signed `amount`; an account's balance is exactly `SUM(entries.amount)`. Money leaving an account is negative, arriving is positive. The double-entry invariant is `SUM(amount) = 0` per transaction. (Real ledgers model debit/credit normal balances per account class; this simplification preserves the invariant and keeps the math auditable. Be ready to explain the difference.)

Single currency: USD only (D-002). API accepts at most 2 decimal places.

### Transaction kinds and their entries

| Kind | Trigger | Entries (example: $100.00, fee 2.9% → $2.90) |
|---|---|---|
| `DEPOSIT` | `POST /v1/deposits` | EXTERNAL −100.00 · CUSTOMER +100.00 |
| `TRANSFER` | `POST /v1/transfers` | FROM −X · TO +X |
| `PAYMENT_AUTHORIZE` | `POST /v1/payments` | CUSTOMER −100.00 · ESCROW +100.00 |
| `PAYMENT_SETTLE` | worker, processor approved | ESCROW −100.00 · MERCHANT +97.10 · PLATFORM_FEES +2.90 |
| `PAYMENT_RELEASE` | worker, processor declined | ESCROW −100.00 · CUSTOMER +100.00 |

**Fee rule:** `fee = round(amount × rate, 2, HALF_EVEN)`; `merchant_amount = amount − fee`. Merchant amount is **never** computed independently — that is how pennies go missing.

### Payment lifecycle
```
POST /v1/payments ──► AUTHORIZED ──(worker: processor approves)──► SETTLED
                                  └─(worker: processor declines)──► FAILED (funds released)
```
Transitions are conditional updates (`... WHERE id = ? AND status = 'AUTHORIZED'`). Zero rows updated = already handled = second dedup layer.

The "order intake → payment confirm → fulfillment event" pipeline maps to: payment request → authorize hold → settlement → `payment.settled` event (the fulfillment trigger). **Inventory reservation is deferred** (D-012) — it is a second domain that adds surface area without a new correctness property.

---

## 5. Schema (Flyway `V1__ledger_core.sql` onward)

```sql
CREATE TABLE accounts (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name           TEXT NOT NULL UNIQUE,
  type           TEXT NOT NULL CHECK (type IN ('CUSTOMER','MERCHANT','ESCROW','PLATFORM_FEES','EXTERNAL')),
  currency       CHAR(3) NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  balance        NUMERIC(19,4) NOT NULL DEFAULT 0,   -- materialized; reconciled against entries
  allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT no_overdraft CHECK (allow_negative OR balance >= 0)
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

-- P1: the invariant. Deferred so multi-row inserts are checked once, at COMMIT.
CREATE FUNCTION assert_transaction_balanced() RETURNS trigger AS $$
DECLARE s NUMERIC; n INT;
BEGIN
  SELECT COALESCE(SUM(amount), 0), COUNT(*) INTO s, n
  FROM entries WHERE transaction_id = NEW.transaction_id;
  IF s <> 0 OR n < 2 THEN
    RAISE EXCEPTION 'unbalanced transaction %: sum=% entries=%', NEW.transaction_id, s, n
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER entries_balanced
  AFTER INSERT ON entries
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION assert_transaction_balanced();

-- Append-only ledger.
CREATE FUNCTION forbid_mutation() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION '% is append-only', TG_TABLE_NAME; END $$ LANGUAGE plpgsql;
CREATE TRIGGER entries_append_only BEFORE UPDATE OR DELETE ON entries
  FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
-- Also: a deferred trigger rejecting a `transactions` row with zero entries (M1 task).
-- Also: app DB roles get no TRUNCATE privilege (row triggers don't fire on TRUNCATE).

CREATE TABLE idempotency_keys (
  client_id      TEXT NOT NULL,
  key            TEXT NOT NULL,
  request_hash   BYTEA NOT NULL,          -- sha256(method + path + canonical JSON body)
  status_code    INT,
  response_body  JSONB,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (client_id, key)
);

CREATE TABLE payments (
  id                   UUID PRIMARY KEY,
  customer_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  merchant_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  amount               NUMERIC(19,4) NOT NULL CHECK (amount > 0),
  fee                  NUMERIC(19,4) NOT NULL CHECK (fee >= 0),
  status               TEXT NOT NULL CHECK (status IN ('AUTHORIZED','SETTLED','FAILED')),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id      UUID NOT NULL UNIQUE,
  topic         TEXT NOT NULL,
  message_key   TEXT NOT NULL,            -- partition key (payment id) → per-payment ordering
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  headers       JSONB NOT NULL DEFAULT '{}'::jsonb,   -- correlation id travels here
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
  consumer_group  TEXT NOT NULL,
  event_id        UUID NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);
```

The SQL above is the design target, not copy-paste-and-forget. Every line must be explainable.

---

## 6. Write paths (the five-minute whiteboard)

### 6.1 `POST /v1/transfers` — synchronous
One Postgres transaction (READ COMMITTED, `lock_timeout = 5s`):
1. Validate body; compute `request_hash`. Missing `Idempotency-Key` → **400**.
2. Redis fast path (M5+): completed response cached → return it. Redis is an optimization, never the guarantee.
3. `INSERT INTO idempotency_keys ... ON CONFLICT DO NOTHING`.
   - **Inserted** → this request owns the key; continue.
   - **Conflict** → `SELECT` the row. Hash differs → **422**. Hash matches → return stored `status_code` + `response_body` verbatim.
   - If a concurrent duplicate is mid-flight, the `INSERT` **blocks** on the uncommitted unique-index entry until the owner commits, then sees the committed row and replays it. This is why all 100 threads get the same body. Lock wait > `lock_timeout` → **409** (in progress, retry).
4. `SELECT ... FROM accounts WHERE id IN (a, b) ORDER BY id FOR UPDATE` — **ascending id lock ordering** prevents A→B / B→A deadlocks (D-007).
5. Insufficient funds → store a 422 problem response against the key, commit (deterministic outcome is idempotent too).
6. Insert `transactions` row, both `entries` (account-id order), update both `accounts.balance`.
7. Store 201 response in `idempotency_keys`; COMMIT. The deferred trigger checks balance here.

### 6.2 `POST /v1/payments` — async
Steps 1–4 as above, then: insert `payments` (AUTHORIZED), post `PAYMENT_AUTHORIZE`, insert `outbox` row `payment.authorized` — **all in the same commit**. No Kafka call inside the request (that would be the dual-write problem).

### 6.3 Outbox relay (inside ledger-api)
Every 200 ms: `SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED` → produce to Kafka (`acks=all`, idempotent producer) → wait for acks → set `published_at` → commit.
- Crash after send, before commit → re-published → consumers dedup. That is the at-least-once seam.
- Selects by `published_at IS NULL`, **not** an `id > last_seen` high-water mark: identity values can commit out of order, and a high-water mark would skip a late-committing row.
- At scale: CDC (Debezium) instead of polling — a "what I'd change" answer, not a build item.

### 6.4 Settlement worker
Consumer group `settlement-worker`, `enable.auto.commit=false`. Per message:
1. Call the **fake processor** with idempotency key = `payment_id` (it returns the same decision for the same id — D-010). Retried calls after a crash cannot double-charge.
2. One Postgres transaction: `INSERT INTO processed_messages ... ON CONFLICT DO NOTHING` (conflict → skip, already applied) · conditional status update on `payments` · post `PAYMENT_SETTLE` or `PAYMENT_RELEASE` (account-id lock order) · outbox row `payment.settled` / `payment.failed` · COMMIT.
3. Commit the Kafka offset **after** the DB commit (D-014).
4. Poison message: after N retries with backoff → `ledger.payments.dlq.v1`, metric incremented, alert.

**Fault-injection points** (env var `FAULT_POINT`, used by the crash test):
- `AFTER_PROCESSOR_BEFORE_DB` — external call happened, effect not committed.
- `AFTER_DB_BEFORE_OFFSET` — effect committed, offset not committed → redelivery → dedup must catch it.

### 6.5 Reconciliation (worker, every 60 s, Redis lock so only one runs)
For every account: `SUM(entries.amount)` vs `accounts.balance`. Any mismatch → `ledger_drift_amount{account}` gauge ≠ 0 → alert. Also: global `SUM(entries.amount) = 0`.

---

## 7. API surface

All JSON amounts are **strings** (`"100.00"`). Errors are RFC 9457 `application/problem+json` with a machine-readable `code`. Auth: `X-Api-Key` header mapped to a `client_id` (idempotency keys are scoped per client).

| Method | Path | Idempotency-Key | Notes |
|---|---|---|---|
| POST | `/v1/accounts` | optional | create account |
| POST | `/v1/deposits` | required | EXTERNAL → account |
| POST | `/v1/transfers` | required | sync double-entry transfer |
| POST | `/v1/payments` | required | authorize → async settle |
| GET | `/v1/payments/{id}` | — | status |
| GET | `/v1/accounts/{id}/balance` | — | `{account_id, balance, as_of}`; Redis cache M5+ |
| GET | `/v1/transactions?limit=&cursor=` | — | keyset pagination, opaque cursor over `(created_at, id)` |
| GET | `/actuator/health`, `/actuator/prometheus` | — | ops |

OpenAPI published via springdoc.

### Event envelope (`ledger.payments.v1`, keyed by payment id, 3 partitions)
```json
{ "event_id": "uuid", "event_type": "payment.authorized", "version": 1,
  "occurred_at": "2026-...Z", "correlation_id": "...",
  "data": { "payment_id": "uuid", "amount": "100.00", "fee": "2.90", "...": "..." } }
```

---

## 8. Stack

Spring Boot (Java 21, Maven wrapper, `JdbcClient` — not JPA — for ledger writes, Flyway, spring-kafka, Micrometer/Prometheus, springdoc, Testcontainers) · Python 3.13 (`asyncio`, `aiokafka`, `asyncpg`, `prometheus_client`, `structlog`, `pytest`, `testcontainers`) · React + TypeScript (Vite) · PostgreSQL 17 · Kafka (`apache/kafka`, KRaft) · Redis 7 · Prometheus · Grafana · kafka-exporter · k6 · Docker Compose · GitHub Actions.

Why `JdbcClient` over JPA for the write path: `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, explicit lock ordering, and exact transaction boundaries must be visible in the code, not hidden behind an ORM flush (D-005).

---

## 9. Milestones

Five build weeks + one optional. Each milestone ends with its proof artifact committed, `PROGRESS.md` updated, and the matching section of `03_INTERVIEW_GATE.md` answered out loud.

| # | Week | Milestone | Proof artifact (must exist before moving on) |
|---|---|---|---|
| M0 | 1 (day 1) | Scaffold: monorepo, Compose (Postgres :5433, Kafka, Redis), Spring app boots, Python worker boots, CI green on an empty test | CI badge green |
| M1 | 1 | Schema + invariant + append-only + roles | `UnbalancedTransactionRejectedTest`, `EntriesAppendOnlyTest` |
| M2 | 1–2 | API: deposits, transfers, balance, keyset `/transactions`; idempotency; lock ordering | **`ConcurrentIdempotentTransferTest`** (100 threads), `IdempotencyPayloadMismatchTest`, `OppositeTransfersNoDeadlockTest` |
| M3 | 2 | Payments + outbox + relay + Kafka; worker consumes with dedup | `test_crash_recovery.py` at both fault points; `OutboxRedeliveryTest` |
| M4 | 3 | Settlement logic, fake idempotent processor, DLQ, reconciliation + drift metric | `test_reconciliation_detects_drift.py` (inject drift → gauge ≠ 0) |
| M5 | 3 | Redis: idempotency fast path, balance cache + invalidation, reconciliation lock | `docs/design/cache-invalidation.md` (strategy + failure mode); cache hit/miss benchmark |
| M6 | 4 | Observability: latency histograms, consumer lag, drift gauge, idempotency hit rate, JSON logs w/ correlation ID across all 3 services, 3 SLOs + error budgets, alert rules | Grafana dashboard JSON committed; one correlation ID traced API → Kafka → worker in logs (screenshot) |
| M7 | 4 | Dashboard: balances, live transaction stream (keyset polling), lag, health | Deployed-ready build; 30-second recruiter view |
| M8 | 5 | k6 at 1x and 10x (p50/p95/p99); chaos ×3; **postmortem** | `docs/load-tests/report.md`, `docs/postmortems/2026-xx-chaos.md` with timeline + screenshots |
| M9 | 5 | Deploy (one ≥4 GB VM running the same Compose file); design doc | Live URL + `docs/DESIGN.md` (read path, write path, failure modes, 100x) |
| M10 | 6 (optional) | Typed-tool agent ("balance of account X", "failed payments yesterday") with 30-question eval via the Data Den v2 harness | Eval scorecard in README |

### M8 chaos scenarios (each: what alerted, what the dashboard showed, time to diagnose, fix)
1. `docker kill` the worker mid-batch.
2. Exhaust the DB pool (Hikari `maximumPoolSize` deliberately small + slow query under load).
3. Stop the Kafka broker; observe outbox backlog grow, then drain on recovery.

### Pressure points to look for in the 10x test (hypotheses, not conclusions)
- **Hot rows:** `ESCROW` and `PLATFORM_FEES` are locked by every payment — expect contention there first.
- Outbox poll interval × batch size as a throughput ceiling.
- Hikari pool size vs. request concurrency.

---

## 10. Anti-goals (load-bearing — each costs ~2 weeks and adds nothing)

No auth beyond API keys · no multi-region · no Kubernetes (Compose only) · no real payment provider · no 4th service · no multi-currency · no inventory domain · no event-sourcing framework · no UI polish beyond functional · no new technology not listed in §8 without a Decision Log entry.

If a request drifts toward any of these, stop and say so.

---

## 11. Definition of done

- [ ] Live deployment; public dashboard or committed screenshots.
- [ ] Concurrency test and crash-recovery test both run in CI and pass.
- [ ] Postmortem committed with real timestamps and real dashboard screenshots.
- [ ] Load test report: p50/p95/p99 at 1x and 10x.
- [ ] `docs/DESIGN.md` written as if for a system design interview.
- [ ] README: architecture diagram, the four properties with links to their proof tests, measured-numbers table, setup, "what I'd do differently."
- [ ] Can whiteboard the write path in five minutes and name three things that break at 100x without hesitating.
- [ ] Every item in `03_INTERVIEW_GATE.md` answered cold.
