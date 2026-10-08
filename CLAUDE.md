# CLAUDE.md — Ledger Service

Payments ledger: Spring Boot API + Python settlement worker + React/TS dashboard, over PostgreSQL, Kafka, Redis, Prometheus/Grafana, Docker Compose. Full spec: `docs/` (PROGRESS.md has the Decision Log — it overrides everything else).

## Environment
- Windows, PowerShell. Give every command in PowerShell syntax (`;` not `&&`, no `<` redirects, `.\mvnw.cmd`, `.\.venv\Scripts\Activate.ps1`).
- Java 21 via Maven wrapper only. Python 3.13 invoked as `python`. Node 22.
- Container Postgres is on host port **5433** (native Postgres owns 5432). DB `ledger`.

## Commands
- Infra: `docker compose up -d postgres kafka redis`
- API tests: `cd services\ledger-api; .\mvnw.cmd test`
- Worker tests: `cd services\settlement-worker; .\.venv\Scripts\Activate.ps1; pytest`
- Dashboard: `cd services\dashboard; npm run build`

## Invariants — never violate, never "simplify away"
1. Ledger entries are written only inside a DB transaction that also writes their `transactions` row; every transaction's entries sum to 0. The DB trigger enforces this — do not disable, weaken, or work around it, including in tests (except the test that proves it rejects unbalanced inserts).
2. `entries` is append-only. Corrections are new reversing transactions.
3. Money is `BigDecimal` / `Decimal` / JSON string. No `double`, `float`, or JS `Number` anywhere in a money path. Rounding mode is always explicit (HALF_EVEN).
4. Lock accounts in ascending id order before updating balances (Java and Python).
5. No Kafka or HTTP calls inside a DB transaction. Events go through the `outbox` table.
6. Consumer side: `processed_messages` insert + effect + outbox in ONE DB transaction; commit the Kafka offset after the DB commit.
7. Idempotency key row and the business effect commit in the same transaction.

Exception (D-017): negative-control tests may deliberately violate invariants 6–7 to prove the proof tests can detect those bugs. They live only in `services/ledger-api/src/test/java/**/negativecontrols/` and `services/settlement-worker/tests/negative_controls/`. Main code never imports them. Each one asserts that the naive variant FAILS (duplicates appear). They never disable, alter, or bypass DB triggers, schema, or roles, and invariants 1–5 still apply inside them. They run in CI like any other test. Any violation outside these folders is a bug.

## Rules
- Flyway migrations live only in `services/ledger-api`. Never edit an applied migration; add a new one.
- Integration tests use Testcontainers (real Postgres/Kafka). Never mock the database for ledger, idempotency, outbox, or dedup logic.
- Show the plan before writing code for anything touching invariants 1–7. One task = one PR (see Git workflow).
- Bug log (D-016): when a test, CI run, drill, or manual check exposes a real defect, STOP before fixing it. Add an entry to `docs/bugs.md` with the symptom, exact error, how it was detected, and a timestamp, then tell me. Fill in root cause, fix commit, and regression test as they happen; show the regression test failing before the fix. Never backfill, embellish, or invent entries. Never log designed-in behavior (a negative control failing, dedup catching a redelivery) as a bug.
- Don't add dependencies, services, or infrastructure not listed in the spec without asking. Out of scope: Kubernetes, multi-region, real payment providers, multi-currency, inventory, a 4th service, auth beyond API keys.
- Don't touch `.env` or print secrets/API keys.
- Logs are JSON and carry `correlation_id`.
- When a decision is made that isn't in the Decision Log, say so explicitly so it can be added.

## Git workflow (one task = one branch = one PR)
- Branch from up-to-date `main`: `m<n>/t<k>-<short-slug>` (e.g. `m1/t2-balance-trigger`).
- One task = one PR = one commit on `main` (squash merge). Conventional title: `feat(api): ...`, `test(worker): ...`, `chore(ci): ...`. Proof tests get their own task and PR.
- Never commit or push directly to `main`. Never merge, force-push, or rewrite pushed history — I merge.
- Before pushing: run the tests for every service touched and paste the summary.
- PR description: Task (M<n> T<k>) · What changed · Invariants touched (1–7 or none) · Tests run + result · Decision Log rows needed (or none) · Bug log entries (or none).
- Open the PR with `gh pr create` if the GitHub CLI is installed and authenticated; otherwise push the branch and give me the compare URL.
