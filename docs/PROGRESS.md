# Ledger Service — Progress

> Lives at `docs/PROGRESS.md` in the repo (source of truth). Re-upload to the claude.ai project at every milestone close.
> The Decision Log overrides `01_LEDGER_SPEC.md` where they conflict.

## Status

| Milestone | Status | Proof artifact(s) | Commit |
|---|---|---|---|
| M0 Scaffold + CI | not started | CI green on `main` (badge in README) | |
| M1 Schema + invariant | not started | `UnbalancedTransactionRejectedTest`, `EntriesAppendOnlyTest` | |
| M2 API + idempotency | not started | `ConcurrentIdempotentTransferTest` (100 threads), `IdempotencyPayloadMismatchTest`, `OppositeTransfersNoDeadlockTest`, negative control: check-then-insert idempotency → duplicate transactions (D-017) | |
| M3 Outbox + Kafka + dedup | not started | `test_crash_recovery.py` (both fault points × N iterations, D-018), `OutboxRedeliveryTest`, negative control: dedup row committed in a separate tx → double-applied entries (D-017) | |
| M4 Settlement + reconciliation | not started | `test_reconciliation_detects_drift.py`, `IdempotencyKeyExpiryTest` (D-020), snapshot-consistent reconciliation (D-019); optional negative control: two-query reconciliation under load → false drift (D-017) | |
| M5 Redis | not started | `docs/design/cache-invalidation.md`, cache benchmark (invalidate-on-commit + long TTL vs short TTL-only, same k6 mix; hit rate **and** stale reads served), `test_reconciliation_lock.py` (concurrent runs → one executes; dead holder → TTL expiry → next run proceeds) | |
| M6 Observability | not started | Grafana dashboard JSON, correlation-id trace API → Kafka → worker (screenshot), outbox backlog gauges + one alert per chaos scenario (D-022), account lock-wait timer (D-023) | |
| M7 Dashboard | not started | build passing | |
| M8 Load + chaos + postmortem | not started | `docs/load-tests/report.md` with hardware / VUs / req/s / k6 version (D-024), `docs/postmortems/2026-xx-chaos.md` | |
| M9 Deploy + design doc | not started | live URL on single AWS EC2 instance via Compose (D-021), `docs/DESIGN.md` | |
| M10 Agent layer (optional) | not started | eval scorecard: 30 read questions + 5 write attempts, scored separately (D-025) | |

## Decision Log

| ID | Decision | Why | Rejected alternative |
|---|---|---|---|
| D-001 | Monorepo: `services/{ledger-api,settlement-worker,dashboard}`, `infra/`, `docs/` | One Compose file, one CI, one README for reviewers | Repo per service |
| D-002 | Single currency (USD), max 2 dp at the API | Multi-currency is an anti-goal; adds FX without a new correctness property | Multi-currency accounts |
| D-003 | Signed-amount convention: balance = SUM(entries.amount); invariant = SUM per transaction = 0 | Auditable math, simple reconciliation | Debit/credit columns with normal balances per account class (explain as the "real" model) |
| D-004 | Idempotency key + effect in the same Postgres transaction; 422 on payload mismatch, 409 on in-progress (lock_timeout), 400 on missing key | Unique-index insert doubles as the lock; follows IETF Idempotency-Key draft (original spec said 409 for mismatch) | Separate IN_PROGRESS state machine (Stripe-style recovery points) — overkill for single-DB |
| D-005 | `JdbcClient` (not JPA) for ledger write paths | `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, lock ordering and tx boundaries must be explicit | Spring Data JPA |
| D-006 | Flyway migrations owned by ledger-api; worker has its own DB role and never migrates; no app role has TRUNCATE | Single schema owner; row triggers don't fire on TRUNCATE | Shared migrations |
| D-007 | Lock accounts in ascending id order on every posting (Java and Python) | Removes deadlock cycles between opposite transfers | Retry-on-deadlock only |
| D-008 | Outbox relay = poller in ledger-api, `FOR UPDATE SKIP LOCKED`, select by `published_at IS NULL` | Simple, no extra infra; avoids high-water-mark skip bug | Debezium CDC (the 100x answer) |
| D-009 | Settlement worker posts ledger entries directly, in one tx with the dedup row and outbox row | Exactly-once effects; DB invariant protects both writers | Worker publishes result, API posts entries |
| D-010 | Fake payment processor is idempotent by `payment_id` | External side effect outside the DB tx must not double-charge on redelivery | Non-idempotent stub |
| D-011 | Money as strings in JSON and TypeScript | JS `Number` is a double | Numeric JSON |
| D-012 | Inventory reservation deferred | Second domain, no new correctness property | Inventory service (would be a 4th service) |
| D-013 | Container Postgres on host port 5433 | Native Postgres 18 occupies 5432 | — |
| D-014 | Kafka offsets committed only after the DB commit | Crash between them → redelivery → dedup, never loss | Auto-commit |
| D-015 | Every number in the README or any external claim comes only from "Measured numbers" below; one canonical set; payload mismatch is always described as 422 (D-004) | Claims must trace to a measured artifact; earlier drafts carried conflicting placeholder figures | Per-document numbers |
| D-016 | `docs/bugs.md` logs every real defect when found (symptom, detection, root cause, fix commit, regression test); "debugged / found / traced" claims come only from it | Problem-solved claims must describe defects that actually happened | Reconstructing bug stories from the intended design |
| D-017 | Negative-control tests live only in test sources (`negativecontrols/` in ledger-api, `tests/negative_controls/` in the worker): check-then-insert idempotency (M2), dedup in a separate tx (M3), optional two-query reconciliation under load (M4). Each asserts the naive variant fails. CLAUDE.md exception to invariants 6–7; main code never imports them; triggers, schema and roles are never bypassed | A proof test never seen red proves nothing; makes "demonstrated" claims honest | Building the naive version in main and "fixing" it |
| D-018 | `test_crash_recovery.py` parameterized as fault point × N iterations; small N in CI, full N in a local soak run; N recorded below | Backs "0 duplicates across N runs" | One run per fault point |
| D-019 | Reconciliation reads balances and entry sums in one snapshot (single statement, or one REPEATABLE READ tx covering per-account and global sums) | Separate reads under READ COMMITTED report transient false drift under write load | Separate queries + alert debounce |
| D-020 | Idempotency key expiry: the conflict path replays until the row is purged; a ledger-api scheduled job batch-deletes rows past `expires_at`; the worker role has no access to `idempotency_keys`. Proof: `IdempotencyKeyExpiryTest` (M4) | Makes the 24h TTL real; ledger-api owns the table | Checking `expires_at` on conflict and overwriting (racy under concurrency) |
| D-021 | M9 host = one AWS EC2 instance running the same Compose file; instance size chosen from `docker stats` on the full local stack; load-test numbers attributed only to the machine they ran on | Closes the deployment-host issue; burstable CPU credits would distort load numbers | ECS Fargate + Terraform (§10 Compose-only, new tech); EKS (§10) |
| D-022 | M6 adds outbox backlog gauges (unpublished count, oldest unpublished age) and one alert per chaos scenario: worker down, Hikari pending connections, outbox age | Chaos #3 is otherwise unobservable; detection time must be measurable (scrape + evaluation interval + `for:`) | Inferring failures from API latency |
| D-023 | Timer around account-lock acquisition in Java and Python, exported to Prometheus | Turns the §9 hot-row hypothesis into a measured 100x answer | Inferring contention from end-to-end latency |
| D-024 | `report.md` records hardware, VUs, req/s and k6 version; README "Reproduce" section uses `docker run grafana/k6`; peer runs are appended with their hardware | Reproduction across machines needs a definition and a cross-platform path | PowerShell-only repro path |
| D-025 | M10 agent is a CLI/eval module that calls only ledger-api GET endpoints, not a service; eval = 30 read questions + 5 write attempts, scored separately; the LLM SDK dependency gets its own row at M10 kickoff | No 4th service; read-only by construction | Text-to-SQL with DB credentials; agent as a service |
| D-026 | One task = one branch = one PR, squash-merged; branch protection on `main` requires the CI jobs (enabled after M0 T5, once the checks exist); T1 bootstrap is the only direct commit to `main` | Every change passes CI before reaching `main`; history stays one commit per task | Direct commits to `main` |
| D-027 | Infra images pinned to exact patch tags: postgres:17.11, apache/kafka:4.3.1, redis:7.4.11; upgrades are explicit PRs | Reproducible stack; latency numbers comparable across runs | Floating major tags (17, 7, latest) |
| D-028 | Kafka log dir on a named volume; `docker compose down -v` is the only full reset and wipes Postgres and Kafka together | Wiping Kafka while Postgres keeps outbox rows marked published silently loses events in dev | Ephemeral Kafka storage |
| D-029 | All published infra ports bind to 127.0.0.1 | No unauthenticated Redis/Postgres/Kafka on the LAN or, at M9, the internet; same Compose file runs on EC2 | Binding 0.0.0.0 and relying on firewalls |

## Measured numbers

Canonical source for every external number (D-015). Blank = not yet measured.

| Metric | Value | Where it's proven |
|---|---|---|
| Concurrency test: requests / transactions created / distinct response bodies | 100 / 1 / 1 (target) | `ConcurrentIdempotentTransferTest` |
| Negative control (check-then-insert): transactions created from 100 identical requests | | M2 negative control |
| Crash recovery: iterations per fault point (CI / soak) / duplicates / lost | | `test_crash_recovery.py` |
| Negative control (separate-tx dedup): duplicates observed | | M3 negative control |
| Transfer p50 / p95 / p99 @ 1x | | `docs/load-tests/report.md` |
| Transfer p50 / p95 / p99 @ 10x | | ″ |
| 1x definition (req/s, VUs) and hardware | | ″ |
| Account lock-wait p99 @ 10x (hottest account) | | Grafana (D-023) |
| Authorize → settled p95 | | Grafana |
| Idempotency cache hit rate under replay load | | Grafana |
| Balance cache: hit rate / stale reads served, invalidate-on-commit vs TTL-only | | M5 benchmark |
| Chaos #1 time to detect / diagnose / recover | | postmortem |
| Chaos #2 ″ | | ″ |
| Chaos #3 ″ | | ″ |
| Ledger drift during all chaos runs | 0 (target) | reconciliation gauge |
| Agent eval: read questions correct / write attempts refused | | M10 scorecard |

## Open issues

- [ ] M10 depends on the Data Den v2 eval harness existing.
- [ ] D-021: pick the EC2 instance size from `docker stats` on the full local stack (by M8); verify current instance specs and pricing.
- [ ] Verify whether `docker kill` bypasses the Compose restart policy; this decides the chaos #1 recovery step.

### Closed

- [x] 2026-10-07 — Reconcile the external claims inventory against the spec → D-015 to D-025.
- [x] 2026-10-07 — Choose deployment host for M9 → D-021.
