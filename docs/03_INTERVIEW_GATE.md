# Ledger Service — Interview Gate

> A milestone is done when its questions can be answered **cold, out loud, in under a minute each**. The "must mention" column is the grading rubric, not a script — answers should come from having built it.

---

## M1 — Schema and the invariant

| Question | A complete answer must mention |
|---|---|
| Why enforce the double-entry invariant in the database instead of the service? | Two writers in two languages; a constraint can't drift; app check is a promise, constraint is a guarantee; also protects against manual SQL/bugs |
| Why is the trigger `DEFERRABLE INITIALLY DEFERRED`? | Entries are inserted one row at a time; mid-transaction the sum is temporarily non-zero; checked once at COMMIT |
| A row-level `CHECK` can't do this. Why? | CHECK sees one row; invariant spans rows |
| What can still bypass the trigger? | TRUNCATE, superuser/`session_replication_role`, disabling triggers → roles without those privileges |
| Why are entries append-only? How do you fix a wrong posting? | Audit trail; corrections are new reversing transactions, never edits |
| Why `NUMERIC` and not `double`? Show what breaks. | Binary floating point can't represent 0.1 exactly; `0.1 + 0.2 ≠ 0.3`; rounding drift compounds across millions of entries; explicit scale and rounding mode |

## M2 — API, idempotency, concurrency

| Question | A complete answer must mention |
|---|---|
| Walk me through `POST /transfers` from request to commit. | Key insert → conflict handling → lock accounts in id order → funds check → transaction + entries + balances → store response → commit → deferred trigger fires |
| 100 identical requests arrive at once. What happens, step by step, at the database? | First INSERT wins the unique index; others block on the uncommitted index entry; owner commits; waiters see conflict, read committed row, replay stored response; one transaction total |
| Same key, different body — why 422 and not 409? | IETF Idempotency-Key draft: 422 = key reused with different payload (client bug); 409 = original still in progress (retry later) |
| Why fingerprint the request at all? | Without it a client bug that reuses a key silently returns the wrong result for a different operation |
| How do you canonicalize the body for the hash? | Hash the parsed/normalized DTO (sorted keys, normalized decimals), not raw bytes — whitespace or key order would change the hash |
| Why sort account locks by id? What happens if you don't? | A→B and B→A concurrently take locks in opposite order → deadlock; Postgres detects and kills one; ordering removes the cycle |
| Insufficient funds — is that response idempotent too? | Yes; stored against the key; replay returns the same 422 even if funds arrive later |
| Why keyset pagination for `/transactions`? When is offset fine? | Offset scans and discards N rows (degrades with depth, unstable under inserts); keyset uses the index; offset is fine for small tables / jump-to-page UIs |

## M3 — Outbox and exactly-once effects

| Question | A complete answer must mention |
|---|---|
| Why not publish to Kafka inside the request? | Dual-write problem: DB commit and Kafka send can't be atomic; either can succeed alone |
| How does the outbox fix it? | Event row written in the same DB transaction as the state change; separate relay publishes; event exists iff state change committed |
| What does "exactly-once" mean in your system? | Exactly-once **effects**, not delivery; delivery is at-least-once; dedup row in the same DB transaction as the effect |
| Worker crashes after the DB commit but before the offset commit. What happens? | Redelivery → `processed_messages` insert conflicts → skip → commit offset; no double effect |
| Worker crashes after calling the processor but before the DB commit? | Redelivery → processor called again with same idempotency key (payment id) → same decision → effect applied once |
| Why poll by `published_at IS NULL` instead of tracking the last published id? | Identity ids can commit out of order; a high-water mark skips late commits |
| How is per-payment ordering preserved? | Kafka key = payment id → same partition → ordered within partition; consumer processes a partition serially |
| What's a poison message and what do you do with it? | Message that always fails; bounded retries with backoff → DLQ topic → alert; never block the partition forever |

## M4 — Settlement and reconciliation

| Question | A complete answer must mention |
|---|---|
| What does reconciliation check and why does a real ledger need it? | Materialized balance vs `SUM(entries)` per account, global sum = 0; catches bugs in any writer, manual edits, partial deploys |
| Drift alert fires at 3 AM. How do you find the cause? | Which account(s), since when (entry ids/timestamps), which transaction kinds, which service/version wrote them, correlation id → logs |
| How do you split a $100 payment with a 2.9% fee without losing a cent? | Fee rounded explicitly; merchant = amount − fee, never computed independently |

## M5 — Redis

| Question | A complete answer must mention |
|---|---|
| Redis loses all data. What breaks? | Nothing correctness-wise — Postgres unique key is the guarantee; only latency rises |
| How do you invalidate the balance cache, and when can a client read a stale balance? | Invalidate after DB commit; window between commit and invalidate (or failed invalidate) → TTL bounds staleness; why that's acceptable for reads but never used for funds checks |
| Why a distributed lock for reconciliation, and what if the lock holder dies? | Prevent concurrent runs; lock TTL; fencing/idempotent job so a double run is harmless anyway |

## M6 — Observability

| Question | A complete answer must mention |
|---|---|
| What are your three SLOs and their error budgets? | Name each SLI, target, window, and what burns the budget |
| What's consumer lag and what does rising lag tell you? | Latest offset − committed offset; worker slower than producers, or stuck/crashed |
| How do you trace one payment across all three services? | Correlation id: HTTP header → MDC → outbox headers → Kafka headers → Python contextvars → logs |
| Why histograms instead of averages? | Averages hide tail latency; p95/p99 are what users feel |

## M8 — Load and chaos

| Question | A complete answer must mention |
|---|---|
| Your p50/p95/p99 at 1x and 10x? What got worse first? | The measured numbers, from memory |
| Tell me about an incident you've handled. | The postmortem: timeline, detection, diagnosis time, root cause, fix, follow-ups |
| What happens when the DB connection pool exhausts — at the client, the pool, the DB? | Client latency spike then timeouts/5xx; pool queues borrowers until `connectionTimeout` → exception; DB sees fewer active connections than requests |
| Kafka goes down for 10 minutes. What do users see? | Transfers keep working (sync); payments authorize (outbox fills); settlement pauses; backlog drains on recovery, no loss |

## M9 — Design and scale

| Question | A complete answer must mention |
|---|---|
| Whiteboard the write path in 5 minutes. | — |
| Name three things that break at 100x. | Measured, not guessed — e.g. hot rows (escrow/fee accounts), outbox poller throughput, single Postgres write node — and the fix for each |
| What would you do differently? | Honest, specific |

---

## Numbers to know from memory (fill in as they're measured)

| Metric | Value | Where it's proven |
|---|---|---|
| Concurrency test: requests / transactions created / distinct response bodies | 100 / 1 / 1 | `ConcurrentIdempotentTransferTest` |
| Transfer p50 / p95 / p99 @ 1x | | `docs/load-tests/report.md` |
| Transfer p50 / p95 / p99 @ 10x | | ″ |
| 1x definition (req/s) | | ″ |
| Authorize → settled p95 | | Grafana |
| Idempotency cache hit rate under replay load | | Grafana |
| Chaos #1 time to detect / diagnose / recover | | postmortem |
| Chaos #2 ″ | | ″ |
| Chaos #3 ″ | | ″ |
| Ledger drift during all chaos runs | 0 (target) | reconciliation gauge |
