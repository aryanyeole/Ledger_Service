# Ledger Service — Interview Gate

> A milestone is done when its questions can be answered **cold, out loud, in under a minute each**. The "must mention" column is the grading rubric, not a script — answers should come from having built it.

---

## M1 — Schema and the invariant

| Question | A complete answer must mention |
|---|---|
| Why enforce the double-entry invariant in the database instead of the service? | Two writers in two languages; a constraint can't drift; app check is a promise, constraint is a guarantee; also protects against manual SQL/bugs |
| Why is the trigger `DEFERRABLE INITIALLY DEFERRED`? | Entries are inserted one row at a time; mid-transaction the sum is temporarily non-zero; checked once at COMMIT |
| A row-level `CHECK` can't do this. Why? | CHECK sees one row; invariant spans rows |
| What can still bypass the trigger? | Superuser (`session_replication_role = replica`, `DISABLE TRIGGER`); owner (drop/disable triggers, regrant itself) → runtime roles own nothing (D-033); TRUNCATE now blocked by a statement trigger (D-041); search_path/temp-table shadowing closed by qualified names + pinned search_path + no TEMP (D-040); a balanced pair appended to an old transaction passes the sum check — enforced by code/grants, not the trigger |
| Why are entries append-only? How do you fix a wrong posting? | Audit trail; corrections are new reversing transactions, never edits |
| Why `NUMERIC` and not `double`? Show what breaks. | Binary floating point can't represent 0.1 exactly; `0.1 + 0.2 ≠ 0.3`; rounding drift compounds across millions of entries; explicit scale and rounding mode |
| Why three DB roles and not two? | An owner can disable/drop triggers and grant itself TRUNCATE; Flyway-only owner, runtime roles own nothing; proven by DbRolePrivilegesTest |
| If the database enforces the invariant, why reconcile? | Trigger guarantees no commit introduces imbalance, not that the ledger is balanced; drift committed around it (superuser) can be silently offset by a later matching commit; materialized balances aren't checked by the trigger at all |
| Why a statement-level trigger for append-only? | Row triggers miss zero-row statements and never see TRUNCATE; one trigger, one firing per statement; 23001 distinguishes it from grants (42501) and balance (23514) — DELETE on transactions failing with 23503 at v2 shows why exact SQLState matters |

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
| Why does a lock timeout sometimes return 409 and sometimes 503? | 55P03 on the key insert = a duplicate still in flight (409, retry with the same key); on the account locks = contention (503 + Retry-After); translated where raised, neither stored, both roll back (D-050) |
| What guarantees an idempotency key never commits without its response? | Key, effect and response in one transaction; stored outcomes roll back to a savepoint taken after the key claim (D-064); a deferred constraint trigger re-reads the row at COMMIT and raises 23502 if the response is missing (D-059) |
| How do you know the concurrency test isn't passing by luck? | The check-then-insert negative control on the same harness lets duplicates through every round; server-side overlap floor; red mutation goes red; CI is a regression gate, the overlap figure comes from the local soak with hardware stated (D-067, BUG-003) |
| What exactly is "byte-identical"? | Response bodies, per key: the owner stores JSONB and returns response_body::text, replays return the same text; headers differ (correlation id); different keys may format the same content differently (D-044) |
| If the database rejects overdrafts, why check funds in Java? | Without it the no_overdraft CHECK fires as 23514 → an unstored 500, so a retry gets another 500; the Java check produces a stored, idempotent 422; the constraint is the backstop |
| What's the known gap in your keyset pagination? | created_at is the posting transaction's start time, so a late commit behind a passed cursor is never seen by that traversal; ordering by id has the same gap; pinned by a characterization test; M7's stream must handle it (D-053) |

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
| Why must authorize lock accounts before inserting the payment? | FK takes KEY SHARE on customer and merchant; cycle with a settle holding ESCROW; two authorizes deadlock upgrading KEY SHARE; locking only {customer, ESCROW} cycles with a transfer when merchant id < customer id; one ascending FOR UPDATE before any account-locking statement (D-075) |
| Why does the relay mark rows only after every ack? What does a crash after the acks do? | At-least-once seam; byte-identical republish; consumer dedup; marking before acks loses records (MockProducer case) |
| Kafka was paused. Did the outbox lose anything, and how did you prove it? | Nothing marked during the outage; a paused broker's kernel still accepts requests, so timeouts later appear as duplicates; pause can't show loss, the MockProducer case shows the loss mechanism |
| Two relays run at once. What ordering do you keep? | FOR UPDATE SKIP LOCKED gives disjoint batches; per-key order isn't kept; causal order of first copies is; stale redeliveries are absorbed by per-event dedup (D-100) |
| Why ON CONFLICT (consumer_group, event_id) and not a bare ON CONFLICT? | A target needs SELECT on its columns; untargeted DO NOTHING treats any unique violation as "processed" → silent loss; targeted fails loudly (D-078) |
| How do you know dedup, not the status guard, caught the redelivery in the crash test? | Separate log events per layer; the mutation that ignores the dedup row count passes the ledger checks and fails only the layer assertion |
| What did your negative control show? | Effect then mark; 20 of 20 double-applied; every posting balanced so the P1 trigger stayed silent; only ESCROW = Σ AUTHORIZED saw it → the M4 reconciliation check |
| Why didn't per-leg locking in leg order deadlock, and why sort anyway? | ESCROW has the lowest id and everyone takes it first — that's luck from seeding order, not a protocol; it also held locks far longer (8.4 s) |
| Why clock_timestamp() for published_at? | now() is the relay transaction's start, before its sends and acks |
| Why does the worker fail on an event for an unknown payment instead of skipping it? | Outbox row and payment commit together, so it can't happen normally; committing the dedup row would hide corruption forever |

## M4 — Settlement and reconciliation

| Question | A complete answer must mention |
|---|---|
| What does reconciliation check and why does a real ledger need it? | Per account: materialized balance vs `SUM(entries)`; global `SUM(entries) = 0`; ESCROW's entry sum = Σ AUTHORIZED payment amounts. The trigger guarantees each commit balances, not that balances match entries or that a balanced posting should exist; catches bugs in either writer, manual edits, partial deploys |
| Why does reconciliation have to read one snapshot? What did your proof and control show? | Two reads under READ COMMITTED see different commits → false drift; one statement = one snapshot (D-019, D-114). Soak: 0 drifted of 2,000 runs, a write in flight at every snapshot; split-snapshot mutation 170/200 drifted; two-query control drifts exactly {A:+x, B:−x} at a barrier, REPEATABLE READ clean; 2,685/10,000 under load (local, i5-1240P) |
| How do you know the snapshot proof isn't passing because nothing was happening? | In-flight tripwire: a transaction with id > the run's max_transaction_id and created_at < as_of started before the snapshot and wasn't in it; floor 11 of 20 calibrated on CI (D-106, D-134); load-disabled run goes red |
| What does the ESCROW = Σ AUTHORIZED check catch that the others can't? | A balanced posting that shouldn't exist (double settle, M3's negative control) and a status change without a posting (drift case D); every per-account and global check stays 0 |
| Drift alert fires at 3 AM. How do you find the cause? | Which accounts and which check; the window last_clean_as_of → first_drift_as_of; transactions in that window by kind and correlation_id → logs; last_entry_id; a balance written without an entry leaves no entry, only the window; which service/version (M6) |
| How do you split a $100 payment with a 2.9% fee without losing a cent? | Fee rounded explicitly (HALF_EVEN, once, in Java at authorize, D-074); merchant = amount − fee, never computed independently |
| What's a poison message and what do you do with it? | A message that fails because of what it contains, every time. Classify first (D-107): classified poison → DLQ on the first failure; unclassified → bounded retries (5) → DLQ; transient → retry indefinitely with backoff, never DLQ; fatal → stop the worker. DLQ keeps original bytes, no dedup row, replayable (D-111, D-125); never block the partition forever |
| Why not dead-letter a DB outage? | It isn't the message's fault: an outage would drain the whole topic into the DLQ and force a mass replay; a revoked grant would do the same. Transient failures retry in place and show up as lag |
| Why does a retry re-run the processor call, and why is that safe? | Retry = redelivery to self (D-108): the same path a Kafka redelivery takes; the processor is idempotent by payment id (D-010), so it returns the decision already made; dedup and the status guard cover the effect |
| The processor timed out. Approve, decline or retry? | Retry. A timeout is "unknown": the processor may have decided and the reply was lost. Mapping it to decline would release escrow for money that was captured. Unavailable = never reached the processor; timeout = may have decided (D-112) |
| Why pause the partition instead of sleeping in the handler? | Sleeping stops polling → max.poll.interval eviction and a rebalance (measured: rejoined after ~2.1 s with a 2 s limit), and blocks every partition; pausing keeps heartbeating and lets other partitions move |
| Why must a paused partition's already-fetched records be dropped? | A committed offset is a position, not a per-message ack; committing a later record's offset acknowledges the failed one, so it's silently skipped (the mutation committed offset 2 past P at 0) |
| The DB connection died during COMMIT. What happened to the settlement? | Unknown outcome; the connection is terminated, the record retried after reconnecting (role re-checked); if the commit landed, dedup skips the effect; if not, it applies once (D-109) |
| A payment.authorized was dead-lettered. Where's the customer's money and how do you recover it? | Still in ESCROW, payment AUTHORIZED; reconciliation stays clean; replay re-produces the original key/value/headers (no dlq.*) — no dedup row exists, so it settles once; a second replay is skipped (D-125) |
| An idempotency key expired 3 hours ago. The same request arrives. What happens? | It replays until the purge deletes it; after the purge the key is new (a second effect). The DB refuses deleting a live key (23514 idempotency_keys_purge, D-119); a request blocks on an in-flight purge and becomes owner or replays depending on commit/rollback |
| When can you delete a processed_messages row? | Only once its message can no longer be delivered: older than the longest retention of any topic the group reads plus any offset-reset look-back (offsets.retention.minutes) plus a margin; otherwise an offset reset readmits it (D-121) |
| A test was deleted and the suite stayed green. How would you catch it? | A deleted test can't fail and counts rose anyway (PR #47); diff test sources against main with rename detection off, per PR; audit across PRs per commit, since a range diff hides add-then-delete (D-130) |
| Why does ledger-api set a login timeout in a driver config file? | Flyway uses a plain driver DataSource that takes no Hikari settings, and pgjdbc ignores DriverManager's login timeout, so a DB that accepts TCP but never answers hung startup forever (BUG-012, reproduced with a paused Postgres and a thread dump); the classpath file reaches every connection and survives a replaced URL (D-132) |

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
| Concurrency test: requests / transactions created / distinct response bodies | 100 / 1 / 1 (local soak 20/20, i5-1240P; CI 15/15, 4-processor runners) | `ConcurrentIdempotentTransferTest` |
| Crash recovery: iterations / duplicates / lost | 200 (100 per fault point, local soak, i5-1240P) / 0 / 0; CI 5 per point | `test_crash_recovery.py` |
| Negative control (separate-tx dedup): double-applied | 20 of 20 (local, i5-1240P); 3 of 3 on CI | `tests/negative_controls/test_separate_tx_dedup.py` |
| Cross-operation lock order: rounds / deadlocks | 20 rounds × 70 requests / 0 deadlocks (local, i5-1240P) | `test_cross_operation_lock_order.py` |
| Server-overlap floors per test (CI minimum over observations) | Transfer 3 (6 over 65); Opposite 2 (3 over 53); Payment 6 (11 over 35); PayLock 2 (4 over 35); 4-processor CI runners | D-106 |
| Reconciliation under load: runs / false drift / write in flight at the snapshot | 2,000 / 0 / 2,000 (local soak, i5-1240P); 20 / 0 / 19 CI | `test_reconciliation_snapshot.py` |
| Split-snapshot mutation: runs drifted | 170 / 200 (local) | ″ (D-051 mutation) |
| Two-query control under load: runs drifted | 2,685 / 10,000 (local); barrier → exactly {A:+7.25, B:−7.25} every run | `test_two_query_reconciliation.py` |
| In-flight tripwire floor | 11 of 20 (CI min 19 over 14 observations) | D-134 |
| Settlement under ESCROW contention (test-load, not M8) | ~22–25/s idle vs 3–7/s at ~30 authorizes/s, one worker | PR #61 |
| Transfer p50 / p95 / p99 @ 1x | | `docs/load-tests/report.md` |
| Transfer p50 / p95 / p99 @ 10x | | ″ |
| 1x definition (req/s) | | ″ |
| Authorize → settled p95 | | Grafana |
| Idempotency cache hit rate under replay load | | Grafana |
| Chaos #1 time to detect / diagnose / recover | | postmortem |
| Chaos #2 ″ | | ″ |
| Chaos #3 ″ | | ″ |
| Ledger drift during all chaos runs | 0 (target) | reconciliation gauge |
