# Bug Log

> D-016. Every real defect found while building, testing, or running drills — opened when found, closed when fixed.
> This file is the only source for any "debugged / found / traced" claim.
>
> **Not bugs:** designed-in behavior doing its job — a negative control failing as intended (D-017), dedup catching a Kafka redelivery, the trigger rejecting an unbalanced insert in its proof test, a chaos drill behaving as predicted.

## Rules

1. Open the entry **when the defect is observed, before the fix**. Symptom and detection are written from what was seen, not reconstructed later.
2. An entry is **fixed** only when it has a fix commit and a regression test that was shown failing before the fix and passing after.
3. Never rewrite Symptom or How detected after the fact. Append corrections under Notes.
4. Numbers (counts, durations) only if measured, and say how they were measured.
5. Timestamps are America/Phoenix.

## Index

| ID | Found | Milestone | Title | Property / invariant | Status | Fix commit |
|---|---|---|---|---|---|---|
| BUG-001 | 2026-10-13 17:36 | M2 | Never-log-the-key test fails whenever an earlier test in its class fails | none (component: ledger-api tests) | fixed | ccbff46 |
| BUG-002 | 2026-10-13 18:12 | M2 | Shared EXTERNAL fixture adds order-dependent failures to UnbalancedTransactionRejectedTest's failing-first run | none (component: ledger-api tests) | fixed | 1b6f5a4 (PR #16) |
| BUG-003 | 2026-10-14 13:26 | M2 | Concurrency harness produces too little overlap on GitHub runners | P2 proof evidence (component: ledger-api tests) | open | |

---

## Entry template

Copy below the line for each new bug. IDs are sequential: `BUG-001`, `BUG-002`, …

---

### BUG-NNN — <short title>

- **Found:** YYYY-MM-DD HH:MM · M<n> · Task T<k> · branch `m<n>/t<k>-...`
- **Status:** open | fixed | won't fix (<why>)
- **Property / invariant touched:** P1–P4 · CLAUDE.md invariant #1–7 · none
- **Symptom:** What was observed. Exact error text, failing assertion, or wrong value (expected vs actual).
- **How detected:** Test name / CI run link / alert name / dashboard panel / manual check.
- **Reproduction:**
  ```powershell
  # minimal command(s) that reproduce it
  ```
- **Time to diagnose:** First observation → confirmed root cause, from the timestamps above and below.
- **Root cause:** The mechanism, not the symptom. Link the file and line(s).
- **Why it wasn't caught earlier:** Missing test, wrong assumption, gap in the spec, …
- **Fix:** What changed and why that removes the mechanism. Commit `<sha>` · PR #<n>.
- **Regression test:** Name and path. Red before the fix: <commit or CI link>. Green after: <commit or CI link>.
- **Decision Log impact:** New row D-xxx, or none.
- **Summary (one sentence):** Written only after Status = fixed.
- **Notes:**

---

### BUG-001 — Never-log-the-key test fails whenever an earlier test in its class fails

- **Found:** 2026-10-13 17:36 · M2 · Task T1 · branch `m2/t1-api-edge`
- **Status:** fixed
- **Component:** ledger-api tests
- **Property / invariant touched:** none (the D-049 "key is never logged" check could not be trusted)
- **Symptom:** `ApiEdgeWebTest.apiKeyIsNeverLogged` failed even though the application never logged the key. Surefire output:
  ```
  java.lang.AssertionError:
  [captured log output]
  Expecting actual:
    "
    .   ____          _            __ _ _
  ...
  not to contain:
    "test-only-key-alpha"
  	at dev.aryanyeole.ledgerapi.api.ApiEdgeWebTest.apiKeyIsNeverLogged(ApiEdgeWebTest.java:100)
  ```
  The key in the captured output came from MockMvc's request dump for other failing tests in the class, e.g. `Headers = [X-Api-Key:"test-only-key-alpha", X-Correlation-Id:"req-123.abc_X"]` for `/v1/probe/whoami`.
- **How detected:** A local mutation run during T1, used to check that the new tests can fail. Two uncommitted mutations were applied: `MoneyDeserializer` accepting JSON numbers, and `MDC.remove` dropped from `CorrelationIdFilter`. The run was `.\mvnw.cmd -q test -Dtest=MoneyCodecWebTest,ApiEdgeWebTest`. The expected MDC failures (`missingCorrelationIdIsGenerated`, `suppliedCorrelationIdIsEchoedAndInMdcDuringRequest`) appeared, and so did the unexpected `apiKeyIsNeverLogged` failure.
- **Reproduction:**
  ```powershell
  # 1. In ApiEdgeWebTest.apiKeyIsNeverLogged, assert on output.getAll() instead of output.getAll().substring(start).
  # 2. Make another ApiEdgeWebTest test fail, e.g. delete MDC.remove(MDC_KEY) in CorrelationIdFilter.
  cd services\ledger-api; .\mvnw.cmd test "-Dtest=ApiEdgeWebTest"
  # Order-dependent: a failing test has to run before apiKeyIsNeverLogged in the same class.
  ```
- **Time to diagnose:** Not measured. First observation 17:36 (failing run's log timestamps); fix committed 17:39:22 in `3e1b0fb`, so diagnosis and fix took at most about 3 minutes.
- **Root cause:** `OutputCaptureExtension` captures output for the whole test class, so `output.getAll()` inside `apiKeyIsNeverLogged` also held the output of tests that ran before it. `@WebMvcTest` auto-configures MockMvc to print request details, all headers included, for every failing MockMvc test. Any earlier failure therefore put the test key into the captured text. [ApiEdgeWebTest.java:97-105](../services/ledger-api/src/test/java/dev/aryanyeole/ledgerapi/api/ApiEdgeWebTest.java#L97-L105)
- **Why it wasn't caught earlier:** The test passed in a fully green class and when run alone. It only fails when another test in the same class fails first, and no run with a failing test had happened before the mutation check.
- **Fix:** The test records the captured length when it starts and asserts only on `output.getAll().substring(start)`, the output written during its own requests. Commit `3e1b0fb` (branch), squash-merged as `ccbff46` · PR #14.
- **Regression test:** `ApiEdgeWebTest.apiKeyIsNeverLogged` (`services/ledger-api/src/test/java/dev/aryanyeole/ledgerapi/api/ApiEdgeWebTest.java`).
  - **Red before the fix:** the local mutation run above. The pre-fix test was never committed, so there is no commit or CI link.
  - **Green after the fix:** a local rerun with only the `MDC.remove` mutation, where the two MDC tests failed and `apiKeyIsNeverLogged` passed.
  - **Still catches a real leak:** a local key-leak mutation (key concatenated into the 401 detail, which Spring's DEBUG resolver log prints) made it fail.
  - **Final green:** `.\mvnw.cmd verify` 132/0 on `3e1b0fb`, and PR #14 CI.
- **Decision Log impact:** none.
- **Summary (one sentence):** The never-log-the-key test read the whole class's captured output, so MockMvc's header dump from any earlier failing test made it fail; it now checks only the output written during its own requests.
- **Notes:**
  - Logged 2026-10-13 17:51, after the fix, at the owner's request. At the time it was found it was treated as a test-harness slip and described only in the PR #14 body. That ran against Rule 1 (open the entry before the fix).
  - The Symptom text is copied from the surefire report as it was displayed during the T1 session. The report file itself was overwritten by later runs.

---

### BUG-002 — Shared EXTERNAL fixture adds order-dependent failures to UnbalancedTransactionRejectedTest's failing-first run

- **Found:** 2026-10-13 18:12 · M2 · Task T2 (PR #15 review fixes) · branch `m2/t2-accounts-balance`
- **Status:** fixed
- **Component:** ledger-api tests
- **Property / invariant touched:** none. The P1 failing-first evidence (D-038) for `UnbalancedTransactionRejectedTest` is muddied; the trigger itself is unaffected.
- **Symptom:** At `-Dspring.flyway.target=1` the class reports 12 failures, where M1 (PR #10) reported 10. The 10 expected `deferred trigger must reject at commit()` failures are all present. The two extra failures come from a test that should pass at v1:
  ```
  [ERROR] Tests run: 14, Failures: 12, Errors: 0, Skipped: 0
  [ERROR]   UnbalancedTransactionRejectedTest.decimalAmountsBalanceExactly:102->assertBalancesMatchEntries:358 [[ledger_api_app] account 1 balance -400.3000 vs entries -800.2798]
  [ERROR]   UnbalancedTransactionRejectedTest.decimalAmountsBalanceExactly:102->assertBalancesMatchEntries:358 [[settlement_worker] account 1 balance -400.6000 vs entries -800.5798]
  ```
  Account 1 is `system:external`, created by `SystemAccountFixture` because V4 is absent at v1.
- **How detected:** The D-038 failing-first run requested in the PR #15 review, after the M1 suites moved to the get-or-create `SystemAccountFixture`:
  - command: `.\mvnw.cmd test "-Dtest=UnbalancedTransactionRejectedTest" "-Dspring.flyway.target=1"`
  - Flyway line: `Successfully applied 1 migration to schema "public", now at version v1`
  - baseline: compared against the PR #10 failure list.
- **Reproduction:**
  ```powershell
  cd services\ledger-api; .\mvnw.cmd test "-Dtest=UnbalancedTransactionRejectedTest" "-Dspring.flyway.target=1"
  # Expect 10 failures (PR #10); observe 12, the extra two being decimalAmountsBalanceExactly for both roles.
  ```
- **Time to diagnose:** Not measured separately. First observation 18:12, root-cause mechanism identified 18:13 (before this entry), from the failure text and the test bodies.
- **Root cause:** All tests in the class now post against one shared `system:external` account.
  - At v1 there is no balance trigger, so the "must be rejected" cases commit their unbalanced EXTERNAL legs.
  - They are the ones the test expects to be rejected: `unbalancedPairIsRejectedAtCommitNotInsert`, `singleEntryIsRejected`, `lateUnbalancedAppendIsRejected`, `fourthDecimalImbalanceIsRejected`.
  - Those committed legs never go through the balance update, so EXTERNAL's `balance` and `SUM(entries.amount)` drift apart.
  - `decimalAmountsBalanceExactly` runs later and asserts `balance = SUM(entries)` on that shared account, so it fails for a reason unrelated to its own posting.
  - In M1 each test inserted its own EXTERNAL account, so one test's committed garbage could not reach another test's assertions.
  - [UnbalancedTransactionRejectedTest.java:102](../services/ledger-api/src/test/java/dev/aryanyeole/ledgerapi/UnbalancedTransactionRejectedTest.java#L102), [:358](../services/ledger-api/src/test/java/dev/aryanyeole/ledgerapi/UnbalancedTransactionRejectedTest.java#L358)
- **Why it wasn't caught earlier:** At v2 and above the trigger rejects those commits, so nothing pollutes EXTERNAL and the full suite is green. The defect only shows below v2, in failing-first mode. The first PR #15 version (seeded-EXTERNAL lookup) could not run below v4 at all, so no failing-first run had been done with a shared EXTERNAL.
- **Fix:** In `balancedPostingCommits`, `decimalAmountsBalanceExactly` and `lateUnbalancedAppendIsRejected`, the shared EXTERNAL account is checked by deltas, not totals.
  - EXTERNAL's balance and `SUM(entries.amount)` are read before the posting and again after the test's assertions. Each read is one statement on a fresh connection that asserts `current_user` (D-036).
  - Both values must have moved by exactly the EXTERNAL leg (`assertMovedBy`).
  - Fresh CUSTOMER accounts keep the absolute `balance = SUM(entries)` check.
  - Other tests' postings, including commits that leak through below v2, can no longer reach this test's assertions.
  - Commit `1b6f5a4` on `m2/fix-bug-002` · PR #16 (squash-merge SHA to be added at merge).
- **Regression test:** The regression check is a local failing-first run (D-038), **not a CI test**. In CI the schema is always at the latest version, where the defect cannot occur.
  - Command: `.\mvnw.cmd test "-Dtest=UnbalancedTransactionRejectedTest" "-Dspring.flyway.target=1"`
  - Red before the fix: the run under Symptom (12 failures, including the 2 extra `decimalAmountsBalanceExactly` failures, one per role). It ran on the PR #15 tree committed as `82e0e27`, squash-merged as `3ec3ae6`.
  - Green after the fix (2026-10-13 18:25, on `1b6f5a4`): `now at version v1`, `Tests run: 14, Failures: 10, Errors: 0`. These are exactly PR #10's 10 `deferred trigger must reject at commit()` failures, with no `decimalAmountsBalanceExactly` failure. "Green" here means the run matches its M1 baseline; the 10 guard failures are designed-in.
  - Also checked: `EntriesAppendOnlyTest` at `-Dspring.flyway.target=2` still shows PR #11's 8 failures, and full `.\mvnw.cmd verify` is 171/0 at v4.
- **Decision Log impact:** none.
- **Summary (one sentence):** With one shared EXTERNAL account, leftovers from earlier tests broke an absolute balance check during failing-first runs; the shared account is now checked by what each test moved, and the v1 run again shows exactly M1's 10 failures.
- **Notes:**
  - `EntriesAppendOnlyTest` at `-Dspring.flyway.target=2` with the same fixture matches PR #11 exactly (22 run, 8 failures, same list), so it is not affected.
  - The line links under Root cause point at `3ec3ae6`, the version where the defect was found. The fix moved those lines.

---

### BUG-003 — Concurrency harness produces too little overlap on GitHub runners

- **Found:** 2026-10-14 13:26 (CI run time, 20:26 UTC) · M2 · after Task T6 (PR #20, merged as `0a414e7`) · branch `main`, then PR #21's branch `m2/t7-idempotency-semantics-proof`
- **Status:** open
- **Component:** ledger-api tests (`ConcurrentRequestHarness`, `ConcurrentIdempotentTransferTest`, `negativecontrols/CheckThenInsertIdempotencyTest`)
- **Property / invariant touched:** P2's proof evidence (D-067). Production behaviour is not implicated: in every failing run the proof's own assertions on effects held (1 transaction, 1 body, 100 × 201).
- **Symptom:** **Found on CI after the local green**: locally the 20-round soaks passed (proof 20/20; control n = 5–34 across runs). PR #20's own CI run passed. On GitHub-hosted runners (image `ubuntu-24.04`, version 20261012.334.1) the harness then failed twice:
  - CI run **38631908435** (push to `main`, `0a414e7`, after #20). Only the proof failed; the control passed with n = 15.
    ```
    CONCURRENCY control round=1 sentBeforeFirstResponse=78 peakInFlight=94 transactions=15 distinctBodies=16 statuses={201=100}
    CONCURRENCY real round=1 sentBeforeFirstResponse=42 peakInFlight=52 transactions=1 distinctBodies=1 statuses={201=100}
    [ERROR]   ConcurrentIdempotentTransferTest.lambda$hundredIdenticalTransfersCreateOneTransaction$0:62->round:124 [round 1: requests sent before the first response (was 42)]
    [ERROR] Tests run: 287, Failures: 1, Errors: 0, Skipped: 0
    ```
  - CI run **38633176987** (PR #21). Both failed:
    ```
    CONCURRENCY control round=1 sentBeforeFirstResponse=53 peakInFlight=66 transactions=1 distinctBodies=2 statuses={201=100}
    CONCURRENCY real round=1 sentBeforeFirstResponse=48 peakInFlight=74 transactions=1 distinctBodies=1 statuses={201=100}
    [ERROR]   ConcurrentIdempotentTransferTest.lambda$hundredIdenticalTransfersCreateOneTransaction$0:62->round:124 [round 1: requests sent before the first response (was 48)]
    [ERROR]   CheckThenInsertIdempotencyTest.lambda$naiveIdempotencyLetsDuplicatesThrough$0:61->round:94 [round 1: harness produced no overlap (new TRANSFER transactions = 1)]
    [ERROR] Tests run: 297, Failures: 2, Errors: 0, Skipped: 0
    ```
- **How detected:** the GitHub Actions `ledger-api` job (`./mvnw -B verify`) on the push to `main` after PR #20 merged, and on PR #21's checks. Reported by the owner; the lines above were taken from `gh run view <id> --log`.
- **Reproduction:**
  ```powershell
  # Not yet reproduced locally; it is timing-dependent on GitHub-hosted runners.
  gh run view 38631908435 --log
  gh run view 38633176987 --log
  ```
- **Time to diagnose:**
- **Root cause:** Partly established; the fix is not settled.
  - **The client-side metric doesn't measure arrival.** It counts threads that called `HttpClient.send`, not requests that reached the server. In run 38633176987 the control had n = 1 while the client counted 53 "sent" before the first response.
  - **A cold measured round funnels its connects through `HttpClient`'s single selector thread** (JDK 21 `PlainHttpConnection.connectAsync` → `ConnectEvent`).
  - **On the 4-processor GitHub runner, overlap stays low even after a warm-up.** Phase 1 (PR #22, commit `2167fa2`, run 38637012761 attempts 1–5) measured server-side "arrived before the first completed":
    - proof 7, 8, 17, 23, 12;
    - control 13, 13, 19, 16, 14 (control n = 7–8 every time).

    Locally the same metric is 30–100. The warm-up opened only 53–100 connections, because requests overlapped too little to need more.
  - **Remaining hypothesis, not yet verified:** the client side on 4 processors, i.e. 100 threads reaching `send()` and the client's selector-thread completion path. That's what keeps arrivals spread over 91–162 ms.
- **Why it wasn't caught earlier:**
- **Fix:**
- **Regression test:**
- **Decision Log impact:**
- **Summary (one sentence):**
- **Notes:**
  - Logged 2026-10-14 13:43, when reported, before any fix.
  - **Timing-dependent, confirmed:** CI run 38633176987 was re-run on the same commit (`ac414a3`) and attempt 2 passed (20:51 UTC). It passed narrowly:
    ```
    CONCURRENCY control round=1 sentBeforeFirstResponse=100 peakInFlight=100 transactions=2 distinctBodies=3 statuses={201=100}
    CONCURRENCY real round=1 sentBeforeFirstResponse=50 peakInFlight=66 transactions=1 distinctBodies=1 statuses={201=100}
    ```
  - Phase 1's floor rule `max(10, ⌊0.6 × CI min⌋)` gives 10 for CI min 7, which is above two of the five calibration values, so no floor was committed (2026-10-14).
  - **PR #20's own CI runs were also marginal:** 38630949271 had proof peak 50 (then the floor) and control peak 36, n = 7. 38631761721 had proof 85/97 and control 67/74, n = 6.
  - The CI logs carry no CPU count for the runner, so the hardware behind these numbers is unknown.
