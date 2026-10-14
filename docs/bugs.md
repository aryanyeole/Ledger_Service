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
