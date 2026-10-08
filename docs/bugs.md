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
| | | | | | | |

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
