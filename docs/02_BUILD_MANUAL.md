# Ledger Service — Build Manual

> HOW the project gets built: environment, repo layout, and the working loop between the claude.ai **Ledger Service** project and Claude Code in VS Code.
> Environment: Windows · VS Code · PowerShell · Java 21 (Temurin) · Python 3.13 (`python`) · Node 22 · Docker Desktop · native Postgres 18 already on :5432.

---

## 1. Two tools, two jobs

| Where | Role | Use it for |
|---|---|---|
| **claude.ai → Ledger Service project** | Architect, reviewer, tutor | Milestone kickoff plans, design questions, reviewing diffs Claude Code produced, debugging from logs, writing docs (design doc, postmortem, README), interview drills |
| **Claude Code in VS Code** (repo root, reads `CLAUDE.md`) | Implementer | Writing code, migrations, tests, Compose, CI; running tests |

Rule: **plans and explanations come from the project chat; keystrokes come from Claude Code.** Paste Claude Code's plan or diff into the project chat whenever a load-bearing piece is touched (trigger, idempotency, lock ordering, outbox, consumer dedup, reconciliation).

---

## 2. One-time setup of the claude.ai project

1. **Custom instructions:** paste the contents of `00_PROJECT_INSTRUCTIONS.md`.
2. **Upload to project knowledge:**
   - `01_LEDGER_SPEC.md`
   - `02_BUILD_MANUAL.md` (this file)
   - `03_INTERVIEW_GATE.md`
   - `PROGRESS.md` (seed copy; re-upload at every milestone close)
   - `04_RESUME_ALIGNMENT.md` (produced by the Resume Optimization project — see §8)
3. **Do not upload** the old Project Specs PDF — it covers WMP and Data Den too, and `01_LEDGER_SPEC.md` supersedes its Ledger section. Two versions of the truth in knowledge = Claude picks the wrong one.

---

## 3. One-time machine setup (PowerShell)

```powershell
# Repo
mkdir C:\Users\Aryan\Projects\ledger-service
cd C:\Users\Aryan\Projects\ledger-service
git init
git branch -M main

# Docker Desktop: Settings → Resources → give it at least 6 GB RAM (Kafka + Postgres + Grafana + Java + Python)

# Sanity checks
java -version        # 21.x
python --version     # 3.13.x
node --version       # v22.x
docker compose version
```

Spring Boot app (`services/ledger-api`): generate at **start.spring.io** — Maven, Java 21, default stable Boot version, group `dev.aryanyeole`, artifact `ledger-api`. Dependencies: Spring Web, Validation, JDBC API, PostgreSQL Driver, Flyway Migration, Spring for Apache Kafka, Spring Boot Actuator, Testcontainers. (Add springdoc, Micrometer Prometheus, Redis, logstash-logback-encoder at their milestones.) Unzip into `services/ledger-api`. Always use the wrapper: `.\mvnw.cmd`, never a global `mvn`.

Python worker (`services/settlement-worker`):
```powershell
cd services\settlement-worker
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"     # after pyproject.toml exists
```

Dashboard (`services/dashboard`, at M7):
```powershell
cd services
npm create vite@latest dashboard -- --template react-ts
```

Line endings — commit this `.gitattributes` on day 1 or shell scripts inside Linux containers break on CRLF:
```
* text=auto
*.sh text eol=lf
*.sql text eol=lf
mvnw text eol=lf
```

---

## 4. Repo layout (monorepo, D-001)

```
ledger-service/
├─ README.md                    # the recruiter-facing front door
├─ CLAUDE.md                    # Claude Code operating rules
├─ docker-compose.yml           # full local stack
├─ .env.example                 # never commit .env
├─ .gitattributes
├─ .github/workflows/ci.yml
├─ docs/
│  ├─ PROGRESS.md               # milestone status, decision log, measured numbers
│  ├─ DESIGN.md                 # system-design-interview doc (M9)
│  ├─ design/                   # focused notes (cache-invalidation.md, idempotency.md, ...)
│  ├─ load-tests/               # k6 reports
│  ├─ postmortems/
│  └─ screenshots/
├─ services/
│  ├─ ledger-api/               # Spring Boot; OWNS Flyway migrations
│  ├─ settlement-worker/        # Python; never runs migrations
│  └─ dashboard/                # React + TS
├─ infra/
│  ├─ postgres/init/            # DB roles (ledger_api_owner, settlement_worker)
│  ├─ prometheus/ (prometheus.yml, alerts.yml)
│  ├─ grafana/ (provisioning/, dashboards/ledger.json)
│  └─ k6/
└─ scripts/                     # PowerShell: chaos-*.ps1, seed.ps1, reset.ps1
```

### Local ports
| Thing | Host port |
|---|---|
| Postgres (container) | **5433** (5432 is taken by native Postgres 18) |
| Kafka | 9092 (host listener) — container-to-container uses the internal listener |
| Redis | 6379 |
| ledger-api | 8080 |
| worker metrics | 8000 |
| Prometheus | 9090 |
| Grafana | 3000 |
| dashboard (Vite dev) | 5173 |

Kafka in Compose needs **two listeners**: one advertised as `kafka:<internal port>` for containers, one as `localhost:9092` for tools/tests on Windows. Getting this wrong is the #1 Compose Kafka bug — if a client connects then hangs, check advertised listeners first.

---

## 5. The milestone loop (every milestone, no exceptions)

### Step 1 — Kickoff (new chat in the claude.ai project, named `M<n> — <title>`)
```
Kickoff M<n>: <title>.
Current state: <paste the "Status" table and last 5 Decision Log rows from PROGRESS.md>
Give me:
1. The concept briefing for this milestone — what I must understand before code exists (max 1 page).
2. An ordered task list sized for Claude Code sessions (each task = one commit).
3. The exact proof artifact(s) and their assertions.
4. The 3 ways this milestone usually goes wrong.
Do not write implementation code yet.
```

### Step 2 — Predict before you run
Before the proof test runs for the first time, write down (in the chat) what you expect to happen and why. For the concurrency test: how many requests block, on what, and what each thread receives. Being wrong here is the most valuable thing that happens all milestone.

### Step 3 — Implement with Claude Code
In VS Code, per task:
```
Task <k> of M<n>: <task text from kickoff>.
Follow CLAUDE.md. Show me the plan first, then implement. Run the relevant tests.
Stop after this task — one commit.
```
For load-bearing pieces, paste Claude Code's plan into the project chat before approving it: `Review this plan against 01_LEDGER_SPEC.md §<x>. What's wrong or missing?`

### Step 4 — Teach-back (gate)
```
Quiz me on M<n> using 03_INTERVIEW_GATE.md. One question at a time. Don't give the answer until I've tried.
Grade each answer: complete / missing <x> / wrong. At the end list what I need to re-study.
```
Milestone is not done until every question is "complete."

### Step 5 — Close out
```
Close out M<n>. Produce the updated PROGRESS.md in full: status table, new Decision Log rows (decision, why, alternative rejected), measured numbers filled in, open issues. Then a 3-line commit-message summary.
```
Commit `docs/PROGRESS.md` to the repo, then **replace** `PROGRESS.md` in project knowledge with the new version.

---

## 6. Engineering conventions

- **Money:** `BigDecimal` (Java), `Decimal` (Python), strings in JSON/TS. Never `double`, `float`, `Number`. Rounding is always explicit (`HALF_EVEN`, scale 2 at the API edge).
- **Migrations:** Flyway in `ledger-api` only. Never edit an applied migration — add a new one.
- **Transactions:** every multi-statement write has an explicit boundary; lock accounts in ascending id order; no network calls (Kafka, HTTP) inside a DB transaction except the outbox relay's documented pattern.
- **Tests:** integration tests hit real Postgres/Kafka via Testcontainers. No mocking the database for anything touching P1–P3.
- **Commits:** one task = one commit, conventional style (`feat(api): idempotent transfers`). Proof tests get their own commit so they're easy to link from the README.
- **Logs:** JSON, always include `correlation_id`; never log full API keys.
- **Scope:** anything in spec §10 Anti-goals → stop and flag. New dependency → Decision Log entry first.

---

## 7. CI (GitHub Actions, set up at M0, grows each milestone)

Jobs on every push/PR:
1. `ledger-api`: `./mvnw verify` (Testcontainers runs on `ubuntu-latest` — Docker is available there).
2. `settlement-worker`: `pip install -e ".[dev]"` + `pytest`.
3. `dashboard` (from M7): `npm ci && npm run build` (typecheck included).

By M3, CI must run `ConcurrentIdempotentTransferTest` and `test_crash_recovery.py`. Badge goes in the README from day 1.

---

## 8. Bringing in the Resume Optimization context

Chat search is scoped per project, so the Resume project's knowledge has to be exported. Run this **in the Resume Optimization project**, save the output as `04_RESUME_ALIGNMENT.md`, and upload it here.

```
I'm starting the real build of the Ledger Service flagship in a separate Claude project. I need everything this project knows about it, exported as one markdown file named 04_RESUME_ALIGNMENT.md. Work only from the resumes, JDs and chats in this project. Do not invent anything; if something isn't here, say "not found."

Context — the project as it will actually be built: a payments ledger with 3 services (Spring Boot API, Python settlement worker, React/TS dashboard), Kafka, PostgreSQL, Redis, Prometheus/Grafana, Docker Compose, GitHub Actions. Core properties: DB-enforced double-entry invariant, Idempotency-Key with request fingerprinting, transactional outbox + consumer dedup (exactly-once effects), NUMERIC/BigDecimal money, reconciliation job with drift metric, 100-thread concurrency test, k6 load test at 1x/10x, chaos tests + postmortem. Out of scope: Kubernetes, multi-region, real payment provider, multi-currency, inventory domain, more than 3 services.

Sections:
1. Bullets verbatim — every Ledger Service bullet (or the payment/ledger/order-processing project bullets it replaces) on any resume version here, quoted exactly, labeled by resume category.
2. Claims inventory — table: claim (each number, technology, pattern, metric as its own row) | source bullet | artifact that would prove it.
3. JD signal — requirements repeated across the backend, fintech/payments, platform, full-stack and AI JDs here that this project can legitimately demonstrate. Table: requirement | # of JDs | example companies | exact JD phrasing.
4. Out-of-scope claims — any claim on an already-submitted resume that needs something in the out-of-scope list. Flag each.
5. Phrasing bank — JD keywords worth using in the README and final bullets, only for things in scope.
Tables over prose. No advice section.
```

First chat in the Ledger project after uploading it:
```
Reconcile 04_RESUME_ALIGNMENT.md against 01_LEDGER_SPEC.md.
1. Claims the spec already proves (claim → proof artifact).
2. Claims the spec doesn't cover: for each, either (a) the smallest in-scope addition that makes it true, with the milestone it belongs in, or (b) a rewrite of the bullet that matches what will exist.
3. JD requirements with high counts that the spec misses but could cover within the anti-goals.
Output proposed Decision Log rows. Don't change scope beyond §10 without flagging it.
```

---

## 9. Command cheat sheet (PowerShell)

```powershell
docker compose up -d                       # whole stack
docker compose up -d postgres kafka redis  # infra only, run services from IDE
docker compose logs -f settlement-worker
docker compose down -v                     # wipe volumes (fresh DB)

cd services\ledger-api; .\mvnw.cmd spring-boot:run
cd services\ledger-api; .\mvnw.cmd test -Dtest=ConcurrentIdempotentTransferTest

cd services\settlement-worker; .\.venv\Scripts\Activate.ps1; pytest -k crash

# k6 (PowerShell has no `<` redirect; pipe instead; reach the host API via host.docker.internal)
Get-Content infra\k6\transfers.js -Raw | docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run -

psql "postgresql://postgres:postgres@localhost:5433/ledger"   # native psql from PG18 install
```
