# Ledger Service

[![CI](https://github.com/aryanyeole/Ledger_Service/actions/workflows/ci.yml/badge.svg)](https://github.com/aryanyeole/Ledger_Service/actions/workflows/ci.yml)

A double-entry payments ledger for a small marketplace. Customers pay merchants,
the platform takes a fee, and payment events travel through Kafka to an
asynchronous settlement worker. It has three services (a Spring Boot API, a
Python settlement worker and a React dashboard) over one PostgreSQL ledger. The
goal is correctness that holds under concurrency, crashes and retries.

Money leaves no room for "mostly correct". That puts the hard backend problems
front and center: idempotent requests, exactly-once effects on top of
at-least-once delivery, explicit transaction boundaries, deterministic lock
ordering, reconciliation, and recovering cleanly when a consumer dies mid-batch.

## Local infra

Copy `.env.example` to `.env`, then:

```powershell
docker compose up -d      # Postgres :5433, Kafka :9092, Redis :6379 (127.0.0.1 only)
docker compose ps         # wait for all services to be healthy
docker compose down       # stop; Postgres and Kafka data are kept
docker compose down -v    # full reset: wipes Postgres and Kafka volumes together
```

**Status:** under construction. See [docs/PROGRESS.md](docs/PROGRESS.md).
