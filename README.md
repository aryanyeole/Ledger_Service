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

**Status:** under construction. See [docs/PROGRESS.md](docs/PROGRESS.md).
