"""Kafka record values built from outbox rows exactly as the relay builds them (D-097, D-103).

Until the relay joins the worker tests (T8), tests feed the worker these bytes: keys in the
envelope's order, occurred_at = created_at in UTC with six fractional digits and Z, data = the
stored payload in its stored key order, compact separators, UTF-8 (as Jackson writes it).
"""

import json
import uuid
from datetime import timezone

import asyncpg


async def authorized_event(conn: asyncpg.Connection, payment_id: uuid.UUID) -> bytes:
    row = await conn.fetchrow(
        "SELECT event_id, event_type, payload::text AS payload, headers->>'correlation_id' AS correlation_id, "
        "created_at FROM outbox WHERE message_key = $1 AND event_type = 'payment.authorized'",
        str(payment_id),
    )
    assert row is not None, f"no payment.authorized outbox row for {payment_id}"
    return envelope_bytes(
        event_id=str(row["event_id"]),
        event_type=row["event_type"],
        occurred_at=row["created_at"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        correlation_id=row["correlation_id"],
        data=json.loads(row["payload"]),
    )


def envelope_bytes(*, event_id, event_type, occurred_at, correlation_id, data, version=1) -> bytes:
    envelope = {
        "event_id": event_id,
        "event_type": event_type,
        "version": version,
        "occurred_at": occurred_at,
        "correlation_id": correlation_id,
        "data": data,
    }
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def with_changes(raw: bytes, *, data: dict | None = None, **envelope) -> bytes:
    """A copy of an event with some envelope fields or data fields replaced."""
    doc = json.loads(raw)
    doc.update(envelope)
    if data:
        doc["data"] = {**doc["data"], **data}
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
