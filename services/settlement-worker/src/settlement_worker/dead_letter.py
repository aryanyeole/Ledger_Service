"""The dead-letter record (D-111). A pure function: no I/O, no clock (failed_at is passed in).

Key and value are the original bytes, unchanged, so a replay is byte-identical. aiokafka refuses a
record with neither key nor value, so that one case gets an empty value and the header
dlq.original_value_null: true (a replay sends a null value again). Headers are the original ones,
in order, followed by:
  dlq.original_topic, dlq.original_partition, dlq.original_offset,
  dlq.original_timestamp   the record's timestamp, epoch milliseconds as a decimal string,
  dlq.consumer_group,
  dlq.error_class          InvalidEvent | UnknownPayment | EventMismatch | Unclassified,
  dlq.error                "Type: message" on one line (CR/LF collapsed to spaces), at most 1024
                           UTF-8 bytes, cut on a character boundary,
  dlq.attempts             how many times the record was processed before its dead letter landed,
                           re-runs forced by an unavailable DLQ included,
  dlq.failed_at            D-097's format (UTC, six fractional digits, Z),
  dlq.event_id             only if the envelope parsed.
Every header value is UTF-8.
"""

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

ERROR_MAX_BYTES = 1024
_LINE_BREAKS = re.compile(r"[\r\n]+")


@dataclass(frozen=True)
class DeadLetter:
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]


def one_line(error: str, max_bytes: int = ERROR_MAX_BYTES) -> str:
    """CR/LF collapsed to spaces, then cut to max_bytes of UTF-8 without splitting a character."""
    flat = _LINE_BREAKS.sub(" ", error)
    encoded = flat.encode("utf-8")
    if len(encoded) <= max_bytes:
        return flat
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def failed_at_text(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def build(
    record,
    *,
    consumer_group: str,
    error_class: str,
    error: str,
    attempts: int,
    event_id: uuid.UUID | None,
    failed_at: datetime,
) -> DeadLetter:
    value = record.value
    original_headers = list(record.headers or ())
    extra: list[tuple[str, bytes]] = []
    if value is None and record.key is None:
        value = b""
        extra.append(("dlq.original_value_null", b"true"))
    headers = original_headers + [
        ("dlq.original_topic", record.topic.encode("utf-8")),
        ("dlq.original_partition", str(record.partition).encode("utf-8")),
        ("dlq.original_offset", str(record.offset).encode("utf-8")),
        ("dlq.original_timestamp", str(record.timestamp).encode("utf-8")),
        ("dlq.consumer_group", consumer_group.encode("utf-8")),
        ("dlq.error_class", error_class.encode("utf-8")),
        ("dlq.error", one_line(error).encode("utf-8")),
        ("dlq.attempts", str(attempts).encode("utf-8")),
        ("dlq.failed_at", failed_at_text(failed_at).encode("utf-8")),
    ]
    if event_id is not None:
        headers.append(("dlq.event_id", str(event_id).encode("utf-8")))
    return DeadLetter(key=record.key, value=value, headers=headers + extra)
