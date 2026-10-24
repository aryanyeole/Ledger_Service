"""D-111: the dead-letter record, exactly (dead_letter.build is pure)."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from settlement_worker.dead_letter import ERROR_MAX_BYTES, build, one_line

FAILED_AT = datetime(2026, 10, 24, 6, 5, 4, 3_002, tzinfo=timezone.utc)
EVENT = uuid.UUID("0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b")


@dataclass
class Record:
    topic: str = "ledger.payments.v1"
    partition: int = 2
    offset: int = 41
    timestamp: int = 1_792_000_000_123
    key: bytes | None = b"5b01a9e5-32ce-4c82-90b0-ac8c5143460c"
    value: bytes | None = b'{"event_id": "x"}'
    headers: tuple = ()


def _build(record, **overrides):
    args = {
        "consumer_group": "settlement-worker",
        "error_class": "EventMismatch",
        "error": "EventMismatch: amount differs",
        "attempts": 1,
        "event_id": EVENT,
        "failed_at": FAILED_AT,
    }
    args.update(overrides)
    return build(record, **args)


def test_exact_record():
    record = Record(headers=(("correlation_id", b"corr-1"), ("trace", b"\x00\x01")))
    letter = _build(record)
    assert letter.key == record.key
    assert letter.value == record.value
    assert letter.headers == [
        ("correlation_id", b"corr-1"),
        ("trace", b"\x00\x01"),
        ("dlq.original_topic", b"ledger.payments.v1"),
        ("dlq.original_partition", b"2"),
        ("dlq.original_offset", b"41"),
        ("dlq.original_timestamp", b"1792000000123"),
        ("dlq.consumer_group", b"settlement-worker"),
        ("dlq.error_class", b"EventMismatch"),
        ("dlq.error", b"EventMismatch: amount differs"),
        ("dlq.attempts", b"1"),
        ("dlq.failed_at", b"2026-10-24T06:05:04.003002Z"),
        ("dlq.event_id", b"0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"),
    ]


def test_no_event_id_when_the_envelope_did_not_parse():
    letter = _build(Record(), error_class="InvalidEvent", event_id=None)
    assert "dlq.event_id" not in dict(letter.headers)


def test_a_non_utf8_value_and_a_missing_key_are_kept_as_they_are():
    record = Record(key=None, value=b"\xff\xfe not json \x00")
    letter = _build(record, error_class="InvalidEvent", event_id=None)
    assert letter.key is None
    assert letter.value == b"\xff\xfe not json \x00"
    assert "dlq.original_value_null" not in dict(letter.headers)


def test_a_null_value_with_a_key_stays_null():
    letter = _build(Record(value=None), error_class="InvalidEvent", event_id=None)
    assert letter.value is None
    assert "dlq.original_value_null" not in dict(letter.headers)


def test_neither_key_nor_value_gets_an_empty_value_and_a_marker():
    letter = _build(Record(key=None, value=None), error_class="InvalidEvent", event_id=None)
    assert letter.key is None
    assert letter.value == b""
    assert dict(letter.headers)["dlq.original_value_null"] == b"true"


def test_error_is_one_line():
    letter = _build(Record(), error="Unclassified: line one\r\nline two\nline three")
    assert dict(letter.headers)["dlq.error"] == b"Unclassified: line one line two line three"


def test_error_is_cut_to_1024_bytes_on_a_character_boundary():
    # 3-byte characters: 341 of them are 1023 bytes, a 342nd would make 1026.
    error = "€" * 400
    cut = one_line(error)
    assert len(cut.encode("utf-8")) <= ERROR_MAX_BYTES
    assert cut == "€" * 341
    letter = _build(Record(), error=error)
    assert dict(letter.headers)["dlq.error"] == ("€" * 341).encode("utf-8")
    assert one_line("short") == "short"
