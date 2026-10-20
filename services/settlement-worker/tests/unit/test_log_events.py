"""D-090: the log events the crash and negative-control tests parse, and their exact keys."""

import io
import json

import pytest
import structlog

from settlement_worker import events


def test_contract():
    assert {name: sorted(keys) for name, keys in events.EVENT_KEYS.items()} == {
        "fault_injected": ["correlation_id", "payment_id", "point"],
        "processor_called": ["correlation_id", "decision", "payment_id"],
        "duplicate_skipped": ["consumer_group", "correlation_id", "event_id", "payment_id"],
        "payment_not_authorized": ["correlation_id", "event_id", "payment_id", "status"],
        "payment_settled": ["correlation_id", "event_id", "payment_id", "transaction_id"],
        "payment_released": ["correlation_id", "event_id", "payment_id", "transaction_id"],
    }


def test_unknown_event_is_refused():
    with pytest.raises(ValueError, match="unknown log event"):
        events.emit(structlog.get_logger(), "payment_done", payment_id="p", correlation_id=None)


@pytest.mark.parametrize(
    "fields",
    [
        {"payment_id": "p", "decision": "APPROVED"},
        {"payment_id": "p", "decision": "APPROVED", "correlation_id": None, "extra": 1},
    ],
    ids=["missing correlation_id", "extra key"],
)
def test_wrong_keys_are_refused(fields):
    with pytest.raises(ValueError, match="needs keys"):
        events.emit(structlog.get_logger(), events.PROCESSOR_CALLED, **fields)


def test_emitted_as_one_json_line():
    out = io.StringIO()
    structlog.configure(
        processors=[structlog.processors.add_log_level, structlog.processors.JSONRenderer()],
        logger_factory=structlog.PrintLoggerFactory(file=out),
    )
    try:
        events.emit(
            structlog.get_logger(),
            events.DUPLICATE_SKIPPED,
            event_id="e",
            payment_id="p",
            consumer_group="settlement-worker",
            correlation_id=None,
        )
    finally:
        structlog.reset_defaults()
    lines = out.getvalue().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        "event": "duplicate_skipped",
        "level": "info",
        "event_id": "e",
        "payment_id": "p",
        "consumer_group": "settlement-worker",
        "correlation_id": None,
    }
