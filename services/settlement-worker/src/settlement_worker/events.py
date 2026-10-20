"""The worker's log events: the observable contract of the crash and negative-control tests (D-090).

Every event name and its exact keys live here; emit() refuses anything else. Each event carries
correlation_id (CLAUDE.md: logs are JSON and carry correlation_id), null when the message had none.
M6 may add keys, but never rename one.
"""

from typing import Any

FAULT_INJECTED = "fault_injected"
PROCESSOR_CALLED = "processor_called"
DUPLICATE_SKIPPED = "duplicate_skipped"
PAYMENT_NOT_AUTHORIZED = "payment_not_authorized"
PAYMENT_SETTLED = "payment_settled"
PAYMENT_RELEASED = "payment_released"

EVENT_KEYS: dict[str, frozenset[str]] = {
    FAULT_INJECTED: frozenset({"point", "payment_id", "correlation_id"}),
    PROCESSOR_CALLED: frozenset({"payment_id", "decision", "correlation_id"}),
    DUPLICATE_SKIPPED: frozenset({"event_id", "payment_id", "consumer_group", "correlation_id"}),
    PAYMENT_NOT_AUTHORIZED: frozenset({"event_id", "payment_id", "status", "correlation_id"}),
    PAYMENT_SETTLED: frozenset({"event_id", "payment_id", "transaction_id", "correlation_id"}),
    PAYMENT_RELEASED: frozenset({"event_id", "payment_id", "transaction_id", "correlation_id"}),
}


def emit(log: Any, name: str, **fields: Any) -> None:
    """Log one contract event at info level; the key set must match EVENT_KEYS exactly."""
    expected = EVENT_KEYS.get(name)
    if expected is None:
        raise ValueError(f"unknown log event: {name}")
    if set(fields) != expected:
        raise ValueError(f"{name} needs keys {sorted(expected)}, got {sorted(fields)}")
    log.info(name, **fields)
