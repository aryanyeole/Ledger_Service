"""The worker's log events: the observable contract of the crash and negative-control tests (D-090).

Every event name and its exact keys live here; emit() refuses anything else. Each event carries
correlation_id (CLAUDE.md: logs are JSON and carry correlation_id): the message's, or null for
worker-level events. Lines are written and flushed by logging_setup.LineWriter. M6 may add keys,
but never rename one.
"""

from typing import Any

FAULT_INJECTED = "fault_injected"
PROCESSOR_CALLED = "processor_called"
DUPLICATE_SKIPPED = "duplicate_skipped"
PAYMENT_NOT_AUTHORIZED = "payment_not_authorized"
PAYMENT_SETTLED = "payment_settled"
PAYMENT_RELEASED = "payment_released"
WORKER_STARTED = "worker_started"
PARTITIONS_ASSIGNED = "partitions_assigned"
PROCESSING_FAILED = "processing_failed"
WORKER_STOPPED = "worker_stopped"
CONFIG_INVALID = "config_invalid"
METRICS_LISTENING = "metrics_listening"

EVENT_KEYS: dict[str, frozenset[str]] = {
    FAULT_INJECTED: frozenset({"point", "payment_id", "correlation_id"}),
    PROCESSOR_CALLED: frozenset({"payment_id", "decision", "correlation_id"}),
    DUPLICATE_SKIPPED: frozenset({"event_id", "payment_id", "consumer_group", "correlation_id"}),
    PAYMENT_NOT_AUTHORIZED: frozenset({"event_id", "payment_id", "status", "correlation_id"}),
    PAYMENT_SETTLED: frozenset({"event_id", "payment_id", "transaction_id", "lock_wait_ms", "correlation_id"}),
    PAYMENT_RELEASED: frozenset({"event_id", "payment_id", "transaction_id", "lock_wait_ms", "correlation_id"}),
    WORKER_STARTED: frozenset({"consumer_group", "fault_point", "correlation_id"}),
    PARTITIONS_ASSIGNED: frozenset({"partitions", "correlation_id"}),
    PROCESSING_FAILED: frozenset({"topic", "partition", "offset", "error", "correlation_id"}),
    WORKER_STOPPED: frozenset({"reason", "correlation_id"}),
    CONFIG_INVALID: frozenset({"error", "correlation_id"}),
    METRICS_LISTENING: frozenset({"host", "port", "correlation_id"}),
}

LEVELS = frozenset({"info", "warning", "error"})


def emit(log: Any, name: str, *, level: str = "info", **fields: Any) -> None:
    """Log one contract event; the key set must match EVENT_KEYS exactly."""
    expected = EVENT_KEYS.get(name)
    if expected is None:
        raise ValueError(f"unknown log event: {name}")
    if set(fields) != expected:
        raise ValueError(f"{name} needs keys {sorted(expected)}, got {sorted(fields)}")
    if level not in LEVELS:
        raise ValueError(f"unknown level: {level}")
    getattr(log, level)(name, **fields)
