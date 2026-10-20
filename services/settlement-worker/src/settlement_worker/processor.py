"""The fake payment processor (D-073): a pure function inside the worker, not a service.

Its idempotency key is the payment id: it always returns the same decision for the same payment,
so calling it again after a crash can't change the outcome (D-010). It declines iff the amount's
cents are 13 (10.13, 0.13, 113.13), which tests use to choose an outcome. No state, no I/O beyond
the log line. M4 adds failure modes.
"""

import enum
import uuid
from decimal import Decimal

import structlog

from settlement_worker import events


class Decision(enum.StrEnum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"


def decide(payment_id: uuid.UUID, amount: Decimal, *, correlation_id: str | None = None) -> Decision:
    decision = Decision.DECLINED if (amount * 100) % 100 == 13 else Decision.APPROVED
    events.emit(
        structlog.get_logger(),
        events.PROCESSOR_CALLED,
        payment_id=str(payment_id),
        decision=decision.value,
        correlation_id=correlation_id,
    )
    return decision
