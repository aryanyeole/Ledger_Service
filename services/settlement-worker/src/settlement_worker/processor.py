"""The fake payment processor (D-073, D-112): a fake inside the worker, not a service.

decide(amount) is the decision, a pure function of the payment: it declines iff the amount's cents
are 13 (10.13, 0.13, 113.13), which tests use to choose an outcome. The payment id is the
idempotency key, so a retry after a crash or a lost reply always gets the decision already made
(D-010).

call(payment_id, amount, attempt) is the processor as the consumer sees it: async, awaited outside
any DB transaction, under the consumer's deadline. Only its availability depends on the attempt
(the record's run number, D-108), never its answer:
  cents 91                attempts 1-2: ProcessorUnavailable; 3+: answers;
  cents 92                attempts 1-2: logs processor_called, then answers after 2 x timeout_ms
                          (so the consumer's deadline expires first); 3+: answers at once;
  outage (FAKE_PROCESSOR_OUTAGE=1)  every call: ProcessorUnavailable.
A call that never reaches the processor (91, outage) logs no processor_called. Tests reserve cents
91 and 92 for the processor failure proofs (tests/unit/test_reserved_amounts.py). No state.
"""

import asyncio
import enum
import uuid
from decimal import Decimal

import structlog

from settlement_worker import events

UNAVAILABLE_CENTS = 91
SLOW_CENTS = 92
# Attempts 1 and 2 of a cents-91 or cents-92 payment fail; the third answers.
FAILING_ATTEMPTS = 2


class Decision(enum.StrEnum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"


class ProcessorUnavailable(Exception):
    """The processor was not reached, so it never decided (D-112: transient)."""


class ProcessorTimeout(Exception):
    """The consumer's deadline expired before the processor answered; it may have decided and the
    reply was lost, so this is never a decline (D-112: transient)."""


def _cents(amount: Decimal) -> Decimal:
    return (amount * 100) % 100


def decide(amount: Decimal) -> Decision:
    return Decision.DECLINED if _cents(amount) == 13 else Decision.APPROVED


async def call(
    payment_id: uuid.UUID,
    amount: Decimal,
    attempt: int,
    *,
    timeout_ms: int,
    outage: bool,
    correlation_id: str | None = None,
) -> Decision:
    if attempt < 1:
        raise ValueError(f"attempt must be at least 1, was {attempt}")
    cents = _cents(amount)
    if outage:
        raise ProcessorUnavailable("processor outage (FAKE_PROCESSOR_OUTAGE=1)")
    if cents == UNAVAILABLE_CENTS and attempt <= FAILING_ATTEMPTS:
        raise ProcessorUnavailable(f"processor unavailable for payment {payment_id} (attempt {attempt})")
    decision = decide(amount)
    events.emit(
        structlog.get_logger(),
        events.PROCESSOR_CALLED,
        payment_id=str(payment_id),
        decision=decision.value,
        attempt=attempt,
        correlation_id=correlation_id,
    )
    if cents == SLOW_CENTS and attempt <= FAILING_ATTEMPTS:
        await asyncio.sleep(2 * timeout_ms / 1000)
    return decision
