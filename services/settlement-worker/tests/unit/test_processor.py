"""D-073: the fake processor declines iff the cents are 13, the same way every time."""

import uuid
from decimal import Decimal

import pytest
from structlog.testing import capture_logs

from settlement_worker import events
from settlement_worker.processor import Decision, decide

PAYMENT_ID = uuid.UUID("5b01a9e5-32ce-4c82-90b0-ac8c5143460c")


@pytest.mark.parametrize(
    "amount, decision",
    [
        ("10.13", Decision.DECLINED),
        ("0.13", Decision.DECLINED),
        ("113.13", Decision.DECLINED),
        ("10.12", Decision.APPROVED),
        ("13.00", Decision.APPROVED),
        ("1.31", Decision.APPROVED),
        ("100.0000", Decision.APPROVED),
        ("10.1300", Decision.DECLINED),
    ],
)
def test_decision_depends_only_on_the_cents(amount, decision):
    assert decide(PAYMENT_ID, Decimal(amount)) is decision
    assert decide(PAYMENT_ID, Decimal(amount)) is decision, "same input, same decision"


def test_each_call_is_logged():
    with capture_logs() as logs:
        decide(PAYMENT_ID, Decimal("10.13"), correlation_id="c-1")
    assert logs == [
        {
            "event": events.PROCESSOR_CALLED,
            "log_level": "info",
            "payment_id": str(PAYMENT_ID),
            "decision": "DECLINED",
            "correlation_id": "c-1",
        }
    ]
