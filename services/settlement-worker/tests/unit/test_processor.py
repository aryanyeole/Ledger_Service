"""D-073, D-112: the fake processor. The decision depends only on the cents (13 declines), the same
way at every attempt; only availability depends on the attempt (91 unavailable, 92 slow, on attempts
1-2; the outage on every call)."""

import asyncio
import time
import uuid
from decimal import Decimal

import pytest
from structlog.testing import capture_logs

from settlement_worker import events
from settlement_worker.processor import Decision, ProcessorUnavailable, call, decide

PAYMENT_ID = uuid.UUID("5b01a9e5-32ce-4c82-90b0-ac8c5143460c")


def _call(amount: str, attempt: int, *, timeout_ms: int = 2000, outage: bool = False, correlation_id=None):
    return asyncio.run(
        call(PAYMENT_ID, Decimal(amount), attempt, timeout_ms=timeout_ms, outage=outage, correlation_id=correlation_id)
    )


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
        ("10.91", Decision.APPROVED),
        ("10.92", Decision.APPROVED),
    ],
)
def test_decision_depends_only_on_the_cents(amount, decision):
    assert decide(Decimal(amount)) is decision
    assert decide(Decimal(amount)) is decision, "same input, same decision"
    # The answer, whenever call() gives one, is that decision at every attempt (D-010, D-112).
    for attempt in (3, 4, 50):
        assert _call(amount, attempt, timeout_ms=1) is decision


def test_each_call_is_logged():
    with capture_logs() as logs:
        _call("10.13", 1, correlation_id="c-1")
    assert logs == [
        {
            "event": events.PROCESSOR_CALLED,
            "log_level": "info",
            "payment_id": str(PAYMENT_ID),
            "decision": "DECLINED",
            "attempt": 1,
            "correlation_id": "c-1",
        }
    ]


@pytest.mark.parametrize("amount", ["10.91", "0.91", "113.91"])
def test_cents_91_is_unavailable_on_attempts_1_and_2_then_answers(amount):
    for attempt in (1, 2):
        with capture_logs() as logs, pytest.raises(ProcessorUnavailable, match=f"attempt {attempt}"):
            _call(amount, attempt)
        assert logs == [], "a call that never reached the processor logs no processor_called"
    with capture_logs() as logs:
        assert _call(amount, 3) is Decision.APPROVED
    assert [(e["event"], e["attempt"]) for e in logs] == [("processor_called", 3)]


def test_cents_92_logs_its_decision_then_answers_after_twice_the_timeout_on_attempts_1_and_2():
    timeout_ms = 40
    for attempt in (1, 2):
        started = time.monotonic_ns()
        with capture_logs() as logs:
            assert _call("10.92", attempt, timeout_ms=timeout_ms) is Decision.APPROVED
        elapsed_ms = (time.monotonic_ns() - started) // 1_000_000
        assert elapsed_ms >= 2 * timeout_ms, elapsed_ms
        assert [(e["event"], e["decision"], e["attempt"]) for e in logs] == [("processor_called", "APPROVED", attempt)]


def test_cents_92_logs_before_it_sleeps():
    """processor_called is out before the slow reply: cancelled mid-sleep (as the consumer's deadline
    does), the call has still logged its decision."""

    async def cancelled_mid_sleep():
        task = asyncio.create_task(call(PAYMENT_ID, Decimal("10.92"), 1, timeout_ms=10_000, outage=False))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    with capture_logs() as logs:
        asyncio.run(cancelled_mid_sleep())
    assert [(e["event"], e["decision"]) for e in logs] == [("processor_called", "APPROVED")]


def test_cents_92_answers_at_once_from_attempt_3():
    started = time.monotonic_ns()
    assert _call("10.92", 3, timeout_ms=10_000) is Decision.APPROVED
    assert (time.monotonic_ns() - started) // 1_000_000 < 1000


@pytest.mark.parametrize("amount", ["10.00", "10.13", "10.91", "10.92"])
def test_an_outage_makes_every_call_unavailable(amount):
    for attempt in (1, 2, 3, 10):
        with capture_logs() as logs, pytest.raises(ProcessorUnavailable, match="FAKE_PROCESSOR_OUTAGE"):
            _call(amount, attempt, outage=True)
        assert logs == []


def test_attempt_must_be_positive():
    with pytest.raises(ValueError, match="attempt must be at least 1"):
        _call("10.00", 0)
