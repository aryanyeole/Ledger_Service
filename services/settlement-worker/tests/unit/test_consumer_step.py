"""The per-record step rejects a bad record before touching the DB or the broker (D-081)."""

import asyncio
import json
from dataclasses import dataclass

import pytest

from settlement_worker import consumer
from settlement_worker.config import Config
from settlement_worker.envelope import InvalidEvent

PAYMENT = "5b01a9e5-32ce-4c82-90b0-ac8c5143460c"
OTHER = "0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


class Untouchable:
    """Any use of the DB connection or the consumer fails the test."""

    def __init__(self, name):
        object.__setattr__(self, "_name", name)

    def __getattr__(self, attribute):
        raise AssertionError(f"{self._name}.{attribute} was used")


def _value() -> bytes:
    return json.dumps(
        {
            "event_id": OTHER,
            "event_type": "payment.authorized",
            "version": 1,
            "occurred_at": "2026-10-19T06:22:05.120000Z",
            "correlation_id": None,
            "data": {
                "payment_id": PAYMENT,
                "customer_account_id": 12,
                "merchant_account_id": 13,
                "amount": "100.00",
                "fee": "2.90",
            },
        }
    ).encode()


@pytest.mark.parametrize("key", [OTHER.encode(), None, b"not-a-uuid", b"\xff"], ids=["other id", "none", "text", "bytes"])
def test_key_must_equal_the_payment_id(key):
    record = Record("ledger.payments.v1", 0, 7, key, _value())
    config = Config.from_env({"SETTLEMENT_WORKER_PASSWORD": "x"})
    with pytest.raises(InvalidEvent):
        asyncio.run(
            consumer.process_record(record, conn=Untouchable("conn"), consumer=Untouchable("consumer"), config=config)
        )


def test_matching_key_is_accepted_case_insensitively():
    event = consumer.classify(Record("t", 0, 0, PAYMENT.upper().encode(), _value()))
    assert str(event.payment_id) == PAYMENT
