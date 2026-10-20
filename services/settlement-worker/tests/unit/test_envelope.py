"""D-102: what the worker accepts from the topic, and every way it refuses."""

import json
import uuid
from decimal import Decimal

import pytest

from settlement_worker import envelope

EVENT_ID = "0f1e2d3c-4b5a-4987-8a6b-5c4d3e2f1a0b"
PAYMENT_ID = "5b01a9e5-32ce-4c82-90b0-ac8c5143460c"


def _valid(**changes):
    doc = {
        "event_id": EVENT_ID,
        "event_type": "payment.authorized",
        "version": 1,
        "occurred_at": "2026-10-19T06:22:05.120000Z",
        "correlation_id": "abc-123",
        "data": {
            "fee": "2.90",
            "amount": "100.00",
            "payment_id": PAYMENT_ID,
            "customer_account_id": 12,
            "merchant_account_id": 13,
        },
    }
    data = changes.pop("data", None)
    doc.update(changes)
    if data is not None:
        doc["data"] = {**doc["data"], **data}
    return doc


def _raw(doc) -> bytes:
    return json.dumps(doc, separators=(",", ":")).encode()


def test_valid_authorized_event():
    event = envelope.parse(_raw(_valid()))
    assert isinstance(event, envelope.AuthorizedEvent)
    assert event.event_id == uuid.UUID(EVENT_ID)
    assert event.payment_id == uuid.UUID(PAYMENT_ID)
    assert (event.customer_account_id, event.merchant_account_id) == (12, 13)
    assert (event.amount, event.fee) == (Decimal("100.00"), Decimal("2.90"))
    assert event.correlation_id == "abc-123"


def test_zero_fee_and_null_correlation_id_are_valid():
    event = envelope.parse(_raw(_valid(correlation_id=None, data={"fee": "0.00"})))
    assert event.fee == Decimal("0.00")
    assert event.correlation_id is None


@pytest.mark.parametrize("event_type", ["payment.settled", "payment.failed"])
def test_own_results_are_recognised(event_type):
    event = envelope.parse(_raw(_valid(event_type=event_type, data={"transaction_id": 9})))
    assert isinstance(event, envelope.OwnResultEvent)
    assert event.event_type == event_type


def _without(key):
    doc = _valid()
    del doc[key]
    return doc


def _without_data(key):
    doc = _valid()
    del doc["data"][key]
    return doc


INVALID_DOCS = {
    "version 2": _valid(version=2),
    "version true": _valid(version=True),
    "version string": _valid(version="1"),
    "unknown event_type": _valid(event_type="payment.refunded"),
    "event_id not canonical": _valid(event_id=EVENT_ID.replace("-", "")),
    "event_id short form": _valid(event_id="1-1-1-1-1"),
    "payment_id not a UUID": _valid(data={"payment_id": "p-1"}),
    "customer id 0": _valid(data={"customer_account_id": 0}),
    "customer id negative": _valid(data={"customer_account_id": -1}),
    "customer id boolean": _valid(data={"customer_account_id": True}),
    "merchant id string": _valid(data={"merchant_account_id": "13"}),
    "amount number": _valid(data={"amount": 100}),
    "amount 3 dp": _valid(data={"amount": "10.001"}),
    "amount zero": _valid(data={"amount": "0"}),
    "amount leading zero": _valid(data={"amount": "01.00"}),
    "amount empty": _valid(data={"amount": ""}),
    "fee negative": _valid(data={"fee": "-0.01"}),
    "occurred_at 5 digits": _valid(occurred_at="2026-10-19T06:22:05.12000Z"),
    "occurred_at offset": _valid(occurred_at="2026-10-19T06:22:05.120000+00:00"),
    "occurred_at no Z": _valid(occurred_at="2026-10-19T06:22:05.120000"),
    "occurred_at impossible date": _valid(occurred_at="2026-02-30T06:22:05.120000Z"),
    "correlation_id number": _valid(correlation_id=5),
    "missing envelope key": _without("correlation_id"),
    "extra envelope key": _valid(trace="x"),
    "missing data key": _without_data("fee"),
    "extra data key": _valid(data={"status": "AUTHORIZED"}),
    "data not an object": _valid(data=None) | {"data": []},
    "own result data not an object": _valid(event_type="payment.settled") | {"data": "x"},
}


@pytest.mark.parametrize("doc", INVALID_DOCS.values(), ids=INVALID_DOCS.keys())
def test_invalid_documents(doc):
    with pytest.raises(envelope.InvalidEvent):
        envelope.parse(_raw(doc))


INVALID_RAW = {
    "version 1.0": _raw(_valid()).replace(b'"version":1', b'"version":1.0'),
    "id with exponent": _raw(_valid()).replace(b'"customer_account_id":12', b'"customer_account_id":1e2'),
    "NaN": _raw(_valid()).replace(b'"customer_account_id":12', b'"customer_account_id":NaN'),
    "Infinity": _raw(_valid()).replace(b'"version":1', b'"version":Infinity'),
    "not UTF-8": b'{"event_id":"\xff"}',
    "not JSON": b"{",
    "not an object": b"[]",
}


@pytest.mark.parametrize("raw", INVALID_RAW.values(), ids=INVALID_RAW.keys())
def test_invalid_bytes(raw):
    with pytest.raises(envelope.InvalidEvent):
        envelope.parse(raw)
