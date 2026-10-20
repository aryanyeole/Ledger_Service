"""Parsing the Kafka record value (D-097 envelope, D-102 rules).

Strict at the edge, like the API (D-047, D-065, D-088): exact key sets (a new field means a new
version), canonical UUIDs, JSON integers that aren't booleans, money as D-047 strings, and no
JSON number with a fraction or exponent, NaN or Infinity anywhere, so a float can never enter.
payment.authorized v1 is the only event the worker acts on; payment.settled and payment.failed
are its own results, recognised (envelope checked) and skipped. Anything else is InvalidEvent.
The Kafka record key isn't visible here: the consumer must check it equals data.payment_id (T8).
"""

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from settlement_worker.money import InvalidAmount, parse_api_amount

AUTHORIZED = "payment.authorized"
SETTLED = "payment.settled"
FAILED = "payment.failed"
VERSION = 1

ENVELOPE_KEYS = frozenset({"event_id", "event_type", "version", "occurred_at", "correlation_id", "data"})
AUTHORIZED_DATA_KEYS = frozenset({"payment_id", "customer_account_id", "merchant_account_id", "amount", "fee"})

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_OCCURRED_AT = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z")


class InvalidEvent(ValueError):
    pass


@dataclass(frozen=True)
class AuthorizedEvent:
    event_id: uuid.UUID
    occurred_at: datetime
    correlation_id: str | None
    payment_id: uuid.UUID
    customer_account_id: int
    merchant_account_id: int
    amount: Decimal
    fee: Decimal


@dataclass(frozen=True)
class OwnResultEvent:
    event_id: uuid.UUID
    event_type: str
    correlation_id: str | None


def _reject_number(text: str):
    raise InvalidEvent(f"JSON number with a fraction or exponent: {text}")


def _reject_constant(text: str):
    raise InvalidEvent(f"JSON constant not allowed: {text}")


def _uuid(value: object, field: str) -> uuid.UUID:
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise InvalidEvent(f"{field} is not a canonical UUID: {value!r}")
    return uuid.UUID(value)


def _positive_id(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEvent(f"{field} must be a JSON integer > 0: {value!r}")
    return value


def _exact_keys(obj: object, keys: frozenset[str], what: str) -> dict:
    if not isinstance(obj, dict):
        raise InvalidEvent(f"{what} is not a JSON object")
    if set(obj) != keys:
        raise InvalidEvent(f"{what} keys must be exactly {sorted(keys)}, got {sorted(obj)}")
    return obj


def parse(raw: bytes) -> AuthorizedEvent | OwnResultEvent:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as ex:
        raise InvalidEvent("record value is not UTF-8") from ex
    try:
        doc = json.loads(text, parse_float=_reject_number, parse_constant=_reject_constant)
    except json.JSONDecodeError as ex:
        raise InvalidEvent(f"record value is not JSON: {ex}") from ex
    envelope = _exact_keys(doc, ENVELOPE_KEYS, "envelope")

    event_id = _uuid(envelope["event_id"], "event_id")
    event_type = envelope["event_type"]
    version = envelope["version"]
    if isinstance(version, bool) or version != VERSION or not isinstance(version, int):
        raise InvalidEvent(f"unsupported version: {version!r}")
    occurred_at_text = envelope["occurred_at"]
    if not isinstance(occurred_at_text, str) or not _OCCURRED_AT.fullmatch(occurred_at_text):
        raise InvalidEvent(f"occurred_at is not in the D-097 format: {occurred_at_text!r}")
    try:
        occurred_at = datetime.fromisoformat(occurred_at_text)
    except ValueError as ex:
        raise InvalidEvent(f"occurred_at is not a valid instant: {occurred_at_text!r}") from ex
    correlation_id = envelope["correlation_id"]
    if correlation_id is not None and not isinstance(correlation_id, str):
        raise InvalidEvent(f"correlation_id must be a string or null: {correlation_id!r}")

    if event_type in (SETTLED, FAILED):
        if not isinstance(envelope["data"], dict):
            raise InvalidEvent("data is not a JSON object")
        return OwnResultEvent(event_id=event_id, event_type=event_type, correlation_id=correlation_id)
    if event_type != AUTHORIZED:
        raise InvalidEvent(f"unknown event_type: {event_type!r}")

    data = _exact_keys(envelope["data"], AUTHORIZED_DATA_KEYS, "data")
    try:
        amount = parse_api_amount(data["amount"])
        fee = parse_api_amount(data["fee"], allow_zero=True)
    except InvalidAmount as ex:
        raise InvalidEvent(str(ex)) from ex
    return AuthorizedEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        correlation_id=correlation_id,
        payment_id=_uuid(data["payment_id"], "data.payment_id"),
        customer_account_id=_positive_id(data["customer_account_id"], "data.customer_account_id"),
        merchant_account_id=_positive_id(data["merchant_account_id"], "data.merchant_account_id"),
        amount=amount,
        fee=fee,
    )
