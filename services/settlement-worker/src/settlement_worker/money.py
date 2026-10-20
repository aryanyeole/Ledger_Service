"""Money at the worker's edges (invariant 3, D-047, D-056, D-093).

Amounts are Decimal, never float. They arrive as API strings in events (the D-047 pattern) or
as NUMERIC(19,4) values from asyncpg, and leave as strings at scale 2. Nothing is ever rounded:
a value with non-zero digits past 2 dp is an error.
"""

import re
from decimal import Decimal

_API_AMOUNT = re.compile(r"(0|[1-9]\d*)(\.\d{1,2})?")
_CENT = Decimal("0.01")


class InvalidAmount(ValueError):
    pass


def parse_api_amount(text: object, *, allow_zero: bool = False) -> Decimal:
    """An API amount string (D-047) as a Decimal: > 0, or >= 0 when allow_zero (a fee)."""
    if not isinstance(text, str) or not _API_AMOUNT.fullmatch(text):
        raise InvalidAmount(f"not an API amount string: {text!r}")
    value = Decimal(text)
    if value < 0 or (value == 0 and not allow_zero):
        raise InvalidAmount(f"amount must be {'>= 0' if allow_zero else '> 0'}: {text!r}")
    return value


def to_api_string(value: Decimal) -> str:
    """A Decimal as an API string at scale 2, e.g. Decimal('100.0000') -> '100.00'. Never rounds."""
    if not isinstance(value, Decimal):
        raise InvalidAmount(f"not a Decimal: {value!r}")
    quantized = value.quantize(_CENT)
    if quantized != value:
        raise InvalidAmount(f"non-zero digits past 2 dp: {value}")
    return format(quantized, "f")
