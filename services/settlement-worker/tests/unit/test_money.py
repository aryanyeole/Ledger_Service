"""Invariant 3, D-047, D-056: API amount strings in, scale-2 strings out, nothing ever rounded."""

from decimal import Decimal

import pytest

from settlement_worker.money import InvalidAmount, parse_api_amount, to_api_string


@pytest.mark.parametrize("text, value", [("100", "100"), ("100.5", "100.5"), ("0.01", "0.01"), ("10.13", "10.13")])
def test_parse(text, value):
    assert parse_api_amount(text) == Decimal(value)


def test_zero_only_for_fees():
    assert parse_api_amount("0.00", allow_zero=True) == Decimal("0")
    with pytest.raises(InvalidAmount):
        parse_api_amount("0.00")


@pytest.mark.parametrize("text", ["10.001", "01.00", "-1.00", "+1.00", "1e2", "", " 1.00", "1.", ".5", 100, None])
def test_parse_rejects(text):
    with pytest.raises(InvalidAmount):
        parse_api_amount(text, allow_zero=True)


@pytest.mark.parametrize(
    "value, text",
    [("100.0000", "100.00"), ("0E-4", "0.00"), ("97.1", "97.10"), ("-10.1300", "-10.13"), ("5", "5.00")],
)
def test_render(value, text):
    assert to_api_string(Decimal(value)) == text


def test_render_never_rounds():
    with pytest.raises(InvalidAmount):
        to_api_string(Decimal("1.0050"))
