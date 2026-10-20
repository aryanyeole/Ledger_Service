"""D-074: the settle and release postings, from the payment row, zero legs omitted."""

from decimal import Decimal

from settlement_worker.processor import Decision
from settlement_worker.settlement import PaymentRow, legs

ESCROW, FEES, CUSTOMER, MERCHANT = 2, 3, 40, 41


def _row(amount, fee):
    return PaymentRow(CUSTOMER, MERCHANT, Decimal(amount), Decimal(fee))


def test_settle():
    assert legs(Decision.APPROVED, _row("100.0000", "2.9000"), ESCROW, FEES) == [
        (ESCROW, Decimal("-100.0000")),
        (MERCHANT, Decimal("97.1000")),
        (FEES, Decimal("2.9000")),
    ]


def test_settle_with_zero_fee_has_no_fee_leg():
    assert legs(Decision.APPROVED, _row("0.0100", "0.0000"), ESCROW, FEES) == [
        (ESCROW, Decimal("-0.0100")),
        (MERCHANT, Decimal("0.0100")),
    ]


def test_settle_with_fee_equal_to_amount_has_no_merchant_leg():
    assert legs(Decision.APPROVED, _row("0.0100", "0.0100"), ESCROW, FEES) == [
        (ESCROW, Decimal("-0.0100")),
        (FEES, Decimal("0.0100")),
    ]


def test_release():
    assert legs(Decision.DECLINED, _row("10.1300", "0.2900"), ESCROW, FEES) == [
        (ESCROW, Decimal("-10.1300")),
        (CUSTOMER, Decimal("10.1300")),
    ]


def test_every_posting_sums_to_zero():
    for decision in Decision:
        for amount, fee in [("100.00", "2.90"), ("0.01", "0.00"), ("0.01", "0.01"), ("10.13", "0.29")]:
            assert sum(delta for _, delta in legs(decision, _row(amount, fee), ESCROW, FEES)) == 0
