"""D-112: the fake processor's reserved cents, across all test sources.

Cents 91 and 92 make the fake processor fail (unavailable, then slow) on a record's first two
attempts, so only the processor failure proof and the processor's own unit test may use them. Cents
13 declines, so it appears only where a test means a decline (or tests the money and legs code with
a declined amount), at exactly the places listed in CENTS_13.

How amounts are found: every *.py under tests/ is parsed with ast, and every string constant that is
money-shaped (an optional minus, digits, a point, digits) is checked, wherever it appears: an API
call, a fixture constant, an expected value. That is a superset of the amounts the tests send, so it
doesn't depend on following a value into authorize(). Cents use the processor's rule, on the absolute
value. An amount computed at runtime can't be seen here; the only one, the separate-tx control's
ballast, is rounded up to whole units and asserts its own cents are 00.
"""

import ast
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]
SELF = "unit/test_reserved_amounts.py"  # holds the allowlist below
MONEY = re.compile(r"-?\d+\.\d+")
RESERVED = (91, 92)

# Files that may use any reserved cents, with the reason.
RESERVED_ALLOWED = {
    "failure_modes/test_processor_failures.py": "the processor failure proof (T10)",
    "unit/test_processor.py": "the processor's own rule, tested directly; never sent to the API",
}

# (file, literal) -> how many times it may appear. Each is a decline the test means.
CENTS_13 = {
    # DECLINED_AMOUNTS: the crash proof's release cases.
    ("e2e/test_crash_recovery.py", "0.13"): 1,
    ("e2e/test_crash_recovery.py", "10.13"): 1,
    ("e2e/test_crash_recovery.py", "113.13"): 1,
    # Every other authorize is a decline, so the worker releases as well as settles.
    ("e2e/test_cross_operation_lock_order.py", "10.13"): 1,
    # The smoke test's release case and its expected legs.
    ("e2e/test_worker_smoke.py", "10.13"): 1,
    ("e2e/test_worker_smoke.py", "10.1300"): 1,
    ("e2e/test_worker_smoke.py", "-10.1300"): 1,
    # The release case of settlement.handle and its expected legs.
    ("integration/test_settlement.py", "10.13"): 3,
    ("integration/test_settlement.py", "-10.13"): 1,
    # AMOUNTS: the negative control's declined iterations.
    ("negative_controls/test_separate_tx_dedup.py", "10.13"): 1,
    ("negative_controls/test_separate_tx_dedup.py", "113.13"): 1,
    # Money parsing and formatting of a declined amount; never sent as a payment.
    ("unit/test_money.py", "10.13"): 2,
    ("unit/test_money.py", "-10.13"): 1,
    ("unit/test_money.py", "-10.1300"): 1,
    # The release legs of a declined payment.
    ("unit/test_settlement_legs.py", "10.13"): 1,
    ("unit/test_settlement_legs.py", "10.1300"): 2,
    ("unit/test_settlement_legs.py", "-10.1300"): 1,
}


def cents(literal: str) -> Decimal:
    return (abs(Decimal(literal)) * 100) % 100


def money_literals(source: str, filename: str = "<source>") -> list[tuple[int, str]]:
    """(line, literal) of every money-shaped string constant in the source."""
    found = []
    for node in ast.walk(ast.parse(source, filename=filename)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and MONEY.fullmatch(node.value):
            found.append((node.lineno, node.value))
    return sorted(found)


def _sources() -> dict[str, str]:
    return {
        path.relative_to(TESTS).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(TESTS.rglob("*.py"))
        if "__pycache__" not in path.parts and path.relative_to(TESTS).as_posix() != SELF
    }


def test_cents_91_and_92_only_in_the_processor_tests():
    hits = [
        f"{name}:{line} {literal!r} (cents {int(cents(literal))})"
        for name, source in _sources().items()
        if name not in RESERVED_ALLOWED
        for line, literal in money_literals(source, name)
        if cents(literal) in RESERVED
    ]
    assert hits == [], "cents 91/92 are reserved for the processor failure proof (D-112):\n" + "\n".join(hits)


def test_cents_13_only_in_the_listed_declines():
    seen = Counter(
        (name, literal)
        for name, source in _sources().items()
        if name not in RESERVED_ALLOWED
        for _, literal in money_literals(source, name)
        if cents(literal) == 13
    )
    unexpected = {key: n for key, n in seen.items() if n > CENTS_13.get(key, 0)}
    assert unexpected == {}, f"cents 13 declines; list each intended decline in CENTS_13 (D-112): {unexpected}"
    stale = {key: n for key, n in CENTS_13.items() if seen.get(key, 0) != n}
    assert stale == {}, f"CENTS_13 lists literals that aren't there any more: {stale}"


def test_the_scan_sees_reserved_and_declining_cents():
    """Not vacuous: the scan finds the reserved cents wherever a money-shaped string sits, and uses
    the processor's rule (10.912 is not cents 91)."""
    source = (
        'amount = "0.91"\n'
        'expected = {"escrow": Decimal("-3.92")}\n'
        'api.authorize(c, m, "10.13")\n'
        'near_miss = "10.912"\n'
        'fee = "2.90"\n'
    )
    found = money_literals(source)
    assert found == [(1, "0.91"), (2, "-3.92"), (3, "10.13"), (4, "10.912"), (5, "2.90")]
    assert [int(cents(literal)) for _, literal in found if cents(literal) in (*RESERVED, 13)] == [91, 92, 13]


def test_the_allowlisted_files_exist():
    names = set(_sources())
    assert "unit/test_processor.py" in names
    assert {name for name, _ in CENTS_13} <= names
