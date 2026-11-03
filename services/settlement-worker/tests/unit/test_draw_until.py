"""draw_until (BUG-011): the draw loop of the partition-targeted setups, driven by scripted partition
sequences instead of a broker. A payment's partition can only be drawn, never chosen, so the cap
(PUBLISH_CAP = 64) can only make a miss unlikely; these tests pin how the loop counts and reports.
"""

import itertools

import pytest

from support.failure_kit import draw_until, newest_position


def _scripted(partitions):
    """draw() returning ("p<n>", (partition, n)) for the n-th scripted partition (n from 0), and the
    list of draws made."""
    source = iter(partitions)
    calls: list[int] = []

    def draw():
        n = len(calls)
        calls.append(n)
        return f"p{n}", (next(source), n)

    return draw, calls


def _lands_on(partition: int):
    return lambda drawn: newest_position(drawn)[0] == partition


def test_a_target_partition_first_drawn_on_draw_13_is_found():
    draw, calls = _scripted([0, 2] * 6 + [1])  # draws 1-12 miss partition 1; draw 13 lands on it
    drawn = draw_until(draw, _lands_on(1))
    assert len(calls) == 13
    assert list(drawn)[-1] == "p12" and newest_position(drawn) == (1, 12)


def test_all_three_partitions_first_covered_on_draw_16_are_found():
    draw, calls = _scripted([0, 2] * 7 + [0, 1])  # partition 1 first appears on draw 16
    drawn = draw_until(draw, lambda d: {partition for partition, _ in d.values()} == {0, 1, 2})
    assert len(calls) == 16 and len(drawn) == 16


def test_a_condition_never_met_fails_after_exactly_64_draws_with_the_tally():
    draw, calls = _scripted(itertools.cycle([0, 2]))
    with pytest.raises(AssertionError) as raised:
        draw_until(draw, _lands_on(1))
    assert len(calls) == 64
    message = str(raised.value)
    assert message.startswith("no draw satisfied the condition within 64 draws; partition tally {0: 32, 2: 32}; drawn: ")
    assert "'p0': (0, 0)" in message and "'p63': (2, 63)" in message, message
