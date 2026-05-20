"""Pure-math tests for :class:`aiokpl.token_bucket.TokenBucket`."""

from __future__ import annotations

import pytest

from aiokpl.token_bucket import TokenBucket


class FakeClock:
    """Hand-advanced monotonic clock."""

    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_initial_full_starts_at_max() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 10.0), (100.0, 200.0)], clock=clock)
    assert tb.stream_count == 2
    assert tb.available(0) == 10.0
    assert tb.available(1) == 200.0


def test_initial_empty_starts_at_zero() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 10.0)], clock=clock, initial_full=False)
    assert tb.available(0) == 0.0


def test_growth_over_time() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 100.0)], clock=clock, initial_full=False)
    clock.advance(5.0)
    assert tb.available(0) == 50.0
    clock.advance(100.0)  # would overshoot cap
    assert tb.available(0) == 100.0


def test_zero_rate_disabled_stream_never_refills() -> None:
    clock = FakeClock()
    tb = TokenBucket([(0.0, 5.0)], clock=clock, initial_full=False)
    clock.advance(1000.0)
    assert tb.available(0) == 0.0


def test_zero_growth_does_not_commit_last() -> None:
    # If the elapsed time is exactly 0, growth==0 and we don't move last,
    # so a subsequent advance from the same wall point still produces
    # tokens. Covers the ``if growth > 0`` guard.
    clock = FakeClock()
    tb = TokenBucket([(1.0, 10.0)], clock=clock, initial_full=False)
    assert tb.available(0) == 0.0
    clock.advance(0.0)
    assert tb.available(0) == 0.0
    clock.advance(3.0)
    assert tb.available(0) == 3.0


def test_try_take_success_debits_all_streams() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 10.0), (100.0, 100.0)], clock=clock)
    assert tb.try_take([3.0, 30.0]) is True
    assert tb.available(0) == 7.0
    assert tb.available(1) == 70.0


def test_try_take_atomic_failure_leaves_bucket_untouched() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 10.0), (100.0, 100.0)], clock=clock)
    # Second stream has only 100; ask for 150 → atomic failure.
    assert tb.try_take([5.0, 150.0]) is False
    assert tb.available(0) == 10.0
    assert tb.available(1) == 100.0


def test_try_take_length_mismatch_raises() -> None:
    tb = TokenBucket([(10.0, 10.0), (100.0, 100.0)], clock=FakeClock())
    with pytest.raises(ValueError, match="expected 2"):
        tb.try_take([1.0])


def test_try_take_negative_amount_raises() -> None:
    tb = TokenBucket([(10.0, 10.0)], clock=FakeClock())
    with pytest.raises(ValueError, match="negative"):
        tb.try_take([-1.0])


def test_try_take_zero_amount_succeeds_on_empty_stream() -> None:
    clock = FakeClock()
    tb = TokenBucket([(10.0, 10.0)], clock=clock, initial_full=False)
    assert tb.try_take([0.0]) is True


def test_default_clock_path() -> None:
    # Exercise the default ``clock=time.monotonic`` branch.
    tb = TokenBucket([(1.0, 1.0)])
    assert tb.stream_count == 1
    assert tb.available(0) == pytest.approx(1.0, abs=1e-3)
