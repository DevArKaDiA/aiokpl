"""Multi-stream token bucket with growth-on-query semantics.

Mirrors ``aws/utils/token_bucket.h`` in the C++ KPL: a small fixed set of
independent token streams sharing one atomic ``try_take`` decision. Growth is
computed lazily — every query advances ``tokens`` by ``rate * (now - last)``
capped at ``max_tokens``. No background refill task, no ``time.sleep``: pure
math, callable from sync or async code alike.

The atomicity contract on :meth:`TokenBucket.try_take` matches the C++
``can_take + take`` pair: either every stream is debited, or none. This is
what makes the Limiter's "records *and* bytes within the same envelope"
guarantee possible.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass


@dataclass(slots=True)
class _Stream:
    """One refilling token bucket — see :class:`TokenBucket` for semantics."""

    rate: float
    max_tokens: float
    tokens: float
    last: float


class TokenBucket:
    """Stateless-style multi-stream token bucket.

    Growth is computed on demand: every query advances ``tokens`` by
    ``rate * (now - last)``, capped at ``max_tokens``. There is no background
    refill task — pure math.

    ``try_take([n0, n1, ...])`` is atomic across all streams: it succeeds and
    decrements every stream iff *all* streams currently have at least the
    requested tokens, else it leaves the bucket untouched and returns False.
    """

    __slots__ = ("_clock", "_streams")

    def __init__(
        self,
        streams: Sequence[tuple[float, float]],
        *,
        clock: Callable[[], float] = time.monotonic,
        initial_full: bool = True,
    ) -> None:
        self._clock = clock
        now = clock()
        self._streams: list[_Stream] = [
            _Stream(
                rate=rate,
                max_tokens=max_tokens,
                tokens=max_tokens if initial_full else 0.0,
                last=now,
            )
            for rate, max_tokens in streams
        ]

    @property
    def stream_count(self) -> int:
        return len(self._streams)

    def available(self, idx: int) -> float:
        """Return current tokens in stream ``idx``, applying growth."""
        s = self._streams[idx]
        self._grow(s)
        return s.tokens

    def try_take(self, amounts: Sequence[float]) -> bool:
        """Atomic multi-stream debit.

        Raises :class:`ValueError` if ``len(amounts) != stream_count`` or any
        amount is negative. Returns True on success (every stream debited);
        False on failure (no stream debited).
        """
        if len(amounts) != len(self._streams):
            raise ValueError(f"amounts has length {len(amounts)}, expected {len(self._streams)}")
        for a in amounts:
            if a < 0:
                raise ValueError(f"negative amount: {a}")

        for s, a in zip(self._streams, amounts, strict=True):
            self._grow(s)
            if a > s.tokens:
                return False

        for s, a in zip(self._streams, amounts, strict=True):
            s.tokens -= a
        return True

    def _grow(self, s: _Stream) -> None:
        # Mirrors C++ TokenStream::tokens(): only commit ``last`` when growth
        # is strictly positive, otherwise rapid back-to-back queries with a
        # low-resolution clock could starve growth indefinitely.
        now = self._clock()
        growth = s.rate * (now - s.last)
        if growth > 0:
            s.tokens = min(s.max_tokens, s.tokens + growth)
            s.last = now


__all__ = ["TokenBucket"]
