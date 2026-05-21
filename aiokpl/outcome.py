"""anyio-friendly one-shot value-bearing event.

:class:`Outcome` replaces :class:`asyncio.Future` for cross-backend portability.
``anyio`` has no ``Future``, but it has :class:`anyio.Event`. We wrap an Event
with a single-slot value (or exception) so callers can ``await outcome.wait()``
on either the asyncio or trio runtime, and any stage in the producer pipeline
can :meth:`set_value` (or :meth:`set_exception`) exactly once when the record's
terminal :class:`aiokpl.result.RecordResult` is known.

The API is deliberately tiny: one-shot semantics, re-setting raises so race
conditions surface immediately, awaiters block until the value is available.
"""

from __future__ import annotations

from typing import Generic, TypeVar

import anyio

T = TypeVar("T")


class Outcome(Generic[T]):
    """One-shot async result; like :class:`asyncio.Future` without the loop coupling.

    Backend-agnostic (works on asyncio and trio via anyio). Set once, awaited
    by any number of consumers. Re-setting raises.
    """

    __slots__ = ("_event", "_exc", "_value")

    def __init__(self) -> None:
        self._event = anyio.Event()
        self._value: T | None = None
        self._exc: BaseException | None = None

    def set_value(self, value: T) -> None:
        """Resolve the outcome with ``value``. Raises if already set."""
        if self._event.is_set():
            raise RuntimeError("Outcome already set")
        self._value = value
        self._event.set()

    def set_exception(self, exc: BaseException) -> None:
        """Resolve the outcome with an exception. Raises if already set."""
        if self._event.is_set():
            raise RuntimeError("Outcome already set")
        self._exc = exc
        self._event.set()

    def is_set(self) -> bool:
        """``True`` once :meth:`set_value` or :meth:`set_exception` has been called."""
        return self._event.is_set()

    async def wait(self) -> T:
        """Block until the outcome is set; return the value or raise the exception."""
        await self._event.wait()
        if self._exc is not None:
            raise self._exc
        # ``_value`` is guaranteed non-None when ``_exc`` is None and the event
        # is set, because ``set_value`` is the only writer that leaves both
        # ``_exc=None`` and ``_event.is_set()=True``. Cast for the type
        # checker — runtime guarded above.
        from typing import cast

        return cast("T", self._value)


__all__ = ["Outcome"]
