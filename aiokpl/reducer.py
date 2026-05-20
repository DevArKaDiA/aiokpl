"""Generic deadline-driven batcher.

A :class:`Reducer` accumulates items of type ``I`` into a batch of type ``B``
until one of three things happens: an item makes the batch exceed its
hard-transport limits, the caller-supplied ``flush_predicate`` fires, or the
earliest item's deadline elapses. The closed batch is then either returned
synchronously from :meth:`Reducer.add` (limit/predicate trigger) or handed to
the async ``on_deadline`` callback (timer trigger).

This mirrors ``aws/kinesis/core/reducer.h`` in the C++ KPL with two
adaptations: the active container is bound to the running asyncio loop, and
flush packing is FIFO-by-deadline with excess re-injection into a fresh
active batch — same algorithm, idiomatic primitives.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Generic, Protocol, TypeVar, runtime_checkable

I = TypeVar("I")  # noqa: E741 — protocol convention for input item type
B = TypeVar("B", bound="Batch")


@runtime_checkable
class Batchable(Protocol):
    """Items added to a :class:`Reducer` must carry a monotonic ``deadline``."""

    @property
    def deadline(self) -> float: ...


@runtime_checkable
class Batch(Protocol[I]):
    """Container interface a :class:`Reducer` manages."""

    def add(self, item: I) -> None: ...
    def remove_last(self) -> I: ...
    @property
    def items(self) -> Sequence[I]: ...
    @property
    def count(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def deadline(self) -> float: ...


class Reducer(Generic[I, B]):
    """Deadline-driven batcher.

    Limits are hard transport bounds; the user contract is the per-item
    deadline. On every add the timer is reprogrammed to the batch's current
    earliest deadline; when it fires, a partial batch is closed and dispatched
    to ``on_deadline`` outside the lock.
    """

    def __init__(
        self,
        *,
        batch_factory: Callable[[], B],
        count_limit: int,
        size_limit: int,
        on_deadline: Callable[[B], Awaitable[None]],
        flush_predicate: Callable[[I, B], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._batch_factory = batch_factory
        self._count_limit = count_limit
        self._size_limit = size_limit
        self._on_deadline = on_deadline
        self._flush_predicate = flush_predicate
        self._clock = clock
        self._loop = loop

        self._active: B = batch_factory()
        self._lock = asyncio.Lock()
        self._timer: asyncio.TimerHandle | None = None
        self._closed = False

    # ─── Public API ────────────────────────────────────────────────────────

    async def add(self, item: I) -> B | None:
        """Insert ``item``. Returns a closed batch if limits or the predicate
        triggered a flush; ``None`` if the batch is still open.

        Closed batches are packed FIFO-by-deadline; excess re-enters the
        active batch.
        """
        async with self._lock:
            active: Batch[I] = self._active  # type: ignore[assignment]
            active.add(item)

            predicate_hit = self._flush_predicate is not None and self._flush_predicate(
                item, self._active
            )
            limits_hit = active.count >= self._count_limit or active.size >= self._size_limit

            if predicate_hit or limits_hit:
                return self._pack_and_split()

            self._reschedule_locked()
            return None

    async def flush(self) -> B | None:
        """Caller-driven close. Returns whatever is buffered (sorted by
        deadline) or ``None`` if empty. Cancels the deadline timer.
        """
        async with self._lock:
            active: Batch[I] = self._active  # type: ignore[assignment]
            if active.count == 0:
                self._cancel_timer_locked()
                return None
            return self._pack_and_split()

    async def aclose(self) -> None:
        """Cancel the deadline timer; drop in-flight items. Idempotent.

        Items are LOST — callers that care must :meth:`flush` first.
        """
        async with self._lock:
            self._closed = True
            self._cancel_timer_locked()
            self._active = self._batch_factory()

    # ─── Internals ─────────────────────────────────────────────────────────

    def _pack_and_split(self) -> B:
        # Sort current items by deadline (stable; FIFO for ties), pack greedily
        # into a fresh ``closed`` batch up to limits, undo the last add when a
        # limit is exceeded, then re-inject the remainder into a fresh active
        # batch. The single-item-too-big case yields a one-item closed batch.
        self._cancel_timer_locked()
        active: Batch[I] = self._active  # type: ignore[assignment]
        candidates = sorted(active.items, key=lambda x: x.deadline)  # type: ignore[attr-defined]

        closed_b = self._batch_factory()
        closed: Batch[I] = closed_b  # type: ignore[assignment]
        remaining: list[I] = []
        for idx, cand in enumerate(candidates):
            closed.add(cand)
            if closed.count > self._count_limit or closed.size > self._size_limit:
                if closed.count == 1:
                    # Oversized single item — surface it as its own closed
                    # batch; downstream (Kinesis) will reject with a clear
                    # error. Reducer's job is to flow data, not silently drop.
                    remaining.extend(candidates[idx + 1 :])
                else:
                    closed.remove_last()
                    remaining.extend(candidates[idx:])
                break

        self._active = self._batch_factory()
        new_active: Batch[I] = self._active  # type: ignore[assignment]
        for r in remaining:
            new_active.add(r)
        if new_active.count > 0:
            self._reschedule_locked()
        return closed_b

    def _cancel_timer_locked(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _reschedule_locked(self) -> None:
        # Defensive: callers gate on count > 0, so the empty-active path here
        # is genuinely unreachable from public API. Kept as a guard against
        # future refactors that might bypass the caller's check.
        self._cancel_timer_locked()
        active: Batch[I] = self._active  # type: ignore[assignment]
        if active.count == 0:  # pragma: no cover - defensive guard
            return
        delay = max(0.0, active.deadline - self._clock())
        loop = self._loop if self._loop is not None else asyncio.get_running_loop()
        self._timer = loop.call_later(delay, self._on_timer_fire)

    def _on_timer_fire(self) -> None:
        # Runs synchronously on the loop. Spawn the async fire path as a task;
        # the task acquires the lock, packs, and dispatches the callback
        # outside the lock.
        self._timer = None
        if self._closed:
            return
        loop = self._loop if self._loop is not None else asyncio.get_running_loop()
        loop.create_task(self._fire())

    async def _fire(self) -> None:
        async with self._lock:
            if self._closed:
                return
            active: Batch[I] = self._active  # type: ignore[assignment]
            if active.count == 0:
                return
            closed = self._pack_and_split()
        await self._on_deadline(closed)
