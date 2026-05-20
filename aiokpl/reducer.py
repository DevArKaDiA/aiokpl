"""Generic deadline-driven batcher.

A :class:`Reducer` accumulates items of type ``I`` into a batch of type ``B``
until one of three things happens: an item makes the batch exceed its
hard-transport limits, the caller-supplied ``flush_predicate`` fires, or the
earliest item's deadline elapses. The closed batch is then either returned
synchronously from :meth:`Reducer.add` (limit/predicate trigger) or handed to
the async ``on_deadline`` callback (timer trigger).

This mirrors ``aws/kinesis/core/reducer.h`` in the C++ KPL with two
adaptations: the deadline timer is implemented as an anyio task spawned in a
caller-supplied :class:`anyio.abc.TaskGroup`, cancellable via a per-timer
:class:`anyio.CancelScope`. Flush packing is FIFO-by-deadline with excess
re-injection into a fresh active batch — same algorithm, idiomatic
primitives — and the code is backend-agnostic (asyncio and trio both work).
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Generic, Protocol, TypeVar, runtime_checkable

import anyio
from anyio.abc import TaskGroup

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

    The deadline timer is a task spawned in the caller-supplied
    :class:`anyio.abc.TaskGroup`; cancellation is via a per-timer
    :class:`anyio.CancelScope`.
    """

    def __init__(
        self,
        *,
        task_group: TaskGroup,
        batch_factory: Callable[[], B],
        count_limit: int,
        size_limit: int,
        on_deadline: Callable[[B], Awaitable[None]],
        flush_predicate: Callable[[I, B], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._task_group = task_group
        self._batch_factory = batch_factory
        self._count_limit = count_limit
        self._size_limit = size_limit
        self._on_deadline = on_deadline
        self._flush_predicate = flush_predicate
        self._clock = clock

        self._active: B = batch_factory()
        self._lock = anyio.Lock()
        self._timer_scope: anyio.CancelScope | None = None
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
        if self._timer_scope is not None:
            self._timer_scope.cancel()
            self._timer_scope = None

    def _reschedule_locked(self) -> None:
        # Defensive: callers gate on count > 0, so the empty-active path here
        # is genuinely unreachable from public API. Kept as a guard against
        # future refactors that might bypass the caller's check.
        self._cancel_timer_locked()
        active: Batch[I] = self._active  # type: ignore[assignment]
        if active.count == 0:  # pragma: no cover - defensive guard
            return
        delay = max(0.0, active.deadline - self._clock())
        scope = anyio.CancelScope()
        self._timer_scope = scope
        self._task_group.start_soon(self._timer_task, scope, delay)

    async def _timer_task(self, scope: anyio.CancelScope, delay: float) -> None:
        # Sleeping task that fires the deadline-driven flush. Cancellation is
        # via the per-timer ``scope``; on cancel we exit cleanly without
        # firing. Reschedules clear ``self._timer_scope`` only when they cancel
        # the previous scope, so a fired scope leaves the slot pointing at
        # itself until ``_fire`` clears it.
        with scope:
            await anyio.sleep(delay)
            if self._closed:
                return
            await self._fire(scope)

    async def _fire(self, scope: anyio.CancelScope | None = None) -> None:
        async with self._lock:
            if self._closed:
                return
            # If a newer reschedule replaced our scope, this fire is stale.
            if scope is not None and self._timer_scope is not scope:
                return
            self._timer_scope = None
            active: Batch[I] = self._active  # type: ignore[assignment]
            if active.count == 0:
                return
            closed = self._pack_and_split()
        await self._on_deadline(closed)
