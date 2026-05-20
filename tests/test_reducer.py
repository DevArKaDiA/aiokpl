"""Unit tests for ``aiokpl.reducer``.

We use a list-backed ``FakeBatch[FakeItem]`` so the tests don't pull in
Aggregator/Collector logic. Deadline-fire tests use very short real delays
(5-20 ms) and ``await asyncio.sleep`` — the timer schedules a task on the
running loop, and a small sleep is the cleanest way to deterministically let
that task run.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from aiokpl.reducer import Batch, Batchable, Reducer


@dataclass(slots=True)
class FakeItem:
    value: int
    deadline: float
    weight: int = 1


@dataclass(slots=True)
class FakeBatch:
    _items: list[FakeItem] = field(default_factory=list)
    _size: int = 0

    def add(self, item: FakeItem) -> None:
        self._items.append(item)
        self._size += item.weight

    def remove_last(self) -> FakeItem:
        item = self._items.pop()
        self._size -= item.weight
        return item

    @property
    def items(self) -> list[FakeItem]:
        return self._items

    @property
    def count(self) -> int:
        return len(self._items)

    @property
    def size(self) -> int:
        return self._size

    @property
    def deadline(self) -> float:
        if not self._items:
            return float("inf")
        return min(it.deadline for it in self._items)


def _make_clock(start: float = 0.0):
    state = {"now": start}

    def clock() -> float:
        return state["now"]

    def advance(dt: float) -> None:
        state["now"] += dt

    return clock, advance


async def _noop(_batch: FakeBatch) -> None:
    return None


def test_protocols_are_runtime_checkable() -> None:
    item = FakeItem(value=1, deadline=0.0)
    assert isinstance(item, Batchable)
    batch = FakeBatch()
    assert isinstance(batch, Batch)


async def test_add_under_limits_returns_none() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=10,
        size_limit=100,
        on_deadline=_noop,
        clock=clock,
    )
    assert await r.add(FakeItem(value=1, deadline=10.0)) is None
    await r.aclose()


async def test_count_limit_triggers_flush() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=2,
        size_limit=1_000,
        on_deadline=_noop,
        clock=clock,
    )
    assert await r.add(FakeItem(value=1, deadline=5.0)) is None
    # Second add reaches the limit (count >= limit) and triggers flush.
    closed = await r.add(FakeItem(value=2, deadline=5.0))
    assert closed is not None
    assert closed.count == 2
    await r.aclose()


async def test_size_limit_triggers_flush() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=5,
        on_deadline=_noop,
        clock=clock,
    )
    assert await r.add(FakeItem(value=1, deadline=1.0, weight=3)) is None
    closed = await r.add(FakeItem(value=2, deadline=2.0, weight=3))
    assert closed is not None
    # First item fits alone; second item pushes over the limit, so the
    # closed batch contains the first only.
    assert [it.value for it in closed.items] == [1]
    await r.aclose()


async def test_flush_predicate_triggers_flush() -> None:
    clock, _ = _make_clock()

    def predicate(item: FakeItem, _b: FakeBatch) -> bool:
        return item.value == 99

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=100,
        on_deadline=_noop,
        flush_predicate=predicate,
        clock=clock,
    )
    assert await r.add(FakeItem(value=1, deadline=1.0)) is None
    closed = await r.add(FakeItem(value=99, deadline=2.0))
    assert closed is not None
    assert {it.value for it in closed.items} == {1, 99}
    await r.aclose()


async def test_flush_returns_none_when_empty() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=10,
        size_limit=100,
        on_deadline=_noop,
        clock=clock,
    )
    assert await r.flush() is None
    await r.aclose()


async def test_manual_flush_returns_batch() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=10,
        size_limit=100,
        on_deadline=_noop,
        clock=clock,
    )
    await r.add(FakeItem(value=1, deadline=10.0))
    await r.add(FakeItem(value=2, deadline=20.0))
    closed = await r.flush()
    assert closed is not None
    assert [it.value for it in closed.items] == [1, 2]
    # Second flush is a no-op
    assert await r.flush() is None
    await r.aclose()


async def test_fifo_by_deadline_ordering_on_size_overflow() -> None:
    # size_limit-driven overflow is what exposes the FIFO-by-deadline packing:
    # the trigger may fire with more total weight than fits, so the sort order
    # matters. (count_limit triggers right at the limit, so there is no excess
    # to re-inject.)
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=10,
        on_deadline=_noop,
        clock=clock,
    )
    await r.add(FakeItem(value=1, deadline=30.0, weight=4))
    await r.add(FakeItem(value=2, deadline=10.0, weight=4))
    # 3rd add brings total weight to 12 (>= 10), triggering flush. After sort
    # by deadline: 2 (d=10), 3 (d=20), 1 (d=30). Pack greedy by weight:
    # 2 (size=4), 3 (size=8), 1 would push size to 12 → undo last, leftover=[1].
    closed = await r.add(FakeItem(value=3, deadline=20.0, weight=4))
    assert closed is not None
    assert [it.value for it in closed.items] == [2, 3]
    leftover = await r.flush()
    assert leftover is not None
    assert [it.value for it in leftover.items] == [1]
    await r.aclose()


async def test_oversized_single_item_is_surfaced_alone() -> None:
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=5,
        on_deadline=_noop,
        clock=clock,
    )
    closed = await r.add(FakeItem(value=42, deadline=1.0, weight=999))
    assert closed is not None
    assert [it.value for it in closed.items] == [42]
    assert await r.flush() is None
    await r.aclose()


async def test_oversized_single_with_excess_carries_remainder() -> None:
    # The oversized-single branch needs to coexist with leftover candidates:
    # add a small item first, then a giant one that wipes the budget.
    clock, _ = _make_clock()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=5,
        on_deadline=_noop,
        clock=clock,
    )
    await r.add(FakeItem(value=1, deadline=20.0, weight=1))
    # The giant item has the earliest deadline → it sorts first and trips
    # the limit on its own; remaining item must be re-injected.
    closed = await r.add(FakeItem(value=2, deadline=10.0, weight=999))
    assert closed is not None
    assert [it.value for it in closed.items] == [2]
    leftover = await r.flush()
    assert leftover is not None
    assert [it.value for it in leftover.items] == [1]
    await r.aclose()


async def test_deadline_fire_invokes_callback() -> None:
    captured: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        captured.append(b)

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
    )
    loop = asyncio.get_running_loop()
    now = loop.time()
    # Submit two items; rescheduling must pick the earliest deadline.
    await r.add(FakeItem(value=1, deadline=now + 0.5))
    await r.add(FakeItem(value=2, deadline=now + 0.01))
    await asyncio.sleep(0.05)
    assert len(captured) == 1
    assert {it.value for it in captured[0].items} == {1, 2}
    await r.aclose()


async def test_deadline_fire_on_empty_is_noop() -> None:
    # Cover the path where the timer fires but the batch was already drained
    # (race between manual flush and an in-flight timer).
    captured: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        captured.append(b)

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
    )
    loop = asyncio.get_running_loop()
    now = loop.time()
    await r.add(FakeItem(value=1, deadline=now + 0.01))
    # Drain manually before the timer fires; if it still fires it must no-op.
    drained = await r.flush()
    assert drained is not None
    # Synthesize a stale fire by re-arming via add → flush sequence.
    await r.add(FakeItem(value=2, deadline=now + 1.0))
    # Replace the active batch under the lock so the timer sees count==0
    # when it eventually fires (simulating a manual flush race).
    # We can do this by calling flush() then directly scheduling a fire.
    await r.flush()
    # Trigger the on-empty fire branch via the internal _fire path.
    await r._fire()
    assert captured == []
    await r.aclose()


async def test_aclose_cancels_timer_and_is_idempotent() -> None:
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
    )
    loop = asyncio.get_running_loop()
    now = loop.time()
    await r.add(FakeItem(value=1, deadline=now + 0.01))
    await r.aclose()
    await r.aclose()  # idempotent
    await asyncio.sleep(0.05)
    assert fired == []


async def test_timer_fire_after_close_is_noop() -> None:
    # Cover the synchronous _on_timer_fire early-return when closed.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
    )
    loop = asyncio.get_running_loop()
    now = loop.time()
    await r.add(FakeItem(value=1, deadline=now + 1.0))
    # Mark closed but keep the timer handle so _on_timer_fire takes the
    # early-return branch.
    r._closed = True
    r._on_timer_fire()
    assert fired == []
    await r.aclose()


async def test_explicit_loop_is_used() -> None:
    # Pass loop explicitly to exercise the `self._loop is not None` branch.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    loop = asyncio.get_running_loop()
    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
        loop=loop,
    )
    now = loop.time()
    await r.add(FakeItem(value=1, deadline=now + 0.01))
    await asyncio.sleep(0.05)
    assert len(fired) == 1
    await r.aclose()


async def test_fire_when_closed_skips_callback() -> None:
    # Cover the _fire `if self._closed` branch.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    r: Reducer[FakeItem, FakeBatch] = Reducer(
        batch_factory=FakeBatch,
        count_limit=100,
        size_limit=1_000,
        on_deadline=on_deadline,
    )
    loop = asyncio.get_running_loop()
    now = loop.time()
    await r.add(FakeItem(value=1, deadline=now + 1.0))
    r._closed = True
    await r._fire()
    assert fired == []
    # restore for clean aclose
    r._closed = False
    await r.aclose()


@pytest.mark.parametrize("dummy", [1])
def test_module_imports(dummy: int) -> None:
    # Smoke test for static import coverage of the module-level branches.
    from aiokpl import reducer

    assert reducer.Reducer is not None
    assert dummy == 1
