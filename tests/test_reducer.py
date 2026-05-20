"""Unit tests for ``aiokpl.reducer``.

We use a list-backed ``FakeBatch[FakeItem]`` so the tests don't pull in
Aggregator/Collector logic. Deadline-fire tests use very short real delays
(5-20 ms) and ``await anyio.sleep`` — the timer schedules a task on the
caller-supplied task group, and a small sleep is the cleanest way to
deterministically let that task run on either asyncio or trio.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import anyio
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
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
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
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=2,
            size_limit=1_000,
            on_deadline=_noop,
            clock=clock,
        )
        assert await r.add(FakeItem(value=1, deadline=5.0)) is None
        closed = await r.add(FakeItem(value=2, deadline=5.0))
        assert closed is not None
        assert closed.count == 2
        await r.aclose()


async def test_size_limit_triggers_flush() -> None:
    clock, _ = _make_clock()
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=5,
            on_deadline=_noop,
            clock=clock,
        )
        assert await r.add(FakeItem(value=1, deadline=1.0, weight=3)) is None
        closed = await r.add(FakeItem(value=2, deadline=2.0, weight=3))
        assert closed is not None
        assert [it.value for it in closed.items] == [1]
        await r.aclose()


async def test_flush_predicate_triggers_flush() -> None:
    clock, _ = _make_clock()

    def predicate(item: FakeItem, _b: FakeBatch) -> bool:
        return item.value == 99

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
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
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
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
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
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
        assert await r.flush() is None
        await r.aclose()


async def test_fifo_by_deadline_ordering_on_size_overflow() -> None:
    clock, _ = _make_clock()
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=10,
            on_deadline=_noop,
            clock=clock,
        )
        await r.add(FakeItem(value=1, deadline=30.0, weight=4))
        await r.add(FakeItem(value=2, deadline=10.0, weight=4))
        closed = await r.add(FakeItem(value=3, deadline=20.0, weight=4))
        assert closed is not None
        assert [it.value for it in closed.items] == [2, 3]
        leftover = await r.flush()
        assert leftover is not None
        assert [it.value for it in leftover.items] == [1]
        await r.aclose()


async def test_oversized_single_item_is_surfaced_alone() -> None:
    clock, _ = _make_clock()
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
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
    clock, _ = _make_clock()
    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=5,
            on_deadline=_noop,
            clock=clock,
        )
        await r.add(FakeItem(value=1, deadline=20.0, weight=1))
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

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        # Submit two items; rescheduling must pick the earliest deadline.
        await r.add(FakeItem(value=1, deadline=now + 0.5))
        await r.add(FakeItem(value=2, deadline=now + 0.01))
        await anyio.sleep(0.1)
        assert len(captured) == 1
        assert {it.value for it in captured[0].items} == {1, 2}
        await r.aclose()


async def test_deadline_fire_on_empty_is_noop() -> None:
    # Cover the path where the timer fires but the batch was already drained
    # (race between manual flush and an in-flight timer).
    captured: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        captured.append(b)

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        await r.add(FakeItem(value=1, deadline=now + 0.01))
        drained = await r.flush()
        assert drained is not None
        await r.add(FakeItem(value=2, deadline=now + 1.0))
        await r.flush()
        # Trigger the on-empty fire branch via the internal _fire path.
        await r._fire()
        assert captured == []
        await r.aclose()


async def test_aclose_cancels_timer_and_is_idempotent() -> None:
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        await r.add(FakeItem(value=1, deadline=now + 0.01))
        await r.aclose()
        await r.aclose()  # idempotent
        await anyio.sleep(0.05)
        assert fired == []


async def test_fire_when_closed_skips_callback() -> None:
    # Cover the _fire `if self._closed` branch.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        await r.add(FakeItem(value=1, deadline=now + 1.0))
        r._closed = True
        await r._fire()
        assert fired == []
        # restore for clean aclose
        r._closed = False
        await r.aclose()


async def test_fire_with_stale_scope_is_noop() -> None:
    # Cover the `scope is not self._timer_scope` early-return branch in _fire.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        await r.add(FakeItem(value=1, deadline=now + 1.0))
        # Synthesize a fire from a stale scope (not the currently-armed one).
        stale = anyio.CancelScope()
        await r._fire(stale)
        assert fired == []
        await r.aclose()


async def test_timer_task_closed_after_sleep_is_noop() -> None:
    # The timer task body checks self._closed after waking up; force that
    # branch by setting _closed between scheduling and firing.
    fired: list[FakeBatch] = []

    async def on_deadline(b: FakeBatch) -> None:
        fired.append(b)

    async with anyio.create_task_group() as tg:
        r: Reducer[FakeItem, FakeBatch] = Reducer(
            task_group=tg,
            batch_factory=FakeBatch,
            count_limit=100,
            size_limit=1_000,
            on_deadline=on_deadline,
            clock=anyio.current_time,
        )
        now = anyio.current_time()
        await r.add(FakeItem(value=1, deadline=now + 0.02))
        r._closed = True
        await anyio.sleep(0.08)
        assert fired == []
        r._closed = False
        await r.aclose()


@pytest.mark.parametrize("dummy", [1])
def test_module_imports(dummy: int) -> None:
    from aiokpl import reducer

    assert reducer.Reducer is not None
    assert dummy == 1
