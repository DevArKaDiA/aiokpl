"""Unit tests for ``aiokpl.aggregator``.

We stub :class:`ShardMap` with a fake exposing only the bits the Aggregator
touches (``state`` and ``predict``). This keeps tests free of the network
layer while still exercising every routing branch.
"""

from __future__ import annotations

from dataclasses import dataclass

import anyio
import pytest

from aiokpl.aggregation import MAGIC, UserRecord, decode_aggregated
from aiokpl.aggregator import AggregatedBatch, Aggregator, _BufferedRecord
from aiokpl.shard_map import ShardMapState


@dataclass
class FakeShardMap:
    state: ShardMapState
    table: dict[int, int]

    def predict(self, hash_key: int) -> int | None:
        if self.state is not ShardMapState.READY:
            return None
        return hash_key % 2


def _make_clock(start: float = 1_000.0):
    state = {"now": start}

    def clock() -> float:
        return state["now"]

    def advance(dt: float) -> None:
        state["now"] += dt

    return clock, advance


async def test_aggregated_batch_basic_size_and_deadline() -> None:
    b = AggregatedBatch(predicted_shard=0)
    assert b.count == 0
    assert b.size == 0
    assert b.deadline == float("inf")
    assert b.items == []

    ur = UserRecord(partition_key="pk1", data=b"hello")
    b.add(_BufferedRecord(user_record=ur, deadline=10.0, hash_key=1))
    s1 = b.size
    assert s1 > 0
    b.add(_BufferedRecord(user_record=ur, deadline=5.0, hash_key=1))
    s2 = b.size
    assert s2 > s1
    assert s2 - s1 < s1
    assert b.deadline == 5.0
    assert b.count == 2


async def test_aggregated_batch_with_explicit_hash_key() -> None:
    b = AggregatedBatch(predicted_shard=None)
    ur = UserRecord(partition_key="pk", data=b"x", explicit_hash_key="42")
    b.add(_BufferedRecord(user_record=ur, deadline=1.0, hash_key=42))
    s1 = b.size
    b.add(_BufferedRecord(user_record=ur, deadline=2.0, hash_key=42))
    delta = b.size - s1
    assert delta < s1


async def test_aggregated_batch_remove_last_rebuilds_estimate() -> None:
    b = AggregatedBatch(predicted_shard=0)
    ur1 = UserRecord(partition_key="a", data=b"x")
    ur2 = UserRecord(partition_key="b", data=b"y", explicit_hash_key="123")
    b.add(_BufferedRecord(user_record=ur1, deadline=1.0, hash_key=1))
    s1 = b.size
    b.add(_BufferedRecord(user_record=ur2, deadline=2.0, hash_key=2))
    popped = b.remove_last()
    assert popped.user_record is ur2
    assert b.size == s1
    assert b.count == 1


async def test_aggregated_batch_routing_keys_aggregated() -> None:
    b = AggregatedBatch(predicted_shard=0)
    ur1 = UserRecord(partition_key="pk1", data=b"a")
    ur2 = UserRecord(partition_key="pk2", data=b"b")
    b.add(_BufferedRecord(user_record=ur1, deadline=1.0, hash_key=111))
    b.add(_BufferedRecord(user_record=ur2, deadline=2.0, hash_key=222))
    assert b.routing_partition_key() == "a"
    assert b.routing_explicit_hash_key() == "111"
    blob = b.to_blob()
    assert blob.startswith(MAGIC)
    decoded = decode_aggregated(blob)
    assert [d.partition_key for d in decoded] == ["pk1", "pk2"]


async def test_aggregated_batch_routing_keys_single_record() -> None:
    b = AggregatedBatch(predicted_shard=0)
    ur = UserRecord(partition_key="solo", data=b"x", explicit_hash_key="99")
    b.add(_BufferedRecord(user_record=ur, deadline=1.0, hash_key=99))
    assert b.routing_partition_key() == "solo"
    assert b.routing_explicit_hash_key() == "99"
    assert b.to_blob() == b"x"


async def test_aggregated_batch_routing_keys_single_no_ehk() -> None:
    b = AggregatedBatch(predicted_shard=0)
    ur = UserRecord(partition_key="solo", data=b"x")
    b.add(_BufferedRecord(user_record=ur, deadline=1.0, hash_key=99))
    assert b.routing_explicit_hash_key() is None


async def test_put_routes_to_per_shard_reducer() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(
        sm,
        on_batch_ready=on_batch_ready,
        record_max_buffered_time_ms=10_000.0,
        clock=clock,
    ) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"0", explicit_hash_key="0"))
        await agg.put(UserRecord(partition_key="y", data=b"1", explicit_hash_key="1"))
        await agg.flush()
        assert len(captured) == 2
        shards = {b.predicted_shard for b in captured}
        assert shards == {0, 1}


async def test_put_falls_back_to_none_when_shard_map_not_ready() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.INVALID, table={})
    async with Aggregator(sm, on_batch_ready=on_batch_ready, clock=clock) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"1"))
        await agg.flush()
        assert len(captured) == 1
        assert captured[0].predicted_shard is None


async def test_aggregation_disabled_yields_single_record_batches() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(
        sm,
        on_batch_ready=on_batch_ready,
        aggregation_enabled=False,
        record_max_buffered_time_ms=10_000.0,
        clock=clock,
    ) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"a", explicit_hash_key="0"))
        await agg.put(UserRecord(partition_key="y", data=b"b", explicit_hash_key="2"))
        assert len(captured) == 2
        for b in captured:
            assert b.count == 1


async def test_put_uses_md5_when_no_explicit_hash_key() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(sm, on_batch_ready=on_batch_ready, clock=clock) as agg:
        await agg.put(UserRecord(partition_key="some-key", data=b"x"))
        await agg.flush()
        assert len(captured) == 1


async def test_size_estimate_monotonically_increases() -> None:
    b = AggregatedBatch(predicted_shard=0)
    last = 0
    for i in range(10):
        ur = UserRecord(partition_key=f"pk{i}", data=b"payload")
        b.add(_BufferedRecord(user_record=ur, deadline=float(i), hash_key=i))
        assert b.size > last
        last = b.size


async def test_get_or_create_reducer_caches_per_shard() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(sm, on_batch_ready=on_batch_ready, clock=clock) as agg:
        r1 = await agg._get_or_create_reducer(0)
        r2 = await agg._get_or_create_reducer(0)
        assert r1 is r2


async def test_get_or_create_reducer_without_context_raises() -> None:
    sm = FakeShardMap(state=ShardMapState.READY, table={})

    async def on_batch_ready(_b: AggregatedBatch) -> None:
        return None

    agg = Aggregator(sm, on_batch_ready=on_batch_ready)
    with pytest.raises(RuntimeError, match="async context manager"):
        await agg._get_or_create_reducer(0)


async def test_deadline_fire_dispatches_via_on_batch_ready() -> None:
    captured: list[AggregatedBatch] = []
    event = anyio.Event()

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)
        event.set()

    sm = FakeShardMap(state=ShardMapState.READY, table={})

    async with Aggregator(
        sm,
        on_batch_ready=on_batch_ready,
        record_max_buffered_time_ms=10.0,
        clock=anyio.current_time,
    ) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"x", explicit_hash_key="0"))
        with anyio.fail_after(1.0):
            await event.wait()
        assert len(captured) == 1


async def test_flush_skips_empty_reducers() -> None:
    captured: list[AggregatedBatch] = []

    async def on_batch_ready(b: AggregatedBatch) -> None:
        captured.append(b)

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(sm, on_batch_ready=on_batch_ready, clock=clock) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"x", explicit_hash_key="0"))
        await agg.flush()
        assert len(captured) == 1
        await agg.flush()
        assert len(captured) == 1


async def test_aclose_is_idempotent() -> None:
    async def on_batch_ready(_b: AggregatedBatch) -> None:
        return None

    clock, _ = _make_clock()
    sm = FakeShardMap(state=ShardMapState.READY, table={})
    async with Aggregator(sm, on_batch_ready=on_batch_ready, clock=clock) as agg:
        await agg.put(UserRecord(partition_key="x", data=b"x", explicit_hash_key="0"))
        await agg.aclose()
        await agg.aclose()


@pytest.mark.parametrize("dummy", [1])
def test_module_imports(dummy: int) -> None:
    from aiokpl import aggregator

    assert aggregator.Aggregator is not None
    assert dummy == 1
