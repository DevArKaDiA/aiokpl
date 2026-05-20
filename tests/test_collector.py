"""Unit tests for ``aiokpl.collector``."""

from __future__ import annotations

import anyio
import pytest

from aiokpl.aggregation import UserRecord
from aiokpl.aggregator import AggregatedBatch, _BufferedRecord
from aiokpl.collector import Collector, PutRecordsBatch


def _agg(shard: int | None, deadline: float, size_bytes: int) -> AggregatedBatch:
    b = AggregatedBatch(predicted_shard=shard)
    ur = UserRecord(partition_key="pk", data=b"x" * max(size_bytes - 20, 1))
    b.add(_BufferedRecord(user_record=ur, deadline=deadline, hash_key=0))
    return b


async def test_putrecordsbatch_basic() -> None:
    p = PutRecordsBatch()
    assert p.count == 0
    assert p.size == 0
    assert p.deadline == float("inf")
    assert p.items == []
    assert p.per_shard_bytes(0) == 0

    a = _agg(0, 1.0, 100)
    p.add(a)
    assert p.count == 1
    assert p.size == a.size
    assert p.per_shard_bytes(0) == a.size
    assert p.deadline == 1.0


async def test_putrecordsbatch_remove_last_zeroes_per_shard_entry() -> None:
    p = PutRecordsBatch()
    a = _agg(0, 1.0, 100)
    p.add(a)
    popped = p.remove_last()
    assert popped is a
    assert p.per_shard_bytes(0) == 0
    assert p.size == 0


async def test_putrecordsbatch_remove_last_preserves_residual() -> None:
    p = PutRecordsBatch()
    a1 = _agg(0, 1.0, 100)
    a2 = _agg(0, 2.0, 100)
    p.add(a1)
    p.add(a2)
    p.remove_last()
    assert p.per_shard_bytes(0) == a1.size
    assert p.size == a1.size


async def test_collector_per_shard_short_circuit() -> None:
    captured: list[PutRecordsBatch] = []

    async def on_ready(b: PutRecordsBatch) -> None:
        captured.append(b)

    async with Collector(
        on_batch_ready=on_ready,
        collection_max_count=500,
        collection_max_size=5 * 1024 * 1024,
        per_shard_short_circuit_bytes=200,
    ) as col:
        await col.put(_agg(0, 10.0, 150))
        await col.put(_agg(0, 11.0, 150))
        assert len(captured) == 1


async def test_collector_count_limit() -> None:
    captured: list[PutRecordsBatch] = []

    async def on_ready(b: PutRecordsBatch) -> None:
        captured.append(b)

    async with Collector(
        on_batch_ready=on_ready,
        collection_max_count=2,
        collection_max_size=10_000_000,
        per_shard_short_circuit_bytes=10_000_000,
    ) as col:
        await col.put(_agg(0, 1.0, 50))
        await col.put(_agg(1, 2.0, 50))
        await col.put(_agg(2, 3.0, 50))
        assert len(captured) == 1
        assert captured[0].count == 2


async def test_collector_size_limit() -> None:
    captured: list[PutRecordsBatch] = []

    async def on_ready(b: PutRecordsBatch) -> None:
        captured.append(b)

    async with Collector(
        on_batch_ready=on_ready,
        collection_max_count=500,
        collection_max_size=400,
        per_shard_short_circuit_bytes=10_000_000,
    ) as col:
        await col.put(_agg(0, 1.0, 250))
        await col.put(_agg(1, 2.0, 250))
        assert len(captured) == 1


async def test_collector_flush_drains() -> None:
    captured: list[PutRecordsBatch] = []

    async def on_ready(b: PutRecordsBatch) -> None:
        captured.append(b)

    async with Collector(
        on_batch_ready=on_ready,
        collection_max_count=500,
        collection_max_size=10_000_000,
        per_shard_short_circuit_bytes=10_000_000,
    ) as col:
        await col.put(_agg(0, 10.0, 100))
        await col.flush()
        assert len(captured) == 1
        await col.flush()
        assert len(captured) == 1


async def test_collector_deadline_fire() -> None:
    captured: list[PutRecordsBatch] = []
    event = anyio.Event()

    async def on_ready(b: PutRecordsBatch) -> None:
        captured.append(b)
        event.set()

    async with Collector(
        on_batch_ready=on_ready,
        collection_max_count=500,
        collection_max_size=10_000_000,
        per_shard_short_circuit_bytes=10_000_000,
        clock=anyio.current_time,
    ) as col:
        soon = anyio.current_time() + 0.02
        await col.put(_agg(0, soon, 100))
        with anyio.fail_after(1.0):
            await event.wait()
        assert len(captured) == 1


async def test_collector_aclose_idempotent() -> None:
    async def on_ready(_b: PutRecordsBatch) -> None:
        return None

    async with Collector(on_batch_ready=on_ready) as col:
        await col.aclose()
        await col.aclose()


async def test_collector_put_without_context_raises() -> None:
    async def on_ready(_b: PutRecordsBatch) -> None:
        return None

    col = Collector(on_batch_ready=on_ready)
    with pytest.raises(RuntimeError, match="async context manager"):
        await col.put(_agg(0, 1.0, 100))
    with pytest.raises(RuntimeError, match="async context manager"):
        await col.flush()
    await col.aclose()  # safe to call before entering


@pytest.mark.parametrize("dummy", [1])
def test_module_imports(dummy: int) -> None:
    from aiokpl import collector

    assert collector.Collector is not None
    assert dummy == 1
