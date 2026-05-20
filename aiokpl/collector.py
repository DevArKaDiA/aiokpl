"""AggregatedBatch → PutRecordsBatch reducer.

Mirrors ``aws/kinesis/core/collector.h`` in the C++ KPL: a single
:class:`Reducer` over a stream of :class:`AggregatedBatch`, with the
``should_flush`` short-circuit that closes the batch once any single shard's
share exceeds 256 KiB. Combined with the 500-record and 5 MiB hard caps
imposed by the Kinesis ``PutRecords`` API, this keeps per-shard latency
bounded even under skewed write patterns.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from aiokpl.aggregator import AggregatedBatch
from aiokpl.reducer import Reducer


@dataclass(slots=True)
class PutRecordsBatch:
    """A collection of :class:`AggregatedBatch` destined for one ``PutRecords`` call."""

    _items: list[AggregatedBatch] = field(default_factory=list)
    _size_bytes: int = 0
    _per_shard_bytes: dict[int | None, int] = field(default_factory=dict)

    def add(self, item: AggregatedBatch) -> None:
        self._items.append(item)
        self._size_bytes += item.size
        self._per_shard_bytes[item.predicted_shard] = (
            self._per_shard_bytes.get(item.predicted_shard, 0) + item.size
        )

    def remove_last(self) -> AggregatedBatch:
        item = self._items.pop()
        self._size_bytes -= item.size
        new_total = self._per_shard_bytes[item.predicted_shard] - item.size
        if new_total <= 0:
            del self._per_shard_bytes[item.predicted_shard]
        else:
            self._per_shard_bytes[item.predicted_shard] = new_total
        return item

    @property
    def items(self) -> list[AggregatedBatch]:
        return self._items

    @property
    def count(self) -> int:
        return len(self._items)

    @property
    def size(self) -> int:
        return self._size_bytes

    @property
    def deadline(self) -> float:
        if not self._items:
            return float("inf")
        return min(it.deadline for it in self._items)

    def per_shard_bytes(self, shard_id: int | None) -> int:
        return self._per_shard_bytes.get(shard_id, 0)


class Collector:
    """Single-instance :class:`Reducer` packing aggregated batches into ``PutRecords`` calls."""

    def __init__(
        self,
        *,
        on_batch_ready: Callable[[PutRecordsBatch], Awaitable[None]],
        collection_max_count: int = 500,
        collection_max_size: int = 5 * 1024 * 1024,
        per_shard_short_circuit_bytes: int = 256 * 1024,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._on_batch_ready = on_batch_ready
        self._threshold = per_shard_short_circuit_bytes

        def predicate(just_added: AggregatedBatch, current: PutRecordsBatch) -> bool:
            return current.per_shard_bytes(just_added.predicted_shard) >= self._threshold

        self._reducer: Reducer[AggregatedBatch, PutRecordsBatch] = Reducer(
            batch_factory=PutRecordsBatch,
            count_limit=collection_max_count,
            size_limit=collection_max_size,
            on_deadline=on_batch_ready,
            flush_predicate=predicate,
            clock=clock,
        )

    async def put(self, batch: AggregatedBatch) -> None:
        closed = await self._reducer.add(batch)
        if closed is not None:
            await self._on_batch_ready(closed)

    async def flush(self) -> None:
        closed = await self._reducer.flush()
        if closed is not None:
            await self._on_batch_ready(closed)

    async def aclose(self) -> None:
        await self._reducer.aclose()


__all__ = ["Collector", "PutRecordsBatch"]
