"""UserRecord → AggregatedBatch reducer, one batch per predicted shard.

Mirrors ``aws/kinesis/core/aggregator.h`` in the C++ KPL: each predicted shard
owns a private :class:`Reducer` whose container is an :class:`AggregatedBatch`.
Records destined for the same shard pile into one aggregated payload; records
destined for different shards never mix — the shard is the unit of
optimization.

When the :class:`ShardMap` is not READY (or returns ``None`` for a hash key),
records route to a catch-all ``None``-shard batch in single-record mode. The
same single-record path is taken when ``aggregation_enabled=False``.

Size estimation follows the C++ ``KinesisRecord::accurate_size`` /
``estimated_size`` distinction: a cheap monotonic running total tracks the
proto framing overhead plus deduped partition-key / explicit-hash-key
references (see ``kinesis_record.cc:145-173``). A fully accurate size is only
needed at serialization time, which the consumer obtains via :meth:`to_blob`.

Lifecycle is structured: enter the :class:`Aggregator` as an async context
manager, which spawns an internal :class:`anyio.abc.TaskGroup` shared by
every per-shard :class:`Reducer`.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol, runtime_checkable

import anyio
from anyio.abc import TaskGroup

from aiokpl.aggregation import UserRecord, encode_aggregated
from aiokpl.hashing import md5_hash_key, parse_explicit_hash_key
from aiokpl.reducer import Reducer
from aiokpl.shard_map import ShardMapState


@runtime_checkable
class _ShardLookup(Protocol):
    """Duck-typed contract the Aggregator needs from a shard map.

    The concrete :class:`aiokpl.shard_map.ShardMap` satisfies this; tests pass
    fakes that satisfy the same shape without inheriting.
    """

    @property
    def state(self) -> ShardMapState: ...
    def predict(self, hash_key: int) -> int | None: ...


# Per-record framing overhead used by the size estimator. Numbers match the
# C++ KPL's accounting for proto field tags (3 bytes each, conservative for
# field-number ≤ 15 with a length varint up to 2 bytes) and the 2-byte
# index reference into the pk/ehk tables.
_RECORD_FRAMING = 3
_FIELD_FRAMING = 3
_INDEX_REFERENCE = 2


@dataclass(slots=True)
class _BufferedRecord:
    """A :class:`UserRecord` decorated with arrival metadata for the Reducer."""

    user_record: UserRecord
    deadline: float
    hash_key: int


@dataclass(slots=True)
class AggregatedBatch:
    """A per-shard accumulation of :class:`UserRecord`s.

    Serializes to a single Kinesis API record: raw bytes when ``count == 1``
    (the C++ KPL "single-record short-circuit"), or the KPL aggregated wire
    format when ``count > 1``.
    """

    predicted_shard: int | None
    _items: list[_BufferedRecord] = field(default_factory=list)
    _size_estimate: int = 0
    _pk_set: set[str] = field(default_factory=set)
    _ehk_set: set[str] = field(default_factory=set)

    def add(self, item: _BufferedRecord) -> None:
        ur = item.user_record
        delta = _RECORD_FRAMING + len(ur.data) + _FIELD_FRAMING + _INDEX_REFERENCE
        if ur.partition_key not in self._pk_set:
            self._pk_set.add(ur.partition_key)
            delta += len(ur.partition_key.encode("utf-8")) + _FIELD_FRAMING
        if ur.explicit_hash_key is not None and ur.explicit_hash_key not in self._ehk_set:
            self._ehk_set.add(ur.explicit_hash_key)
            delta += len(ur.explicit_hash_key.encode("utf-8")) + _FIELD_FRAMING
        self._items.append(item)
        self._size_estimate += delta

    def remove_last(self) -> _BufferedRecord:
        # Rebuild dedup sets from scratch — cheaper than tracking ref-counts
        # for the rare undo path and matches the C++ pattern of recomputing on
        # the fly. The Reducer's pack loop calls this at most once per flush.
        item = self._items.pop()
        self._pk_set.clear()
        self._ehk_set.clear()
        self._size_estimate = 0
        kept = self._items
        self._items = []
        for k in kept:
            self.add(k)
        return item

    @property
    def items(self) -> list[_BufferedRecord]:
        return self._items

    @property
    def count(self) -> int:
        return len(self._items)

    @property
    def size(self) -> int:
        return self._size_estimate

    @property
    def deadline(self) -> float:
        if not self._items:
            return float("inf")
        return min(it.deadline for it in self._items)

    def to_blob(self) -> bytes:
        """Encode to the on-the-wire Kinesis Data payload."""
        return encode_aggregated([it.user_record for it in self._items])

    def routing_partition_key(self) -> str:
        """API-level partition key. ``"a"`` for aggregated batches; the
        single record's partition key otherwise.
        """
        if len(self._items) > 1:
            return "a"
        return self._items[0].user_record.partition_key

    def routing_explicit_hash_key(self) -> str | None:
        """API-level explicit hash key.

        For aggregated batches we anchor on the first record's hash key
        (mid-range of the predicted shard, guaranteed to route correctly).
        For singles we forward whatever the user provided, or ``None``.
        """
        if len(self._items) > 1:
            return str(self._items[0].hash_key)
        return self._items[0].user_record.explicit_hash_key


class Aggregator:
    """Per-shard :class:`Reducer` orchestrator producing :class:`AggregatedBatch`.

    The catch-all ``None`` reducer is used when the shard map is not READY,
    when the predicted shard falls outside the cached range, and as the only
    reducer when aggregation is globally disabled (in which case its
    ``count_limit`` is 1 — every record flushes as its own batch).

    Enter via ``async with Aggregator(...) as agg:``. The internal
    :class:`anyio.abc.TaskGroup` is shared with every per-shard
    :class:`Reducer`.
    """

    def __init__(
        self,
        shard_map: _ShardLookup,
        *,
        on_batch_ready: Callable[[AggregatedBatch], Awaitable[None]],
        aggregation_enabled: bool = True,
        record_max_buffered_time_ms: float = 100.0,
        aggregation_max_count: int = 4_294_967_295,
        aggregation_max_size: int = 51_200,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._shard_map = shard_map
        self._on_batch_ready = on_batch_ready
        self._aggregation_enabled = aggregation_enabled
        self._buffered_time = record_max_buffered_time_ms / 1000.0
        self._max_count = aggregation_max_count if aggregation_enabled else 1
        self._max_size = aggregation_max_size
        self._clock = clock

        self._reducers: dict[int | None, Reducer[_BufferedRecord, AggregatedBatch]] = {}
        self._lock = anyio.Lock()
        self._closed = False
        self._tg: TaskGroup | None = None

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> Aggregator:
        tg = anyio.create_task_group()
        await tg.__aenter__()
        self._tg = tg
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
        tg = self._tg
        self._tg = None
        assert tg is not None
        await tg.__aexit__(exc_type, exc, tb)

    # ─── Public API ────────────────────────────────────────────────────────

    async def put(self, user_record: UserRecord) -> None:
        """Compute hash key, predict shard, route to the per-shard reducer.

        If the reducer returns a closed batch (limit-trigger), dispatch it to
        ``on_batch_ready`` immediately. Deadline-triggered closures go through
        the same callback wired into each reducer.
        """
        if user_record.explicit_hash_key is not None:
            hash_key = parse_explicit_hash_key(user_record.explicit_hash_key)
        else:
            hash_key = md5_hash_key(user_record.partition_key)

        if self._shard_map.state is ShardMapState.READY:
            predicted = self._shard_map.predict(hash_key)
        else:
            predicted = None

        buffered = _BufferedRecord(
            user_record=user_record,
            deadline=self._clock() + self._buffered_time,
            hash_key=hash_key,
        )

        reducer = await self._get_or_create_reducer(predicted)
        closed = await reducer.add(buffered)
        if closed is not None:
            await self._on_batch_ready(closed)

    async def flush(self) -> None:
        """Close every per-shard batch and dispatch ``on_batch_ready``."""
        async with self._lock:
            reducers = list(self._reducers.values())
        for r in reducers:
            closed = await r.flush()
            if closed is not None:
                await self._on_batch_ready(closed)

    async def aclose(self) -> None:
        """Drop in-flight items; cancel every per-shard timer. Idempotent."""
        async with self._lock:
            self._closed = True
            reducers = list(self._reducers.values())
            self._reducers.clear()
        for r in reducers:
            await r.aclose()

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _get_or_create_reducer(
        self, predicted: int | None
    ) -> Reducer[_BufferedRecord, AggregatedBatch]:
        async with self._lock:
            existing = self._reducers.get(predicted)
            if existing is not None:
                return existing
            shard_key = predicted

            def factory() -> AggregatedBatch:
                return AggregatedBatch(predicted_shard=shard_key)

            async def on_deadline(batch: AggregatedBatch) -> None:
                await self._on_batch_ready(batch)

            tg = self._tg
            if tg is None:
                raise RuntimeError("Aggregator must be used as an async context manager")
            reducer: Reducer[_BufferedRecord, AggregatedBatch] = Reducer(
                task_group=tg,
                batch_factory=factory,
                count_limit=self._max_count,
                size_limit=self._max_size,
                on_deadline=on_deadline,
                clock=self._clock,
            )
            self._reducers[predicted] = reducer
            return reducer


__all__ = ["AggregatedBatch", "Aggregator"]
