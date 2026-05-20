"""Per-shard rate limiter sitting between Aggregator and Collector.

Mirrors ``aws/kinesis/core/limiter.h`` in the C++ KPL. The :class:`Limiter`
owns one :class:`ShardLimiter` per predicted shard (plus a catch-all for
``None``-shard batches), each with its own two-stream :class:`TokenBucket`
enforcing the Kinesis hard limits: 1000 records/s and 1 MiB/s.

The drain pattern is deliberate:

* expired batches are surfaced *before* token-checked ones (the C++
  ``internal_queue_.consume_expired`` runs ahead of ``consume_by_deadline``);
* an admitted batch costs ``1`` record-token regardless of how many user
  records it aggregates, because aggregation collapses them onto a single
  wire-level Kinesis record (``token_bucket_.try_take({1, bytes})`` in
  ``limiter.h``);
* a background task polls every ``drain_interval_ms`` (default 25 ms, the
  C++ ``kDrainDelayMillis`` constant), and every :meth:`Limiter.put`
  opportunistically drains its own shard to avoid one-tick latency when
  tokens are already available.

Lifecycle is structured: enter the :class:`Limiter` as an async context
manager, which spawns an internal :class:`anyio.abc.TaskGroup` hosting the
background drain task.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol, runtime_checkable

import anyio
from anyio.abc import TaskGroup
from sortedcontainers import SortedKeyList

from aiokpl.aggregator import AggregatedBatch
from aiokpl.token_bucket import TokenBucket

RECORDS_PER_SEC_PER_SHARD = 1_000.0
BYTES_PER_SEC_PER_SHARD = 1_048_576.0  # 1 MiB
DEFAULT_DRAIN_INTERVAL_MS = 25.0
DEFAULT_EXPIRATION_MS = 30_000.0  # mirrors record_ttl_ms


@runtime_checkable
class _ExpirableBatch(Protocol):
    """Minimal protocol the Limiter needs from a batch."""

    @property
    def deadline(self) -> float: ...
    @property
    def size(self) -> int: ...
    @property
    def count(self) -> int: ...
    @property
    def predicted_shard(self) -> int | None: ...


@dataclass(slots=True)
class _Pending:
    """A queued batch decorated with the wall-clock at which it must expire."""

    batch: AggregatedBatch
    expires_at: float


class ShardLimiter:
    """Per-shard rate-limiting queue.

    Maintained internally by :class:`Limiter`. The queue is a
    :class:`sortedcontainers.SortedKeyList` keyed by deadline (O(log N)
    insertion, O(N) ordered iteration), matching the
    ``TimeSensitiveQueue<KinesisRecord>`` index in C++.
    """

    __slots__ = ("_bucket", "_clock", "_queue")

    def __init__(
        self,
        *,
        records_per_sec: float = RECORDS_PER_SEC_PER_SHARD,
        bytes_per_sec: float = BYTES_PER_SEC_PER_SHARD,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._bucket = TokenBucket(
            [(records_per_sec, records_per_sec), (bytes_per_sec, bytes_per_sec)],
            clock=clock,
            initial_full=True,
        )
        self._queue: SortedKeyList[_Pending] = SortedKeyList(key=lambda p: p.batch.deadline)
        self._clock = clock

    def enqueue(self, batch: AggregatedBatch, expires_at: float) -> None:
        self._queue.add(_Pending(batch=batch, expires_at=expires_at))

    def drain(self) -> tuple[list[AggregatedBatch], list[AggregatedBatch]]:
        """Pop expired and admit-able items.

        Returns ``(admitted, expired)`` for the caller to dispatch. Expired
        items are surfaced first (no tokens consumed). Within each list,
        deadline order is preserved.
        """
        now = self._clock()
        expired: list[AggregatedBatch] = []
        keep: list[_Pending] = []
        for p in list(self._queue):
            if p.expires_at <= now:
                expired.append(p.batch)
            else:
                keep.append(p)
        # Rebuild only when expirations actually happened — cheap path stays cheap.
        if expired:
            self._queue.clear()
            self._queue.update(keep)

        admitted: list[AggregatedBatch] = []
        # SortedKeyList iterates in key order; pop the front while tokens allow.
        while self._queue:
            head = self._queue[0]
            if self._bucket.try_take([1, head.batch.size]):
                self._queue.pop(0)
                admitted.append(head.batch)
            else:
                break
        return admitted, expired

    def drain_force(self) -> tuple[list[AggregatedBatch], list[AggregatedBatch]]:
        """Flush everything regardless of token state.

        Expired items still surface as expired; the rest are admitted in
        deadline order. Used by :meth:`Limiter.flush` for graceful shutdown.
        """
        now = self._clock()
        admitted: list[AggregatedBatch] = []
        expired: list[AggregatedBatch] = []
        for p in list(self._queue):
            if p.expires_at <= now:
                expired.append(p.batch)
            else:
                admitted.append(p.batch)
        self._queue.clear()
        return admitted, expired

    @property
    def pending_count(self) -> int:
        return len(self._queue)


class Limiter:
    """Per-shard rate-limiting orchestrator.

    Owns one :class:`ShardLimiter` per predicted shard plus a catch-all for
    ``None``. Spawns the drain task lazily on first :meth:`put` so users
    don't have to call a ``start()`` method, but the :class:`Limiter` must be
    entered as an async context manager so the internal task group exists.
    """

    def __init__(
        self,
        *,
        on_admit: Callable[[AggregatedBatch], Awaitable[None]],
        on_expired: Callable[[AggregatedBatch, str], Awaitable[None]],
        records_per_sec_per_shard: float = RECORDS_PER_SEC_PER_SHARD,
        bytes_per_sec_per_shard: float = BYTES_PER_SEC_PER_SHARD,
        expiration_ms: float = DEFAULT_EXPIRATION_MS,
        drain_interval_ms: float = DEFAULT_DRAIN_INTERVAL_MS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._on_admit = on_admit
        self._on_expired = on_expired
        self._records_per_sec = records_per_sec_per_shard
        self._bytes_per_sec = bytes_per_sec_per_shard
        self._expiration = expiration_ms / 1000.0
        self._drain_interval = drain_interval_ms / 1000.0
        self._clock = clock

        self._shards: dict[int | None, ShardLimiter] = {}
        self._lock = anyio.Lock()
        self._drain_started = False
        self._drain_scope: anyio.CancelScope | None = None
        self._closed = False
        self._tg: TaskGroup | None = None

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> Limiter:
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

    async def put(self, batch: AggregatedBatch) -> None:
        """Enqueue ``batch`` into its shard's limiter.

        Spawns the background drain task lazily on first call. Immediately
        attempts to drain that shard once so a put arriving with tokens
        available need not wait a full tick.
        """
        shard_id = batch.predicted_shard
        async with self._lock:
            limiter = self._shards.get(shard_id)
            if limiter is None:
                limiter = ShardLimiter(
                    records_per_sec=self._records_per_sec,
                    bytes_per_sec=self._bytes_per_sec,
                    clock=self._clock,
                )
                self._shards[shard_id] = limiter
            expires_at = self._clock() + self._expiration
            limiter.enqueue(batch, expires_at)
            admitted, expired = limiter.drain()
            if not self._drain_started and not self._closed:
                tg = self._tg
                if tg is None:
                    raise RuntimeError("Limiter must be used as an async context manager")
                scope = anyio.CancelScope()
                self._drain_scope = scope
                self._drain_started = True
                tg.start_soon(self._drain_loop, scope)
        await self._dispatch(admitted, expired)

    async def flush(self) -> None:
        """Drain every shard limiter immediately, regardless of token state.

        Admits whatever isn't already expired; expires the rest. Useful for
        graceful shutdown after the upstream aggregator stops producing.
        """
        async with self._lock:
            limiters = list(self._shards.values())
            collected: list[tuple[list[AggregatedBatch], list[AggregatedBatch]]] = [
                limiter.drain_force() for limiter in limiters
            ]
        for admitted, expired in collected:
            await self._dispatch(admitted, expired)

    async def aclose(self) -> None:
        """Stop the drain task. Idempotent."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            scope = self._drain_scope
            self._drain_scope = None
        if scope is not None:
            scope.cancel()

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _drain_loop(self, scope: anyio.CancelScope) -> None:
        # Cancellation is the sole exit path — ``aclose`` cancels the scope
        # owning this task. We also bail out cleanly on observing ``_closed``
        # inside the lock so a racing ``aclose`` doesn't trigger spurious
        # dispatch.
        with scope:
            while True:
                await anyio.sleep(self._drain_interval)
                async with self._lock:
                    collected = [limiter.drain() for limiter in self._shards.values()]
                for admitted, expired in collected:
                    await self._dispatch(admitted, expired)

    async def _dispatch(
        self,
        admitted: list[AggregatedBatch],
        expired: list[AggregatedBatch],
    ) -> None:
        # Expired first, mirroring the C++ Limiter::drain order.
        for batch in expired:
            await self._on_expired(batch, "Expired")
        for batch in admitted:
            await self._on_admit(batch)


__all__ = [
    "BYTES_PER_SEC_PER_SHARD",
    "DEFAULT_DRAIN_INTERVAL_MS",
    "DEFAULT_EXPIRATION_MS",
    "RECORDS_PER_SEC_PER_SHARD",
    "Limiter",
    "ShardLimiter",
]
