"""In-process metric accumulator + scheduled flush onto a :class:`MetricsSink`.

Mirrors ``aws/metrics/metrics_manager.{h,cc}`` plus ``aws/metrics/accumulator.h``
of the C++ KPL. The data model is intentionally simple: each named metric owns
a rolling 60-second window of ``(count, sum, min, max)`` bucketed by integer
monotonic second. Metric names follow the C++ KPL constants verbatim so
operators reading aiokpl dashboards next to native-KPL dashboards see the
same labels.

The :class:`MetricsManager` is **off by default**: when
``level == MetricsLevel.NONE`` :meth:`put` returns immediately without any
allocation, and ``__aenter__`` spawns no upload task. The Manager flushes
aggregated snapshots onto a :class:`aiokpl.sinks.MetricsSink`. The library
itself knows nothing about CloudWatch, OpenTelemetry, or Datadog — those are
first-party sinks shipped under :mod:`aiokpl.sinks`.
"""

from __future__ import annotations

import enum
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import TracebackType

import anyio
from anyio.abc import TaskGroup

from aiokpl.sinks import (
    EventfulMetricsSink,
    MetricEvent,
    MetricSnapshot,
    MetricsSink,
    NullSink,
)


class MetricsLevel(enum.Enum):
    """How much detail :class:`MetricsManager` records.

    ``NONE`` is the zero-overhead default: :meth:`MetricsManager.put` returns
    immediately, no upload task is spawned, no sink is entered.
    ``SUMMARY`` keeps only the global+stream dimensions (``shard_id`` and
    ``error_code`` are dropped). ``DETAILED`` keeps every dimension.
    """

    NONE = "none"
    SUMMARY = "summary"
    DETAILED = "detailed"


# Metric names taken verbatim from aws/metrics/metrics_constants.h.
NAME_USER_RECORDS_RECEIVED = "UserRecordsReceived"
NAME_USER_RECORDS_PUT = "UserRecordsPut"
NAME_USER_RECORDS_DATA_PUT = "UserRecordsDataPut"
NAME_USER_RECORDS_PENDING = "UserRecordsPending"
NAME_KINESIS_RECORDS_PUT = "KinesisRecordsPut"
NAME_KINESIS_RECORDS_DATA_PUT = "KinesisRecordsDataPut"
NAME_ALL_ERRORS = "AllErrors"
NAME_ERRORS_BY_CODE = "ErrorsByCode"
NAME_RETRIES_PER_RECORD = "RetriesPerRecord"
NAME_BUFFERED_TIME = "BufferedTime"
NAME_REQUEST_TIME = "RequestTime"
NAME_EXPIRED_RECORDS = "ExpiredRecords"

_WINDOW_SECONDS = 60


@dataclass(slots=True)
class _Bucket:
    """One integer-second slot inside a rolling :class:`_Accumulator`."""

    count: int = 0
    sum: float = 0.0
    min: float = math.inf
    max: float = -math.inf


class _Accumulator:
    """Rolling 60-second window of ``(count, sum, min, max)``.

    Modelled after ``aws/metrics/accumulator.h``: a fixed-size circular
    buffer of per-second buckets. The eviction policy is lazy — buckets older
    than the window are dropped on the next :meth:`put` or :meth:`stats`. The
    class is intentionally **not** thread-safe; aiokpl is single-loop.
    """

    __slots__ = ("_buckets", "_clock", "_window")

    def __init__(
        self,
        *,
        window_seconds: int = _WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._window = window_seconds
        self._clock = clock
        self._buckets: deque[tuple[int, _Bucket]] = deque()

    def _evict(self, now_sec: int) -> None:
        cutoff = now_sec - self._window + 1
        while self._buckets and self._buckets[0][0] < cutoff:
            self._buckets.popleft()

    def put(self, value: float) -> None:
        """Record one observation in the current second's bucket."""
        now_sec = int(self._clock())
        self._evict(now_sec)
        if self._buckets and self._buckets[-1][0] == now_sec:
            bucket = self._buckets[-1][1]
        else:
            bucket = _Bucket()
            self._buckets.append((now_sec, bucket))
        bucket.count += 1
        bucket.sum += value
        if value < bucket.min:
            bucket.min = value
        if value > bucket.max:
            bucket.max = value

    def stats(self) -> tuple[int, float, float, float] | None:
        """Return ``(count, sum, min, max)`` over the live window, or ``None``."""
        now_sec = int(self._clock())
        self._evict(now_sec)
        if not self._buckets:
            return None
        count = 0
        total = 0.0
        mn = math.inf
        mx = -math.inf
        for _, b in self._buckets:
            count += b.count
            total += b.sum
            if b.min < mn:
                mn = b.min
            if b.max > mx:
                mx = b.max
        return count, total, mn, mx

    def window_bounds(self) -> tuple[float, float]:
        """Return ``(window_start, window_end)`` in seconds.

        Returns ``(0.0, 0.0)`` if the accumulator is empty.
        """
        if not self._buckets:
            return 0.0, 0.0
        return float(self._buckets[0][0]), float(self._buckets[-1][0] + 1)


@dataclass(slots=True, frozen=True)
class MetricKey:
    """The identity of a metric: ``name`` plus optional dimensions.

    Dimensions are independent — two metrics with the same ``name`` but
    different ``stream`` are distinct. Frozen so it is hashable and safe as a
    ``dict`` key.
    """

    name: str
    stream: str | None = None
    shard_id: str | None = None
    error_code: str | None = None

    def dimensions(self) -> tuple[tuple[str, str], ...]:
        """Render this key as the ``(name, value)`` tuple sinks consume."""
        dims: list[tuple[str, str]] = []
        if self.stream is not None:
            dims.append(("stream", self.stream))
        if self.shard_id is not None:
            dims.append(("shard", self.shard_id))
        if self.error_code is not None:
            dims.append(("error_code", self.error_code))
        return tuple(dims)


class Metric:
    """A named counter/distribution holding one :class:`_Accumulator`."""

    __slots__ = ("_acc", "key")

    def __init__(
        self,
        key: MetricKey,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.key = key
        self._acc = _Accumulator(clock=clock)

    def put(self, value: float = 1.0) -> None:
        self._acc.put(value)

    def stats(self) -> tuple[int, float, float, float] | None:
        return self._acc.stats()

    def window_bounds(self) -> tuple[float, float]:
        return self._acc.window_bounds()


@dataclass(slots=True)
class _UploadState:
    """Mutable state attached to an active uploader task."""

    scope: anyio.CancelScope | None = None
    last_upload_at: float = field(default=0.0)


class MetricsManager:
    """Owns the metric registry and schedules flushes onto a :class:`MetricsSink`.

    When ``level == MetricsLevel.NONE``: :meth:`put` is a no-op, no upload
    task is spawned, the sink is not entered. This is the zero-overhead path
    the Producer relies on for the default config.

    When ``level == MetricsLevel.SUMMARY``: ``shard_id`` and ``error_code``
    dimensions are dropped before registry lookup so only coarse keys exist.

    When ``level == MetricsLevel.DETAILED``: every dimension is preserved.

    The sink owns its own transport lifecycle; the manager enters and exits
    it as part of its own ``async with`` so callers only manage one context.
    """

    def __init__(
        self,
        *,
        level: MetricsLevel = MetricsLevel.NONE,
        sink: MetricsSink | None = None,
        upload_interval_ms: float = 60_000.0,
        clock: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], Awaitable[None]] = anyio.sleep,
    ) -> None:
        self._level = level
        self._sink: MetricsSink = sink if sink is not None else NullSink()
        self._upload_interval = upload_interval_ms / 1000.0
        self._clock = clock
        self._sleep_fn = sleep_fn

        self._registry: dict[MetricKey, Metric] = {}
        self._tg: TaskGroup | None = None
        self._state: _UploadState | None = None
        self._closed = False

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> MetricsManager:
        if self._level is MetricsLevel.NONE:
            # Fast path: no task group, no sink entry, no upload task.
            return self
        await self._sink.__aenter__()
        tg = anyio.create_task_group()
        await tg.__aenter__()
        self._tg = tg
        state = _UploadState()
        self._state = state
        scope = anyio.CancelScope()
        state.scope = scope
        tg.start_soon(self._upload_loop, scope)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._level is MetricsLevel.NONE:
            return
        self._closed = True
        # Final flush before tearing down so we don't lose the last window.
        await self.flush()
        state = self._state
        assert state is not None
        assert state.scope is not None
        state.scope.cancel()
        tg = self._tg
        self._tg = None
        assert tg is not None
        await tg.__aexit__(exc_type, exc, tb)
        await self._sink.__aexit__(exc_type, exc, tb)

    # ─── Public API ────────────────────────────────────────────────────────

    @property
    def level(self) -> MetricsLevel:
        return self._level

    @property
    def sink(self) -> MetricsSink:
        return self._sink

    def put(
        self,
        name: str,
        value: float = 1.0,
        *,
        stream: str | None = None,
        shard_id: str | None = None,
        error_code: str | None = None,
    ) -> None:
        """Record an observation. No-op when ``level == NONE``.

        When ``level == SUMMARY`` the ``shard_id`` and ``error_code``
        dimensions are dropped at lookup so the registry stays coarse.

        When the underlying sink implements :class:`EventfulMetricsSink`,
        :meth:`record` is invoked synchronously with a :class:`MetricEvent`.
        """
        if self._level is MetricsLevel.NONE:
            return
        if self._level is MetricsLevel.SUMMARY:
            key = MetricKey(name=name, stream=stream)
        else:
            key = MetricKey(
                name=name,
                stream=stream,
                shard_id=shard_id,
                error_code=error_code,
            )
        metric = self._registry.get(key)
        if metric is None:
            metric = Metric(key, clock=self._clock)
            self._registry[key] = metric
        metric.put(value)
        sink = self._sink
        if isinstance(sink, EventfulMetricsSink):
            sink.record(
                MetricEvent(
                    name=name,
                    value=value,
                    timestamp=self._clock(),
                    dimensions=key.dimensions(),
                )
            )

    def snapshot(self) -> dict[MetricKey, tuple[int, float, float, float]]:
        """Return ``(count, sum, min, max)`` per metric for the live window.

        Kept for backward compatibility with in-process inspectors (tests,
        embedded callers). Sinks consume :meth:`snapshots` instead.
        """
        result: dict[MetricKey, tuple[int, float, float, float]] = {}
        for key, metric in self._registry.items():
            stats = metric.stats()
            if stats is not None:
                result[key] = stats
        return result

    def snapshots(self) -> tuple[MetricSnapshot, ...]:
        """Build :class:`MetricSnapshot` instances for the live window."""
        out: list[MetricSnapshot] = []
        for key, metric in self._registry.items():
            stats = metric.stats()
            if stats is None:
                continue
            count, total, mn, mx = stats
            ws, we = metric.window_bounds()
            out.append(
                MetricSnapshot(
                    name=key.name,
                    count=count,
                    sum=total,
                    min=mn,
                    max=mx,
                    dimensions=key.dimensions(),
                    window_start=ws,
                    window_end=we,
                )
            )
        return tuple(out)

    async def flush(self) -> None:
        """Build a snapshot and call ``sink.export``. No-op when level is NONE."""
        if self._level is MetricsLevel.NONE:
            return
        snaps = self.snapshots()
        if not snaps:
            return
        await self._sink.export(snaps)

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _upload_loop(self, scope: anyio.CancelScope) -> None:
        with scope:
            while True:
                await self._sleep_fn(self._upload_interval)
                await self.flush()


__all__ = [
    "NAME_ALL_ERRORS",
    "NAME_BUFFERED_TIME",
    "NAME_ERRORS_BY_CODE",
    "NAME_EXPIRED_RECORDS",
    "NAME_KINESIS_RECORDS_DATA_PUT",
    "NAME_KINESIS_RECORDS_PUT",
    "NAME_REQUEST_TIME",
    "NAME_RETRIES_PER_RECORD",
    "NAME_USER_RECORDS_DATA_PUT",
    "NAME_USER_RECORDS_PENDING",
    "NAME_USER_RECORDS_PUT",
    "NAME_USER_RECORDS_RECEIVED",
    "Metric",
    "MetricKey",
    "MetricsLevel",
    "MetricsManager",
]
