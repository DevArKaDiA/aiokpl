"""In-process metrics with optional periodic CloudWatch upload.

Mirrors ``aws/metrics/metrics_manager.{h,cc}`` plus ``aws/metrics/accumulator.h``
of the C++ KPL. The data model is intentionally simple: each named metric owns
a rolling 60-second window of (count, sum, min, max) bucketed by integer
monotonic second. Metric names follow the C++ KPL constants verbatim so
operators reading aiokpl dashboards next to native-KPL dashboards see the
same labels.

The :class:`MetricsManager` is **off by default**: when
``level == MetricsLevel.NONE`` the :meth:`put` fast-path returns immediately
without any allocation, and ``__aenter__`` spawns no upload task. This is the
toggle the rest of the pipeline checks — stages call ``self._metrics.put(...)``
unconditionally if a manager is set, and the manager itself short-circuits.

CloudWatch upload is asyncio-only (``aiobotocore`` is asyncio-only), matching
the same constraint as the Sender (see ``CLAUDE.md`` "Concurrency model").
Stages stay backend-agnostic because they only call :meth:`put`.
"""

from __future__ import annotations

import enum
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

import anyio
from anyio.abc import TaskGroup


class MetricsLevel(enum.Enum):
    """How much detail :class:`MetricsManager` records.

    ``NONE`` is the zero-overhead default: :meth:`MetricsManager.put` returns
    immediately, no upload task is spawned, no CloudWatch client is created.
    ``SUMMARY`` keeps only the global+stream dimensions (``shard_id`` and
    ``error_code`` are dropped). ``DETAILED`` keeps every dimension.
    """

    NONE = "none"
    SUMMARY = "summary"
    DETAILED = "detailed"


# Metric names taken verbatim from aws/metrics/metrics_constants.h. Keeping
# the strings identical to the C++ KPL means operators can reuse existing
# dashboards.
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
# CloudWatch PutMetricData hard limit on MetricData entries per call.
_CLOUDWATCH_BATCH_LIMIT = 1000


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


@dataclass(slots=True)
class _UploadState:
    """Mutable state attached to an active uploader task."""

    cw_client: Any = None
    scope: anyio.CancelScope | None = None
    last_upload_at: float = field(default=0.0)


class MetricsManager:
    """Owns the metric registry and the periodic CloudWatch uploader.

    When ``level == MetricsLevel.NONE``: :meth:`put` is a no-op, no upload
    task is spawned, no CloudWatch client is ever created. This is the
    zero-overhead path the Producer relies on for the default config.

    When ``level == MetricsLevel.SUMMARY``: ``shard_id`` and ``error_code``
    dimensions are dropped before registry lookup so only coarse keys exist.

    When ``level == MetricsLevel.DETAILED``: every dimension is preserved.
    """

    def __init__(
        self,
        *,
        level: MetricsLevel = MetricsLevel.NONE,
        namespace: str = "aiokpl",
        upload_interval_ms: float = 60_000.0,
        cw_client_factory: Callable[[], AbstractAsyncContextManager[Any]] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], Awaitable[None]] = anyio.sleep,
    ) -> None:
        self._level = level
        self._namespace = namespace
        self._upload_interval = upload_interval_ms / 1000.0
        self._cw_client_factory = cw_client_factory
        self._clock = clock
        self._sleep_fn = sleep_fn

        self._registry: dict[MetricKey, Metric] = {}
        self._tg: TaskGroup | None = None
        self._state: _UploadState | None = None
        self._client_ctx: AbstractAsyncContextManager[Any] | None = None
        self._closed = False

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> MetricsManager:
        if self._level is MetricsLevel.NONE:
            # Fast path: no task group, no client, no upload task. The
            # manager is a no-op container.
            return self
        tg = anyio.create_task_group()
        await tg.__aenter__()
        self._tg = tg
        state = _UploadState()
        self._state = state
        if self._cw_client_factory is not None:
            client_ctx = self._cw_client_factory()
            state.cw_client = await client_ctx.__aenter__()
            self._client_ctx = client_ctx
        scope = anyio.CancelScope()
        state.scope = scope
        tg.start_soon(self._upload_loop, scope, state.cw_client)
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
        state = self._state
        assert state is not None
        # Final upload before tearing down so we don't lose the last window.
        if state.cw_client is not None:
            await self._upload_now(state.cw_client)
        assert state.scope is not None
        state.scope.cancel()
        tg = self._tg
        self._tg = None
        assert tg is not None
        await tg.__aexit__(exc_type, exc, tb)
        ctx = self._client_ctx
        if ctx is not None:
            await ctx.__aexit__(exc_type, exc, tb)

    # ─── Public API ────────────────────────────────────────────────────────

    @property
    def level(self) -> MetricsLevel:
        return self._level

    @property
    def namespace(self) -> str:
        return self._namespace

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

    def snapshot(self) -> dict[MetricKey, tuple[int, float, float, float]]:
        """Return ``(count, sum, min, max)`` per metric for the live window."""
        result: dict[MetricKey, tuple[int, float, float, float]] = {}
        for key, metric in self._registry.items():
            stats = metric.stats()
            if stats is not None:
                result[key] = stats
        return result

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _upload_loop(self, scope: anyio.CancelScope, cw_client: Any) -> None:
        # ``cw_client`` is captured at task-spawn time; when no factory was
        # provided the loop still runs but skips the upload step so the
        # in-memory snapshot stays alive for tests/inspection.
        with scope:
            while True:
                await self._sleep_fn(self._upload_interval)
                if cw_client is not None:
                    await self._upload_now(cw_client)

    async def _upload_now(self, cw_client: Any) -> None:
        snap = self.snapshot()
        if not snap:
            return
        datums = [self._key_to_datum(key, stats) for key, stats in snap.items()]
        for i in range(0, len(datums), _CLOUDWATCH_BATCH_LIMIT):
            chunk = datums[i : i + _CLOUDWATCH_BATCH_LIMIT]
            await cw_client.put_metric_data(
                Namespace=self._namespace,
                MetricData=chunk,
            )

    def _key_to_datum(
        self,
        key: MetricKey,
        stats: tuple[int, float, float, float],
    ) -> dict[str, Any]:
        count, total, mn, mx = stats
        dims: list[dict[str, str]] = []
        if key.stream is not None:
            dims.append({"Name": "StreamName", "Value": key.stream})
        if key.shard_id is not None:
            dims.append({"Name": "ShardId", "Value": key.shard_id})
        if key.error_code is not None:
            dims.append({"Name": "ErrorCode", "Value": key.error_code})
        return {
            "MetricName": key.name,
            "Dimensions": dims,
            "StatisticValues": {
                "SampleCount": float(count),
                "Sum": total,
                "Minimum": mn,
                "Maximum": mx,
            },
        }


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
