"""Top-level :class:`Producer`: wires every previous phase together.

Mirrors ``aws/kinesis/core/kinesis_producer.{h,cc}`` and the per-stream
``aws/kinesis/core/pipeline.h`` orchestration. One :class:`Producer` instance
owns:

* a shared ``aiobotocore`` Kinesis client (one HTTP/2 connection pool reused
  across streams) lifecycle-managed by an :class:`AsyncExitStack`;
* a lazily-populated ``dict[str, _StreamPipeline]`` keyed by stream name —
  each pipeline composes a :class:`ShardMap`, :class:`Aggregator`,
  :class:`Limiter`, :class:`Collector`, :class:`Sender` and :class:`Retrier`;
* a single background :class:`anyio.abc.TaskGroup` into which collector-driven
  send/retry tasks are dispatched, decoupling intake latency from network
  latency (mirrors ``pipeline.h:206``);
* a backpressure :class:`anyio.Semaphore` capped at
  :attr:`Config.max_outstanding_records`.

The callback chain is the wiring described in ``CLAUDE.md``:

* :meth:`Aggregator.on_batch_ready`  →  :meth:`Limiter.put`
* :meth:`Limiter.on_admit`           →  :meth:`Collector.put`
* :meth:`Limiter.on_expired`         →  synthesize a :class:`SendOutcome`
  with ``request_error=("Expired", reason)`` so the Retrier classifies the
  expiry path through the same code that handles network errors.
* :meth:`Collector.on_batch_ready`   →  spawn ``sender.send + retrier.handle``
  in the background task group (so the next batch can be assembled while the
  prior is in flight).
* :meth:`Retrier.on_finish`          →  resolve the :class:`Outcome`, release
  the backpressure semaphore.
* :meth:`Retrier.on_retry`           →  :meth:`Aggregator.put_buffered`.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

import aiobotocore.session
import anyio
import anyio.lowlevel
from anyio.abc import TaskGroup

from aiokpl.aggregation import Tag, UserRecord
from aiokpl.aggregator import AggregatedBatch, Aggregator, _BufferedRecord
from aiokpl.collector import Collector, PutRecordsBatch
from aiokpl.config import Config
from aiokpl.limiter import Limiter
from aiokpl.metrics import NAME_USER_RECORDS_PENDING, MetricsLevel, MetricsManager
from aiokpl.outcome import Outcome
from aiokpl.result import RecordResult
from aiokpl.retrier import EXPIRED_ERROR_CODE, EXPIRED_ERROR_MESSAGE, Retrier
from aiokpl.sender import PerRecordOutcome, Sender, SendOutcome
from aiokpl.shard_map import ShardMap
from aiokpl.sinks import NullSink

_PENDING_REPORT_INTERVAL_S = 5.0


@dataclass(slots=True)
class _StreamPipeline:
    """Per-stream composition of :class:`ShardMap` → … → :class:`Retrier`.

    Created lazily on first :meth:`Producer.put_record` for a given stream.
    Each pipeline owns its own :class:`AsyncExitStack` so individual streams
    can in principle be torn down independently (we don't expose that
    publicly in v0.1 — the whole producer's exit closes them all).
    """

    stream: str
    shard_map: ShardMap
    aggregator: Aggregator
    limiter: Limiter
    collector: Collector
    sender: Sender
    retrier: Retrier
    pending: dict[int, Outcome[RecordResult]] = field(default_factory=dict)


class Producer:
    """Top-level KPL-equivalent producer.

    Lifecycle is ``async with Producer(config) as producer:``. On enter, opens
    the aiobotocore session and Kinesis client; per-stream pipelines are
    created lazily on first :meth:`put_record`. On exit, performs a final
    :meth:`flush` and tears every pipeline down via the shared exit stack.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._exit_stack = AsyncExitStack()
        self._kinesis_client: Any = None
        self._pipelines: dict[str, _StreamPipeline] = {}
        self._pipeline_lock = anyio.Lock()
        self._semaphore = anyio.Semaphore(config.max_outstanding_records)
        self._outstanding = 0
        self._task_group: TaskGroup | None = None
        self._closed = False
        sink = config.metrics_sink if config.metrics_sink is not None else NullSink()
        self._metrics = MetricsManager(
            level=config.metrics_level,
            sink=sink,
            upload_interval_ms=config.metrics_upload_interval_ms,
        )

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> Producer:
        await self._exit_stack.__aenter__()
        # Background task group hosts Collector-driven send/retry tasks.
        tg = anyio.create_task_group()
        await self._exit_stack.enter_async_context(tg)
        self._task_group = tg

        session = aiobotocore.session.get_session()
        client_ctx = session.create_client(
            "kinesis",
            region_name=self._config.region,
            endpoint_url=self._config.endpoint_url,
            verify=self._config.verify_ssl,
            aws_access_key_id=self._config.aws_access_key_id,
            aws_secret_access_key=self._config.aws_secret_access_key,
            aws_session_token=self._config.aws_session_token,
        )
        self._kinesis_client = await self._exit_stack.enter_async_context(client_ctx)
        await self._exit_stack.enter_async_context(self._metrics)
        if self._config.metrics_level is not MetricsLevel.NONE:
            tg.start_soon(self._pending_reporter)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if not self._closed:
                await self.flush()
        finally:
            self._closed = True
            self._task_group = None
            await self._exit_stack.__aexit__(exc_type, exc, tb)

    # ─── Public API ────────────────────────────────────────────────────────

    @property
    def outstanding_records(self) -> int:
        """Records currently in flight (between :meth:`put_record` and resolution)."""
        return self._outstanding

    @property
    def metrics(self) -> MetricsManager:
        """Read-only handle to the in-process :class:`MetricsManager`.

        Always non-None; when ``config.metrics_level == MetricsLevel.NONE``
        every :meth:`MetricsManager.put` is a no-op and the snapshot is empty.
        """
        return self._metrics

    async def put_record(
        self,
        *,
        stream: str,
        partition_key: str,
        data: bytes,
        explicit_hash_key: str | None = None,
        tags: tuple[Tag, ...] = (),
    ) -> Outcome[RecordResult]:
        """Submit a record. Returns an :class:`Outcome` resolved with the terminal result.

        Awaits the backpressure semaphore if ``max_outstanding_records`` is
        saturated. The returned Outcome resolves once the Retrier classifies
        the record as terminal — success or final failure — carrying the
        full attempt history.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")
        if not partition_key:
            raise ValueError("partition_key must be non-empty")

        await self._semaphore.acquire()
        self._outstanding += 1
        try:
            pipeline = await self._get_or_create_pipeline(stream)
            user_record = UserRecord(
                partition_key=partition_key,
                data=data,
                explicit_hash_key=explicit_hash_key,
                tags=tags,
            )
            # Build the buffered record FIRST so we can register its Outcome
            # before routing. Routing may flush synchronously, which would
            # fire on_finish — if the Outcome is not registered yet, we'd
            # have nowhere to deliver the result.
            buffered = pipeline.aggregator.build_buffered(user_record)
            outcome: Outcome[RecordResult] = Outcome()
            pipeline.pending[id(buffered)] = outcome
            await pipeline.aggregator.put_buffered(buffered)
            return outcome
        except BaseException:
            self._outstanding -= 1
            self._semaphore.release()
            raise

    async def flush(self) -> None:
        """Drain every per-stream pipeline.

        Walks aggregator → limiter → collector for each stream; the resulting
        send/retry tasks run in the background task group. Does NOT block on
        Outcome resolution — callers that want strong drain semantics should
        ``await outcome.wait()`` on every pending Outcome.
        """
        # Snapshot to avoid mutating ``_pipelines`` mid-iteration.
        async with self._pipeline_lock:
            pipelines = list(self._pipelines.values())
        for p in pipelines:
            await p.aggregator.flush()
            await p.limiter.flush()
            await p.collector.flush()
        # Give the background task group a chance to schedule the send tasks
        # the flush above enqueued; without a checkpoint they could remain
        # pending until the very next await elsewhere.
        await anyio.lowlevel.checkpoint()

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _get_or_create_pipeline(self, stream: str) -> _StreamPipeline:
        async with self._pipeline_lock:
            existing = self._pipelines.get(stream)
            if existing is not None:
                return existing
            pipeline = await self._build_pipeline(stream)
            self._pipelines[stream] = pipeline
            return pipeline

    async def _build_pipeline(self, stream: str) -> _StreamPipeline:
        cfg = self._config

        async def list_shards_fn(**kwargs: Any) -> dict[str, Any]:
            return await self._kinesis_client.list_shards(**kwargs)

        shard_map = ShardMap(stream, list_shards_fn)
        await self._exit_stack.enter_async_context(shard_map)

        # Pre-build the pipeline shell so callbacks can close over it.
        pipeline_holder: dict[str, _StreamPipeline] = {}

        async def on_aggregator_batch(batch: AggregatedBatch) -> None:
            await pipeline_holder["p"].limiter.put(batch)

        async def on_limiter_admit(batch: AggregatedBatch) -> None:
            await pipeline_holder["p"].collector.put(batch)

        async def on_limiter_expired(batch: AggregatedBatch, reason: str) -> None:
            await self._handle_expired(pipeline_holder["p"], batch, reason)

        async def on_collector_batch(batch: PutRecordsBatch) -> None:
            # Dispatch the actual PutRecords + retry classification in the
            # background task group so collecting more records is not blocked
            # on the network round-trip. Mirrors the C++ note in
            # ``pipeline.h:206`` about not hammering downstream from SDK
            # callback threads.
            tg = self._task_group
            assert tg is not None
            tg.start_soon(self._send_and_handle, pipeline_holder["p"], batch)

        async def on_retrier_finish(buffered: _BufferedRecord, result: RecordResult) -> None:
            p = pipeline_holder["p"]
            outcome = p.pending.pop(id(buffered), None)
            if outcome is not None and not outcome.is_set():
                outcome.set_value(result)
            self._outstanding -= 1
            self._semaphore.release()

        async def on_retrier_retry(buffered: _BufferedRecord) -> None:
            await pipeline_holder["p"].aggregator.put_buffered(buffered)

        aggregator = Aggregator(
            shard_map=shard_map,
            on_batch_ready=on_aggregator_batch,
            aggregation_enabled=cfg.aggregation_enabled,
            record_max_buffered_time_ms=cfg.record_max_buffered_time_ms,
            aggregation_max_count=cfg.aggregation_max_count,
            aggregation_max_size=cfg.aggregation_max_size,
            metrics=self._metrics,
            stream_name=stream,
        )
        await self._exit_stack.enter_async_context(aggregator)

        limiter = Limiter(
            on_admit=on_limiter_admit,
            on_expired=on_limiter_expired,
            records_per_sec_per_shard=cfg.rate_limit_records_per_sec_per_shard,
            bytes_per_sec_per_shard=cfg.rate_limit_bytes_per_sec_per_shard,
            expiration_ms=cfg.record_ttl_ms,
            drain_interval_ms=cfg.drain_interval_ms,
            metrics=self._metrics,
            stream_name=stream,
        )
        await self._exit_stack.enter_async_context(limiter)

        collector = Collector(
            on_batch_ready=on_collector_batch,
            collection_max_count=cfg.collection_max_count,
            collection_max_size=cfg.collection_max_size,
        )
        await self._exit_stack.enter_async_context(collector)

        sender = Sender(
            stream_name=stream,
            client=self._kinesis_client,
            metrics=self._metrics,
        )

        retrier = Retrier(
            shard_map=shard_map,
            on_finish=on_retrier_finish,
            on_retry=on_retrier_retry,
            record_ttl_ms=cfg.record_ttl_ms,
            fail_if_throttled=cfg.fail_if_throttled,
            retry_deadline_ms=cfg.retry_deadline_ms,
            metrics=self._metrics,
            stream_name=stream,
        )

        pipeline = _StreamPipeline(
            stream=stream,
            shard_map=shard_map,
            aggregator=aggregator,
            limiter=limiter,
            collector=collector,
            sender=sender,
            retrier=retrier,
        )
        pipeline_holder["p"] = pipeline

        # Kick off the initial shard-map refresh so prediction becomes
        # available quickly; not awaiting blocks here is intentional — the
        # aggregator falls back to single-record mode until READY.
        await shard_map.start()
        return pipeline

    async def _pending_reporter(self) -> None:
        # Periodic gauge of in-flight records, mirroring the C++ KPL's
        # ``UserRecordsPending`` (``kinesis_producer.cc:412-431``). One sample
        # per tick; aggregation in CloudWatch turns this into a gauge over
        # the upload interval.
        while not self._closed:
            self._metrics.put(NAME_USER_RECORDS_PENDING, float(self._outstanding))
            await anyio.sleep(_PENDING_REPORT_INTERVAL_S)

    async def _send_and_handle(self, pipeline: _StreamPipeline, batch: PutRecordsBatch) -> None:
        outcome = await pipeline.sender.send(batch)
        await pipeline.retrier.handle(outcome)

    async def _handle_expired(
        self,
        pipeline: _StreamPipeline,
        batch: AggregatedBatch,
        reason: str,
    ) -> None:
        # Synthesize a SendOutcome with a request_error so the Retrier's
        # request-error path classifies every UR in the batch as Expired.
        # Routing this through the same code as a network failure keeps the
        # terminal/attempt accounting unified — see CLAUDE.md "Failures are
        # data" and the C++ pipeline.h:170-200 region.
        now = anyio.current_time()
        synthetic = SendOutcome(
            stream_name=pipeline.stream,
            started_at=now,
            ended_at=now,
            request_error=(EXPIRED_ERROR_CODE, f"{EXPIRED_ERROR_MESSAGE}: {reason}"),
            per_record=(),
            batch_items=(batch,),
        )
        await pipeline.retrier.handle(synthetic)


# Re-export PerRecordOutcome so static-analysis sees it as used (the synthetic
# SendOutcome path leaves ``per_record`` empty but the symbol is in scope).
_ = PerRecordOutcome

__all__ = ["Producer"]
