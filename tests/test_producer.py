"""Unit tests for :class:`aiokpl.producer.Producer`.

We swap aiobotocore's session for a fake whose ``create_client`` returns an
async-context-manager wrapping a :class:`_FakeClient`. The fake satisfies the
shape the Producer touches: ``put_records`` and ``list_shards``. Tests cover
the entire callback chain Aggregator → Limiter → Collector → Sender → Retrier,
the expired branch, the throttle/fail-if-throttled branch, backpressure, and
multi-stream lazy pipeline creation.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import anyio
import anyio.lowlevel
import pytest

import aiokpl.producer as producer_mod
from aiokpl.config import Config
from aiokpl.outcome import Outcome
from aiokpl.producer import Producer
from aiokpl.result import RecordResult


# aiobotocore is asyncio-only — see CLAUDE.md, "Concurrency model". The
# Producer composes aiobotocore via its client, so its tests live on
# asyncio only. The cross-backend matrix is exercised by the lower
# (Phase 1-4) modules whose anyio_backend fixture parametrizes both.
@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ────────────────────────────────────────────────────────────────────────────
# Fakes
# ────────────────────────────────────────────────────────────────────────────


class _FakeClient:
    """Duck-typed stand-in for an aiobotocore Kinesis client.

    Per-stream queues of ``put_records`` responses; if a response is callable
    it is invoked with the kwargs (used for the throttle-then-success test).
    A queued ``BaseException`` is raised in place of returning a dict.
    ``list_shards`` returns a canned 2-shard, full-uint128 split.
    """

    def __init__(self) -> None:
        self.put_responses: list[Any] = []
        self.list_shards_responses: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self.list_calls: list[dict[str, Any]] = []

    def queue_put(self, response: Any) -> None:
        self.put_responses.append(response)

    def queue_list_shards(self, response: dict[str, Any]) -> None:
        self.list_shards_responses.append(response)

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if not self.put_responses:
            raise RuntimeError("no queued put_records response")
        response = self.put_responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(kwargs)
        return response

    async def list_shards(self, **kwargs: Any) -> dict[str, Any]:
        self.list_calls.append(kwargs)
        if self.list_shards_responses:
            return self.list_shards_responses.pop(0)
        return _single_shard_response()

    async def close(self) -> None:
        return None


def _single_shard_response() -> dict[str, Any]:
    return {
        "Shards": [
            {
                "ShardId": "shardId-0",
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": str((1 << 128) - 1),
                },
            },
        ],
    }


def _two_shard_response() -> dict[str, Any]:
    half = (1 << 128) // 2
    return {
        "Shards": [
            {
                "ShardId": "shardId-0",
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": str(half - 1),
                },
            },
            {
                "ShardId": "shardId-1",
                "HashKeyRange": {
                    "StartingHashKey": str(half),
                    "EndingHashKey": str((1 << 128) - 1),
                },
            },
        ],
    }


class _FakeClientCtx:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    async def __aenter__(self) -> _FakeClient:
        return self._client

    async def __aexit__(self, *exc_info: Any) -> None:
        await self._client.close()


class _FakeSession:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client
        self.create_client_kwargs: dict[str, Any] = {}

    def create_client(self, service_name: str, **kwargs: Any) -> Any:
        assert service_name == "kinesis"
        self.create_client_kwargs = kwargs
        return _FakeClientCtx(self._client)


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeClient]:
    client = _FakeClient()
    session = _FakeSession(client)

    def fake_get_session() -> _FakeSession:
        return session

    monkeypatch.setattr(producer_mod.aiobotocore.session, "get_session", fake_get_session)
    yield client


def _ok_response(seq: str = "seq-1", shard: str = "shardId-0") -> dict[str, Any]:
    return {
        "Records": [{"SequenceNumber": seq, "ShardId": shard}],
        "FailedRecordCount": 0,
    }


def _multi_ok_response(*pairs: tuple[str, str]) -> dict[str, Any]:
    return {
        "Records": [{"SequenceNumber": s, "ShardId": sid} for s, sid in pairs],
        "FailedRecordCount": 0,
    }


# ────────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────────


def _cfg(**overrides: Any) -> Config:
    return Config(region="us-east-1", **overrides)


async def test_happy_path_single_record(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-A", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="stream-1",
            partition_key="user-A",
            data=b"hello",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert isinstance(result, RecordResult)
        assert result.success is True
        assert result.sequence_number == "seq-A"
        assert result.shard_id == "shardId-0"
        assert len(result.attempts) >= 1
        assert result.attempts[-1].success is True
    assert producer.outstanding_records == 0


async def test_two_records_different_shards(fake_client: _FakeClient) -> None:
    # Use explicit hash keys to force different predicted shards.
    half = (1 << 128) // 2
    fake_client.queue_list_shards(_two_shard_response())
    fake_client.queue_put(_multi_ok_response(("seq-0", "shardId-0"), ("seq-1", "shardId-1")))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        o0 = await producer.put_record(
            stream="s",
            partition_key="pk0",
            data=b"a",
            explicit_hash_key="0",
        )
        o1 = await producer.put_record(
            stream="s",
            partition_key="pk1",
            data=b"b",
            explicit_hash_key=str(half),
        )
        with anyio.fail_after(5.0):
            r0 = await o0.wait()
            r1 = await o1.wait()
        assert r0.success and r1.success
        # Both flushed in the same PutRecords call (one collector batch).
        assert len(fake_client.put_calls) == 1
        seqs = {r0.sequence_number, r1.sequence_number}
        assert seqs == {"seq-0", "seq-1"}


async def test_aggregation_merges_same_shard(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-AGG", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        o0 = await producer.put_record(
            stream="s",
            partition_key="pk-a",
            data=b"a",
            explicit_hash_key="0",
        )
        o1 = await producer.put_record(
            stream="s",
            partition_key="pk-b",
            data=b"b",
            explicit_hash_key="1",
        )
        with anyio.fail_after(5.0):
            r0 = await o0.wait()
            r1 = await o1.wait()
        assert r0.success and r1.success
        assert r0.sequence_number == r1.sequence_number == "seq-AGG"
        assert r0.shard_id == r1.shard_id == "shardId-0"
        # One PutRecords containing one aggregated record.
        assert len(fake_client.put_calls) == 1
        records = fake_client.put_calls[0]["Records"]
        assert len(records) == 1


async def test_retry_then_success(fake_client: _FakeClient) -> None:
    # First call returns a per-record failure; second returns success.
    failed = {
        "Records": [
            {
                "ErrorCode": "InternalFailure",
                "ErrorMessage": "transient",
            }
        ],
        "FailedRecordCount": 1,
    }
    success = _ok_response("seq-RETRY", "shardId-0")
    fake_client.queue_put(failed)
    fake_client.queue_put(success)
    cfg = _cfg(record_max_buffered_time_ms=20.0, retry_deadline_ms=20.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is True
        assert result.sequence_number == "seq-RETRY"
        assert len(result.attempts) >= 2
        assert result.attempts[0].success is False
        assert result.attempts[-1].success is True


async def test_throttle_fail_if_throttled(fake_client: _FakeClient) -> None:
    throttled = {
        "Records": [
            {
                "ErrorCode": "ProvisionedThroughputExceededException",
                "ErrorMessage": "rate exceeded",
            }
        ],
        "FailedRecordCount": 1,
    }
    fake_client.queue_put(throttled)
    cfg = _cfg(
        record_max_buffered_time_ms=20.0,
        fail_if_throttled=True,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is False
        assert result.attempts[-1].error_code == "ProvisionedThroughputExceededException"
        # Exactly one Sender trip — no retry.
        assert len(fake_client.put_calls) == 1


async def test_expired_via_tiny_ttl(fake_client: _FakeClient) -> None:
    # First call: per-record failure. Retry must observe the record as expired
    # because record_ttl_ms is tiny. The retrier's _retry_not_expired path
    # appends an Expired attempt and finishes with success=False.
    failed = {
        "Records": [
            {
                "ErrorCode": "InternalFailure",
                "ErrorMessage": "transient",
            }
        ],
        "FailedRecordCount": 1,
    }
    fake_client.queue_put(failed)
    cfg = _cfg(
        record_max_buffered_time_ms=5.0,
        record_ttl_ms=1.0,
        retry_deadline_ms=5.0,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        # Let the limiter / sender complete and the retry check fail.
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is False
        # The terminal classification should mark the record as Expired.
        assert result.attempts[-1].error_code == "Expired"


async def test_backpressure_blocks_third_put(fake_client: _FakeClient) -> None:
    half = (1 << 128) // 2
    # Two distinct PutRecords calls because the first two records share a
    # collector flush but the third arrives after the semaphore unblocks.
    # Drive the pipeline with explicit flush() calls rather than relying
    # on the 20 ms deadline timer — on loaded CI hosts the timer can be
    # starved long enough to wedge the test.
    fake_client.queue_list_shards(_two_shard_response())
    fake_client.queue_put(_multi_ok_response(("s-0", "shardId-0"), ("s-1", "shardId-1")))
    fake_client.queue_put(_ok_response("s-2", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=10_000.0, max_outstanding_records=2)

    third_finished = anyio.Event()
    third_started = anyio.Event()
    third_container: list[Outcome[RecordResult]] = []

    async def submit_third(producer: Producer) -> None:
        third_started.set()
        out = await producer.put_record(
            stream="s",
            partition_key="pk-third",
            data=b"c",
            explicit_hash_key="2",
        )
        third_container.append(out)
        third_finished.set()

    async with Producer(cfg) as producer:
        o0 = await producer.put_record(
            stream="s",
            partition_key="pk-a",
            data=b"a",
            explicit_hash_key="0",
        )
        o1 = await producer.put_record(
            stream="s",
            partition_key="pk-b",
            data=b"b",
            explicit_hash_key=str(half),
        )
        assert producer.outstanding_records == 2

        async with anyio.create_task_group() as tg:
            tg.start_soon(submit_third, producer)
            # Wait for the third task to be scheduled and immediately block
            # on the semaphore (max_outstanding_records=2 is already used).
            with anyio.fail_after(5.0):
                await third_started.wait()
            await anyio.sleep(0.05)  # give the semaphore.acquire a chance to block
            assert not third_finished.is_set()
            assert producer.outstanding_records == 2

            # Explicit flush forces the first two records through the
            # pipeline without depending on the deadline timer firing.
            await producer.flush()
            with anyio.fail_after(10.0):
                await o0.wait()
                await o1.wait()
            # First two outcomes resolved → semaphore released → submit_third
            # proceeds → third record enters the pipeline. Flush again so it
            # actually goes to PutRecords without waiting on a timer.
            with anyio.fail_after(5.0):
                await third_finished.wait()
            await producer.flush()
            with anyio.fail_after(10.0):
                await third_container[0].wait()

        assert len(third_container) == 1


async def test_multi_stream_lazy_pipelines(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-X", "shardId-0"))
    fake_client.queue_put(_ok_response("seq-Y", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        oX = await producer.put_record(
            stream="stream-X",
            partition_key="px",
            data=b"x",
            explicit_hash_key="0",
        )
        oY = await producer.put_record(
            stream="stream-Y",
            partition_key="py",
            data=b"y",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            rX = await oX.wait()
            rY = await oY.wait()
        assert rX.sequence_number == "seq-X"
        assert rY.sequence_number == "seq-Y"
        streams = {call["StreamName"] for call in fake_client.put_calls}
        assert streams == {"stream-X", "stream-Y"}


async def test_outstanding_records_tracks(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        assert producer.outstanding_records == 0
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        assert producer.outstanding_records == 1
        with anyio.fail_after(5.0):
            await outcome.wait()
        # After resolution, the on_finish callback decrements.
        await anyio.lowlevel.checkpoint()
        assert producer.outstanding_records == 0


async def test_flush_drains_pending(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    # Very long buffered time: without flush, the record would sit in the
    # aggregator until the deadline timer fires.
    cfg = _cfg(record_max_buffered_time_ms=60_000.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        await producer.flush()
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is True


async def test_aexit_flushes_and_blocks_further_puts(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-Z", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=60_000.0)
    producer = Producer(cfg)
    await producer.__aenter__()
    outcome = await producer.put_record(
        stream="s",
        partition_key="pk",
        data=b"x",
        explicit_hash_key="0",
    )
    await producer.__aexit__(None, None, None)
    with anyio.fail_after(5.0):
        result = await outcome.wait()
    assert result.success is True
    with pytest.raises(RuntimeError, match="closed"):
        await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"y",
            explicit_hash_key="0",
        )


async def test_empty_partition_key_raises(fake_client: _FakeClient) -> None:
    cfg = _cfg()
    async with Producer(cfg) as producer:
        with pytest.raises(ValueError, match="partition_key"):
            await producer.put_record(
                stream="s",
                partition_key="",
                data=b"x",
            )


async def test_empty_data_ok(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is True


async def test_expired_via_limiter_synthetic_outcome(
    fake_client: _FakeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force the limiter to expire batches by patching the ShardLimiter's
    # internal token bucket to always refuse. We accomplish this indirectly
    # by setting rate limits so low that no batch can ever be admitted, and
    # by setting record_ttl_ms tiny so the limiter expires items before the
    # drain loop can admit them. The synthetic SendOutcome path through the
    # Retrier should then mark each record terminal with code "Expired".
    cfg = _cfg(
        record_max_buffered_time_ms=5.0,
        record_ttl_ms=2.0,
        rate_limit_records_per_sec_per_shard=0.0,
        rate_limit_bytes_per_sec_per_shard=0.0,
        retry_deadline_ms=1.0,
        drain_interval_ms=5.0,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is False
        assert result.attempts[-1].error_code == "Expired"


async def test_request_error_raises_no_double_set(fake_client: _FakeClient) -> None:
    # botocore-style ClientError: the sender catches BaseException and
    # surfaces it via SendOutcome.request_error, routing through the
    # request-error branch in the Retrier.
    fake_client.queue_put(RuntimeError("boom"))
    fake_client.queue_put(_ok_response("seq-OK", "shardId-0"))
    cfg = _cfg(
        record_max_buffered_time_ms=20.0,
        retry_deadline_ms=10.0,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            result = await outcome.wait()
        assert result.success is True
        assert result.sequence_number == "seq-OK"
        # First attempt was the boom failure, the retry succeeded.
        assert len(result.attempts) >= 2
        assert result.attempts[0].success is False


async def test_aexit_skips_flush_when_already_closed(fake_client: _FakeClient) -> None:
    """Cover the `_closed=True` short-circuit branch in __aexit__."""
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    producer = Producer(cfg)
    await producer.__aenter__()
    # Force the closed flag so the next __aexit__ skips flush().
    producer._closed = True
    flush_called = [False]
    original_flush = producer.flush

    async def tracking_flush() -> None:
        flush_called[0] = True
        await original_flush()

    # Shadow the bound method on the instance to observe whether __aexit__
    # calls it; static type-check noise is suppressed by going through
    # __dict__ directly.
    producer.__dict__["flush"] = tracking_flush
    await producer.__aexit__(None, None, None)
    assert flush_called[0] is False


async def test_finish_callback_handles_unknown_buffered(fake_client: _FakeClient) -> None:
    """Cover the `outcome is None or already set` branch in on_retrier_finish.

    Trigger this by calling the registered callback with a buffered whose id
    was never registered in pending — what would happen if the retrier
    classified a record we didn't track.
    """
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            await outcome.wait()
        pipeline = producer._pipelines["s"]
        # Synthesize an unknown buffered and feed it back through the retrier
        # via the on_finish callback. The semaphore is replenished without an
        # Outcome resolution.
        from aiokpl.aggregation import UserRecord
        from aiokpl.aggregator import _BufferedRecord
        from aiokpl.result import RecordResult

        rogue = _BufferedRecord(
            user_record=UserRecord(partition_key="pk", data=b""),
            deadline=0.0,
            hash_key=0,
        )
        rr = RecordResult(success=True, shard_id="shardId-0", sequence_number="x", attempts=())
        # Pre-acquire one slot so the bookkeeping balances.
        await producer._semaphore.acquire()
        producer._outstanding += 1
        await pipeline.retrier._on_finish(rogue, rr)
        # Counter went back down; no outcome was set (since none registered).
        assert producer.outstanding_records == 0


# ────────────────────────────────────────────────────────────────────────────
# Metrics integration
# ────────────────────────────────────────────────────────────────────────────


async def test_producer_metrics_property_default_none(fake_client: _FakeClient) -> None:
    cfg = _cfg()
    async with Producer(cfg) as producer:
        assert producer.metrics.level.value == "none"
        assert producer.metrics.snapshot() == {}


async def test_producer_metrics_records_user_records_received_and_put(
    fake_client: _FakeClient,
) -> None:
    from aiokpl.metrics import (
        NAME_REQUEST_TIME,
        NAME_USER_RECORDS_PUT,
        NAME_USER_RECORDS_RECEIVED,
        MetricsLevel,
    )
    from aiokpl.sinks import InMemorySink

    fake_client.queue_put(_ok_response("seq-A", "shardId-0"))
    sink = InMemorySink()
    cfg = _cfg(
        record_max_buffered_time_ms=20.0,
        metrics_level=MetricsLevel.DETAILED,
        metrics_sink=sink,
        metrics_upload_interval_ms=60_000.0,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="stream-1",
            partition_key="pk",
            data=b"hello",
        )
        with anyio.fail_after(5.0):
            await outcome.wait()
        # Drive one iteration of the pending reporter without waiting 5s.
        await anyio.lowlevel.checkpoint()
        snap = producer.metrics.snapshot()
        keys_by_name = {k.name for k in snap}
        assert NAME_USER_RECORDS_RECEIVED in keys_by_name
        assert NAME_USER_RECORDS_PUT in keys_by_name
        assert NAME_REQUEST_TIME in keys_by_name
    # Final __aexit__ flush surfaces snapshots in the sink.
    names = {s.name for s in sink.all_snapshots}
    assert NAME_USER_RECORDS_RECEIVED in names


async def test_pending_reporter_emits_with_injected_sink(
    fake_client: _FakeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Exercises the periodic pending reporter and the injected-sink path
    # (lines covered: ``_pending_reporter`` loop + sink lifecycle wiring).
    from aiokpl.metrics import NAME_USER_RECORDS_PENDING, MetricsLevel
    from aiokpl.sinks import InMemorySink

    monkeypatch.setattr(producer_mod, "_PENDING_REPORT_INTERVAL_S", 0.001)
    sink = InMemorySink()
    cfg = _cfg(
        record_max_buffered_time_ms=20.0,
        metrics_level=MetricsLevel.DETAILED,
        metrics_sink=sink,
        metrics_upload_interval_ms=3_600_000.0,  # effectively never
    )
    async with Producer(cfg) as producer:
        with anyio.fail_after(5.0):
            while True:
                await anyio.sleep(0.005)
                snap = producer.metrics.snapshot()
                if any(k.name == NAME_USER_RECORDS_PENDING for k in snap):
                    break


async def test_put_record_releases_semaphore_on_internal_failure(
    fake_client: _FakeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If aggregator.put_buffered raises, the semaphore is released."""
    cfg = _cfg(record_max_buffered_time_ms=20.0, max_outstanding_records=1)
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    async with Producer(cfg) as producer:
        # Prime the pipeline so put_buffered has something to patch.
        first = await producer.put_record(
            stream="s",
            partition_key="pk",
            data=b"x",
            explicit_hash_key="0",
        )
        with anyio.fail_after(5.0):
            await first.wait()
        # Now force put_buffered to raise so we can verify the release path.
        pipeline = producer._pipelines["s"]

        async def boom(_buf: Any) -> None:
            raise RuntimeError("pipeline broken")

        monkeypatch.setattr(pipeline.aggregator, "put_buffered", boom)
        before = producer.outstanding_records
        with pytest.raises(RuntimeError, match="pipeline broken"):
            await producer.put_record(
                stream="s",
                partition_key="pk",
                data=b"x",
                explicit_hash_key="0",
            )
        # Semaphore released and counter rolled back.
        assert producer.outstanding_records == before
