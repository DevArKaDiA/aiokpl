"""End-to-end integration test for Sender + Retrier against kinesis-mock.

The unit tests cover every cell of the classification table with hand-built
:class:`SendOutcome` instances. This test instead proves the wiring: a real
:class:`ShardMap`, a real :class:`Sender` wrapping kinesis-mock's HTTP API,
and a :class:`Retrier` with capturing callbacks. Two records land on two
different shards, both succeed, and the Retrier resolves them through
``on_finish``.

Aligned with ``tests/integration/test_shard_map_integration.py``: kinesis-mock
is sync (botocore), so we wrap each call with ``anyio.to_thread.run_sync`` —
this keeps the harness backend-agnostic without pulling aiobotocore into the
integration surface.
"""

from __future__ import annotations

import contextlib
import functools
import time
from typing import Any
from uuid import uuid4

import anyio
import anyio.to_thread
import pytest

from aiokpl.aggregation import UserRecord
from aiokpl.aggregator import AggregatedBatch, _BufferedRecord
from aiokpl.collector import PutRecordsBatch
from aiokpl.hashing import md5_hash_key
from aiokpl.result import RecordResult
from aiokpl.retrier import Retrier
from aiokpl.sender import Sender
from aiokpl.shard_map import ShardMap, ShardMapState


async def _to_thread(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await anyio.to_thread.run_sync(functools.partial(fn, *args, **kwargs))


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


class _SyncClientAsyncAdapter:
    """Adapt a sync botocore Kinesis client into the async ``_KinesisClient`` Protocol."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        return await _to_thread(self._client.put_records, **kwargs)

    async def list_shards(self, **kwargs: Any) -> dict[str, Any]:
        return await _to_thread(self._client.list_shards, **kwargs)


@pytest.mark.integration
async def test_sender_retrier_end_to_end(kinesis_client: Any) -> None:
    stream_name = f"aiokpl-retr-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=2)
    try:
        await _to_thread(_wait_stream_active, kinesis_client, stream_name)
        adapter = _SyncClientAsyncAdapter(kinesis_client)

        async with ShardMap(stream_name, adapter.list_shards) as sm:
            await sm.start()
            assert sm.state is ShardMapState.READY

            # Pick two partition keys whose md5 hash routes them to different
            # shards according to ShardMap. Cheap brute force: try a handful.
            seen: dict[int, str] = {}
            for i in range(2_000):
                pk = f"probe-{i}"
                shard = sm.predict(md5_hash_key(pk))
                if shard is not None and shard not in seen:
                    seen[shard] = pk
                if len(seen) >= 2:
                    break
            assert len(seen) >= 2, "could not produce keys for two distinct shards"

            batch = PutRecordsBatch()
            urs: list[UserRecord] = []
            for shard_id, pk in list(seen.items())[:2]:
                ur = UserRecord(partition_key=pk, data=b"hello")
                ar = AggregatedBatch(predicted_shard=shard_id)
                ar.add(
                    _BufferedRecord(
                        user_record=ur,
                        deadline=10.0,
                        hash_key=md5_hash_key(pk),
                        arrival_time=time.monotonic(),
                    )
                )
                batch.add(ar)
                urs.append(ur)

            sender = Sender(stream_name=stream_name, client=adapter)
            outcome = await sender.send(batch)
            assert outcome.request_error is None, outcome.request_error
            assert len(outcome.per_record) == 2
            assert all(pr.success for pr in outcome.per_record)

            finishes: list[tuple[_BufferedRecord, RecordResult]] = []
            retries: list[_BufferedRecord] = []

            async def on_finish(buf: _BufferedRecord, res: RecordResult) -> None:
                finishes.append((buf, res))

            async def on_retry(buf: _BufferedRecord) -> None:
                retries.append(buf)

            retrier = Retrier(shard_map=sm, on_finish=on_finish, on_retry=on_retry)
            await retrier.handle(outcome)

            assert retries == []
            assert len(finishes) == 2
            for _, res in finishes:
                assert res.success
                assert res.sequence_number is not None
                assert res.shard_id is not None
                assert len(res.attempts) == 1 and res.attempts[0].success
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
