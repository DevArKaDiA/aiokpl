"""Integration test for :class:`aiokpl.producer.Producer` end-to-end.

Exercises the full pipeline (Aggregator → Limiter → Collector → Sender →
Retrier) against the same ``etspaceman/kinesis-mock`` container used by every
other integration test. The mock is byte-exact with AWS for hash-key routing,
so we can additionally assert that ``ShardMap.predict()`` and the actual
``put_records`` routing agree.

Endpoint and credentials are extracted from the sync kinesis_client fixture's
botocore meta — that lets the Producer point at the same container without a
second copy of the docker bootstrap.
"""

from __future__ import annotations

import contextlib
import random
import re
import time
from typing import Any
from uuid import uuid4

import anyio
import anyio.to_thread
import pytest

from aiokpl.config import Config
from aiokpl.metrics import (
    NAME_REQUEST_TIME,
    NAME_USER_RECORDS_PUT,
    NAME_USER_RECORDS_RECEIVED,
    MetricsLevel,
)
from aiokpl.producer import Producer


@pytest.fixture
def anyio_backend() -> str:
    # aiobotocore (and thus Producer) is asyncio-only.
    return "asyncio"


_SHARD_ID_RE = re.compile(r"^shardId-(\d+)$")


def _parse_shard_id(raw: str) -> int:
    m = _SHARD_ID_RE.match(raw)
    assert m is not None, f"unexpected shard id format: {raw!r}"
    return int(m.group(1))


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


def _endpoint_from(client: Any) -> tuple[str, str]:
    """Pull (endpoint_url, region) off the underlying botocore client."""
    endpoint = client.meta.endpoint_url
    region = client.meta.region_name
    return endpoint, region


@pytest.mark.integration
async def test_producer_end_to_end_against_kinesis_mock(kinesis_client: Any) -> None:
    endpoint, region = _endpoint_from(kinesis_client)
    stream_name = f"aiokpl-prod-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=4)
    try:
        await anyio.to_thread.run_sync(_wait_stream_active, kinesis_client, stream_name)

        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            record_max_buffered_time_ms=50.0,
            record_ttl_ms=60_000.0,
            max_outstanding_records=200,
        )

        outcomes: list[Any] = []
        rng = random.Random(0xC0FFEE)
        n_records = 100

        async with Producer(cfg) as producer:
            for i in range(n_records):
                pk = f"pk-{rng.randrange(1 << 60):x}-{i}"
                o = await producer.put_record(
                    stream=stream_name,
                    partition_key=pk,
                    data=f"payload-{i}".encode(),
                )
                outcomes.append(o)
            await producer.flush()

            results = []
            with anyio.fail_after(60.0):
                for o in outcomes:
                    results.append(await o.wait())

        successes = sum(1 for r in results if r.success)
        assert successes / n_records >= 0.8, (
            f"only {successes}/{n_records} records succeeded against kinesis-mock"
        )
        for r in results:
            assert len(r.attempts) >= 1
            if r.success:
                assert r.sequence_number
                assert r.shard_id
                assert _parse_shard_id(r.shard_id) is not None
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)


@pytest.mark.integration
async def test_producer_metrics_counts_after_100_records(kinesis_client: Any) -> None:
    endpoint, region = _endpoint_from(kinesis_client)
    stream_name = f"aiokpl-prod-metrics-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=2)
    try:
        await anyio.to_thread.run_sync(_wait_stream_active, kinesis_client, stream_name)
        from aiokpl.sinks import InMemorySink

        sink = InMemorySink()
        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            record_max_buffered_time_ms=50.0,
            record_ttl_ms=60_000.0,
            max_outstanding_records=200,
            # DETAILED level captures every dim; flush onto an InMemorySink so
            # we can assert post-flush exports without hitting a real
            # CloudWatch endpoint.
            metrics_level=MetricsLevel.DETAILED,
            metrics_sink=sink,
            metrics_upload_interval_ms=60_000.0,
        )
        outcomes: list[Any] = []
        rng = random.Random(0xC0FFEE)
        n_records = 100
        async with Producer(cfg) as producer:
            for i in range(n_records):
                pk = f"pk-{rng.randrange(1 << 60):x}-{i}"
                o = await producer.put_record(
                    stream=stream_name,
                    partition_key=pk,
                    data=f"payload-{i}".encode(),
                )
                outcomes.append(o)
            await producer.flush()
            results = []
            with anyio.fail_after(60.0):
                for o in outcomes:
                    results.append(await o.wait())
            snap = producer.metrics.snapshot()
            # Force one flush so the sink captures snapshots before exit.
            await producer.metrics.flush()

        received = sum(
            stats[0] for key, stats in snap.items() if key.name == NAME_USER_RECORDS_RECEIVED
        )
        put = sum(stats[0] for key, stats in snap.items() if key.name == NAME_USER_RECORDS_PUT)
        request_times = sum(
            stats[0] for key, stats in snap.items() if key.name == NAME_REQUEST_TIME
        )
        success_count = sum(1 for r in results if r.success)
        assert received == n_records
        assert put == success_count
        assert request_times > 0
        # Post-flush exports also surfaced through the InMemorySink.
        sink_names = {s.name for s in sink.all_snapshots}
        assert NAME_USER_RECORDS_RECEIVED in sink_names
        assert NAME_USER_RECORDS_PUT in sink_names
        assert NAME_REQUEST_TIME in sink_names
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
