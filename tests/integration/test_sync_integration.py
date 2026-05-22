"""Integration test for :class:`aiokpl.sync.SyncProducer`.

Drives 20 records through the synchronous bridge against the same
``etspaceman/kinesis-mock`` container the async integration tests use. The
test is intentionally **sync** — no ``async def``, no ``@pytest.mark.anyio``
— because the whole point of :class:`SyncProducer` is to be callable from
plain blocking code.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any
from uuid import uuid4

import pytest

from aiokpl.config import Config
from aiokpl.sync import SyncProducer


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


@pytest.mark.integration
def test_sync_producer_against_kinesis_mock(kinesis_client: Any) -> None:
    endpoint = kinesis_client.meta.endpoint_url
    region = kinesis_client.meta.region_name
    stream_name = f"aiokpl-sync-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=2)
    try:
        _wait_stream_active(kinesis_client, stream_name)
        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            record_max_buffered_time_ms=50.0,
            record_ttl_ms=60_000.0,
            max_outstanding_records=100,
        )
        n_records = 20
        outcomes = []
        with SyncProducer(cfg) as producer:
            for i in range(n_records):
                outcome = producer.put_record(
                    stream=stream_name,
                    partition_key=f"pk-{i}",
                    data=f"payload-{i}".encode(),
                )
                outcomes.append(outcome)
            producer.flush(timeout=30.0)
            results = [o.wait(timeout=30.0) for o in outcomes]
        successes = sum(1 for r in results if r.success)
        assert successes == n_records, (
            f"only {successes}/{n_records} records succeeded against kinesis-mock"
        )
        for r in results:
            assert r.sequence_number
            assert r.shard_id
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
