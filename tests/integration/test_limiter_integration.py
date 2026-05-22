"""End-to-end test for the per-shard rate limiter.

Configures the Producer at a deliberately low ``rate_limit_records_per_sec_per_shard``
and disables aggregation so every user record becomes one Kinesis record on
the wire — no aggregation hides the actual rate. Submits N records to a
1-shard stream and asserts the wall-clock matches what the token bucket
would yield.

Marked ``slow`` because the test deliberately waits multiple seconds for the
bucket to refill.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any
from uuid import uuid4

import anyio
import anyio.to_thread
import pytest

from aiokpl.config import Config
from aiokpl.producer import Producer


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


def _endpoint_from(client: Any) -> tuple[str, str]:
    return client.meta.endpoint_url, client.meta.region_name


@pytest.mark.slow
@pytest.mark.integration
async def test_limiter_throttles_to_configured_rate(kinesis_client: Any) -> None:
    endpoint, region = _endpoint_from(kinesis_client)
    stream_name = f"aiokpl-lim-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
    try:
        await anyio.to_thread.run_sync(_wait_stream_active, kinesis_client, stream_name)

        records_per_sec = 10.0
        n_records = 50

        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            # No aggregation — every user record consumes one record-token.
            aggregation_enabled=False,
            rate_limit_records_per_sec_per_shard=records_per_sec,
            # Keep byte budget high so it never closes the door first.
            rate_limit_bytes_per_sec_per_shard=10_000_000.0,
            # Short buffered time so the aggregator doesn't dominate latency.
            record_max_buffered_time_ms=10.0,
            record_ttl_ms=60_000.0,
            max_outstanding_records=200,
        )

        outcomes: list[Any] = []
        async with Producer(cfg) as producer:
            t0 = time.monotonic()
            for i in range(n_records):
                o = await producer.put_record(
                    stream=stream_name,
                    partition_key=f"pk-{i}",
                    data=f"payload-{i}".encode(),
                )
                outcomes.append(o)
            # NOTE: deliberately NOT calling producer.flush() here — flush
            # triggers Limiter.drain_force(), bypassing token-bucket throttle
            # by design (graceful-shutdown contract). To observe the
            # configured rate we must let the background drain task pace
            # the admissions.
            with anyio.fail_after(30.0):
                results = [await o.wait() for o in outcomes]
            elapsed = time.monotonic() - t0

        # Bucket starts full with 10 tokens; the remaining 40 records pay
        # the rate. Floor: (50-10)/10 = 4s. Margin allows for scheduling
        # jitter without permitting "no throttle at all" (which would
        # finish in ~well under 1s on this hardware).
        assert elapsed >= 3.5, (
            f"limiter did not throttle: elapsed={elapsed:.3f}s for {n_records} records "
            f"at {records_per_sec} rec/s"
        )
        assert elapsed <= 6.5, f"limiter over-throttled: elapsed={elapsed:.3f}s, expected <= 6.5s"

        # Sanity: nearly all records succeeded against the mock.
        successes = sum(1 for r in results if r.success)
        assert successes >= int(n_records * 0.9), (
            f"only {successes}/{n_records} succeeded under throttle"
        )
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
