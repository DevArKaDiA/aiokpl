"""Integration tests for the Aggregator+Collector wire behavior.

Two stages working together against the real ``etspaceman/kinesis-mock``:

1. ``test_aggregation_deaggregates_via_kcl_format``: many records sharing one
   partition key pile into a single AggregatedBatch; we round-trip that
   through Kinesis and prove a KCL-style consumer recovers every user record
   byte-exact (validates the wire format and the single-shard aggregation
   path end-to-end).
2. ``test_collector_packs_multiple_shards_in_one_putrecords``: records
   spread across 4 shards pack into ONE ``PutRecords`` call (validates that
   the Collector batches across shards rather than per-shard).

aiobotocore is asyncio-only, so the ``anyio_backend`` fixture is fixed.
"""

from __future__ import annotations

import contextlib
import re
import time
from typing import Any
from uuid import uuid4

import anyio
import anyio.to_thread
import pytest

from aiokpl import decode_aggregated, is_aggregated
from aiokpl.config import Config
from aiokpl.producer import Producer


@pytest.fixture
def anyio_backend() -> str:
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
    return client.meta.endpoint_url, client.meta.region_name


def _get_shard_iterator(client: Any, stream: str, shard_id: str) -> str:
    out = client.get_shard_iterator(
        StreamName=stream,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    return out["ShardIterator"]


def _drain_records(
    client: Any, stream: str, shard_id: str, max_attempts: int = 20
) -> list[dict[str, Any]]:
    """Loop GetRecords until we either get records or exhaust attempts.

    kinesis-mock honors TRIM_HORIZON, so a freshly-written record is
    available within a few hundred ms.
    """
    it = _get_shard_iterator(client, stream, shard_id)
    collected: list[dict[str, Any]] = []
    for _ in range(max_attempts):
        resp = client.get_records(ShardIterator=it, Limit=10_000)
        collected.extend(resp.get("Records", []))
        it = resp.get("NextShardIterator")
        if collected:
            return collected
        if it is None:
            break
        time.sleep(0.2)
    return collected


@pytest.mark.integration
async def test_aggregation_deaggregates_via_kcl_format(kinesis_client: Any) -> None:
    endpoint, region = _endpoint_from(kinesis_client)
    stream_name = f"aiokpl-agg-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
    try:
        await anyio.to_thread.run_sync(_wait_stream_active, kinesis_client, stream_name)

        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            # Plenty of room so all 50 records pile into one aggregated blob.
            record_max_buffered_time_ms=200.0,
            record_ttl_ms=30_000.0,
            aggregation_max_count=1000,
            aggregation_max_size=1_000_000,
            max_outstanding_records=200,
        )

        partition_key = "pk-shared"
        n_records = 50
        payloads = [f"payload-{i:03d}".encode() for i in range(n_records)]

        outcomes: list[Any] = []
        async with Producer(cfg) as producer:
            for data in payloads:
                o = await producer.put_record(
                    stream=stream_name,
                    partition_key=partition_key,
                    data=data,
                )
                outcomes.append(o)
            await producer.flush()
            results = []
            with anyio.fail_after(60.0):
                for o in outcomes:
                    results.append(await o.wait())

        # All 50 user records succeed (one-shard stream + shared partition
        # key forces a single aggregated batch).
        assert all(r.success for r in results), [
            (r.success, r.attempts[-1].error_code if r.attempts else None)
            for r in results
            if not r.success
        ]

        # One Kinesis record on the shard. List the shard then drain it via
        # the sync client (already on the integration surface).
        shards = kinesis_client.list_shards(StreamName=stream_name)["Shards"]
        assert len(shards) == 1
        shard_id = shards[0]["ShardId"]
        records = await anyio.to_thread.run_sync(
            _drain_records, kinesis_client, stream_name, shard_id
        )
        assert len(records) == 1, (
            f"expected exactly one aggregated Kinesis record, got {len(records)}"
        )
        blob = records[0]["Data"]
        assert is_aggregated(blob), "Kinesis returned a non-aggregated blob"

        decoded = decode_aggregated(blob)
        assert len(decoded) == n_records, (
            f"deaggregated {len(decoded)} records, expected {n_records}"
        )
        for i, rec in enumerate(decoded):
            assert rec.partition_key == partition_key, (
                f"record {i} partition_key={rec.partition_key!r}"
            )
            assert rec.data == payloads[i], (
                f"record {i} data mismatch: {rec.data!r} vs {payloads[i]!r}"
            )
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)


@pytest.mark.integration
async def test_collector_packs_multiple_shards_in_one_putrecords(
    kinesis_client: Any,
) -> None:
    """Records on different shards land in ONE PutRecords call.

    Strategy: spread N records across 4 shards using explicit_hash_keys at
    quartile boundaries. Wrap the aiobotocore Kinesis client so we can count
    ``put_records`` invocations; assert exactly one call carrying every
    record.
    """
    endpoint, region = _endpoint_from(kinesis_client)
    stream_name = f"aiokpl-coll-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=4)
    try:
        await anyio.to_thread.run_sync(_wait_stream_active, kinesis_client, stream_name)

        # Probe the actual shard ranges; routing depends on the emulator's
        # split, not on our assumptions.
        shards = kinesis_client.list_shards(StreamName=stream_name)["Shards"]
        assert len(shards) == 4

        # Pick one explicit_hash_key in each shard's range so each record is
        # routed to a different shard.
        ehks: list[str] = []
        for s in shards:
            start = int(s["HashKeyRange"]["StartingHashKey"])
            end = int(s["HashKeyRange"]["EndingHashKey"])
            ehks.append(str((start + end) // 2))

        cfg = Config(
            region=region,
            endpoint_url=endpoint,
            verify_ssl=False,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            # Big buffered window so the collector deadline triggers, not the
            # aggregator's per-record deadline. The flush() call below
            # short-circuits the wait anyway.
            record_max_buffered_time_ms=500.0,
            record_ttl_ms=30_000.0,
            aggregation_enabled=False,
            max_outstanding_records=200,
        )

        # Instrument: monkey-patch the aiobotocore client's put_records to
        # count calls. We do this by wrapping the client AFTER Producer
        # opens it (the simplest hook the public surface gives us).
        call_count = 0
        record_counts: list[int] = []

        outcomes: list[Any] = []
        async with Producer(cfg) as producer:
            inner = producer._kinesis_client
            original_put_records = inner.put_records

            async def counting_put_records(**kwargs: Any) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                record_counts.append(len(kwargs.get("Records", [])))
                return await original_put_records(**kwargs)

            inner.put_records = counting_put_records  # type: ignore[method-assign]

            for i, ehk in enumerate(ehks):
                o = await producer.put_record(
                    stream=stream_name,
                    partition_key=f"pk-{i}",
                    explicit_hash_key=ehk,
                    data=f"payload-{i}".encode(),
                )
                outcomes.append(o)
            await producer.flush()
            results = []
            with anyio.fail_after(60.0):
                for o in outcomes:
                    results.append(await o.wait())

        assert all(r.success for r in results)
        assert call_count == 1, f"expected one PutRecords call packing all shards, got {call_count}"
        assert record_counts == [len(ehks)], (
            f"PutRecords call carried {record_counts} records, expected one batch of {len(ehks)}"
        )

        # All four shards saw their record (sanity probe on the response).
        landed_shards = {r.shard_id for r in results if r.shard_id is not None}
        assert len(landed_shards) == 4, (
            f"expected records on 4 distinct shards, got {landed_shards}"
        )
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
