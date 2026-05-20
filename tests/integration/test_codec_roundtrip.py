"""End-to-end proof that the hand-rolled aggregation codec produces bytes a
real Kinesis-compatible service accepts, stores verbatim, and returns.

Without this test, every assertion in ``test_aggregation.py`` is
self-referential: encode/decode round-trip our own bytes. Here we put the blob
through Floci's Kinesis implementation and confirm:

1. ``put_record`` accepts our magic-prefixed payload (no 400).
2. ``get_records`` returns the **identical** bytes (Kinesis stores the body
   opaquely — any silent re-framing would be a wire-format bug).
3. ``decode_aggregated`` on the round-tripped bytes recovers all three
   user records, partition keys, the explicit hash key, and the tag.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any
from uuid import uuid4

import pytest

from aiokpl.aggregation import (
    DecodedRecord,
    Tag,
    UserRecord,
    decode_aggregated,
    encode_aggregated,
)
from aiokpl.hashing import md5_hash_key


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


@pytest.mark.integration
def test_aggregated_blob_roundtrips_through_kinesis(kinesis_client: Any) -> None:
    stream_name = f"aiokpl-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=2)
    try:
        _wait_stream_active(kinesis_client, stream_name)

        records = [
            UserRecord(partition_key="alpha", data=b"one"),
            UserRecord(
                partition_key="beta",
                data=b"two",
                explicit_hash_key="170141183460469231731687303715884105727",
            ),
            UserRecord(
                partition_key="gamma",
                data=b"three",
                tags=(Tag(key="env", value="test"),),
            ),
        ]
        blob = encode_aggregated(records)

        # When aggregating, the API-level partition key is the literal "a" and
        # the ExplicitHashKey routes the whole aggregate to the shard the first
        # record would have landed on if sent unaggregated.
        routing_ehk = str(md5_hash_key("alpha"))
        put = kinesis_client.put_record(
            StreamName=stream_name,
            Data=blob,
            PartitionKey="a",
            ExplicitHashKey=routing_ehk,
        )
        shard_id = put["ShardId"]

        iterator = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        records_out: list[dict[str, Any]] = []
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            resp = kinesis_client.get_records(ShardIterator=iterator, Limit=10)
            records_out = resp.get("Records", [])
            if records_out:
                break
            iterator = resp["NextShardIterator"]
            time.sleep(0.1)

        assert len(records_out) == 1, f"expected exactly one Kinesis record, got {records_out}"
        assert records_out[0]["Data"] == blob, "Kinesis did not return our bytes verbatim"

        decoded = decode_aggregated(records_out[0]["Data"])
        assert decoded == [
            DecodedRecord(partition_key="alpha", explicit_hash_key=None, data=b"one", tags=()),
            DecodedRecord(
                partition_key="beta",
                explicit_hash_key="170141183460469231731687303715884105727",
                data=b"two",
                tags=(),
            ),
            DecodedRecord(
                partition_key="gamma",
                explicit_hash_key=None,
                data=b"three",
                tags=(Tag(key="env", value="test"),),
            ),
        ]
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
