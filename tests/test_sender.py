"""Unit tests for :class:`aiokpl.sender.Sender`.

Cover every branch the C++ ``put_records_context`` produces and the
``RecordCountMismatch`` sanity check from ``retrier.cc:170-180``. We inject
fakes that satisfy the ``_KinesisClient`` Protocol so the unit-test path
never imports aiobotocore.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
from botocore.exceptions import ClientError

from aiokpl.aggregation import UserRecord
from aiokpl.aggregator import AggregatedBatch, _BufferedRecord
from aiokpl.collector import PutRecordsBatch
from aiokpl.sender import Sender, _KinesisClient


def _make_batch(*specs: tuple[str, bytes, str | None, int]) -> PutRecordsBatch:
    """Build a PutRecordsBatch holding one AR per spec ``(pk, data, ehk, hk)``."""
    batch = PutRecordsBatch()
    for pk, data, ehk, hk in specs:
        ur = UserRecord(partition_key=pk, data=data, explicit_hash_key=ehk)
        ar = AggregatedBatch(predicted_shard=0)
        ar.add(_BufferedRecord(user_record=ur, deadline=1.0, hash_key=hk, arrival_time=0.0))
        batch.add(ar)
    return batch


class _FakeClient:
    def __init__(
        self,
        response: dict[str, Any] | None = None,
        exc: BaseException | None = None,
    ) -> None:
        self.response = response
        self.exc = exc
        self.calls: list[dict[str, Any]] = []

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        assert self.response is not None
        return self.response


def test_protocol_runtime_check() -> None:
    assert isinstance(_FakeClient(), _KinesisClient)


async def test_happy_path_two_records() -> None:
    batch = _make_batch(("pk1", b"a", None, 1), ("pk2", b"b", "5", 5))
    client = _FakeClient(
        response={
            "Records": [
                {"SequenceNumber": "seq-1", "ShardId": "shardId-0"},
                {"SequenceNumber": "seq-2", "ShardId": "shardId-1"},
            ],
            "FailedRecordCount": 0,
        }
    )
    clock_state = {"t": 100.0}

    def clock() -> float:
        v = clock_state["t"]
        clock_state["t"] += 0.5
        return v

    sender = Sender(stream_name="stream", client=client, clock=clock)
    outcome = await sender.send(batch)

    assert outcome.stream_name == "stream"
    assert outcome.started_at == 100.0
    assert outcome.ended_at == 100.5
    assert outcome.request_error is None
    assert len(outcome.per_record) == 2
    assert outcome.batch_items == tuple(batch.items)
    assert outcome.per_record[0].success
    assert outcome.per_record[0].shard_id == "shardId-0"
    assert outcome.per_record[0].sequence_number == "seq-1"
    assert outcome.per_record[1].success
    # First record has no EHK, second does — verify kwargs alignment.
    sent = client.calls[0]
    assert sent["StreamName"] == "stream"
    assert sent["Records"][0]["PartitionKey"] == "pk1"
    assert "ExplicitHashKey" not in sent["Records"][0]
    assert sent["Records"][1]["ExplicitHashKey"] == "5"


async def test_per_record_failure_surfaced() -> None:
    batch = _make_batch(("pk1", b"a", None, 1), ("pk2", b"b", None, 2))
    client = _FakeClient(
        response={
            "Records": [
                {"SequenceNumber": "seq-1", "ShardId": "shardId-0"},
                {"ErrorCode": "InternalFailure", "ErrorMessage": "boom"},
            ]
        }
    )
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error is None
    assert outcome.per_record[0].success
    assert not outcome.per_record[1].success
    assert outcome.per_record[1].error_code == "InternalFailure"
    assert outcome.per_record[1].error_message == "boom"
    assert outcome.per_record[1].shard_id is None
    assert outcome.per_record[1].sequence_number is None


async def test_request_level_client_error() -> None:
    batch = _make_batch(("pk", b"d", None, 1))
    exc = ClientError({"Error": {"Code": "Throttling", "Message": "rate exceeded"}}, "PutRecords")
    client = _FakeClient(exc=exc)
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error == ("Throttling", "rate exceeded")
    assert outcome.per_record == ()
    assert outcome.batch_items == tuple(batch.items)


async def test_request_level_unknown_exception() -> None:
    batch = _make_batch(("pk", b"d", None, 1))
    client = _FakeClient(exc=RuntimeError("network down"))
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error == ("Internal", "network down")
    assert outcome.per_record == ()


async def test_request_error_with_malformed_response_attribute() -> None:
    # An exception that carries a ``response`` attribute but not a dict/Error
    # nested correctly should fall through to the ``Internal`` branch.
    class _OddError(Exception):
        response: ClassVar[dict[str, Any]] = {"Other": "thing"}  # no "Error" key

    batch = _make_batch(("pk", b"d", None, 1))
    client = _FakeClient(exc=_OddError("nope"))
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error == ("Internal", "nope")


async def test_request_error_with_non_string_error_fields() -> None:
    class _OddError(Exception):
        response: ClassVar[dict[str, Any]] = {"Error": {"Code": 500, "Message": "x"}}

    batch = _make_batch(("pk", b"d", None, 1))
    client = _FakeClient(exc=_OddError("fallback"))
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error == ("Internal", "fallback")


async def test_record_count_mismatch() -> None:
    batch = _make_batch(("pk1", b"a", None, 1), ("pk2", b"b", None, 2))
    client = _FakeClient(response={"Records": [{"SequenceNumber": "x", "ShardId": "shardId-0"}]})
    outcome = await Sender(stream_name="s", client=client).send(batch)
    assert outcome.request_error is not None
    assert outcome.request_error[0] == "RecordCountMismatch"
    assert "1 records for a batch of 2" in outcome.request_error[1]
    assert outcome.per_record == ()


async def test_empty_batch_raises() -> None:
    batch = PutRecordsBatch()
    sender = Sender(stream_name="s", client=_FakeClient(response={"Records": []}))
    with pytest.raises(ValueError, match="empty"):
        await sender.send(batch)
