"""Unit tests for :class:`aiokpl.retrier.Retrier`.

Exercise every row of the classification table from ``CLAUDE.md`` (which
mirrors ``aws/kinesis/core/retrier.cc``). We build :class:`SendOutcome`
instances by hand and assert which callback (``on_finish`` / ``on_retry``)
fires and what :class:`Attempt` / :class:`RecordResult` the buffered record
ends up with.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from aiokpl.aggregation import UserRecord
from aiokpl.aggregator import AggregatedBatch, _BufferedRecord
from aiokpl.result import RecordResult
from aiokpl.retrier import (
    EXPIRED_ERROR_CODE,
    PROVISIONED_THROUGHPUT_EXCEEDED,
    WRONG_SHARD_ERROR_CODE,
    Retrier,
    _parse_actual_shard_id,
)
from aiokpl.sender import PerRecordOutcome, SendOutcome


@dataclass
class FakeShardMap:
    ranges: dict[int, tuple[int, int]] = field(default_factory=dict)
    invalidate_calls: list[tuple[float, int | None]] = field(default_factory=list)

    def hashrange(self, shard_id: int) -> tuple[int, int] | None:
        return self.ranges.get(shard_id)

    async def invalidate(self, seen_at: float, predicted_shard: int | None) -> None:
        self.invalidate_calls.append((seen_at, predicted_shard))


@dataclass
class CaptureCallbacks:
    finished: list[tuple[_BufferedRecord, RecordResult]] = field(default_factory=list)
    retried: list[_BufferedRecord] = field(default_factory=list)

    async def on_finish(self, buffered: _BufferedRecord, result: RecordResult) -> None:
        self.finished.append((buffered, result))

    async def on_retry(self, buffered: _BufferedRecord) -> None:
        self.retried.append(buffered)


def _make_buffered(*, hash_key: int = 0, arrival_time: float = 0.0) -> _BufferedRecord:
    return _BufferedRecord(
        user_record=UserRecord(partition_key=f"pk-{hash_key}", data=b"x"),
        deadline=10.0,
        hash_key=hash_key,
        arrival_time=arrival_time,
    )


def _ar_with(buffereds: list[_BufferedRecord], predicted: int | None) -> AggregatedBatch:
    ar = AggregatedBatch(predicted_shard=predicted)
    for b in buffereds:
        ar.add(b)
    return ar


def _outcome(
    *,
    request_error: tuple[str, str] | None = None,
    per_record: tuple[PerRecordOutcome, ...] = (),
    batch_items: tuple[AggregatedBatch, ...] = (),
    started_at: float = 1.0,
    ended_at: float = 1.1,
) -> SendOutcome:
    return SendOutcome(
        stream_name="s",
        started_at=started_at,
        ended_at=ended_at,
        request_error=request_error,
        per_record=per_record,
        batch_items=batch_items,
    )


def _retrier(
    cb: CaptureCallbacks,
    sm: FakeShardMap,
    *,
    fail_if_throttled: bool = False,
    record_ttl_ms: float = 30_000.0,
    clock_value: float = 5.0,
) -> Retrier:
    return Retrier(
        shard_map=sm,
        on_finish=cb.on_finish,
        on_retry=cb.on_retry,
        record_ttl_ms=record_ttl_ms,
        fail_if_throttled=fail_if_throttled,
        retry_deadline_ms=100.0,
        clock=lambda: clock_value,
    )


# ─── _parse_actual_shard_id helper ─────────────────────────────────────────


def test_parse_actual_shard_id_valid() -> None:
    assert _parse_actual_shard_id("shardId-7") == 7


def test_parse_actual_shard_id_none() -> None:
    assert _parse_actual_shard_id(None) is None


def test_parse_actual_shard_id_empty() -> None:
    assert _parse_actual_shard_id("") is None


def test_parse_actual_shard_id_wrong_prefix() -> None:
    assert _parse_actual_shard_id("shard-7") is None


def test_parse_actual_shard_id_non_digit() -> None:
    assert _parse_actual_shard_id("shardId-abc") is None


# ─── Request-level branch ──────────────────────────────────────────────────


async def test_request_throttle_fail_if_throttled_fails_all() -> None:
    b1, b2 = _make_buffered(hash_key=1), _make_buffered(hash_key=2)
    ar1 = _ar_with([b1], predicted=0)
    ar2 = _ar_with([b2], predicted=0)
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm, fail_if_throttled=True)
    outcome = _outcome(
        request_error=(PROVISIONED_THROUGHPUT_EXCEEDED, "slow down"),
        batch_items=(ar1, ar2),
    )
    await retrier.handle(outcome)
    assert len(cb.finished) == 2
    assert cb.retried == []
    for _, res in cb.finished:
        assert not res.success
        assert res.attempts[-1].error_code == PROVISIONED_THROUGHPUT_EXCEEDED


async def test_request_throttle_no_fail_retries_all() -> None:
    b1, b2 = _make_buffered(hash_key=1), _make_buffered(hash_key=2)
    ar = _ar_with([b1, b2], predicted=0)
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm, fail_if_throttled=False)
    outcome = _outcome(
        request_error=(PROVISIONED_THROUGHPUT_EXCEEDED, "slow"),
        batch_items=(ar,),
    )
    await retrier.handle(outcome)
    assert cb.finished == []
    assert cb.retried == [b1, b2]
    for buf in cb.retried:
        assert buf.attempts[-1].error_code == PROVISIONED_THROUGHPUT_EXCEEDED


async def test_request_other_error_retries_all() -> None:
    b1 = _make_buffered(hash_key=1)
    ar = _ar_with([b1], predicted=0)
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm, fail_if_throttled=True)
    outcome = _outcome(request_error=("Internal", "boom"), batch_items=(ar,))
    await retrier.handle(outcome)
    assert cb.retried == [b1]


# ─── Per-record success branch ─────────────────────────────────────────────


async def test_success_predicted_matches_actual() -> None:
    buf = _make_buffered(hash_key=10)
    ar = _ar_with([buf], predicted=3)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-3",
        sequence_number="seq",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 1
    _, res = cb.finished[0]
    assert res.success
    assert res.shard_id == "shardId-3"
    assert res.sequence_number == "seq"
    assert sm.invalidate_calls == []


async def test_success_predicted_none_finishes_without_invalidate() -> None:
    buf = _make_buffered(hash_key=10)
    ar = _ar_with([buf], predicted=None)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-9",
        sequence_number="seq",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 1 and cb.finished[0][1].success
    assert sm.invalidate_calls == []


async def test_success_actual_shard_unparseable_treated_as_no_validation() -> None:
    # If the ShardId is not parseable (malformed service response), we cannot
    # compare predicted vs actual — finish as success without invalidating.
    buf = _make_buffered(hash_key=10)
    ar = _ar_with([buf], predicted=3)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="garbage",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 1 and cb.finished[0][1].success
    assert sm.invalidate_calls == []


async def test_success_hash_in_actual_range_finishes_and_invalidates() -> None:
    buf = _make_buffered(hash_key=50)
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-1",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap(ranges={1: (0, 100)})
    retrier = _retrier(cb, sm, clock_value=42.0)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 1 and cb.finished[0][1].success
    assert sm.invalidate_calls == [(42.0, 1)]


async def test_success_hash_outside_actual_range_retries_wrong_shard() -> None:
    buf = _make_buffered(hash_key=500)
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-1",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap(ranges={1: (0, 100)})
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.finished == []
    assert cb.retried == [buf]
    assert buf.attempts[-1].error_code == WRONG_SHARD_ERROR_CODE
    assert sm.invalidate_calls and sm.invalidate_calls[0][1] == 1


async def test_success_actual_range_unknown_retries_and_invalidates() -> None:
    # Hash range not in the FakeShardMap → treat as "outside", retry as Wrong
    # Shard. The invalidate still fires once.
    buf = _make_buffered(hash_key=50)
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-7",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.retried == [buf]
    assert sm.invalidate_calls and sm.invalidate_calls[0][1] == 7


async def test_success_invalidate_called_once_for_multi_record_ar() -> None:
    # When the AR holds multiple URs and predicted != actual, the AR-level
    # invalidate happens exactly once even though each UR is classified.
    b1, b2 = _make_buffered(hash_key=10), _make_buffered(hash_key=20)
    ar = _ar_with([b1, b2], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-1",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap(ranges={1: (0, 100)})
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 2
    assert len(sm.invalidate_calls) == 1


# ─── Per-record failure branch ─────────────────────────────────────────────


async def test_per_record_throttle_fail_if_throttled() -> None:
    buf = _make_buffered()
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code=PROVISIONED_THROUGHPUT_EXCEEDED,
        error_message="slow",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm, fail_if_throttled=True)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert len(cb.finished) == 1 and not cb.finished[0][1].success
    assert cb.retried == []


async def test_per_record_throttle_no_fail_retries() -> None:
    buf = _make_buffered()
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code=PROVISIONED_THROUGHPUT_EXCEEDED,
        error_message="slow",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.retried == [buf]


async def test_per_record_other_error_retries() -> None:
    buf = _make_buffered()
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code="InternalFailure",
        error_message="oops",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm, fail_if_throttled=True)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.retried == [buf]


async def test_per_record_missing_error_fields_uses_defaults() -> None:
    # ErrorCode=None falls back to "Unknown"; ErrorMessage=None to empty str.
    buf = _make_buffered()
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.retried == [buf]
    assert buf.attempts[-1].error_code == "Unknown"
    assert buf.attempts[-1].error_message == ""


# ─── Expiry ────────────────────────────────────────────────────────────────


async def test_expired_record_fails_with_expired_code() -> None:
    buf = _make_buffered(arrival_time=0.0)
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code="InternalFailure",
        error_message="oops",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    # arrival_time=0, clock=100, ttl=10s → expired.
    retrier = _retrier(cb, sm, record_ttl_ms=10_000.0, clock_value=100.0)
    await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    assert cb.retried == []
    assert len(cb.finished) == 1
    _, res = cb.finished[0]
    assert not res.success
    assert res.attempts[-1].error_code == EXPIRED_ERROR_CODE


# ─── Mixed batch ───────────────────────────────────────────────────────────


async def test_retrier_emits_success_metrics() -> None:
    from aiokpl.metrics import (
        NAME_KINESIS_RECORDS_DATA_PUT,
        NAME_KINESIS_RECORDS_PUT,
        NAME_RETRIES_PER_RECORD,
        NAME_USER_RECORDS_DATA_PUT,
        NAME_USER_RECORDS_PUT,
        MetricsLevel,
        MetricsManager,
    )

    buf = _make_buffered(hash_key=10)
    ar = _ar_with([buf], predicted=3)
    pr = PerRecordOutcome(
        batch=ar,
        success=True,
        shard_id="shardId-3",
        sequence_number="seq",
        error_code=None,
        error_message=None,
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    mgr = MetricsManager(level=MetricsLevel.DETAILED)
    async with mgr:
        retrier = Retrier(
            shard_map=sm,
            on_finish=cb.on_finish,
            on_retry=cb.on_retry,
            record_ttl_ms=30_000.0,
            fail_if_throttled=False,
            retry_deadline_ms=100.0,
            clock=lambda: 5.0,
            metrics=mgr,
            stream_name="s",
        )
        await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    snap = mgr.snapshot()
    names = {k.name for k in snap}
    assert NAME_USER_RECORDS_PUT in names
    assert NAME_USER_RECORDS_DATA_PUT in names
    assert NAME_KINESIS_RECORDS_PUT in names
    assert NAME_KINESIS_RECORDS_DATA_PUT in names
    assert NAME_RETRIES_PER_RECORD in names


async def test_retrier_emits_error_metrics_on_retry_and_fail() -> None:
    from aiokpl.metrics import (
        NAME_ALL_ERRORS,
        NAME_ERRORS_BY_CODE,
        MetricsLevel,
        MetricsManager,
    )

    buf = _make_buffered()
    ar = _ar_with([buf], predicted=0)
    pr = PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code=PROVISIONED_THROUGHPUT_EXCEEDED,
        error_message="slow",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    mgr = MetricsManager(level=MetricsLevel.DETAILED)
    async with mgr:
        retrier = Retrier(
            shard_map=sm,
            on_finish=cb.on_finish,
            on_retry=cb.on_retry,
            record_ttl_ms=30_000.0,
            fail_if_throttled=True,
            retry_deadline_ms=100.0,
            clock=lambda: 5.0,
            metrics=mgr,
            stream_name="s",
        )
        await retrier.handle(_outcome(per_record=(pr,), batch_items=(ar,)))
    snap = mgr.snapshot()
    names = {k.name for k in snap}
    assert NAME_ALL_ERRORS in names
    assert NAME_ERRORS_BY_CODE in names


async def test_mixed_success_and_failure() -> None:
    b1, b2 = _make_buffered(hash_key=10), _make_buffered(hash_key=20)
    ar1 = _ar_with([b1], predicted=0)
    ar2 = _ar_with([b2], predicted=0)
    pr1 = PerRecordOutcome(
        batch=ar1,
        success=True,
        shard_id="shardId-0",
        sequence_number="s",
        error_code=None,
        error_message=None,
    )
    pr2 = PerRecordOutcome(
        batch=ar2,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code="InternalFailure",
        error_message="x",
    )
    cb, sm = CaptureCallbacks(), FakeShardMap()
    retrier = _retrier(cb, sm)
    await retrier.handle(_outcome(per_record=(pr1, pr2), batch_items=(ar1, ar2)))
    assert len(cb.finished) == 1 and cb.finished[0][0] is b1
    assert cb.retried == [b2]
