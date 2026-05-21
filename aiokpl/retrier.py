"""Classify ``PutRecords`` outcomes; dispatch finish or retry.

Mirrors ``aws/kinesis/core/retrier.{h,cc}`` in the C++ KPL. This is the most
important policy module in the library — see "Retrier classification" in
``CLAUDE.md`` for the source-of-truth table.

Per-record classification:

============================================ ================
Outcome                                       Action
============================================ ================
Success, predicted == actual or predicted is finish(success)
``None``
Success, hash key inside actual shard's      finish(success)
range (child after split)                    + invalidate
Success, hash key outside actual shard's     retry("Wrong
range                                        Shard") + invalidate
Per-record throttle + ``fail_if_throttled``  fail
Per-record throttle, no fail flag            retry
Per-record any other error                   retry
============================================ ================

Request-level error (no per-record results) applies the throttle rule to every
:class:`UserRecord` in the batch (retrier.cc:55-66).

The Retrier does NOT own the user-facing :class:`asyncio.Future`. It receives
two injected callbacks: ``on_finish`` (terminal — resolve the future) and
``on_retry`` (re-enqueue at the Aggregator). Phase 6's Producer wires them.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from aiokpl.aggregator import AggregatedBatch, _BufferedRecord
from aiokpl.metrics import (
    NAME_ALL_ERRORS,
    NAME_ERRORS_BY_CODE,
    NAME_KINESIS_RECORDS_DATA_PUT,
    NAME_KINESIS_RECORDS_PUT,
    NAME_RETRIES_PER_RECORD,
    NAME_USER_RECORDS_DATA_PUT,
    NAME_USER_RECORDS_PUT,
    MetricsManager,
)
from aiokpl.result import Attempt, RecordResult
from aiokpl.sender import PerRecordOutcome, SendOutcome

PROVISIONED_THROUGHPUT_EXCEEDED = "ProvisionedThroughputExceededException"
EXPIRED_ERROR_CODE = "Expired"
EXPIRED_ERROR_MESSAGE = "Record TTL exceeded"
WRONG_SHARD_ERROR_CODE = "Wrong Shard"
WRONG_SHARD_ERROR_MESSAGE = (
    "Record landed on a shard whose hash range does not contain the record's hash key"
)


@runtime_checkable
class _ShardMapForRetrier(Protocol):
    """The Retrier needs ``hashrange()`` and ``invalidate()`` — nothing else.

    Matches the duck-typed pattern used elsewhere in aiokpl (see
    :class:`aiokpl.aggregator._ShardLookup`): a minimal Protocol so tests can
    inject fakes and the runtime contract is explicit.
    """

    def hashrange(self, shard_id: int) -> tuple[int, int] | None: ...
    async def invalidate(self, seen_at: float, predicted_shard: int | None) -> None: ...


_FinishCB = Callable[[_BufferedRecord, RecordResult], Awaitable[None]]
_RetryCB = Callable[[_BufferedRecord], Awaitable[None]]


def _parse_actual_shard_id(shard_id: str | None) -> int | None:
    # Kinesis returns ``shardId-<n>`` strings; the Retrier wants the integer to
    # compare with the predicted shard. Defensive on malformed inputs because
    # the C++ retrier just logs and continues, never crashes.
    if not shard_id or not shard_id.startswith("shardId-"):
        return None
    suffix = shard_id[len("shardId-") :]
    if not suffix.isdigit():
        return None
    return int(suffix)


class Retrier:
    """Execute the classification table from ``CLAUDE.md``."""

    __slots__ = (
        "_clock",
        "_fail_if_throttled",
        "_metrics",
        "_on_finish",
        "_on_retry",
        "_record_ttl",
        "_retry_deadline",
        "_shard_map",
        "_stream_name",
    )

    def __init__(
        self,
        *,
        shard_map: _ShardMapForRetrier,
        on_finish: _FinishCB,
        on_retry: _RetryCB,
        record_ttl_ms: float = 30_000.0,
        fail_if_throttled: bool = False,
        retry_deadline_ms: float = 50.0,
        clock: Callable[[], float] = time.monotonic,
        metrics: MetricsManager | None = None,
        stream_name: str | None = None,
    ) -> None:
        self._shard_map = shard_map
        self._on_finish = on_finish
        self._on_retry = on_retry
        self._record_ttl = record_ttl_ms / 1000.0
        self._fail_if_throttled = fail_if_throttled
        # retrier.cc:160 — re-enqueued records are given half of the
        # ``record_max_buffered_time`` so they get a faster second chance.
        self._retry_deadline = retry_deadline_ms / 1000.0 / 2.0
        self._clock = clock
        self._metrics = metrics
        self._stream_name = stream_name

    async def handle(self, outcome: SendOutcome) -> None:
        """Walk every record in the batch, classify, dispatch."""
        if outcome.request_error is not None:
            await self._handle_request_error(outcome)
            return
        for per_record in outcome.per_record:
            await self._handle_per_record(outcome, per_record)

    # ─── Request-level branch ──────────────────────────────────────────────

    async def _handle_request_error(self, outcome: SendOutcome) -> None:
        assert outcome.request_error is not None
        code, message = outcome.request_error
        throttled = code == PROVISIONED_THROUGHPUT_EXCEEDED
        for ar in outcome.batch_items:
            for buffered in ar.items:
                if throttled and self._fail_if_throttled:
                    await self._fail(outcome, buffered, code, message)
                else:
                    await self._retry_not_expired(outcome, buffered, code, message)

    # ─── Per-record branch ─────────────────────────────────────────────────

    async def _handle_per_record(self, outcome: SendOutcome, per_record: PerRecordOutcome) -> None:
        if per_record.success:
            await self._handle_success(outcome, per_record)
            return
        code = per_record.error_code or "Unknown"
        message = per_record.error_message or ""
        throttled = code == PROVISIONED_THROUGHPUT_EXCEEDED
        for buffered in per_record.batch.items:
            if throttled and self._fail_if_throttled:
                await self._fail(outcome, buffered, code, message)
            else:
                await self._retry_not_expired(outcome, buffered, code, message)

    async def _handle_success(self, outcome: SendOutcome, per_record: PerRecordOutcome) -> None:
        predicted = per_record.batch.predicted_shard
        actual_int = _parse_actual_shard_id(per_record.shard_id)
        # No predicted shard means the ShardMap was not READY when the record
        # was buffered; we have nothing to validate against, just accept.
        if predicted is None or actual_int is None or predicted == actual_int:
            self._emit_kinesis_record_metrics(per_record.batch, per_record.shard_id)
            for buffered in per_record.batch.items:
                await self._finish_success(outcome, buffered, per_record)
            return

        # Predicted differs from actual. Inspect the actual shard's hash range:
        # if the record's hash key lies inside it, we hit a child after a
        # split — accept the success but invalidate the cached map. If the
        # hash key is outside, Kinesis routed the record somewhere we did not
        # ask for; retry as Wrong Shard.
        actual_range = self._shard_map.hashrange(actual_int)
        invalidated = False
        emitted_kinesis = False
        for buffered in per_record.batch.items:
            in_range = (
                actual_range is not None and actual_range[0] <= buffered.hash_key <= actual_range[1]
            )
            if in_range:
                if not emitted_kinesis:
                    self._emit_kinesis_record_metrics(per_record.batch, per_record.shard_id)
                    emitted_kinesis = True
                await self._finish_success(outcome, buffered, per_record)
            else:
                await self._retry_not_expired(
                    outcome, buffered, WRONG_SHARD_ERROR_CODE, WRONG_SHARD_ERROR_MESSAGE
                )
            if not invalidated:
                # retrier.cc:108-115 — invalidate exactly once per KR, no
                # matter how many URs it contains.
                await self._shard_map.invalidate(self._clock(), actual_int)
                invalidated = True

    # ─── Terminal helpers ──────────────────────────────────────────────────

    async def _finish_success(
        self,
        outcome: SendOutcome,
        buffered: _BufferedRecord,
        per_record: PerRecordOutcome,
    ) -> None:
        buffered.attempts.append(
            Attempt(
                started_at=outcome.started_at,
                ended_at=outcome.ended_at,
                success=True,
                shard_id=per_record.shard_id,
                sequence_number=per_record.sequence_number,
            )
        )
        result = RecordResult(
            success=True,
            shard_id=per_record.shard_id,
            sequence_number=per_record.sequence_number,
            attempts=tuple(buffered.attempts),
        )
        self._emit_user_record_success(buffered, per_record.shard_id)
        await self._on_finish(buffered, result)

    async def _fail(
        self,
        outcome: SendOutcome,
        buffered: _BufferedRecord,
        code: str,
        message: str,
    ) -> None:
        buffered.attempts.append(
            Attempt(
                started_at=outcome.started_at,
                ended_at=outcome.ended_at,
                success=False,
                error_code=code,
                error_message=message,
            )
        )
        result = RecordResult(
            success=False,
            shard_id=None,
            sequence_number=None,
            attempts=tuple(buffered.attempts),
        )
        self._emit_error(code)
        await self._on_finish(buffered, result)

    async def _retry_not_expired(
        self,
        outcome: SendOutcome,
        buffered: _BufferedRecord,
        code: str,
        message: str,
    ) -> None:
        buffered.attempts.append(
            Attempt(
                started_at=outcome.started_at,
                ended_at=outcome.ended_at,
                success=False,
                error_code=code,
                error_message=message,
            )
        )
        self._emit_error(code)
        now = self._clock()
        if now - buffered.arrival_time > self._record_ttl:
            # The failed Attempt is already recorded above; tack on an Expired
            # attempt so the user sees both the trigger and the verdict.
            buffered.attempts.append(
                Attempt(
                    started_at=now,
                    ended_at=now,
                    success=False,
                    error_code=EXPIRED_ERROR_CODE,
                    error_message=EXPIRED_ERROR_MESSAGE,
                )
            )
            result = RecordResult(
                success=False,
                shard_id=None,
                sequence_number=None,
                attempts=tuple(buffered.attempts),
            )
            self._emit_error(EXPIRED_ERROR_CODE)
            await self._on_finish(buffered, result)
            return
        buffered.deadline = now + self._retry_deadline
        await self._on_retry(buffered)

    # ─── Metrics helpers ───────────────────────────────────────────────────

    def _emit_user_record_success(self, buffered: _BufferedRecord, shard_id: str | None) -> None:
        if self._metrics is None:
            return
        self._metrics.put(
            NAME_USER_RECORDS_PUT,
            1.0,
            stream=self._stream_name,
            shard_id=shard_id,
        )
        self._metrics.put(
            NAME_USER_RECORDS_DATA_PUT,
            float(len(buffered.user_record.data)),
            stream=self._stream_name,
            shard_id=shard_id,
        )
        # ``len(attempts) - 1`` matches the C++ KPL definition of "retries":
        # the terminal attempt does not count as a retry.
        retries = max(len(buffered.attempts) - 1, 0)
        self._metrics.put(
            NAME_RETRIES_PER_RECORD,
            float(retries),
            stream=self._stream_name,
            shard_id=shard_id,
        )

    def _emit_kinesis_record_metrics(self, batch: AggregatedBatch, shard_id: str | None) -> None:
        if self._metrics is None:
            return
        self._metrics.put(
            NAME_KINESIS_RECORDS_PUT,
            1.0,
            stream=self._stream_name,
            shard_id=shard_id,
        )
        self._metrics.put(
            NAME_KINESIS_RECORDS_DATA_PUT,
            float(batch.size),
            stream=self._stream_name,
            shard_id=shard_id,
        )

    def _emit_error(self, code: str) -> None:
        if self._metrics is None:
            return
        self._metrics.put(NAME_ALL_ERRORS, 1.0, stream=self._stream_name)
        self._metrics.put(
            NAME_ERRORS_BY_CODE,
            1.0,
            stream=self._stream_name,
            error_code=code,
        )


__all__ = [
    "PROVISIONED_THROUGHPUT_EXCEEDED",
    "Retrier",
]
