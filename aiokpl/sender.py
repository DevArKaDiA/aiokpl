"""``PutRecordsBatch`` → Kinesis ``PutRecords`` glue.

Mirrors ``aws/kinesis/core/put_records_context.{h,cc}`` plus the dispatch slice
of ``aws/kinesis/core/pipeline.h``: the Sender owns no state beyond a client +
stream binding and a clock; it just builds the request, awaits the SDK call,
times it, and packages the response into a :class:`SendOutcome` aligned with
the input batch's items.

The Sender deliberately does NOT classify retries. Classification lives in
:class:`aiokpl.retrier.Retrier` so the "what happened to this record" data
type is independent from the "what should we do next" policy — see the
philosophy section of ``CLAUDE.md`` on "Failures are data" and "Each stage
has one responsibility".

``aiobotocore`` is asyncio-only; tests inject fakes via the duck-typed
:class:`_KinesisClient` Protocol so the unit-test path stays
backend-agnostic.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from aiokpl.aggregator import AggregatedBatch
from aiokpl.collector import PutRecordsBatch
from aiokpl.metrics import NAME_REQUEST_TIME, MetricsManager


@runtime_checkable
class _KinesisClient(Protocol):
    """Minimal duck-type the Sender needs from aiobotocore's Kinesis client.

    Decouples the Sender from aiobotocore so tests can inject fakes and so we
    can swap clients (a future per-region client cache, for instance) without
    touching the Sender.
    """

    async def put_records(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(slots=True, frozen=True)
class PerRecordOutcome:
    """One slot in the ``PutRecords`` response, aligned with the batch items."""

    batch: AggregatedBatch
    success: bool
    shard_id: str | None
    sequence_number: str | None
    error_code: str | None
    error_message: str | None


@dataclass(slots=True)
class SendOutcome:
    """The Sender's report to the Retrier.

    If the request itself failed (network / botocore exception) or the response
    record count did not match the request, ``request_error`` is set and
    ``per_record`` is empty. Otherwise ``request_error`` is ``None`` and
    ``per_record`` is aligned position-by-position with ``batch.items``.

    ``batch_items`` always carries the original :class:`AggregatedBatch`
    sequence in input order — the Retrier needs it on the request-error path
    where ``per_record`` is empty but every :class:`UserRecord` still has to
    be classified (matches ``retrier.cc:55-66``).
    """

    stream_name: str
    started_at: float
    ended_at: float
    request_error: tuple[str, str] | None
    per_record: tuple[PerRecordOutcome, ...]
    batch_items: tuple[AggregatedBatch, ...]


def _classify_request_exception(exc: BaseException) -> tuple[str, str]:
    # botocore.exceptions.ClientError carries a structured Error/Code we can
    # surface verbatim; anything else is an unexpected runtime failure that
    # the Retrier should treat as transient ("Internal" matches the C++ code
    # used by retrier.cc for non-throttle service errors).
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        err = response.get("Error")
        if isinstance(err, dict):
            code = err.get("Code")
            message = err.get("Message")
            if isinstance(code, str) and isinstance(message, str):
                return (code, message)
    return ("Internal", str(exc))


class Sender:
    """Send a :class:`PutRecordsBatch` to Kinesis. Stateless beyond client+stream."""

    __slots__ = ("_client", "_clock", "_metrics", "_stream_name")

    def __init__(
        self,
        *,
        stream_name: str,
        client: _KinesisClient,
        clock: Callable[[], float] = time.monotonic,
        metrics: MetricsManager | None = None,
    ) -> None:
        self._stream_name = stream_name
        self._client = client
        self._clock = clock
        self._metrics = metrics

    async def send(self, batch: PutRecordsBatch) -> SendOutcome:
        """Issue ``PutRecords``, return outcome.

        Never raises on network/AWS errors — surfaces them via
        ``SendOutcome.request_error``. Raises ``ValueError`` only on
        programmer errors (an empty batch).
        """
        if batch.count == 0:
            raise ValueError("Sender.send() called with an empty PutRecordsBatch")

        batch_items = tuple(batch.items)
        records = []
        for ar in batch.items:
            entry: dict[str, Any] = {
                "Data": ar.to_blob(),
                "PartitionKey": ar.routing_partition_key(),
            }
            ehk = ar.routing_explicit_hash_key()
            # boto rejects ``ExplicitHashKey=None``; omit the kwarg instead.
            if ehk is not None:
                entry["ExplicitHashKey"] = ehk
            records.append(entry)

        started_at = self._clock()
        try:
            response = await self._client.put_records(
                StreamName=self._stream_name,
                Records=records,
            )
        except BaseException as exc:
            ended_at = self._clock()
            self._emit_request_time(started_at, ended_at)
            return SendOutcome(
                stream_name=self._stream_name,
                started_at=started_at,
                ended_at=ended_at,
                request_error=_classify_request_exception(exc),
                per_record=(),
                batch_items=batch_items,
            )
        ended_at = self._clock()
        self._emit_request_time(started_at, ended_at)

        response_records = response.get("Records", ())
        if len(response_records) != batch.count:
            # Matches retrier.cc:170-180: a count mismatch is a service-side
            # protocol violation, not a per-record failure. Bail without
            # trying to align slots.
            msg = (
                f"PutRecords returned {len(response_records)} records for a batch of {batch.count}"
            )
            return SendOutcome(
                stream_name=self._stream_name,
                started_at=started_at,
                ended_at=ended_at,
                request_error=("RecordCountMismatch", msg),
                per_record=(),
                batch_items=batch_items,
            )

        per_record = tuple(
            _build_per_record(ar, slot)
            for ar, slot in zip(batch.items, response_records, strict=True)
        )
        return SendOutcome(
            stream_name=self._stream_name,
            started_at=started_at,
            ended_at=ended_at,
            request_error=None,
            per_record=per_record,
            batch_items=batch_items,
        )

    def _emit_request_time(self, started_at: float, ended_at: float) -> None:
        if self._metrics is None:
            return
        self._metrics.put(
            NAME_REQUEST_TIME,
            (ended_at - started_at) * 1000.0,
            stream=self._stream_name,
        )


def _build_per_record(ar: AggregatedBatch, slot: dict[str, Any]) -> PerRecordOutcome:
    seq = slot.get("SequenceNumber")
    if seq:
        return PerRecordOutcome(
            batch=ar,
            success=True,
            shard_id=slot.get("ShardId"),
            sequence_number=seq,
            error_code=None,
            error_message=None,
        )
    return PerRecordOutcome(
        batch=ar,
        success=False,
        shard_id=None,
        sequence_number=None,
        error_code=slot.get("ErrorCode"),
        error_message=slot.get("ErrorMessage"),
    )


__all__ = ["PerRecordOutcome", "SendOutcome", "Sender"]
