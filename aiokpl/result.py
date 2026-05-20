"""Terminal outcome types handed to the caller after a record's pipeline trip.

Mirrors ``aws/kinesis/core/attempt.h`` and the ``Attempt`` accumulation pattern
in ``aws/kinesis/core/user_record.{h,cc}::to_put_record_result``: every trip
through Sender + Retrier appends one :class:`Attempt`; the terminal
:class:`RecordResult` carries the full attempt history.

These dataclasses are frozen + slotted because they are returned by value to
the user's future and we want both immutability and a small footprint.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Attempt:
    """One trip through the pipeline — success or failure."""

    started_at: float
    ended_at: float
    success: bool
    shard_id: str | None = None
    sequence_number: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(slots=True, frozen=True)
class RecordResult:
    """Terminal outcome handed to the caller after all attempts."""

    success: bool
    shard_id: str | None
    sequence_number: str | None
    attempts: tuple[Attempt, ...]


__all__ = ["Attempt", "RecordResult"]
