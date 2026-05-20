# Phase 5 — Sender + Retrier

**Status:** Done.

## What ships

- [`aiokpl.sender`](../reference/aiokpl/sender.md) — sends a
  `PutRecordsBatch` to Kinesis via the duck-typed `_KinesisClient`
  Protocol (aiobotocore in production, fakes in tests). Captures timing,
  classifies request-level errors and count mismatches, surfaces
  per-record outcomes aligned with the input batch.
- [`aiokpl.retrier`](../reference/aiokpl/retrier.md) — implements the
  classification table below. Distinguishes throttle, transient,
  wrong-shard, and expired outcomes. Calls back into the Aggregator
  (`on_retry`) or the user-facing future resolver (`on_finish`).
- [`aiokpl.result`](../reference/aiokpl/result.md) — `Attempt` and
  `RecordResult` dataclasses (frozen, slotted) carrying the full attempt
  history a caller sees on the user-facing future.

`aiobotocore` is now a runtime dependency; the `integration` extra keeps
only `docker` + `urllib3`. aiobotocore is asyncio-only — Sender/Retrier
lose trio support, while Phase 4 and below stay backend-agnostic.

## Pipeline placement

```
PutRecordsBatch
   │
   ▼
Sender ─── aiobotocore.put_records ──► Kinesis
   │
   ▼
SendOutcome (timing + per-record slots + batch_items)
   │
   ▼
Retrier ── classify ── on_finish(buffered, RecordResult)
                    └─ on_retry(buffered)  ─► Aggregator.put_buffered
```

`SendOutcome.batch_items` is always populated so the Retrier can iterate
the original `AggregatedBatch` list even on the request-error path where
`per_record` is empty.

## Classification table

The Retrier mirrors `aws/kinesis/core/retrier.cc` in the C++ KPL.

| Outcome | Action |
|---|---|
| Request-level error, code is `ProvisionedThroughputExceededException`, `fail_if_throttled=True` | fail every UR with that code/msg |
| Request-level error, any other code or `fail_if_throttled=False` | retry every UR |
| Per-record success, `predicted == actual` or `predicted is None` | `on_finish(success)` |
| Per-record success, hash key lies inside actual shard's range (child after split) | `on_finish(success)` + `ShardMap.invalidate` |
| Per-record success, hash key lies outside actual shard's range | `on_retry("Wrong Shard")` + `ShardMap.invalidate` |
| Per-record failure, code is `ProvisionedThroughputExceededException`, `fail_if_throttled=True` | fail |
| Per-record failure, any other code or `fail_if_throttled=False` | retry |

`retry_not_expired` body, in order:

1. Append `Attempt(success=False, code, message)` to the buffered
   record's `attempts` list.
2. If `clock() - arrival_time > record_ttl`, append a second
   `Attempt(code="Expired")` and call `on_finish` with a failed
   `RecordResult` — no `on_retry`.
3. Otherwise bump the buffered record's deadline by
   `retry_deadline_ms / 2` (matches `retrier.cc:160`) and call
   `on_retry(buffered)`. The Aggregator's `put_buffered` re-enqueues the
   same `_BufferedRecord` so the attempt history and `arrival_time`
   survive across retries.

For multi-record aggregated batches, the AR-level `invalidate` fires
exactly once even though each `UserRecord` is classified individually
(matches `retrier.cc:108-115`).

## Attempt history

Every trip through Sender + Retrier appends one `Attempt`. The terminal
`RecordResult` snapshots `attempts` as an immutable tuple. Callers
receive the full history through `RecordResult.attempts`, which lets
them tell a single-try transient error apart from a 30-second-long
throttle storm.

## Sender error mapping

- `botocore.exceptions.ClientError` is surfaced as
  `(response["Error"]["Code"], response["Error"]["Message"])`.
- Any other exception (including non-`Exception` `BaseException`) is
  caught and surfaced as `("Internal", str(exc))` — matches the C++
  retrier's "Internal" code for unclassifiable service errors.
- `len(response["Records"]) != batch.count` becomes
  `("RecordCountMismatch", "...")` and treats every UR as a request-
  level failure. Matches `retrier.cc:170-180`.
