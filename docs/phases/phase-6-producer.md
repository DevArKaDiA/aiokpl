# Phase 6 — Producer + lifecycle (v0.1)

**Status:** Done. First usable release.

## What ships

- [`aiokpl.producer.Producer`](../reference/aiokpl/producer.md) — top-level
  orchestrator. `async with Producer(config)` lifecycle, per-stream
  pipelines composed lazily on first `put_record`, single shared
  `aiobotocore` Kinesis client owned by an `AsyncExitStack`.
- [`aiokpl.config.Config`](../reference/aiokpl/config.md) — frozen dataclass
  with every tunable: aggregation, deadlines, rate limits, retry policy,
  backpressure, AWS endpoint overrides.
- [`aiokpl.outcome.Outcome`](../reference/aiokpl/outcome.md) — anyio-friendly
  one-shot value-bearing event. Replaces `asyncio.Future` for cross-backend
  portability — works on both `asyncio` and `trio`. `put_record` returns
  one; the caller `await`s it for the terminal `RecordResult`.

`aiobotocore` is asyncio-only, so the public `Producer` API is asyncio-only.
Phases 1–4 remain backend-agnostic and continue to be tested on both
backends.

## Pipeline composition

Each stream gets its own `_StreamPipeline` wired through six private
callbacks, one per stage transition:

```
UserRecord
   │  Producer.put_record()
   ▼
Aggregator   ──on_batch_ready──► Limiter.put
   ▼
Limiter      ──on_admit────────► Collector.put
             ──on_expired───────► synthesize SendOutcome("Expired") ► Retrier.handle
   ▼
Collector    ──on_batch_ready──► tg.start_soon(Sender.send + Retrier.handle)
   ▼
Sender       ─ aiobotocore.put_records ─► Kinesis
   ▼
Retrier      ──on_finish────────► Outcome.set_value(RecordResult), release semaphore
             ──on_retry─────────► Aggregator.put_buffered
```

The dispatch from Collector to Sender is `tg.start_soon`-ed in the
Producer's background task group so collecting more records is not blocked
on the network round-trip. This mirrors the C++ KPL comment in
`pipeline.h:206` about not hammering downstream from SDK callback threads.

## Expired-batch unification

When the `Limiter`'s TTL elapses before tokens become available, it calls
`on_expired(batch, reason)`. The Producer synthesizes a `SendOutcome` with
`request_error=("Expired", reason)` and routes it through `Retrier.handle`.
That keeps a single classification code path: every record's attempt list
and terminal `RecordResult` is assembled the same way whether the failure
came from the wire or from the rate limiter.

## Backpressure

`Config.max_outstanding_records` caps the number of records in the pipeline
between `put_record` and outcome resolution. The Producer's
`anyio.Semaphore` is acquired on submit and released by the Retrier's
`on_finish` (the terminal callback). When saturated, `put_record` blocks —
the user gets bounded memory by default.

## Get started

```python
import anyio
from aiokpl import Producer, Config

async def main() -> None:
    cfg = Config(
        region="us-east-1",
        aggregation_enabled=True,
        record_max_buffered_time_ms=100,
        record_ttl_ms=30_000,
        fail_if_throttled=False,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="my-stream",
            partition_key="user-123",
            data=b"hello",
        )
        result = await outcome.wait()
        if result.success:
            print(result.shard_id, result.sequence_number)
        else:
            print("failed:", result.attempts[-1].error_code)

anyio.run(main)
```

## Coverage

100% line + branch on every module in `aiokpl/`. Producer is exercised
end-to-end against `etspaceman/kinesis-mock` with 100 random-partition-key
records (`tests/integration/test_producer_integration.py`).
