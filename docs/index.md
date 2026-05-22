# aiokpl

**Pure-Python async Kinesis producer. KPL-equivalent without a native daemon.**

> A library that respects the shard as the unit of optimization, measures time
> before size, and treats failures as user-visible information — not noise to
> hide.

!!! success "v0.2 available"
    Every phase from scaffolding through the sync bridge is done. The
    full async pipeline ships, metrics are vendor-neutral, and
    non-async callers have a `SyncProducer` over the same core. See
    the [Roadmap](phases/index.md) for per-phase details.

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo scaffolding, design docs | Done |
| 1 | Aggregation codec (KPL wire format) | Done |
| 2 | ShardMap + prediction | Done |
| 3 | Reducer, Aggregator, Collector | Done |
| 4 | Limiter + TokenBucket | Done |
| 5 | Sender + Retrier | Done |
| 6 | Producer + lifecycle (first usable release: v0.1) | Done |
| 7 | CloudWatch metrics (opt-in) | Done |
| 8 | Sync bridge (`SyncProducer`) | Done |

## What v0.2 ships

- **The full pipeline** — `Producer` → `Aggregator` → `Limiter` →
  `Collector` → `Sender` → `Retrier` — wired end-to-end and exercised
  against `etspaceman/kinesis-mock` in CI. Deadline-driven batching at
  two levels, per-shard rate limiting at the Kinesis hard caps, smart
  retry classification, per-record attempt history.
- **Vendor-neutral metrics.** The library emits semantic events and
  hands them to a `MetricsSink` you plug in. Default is
  [`NullSink`][aiokpl.sinks.NullSink] (zero overhead). First-party
  sinks: [`InMemorySink`][aiokpl.sinks.InMemorySink] for tests,
  [`CloudWatchSink`][aiokpl.sinks.CloudWatchSink] bundled (since
  `aiobotocore` is already a dep), plus `OpenTelemetrySink` and
  `DatadogSink` behind the `aiokpl[otel]` and `aiokpl[datadog]`
  extras. The core has no vendor strings.
- **Sync bridge.** [`SyncProducer`][aiokpl.SyncProducer] wraps the async
  `Producer` behind `anyio.from_thread.start_blocking_portal()` so
  Flask/Django handlers, Jupyter cells, and plain scripts can submit
  records without an event loop. Thread-safe `put_record`; bounded
  `wait(timeout=)` and `flush(timeout=)`.

## Why aiokpl

AWS ships the Kinesis Producer Library as a native C++ binary wrapped in
Java/.NET sidecars. The Python ecosystem has never had a real KPL — only a
handful of thin batchers over `boto3` and a Python codec for the aggregation
format. None of them give you what the C++ KPL gives you: shard-aware
batching, deadline-driven flushes, smart retry classification, and per-record
attempt history.

`aiokpl` is a clean-room reimplementation of those ideas in idiomatic async
Python, built on `anyio` so the same code runs on both the `asyncio` and
`trio` runtimes. It preserves what was worth preserving (shard-aware
pipeline, deadline-driven batching, byte-exact aggregation) and drops what
was an accident of being written in C++ (IPC, named pipes, child process,
custom spinlocks, static binaries, packaging hell).

It is not a wrapper around the C++ binary. It is a reimplementation of its
ideas in a language where you do not need a daemon.

## Key principles

- **The shard is the unit of optimization, not the stream.**
- **Predict before asking** — sharding is deterministic.
- **Batching is governed by deadlines, not sizes.**
- **Each stage has one responsibility and one downstream callback.**
- **Failures are data, not exceptions.**
- **Bounded latency beats maximum throughput.**
- **Backend-agnostic**: built on `anyio`, so the same code runs on both
  `asyncio` and `trio`.

See [Philosophy](philosophy.md) for the full rationale.

## Get started

```python
import anyio
from aiokpl import Producer, Config

async def main():
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

The `Producer` is asyncio-only (aiobotocore is asyncio-only). The lower
phases (codec, ShardMap, Reducer, Aggregator, Collector, Limiter) remain
backend-agnostic and are tested on both asyncio and trio.

## Synchronous usage

For callers that don't run an async event loop (Flask/Django handlers,
scripts, Jupyter), use [`SyncProducer`](phases/phase-8-sync.md):

```python
from aiokpl import Config, SyncProducer

with SyncProducer(Config(region="us-east-1")) as producer:
    outcome = producer.put_record(
        stream="my-stream",
        partition_key="user-123",
        data=b"hello",
    )
    result = outcome.wait(timeout=5.0)
    if result.success:
        print(result.shard_id, result.sequence_number)
```

Same shape as the async API. Under the hood a private
`anyio.from_thread.BlockingPortal` runs the async `Producer` on a
background thread; `put_record` is thread-safe and `wait()` / `flush()`
accept timeouts.
