# aiokpl

**Pure-Python async Kinesis producer. KPL-equivalent without a native daemon.**

> A library that respects the shard as the unit of optimization, measures time
> before size, and treats failures as user-visible information — not noise to
> hide.

!!! warning "Pre-alpha"
    `aiokpl` is in active design and early implementation. It is not on PyPI
    yet, the public API may change without notice, and only the codec and
    shard map are shipped today. See the [Roadmap](phases/index.md) for what
    is coming and when.

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo scaffolding, design docs | Done |
| 1 | Aggregation codec (KPL wire format) | Done |
| 2 | ShardMap + prediction | Done |
| 3 | Reducer, Aggregator, Collector | Next |
| 4 | Limiter + TokenBucket | Planned |
| 5 | Sender + Retrier | Planned |
| 6 | Producer + lifecycle (first usable release: v0.1) | Planned |
| 7 | CloudWatch metrics | Optional |
| 8 | Sync bridge | Optional |

## Why aiokpl

AWS ships the Kinesis Producer Library as a native C++ binary wrapped in
Java/.NET sidecars. The Python ecosystem has never had a real KPL — only a
handful of thin batchers over `boto3` and a Python codec for the aggregation
format. None of them give you what the C++ KPL gives you: shard-aware
batching, deadline-driven flushes, smart retry classification, and per-record
attempt history.

`aiokpl` is a clean-room reimplementation of those ideas in idiomatic
`asyncio` Python. It preserves what was worth preserving (shard-aware
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
import asyncio
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
        fut = await producer.put_record(
            stream="my-stream",
            partition_key="user-123",
            data=b"hello",
        )
        result = await fut
        if result.success:
            print(result.shard_id, result.sequence_number)
        else:
            print("failed:", result.attempts[-1].error_code)

asyncio.run(main())
```

!!! info "The `Producer` class is Phase 6"
    The snippet above shows the **target** public API. Today, only the codec
    (`encode_aggregated` / `decode_aggregated`) and `ShardMap` are usable
    directly. Tracking issue: see the [Roadmap](phases/index.md).
