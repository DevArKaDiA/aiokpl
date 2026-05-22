# aiokpl

**Pure-Python async Kinesis producer. KPL-equivalent without a native daemon.**

> A library that respects the shard as the unit of optimization, measures time
> before size, and treats failures as user-visible information — not noise to
> hide.

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

## What aiokpl is

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

## Where to go next

- **[Why aiokpl?](why.md)** — the gap in the Python ecosystem, what makes
  this different from `aws-kinesis-agg` + `boto3`, and when *not* to use it.
- **[Get started](getting-started.md)** — installation, the first program,
  every `Config` knob, sinks, troubleshooting.
- **[Learn the design](philosophy.md)** — the six principles in depth.
- **[Architecture](architecture.md)** — the pipeline, stage by stage.
<!-- benchmarks.md owned by another agent; link added when the page lands. -->
