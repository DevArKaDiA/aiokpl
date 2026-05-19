# aiokpl

Pure-Python async Kinesis producer. KPL-equivalent without a native daemon.

> A library that respects the shard as the unit of optimization, measures time
> before size, and treats failures as user-visible information — not noise to hide.

## Status

Pre-alpha. Design phase. See `CLAUDE.md` for the implementation blueprint.

## Why

AWS ships the official Kinesis Producer Library as a native C++ binary wrapped
in Java/.NET sidecars. The Python ecosystem has never had a real KPL — only
abandoned `boto3` batchers and the `aws-kinesis-agg` codec.

`aiokpl` is a clean-room reimplementation in idiomatic asyncio Python that
preserves what's worth preserving from the C++ KPL (shard-aware pipeline,
deadline-driven batching, smart retry classification) and drops what was an
accident of C++ (IPC, custom spinlocks, static binaries).

## Core principles

1. The shard is the unit of optimization, not the stream.
2. Predict before asking — the sharding algorithm is deterministic.
3. Batching is governed by deadlines, not by sizes.
4. Each stage has one responsibility and one downstream callback.
5. Failures are data, not exceptions.
6. Bounded latency beats maximum throughput.

## Design

See [`CLAUDE.md`](./CLAUDE.md).
