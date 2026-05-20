# Roadmap

`aiokpl` is shipped in phases. Each phase delivers something testable on
its own and never speculates ahead — a stage lands only when its
predecessors are green.

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

## Per-phase summary

- **Phase 1 — Codec.** Byte-exact KPL aggregation: encode, decode, and the
  hash-key helpers used to predict shards. Zero runtime dependencies.
  Conformance-tested against `kinesis-mock` for byte-exact roundtrip.
  [Details](phase-1-codec.md).
- **Phase 2 — ShardMap.** Cached, async-refreshed list of shards with
  `bisect_left` lookup and `invalidate` semantics. Transport-agnostic:
  takes an injected `list_shards_fn` so it can be wired to `aiobotocore`
  (production) or a fake (tests). [Details](phase-2-shardmap.md).
- **Phase 3 — Reducer / Aggregator / Collector.** The generic deadline-driven
  batcher (`reducer.py`) and the two batchers built on top: per-shard
  aggregation and `PutRecords`-batch collection.
- **Phase 4 — Limiter + TokenBucket.** Multi-stream token bucket
  (records/s + bytes/s), per-shard limiter with a 25 ms drain loop.
- **Phase 5 — Sender + Retrier.** Glue to `aiobotocore.put_records` and
  the full retry classification table.
- **Phase 6 — Producer + lifecycle.** First usable release. Per-stream
  pipelines, graceful shutdown, backpressure semaphore.
- **Phase 7 — Metrics (optional).** CloudWatch counters per
  (stream, shard, name).
- **Phase 8 — Sync bridge (optional).** Thin wrapper for non-asyncio
  callers.
