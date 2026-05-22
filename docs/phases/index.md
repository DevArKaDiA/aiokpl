# Roadmap

`aiokpl` is shipped in phases. Each phase delivers something testable on
its own and never speculates ahead — a stage lands only when its
predecessors are green.

!!! success "v0.2 — all phases complete"
    Every phase from scaffolding through the sync bridge is done. The
    full async pipeline is wired, exercised end-to-end against
    `kinesis-mock`, metrics ship vendor-neutral with first-party
    CloudWatch / OpenTelemetry / Datadog sinks, and non-async callers
    have a thread-safe `SyncProducer` over the same core.

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

## Per-phase summary

- **Phase 1 — Codec.** Byte-exact KPL aggregation: encode, decode, and the
  hash-key helpers used to predict shards. Zero runtime dependencies.
  Conformance-tested against `kinesis-mock` for byte-exact roundtrip.
  [Details](phase-1-codec.md).
- **Phase 2 — ShardMap.** Cached, async-refreshed list of shards with
  `bisect_left` lookup and `invalidate` semantics. Transport-agnostic:
  takes an injected `list_shards_fn` so it can be wired to `aiobotocore`
  (production) or a fake (tests). [Details](phase-2-shardmap.md).
- **Phase 3 — Reducer / Aggregator / Collector.** The generic
  deadline-driven batcher (`reducer.py`) and the two batchers built on
  top: per-shard aggregation and `PutRecords`-batch collection with the
  256 KiB-per-shard short-circuit. [Details](phase-3-batching.md).
- **Phase 4 — Limiter + TokenBucket.** Multi-stream token bucket
  (records/s + bytes/s), per-shard limiter with a 25 ms drain loop and
  an expired-record path that surfaces through the same Retrier
  classification as network errors. [Details](phase-4-rate-limiting.md).
- **Phase 5 — Sender + Retrier.** Glue to `aiobotocore.put_records` and
  the full retry classification table. [Details](phase-5-sender-retrier.md).
- **Phase 6 — Producer + lifecycle.** First usable release (v0.1).
  Per-stream pipelines, graceful shutdown, backpressure semaphore.
  [Details](phase-6-producer.md).
- **Phase 7 — Metrics.** Vendor-neutral semantic events with a
  `MetricsSink` Protocol. First-party sinks for CloudWatch (bundled),
  OpenTelemetry (via `aiokpl[otel]`), and Datadog (via
  `aiokpl[datadog]`). Default `NullSink` is zero-overhead.
  [Details](phase-7-metrics.md).
- **Phase 8 — Sync bridge.** Thread-safe `SyncProducer` over the async
  core via `anyio.from_thread.start_blocking_portal()`. Bounded
  `wait(timeout=)` and `flush(timeout=)`. [Details](phase-8-sync.md).
