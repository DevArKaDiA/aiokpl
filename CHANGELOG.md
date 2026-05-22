# Changelog

All notable changes to `aiokpl` are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [SemVer](https://semver.org/spec/v2.0.0.html). Pre-1.0 the
public API may change between minor versions; the on-the-wire KPL
aggregation format is frozen.

## [Unreleased]

## [0.2.0] — 2026-05-21

First public release.

### Added

- **Producer** (`async with Producer(config) as producer:`) wiring the full
  pipeline: `Aggregator` → `Limiter` → `Collector` → `Sender` → `Retrier`.
  Per-stream pipelines lazily created, backpressure via
  `max_outstanding_records` semaphore, graceful shutdown via
  `flush()` + `__aexit__`.
- **`SyncProducer`** for callers not running an async event loop. Spawns an
  internal `anyio.from_thread.BlockingPortal`; thread-safe; `wait(timeout=)`
  and `flush(timeout=)` raise `TimeoutError` on elapse.
- **`Outcome[T]`** — backend-agnostic one-shot future replacing
  `asyncio.Future` so the producer works on both `asyncio` and `trio`.
- **Byte-exact KPL aggregation codec** (`encode_aggregated` /
  `decode_aggregated`) with hand-rolled protobuf — zero dependency on the
  `protobuf` package.
- **`ShardMap`** with `bisect_left` prediction over a cached, async-refreshed
  list of shards. State machine `INVALID → UPDATING → READY` with
  `invalidate()` semantics, exponential backoff (1 s → 30 s), and
  closed-shard cleanup TTL.
- **`Reducer[I, B]`** generic deadline-driven batcher with FIFO-by-deadline
  packing and excess re-injection.
- **`Aggregator`** producing per-shard aggregated batches; falls back to
  single-record mode when the shard map is not ready or aggregation is
  disabled.
- **`Collector`** with 500-record / 5-MiB / 256-KiB-per-shard short-circuit.
- **`Limiter` + `TokenBucket`** — multi-stream token bucket (records/s +
  bytes/s) with a 25 ms drain loop; per-shard isolation; `Expired` route
  for records that age past their TTL.
- **`Sender`** wrapping `aiobotocore` for the `PutRecords` call. Captures
  per-record outcomes and surfaces request-level failures uniformly.
- **`Retrier`** with the full classification table: throttle / transient /
  wrong-shard (split-aware) / expired. Per-record `Attempt` history visible
  to the user via `RecordResult.attempts`.
- **Vendor-neutral metrics** via the `MetricsSink` Protocol. First-party
  sinks: `NullSink` (default, zero overhead), `InMemorySink`,
  `CloudWatchSink`. Optional sinks behind extras: `OpenTelemetrySink`
  (`aiokpl[otel]`), `DatadogSink` (`aiokpl[datadog]`).
- **`anyio`** as the async runtime — works on both `asyncio` and `trio`
  for the non-network stages. Network layer (`Sender`, `Retrier`,
  `CloudWatchSink`) is asyncio-only because `aiobotocore` is asyncio-only.

### Tested

- 480 unit tests, every async test parametrized across both `asyncio` and
  `trio` backends.
- 20 integration tests against `etspaceman/kinesis-mock` (byte-exact
  hash-key routing, paginated `ListShards`, split-shard children, retry
  paths, backpressure, end-to-end aggregation roundtrip).
- 100 % line and branch coverage on the `aiokpl/` package.
- `ruff check`, `ruff format`, `ty check`, and `mkdocs build --strict` all
  clean.

[Unreleased]: https://github.com/DevArKaDiA/aiokpl/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/DevArKaDiA/aiokpl/releases/tag/v0.2.0
