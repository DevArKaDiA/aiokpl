# Why aiokpl

## The gap

AWS ships the official Kinesis Producer Library as a native C++ binary
(`amazon-kinesis-producer`) wrapped in Java/.NET sidecars. The Python
ecosystem has never had a real KPL — only:

- **`aws-kinesis-agg`** — Python codec for the aggregation wire format.
  Useful, but not a producer. You still call `boto3.put_records` yourself,
  you still pick the partition key, you still write the retry loop, and you
  still have no idea which shard your record landed on.
- **`kiner`, `kinesis-python`, `kinesis-producer` (ludia)** — abandoned
  community attempts. All thin batchers over `boto3`. None of them predict
  the destination shard, none of them limit per-shard, none of them classify
  retries, none of them carry a per-record attempt history back to the
  caller.

`aiokpl` is a clean-room reimplementation in idiomatic async Python — built
on `anyio` so the same code runs on both the `asyncio` and `trio` runtimes
— that preserves what is worth preserving from the C++ KPL (shard-aware
pipeline, deadline-driven batching, smart retry classification, byte-exact
aggregation) and drops what was an accident of C++: IPC, named pipes, child
process, custom spinlocks, static binaries, packaging hell.

It is not a wrapper around the C++ binary. It is a reimplementation of its
ideas in a language where you do not need a daemon.

## What about LocalStack / `boto3` batching?

A reasonable question: if `aws-kinesis-agg` already implements the wire
format and `boto3` already implements `PutRecords`, what is left?

A lot, actually.

- **`aws-kinesis-agg` is the wire format, not the producer.** It encodes one
  blob from a list of records you already collected. It does not know which
  shard a record will land on, does not group records by predicted shard,
  does not know that Kinesis enforces 1 MiB/s and 1000 records/s **per
  shard** (not per stream), and does not handle the wrong-shard-after-split
  case where a record routes to the parent of a freshly-split shard. The
  KPL does all of that.
- **`boto3.put_records` is a single API call, not a pipeline.** It does not
  know about deadlines, does not back off, does not classify
  `ProvisionedThroughputExceededException` separately from generic
  transient errors, does not know that a successful response can still mean
  *"your shard map is stale"*, and does not carry a per-record attempt
  history. All of that is the KPL's job.
- **LocalStack / `kinesis-mock` are emulators, not clients.** They are how
  we *test* the producer. They are not what users put in front of their
  Kinesis stream in production.

The C++ KPL solves the hard part — turning a stream of user records into a
shard-aware, deadline-bounded, retry-classified `PutRecords` pipeline — and
the Python ecosystem has never had an equivalent. `aiokpl` is that
equivalent.

## Why now, why async

The C++ KPL exists because in 2015 Python did not have `asyncio`, AWS SDKs
did not have async clients, and writing a shard-aware concurrent producer
in CPython was painful. None of those things are true in 2026. `aiobotocore`
gives us non-blocking AWS calls, `anyio` gives us cheap concurrency that
runs on both `asyncio` and `trio` backends, and modern Python gives us the
type system and dataclasses we need to express the pipeline cleanly.

The C++ KPL is a 30k-line C++ program plus a Java sidecar that spawns it as
a subprocess and talks to it over a named pipe. `aiokpl` aims for roughly
2k lines of pure Python with zero native dependencies. The semantics are
the same — the engineering footprint is two orders of magnitude smaller.

## Comparison

The Python ecosystem has tools that solve adjacent slices of the producer
problem. None of them solve the full slice the C++ KPL does. Here's the
honest matrix.

| Capability | aiokpl | `aws-kinesis-agg` + `boto3` | raw `boto3` | `kiner` (Buffer) |
|---|:---:|:---:|:---:|:---:|
| Byte-exact KPL aggregation | yes | yes | no | no |
| Shard prediction (no per-record RPC) | yes | no | no | no |
| Per-shard rate limiting (1000 rec/s + 1 MiB/s) | yes | no | no | no |
| Deadline-driven batching | yes (two-level) | no | no | size-only |
| Smart retry classification (split-aware) | yes | no | no | no |
| Per-record attempt history surfaced to the caller | yes | no | no | no |
| Backpressure (bounded `max_outstanding_records`) | yes | no | no | no |
| Vendor-neutral metrics (CW / OTel / Datadog pluggable) | yes | no | no | no |
| Sync bridge for non-async callers | yes (`SyncProducer`) | n/a (sync-only) | n/a (sync-only) | n/a (sync-only) |
| asyncio and trio backends | yes (via `anyio`) | no | no | no |
| Zero native binary (pure-Python) | yes | yes | yes | yes |

`aws-kinesis-agg` is a codec, not a producer — you bring your own batcher,
your own retry loop, and your own rate limiter. `boto3.put_records` is a
single API call, not a pipeline. `kiner` is a thin Buffer-style batcher with
no shard awareness and no async support. Each is a reasonable tool for the
slice it covers; none is a drop-in replacement for the C++ KPL.

## When NOT to use aiokpl

`aiokpl` is the right tool when you'd otherwise reach for the C++ KPL.
It's the wrong tool in several cases — listed here honestly so you can
skip it without regret.

- **You send fewer than ~100 records/second total.** A plain
  `boto3.put_records` call (or `aiobotocore.put_records`) is enough.
  Aggregation, prediction, and per-shard rate limiting don't pay for their
  configuration cost at that volume.
- **Your records are already larger than ~100 KB each.** Aggregation
  doesn't help — Kinesis already lets you pack up to 1 MB per record, and
  the KPL aggregation envelope is overhead on top. Just batch with
  `put_records` and keep the per-record partition keys.
- **You cannot run an async event loop.** [`SyncProducer`][aiokpl.SyncProducer]
  exists for exactly this, but it costs a background thread and a portal
  per producer instance. If you're already running threaded code at high
  fan-out, `aws-kinesis-agg` + `boto3` with your own thread pool is a
  simpler dependency.
- **You need batch writes to a non-Kinesis target.** DynamoDB BatchWriteItem,
  SQS SendMessageBatch, S3 multipart uploads — different APIs, different
  optimization targets. `aiokpl` is Kinesis-only.
- **You're using Kinesis Firehose.** Firehose is a different service with a
  different API (`PutRecordBatch`) and different optimization rules — the
  shard model doesn't apply, the aggregation format isn't used. Firehose has
  its own producer libraries.
- **You need on-disk durability before sending.** `aiokpl` buffers in
  memory. If a record's outcome needs to survive a process crash, write to
  a local queue (RocksDB, SQLite, Kafka, …) and have a consumer of *that*
  drive `aiokpl`.

## Migrating from the KPL Java sidecar

A natural audience: you already run the C++ KPL via the Java sidecar (or
.NET wrapper) and would like to drop the binary. The semantics carry over
cleanly. The mapping below is the working translation table.

| C++ KPL (Java sidecar) | aiokpl |
|---|---|
| `KinesisProducer kpl = new KinesisProducer(cfg);` | `producer = Producer(cfg)` inside `async with` |
| `kpl.addUserRecord(stream, pk, data)` | `await producer.put_record(stream=, partition_key=, data=)` |
| `kpl.addUserRecord(stream, pk, ehk, data)` | `await producer.put_record(stream=, partition_key=, data=, explicit_hash_key=)` |
| `ListenableFuture<UserRecordResult>` | `Outcome[RecordResult]` (`await outcome.wait()`) |
| `UserRecordResult.getAttempts()` | `result.attempts` (tuple of `Attempt`) |
| `Attempt.getDelay()` / `getDuration()` | `attempt.ended_at - attempt.started_at` |
| `UserRecordResult.getShardId()` | `result.shard_id` |
| `UserRecordResult.getSequenceNumber()` | `result.sequence_number` |
| `KinesisProducerConfiguration` (builder) | [`Config`][aiokpl.Config] (frozen dataclass) |
| `setRegion("us-east-1")` | `Config(region="us-east-1")` |
| `setAggregationEnabled(true)` | `Config(aggregation_enabled=True)` |
| `setRecordMaxBufferedTime(100)` | `Config(record_max_buffered_time_ms=100)` |
| `setRecordTtl(30_000)` | `Config(record_ttl_ms=30_000)` |
| `setFailIfThrottled(false)` | `Config(fail_if_throttled=False)` |
| `setMaxConnections(24)` | not exposed — aiobotocore pool sizing is its own concern |
| `setRateLimit(...)` | `Config(rate_limit_records_per_sec_per_shard=…, rate_limit_bytes_per_sec_per_shard=…)` |
| `setMetricsLevel("summary")` | `Config(metrics_level=MetricsLevel.SUMMARY)` |
| `setMetricsNamespace("...")` | `CloudWatchSink(namespace="...")` + `Config(metrics_sink=…)` |
| `setMetricsGranularity("shard")` | `MetricsLevel.DETAILED` (per-shard dimensions emitted) |
| `kpl.flush()` / `kpl.flushSync()` | `await producer.flush()` |
| `kpl.destroy()` | `async with` exit — `__aexit__` drains and tears down |
| Per-stream `Pipeline` (visible in C++) | `_StreamPipeline` (private — created lazily per stream) |
| Sidecar process + named pipe | gone — everything is in-process |
| `kinesis_producer` binary + bootstrap.sh | gone — pure Python install |

A few notes the table doesn't capture:

- **There is no `Pipeline` object in the public API.** The C++ KPL exposes
  per-stream pipelines because users sometimes wanted per-stream metrics
  or per-stream lifecycle. In `aiokpl` a single `Producer` instance handles
  every stream you give it; per-stream state is internal and created on the
  first `put_record(stream=…)` call. Metrics are still dimensioned by
  `stream` so dashboards stay equivalent.
- **The CloudWatch knobs moved to the sink.** In the Java config you set
  the metrics namespace, credentials, and granularity on
  `KinesisProducerConfiguration`. In `aiokpl` those live on
  [`CloudWatchSink`][aiokpl.CloudWatchSink] (or whichever sink you use) —
  the core knows nothing about CloudWatch. This is the entire point of the
  pluggable sink design; see [Custom sinks](dev/sinks.md).
- **Lifecycle is an `async with`.** The Java API made you remember to call
  `destroy()`. `aiokpl` makes the language remember for you. If you really
  need manual lifecycle, drive `__aenter__` and `__aexit__` yourself, but
  the `async with` form is the supported path.
- **Attempts are returned in full, every time.** The Java
  `UserRecordResult.getAttempts()` is the same shape — `aiokpl` matches it.
  Every retry, including transient classifications and wrong-shard
  invalidations, shows up in `result.attempts`.
