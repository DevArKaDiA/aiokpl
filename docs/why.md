# Why aiokpl

AWS ships the official Kinesis Producer Library (KPL) as a native C++
binary wrapped in Java/.NET sidecars. The Python ecosystem has never had
an equivalent — until now.

`aiokpl` is a clean-room reimplementation of the KPL in idiomatic async
Python: shard-aware pipeline, deadline-driven batching, smart retry
classification, byte-exact aggregation. Built on `anyio`, so the same
code runs on both the `asyncio` and `trio` runtimes. Pure Python — no
binary, no subprocess, no IPC, no packaging hell.

## What exists in Python today

Three building blocks, none of which is a producer on its own:

- **[`aws-kinesis-agg`](https://github.com/awslabs/kinesis-aggregation)** —
  a codec for the KPL aggregation wire format. Encodes one blob from a
  list of records you already collected. Doesn't predict shards, doesn't
  batch, doesn't retry, doesn't rate-limit.
- **`boto3.put_records`** / **`aiobotocore.put_records`** — a single API
  call. Sends up to 500 records or 5 MiB in one round-trip. No pipeline,
  no aggregation, no shard-aware grouping, no attempt history.
- **`kiner`, `kinesis-python`, `kinesis-producer` (ludia)** — community
  batchers over `boto3`. All abandoned. None predict the destination
  shard, none rate-limit per shard, none classify retries.

A real producer has to compose these — and the composition is the hard
part. That composition is what `aiokpl` ships.

## Comparison

| Capability | aiokpl | `aws-kinesis-agg` + `boto3` | raw `boto3` | `kiner` (Buffer) |
|---|:---:|:---:|:---:|:---:|
| Byte-exact KPL aggregation | ✅ | ✅ | ❌ | ❌ |
| Shard prediction (no per-record RPC) | ✅ | ❌ | ❌ | ❌ |
| Per-shard rate limiting (1000 rec/s + 1 MiB/s) | ✅ | ❌ | ❌ | ❌ |
| Deadline-driven batching | ✅ (two-level) | ❌ | ❌ | size-only |
| Smart retry classification (split-aware) | ✅ | ❌ | ❌ | ❌ |
| Per-record attempt history surfaced to the caller | ✅ | ❌ | ❌ | ❌ |
| Backpressure (bounded `max_outstanding_records`) | ✅ | ❌ | ❌ | ❌ |
| Vendor-neutral metrics (CW / OTel / Datadog pluggable) | ✅ | ❌ | ❌ | ❌ |
| Sync bridge for non-async callers | ✅ (`SyncProducer`) | n/a | n/a | n/a |
| asyncio + trio backends | ✅ (via `anyio`) | ❌ | ❌ | ❌ |
| Zero native binary | ✅ | ✅ | ✅ | ✅ |
| Maintained in 2026 | ✅ | ✅ | ✅ | ❌ |

Each existing option is a reasonable tool for the slice it covers. None
is a drop-in replacement for the C++ KPL. `aiokpl` is.

!!! note "What about LocalStack or `kinesis-mock`?"
    Those are **Kinesis emulators**, not producer libraries — they
    replace the AWS service for local testing. `aiokpl` runs against them
    just as it runs against real Kinesis (and our integration suite
    proves byte-exact compatibility — see [Testing](dev/testing.md)).
    They're not alternatives, they're infrastructure.

## When NOT to use aiokpl

`aiokpl` is the right tool when you'd otherwise reach for the C++ KPL.
It's the wrong tool in several cases — listed here honestly so you can
skip it without regret.

- **You send fewer than ~100 records/second total.** A plain
  `boto3.put_records` call (or `aiobotocore.put_records`) is enough.
  Aggregation, prediction, and per-shard rate limiting don't pay for
  their configuration cost at that volume.
- **Your records are already larger than ~100 KB each.** Aggregation
  doesn't help — Kinesis already lets you pack up to 1 MB per record,
  and the KPL aggregation envelope is overhead on top. Just batch with
  `put_records` and keep the per-record partition keys.
- **You cannot run an async event loop.** `SyncProducer` exists for
  exactly this, but it costs a background thread and a portal per
  producer instance. If you're already running threaded code at high
  fan-out, `aws-kinesis-agg` + `boto3` with your own thread pool is a
  simpler dependency.
- **You need batch writes to a non-Kinesis target.** DynamoDB
  `BatchWriteItem`, SQS `SendMessageBatch`, S3 multipart uploads —
  different APIs, different optimization targets. `aiokpl` is
  Kinesis-only.
- **You're using Kinesis Firehose.** Firehose is a different service
  with a different API (`PutRecordBatch`) and different optimization
  rules — the shard model doesn't apply, the aggregation format isn't
  used. Firehose has its own producer libraries.
- **You need on-disk durability before sending.** `aiokpl` buffers in
  memory. If a record's outcome needs to survive a process crash, write
  to a local queue (RocksDB, SQLite, Kafka, …) and have a consumer of
  *that* drive `aiokpl`.

## Migrating from the KPL Java sidecar

A natural audience: you already run the C++ KPL via the Java sidecar (or
.NET wrapper) and would like to drop the binary. The semantics carry
over cleanly.

| C++ KPL (Java sidecar) | aiokpl |
|---|---|
| `KinesisProducer kpl = new KinesisProducer(cfg);` | `producer = Producer(cfg)` inside `async with` |
| `kpl.addUserRecord(stream, pk, data)` | `await producer.put_record(stream=, partition_key=, data=)` |
| `kpl.addUserRecord(stream, pk, ehk, data)` | `await producer.put_record(..., explicit_hash_key=)` |
| `ListenableFuture<UserRecordResult>` | `Outcome[RecordResult]` (`await outcome.wait()`) |
| `UserRecordResult.getAttempts()` | `result.attempts` (tuple of `Attempt`) |
| `Attempt.getDelay()` / `getDuration()` | `attempt.ended_at - attempt.started_at` |
| `UserRecordResult.getShardId()` | `result.shard_id` |
| `UserRecordResult.getSequenceNumber()` | `result.sequence_number` |
| `KinesisProducerConfiguration` (builder) | `Config` (frozen dataclass) |
| `setRegion("us-east-1")` | `Config(region="us-east-1")` |
| `setAggregationEnabled(true)` | `Config(aggregation_enabled=True)` |
| `setRecordMaxBufferedTime(100)` | `Config(record_max_buffered_time_ms=100)` |
| `setRecordTtl(30_000)` | `Config(record_ttl_ms=30_000)` |
| `setFailIfThrottled(false)` | `Config(fail_if_throttled=False)` |
| `setRateLimit(...)` | `Config(rate_limit_records_per_sec_per_shard=…, rate_limit_bytes_per_sec_per_shard=…)` |
| `setMetricsLevel("summary")` | `Config(metrics_level=MetricsLevel.SUMMARY)` |
| `setMetricsNamespace("...")` | `CloudWatchSink(namespace="...")` + `Config(metrics_sink=…)` |
| `setMetricsGranularity("shard")` | `MetricsLevel.DETAILED` |
| `kpl.flush()` / `kpl.flushSync()` | `await producer.flush()` |
| `kpl.destroy()` | `async with` exit — `__aexit__` drains and tears down |
| Sidecar process + named pipe | **gone** — everything is in-process |
| `kinesis_producer` binary + `bootstrap.sh` | **gone** — pure-Python install |

A few details the table doesn't capture:

- **There is no `Pipeline` object in the public API.** The C++ KPL
  exposes per-stream pipelines because users sometimes wanted per-stream
  metrics or per-stream lifecycle. In `aiokpl` a single `Producer`
  instance handles every stream you give it; per-stream state is
  internal and created on the first `put_record(stream=…)` call.
  Metrics are still dimensioned by `stream` so dashboards stay
  equivalent.
- **CloudWatch knobs moved to the sink.** In the Java config you set the
  metrics namespace, credentials, and granularity on
  `KinesisProducerConfiguration`. In `aiokpl` those live on the
  `CloudWatchSink` (or whichever sink you use) — the core knows nothing
  about CloudWatch. See [Custom sinks](dev/sinks.md).
- **Lifecycle is an `async with`.** The Java API made you remember to
  call `destroy()`. `aiokpl` makes the language remember for you.
- **Attempts are returned in full, every time.** Every retry — transient
  classifications, wrong-shard invalidations, throttle backoff — shows
  up in `result.attempts`.

## Why now, why async

The C++ KPL exists because in 2015 Python did not have `asyncio`, AWS
SDKs did not have async clients, and writing a shard-aware concurrent
producer in CPython was painful. None of that is still true.
`aiobotocore` gives us non-blocking AWS calls, `anyio` gives us cheap
concurrency that runs on both `asyncio` and `trio`, and modern Python
gives us the type system and dataclasses we need to express the pipeline
cleanly.

The C++ KPL is roughly a 30k-line C++ program plus a Java sidecar that
spawns it as a subprocess and talks to it over a named pipe. `aiokpl` is
~2k lines of pure Python with no native dependencies. The semantics are
the same — the engineering footprint is two orders of magnitude
smaller.
