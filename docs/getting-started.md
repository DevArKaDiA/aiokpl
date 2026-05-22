# Getting started

Everything you need to put records into Kinesis with `aiokpl` — installation,
first program, the configuration knobs, the per-record outcome, metrics
wiring, and the troubleshooting tree for the questions that actually come up.

## Installation

`aiokpl` requires **Python 3.10+** and a recent `pip`.

```bash
# Base install — async Producer, SyncProducer, CloudWatchSink, InMemorySink.
pip install aiokpl

# With the OpenTelemetry sink.
pip install "aiokpl[otel]"

# With the Datadog sink.
pip install "aiokpl[datadog]"

# Everything (tests, lint, docs, both optional sinks). For contributors.
pip install "aiokpl[dev]"
```

The extras only gate **optional metrics sinks**; the producer itself is fully
functional with the base install. Each extra in one sentence:

- `otel` — pulls `opentelemetry-api` and `opentelemetry-sdk`, unlocks
  `aiokpl.sinks.opentelemetry.OpenTelemetrySink`.
- `datadog` — pulls `datadog-api-client`, unlocks
  `aiokpl.sinks.datadog.DatadogSink`.
- `dev` — everything contributors need: test runner, lint, type checker, docs
  toolchain, integration deps, both optional sinks.

!!! note "`aiobotocore` is asyncio-only"
    The network edge (`Producer`, `Sender`, `Retrier`, `CloudWatchSink`) runs
    on `asyncio` because `aiobotocore` is asyncio-only. The codec, ShardMap,
    Reducer, Aggregator, Collector, Limiter, and TokenBucket are
    backend-agnostic and the test suite exercises them on both `asyncio` and
    `trio`.

## Quick start (async)

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

`put_record()` returns immediately with an [`Outcome`][aiokpl.Outcome]; the
record itself is queued, aggregated with siblings going to the same shard,
rate-limited, and sent in the background. `outcome.wait()` blocks until the
record reaches a terminal state (success or final failure) and carries the
full attempt history.

## Quick start (synchronous)

For Flask/Django request handlers, Celery tasks, plain scripts, or notebooks
that don't run an event loop, use [`SyncProducer`][aiokpl.SyncProducer]. Same
shape as the async API; internally it owns an `anyio` `BlockingPortal` running
the async `Producer` on a single background thread.

```python
from aiokpl import Config, SyncProducer

cfg = Config(region="us-east-1")

with SyncProducer(cfg) as producer:
    outcome = producer.put_record(
        stream="my-stream",
        partition_key="user-123",
        data=b"hello",
    )
    try:
        result = outcome.wait(timeout=5.0)
    except TimeoutError:
        # The record didn't reach a terminal state within 5 seconds.
        # Possible causes: backpressure, network stall, very large
        # record_ttl_ms. Outcome is still tracked — try again or cancel.
        outcome.cancel()
        raise
    if result.success:
        print(result.shard_id, result.sequence_number)
```

`put_record` is thread-safe — you can call it from any OS thread.
`outcome.wait(timeout=)` and `producer.flush(timeout=)` both raise
`TimeoutError` if the deadline elapses; the in-flight Kinesis request is
**not** stopped, only the local handle is released.

## Configuration

Every tunable lives on [`Config`][aiokpl.Config]. It's a frozen dataclass —
construct once, hand to the producer, never mutate.

| Field | Default | One-line meaning |
|---|---|---|
| `region` | *(required)* | AWS region for the Kinesis client. |
| `endpoint_url` | `None` | Override the Kinesis endpoint (LocalStack, `kinesis-mock`, VPC endpoint). |
| `verify_ssl` | `True` | Set `False` for self-signed test certs. |
| `aws_access_key_id` | `None` | Explicit credential; otherwise the default chain runs. |
| `aws_secret_access_key` | `None` | Pair to the above. |
| `aws_session_token` | `None` | Session credentials (STS, SSO). |
| `aggregation_enabled` | `True` | Pack many user records into one Kinesis record (KPL wire format). |
| `aggregation_max_count` | `4_294_967_295` | Hard cap on user records per aggregated record. |
| `aggregation_max_size` | `51_200` | Bytes; the aggregated record stops growing past this size. |
| `record_max_buffered_time_ms` | `100.0` | Soft deadline an aggregator/collector batch waits for siblings. |
| `record_ttl_ms` | `30_000.0` | Hard expiration; once exceeded the record fails with `Expired`. |
| `collection_max_count` | `500` | Kinesis hard limit: 500 records per `PutRecords`. |
| `collection_max_size` | `5_242_880` | Kinesis hard limit: 5 MiB per `PutRecords`. |
| `rate_limit_records_per_sec_per_shard` | `1_000.0` | Kinesis hard limit, per shard. |
| `rate_limit_bytes_per_sec_per_shard` | `1_048_576.0` | 1 MiB/s, per shard. |
| `drain_interval_ms` | `25.0` | How often the limiter polls its per-shard queues. |
| `fail_if_throttled` | `False` | If `True`, throttled records fail immediately instead of retrying. |
| `retry_deadline_ms` | `50.0` | Bumps a record's batching deadline this much on retry. |
| `max_outstanding_records` | `100_000` | Backpressure cap; `put_record` blocks when saturated. |
| `metrics_level` | `MetricsLevel.NONE` | `NONE` / `SUMMARY` / `DETAILED`. |
| `metrics_sink` | `None` | Any `MetricsSink`; defaults to `NullSink`. |
| `metrics_upload_interval_ms` | `60_000.0` | How often the `MetricsManager` flushes to the sink. |

**AWS endpoints.** Leave `endpoint_url` / credentials at their defaults in
production — `aiobotocore` resolves credentials via the standard chain
(env vars, shared config, instance metadata). The overrides exist for tests
against `kinesis-mock` or LocalStack, and for VPC-private endpoints.

**Aggregation.** Defaults match the C++ KPL: aggregation on, capped at 50 KiB
per aggregated record. The high `aggregation_max_count` ceiling is intentional
— size and time deadlines are the actual brakes. Disable aggregation
(`aggregation_enabled=False`) only if your individual records are already
large (> ~100 KiB) or your consumer can't deaggregate.

**Batching deadlines.** `record_max_buffered_time_ms = 100` is the C++ KPL
default and a good sweet spot: long enough to amortize a `PutRecords` round
trip over many records, short enough that latency-sensitive callers don't
notice. `record_ttl_ms = 30_000` matches the C++ KPL — records that age past
that fail with `Expired` rather than blocking indefinitely.

**Collector.** `collection_max_count` and `collection_max_size` are Kinesis
hard limits, not preferences. They exist on `Config` so tests can shrink
them, not so production callers can change them.

**Per-shard rate limiting.** `1000 records/s + 1 MiB/s` per shard are the
Kinesis hard caps. The limiter throttles *predicted* shards, not the stream
as a whole — that's the entire point of the shard-aware design. Lower the
records-per-second knob if you're sharing a shard with other producers and
want headroom.

**Retrier policy.** `fail_if_throttled=False` matches the C++ KPL: throttles
are treated as transient. Flip to `True` only if you'd rather see the failure
than spend the record's TTL retrying.

**Backpressure.** `max_outstanding_records=100_000` is a soft memory cap.
`put_record` awaits an internal semaphore released by the retrier's terminal
callback; lower this if you're embedded in a process with tight memory.

**Metrics.** Default is `NONE` + `NullSink` — zero overhead, zero allocations.
Enable by setting both `metrics_level` *and* `metrics_sink`. See
[Metrics](#metrics-pluggable-sinks) below.

## Working with the awaitable outcome

[`put_record`][aiokpl.Producer.put_record] returns an
[`Outcome[RecordResult]`][aiokpl.Outcome] — a backend-agnostic one-shot value.
`Outcome.wait()` blocks until the pipeline classifies the record as terminal
(success or final failure).

The terminal value is a [`RecordResult`][aiokpl.RecordResult]:

```python
@dataclass(slots=True, frozen=True)
class RecordResult:
    success: bool
    shard_id: str | None
    sequence_number: str | None
    attempts: tuple[Attempt, ...]
```

Each [`Attempt`][aiokpl.Attempt] carries `started_at` / `ended_at`, `success`,
and either `(shard_id, sequence_number)` on success or
`(error_code, error_message)` on failure.

### Happy path

```python
outcome = await producer.put_record(
    stream="events", partition_key="u-1", data=b"payload"
)
result = await outcome.wait()
assert result.success
print(result.shard_id, result.sequence_number)
```

### Failure path

```python
result = await outcome.wait()
if not result.success:
    last = result.attempts[-1]
    print(f"final failure: {last.error_code} — {last.error_message}")
    print(f"retried {len(result.attempts)} time(s)")
    for i, a in enumerate(result.attempts):
        kind = "OK" if a.success else f"{a.error_code}"
        print(f"  attempt {i}: {kind} ({a.ended_at - a.started_at:.3f}s)")
```

Common terminal `error_code` values: `ProvisionedThroughputExceededException`
(after final retry, when `fail_if_throttled=True`), `Expired` (TTL elapsed
in the limiter or aggregator), `Wrong Shard` (split-driven; the record was
re-enqueued and a later attempt won), `Record Count Mismatch` (Kinesis
returned a per-record list of the wrong length — the whole batch fails).

### Submitting many records concurrently

The producer is built for fan-in. Stuff records in from many tasks; the
aggregator groups them by predicted shard.

```python
import anyio

async def push(producer, i):
    outcome = await producer.put_record(
        stream="events",
        partition_key=f"user-{i}",
        data=f"event-{i}".encode(),
    )
    return await outcome.wait()

async with Producer(cfg) as producer:
    async with anyio.create_task_group() as tg:
        results: list = [None] * 10_000
        async def one(i):
            results[i] = await push(producer, i)
        for i in range(10_000):
            tg.start_soon(one, i)
    failed = sum(1 for r in results if not r.success)
    print(f"{10_000 - failed}/{10_000} succeeded")
```

Because `put_record` only blocks on the backpressure semaphore, fan-out
scales until you hit either `max_outstanding_records` or the per-shard rate
limit.

## Metrics: pluggable sinks

The library knows nothing about CloudWatch, Datadog, or OpenTelemetry. It
emits semantic events to a [`MetricsSink`][aiokpl.MetricsSink] you plug in.
Sink design rationale lives in [Custom sinks](dev/sinks.md).

Two knobs together turn metrics on:

```python
from aiokpl import Config, MetricsLevel
cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.SUMMARY,   # or DETAILED
    metrics_sink=...,                     # one of the sinks below
)
```

### `NullSink` (default)

Zero overhead, zero allocations. Used automatically when `metrics_sink=None`.

```python
from aiokpl import Config, NullSink
cfg = Config(region="us-east-1", metrics_sink=NullSink())  # equivalent to None
```

### `InMemorySink` (test + embed)

Captures every exported snapshot batch in process. Handy in tests and in
embedded scenarios where the host already publishes its own metrics.

```python
from aiokpl import Config, InMemorySink, MetricsLevel, Producer

sink = InMemorySink()
cfg = Config(region="us-east-1", metrics_level=MetricsLevel.SUMMARY,
             metrics_sink=sink)
async with Producer(cfg) as producer:
    ...  # exercise the producer
# Inspect after shutdown:
for snapshot in sink.by_name("UserRecordsPut"):
    print(snapshot.sum, snapshot.dimensions)
```

### `CloudWatchSink` (production AWS)

Bundled — no extra needed, since `aiobotocore` is already a runtime dep.

```python
from aiokpl import CloudWatchSink, Config, MetricsLevel, Producer

sink = CloudWatchSink(region="us-east-1", namespace="myapp/kinesis")
cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.SUMMARY,
    metrics_sink=sink,
    metrics_upload_interval_ms=60_000,
)
async with Producer(cfg) as producer:
    ...
```

Metric names match the C++ KPL constants verbatim
(`UserRecordsReceived`, `UserRecordsPut`, `KinesisRecordsPut`,
`BufferedTime`, `RequestTime`, `RetriesPerRecord`, `ErrorsByCode`,
`UserRecordsPending`, …) — existing C++ KPL dashboards keep working.

### `OpenTelemetrySink` (gated by `[otel]`)

```python
# pip install "aiokpl[otel]"
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from aiokpl import Config, MetricsLevel, OpenTelemetrySink, Producer

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://collector:4317", insecure=True),
)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

sink = OpenTelemetrySink(instrument_prefix="aiokpl.")
cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.SUMMARY,
    metrics_sink=sink,
)
async with Producer(cfg) as producer:
    ...
```

Counts become OTel `Counter`s, distributions become `Histogram`s, gauges
become `UpDownCounter`s. The sink only emits instruments — exporters,
batching, and resource attribution live on your OTel SDK configuration.

### `DatadogSink` (gated by `[datadog]`)

```python
# pip install "aiokpl[datadog]"
# Reads DD_API_KEY / DD_APP_KEY from the environment by default.
import os
from aiokpl import Config, DatadogSink, MetricsLevel, Producer

sink = DatadogSink(
    api_key=os.environ["DD_API_KEY"],
    site="datadoghq.com",
    metric_prefix="aiokpl.",
)
cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.SUMMARY,
    metrics_sink=sink,
)
async with Producer(cfg) as producer:
    ...
```

Counts map to Datadog `count`, distributions to Datadog `distribution`,
gauges to Datadog `gauge`.

## Multi-stream usage

One `Producer` handles every stream. Per-stream pipelines (ShardMap,
Aggregator, Limiter, Collector, Sender, Retrier) are created lazily the
first time you call `put_record(stream=...)` for a new stream name; they
share the same `aiobotocore` Kinesis client (one HTTP connection pool).

```python
async with Producer(cfg) as producer:
    await producer.put_record(stream="events",  partition_key="u1", data=b"...")
    await producer.put_record(stream="audit",   partition_key="u1", data=b"...")
    await producer.put_record(stream="metrics", partition_key="u1", data=b"...")
```

There's no `streams=` constructor argument and no need to declare them
upfront — the first record for a stream creates the pipeline, and the
producer's `__aexit__` tears every pipeline down.

## Graceful shutdown

`async with Producer(cfg) as producer:` is the only correct way to drive the
producer. The context manager guarantees:

1. On entry: `aiobotocore` session opens, the background task group starts,
   the metrics manager spins up its flush task.
2. On exit: `flush()` runs — every aggregator, limiter, and collector drains
   into the network — then the task group quiesces, the metrics manager
   uploads one last batch, and the `aiobotocore` client closes.

`flush()` is exposed for callers that want to drain *without* tearing the
producer down (typical in long-lived services at checkpoint time):

```python
async with Producer(cfg) as producer:
    for record in batch:
        await producer.put_record(stream="events",
                                  partition_key=record.key,
                                  data=record.payload)
    await producer.flush()
    # producer is still alive; you can keep going.
```

!!! warning "Don't forget to `await`"
    Forgetting to use the `async with` (or `with` for `SyncProducer`) leaks
    every per-stream pipeline's task group and the `aiobotocore` HTTP pool.
    `Producer.__init__` is cheap and pure — every side effect lives in
    `__aenter__`.

## Patterns

A short tasting menu of the levers you'll actually pull.

### Backpressure tuning

```python
# Tight memory? Drop the cap. put_record will block when saturated; the
# producer surfaces backpressure as latency, never as silent drops.
cfg = Config(region="us-east-1", max_outstanding_records=10_000)
```

You can inspect `producer.outstanding_records` at any time for a live count.

### Time-bounded delivery

```python
# 25 ms batching window (low-latency dashboards), but tolerate up to 5 s of
# retries before giving up.
cfg = Config(
    region="us-east-1",
    record_max_buffered_time_ms=25,
    record_ttl_ms=5_000,
)
```

Lowering `record_max_buffered_time_ms` shrinks the aggregator/collector
deadlines uniformly; lowering `record_ttl_ms` makes the retrier give up
sooner on stragglers.

### Throttle policy

```python
# Treat every ProvisionedThroughputExceededException as terminal.
cfg = Config(region="us-east-1", fail_if_throttled=True)
```

Useful for batch jobs that prefer failing fast (so an upstream queue can
back off) over filling the record's TTL with retries.

### Aggregation off

```python
# Records already > 100 KiB each — aggregation overhead beats its benefit.
cfg = Config(region="us-east-1", aggregation_enabled=False)
```

The Aggregator falls back to single-record mode: one user record per
Kinesis record, original partition key preserved. The Collector still
batches and the Limiter still rate-limits.

## Troubleshooting

### "I get `RuntimeError: Producer is closed`"

You called `put_record` after `__aexit__` ran (e.g. outside the `async with`
block). The producer is single-use — wrap your work in `async with`.

### "All my records are going to one shard"

Your partition keys lack entropy. `aiokpl` predicts the shard from
`md5(partition_key)` — if you pass the same key (or a small set of keys),
records concentrate on the same predicted shard and you hit the 1000 rec/s
/ 1 MiB/s limit. Fix: use a per-record key (user id, event id, hash of the
payload). If you must control routing yourself, pass `explicit_hash_key=` —
the producer uses that integer directly.

### "Throughput is lower than I expected"

Three usual suspects, in order:

1. **Aggregation is off** when it didn't have to be — flip
   `aggregation_enabled=True`. With small records (< 1 KiB), aggregation
   pushes per-`PutRecords` throughput up by 10–50×.
2. **Shard count is the cap.** Kinesis is 1000 rec/s + 1 MiB/s per shard.
   Predicted shards that route to the same physical shard share that
   budget. Add shards or improve key entropy.
3. **`max_outstanding_records` is too low.** If `put_record` calls are
   blocking on the semaphore, raise it.

### "Records are timing out"

Look at `result.attempts[-1].error_code == "Expired"`. The record aged past
`record_ttl_ms` before the Sender could push it. Either raise `record_ttl_ms`,
add shards, or reduce upstream rate.

### "Outcomes never resolve"

The most common cause: you exited the `async with` block without awaiting
the outcomes. Once `__aexit__` returns, the background task group is gone
and no further classification happens. Either `await outcome.wait()` *inside*
the block, or hold an `asyncio.gather`/`anyio.create_task_group` over all
outcomes before exiting.

### "OTel/Datadog sink import error"

`ImportError: OpenTelemetrySink requires …` or the Datadog equivalent. You
installed the base `aiokpl` without the extra. Fix:

```bash
pip install "aiokpl[otel]"     # or
pip install "aiokpl[datadog]"
```

### "`kinesis-mock` works in tests but real AWS doesn't"

Three things differ between the emulator and prod:

1. **Credentials.** Real AWS expects a valid principal; the default chain
   needs an env var, profile, IRSA role, or instance metadata. The test
   harness uses hard-coded fakes.
2. **Region.** `kinesis-mock` ignores it; real AWS does not. Match the
   region of your stream.
3. **`endpoint_url`.** Leave it `None` against real AWS; it only exists
   for emulators and VPC endpoints.

### "Memory grows over time"

A leak typically points at `max_outstanding_records`. If a record never
reaches a terminal state (network black hole, no `flush()`, very large
`record_ttl_ms`), its `Outcome` stays in the pending dict. Either lower
`record_ttl_ms` so stuck records expire, or lower
`max_outstanding_records` so the semaphore caps the queue length.
