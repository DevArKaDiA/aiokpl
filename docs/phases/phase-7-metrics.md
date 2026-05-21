# Phase 7 — CloudWatch metrics

In-process metrics with optional periodic CloudWatch upload. Off by default,
zero overhead when off. Mirrors the C++ KPL's `aws/metrics/metrics_manager.*`
without the IPC layer.

## Toggle

Metrics are opt-in via `Config`:

```python
from aiokpl import Config, MetricsLevel, Producer

cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.DETAILED,
    metrics_namespace="my-app/aiokpl",
    metrics_upload_interval_ms=60_000,
    # metrics_cloudwatch_enabled=False to keep in-process counters only.
)
async with Producer(cfg) as producer:
    ...
    snap = producer.metrics.snapshot()
```

`MetricsLevel.NONE` is the default. At `NONE`, `MetricsManager.put()` is a
no-op, no CloudWatch client is created, and no upload task is spawned.

## Levels

| Level | Dimensions kept | Use case |
|---|---|---|
| `NONE` | n/a (no-op) | Default. Zero overhead. |
| `SUMMARY` | `stream` | Coarse dashboards, low cardinality. |
| `DETAILED` | `stream`, `shard_id`, `error_code` | Per-shard alerting, per-code error trends. |

## Metric names

Names match the C++ KPL constants verbatim (see
`aws/metrics/metrics_constants.h`) so operators can reuse existing
dashboards.

| Name | Unit | Where it fires |
|---|---|---|
| `UserRecordsReceived` | count | One per `put_record` (`Aggregator.build_buffered`). |
| `UserRecordsPut` | count | One per terminal success (`Retrier._finish_success`). |
| `UserRecordsDataPut` | bytes | Per-success, value = `len(data)`. |
| `UserRecordsPending` | gauge | Sampled every 5 s by the Producer. |
| `KinesisRecordsPut` | count | One per successfully-routed AggregatedRecord. |
| `KinesisRecordsDataPut` | bytes | Per-success, value = `batch.size`. |
| `AllErrors` | count | Any retrier classification that records an error. |
| `ErrorsByCode` | count | Same as above, dimensioned by `error_code`. |
| `RetriesPerRecord` | distribution | `len(attempts) - 1` at terminal success. |
| `BufferedTime` | ms | Limiter admits → time since the earliest item arrived. |
| `RequestTime` | ms | Sender's PutRecords latency. |
| `ExpiredRecords` | count | Limiter expiry path. |

## CloudWatch payload format

Each metric uploads as one `MetricDatum` with:

- `MetricName`
- `Dimensions` — populated from the live `MetricKey` (`StreamName`, `ShardId`,
  `ErrorCode`, in that order, only the ones that are set).
- `StatisticValues` — `SampleCount`, `Sum`, `Minimum`, `Maximum` over the
  rolling 60 s window.

Chunking: CloudWatch caps `PutMetricData` at 1000 entries per call. When a
snapshot has more, `MetricsManager` splits into multiple sequential calls.

## Internals

- `_Accumulator` is a 60 s rolling window of integer-second buckets, each a
  `(count, sum, min, max)` quad. Buckets older than the window are dropped
  lazily on the next `put` or `stats`.
- `MetricsManager.__aenter__` spawns the upload loop only when
  `level != NONE`. The loop awaits `anyio.sleep(upload_interval_ms / 1000)`
  and posts a snapshot per tick. `__aexit__` drains one final upload before
  cancelling the task.
- The Producer threads the same `MetricsManager` into every stage
  (`Aggregator`, `Limiter`, `Sender`, `Retrier`). The stages call
  `metrics.put(...)` unconditionally; the manager itself short-circuits at
  `NONE`. That keeps the toggle centralised.

## Constraints

- **CloudWatch upload is asyncio-only.** `aiobotocore` is asyncio-only, so
  uploading inherits that constraint — same as the Sender (see
  `CLAUDE.md`'s "Concurrency model"). The lower stages' calls to
  `metrics.put()` stay backend-agnostic because the manager itself uses
  `anyio.sleep` and `anyio.CancelScope`.
- **No new runtime deps.** `aiobotocore` was already a Phase 5 dep for the
  Kinesis client; the CloudWatch client reuses it.

See also: [CloudWatch PutMetricData][cw-docs].

[cw-docs]: https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html
