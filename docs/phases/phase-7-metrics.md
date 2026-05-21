# Phase 7 — Vendor-neutral metrics

aiokpl emits **semantic events**. The destination — CloudWatch, OpenTelemetry,
Datadog, or none — is a pluggable sink. The library itself knows nothing
about any vendor: it builds rolling-window aggregates and hands them to the
sink you plug in.

## Design

```
stages.put("UserRecordsPut", 1.0, stream=...)
        │
        ▼
MetricsManager  ── rolling 60 s windows per (name, dims)
        │
        ▼  every upload_interval_ms
   sink.export([MetricSnapshot, ...])
        │
        ▼
   CloudWatch / OTLP / Prometheus / Datadog / your own backend
```

A sink is anything that satisfies the
[`MetricsSink`][aiokpl.sinks.MetricsSink] Protocol:

```python
from aiokpl.sinks import MetricSnapshot, MetricsSink

class MyBackend:
    async def export(self, snapshots):
        for s in snapshots:
            ...

    async def __aenter__(self): return self
    async def __aexit__(self, *exc): ...
```

If your sink wants per-event resolution (not just aggregated snapshots),
implement [`EventfulMetricsSink`][aiokpl.sinks.EventfulMetricsSink] too:
`MetricsManager` will call `sink.record(event)` synchronously inside
`MetricsManager.put`. Note that `record` must be sync — async work belongs
in `export`.

## Toggle

```python
from aiokpl import Config, MetricsLevel, Producer
from aiokpl.sinks import InMemorySink

cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.DETAILED,
    metrics_sink=InMemorySink(),         # or any MetricsSink
    metrics_upload_interval_ms=60_000,
)
async with Producer(cfg) as producer:
    ...
```

`metrics_sink=None` (the default) uses [`NullSink`][aiokpl.sinks.NullSink]:
zero overhead, no network, no allocations.

## Levels

| Level | Dimensions kept | Use case |
|---|---|---|
| `NONE` | n/a (no-op) | Default. Zero overhead. |
| `SUMMARY` | `stream` | Coarse dashboards, low cardinality. |
| `DETAILED` | `stream`, `shard`, `error_code` | Per-shard alerting. |

## First-party sinks

=== "Off (default)"

    ```python
    cfg = Config(region="us-east-1")   # metrics_sink defaults to None → NullSink
    ```

=== "CloudWatch"

    ```python
    from aiokpl.sinks import CloudWatchSink

    cfg = Config(
        region="us-east-1",
        metrics_level=MetricsLevel.DETAILED,
        metrics_sink=CloudWatchSink(
            region="us-east-1",
            namespace="my-app/aiokpl",
        ),
    )
    ```

    Bundled because `aiobotocore` is already a runtime dep (Kinesis client).
    Dimension translation: `stream` → `StreamName`, `shard` → `ShardId`,
    `error_code` → `ErrorCode`. Snapshots upload as `StatisticValues`
    payloads, chunked at the 1000-entry CloudWatch limit.

=== "OpenTelemetry"

    Install: `pip install 'aiokpl[otel]'`

    ```python
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    otel_metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

    from aiokpl.sinks.opentelemetry import OpenTelemetrySink

    cfg = Config(
        region="us-east-1",
        metrics_level=MetricsLevel.DETAILED,
        metrics_sink=OpenTelemetrySink(instrument_prefix="aiokpl."),
    )
    ```

    Recommended path for new deployments — it brings the full exporter
    ecosystem (Prometheus, OTLP collector, Honeycomb, Grafana, …).

=== "Datadog"

    Install: `pip install 'aiokpl[datadog]'`. Set `DD_API_KEY` /
    `DD_APP_KEY` in the environment, or pass them explicitly.

    ```python
    from aiokpl.sinks.datadog import DatadogSink

    cfg = Config(
        region="us-east-1",
        metrics_level=MetricsLevel.DETAILED,
        metrics_sink=DatadogSink(site="datadoghq.com", metric_prefix="aiokpl."),
    )
    ```

    Counts go to the `count` API, distributions to
    `submit_distribution_points`, gauges to `gauge`.

## Metric names → instrument types

Names match the C++ KPL constants verbatim
(`aws/metrics/metrics_constants.h`) so existing dashboards keep working.

| Name | OTel instrument | Datadog type | Where it fires |
|---|---|---|---|
| `UserRecordsReceived` | Counter | count | `Aggregator.build_buffered` |
| `UserRecordsPut` | Counter | count | `Retrier._finish_success` |
| `UserRecordsDataPut` | Counter | count | per-success, value = `len(data)` |
| `UserRecordsPending` | UpDownCounter | gauge | sampled every 5 s by the Producer |
| `KinesisRecordsPut` | Counter | count | per AggregatedRecord success |
| `KinesisRecordsDataPut` | Counter | count | per-success, value = `batch.size` |
| `AllErrors` | Counter | count | any retrier error classification |
| `ErrorsByCode` | Counter | count | dimensioned by `error_code` |
| `RetriesPerRecord` | Histogram | distribution | `len(attempts) - 1` |
| `BufferedTime` | Histogram | distribution | Limiter admits |
| `RequestTime` | Histogram | distribution | Sender's PutRecords latency |
| `ExpiredRecords` | Counter | count | Limiter expiry |

## In-process inspection

```python
snap = producer.metrics.snapshot()       # dict[MetricKey, (count, sum, min, max)]
snaps = producer.metrics.snapshots()     # tuple[MetricSnapshot, ...]
await producer.metrics.flush()           # force one export through the sink
```

These accessors stay available regardless of which sink is plugged in;
sinks that want to expose their own state do so on their own surface (e.g.
[`InMemorySink.by_name`][aiokpl.sinks.InMemorySink.by_name]).

## Constraints

* **CloudWatch upload is asyncio-only** — `aiobotocore` is asyncio-only.
  OTel + Datadog + Null + InMemory sinks are backend-agnostic.
* **No vendor strings in core.** `aiokpl/metrics.py` and `Config` do not
  mention CloudWatch, Datadog, or OpenTelemetry; provider knobs live on
  the sink constructor.

See also: [Writing a custom MetricsSink](../dev/sinks.md).
