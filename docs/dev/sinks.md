# Writing a custom `MetricsSink`

aiokpl emits semantic events; the destination is yours. To plug a new
backend in, satisfy the [`MetricsSink`][aiokpl.sinks.MetricsSink] Protocol.

## Minimal sink

```python
from collections.abc import Sequence
from aiokpl.sinks import MetricSnapshot

class StdoutSink:
    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        for s in snapshots:
            print(f"{s.name} count={s.count} sum={s.sum} dims={s.dimensions}")

    async def __aenter__(self) -> "StdoutSink":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None
```

Plug it into the Producer:

```python
from aiokpl import Config, MetricsLevel, Producer

cfg = Config(
    region="us-east-1",
    metrics_level=MetricsLevel.DETAILED,
    metrics_sink=StdoutSink(),
    metrics_upload_interval_ms=10_000,
)
async with Producer(cfg) as producer:
    ...
```

## Per-event resolution

If you want every observation (not just aggregated snapshots), implement
[`EventfulMetricsSink`][aiokpl.sinks.EventfulMetricsSink] too — add a
synchronous `record(event)` method:

```python
from aiokpl.sinks import MetricEvent

class EventfulStdoutSink(StdoutSink):
    def record(self, event: MetricEvent) -> None:
        print(f"event {event.name}={event.value} dims={event.dimensions}")
```

`MetricsManager` checks `isinstance(sink, EventfulMetricsSink)` on every
`put` and calls `record(event)` when the check passes. `record` is sync on
purpose: it runs inside the hot path. Async I/O belongs in `export`.

## Lifecycle

`MetricsManager.__aenter__` enters the sink and starts the upload loop.
`__aexit__` runs a final `flush()` before tearing the sink down, so no
window is dropped. Sinks should release their transports in `__aexit__`.

## Dimensions

`MetricSnapshot.dimensions` is a `tuple[tuple[str, str], ...]`. aiokpl uses
the keys `"stream"`, `"shard"`, `"error_code"`. Translate to whatever your
backend expects (tags, attributes, dimensions, labels). The CloudWatch sink
uses a small lookup table; OpenTelemetry passes dimensions through as
attributes; Datadog renders them as `key:value` tags. Unknown keys are
forwarded verbatim.
