"""Vendor-neutral metrics sinks for aiokpl.

The library emits semantic events; the sink decides where they go. Sinks are
plugged into :class:`aiokpl.metrics.MetricsManager` via the
:class:`MetricsSink` Protocol.

Hot-path code only knows about :class:`MetricEvent` and
:class:`MetricSnapshot`. First-party sinks (Null, InMemory, CloudWatch) ship
in this package; OpenTelemetry and Datadog implementations are gated behind
``aiokpl[otel]`` / ``aiokpl[datadog]`` extras and live in their own modules
(``aiokpl.sinks.opentelemetry``, ``aiokpl.sinks.datadog``).
"""

from __future__ import annotations

from aiokpl.sinks._types import (
    EventfulMetricsSink,
    MetricEvent,
    MetricSnapshot,
    MetricsSink,
)
from aiokpl.sinks.cloudwatch import CloudWatchSink
from aiokpl.sinks.memory import InMemorySink
from aiokpl.sinks.null import NullSink

__all__ = [
    "CloudWatchSink",
    "EventfulMetricsSink",
    "InMemorySink",
    "MetricEvent",
    "MetricSnapshot",
    "MetricsSink",
    "NullSink",
]
