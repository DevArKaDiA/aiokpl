""":class:`OpenTelemetrySink` — forwards metrics to a user-supplied OTel Meter.

Importing this module fails with a clear hint when the OpenTelemetry SDK is
not installed; the rest of aiokpl keeps working. The user is expected to
configure exporters (OTLP, Prometheus, console, …) on the OTel SDK side; we
only emit instruments.

Metric → instrument mapping is hard-coded based on what each KPL metric
represents:

* counts (UserRecordsReceived, UserRecordsPut, …) → ``Counter``
* distributions (BufferedTime, RequestTime, RetriesPerRecord) → ``Histogram``
* gauges (UserRecordsPending) → ``UpDownCounter``
"""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Any

try:
    from opentelemetry import metrics as _otel_metrics
except ImportError as exc:  # pragma: no cover - exercised in env without OTel
    raise ImportError(
        "OpenTelemetrySink requires the `opentelemetry-api` and "
        "`opentelemetry-sdk` packages. Install with: "
        "`pip install 'aiokpl[otel]'`"
    ) from exc

from aiokpl.sinks._types import MetricSnapshot

# Names taken from aws/metrics/metrics_constants.h. Categorising by the
# instrument that best fits the semantics of each metric.
_COUNTER_NAMES = frozenset(
    {
        "UserRecordsReceived",
        "UserRecordsPut",
        "UserRecordsDataPut",
        "KinesisRecordsPut",
        "KinesisRecordsDataPut",
        "AllErrors",
        "ErrorsByCode",
        "ExpiredRecords",
    }
)

_HISTOGRAM_NAMES = frozenset(
    {
        "BufferedTime",
        "RequestTime",
        "RetriesPerRecord",
    }
)

_UPDOWN_NAMES = frozenset(
    {
        "UserRecordsPending",
    }
)


class OpenTelemetrySink:
    """Bridges :class:`MetricSnapshot` exports onto OTel instruments."""

    __slots__ = (
        "_counters",
        "_histograms",
        "_instrument_prefix",
        "_last_count",
        "_meter",
        "_meter_arg",
        "_updowns",
    )

    def __init__(
        self,
        *,
        meter: Any = None,
        instrument_prefix: str = "aiokpl.",
    ) -> None:
        self._meter_arg = meter
        self._meter: Any = meter
        self._instrument_prefix = instrument_prefix
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._updowns: dict[str, Any] = {}
        # Counters in OTel only accept positive increments. Track the last
        # cumulative count per (name, attrs) so each export adds the delta.
        self._last_count: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    async def __aenter__(self) -> OpenTelemetrySink:
        if self._meter is None:
            self._meter = _otel_metrics.get_meter(self._instrument_prefix.rstrip("."))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        for s in snapshots:
            attrs = dict(s.dimensions)
            if s.name in _COUNTER_NAMES:
                self._record_counter(s, attrs)
            elif s.name in _HISTOGRAM_NAMES:
                self._record_histogram(s, attrs)
            elif s.name in _UPDOWN_NAMES:
                self._record_updown(s, attrs)
            else:
                # Unknown name → default to counter so users still see data.
                self._record_counter(s, attrs)

    # ─── Internals ────────────────────────────────────────────────────────

    def _instrument_name(self, name: str) -> str:
        return f"{self._instrument_prefix}{name}"

    def _record_counter(self, s: MetricSnapshot, attrs: dict[str, str]) -> None:
        counter = self._counters.get(s.name)
        if counter is None:
            counter = self._meter.create_counter(self._instrument_name(s.name))
            self._counters[s.name] = counter
        key = (s.name, s.dimensions)
        prev = self._last_count.get(key, 0.0)
        # Sum carries the cumulative running total of contributions for this
        # window; the delta against the previously-exported total is what
        # gets added to the OTel counter.
        delta = s.sum - prev
        if delta < 0.0:
            delta = s.sum
        self._last_count[key] = s.sum
        counter.add(delta, attributes=attrs)

    def _record_histogram(self, s: MetricSnapshot, attrs: dict[str, str]) -> None:
        histogram = self._histograms.get(s.name)
        if histogram is None:
            histogram = self._meter.create_histogram(self._instrument_name(s.name))
            self._histograms[s.name] = histogram
        avg = s.sum / s.count if s.count else 0.0
        histogram.record(avg, attributes=attrs)

    def _record_updown(self, s: MetricSnapshot, attrs: dict[str, str]) -> None:
        gauge = self._updowns.get(s.name)
        if gauge is None:
            gauge = self._meter.create_up_down_counter(self._instrument_name(s.name))
            self._updowns[s.name] = gauge
        # For gauge-like metrics, max is the most recent observation in the
        # window; treat that as the current value.
        gauge.add(s.max, attributes=attrs)


__all__ = ["OpenTelemetrySink"]
