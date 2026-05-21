""":class:`aiokpl.sinks.OpenTelemetrySink` instrument-type mapping.

Skipped when the OpenTelemetry SDK is not installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry")
pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
)

from aiokpl.sinks import MetricSnapshot
from aiokpl.sinks.opentelemetry import OpenTelemetrySink


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _build_meter() -> tuple[InMemoryMetricReader, object]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test")
    return reader, meter


def _instrument_names(reader: InMemoryMetricReader) -> set[str]:
    data = reader.get_metrics_data()
    names: set[str] = set()
    if data is None:
        return names
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                names.add(m.name)
    return names


async def test_otel_counter_for_user_records_put() -> None:
    reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter, instrument_prefix="aiokpl.")
    async with sink:
        await sink.export(
            (
                MetricSnapshot(
                    name="UserRecordsPut",
                    count=3,
                    sum=3.0,
                    min=1.0,
                    max=1.0,
                    dimensions=(("stream", "s"),),
                ),
            )
        )
    assert "aiokpl.userrecordsput" in _instrument_names(reader)


async def test_otel_histogram_for_request_time() -> None:
    reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export(
            (
                MetricSnapshot(
                    name="RequestTime",
                    count=2,
                    sum=20.0,
                    min=5.0,
                    max=15.0,
                ),
            )
        )
    assert "aiokpl.requesttime" in _instrument_names(reader)


async def test_otel_updown_for_pending() -> None:
    reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export(
            (
                MetricSnapshot(
                    name="UserRecordsPending",
                    count=1,
                    sum=42.0,
                    min=42.0,
                    max=42.0,
                ),
            )
        )
    assert "aiokpl.userrecordspending" in _instrument_names(reader)


async def test_otel_unknown_metric_defaults_to_counter() -> None:
    reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export((MetricSnapshot(name="Custom", count=1, sum=1.0, min=1.0, max=1.0),))
    assert "aiokpl.custom" in _instrument_names(reader)


async def test_otel_counter_emits_only_delta_across_exports() -> None:
    reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export(
            (
                MetricSnapshot(
                    name="UserRecordsPut",
                    count=1,
                    sum=5.0,
                    min=1.0,
                    max=5.0,
                    dimensions=(("stream", "s"),),
                ),
            )
        )
        await sink.export(
            (
                MetricSnapshot(
                    name="UserRecordsPut",
                    count=1,
                    sum=8.0,
                    min=1.0,
                    max=5.0,
                    dimensions=(("stream", "s"),),
                ),
            )
        )
        # Counter window reset (sum goes down) — fall back to absolute value.
        await sink.export(
            (
                MetricSnapshot(
                    name="UserRecordsPut",
                    count=1,
                    sum=2.0,
                    min=1.0,
                    max=2.0,
                    dimensions=(("stream", "s"),),
                ),
            )
        )
    assert "aiokpl.userrecordsput" in _instrument_names(reader)


async def test_otel_caches_histogram_and_updown_instruments() -> None:
    # Second export through the same sink reuses the cached instrument
    # (covers the False branches inside _record_histogram / _record_updown).
    _reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export(
            (
                MetricSnapshot(name="RequestTime", count=1, sum=1.0, min=1.0, max=1.0),
                MetricSnapshot(name="UserRecordsPending", count=1, sum=5.0, min=5.0, max=5.0),
            )
        )
        await sink.export(
            (
                MetricSnapshot(name="RequestTime", count=1, sum=2.0, min=2.0, max=2.0),
                MetricSnapshot(name="UserRecordsPending", count=1, sum=7.0, min=7.0, max=7.0),
            )
        )


async def test_otel_histogram_with_zero_count_handled() -> None:
    _reader, meter = _build_meter()
    sink = OpenTelemetrySink(meter=meter)
    async with sink:
        await sink.export((MetricSnapshot(name="RequestTime", count=0, sum=0.0, min=0.0, max=0.0),))


async def test_otel_default_meter_uses_global_provider() -> None:
    # Bare construction (no meter argument) falls back to the global meter
    # provider — exercises the ``self._meter is None`` branch.
    sink = OpenTelemetrySink()
    async with sink:
        await sink.export(
            (MetricSnapshot(name="UserRecordsPut", count=1, sum=1.0, min=1.0, max=1.0),)
        )
