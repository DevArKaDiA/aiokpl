"""Structural conformance checks for the sink Protocols.

These guarantee :class:`aiokpl.sinks.MetricsSink` /
:class:`aiokpl.sinks.EventfulMetricsSink` recognise both the first-party
sinks and user-defined ducks via :func:`isinstance` thanks to
``@runtime_checkable``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from aiokpl.sinks import (
    CloudWatchSink,
    EventfulMetricsSink,
    InMemorySink,
    MetricEvent,
    MetricSnapshot,
    MetricsSink,
    NullSink,
)


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def test_metric_event_is_frozen_and_hashable() -> None:
    dims = (("k", "v"),)
    e = MetricEvent(name="X", value=1.0, timestamp=0.0, dimensions=dims)
    other = MetricEvent(name="X", value=1.0, timestamp=0.0, dimensions=dims)
    assert hash(e) == hash(other)


def test_metric_snapshot_carries_dimensions_and_window() -> None:
    s = MetricSnapshot(
        name="X",
        count=2,
        sum=3.0,
        min=1.0,
        max=2.0,
        dimensions=(("stream", "s"),),
        window_start=10.0,
        window_end=70.0,
    )
    assert s.window_end - s.window_start == 60.0
    assert s.dimensions == (("stream", "s"),)


def test_first_party_sinks_match_metrics_sink_protocol() -> None:
    assert isinstance(NullSink(), MetricsSink)
    assert isinstance(InMemorySink(), MetricsSink)
    assert isinstance(CloudWatchSink(region="us-east-1"), MetricsSink)


def test_eventful_protocol_recognises_record_method() -> None:
    class Eventful:
        def record(self, event: MetricEvent) -> None:
            pass

        async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
            return None

        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

    assert isinstance(Eventful(), EventfulMetricsSink)
    # NullSink lacks `record` — it is NOT an EventfulMetricsSink.
    assert not isinstance(NullSink(), EventfulMetricsSink)
