"""Trivial coverage for :class:`aiokpl.sinks.NullSink`."""

from __future__ import annotations

import pytest

from aiokpl.sinks import MetricSnapshot, NullSink


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_null_sink_discards_export() -> None:
    sink = NullSink()
    async with sink as s:
        assert s is sink
        await s.export(
            (
                MetricSnapshot(name="X", count=1, sum=1.0, min=1.0, max=1.0),
                MetricSnapshot(name="Y", count=0, sum=0.0, min=0.0, max=0.0),
            )
        )
        await s.export(())
