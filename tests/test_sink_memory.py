""":class:`aiokpl.sinks.InMemorySink` accessors and ordering."""

from __future__ import annotations

import pytest

from aiokpl.sinks import InMemorySink, MetricSnapshot


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _snap(name: str, val: float = 1.0) -> MetricSnapshot:
    return MetricSnapshot(name=name, count=1, sum=val, min=val, max=val)


async def test_in_memory_records_batches_in_order() -> None:
    sink = InMemorySink()
    async with sink as s:
        await s.export((_snap("A"),))
        await s.export((_snap("B"), _snap("C")))
    batches = sink.exports
    assert len(batches) == 2
    assert [b[0].name for b in batches] == ["A", "B"]
    # all_snapshots is the flattened view, in call order.
    assert tuple(s.name for s in sink.all_snapshots) == ("A", "B", "C")


async def test_in_memory_by_name_filters() -> None:
    sink = InMemorySink()
    async with sink:
        await sink.export((_snap("A", 1.0), _snap("B", 2.0), _snap("A", 3.0)))
    matches = sink.by_name("A")
    assert tuple(s.sum for s in matches) == (1.0, 3.0)
    assert sink.by_name("Z") == ()


async def test_in_memory_empty_state() -> None:
    sink = InMemorySink()
    assert sink.exports == ()
    assert sink.all_snapshots == ()
    assert sink.by_name("missing") == ()
