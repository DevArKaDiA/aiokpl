"""Unit tests for :mod:`aiokpl.metrics`.

Exercises:

* :class:`_Accumulator` window mechanics with a fake clock — empty stats,
  single observations, late observations that roll forward, eviction of
  expired buckets.
* :class:`MetricsLevel` enum identity.
* :class:`MetricKey` equality + hashing + dimensions rendering.
* :class:`MetricsManager` zero-overhead path when ``level == NONE``: no
  upload task, no sink entered, snapshot stays empty after :meth:`put`.
* :class:`MetricsManager` dimension trimming at ``SUMMARY`` vs ``DETAILED``.
* :class:`MetricsManager` periodic flush onto an :class:`InMemorySink`.
* :class:`MetricsManager` dispatch into an :class:`EventfulMetricsSink` on
  every :meth:`put`.
* Clean teardown: final drain + cancellation of the upload task.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import anyio
import anyio.lowlevel
import pytest

from aiokpl.metrics import (
    NAME_REQUEST_TIME,
    NAME_USER_RECORDS_PUT,
    NAME_USER_RECORDS_RECEIVED,
    Metric,
    MetricKey,
    MetricsLevel,
    MetricsManager,
    _Accumulator,
)
from aiokpl.sinks import InMemorySink, MetricEvent, MetricSnapshot, NullSink

# ─── _Accumulator ──────────────────────────────────────────────────────────


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def test_accumulator_empty_stats_is_none() -> None:
    acc = _Accumulator(clock=FakeClock())
    assert acc.stats() is None
    assert acc.window_bounds() == (0.0, 0.0)


def test_accumulator_single_put() -> None:
    acc = _Accumulator(clock=FakeClock())
    acc.put(7.5)
    assert acc.stats() == (1, 7.5, 7.5, 7.5)
    ws, we = acc.window_bounds()
    assert we == ws + 1.0


def test_accumulator_aggregates_within_bucket() -> None:
    clk = FakeClock()
    acc = _Accumulator(clock=clk)
    acc.put(1.0)
    acc.put(3.0)
    acc.put(2.0)
    assert acc.stats() == (3, 6.0, 1.0, 3.0)


def test_accumulator_rolls_forward_across_seconds() -> None:
    clk = FakeClock()
    acc = _Accumulator(clock=clk)
    acc.put(1.0)
    clk.advance(1.0)
    acc.put(5.0)
    stats = acc.stats()
    assert stats == (2, 6.0, 1.0, 5.0)


def test_accumulator_evicts_old_buckets() -> None:
    clk = FakeClock()
    acc = _Accumulator(window_seconds=60, clock=clk)
    acc.put(99.0)
    clk.advance(120.0)
    assert acc.stats() is None
    acc.put(2.0)
    assert acc.stats() == (1, 2.0, 2.0, 2.0)


def test_accumulator_keeps_buckets_at_window_edge() -> None:
    clk = FakeClock()
    acc = _Accumulator(window_seconds=3, clock=clk)
    acc.put(1.0)
    clk.advance(2.0)
    acc.put(2.0)
    stats = acc.stats()
    assert stats == (2, 3.0, 1.0, 2.0)


# ─── MetricsLevel ──────────────────────────────────────────────────────────


def test_metrics_level_values() -> None:
    assert MetricsLevel.NONE.value == "none"
    assert MetricsLevel.SUMMARY.value == "summary"
    assert MetricsLevel.DETAILED.value == "detailed"


# ─── MetricKey ─────────────────────────────────────────────────────────────


def test_metric_key_equality_and_hash() -> None:
    a = MetricKey(name="x", stream="s")
    b = MetricKey(name="x", stream="s")
    c = MetricKey(name="x", stream="t")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    d = {a: 1}
    assert d[b] == 1


def test_metric_key_dimensions_rendering() -> None:
    assert MetricKey(name="A").dimensions() == ()
    assert MetricKey(name="A", stream="s").dimensions() == (("stream", "s"),)
    assert MetricKey(name="A", shard_id="7").dimensions() == (("shard", "7"),)
    assert MetricKey(name="A", error_code="X").dimensions() == (("error_code", "X"),)
    full = MetricKey(name="A", stream="s", shard_id="7", error_code="X").dimensions()
    assert full == (("stream", "s"), ("shard", "7"), ("error_code", "X"))


# ─── Metric ────────────────────────────────────────────────────────────────


def test_metric_put_and_stats() -> None:
    clk = FakeClock()
    m = Metric(MetricKey(name="n"), clock=clk)
    assert m.stats() is None
    m.put(2.0)
    m.put(4.0)
    assert m.stats() == (2, 6.0, 2.0, 4.0)
    assert m.window_bounds()[1] > 0.0


# ─── MetricsManager: NONE level ────────────────────────────────────────────


async def test_manager_none_level_is_noop() -> None:
    sleep_calls: list[float] = []

    async def fake_sleep(t: float) -> None:
        sleep_calls.append(t)

    sink = InMemorySink()
    mgr = MetricsManager(
        level=MetricsLevel.NONE,
        sink=sink,
        sleep_fn=fake_sleep,
    )
    async with mgr:
        mgr.put(NAME_USER_RECORDS_RECEIVED, 1.0, stream="s", shard_id="0")
        mgr.put(NAME_USER_RECORDS_PUT, 1.0)
        await mgr.flush()  # also no-op
    assert mgr.snapshot() == {}
    assert mgr.snapshots() == ()
    assert sleep_calls == []
    assert sink.exports == ()


async def test_manager_default_sink_is_null() -> None:
    mgr = MetricsManager(level=MetricsLevel.DETAILED)
    assert isinstance(mgr.sink, NullSink)
    async with mgr:
        mgr.put("X", 1.0, stream="s")
        await mgr.flush()
    assert mgr.level is MetricsLevel.DETAILED


# ─── MetricsManager: SUMMARY vs DETAILED ───────────────────────────────────


async def test_manager_summary_drops_shard_and_error_code() -> None:
    sink = InMemorySink()
    mgr = MetricsManager(level=MetricsLevel.SUMMARY, sink=sink)
    async with mgr:
        mgr.put("X", 1.0, stream="s", shard_id="7", error_code="boom")
        mgr.put("X", 1.0, stream="s", shard_id="9", error_code="boom")
    snap = mgr.snapshot()
    keys = list(snap.keys())
    assert len(keys) == 1
    assert keys[0].shard_id is None
    assert keys[0].error_code is None
    assert snap[keys[0]][0] == 2


async def test_manager_detailed_keeps_all_dims() -> None:
    mgr = MetricsManager(level=MetricsLevel.DETAILED, sink=InMemorySink())
    async with mgr:
        mgr.put("X", 1.0, stream="s", shard_id="7", error_code="boom")
        mgr.put("X", 1.0, stream="s", shard_id="9", error_code="boom")
    snap = mgr.snapshot()
    assert len(snap) == 2


# ─── MetricsManager: periodic flush onto InMemorySink ──────────────────────


async def test_manager_flushes_on_interval_to_sink() -> None:
    sink = InMemorySink()
    release = anyio.Event()
    done = anyio.Event()

    async def fake_sleep(_t: float) -> None:
        await release.wait()
        done.set()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        sink=sink,
        upload_interval_ms=10.0,
        sleep_fn=fake_sleep,
    )
    async with mgr:
        mgr.put(NAME_REQUEST_TIME, 12.0, stream="s")
        mgr.put(NAME_USER_RECORDS_PUT, 1.0, stream="s", shard_id="shardId-0")
        release.set()
        with anyio.fail_after(2.0):
            await done.wait()
        await anyio.lowlevel.checkpoint()
    # At least one periodic + the final __aexit__ flush.
    assert len(sink.exports) >= 1
    names = {s.name for s in sink.all_snapshots}
    assert NAME_REQUEST_TIME in names
    assert NAME_USER_RECORDS_PUT in names
    # Snapshots carry dimensions and window bounds.
    snap_for_req = sink.by_name(NAME_REQUEST_TIME)[0]
    assert isinstance(snap_for_req, MetricSnapshot)
    assert snap_for_req.dimensions == (("stream", "s"),)
    assert snap_for_req.window_end > snap_for_req.window_start


async def test_manager_flush_skipped_when_no_metrics() -> None:
    sink = InMemorySink()

    async def block_sleep(_t: float) -> None:
        await anyio.sleep_forever()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        sink=sink,
        sleep_fn=block_sleep,
    )
    async with mgr:
        pass
    assert sink.exports == ()


async def test_manager_explicit_flush_drains_sink() -> None:
    sink = InMemorySink()

    async def block_sleep(_t: float) -> None:
        await anyio.sleep_forever()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        sink=sink,
        sleep_fn=block_sleep,
    )
    async with mgr:
        mgr.put("X", 1.0, stream="s")
        await mgr.flush()
        assert len(sink.exports) == 1


async def test_manager_uses_null_sink_by_default_and_level_off() -> None:
    mgr = MetricsManager()
    assert isinstance(mgr.sink, NullSink)
    async with mgr:
        mgr.put("X", 1.0)
    assert mgr.snapshot() == {}


# ─── MetricsManager + EventfulMetricsSink ──────────────────────────────────


class CapturingEventfulSink:
    """Records each event and each export batch."""

    def __init__(self) -> None:
        self.events: list[MetricEvent] = []
        self.exports: list[tuple[MetricSnapshot, ...]] = []

    def record(self, event: MetricEvent) -> None:
        self.events.append(event)

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        self.exports.append(tuple(snapshots))

    async def __aenter__(self) -> CapturingEventfulSink:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        return None


async def test_manager_dispatches_record_to_eventful_sink() -> None:
    sink = CapturingEventfulSink()
    mgr = MetricsManager(level=MetricsLevel.DETAILED, sink=sink)
    async with mgr:
        mgr.put("X", 2.5, stream="s", shard_id="7")
        mgr.put("X", 1.0, stream="s", shard_id="7")
    assert len(sink.events) == 2
    assert sink.events[0].name == "X"
    assert sink.events[0].value == 2.5
    assert sink.events[0].dimensions == (("stream", "s"), ("shard", "7"))


# ─── Snapshot edge cases ──────────────────────────────────────────────────


def test_accumulator_second_bucket_min_max_unchanged() -> None:
    clk = FakeClock()
    acc = _Accumulator(clock=clk)
    acc.put(0.0)
    acc.put(10.0)
    clk.advance(1.0)
    acc.put(5.0)
    assert acc.stats() == (3, 15.0, 0.0, 10.0)


async def test_snapshot_drops_metrics_whose_window_is_empty() -> None:
    clk = FakeClock()
    sink = InMemorySink()
    mgr = MetricsManager(level=MetricsLevel.DETAILED, sink=sink, clock=clk)
    async with mgr:
        mgr.put("X", 1.0, stream="s")
        clk.advance(120.0)
        snap = mgr.snapshot()
        snaps = mgr.snapshots()
    assert snap == {}
    assert snaps == ()


def test_metric_acc_min_max_at_minus_inf_for_negative_values() -> None:
    clk = FakeClock()
    acc = _Accumulator(clock=clk)
    acc.put(-1.0)
    assert acc.stats() == (1, -1.0, -1.0, -1.0)
    acc.put(-5.0)
    s = acc.stats()
    assert s is not None
    assert s[2] == -5.0
    assert s[3] == -1.0
    assert not math.isinf(s[2])
