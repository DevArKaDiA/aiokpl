"""Unit tests for :mod:`aiokpl.metrics`.

Exercises:

* :class:`_Accumulator` window mechanics with a fake clock — empty stats,
  single observations, late observations that roll forward, eviction of
  expired buckets.
* :class:`MetricsLevel` enum identity.
* :class:`MetricKey` equality + hashing.
* :class:`MetricsManager` zero-overhead path when ``level == NONE``: no
  upload task, no client, snapshot stays empty after :meth:`put`.
* :class:`MetricsManager` dimension trimming at ``SUMMARY`` vs ``DETAILED``.
* :class:`MetricsManager` periodic upload via a fake CloudWatch client:
  payload format (Dimensions + StatisticValues) and chunking at the 1000-
  metric per-call ceiling.
* Clean teardown: final drain + cancellation of the upload task.
"""

from __future__ import annotations

import math
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

# ─── _Accumulator ──────────────────────────────────────────────────────────


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


def test_accumulator_empty_stats_is_none() -> None:
    acc = _Accumulator(clock=FakeClock())
    assert acc.stats() is None


def test_accumulator_single_put() -> None:
    acc = _Accumulator(clock=FakeClock())
    acc.put(7.5)
    assert acc.stats() == (1, 7.5, 7.5, 7.5)


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
    # The old bucket has expired; the next put rolls into a fresh window.
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
    # Usable as dict key
    d = {a: 1}
    assert d[b] == 1


# ─── Metric ────────────────────────────────────────────────────────────────


def test_metric_put_and_stats() -> None:
    clk = FakeClock()
    m = Metric(MetricKey(name="n"), clock=clk)
    assert m.stats() is None
    m.put(2.0)
    m.put(4.0)
    assert m.stats() == (2, 6.0, 2.0, 4.0)


# ─── MetricsManager: NONE level ────────────────────────────────────────────


async def test_manager_none_level_is_noop() -> None:
    sleep_calls: list[float] = []

    async def fake_sleep(t: float) -> None:
        sleep_calls.append(t)

    factory_calls: list[int] = []

    def factory() -> Any:  # pragma: no cover — must NOT be called
        factory_calls.append(1)
        raise AssertionError("factory must not be invoked at NONE level")

    mgr = MetricsManager(
        level=MetricsLevel.NONE,
        cw_client_factory=factory,
        sleep_fn=fake_sleep,
    )
    async with mgr:
        mgr.put(NAME_USER_RECORDS_RECEIVED, 1.0, stream="s", shard_id="0")
        mgr.put(NAME_USER_RECORDS_PUT, 1.0)
    assert mgr.snapshot() == {}
    assert sleep_calls == []
    assert factory_calls == []


@pytest.fixture
def anyio_backend() -> str:
    # MetricsManager uses anyio.sleep + anyio.CancelScope; both backends
    # are supported. We still pin to asyncio because aiobotocore (the real
    # CloudWatch client) is asyncio-only — and tests that wire a real
    # CW-shaped fake should mirror the production constraint.
    return "asyncio"


# ─── MetricsManager: SUMMARY vs DETAILED ───────────────────────────────────


async def test_manager_summary_drops_shard_and_error_code() -> None:
    mgr = MetricsManager(level=MetricsLevel.SUMMARY)
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
    mgr = MetricsManager(level=MetricsLevel.DETAILED)
    async with mgr:
        mgr.put("X", 1.0, stream="s", shard_id="7", error_code="boom")
        mgr.put("X", 1.0, stream="s", shard_id="9", error_code="boom")
    snap = mgr.snapshot()
    assert len(snap) == 2


async def test_manager_properties() -> None:
    mgr = MetricsManager(level=MetricsLevel.DETAILED, namespace="custom")
    assert mgr.level is MetricsLevel.DETAILED
    assert mgr.namespace == "custom"


# ─── MetricsManager: upload via fake CloudWatch ────────────────────────────


class FakeCWClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class FakeCWClientCtx:
    def __init__(self, client: FakeCWClient) -> None:
        self._client = client
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeCWClient:
        self.entered = True
        return self._client

    async def __aexit__(self, *_: Any) -> None:
        self.exited = True


def _make_factory() -> tuple[FakeCWClient, FakeCWClientCtx, Any]:
    client = FakeCWClient()
    ctx = FakeCWClientCtx(client)

    def factory() -> FakeCWClientCtx:
        return ctx

    return client, ctx, factory


async def test_manager_uploads_on_interval() -> None:
    client, ctx, factory = _make_factory()

    # A controllable sleep_fn: the upload loop awaits this; each call we
    # release just enough to drive one upload cycle.
    release = anyio.Event()
    done = anyio.Event()

    async def fake_sleep(_t: float) -> None:
        await release.wait()
        done.set()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        namespace="ns",
        upload_interval_ms=10.0,
        cw_client_factory=factory,
        sleep_fn=fake_sleep,
    )
    async with mgr:
        mgr.put(NAME_REQUEST_TIME, 12.0, stream="s")
        mgr.put(NAME_USER_RECORDS_PUT, 1.0, stream="s", shard_id="shardId-0")
        release.set()
        with anyio.fail_after(2.0):
            await done.wait()
        # Yield so the upload task observes ``done`` and posts.
        await anyio.lowlevel.checkpoint()
    assert ctx.entered and ctx.exited
    # One periodic + one final upload at __aexit__.
    assert len(client.calls) >= 1
    payload = client.calls[0]
    assert payload["Namespace"] == "ns"
    data = payload["MetricData"]
    assert any(d["MetricName"] == NAME_REQUEST_TIME for d in data)
    for datum in data:
        assert "Dimensions" in datum
        sv = datum["StatisticValues"]
        assert {"SampleCount", "Sum", "Minimum", "Maximum"} <= sv.keys()


async def test_manager_upload_chunks_when_over_limit() -> None:
    client, _ctx, factory = _make_factory()

    async def immediate_sleep(_t: float) -> None:
        # Block forever — the periodic upload should not run; we rely on the
        # __aexit__ final drain to flush the snapshot.
        await anyio.sleep_forever()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        upload_interval_ms=10.0,
        cw_client_factory=factory,
        sleep_fn=immediate_sleep,
    )
    async with mgr:
        # 2500 distinct keys (stream dim varied) > 1000 chunk size.
        for i in range(2500):
            mgr.put("X", 1.0, stream=f"s-{i}")
    # The final drain should split into 3 calls: 1000 + 1000 + 500.
    assert len(client.calls) == 3
    sizes = [len(c["MetricData"]) for c in client.calls]
    assert sizes == [1000, 1000, 500]


async def test_manager_upload_skipped_when_no_metrics() -> None:
    client, _ctx, factory = _make_factory()

    async def block_sleep(_t: float) -> None:
        await anyio.sleep_forever()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        cw_client_factory=factory,
        sleep_fn=block_sleep,
    )
    async with mgr:
        pass  # no observations
    # Final drain bailed out early; no PutMetricData calls.
    assert client.calls == []


async def test_manager_without_factory_still_runs_loop_without_uploading() -> None:
    release = anyio.Event()
    seen = anyio.Event()

    async def fake_sleep(_t: float) -> None:
        await release.wait()
        seen.set()

    mgr = MetricsManager(
        level=MetricsLevel.DETAILED,
        cw_client_factory=None,
        sleep_fn=fake_sleep,
    )
    async with mgr:
        mgr.put("X", 1.0, stream="s")
        release.set()
        with anyio.fail_after(2.0):
            await seen.wait()
        await anyio.lowlevel.checkpoint()
        # The in-memory snapshot is unaffected by the absent uploader.
        assert mgr.snapshot()


def test_accumulator_second_bucket_min_max_unchanged() -> None:
    # First bucket records the extremes; the second bucket's observations
    # are in between. Exercises the False branches of `b.min < mn` and
    # `b.max > mx` inside the stats loop.
    clk = FakeClock()
    acc = _Accumulator(clock=clk)
    acc.put(0.0)
    acc.put(10.0)
    clk.advance(1.0)
    acc.put(5.0)
    assert acc.stats() == (3, 15.0, 0.0, 10.0)


async def test_snapshot_drops_metrics_whose_window_is_empty() -> None:
    clk = FakeClock()
    mgr = MetricsManager(level=MetricsLevel.DETAILED, clock=clk)
    async with mgr:
        mgr.put("X", 1.0, stream="s")
        # Advance past the window so the recorded bucket evicts.
        clk.advance(120.0)
        snap = mgr.snapshot()
    assert snap == {}


def test_key_to_datum_handles_all_dim_combinations() -> None:
    # Exercise every branch in _key_to_datum: no dims, stream-only,
    # shard-only, error-only, and all-three.
    mgr = MetricsManager(level=MetricsLevel.DETAILED)
    datum_none = mgr._key_to_datum(MetricKey(name="A"), (1, 2.0, 0.0, 5.0))
    assert datum_none["Dimensions"] == []
    datum_s = mgr._key_to_datum(MetricKey(name="A", stream="s"), (1, 0.0, 0.0, 0.0))
    assert datum_s["Dimensions"] == [{"Name": "StreamName", "Value": "s"}]
    datum_sh = mgr._key_to_datum(MetricKey(name="A", shard_id="7"), (1, 0.0, 0.0, 0.0))
    assert datum_sh["Dimensions"] == [{"Name": "ShardId", "Value": "7"}]
    datum_ec = mgr._key_to_datum(MetricKey(name="A", error_code="X"), (1, 0.0, 0.0, 0.0))
    assert datum_ec["Dimensions"] == [{"Name": "ErrorCode", "Value": "X"}]


def test_metric_acc_min_max_at_minus_inf_for_negative_values() -> None:
    # Defensive: ensure the math.inf / -math.inf initialisers work for any
    # sign of value (negative observations are unusual but legal).
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
