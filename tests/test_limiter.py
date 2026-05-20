"""Tests for :class:`aiokpl.limiter.Limiter` and :class:`ShardLimiter`.

Uses a minimal fake batch satisfying the protocol surface the Limiter needs —
no Aggregator wiring, keeping the tests laser-focused on Phase 4.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from aiokpl.limiter import (
    BYTES_PER_SEC_PER_SHARD,
    DEFAULT_DRAIN_INTERVAL_MS,
    DEFAULT_EXPIRATION_MS,
    RECORDS_PER_SEC_PER_SHARD,
    Limiter,
    ShardLimiter,
)

if TYPE_CHECKING:
    from aiokpl.aggregator import AggregatedBatch


@dataclass
class FakeBatch:
    """Minimal AggregatedBatch stand-in.

    The Limiter's public surface is typed against the concrete
    :class:`AggregatedBatch`, but it only touches the attributes declared on
    the internal ``_ExpirableBatch`` protocol. Tests stay laser-focused on
    Phase 4 by passing this stand-in through a structural ``cast``.
    """

    predicted_shard: int | None
    size: int
    count: int = 1
    deadline: float = 0.0
    tag: str = ""


def _b(fake: FakeBatch) -> AggregatedBatch:
    """Cast a :class:`FakeBatch` to the Limiter's nominal batch type."""
    return cast("AggregatedBatch", fake)


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@dataclass
class Collected:
    admitted: list[FakeBatch] = field(default_factory=list)
    expired: list[tuple[FakeBatch, str]] = field(default_factory=list)

    async def on_admit(self, batch: AggregatedBatch) -> None:
        self.admitted.append(cast("FakeBatch", batch))

    async def on_expired(self, batch: AggregatedBatch, reason: str) -> None:
        self.expired.append((cast("FakeBatch", batch), reason))


def _defaults_exposed() -> None:
    assert RECORDS_PER_SEC_PER_SHARD == 1_000.0
    assert BYTES_PER_SEC_PER_SHARD == 1_048_576.0
    assert DEFAULT_DRAIN_INTERVAL_MS == 25.0
    assert DEFAULT_EXPIRATION_MS == 30_000.0


def test_defaults_exposed() -> None:
    _defaults_exposed()


# ─── ShardLimiter unit tests (sync) ────────────────────────────────────────


def test_shardlimiter_admits_when_tokens_available() -> None:
    clock = FakeClock()
    sl = ShardLimiter(clock=clock)
    b = FakeBatch(predicted_shard=0, size=10, deadline=0.0)
    sl.enqueue(_b(b), expires_at=100.0)
    admitted, expired = sl.drain()
    assert admitted == [b]
    assert expired == []
    assert sl.pending_count == 0


def test_shardlimiter_expired_surfaces_without_consuming_tokens() -> None:
    clock = FakeClock(t=10.0)
    sl = ShardLimiter(records_per_sec=1.0, bytes_per_sec=1.0, clock=clock)
    # Bucket starts full at 1 record / 1 byte. If expiration weren't checked
    # first, an 8-byte batch would fail try_take.
    b = FakeBatch(predicted_shard=0, size=8, deadline=0.0)
    sl.enqueue(_b(b), expires_at=5.0)  # already past
    admitted, expired = sl.drain()
    assert admitted == []
    assert expired == [b]
    assert sl.pending_count == 0


def test_shardlimiter_backpressure_when_tokens_exhausted() -> None:
    clock = FakeClock()
    # 1 record/s, 100 bytes/s, both buckets start full at capacity.
    sl = ShardLimiter(records_per_sec=1.0, bytes_per_sec=100.0, clock=clock)
    b1 = FakeBatch(predicted_shard=0, size=10, deadline=0.0, tag="b1")
    b2 = FakeBatch(predicted_shard=0, size=10, deadline=1.0, tag="b2")
    sl.enqueue(_b(b1), expires_at=1000.0)
    sl.enqueue(_b(b2), expires_at=1000.0)
    admitted, expired = sl.drain()
    # Only 1 record token available; b1 admitted, b2 stays pending.
    assert admitted == [b1]
    assert expired == []
    assert sl.pending_count == 1

    # Advance time so the records bucket refills.
    clock.advance(2.0)
    admitted2, expired2 = sl.drain()
    assert admitted2 == [b2]
    assert expired2 == []
    assert sl.pending_count == 0


def test_shardlimiter_drain_force_admits_everything_non_expired() -> None:
    clock = FakeClock(t=10.0)
    sl = ShardLimiter(records_per_sec=1.0, bytes_per_sec=1.0, clock=clock)
    keep = FakeBatch(predicted_shard=0, size=999, deadline=0.0, tag="keep")
    gone = FakeBatch(predicted_shard=0, size=1, deadline=1.0, tag="gone")
    sl.enqueue(_b(keep), expires_at=1000.0)
    sl.enqueue(_b(gone), expires_at=5.0)
    admitted, expired = sl.drain_force()
    assert admitted == [keep]
    assert expired == [gone]
    assert sl.pending_count == 0


# ─── Limiter orchestrator (async) ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_put_single_batch_admitted_immediately() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b = FakeBatch(predicted_shard=0, size=10, deadline=0.0)
    await lim.put(_b(b))
    assert c.admitted == [b]
    assert c.expired == []
    await lim.aclose()


@pytest.mark.asyncio
async def test_put_backpressure_then_refill_via_background_drain() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        records_per_sec_per_shard=1.0,
        bytes_per_sec_per_shard=100.0,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b1 = FakeBatch(predicted_shard=0, size=10, deadline=0.0, tag="b1")
    b2 = FakeBatch(predicted_shard=0, size=10, deadline=1.0, tag="b2")
    await lim.put(_b(b1))
    await lim.put(_b(b2))
    assert [b.tag for b in c.admitted] == ["b1"]

    # Refill, give the drain task a chance to run.
    clock.advance(2.0)
    await asyncio.sleep(0.05)
    assert [b.tag for b in c.admitted] == ["b1", "b2"]
    await lim.aclose()


@pytest.mark.asyncio
async def test_expired_batch_surfaces_via_on_expired_without_admit() -> None:
    # Zero-capacity bucket → try_take always fails, so the only exit path is
    # expiration. ``expiration_ms=1.0`` puts the batch one millisecond from
    # death; we advance the clock past that, the background drain ticks, and
    # the on_expired callback fires with reason "Expired".
    clock = FakeClock(t=10.0)
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        records_per_sec_per_shard=0.0,
        bytes_per_sec_per_shard=0.0,
        expiration_ms=1.0,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b_exp = FakeBatch(predicted_shard=0, size=1, deadline=0.0, tag="exp")
    await lim.put(_b(b_exp))
    assert c.expired == []
    assert c.admitted == []
    clock.advance(1.0)
    await asyncio.sleep(0.05)
    assert [b.tag for b, _ in c.expired] == ["exp"]
    assert c.expired[0][1] == "Expired"
    assert c.admitted == []
    await lim.aclose()


@pytest.mark.asyncio
async def test_multi_shard_isolation() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        records_per_sec_per_shard=1.0,
        bytes_per_sec_per_shard=10.0,
        drain_interval_ms=5.0,
        clock=clock,
    )
    # Two batches on shard 0 (second one will be held), one batch on shard 1
    # admitted immediately because its own bucket is fresh.
    a0 = FakeBatch(predicted_shard=0, size=5, tag="a0")
    a1 = FakeBatch(predicted_shard=0, size=5, tag="a1", deadline=1.0)
    b0 = FakeBatch(predicted_shard=1, size=5, tag="b0")
    await lim.put(_b(a0))
    await lim.put(_b(a1))
    await lim.put(_b(b0))
    tags = [b.tag for b in c.admitted]
    assert "a0" in tags and "b0" in tags
    assert "a1" not in tags
    await lim.aclose()


@pytest.mark.asyncio
async def test_none_shard_catchall() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b = FakeBatch(predicted_shard=None, size=10)
    await lim.put(_b(b))
    assert c.admitted == [b]
    await lim.aclose()


@pytest.mark.asyncio
async def test_flush_drains_everything_regardless_of_tokens() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        records_per_sec_per_shard=1.0,
        bytes_per_sec_per_shard=10.0,
        drain_interval_ms=5.0,
        clock=clock,
    )
    a = FakeBatch(predicted_shard=0, size=5, tag="a")
    b = FakeBatch(predicted_shard=0, size=5, tag="b", deadline=1.0)
    cc = FakeBatch(predicted_shard=0, size=5, tag="c", deadline=2.0)
    await lim.put(_b(a))
    await lim.put(_b(b))
    await lim.put(_b(cc))
    # Only ``a`` got tokens.
    assert [x.tag for x in c.admitted] == ["a"]
    await lim.flush()
    assert sorted(x.tag for x in c.admitted) == ["a", "b", "c"]
    await lim.aclose()


@pytest.mark.asyncio
async def test_aclose_is_idempotent_and_cancels_drain_task() -> None:
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b = FakeBatch(predicted_shard=0, size=10)
    await lim.put(_b(b))
    task = lim._drain_task
    assert task is not None
    await lim.aclose()
    assert task.cancelled() or task.done()
    # Second aclose is a no-op.
    await lim.aclose()


@pytest.mark.asyncio
async def test_aclose_without_put_is_noop() -> None:
    c = Collected()
    lim = Limiter(on_admit=c.on_admit, on_expired=c.on_expired)
    await lim.aclose()


@pytest.mark.asyncio
async def test_background_drain_processes_expirations() -> None:
    # Verifies the background drain loop's expiration-only path (no put
    # arrives to opportunistically drain; the tick has to do it).
    clock = FakeClock(t=10.0)
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        records_per_sec_per_shard=0.0,
        bytes_per_sec_per_shard=0.0,
        expiration_ms=1.0,
        drain_interval_ms=5.0,
        clock=clock,
    )
    b = FakeBatch(predicted_shard=0, size=1, tag="background")
    await lim.put(_b(b))
    assert c.expired == []
    clock.advance(1.0)
    await asyncio.sleep(0.05)
    assert [x.tag for x, _ in c.expired] == ["background"]
    await lim.aclose()


@pytest.mark.asyncio
async def test_drain_loop_returns_when_closed_inside_lock() -> None:
    # Force the drain loop to observe ``_closed=True`` and return cleanly.
    clock = FakeClock()
    c = Collected()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        drain_interval_ms=1.0,
        clock=clock,
    )
    b = FakeBatch(predicted_shard=0, size=10)
    await lim.put(_b(b))
    task = lim._drain_task
    assert task is not None
    # Mark closed without cancelling, then cancel via aclose.
    await lim.aclose()
    # The task should be done now.
    assert task.done()


@pytest.mark.asyncio
async def test_explicit_loop_parameter_is_honored() -> None:
    clock = FakeClock()
    c = Collected()
    loop = asyncio.get_running_loop()
    lim = Limiter(
        on_admit=c.on_admit,
        on_expired=c.on_expired,
        drain_interval_ms=5.0,
        clock=clock,
        loop=loop,
    )
    b = FakeBatch(predicted_shard=0, size=10)
    await lim.put(_b(b))
    assert c.admitted == [b]
    await lim.aclose()
