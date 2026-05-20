"""Tests for ``aiokpl.shard_map`` — cached shard list and hash-key prediction."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from aiokpl.shard_map import (
    Shard,
    ShardMap,
    ShardMapState,
    _parse_hash_key,
    _parse_shard_id,
)

# ─── Helpers ───────────────────────────────────────────────────────────────

_UINT128_MAX = (1 << 128) - 1


def _shard_dict(
    shard_id: int,
    start: int,
    end: int,
    *,
    raw_id: str | None = None,
) -> dict:
    return {
        "ShardId": raw_id if raw_id is not None else f"shardId-{shard_id:012d}",
        "HashKeyRange": {
            "StartingHashKey": str(start),
            "EndingHashKey": str(end),
        },
    }


def _even_split(n: int) -> list[dict]:
    step = (_UINT128_MAX + 1) // n
    out = []
    for i in range(n):
        s = i * step
        e = (i + 1) * step - 1 if i < n - 1 else _UINT128_MAX
        out.append(_shard_dict(i, s, e))
    return out


def make_list_shards_fn(
    pages: list[dict],
) -> Callable[..., Awaitable[dict]]:
    """Return a callable that yields ``pages`` in order, asserting tokens."""
    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        page = pages[i]
        if i == 0:
            assert "StreamName" in kwargs
            assert kwargs.get("ShardFilter") == {"Type": "AT_LATEST"}
        else:
            assert "NextToken" in kwargs
        state["i"] = i + 1
        return page

    return fn


class _Clock:
    """Deterministic monotonic clock that callers can advance."""

    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, dt: float) -> None:
        self.value += dt


# ─── Parsing helpers ───────────────────────────────────────────────────────


def test_parse_shard_id_ok():
    assert _parse_shard_id("shardId-000000000003") == 3


@pytest.mark.parametrize(
    "raw",
    ["shard-000000000003", "shardId-", "shardId-abc", "", "shardId-3a"],
)
def test_parse_shard_id_malformed(raw: str):
    with pytest.raises(ValueError):
        _parse_shard_id(raw)


@pytest.mark.parametrize("s", ["", " 1", "1 ", "-1", "1_000", "1a", "٠"])  # noqa: RUF001
def test_parse_hash_key_rejects_noncanonical(s: str):
    with pytest.raises(ValueError):
        _parse_hash_key(s)


def test_parse_hash_key_overflow():
    with pytest.raises(ValueError):
        _parse_hash_key(str(_UINT128_MAX + 1))


def test_parse_hash_key_ok():
    assert _parse_hash_key("0") == 0
    assert _parse_hash_key(str(_UINT128_MAX)) == _UINT128_MAX


# ─── Lifecycle ─────────────────────────────────────────────────────────────


async def test_initial_state_is_invalid():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": []}]))
    assert sm.state is ShardMapState.INVALID
    assert sm.updated_at is None
    assert sm.predict(0) is None
    assert sm.hashrange(0) is None
    await sm.aclose()


async def test_start_single_page_ready_and_predict():
    shards = _even_split(4)
    fn = make_list_shards_fn([{"Shards": shards}])
    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock)
    await sm.start()

    assert sm.state is ShardMapState.READY
    assert sm.updated_at == 1000.0

    # Hit each shard at its start and end.
    step = (_UINT128_MAX + 1) // 4
    for i in range(4):
        s = i * step
        e = (i + 1) * step - 1 if i < 3 else _UINT128_MAX
        assert sm.predict(s) == i
        assert sm.predict(e) == i
        assert sm.hashrange(i) == (s, e)

    # Unknown shard
    assert sm.hashrange(999) is None
    await sm.aclose()


async def test_start_paginated_pages():
    page1 = {"Shards": _even_split(2)[:1], "NextToken": "tok1"}
    page2 = {"Shards": _even_split(2)[1:]}
    sm = ShardMap("stream", make_list_shards_fn([page1, page2]))
    await sm.start()
    assert sm.state is ShardMapState.READY
    assert sm.predict(0) == 0
    assert sm.predict(_UINT128_MAX) == 1
    await sm.aclose()


async def test_start_passes_stream_arn():
    seen: dict[str, object] = {}

    async def fn(**kwargs: object) -> dict:
        seen.update(kwargs)
        return {"Shards": _even_split(1)}

    sm = ShardMap("stream", fn, stream_arn="arn:aws:kinesis:...:stream/x")
    await sm.start()
    assert seen["StreamARN"] == "arn:aws:kinesis:...:stream/x"
    await sm.aclose()


# ─── Backoff / retry ───────────────────────────────────────────────────────


async def test_refresh_failure_then_success(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}
    pages = _even_split(2)

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("boom")
        return {"Shards": pages}

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    sm = ShardMap(
        "stream",
        fn,
        min_backoff=0.5,
        max_backoff=4.0,
        sleep_fn=fake_sleep,
    )
    await sm.start()
    assert sm.state is ShardMapState.READY
    assert calls["n"] == 3
    # Backoff doubles: 0.5, then 1.0
    assert sleeps == [0.5, 1.0]
    await sm.aclose()


async def test_refresh_backoff_caps_at_max(monkeypatch: pytest.MonkeyPatch):
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        if calls["n"] < 6:
            raise RuntimeError("boom")
        return {"Shards": _even_split(1)}

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    sm = ShardMap("stream", fn, min_backoff=1.0, max_backoff=4.0, sleep_fn=fake_sleep)
    await sm.start()
    assert sm.state is ShardMapState.READY
    # 1, 2, 4, 4, 4 (capped)
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 4.0]
    await sm.aclose()


async def test_aclose_cancels_retry_loop():
    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("always")

    stop_sleep = asyncio.Event()

    async def fake_sleep(d: float) -> None:
        # Block until cancelled to simulate a long backoff window.
        try:
            await stop_sleep.wait()
        finally:
            pass

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep)
    # Kick off refresh but don't await readiness.
    await sm._spawn_refresh()
    await asyncio.sleep(0)  # let the task start
    await sm.aclose()
    # idempotent
    await sm.aclose()
    assert sm.state is ShardMapState.UPDATING


# ─── Predict edge cases ────────────────────────────────────────────────────


async def test_predict_returns_none_when_not_ready():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": []}]))
    assert sm.predict(0) is None
    await sm.aclose()


async def test_predict_returns_none_when_hash_beyond_endings(caplog):
    # Build a map that doesn't cover the full range — force the bisect to
    # overshoot.
    shards = [_shard_dict(0, 0, 100)]
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": shards}]))
    await sm.start()
    import logging as _logging

    with caplog.at_level(_logging.ERROR, logger="aiokpl.shard_map"):
        assert sm.predict(200) is None
    assert any("could not map hash key" in r.message for r in caplog.records)
    await sm.aclose()


# ─── Invalidate ────────────────────────────────────────────────────────────


async def test_invalidate_seen_at_le_updated_at_is_noop():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock)
    await sm.start()
    assert calls["n"] == 1
    assert sm.updated_at == 1000.0
    await sm.invalidate(seen_at=999.0, predicted_shard=0)
    assert calls["n"] == 1
    await sm.aclose()


async def test_invalidate_known_open_shard_triggers_refresh():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock)
    await sm.start()
    clock.advance(5.0)
    await sm.invalidate(seen_at=1003.0, predicted_shard=0)
    # Wait for the spawned task.
    task = sm._refresh_task
    assert task is not None
    await task
    assert calls["n"] == 2
    await sm.aclose()


async def test_invalidate_unknown_shard_skipped():
    # predicted_shard given but not in current open_shards -> no refresh.
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock)
    await sm.start()
    await sm.invalidate(seen_at=2000.0, predicted_shard=99)
    assert calls["n"] == 1
    await sm.aclose()


async def test_invalidate_none_predicted_triggers_refresh():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock)
    await sm.start()
    await sm.invalidate(seen_at=2000.0, predicted_shard=None)
    task = sm._refresh_task
    assert task is not None
    await task
    assert calls["n"] == 2
    await sm.aclose()


async def test_invalidate_after_close_is_noop():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    await sm.start()
    await sm.aclose()
    await sm.invalidate(seen_at=9999.0, predicted_shard=None)
    # No assertion errors raised; state stayed READY but no new task spawned.


async def test_invalidate_before_first_refresh_runs():
    # updated_at is None -> guard passes, refresh runs.
    pages = [{"Shards": _even_split(1)}]
    sm = ShardMap("stream", make_list_shards_fn(pages))
    await sm.invalidate(seen_at=0.0, predicted_shard=None)
    task = sm._refresh_task
    assert task is not None
    await task
    assert sm.state is ShardMapState.READY
    await sm.aclose()


async def test_spawn_refresh_while_updating_is_noop():
    # Second spawn while UPDATING returns early.
    blocker = asyncio.Event()

    async def fn(**kwargs: object) -> dict:
        await blocker.wait()
        return {"Shards": _even_split(1)}

    sm = ShardMap("stream", fn)
    await sm._spawn_refresh()
    first = sm._refresh_task
    await sm._spawn_refresh()
    second = sm._refresh_task
    assert first is second
    blocker.set()
    assert first is not None
    await first
    await sm.aclose()


# ─── Closed-shard TTL ──────────────────────────────────────────────────────


async def test_closed_shard_kept_until_ttl_then_purged():
    # First refresh: 2 shards. Second refresh: 1 shard (shard 1 closed).
    first = {"Shards": _even_split(2)}
    second = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}

    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        # Token-aware: each refresh is a fresh sequence.
        if "NextToken" not in kwargs:
            page = [first, second][i]
            state["i"] = i + 1
            return page
        raise AssertionError("no pagination expected")

    clock = _Clock()
    sm = ShardMap("stream", fn, closed_shard_ttl=10.0, clock=clock)
    await sm.start()
    # Shard 1 known.
    assert sm.hashrange(1) is not None

    # Move time and force a second refresh via invalidate.
    clock.advance(1.0)
    await sm.invalidate(seen_at=1000.5, predicted_shard=None)
    task = sm._refresh_task
    assert task is not None
    await task

    # Shard 1 is now closed but still answerable within TTL.
    assert sm.hashrange(1) is not None
    assert sm.hashrange(2) is not None

    # Advance time past TTL and invoke the cleanup callback directly.
    clock.advance(20.0)
    sm._cleanup()
    assert sm.hashrange(1) is None
    assert sm.hashrange(2) is not None
    await sm.aclose()


async def test_cleanup_noop_when_no_expired():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    await sm.start()
    # No closed shards -> cleanup returns silently.
    sm._cleanup()
    await sm.aclose()


async def test_cleanup_skipped_when_not_ready():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    # Never started; state is INVALID.
    sm._cleanup()
    assert sm.state is ShardMapState.INVALID
    await sm.aclose()


async def test_cleanup_skipped_after_close():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    await sm.start()
    await sm.aclose()
    sm._cleanup()


async def test_reopened_shard_drops_closed_marker():
    # Shard 0 disappears then reappears in the next refresh.
    p1 = {"Shards": _even_split(2)}
    p2 = {"Shards": [_shard_dict(1, 0, _UINT128_MAX)]}
    p3 = {"Shards": _even_split(2)}

    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        if "NextToken" in kwargs:
            raise AssertionError("no pagination")
        i = state["i"]
        state["i"] = i + 1
        return [p1, p2, p3][i]

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock, closed_shard_ttl=100.0)
    await sm.start()
    clock.advance(1.0)
    await sm.invalidate(seen_at=1000.5, predicted_shard=None)
    assert sm._refresh_task is not None
    await sm._refresh_task
    assert 0 in sm._closed_at
    clock.advance(1.0)
    await sm.invalidate(seen_at=1002.5, predicted_shard=None)
    assert sm._refresh_task is not None
    await sm._refresh_task
    assert 0 not in sm._closed_at
    await sm.aclose()


# ─── Malformed input propagation ───────────────────────────────────────────


async def test_malformed_shard_id_propagates(monkeypatch: pytest.MonkeyPatch):
    bad = [_shard_dict(0, 0, _UINT128_MAX, raw_id="not-a-shard-id")]

    async def fn(**kwargs: object) -> dict:
        return {"Shards": bad}

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        # After one retry attempt, raise CancelledError to break out of the
        # forever retry loop.
        raise asyncio.CancelledError

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep, min_backoff=0.1)
    await sm.start()
    # State remained UPDATING; we cancelled out of the retry loop.
    assert sm.state is ShardMapState.UPDATING
    assert len(sleeps) == 1
    await sm.aclose()


async def test_negative_hash_key_string_propagates():
    bad = [
        {
            "ShardId": "shardId-000000000000",
            "HashKeyRange": {"StartingHashKey": "-1", "EndingHashKey": "100"},
        }
    ]

    async def fn(**kwargs: object) -> dict:
        return {"Shards": bad}

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        raise asyncio.CancelledError

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep)
    await sm.start()
    assert sm.state is ShardMapState.UPDATING
    assert len(sleeps) == 1
    await sm.aclose()


async def test_start_with_inverted_range_propagates():
    bad = [
        {
            "ShardId": "shardId-000000000000",
            "HashKeyRange": {"StartingHashKey": "100", "EndingHashKey": "10"},
        }
    ]

    async def fn(**kwargs: object) -> dict:
        return {"Shards": bad}

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        raise asyncio.CancelledError

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep)
    await sm.start()
    assert sm.state is ShardMapState.UPDATING
    await sm.aclose()


# ─── Cleanup scheduling via call_later ─────────────────────────────────────


async def test_cleanup_call_later_fires():
    # Use a tiny TTL and let the real event loop schedule the cleanup.
    p1 = {"Shards": _even_split(2)}
    p2 = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}
    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        state["i"] = i + 1
        return [p1, p2][i]

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock, closed_shard_ttl=0.01)
    await sm.start()
    await sm.invalidate(seen_at=2000.0, predicted_shard=None)
    assert sm._refresh_task is not None
    await sm._refresh_task
    # Advance the fake clock so cleanup sees the TTL as expired.
    clock.advance(1.0)
    # Wait long enough for the call_later to fire.
    await asyncio.sleep(0.05)
    assert sm.hashrange(0) is None
    assert sm.hashrange(1) is None
    assert sm.hashrange(2) is not None
    await sm.aclose()


# ─── Shard dataclass smoke ─────────────────────────────────────────────────


async def test_start_after_close_no_task_to_await():
    # After aclose, state stays UPDATING but _refresh_task is None.
    # Calling _spawn_refresh re-enters and returns immediately because state is
    # already UPDATING, leaving _refresh_task None — start() must tolerate that.
    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("never")

    async def fake_sleep(d: float) -> None:
        raise asyncio.CancelledError

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep)
    await sm._spawn_refresh()
    await sm.aclose()
    # Now state is UPDATING and _refresh_task is None. Call start() — it spawns
    # nothing (state already UPDATING) and the task-is-None branch executes.
    await sm.start()
    assert sm._refresh_task is None


async def test_refresh_loop_exits_when_closed_during_sleep():
    # Drive the `while not self._closed` exit path: the sleep_fn flips _closed
    # so the loop condition fails on the next iteration.
    sm_holder: dict[str, ShardMap] = {}

    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("boom")

    async def fake_sleep(d: float) -> None:
        sm_holder["sm"]._closed = True

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep, min_backoff=0.01)
    sm_holder["sm"] = sm
    await sm._spawn_refresh()
    task = sm._refresh_task
    assert task is not None
    await task
    # Loop exited cleanly via the `while not self._closed` predicate.
    assert sm._closed


async def test_install_snapshot_drops_already_expired_closed_shard():
    # Force the `closed_at + ttl > now` branch to be False: pre-seed _closed_at
    # with a stale timestamp so the disappeared shard isn't carried over.
    p1 = {"Shards": _even_split(2)}
    p2 = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}
    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        state["i"] = i + 1
        return [p1, p2][i]

    clock = _Clock()
    sm = ShardMap("stream", fn, clock=clock, closed_shard_ttl=5.0)
    await sm.start()
    # Pre-seed: pretend shard 0 was closed long ago.
    sm._closed_at[0] = -1000.0
    sm._closed_at[1] = -1000.0
    await sm.invalidate(seen_at=2000.0, predicted_shard=None)
    assert sm._refresh_task is not None
    await sm._refresh_task
    # Both stale-closed shards must have been dropped immediately.
    assert sm.hashrange(0) is None
    assert sm.hashrange(1) is None
    assert sm.hashrange(2) is not None
    await sm.aclose()


async def test_schedule_cleanup_skips_when_closed():
    # Direct call to _schedule_cleanup after close exercises the early return.
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    await sm.start()
    await sm.aclose()
    sm._schedule_cleanup()
    assert sm._cleanup_handle is None


def test_shard_dataclass_is_frozen():
    s = Shard(shard_id=0, raw_shard_id="shardId-0", starting_hash_key=0, ending_hash_key=1)
    attr = "shard_id"
    with pytest.raises(AttributeError):
        setattr(s, attr, 5)
