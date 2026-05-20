"""Tests for ``aiokpl.shard_map`` — cached shard list and hash-key prediction."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import anyio
import anyio.lowlevel
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
    async with ShardMap("stream", make_list_shards_fn([{"Shards": []}])) as sm:
        assert sm.state is ShardMapState.INVALID
        assert sm.updated_at is None
        assert sm.predict(0) is None
        assert sm.hashrange(0) is None


async def test_start_single_page_ready_and_predict():
    shards = _even_split(4)
    fn = make_list_shards_fn([{"Shards": shards}])
    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock) as sm:
        await sm.start()
        assert sm.state is ShardMapState.READY
        assert sm.updated_at == 1000.0

        step = (_UINT128_MAX + 1) // 4
        for i in range(4):
            s = i * step
            e = (i + 1) * step - 1 if i < 3 else _UINT128_MAX
            assert sm.predict(s) == i
            assert sm.predict(e) == i
            assert sm.hashrange(i) == (s, e)

        assert sm.hashrange(999) is None


async def test_start_paginated_pages():
    page1 = {"Shards": _even_split(2)[:1], "NextToken": "tok1"}
    page2 = {"Shards": _even_split(2)[1:]}
    async with ShardMap("stream", make_list_shards_fn([page1, page2])) as sm:
        await sm.start()
        assert sm.state is ShardMapState.READY
        assert sm.predict(0) == 0
        assert sm.predict(_UINT128_MAX) == 1


async def test_start_passes_stream_arn():
    seen: dict[str, object] = {}

    async def fn(**kwargs: object) -> dict:
        seen.update(kwargs)
        return {"Shards": _even_split(1)}

    async with ShardMap("stream", fn, stream_arn="arn:aws:kinesis:...:stream/x") as sm:
        await sm.start()
        assert seen["StreamARN"] == "arn:aws:kinesis:...:stream/x"


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

    async with ShardMap(
        "stream",
        fn,
        min_backoff=0.5,
        max_backoff=4.0,
        sleep_fn=fake_sleep,
    ) as sm:
        await sm.start()
        assert sm.state is ShardMapState.READY
        assert calls["n"] == 3
        assert sleeps == [0.5, 1.0]


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

    async with ShardMap("stream", fn, min_backoff=1.0, max_backoff=4.0, sleep_fn=fake_sleep) as sm:
        await sm.start()
        assert sm.state is ShardMapState.READY
        assert sleeps == [1.0, 2.0, 4.0, 4.0, 4.0]


async def test_aclose_cancels_retry_loop():
    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("always")

    stop_sleep = anyio.Event()

    async def fake_sleep(d: float) -> None:
        await stop_sleep.wait()

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep)
    async with sm:
        await sm._spawn_refresh()
        await anyio.lowlevel.checkpoint()
        await sm.aclose()
        await sm.aclose()  # idempotent
        assert sm.state is ShardMapState.UPDATING


# ─── Predict edge cases ────────────────────────────────────────────────────


async def test_predict_returns_none_when_not_ready():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": []}])) as sm:
        assert sm.predict(0) is None


async def test_predict_returns_none_when_hash_beyond_endings(caplog):
    shards = [_shard_dict(0, 0, 100)]
    async with ShardMap("stream", make_list_shards_fn([{"Shards": shards}])) as sm:
        await sm.start()
        import logging as _logging

        with caplog.at_level(_logging.ERROR, logger="aiokpl.shard_map"):
            assert sm.predict(200) is None
        assert any("could not map hash key" in r.message for r in caplog.records)


# ─── Invalidate ────────────────────────────────────────────────────────────


async def test_invalidate_seen_at_le_updated_at_is_noop():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock) as sm:
        await sm.start()
        assert calls["n"] == 1
        assert sm.updated_at == 1000.0
        await sm.invalidate(seen_at=999.0, predicted_shard=0)
        assert calls["n"] == 1


async def test_invalidate_known_open_shard_triggers_refresh():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock) as sm:
        await sm.start()
        clock.advance(5.0)
        await sm.invalidate(seen_at=1003.0, predicted_shard=0)
        await sm._refresh_done.wait()
        assert calls["n"] == 2


async def test_invalidate_unknown_shard_skipped():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock) as sm:
        await sm.start()
        await sm.invalidate(seen_at=2000.0, predicted_shard=99)
        assert calls["n"] == 1


async def test_invalidate_none_predicted_triggers_refresh():
    calls = {"n": 0}

    async def fn(**kwargs: object) -> dict:
        calls["n"] += 1
        return {"Shards": _even_split(1)}

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock) as sm:
        await sm.start()
        await sm.invalidate(seen_at=2000.0, predicted_shard=None)
        await sm._refresh_done.wait()
        assert calls["n"] == 2


async def test_invalidate_after_close_is_noop():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}])) as sm:
        await sm.start()
        await sm.aclose()
        await sm.invalidate(seen_at=9999.0, predicted_shard=None)


async def test_invalidate_before_first_refresh_runs():
    pages = [{"Shards": _even_split(1)}]
    async with ShardMap("stream", make_list_shards_fn(pages)) as sm:
        await sm.invalidate(seen_at=0.0, predicted_shard=None)
        await sm._refresh_done.wait()
        assert sm.state is ShardMapState.READY


async def test_spawn_refresh_while_updating_is_noop():
    blocker = anyio.Event()

    async def fn(**kwargs: object) -> dict:
        await blocker.wait()
        return {"Shards": _even_split(1)}

    async with ShardMap("stream", fn) as sm:
        await sm._spawn_refresh()
        first_event = sm._refresh_done
        await sm._spawn_refresh()
        second_event = sm._refresh_done
        # Second spawn while UPDATING leaves the same in-flight event.
        assert first_event is second_event
        blocker.set()
        await sm._refresh_done.wait()


# ─── Closed-shard TTL ──────────────────────────────────────────────────────


async def test_closed_shard_kept_until_ttl_then_purged():
    first = {"Shards": _even_split(2)}
    second = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}

    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        if "NextToken" not in kwargs:
            page = [first, second][i]
            state["i"] = i + 1
            return page
        raise AssertionError("no pagination expected")

    clock = _Clock()
    async with ShardMap("stream", fn, closed_shard_ttl=10.0, clock=clock) as sm:
        await sm.start()
        assert sm.hashrange(1) is not None

        clock.advance(1.0)
        await sm.invalidate(seen_at=1000.5, predicted_shard=None)
        await sm._refresh_done.wait()

        assert sm.hashrange(1) is not None
        assert sm.hashrange(2) is not None

        clock.advance(20.0)
        sm._cleanup()
        assert sm.hashrange(1) is None
        assert sm.hashrange(2) is not None


async def test_cleanup_noop_when_no_expired():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}])) as sm:
        await sm.start()
        sm._cleanup()


async def test_cleanup_skipped_when_not_ready():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}])) as sm:
        sm._cleanup()
        assert sm.state is ShardMapState.INVALID


async def test_cleanup_skipped_after_close():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}])) as sm:
        await sm.start()
        await sm.aclose()
        sm._cleanup()


async def test_reopened_shard_drops_closed_marker():
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
    async with ShardMap("stream", fn, clock=clock, closed_shard_ttl=100.0) as sm:
        await sm.start()
        clock.advance(1.0)
        await sm.invalidate(seen_at=1000.5, predicted_shard=None)
        await sm._refresh_done.wait()
        assert 0 in sm._closed_at
        clock.advance(1.0)
        await sm.invalidate(seen_at=1002.5, predicted_shard=None)
        await sm._refresh_done.wait()
        assert 0 not in sm._closed_at


# ─── Malformed input propagation ───────────────────────────────────────────


async def test_malformed_shard_id_propagates(monkeypatch: pytest.MonkeyPatch):
    bad = [_shard_dict(0, 0, _UINT128_MAX, raw_id="not-a-shard-id")]

    async def fn(**kwargs: object) -> dict:
        return {"Shards": bad}

    sleeps: list[float] = []
    stop = anyio.Event()

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        # Block until the test ends, simulating a long backoff.
        await stop.wait()

    async with ShardMap("stream", fn, sleep_fn=fake_sleep, min_backoff=0.1) as sm:
        await sm._spawn_refresh()
        # Give the refresh a chance to fail and call sleep.
        for _ in range(20):
            if sleeps:
                break
            await anyio.sleep(0.01)
        assert sm.state is ShardMapState.UPDATING
        assert len(sleeps) == 1
        stop.set()


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
    stop = anyio.Event()

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        await stop.wait()

    async with ShardMap("stream", fn, sleep_fn=fake_sleep) as sm:
        await sm._spawn_refresh()
        for _ in range(20):
            if sleeps:
                break
            await anyio.sleep(0.01)
        assert sm.state is ShardMapState.UPDATING
        assert len(sleeps) == 1
        stop.set()


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
    stop = anyio.Event()

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)
        await stop.wait()

    async with ShardMap("stream", fn, sleep_fn=fake_sleep) as sm:
        await sm._spawn_refresh()
        for _ in range(20):
            if sleeps:
                break
            await anyio.sleep(0.01)
        assert sm.state is ShardMapState.UPDATING
        stop.set()


# ─── Cleanup scheduling ────────────────────────────────────────────────────


async def test_cleanup_task_fires():
    p1 = {"Shards": _even_split(2)}
    p2 = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}
    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        state["i"] = i + 1
        return [p1, p2][i]

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock, closed_shard_ttl=0.01) as sm:
        await sm.start()
        await sm.invalidate(seen_at=2000.0, predicted_shard=None)
        await sm._refresh_done.wait()
        # Advance the fake clock so cleanup sees the TTL as expired.
        clock.advance(1.0)
        # Wait long enough for the scheduled cleanup task to fire.
        await anyio.sleep(0.1)
        assert sm.hashrange(0) is None
        assert sm.hashrange(1) is None
        assert sm.hashrange(2) is not None


async def test_start_after_close_no_task_to_await():
    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("never")

    stop = anyio.Event()

    async def fake_sleep(d: float) -> None:
        await stop.wait()

    async with ShardMap("stream", fn, sleep_fn=fake_sleep) as sm:
        await sm._spawn_refresh()
        stop.set()
        await sm.aclose()
        # State is UPDATING; start() is now a no-op because UPDATING + the
        # already-set refresh_done event short-circuit cleanly.
        await sm.start()


async def test_refresh_loop_exits_when_closed_during_sleep():
    sm_holder: dict[str, ShardMap] = {}

    async def fn(**kwargs: object) -> dict:
        raise RuntimeError("boom")

    async def fake_sleep(d: float) -> None:
        sm_holder["sm"]._closed = True

    sm = ShardMap("stream", fn, sleep_fn=fake_sleep, min_backoff=0.01)
    sm_holder["sm"] = sm
    async with sm:
        await sm._spawn_refresh()
        await sm._refresh_done.wait()
        assert sm._closed


async def test_install_snapshot_drops_already_expired_closed_shard():
    p1 = {"Shards": _even_split(2)}
    p2 = {"Shards": [_shard_dict(2, 0, _UINT128_MAX)]}
    state = {"i": 0}

    async def fn(**kwargs: object) -> dict:
        i = state["i"]
        state["i"] = i + 1
        return [p1, p2][i]

    clock = _Clock()
    async with ShardMap("stream", fn, clock=clock, closed_shard_ttl=5.0) as sm:
        await sm.start()
        sm._closed_at[0] = -1000.0
        sm._closed_at[1] = -1000.0
        await sm.invalidate(seen_at=2000.0, predicted_shard=None)
        await sm._refresh_done.wait()
        assert sm.hashrange(0) is None
        assert sm.hashrange(1) is None
        assert sm.hashrange(2) is not None


async def test_schedule_cleanup_skips_when_closed():
    async with ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}])) as sm:
        await sm.start()
        await sm.aclose()
        sm._schedule_cleanup()
        assert sm._cleanup_scope is None


async def test_spawn_refresh_without_context_raises():
    sm = ShardMap("stream", make_list_shards_fn([{"Shards": _even_split(1)}]))
    with pytest.raises(RuntimeError, match="async context manager"):
        await sm._spawn_refresh()


def test_shard_dataclass_is_frozen():
    s = Shard(shard_id=0, raw_shard_id="shardId-0", starting_hash_key=0, ending_hash_key=1)
    attr = "shard_id"
    with pytest.raises(AttributeError):
        setattr(s, attr, 5)
