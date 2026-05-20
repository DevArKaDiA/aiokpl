"""Cached, async-refreshed Kinesis shard map with hash-key prediction.

Predict the destination shard for a record locally from a cached snapshot of
``ListShards``, rather than asking Kinesis every time. This mirrors the C++ KPL
shard map (``aws/kinesis/core/shard_map.{h,cc}``): a state machine
``INVALID → UPDATING → READY``, paginated refresh against ``ListShards`` with
``ShardFilter=AT_LATEST``, ``bisect_left`` on the sorted list of ending hash
keys, and a closed-shard TTL so that ``hashrange()`` still answers for shards
that have just been split.

Transport-agnostic on purpose: the constructor takes an injected
``list_shards_fn`` async callable that returns a Kinesis ``ListShards`` response
dict. Wiring to ``aiobotocore`` happens in the Sender/Producer phases. Tests
inject a fake callable.

Lifecycle is structured: enter the :class:`ShardMap` as an async context
manager, which owns an :class:`anyio.abc.TaskGroup` for the background refresh
and closed-shard-cleanup tasks. ``aclose()`` stays as a backward-compatible
alias for context exit.
"""

from __future__ import annotations

import bisect
import enum
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType

import anyio
from anyio.abc import TaskGroup

logger = logging.getLogger("aiokpl.shard_map")

ListShardsFn = Callable[..., Awaitable[dict]]
SleepFn = Callable[[float], Awaitable[None]]

_UINT128_MAX = (1 << 128) - 1
_SHARD_ID_RE = re.compile(r"^shardId-(\d+)$")


class ShardMapState(enum.Enum):
    """Lifecycle of the cached shard list."""

    INVALID = "invalid"
    UPDATING = "updating"
    READY = "ready"


@dataclass(slots=True, frozen=True)
class Shard:
    """A Kinesis shard reduced to the fields the producer cares about."""

    shard_id: int
    raw_shard_id: str
    starting_hash_key: int
    ending_hash_key: int


@dataclass(slots=True, frozen=True)
class _Snapshot:
    """Immutable view of the shard table for atomic rebinding."""

    endings: tuple[int, ...]
    shard_ids: tuple[int, ...]
    shards: dict[int, Shard]


def _parse_shard_id(raw: str) -> int:
    m = _SHARD_ID_RE.match(raw)
    if not m:
        raise ValueError(f"malformed shard id: {raw!r}")
    return int(m.group(1))


def _parse_hash_key(s: str) -> int:
    # Decimal strings from the Kinesis API. Reject anything that wouldn't
    # round-trip as a canonical uint128.
    if not s or not s.isascii() or not all("0" <= c <= "9" for c in s):
        raise ValueError(f"not a canonical decimal uint128: {s!r}")
    value = int(s)
    if value > _UINT128_MAX:
        raise ValueError(f"hash key out of uint128 range: {s!r}")
    return value


class ShardMap:
    """Cached, async-refreshed shard list with O(log N) shard prediction."""

    def __init__(
        self,
        stream_name: str,
        list_shards_fn: ListShardsFn,
        *,
        stream_arn: str | None = None,
        min_backoff: float = 1.0,
        max_backoff: float = 30.0,
        closed_shard_ttl: float = 60.0,
        max_results_per_page: int = 1000,
        clock: Callable[[], float] = time.monotonic,
        sleep_fn: SleepFn = anyio.sleep,
    ) -> None:
        self._stream_name = stream_name
        self._stream_arn = stream_arn
        self._list_shards_fn = list_shards_fn
        self._min_backoff = min_backoff
        self._max_backoff = max_backoff
        self._closed_shard_ttl = closed_shard_ttl
        self._max_results_per_page = max_results_per_page
        self._clock = clock
        self._sleep_fn = sleep_fn

        self._state = ShardMapState.INVALID
        self._snapshot = _Snapshot(endings=(), shard_ids=(), shards={})
        self._closed_at: dict[int, float] = {}
        self._updated_at: float | None = None
        self._backoff = min_backoff
        self._lock = anyio.Lock()
        self._refresh_done = anyio.Event()
        self._refresh_in_flight = False
        self._cleanup_scope: anyio.CancelScope | None = None
        self._refresh_scope: anyio.CancelScope | None = None
        self._closed = False
        self._tg: TaskGroup | None = None

    # ─── Public read-only views ────────────────────────────────────────────

    @property
    def state(self) -> ShardMapState:
        return self._state

    @property
    def updated_at(self) -> float | None:
        return self._updated_at

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> ShardMap:
        tg = anyio.create_task_group()
        await tg.__aenter__()
        self._tg = tg
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
        tg = self._tg
        self._tg = None
        assert tg is not None
        await tg.__aexit__(exc_type, exc, tb)

    async def start(self) -> None:
        """Trigger an initial refresh. Returns once READY or refresh failed."""
        await self._spawn_refresh()
        # Wait for the in-flight refresh to finish (READY or cancelled).
        if self._refresh_in_flight:
            await self._refresh_done.wait()

    async def aclose(self) -> None:
        """Cancel any background refresh / cleanup task. Idempotent.

        Setting ``_closed`` plus cancelling the per-task cancel scope of the
        cleanup task is enough; the refresh loop observes ``_closed`` on its
        next iteration. We deliberately do NOT cancel ``self._tg``'s scope
        because that would also cancel the caller, which holds the outer
        ``async with``.
        """
        self._closed = True
        if self._cleanup_scope is not None:
            self._cleanup_scope.cancel()
            self._cleanup_scope = None
        # Cancel an in-flight refresh task via the dedicated scope, if any.
        if self._refresh_scope is not None:
            self._refresh_scope.cancel()
            self._refresh_scope = None
        # Unblock any awaiter on start().
        if not self._refresh_done.is_set():
            self._refresh_done.set()

    # ─── Prediction ────────────────────────────────────────────────────────

    def predict(self, hash_key: int) -> int | None:
        """Return the predicted shard id for ``hash_key`` (uint128), or ``None``.

        Returns ``None`` if the map is not READY, or if ``hash_key`` falls past
        the last known ending — the latter indicates a stale shard map and the
        caller is expected to ``invalidate()``.
        """
        if self._state is not ShardMapState.READY:
            return None
        snap = self._snapshot
        idx = bisect.bisect_left(snap.endings, hash_key)
        if idx == len(snap.endings):
            logger.error(
                "could not map hash key to shard id for stream %r; hash_key=%d",
                self._stream_name,
                hash_key,
            )
            return None
        return snap.shard_ids[idx]

    def hashrange(self, shard_id: int) -> tuple[int, int] | None:
        """Inclusive ``(start, end)`` hash range for ``shard_id``, or ``None``."""
        shard = self._snapshot.shards.get(shard_id)
        if shard is None:
            return None
        return (shard.starting_hash_key, shard.ending_hash_key)

    # ─── Invalidation ──────────────────────────────────────────────────────

    async def invalidate(
        self,
        seen_at: float,
        predicted_shard: int | None,
    ) -> None:
        """Mark the map stale and trigger a refresh, with C++-KPL semantics.

        The guard ``seen_at > updated_at`` prevents redundant refreshes when
        the divergence was already addressed by an earlier refresh. We also
        skip if the predicted shard is known to be already closed — in that
        case the next refresh would learn nothing new.
        """
        if self._closed:
            return
        if self._updated_at is not None and seen_at <= self._updated_at:
            return
        if predicted_shard is not None and predicted_shard not in self._snapshot.shards:
            return
        await self._spawn_refresh()

    # ─── Internals ─────────────────────────────────────────────────────────

    async def _spawn_refresh(self) -> None:
        async with self._lock:
            if self._state is ShardMapState.UPDATING:
                return
            self._state = ShardMapState.UPDATING
            self._backoff = self._min_backoff
            self._refresh_done = anyio.Event()
            self._refresh_in_flight = True
            tg = self._tg
            if tg is None:
                raise RuntimeError("ShardMap must be used as an async context manager")
            scope = anyio.CancelScope()
            self._refresh_scope = scope
            tg.start_soon(self._refresh_loop, scope)

    async def _refresh_loop(self, scope: anyio.CancelScope) -> None:
        # Retry forever (until aclose) on transient failures with exponential
        # backoff. State stays UPDATING throughout — callers that want READY
        # await ``start()`` which awaits the first iteration only.
        with scope:
            try:
                while not self._closed:
                    try:
                        snapshot = await self._fetch_all_pages()
                    except Exception:
                        logger.warning(
                            "shard map refresh failed for stream %r; retrying in %.3fs",
                            self._stream_name,
                            self._backoff,
                            exc_info=True,
                        )
                        backoff = self._backoff
                        self._backoff = min(self._backoff * 2.0, self._max_backoff)
                        await self._sleep_fn(backoff)
                        continue
                    await self._install_snapshot(snapshot)
                    return
            finally:
                self._refresh_in_flight = False
                self._refresh_done.set()

    async def _fetch_all_pages(self) -> _Snapshot:
        endings: list[tuple[int, int]] = []
        shards: dict[int, Shard] = {}

        next_token: str | None = None
        while True:
            kwargs: dict[str, object] = {"MaxResults": self._max_results_per_page}
            if next_token is None:
                kwargs["StreamName"] = self._stream_name
                kwargs["ShardFilter"] = {"Type": "AT_LATEST"}
                if self._stream_arn is not None:
                    kwargs["StreamARN"] = self._stream_arn
            else:
                kwargs["NextToken"] = next_token
            response = await self._list_shards_fn(**kwargs)
            for raw in response.get("Shards", ()):
                shard = self._build_shard(raw)
                shards[shard.shard_id] = shard
                endings.append((shard.ending_hash_key, shard.shard_id))
            next_token = response.get("NextToken")
            if not next_token:
                break

        endings.sort()
        return _Snapshot(
            endings=tuple(e for e, _ in endings),
            shard_ids=tuple(sid for _, sid in endings),
            shards=shards,
        )

    @staticmethod
    def _build_shard(raw: dict) -> Shard:
        raw_id = raw["ShardId"]
        shard_id = _parse_shard_id(raw_id)
        hkr = raw["HashKeyRange"]
        start = _parse_hash_key(hkr["StartingHashKey"])
        end = _parse_hash_key(hkr["EndingHashKey"])
        if start > end:
            raise ValueError(f"starting hash key {start} > ending hash key {end} for {raw_id!r}")
        return Shard(
            shard_id=shard_id,
            raw_shard_id=raw_id,
            starting_hash_key=start,
            ending_hash_key=end,
        )

    async def _install_snapshot(self, snapshot: _Snapshot) -> None:
        async with self._lock:
            previous = self._snapshot
            now = self._clock()
            # Mark previously-open shards that disappeared as closed-at-now;
            # they remain answerable from hashrange() until the TTL elapses.
            new_ids = snapshot.shards
            merged_shards = dict(new_ids)
            for sid, shard in previous.shards.items():
                if sid not in new_ids:
                    self._closed_at.setdefault(sid, now)
                    if self._closed_at[sid] + self._closed_shard_ttl > now:
                        merged_shards[sid] = shard
                else:
                    self._closed_at.pop(sid, None)

            installed = _Snapshot(
                endings=snapshot.endings,
                shard_ids=snapshot.shard_ids,
                shards=merged_shards,
            )
            self._snapshot = installed
            self._state = ShardMapState.READY
            self._updated_at = now
            self._backoff = self._min_backoff
            logger.info(
                "updated shard map for %r with %d shards",
                self._stream_name,
                len(snapshot.shard_ids),
            )
            self._schedule_cleanup()

    def _schedule_cleanup(self) -> None:
        if self._closed:
            return
        if self._cleanup_scope is not None:
            self._cleanup_scope.cancel()
        tg = self._tg
        if tg is None:  # pragma: no cover - defensive; install requires entered context
            return
        scope = anyio.CancelScope()
        self._cleanup_scope = scope
        tg.start_soon(self._cleanup_task, scope, self._closed_shard_ttl)

    async def _cleanup_task(self, scope: anyio.CancelScope, delay: float) -> None:
        with scope:
            await anyio.sleep(delay)
            self._cleanup()

    def _cleanup(self) -> None:
        # Purge closed shards whose TTL has expired. Invoked from the spawned
        # cleanup task or directly from tests; safe to call repeatedly.
        self._cleanup_scope = None
        if self._closed or self._state is not ShardMapState.READY:
            return
        now = self._clock()
        expired = [
            sid
            for sid, closed_at in self._closed_at.items()
            if closed_at + self._closed_shard_ttl <= now
        ]
        if not expired:
            return
        snap = self._snapshot
        new_shards = {sid: s for sid, s in snap.shards.items() if sid not in expired}
        self._snapshot = _Snapshot(
            endings=snap.endings,
            shard_ids=snap.shard_ids,
            shards=new_shards,
        )
        for sid in expired:
            self._closed_at.pop(sid, None)
