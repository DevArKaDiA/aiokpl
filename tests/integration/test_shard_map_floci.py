"""Floci-backed integration tests for :class:`aiokpl.shard_map.ShardMap`.

Unit tests exercise the shard map with a fake ``list_shards_fn``. Those tests
only prove the map is internally consistent with our parsing. The tests in this
module aim to prove a stronger property: against a real Kinesis-compatible
service (Floci), ``ShardMap.predict()`` agrees byte-exactly with the
``ShardId`` the service itself returns from ``put_record``. This validates the
shard-id regex, the hash-range parsing, the bisect algorithm, and the
paginated ``ListShards`` handling end-to-end.

``ShardMap`` requires an *async* ``list_shards_fn``; the conftest's Kinesis
client is sync (botocore, not aiobotocore). We wrap each call with
``asyncio.to_thread`` so the test harness stays sync-only and we don't have to
pull aiobotocore into the test surface.

A Floci caveat: as of 2026-05, Floci reports every shard as covering the full
``[0, 2**128 - 1]`` hash range — disjoint partitioning is not modelled — and
routes ``put_record`` round-robin across shard ids regardless of partition key.
Byte-exact routing equivalence with our hash-based predictor is therefore not
testable against this emulator; the relevant tests detect this at runtime and
skip with a clear reason rather than xfail on a moving target. The tests still
exercise ShardMap's parsing, pagination, refresh, and ``hashrange()`` against
the real service.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import re
import time
from typing import Any
from uuid import uuid4

import pytest

from aiokpl.hashing import md5_hash_key
from aiokpl.shard_map import ShardMap, ShardMapState

_SHARD_ID_RE = re.compile(r"^shardId-(\d+)$")


def _parse_shard_id(raw: str) -> int:
    m = _SHARD_ID_RE.match(raw)
    assert m is not None, f"unexpected shard id format: {raw!r}"
    return int(m.group(1))


def _wait_stream_active(client: Any, stream_name: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_stream(StreamName=stream_name)
        if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise TimeoutError(f"stream {stream_name} did not become ACTIVE in {timeout}s")


def _make_async_adapter(client: Any):
    async def list_shards_fn(**kwargs: Any) -> dict[str, Any]:
        return await asyncio.to_thread(client.list_shards, **kwargs)

    return list_shards_fn


def _has_disjoint_hash_ranges(shards: list[dict[str, Any]]) -> bool:
    """Whether the service reports proper disjoint hash partitioning.

    Floci reports every shard with the full ``[0, 2**128 - 1]`` range, so it
    routes round-robin instead of by hash. Real Kinesis assigns disjoint
    contiguous ranges. We detect the degenerate case so the byte-exact routing
    assertion can be skipped with a clear reason on emulators.
    """
    if len(shards) <= 1:
        return True
    ranges = sorted(
        (int(s["HashKeyRange"]["StartingHashKey"]), int(s["HashKeyRange"]["EndingHashKey"]))
        for s in shards
    )
    prev_end = -1
    for start, end in ranges:
        if start <= prev_end or start > end:
            return False
        prev_end = end
    return True


@pytest.mark.integration
async def test_shardmap_predicts_actual_routing(kinesis_client: Any) -> None:
    stream_name = f"aiokpl-sm-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=4)
    try:
        await asyncio.to_thread(_wait_stream_active, kinesis_client, stream_name)

        truth = await asyncio.to_thread(kinesis_client.list_shards, StreamName=stream_name)
        disjoint = _has_disjoint_hash_ranges(truth["Shards"])
        truth_ids = {_parse_shard_id(s["ShardId"]) for s in truth["Shards"]}

        list_shards_fn = _make_async_adapter(kinesis_client)
        sm = ShardMap(stream_name, list_shards_fn)
        try:
            await sm.start()
            assert sm.state is ShardMapState.READY
            # Even on a backend that doesn't honor hash-based routing, the
            # ShardMap must learn every shard id the service reports and the
            # range it reports for them.
            for s in truth["Shards"]:
                sid = _parse_shard_id(s["ShardId"])
                start = int(s["HashKeyRange"]["StartingHashKey"])
                end = int(s["HashKeyRange"]["EndingHashKey"])
                assert sm.hashrange(sid) == (start, end), (
                    f"hashrange({sid}) mismatch vs service truth"
                )

            if not disjoint:
                pytest.skip(
                    "emulator reports overlapping/full-range shards and routes "
                    "round-robin; hash-based predict cannot agree with put_record "
                    "routing on this backend"
                )

            rng = random.Random(0xA10CB1)
            mismatches: list[tuple[str, int, int]] = []
            for _ in range(50):
                pk = f"pk-{rng.randrange(1 << 60):x}-{rng.randrange(1 << 60):x}"
                hk = md5_hash_key(pk)
                predicted = sm.predict(hk)
                assert predicted in truth_ids
                response = await asyncio.to_thread(
                    kinesis_client.put_record,
                    StreamName=stream_name,
                    Data=b"x",
                    PartitionKey=pk,
                )
                actual = _parse_shard_id(response["ShardId"])
                if predicted != actual:
                    mismatches.append((pk, predicted if predicted is not None else -1, actual))
            assert not mismatches, f"predict() disagreed with Kinesis on: {mismatches[:5]}"
        finally:
            await sm.aclose()
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)


@pytest.mark.integration
async def test_shardmap_handles_split_via_invalidate(kinesis_client: Any) -> None:
    stream_name = f"aiokpl-sm-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=2)
    try:
        await asyncio.to_thread(_wait_stream_active, kinesis_client, stream_name)

        list_shards_fn = _make_async_adapter(kinesis_client)
        sm = ShardMap(stream_name, list_shards_fn)
        try:
            await sm.start()
            assert sm.state is ShardMapState.READY
            initial_updated_at = sm.updated_at
            assert initial_updated_at is not None

            initial = await asyncio.to_thread(kinesis_client.list_shards, StreamName=stream_name)
            initial_shards = initial["Shards"]
            assert len(initial_shards) == 2

            target = initial_shards[0]
            target_id = target["ShardId"]
            start = int(target["HashKeyRange"]["StartingHashKey"])
            end = int(target["HashKeyRange"]["EndingHashKey"])
            mid = (start + end) // 2

            try:
                await asyncio.to_thread(
                    kinesis_client.split_shard,
                    StreamName=stream_name,
                    ShardToSplit=target_id,
                    NewStartingHashKey=str(mid),
                )
            except Exception as exc:  # pragma: no cover - emulator-dependent
                pytest.xfail(f"floci split_shard not supported / failed: {exc!r}")

            await asyncio.to_thread(_wait_stream_active, kinesis_client, stream_name, 60.0)

            post_split = await asyncio.to_thread(kinesis_client.list_shards, StreamName=stream_name)
            new_child = None
            for s in post_split["Shards"]:
                hkr = s["HashKeyRange"]
                if int(hkr["StartingHashKey"]) == mid and s.get("ParentShardId") == target_id:
                    new_child = s
                    break
            if new_child is None:
                pytest.xfail(
                    "floci did not produce a child shard with the requested "
                    f"NewStartingHashKey={mid} for parent {target_id}; "
                    f"post-split shards: {post_split['Shards']!r}"
                )
            new_child_id = _parse_shard_id(new_child["ShardId"])
            new_child_start = int(new_child["HashKeyRange"]["StartingHashKey"])
            new_child_end = int(new_child["HashKeyRange"]["EndingHashKey"])

            await asyncio.sleep(0.01)
            await sm.invalidate(seen_at=time.monotonic(), predicted_shard=None)

            refresh_deadline = time.monotonic() + 30.0
            refreshed = False
            while time.monotonic() < refresh_deadline:
                if (
                    sm.state is ShardMapState.READY
                    and sm.updated_at is not None
                    and sm.updated_at > initial_updated_at
                ):
                    refreshed = True
                    break
                await asyncio.sleep(0.05)
            if not refreshed:
                pytest.fail("ShardMap did not refresh after invalidate()")

            # Floci's ListShards may still report identical full-range shards
            # post-split. If so we can only assert the refresh happened and that
            # the new child is in the map; predict() vs routing equivalence is
            # not testable on this backend.
            if not _has_disjoint_hash_ranges(post_split["Shards"]):
                assert sm.hashrange(new_child_id) is not None
                pytest.skip(
                    "emulator reports overlapping shard ranges after split_shard; "
                    "byte-exact routing equivalence is not testable here"
                )

            probe_pk = None
            for i in range(10_000):
                candidate = f"probe-{i}"
                hk = md5_hash_key(candidate)
                if new_child_start <= hk <= new_child_end and sm.predict(hk) == new_child_id:
                    probe_pk = candidate
                    break
            assert probe_pk is not None, (
                f"could not find a partition key whose hash lands in child "
                f"{new_child_id} range [{new_child_start}, {new_child_end}]"
            )
            response = await asyncio.to_thread(
                kinesis_client.put_record,
                StreamName=stream_name,
                Data=b"x",
                PartitionKey=probe_pk,
            )
            assert _parse_shard_id(response["ShardId"]) == new_child_id
        finally:
            await sm.aclose()
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)


@pytest.mark.integration
async def test_shardmap_pagination(kinesis_client: Any) -> None:
    stream_name = f"aiokpl-sm-it-{uuid4().hex[:8]}"
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=12)
    try:
        await asyncio.to_thread(_wait_stream_active, kinesis_client, stream_name)

        list_shards_fn = _make_async_adapter(kinesis_client)
        sm = ShardMap(stream_name, list_shards_fn, max_results_per_page=5)
        try:
            await sm.start()
            assert sm.state is ShardMapState.READY

            truth = await asyncio.to_thread(kinesis_client.list_shards, StreamName=stream_name)
            truth_shards = truth["Shards"]
            assert len(truth_shards) == 12

            # Detect whether the backend actually paginates. Floci truncates
            # to MaxResults and returns no NextToken, which means our paginated
            # path can only ever see the first page. We exercise the page-1
            # behaviour here but skip the cross-page assertions if the backend
            # doesn't honor pagination.
            probe = await asyncio.to_thread(
                kinesis_client.list_shards,
                StreamName=stream_name,
                MaxResults=5,
                ShardFilter={"Type": "AT_LATEST"},
            )
            paginates = bool(probe.get("NextToken"))

            disjoint = _has_disjoint_hash_ranges(truth_shards)
            expected_known = 12 if paginates else min(12, len(probe["Shards"]))
            known = 0
            for s in truth_shards:
                shard_id = _parse_shard_id(s["ShardId"])
                start = int(s["HashKeyRange"]["StartingHashKey"])
                end = int(s["HashKeyRange"]["EndingHashKey"])
                got = sm.hashrange(shard_id)
                if got is None:
                    continue
                known += 1
                assert got == (start, end), (
                    f"hashrange({shard_id}) = {got}, expected {(start, end)}"
                )
                if disjoint:
                    assert sm.predict(start) == shard_id
                    assert sm.predict(end) == shard_id
            assert known == expected_known, (
                f"expected ShardMap to know {expected_known} shards, knows {known}"
            )
            if not paginates:
                pytest.skip(
                    "emulator does not paginate ListShards (truncates to MaxResults "
                    "and returns no NextToken); cross-page behaviour not testable"
                )
        finally:
            await sm.aclose()
    finally:
        with contextlib.suppress(Exception):
            kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
