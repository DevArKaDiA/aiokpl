# Phase 2 — ShardMap

**Status:** Done.

## What ships

[`aiokpl.shard_map`](../reference/aiokpl/shard_map.md) — a cached,
async-refreshed shard list with O(log N) shard prediction.

## State machine

```
INVALID  ─── start() ───►  UPDATING  ─── ListShards OK ───►  READY
   ▲                          │                                │
   │                          └── refresh failed ──┐           │
   │                                                ▼           │
   └────────── invalidate(seen_at, predicted) ◄────┴───────────┘
```

- **INVALID.** No snapshot. `predict()` returns `None`. The aggregator
  falls back to single-record mode so the pipeline still flows.
- **UPDATING.** A background refresh task is in flight against
  `ListShards` with `ShardFilter=AT_LATEST`. Pagination is handled
  transparently. On transient error, retries with exponential backoff
  (1s → 30s).
- **READY.** A snapshot is installed and `predict()` answers in O(log N).

## Predict and bisect

The snapshot is a pair of parallel tuples — `endings: (uint128, …)` and
`shard_ids: (int, …)` — kept sorted by ending hash key.

```python
idx = bisect.bisect_left(snap.endings, hash_key)
return snap.shard_ids[idx]
```

`bisect_left` is correct because Kinesis hash ranges are closed and
contiguous: a record with hash key `h` belongs to the shard whose
`EndingHashKey` is the smallest one `>= h`.

## Invalidate semantics

`invalidate(seen_at, predicted_shard)` is the C++-KPL contract:

- If `seen_at <= updated_at`, ignore — the divergence has already been
  observed by a more recent refresh.
- If `predicted_shard` is already absent from the snapshot, ignore — a
  refresh will not teach us anything new.
- Otherwise, trigger a refresh (idempotent: if one is in flight, do
  nothing).

This guard is what makes the retrier safe to call `invalidate()`
liberally without thundering-herd refreshes.

## Closed-shard TTL

When a refresh produces a new snapshot, shards that **disappeared** are
not deleted immediately — they are kept in the snapshot for
`closed_shard_ttl` (default 60 s) so that `hashrange()` still answers for
records that were already in flight when the split landed. After the TTL
expires, a sleeping task spawned in the `ShardMap`'s `anyio.TaskGroup`
purges them.

## Transport-agnostic injection

The constructor takes a `list_shards_fn: Callable[..., Awaitable[dict]]`
parameter. In production this is bound to an `aiobotocore` Kinesis client.
In tests it is a hand-written async function returning canned `ListShards`
responses. The `ShardMap` itself never imports `aiobotocore`.

```python
async with ShardMap(
    stream_name="my-stream",
    list_shards_fn=client.list_shards,  # or a fake in tests
    closed_shard_ttl=60.0,
) as shard_map:
    await shard_map.start()
    shard_id = shard_map.predict(md5_hash_key("user-123"))
```

This is also why the integration tests can target `etspaceman/kinesis-mock`
without modifying the `ShardMap` — `aiobotocore` is just configured to
point at the mock's endpoint.
