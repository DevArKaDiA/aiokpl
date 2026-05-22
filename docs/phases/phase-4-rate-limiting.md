# Phase 4 — Rate limiting (Limiter + TokenBucket)

**Status:** Done.

Phase 4 sits between Aggregator and Collector. It is the stage that
turns the per-shard accounting we worked so hard to set up in Phase 3
into actual back-pressure against Kinesis's hard rate limits — without
ever blocking the upstream task.

## Why two pieces

[`TokenBucket`](../reference/aiokpl/token_bucket.md) is the math.
[`Limiter`](../reference/aiokpl/limiter.md) is the queueing and the
expiration policy. Keeping them separate means the token bucket has no
notion of "batch" or "shard" or "expire" — it is a small, tested,
sync-callable primitive — and the Limiter contains no time arithmetic.

## TokenBucket

A multi-stream token bucket with **growth-on-query** semantics: every
query advances `tokens` by `rate * (now - last)` capped at `max_tokens`.
There is no background refill task, no `time.sleep`, no clock interrupt
— pure math, callable from sync or async code alike.

The atomicity contract on `try_take([n0, n1, ...])` matches the C++
`can_take + take` pair: either every stream is debited, or none. This
is what makes the Limiter's "records *and* bytes within the same
envelope" guarantee possible — a batch is admitted only when both
streams have the tokens it needs.

```python
bucket = TokenBucket([(1_000.0, 1_000.0), (1_048_576.0, 1_048_576.0)])
if bucket.try_take([1, batch.size]):
    # admitted
    ...
```

Cross-reference: `aws/utils/token_bucket.h` in the C++ KPL.

## ShardLimiter

[`ShardLimiter`](../reference/aiokpl/limiter.md#aiokpl.limiter.ShardLimiter)
is the per-shard half of the picture. It owns:

- One `TokenBucket` configured with both stream limits.
- A `sortedcontainers.SortedKeyList` of pending batches, keyed by
  deadline — the Python answer to the C++ KPL's
  `TimeSensitiveQueue<KinesisRecord>` (a boost `multi_index_container`
  ordered by deadline).

`drain()` runs in two phases:

1. **Expired first.** Walk the queue, surface every batch whose
   `expires_at <= now` as expired *without consuming any tokens*. This
   mirrors `internal_queue_.consume_expired` running before
   `consume_by_deadline` in the C++ Limiter — expired records cannot be
   silently dropped just because tokens happen to be available.
2. **Admit while tokens last.** Iterate the queue in deadline order,
   pop and admit as long as `bucket.try_take([1, batch.size])` succeeds.
   Stop on the first failure — keeping deadline order across shards.

Note the `1`: an admitted *aggregated* batch costs one record-token
regardless of how many user records it carries. This matches the C++
`token_bucket_.try_take({1, bytes})` call in `limiter.h` and is
correct because the wire-level Kinesis record is one record from
Kinesis's accounting perspective, no matter how many user records the
aggregation framing combines into it.

## Limiter (orchestrator)

The top-level [`Limiter`](../reference/aiokpl/limiter.md#aiokpl.limiter.Limiter)
owns one `ShardLimiter` per predicted shard (plus a catch-all for
`None`-shard batches), and drives a single background task spawned in
its `anyio.TaskGroup`:

- The drain task polls every **25 ms** (the C++ `kDrainDelayMillis`
  constant), calling `drain()` on every shard limiter and dispatching
  the results.
- Every `put()` also opportunistically drains its own shard so a record
  arriving with tokens already available doesn't sit through a full
  tick of latency.
- `flush()` calls `drain_force()` — admits everything not yet expired,
  bypassing tokens — used during graceful shutdown.

## The two stream limits

The Kinesis service contract is:

- **1 000 records/second per shard.**
- **1 MiB/second per shard.**

These are the values the C++ KPL pins, and the values aiokpl pins by
default:

```python
RECORDS_PER_SEC_PER_SHARD = 1_000.0
BYTES_PER_SEC_PER_SHARD = 1_048_576.0   # 1 MiB
```

They come from the published Kinesis Data Streams quotas and have not
changed since the service launched. They are hard service-side caps —
exceeding them triggers `ProvisionedThroughputExceededException`. The
Limiter's job is to **never exceed them on the wire**, which is what
makes the Retrier's "throttle vs transient" classification meaningful
(a real throttle is rare, an indicator of write skew or a misconfigured
shard count, not a self-inflicted firehose).

## Expired-record path

A pending batch carries an `expires_at: float` set at enqueue time as
`clock() + expiration` (default `30_000 ms`, mirrors
`record_ttl_ms`). When `drain()` sees `expires_at <= now`, the batch
surfaces via `on_expired(batch, "Expired")` rather than `on_admit`.

The Producer wires `on_expired` to a synthetic `SendOutcome` with
`request_error=("Expired", reason)` and routes it through the same
`Retrier.handle` code path as a network error. That keeps the
classification table single-sourced: every record's terminal
`RecordResult` and `Attempt` history is assembled the same way whether
the failure came from the wire or from the rate limiter.

## Overriding limits via Config

The default service caps are baked in but adjustable per
`Config`-driven Limiter when you have a reason (lower caps for
controlled load tests, higher caps when AWS has granted you a shard
quota increase):

```python
from aiokpl import Config

cfg = Config(
    region="us-east-1",
    # ... usual knobs ...
    rate_limit_records_per_sec_per_shard=800.0,   # 80% of service cap
    rate_limit_bytes_per_sec_per_shard=900_000.0, # ~858 KiB/s
)
```

Both knobs flow into `Limiter` at pipeline construction time and into
every per-shard `TokenBucket` from there. There is no global rate limit
— **the shard is the unit of optimization, not the stream.**

## Testing surface

- **TokenBucket.** Multi-stream atomicity (one stream short ⇒ no debit
  on any), growth-on-query math, growth saturation at `max_tokens`,
  clock-resolution edge (growth committed only when strictly positive).
- **ShardLimiter.** Expired before admitted ordering, deadline ordering
  on admission, `drain_force()` shape, queue rebuild only when
  expirations actually happened (cheap path stays cheap).
- **Limiter.** Background drain cadence, opportunistic drain on `put`,
  lazy shard creation, `aclose()` idempotence and `_drain_scope`
  cancellation.

Every test runs on both `asyncio` and `trio` via the parametrised
`anyio_backend` fixture. Phase 4 is still entirely backend-agnostic —
the asyncio-only edge of `aiobotocore` does not enter until Phase 5.
