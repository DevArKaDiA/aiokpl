# Philosophy

Six principles drove the design. Every PR is judged against them. If
something here contradicts a clever optimization you want to ship, the
principle wins.

## 1. The shard is the unit of optimization, not the stream

Kinesis enforces its hard limits — 1000 records/s and 1 MiB/s — **per
shard**, not per stream. A producer that aggregates and rate-limits at the
stream level is leaving capacity on the floor for streams with many shards
and silently dropping records for streams with hot keys.

Every batch, every token bucket, every queue in `aiokpl` is keyed by the
**predicted destination shard**. Anything that serializes work across shards
is a bug.

!!! info "Implication"
    The aggregator does not produce one `AggregatedRecord` per call. It
    produces one per predicted shard, in parallel, each on its own deadline.

## 2. Predict before asking

The shard a record will land on is **deterministic** given the partition
key (or explicit hash key) and the current shard map. There is no need to
make an RPC to find out.

```python
hash_key = int.from_bytes(md5(partition_key.encode()).digest(), "big")
shard_id = shard_map.predict(hash_key)  # O(log N), zero network
```

The C++ KPL caches the shard map and refreshes it lazily on signal from the
retrier. `aiokpl` does the same.

!!! info "Implication"
    The shard map has a state machine — `INVALID → UPDATING → READY` — and
    the rest of the pipeline gracefully degrades to single-record-mode when
    it is not READY. Aggregation is an optimization, not a precondition.

## 3. Batching is governed by deadlines, not sizes

Sizes (500 records, 5 MiB, 1 MiB-per-shard) are **transport limits** — they
are what Kinesis will reject. They are not the user contract.

The user contract is *"my record either lands or fails within
`record_ttl_ms`, and I want to know which"*. That is a **time** contract.
Every record carries a deadline; every stage flushes on the earliest one;
size limits exist only to avoid an outright `PutRecords` rejection.

!!! info "Implication"
    The reducer fires on a deadline-driven task spawned in an `anyio.TaskGroup`,
    not on a size threshold. Size is a short-circuit, not a trigger.
    Structured concurrency is enforced everywhere: every background task is
    owned by a parent task group and lives only as long as its component.

## 4. Each stage has one responsibility and one downstream callback

The pipeline is wiring. Stages do not know each other.

- The aggregator knows how to turn `UserRecord`s into `AggregatedRecord`s.
  It does not know about rate limits, batches, or the SDK.
- The limiter knows how to throttle a single shard. It does not know how
  records were aggregated.
- The collector knows how to group `AggregatedRecord`s into `PutRecords`
  batches. It does not know how rate limiting worked.

Each stage exposes one async method (`add` / `enqueue` / `submit`) and one
callback (`on_full` / `on_deadline` / `on_finished`). Composition is
external.

!!! info "Implication"
    Testing each stage in isolation is straightforward. Plug in a fake
    downstream callback and assert outputs.

## 5. Failures are data

Inside the pipeline, an error is not raised. It is appended to the record's
`attempts: list[Attempt]`. The retrier reads the list, classifies the latest
outcome — `throttle` vs `transient` vs `wrong-shard` vs `expired` — and
decides to retry or to terminate.

Only at the final stage (`finish_user_record`) does the outcome cross the
async boundary back to the user: as a `RecordResult` (success or failure)
carrying the full attempt history.

```python
result = await fut
if not result.success:
    for attempt in result.attempts:
        log.warning("attempt failed", code=attempt.error_code, shard=attempt.shard_id)
```

!!! info "Implication"
    Users never lose visibility into retries. The history is always there,
    even on success.

## 6. Bounded latency beats maximum throughput

Records expire (`record_ttl_ms`). Buffers are bounded
(`max_outstanding_records`). Backpressure is a **feature**, not a failure
mode.

A producer that maximises throughput at the cost of unbounded latency is
useless in any system that has SLOs. `aiokpl` chooses to fail records that
cannot be sent within their TTL, and to backpressure callers when the
in-flight set is full, rather than to drop on the floor or to queue
forever.

!!! info "Implication"
    `put_record` may `await` on the backpressure semaphore. This is
    intentional. Use the future result to learn about terminal outcomes;
    use the semaphore to learn about admission.
