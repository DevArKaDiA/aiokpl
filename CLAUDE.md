# aiokpl — implementation guide for Claude

Read this top to bottom before touching code. If something here contradicts an
old chat, this file wins.

---

## What this is

`aiokpl` is a **pure-Python async reimplementation of the Amazon Kinesis
Producer Library**. Equivalent in spirit and behavior to the C++ KPL
(`amazon-kinesis-producer`), but with no native binary, no IPC, no Protobuf
framing, no child process.

Reference C++ code: `/Users/juanro/Documents/githings/amazon-kinesis-producer/`.
A deep walkthrough of that codebase exists in the repo's git history of
conversations; if missing, re-read `aws/kinesis/core/*`, `aws/utils/*`,
`aws/metrics/*`, `aws/kinesis/protobuf/messages.proto`, and `aggregation-format.md`.

Companion skill: `~/.claude/skills/aiokpl/SKILL.md` — load it when working on
this repo.

---

## Philosophy (non-negotiable)

These six principles drove the design. Every PR is judged against them.

1. **The shard is the unit of optimization, not the stream.** Anything that
   serializes across shards is a bug.
2. **Predict before asking.** The shard for a record is deterministic from
   `md5(partition_key)` (or explicit hash key) + a cached shard map. No RPC.
3. **Batching is governed by deadlines, not sizes.** Sizes are transport
   limits; time is the user contract. Every record carries a deadline.
4. **Each stage has one responsibility and one downstream callback.** Pipeline
   is wiring, stages don't know each other.
5. **Failures are data.** Each record accumulates an attempt history. Errors
   are classified: throttle vs transient vs wrong-shard vs expired.
6. **Bounded latency > max throughput.** Records expire; buffers are bounded;
   backpressure is a feature.

---

## Architecture mapping (C++ → Python)

Same pipeline, idiomatic primitives.

```
UserRecord
  ↓ producer.put_record()
Aggregator    — groups UserRecords into AggregatedRecords by predicted shard
  ↓
Limiter       — per-shard token bucket (1000 rec/s + 1 MiB/s)
  ↓
Collector     — groups AggregatedRecords into PutRecords batches (500/5MiB/256KiB-per-shard)
  ↓
Sender        — aiobotocore.put_records, async
  ↓
Retrier       — classifies outcomes, retries or finishes
  ↓
finish_user_record → resolve user's asyncio.Future
```

Translation table:

| C++ KPL | aiokpl |
|---|---|
| `KinesisProducer` (root) | `Producer` class, `async with` lifecycle |
| `Pipeline` per stream | `_StreamPipeline` per stream, lazy-created in a dict |
| `Aggregator` + `Reducer<UR, KR>` | `Aggregator` with per-shard `_Batch` + deadline task in an anyio TaskGroup |
| `Limiter` + `TokenBucket` | `Limiter` with per-shard `TokenBucket` (records+bytes streams) |
| `Collector` + `Reducer<KR, PRR>` | `Collector` with deadline + 256 KiB/shard predicate |
| `ShardMap` (binary search) | `ShardMap` with `bisect_left` on `end_hash_key` |
| `Retrier::handle_put_records_result` | `Retrier.handle(outcome)` |
| `UserRecord::to_put_record_result` | `RecordResult` dataclass |
| `Attempt` | `Attempt` dataclass |
| `IoServiceExecutor` | `anyio.abc.TaskGroup` owned by each stage |
| `ConcurrentHashMap` (lazy factory) | `defaultdict` under an `anyio.Lock`, or `setdefault` |
| `TicketSpinLock`, `ConcurrentLinkedQueue` | `anyio.Lock` + memory-object streams |
| `TimeSensitiveQueue` (boost multi_index) | `sortedcontainers.SortedKeyList` on deadline |
| `TimeSensitive` mixin | `deadline: float` + `expiration: float` fields |
| IPC + Protobuf framing | **dropped** — we are in-process |
| `MutableStaticCredentialsProvider` | aiobotocore's credential refresh |
| Signal handlers, backtrace | **dropped** |
| Static linking, bootstrap.sh | **dropped** |

---

## Locked decisions (don't relitigate)

- **Async runtime is `anyio`.** Works on both `asyncio` and `trio` backends.
  Every stage (`Reducer`, `Aggregator`, `Collector`, `ShardMap`, `Limiter`)
  is an `async with` async context manager that owns an internal
  `anyio.abc.TaskGroup`. The test suite parametrizes the `anyio_backend`
  fixture across `asyncio` and `trio`, so every async test runs twice.
- **Protobuf codec is hand-rolled.** No `protobuf` dep, no `protoc` build step,
  no vendored `aws-kinesis-agg`. The KPL aggregation schema has 3 messages and
  ~7 fields total; wire format is varints + length-delimited. ~150 lines of
  encoder + decoder in `aiokpl/aggregation.py`. The schema is frozen by AWS;
  we will never regenerate.
- **Type checker is `ty`** (Astral, pre-release on PyPI). Not mypy. Not pyright.
- **Test runner is `pytest`**, runner+orchestrator is `nox` (`noxfile.py`).
- **AWS emulator for integration tests is `etspaceman/kinesis-mock`**
  (`ghcr.io/etspaceman/kinesis-mock:0.5.2`) — the same Scala backend LocalStack
  uses for Kinesis. Byte-exact compatible with AWS for hash-key routing,
  `ListShards` pagination, and `SplitShard` child ranges. We started on Floci
  but pivoted: Floci faked shard ranges and routed round-robin, making
  byte-exact prediction tests impossible. Conftest spawns the container via
  the `docker` SDK directly (no testcontainers wrapper). API is HTTPS on port
  4567 with a self-signed cert (`verify=False` in the test harness),
  healthcheck is plain HTTP on port 4568.
- **Coverage gate is 100%** (`fail_under = 100`). No exceptions, no excludes
  for "hard to test" — if you can't cover a branch, delete it.
- **Package manager is `uv`**.
- **Lint+format is `ruff`** with rules in `pyproject.toml`.
- **No runtime deps in Phase 1.** Pure stdlib + hashlib.

---

## Aggregation format

This is the **only** wire format that matters and we must produce it byte-exact
so KCL consumers deaggregate transparently.

```
[ \xF3\x89\x9A\xC2 | protobuf(AggregatedRecord) | MD5(protobuf)[16 bytes] ]
```

Schema (from `aws/kinesis/protobuf/messages.proto`):

```protobuf
message AggregatedRecord {
  repeated string partition_key_table = 1;
  repeated string explicit_hash_key_table = 2;
  repeated Record records = 3;
}
message Record {
  required uint64 partition_key_index = 1;
  optional uint64 explicit_hash_key_index = 2;
  required bytes data = 3;
  repeated Tag tags = 4;
}
```

Options for the implementation:

- **Preferred**: vendor `aws-kinesis-agg` (Apache-2.0, ~300 lines). Avoids a
  protoc step. The repo lives at <https://github.com/awslabs/kinesis-aggregation>.
- Alternative: pre-generate `messages_pb2.py` from the upstream `.proto` and
  check it in. We then build the message ourselves.

Single-record edge case: if the batch has exactly 1 record, **do not aggregate**
— send the raw bytes with the original partition key. This matches C++
`KinesisRecord::serialize`.

When aggregated, the API-level partition key is `"a"` and we set
`ExplicitHashKey` to a value in the predicted shard's range (the first record's
hash key works).

---

## Shard prediction

```python
hash_key = int.from_bytes(md5(partition_key.encode()).digest(), "big")
# or, if user supplied explicit_hash_key:
hash_key = int(explicit_hash_key)
```

Then `bisect_left` on a sorted list of `(end_hash_key, shard_id)`. O(log N).

`ShardMap` invariants:

- State machine: `INVALID → UPDATING → READY`. While not READY, aggregation
  falls back to single-record mode (one UR per KR).
- Refresh on `invalidate(seen_at, predicted_shard)` from the Retrier, with the
  guard `seen_at > updated_at` to avoid duplicate refreshes.
- Background refresh task uses exponential backoff (1s → 30s).
- Use `ListShards` paginated with `ShardFilter=AT_LATEST`.
- Closed shards purged after `closed_shard_ttl = 60s`.

---

## Retrier classification (the most important code in the library)

For each per-record result inside a `PutRecords` response:

| Outcome | Action |
|---|---|
| Success, `predicted == actual_shard` | finish(success) |
| Success, actual shard's hash range contains the record's hash key (child after split) | finish(success) + ShardMap.invalidate |
| Success, actual shard's hash range does NOT contain the hash key | retry_not_expired("Wrong Shard") + ShardMap.invalidate |
| Error `ProvisionedThroughputExceededException` + `fail_if_throttled=True` | fail |
| Error `ProvisionedThroughputExceededException` + `fail_if_throttled=False` | retry_not_expired |
| Other error | retry_not_expired |

Request-level failure (no per-record results): apply the throttle rule to every
KR in the batch.

`retry_not_expired`:
- Append `Attempt`.
- If `record.expired()` (`now > arrival + record_ttl`), `fail("Expired")`.
- Else: bump deadline by `record_max_buffered_time / 2` and re-enqueue at the
  aggregator (full loop, predicted shard may change).

`fail` and successful `finish`: resolve the user's `asyncio.Future` with a
`RecordResult` carrying full attempt history.

Count-mismatch sanity: if `len(response.Records) != len(batch)`, fail every
record in the batch with `"Record Count Mismatch"`. Don't try to be clever.

---

## Concurrency model

The producer runs on `anyio`, so the same code drives either the asyncio or
trio runtime. Public API is async. Sync users get a thin bridge
(`Producer.sync()`) when Phase 8 ships.

- Every stage is entered via `async with` and owns an internal
  `anyio.abc.TaskGroup`. Background tasks (timer, drain loop, refresh,
  cleanup) are spawned into that group and live exactly as long as the
  stage.
- All stage methods are `async def` and protected by `anyio.Lock` where
  shared state is touched. Per-task `anyio.CancelScope` instances cancel
  individual timers without taking down siblings.
- No background OS threads, except those spawned through
  `anyio.to_thread.run_sync` (used in integration tests to bridge sync
  boto3 calls).
- `aiobotocore` calls run as awaited tasks; their `done_callback` does **not**
  call the retrier directly — it enqueues an internal item the retrier task
  pulls from. (This mirrors the C++ note in `pipeline.h:206` about not
  hammering downstream from SDK callback threads.) Note that `aiobotocore`
  is asyncio-only, so the Sender/Retrier (Phase 5+) loses trio support,
  but Phase 4 and below remain backend-agnostic.

---

## Public API (target shape)

```python
import asyncio
from aiokpl import Producer, Config

async def main():
    cfg = Config(
        region="us-east-1",
        aggregation_enabled=True,
        record_max_buffered_time_ms=100,
        record_ttl_ms=30_000,
        fail_if_throttled=False,
    )
    async with Producer(cfg) as producer:
        fut = await producer.put_record(
            stream="my-stream",
            partition_key="user-123",
            data=b"hello",
        )
        result = await fut  # RecordResult(success, shard_id, sequence_number, attempts)
```

`put_record` returns an `Outcome[RecordResult]` — an anyio-friendly one-shot
event that replaces `asyncio.Future` so the resolution mechanism stays
backend-agnostic. Callers `await outcome.wait()`. `flush()` and graceful
`aclose()` drain in-flight records up to a deadline.

---

## Roadmap (phased)

The phasing is deliberate. Don't skip ahead — each phase ships something
testable.

### Phase 0 — scaffolding (this commit)

- Repo created, pyproject, README, this file.
- Empty `aiokpl/__init__.py`.
- No code yet.

### Phase 1 — aggregation codec

- `aiokpl/aggregation.py`: encode/decode of the KPL aggregated record format.
  Magic + protobuf + MD5. Includes dedup tables for partition keys and EHKs.
- `aiokpl/hashing.py`: `partition_key_to_hash(pk)` + `explicit_hash_to_int`.
- Tests: round-trip with `aws-kinesis-agg`'s output if installed; otherwise
  golden bytes from the C++ KPL's serialized records.

### Phase 2 — ShardMap

- `aiokpl/shard_map.py`: state machine, async refresh, `bisect_left` lookup,
  `invalidate`. Uses `aiobotocore` `list_shards` paginated.
- Tests with `moto` for `ListShards`; assert binary-search correctness across
  splits.

### Phase 3 — Reducer + Aggregator + Collector

- `aiokpl/reducer.py`: generic deadline-driven batcher. Generic over
  `(item, batch)` types. `add()` returns a closed batch or `None`. Deadline
  reprogrammed via `loop.call_later`, cancellable.
- `aiokpl/aggregator.py`: per-shard reducers producing `AggregatedRecord`s.
  Falls back to single-record mode if `ShardMap` not READY.
- `aiokpl/collector.py`: reducer over `(AggregatedRecord, PutRecordsBatch)`
  with the 256 KiB/shard short-circuit.
- Tests: deadline ordering (FIFO fairness on flush), excess re-injection,
  cancellation under shutdown.

### Phase 4 — Limiter + TokenBucket

- `aiokpl/token_bucket.py`: multi-stream token bucket, `try_take([n_records,
  n_bytes])`. Pure math, no time.sleep — query-on-demand growth model.
- `aiokpl/limiter.py`: per-shard `ShardLimiter` with internal queue and a
  `drain()` polled every 25 ms by a background task.
- Tests: rate envelope under sustained load, expiry path.

### Phase 5 — Sender + Retrier

- `aiokpl/sender.py`: glue to `aiobotocore.client.put_records`, builds
  `PutRecordsRequestEntry` with the correct partition key and EHK for
  aggregated vs single records.
- `aiokpl/retrier.py`: the classification table above. Re-enqueues to the
  aggregator on retry. Resolves user futures on terminal outcomes.
- Tests: every row of the classification table, with `moto` and with hand-built
  fake clients for the wrong-shard cases.

### Phase 6 — Producer + lifecycle ✅ done (v0.1)

- `aiokpl/producer.py`: `async with Producer(cfg)` wiring. Per-stream pipelines
  in a `dict[str, _StreamPipeline]`. Graceful shutdown: stop intake, drain,
  cancel timers, close aiobotocore session.
- `aiokpl/config.py`: frozen `Config` dataclass with every tunable.
- `aiokpl/outcome.py`: `Outcome[T]` — anyio-friendly one-shot value-bearing
  event. Replaces `asyncio.Future` so the Outcome itself stays
  backend-portable (the surrounding `Producer` is asyncio-only because
  `aiobotocore` is asyncio-only).
- Backpressure: `max_outstanding_records` knob, `put_record` awaits a
  semaphore released by the Retrier's terminal callback.
- Expired-batch path: synthesize `SendOutcome(request_error=("Expired", …))`
  and route through the same Retrier classification as network errors.
- Public API: `from aiokpl import Producer, Config, Outcome`.

### Phase 7 — Metrics (optional, last)

- `aiokpl/metrics.py`: in-process counters per (stream, shard, name). Periodic
  flush to CloudWatch via aiobotocore. Toggleable. Default off in v0.

### Phase 8 — Sync bridge (optional)

- `aiokpl/sync.py`: wrapper for non-async callers. Spawns a private loop in a
  background thread. Only ship if asked.

---

## Configuration (subset of C++ Config worth keeping)

These map 1:1 to fields in `aws/kinesis/protobuf/config.proto`. Names kept for
recognizability. Defaults match the C++ KPL unless noted.

| Field | Default | Notes |
|---|---|---|
| `region` | required | |
| `aggregation_enabled` | `True` | |
| `aggregation_max_count` | 4294967295 | from C++ |
| `aggregation_max_size` | 51200 | bytes |
| `collection_max_count` | 500 | Kinesis hard limit |
| `collection_max_size` | 5 * 1024 * 1024 | Kinesis hard limit (5 MiB) |
| `record_max_buffered_time_ms` | 100 | deadline target |
| `record_ttl_ms` | 30000 | hard expiration |
| `fail_if_throttled` | `False` | |
| `request_timeout_ms` | 6000 | |
| `max_connections` | 24 | aiobotocore session |
| `metrics_level` | `"none"` | `"summary"`, `"detailed"` |
| `metrics_upload_delay_ms` | 60000 | |

Skip from C++ config (not applicable): `enable_core_dumps`, `native_process_*`,
`kinesis_endpoint` (use boto3 endpoint URL), `verify_certificate` (boto session).

---

## Testing strategy

- **Unit**: pure logic — aggregation codec, token bucket math, reducer
  deadline ordering, retrier classification.
- **Integration with `moto`**: ShardMap refresh against fake Kinesis,
  PutRecords with synthetic responses.
- **Property-based** (optional, hypothesis): shard prediction matches actual
  shard returned by `moto` for random partition keys.
- **Conformance**: byte-exact aggregation output vs `aws-kinesis-agg` Python
  reference, for a fixed set of records.

CI: GitHub Actions, Python 3.10/3.11/3.12/3.13, ruff + mypy + pytest.

---

## Non-goals

- No native binary, no daemon, no subprocess. Ever.
- No support for Python < 3.10.
- No sync-first API. Sync bridge is a thin afterthought.
- No CloudWatch in v0–v1. Ship later.
- No protobuf wire compatibility with the IPC layer of the C++ KPL. We only
  match the **on-the-wire aggregation format** to Kinesis.
- No KCL / consumer side. Producer only.

---

## How to resume work in a new chat

1. Read this file.
2. Read `~/.claude/skills/aiokpl/SKILL.md`.
3. `git -C /Users/juanro/Documents/githings/aiokpl log --oneline` to see what's
   landed.
4. Pick the next phase from the Roadmap and start.
5. Cross-reference the C++ source at
   `/Users/juanro/Documents/githings/amazon-kinesis-producer/aws/` when in
   doubt about semantics. Cite `file:line` in commit messages.

---

## House rules

- Idiomatic Python: `dataclasses`, `asyncio`, type hints everywhere.
- No comments that restate the code. Comments only for **why** (an invariant,
  a non-obvious interaction with Kinesis, a workaround).
- Errors are values until they reach the user. Inside the pipeline, an error
  is an `Attempt`, not an exception.
- Public surface is intentional. Anything not in `__all__` is internal.
- No git co-author lines for Claude, ever.
