# aiokpl

![CI](https://github.com/juanrojas/aiokpl/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/juanrojas/aiokpl/branch/main/graph/badge.svg)](https://codecov.io/gh/juanrojas/aiokpl)
![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**📚 [Documentation](https://juanrojas.github.io/aiokpl/)**

**Pure-Python async Kinesis producer. KPL-equivalent without a native daemon.**

> **v0.1 available** — the full pipeline ships and is exercised end-to-end
> against `kinesis-mock`. See [Phase 6](docs/phases/phase-6-producer.md).

> A library that respects the shard as the unit of optimization, measures time
> before size, and treats failures as user-visible information — not noise to hide.

---

## Status

**v0.1 — first usable release.** Full async pipeline lands with Phase 6:
`Producer` → `Aggregator` → `Limiter` → `Collector` → `Sender` → `Retrier`.
Not on PyPI yet — install via the repo URL.

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo scaffolding, design docs | ✅ Done |
| 1 | Aggregation codec (KPL wire format) | ✅ Done |
| 2 | ShardMap + prediction | ✅ Done |
| 3 | Reducer, Aggregator, Collector | ✅ Done |
| 4 | Limiter + TokenBucket | ✅ Done |
| 5 | Sender + Retrier | ✅ Done |
| 6 | Producer + lifecycle (first usable release: **v0.1**) | ✅ Done |
| 7 | CloudWatch metrics | 💤 Optional |
| 8 | Sync bridge | 💤 Optional |

---

## Why this exists

AWS ships the official Kinesis Producer Library as a native C++ binary
(`amazon-kinesis-producer`) wrapped in Java/.NET sidecars. The Python ecosystem
has never had a real KPL — only:

- `aws-kinesis-agg` — Python codec for the aggregation format. Useful, but not a
  producer. You still call `boto3.put_records` yourself.
- `kiner`, `kinesis-python`, `kinesis-producer` (ludia) — abandoned community
  attempts. All thin batchers over boto3.

`aiokpl` is a clean-room reimplementation in idiomatic async Python — built
on `anyio` so the same code runs on both `asyncio` and `trio` — that
**preserves what's worth preserving from the C++ KPL** (shard-aware pipeline,
deadline-driven batching, smart retry classification, byte-exact aggregation)
and **drops what was an accident of C++** (IPC, named pipes, child process,
custom spinlocks, static binaries, packaging hell).

It is not a wrapper around the C++ binary. It is a reimplementation of its
ideas in a language where you don't need a daemon.

---

## Core principles

1. **The shard is the unit of optimization, not the stream.**
2. **Predict before asking** — the sharding algorithm is deterministic.
3. **Batching is governed by deadlines, not sizes.**
4. **Each stage has one responsibility and one downstream callback.**
5. **Failures are data, not exceptions.**
6. **Bounded latency beats maximum throughput.**

The full design rationale lives in [`CLAUDE.md`](./CLAUDE.md).

---

## Features

### What v0.1 will do

- **Backend-agnostic**: works on both `asyncio` and `trio` via `anyio`.
- **Async-first API** built on `anyio` and (for the network layer)
  `aiobotocore`.
- **Byte-exact KPL aggregation** on the wire — KCL consumers deaggregate
  transparently.
- **Shard prediction** via `md5(partition_key)` + cached `ListShards`, O(log N)
  lookup with `bisect`.
- **Per-shard rate limiting** with a multi-stream token bucket
  (1000 records/s + 1 MiB/s, matching Kinesis hard limits).
- **Deadline-driven batching** at two levels: UserRecord → AggregatedRecord and
  AggregatedRecord → `PutRecords` batch.
- **Smart retry classification** distinguishing throttle, transient,
  wrong-shard (with split detection), and expired.
- **Per-record attempt history** returned to the caller — every retry is
  visible.
- **Bounded backpressure** via `max_outstanding_records`.
- **Graceful shutdown** via `async with` + `flush()`.

### What v0.1 will NOT do

- No CloudWatch metrics (Phase 7).
- No sync API (Phase 8).
- No producer-side consumer / KCL replacement.
- No native binary, ever. No subprocess, no IPC, no protobuf framing.
- No Python < 3.10.

---

## Intended usage

```python
import anyio
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
        outcome = await producer.put_record(
            stream="my-stream",
            partition_key="user-123",
            data=b"hello",
        )
        result = await outcome.wait()
        if result.success:
            print(result.shard_id, result.sequence_number)
        else:
            print("failed:", result.attempts[-1].error_code)

anyio.run(main)  # works on asyncio (default) or pass backend="trio"
```

`put_record()` returns an awaitable future. It resolves when the record
reaches a terminal state — success or final failure — and the `attempts`
list always carries the full retry history.

---

## Architecture (target)

```
UserRecord
   │  producer.put_record()
   ▼
Aggregator   ──► AggregatedRecord  (per predicted shard, deadline-driven)
   ▼
Limiter      ──► throttled to 1000 rec/s + 1 MiB/s per shard
   ▼
Collector    ──► PutRecordsBatch   (500 records / 5 MiB / 256 KiB-per-shard)
   ▼
Sender       ──► aiobotocore.put_records (async)
   ▼
Retrier      ──► classify outcome (throttle / transient / wrong-shard / expired)
   ▼
finish_user_record  →  resolves the user's awaitable future
```

Same pipeline as the C++ KPL, in idiomatic anyio primitives. See
[`CLAUDE.md`](./CLAUDE.md#mapping-c--python) for the C++↔Python translation
table.

---

## Roadmap

Phased on purpose. Each phase ships something testable on its own.

### Phase 1 — Aggregation codec ✅

- `aiokpl/aggregation.py`: encode/decode the KPL aggregated record format.
- `aiokpl/hashing.py`: partition-key → 128-bit hash, explicit hash key parsing.
- Conformance tests against `aws-kinesis-agg` and golden bytes captured from
  the C++ KPL.

### Phase 2 — ShardMap ✅

- Async refresh, state machine, `bisect_left` lookup, `invalidate()` from the
  retrier, exponential backoff (1s → 30s), background cleanup of closed shards
  after 60s.
- Tests with `moto` for `ListShards` paginated.

### Phase 3 — Reducer, Aggregator, Collector ✅

- Generic deadline-driven batcher (`reducer.py`) — the core abstraction reused
  twice.
- `aggregator.py` produces aggregated records per predicted shard, falling
  back to single-record mode when the ShardMap isn't ready.
- `collector.py` produces `PutRecords` batches with the 256 KiB/shard
  short-circuit.

### Phase 4 — Limiter + TokenBucket ✅

- `token_bucket.py`: multi-stream, query-on-demand growth, no sleep.
- `limiter.py`: per-shard `ShardLimiter` with a 25 ms drain loop.

### Phase 5 — Sender + Retrier ✅

- Glue to `aiobotocore.put_records`.
- The full classification table — every row covered in unit tests, including
  wrong-shard-after-split.

### Phase 6 — Producer + lifecycle  →  **v0.1 release** ✅

- Per-stream pipeline wiring, graceful shutdown, backpressure semaphore,
  configurable knobs.

### Phase 7 — CloudWatch metrics (optional)  🚧 next

- Per-shard / per-stream / global counters, periodic upload.

### Phase 8 — Sync bridge (optional)

- `Producer.sync()` for synchronous callers (no async runtime required).

---

## Non-goals

- Wrapping the C++ KPL daemon. Solved problem solved differently.
- Compatibility with the KPL **IPC** protobuf (`messages.proto`). We only
  match the **on-the-wire aggregation format** Kinesis sees.
- KCL / consumer side. Producer only.
- Python < 3.10.
- Sync-first API.

---

## Development

Requirements: Python 3.10+ and `uv` (or `pip`).

```bash
git clone <repo>
cd aiokpl
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest
```

CI matrix: Python 3.10 / 3.11 / 3.12 / 3.13. `ruff` + `mypy` + `pytest`.

---

## Reference

Implementation context — including the philosophy, the C++↔Python mapping, the
retrier classification table, and the resume guide — lives in
[`CLAUDE.md`](./CLAUDE.md).

Original C++ KPL source for cross-referencing:
<https://github.com/awslabs/amazon-kinesis-producer>.

---

## License

Apache-2.0.
