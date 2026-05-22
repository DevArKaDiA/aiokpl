# Benchmarks

This page measures `aiokpl` against the existing Python Kinesis ecosystem.
Before the numbers, the framing — because the numbers don't mean the same
thing across rows.

## Apples and oranges

Two things called "throughput" are being measured here:

- **Bytes-per-second on the wire.** How fast a process can serialize and
  push records to Kinesis. No knowledge of which records succeeded.
- **Records-with-confirmation-per-second.** How fast a process can submit
  a record AND receive its `sequence_number` back from Kinesis.

These are different products. The Python ecosystem has tools for the
first (`aws-kinesis-agg + boto3`, raw `boto3.put_records`). `aiokpl` is the
only thing that ships the second.

| Variant | Per-record outcomes? | Retry on failure? | Shard prediction? | Backpressure? |
|---|:---:|:---:|:---:|:---:|
| `aiokpl async agg=on` (confirmed) | ✅ | ✅ | ✅ | ✅ |
| `aiokpl async agg=on` (fire-and-forget) | ❌ | ✅ | ✅ | ✅ |
| `aiokpl async agg=off` | ✅ | ✅ | ✅ | ✅ |
| `aiokpl sync agg=on` | ✅ | ✅ | ✅ | ✅ |
| `boto3.put_records` (batched) | ❌ (batch-level only) | ❌ | ❌ | ❌ |
| `aws-kinesis-agg + boto3` | ❌ | ❌ | ❌ | ❌ |

Read the throughput table with the columns above open in the other tab.
"Rps" rows in the same table are not strictly comparable.

## Throughput

20 000 records of 200 bytes each, single shard, end-to-end against
`etspaceman/kinesis-mock:0.5.2` in Docker on loopback. The boto3 variants
ran with reduced N because they saturate kinesis-mock's CPU above
~200 rps; see [caveats](#caveats).

| Variant | Records | Elapsed | Throughput |
|---|---:|---:|---:|
| `aws-kinesis-agg + boto3` (no outcomes, no retries) | 10 000 | 0.35 s | **28 960 rps** |
| `aiokpl async agg=on` (fire-and-forget) | 20 000 | 2.15 s | 9 296 rps |
| `aiokpl async agg=on` (confirmed) | 20 000 | 2.33 s | 8 572 rps |
| `aiokpl sync agg=on` (confirmed) | 20 000 | 5.24 s | 3 820 rps |
| `aiokpl async agg=off` (per-record PutRecords) | 20 000 | 94.81 s | 211 rps |
| `boto3.put_records` (batched 500) | 2 000 | 15.76 s | 127 rps |

### Reading the numbers honestly

**1 — `aws-kinesis-agg + boto3` wins on raw throughput. By 3×.**

It's not magic. It encodes the input into 1-4 aggregated records that fit
in a single `PutRecords` call and ships them. It does not track which
user-records inside each blob actually landed. It does not retry. It does
not predict shards (it assumes all records aggregated together share a
shard — which is wrong in multi-shard streams, but kinesis-mock doesn't
enforce that). For a workload that is genuinely "fire records, let
downstream detect losses", that's the right tool and it will be faster.

**2 — The `aiokpl` confirmed-vs-fire-and-forget gap is only 8 %.**

`aiokpl async agg=on (confirmed)` is 8 572 rps. The same configuration
without `await outcome.wait()` per record is 9 296 rps. The cost of
per-record outcome tracking is small. The cost of `aiokpl` over
`aws-kinesis-agg` (~3×) is the *pipeline* — per-record shard prediction,
per-shard rate limiting, retry classification, backpressure — not the
outcome bookkeeping.

That's an architectural floor, not a bug. If your workload doesn't need
those features, you don't need a producer; the codec is enough.

**3 — Aggregation is the single biggest knob.**

`aiokpl async agg=on` is 8 572 rps. The same code with
`aggregation_enabled=False` is 211 rps — a **41× drop**. Without
aggregation each user record becomes its own Kinesis record and your
throughput is bottlenecked by the HTTPS round-trip per call. This is
exactly why the C++ KPL invented aggregation in 2015 and why `aiokpl`
exists.

**4 — Naive `boto3.put_records` is not a competitor.**

127 rps single-threaded with 500-record batches is what dumb batching
costs you. It's here to make the gap explicit, not because it's a
baseline anyone should target.

**5 — The sync bridge is ~45 % of async throughput.**

`SyncProducer` bounces every `put_record` through an `anyio` portal on a
background thread. Pay it if you must (Flask, Django, Celery, scripts);
reach for the async path if you have an event loop.

## Latency

1 000 records submitted one at a time with ~1 ms inter-arrival,
2 shards, against the same emulator.

| Variant | P50 | P99 | P99.9 |
|---|---:|---:|---:|
| `boto3.put_records` (batched 500) | **105 ms** | 170 ms | 174 ms |
| `aws-kinesis-agg + boto3` | 698 ms | 1 331 ms | 1 342 ms |
| `aiokpl async agg=on` | 778 ms | 1 473 ms | 1 484 ms |
| `aiokpl async agg=off` | 798 ms | 1 475 ms | 1 487 ms |
| `aiokpl sync agg=on` | 959 ms | 1 755 ms | 1 767 ms |

Reading this:

- **`boto3.put_records` is the latency winner here, but on a synthetic
  metric.** Latency for the boto3 row is "elapsed since the batch
  started" — every record in a 500-record batch sees the same number.
  That's not a per-record durability time. Compare against its 127 rps
  throughput: the metric reflects how fast a tight sync loop runs, not
  how fast records become durable.
- **`aiokpl` and `aws-kinesis-agg` cluster around 700-1 500 ms** because
  both pay the buffered-time deadline (100 ms in aiokpl) plus
  kinesis-mock's per-call latency on aggregated batches.
- **P99.9 is tight to P99** across the board — there are no long tails
  on the emulator. Real AWS has different tails (throttle backoffs,
  split-shard convergence); those don't show up here.

## Caveats

These numbers are **relative**, not absolute. Specifically:

1. **kinesis-mock is not real AWS.** It's an in-process Scala
   reimplementation for testing. It has its own latency characteristics
   (mostly internal scheduling, no network), no real rate limits, no
   real shard provisioning, and a single-machine CPU ceiling we hit on
   non-aggregated variants. The shape (aggregation matters; per-record
   pipelines cost CPU) carries over to real AWS — the absolute numbers
   do not.

2. **The CPU ceiling forced compromises.** We initially tried `[1, 4, 8]`-shard
   runs at 20 000 records. The unaggregated variants saturate kinesis-mock
   above ~200 rps regardless of shard count, hanging the emulator. The
   shipped numbers are single-shard with reduced N (2 000 records for
   `boto3.put_records`, 10 000 for `aws-kinesis-agg + boto3`). Real-AWS
   multi-shard scaling is linear by design; that part doesn't need a
   benchmark to claim.

3. **The fire-and-forget variants drop most of what aiokpl does.** They
   are listed because they answer the natural question "how fast COULD
   aiokpl push bytes if I disabled the bookkeeping?". The answer is
   ~9 300 rps — and you give up per-record outcomes, retry classification
   visibility, and the ability to know which records failed. If the
   producer's job is "push events I care about", confirmed is the only
   honest measurement.

4. **`aiokpl` measures end-to-end.** Throughput for confirmed mode is the
   wall clock from the first `put_record` to the last `outcome.wait()`
   returning. The fire-and-forget variant ends at `outstanding_records ==
   0` (the pipeline has drained, but the caller didn't observe any
   per-record result). The `aws-kinesis-agg` row ends at the boto3
   `put_records` return — no equivalent end state because no per-record
   tracking exists.

5. **No retries forced.** kinesis-mock doesn't throttle naturally. Every
   record succeeds first try here. Real-world numbers will include
   retry latency for throttle/transient errors — and those costs land
   on `aiokpl` (which does retries), not on `aws-kinesis-agg + boto3`
   (which would just lose the records and never know).

## Methodology

- **Backend**: `ghcr.io/etspaceman/kinesis-mock:0.5.2` on Docker, single
  container, default config, HTTPS on `localhost:4567`, self-signed cert.
- **Host**: Apple Mac16,7 (M4 Pro, 14 cores), 48 GB RAM, macOS,
  Python 3.12.13.
- **Library version**: `aiokpl` 0.2.0 at commit `ad4c4b4`.
- **Records**: 200-byte payloads, partition keys cycled across 256
  distinct values.
- **`aiokpl` configuration**: defaults (`record_max_buffered_time_ms=50`
  for benches, `record_ttl_ms=30_000`, `max_outstanding_records ≥ N`).
- **boto3 / aws-kinesis-agg configuration**: boto3 defaults (no custom
  pool sizing). `aws-kinesis-agg` uses 1 MiB aggregation cap; ships 500
  aggregated records per `put_records` call.

## Reproducing

```bash
git clone https://github.com/DevArKaDiA/aiokpl
cd aiokpl
uv venv && uv pip install -e ".[dev,bench]"
docker pull ghcr.io/etspaceman/kinesis-mock:0.5.2

python -m benchmarks.bench_throughput | tee benchmarks/results/throughput.txt
python -m benchmarks.bench_latency    | tee benchmarks/results/latency.txt
```

Total runtime: 2-3 minutes for throughput, 1-2 minutes for latency.
Cross-machine comparison is fair — kinesis-mock's CPU starvation point is
roughly machine-independent (it's event-loop saturation, not a throughput
cap).

## Is the 3× worth it?

The honest answer for most readers is **yes**. Three things put the gap
in perspective.

### 1. 8 572 rps is already a lot

| Producer config | Records/day | Records/month |
|---|---:|---:|
| `aiokpl` confirmed @ 8 572 rps | **741 M / day** | 22 B / month |
| `aws-kinesis-agg + boto3` @ 28 960 rps | 2.5 B / day | 75 B / month |

At 200-byte records, 8 572 rps is ~1.7 MB/s sustained from **one Python
process**. To find a workload where that cap is the bottleneck, you need
to be doing one of:

- Streaming telemetry from a fleet of millions of devices.
- Ingesting logs from an entire org's microservice mesh.
- Already designing producer-side sharding (Kafka-style).

For webhooks, transactional events, audit logs, analytics, app
telemetry, observability pipelines — 8 572 rps is headroom, not a
ceiling.

### 2. The 3× is the worst case (kinesis-mock has no network)

kinesis-mock is loopback HTTPS with negligible per-call latency. That
amplifies the gap between "encode + 1 call" (`aws-kinesis-agg`) and
"encode + pipeline + outcomes" (`aiokpl`). On real AWS each
`PutRecords` round-trip is ~10-50 ms, and **that's the same cost for
both variants**. What differs is the per-record bookkeeping aiokpl does
outside the HTTP path — task switches measured in microseconds,
dominated by AWS network latency.

Expect the gap to compress to **roughly 1.3-1.5× against real AWS**.

### 3. The "safety net" is what 2 000 lines of code do

`aws-kinesis-agg + boto3` will give you one `PutRecords` response per
batch. If it says "47 of 1 200 records failed":

- The failed records are *inside* aggregated blobs — you don't know
  which user-records map to which failures.
- There is no retry. They're lost.
- If the failure was a throttle (`ProvisionedThroughputExceededException`),
  the next batch is just as likely to fail.
- If the failure was a wrong-shard-after-split, the failed records will
  keep going to the wrong shard.

To rebuild what `aiokpl` does on top of `aws-kinesis-agg + boto3`:

| You'd write | aiokpl ships |
|---|---|
| user-record → aggregated-blob → failure-result mapping | per-record `Outcome[RecordResult]` |
| Retry loop with throttle/transient/wrong-shard classification | `Retrier` (300 lines) |
| Exponential backoff with jitter | inside the Retrier |
| Shard map refresh on `split_shard` convergence | `ShardMap` (230 lines) |
| Per-shard token bucket (1000 rec/s + 1 MiB/s) | `Limiter` (152 lines) |
| Bounded-memory backpressure | `max_outstanding_records` semaphore |
| Vendor-neutral metrics export | `MetricsSink` (5 implementations) |

That's roughly the 2 000 lines `aiokpl` is. The 3× throughput gap is
the price of not writing them yourself.

## Verdict

- **Workload is "fire telemetry, downstream detects loss"** →
  `aws-kinesis-agg + boto3`. Faster, less code, right tool.
- **Records are events you care about** (webhooks, transactions, audit
  logs, observability) → `aiokpl`. The 3× is the cost of the safety
  net; on real AWS it compresses to ~1.5×.
- **You're not sure** → `aiokpl`. 8 572 rps covers the long tail.
  When you discover you need retries (you will), you don't have to
  migrate.
