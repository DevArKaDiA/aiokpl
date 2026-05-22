# Benchmarks

These numbers measure `aiokpl` against the existing Python Kinesis
ecosystem on a local emulator. Read the [caveats](#caveats) before reading
the tables — kinesis-mock is not real AWS, and one of the variants
(`aws-kinesis-agg + boto3`) is fire-and-forget while the others track
per-record delivery. The numbers tell a story, but only the right story.

## Variants compared

| Variant | What it does | Per-record outcome? |
|---|---|:---:|
| `aiokpl async agg=on` | Full pipeline, aggregation enabled | ✅ |
| `aiokpl async agg=off` | Same pipeline, aggregation off (each user record = one Kinesis record) | ✅ |
| `aiokpl sync agg=on` | `SyncProducer` (anyio portal on a background thread) | ✅ |
| `boto3.put_records` | Naive sync batcher: 500 records per call, no aggregation | partial (whole batch) |
| `aws-kinesis-agg + boto3` | Encode aggregated records via `aws-kinesis-agg`, send via `boto3.put_records` | ❌ fire-and-forget |

`aws-kinesis-agg + boto3` does not track per-record outcomes — it ships an
aggregated blob and never knows which user-records inside it succeeded.
Apples to oranges with everything else in the table. We keep it because
it's the closest approximation of "what people do in Python today" and the
gap is part of why `aiokpl` exists.

## Throughput

20 000 records of 200 bytes each, single shard, run end-to-end against
`etspaceman/kinesis-mock:0.5.2` in Docker on loopback.

| Variant | Records sent | Elapsed | Throughput |
|---|---:|---:|---:|
| `aiokpl async agg=on` | 20 000 | 2.32 s | **8 637 rps** |
| `aiokpl sync agg=on` | 20 000 | 5.13 s | 3 896 rps |
| `aiokpl async agg=off` | 20 000 | 95.39 s | 210 rps |
| `boto3.put_records` (batched 500) | 2 000 | 16.19 s | 124 rps |
| `aws-kinesis-agg + boto3` (fire-and-forget) | 10 000 | 0.33 s | 30 093 rps |

Reading this:

- **Aggregation is the whole game.** `aiokpl` with aggregation moves
  ~41× more records per second than the same pipeline with aggregation
  disabled. This is the single most consequential knob in the library.
- **Per-record outcomes cost ~3×** vs fire-and-forget on kinesis-mock.
  `aws-kinesis-agg + boto3` ships at 30 k rps but never tells you which
  records failed. `aiokpl` ships at 8.6 k rps and resolves a
  `RecordResult` (with shard id, sequence number, and full attempt
  history) for every single record. If you can afford to lose visibility
  on retries, you don't need a real producer.
- **Naive `boto3.put_records` is not a competitor.** 124 rps single-threaded
  is what it looks like when nobody is grouping by predicted shard or
  amortizing HTTPS connections. The number is here to make the gap
  explicit, not because it's a baseline you should target.
- **The sync bridge is ~55 % of async throughput.** The cost of bouncing
  through `anyio.from_thread.BlockingPortal` for every `put_record`.
  Pay it if you have to (Flask, Django, Celery, scripts); reach for the
  async path if you have an event loop.

`aiokpl async agg=off` and `boto3.put_records` were run with reduced N
because they saturate kinesis-mock's CPU above ~200 rps. See
[caveats](#caveats).

## Latency

1 000 records submitted one at a time with ~1 ms inter-arrival, 2 shards,
end-to-end against the same emulator.

| Variant | P50 | P99 | P99.9 |
|---|---:|---:|---:|
| `aiokpl async agg=on` | 778 ms | 1 473 ms | 1 484 ms |
| `aiokpl async agg=off` | 798 ms | 1 475 ms | 1 487 ms |
| `aiokpl sync agg=on` | 959 ms | 1 755 ms | 1 767 ms |
| `boto3.put_records` (batched 500) | **105 ms** | 170 ms | 174 ms |
| `aws-kinesis-agg + boto3` | 698 ms | 1 331 ms | 1 342 ms |

Reading this:

- **`boto3.put_records` wins on P50** — it's a tight sync loop that
  records a "latency" as the wall-clock since the batch started. There's
  no concept of per-record durability point. Compare it with the
  throughput row: 105 ms P50 at 124 rps is what you get when you don't
  batch.
- **`aiokpl` and `aws-kinesis-agg` cluster around 700-1 500 ms** because
  both pay the buffered-time deadline (default 100 ms in aiokpl) plus
  kinesis-mock's per-call latency on aggregated batches.
- **P99.9 is tight to P99** for every variant — there are no long tails
  on the emulator. Real AWS will have a different tail; you'll see
  throttle backoffs and split-shard convergence in production where
  there are none here.

## Caveats

These results are **relative**, not absolute. Specifically:

1. **kinesis-mock is not real AWS.** It's an in-process Scala
   reimplementation of the Kinesis API for testing. It has its own latency
   characteristics (mostly internal scheduling, no network), no real rate
   limits, no real shard provisioning, and a single-machine CPU ceiling
   we hit hard on non-aggregated variants. The shape of the results
   (aggregation matters, per-record outcomes have a cost) is the same
   shape you'll see against real AWS — the absolute numbers will not be.

2. **The CPU ceiling forced compromises.** We initially tried
   `[1, 4, 8]`-shard runs at 20 000 records. The unaggregated variants
   (`aiokpl agg=off`, `boto3 raw`) saturate kinesis-mock above
   ~200 rps regardless of shard count, hanging the emulator on the
   higher loads. The shipped numbers are single-shard with reduced N
   (2 000 records for the boto3 variants), enough to be statistically
   meaningful without crashing the emulator. Real AWS multi-shard scaling
   is linear; that part doesn't need a benchmark to claim.

3. **`aws-kinesis-agg + boto3` does not track per-record outcomes.**
   It encodes an aggregated blob and ships it. If the call fails, the
   caller knows; if 47 out of 1 200 records inside the blob were the
   ones that hit a throttle, the caller does not know. Every other
   variant in the tables surfaces a `RecordResult` for each user
   record. Compare the rows with that in mind.

4. **`aiokpl` measures end-to-end including outcome resolution.**
   "Throughput" = time from the first `put_record` to the last
   `outcome.wait()` resolving. "Latency" = `outcome.wait()` resolution
   timestamp minus `put_record` invocation timestamp. The
   `aws-kinesis-agg` variant has no equivalent end-state to measure
   against — its synthetic latency is the inter-call interval.

5. **Sync variants are limited by GIL + single-thread.** No threading,
   no multiprocessing. `SyncProducer` spends most of its CPU bouncing
   between the calling thread and the portal thread; replacing that
   with a thread pool wouldn't help (the bottleneck is per-record
   coordination, not throughput).

## Methodology

- **Backend**: `ghcr.io/etspaceman/kinesis-mock:0.5.2` on Docker, single
  container, default config, talking HTTPS on `localhost:4567` with a
  self-signed cert.
- **Host**: Apple Mac16,7 (M4 Pro, 14 cores), 48 GB RAM, macOS,
  Python 3.12.13.
- **Library version**: `aiokpl` 0.2.0 (commit `ad4c4b4`).
- **Records**: 200-byte payloads, partition keys cycled across 256
  distinct values to spread across shards.
- **Knobs**: every variant runs with `aiokpl`'s default `Config`
  (`record_max_buffered_time_ms=100`, `record_ttl_ms=30_000`,
  `max_outstanding_records=100_000`, etc.) for `aiokpl` variants. The
  boto3-based variants use boto3 defaults.
- **No retries forced.** kinesis-mock doesn't throttle naturally; every
  record succeeds on the first try in these runs. Real-world numbers
  will include retry latency for throttle/transient errors.

## Reproducing

```bash
git clone https://github.com/DevArKaDiA/aiokpl
cd aiokpl
uv venv && uv pip install -e ".[dev,bench]"
docker pull ghcr.io/etspaceman/kinesis-mock:0.5.2

python -m benchmarks.bench_throughput | tee benchmarks/results/throughput.txt
python -m benchmarks.bench_latency    | tee benchmarks/results/latency.txt
```

Results land as Markdown tables in stdout and as JSON in
`benchmarks/results/*.json`. Cross-machine comparison is fair —
kinesis-mock's CPU starvation point is roughly machine-independent (it's
an event-loop saturation, not a throughput cap).

## What's *not* here

- **Real-AWS numbers.** Would cost money, would need a stable runner,
  and wouldn't be reproducible across readers. Out of scope for the
  shipped tables; if you want them, run the scripts against your own
  account with `AIOKPL_ENDPOINT_URL` unset.
- **Throttle behavior.** kinesis-mock doesn't throttle. Retry-path
  latency under throttling is a real-AWS-only measurement.
- **Split-shard convergence.** `ShardMap.invalidate` is tested in
  integration but not benchmarked — the latency cost is dominated by
  `ListShards` round-trip, not by any aiokpl logic.
