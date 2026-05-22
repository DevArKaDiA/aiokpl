# aiokpl benchmarks

Two scripts that compare aiokpl against the rest of the Python Kinesis
ecosystem against `etspaceman/kinesis-mock` running in Docker.

**Requires Docker. Takes 5-10 minutes to run all variants.**

## Install

```bash
pip install -e ".[dev,bench]"
docker pull ghcr.io/etspaceman/kinesis-mock:0.5.2
```

## Run

```bash
python -m benchmarks.bench_throughput | tee benchmarks/results/throughput.txt
python -m benchmarks.bench_latency    | tee benchmarks/results/latency.txt
```

Each script writes a JSON copy of its results to `benchmarks/results/`.

## What gets measured

- `bench_throughput.py` — sustained records-per-second under each variant,
  across 1, 4, and 8 shards.
- `bench_latency.py` — per-record latency (P50/P99/P99.9) at low rate
  (~1 ms inter-arrival).

Variants:

1. `aiokpl` async, aggregation ON
2. `aiokpl` async, aggregation OFF
3. `aiokpl` `SyncProducer`
4. raw `boto3.put_records` (naive 500-per-call batching)
5. `aws-kinesis-agg` aggregator + `boto3.put_records`

## Important caveat

`kinesis-mock` is an in-process Scala Kinesis emulator. It has **no**
real network, **no** real shard throttling, **no** real propagation
delay. The numbers in `docs/benchmarks.md` are **RELATIVE** comparisons
between producer designs, not absolute predictions of throughput against
real AWS Kinesis. Treat them accordingly.
