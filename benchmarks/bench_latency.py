"""Per-record-latency benchmark — five variants, one 4-shard stream.

Run:

    python -m benchmarks.bench_latency

Submits ``N`` records one-by-one with a ~1 ms inter-arrival pause so
backpressure doesn't dominate the measurement. Measures
``t_resolved - t_submitted`` per record and reports P50/P99/P99.9.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
import uuid
from typing import Any

from benchmarks._harness import (
    BenchResult,
    create_stream,
    delete_stream,
    make_boto3_client,
    percentiles_ms,
    print_table,
    save_results_json,
    spin_up_kinesis_mock,
    wait_active,
)

N_LAT = 1_000   # kinesis-mock CPU-starves on per-record loops above this
N_BOTO3 = 1_000
RECORD_SIZE = 200
PARTITION_KEYS = 256
SHARDS = 2      # single-shard collapses prediction; 2 is the smallest meaningful
INTER_ARRIVAL_S = 0.001


def _payload() -> bytes:
    return b"x" * RECORD_SIZE


# ─── aiokpl ───────────────────────────────────────────────────────────────


async def _aiokpl_async(
    endpoint_url: str,
    stream: str,
    n: int,
    *,
    aggregation: bool,
) -> tuple[float, list[float]]:
    from aiokpl import Config, Producer

    cfg = Config(
        region="us-east-1",
        endpoint_url=endpoint_url,
        verify_ssl=False,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aggregation_enabled=aggregation,
        record_max_buffered_time_ms=50.0,
        max_outstanding_records=n + 1000,
    )
    payload = _payload()
    submit_times: list[float] = []
    latencies: list[float] = []

    async with Producer(cfg) as producer:
        outcomes = []
        t0 = time.perf_counter()
        for i in range(n):
            submit_times.append(time.perf_counter())
            o = await producer.put_record(
                stream=stream, partition_key=f"pk-{i % PARTITION_KEYS}", data=payload
            )
            outcomes.append(o)
            await asyncio.sleep(INTER_ARRIVAL_S)
        await producer.flush()

        async def _await(i: int, o: Any) -> None:
            await o.wait()
            latencies.append(time.perf_counter() - submit_times[i])

        async with asyncio.TaskGroup() as tg:
            for i, o in enumerate(outcomes):
                tg.create_task(_await(i, o))
        elapsed = time.perf_counter() - t0
    return elapsed, latencies


def _aiokpl_sync(
    endpoint_url: str,
    stream: str,
    n: int,
    *,
    aggregation: bool,
) -> tuple[float, list[float]]:
    from aiokpl import Config, SyncProducer

    cfg = Config(
        region="us-east-1",
        endpoint_url=endpoint_url,
        verify_ssl=False,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aggregation_enabled=aggregation,
        record_max_buffered_time_ms=50.0,
        max_outstanding_records=n + 1000,
    )
    payload = _payload()
    submit_times: list[float] = []
    latencies: list[float] = []

    with SyncProducer(cfg) as producer:
        outcomes = []
        t0 = time.perf_counter()
        for i in range(n):
            submit_times.append(time.perf_counter())
            outcomes.append(
                producer.put_record(
                    stream=stream, partition_key=f"pk-{i % PARTITION_KEYS}", data=payload
                )
            )
            time.sleep(INTER_ARRIVAL_S)
        producer.flush(timeout=120.0)
        for i, o in enumerate(outcomes):
            with contextlib.suppress(Exception):
                o.wait(timeout=120.0)
            latencies.append(time.perf_counter() - submit_times[i])
        elapsed = time.perf_counter() - t0
    return elapsed, latencies


# ─── boto3 (synthetic submit timestamp per record) ────────────────────────


def _boto3_raw(endpoint_url: str, stream: str, n: int) -> tuple[float, list[float]]:
    client = make_boto3_client(endpoint_url)
    payload = _payload()
    latencies: list[float] = []
    submit_times: list[float] = []
    batch: list[dict[str, Any]] = []
    indices: list[int] = []
    BATCH = 100
    t0 = time.perf_counter()
    for i in range(n):
        submit_times.append(time.perf_counter())
        batch.append({"Data": payload, "PartitionKey": f"pk-{i % PARTITION_KEYS}"})
        indices.append(i)
        time.sleep(INTER_ARRIVAL_S)
        if len(batch) >= BATCH:
            client.put_records(StreamName=stream, Records=batch)
            now = time.perf_counter()
            for j in indices:
                latencies.append(now - submit_times[j])
            batch = []
            indices = []
    if batch:
        client.put_records(StreamName=stream, Records=batch)
        now = time.perf_counter()
        for j in indices:
            latencies.append(now - submit_times[j])
    elapsed = time.perf_counter() - t0
    return elapsed, latencies


def _boto3_kinesis_agg(endpoint_url: str, stream: str, n: int) -> tuple[float, list[float]]:
    from aws_kinesis_agg.aggregator import RecordAggregator

    client = make_boto3_client(endpoint_url)
    payload = _payload()
    latencies: list[float] = []
    submit_times: list[float] = []
    # Track which user-record indices ended up in each aggregated entry, so
    # we can attribute the post-PutRecords time back to every record in the
    # aggregate.
    agg = RecordAggregator()
    aggregated_entries: list[dict[str, Any]] = []
    aggregated_indices: list[list[int]] = []
    current_indices: list[int] = []
    BATCH_AGG = 50  # aggregated entries per put_records
    t0 = time.perf_counter()

    def _close_agg(rec: Any) -> None:
        if rec is None:
            return
        pk, ehk, data = rec.get_contents()
        entry: dict[str, Any] = {"Data": data, "PartitionKey": pk}
        if ehk is not None:
            entry["ExplicitHashKey"] = ehk
        aggregated_entries.append(entry)
        aggregated_indices.append(current_indices.copy())
        current_indices.clear()

    for i in range(n):
        submit_times.append(time.perf_counter())
        current_indices.append(i)
        full = agg.add_user_record(f"pk-{i % PARTITION_KEYS}", payload)
        if full is not None:
            _close_agg(full)
        time.sleep(INTER_ARRIVAL_S)
        if len(aggregated_entries) >= BATCH_AGG:
            client.put_records(StreamName=stream, Records=aggregated_entries)
            now = time.perf_counter()
            for idxs in aggregated_indices:
                for j in idxs:
                    latencies.append(now - submit_times[j])
            aggregated_entries = []
            aggregated_indices = []
    _close_agg(agg.clear_and_get())
    if aggregated_entries:
        client.put_records(StreamName=stream, Records=aggregated_entries)
        now = time.perf_counter()
        for idxs in aggregated_indices:
            for j in idxs:
                latencies.append(now - submit_times[j])
    elapsed = time.perf_counter() - t0
    return elapsed, latencies


# ─── Runner ───────────────────────────────────────────────────────────────


def _run_one(label: str, fn: Any, n: int) -> BenchResult:
    elapsed, latencies = fn()
    p50, p99, p999 = percentiles_ms(latencies)
    rps = n / elapsed if elapsed > 0 else 0.0
    return BenchResult(
        label=label,
        total_records=n,
        elapsed_s=elapsed,
        throughput_rps=rps,
        p50_ms=p50,
        p99_ms=p99,
        p999_ms=p999,
        shards=SHARDS,
    )


def main() -> int:
    print("[bench_latency] starting kinesis-mock…", flush=True)
    endpoint, container = spin_up_kinesis_mock()
    print(f"[bench_latency] endpoint={endpoint}", flush=True)
    results: list[BenchResult] = []
    try:
        boto = make_boto3_client(endpoint)
        stream = f"bench-lat-{uuid.uuid4().hex[:8]}"
        create_stream(boto, stream, SHARDS)
        wait_active(boto, stream)
        try:
            variants: list[tuple[str, Any, int]] = [
                (
                    "aiokpl async agg=on",
                    lambda: asyncio.run(_aiokpl_async(endpoint, stream, N_LAT, aggregation=True)),
                    N_LAT,
                ),
                (
                    "aiokpl async agg=off",
                    lambda: asyncio.run(_aiokpl_async(endpoint, stream, N_LAT, aggregation=False)),
                    N_LAT,
                ),
                (
                    "aiokpl sync agg=on",
                    lambda: _aiokpl_sync(endpoint, stream, N_LAT, aggregation=True),
                    N_LAT,
                ),
                (
                    "boto3 put_records",
                    lambda: _boto3_raw(endpoint, stream, N_BOTO3),
                    N_BOTO3,
                ),
                (
                    "aws-kinesis-agg + boto3",
                    lambda: _boto3_kinesis_agg(endpoint, stream, N_BOTO3),
                    N_BOTO3,
                ),
            ]
            for label, fn, n in variants:
                print(f"  running {label}…", flush=True)
                try:
                    r = _run_one(label, fn, n)
                    print(
                        f"    -> p50={r.p50_ms:.2f}ms p99={r.p99_ms:.2f}ms p99.9={r.p999_ms:.2f}ms",
                        flush=True,
                    )
                    results.append(r)
                except Exception as exc:
                    print(f"    !! {label} FAILED: {exc!r}", flush=True)
                    results.append(
                        BenchResult(
                            label=label,
                            total_records=n,
                            elapsed_s=0.0,
                            throughput_rps=0.0,
                            p50_ms=0.0,
                            p99_ms=0.0,
                            p999_ms=0.0,
                            shards=SHARDS,
                            notes=f"FAILED: {exc!r}"[:80],
                        )
                    )
        finally:
            delete_stream(boto, stream)
    finally:
        with contextlib.suppress(Exception):
            container.stop(timeout=5)

    print("\n=== Latency results ===")
    print_table(results)
    save_results_json(results, "benchmarks/results/latency.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
