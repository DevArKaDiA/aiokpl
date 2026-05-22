"""Sustained-throughput benchmark — five variants, one stream per run.

Run:

    python -m benchmarks.bench_throughput

Submits ``N`` records as fast as the producer accepts them, measures wall
clock from first submit to last outcome resolved, prints + JSON-dumps the
table. Variants:

1. aiokpl async, aggregation ON
2. aiokpl async, aggregation OFF
3. aiokpl SyncProducer (aggregation ON)
4. raw boto3 ``put_records`` (naive batching by 500/5 MiB)
5. ``aws-kinesis-agg`` aggregator + boto3 ``put_records``
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

# Tunable; smaller subset for the slow variants.
N_AIOKPL = 20_000
N_BOTO3_RAW = 2_000   # single-record sync put_records saturates kinesis-mock above this
N_BOTO3_AGG = 10_000
RECORD_SIZE = 200  # bytes
PARTITION_KEYS = 256  # spread over many keys -> spread over shards
# Restricted to 1 shard because the unaggregated variants (aiokpl agg=off,
# boto3 raw) saturate kinesis-mock's CPU above ~200 rps and the larger shard
# counts hang the emulator for minutes. Real-AWS multi-shard scaling is
# linear; see docs/benchmarks.md for caveats.
SHARD_COUNTS = [1]


def _make_records(n: int) -> list[tuple[str, bytes]]:
    payload = b"x" * RECORD_SIZE
    return [(f"pk-{i % PARTITION_KEYS}", payload) for i in range(n)]


# ─── aiokpl variants ──────────────────────────────────────────────────────


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
    records = _make_records(n)
    submit_times: list[float] = [0.0] * n
    latencies: list[float] = []

    async with Producer(cfg) as producer:
        outcomes = []
        t0 = time.perf_counter()
        for i, (pk, data) in enumerate(records):
            submit_times[i] = time.perf_counter()
            o = await producer.put_record(stream=stream, partition_key=pk, data=data)
            outcomes.append((i, o))
        await producer.flush()

        async def _await(i: int, o: Any) -> None:
            await o.wait()
            latencies.append(time.perf_counter() - submit_times[i])

        async with asyncio.TaskGroup() as tg:
            for i, o in outcomes:
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
    records = _make_records(n)
    latencies: list[float] = []

    with SyncProducer(cfg) as producer:
        outcomes = []
        submit_times = []
        t0 = time.perf_counter()
        for pk, data in records:
            submit_times.append(time.perf_counter())
            outcomes.append(producer.put_record(stream=stream, partition_key=pk, data=data))
        producer.flush(timeout=120.0)
        for i, o in enumerate(outcomes):
            with contextlib.suppress(Exception):
                o.wait(timeout=120.0)
            latencies.append(time.perf_counter() - submit_times[i])
        elapsed = time.perf_counter() - t0
    return elapsed, latencies


# ─── boto3 variants ───────────────────────────────────────────────────────


def _chunks(xs: list[Any], size: int) -> list[list[Any]]:
    return [xs[i : i + size] for i in range(0, len(xs), size)]


def _boto3_raw(endpoint_url: str, stream: str, n: int) -> tuple[float, list[float]]:
    """Naive batching: 500 records per ``put_records``."""
    client = make_boto3_client(endpoint_url)
    records = _make_records(n)
    entries = [{"Data": data, "PartitionKey": pk} for pk, data in records]

    latencies: list[float] = []
    submit_t = time.perf_counter()
    t0 = submit_t
    for batch in _chunks(entries, 500):
        client.put_records(StreamName=stream, Records=batch)
        now = time.perf_counter()
        # Synthetic per-record latency: every record in the batch sees the
        # full inter-call interval. Documented in docs/benchmarks.md.
        per = now - submit_t
        latencies.extend([per] * len(batch))
        submit_t = now
    elapsed = time.perf_counter() - t0
    return elapsed, latencies


def _boto3_kinesis_agg(endpoint_url: str, stream: str, n: int) -> tuple[float, list[float]]:
    """``aws-kinesis-agg`` aggregator + boto3 ``put_records``."""
    from aws_kinesis_agg.aggregator import RecordAggregator

    client = make_boto3_client(endpoint_url)
    records = _make_records(n)

    latencies: list[float] = []
    agg = RecordAggregator()
    aggregated_entries: list[dict[str, Any]] = []
    submit_t = time.perf_counter()
    t0 = submit_t

    def _flush_agg_record(rec: Any) -> None:
        if rec is None:
            return
        pk, ehk, data = rec.get_contents()
        entry: dict[str, Any] = {"Data": data, "PartitionKey": pk}
        if ehk is not None:
            entry["ExplicitHashKey"] = ehk
        aggregated_entries.append(entry)

    for pk, data in records:
        full = agg.add_user_record(pk, data)
        if full is not None:
            _flush_agg_record(full)
        if len(aggregated_entries) >= 500:
            client.put_records(StreamName=stream, Records=aggregated_entries)
            now = time.perf_counter()
            latencies.extend([now - submit_t] * len(aggregated_entries))
            aggregated_entries = []
            submit_t = now
    _flush_agg_record(agg.clear_and_get())
    if aggregated_entries:
        client.put_records(StreamName=stream, Records=aggregated_entries)
        now = time.perf_counter()
        latencies.extend([now - submit_t] * len(aggregated_entries))

    elapsed = time.perf_counter() - t0
    # The latencies list was sized per-aggregated-call * len(agg-entries),
    # which is < n; pad to n for table comparability.
    if len(latencies) < n:
        latencies.extend([latencies[-1] if latencies else 0.0] * (n - len(latencies)))
    return elapsed, latencies[:n]


# ─── Runner ───────────────────────────────────────────────────────────────


def _run_one(
    label: str,
    fn: Any,
    n: int,
    shards: int,
    notes: str = "",
) -> BenchResult:
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
        shards=shards,
        notes=notes,
    )


def _variants(endpoint: str, stream: str) -> list[tuple[str, Any, int]]:
    return [
        (
            "aiokpl async agg=on",
            lambda: asyncio.run(_aiokpl_async(endpoint, stream, N_AIOKPL, aggregation=True)),
            N_AIOKPL,
        ),
        (
            "aiokpl async agg=off",
            lambda: asyncio.run(_aiokpl_async(endpoint, stream, N_AIOKPL, aggregation=False)),
            N_AIOKPL,
        ),
        (
            "aiokpl sync agg=on",
            lambda: _aiokpl_sync(endpoint, stream, N_AIOKPL, aggregation=True),
            N_AIOKPL,
        ),
        (
            "boto3 put_records",
            lambda: _boto3_raw(endpoint, stream, N_BOTO3_RAW),
            N_BOTO3_RAW,
        ),
        (
            "aws-kinesis-agg + boto3",
            lambda: _boto3_kinesis_agg(endpoint, stream, N_BOTO3_AGG),
            N_BOTO3_AGG,
        ),
    ]


def main() -> int:
    print("[bench_throughput] starting kinesis-mock…", flush=True)
    endpoint, container = spin_up_kinesis_mock()
    print(f"[bench_throughput] endpoint={endpoint}", flush=True)
    results: list[BenchResult] = []
    try:
        boto = make_boto3_client(endpoint)
        for shards in SHARD_COUNTS:
            print(f"\n=== shards={shards} ===", flush=True)
            stream = f"bench-tp-{shards}-{uuid.uuid4().hex[:8]}"
            create_stream(boto, stream, shards)
            wait_active(boto, stream)
            try:
                for label, fn, n in _variants(endpoint, stream):
                    print(f"  running {label}…", flush=True)
                    try:
                        r = _run_one(label, fn, n, shards)
                        print(
                            f"    -> rps={r.throughput_rps:,.0f} elapsed={r.elapsed_s:.2f}s",
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
                                shards=shards,
                                notes=f"FAILED: {exc!r}"[:80],
                            )
                        )
            finally:
                delete_stream(boto, stream)
    finally:
        with contextlib.suppress(Exception):
            container.stop(timeout=5)

    print("\n=== Throughput results ===")
    print_table(results)
    save_results_json(results, "benchmarks/results/throughput.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
