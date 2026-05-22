"""Shared infrastructure for the aiokpl benchmark scripts.

The benchmarks compare ``aiokpl`` against the Python-ecosystem alternatives
(naive ``boto3.put_records`` and ``aws-kinesis-agg`` + boto3) against a
``etspaceman/kinesis-mock`` container — same backend the integration tests
use. Everything here is runnable standalone (no pytest).
"""

from __future__ import annotations

import contextlib
import json
import socket
import time
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

KINESIS_MOCK_IMAGE = "ghcr.io/etspaceman/kinesis-mock:0.5.2"
KINESIS_PORT = 4567
HEALTH_PORT = 4568
READY_TIMEOUT_SECS = 60.0


@dataclass(slots=True)
class BenchResult:
    """One row of the benchmark output table."""

    label: str
    total_records: int
    elapsed_s: float
    throughput_rps: float
    p50_ms: float
    p99_ms: float
    p999_ms: float
    shards: int = 1
    notes: str = ""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def spin_up_kinesis_mock() -> tuple[str, Any]:
    """Start a kinesis-mock container; return (endpoint_url, container handle).

    Mirrors ``tests/integration/conftest.py`` — same image, same healthcheck.
    Caller is responsible for ``container.stop(timeout=5)`` in a ``finally``.
    """
    import docker

    client = docker.from_env()
    client.ping()

    api_port = _free_port()
    health_port = _free_port()

    container = client.containers.run(
        KINESIS_MOCK_IMAGE,
        detach=True,
        remove=True,
        ports={
            f"{KINESIS_PORT}/tcp": api_port,
            f"{HEALTH_PORT}/tcp": health_port,
        },
        environment={"LOG_LEVEL": "WARN"},
    )

    deadline = time.monotonic() + READY_TIMEOUT_SECS
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://localhost:{health_port}/healthcheck",
                timeout=2.0,
            ) as resp:
                if resp.status == 200:
                    break
        except Exception as exc:
            last_err = exc
            time.sleep(0.3)
    else:
        with contextlib.suppress(Exception):
            container.stop(timeout=5)
        raise RuntimeError(
            f"kinesis-mock did not become healthy in {READY_TIMEOUT_SECS}s: {last_err}"
        )

    return f"https://localhost:{api_port}", container


def create_stream(client: Any, name: str, shard_count: int) -> None:
    """Create a stream with ``shard_count`` shards (PROVISIONED mode)."""
    client.create_stream(StreamName=name, ShardCount=shard_count)


def wait_active(client: Any, name: str, timeout_s: float = 60.0) -> None:
    """Poll ``DescribeStreamSummary`` until ``StreamStatus == ACTIVE``."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        desc = client.describe_stream_summary(StreamName=name)
        if desc["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE":
            return
        time.sleep(0.2)
    raise RuntimeError(f"stream {name} never became ACTIVE")


def delete_stream(client: Any, name: str) -> None:
    """Best-effort stream deletion."""
    with contextlib.suppress(Exception):
        client.delete_stream(StreamName=name, EnforceConsumerDeletion=True)


def percentiles_ms(latencies_s: list[float]) -> tuple[float, float, float]:
    """Return (P50, P99, P99.9) in milliseconds. Empty list returns zeros."""
    if not latencies_s:
        return 0.0, 0.0, 0.0
    xs = sorted(latencies_s)
    n = len(xs)

    def pct(p: float) -> float:
        idx = min(n - 1, max(0, round(p * (n - 1))))
        return xs[idx] * 1000.0

    return pct(0.50), pct(0.99), pct(0.999)


def print_table(results: list[BenchResult]) -> None:
    """Print a markdown-friendly table of results."""
    try:
        from tabulate import tabulate

        rows = [
            [
                r.label,
                r.shards,
                r.total_records,
                f"{r.elapsed_s:.3f}",
                f"{r.throughput_rps:,.0f}",
                f"{r.p50_ms:.2f}",
                f"{r.p99_ms:.2f}",
                f"{r.p999_ms:.2f}",
                r.notes,
            ]
            for r in results
        ]
        print(
            tabulate(
                rows,
                headers=[
                    "variant",
                    "shards",
                    "N",
                    "elapsed (s)",
                    "rps",
                    "p50 ms",
                    "p99 ms",
                    "p99.9 ms",
                    "notes",
                ],
                tablefmt="github",
            )
        )
    except ImportError:
        for r in results:
            print(
                f"{r.label} shards={r.shards} N={r.total_records} "
                f"elapsed={r.elapsed_s:.3f}s rps={r.throughput_rps:.0f} "
                f"p50={r.p50_ms:.2f}ms p99={r.p99_ms:.2f}ms p99.9={r.p999_ms:.2f}ms "
                f"{r.notes}"
            )


def save_results_json(results: list[BenchResult], path: str | Path) -> None:
    """Dump results to JSON at ``path``."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps([asdict(r) for r in results], indent=2))


def make_boto3_client(endpoint_url: str) -> Any:
    """Sync boto3 Kinesis client wired to the mock."""
    import boto3
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    return boto3.client(
        "kinesis",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        verify=False,
    )


__all__ = [
    "BenchResult",
    "create_stream",
    "delete_stream",
    "make_boto3_client",
    "percentiles_ms",
    "print_table",
    "save_results_json",
    "spin_up_kinesis_mock",
    "wait_active",
]
