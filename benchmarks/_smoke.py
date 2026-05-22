"""Tiny smoke test — verifies harness + each variant runs end-to-end.

Not part of the deliverable; can be deleted. Useful for debugging.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid

from benchmarks._harness import (
    create_stream,
    delete_stream,
    make_boto3_client,
    spin_up_kinesis_mock,
    wait_active,
)
from benchmarks.bench_throughput import (
    _aiokpl_async,
    _aiokpl_sync,
    _boto3_kinesis_agg,
    _boto3_raw,
)


def main() -> int:
    endpoint, container = spin_up_kinesis_mock()
    print(f"endpoint={endpoint}")
    try:
        boto = make_boto3_client(endpoint)
        stream = f"smoke-{uuid.uuid4().hex[:8]}"
        create_stream(boto, stream, 2)
        wait_active(boto, stream)
        try:
            print("aiokpl async agg=on")
            e, lats = asyncio.run(_aiokpl_async(endpoint, stream, 100, aggregation=True))
            print(f"  ok: {e:.3f}s, {len(lats)} resolved")
            print("aiokpl async agg=off")
            e, lats = asyncio.run(_aiokpl_async(endpoint, stream, 100, aggregation=False))
            print(f"  ok: {e:.3f}s, {len(lats)} resolved")
            print("aiokpl sync agg=on")
            e, lats = _aiokpl_sync(endpoint, stream, 100, aggregation=True)
            print(f"  ok: {e:.3f}s, {len(lats)} resolved")
            print("boto3 raw")
            e, lats = _boto3_raw(endpoint, stream, 100)
            print(f"  ok: {e:.3f}s, {len(lats)} resolved")
            print("aws-kinesis-agg")
            e, lats = _boto3_kinesis_agg(endpoint, stream, 100)
            print(f"  ok: {e:.3f}s, {len(lats)} resolved")
        finally:
            delete_stream(boto, stream)
    finally:
        with contextlib.suppress(Exception):
            container.stop(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
