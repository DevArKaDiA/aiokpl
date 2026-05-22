"""Producer backpressure stress test.

Validates that ``max_outstanding_records`` keeps the in-flight count bounded
under sustained submission. A slow fake aiobotocore client (~50ms per
``put_records``) is injected so the semaphore actually saturates; a watcher
task samples ``producer.outstanding_records`` throughout the run.

Marked ``slow`` because it deliberately throttles for ~500 ms total to
expose the bounded-buffer invariant.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import anyio
import pytest

import aiokpl.producer as producer_mod
from aiokpl.config import Config
from aiokpl.producer import Producer


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _single_shard_response() -> dict[str, Any]:
    return {
        "Shards": [
            {
                "ShardId": "shardId-0",
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": str((1 << 128) - 1),
                },
            },
        ],
    }


class _SlowClient:
    """Async Kinesis client that sleeps ~50ms per put_records.

    Returns the success shape aligned to the inbound batch — the producer
    sends one ``Records`` entry per aggregated batch, so we just emit one
    OK slot per record in the request.
    """

    def __init__(self, per_call_delay_s: float = 0.05) -> None:
        self._delay = per_call_delay_s

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        await anyio.sleep(self._delay)
        n = len(kwargs.get("Records", []))
        return {
            "Records": [{"SequenceNumber": f"seq-{i}", "ShardId": "shardId-0"} for i in range(n)],
            "FailedRecordCount": 0,
        }

    async def list_shards(self, **kwargs: Any) -> dict[str, Any]:
        return _single_shard_response()

    async def close(self) -> None:
        return None


class _Ctx:
    def __init__(self, client: _SlowClient) -> None:
        self._client = client

    async def __aenter__(self) -> _SlowClient:
        return self._client

    async def __aexit__(self, *exc_info: Any) -> None:
        await self._client.close()


class _Session:
    def __init__(self, client: _SlowClient) -> None:
        self._client = client

    def create_client(self, service_name: str, **kwargs: Any) -> Any:
        assert service_name == "kinesis"
        return _Ctx(self._client)


@pytest.fixture
def slow_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[_SlowClient]:
    client = _SlowClient()
    session = _Session(client)
    monkeypatch.setattr(producer_mod.aiobotocore.session, "get_session", lambda: session)
    yield client


@pytest.mark.slow
@pytest.mark.integration
async def test_backpressure_stress(slow_client: _SlowClient) -> None:
    cap = 10
    n_records = 100

    cfg = Config(
        region="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        endpoint_url="https://example.invalid",
        verify_ssl=False,
        max_outstanding_records=cap,
        # No aggregation: each put_record needs a distinct in-flight slot.
        aggregation_enabled=False,
        # Tight buffered time so records flow immediately into the limiter.
        record_max_buffered_time_ms=5.0,
        record_ttl_ms=60_000.0,
    )

    samples: list[int] = []
    outcomes: list[Any] = []

    async with anyio.create_task_group() as outer:
        async with Producer(cfg) as producer:
            stop = anyio.Event()

            async def watcher() -> None:
                while not stop.is_set():
                    samples.append(producer.outstanding_records)
                    await anyio.sleep(0.005)

            outer.start_soon(watcher)

            t0 = anyio.current_time()
            for i in range(n_records):
                o = await producer.put_record(
                    stream="s",
                    partition_key=f"pk-{i}",
                    data=b"x",
                )
                outcomes.append(o)
            await producer.flush()
            with anyio.fail_after(30.0):
                results = [await o.wait() for o in outcomes]
            elapsed = anyio.current_time() - t0
            stop.set()

    assert len(results) == n_records
    assert all(r.success for r in results), [
        (r.success, r.attempts[-1].error_code if r.attempts else None)
        for r in results
        if not r.success
    ]

    # Peak in-flight must never exceed the configured cap. The +1 slack is
    # the moment between semaphore-acquire and ``_outstanding += 1`` (the
    # increment runs after the await returns; the value we read is the new
    # one, so cap is the hard upper bound on the *recorded* count).
    peak = max(samples) if samples else 0
    assert peak <= cap, f"in-flight peaked at {peak}, expected <= {cap}"
    # Sanity: the watcher saw the buffer fill at least once.
    assert peak >= 1, samples

    # With 100 records, cap=10, 50ms per send: even one batch in flight
    # means total >= ~50ms. We don't require the full 500ms because
    # aggregation_enabled=False still allows the collector to coalesce
    # multiple aggregated batches into a single PutRecords. Just confirm
    # we paid SOMETHING for the throttle.
    assert elapsed >= 0.05, f"submission finished too quickly ({elapsed:.3f}s)"
