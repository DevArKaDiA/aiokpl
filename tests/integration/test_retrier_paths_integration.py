"""End-to-end retrier paths: transient recovery, fail-fast throttle, expiry.

These three tests cover branches the Phase 6 unit tests touch with fake
:class:`SendOutcome` instances, but here we exercise the FULL Producer
pipeline — Sender, Retrier, callbacks, semaphore — by wrapping the
aiobotocore session so the Producer talks to a synthetic async client.

The "Wrong Shard" classification path is intentionally NOT covered here:
kinesis-mock honours hash-key routing (Phase 2 proved this byte-exact),
so we cannot force a wrong-shard response against a real backend without
also lying about the shard map. The unit tests in ``tests/test_retrier.py``
cover that branch with a hand-built ``PerRecordOutcome``; integration
coverage would not add value.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from typing import Any

import anyio
import pytest

import aiokpl.producer as producer_mod
from aiokpl.config import Config
from aiokpl.producer import Producer


@pytest.fixture
def anyio_backend() -> str:
    # aiobotocore is asyncio-only; same constraint as test_producer.py.
    return "asyncio"


# ────────────────────────────────────────────────────────────────────────────
# Synthetic aiobotocore session that yields a programmable client.
#
# We do NOT need kinesis-mock for these tests: the retrier classification
# branches under test are entirely a function of the response shape, and
# kinesis-mock by design returns happy-path responses. Mirroring the unit-test
# pattern from ``tests/test_producer.py`` here keeps the integration layer
# focused on lifecycle + wiring while still exercising every line on the
# put_record → Sender → Retrier → finish path against a Producer started in
# the normal ``async with`` shape.
# ────────────────────────────────────────────────────────────────────────────


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


class _ProgrammableClient:
    """An async Kinesis client with a queue of put_records behaviors.

    Each queued entry is either a ``dict`` (returned as the response), a
    ``BaseException`` (raised), or a ``callable(kwargs)`` (invoked).
    """

    def __init__(self) -> None:
        self.put_responses: list[Any] = []
        self.put_calls: list[dict[str, Any]] = []

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if not self.put_responses:
            raise RuntimeError("no queued put_records response")
        response = self.put_responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            value = response(kwargs)
            if inspect.isawaitable(value):
                value = await value
            return value
        return response

    async def list_shards(self, **kwargs: Any) -> dict[str, Any]:
        return _single_shard_response()

    async def close(self) -> None:
        return None


class _Ctx:
    def __init__(self, client: _ProgrammableClient) -> None:
        self._client = client

    async def __aenter__(self) -> _ProgrammableClient:
        return self._client

    async def __aexit__(self, *exc_info: Any) -> None:
        await self._client.close()


class _Session:
    def __init__(self, client: _ProgrammableClient) -> None:
        self._client = client

    def create_client(self, service_name: str, **kwargs: Any) -> Any:
        assert service_name == "kinesis"
        return _Ctx(self._client)


@pytest.fixture
def programmable_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[_ProgrammableClient]:
    client = _ProgrammableClient()
    session = _Session(client)
    monkeypatch.setattr(producer_mod.aiobotocore.session, "get_session", lambda: session)
    yield client


def _ok_record(seq: str, shard: str = "shardId-0") -> dict[str, str]:
    return {"SequenceNumber": seq, "ShardId": shard}


def _throttled_record() -> dict[str, str]:
    return {
        "ErrorCode": "ProvisionedThroughputExceededException",
        "ErrorMessage": "throttled",
    }


def _transient_record() -> dict[str, str]:
    return {"ErrorCode": "InternalFailure", "ErrorMessage": "transient"}


def _make_cfg(**overrides: Any) -> Config:
    base: dict[str, Any] = {
        "region": "us-east-1",
        "endpoint_url": "https://example.invalid",
        "verify_ssl": False,
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
        "record_max_buffered_time_ms": 5.0,
        "record_ttl_ms": 30_000.0,
        "aggregation_enabled": False,
        "max_outstanding_records": 50,
    }
    base.update(overrides)
    return Config(**base)


# ────────────────────────────────────────────────────────────────────────────
# 1. Transient per-record failure recovers on retry.
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_retry_recovers_from_transient_per_record_failure(
    programmable_client: _ProgrammableClient,
) -> None:
    # First call: the one record returns InternalFailure.
    # Second call (retry): success.
    programmable_client.put_responses.append(
        {"Records": [_transient_record()], "FailedRecordCount": 1}
    )
    programmable_client.put_responses.append(
        {"Records": [_ok_record("seq-retry-1")], "FailedRecordCount": 0}
    )

    cfg = _make_cfg(retry_deadline_ms=20.0)
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s1",
            partition_key="pk-1",
            data=b"payload",
        )
        await producer.flush()
        with anyio.fail_after(10.0):
            result = await outcome.wait()

    assert result.success, result.attempts
    assert result.sequence_number == "seq-retry-1"
    assert len(result.attempts) >= 2, [(a.success, a.error_code) for a in result.attempts]
    assert result.attempts[0].success is False
    assert result.attempts[0].error_code == "InternalFailure"
    assert result.attempts[-1].success is True


# ────────────────────────────────────────────────────────────────────────────
# 2. fail_if_throttled=True surfaces throttle as terminal on first attempt.
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_throttle_with_fail_if_throttled_true(
    programmable_client: _ProgrammableClient,
) -> None:
    # Queue enough responses for 3 records; we expect ONE call covering all
    # three. Pre-queue several copies in case the producer issues separate
    # PutRecords per record under aggregation_enabled=False — either path
    # ends with len(attempts) == 1 on every outcome.
    for _ in range(5):
        programmable_client.put_responses.append(
            {
                "Records": [_throttled_record(), _throttled_record(), _throttled_record()],
                "FailedRecordCount": 3,
            }
        )
        programmable_client.put_responses.append(
            {"Records": [_throttled_record()], "FailedRecordCount": 1}
        )

    cfg = _make_cfg(fail_if_throttled=True)
    outcomes: list[Any] = []
    async with Producer(cfg) as producer:
        for i in range(3):
            o = await producer.put_record(
                stream="s2",
                partition_key=f"pk-{i}",
                data=b"x",
            )
            outcomes.append(o)
        await producer.flush()
        with anyio.fail_after(10.0):
            results = [await o.wait() for o in outcomes]

    assert len(results) == 3
    for r in results:
        assert r.success is False, r
        assert len(r.attempts) == 1, [(a.success, a.error_code) for a in r.attempts]
        assert r.attempts[0].error_code == "ProvisionedThroughputExceededException"


# ────────────────────────────────────────────────────────────────────────────
# 3. Records expire via record_ttl_ms when the client hangs.
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_expired_records_surface_with_expired_code(
    programmable_client: _ProgrammableClient,
) -> None:
    # Each put_records sleeps long enough that record_ttl_ms is exceeded by
    # the time the response comes back. The Retrier's _retry_not_expired
    # branch should then classify the next attempt as Expired.

    async def slow_response(_kwargs: dict[str, Any]) -> dict[str, Any]:
        await anyio.sleep(0.5)
        # On the second call (if retried), return a transient so the
        # retrier loops once more and trips the TTL check.
        return {"Records": [_transient_record()], "FailedRecordCount": 1}

    # Pre-queue enough slow responses for whatever retries happen.
    for _ in range(10):
        programmable_client.put_responses.append(slow_response)

    cfg = _make_cfg(
        record_ttl_ms=200.0,
        retry_deadline_ms=20.0,
    )
    async with Producer(cfg) as producer:
        outcome = await producer.put_record(
            stream="s3",
            partition_key="pk-exp",
            data=b"x",
        )
        await producer.flush()
        with anyio.fail_after(5.0):
            result = await outcome.wait()

    assert result.success is False
    assert result.attempts, "expected at least one attempt"
    assert result.attempts[-1].error_code == "Expired", [
        (a.success, a.error_code) for a in result.attempts
    ]
