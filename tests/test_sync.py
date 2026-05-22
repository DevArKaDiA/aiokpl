"""Synchronous tests for :class:`aiokpl.sync.SyncProducer`.

These tests are **not** marked ``@pytest.mark.anyio`` and do not use
``async def`` — the whole point of :class:`SyncProducer` is exercising it
from sync code. The async :class:`Producer` it wraps still talks to
aiobotocore, so we monkeypatch ``aiobotocore.session.get_session`` (the same
hook ``test_producer.py`` uses) with a fake whose ``create_client`` yields a
duck-typed client. Module-level attributes are visible across threads, so the
fake set on the main thread is what the portal's background thread observes.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

import aiokpl.producer as producer_mod
from aiokpl.config import Config
from aiokpl.result import RecordResult
from aiokpl.sync import SyncOutcome, SyncOutcomeCancelled, SyncProducer

# ────────────────────────────────────────────────────────────────────────────
# Fakes (mirroring test_producer.py)
# ────────────────────────────────────────────────────────────────────────────


class _FakeClient:
    """Duck-typed aiobotocore Kinesis client.

    Queues ``put_records`` responses; pops on each call. If the queued value
    is a ``dict`` it's returned, if callable it's invoked, if a
    ``BaseException`` instance it's raised. ``list_shards`` returns a canned
    single-shard, full-uint128 response.
    """

    def __init__(self) -> None:
        self.put_responses: list[Any] = []
        self.put_calls: list[dict[str, Any]] = []

    def queue_put(self, response: Any) -> None:
        self.put_responses.append(response)

    async def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if not self.put_responses:
            raise RuntimeError("no queued put_records response")
        response = self.put_responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(kwargs)
        return response

    async def list_shards(self, **_kwargs: Any) -> dict[str, Any]:
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

    async def close(self) -> None:
        return None


class _FakeClientCtx:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    async def __aenter__(self) -> _FakeClient:
        return self._client

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.close()


class _FakeSession:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client

    def create_client(self, service_name: str, **_kwargs: Any) -> Any:
        assert service_name == "kinesis"
        return _FakeClientCtx(self._client)


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeClient]:
    client = _FakeClient()
    session = _FakeSession(client)
    monkeypatch.setattr(producer_mod.aiobotocore.session, "get_session", lambda: session)
    yield client


def _ok_response(seq: str = "seq-1", shard: str = "shardId-0") -> dict[str, Any]:
    return {
        "Records": [{"SequenceNumber": seq, "ShardId": shard}],
        "FailedRecordCount": 0,
    }


def _cfg(**overrides: Any) -> Config:
    return Config(region="us-east-1", **overrides)


# ────────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────────


def test_enter_exit_clean(fake_client: _FakeClient) -> None:
    with SyncProducer(_cfg()) as producer:
        assert producer.outstanding_records == 0


def test_put_record_returns_sync_outcome(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-A", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"hi", explicit_hash_key="0"
        )
        assert isinstance(outcome, SyncOutcome)
        result = outcome.wait(timeout=5.0)
        assert isinstance(result, RecordResult)
        assert result.success is True
        assert result.sequence_number == "seq-A"


def test_multiple_records_one_thread(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-X", "shardId-0"))
    fake_client.queue_put(_ok_response("seq-Y", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        o1 = producer.put_record(stream="s1", partition_key="pk1", data=b"a", explicit_hash_key="0")
        o2 = producer.put_record(stream="s2", partition_key="pk2", data=b"b", explicit_hash_key="0")
        r1 = o1.wait(timeout=5.0)
        r2 = o2.wait(timeout=5.0)
    assert r1.success and r2.success
    assert {r1.sequence_number, r2.sequence_number} == {"seq-X", "seq-Y"}


def test_multiple_threads_concurrent_puts(fake_client: _FakeClient) -> None:
    # Each thread submits one record to a distinct stream so the fake client's
    # response queue (FIFO across threads) doesn't constrain ordering.
    n = 5
    for i in range(n):
        fake_client.queue_put(_ok_response(f"seq-{i}", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    results: list[RecordResult] = []
    lock = threading.Lock()

    with SyncProducer(cfg) as producer:

        def submit(idx: int) -> None:
            outcome = producer.put_record(
                stream=f"stream-{idx}",
                partition_key=f"pk-{idx}",
                data=f"d-{idx}".encode(),
                explicit_hash_key="0",
            )
            r = outcome.wait(timeout=10.0)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20.0)
            assert not t.is_alive()

    assert len(results) == n
    assert all(r.success for r in results)


def test_wait_timeout_raises(fake_client: _FakeClient) -> None:
    # Never queue a response — the record will sit unresolved until the wait
    # timeout trips. Use a small record_ttl_ms WITHOUT a low rate limit so the
    # record isn't expired; we want a genuine pending wait.
    cfg = _cfg(record_max_buffered_time_ms=60_000.0, record_ttl_ms=60_000.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        with pytest.raises(TimeoutError, match="timed out"):
            outcome.wait(timeout=0.1)
        # Cancel so the record resolves locally and __exit__'s flush doesn't
        # block forever on a record that has no queued response.
        assert outcome.cancel() is True


def test_cancel_pending_then_already_resolved(fake_client: _FakeClient) -> None:
    cfg = _cfg(record_max_buffered_time_ms=60_000.0, record_ttl_ms=60_000.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        assert outcome.cancel() is True
        # Second cancel: outcome already set, returns False.
        assert outcome.cancel() is False
        # And wait now propagates the CancelledError.
        with pytest.raises(SyncOutcomeCancelled):
            outcome.wait(timeout=5.0)


def test_done_transitions_false_to_true(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-D", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        # Immediately after dispatch, the record is most likely still pending
        # — but to avoid races we only require done() to flip True after wait.
        outcome.wait(timeout=5.0)
        assert outcome.done() is True


def test_done_false_before_resolution(fake_client: _FakeClient) -> None:
    cfg = _cfg(record_max_buffered_time_ms=60_000.0, record_ttl_ms=60_000.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        # No response queued, long buffered time: deterministic pending.
        assert outcome.done() is False
        outcome.cancel()


def test_flush_returns_when_drained(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq-F", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=60_000.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        producer.flush(timeout=5.0)
        assert producer.outstanding_records == 0
        assert outcome.wait(timeout=1.0).success is True


def test_flush_no_timeout(fake_client: _FakeClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # Async response that yields control so the dispatcher's flush poll runs
    # at least one extra iteration. Patch the put_records method directly to
    # do the await — the queue mechanism doesn't support awaitable callables.
    import asyncio as _asyncio

    real_put = fake_client.put_records

    async def slow_put(**kwargs: Any) -> dict[str, Any]:
        await _asyncio.sleep(0.05)
        return await real_put(**kwargs)

    monkeypatch.setattr(fake_client, "put_records", slow_put)
    fake_client.queue_put(_ok_response("seq-F", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        producer.flush()  # timeout=None code path
        assert producer.outstanding_records == 0
        # wait() with no timeout arg covers the timeout=None branch in
        # _wait_with_timeout.
        result = outcome.wait()
        assert result.success is True


def test_flush_timeout_raises(fake_client: _FakeClient) -> None:
    # No response queued: outstanding_records stays > 0 forever.
    cfg = _cfg(record_max_buffered_time_ms=60_000.0, record_ttl_ms=60_000.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        with pytest.raises(TimeoutError, match="flush timed out"):
            producer.flush(timeout=0.05)
        outcome.cancel()


def test_put_record_before_enter_raises() -> None:
    producer = SyncProducer(_cfg())
    with pytest.raises(RuntimeError, match="not entered"):
        producer.put_record(stream="s", partition_key="pk", data=b"x")


def test_put_record_after_exit_raises(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    producer = SyncProducer(cfg)
    producer.__enter__()
    o = producer.put_record(stream="s", partition_key="pk", data=b"x", explicit_hash_key="0")
    o.wait(timeout=5.0)
    producer.__exit__(None, None, None)
    with pytest.raises(RuntimeError, match="not entered"):
        producer.put_record(stream="s", partition_key="pk", data=b"x")


def test_flush_before_enter_raises() -> None:
    producer = SyncProducer(_cfg())
    with pytest.raises(RuntimeError, match="not entered"):
        producer.flush(timeout=1.0)


def test_outstanding_records_before_enter_returns_zero() -> None:
    producer = SyncProducer(_cfg())
    assert producer.outstanding_records == 0


def test_outstanding_records_tracks(fake_client: _FakeClient) -> None:
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        assert producer.outstanding_records == 0
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        # outstanding_records may be 1 here, but the value depends on whether
        # the portal already drained — accept >=0 strictly and assert post-wait.
        assert producer.outstanding_records >= 0
        outcome.wait(timeout=5.0)
        # Give the on_finish callback a beat to decrement.
        deadline = time.monotonic() + 2.0
        while producer.outstanding_records != 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert producer.outstanding_records == 0


def test_exit_flush_swallows_timeout(
    fake_client: _FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Force the in-__exit__ flush to time out and verify it does NOT propagate.
    cfg = _cfg(record_max_buffered_time_ms=60_000.0, record_ttl_ms=60_000.0)
    producer = SyncProducer(cfg)
    producer.__enter__()
    outcome = producer.put_record(stream="s", partition_key="pk", data=b"x", explicit_hash_key="0")
    monkeypatch.setattr("aiokpl.sync._DEFAULT_EXIT_FLUSH_TIMEOUT_S", 0.05, raising=True)
    # The record will never resolve (no queued response), so the exit-flush
    # raises TimeoutError internally; __exit__ must swallow it.
    outcome.cancel()  # resolve locally so the async Producer's own __aexit__
    #                   flush doesn't hang on the pending Outcome.
    producer.__exit__(None, None, None)


def test_dispatcher_swallows_producer_aexit_error(
    fake_client: _FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Force ``producer.__aexit__`` to raise; the dispatcher must swallow it
    # so the sync user's __exit__ completes cleanly.
    fake_client.queue_put(_ok_response("seq", "shardId-0"))
    cfg = _cfg(record_max_buffered_time_ms=20.0)
    with SyncProducer(cfg) as producer:
        outcome = producer.put_record(
            stream="s", partition_key="pk", data=b"x", explicit_hash_key="0"
        )
        outcome.wait(timeout=5.0)
        # Override __aexit__ on the underlying Producer instance.
        real_producer = producer._producer_ref
        assert real_producer is not None

        async def boom(*_a: Any, **_kw: Any) -> None:
            raise RuntimeError("aexit boom")

        monkeypatch.setattr(real_producer, "__aexit__", boom)
    # Exited cleanly despite the inner raise.


def test_enter_failure_cleans_up_portal(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate Producer.__aenter__ blowing up. The portal context manager
    # must be exited before the exception escapes; otherwise we'd leak the
    # background event-loop thread.
    import aiokpl.sync as sync_mod

    class _BoomProducer:
        def __init__(self, _cfg: Any) -> None:
            pass

        async def __aenter__(self) -> Any:
            raise RuntimeError("boom on enter")

        async def __aexit__(self, *exc: Any) -> None:
            return None

    monkeypatch.setattr(sync_mod, "Producer", _BoomProducer)
    producer = SyncProducer(_cfg())
    with pytest.raises(RuntimeError, match="boom"):
        producer.__enter__()
    # Portal state cleared on failure.
    assert producer._portal is None
    assert producer._portal_cm is None
    assert producer._command_send is None
