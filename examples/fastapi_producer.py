"""FastAPI REST producer that pushes events to Kinesis via aiokpl.

Two endpoints showing the two patterns most apps want:

* ``POST /events/confirmed`` — synchronous: the request awaits Kinesis's
  ``PutRecords`` response and returns ``shard_id``/``sequence_number``. Use
  this when the caller needs strong "your event is in Kinesis" semantics
  (e.g., a webhook that retries on non-2xx).
* ``POST /events`` — fire-and-forget: returns 202 immediately and lets a
  background task observe the outcome (so a failure becomes a log line, not
  a dropped record). Use this when the caller is a hot ingest path and you
  can tolerate "best-effort delivery, observable on the server side".

Run it::

    pip install "aiokpl" fastapi 'uvicorn[standard]'
    AIOKPL_REGION=us-east-1 AIOKPL_STREAM=events \\
        uvicorn examples.fastapi_producer:app --reload

Set ``AIOKPL_ENDPOINT_URL`` to a kinesis-mock or LocalStack endpoint for
local development.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from aiokpl import Config, Outcome, Producer, RecordResult

log = logging.getLogger("aiokpl.example.fastapi")


# ─── Pydantic request / response shapes ───────────────────────────────────


class EventIn(BaseModel):
    """One event the client wants to push."""

    partition_key: str = Field(..., min_length=1, max_length=256)
    payload: dict = Field(..., description="Arbitrary JSON payload — serialized to bytes.")


class EventAccepted(BaseModel):
    accepted: bool = True


class EventConfirmed(BaseModel):
    shard_id: str
    sequence_number: str
    attempts: int


# ─── Producer lifecycle (one instance per app, drained on shutdown) ───────


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open one ``Producer`` for the life of the app.

    ``async with Producer(cfg)`` does the heavy lifting: opens an aiobotocore
    Kinesis client, spawns the per-stage background tasks, and on exit drains
    everything in flight before tearing down.
    """
    cfg = Config(
        region=os.environ.get("AIOKPL_REGION", "us-east-1"),
        endpoint_url=os.environ.get("AIOKPL_ENDPOINT_URL") or None,
        verify_ssl=os.environ.get("AIOKPL_VERIFY_SSL", "true").lower() != "false",
        # ~100 ms of buffered time is plenty for a REST front-end: short enough
        # that confirmed-mode latency stays under 200 ms P99, long enough that
        # the aggregator pays for itself under sustained load.
        record_max_buffered_time_ms=100.0,
        record_ttl_ms=30_000.0,
        # Backpressure cap. If the ingress rate exceeds what Kinesis can
        # accept, FastAPI will park requests on the semaphore — the load
        # balancer will see latency before drops. Tune for your fan-in.
        max_outstanding_records=10_000,
    )
    async with Producer(cfg) as producer:
        app.state.producer = producer
        app.state.stream = os.environ["AIOKPL_STREAM"]
        yield


app = FastAPI(title="aiokpl FastAPI example", lifespan=lifespan)


# ─── Helpers ──────────────────────────────────────────────────────────────


def _serialize(payload: dict) -> bytes:
    # FastAPI's response_class would JSON-encode for the client; for Kinesis
    # we pick the wire format. Plain JSON keeps it readable in downstream
    # consumers. Swap for msgpack/proto if your downstream prefers binary.
    import json

    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


async def _log_failure_if_any(outcome: Outcome[RecordResult], partition_key: str) -> None:
    """Background task: wait for the outcome and log on failure.

    Runs after the HTTP response is sent. If the record eventually fails
    (retries exhausted, TTL expired, throttle with fail_if_throttled), the
    failure ends up in your application logs with the partition key so you
    can replay it from upstream.
    """
    result = await outcome.wait()
    if not result.success:
        last = result.attempts[-1] if result.attempts else None
        log.error(
            "aiokpl record failed pk=%s code=%s message=%s attempts=%d",
            partition_key,
            last.error_code if last else "Unknown",
            last.error_message if last else "",
            len(result.attempts),
        )


# ─── Endpoints ────────────────────────────────────────────────────────────


@app.post("/events/confirmed", response_model=EventConfirmed)
async def post_event_confirmed(evt: EventIn) -> EventConfirmed:
    """Synchronous: await the outcome before returning.

    The client gets ``shard_id`` and ``sequence_number`` proving the record
    is durable in Kinesis. P99 latency is dominated by the producer's
    buffered time + the AWS round-trip; with the defaults above expect
    ~100-200 ms.
    """
    producer: Producer = app.state.producer
    outcome = await producer.put_record(
        stream=app.state.stream,
        partition_key=evt.partition_key,
        data=_serialize(evt.payload),
    )
    result = await outcome.wait()
    if not result.success:
        last = result.attempts[-1] if result.attempts else None
        # 502 because the upstream (Kinesis) rejected. The client should
        # retry — the response carries the full attempt history so an
        # operator can see what happened.
        raise HTTPException(
            status_code=502,
            detail={
                "error_code": last.error_code if last else "Unknown",
                "error_message": last.error_message if last else "",
                "attempts": len(result.attempts),
            },
        )
    return EventConfirmed(
        shard_id=result.shard_id or "",
        sequence_number=result.sequence_number or "",
        attempts=len(result.attempts),
    )


@app.post("/events", status_code=202, response_model=EventAccepted)
async def post_event(evt: EventIn, bg: BackgroundTasks) -> EventAccepted:
    """Fire-and-forget: 202 Accepted immediately.

    The producer accepts the record (acquires the backpressure semaphore,
    enqueues into the aggregator) and we hand off observation of the
    outcome to a background task. If the record eventually fails we log it;
    the client never sees the error.

    Important: ``put_record`` itself may still ``await`` briefly on the
    backpressure semaphore if ``max_outstanding_records`` is saturated.
    That's the right behavior — backpressure surfaces as request latency
    rather than dropped data.
    """
    producer: Producer = app.state.producer
    outcome = await producer.put_record(
        stream=app.state.stream,
        partition_key=evt.partition_key,
        data=_serialize(evt.payload),
    )
    bg.add_task(_log_failure_if_any, outcome, evt.partition_key)
    return EventAccepted()


@app.get("/health")
async def health() -> dict[str, int | bool]:
    """Liveness + a peek at in-flight pressure for ops dashboards."""
    producer: Producer = app.state.producer
    return {"ok": True, "outstanding_records": producer.outstanding_records}
