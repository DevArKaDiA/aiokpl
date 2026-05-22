# Phase 8 — SyncProducer bridge

**Status:** Done.

Most of aiokpl is asynchronous because the C++ KPL's pipeline maps cleanly
onto `anyio` primitives — that's the right shape for the library. But not
every caller has an event loop. Scripts, Flask/Django request handlers,
Jupyter cells, Celery tasks — these are blocking code, and asking them to
restructure their world just to publish a record to Kinesis is a tax we'd
rather not levy.

`SyncProducer` is the bridge. It runs the async [`Producer`][aiokpl.Producer]
on a dedicated background event loop and exposes a normal synchronous API
over the top.

## Lifecycle

```python
from aiokpl import Config, SyncProducer

cfg = Config(region="us-east-1", record_max_buffered_time_ms=100)
with SyncProducer(cfg) as producer:
    outcome = producer.put_record(
        stream="my-stream",
        partition_key="user-123",
        data=b"hello",
    )
    result = outcome.wait(timeout=5.0)
    if result.success:
        print(result.shard_id, result.sequence_number)
```

The context manager owns a private `anyio.from_thread.BlockingPortal` that
runs an `anyio` event loop on one background thread, plus a long-lived
dispatcher task on that loop that holds the async `Producer`. On `__exit__` the dispatcher drains in-flight
records (subject to a 30-second default flush timeout), runs the async
`Producer.__aexit__`, then shuts the portal down.

## Thread-safety

[`SyncProducer.put_record`][aiokpl.SyncProducer.put_record] is safe to call
concurrently from any number of OS threads. Each call enqueues a command
onto the dispatcher's internal stream; the dispatcher serializes them onto
the event loop. There's exactly one event loop and exactly one task driving
the Producer, so cancel-scope binding stays coherent.

## Why a portal + dispatcher, not raw threading

We considered spawning a `threading.Thread` and calling `asyncio.run` inside
it. `anyio.from_thread.start_blocking_portal` already does that — picks a
backend (`asyncio` or `trio`), boots a loop on a worker thread, exposes
`portal.call` for thread-safe dispatch, and tears everything down on exit.
Hand-rolling it would duplicate logic anyio has already debugged.

What we **did** have to hand-roll is the dispatcher task. `anyio.TaskGroup`
and `anyio.CancelScope` bind to the task that opened them. The async
`Producer` lazily creates per-stream pipelines (each with its own
`TaskGroup`) on the first `put_record`; if those creations happened inside
whichever ad-hoc task `portal.call` spawned for that operation, the stages
couldn't be cleanly exited later from the producer's owning task. The
dispatcher fixes this by ensuring every operation runs in the same task as
the `Producer.__aenter__` that created it.

## SyncOutcome semantics

```python
outcome = producer.put_record(...)

outcome.done()                         # True iff resolved
outcome.wait()                         # block forever
outcome.wait(timeout=5.0)              # block up to 5 s, raise TimeoutError
outcome.cancel()                       # resolve locally with SyncOutcomeCancelled
```

`cancel()` is a **local** operation — it does not stop the in-flight Kinesis
request (we can't; aiobotocore is mid-await). It resolves the local handle
so a thread blocked in `wait()` unblocks with
[`SyncOutcomeCancelled`][aiokpl.sync.SyncOutcomeCancelled]. Re-cancelling
returns `False`. We use a dedicated exception type rather than
`asyncio.CancelledError` because `concurrent.futures` treats the latter as
future-cancellation and would surface it as
`concurrent.futures.CancelledError` instead of propagating through normal
exception handling.

## flush()

```python
producer.flush()                   # block until in-flight reaches 0, forever
producer.flush(timeout=10.0)       # bounded; raise TimeoutError on expiry
```

`flush` kicks every pipeline's aggregator → limiter → collector, then polls
`outstanding_records` until it drops to zero. The polling cadence is 10 ms;
fine-grained enough for tests, coarse enough to keep CPU cost negligible.

## Example: Flask handler

```python
from flask import Flask, jsonify, request
from aiokpl import Config, SyncProducer

app = Flask(__name__)
producer = SyncProducer(Config(region="us-east-1"))
producer.__enter__()  # lifecycle tied to the Flask app

@app.route("/event", methods=["POST"])
def event():
    payload = request.get_data()
    outcome = producer.put_record(
        stream="events",
        partition_key=request.headers.get("X-User", "anon"),
        data=payload,
    )
    result = outcome.wait(timeout=2.0)
    return jsonify(
        success=result.success,
        shard_id=result.shard_id,
        sequence_number=result.sequence_number,
        attempts=len(result.attempts),
    )
```

Hook `producer.__exit__(None, None, None)` into Flask's `atexit` or a
teardown handler so the background loop shuts down cleanly.

## Backend parameter

```python
SyncProducer(config, backend="asyncio")   # default
SyncProducer(config, backend="trio")      # accepted but will fail at __enter__
```

The `backend` argument is forwarded to
`anyio.from_thread.start_blocking_portal`. `aiobotocore` (the HTTP client
the async `Producer` uses) is asyncio-only, so passing `"trio"` will fail
when the producer tries to import its session. The parameter exists for
future flexibility — if a Kinesis client emerges that's trio-friendly, the
sync bridge is already prepared for it.
