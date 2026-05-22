# FastAPI producer

A common use case for `aiokpl` is a REST front-end that accepts events from
clients (web app, mobile, server-to-server webhooks) and forwards them to a
Kinesis data stream. FastAPI is the natural pairing: both are
asyncio-native, both lean hard on dataclasses, and FastAPI's `lifespan`
context manager is exactly the shape `Producer.__aenter__` was designed
for.

The full example lives at [`examples/fastapi_producer.py`][source] in the
repo. This page walks through the design decisions.

[source]: https://github.com/DevArKaDiA/aiokpl/blob/main/examples/fastapi_producer.py

## Two endpoints, two patterns

REST producers usually want one of two semantics — and most real apps
expose both:

| Semantics | Endpoint | When to use |
|---|---|---|
| **Confirmed** — the client waits until Kinesis has the record | `POST /events/confirmed` → 200 with `shard_id`/`sequence_number` | Webhooks. Anything where the caller will retry on non-2xx and you want at-least-once with proof. |
| **Fire-and-forget** — return immediately, log failures server-side | `POST /events` → 202 Accepted | Hot ingest. Mobile telemetry. Anything where latency dominates and the upstream can't usefully retry. |

Both endpoints use the **same** `Producer` instance. There's no "async
producer" vs "fast producer" — the only difference is whether the request
handler awaits the `Outcome` or hands it to a background task.

## Lifespan: one `Producer` per app

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    cfg = Config(
        region=os.environ.get("AIOKPL_REGION", "us-east-1"),
        endpoint_url=os.environ.get("AIOKPL_ENDPOINT_URL") or None,
        record_max_buffered_time_ms=100.0,
        record_ttl_ms=30_000.0,
        max_outstanding_records=10_000,
    )
    async with Producer(cfg) as producer:
        app.state.producer = producer
        app.state.stream = os.environ["AIOKPL_STREAM"]
        yield


app = FastAPI(lifespan=lifespan)
```

`async with Producer(cfg)` opens the aiobotocore Kinesis client, spawns the
per-stage background tasks (aggregator timer, limiter drain loop,
shard-map refresh), and on exit drains in-flight records before tearing
the client down. FastAPI invokes the post-`yield` code on a graceful
shutdown signal, so SIGTERM from your orchestrator triggers a clean flush
— not a record dropped.

!!! note "One Producer handles every stream"
    The Producer creates per-stream pipelines lazily on the first
    `put_record(stream=…)`. You don't need (and shouldn't have) one
    Producer per stream — that wastes connections and breaks the
    backpressure semaphore.

## Confirmed mode: `await outcome.wait()`

```python
@app.post("/events/confirmed", response_model=EventConfirmed)
async def post_event_confirmed(evt: EventIn) -> EventConfirmed:
    producer: Producer = app.state.producer
    outcome = await producer.put_record(
        stream=app.state.stream,
        partition_key=evt.partition_key,
        data=_serialize(evt.payload),
    )
    result = await outcome.wait()
    if not result.success:
        last = result.attempts[-1] if result.attempts else None
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
```

Two awaits worth noticing:

1. `await producer.put_record(...)` returns the `Outcome` quickly — it
   only blocks if the backpressure semaphore is full. The record is now in
   the pipeline.
2. `await outcome.wait()` blocks until the Retrier marks the record done
   (success or terminal failure). With `record_max_buffered_time_ms=100`
   and a healthy stream this resolves in 100-200 ms.

On failure we return **502 Bad Gateway** — Kinesis (the upstream) rejected.
The body carries the full attempt history so an operator can tell a
throttle from an expired record from a transient AWS hiccup.

!!! warning "Long-tail latency"
    A single record's outcome includes all retries. A persistently
    throttled shard can keep it in the pipeline up to `record_ttl_ms`
    (default 30 s). If your confirmed clients have a tight HTTP timeout,
    drop `record_ttl_ms` to match, or use fire-and-forget.

## Fire-and-forget: `BackgroundTasks` + outcome observer

```python
@app.post("/events", status_code=202, response_model=EventAccepted)
async def post_event(evt: EventIn, bg: BackgroundTasks) -> EventAccepted:
    producer: Producer = app.state.producer
    outcome = await producer.put_record(
        stream=app.state.stream,
        partition_key=evt.partition_key,
        data=_serialize(evt.payload),
    )
    bg.add_task(_log_failure_if_any, outcome, evt.partition_key)
    return EventAccepted()


async def _log_failure_if_any(outcome: Outcome[RecordResult], partition_key: str) -> None:
    result = await outcome.wait()
    if not result.success:
        last = result.attempts[-1] if result.attempts else None
        log.error(
            "aiokpl record failed pk=%s code=%s message=%s attempts=%d",
            partition_key, last.error_code if last else "Unknown",
            last.error_message if last else "", len(result.attempts),
        )
```

The pattern:

1. `await put_record` to enqueue.
2. Register a background task that will `await outcome.wait()` *after* the
   response is sent.
3. Return 202 immediately.

A few details that matter:

- **`BackgroundTasks` runs the function after the response.** The `await`
  inside `_log_failure_if_any` happens off the request's critical path.
- **Outcomes are never dropped silently.** Even though the client gets a
  202, a failure shows up in your structured logs with the partition key.
  Pipe those logs into your existing alerting and you have observability
  without coupling the client to Kinesis.
- **Backpressure is not bypassed.** If `max_outstanding_records` is
  saturated, `put_record` still blocks. The client experiences this as
  request latency, not as a 5xx. That's the right tradeoff — better to
  apply load shedding upstream than to silently drop records.

## Health endpoint

```python
@app.get("/health")
async def health() -> dict[str, int | bool]:
    producer: Producer = app.state.producer
    return {"ok": True, "outstanding_records": producer.outstanding_records}
```

`producer.outstanding_records` is the live count of records inside the
pipeline. Plot it. When it approaches `max_outstanding_records` you're
backpressuring — Kinesis is your bottleneck and either the stream needs
more shards or your downstream consumer is falling behind.

## Configuration knobs worth tuning per workload

| Knob | Default | Bump if… |
|---|---|---|
| `record_max_buffered_time_ms` | 100 | You want lower latency in confirmed mode — drop to 20-50. Or higher throughput on bursty traffic — raise to 250. |
| `record_ttl_ms` | 30_000 | Confirmed clients have tight timeouts. Match it. |
| `max_outstanding_records` | 100_000 | Memory pressure visible in `outstanding_records` near the cap. Raise it, OR add shards. |
| `aggregation_enabled` | `True` | Your records are already > 50 KB each — aggregation overhead doesn't pay. Set to `False`. |
| `fail_if_throttled` | `False` | You'd rather surface throttle errors to the client immediately than retry silently. Set to `True`. |

The full table lives in [Configuration](../getting-started.md#configuration).

## Running locally against `kinesis-mock`

```bash
# 1. Spin up kinesis-mock
docker run -d --name kinesis-mock -p 4567:4567 -p 4568:4568 \
    ghcr.io/etspaceman/kinesis-mock:0.5.2

# 2. Create the stream
AWS_ACCESS_KEY_ID=t AWS_SECRET_ACCESS_KEY=t AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url https://localhost:4567 --no-verify-ssl \
    kinesis create-stream --stream-name events --shard-count 4

# 3. Run the app
AIOKPL_REGION=us-east-1 \
AIOKPL_STREAM=events \
AIOKPL_ENDPOINT_URL=https://localhost:4567 \
AIOKPL_VERIFY_SSL=false \
AWS_ACCESS_KEY_ID=t AWS_SECRET_ACCESS_KEY=t \
    uvicorn examples.fastapi_producer:app --reload

# 4. Push an event
curl -X POST http://127.0.0.1:8000/events/confirmed \
    -H 'content-type: application/json' \
    -d '{"partition_key":"user-42","payload":{"event":"signup"}}'
```

## What's not in this example (yet)

- **Authentication / authorization.** Add your usual FastAPI dependencies.
- **Schema versioning.** The example serializes the request payload as JSON
  verbatim — in production you'd want a versioned envelope.
- **Metrics.** Pair the producer with a `MetricsSink` (see
  [Custom sinks](../dev/sinks.md)). For a FastAPI app, `OpenTelemetrySink`
  composes naturally with FastAPI's OTel instrumentation.
- **Sync workers.** If part of your stack is sync (Celery, RQ),
  [`SyncProducer`](../getting-started.md#quick-start-synchronous) is a
  drop-in.
