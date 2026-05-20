# Why aiokpl

## The gap

AWS ships the official Kinesis Producer Library as a native C++ binary
(`amazon-kinesis-producer`) wrapped in Java/.NET sidecars. The Python
ecosystem has never had a real KPL — only:

- **`aws-kinesis-agg`** — Python codec for the aggregation wire format.
  Useful, but not a producer. You still call `boto3.put_records` yourself,
  you still pick the partition key, you still write the retry loop, and you
  still have no idea which shard your record landed on.
- **`kiner`, `kinesis-python`, `kinesis-producer` (ludia)** — abandoned
  community attempts. All thin batchers over `boto3`. None of them predict
  the destination shard, none of them limit per-shard, none of them classify
  retries, none of them carry a per-record attempt history back to the
  caller.

`aiokpl` is a clean-room reimplementation in idiomatic async Python — built
on `anyio` so the same code runs on both the `asyncio` and `trio` runtimes
— that preserves what is worth preserving from the C++ KPL (shard-aware
pipeline, deadline-driven batching, smart retry classification, byte-exact
aggregation) and drops what was an accident of C++: IPC, named pipes, child
process, custom spinlocks, static binaries, packaging hell.

It is not a wrapper around the C++ binary. It is a reimplementation of its
ideas in a language where you do not need a daemon.

## What about LocalStack / `boto3` batching?

A reasonable question: if `aws-kinesis-agg` already implements the wire
format and `boto3` already implements `PutRecords`, what is left?

A lot, actually.

- **`aws-kinesis-agg` is the wire format, not the producer.** It encodes one
  blob from a list of records you already collected. It does not know which
  shard a record will land on, does not group records by predicted shard,
  does not know that Kinesis enforces 1 MiB/s and 1000 records/s **per
  shard** (not per stream), and does not handle the wrong-shard-after-split
  case where a record routes to the parent of a freshly-split shard. The
  KPL does all of that.
- **`boto3.put_records` is a single API call, not a pipeline.** It does not
  know about deadlines, does not back off, does not classify
  `ProvisionedThroughputExceededException` separately from generic
  transient errors, does not know that a successful response can still mean
  *"your shard map is stale"*, and does not carry a per-record attempt
  history. All of that is the KPL's job.
- **LocalStack / `kinesis-mock` are emulators, not clients.** They are how
  we *test* the producer. They are not what users put in front of their
  Kinesis stream in production.

The C++ KPL solves the hard part — turning a stream of user records into a
shard-aware, deadline-bounded, retry-classified `PutRecords` pipeline — and
the Python ecosystem has never had an equivalent. `aiokpl` is that
equivalent.

## Why now, why async

The C++ KPL exists because in 2015 Python did not have `asyncio`, AWS SDKs
did not have async clients, and writing a shard-aware concurrent producer
in CPython was painful. None of those things are true in 2026. `aiobotocore`
gives us non-blocking AWS calls, `anyio` gives us cheap concurrency that
runs on both `asyncio` and `trio` backends, and modern Python gives us the
type system and dataclasses we need to express the pipeline cleanly.

The C++ KPL is a 30k-line C++ program plus a Java sidecar that spawns it as
a subprocess and talks to it over a named pipe. `aiokpl` aims for roughly
2k lines of pure Python with zero native dependencies. The semantics are
the same — the engineering footprint is two orders of magnitude smaller.
