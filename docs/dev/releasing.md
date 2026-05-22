# Releasing

This page is the **policy** for aiokpl releases. The mechanics — how the
release pipeline is wired in CI — live alongside the release workflow
itself (see `.github/workflows`) and are intentionally not duplicated
here.

## Versioning

aiokpl follows [Semantic Versioning](https://semver.org/).

- **0.x** — public API may change between minor versions. Any such
  change is called out in the CHANGELOG, and the deprecation, when
  practical, ships one minor ahead of the removal.
- **1.0** will be cut when the public API is considered frozen.
  Breaking changes after 1.0 will require a major bump and an
  explicit migration note.

## How a release happens

1. Land everything that's going into the release on `main`.
2. Update the CHANGELOG entry for the version.
3. Tag the commit `vX.Y.Z` on `main` and push the tag.
4. The release workflow picks the tag up and takes it from there
   (build, publish, GitHub release notes). See the release workflow
   under `.github/workflows`.

There is no manual `twine upload` or `uv publish` step — anything that
isn't reproducible from a tag is a bug in the workflow, not a thing to
work around by hand.

## Compatibility

- **Python.** 3.10+. We test on 3.10, 3.11, 3.12, 3.13.
- **anyio.** 4+. We do not test against 3.x; the API surface we use
  (`from_thread.start_blocking_portal`, `CancelScope` semantics,
  `MemoryObjectStream`) all assume 4.
- **Async backend.**
    - The `Producer`, `SyncProducer`, and the Sender/Retrier are
      asyncio-only because `aiobotocore` (the Kinesis HTTP client) is
      asyncio-only.
    - The codec, `ShardMap`, `Reducer`, `Aggregator`, `Collector`,
      `Limiter`, and `TokenBucket` are backend-agnostic and tested on
      both `asyncio` and `trio` via the parametrised `anyio_backend`
      fixture.
- **AWS SDK.** `aiobotocore` 2+. `botocore` follows whatever
  `aiobotocore` pins.

## Pre-1.0 stability guarantees

Even in 0.x, some things are pinned:

- **Aggregation wire format is FROZEN.** It matches the AWS Kinesis
  aggregation specification byte-for-byte. We will never change it —
  KCL consumers depend on it.
- **Public API of `Producer`, `SyncProducer`, `Config`, `Outcome`,
  `RecordResult`, `Attempt`, `MetricsSink`** is stable for v0.x. Names
  inside these surfaces may move between minor versions, but any move
  is documented in the CHANGELOG and the old name continues to work
  for one minor where practical.
- **`MetricsLevel` enum values** (`NONE`, `SUMMARY`, `DETAILED`) and
  metric names (which match the C++ KPL constants verbatim) are
  stable.

Anything not in `__all__` is internal and may change without notice.

## Where to file things

- **Bugs and questions.** Open an issue at
  <https://github.com/DevArKaDiA/aiokpl/issues>.
- **Security.** Email the maintainer rather than opening a public
  issue.
- **Feature ideas.** Open an issue with the `enhancement` label.
  Anything that crosses one of the "Non-goals" in the README will be
  closed with a pointer to the rationale.
