# Testing

## Markers

Tests are split by `pytest` marker:

- **Unit tests** (default). Pure logic — codec, hashing, shard map state
  machine, reducer, retrier classification. No network, no Docker.
- **Integration tests** (`@pytest.mark.integration`). Use the
  `etspaceman/kinesis-mock` container as a stand-in for the real Kinesis
  API. Require a running Docker daemon.
- **Slow tests** (`@pytest.mark.slow`). Run by default but tagged so they
  can be excluded under `-m "not slow"`.

```bash
pytest                          # unit + slow, no integration
pytest -m integration           # integration only
pytest -m "not slow"            # fast subset
```

The `nox -s tests` session runs `-m "not integration"`. The `nox -s
integration` session runs `-m integration` separately.

## Coverage gate — 100%

The coverage gate is **100%**, with `fail_under = 100` in
`pyproject.toml`. No exceptions, no excludes for "hard to test" branches.

!!! warning "If you can't cover a branch, delete it."
    The gate is a forcing function. A branch that cannot be exercised by
    tests is a branch that is not exercised in production either. Drop it.

CI runs the matrix per Python version, uploads partial coverage data, and
the `coverage-gate` job downloads everything, combines, and enforces the
threshold across the whole matrix.

## The kinesis-mock fixture

Integration tests spawn `ghcr.io/etspaceman/kinesis-mock:0.5.2` via the
Docker SDK (no `testcontainers` wrapper). The API is HTTPS on port 4567
with a self-signed cert (`verify=False` in the harness), and the
healthcheck is plain HTTP on port 4568.

We pivoted to `kinesis-mock` after a failed attempt with Floci: Floci
faked shard ranges and routed round-robin, which made byte-exact shard
prediction tests impossible. `kinesis-mock` uses the same Scala backend
LocalStack uses for Kinesis and is byte-exact compatible with AWS for
hash-key routing, `ListShards` pagination, and `SplitShard` child ranges.

## Running without Docker

If Docker is not running, integration tests **skip cleanly** rather than
fail. The conftest detects the daemon up-front and emits a skip reason
for the whole module. Unit tests always run.

```bash
pytest -m "not integration"     # safe everywhere
```
