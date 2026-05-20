# Setup

`aiokpl` uses [`uv`](https://docs.astral.sh/uv/) for environment and
dependency management and [`nox`](https://nox.thea.codes/) for orchestrating
lint, typecheck, tests, and coverage.

## Clone and install

```bash
git clone https://github.com/juanrojas/aiokpl.git
cd aiokpl
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

The `dev` extra pulls in `test`, `integration`, `lint`, and `docs`.

## Nox sessions

```bash
# Default sessions — lint, typecheck, tests across the matrix, coverage gate.
nox

# Single session.
nox -s lint
nox -s typecheck
nox -s tests-3.12
nox -s coverage

# Integration tests (require Docker for kinesis-mock).
nox -s integration

# Documentation build / live preview.
nox -s docs
nox -s docs-serve
```

Sessions reuse their virtualenvs (`nox.options.reuse_existing_virtualenvs =
True`), so re-running a session after a code change is fast.

## Building the docs locally

```bash
nox -s docs-serve
# open http://127.0.0.1:8000
```

`docs-serve` runs `mkdocs serve` with autoreload. `docs` runs
`mkdocs build --strict` (the same thing CI does for pull requests).
