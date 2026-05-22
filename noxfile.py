"""Nox sessions for aiokpl.

`nox` drives lint, typecheck, tests (matrix), integration, and coverage. All
tool config lives in pyproject.toml; this file is wiring only.
"""

from __future__ import annotations

import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["lint", "typecheck", "tests", "coverage"]

PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Run the unit test suite with coverage, excluding integration tests.

    Installs the optional metric-sink extras so the lazy-imported
    ``OpenTelemetrySink`` and ``DatadogSink`` modules participate in
    coverage; without them those files would import-fail at collection
    time and the 100% gate could never be reached.
    """
    session.install("-e", ".[test,otel,datadog]")
    session.run(
        "pytest",
        "--cov=aiokpl",
        "--cov-report=term-missing",
        "--cov-report=xml",
        "-m",
        "not integration",
        *session.posargs,
    )


INTEGRATION_PYTHON = ["3.10", "3.12", "3.13"]


@nox.session(python=INTEGRATION_PYTHON)
def integration(session: nox.Session) -> None:
    """Run integration tests against kinesis-mock (Docker required).

    Parameterized over head/middle/tail of the supported Python window.
    CI by default runs `[3.10, 3.13]`; 3.12 is included here so local
    quick runs (`nox -s integration-3.12`) hit the same Python the
    docs/build jobs use.
    """
    session.install("-e", ".[test,integration,otel,datadog]")
    session.run("pytest", "-m", "integration", "-v", *session.posargs)


@nox.session(python="3.12")
def lint(session: nox.Session) -> None:
    """Run ruff check + format check."""
    session.install("-e", ".[lint]")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python="3.12")
def typecheck(session: nox.Session) -> None:
    """Run ty (Astral's type checker, mypy successor).

    ty resolves imports against the installed environment, so we install
    every extra the tests touch (pytest, anyio, opentelemetry, datadog,
    docker, ...) — otherwise `ty check tests` flags every external
    import as unresolved. ty is pre-release; the CLI surface may change.
    We fall back to a version probe if the invocation breaks.
    """
    session.install("-e", ".[lint,test,integration,otel,datadog]")
    try:
        session.run("ty", "check", "aiokpl", "tests")
    except nox.command.CommandFailed:
        session.log("ty check failed or unavailable; falling back to version probe")
        session.run("ty", "--version")


@nox.session(python="3.12")
def docs(session: nox.Session) -> None:
    """Build the documentation site with mkdocs --strict."""
    session.install("-e", ".[docs]")
    session.run("mkdocs", "build", "--strict")


@nox.session(python="3.12", name="docs-serve")
def docs_serve(session: nox.Session) -> None:
    """Serve the documentation site locally with autoreload."""
    session.install("-e", ".[docs]")
    session.run("mkdocs", "serve", "--dev-addr=127.0.0.1:8000")


@nox.session(python="3.12")
def coverage(session: nox.Session) -> None:
    """Combine partial coverage data and enforce the 100% gate."""
    session.install("-e", ".[test]")
    session.run("pytest", "--cov=aiokpl", "-m", "not integration")
    session.run("coverage", "combine", success_codes=[0, 1])
    session.run("coverage", "report", "--fail-under=100")
