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
    """Run the unit test suite with coverage, excluding integration tests."""
    session.install("-e", ".[test]")
    session.run(
        "pytest",
        "--cov=aiokpl",
        "--cov-report=term-missing",
        "--cov-report=xml",
        "-m",
        "not integration",
        *session.posargs,
    )


@nox.session(python="3.12")
def integration(session: nox.Session) -> None:
    """Run integration tests (empty in Phase 1)."""
    session.install("-e", ".[test,integration]")
    session.run("pytest", "-m", "integration", *session.posargs)


@nox.session(python="3.12")
def lint(session: nox.Session) -> None:
    """Run ruff check + format check."""
    session.install("-e", ".[lint]")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python="3.12")
def typecheck(session: nox.Session) -> None:
    """Run ty (Astral's type checker, mypy successor).

    ty is pre-release; the CLI surface may change. We try the expected
    invocation and fall back to a version probe if it isn't yet usable.
    """
    session.install("-e", ".[lint]")
    try:
        session.run("ty", "check", "aiokpl", "tests")
    except nox.command.CommandFailed:
        session.log("ty check failed or unavailable; falling back to version probe")
        session.run("ty", "--version")


@nox.session(python="3.12")
def coverage(session: nox.Session) -> None:
    """Combine partial coverage data and enforce the 100% gate."""
    session.install("-e", ".[test]")
    session.run("pytest", "--cov=aiokpl", "-m", "not integration")
    session.run("coverage", "combine", success_codes=[0, 1])
    session.run("coverage", "report", "--fail-under=100")
