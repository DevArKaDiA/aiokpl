"""Integration-test fixtures.

Spins up a single Floci container per test session and yields a configured
synchronous Kinesis client. Heavy imports (``botocore``, ``floci``, ``docker``)
are done inside the fixture so the unit-test path never pays for them and
``coverage`` doesn't see them.

The client is built directly from ``botocore.session.Session`` rather than
``boto3``: ``boto3`` is not part of the ``integration`` extra (aiobotocore is,
and it pulls ``botocore`` already), and a sync low-level client is enough — the
async surface is for the producer, not the test harness.

Floci is LocalStack's drop-in successor; LocalStack Community was archived in
March 2026. See ``CLAUDE.md`` for the rationale.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def kinesis_client() -> Iterator[object]:
    """Yield a Kinesis client bound to a freshly started Floci container.

    Skips the whole integration session cleanly if Docker is not reachable, so
    non-Docker CI lanes don't fail.
    """
    try:
        import docker
        from botocore.config import Config as BotoConfig
        from botocore.session import Session as BotoSession
        from floci import FlociContainer
    except ImportError as exc:  # pragma: no cover - integration-only path
        pytest.skip(f"integration deps missing: {exc}", allow_module_level=False)

    try:
        docker.from_env().ping()
    except Exception as exc:
        pytest.skip(f"Docker is not available: {exc}")

    container = FlociContainer()
    container.start()
    try:
        endpoint = container.get_endpoint()
        client = BotoSession().create_client(
            "kinesis",
            endpoint_url=endpoint,
            region_name=container.get_region(),
            aws_access_key_id=container.get_access_key(),
            aws_secret_access_key=container.get_secret_key(),
            config=BotoConfig(retries={"max_attempts": 3, "mode": "standard"}),
        )
        # Sanity probe — proves the control plane is actually live, not just the
        # health endpoint.
        client.list_streams()
        yield client
    finally:
        container.stop()
