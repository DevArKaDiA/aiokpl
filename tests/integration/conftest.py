"""Integration-test fixtures.

Spins up a single ``etspaceman/kinesis-mock`` container per test session and
yields a configured synchronous Kinesis client. The mock is the same Scala
backend LocalStack uses internally — byte-exact compatible with AWS for
hash-key routing, ``ListShards`` pagination, and ``SplitShard`` child ranges,
which the previous Floci-based fixture could not provide.

Heavy imports (``botocore``, ``docker``, ``urllib3``) are done inside the
fixture so the unit-test path never pays for them and ``coverage`` doesn't
see them.

The Kinesis API is served over HTTPS on a self-signed cert; ``verify=False``
is intentional and limited to the test harness.
"""

from __future__ import annotations

import contextlib
import socket
import time
import urllib.request
from collections.abc import Iterator

import pytest

KINESIS_MOCK_IMAGE = "ghcr.io/etspaceman/kinesis-mock:0.5.2"
KINESIS_PORT = 4567
HEALTH_PORT = 4568
READY_TIMEOUT_SECS = 30.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def kinesis_client() -> Iterator[object]:
    """Yield a sync Kinesis client bound to a fresh kinesis-mock container.

    Skips cleanly if Docker is unreachable.
    """
    try:
        import docker
        from botocore.config import Config as BotoConfig
        from botocore.session import Session as BotoSession
    except ImportError as exc:  # pragma: no cover - integration-only path
        pytest.skip(f"integration deps missing: {exc}", allow_module_level=False)

    try:
        client = docker.from_env()
        client.ping()
    except Exception as exc:
        pytest.skip(f"Docker is not available: {exc}")

    api_port = _free_port()
    health_port = _free_port()

    container = client.containers.run(
        KINESIS_MOCK_IMAGE,
        detach=True,
        remove=True,
        ports={
            f"{KINESIS_PORT}/tcp": api_port,
            f"{HEALTH_PORT}/tcp": health_port,
        },
        environment={
            "LOG_LEVEL": "WARN",
        },
    )

    try:
        # kinesis-mock serves plain HTTP healthcheck on 4568 and HTTPS
        # (self-signed cert) for the Kinesis API on 4567.
        deadline = time.monotonic() + READY_TIMEOUT_SECS
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://localhost:{health_port}/healthcheck",
                    timeout=2.0,
                ) as resp:
                    if resp.status == 200:
                        break
            except Exception as exc:
                last_err = exc
                time.sleep(0.3)
        else:
            raise RuntimeError(
                f"kinesis-mock did not become healthy in {READY_TIMEOUT_SECS}s: {last_err}"
            )

        kinesis = BotoSession().create_client(
            "kinesis",
            endpoint_url=f"https://localhost:{api_port}",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            verify=False,
            config=BotoConfig(retries={"max_attempts": 3, "mode": "standard"}),
        )
        # Sanity probe — proves the control plane is actually live.
        kinesis.list_streams()
        yield kinesis
    finally:
        with contextlib.suppress(Exception):
            container.stop(timeout=5)
