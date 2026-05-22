"""End-to-end smoke test for :class:`CloudWatchSink`.

The unit tests in ``tests/test_sink_cloudwatch.py`` cover snapshot →
``put_metric_data`` translation with a fake aiobotocore client. This test
goes one layer deeper: it points the sink at a real HTTP server (stdlib
``http.server.ThreadingHTTPServer`` running in a background thread) and
asserts aiobotocore actually delivers a ``PutMetricData`` form-encoded
request over the wire.

Choosing stdlib over aiohttp keeps zero new runtime / test dependencies.
The server runs in a daemon thread; aiobotocore drives the request from
the asyncio loop. They share nothing but the TCP socket.
"""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from aiokpl.metrics import (
    NAME_USER_RECORDS_PUT,
    MetricsLevel,
    MetricsManager,
)
from aiokpl.sinks import CloudWatchSink


@pytest.fixture
def anyio_backend() -> str:
    # aiobotocore is asyncio-only; CloudWatchSink uses aiobotocore directly.
    return "asyncio"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _CaptureState:
    """Shared state between the HTTP server thread and the test thread."""

    def __init__(self) -> None:
        self.bodies: list[Any] = []
        self.raw_bodies: list[bytes] = []
        self.paths: list[str] = []
        self.targets: list[str] = []
        self.headers: list[dict[str, str]] = []
        self.lock = threading.Lock()


def _make_handler(state: _CaptureState) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        # Silence the default stderr access log; pytest catches warnings.
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b""
            parsed: Any
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                parsed = None
            target = self.headers.get("X-Amz-Target", "")
            with state.lock:
                state.bodies.append(parsed)
                state.raw_bodies.append(raw)
                state.paths.append(self.path)
                state.targets.append(target)
                state.headers.append(dict(self.headers.items()))
            # CloudWatch's JSON protocol returns an empty body on 200.
            body = b"{}"
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.0")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return _Handler


@pytest.fixture
def fake_cloudwatch() -> Iterator[tuple[str, _CaptureState]]:
    state = _CaptureState()
    port = _free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


@pytest.mark.integration
async def test_cloudwatch_sink_posts_putmetricdata_over_http(
    fake_cloudwatch: tuple[str, _CaptureState],
) -> None:
    endpoint, state = fake_cloudwatch

    sink = CloudWatchSink(
        region="us-east-1",
        namespace="aiokpl-it",
        endpoint_url=endpoint,
        verify_ssl=False,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    manager = MetricsManager(
        level=MetricsLevel.DETAILED,
        sink=sink,
        upload_interval_ms=60_000.0,
    )
    async with manager:
        manager.put(NAME_USER_RECORDS_PUT, 1.0, stream="s", shard_id="shardId-0")
        manager.put(NAME_USER_RECORDS_PUT, 1.0, stream="s", shard_id="shardId-0")
        await manager.flush()

    with state.lock:
        assert state.bodies, "fake CloudWatch endpoint never saw a request"
        # Modern aiobotocore talks CloudWatch over the AWS JSON 1.0 protocol;
        # the action ends up in the ``X-Amz-Target`` header
        # (``GraniteServiceVersion20100801.PutMetricData``) and the body is
        # JSON with ``Namespace`` + ``MetricData`` keys.
        assert any("PutMetricData" in t for t in state.targets), state.targets
        bodies = [b for b in state.bodies if isinstance(b, dict)]
        assert bodies, state.bodies
        first = bodies[0]
        assert first.get("Namespace") == "aiokpl-it", first
        md = first.get("MetricData") or []
        assert md, first
        names = [d.get("MetricName") for d in md]
        assert NAME_USER_RECORDS_PUT in names, names
