""":class:`aiokpl.sinks.CloudWatchSink` payload, chunking, and lifecycle."""

from __future__ import annotations

from typing import Any

import pytest

from aiokpl.sinks import CloudWatchSink, MetricSnapshot


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class FakeCWClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class FakeCWCtx:
    def __init__(self, client: FakeCWClient) -> None:
        self._client = client
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeCWClient:
        self.entered = True
        return self._client

    async def __aexit__(self, *_: Any) -> None:
        self.exited = True


def _snap(name: str, **dims: str) -> MetricSnapshot:
    return MetricSnapshot(
        name=name,
        count=1,
        sum=1.0,
        min=1.0,
        max=1.0,
        dimensions=tuple(dims.items()),
    )


async def test_cw_sink_uploads_namespace_and_payload_shape() -> None:
    client = FakeCWClient()
    ctx = FakeCWCtx(client)

    sink = CloudWatchSink(
        region="us-east-1",
        namespace="ns",
        client_factory=lambda: ctx,
    )
    async with sink:
        assert ctx.entered
        await sink.export(
            (
                _snap("RequestTime", stream="s"),
                _snap("UserRecordsPut", stream="s", shard="0"),
            )
        )
    assert ctx.exited
    assert len(client.calls) == 1
    payload = client.calls[0]
    assert payload["Namespace"] == "ns"
    data = payload["MetricData"]
    by_name = {d["MetricName"]: d for d in data}
    assert by_name["RequestTime"]["Dimensions"] == [{"Name": "StreamName", "Value": "s"}]
    user_dims = by_name["UserRecordsPut"]["Dimensions"]
    assert {"Name": "StreamName", "Value": "s"} in user_dims
    assert {"Name": "ShardId", "Value": "0"} in user_dims
    sv = by_name["RequestTime"]["StatisticValues"]
    assert {"SampleCount", "Sum", "Minimum", "Maximum"} <= sv.keys()


async def test_cw_sink_chunks_at_1000() -> None:
    client = FakeCWClient()
    ctx = FakeCWCtx(client)
    sink = CloudWatchSink(region="us-east-1", client_factory=lambda: ctx)
    snaps = tuple(_snap("X", stream=f"s-{i}") for i in range(2500))
    async with sink:
        await sink.export(snaps)
    assert [len(c["MetricData"]) for c in client.calls] == [1000, 1000, 500]


async def test_cw_sink_export_is_noop_with_empty_snapshots() -> None:
    client = FakeCWClient()
    ctx = FakeCWCtx(client)
    sink = CloudWatchSink(region="us-east-1", client_factory=lambda: ctx)
    async with sink:
        await sink.export(())
    assert client.calls == []


async def test_cw_sink_unknown_dimension_key_forwarded_verbatim() -> None:
    client = FakeCWClient()
    ctx = FakeCWCtx(client)
    sink = CloudWatchSink(region="us-east-1", client_factory=lambda: ctx)
    snap = MetricSnapshot(
        name="X",
        count=1,
        sum=0.0,
        min=0.0,
        max=0.0,
        dimensions=(("custom", "value"),),
    )
    async with sink:
        await sink.export((snap,))
    assert client.calls[0]["MetricData"][0]["Dimensions"] == [
        {"Name": "custom", "Value": "value"},
    ]


async def test_cw_sink_export_without_open_client_is_noop() -> None:
    sink = CloudWatchSink(region="us-east-1", client_factory=lambda: FakeCWCtx(FakeCWClient()))
    # Never entered: export should bail out before touching client.
    await sink.export((_snap("X"),))


async def test_cw_sink_namespace_property() -> None:
    sink = CloudWatchSink(region="us-east-1", namespace="custom")
    assert sink.namespace == "custom"


async def test_cw_sink_default_factory_builds_aiobotocore_session() -> None:
    sink = CloudWatchSink(
        region="us-east-1",
        endpoint_url="http://localhost:1234",
        verify_ssl=False,
        aws_access_key_id="ak",
        aws_secret_access_key="sk",
        aws_session_token="tok",
    )
    ctx = sink._default_factory()
    assert hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__")
    # Enter+exit so we don't leak the coroutine object aiobotocore allocates.
    client = await ctx.__aenter__()
    assert client is not None
    await ctx.__aexit__(None, None, None)
