""":class:`aiokpl.sinks.DatadogSink` payload shape.

Skipped when ``datadog-api-client`` is not installed. Uses a fake
``MetricsApi`` to capture calls.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

pytest.importorskip("datadog_api_client")

from aiokpl.sinks import MetricSnapshot
from aiokpl.sinks.datadog import DatadogSink


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


class FakeMetricsApi:
    def __init__(self) -> None:
        self.metric_calls: list[Any] = []
        self.dist_calls: list[Any] = []

    async def submit_metrics(self, body: Any) -> None:
        self.metric_calls.append(body)

    async def submit_distribution_points(self, body: Any) -> None:
        self.dist_calls.append(body)


def _install_fake(sink: DatadogSink, fake: FakeMetricsApi) -> None:
    sink._metrics_api = fake  # type: ignore[attr-defined]


async def test_dd_sink_counts_go_to_submit_metrics() -> None:
    sink = DatadogSink(api_key="ak", app_key="appk", metric_prefix="aiokpl.")
    fake = FakeMetricsApi()
    _install_fake(sink, fake)
    snaps: Sequence[MetricSnapshot] = (
        MetricSnapshot(
            name="UserRecordsPut",
            count=1,
            sum=5.0,
            min=1.0,
            max=5.0,
            dimensions=(("stream", "s"),),
            window_end=1.0,
        ),
    )
    await sink.export(snaps)
    assert len(fake.metric_calls) == 1
    body = fake.metric_calls[0]
    series = body.series
    assert series[0].metric == "aiokpl.UserRecordsPut"
    assert series[0].type == "count"
    assert "stream:s" in list(series[0].tags)


async def test_dd_sink_distributions_go_to_distribution_points() -> None:
    sink = DatadogSink()
    fake = FakeMetricsApi()
    _install_fake(sink, fake)
    snaps = (
        MetricSnapshot(
            name="RequestTime",
            count=2,
            sum=20.0,
            min=5.0,
            max=15.0,
            dimensions=(("stream", "s"),),
            window_start=1.0,
        ),
    )
    await sink.export(snaps)
    assert fake.metric_calls == []
    assert len(fake.dist_calls) == 1
    body = fake.dist_calls[0]
    series = body.series
    assert series[0].metric == "aiokpl.RequestTime"


async def test_dd_sink_gauges_use_gauge_type() -> None:
    sink = DatadogSink()
    fake = FakeMetricsApi()
    _install_fake(sink, fake)
    snaps = (
        MetricSnapshot(
            name="UserRecordsPending",
            count=1,
            sum=42.0,
            min=42.0,
            max=42.0,
            window_end=2.0,
        ),
    )
    await sink.export(snaps)
    series = fake.metric_calls[0].series
    assert series[0].type == "gauge"


async def test_dd_sink_unknown_metric_defaults_to_count() -> None:
    sink = DatadogSink()
    fake = FakeMetricsApi()
    _install_fake(sink, fake)
    snaps = (MetricSnapshot(name="Custom", count=1, sum=1.0, min=1.0, max=1.0, window_end=1.0),)
    await sink.export(snaps)
    series = fake.metric_calls[0].series
    assert series[0].type == "count"


async def test_dd_sink_export_is_noop_when_not_entered() -> None:
    sink = DatadogSink()
    # No fake installed; metrics_api is None — export should bail.
    await sink.export(
        (MetricSnapshot(name="X", count=1, sum=1.0, min=1.0, max=1.0),),
    )


async def test_dd_sink_export_with_empty_snapshots_is_noop() -> None:
    sink = DatadogSink()
    fake = FakeMetricsApi()
    _install_fake(sink, fake)
    await sink.export(())
    assert fake.metric_calls == []
    assert fake.dist_calls == []


async def test_dd_sink_lifecycle_without_keys_skips_auth_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without DD keys, the auth fields are not populated — covers the
    falsey branches in ``__aenter__``."""
    from aiokpl.sinks import datadog as dd_mod

    class FakeClient:
        def __init__(self, cfg: Any) -> None:
            self.cfg = cfg

        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

    class FakeApi:
        def __init__(self, client: Any) -> None:
            self.client = client

    monkeypatch.setenv("DD_API_KEY", "")
    monkeypatch.setenv("DD_APP_KEY", "")
    monkeypatch.delenv("DD_API_KEY", raising=False)
    monkeypatch.delenv("DD_APP_KEY", raising=False)
    monkeypatch.setattr(dd_mod, "AsyncApiClient", FakeClient)
    monkeypatch.setattr(dd_mod, "MetricsApi", FakeApi)

    sink = DatadogSink()
    async with sink:
        pass


async def test_dd_sink_lifecycle_opens_and_closes_api_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The async context manager must build an AsyncApiClient + MetricsApi."""
    from aiokpl.sinks import datadog as dd_mod

    class FakeClient:
        def __init__(self, cfg: Any) -> None:
            self.entered = False
            self.exited = False
            self.cfg = cfg

        async def __aenter__(self) -> Any:
            self.entered = True
            return self

        async def __aexit__(self, *_: Any) -> None:
            self.exited = True

    built_clients: list[FakeClient] = []

    def fake_client_cls(cfg: Any) -> FakeClient:
        c = FakeClient(cfg)
        built_clients.append(c)
        return c

    class FakeApi:
        def __init__(self, client: Any) -> None:
            self.client = client

    monkeypatch.setattr(dd_mod, "AsyncApiClient", fake_client_cls)
    monkeypatch.setattr(dd_mod, "MetricsApi", FakeApi)

    sink = DatadogSink(api_key="ak", app_key="appk", site="datadoghq.eu")
    async with sink:
        assert built_clients[0].entered is True
    assert built_clients[0].exited is True
