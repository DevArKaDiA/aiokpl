""":class:`DatadogSink` — first-party sink for Datadog metrics.

Implemented on top of ``datadog-api-client``: ``export`` submits a batch of
metrics through the Datadog HTTP API. The library does *not* depend on
``datadog-api-client`` at install time; importing this module raises a
clear ``ImportError`` with the install hint when the optional package is
missing.

Metric → Datadog type mapping mirrors the OpenTelemetry sink's logic:

* counts (UserRecordsReceived, …) → ``count``
* distributions (BufferedTime, RequestTime, RetriesPerRecord) → ``distribution``
* gauges (UserRecordsPending) → ``gauge``
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from types import TracebackType
from typing import Any

try:
    from datadog_api_client import AsyncApiClient, Configuration
    from datadog_api_client.v1.api.metrics_api import MetricsApi
    from datadog_api_client.v1.model.distribution_points_payload import (
        DistributionPointsPayload,
    )
    from datadog_api_client.v1.model.distribution_point import DistributionPoint
    from datadog_api_client.v1.model.distribution_points_series import (
        DistributionPointsSeries,
    )
    from datadog_api_client.v1.model.metrics_payload import MetricsPayload
    from datadog_api_client.v1.model.point import Point
    from datadog_api_client.v1.model.series import Series
except ImportError as exc:  # pragma: no cover - exercised in env without DD
    raise ImportError(
        "DatadogSink requires the `datadog-api-client` package. Install with: "
        "`pip install 'aiokpl[datadog]'`"
    ) from exc

from aiokpl.sinks._types import MetricSnapshot

_DISTRIBUTION_NAMES = frozenset(
    {
        "BufferedTime",
        "RequestTime",
        "RetriesPerRecord",
    }
)

_GAUGE_NAMES = frozenset(
    {
        "UserRecordsPending",
    }
)


class DatadogSink:
    """Submits :class:`MetricSnapshot` batches to the Datadog HTTP API.

    Credentials default to environment variables (``DD_API_KEY``,
    ``DD_APP_KEY``); ``site`` selects the Datadog region.
    """

    __slots__ = (
        "_api_client",
        "_api_key",
        "_app_key",
        "_configuration",
        "_metric_prefix",
        "_metrics_api",
        "_site",
    )

    def __init__(
        self,
        *,
        api_key: str | None = None,
        app_key: str | None = None,
        site: str = "datadoghq.com",
        metric_prefix: str = "aiokpl.",
    ) -> None:
        self._api_key = api_key or os.environ.get("DD_API_KEY")
        self._app_key = app_key or os.environ.get("DD_APP_KEY")
        self._site = site
        self._metric_prefix = metric_prefix
        self._configuration: Any = None
        self._api_client: Any = None
        self._metrics_api: Any = None

    async def __aenter__(self) -> DatadogSink:
        configuration = Configuration()
        configuration.server_variables["site"] = self._site
        if self._api_key is not None:
            configuration.api_key["apiKeyAuth"] = self._api_key
        if self._app_key is not None:
            configuration.api_key["appKeyAuth"] = self._app_key
        self._configuration = configuration
        self._api_client = AsyncApiClient(configuration)
        await self._api_client.__aenter__()
        self._metrics_api = MetricsApi(self._api_client)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        client = self._api_client
        assert client is not None
        self._api_client = None
        self._metrics_api = None
        self._configuration = None
        await client.__aexit__(exc_type, exc, tb)

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        if not snapshots or self._metrics_api is None:
            return
        series: list[Series] = []
        distributions: list[DistributionPointsSeries] = []
        for s in snapshots:
            tags = [f"{k}:{v}" for k, v in s.dimensions]
            full_name = f"{self._metric_prefix}{s.name}"
            ts = s.window_end if s.window_end > 0.0 else s.window_start
            if s.name in _DISTRIBUTION_NAMES:
                # Datadog distribution points: list of (ts, [values]). We only
                # have aggregate sum/count, so emit the mean as a single point.
                avg = s.sum / s.count if s.count else 0.0
                distributions.append(
                    DistributionPointsSeries(
                        metric=full_name,
                        points=[DistributionPoint([ts, [avg]])],
                        tags=tags,
                    )
                )
            elif s.name in _GAUGE_NAMES:
                series.append(
                    Series(
                        metric=full_name,
                        type="gauge",
                        points=[Point([ts, s.max])],
                        tags=tags,
                    )
                )
            else:
                # Default + explicit count metrics map to Datadog "count".
                series.append(
                    Series(
                        metric=full_name,
                        type="count",
                        points=[Point([ts, s.sum])],
                        tags=tags,
                    )
                )

        if series:
            await self._metrics_api.submit_metrics(body=MetricsPayload(series=series))
        if distributions:
            await self._metrics_api.submit_distribution_points(
                body=DistributionPointsPayload(series=distributions)
            )


__all__ = ["DatadogSink"]
