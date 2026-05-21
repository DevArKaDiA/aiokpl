""":class:`CloudWatchSink` — first-party sink that uploads to CloudWatch.

Extracted from the original Phase 7 ``MetricsManager``. The transport
remains ``aiobotocore``'s asyncio CloudWatch client; the chunking, payload
shape, and dimension naming are unchanged so existing dashboards keep
working without edits.

The sink does not know about :class:`aiokpl.metrics.MetricsManager`; it
takes pre-built :class:`MetricSnapshot` instances and posts them on
``export``. Constructor arguments cover the same surface ``Config`` used to
expose for CloudWatch (region, endpoint, credentials, namespace).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any

import aiobotocore.session

from aiokpl.sinks._types import MetricSnapshot

# CloudWatch PutMetricData hard limit on MetricData entries per call.
_CLOUDWATCH_BATCH_LIMIT = 1000

# Public dimension keys we know how to translate to CloudWatch dimension
# names. Anything else is forwarded verbatim with its raw key.
_DIMENSION_NAMES = {
    "stream": "StreamName",
    "shard": "ShardId",
    "error_code": "ErrorCode",
}


class CloudWatchSink:
    """Forwards aggregated :class:`MetricSnapshot` batches to CloudWatch.

    Parameters
    ----------
    region:
        AWS region for the CloudWatch endpoint.
    namespace:
        CloudWatch namespace under which every datum is posted.
    endpoint_url:
        Override CloudWatch endpoint (for local stacks / tests).
    verify_ssl:
        Forwarded to ``aiobotocore``. Set to ``False`` for self-signed certs.
    aws_access_key_id / aws_secret_access_key / aws_session_token:
        Explicit credentials. If ``None`` the default credential chain runs.
    client_factory:
        Override the default ``aiobotocore`` client factory. Used by tests to
        inject a fake CloudWatch client.
    """

    __slots__ = (
        "_aws_access_key_id",
        "_aws_secret_access_key",
        "_aws_session_token",
        "_client",
        "_client_ctx",
        "_client_factory",
        "_endpoint_url",
        "_namespace",
        "_region",
        "_verify_ssl",
    )

    def __init__(
        self,
        *,
        region: str,
        namespace: str = "aiokpl",
        endpoint_url: str | None = None,
        verify_ssl: bool = True,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        client_factory: Callable[[], AbstractAsyncContextManager[Any]] | None = None,
    ) -> None:
        self._region = region
        self._namespace = namespace
        self._endpoint_url = endpoint_url
        self._verify_ssl = verify_ssl
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._client_factory = client_factory or self._default_factory
        self._client: Any = None
        self._client_ctx: AbstractAsyncContextManager[Any] | None = None

    # ─── Lifecycle ────────────────────────────────────────────────────────

    async def __aenter__(self) -> CloudWatchSink:
        ctx = self._client_factory()
        self._client_ctx = ctx
        self._client = await ctx.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        ctx = self._client_ctx
        assert ctx is not None
        self._client = None
        self._client_ctx = None
        await ctx.__aexit__(exc_type, exc, tb)

    # ─── Export ───────────────────────────────────────────────────────────

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        if not snapshots or self._client is None:
            return
        datums = [self._snapshot_to_datum(s) for s in snapshots]
        for i in range(0, len(datums), _CLOUDWATCH_BATCH_LIMIT):
            chunk = datums[i : i + _CLOUDWATCH_BATCH_LIMIT]
            await self._client.put_metric_data(
                Namespace=self._namespace,
                MetricData=chunk,
            )

    # ─── Properties (handy for tests / introspection) ─────────────────────

    @property
    def namespace(self) -> str:
        return self._namespace

    # ─── Internals ────────────────────────────────────────────────────────

    def _default_factory(self) -> AbstractAsyncContextManager[Any]:
        session = aiobotocore.session.get_session()
        return session.create_client(
            "cloudwatch",
            region_name=self._region,
            endpoint_url=self._endpoint_url,
            verify=self._verify_ssl,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
        )

    @staticmethod
    def _snapshot_to_datum(snapshot: MetricSnapshot) -> dict[str, Any]:
        dims: list[dict[str, str]] = []
        for key, value in snapshot.dimensions:
            name = _DIMENSION_NAMES.get(key, key)
            dims.append({"Name": name, "Value": value})
        return {
            "MetricName": snapshot.name,
            "Dimensions": dims,
            "StatisticValues": {
                "SampleCount": float(snapshot.count),
                "Sum": snapshot.sum,
                "Minimum": snapshot.min,
                "Maximum": snapshot.max,
            },
        }


__all__ = ["CloudWatchSink"]
