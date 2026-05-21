""":class:`NullSink` — the zero-overhead default sink.

Used when the user does not configure a metrics sink. Every callback is a
no-op; the sink does not allocate, does not open any network client, and
satisfies the :class:`MetricsSink` Protocol without any side effects.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType

from aiokpl.sinks._types import MetricSnapshot


class NullSink:
    """Discards every metric. Default sink when ``config.metrics_sink`` is ``None``."""

    __slots__ = ()

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        return None

    async def __aenter__(self) -> NullSink:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


__all__ = ["NullSink"]
