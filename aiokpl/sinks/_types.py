"""Sink protocol types — kept separate to avoid circular imports.

The first-party sinks (``null``, ``memory``, ``cloudwatch``) import from
this module; the package ``__init__`` re-exports the public surface.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol, runtime_checkable


@dataclass(slots=True, frozen=True)
class MetricEvent:
    """A single observation emitted by the library.

    ``dimensions`` is a tuple of ``(name, value)`` pairs so the event is
    hashable and frozen. Common keys: ``"stream"``, ``"shard"``,
    ``"error_code"``.
    """

    name: str
    value: float
    timestamp: float
    dimensions: tuple[tuple[str, str], ...] = ()


@dataclass(slots=True, frozen=True)
class MetricSnapshot:
    """A flushed aggregate over a window.

    Produced by :class:`aiokpl.metrics.MetricsManager` on every export tick.
    Sinks consume these in :meth:`MetricsSink.export`.
    """

    name: str
    count: int
    sum: float
    min: float
    max: float
    dimensions: tuple[tuple[str, str], ...] = ()
    window_start: float = 0.0
    window_end: float = 0.0


@runtime_checkable
class MetricsSink(Protocol):
    """Vendor-neutral sink for metric snapshots.

    The :class:`aiokpl.metrics.MetricsManager` batches observations into
    :class:`MetricSnapshot` windows and calls :meth:`export` on a schedule.
    Sinks that want per-event resolution can additionally implement
    :class:`EventfulMetricsSink` and the manager will dispatch every
    observation through :meth:`EventfulMetricsSink.record`.
    """

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None: ...

    async def __aenter__(self) -> MetricsSink: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


@runtime_checkable
class EventfulMetricsSink(Protocol):
    """Optional extension Protocol for sinks that consume per-event data.

    ``record`` is synchronous on purpose: it runs on the hot path inside
    :meth:`MetricsManager.put` (which is itself sync). Async work belongs in
    :meth:`MetricsSink.export`, which is dispatched on the upload loop.
    """

    def record(self, event: MetricEvent) -> None: ...
