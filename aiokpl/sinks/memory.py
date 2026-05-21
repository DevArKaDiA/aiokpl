""":class:`InMemorySink` — keeps every exported snapshot batch.

Used by tests to assert post-flush exports without touching the network, and
useful in embedded scenarios where the host process publishes metrics via
some other channel.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType

from aiokpl.sinks._types import MetricSnapshot


class InMemorySink:
    """Captures every batch of snapshots ``export`` is called with.

    Order is preserved (insertion order). Both :attr:`exports` and the
    convenience accessors return immutable views.
    """

    __slots__ = ("_exports",)

    def __init__(self) -> None:
        self._exports: list[tuple[MetricSnapshot, ...]] = []

    async def export(self, snapshots: Sequence[MetricSnapshot]) -> None:
        self._exports.append(tuple(snapshots))

    async def __aenter__(self) -> InMemorySink:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    # ─── Test conveniences ────────────────────────────────────────────────

    @property
    def exports(self) -> tuple[tuple[MetricSnapshot, ...], ...]:
        """Every batch of snapshots ever exported, in call order."""
        return tuple(self._exports)

    @property
    def all_snapshots(self) -> tuple[MetricSnapshot, ...]:
        """Flatten every batch into a single tuple, preserving order."""
        return tuple(s for batch in self._exports for s in batch)

    def by_name(self, name: str) -> tuple[MetricSnapshot, ...]:
        """Return every snapshot whose ``name`` matches the argument."""
        return tuple(s for s in self.all_snapshots if s.name == name)


__all__ = ["InMemorySink"]
