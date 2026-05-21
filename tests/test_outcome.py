"""Unit tests for :class:`aiokpl.outcome.Outcome`."""

from __future__ import annotations

import anyio
import anyio.lowlevel
import pytest

from aiokpl.outcome import Outcome


async def test_set_value_resolves_wait() -> None:
    o: Outcome[int] = Outcome()
    assert not o.is_set()
    o.set_value(42)
    assert o.is_set()
    assert await o.wait() == 42


async def test_set_exception_raises_on_wait() -> None:
    o: Outcome[int] = Outcome()
    exc = RuntimeError("boom")
    o.set_exception(exc)
    with pytest.raises(RuntimeError, match="boom"):
        await o.wait()


async def test_double_set_value_raises() -> None:
    o: Outcome[int] = Outcome()
    o.set_value(1)
    with pytest.raises(RuntimeError, match="already set"):
        o.set_value(2)


async def test_double_set_exception_raises() -> None:
    o: Outcome[int] = Outcome()
    o.set_exception(ValueError("a"))
    with pytest.raises(RuntimeError, match="already set"):
        o.set_exception(ValueError("b"))


async def test_set_value_then_exception_raises() -> None:
    o: Outcome[int] = Outcome()
    o.set_value(7)
    with pytest.raises(RuntimeError, match="already set"):
        o.set_exception(ValueError("x"))


async def test_wait_blocks_until_set() -> None:
    o: Outcome[str] = Outcome()
    results: list[str] = []

    async def waiter() -> None:
        results.append(await o.wait())

    async with anyio.create_task_group() as tg:
        tg.start_soon(waiter)
        await anyio.lowlevel.checkpoint()
        assert not o.is_set()
        o.set_value("hello")
    assert results == ["hello"]


async def test_wait_multiple_consumers() -> None:
    o: Outcome[int] = Outcome()
    results: list[int] = []

    async def waiter() -> None:
        results.append(await o.wait())

    async with anyio.create_task_group() as tg:
        for _ in range(3):
            tg.start_soon(waiter)
        await anyio.lowlevel.checkpoint()
        o.set_value(99)
    assert results == [99, 99, 99]
