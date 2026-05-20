"""Shared pytest configuration for the test suite.

The anyio pytest plugin (shipped with ``anyio>=4``) replaces ``pytest-asyncio``
for this project. Two settings make the test surface ergonomic:

1. ``anyio_mode = "auto"`` in ``pyproject.toml`` auto-marks every async test
   function with ``pytest.mark.anyio``, removing the need for a per-test
   decorator. This is the equivalent of the old ``asyncio_mode = "auto"``
   pytest-asyncio setting.
2. The ``anyio_backend`` fixture below is parametrized across ``asyncio``
   and ``trio``, so every async test runs once per backend. Coverage is the
   union of both runs.
"""

from __future__ import annotations

import pytest


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param
