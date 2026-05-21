"""Unit tests for :class:`aiokpl.config.Config`."""

from __future__ import annotations

import dataclasses

import pytest

from aiokpl.config import Config


def test_defaults() -> None:
    cfg = Config(region="us-east-1")
    assert cfg.region == "us-east-1"
    assert cfg.aggregation_enabled is True
    assert cfg.aggregation_max_count == 4_294_967_295
    assert cfg.aggregation_max_size == 51_200
    assert cfg.record_max_buffered_time_ms == 100.0
    assert cfg.record_ttl_ms == 30_000.0
    assert cfg.collection_max_count == 500
    assert cfg.collection_max_size == 5 * 1024 * 1024
    assert cfg.rate_limit_records_per_sec_per_shard == 1_000.0
    assert cfg.rate_limit_bytes_per_sec_per_shard == 1_048_576.0
    assert cfg.drain_interval_ms == 25.0
    assert cfg.fail_if_throttled is False
    assert cfg.retry_deadline_ms == 50.0
    assert cfg.max_outstanding_records == 100_000
    assert cfg.endpoint_url is None
    assert cfg.verify_ssl is True
    assert cfg.aws_access_key_id is None
    assert cfg.aws_secret_access_key is None
    assert cfg.aws_session_token is None


def test_frozen() -> None:
    cfg = Config(region="us-east-1")
    # object.__setattr__ bypasses the static type-checker's read-only check;
    # the runtime ``__setattr__`` injected by the frozen dataclass raises.
    with pytest.raises(dataclasses.FrozenInstanceError):
        type(cfg).__setattr__(cfg, "region", "eu-west-1")


def test_slots() -> None:
    cfg = Config(region="us-east-1")
    assert not hasattr(cfg, "__dict__")


def test_override() -> None:
    cfg = Config(
        region="us-west-2",
        aggregation_enabled=False,
        record_max_buffered_time_ms=10.0,
        max_outstanding_records=5,
        endpoint_url="https://localhost:4567",
        verify_ssl=False,
        aws_access_key_id="k",
        aws_secret_access_key="s",
        aws_session_token="t",
        fail_if_throttled=True,
    )
    assert cfg.region == "us-west-2"
    assert cfg.aggregation_enabled is False
    assert cfg.record_max_buffered_time_ms == 10.0
    assert cfg.max_outstanding_records == 5
    assert cfg.endpoint_url == "https://localhost:4567"
    assert cfg.verify_ssl is False
    assert cfg.aws_access_key_id == "k"
    assert cfg.aws_secret_access_key == "s"
    assert cfg.aws_session_token == "t"
    assert cfg.fail_if_throttled is True
