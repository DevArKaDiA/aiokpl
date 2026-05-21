"""Frozen :class:`Config` dataclass: single source of truth for producer tunables.

Names mirror ``aws/kinesis/protobuf/config.proto`` for recognizability; defaults
match the C++ KPL except where noted in ``CLAUDE.md``. Fields are grouped by
the pipeline stage they feed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Config:
    """All producer tunables in one immutable dataclass.

    Constructed by the user, handed to :class:`aiokpl.producer.Producer`. Frozen
    so each per-stream pipeline can safely read fields without locks; slotted
    for footprint.
    """

    region: str

    # ── Aggregation ────────────────────────────────────────────────────────
    aggregation_enabled: bool = True
    aggregation_max_count: int = 4_294_967_295
    aggregation_max_size: int = 51_200

    # ── Batching deadlines ────────────────────────────────────────────────
    record_max_buffered_time_ms: float = 100.0
    record_ttl_ms: float = 30_000.0

    # ── Collector (Kinesis hard limits) ───────────────────────────────────
    collection_max_count: int = 500
    collection_max_size: int = 5 * 1024 * 1024

    # ── Per-shard rate limits ─────────────────────────────────────────────
    rate_limit_records_per_sec_per_shard: float = 1_000.0
    rate_limit_bytes_per_sec_per_shard: float = 1_048_576.0
    drain_interval_ms: float = 25.0

    # ── Retrier policy ────────────────────────────────────────────────────
    fail_if_throttled: bool = False
    retry_deadline_ms: float = 50.0

    # ── Producer backpressure ─────────────────────────────────────────────
    max_outstanding_records: int = 100_000

    # ── AWS endpoints (mainly for tests / custom routing) ─────────────────
    endpoint_url: str | None = None
    verify_ssl: bool = True
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None


__all__ = ["Config"]
