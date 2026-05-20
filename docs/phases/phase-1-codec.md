# Phase 1 — Aggregation codec

**Status:** Done.

## What ships

- [`aiokpl.aggregation`](../reference/aiokpl/aggregation.md) — byte-exact
  encoder and decoder for the KPL aggregated-record wire format. Hand-rolled
  protobuf primitives (varint and length-delimited only), `MAGIC` prefix,
  MD5 footer, and dedup tables for partition keys and explicit hash keys.
- [`aiokpl.hashing`](../reference/aiokpl/hashing.md) — `md5_hash_key` and
  `parse_explicit_hash_key`, used to convert a partition key or an explicit
  hash key into the uint128 the shard map operates on.

## Conformance

The codec is conformance-tested against the C++ KPL bytes captured by
`etspaceman/kinesis-mock` (the same Scala backend LocalStack uses for
Kinesis). For every fixed set of records, our serialized output is **byte-for
-byte equal** to the C++ KPL's, and our decoder round-trips through both
our own encoder and `aws-kinesis-agg`.

The single-record short-circuit matches `KinesisRecord::serialize` in the
C++ source: a batch of exactly one record is emitted un-aggregated with
the user's original partition key.

## Zero runtime dependencies

!!! info "This is a deliberate feature, not an oversight."
    The KPL aggregation schema has 3 messages and 7 fields total, all of
    which use only two protobuf wire types: varint and length-delimited.
    Pulling in the `protobuf` runtime to handle this would dominate the
    package's footprint, add an FFI surface, and introduce a `protoc`
    build step. So we wrote ~150 lines of encoder/decoder by hand.

    The schema is frozen by AWS — there will never be a version 2.

## Public surface

```python
from aiokpl import (
    UserRecord, DecodedRecord, Tag,
    encode_aggregated, decode_aggregated, is_aggregated,
    md5_hash_key, parse_explicit_hash_key,
    MAGIC,
)
```

Full details, parameter types, and exceptions are in the
[API reference](../reference/aiokpl/aggregation.md).
