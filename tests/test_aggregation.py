"""Tests for ``aiokpl.aggregation`` — the KPL aggregation codec."""

from __future__ import annotations

import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aiokpl.aggregation import (
    MAGIC,
    DecodedRecord,
    Tag,
    UserRecord,
    _AggregatedBuilder,
    _decode_varint,
    _encode_varint,
    _skip_unknown,
    decode_aggregated,
    encode_aggregated,
    is_aggregated,
)

# ─── Varint primitives ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (300, b"\xac\x02"),
        ((1 << 63) - 1, b"\xff\xff\xff\xff\xff\xff\xff\xff\x7f"),
    ],
)
def test_varint_encode_known(value: int, expected: bytes) -> None:
    out = bytearray()
    _encode_varint(value, out)
    assert bytes(out) == expected


@given(st.integers(min_value=0, max_value=(1 << 64) - 1))
def test_varint_roundtrip(n: int) -> None:
    out = bytearray()
    _encode_varint(n, out)
    decoded, pos = _decode_varint(bytes(out), 0)
    assert decoded == n
    assert pos == len(out)


def test_decode_varint_truncated() -> None:
    with pytest.raises(ValueError, match="truncated varint"):
        _decode_varint(b"\x80\x80", 0)


def test_decode_varint_too_long() -> None:
    # 11 continuation bytes — exceeds uint64 representable range.
    with pytest.raises(ValueError, match="varint too long"):
        _decode_varint(b"\x80" * 11, 0)


# ─── _skip_unknown ─────────────────────────────────────────────────────────


def test_skip_unknown_varint() -> None:
    assert _skip_unknown(b"\x05extra", 0, 0) == 1


def test_skip_unknown_length_delimited() -> None:
    assert _skip_unknown(b"\x03abcXX", 0, 2) == 4


def test_skip_unknown_truncated_length_delimited() -> None:
    with pytest.raises(ValueError, match="truncated"):
        _skip_unknown(b"\x05ab", 0, 2)


def test_skip_unknown_bad_wire_type() -> None:
    with pytest.raises(ValueError, match="unsupported wire type"):
        _skip_unknown(b"", 0, 5)


# ─── encode_aggregated short-circuit ───────────────────────────────────────


def test_encode_empty_raises() -> None:
    with pytest.raises(ValueError, match="at least one"):
        encode_aggregated([])


def test_encode_single_record_is_raw() -> None:
    ur = UserRecord(partition_key="pk", data=b"hello")
    assert encode_aggregated([ur]) == b"hello"


# ─── encode + decode round-trips ───────────────────────────────────────────


def test_encode_decode_basic_two_records() -> None:
    records = [
        UserRecord(partition_key="alpha", data=b"one"),
        UserRecord(partition_key="beta", data=b"two", explicit_hash_key="42"),
    ]
    blob = encode_aggregated(records)
    assert blob.startswith(MAGIC)
    assert is_aggregated(blob)

    decoded = decode_aggregated(blob)
    assert decoded == [
        DecodedRecord(partition_key="alpha", explicit_hash_key=None, data=b"one", tags=()),
        DecodedRecord(partition_key="beta", explicit_hash_key="42", data=b"two", tags=()),
    ]


def test_encode_decode_with_tags() -> None:
    records = [
        UserRecord(
            partition_key="pk",
            data=b"x",
            tags=(Tag(key="k1", value="v1"), Tag(key="k2")),
        ),
        UserRecord(partition_key="pk", data=b"y"),
    ]
    blob = encode_aggregated(records)
    decoded = decode_aggregated(blob)
    assert decoded[0].tags == (Tag(key="k1", value="v1"), Tag(key="k2", value=None))
    assert decoded[1].tags == ()
    # PK dedup: both records share the same partition key.
    assert decoded[0].partition_key == decoded[1].partition_key == "pk"


def test_pk_table_dedup_is_order_preserving() -> None:
    # Build via the builder so we can inspect the table directly.
    builder = _AggregatedBuilder()
    for pk in ["a", "b", "a", "c", "b"]:
        builder.add(UserRecord(partition_key=pk, data=b""))
    assert builder.partition_keys == ["a", "b", "c"]


def test_ehk_table_dedup_is_order_preserving() -> None:
    builder = _AggregatedBuilder()
    for ehk in ["1", "2", "1", "3"]:
        builder.add(UserRecord(partition_key="pk", data=b"", explicit_hash_key=ehk))
    assert builder.explicit_hash_keys == ["1", "2", "3"]


# ─── is_aggregated ─────────────────────────────────────────────────────────


def test_is_aggregated_true() -> None:
    blob = encode_aggregated(
        [UserRecord(partition_key="a", data=b"1"), UserRecord(partition_key="b", data=b"2")]
    )
    assert is_aggregated(blob)


def test_is_aggregated_false_no_magic() -> None:
    assert not is_aggregated(b"plain bytes that are long enough to exceed footer length zzzzzz")


def test_is_aggregated_false_too_short() -> None:
    assert not is_aggregated(MAGIC + b"\x00")


def test_is_aggregated_false_long_enough_no_magic() -> None:
    assert not is_aggregated(b"\x00" * 32)


# ─── decode error paths ────────────────────────────────────────────────────


def test_decode_too_short() -> None:
    with pytest.raises(ValueError, match="too short"):
        decode_aggregated(b"\x00")


def test_decode_bad_magic() -> None:
    payload = b"XXXX" + b"\x00" * 16
    with pytest.raises(ValueError, match="bad magic"):
        decode_aggregated(payload)


def test_decode_bad_md5() -> None:
    blob = encode_aggregated(
        [UserRecord(partition_key="a", data=b"1"), UserRecord(partition_key="b", data=b"2")]
    )
    tampered = blob[:-1] + bytes([blob[-1] ^ 0xFF])
    with pytest.raises(ValueError, match="MD5"):
        decode_aggregated(tampered)


def test_decode_malformed_varint_inside_proto() -> None:
    # A proto with a single byte that looks like a continuation but is truncated.
    proto = b"\x80"
    md5 = hashlib.md5(proto).digest()
    blob = MAGIC + proto + md5
    with pytest.raises(ValueError):
        decode_aggregated(blob)


def _make_proto_with(record_blob: bytes, pk_table: list[str], ehk_table: list[str]) -> bytes:
    """Tiny helper that hand-builds a proto from precomputed pieces."""
    out = bytearray()
    for pk in pk_table:
        b = pk.encode("utf-8")
        out += b"\x0a" + bytes([len(b)]) + b
    for ehk in ehk_table:
        b = ehk.encode("utf-8")
        out += b"\x12" + bytes([len(b)]) + b
    out += b"\x1a" + bytes([len(record_blob)]) + record_blob
    return bytes(out)


def _wrap(proto: bytes) -> bytes:
    return MAGIC + proto + hashlib.md5(proto).digest()


def test_decode_pk_index_out_of_range() -> None:
    # Record has pk_index=5 but the table is empty.
    record = b"\x08\x05\x1a\x00"  # pk_index=5, data=b""
    blob = _wrap(_make_proto_with(record, pk_table=[], ehk_table=[]))
    with pytest.raises(ValueError, match="partition_key_index"):
        decode_aggregated(blob)


def test_decode_ehk_index_out_of_range() -> None:
    record = b"\x08\x00\x10\x05\x1a\x00"  # pk=0, ehk=5, data=""
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match="explicit_hash_key_index"):
        decode_aggregated(blob)


def test_decode_missing_required_pk_index() -> None:
    record = b"\x1a\x00"  # data=b"", no pk_index
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match="partition_key_index is required"):
        decode_aggregated(blob)


def test_decode_missing_required_data() -> None:
    record = b"\x08\x00"  # pk_index=0, no data
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match="data is required"):
        decode_aggregated(blob)


def test_decode_truncated_record_data_field() -> None:
    # Inside the record blob: tag for data, length=10, but only 1 byte follows.
    record = b"\x08\x00\x1a\x0aX"
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match=r"truncated record.data"):
        decode_aggregated(blob)


def test_decode_truncated_pk_table_entry() -> None:
    proto = b"\x0a\x0aab"  # length=10 but only 2 bytes follow
    blob = _wrap(proto)
    with pytest.raises(ValueError, match="truncated partition_key_table"):
        decode_aggregated(blob)


def test_decode_truncated_ehk_table_entry() -> None:
    proto = b"\x12\x0aab"
    blob = _wrap(proto)
    with pytest.raises(ValueError, match="truncated explicit_hash_key_table"):
        decode_aggregated(blob)


def test_decode_truncated_record_entry() -> None:
    proto = b"\x1a\x0aab"
    blob = _wrap(proto)
    with pytest.raises(ValueError, match="truncated record entry"):
        decode_aggregated(blob)


def test_decode_unknown_field_in_aggregated_is_skipped() -> None:
    # Add an unknown varint field (field_number=99, wire=0) after the records.
    records = [
        UserRecord(partition_key="a", data=b"1"),
        UserRecord(partition_key="b", data=b"2"),
    ]
    blob = encode_aggregated(records)
    proto = blob[len(MAGIC) : -16]
    extra = b"\xf8\x06\x2a"  # tag for field 99, varint wire; value 42
    new_proto = proto + extra
    new_blob = MAGIC + new_proto + hashlib.md5(new_proto).digest()
    decoded = decode_aggregated(new_blob)
    assert [r.data for r in decoded] == [b"1", b"2"]


def test_decode_unknown_field_in_record_is_skipped() -> None:
    # Record with an extra unknown length-delimited field 99.
    extra = b"\xfa\x06\x03abc"  # field 99, length-delimited, "abc"
    record = b"\x08\x00\x1a\x01x" + extra
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    decoded = decode_aggregated(blob)
    assert decoded[0].data == b"x"


def test_decode_tag_missing_key() -> None:
    # Tag with only field 2 (value) but no field 1 (key).
    tag_blob = b"\x12\x01v"
    record = b"\x08\x00\x1a\x00\x22" + bytes([len(tag_blob)]) + tag_blob
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match=r"Tag.key is required"):
        decode_aggregated(blob)


def test_decode_tag_truncated_key() -> None:
    tag_blob = b"\x0a\x0aX"
    record = b"\x08\x00\x1a\x00\x22" + bytes([len(tag_blob)]) + tag_blob
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match=r"truncated tag.key"):
        decode_aggregated(blob)


def test_decode_tag_truncated_value() -> None:
    tag_blob = b"\x0a\x01k\x12\x0aY"
    record = b"\x08\x00\x1a\x00\x22" + bytes([len(tag_blob)]) + tag_blob
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match=r"truncated tag.value"):
        decode_aggregated(blob)


def test_decode_tag_unknown_field_skipped() -> None:
    # Tag with a key and an unknown varint field — must round-trip the key.
    tag_blob = b"\x0a\x01k\x18\x07"  # field 1 string "k", field 3 varint 7
    record = b"\x08\x00\x1a\x00\x22" + bytes([len(tag_blob)]) + tag_blob
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    decoded = decode_aggregated(blob)
    assert decoded[0].tags == (Tag(key="k", value=None),)


def test_decode_truncated_record_tag_field() -> None:
    # Record claims a tag field of length 10 but only 1 byte follows.
    record = b"\x08\x00\x1a\x00\x22\x0aX"
    blob = _wrap(_make_proto_with(record, pk_table=["pk"], ehk_table=[]))
    with pytest.raises(ValueError, match=r"truncated record.tag"):
        decode_aggregated(blob)


# ─── Golden bytes ──────────────────────────────────────────────────────────


def test_golden_bytes_two_record_blob() -> None:
    """Exact byte expectation for a small, hand-traced blob.

    Records:
        UserRecord(partition_key="a", data=b"x")
        UserRecord(partition_key="b", data=b"y")

    Proto layout::

        pk_table:        0a 01 61          (field 1, len 1, "a")
                         0a 01 62          (field 1, len 1, "b")
        records[0]:      1a 05 08 00 1a 01 78
                         (field 3 len 5 -> Record { pk_idx=0, data="x" })
                         inner: 08 00  (pk_idx=0)
                                1a 01 78  (data="x")
        records[1]:      1a 05 08 01 1a 01 79
    """
    proto = b"\x0a\x01a\x0a\x01b\x1a\x05\x08\x00\x1a\x01x\x1a\x05\x08\x01\x1a\x01y"
    expected = MAGIC + proto + hashlib.md5(proto).digest()
    blob = encode_aggregated(
        [
            UserRecord(partition_key="a", data=b"x"),
            UserRecord(partition_key="b", data=b"y"),
        ]
    )
    assert blob == expected


# ─── Property-based round-trip ─────────────────────────────────────────────


_pk_strategy = st.text(min_size=1, max_size=16)
_data_strategy = st.binary(max_size=32)
_ehk_strategy = st.one_of(
    st.none(),
    st.integers(min_value=0, max_value=(1 << 128) - 1).map(str),
)
_tag_strategy = st.builds(
    Tag,
    key=st.text(min_size=1, max_size=8),
    value=st.one_of(st.none(), st.text(max_size=8)),
)
_user_record_strategy = st.builds(
    UserRecord,
    partition_key=_pk_strategy,
    data=_data_strategy,
    explicit_hash_key=_ehk_strategy,
    tags=st.lists(_tag_strategy, max_size=3).map(tuple),
)


@given(st.lists(_user_record_strategy, min_size=2, max_size=8))
def test_roundtrip_property(records: list[UserRecord]) -> None:
    blob = encode_aggregated(records)
    decoded = decode_aggregated(blob)
    assert len(decoded) == len(records)
    for original, recovered in zip(records, decoded, strict=True):
        assert recovered.partition_key == original.partition_key
        assert recovered.data == original.data
        assert recovered.explicit_hash_key == original.explicit_hash_key
        assert recovered.tags == original.tags


@given(st.lists(_pk_strategy, min_size=1, max_size=10))
def test_pk_dedup_idempotent(pks: list[str]) -> None:
    builder1 = _AggregatedBuilder()
    builder2 = _AggregatedBuilder()
    for pk in pks:
        builder1.add(UserRecord(partition_key=pk, data=b""))
    for pk in pks * 2:  # add each twice — table must be the same
        builder2.add(UserRecord(partition_key=pk, data=b""))
    assert builder1.partition_keys == builder2.partition_keys


# ─── Optional conformance vs aws-kinesis-agg ───────────────────────────────


def test_conformance_aws_kinesis_agg() -> None:
    pytest.importorskip("aws_kinesis_agg")
    from aws_kinesis_agg.aggregator import RecordAggregator  # ty: ignore[unresolved-import]

    agg = RecordAggregator()
    agg.add_user_record("a", b"x")
    agg.add_user_record("b", b"y")
    pk, ehk, data = agg.clear_and_get().get_contents()
    # The reference implementation should produce a byte-identical blob.
    ours = encode_aggregated(
        [
            UserRecord(partition_key="a", data=b"x"),
            UserRecord(partition_key="b", data=b"y"),
        ]
    )
    assert ours == data
    assert pk in {"a", "b"}
    assert ehk is None or isinstance(ehk, str)
