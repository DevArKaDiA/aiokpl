"""KPL aggregation wire-format codec.

Byte-exact reimplementation of the Amazon Kinesis Producer Library's
aggregated-record framing, in pure Python with a hand-rolled protobuf
encoder/decoder. No third-party dependencies — the schema is frozen by AWS
and only uses two protobuf wire types (varint and length-delimited).

Wire format::

    [ MAGIC (4 bytes) | protobuf(AggregatedRecord) | MD5(protobuf) (16 bytes) ]

Schema (from ``aws/kinesis/protobuf/messages.proto``)::

    message Tag             { required string key=1; optional string value=2; }
    message Record          { required uint64 partition_key_index=1;
                              optional uint64 explicit_hash_key_index=2;
                              required bytes  data=3;
                              repeated Tag    tags=4; }
    message AggregatedRecord{ repeated string partition_key_table=1;
                              repeated string explicit_hash_key_table=2;
                              repeated Record records=3; }

Encoder semantics match ``aws/kinesis/core/kinesis_record.cc`` in the C++ KPL:
a one-record "batch" is sent raw (no magic, no MD5) — this is the caller's
responsibility to interpret at the Kinesis API level.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field

MAGIC: bytes = b"\xf3\x89\x9a\xc2"
_MD5_LEN = 16
_WIRE_VARINT = 0
_WIRE_LENGTH_DELIMITED = 2


# ─── Public dataclasses ────────────────────────────────────────────────────


@dataclass(slots=True, frozen=True)
class Tag:
    """A user-defined ``(key, value?)`` tag attached to a record."""

    key: str
    value: str | None = None


@dataclass(slots=True, frozen=True)
class UserRecord:
    """A logical record submitted by the user, prior to aggregation."""

    partition_key: str
    data: bytes
    explicit_hash_key: str | None = None
    tags: tuple[Tag, ...] = ()


@dataclass(slots=True, frozen=True)
class DecodedRecord:
    """A record recovered from an aggregated blob."""

    partition_key: str
    explicit_hash_key: str | None
    data: bytes
    tags: tuple[Tag, ...]


# ─── Hand-rolled protobuf primitives ───────────────────────────────────────


def _encode_varint(value: int, out: bytearray) -> None:
    # Non-negative uint64 only; the schema uses no signed/zigzag fields.
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value & 0x7F)


def _decode_varint(buf: bytes, pos: int) -> tuple[int, int]:
    # Reads a uint64 varint at ``pos`` and returns (value, new_pos). After 10
    # bytes (the max for a uint64 varint) ``shift`` reaches 70 and we abort —
    # any further bytes would overflow uint64.
    result = 0
    shift = 0
    n = len(buf)
    while True:
        if pos >= n:
            raise ValueError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long")


def _encode_tag(field_number: int, wire_type: int, out: bytearray) -> None:
    _encode_varint((field_number << 3) | wire_type, out)


def _encode_length_delimited(field_number: int, payload: bytes, out: bytearray) -> None:
    _encode_tag(field_number, _WIRE_LENGTH_DELIMITED, out)
    _encode_varint(len(payload), out)
    out.extend(payload)


def _encode_uint64_field(field_number: int, value: int, out: bytearray) -> None:
    _encode_tag(field_number, _WIRE_VARINT, out)
    _encode_varint(value, out)


def _skip_unknown(buf: bytes, pos: int, wire_type: int) -> int:
    # Tolerate unknown fields with known wire types — forward-compat with any
    # future Kinesis-side additions. Unknown wire types are a parse error.
    if wire_type == _WIRE_VARINT:
        _, pos = _decode_varint(buf, pos)
        return pos
    if wire_type == _WIRE_LENGTH_DELIMITED:
        length, pos = _decode_varint(buf, pos)
        end = pos + length
        if end > len(buf):
            raise ValueError("truncated length-delimited field")
        return end
    raise ValueError(f"unsupported wire type: {wire_type}")


# ─── Message-level encoders ────────────────────────────────────────────────


def _encode_tag_msg(t: Tag) -> bytes:
    out = bytearray()
    _encode_length_delimited(1, t.key.encode("utf-8"), out)
    if t.value is not None:
        _encode_length_delimited(2, t.value.encode("utf-8"), out)
    return bytes(out)


def _encode_record_msg(
    pk_index: int,
    ehk_index: int | None,
    data: bytes,
    tags: tuple[Tag, ...],
) -> bytes:
    out = bytearray()
    _encode_uint64_field(1, pk_index, out)
    if ehk_index is not None:
        _encode_uint64_field(2, ehk_index, out)
    _encode_length_delimited(3, data, out)
    for t in tags:
        _encode_length_delimited(4, _encode_tag_msg(t), out)
    return bytes(out)


@dataclass(slots=True)
class _AggregatedBuilder:
    partition_keys: list[str] = field(default_factory=list)
    explicit_hash_keys: list[str] = field(default_factory=list)
    _pk_index: dict[str, int] = field(default_factory=dict)
    _ehk_index: dict[str, int] = field(default_factory=dict)
    records: list[bytes] = field(default_factory=list)

    def add(self, record: UserRecord) -> None:
        pk = record.partition_key
        idx = self._pk_index.get(pk)
        if idx is None:
            idx = len(self.partition_keys)
            self._pk_index[pk] = idx
            self.partition_keys.append(pk)

        ehk_idx: int | None = None
        if record.explicit_hash_key is not None:
            ehk = record.explicit_hash_key
            ehk_idx = self._ehk_index.get(ehk)
            if ehk_idx is None:
                ehk_idx = len(self.explicit_hash_keys)
                self._ehk_index[ehk] = ehk_idx
                self.explicit_hash_keys.append(ehk)

        self.records.append(_encode_record_msg(idx, ehk_idx, record.data, record.tags))

    def serialize(self) -> bytes:
        out = bytearray()
        for pk in self.partition_keys:
            _encode_length_delimited(1, pk.encode("utf-8"), out)
        for ehk in self.explicit_hash_keys:
            _encode_length_delimited(2, ehk.encode("utf-8"), out)
        for rec in self.records:
            _encode_length_delimited(3, rec, out)
        return bytes(out)


# ─── Public encode / decode ────────────────────────────────────────────────


def encode_aggregated(records: Sequence[UserRecord]) -> bytes:
    """Encode one or more :class:`UserRecord` into the KPL wire format.

    A single-record batch is short-circuited to the raw ``data`` bytes — this
    matches ``KinesisRecord::serialize`` in the C++ KPL, where the producer
    sends a single record un-aggregated using the user's partition key.
    """
    if not records:
        raise ValueError("encode_aggregated requires at least one record")
    if len(records) == 1:
        return records[0].data

    builder = _AggregatedBuilder()
    for r in records:
        builder.add(r)
    payload = builder.serialize()
    checksum = hashlib.md5(payload, usedforsecurity=False).digest()
    return MAGIC + payload + checksum


def is_aggregated(payload: bytes) -> bool:
    """Cheap check: magic prefix and room for an MD5 footer."""
    return len(payload) >= len(MAGIC) + _MD5_LEN and payload.startswith(MAGIC)


def _decode_tag_msg(buf: bytes) -> Tag:
    pos = 0
    n = len(buf)
    key: str | None = None
    value: str | None = None
    while pos < n:
        tag, pos = _decode_varint(buf, pos)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if field_number == 1 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(buf, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated tag.key")
            key = buf[pos:end].decode("utf-8")
            pos = end
        elif field_number == 2 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(buf, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated tag.value")
            value = buf[pos:end].decode("utf-8")
            pos = end
        else:
            pos = _skip_unknown(buf, pos, wire_type)
    if key is None:
        raise ValueError("Tag.key is required")
    return Tag(key=key, value=value)


def _decode_record_msg(
    buf: bytes,
    pk_table: list[str],
    ehk_table: list[str],
) -> DecodedRecord:
    pos = 0
    n = len(buf)
    pk_index: int | None = None
    ehk_index: int | None = None
    data: bytes | None = None
    tags: list[Tag] = []
    while pos < n:
        tag, pos = _decode_varint(buf, pos)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if field_number == 1 and wire_type == _WIRE_VARINT:
            pk_index, pos = _decode_varint(buf, pos)
        elif field_number == 2 and wire_type == _WIRE_VARINT:
            ehk_index, pos = _decode_varint(buf, pos)
        elif field_number == 3 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(buf, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated record.data")
            data = buf[pos:end]
            pos = end
        elif field_number == 4 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(buf, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated record.tag")
            tags.append(_decode_tag_msg(buf[pos:end]))
            pos = end
        else:
            pos = _skip_unknown(buf, pos, wire_type)
    if pk_index is None:
        raise ValueError("Record.partition_key_index is required")
    if data is None:
        raise ValueError("Record.data is required")
    if pk_index >= len(pk_table):
        raise ValueError(f"partition_key_index {pk_index} out of range")
    ehk: str | None = None
    if ehk_index is not None:
        if ehk_index >= len(ehk_table):
            raise ValueError(f"explicit_hash_key_index {ehk_index} out of range")
        ehk = ehk_table[ehk_index]
    return DecodedRecord(
        partition_key=pk_table[pk_index],
        explicit_hash_key=ehk,
        data=data,
        tags=tuple(tags),
    )


def decode_aggregated(payload: bytes) -> list[DecodedRecord]:
    """Decode a KPL-aggregated wire blob into a list of :class:`DecodedRecord`.

    Raises :class:`ValueError` on missing/incorrect magic, MD5 mismatch,
    malformed protobuf, out-of-range table indices, or truncated fields.
    """
    if len(payload) < len(MAGIC) + _MD5_LEN:
        raise ValueError("payload too short to be an aggregated record")
    if not payload.startswith(MAGIC):
        raise ValueError("bad magic number")
    proto = payload[len(MAGIC) : -_MD5_LEN]
    expected = payload[-_MD5_LEN:]
    actual = hashlib.md5(proto, usedforsecurity=False).digest()
    if actual != expected:
        raise ValueError("MD5 checksum mismatch")

    pk_table: list[str] = []
    ehk_table: list[str] = []
    record_blobs: list[bytes] = []

    pos = 0
    n = len(proto)
    while pos < n:
        tag, pos = _decode_varint(proto, pos)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if field_number == 1 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(proto, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated partition_key_table entry")
            pk_table.append(proto[pos:end].decode("utf-8"))
            pos = end
        elif field_number == 2 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(proto, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated explicit_hash_key_table entry")
            ehk_table.append(proto[pos:end].decode("utf-8"))
            pos = end
        elif field_number == 3 and wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _decode_varint(proto, pos)
            end = pos + length
            if end > n:
                raise ValueError("truncated record entry")
            record_blobs.append(proto[pos:end])
            pos = end
        else:
            pos = _skip_unknown(proto, pos, wire_type)

    return [_decode_record_msg(blob, pk_table, ehk_table) for blob in record_blobs]
