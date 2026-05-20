"""Hash-key arithmetic used to predict the destination shard.

The Kinesis service derives a 128-bit hash from each record's partition key by
taking the MD5 digest and interpreting it as an unsigned big-endian integer.
Users can also bypass this by supplying an explicit hash key as a decimal
string. Both representations are normalised to ``int`` here.

References:
- ``aws/kinesis/core/user_record.cc`` in the C++ KPL — same construction.
"""

from __future__ import annotations

import hashlib

_UINT128_MAX = (1 << 128) - 1


def md5_hash_key(partition_key: str) -> int:
    """Return ``md5(partition_key.utf8)`` as a big-endian uint128."""
    digest = hashlib.md5(partition_key.encode("utf-8"), usedforsecurity=False).digest()
    return int.from_bytes(digest, "big")


def parse_explicit_hash_key(s: str) -> int:
    """Parse a canonical decimal string into a uint128.

    Only digits ``0-9`` are accepted; leading/trailing whitespace, signs, and
    underscores are rejected so that the canonical Kinesis representation
    round-trips exactly.
    """
    if not s or not s.isascii() or not all("0" <= c <= "9" for c in s):
        raise ValueError(f"not a canonical decimal uint128: {s!r}")
    value = int(s)
    if value > _UINT128_MAX:
        raise ValueError(f"explicit hash key out of uint128 range: {s!r}")
    return value
