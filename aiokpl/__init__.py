"""aiokpl — pure-Python async Kinesis producer."""

from aiokpl.aggregation import (
    MAGIC,
    DecodedRecord,
    Tag,
    UserRecord,
    decode_aggregated,
    encode_aggregated,
    is_aggregated,
)
from aiokpl.hashing import md5_hash_key, parse_explicit_hash_key

__all__ = [
    "MAGIC",
    "DecodedRecord",
    "Tag",
    "UserRecord",
    "decode_aggregated",
    "encode_aggregated",
    "is_aggregated",
    "md5_hash_key",
    "parse_explicit_hash_key",
]
__version__ = "0.0.1"
