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
from aiokpl.aggregator import AggregatedBatch, Aggregator
from aiokpl.collector import Collector, PutRecordsBatch
from aiokpl.hashing import md5_hash_key, parse_explicit_hash_key
from aiokpl.reducer import Batch, Batchable, Reducer
from aiokpl.shard_map import Shard, ShardMap, ShardMapState

__all__ = [
    "MAGIC",
    "AggregatedBatch",
    "Aggregator",
    "Batch",
    "Batchable",
    "Collector",
    "DecodedRecord",
    "PutRecordsBatch",
    "Reducer",
    "Shard",
    "ShardMap",
    "ShardMapState",
    "Tag",
    "UserRecord",
    "decode_aggregated",
    "encode_aggregated",
    "is_aggregated",
    "md5_hash_key",
    "parse_explicit_hash_key",
]
__version__ = "0.0.1"
