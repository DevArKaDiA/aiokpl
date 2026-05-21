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
from aiokpl.config import Config
from aiokpl.hashing import md5_hash_key, parse_explicit_hash_key
from aiokpl.limiter import Limiter, ShardLimiter
from aiokpl.outcome import Outcome
from aiokpl.producer import Producer
from aiokpl.reducer import Batch, Batchable, Reducer
from aiokpl.result import Attempt, RecordResult
from aiokpl.retrier import Retrier
from aiokpl.sender import PerRecordOutcome, Sender, SendOutcome
from aiokpl.shard_map import Shard, ShardMap, ShardMapState
from aiokpl.token_bucket import TokenBucket

__all__ = [
    "MAGIC",
    "AggregatedBatch",
    "Aggregator",
    "Attempt",
    "Batch",
    "Batchable",
    "Collector",
    "Config",
    "DecodedRecord",
    "Limiter",
    "Outcome",
    "PerRecordOutcome",
    "Producer",
    "PutRecordsBatch",
    "RecordResult",
    "Reducer",
    "Retrier",
    "SendOutcome",
    "Sender",
    "Shard",
    "ShardLimiter",
    "ShardMap",
    "ShardMapState",
    "Tag",
    "TokenBucket",
    "UserRecord",
    "decode_aggregated",
    "encode_aggregated",
    "is_aggregated",
    "md5_hash_key",
    "parse_explicit_hash_key",
]
__version__ = "0.0.1"
