# Aggregation format

The KPL aggregation format is the **only** wire format that matters: KCL
consumers and the AWS-published deaggregation libraries all expect it.
`aiokpl` produces it byte-exact so existing consumers deaggregate
transparently.

The canonical specification lives in the C++ KPL repository at
[`aggregation-format.md`](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md).
Everything below is a summary of how `aiokpl` implements it.

## Byte layout

```
0               4                  N          N+15
+---+---+---+---+==================+---+...+---+
|  MAGIC NUMBER | PROTOBUF MESSAGE |    MD5    |
+---+---+---+---+==================+---+...+---+
```

- **Magic number** (4 bytes): `0xF3 0x89 0x9A 0xC2`. Lets a KCL consumer
  detect an aggregated record on sight.
- **Protobuf message**: the serialized `AggregatedRecord` (see schema
  below). Uses proto2.
- **MD5 footer** (16 bytes): MD5 of the serialized protobuf bytes. Used
  for corruption detection only — not for security.

## Schema

```protobuf
message AggregatedRecord {
  repeated string partition_key_table     = 1;
  repeated string explicit_hash_key_table = 2;
  repeated Record records                 = 3;
}

message Record {
  required uint64 partition_key_index     = 1;
  optional uint64 explicit_hash_key_index = 2;
  required bytes  data                    = 3;
  repeated Tag    tags                    = 4;
}

message Tag {
  required string key   = 1;
  optional string value = 2;
}
```

The schema is **frozen** by AWS. `aiokpl` does not generate code from a
`.proto` file — it hand-rolls a ~150-line encoder / decoder for these
seven fields in pure Python, with no `protobuf` runtime dependency. See
[`aiokpl.aggregation`](reference/aiokpl/aggregation.md).

## What we depend on

- **Single-record short-circuit.** A batch of exactly one record is sent
  **un-aggregated** — raw user bytes with the original partition key. This
  matches `KinesisRecord::serialize` in the C++ KPL. KCL consumers handle
  both cases transparently.
- **Dedup tables.** When two records in the same aggregate share a
  partition key (or explicit hash key), `aiokpl` emits the key once in
  the table and references its index from both records. This is a packing
  optimization the format allows but does not require.
- **MD5 footer is corruption detection, not security.** We construct the
  MD5 with `usedforsecurity=False` so FIPS-restricted runtimes do not
  refuse to compute it.

## What we do not depend on

- **Tags.** The schema reserves field 4 of `Record` for tags. KPL and KCL
  have never implemented them. `aiokpl` can encode and decode tags
  losslessly, but no production path emits them.
- **Wire ordering of the tables.** The protobuf format does not constrain
  field ordering between repeated entries. `aiokpl` emits all
  `partition_key_table` entries first, then all `explicit_hash_key_table`
  entries, then all records — matching the C++ KPL byte-for-byte for
  conformance testing.

## When aggregated

When a batch is aggregated, the API-level partition key submitted to
`PutRecords` is `"a"` and `ExplicitHashKey` is set to a value inside the
predicted shard's hash range (`aiokpl` uses the first record's hash key).
This is how the producer steers an aggregated blob at a specific shard
even though its API-level partition key is meaningless.

See [`aiokpl.aggregation`](reference/aiokpl/aggregation.md) for the
encoder, decoder, and `UserRecord` / `DecodedRecord` dataclasses.
