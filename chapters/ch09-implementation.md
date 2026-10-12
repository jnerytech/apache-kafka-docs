# Chapter 09: Implementation — Network Layer, Message Format, Log and Offsets

## Core Idea
Kafka uses the same versioned binary **record batch** format end to end: producer wire, broker disk and consumer fetch. That lets data move without being copied or converted (sendfile/zero-copy). The partition log is a sequence of offset-named segment files, appended at the tail, read by binary search and deleted one whole segment at a time. The source section is short (≈15 KB), so this chapter is mostly format and internals rather than tuning guidance.

## Frameworks Introduced
- **Record Batch (magic v2) format**: every record is written inside a batch. The batch header holds `baseOffset`, `batchLength`, `partitionLeaderEpoch`, `magic`=2, `crc`, `attributes`, `lastOffsetDelta`, `baseTimestamp`, `maxTimestamp`, `producerId`, `producerEpoch`, `baseSequence` and `recordsCount`, followed by the records.
  - When to use: when debugging corrupt or unexpected log data, writing a non-Java client, reading `kafka-dump-log` output, or reasoning about compression, idempotence and transactions at the byte level.
  - How: (1) read `baseOffset` (8 B) and `batchLength` (4 B). The batch takes `batchLength + 12` bytes on disk. (2) Read `magic` *before* trying to interpret the bytes between `batchLength` and `magic`. (3) Check the CRC-32C (Castagnoli), which covers everything from `attributes` to the end of the batch. (4) Decode `attributes`: bits 0–2 are the codec (0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd), bit 3 is timestampType, bit 4 isTransactional, bit 5 isControlBatch and bit 6 hasDeleteHorizonMs. (5) When the batch is compressed, the compressed record bytes start right after `recordsCount`.
  - Why it works: compression, the producer identity used for idempotence (`producerId`, `producerEpoch`, `baseSequence`) and the transaction flags all live **once per batch**, not once per record. Records then store only deltas as varints, so the per-record overhead is small. `partitionLeaderEpoch` is deliberately left **out of the CRC**, so the broker can stamp it on every incoming batch without recomputing the checksum.
  - Failure mode: a client that guesses the layout before reading `magic` will misread the bytes of old message formats (message sets, which existed before Kafka 0.11).
- **Compaction-preserving batch metadata**: when the log is cleaned, the cleaner keeps each batch's first and last offset and first and last sequence number, even if every record in the batch was removed.
  - When to use: when you see empty batches in a compacted or transactional topic, or `OutOfSequence` errors after a leader failover.
  - How: the broker detects duplicates by checking that the first and last sequence of an incoming batch match the last sequence it holds for that producer. Rebuilding producer state when a log reloads needs those sequence numbers, so empty batches are kept on purpose.
  - Failure mode: if the last sequence were dropped, a producer could get `OutOfSequence` after a leader change.
- **Segmented append-only log**: each partition is a directory (`my-topic-0`, `my-topic-1`). Each segment file is named after the first offset it contains (`00000000000000000000.log`, …). Appends always go to the last segment, which rolls to a new file once it reaches size *S* (for example 1 GB).
  - How: to read at an offset, (1) binary-search the in-memory range of segments to find the right file, (2) convert the global offset to an offset inside that file, (3) read a chunk of up to *S* bytes. Deletes use a copy-on-write segment list, so reads search an immutable snapshot while deletes run.
- **Startup log recovery**: on startup, the broker walks every entry of the **newest** segment. An entry is valid if `size + offset < file length` **and** its CRC matches. At the first invalid entry, the log is truncated to the last valid offset.
  - Why it works: the OS does not guarantee the order in which the inode and the data blocks are written. A crash can therefore *lose* blocks (truncation) or *add* garbage (the inode grew but the block was never written). The CRC catches the second case.
- **Group coordinator offset storage**: a consumer group commits its offsets to one broker, the group coordinator. The coordinator is chosen from the group name and found with `FindCoordinatorRequest`. Commits are appended to the compacted topic `__consumer_offsets` and also cached in memory for fast fetches.

## Key Concepts
- **Record batch**: the unit of write, storage and compression. It contains one or more records and its own header (magic 2).
- **Control batch / control record**: a batch with a single control record whose key is `version:int16, type:int16` (0 = abort, 1 = commit). Consumers use it to filter out aborted transactional data, and it is never delivered to applications.
- **Delete horizon (attribute bit 6)**: during compaction, `baseTimestamp` may be overwritten with the time at which tombstones (null payloads) or abort markers in the batch can be deleted. Bit 6 is then set.
- **Varint/varlong**: Protobuf-style variable-length integers. They are used for the per-record fields (length, timestampDelta, offsetDelta, key and value lengths, header count).
- **Record header**: a key/value pair with a non-null String key and a nullable `byte[]` value. Header order is preserved from producer to consumer.
- **Offset as message ID**: a monotonically increasing per-partition counter, used instead of producer-generated GUIDs. Kafka never needs a GUID-to-offset index.
- **`TransferableRecords.writeTo`**: the hook that lets data backed by a file use `FileChannel.transferTo`, which is sendfile/zero-copy, instead of a buffered copy inside the process.
- **Threading model**: one acceptor thread plus *N* NIO processor threads, each handling a fixed set of connections.
- **`CoordinatorLoadInProgressException`**: the error an offset fetch gets while a newly elected coordinator is still loading its `__consumer_offsets` partition. Back off and retry.

## Mental Models
- **Think of the batch header as "per-batch facts" and the record as "deltas from the header".** Timestamp and offset in each record are deltas from `baseTimestamp` and `baseOffset`. A batch's identity for idempotence is `producerId`, `producerEpoch` and `baseSequence`, plus `lastOffsetDelta`.
- **Use bigger batches when you want cheaper bytes.** Compression, CRC and producer metadata are paid once per batch, so batching (`linger.ms`/`batch.size`, ch05) reduces both the size on disk and the CPU spent per record.
- **Think of retention as working on segments, not records.** A segment is deleted only when its **largest** timestamp is older than the retention time, or when size-based retention needs space back. So data can stay well past the retention time until the active segment rolls.
- **Treat a committed offset like any other replicated write.** The commit is acknowledged only after all replicas of the `__consumer_offsets` partition have it. A slow ISR there means slow commits for every group mapped to that partition.

## Anti-patterns
- **Expecting `baseTimestamp` to stay fixed**: compaction rewrites it when the first record is removed, or sets it to the delete horizon. Use the timestamps of individual records, or `maxTimestamp`.
- **Treating empty batches in compacted logs as corruption**: they are kept on purpose so that producer sequence state survives.
- **Expecting exact time-based retention**: the whole segment is kept until its newest record expires, and the active segment is never deleted.
- **Relying on flush settings (*M* messages / *S* seconds) for durability**: they only limit how much a single crashed node can lose. Kafka's real durability comes from replication (ch07). Forcing frequent fsync hurts throughput.
- **Passing control records to application code**: a hand-written client must hide them and use them only to filter aborted transactions.
- **Treating `CoordinatorLoadInProgressException` as fatal**: it is a temporary state after a coordinator moves. Retry with backoff, and rediscover the coordinator if it has moved.

## Code Examples
```text
# On-disk batch overhead check
total_batch_bytes = batchLength + 12   # 8 B baseOffset + 4 B batchLength
CRC-32C scope    = attributes .. end of batch   (partitionLeaderEpoch, magic excluded)
```
- **What it demonstrates**: how to frame batches when parsing a `.log` file by hand, and why the leader epoch can change without the CRC changing.

```text
Record      := length:varint attributes:int8 timestampDelta:varlong offsetDelta:varint
               keyLength:varint key:byte[] valueLength:varint value:byte[]
               headersCount:varint [headerKeyLength:varint headerKey:String
                                    headerValueLength:varint value:byte[]]
ControlKey  := version:int16(=0) type:int16 (0=ABORT, 1=COMMIT)
```
- **What it demonstrates**: how compact a v2 record is (everything except `attributes` is a varint) and how commit and abort markers are encoded.

```bash
# Inspect batches/records on disk (standard Kafka tool)
bin/kafka-dump-log.sh --files /var/lib/kafka/my-topic-0/00000000000000000000.log --print-data-log
```
- **What it demonstrates**: how to see `baseOffset`, `lastOffsetDelta`, `producerId`, `isTransactional`, `isControl`, the codec and the CRC for each batch in a real segment.

## Reference Tables

| Attribute bits | Meaning | Values |
|---|---|---|
| 0–2 | compression codec | 0 none · 1 gzip · 2 snappy · 3 lz4 · 4 zstd |
| 3 | timestampType | CreateTime / LogAppendTime |
| 4 | isTransactional | 0 = not transactional |
| 5 | isControlBatch | 0 = data batch |
| 6 | hasDeleteHorizonMs | 1 = `baseTimestamp` is the compaction delete horizon |
| 7–15 | unused | — |

The *M*/*S* parameters in the source map to the broker and topic configs below. The config names and defaults come from the configuration reference (ch04), not from this section.

| Mechanism (source) | Config | Default | When to change |
|---|---|---|---|
| Segment roll size *S* | `log.segment.bytes` / `segment.bytes` | 1 GiB | Smaller for faster retention or compaction granularity, at the cost of more files and file handles |
| Flush after *M* messages | `log.flush.interval.messages` / `flush.messages` | Long.MAX (off) | Rarely. Rely on replication instead |
| Flush after *S* time | `log.flush.interval.ms` / `flush.ms` | unset (OS flush) | Rarely, same reason |
| Time retention (max timestamp in segment) | `retention.ms` | 7 days | Per-topic SLA |
| Size retention (disabled by default) | `retention.bytes` | -1 | To cap disk usage per partition |
| Max message / fetch size | `message.max.bytes`, `max.partition.fetch.bytes` | ~1 MB | Large records. Keep producer, broker and consumer limits aligned |
| Offset commit replication timeout | `offsets.commit.timeout.ms` | 5000 | When commits time out on a slow offsets ISR |

## Worked Example
**A transactional batch in a compacted topic, from produce to consumer restart.**

1. The producer (`producerId=42`, `producerEpoch=0`) sends a zstd batch of 3 records, offsets 100–102. The batch header has `baseOffset=100`, `lastOffsetDelta=2`, `baseSequence=7`, attribute bits 0–2 set to 4 (zstd) and bit 4 set to 1 (transactional). The broker sets `partitionLeaderEpoch` and leaves the CRC unchanged.
2. The broker checks for duplicates. Sequences 7–9 follow on from the last sequence it holds for producer 42, so it appends the batch to the active segment, for example `00000000000000000000.log`.
3. The transaction aborts. The coordinator writes a **control batch** with isControlBatch=1 and a control key of `type=0` (ABORT). A `read_committed` consumer that fetches offsets 100–103 reads the abort marker, drops records 100–102 and never sees the control record.
4. Later, compaction removes all three records because their keys were overwritten. The cleaner keeps an **empty batch** that still holds `baseOffset=100`, `lastOffsetDelta=2`, `baseSequence=7` and last sequence 9. Producer 42's state therefore survives a reload, and after a leader failover the producer does not get `OutOfSequence`. For the abort marker, `baseTimestamp` is rewritten to the delete horizon and bit 6 is set.
5. Meanwhile the consumer commits offset 104. Its group coordinator appends the commit to `__consumer_offsets`, waits until every replica has it, updates the in-memory cache and then acknowledges.
6. The coordinator broker fails. Another broker becomes leader for that `__consumer_offsets` partition and loads it. The restarted consumer's `OffsetFetchRequest` first gets `CoordinatorLoadInProgressException`. The consumer backs off, retries, receives 104 and resumes.

## Key Takeaways
1. Every record is stored inside a v2 record batch. Compression, idempotence and transaction metadata are per batch, so batching is how you lower the cost of each record.
2. When parsing, read `magic` before anything else and frame each batch as `batchLength + 12` bytes. The CRC-32C leaves out `partitionLeaderEpoch`.
3. Empty batches and rewritten `baseTimestamp` values in compacted logs are expected. They preserve producer sequence state and the tombstone delete horizon.
4. Retention deletes whole segments, using the segment's newest timestamp. Size-based retention is off by default. If both are enabled, either one can trigger a delete.
5. Durability comes from replication, not from flush settings. Startup recovery only checks the newest segment, using size and CRC, and truncates at the first bad entry.
6. Offset commits are writes to the compacted `__consumer_offsets` topic that are acknowledged only after full replication. Retry `CoordinatorLoadInProgressException` with backoff.

## Connects To
- **ch05**: zero-copy/sendfile, batching and compression design that this format makes possible.
- **ch06**: idempotent producer sequences and transactions, which the batch `producerId`/`baseSequence` fields and control markers implement.
- **ch07**: log compaction, tombstones and the delete horizon, and why replication matters more than fsync.
- **ch08**: the wire protocol that carries record batches unchanged (Produce, Fetch, FindCoordinator, OffsetCommit/OffsetFetch).
- **ch04**: segment, retention and flush topic configs.
- **ch17**: consumer group coordinator behavior under the KIP-848 rebalance protocol and the KIP-890 transaction protocol.
- **ch16**: tiered storage, which offloads closed segments from this log layout.
