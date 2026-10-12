# Chapter 05: Design — Persistence, Efficiency, Producer and Consumer

## Core Idea
Kafka is built like a database log, not a traditional queue. It appends batches sequentially to files, leaves caching to the OS pagecache, sends bytes zero-copy in one shared binary format, and tracks consumption as a single offset per partition. That design is why Kafka stays fast as data grows, why it can keep data for days, and why consumers can rewind. (The source is a conceptual design essay with few tunables. Config names marked † in the tables come from the client config reference, not from this section.)

## Frameworks Introduced
- **Pagecache-centric persistence ("Don't fear the filesystem")**: write every record straight to an on-disk log, which in practice means into the kernel pagecache with no forced fsync. Let the OS do read-ahead and write-behind, and keep no in-process cache.
  - When to use: sizing broker memory, explaining why the broker JVM heap stays small, and judging any "keep it in memory" proposal.
  - How: (1) give the JVM a modest heap and leave the rest of RAM to the OS. On a 32 GB host that yields about 28–30 GB of cache. (2) Rely on linear reads and writes. On the cited 6×7200rpm array, linear writes reach about 600 MB/s while random writes manage about 100 kB/s, a gap of more than 6000×. (3) Rely on cache that survives a process restart. A rebuilt in-process cache would take about 10 minutes for 10 GB. (4) Let the OS keep cache and file coherent.
  - Why it works / failure mode: Java objects roughly double the size of the data, and GC slows as the heap grows. The pagecache holds compact bytes and costs no GC. An in-process cache would also be stored twice, once in the heap and once in pagecache. It breaks down when other processes compete for RAM or when access is random. In those cases reads go to disk and lagging consumers slow everyone down.
- **Constant-time log structure (O(1) append/read vs O(log N) BTree)**: a persistent queue made of plain file appends and sequential reads. Reads never block writes or each other, and speed does not depend on data size.
  - When to use: the case for long retention (for example a week) and for cheap, large, slow-seek SATA disks.
  - How: avoid per-message metadata trees. Each disk seek costs about 10 ms and runs serially. BTree performance becomes *superlinear* once data outgrows the cache, so doubling the data more than doubles the slowdown.
- **Batching via the "message set" abstraction**: group records in the protocol, in the broker's log append and in consumer fetches. This removes the small-I/O problem.
  - How: the producer accumulates per partition, the broker appends whole chunks and the consumer fetches large linear chunks. The result is larger packets, larger sequential disk operations and contiguous memory, often orders of magnitude faster.
- **Zero-copy transfer (`sendfile`)**: producer, broker log and consumer share one standardized binary format, so the broker sends log chunks from pagecache straight to the socket.
  - How: the normal path is 4 copies plus 2 syscalls (disk→pagecache→user buffer→socket buffer→NIC). `sendfile` leaves only the copy to the NIC. Data enters pagecache once and is reused for every consumer, so caught-up consumers cause **no disk reads**.
  - Failure mode: **disabled under TLS**. SSL runs in user space and Kafka does not support in-kernel `SSL_sendfile`, so `security.protocol` / `security.inter.broker.protocol` set to SSL lose zero-copy.
- **End-to-end batch compression**: compress a whole batch on the producer. The broker decompresses it only to validate, for example that the record count matches the batch header, then stores it compressed. Consumers receive it compressed and decompress it themselves. Codecs: GZIP, Snappy, LZ4, ZStandard.
- **Pull-based consumption with long poll**: consumers pull from partition leaders at their own pace, and a long poll blocks until data, or a minimum number of bytes, is available.
- **Offset as consumer position**: each partition is read by exactly one consumer per group, so consumption state is one integer per partition, checkpointed periodically.
- **Static membership (KIP-345)**: members supply a persistent `group.instance.id`, so restarts and deploys do not trigger rebalances.

## Key Concepts
- **Pagecache**: the OS's unified disk cache. It uses all free memory and serves all file I/O, and Kafka uses it as its main cache.
- **Message set / record batch**: a group of records that is the unit of network transfer, log append, compression and fetch.
- **Zero-copy**: sending pagecache bytes to the NIC via `sendfile` without copying them into user space.
- **Semantic partitioning**: the producer hashes a record key to a partition (for example a user id), so all records for that key land together and stay in order.
- **Leader-direct produce**: any broker can answer metadata requests (live brokers, partition leaders), so producers send straight to the leader with no routing tier in between.
- **Offset**: the position of the next record to consume in a partition. It is the only per-partition state, and Kafka's cheap stand-in for acks.
- **Rewind**: deliberately resetting an offset backwards to re-consume, for example after fixing a consumer bug. Classic queues forbid this.
- **Dynamic vs static membership**: dynamic member IDs are ephemeral and change on every rejoin. Static IDs (`group.instance.id`) persist across restarts.
- **Fencing (`FencedInstanceIdException`)**: if two live instances use the same static ID, the broker shuts the duplicate down.

## Mental Models
- Think of the broker as **a thin process over the OS pagecache**. RAM that is not in the JVM heap is Kafka's real cache.
- Use **sequential, batched I/O** as the test for any design change. Anything that adds per-message seeks or per-message copies works against the architecture.
- Think of **push vs pull** this way: with push, the broker sets the rate, so a slow consumer gets flooded (in effect a DoS). With pull, a slow consumer just falls behind and catches up later, and each pull can take everything available, which batches well without adding latency.
- Think of the consumer offset as **a bookmark, not a set of per-message acks**. Moving it is cheap, and that makes replay, reprocessing and periodic bulk loads possible.

## Anti-patterns
- **Large in-heap caches on brokers**: the data ends up stored twice, objects add size overhead, GC pauses get worse, and the cache is cold after every restart.
- **Compressing records one at a time in the application**: the ratio is poor because most redundancy is *between* records (JSON field names, user agents). Let Kafka compress the whole batch.
- **Producing with no batching (`linger` of zero, tiny batches) on a high-volume path**: you pay a network round trip and a small disk write per record.
- **Expecting zero-copy throughput with TLS on**: `sendfile` is bypassed, so plan for extra CPU and copies.
- **Broker-side per-message ack state (the classic MQ model)**: it causes lost messages if records are marked consumed when sent, duplicates if marked only after an ack, and per-message locking state. Kafka avoids all of this with offsets.
- **Store-and-forward producers (a local log on each producer)**: thousands of producer disks make the system harder to operate without making it more reliable.
- **Reusing the same `group.instance.id` across instances**: the broker fences one of them with `FencedInstanceIdException`.
- **Setting `group.instance.id` against brokers older than 2.3**: the client detects the old broker and throws `UnsupportedException`.

## Code Examples
```properties
# Producer: trade a little latency for throughput + better compression
batch.size=65536          # ~64 kB per-partition batch (source's example bound)
linger.ms=10              # wait up to 10 ms to fill a batch
compression.type=zstd     # gzip | snappy | lz4 | zstd — whole-batch compression
```
- **What it demonstrates**: the asynchronous-send accumulator, which waits for at most N bytes or T ms, combined with end-to-end batch compression. Larger batches compress better and make fewer, larger I/O operations on the broker.

```properties
# Consumer: long poll instead of busy-waiting
fetch.min.bytes=65536     # broker holds the fetch until this many bytes...
fetch.max.wait.ms=500     # ...or this much time passes
group.instance.id=orders-svc-pod-3   # static membership: unique per instance
```
- **What it demonstrates**: pull with long poll, where `fetch.min.bytes` ensures large transfers, plus static membership. For Kafka Streams, set one unique `group.instance.id` per KafkaStreams instance, however many threads it runs.

```java
// Keyed send → semantic partitioning: all events for user 42 hit one partition, in order
producer.send(new ProducerRecord<>("page-views", "user-42", eventJson));
```
- **What it demonstrates**: the key hashes to a partition, which gives consumers locality and ordering per key. Leave the key null to spread load. A custom partitioner can override the mapping.

## Reference Tables
| Config | Default† | Meaning | When to change |
|---|---|---|---|
| `batch.size` (producer) | 16384 | Max bytes per partition batch | Raise (64 kB–1 MB) for high-volume or WAN pipelines |
| `linger.ms` (producer) | 5 (AK 4.x) | Max wait to fill a batch | Raise for throughput and compression ratio; lower only for strict latency |
| `compression.type` (producer) | none | Batch codec: gzip/snappy/lz4/zstd | Enable when network-bound, especially cross-DC |
| `fetch.min.bytes` (consumer) | 1 | Minimum bytes before the long poll returns | Raise to force larger transfers |
| `fetch.max.wait.ms` (consumer) | 500 | Long-poll upper bound | Lower for latency, raise with `fetch.min.bytes` |
| `group.instance.id` (consumer) | null (dynamic) | Static member identity | Set on stateful apps and rolling deploys |
| `security.protocol` / `security.inter.broker.protocol` | PLAINTEXT | Listener security | SSL turns off `sendfile` zero-copy |

| Design choice | Traditional MQ | Kafka | Consequence |
|---|---|---|---|
| Storage structure | Per-consumer queue + BTree, O(log N) | Append-only log, O(1) | Speed independent of data size, long retention |
| Cache | In-process heap | OS pagecache | About 2× more memory, survives restarts, no GC cost |
| Consumed state | Per-message sent/acked flags on broker | One offset per partition | Cheap acks, rewind possible |
| Delivery | Push | Pull + long poll | Consumer sets its own rate, natural batching |
| Byte path | Read → user space → socket | `sendfile` from pagecache | Consume close to network line rate |

## Worked Example
**Cross-datacenter web-activity pipeline with a nightly warehouse load.**
1. **Produce**: page-view events are keyed by user id, so each user's events go to one partition. The producer fetches metadata from any broker and sends straight to each partition leader.
2. **Batch + compress**: the WAN link is the bottleneck. The producer uses `linger.ms=10`, `batch.size=65536` and `compression.type=zstd`. Repeated JSON field names across the 64 kB batch compress far better than single records would.
3. **Broker**: the broker decompresses the batch only to validate it (record count against the header), then appends it to the log *still compressed*. The write lands in pagecache with no immediate fsync.
4. **Real-time consumers**: several consumer groups read the topic and stay caught up. Each fetch is served with `sendfile` straight from pagecache, so disks show *no read activity*. The batch is still compressed on the wire and each consumer decompresses it. If the cluster later enables SSL, expect zero-copy to stop and broker CPU to rise.
5. **Offline load**: each night a batch job runs one task per topic-partition and reads from its saved offsets into the warehouse. Retention covers a week, so the backlog is still on disk. A failed task restarts from its starting offset with no duplicate output.
6. **Bug fix**: a consumer bug is found after a day's processing. Fix it, rewind the group's offsets to before the bad deploy and re-consume.
7. **Rolling deploy**: each stateful consumer pod has its own `group.instance.id` (brokers and clients are 2.3+). Restarts keep the same identity, so partitions are not reshuffled and local state is not rebuilt.

## Key Takeaways
1. Size brokers for **pagecache, not heap**. Free RAM is Kafka's read cache, and caught-up consumers should cause no disk reads.
2. **Batch everywhere**. Tune `linger.ms` and `batch.size` to trade a few ms of latency for much higher throughput and fewer I/O operations.
3. **Compress at the batch level** with `compression.type`, especially across a WAN. Never compress record by record in the application.
4. **TLS costs zero-copy**, so budget CPU and copies when you enable SSL.
5. **Use keys deliberately**. Keys decide the partition and therefore per-key ordering and locality for consumers.
6. **Offsets are cheap state**. Use rewind for reprocessing and long retention for periodic bulk consumers.
7. **Set a unique `group.instance.id`** on stateful or frequently deployed consumers and Streams instances to avoid rebalance storms (KIP-345).

## Connects To
- **ch03**: the producer and consumer APIs that expose batching, keys and offsets.
- **ch04**: topic and client configs (`compression.type`, retention) used here.
- **ch06**: how offsets plus commit timing produce at-most-once, at-least-once or exactly-once delivery.
- **ch07**: replication and log compaction build on the same append-only log.
- **ch09**: the record-batch format and on-disk log layout behind the message set and zero-copy.
- **ch13**: OS and pagecache tuning, filesystem and disk choices that this design relies on.
- **ch17**: the KIP-848 consumer rebalance protocol, which complements static membership.
- **ch18**: SSL listeners, which turn off `sendfile`.
- **ch27 / ch29**: Streams configuration and rebalancing, where static membership helps stateful apps most.
