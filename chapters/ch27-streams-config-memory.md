# Chapter 27: Kafka Streams — Configuration, Memory Management and Running Apps

## Core Idea
A Streams app is production-ready only after you override the defaults that are tuned for development (no standbys, `at_least_once`, state in `/tmp`, unbounded RocksDB off-heap memory, auto-named internal topics) and size memory across three layers — the record cache, RocksDB, and the embedded clients — while relying on warmup/standby replicas and clean shutdown to keep task migration and restarts cheap.

## Frameworks Introduced
- **Resiliency baseline (4 knobs)**: `acks=all` (producer; default since 3.0), internal-topic `replication.factor=3` (default `-1` = broker default, so set broker `default.replication.factor=3` on 2.4+), `min.insync.replicas=2` (via `topic.` prefix), `num.standby.replicas=1`.
  - When to use: every production app; EOS without RF 3 + min ISR 2 is effectively void.
  - How: set them in the same `Properties` passed to `KafkaStreams`; provision n+1 instances for n standbys; budget 3x broker storage (RF 3) and 2x client disk (1 standby).
  - Why it works / failure mode: RF 3 tolerates 2 broker failures; standby gives near-instant failover instead of a changelog replay. Skipping standbys means every failover blocks the task on full restoration.
- **High-availability task assignment (KIP-441, warmup + probing rebalances)**: stateful active tasks go only to instances within `acceptable.recovery.lag` (default 10000 offsets); others get *warmup replicas* that restore in the background; a *probing rebalance* every `probing.rebalance.interval.ms` (default 10 min, min 1 min) promotes caught-up warmups.
  - When to use: default assignor; tune when scaling out with large state.
  - How: (1) size `acceptable.recovery.lag` to well under a minute of restore; (2) raise `max.warmup.replicas` (default 2, min 1) to migrate more tasks concurrently; (3) max migration rate ≈ `max.warmup.replicas / probing.rebalance.interval.ms`; (4) set `acceptable.recovery.lag=Long.MAX_VALUE` to disable warmups and move tasks immediately (accepting downtime).
  - Why it works / failure mode: a task keeps running on the caught-up instance while its new home warms up, so migration is usually downtime-free. Exception: if no instance is caught up, the active task is placed anyway and blocks on restoration — the reason to enable standbys.
- **Record cache = commit interval OR cache pressure**: DSL caches for source/aggregation `KTable`s dedupe (compact) updates per key in memory; flush to store + downstream happens at the earliest of `commit.interval.ms` or the cache filling `statestore.cache.max.bytes` (default 10 MB, split evenly `C/T` across `T` threads, LRU eviction).
  - How: `0` bytes disables caching (every update emitted); bigger cache → commit interval dominates; smaller cache → eviction pressure dominates. Both are global, not per-node.
- **Bounded RocksDB memory**: share one `LRUCache` and one `WriteBufferManager` (memtables counted against the cache) across all stores, cache index/filter blocks in the block cache — caps total off-heap memory (since 2.3).
- **Three-tier exception handling**: `deserialization.exception.handler` (FAIL/CONTINUE), `processing.exception.handler` (FAIL/CONTINUE, built-in DLQ via `Response.resume(List.of(dlqRecord))`), `production.exception.handler` (FAIL/CONTINUE/RETRY; RETRY only valid for `RetriableException`, else treated as FAIL).

## Key Concepts
- **`application.id`**: required, unique per cluster, shared by all instances; becomes `group.id`, `client.id` prefix, `state.dir` subdirectory, and internal topic prefix — change it (e.g. `my-app-v1.0.2`) to start fresh state.
- **Standby replica**: shadow copy of a task's state store kept up to date on another instance for fast failover; `num.standby.replicas` default `0`.
- **Warmup replica**: temporary extra standby (beyond `num.standby.replicas`) assigned to restore state before a task migrates; one warmup = one task.
- **`max.task.idle.ms`**: how long a multi-input task (join/merge) waits for an empty-but-caught-up partition; `0` = wait only for data already on the broker, `>0` = extra wait for slow producers, `-1` = never wait (max throughput, out-of-order risk).
- **Timestamp extractor**: `FailOnInvalidTimestamp` (default, throws on negative), `LogAndSkipOnInvalidTimestamp` (drops), `UsePartitionTimeOnInvalidTimestamp` (estimates), `WallclockTimestampExtractor` (processing-time).
- **Changelog offset in RocksDB (KIP-1035, 4.3)**: offsets live inside RocksDB, not a `.checkpoint` file; WAL is off, so offsets are durable only on memtable flush (`write_buffer_size`, 16 MB) or clean close — no force-flush per commit anymore.
- **Client config prefixes**: `consumer.`, `producer.`, `admin.`, plus `main.consumer.`, `restore.consumer.`, `global.consumer.`, and `topic.` for internal topics.
- **`PENDING_ERROR` state**: entered on crash to release resources before `ERROR`; not recoverable — only a restart returns to `RUNNING`.
- **Internal topics**: changelog/repartition topics auto-created by the app (needs admin ACLs when security is on); named `<application.id>-<operatorName>-<suffix>` (not guaranteed).

## Mental Models
- Think of the record cache as **client-side log compaction before the store**: it trades latency (updates held until commit/eviction) for fewer writes and fewer downstream records (15 → 8 in the docs' example). In the Processor API it compacts only store writes, never forwarded records.
- Use `topology.optimization` as **two steps, not one**: setting the config without `streamsBuilder.build(props)` silently does nothing.
- Think of partition count as **the hard ceiling on parallelism**: 10 input partitions → at most 10 useful instances (extra ones idle). Balance both data volume and processing cost across partitions.
- Think of a low-traffic RocksDB store as **a store whose offset is only as fresh as its last flush**: unclean exits then cause full re-restores, so clean shutdown is the real defence.

## Anti-patterns
- **Leaving `state.dir` at `${java.io.tmpdir}/kafka-streams`**: tmp is often wiped → full restore on every restart; also must differ per instance on the same host.
- **Running EOS with RF < 3**: voids exactly-once; apply RF 3 + `min.insync.replicas=2` to `__transaction_state`, `__consumer_offsets`, internal and user topics.
- **Relying on broker auto-create for user topics**: may be disabled and applies wrong defaults (RF etc.); create input/output topics ahead of time.
- **Not closing store iterators** (`KeyValueIterator`, `WindowStoreIterator`): leaks file handles/buffers until OOM — use try-with-resources.
- **Closing a shared `Cache`/`WriteBufferManager` in `RocksDBConfigSetter#close`**: they are shared by all stores; only close per-store objects you created (otherwise you leak RocksObjects).
- **Creating a new `BlockBasedTableConfig`** instead of reading `options.tableFormatConfig()`: overwrites defaults like the Bloom filter.
- **Manual `flush()` before iterating a store**: degrades RocksDB performance; avoid.
- **`SIGKILL` before `KafkaStreams#close` completes**: on Kubernetes keep the close timeout under the pod grace period (default 30s).
- **Monitoring only `ERROR`**: `PENDING_ERROR` is already terminal.
- **Using `topology.optimization=all` in production**: list specific optimizations so library upgrades don't change topology shape.

## Code Examples
```java
Properties p = new Properties();
p.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-agg-v1");
p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
p.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
      "reuse.ktable.source.topics,merge.repartition.topics");
p.put(StreamsConfig.restoreConsumerPrefix("max.poll.records"), 5000);
Topology t = builder.build(p);           // required for optimizations
KafkaStreams streams = new KafkaStreams(t, p);
```
- **What it demonstrates**: resiliency baseline, EOS, explicit optimization list applied via `build(props)`, and per-consumer-type prefixes.

```java
public static class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {
  private static Cache cache = new LRUCache(TOTAL_OFF_HEAP_MEMORY, -1, false, INDEX_FILTER_BLOCK_RATIO);
  private static WriteBufferManager wbm = new WriteBufferManager(TOTAL_MEMTABLE_MEMORY, cache);
  public void setConfig(String storeName, Options options, Map<String,Object> configs) {
    BlockBasedTableConfig tc = (BlockBasedTableConfig) options.tableFormatConfig();
    tc.setBlockCache(cache);
    tc.setCacheIndexAndFilterBlocks(true);
    options.setWriteBufferManager(wbm);
    tc.setCacheIndexAndFilterBlocksWithHighPriority(true); // makes the ratio effective
    tc.setPinTopLevelIndexAndFilter(true);
    options.setTableFormatConfig(tc);
  }
  public void close(String storeName, Options options) { /* shared: do not close */ }
}
// props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class);
```
- **What it demonstrates**: capping all RocksDB off-heap memory to one shared budget; static shared objects; don't close them per store.

```bash
apt install -y libjemalloc-dev
export LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
java -cp path-to-app-fatjar.jar com.example.MyStreamsApp
```
- **What it demonstrates**: swap RocksDB's default allocator for jemalloc (reduces memory bloat) and launch as a plain fat JAR.

## Reference Tables

| Config | Default | Meaning | When to change |
|---|---|---|---|
| `num.standby.replicas` | `0` | Shadow state copies per task | Set `1` for HA (High importance) |
| `state.dir` | `${java.io.tmpdir}/kafka-streams` | Local state location | Always, to persistent disk (High) |
| `ensure.explicit.internal.resource.naming` | `false` | Refuse start on auto-named internal topics/stores | Enable to keep topology upgrade-safe (High) |
| `processing.guarantee` | `at_least_once` | Or `exactly_once_v2` (brokers 2.5+) | EOS needs; changes `commit.interval.ms` to 100 |
| `commit.interval.ms` | `30000` (ALOS) / `100` (EOS) | Offset commit + cache flush cadence | Lower for fresher output |
| `statestore.cache.max.bytes` | `10485760` | Record cache across all threads (replaces `cache.max.bytes.buffering`) | `0` to see every update; raise to reduce writes |
| `num.stream.threads` | `1` | Processing threads per instance | Scale vertically |
| `acceptable.recovery.lag` | `10000` | Offsets "caught up" threshold | ~ <1 min of restore |
| `max.warmup.replicas` | `2` | Concurrent warmups | Raise to scale out faster |
| `probing.rebalance.interval.ms` | `600000` | Warmup promotion probe | Lower (≥1 min) for faster migrations |
| `max.task.idle.ms` | `0` | Join/merge wait for empty partitions | `>0` for slow producers; `-1` for throughput |
| `replication.factor` | `-1` (broker default) | Internal topic RF | `3` |
| `topology.optimization` | `none` | `reuse.ktable.source.topics`, `merge.repartition.topics`, `single.store.self.join` | Enable explicit list |
| `group.protocol` | `classic` | `streams` = new Streams group protocol (broker-side enablement) | Opt in on new brokers |
| `rack.aware.assignment.strategy` | `none` | `min_traffic` / `balance_subtopology` (needs `broker.rack` + `client.rack`) | Cut cross-AZ traffic |
| `rack.aware.assignment.tags` | empty | Spread standbys over `client.tag.<k>` values | Zone-aware standbys |
| `task.assignor.class` | HA assignor | `StickyTaskAssignor` = pre-KIP-441, less movement, less availability | Rarely |
| `dsl.store.format` | `default` | `headers` = headers-aware stores (KIP-1271) | Need headers in state |
| `upgrade.from` | `null` | Two-bounce rolling upgrade | Upgrading to 3.4+ from <3.4 |

| Client config Streams overrides | Value | User-settable? |
|---|---|---|
| `auto.offset.reset` / `linger.ms` / `max.poll.records` | `earliest` / `100` / `1000` | Yes (different defaults) |
| `transaction.timeout.ms` / `delivery.timeout.ms` (EOS) | `10000` / `Integer.MAX_VALUE` | Yes |
| `group.id`, `enable.auto.commit`, `allow.auto.create.topics`, `partition.assignment.strategy` | `application.id`, `false`, `false`, `StreamsPartitionAssignor` | No (ignored) |
| `isolation.level` / `enable.idempotence` (EOS) | `READ_COMMITTED` / `true` | No |

| Internal topic | Cleanup policy / retention |
|---|---|
| Repartition | `delete`, retention `-1` (purged every `repartition.purge.interval.ms`, 30s) |
| KV changelog | `compact` |
| Windowed changelog | `delete,compact`, retention = window retention + 24h (`windowstore.changelog.additional.retention.ms`) |
| Versioned changelog | `compact`, `min.compaction.lag.ms` = historyRetention + 24h |

Other memory to budget: producer `buffer.memory`; consumer via `fetch.max.bytes`/`fetch.max.wait.ms`; TCP `send.buffer.bytes`/`receive.buffer.bytes`; deserialized buffers via `buffered.records.per.partition` (1000).

## Worked Example
**Scaling a stateful app from 2 to 3 instances without downtime.** Input topic has 6 partitions → 6 stateful tasks; the app runs 2 instances (the minimum for `num.standby.replicas=1`, since n standbys need n+1 instances), 3 active tasks each. (1) Launch a third instance with the same `application.id`; it joins the group and the HA assignor computes a balanced target. (2) The new instance's stores lag far beyond `acceptable.recovery.lag` (10000), so instead of moving active tasks it receives up to `max.warmup.replicas=2` warmup tasks that restore from changelogs in the background while actives keep running on the old instances. (3) Every `probing.rebalance.interval.ms` (10 min) a probing rebalance checks warmup lag; caught-up warmups become actives, the old copies may become standbys. (4) Probing repeats until balanced — 2 tasks per 10 min here, so moving 2 tasks takes one probe; to move more, raise `max.warmup.replicas` or shorten the probe interval. (5) On scale-in, stopped instances leave the group; their tasks fail over preferentially to instances holding standbys (near-instant), else replay changelog from the checkpointed offset (local state exists) or from earliest (no local state). Ensure each instance exits via `KafkaStreams#close` within the pod grace period so low-traffic RocksDB stores persist a fresh changelog offset (KIP-1035) and avoid an `OffsetOutOfRangeException`/`TaskCorruptedException` full re-restore.

## Key Takeaways
1. Override the four resiliency knobs plus `state.dir` before production; provision n+1 instances for n standbys.
2. Name every internal topic/store and set `ensure.explicit.internal.resource.naming=true`; list topology optimizations explicitly and pass props to `build()`.
3. Tune output freshness vs. write volume with the pair `commit.interval.ms` + `statestore.cache.max.bytes`; both are global.
4. Bound RocksDB off-heap with a shared `LRUCache` + `WriteBufferManager`, and use jemalloc via `LD_PRELOAD`.
5. Since 4.3, clean shutdown matters: low-traffic stores only persist offsets on flush/close; shrink `write_buffer_size` per store if needed.
6. Choose exception handlers deliberately (DLQ via `Response.resume`) and alert on `PENDING_ERROR`, not just `ERROR`.
7. Partition count caps parallelism; spread both data and processing cost evenly.

## Connects To
- **ch04**: topic configs (`min.insync.replicas`, cleanup policies) applied via the `topic.` prefix.
- **ch06**: EOS/transactions underlying `exactly_once_v2`.
- **ch07**: log compaction semantics mirrored by the record cache and changelogs.
- **ch20**: ACLs needed for internal topic creation.
- **ch24**: tasks, threads, standby replicas and the parallelism model.
- **ch25**: DSL operators, `Materialized`, naming of stores/repartition topics.
- **ch26**: Processor API stores, caching on `StoreBuilder`, serdes.
- **ch28**: `application.server` for interactive queries.
- **ch29**: application reset tool, Streams rebalance protocol (`group.protocol=streams`), security.
- **ch30**: `upgrade.from` two-bounce upgrades.
