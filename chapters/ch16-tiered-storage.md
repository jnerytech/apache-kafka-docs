# Chapter 16: Tiered Storage

## Core Idea
Tiered storage (KIP-405) splits a partition's log into a hot **local tier** on broker disks and a cold **remote tier** (S3, HDFS, ...) that holds completed segments. Retention then becomes two separate decisions: keep local data short for fast tail reads, and keep total data long for cheap backfill. The source material is short (overview, configs, quick start, disable paths, limitations), so this chapter is shorter than the target length.

## Frameworks Introduced
- **Two-tier log (local + remote)**: closed (rolled) log segments are copied to remote storage. After a segment is uploaded and passes the `local.*` retention limit, it is deleted locally. Remote copies are deleted when they pass `retention.ms` / `retention.bytes`.
  - When to use: most consumers read the tail, which the page cache serves, and old data is read only for backfill or recovery. You want long retention without paying for broker disk.
  - How: (1) enable the feature on the broker (`remote.log.storage.system.enable=true`). (2) Plug in a `RemoteStorageManager` (RSM) implementation. (3) Configure the `RemoteLogMetadataManager` (RLMM): the default is backed by an internal topic and needs `remote.log.metadata.manager.listener.name`. (4) On each topic, set `remote.storage.enable=true`. (5) Set `local.retention.*` (hot window) and `retention.*` (total window).
  - Why it works: tail reads come from the page cache, so a small local tier serves almost all traffic. The rare cold reads go to cheap object storage.
  - Failure mode: a local segment can be deleted **only after it has been uploaded**. If the RSM is broken or slow, local segments pile up and the disk can fill even with a small `local.retention.ms`.
- **Pluggable storage interfaces (RSM + RLMM)**:
  - `RemoteStorageManager`: handles the lifecycle of remote segments and their indexes (copy, fetch, delete). Kafka ships **no production implementation**, so you must supply `remote.log.storage.manager.class.name` and `remote.log.storage.manager.class.path`.
  - `RemoteLogMetadataManager`: handles the lifecycle of metadata about remote segments and must be **strongly consistent**. The default stores it in an internal Kafka topic. You can override it with `remote.log.metadata.manager.class.name` / `.class.path`.
  - How: pass implementation-specific settings through prefixes. The defaults are `remote.log.storage.manager.impl.prefix=rsm.config.` and `remote.log.metadata.manager.impl.prefix=rlmm.config.`, for example `rsm.config.dir=...` or `rlmm.config.remote.log.metadata.topic.replication.factor=...`.
- **Topic-level tiering lifecycle (KRaft)**: once a topic is tiered, it can move through these states:
  - **Enabled**: `remote.storage.enable=true`.
  - **Read-only remote**: `remote.storage.enable=true,remote.log.copy.disable=true`. Existing remote data stays readable and no new segments are uploaded. You must also align local retention: `local.retention.*` must equal `retention.*`, or be set to `-2`.
  - **Disabled + purged**: `remote.storage.enable=false,remote.log.delete.on.disable=true` deletes all remote logs.
  - **Re-enabled**: tiering can be switched back on for a topic.
  - **Cluster-wide disable**: first delete every tiered topic, then set `remote.log.storage.system.enable=false`.

## Key Concepts
- **Local tier**: the broker's own log dirs. It holds the active segment plus recent closed segments, up to `local.retention.ms` / `local.retention.bytes`.
- **Remote tier**: external storage (S3, HDFS) that holds completed segments with their indexes, producer snapshot and leader-epoch checkpoint.
- **`remote.log.storage.system.enable`**: the broker-level master switch. Default `false`.
- **`remote.storage.enable`**: the topic-level opt-in. Default `false`.
- **`local.retention.ms` / `local.retention.bytes`**: how long, or how much, data stays on local disk before local deletion. Unset or `-2` means inherit `retention.ms` / `retention.bytes`.
- **`retention.ms` / `retention.bytes`**: on a tiered topic, the **total** retention. Data past these limits is deleted from remote storage.
- **`remote.log.copy.disable`**: stops uploads and makes the existing remote log read-only.
- **`remote.log.delete.on.disable`**: when `remote.storage.enable` is set to false, also deletes all remote segments.
- **`LocalTieredStorage`**: a test-only RSM from the Kafka `storage` test jar. It simulates remote storage with a local directory and is for previews only.
- **Remote segment files**: each uploaded segment is stored as `<baseOffset>-<segmentUuid>.{log,index,timeindex,snapshot,leader_epoch_checkpoint}` under `<topic>-<partition>-<topicId>/`.

## Mental Models
- Think of `local.retention.*` as **cache size** and `retention.*` as **archive size**. The local window should cover consumer lag plus the replay window where you need fast reads. The total window comes from business or compliance needs.
- Think of an upload as a **gate** in front of local deletion: local retention can only remove segments that have already been uploaded.
- Use tiered storage when the workload is mostly tail reads with occasional historical backfill. Avoid it for compacted topics (unsupported) and for topics where every consumer constantly re-reads old data.
- Think of disabling tiering as **topic-first, broker-last**. The broker switch cannot be turned off while tiered topics still exist.

## Anti-patterns
- **Expecting a built-in S3 plugin**: Kafka ships no production RSM. If you configure `remote.log.storage.system.enable=true` without an RSM implementation, the setup is incomplete.
- **Using `LocalTieredStorage` in production**: it is an integration-test helper that writes to a local temporary directory.
- **Enabling tiering on compacted topics**: compaction is not supported.
- **Setting `remote.log.copy.disable=true` and leaving a small `local.retention.ms`**: once copying stops, local retention no longer applies as configured. This is confusing and can unexpectedly fill the disk. Set `local.retention.*` equal to `retention.*`, or to `-2`.
- **Setting `remote.log.storage.system.enable=false` while tiered topics exist**: the broker throws an exception during startup. Delete those topics first.
- **Default RLMM settings on a 1- or 2-broker cluster**: the metadata topic defaults to replication factor 3 and `min.isr` 2. Lower both with `rlmm.config.remote.log.metadata.topic.replication.factor` and `rlmm.config.remote.log.metadata.topic.min.isr`.
- **Tiering pre-2.8.0 topics**: segments that lack a producer snapshot file are not supported.
- **Running tiering admin operations from old clients**: tiered-storage admin actions require clients version 3.0 or later.

## Code Examples
```properties
# server.properties (KRaft broker, PLAINTEXT://:9092)
remote.log.storage.system.enable=true
remote.log.metadata.manager.listener.name=PLAINTEXT
remote.log.storage.manager.class.name=org.apache.kafka.server.log.remote.storage.LocalTieredStorage
remote.log.storage.manager.class.path=/PATH/TO/kafka-storage-4.3.1-test.jar
remote.log.storage.manager.impl.prefix=rsm.config.
remote.log.metadata.manager.impl.prefix=rlmm.config.
rsm.config.dir=/tmp/kafka-remote-storage
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.min.isr=1
log.retention.check.interval.ms=1000   # test only: faster retention checks
```
- **What it demonstrates**: the minimum broker wiring (master switch, RLMM listener and RSM class/path), and how prefixed pass-through settings reach the RSM (`rsm.config.*`) and the RLMM (`rlmm.config.*`).

```bash
# Make remote read-only (stop uploads); align local retention to avoid disk-full
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type topics --entity-name tieredTopic \
  --add-config 'remote.storage.enable=true,remote.log.copy.disable=true,local.retention.ms=-2,local.retention.bytes=-2'

# Fully disable tiering on the topic and delete all remote data
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type topics --entity-name tieredTopic \
  --add-config 'remote.storage.enable=false,remote.log.delete.on.disable=true'
```
- **What it demonstrates**: the two topic-level off-ramps, freezing the remote log versus purging it. Both are dynamic topic config changes and need no broker restart.

## Reference Tables

| Config | Scope | Default | Meaning | When to change |
|---|---|---|---|---|
| `remote.log.storage.system.enable` | broker | `false` | Enables the tiered-storage subsystem | Set `true` to use tiering. Set `false` only after all tiered topics are deleted |
| `remote.log.storage.manager.class.name` / `.class.path` | broker | none (mandatory) | RSM implementation and its jar | Always, to point at your S3/HDFS plugin |
| `remote.log.metadata.manager.class.name` / `.class.path` | broker | built-in topic-based RLMM | Remote metadata store | Only with a custom strongly-consistent store |
| `remote.log.metadata.manager.listener.name` | broker | none (mandatory with default RLMM) | Listener the RLMM's internal clients use to reach brokers | Always, with the default RLMM |
| `remote.log.storage.manager.impl.prefix` | broker | `rsm.config.` | Prefix for pass-through RSM settings | Rarely |
| `remote.log.metadata.manager.impl.prefix` | broker | `rlmm.config.` | Prefix for pass-through RLMM settings | Rarely |
| `rlmm.config.remote.log.metadata.topic.replication.factor` | broker | `3` | RF of the remote-metadata topic | Clusters with fewer than 3 brokers |
| `rlmm.config.remote.log.metadata.topic.min.isr` | broker | `2` | min ISR for the remote-metadata topic | Lower it when RF is lowered |
| `remote.storage.enable` | topic | `false` | Per-topic opt-in | Per topic, for non-compacted topics |
| `local.retention.ms` / `local.retention.bytes` | topic | unset / `-2` → inherit `retention.*` | Local (hot) window | Shrink to save broker disk |
| `retention.ms` / `retention.bytes` | topic | broker default | Total window (remote deletion) | Business or compliance retention |
| `remote.log.copy.disable` | topic | `false` | Stop uploads, keep remote readable | Freeze or retire tiering. Also set `local.retention.*=-2` |
| `remote.log.delete.on.disable` | topic | `false` | Delete remote data when disabling | Full topic-level opt-out |

| Limitation | Consequence |
|---|---|
| No compacted topics | Keep `cleanup.policy=compact` topics on the local tier only |
| Broker disable needs tiered topics deleted first | Otherwise the broker throws an exception at startup |
| Admin actions need clients 3.0 or later | Upgrade tooling before managing tiered topics |
| Segments without a producer snapshot (topics created before 2.8.0) | Not supported |

## Worked Example
**Preview tiering on a single local broker with `LocalTieredStorage`.**
1. Build the test RSM from the matching version tag (`git checkout 4.3.1`), then run `./gradlew clean :storage:testJar`. This produces `storage/build/libs/kafka-storage-4.3.1-test.jar`.
2. Add the broker properties from the Code Examples section. Use RF and min ISR of 1 for the metadata topic, because the cluster has one broker. Start the broker as in the quickstart.
3. Create a tiered topic with a tiny local window and small segments, so that segments roll quickly:
   ```bash
   bin/kafka-topics.sh --create --topic tieredTopic --bootstrap-server localhost:9092 \
     --config remote.storage.enable=true --config local.retention.ms=1000 \
     --config retention.ms=3600000 --config segment.bytes=1048576 --config file.delete.delay.ms=1000
   ```
   Local data is kept for 1 s **after upload**, and remote data for 1 h.
4. Produce about 1.2 MB so the 1 MiB segment rolls: `bin/kafka-producer-perf-test.sh --bootstrap-server localhost:9092 --topic tieredTopic --num-records 1200 --record-size 1024 --throughput -1`.
5. Check the upload: `ls /tmp/kafka-remote-storage/kafka-tiered-storage/tieredTopic-0-<topicId>/` should list `00000000000000000000-<uuid>.{log,index,timeindex,snapshot,leader_epoch_checkpoint}`. The local copy is deleted soon after.
6. Prove remote reads work. `bin/kafka-console-consumer.sh --topic tieredTopic --from-beginning --max-messages 1 --bootstrap-server localhost:9092 --formatter-property print.offset=true` should return offset 0, which is now served from the remote tier.
7. Tear down. Delete the topic with `kafka-topics.sh --delete --topic tieredTopic`, then set `remote.log.storage.system.enable=false`.

## Key Takeaways
1. Tiering needs both switches: the broker's `remote.log.storage.system.enable=true` and each topic's `remote.storage.enable=true`. Both default to off.
2. Budget for an RSM plugin. Kafka ships only the test `LocalTieredStorage`, so a real S3 or HDFS implementation must come from elsewhere.
3. On tiered topics, read `retention.*` as total retention and `local.retention.*` as the hot local window. Size the local window to cover consumer lag.
4. Local deletion waits for a successful upload. Monitor upload health, or disks can fill regardless of local retention.
5. On small clusters, lower the RLMM metadata topic's replication factor and `min.isr`. The defaults (3 and 2) assume at least 3 brokers.
6. To retire tiering, use `remote.log.copy.disable=true` (freeze) or `remote.log.delete.on.disable=true` (purge) per topic. Delete tiered topics before turning off the broker switch.
7. Do not tier compacted topics or topics created before 2.8.0 whose segments lack producer snapshots.

## Connects To
- **ch04**: dynamic topic configs (`kafka-configs.sh --alter`) are how you toggle `remote.storage.enable`, `remote.log.copy.disable` and local retention.
- **ch05**: page-cache-based tail reads are the premise for keeping the local tier small.
- **ch07**: log compaction is unsupported with tiering, and replication/min ISR settings apply to the RLMM metadata topic.
- **ch09**: segments, indexes, producer snapshots and leader-epoch checkpoints are the units that get uploaded.
- **ch10**: topic creation and deletion, and retention operations.
- **ch13**: tiering changes disk sizing for brokers.
- **ch14**: monitoring remote copy, fetch and delete health.
- **ch15**: the topic-level disable and read-only paths are KRaft-mode features.
- **ch30**: upgrade notes on tiered-storage availability and client version requirements.
