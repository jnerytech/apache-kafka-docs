# Chapter 13: Hardware, OS and Java

## Core Idea
Kafka's performance is bounded by disk throughput and the OS page cache, not by the JVM heap: give the broker a modest G1 heap, leave the rest of RAM to the page cache, rely on replication (not fsync) for durability, and raise the OS limits (file descriptors, `vm.max_map_count`) that scale with partition and segment count. The source material is short and operational; this chapter is correspondingly compact.

## Frameworks Introduced
- **Replication-over-fsync durability model**: Kafka writes every record to the filesystem immediately (into page cache) but, by default, never forces an application-level fsync; durability comes from replicas, and crash recovery re-validates any segment not known to be fsync'd.
  - When to use: always, unless you have a specific "belt and braces" requirement for local-disk sync on top of replication.
  - How: (1) keep the default flush settings (application fsync disabled); (2) let the OS flusher threads plus Kafka's background flush write dirty pages; (3) on unclean restart, Kafka checks each message's CRC and rebuilds the offset index for every unflushed segment; (4) a node whose disk is lost recovers from its replicas.
  - Why it works / failure mode: background flushing uses fine-grained page-level locking and lets the I/O scheduler batch and reorder writes; forcing fsync (by time or by message count) blocks writes to the file on most Linux filesystems and reduces the scheduler's freedom, adding latency and lowering throughput. The real risk is correlated failure (e.g. a datacenter power outage taking out all replicas before the OS flushes) — that is the only case where application fsync buys anything.
- **Memory sizing by buffered time**: size RAM so the page cache can hold the active working set of readers and writers.
  - When to use: capacity planning for a new broker.
  - How: estimate `write_throughput * 30` (30 seconds of buffering) as the memory that must stay available to the page cache; add a fixed JVM heap (~6 GB) on top; do not grow the heap to "use" the RAM — the page cache uses free memory automatically.
- **Map-area / file-descriptor budget per broker**: OS resources scale with segments, not just partitions.
  - When to use: before raising the partition count on a broker, or when a broker dies with `OutOfMemoryError (Map failed)` or "too many open files".
  - How: file descriptors needed ≥ `number_of_partitions * (partition_size / segment_size)` + open connections → start at ≥ 100000 FDs. Map areas: each log segment maps an index and a timeindex file = 2 map areas per segment, so ≥ 2 per partition (more as segments accumulate). Compare against `vm.max_map_count` (≈ 65535 by default on many distros). Note that `mmap()` holds an extra file reference that `close()` does not release, so mapped files keep consuming resources until unmapped.
  - Why it works / failure mode: 50,000 partitions on one broker = ≥ 100,000 map areas → exceeds the default `vm.max_map_count` and the broker crashes with `OutOfMemoryError (Map failed)` even though heap is fine.
- **JBOD vs RAID data-directory layout**: choose between multiple `log.dirs` (one mount per disk) or a single RAID volume.
  - When to use: provisioning disks for brokers.
  - How: with multiple data directories, partitions are assigned round-robin and each partition lives entirely in one directory — skewed partition sizes cause disk imbalance. RAID balances at block level (not always well) and tolerates disk loss, but costs write throughput and capacity, and a RAID rebuild is so I/O-heavy it effectively takes the broker out of service anyway. Since replication already provides redundancy, RAID's availability benefit is small.
- **Safe KRaft controller disk replacement**: never format and start a replacement controller until a majority of controllers already hold all committed metadata.
  - When to use: a controller's `metadata.log.dir` (or first log dir, if unset) is lost or its hardware is replaced.
  - How: see Worked Example.

## Key Concepts
- **Page cache**: OS memory holding file data before writeback; Kafka relies on it instead of an in-process cache, and it automatically uses all free RAM.
- **Flusher threads (pdflush)**: Linux background threads that write dirty pages to disk; if they cannot keep up, the writing process blocks, adding write latency.
- **Application-level flush policy**: optional broker settings that force fsync after a time interval or a number of messages; disabled by default and generally not recommended.
- **`vm.max_map_count`**: Linux limit on memory-map areas per process (~65535 default on many systems); each Kafka log segment consumes 2 (index + timeindex).
- **File descriptor limit**: per-process cap covering log segments and network connections; recommend ≥ 100000 for brokers.
- **`noatime`**: mount option that stops access-time updates on reads; Kafka never uses atime, so it removes pure overhead (notably during consumer bootstrapping).
- **Request Local Time**: broker metric for how long appends take locally; the yardstick used to compare filesystems (XFS ~160 ms vs 250 ms+ for best-tuned EXT4 under heavy load).
- **`metadata.log.dir`**: where a KRaft controller stores the cluster metadata log; defaults to the first log directory if not set.
- **`--ignore-formatted`**: `kafka-storage.sh format` flag to skip already-formatted dirs; valid only in combined mode when just the metadata dir was lost.

## Mental Models
- Think of the JVM heap as the control plane and the page cache as the data plane: a small, stable heap (6 GB) keeps GC pauses ~20 ms, while everything else in RAM serves reads/writes from cache.
- Think of disk throughput as the default bottleneck: more spindles/disks beat faster disks unless you force frequent flushes (then high-RPM SAS helps).
- Use replication, not local fsync, as your durability guarantee; treat fsync policies as a latency tax paid only for correlated-failure paranoia.
- Think of every partition as a recurring OS-resource bill (FDs + 2 map areas per segment) that grows with retention and shrinking segment size.

## Anti-patterns
- **Sharing Kafka data disks with OS or application logs**: unrelated I/O destroys sequential-write latency; dedicate drives to `log.dirs`.
- **Enabling aggressive application flush (fsync) "for safety"**: blocks writers during fsync, reduces I/O reordering, and adds little over replication.
- **Big JVM heaps**: steals memory from the page cache and lengthens GC pauses; the reference cluster ran a 6 GB heap.
- **Running high-partition brokers on default OS limits**: default `vm.max_map_count` and FD limits crash brokers at tens of thousands of partitions.
- **Relying on RAID rebuild for availability**: rebuild I/O effectively disables the server; replication already covers disk loss.
- **Production on Windows**: not a well-supported platform; use Linux (Solaris also tested).
- **Formatting a replacement KRaft controller immediately**: if a majority has not caught up, you risk losing committed metadata.
- **EXT4 unsafe mount tuning on clusters exposed to correlated failures**: `data=writeback`/no journal can cause unrecoverable corruption after a power outage.
- **Staying on an old Java patch release**: older patches carry disclosed CVEs.

## Code Examples
```bash
# Broker JVM flags (OpenJDK/Oracle JDK), as used on a LinkedIn peak cluster
-Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC
-XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
-XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent
```
- **What it demonstrates**: fixed-size heap (Xms = Xmx), G1 with a 20 ms pause target and early concurrent marking (IHOP 35%). Result at 60 brokers, 50k partitions (RF 2), 800k msg/s, 300 MB/s in, 1 GB/s+ out: p90 GC pause ≈ 21 ms, < 1 young GC/s.

```bash
# Check OS memory / dirty page state
$ cat /proc/meminfo
# Example /etc/fstab entry for a dedicated Kafka data disk (XFS, defaults + noatime)
/dev/sdb1  /var/kafka-data-1  xfs  defaults,noatime  0 0
```
- **What it demonstrates**: inspecting page-cache usage and mounting data dirs with `noatime` on XFS, which needs no other tuning.

```bash
$ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
$ bin/kafka-storage.sh format --cluster-id <uuid> --config config/server.properties
$ bin/kafka-server-start.sh config/server.properties
```
- **What it demonstrates**: the verify → format → start sequence for replacing a KRaft controller's metadata disk.

## Reference Tables

**Java support (AK 4.3)**

| Java | Status |
|---|---|
| 25 | Fully supported; recommended (latest LTS) |
| 17, 21 | Fully supported |
| 11 | Clients, Streams and related modules only |
| Newer non-LTS | Best effort; only latest non-LTS typically tested |

**OS-level limits**

| Setting | Default / guidance | Why | When to raise |
|---|---|---|---|
| Open file descriptors (`ulimit -n`) | ≥ 100000 starting point | segments + connections | partitions × segments-per-partition approaches limit |
| `vm.max_map_count` | ~65535 on many distros | 2 map areas per segment | > ~30k segments per broker; `Map failed` OOM |
| Max socket buffer size | OS default | TCP window for high-BDP links | cross-datacenter replication/mirroring |

**Filesystem choice and mount options**

| Option | FS | Effect | Safety |
|---|---|---|---|
| `noatime` | any | removes atime writes on reads | safe — recommended |
| defaults | XFS | auto-tuned; best measured latency, lower variance | safe — preferred FS |
| `largeio` | XFS | larger preferred I/O size | safe; minimal/no measured gain |
| `nobarrier` | XFS | skips write flushes | only with battery-backed cache |
| `data=writeback` | EXT4 | drops write ordering (default `data=ordered`); big latency win | unsafe on multi-failure |
| no journal | EXT4 | removes locking/latency spikes; slower reboot | unsafe on multi-failure |
| `commit=num_secs` | EXT4 | lower = less loss on crash; higher = throughput | trade-off |
| `nobh` | EXT4 | removes extra ordering with writeback | safe for Kafka |
| `delalloc` | EXT4 | large sequential extents, throughput | some latency variance |
| `fast_commit` | EXT4 (Linux ≥ 5.10) | lighter journaling with `data=ordered`; lower latency | safe |

**Disk layout**

| Layout | Pros | Cons |
|---|---|---|
| Multiple `log.dirs` (JBOD) | full write throughput and capacity | round-robin placement → imbalance with skewed partitions |
| RAID volume | block-level balancing, survives disk loss | write-throughput hit, less capacity, rebuild cripples broker |

## Worked Example
**Replacing a failed KRaft controller metadata disk.**
1. Controller 3's disk holding `metadata.log.dir` dies; the node is reprovisioned with an empty disk. Do not format or start it yet.
2. From any node, run `bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication`. Output lists `NodeId`, `DirectoryId`, `LogEndOffset`, `Lag`, `LastFetchTimestamp`, `LastCaughtUpTimestamp`, `Status` (Leader/Follower/Observer).
3. Wait until a majority of controllers show small `Lag`. If the leader's `LogEndOffset` is static, wait for `Lag` = 0 on a majority; if it keeps moving, note the current leader end offset and wait until replicas reach it. Also confirm `LastFetchTimestamp` ≈ `LastCaughtUpTimestamp` for the majority.
4. Format the new metadata dir: `bin/kafka-storage.sh format --cluster-id <uuid> --config config/server.properties`.
5. If it fails with `Log directory ... is already formatted`, and only if the node runs in combined mode and only the metadata dir was lost, rerun with `--ignore-formatted`.
6. Start the controller: `bin/kafka-server-start.sh config/server.properties`; it fetches the metadata log from the quorum leader.

## Key Takeaways
1. Run brokers on the latest Java LTS (25) at the latest patch; use a fixed ~6 GB G1 heap with `MaxGCPauseMillis=20`.
2. Budget RAM as `write_throughput * 30` for page cache; do not enlarge the heap to consume it.
3. Use many dedicated data disks; disk throughput is the usual bottleneck, and they must not share I/O with OS/app logs.
4. Keep default flush settings (no application fsync); durability comes from replication.
5. Before scaling partitions, raise FD limits (≥ 100000) and `vm.max_map_count` (2 map areas per segment).
6. Prefer XFS with `noatime`; tune EXT4 only if you accept corruption risk on correlated failures.
7. When replacing a KRaft controller disk, verify majority catch-up with `kafka-metadata-quorum.sh describe --replication` before `kafka-storage.sh format`.

## Connects To
- **ch05**: persistence/page-cache design ("don't fear the filesystem") that motivates the small-heap, big-cache layout.
- **ch07**: replication is what makes skipping fsync safe.
- **ch09**: log segments and index/timeindex files are what consume FDs and map areas.
- **ch04**: broker configs (`log.dirs`, flush settings, `metadata.log.dir`).
- **ch10**: basic operations, including partition reassignment to fix JBOD imbalance.
- **ch11**: socket buffer tuning for cross-datacenter replication.
- **ch14**: Request Local Time and GC metrics to validate hardware/OS choices.
- **ch15**: KRaft quorum, `kafka-metadata-quorum.sh` and `kafka-storage.sh`.
