# Chapter 14: Monitoring

## Core Idea
Kafka exposes everything over JMX (Yammer Metrics on the server, Kafka Metrics in the Java clients), and a small set of "should be 0 / should be even / should be > 0.3" broker signals — under-replicated and under-min-ISR partitions, offline partitions, active controller count, handler/network idle ratios — plus consumer lag and poll gaps tell you almost everything about cluster health. (The source's producer-sender, consumer-fetch and Connect metric tables are generated includes and are absent from this material; only their section headers survive.)

## Frameworks Introduced
- **"Normal value" alerting baseline**: the broker metric table pairs each MBean with its expected steady-state value; alert on deviation, not on raw magnitude.
  - When to use: building the first dashboard/alert set for any cluster.
  - How: (1) Alert when these are non-zero: `UnderReplicatedPartitions`, `UnderMinIsrPartitionCount`, `OfflineReplicaCount`, `OfflineLogDirectoryCount`, `UncleanLeaderElectionsPerSec`, `ElectionFromEligibleLeaderReplicasPerSec`, `FailedIsrUpdatesPerSec`, the `Invalid*Records`/`NoKeyCompactedTopicRecordsPerSec` validation rates. (2) Alert when `ActiveControllerCount` summed across the cluster ≠ 1. (3) Alert when `RequestHandlerAvgIdlePercent` or `NetworkProcessorAvgIdlePercent` drops below ~0.3. (4) Watch for skew: `PartitionCount` and `LeaderCount` should be "mostly even across brokers". (5) Treat `IsrShrinksPerSec`/`IsrExpandsPerSec` as 0 except around a broker restart (shrink on down, expand once caught up).
  - Why it works / failure mode: these are invariants of a healthy replicated log, so any deviation is a real event. Failure mode: alerting on `LeaderElectionRateAndTimeMs` alone — it is legitimately non-zero during broker failures/rolls; pair it with URP/offline counts.
- **Request time decomposition**: `TotalTimeMs` per request type (`Produce`, `FetchConsumer`, `FetchFollower`) = `RequestQueueTimeMs` + `LocalTimeMs` + `RemoteTimeMs` + `ResponseQueueTimeMs` + `ResponseSendTimeMs`.
  - When to use: diagnosing slow produce/fetch latency.
  - How: find the dominant component. High request-queue time → I/O threads saturated (check `RequestHandlerAvgIdlePercent`, `RequestQueueSize`). High local time → disk/leader append cost (check `LogFlushRateAndTimeMs`, message conversion `MessageConversionsTimeMs`). High remote time on Produce → waiting for followers with `acks=-1` (check follower lag, ISR). High response-queue/send → network threads or slow clients.
  - Why it works / failure mode: it localizes latency to a specific thread pool or dependency instead of guessing. Remote time being non-zero for `acks=-1` produce is normal, not a fault.
- **Consumer keep-up rule**: a consumer keeps up when max lag (`records-lag-max`, published by the consumer, not the broker) stays below a threshold and min fetch rate stays > 0.
  - How: alert on `records-lag-max` trend, `last-poll-seconds-ago` growing, `time-between-poll-max` approaching `max.poll.interval.ms`, and `rebalance-rate-per-hour`/`failed-rebalance-total` rising.
- **Streams 4-layer metric hierarchy + recording levels**: client → stream thread → task → processor node / state store / record cache; each metric has a recording level `info`, `debug` or `trace`, selected with `metrics.recording.level`.
  - How: run production at `info` (client/thread metrics, task `dropped-records-*` and `active-process-ratio`, node `record-e2e-latency-*`, store `num-open-iterators`/`num-keys`); switch to `debug` to get task, node, store latency/rate and RocksDB statistics; `trace` adds store-level `record-e2e-latency-*`.

## Key Concepts
- **Yammer vs Kafka Metrics**: broker uses Yammer Metrics; Java clients use the built-in Kafka Metrics registry to avoid pulling transitive dependencies — both export via JMX and pluggable reporters.
- **`-total` companion**: every rate metric has a cumulative counterpart, e.g. `records-consumed-rate` ↔ `records-consumed-total`.
- **Topic-scoped MBeans**: most `BrokerTopicMetrics` take `topic=(...)`; omitting it yields the all-topic aggregate.
- **Purgatory size**: `PurgatorySize,delayedOperation=Produce` is non-zero when `acks=-1`; the Fetch purgatory size depends on consumers' `fetch.wait.max.ms` (same for `ShareFetch`).
- **Follower lag**: `ReplicaFetcherManager MaxLag` and per-partition `FetcherLagMetrics ConsumerLag` should be proportional to the max produce batch size, not growing.
- **`RetentionSizeInPercent`**: partition size as % of `retention.bytes`; returns 0 when unlimited or for tiered topics (then reported by RemoteLogManager); may exceed 100% if cleanup lags.
- **Remote copy/delete lag**: `RemoteCopyLagBytes/Segments` (eligible for tiering but not yet uploaded) and `RemoteDeleteLagBytes/Segments` (eligible for deletion but not yet deleted) — the tiered-storage backlog signals.
- **KRaft metadata lag**: `LastAppliedRecordLagMs` (controller and `broker-metadata-metrics`) = now − timestamp of last applied metadata record; always 0 on the active controller.
- **Raft `current-state`**: one of leader, candidate, voted, follower, unattached, observer (`kafka.server:type=raft-metrics`), reported on both controllers and brokers.
- **Share partition in-flight**: `InFlightMessageCount`/`InFlightBatchCount` per share partition plus `AcquisitionLockTimeoutPerSec` (records not acknowledged within the lock timeout).

## Mental Models
- Think of the broker as a pipeline of thread pools (network processors → request queue → I/O handlers → purgatory → response queue); idle ratios and queue sizes tell you which stage is the bottleneck.
- Use replication health (URP, under-min-ISR, at-min-ISR) as your durability SLO: `AtMinIsrPartitionCount` > 0 means one more failure blocks `acks=all` writes.
- Think of KRaft controllers as a single replicated state machine: exactly one `ActiveControllerCount=1`, metadata applied lag near 0, `MetadataErrorCount`/`MetadataLoadErrorCount`/`MetadataApplyErrorCount` at 0.
- Use client metrics for "is my app healthy", broker metrics for "is the cluster healthy"; lag lives on the consumer, quotas/throttling on the broker per (user, client-id).

## Anti-patterns
- **Enabling remote JMX without security**: Kafka disables remote JMX by default and JMX authentication is off; exposing `JMX_PORT` without hardening `KAFKA_JMX_OPTS` lets anyone monitor *and control* the broker and host.
- **Reading lag from the broker**: `records-lag-max` is published by the consumer; a broker-only dashboard misses it.
- **Leaving Streams at `debug` in production without reason**: RocksDB statistics-based metrics are `debug` because collecting them costs performance.
- **Ignoring skew**: uneven `PartitionCount`/`LeaderCount` or `PreferredReplicaImbalanceCount` > 0 means hotspots even when nothing is "failing".
- **Relying on removed RocksDB counters**: `number-open-files` now always returns -1 (RocksDB 9.7.3 removed `NO_FILE_CLOSES`).
- **Using deprecated store flush metrics**: `flush-latency-*`/`flush-rate` are deprecated — use `commit-latency-*`/`commit-rate`.

## Code Examples
```bash
# Enable remote JMX for a CLI-started broker, with security overrides
export JMX_PORT=9999
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=true \
  -Dcom.sun.management.jmxremote.ssl=true \
  -Dcom.sun.management.jmxremote.password.file=/etc/kafka/jmx.password \
  -Dcom.sun.management.jmxremote.access.file=/etc/kafka/jmx.access"
bin/kafka-server-start.sh config/server.properties
# Browse all MBeans interactively
jconsole localhost:9999
```
- **What it demonstrates**: remote JMX is opt-in via `JMX_PORT`; auth is off by default, so production deployments must override it through `KAFKA_JMX_OPTS` (standard Java `jmxremote` properties). jconsole is the quickest way to discover available metrics.

```properties
# Kafka Streams: choose metric granularity (info | debug | trace)
metrics.recording.level=info
```
- **What it demonstrates**: one switch controls which layers of the Streams hierarchy are recorded; the client exposes the active level as `recording-level` (0=INFO, 1=DEBUG, 2=TRACE).

```text
kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions          -> 0
kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount          -> 0
kafka.controller:type=KafkaController,name=ActiveControllerCount        -> sum over cluster = 1
kafka.controller:type=KafkaController,name=OfflinePartitionsCount       -> 0
kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent -> > 0.3
kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent     -> > 0.3
```
- **What it demonstrates**: the minimal broker alert set expressed as MBean → expected value.

## Reference Tables

**Core broker health MBeans**

| MBean (`name=`) | Normal value | Act when |
|---|---|---|
| `ReplicaManager UnderReplicatedPartitions` | 0 | > 0 outside a broker restart: dead/slow follower |
| `ReplicaManager UnderMinIsrPartitionCount` | 0 | > 0: `acks=all` producers get `NotEnoughReplicas` |
| `ReplicaManager AtMinIsrPartitionCount` | 0 | > 0: one more failure blocks writes |
| `ReplicaManager OfflineReplicaCount` / `LogManager OfflineLogDirectoryCount` | 0 | disk/log-dir failure |
| `ControllerStats UncleanLeaderElectionsPerSec` | 0 | data loss possible |
| `ControllerStats ElectionFromEligibleLeaderReplicasPerSec` | 0 | ELR election happened (ch17) |
| `KafkaController ActiveControllerCount` | exactly one node = 1 | 0 or > 1 across cluster |
| `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent` | 0–1, ideally > 0.3 | add `num.io.threads`/`num.network.threads` or capacity |
| `IsrShrinksPerSec` / `IsrExpandsPerSec` | 0 except around restarts | flapping ISR = network/GC/disk issues |
| `ReassignmentBytesIn/OutPerSec` | 0; non-zero during reassignment | throttle reassignment (ch10) |
| `BytesRejectedPerSec` | — | batches > `max.message.bytes` |
| Quota `throttle-time` (`type={Produce,Fetch,Request},user,client-id`) | 0 | client is hitting its quota (ch07/ch12) |

**KRaft (controller/quorum) MBeans**

| MBean | Watch for |
|---|---|
| `raft-metrics` current-state / current-leader / current-epoch | leader = -1 (unknown), frequent epoch bumps |
| `raft-metrics` commit-latency avg/max, election-latency avg/max | rising commit/election latency |
| `KafkaController FencedBrokerCount`, `ActiveBrokerCount` | fenced brokers > 0 |
| `KafkaController OfflinePartitionsCount`, `PreferredReplicaImbalanceCount` | > 0 |
| `KafkaController TimedOutBrokerHeartbeatCount` | increases (only on active controller) |
| `KafkaController EventQueueOperationsTimedOutCount`, `ControllerEventManager EventQueueTimeMs` | controller overload |
| `LastAppliedRecordLagMs` (controller and `broker-metadata-metrics`) | growing lag on standby controllers/brokers |
| `MetadataLoader CurrentMetadataVersion`, `SnapshotEmitter LatestSnapshotGeneratedAgeMs` | stale snapshots, MV mismatch during upgrade |

**Client signals**

| Metric (group) | Meaning / threshold |
|---|---|
| `records-lag-max` (`consumer-fetch-manager-metrics`) | must stay below a threshold |
| `last-poll-seconds-ago`, `time-between-poll-max` (`consumer-metrics`) | approaching `max.poll.interval.ms` → member evicted |
| `poll-idle-ratio-avg` | low = app processing is the bottleneck |
| `rebalance-rate-per-hour`, `failed-rebalance-total`, `partitions-revoked-latency-max` (`consumer-coordinator-metrics`) | rebalance storms, slow listeners |
| `buffer-available-bytes`, `waiting-threads`, `bufferpool-wait-ratio`, `buffer-exhausted-rate` (`producer-metrics`) | `buffer.memory` exhausted → `send()` blocks/drops |
| `txn-commit-time-ns-total`, `metadata-wait-time-ns-total` | EOS / metadata stalls |
| `failed-authentication-rate`, `successful-authentication-no-reauth-total` (common) | auth problems; pre-2.2 clients without re-auth |

## Worked Example
**Diagnosing a produce latency spike on a 3-broker KRaft cluster (`acks=all`, `min.insync.replicas=2`).**
1. Client side: producer `request-latency-avg` (per-node `producer-node-metrics`) jumps for node 2 only; `bufferpool-wait-ratio` and `waiting-threads` rise, so `send()` starts blocking.
2. Broker 1 (leader for affected partitions): `TotalTimeMs,request=Produce` is up; decomposing shows `RemoteTimeMs` dominant while `RequestQueueTimeMs`/`LocalTimeMs` are flat → leader is waiting on followers, and `PurgatorySize,delayedOperation=Produce` grows.
3. Replication: `ReplicaFetcherManager MaxLag` on broker 2 climbs; `IsrShrinksPerSec` fires, `UnderReplicatedPartitions` > 0 and `AtMinIsrPartitionCount` > 0 (ISR = 2 = min ISR). `UnderMinIsrPartitionCount` still 0, so writes succeed but have no headroom.
4. Broker 2 itself: `RequestHandlerAvgIdlePercent` ≈ 0.05 and `LogFlushRateAndTimeMs` high → disk-bound broker. `ReassignmentBytesInPerSec` non-zero reveals an unthrottled reassignment saturating it.
5. Fix: apply a replication throttle to the reassignment (ch10). Confirm recovery: `IsrExpandsPerSec` ticks, URP and `AtMinIsrPartitionCount` return to 0, `RemoteTimeMs` and producer latency normalize, controller `OfflinePartitionsCount` stayed 0 throughout.

## Key Takeaways
1. Start with the zero-valued invariants (URP, under/at-min-ISR, offline replicas/log dirs, unclean elections) plus `ActiveControllerCount` = 1 across the cluster.
2. Keep both I/O-handler and network-processor idle ratios above ~0.3; below that, request queue time dominates latency.
3. Break slow requests into queue/local/remote/response time before tuning anything.
4. Monitor lag from the consumer (`records-lag-max`) together with poll gaps and rebalance rates; min fetch rate must stay > 0.
5. In KRaft, watch metadata applied lag, fenced brokers, heartbeat timeouts, and the metadata error counters on both controllers and brokers.
6. For tiered storage, alert on `RemoteCopyErrorsPerSec`/`RemoteFetchErrorsPerSec` and growing `RemoteCopyLagBytes`; use `LocalRetentionSizeInPercent` for local disk pressure.
7. Secure remote JMX (`KAFKA_JMX_OPTS`) before exposing it; also monitor GC time, CPU, and I/O service time on every host.

## Connects To
- **ch07**: replication/ISR and quotas — the semantics behind URP, min-ISR and `throttle-time` metrics.
- **ch10**: basic operations — reassignment throttles and preferred-leader balancing surfaced by `ReassignmentBytes*` and `PreferredReplicaImbalanceCount`.
- **ch12**: multi-tenancy — per-(user, client-id) quota metrics.
- **ch13**: hardware/OS/Java — GC, disk and CPU stats to monitor alongside Kafka metrics.
- **ch15**: KRaft — the quorum, controller and metadata-log metrics.
- **ch16**: tiered storage — `RemoteCopy*`, `RemoteFetch*`, `RemoteLogManager` metrics.
- **ch17**: KIP-848 consumer groups and ELR — group-coordinator state counts and ELR election rate.
- **ch06**: share consumers — share-group, share-partition and share-coordinator metrics.
- **ch18/ch19**: SSL/SASL — authentication and re-authentication metrics, `expired-connections-killed-count`.
- **ch27/ch29**: Streams config and ops — `metrics.recording.level`, RocksDB and state-store metrics.
