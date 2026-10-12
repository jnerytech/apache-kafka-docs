# Patterns & Techniques — Apache Kafka 4.3 Documentation

**Core log & clients**

## Key-Based Ordering
**When to use**: events for one entity must be processed in order.
**How**: use the entity ID as key; every producer must hash keys the same way; leave the key null when you only need load spread (ch01, ch05, ch08)
**Trade-offs**: order is per partition only; skewed keys make hot partitions; adding partitions remaps `hash(key) % N`.

## Batch-and-Compress Producer
**When to use**: high-volume or WAN/cross-DC produce paths.
**How**: raise `linger.ms` (e.g. 10) and `batch.size` (e.g. 64 kB), set `compression.type=zstd|lz4`; batches stay compressed end to end (ch05)
**Trade-offs**: a few ms latency and producer CPU for far higher throughput and compression ratio.

## Share Group as Work Queue
**When to use**: more workers than partitions, per-record ack/retry, no ordering needed.
**How**: KafkaShareConsumer in one share group; records are acquired under a lock then accepted/released/rejected; size `share.record.lock.duration.ms` to processing time (ch03, ch06)
**Trade-offs**: no partition ordering; expired locks redeliver (duplicate work).

## Online Migration to the KIP-848 Consumer Protocol
**When to use**: classic group whose assignor embeds no custom metadata.
**How**: map assignors (Range→`range`, others→`uniform`), drop `session.timeout.ms`/`heartbeat.interval.ms`/`partition.assignment.strategy`, move tuning to broker `group.consumer.*`, roll with `group.protocol=consumer` (ch17)
**Trade-offs**: custom metadata assignors need offline (empty-group) conversion; rack-aware assignment incomplete.

## Compacted Changelog Topic
**When to use**: keyed mutable state (CDC, entities, KTable output) consumers must rebuild.
**How**: `cleanup.policy=compact`, null-value tombstones for deletes, `max.compaction.lag.ms` for low-traffic topics, `delete.retention.ms` > slowest full replay (ch07, ch23)
**Trade-offs**: compaction is lazy (duplicates visible, active segment never cleaned); slow replayers miss tombstones.

**Reliability & exactly-once**

## Durable Write Configuration
**When to use**: any topic where an acked write must never be lost.
**How**: RF=3, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false`; alert on `AtMinIsrPartitionCount` > 0 (ch07, ch14)
**Trade-offs**: writes fail with NotEnoughReplicas when ISR < 2; latency follows the slowest ISR member.

## Exactly-Once Read-Process-Write (Plain Clients)
**When to use**: Kafka→Kafka without Kafka Streams.
**How**: one transactional producer per consumer with a stable `transactional.id`; consumer `read_committed` + `enable.auto.commit=false`; begin → send → `sendOffsetsToTransaction(offsets, groupMetadata())` → commit; on abort, rewind with `committed()` + `seek()` (ch06)
**Trade-offs**: per-transaction overhead; Kafka sinks only; Streams `exactly_once_v2` does this for you.

## Replication over fsync, Small Heap + Big Pagecache
**When to use**: sizing and durability posture of every broker.
**How**: fixed ~6 GB G1 heap, rest of RAM to pagecache (≈ write throughput × 30); keep default no-fsync flush; rely on replicas for recovery (ch05, ch13)
**Trade-offs**: correlated power loss across all replicas can lose unflushed data; TLS disables zero-copy.

**Operations**

## Throttled Reassignment (incl. Decommission)
**When to use**: expansion, decommission, raising RF.
**How**: `--generate` or hand plan → save rollback JSON → `--execute --throttle <B/s>` → `--verify` until done (clears throttle). Decommission: `cordoned.log.dirs="*"`, drain by reassignment, stop, `kafka-cluster.sh unregister` (ch10)
**Trade-offs**: throttle below max BytesInPerSec never converges; skipping `--verify` leaves replication throttled.

## Two-Phase Upgrade (Roll, Soak, Finalize)
**When to use**: KRaft 3.3+ → 4.x upgrades.
**How**: ZK or MV < 3.3 → go via 3.9.x first; roll nodes onto new binaries; soak; `kafka-features.sh upgrade --release-version 4.3` (ch30)
**Trade-offs**: binary rollback only until finalization; 4.0/4.3 MVs block metadata downgrade.

## Grow a Dynamic KRaft Quorum
**When to use**: new production cluster or controller replacement.
**How**: `random-uuid`; format controller 1 `--standalone`, others `--no-initial-controllers`; wait for catch-up (`describe --replication`), then `add-controller`; remove with `remove-controller --controller-directory-id` before shutdown (ch15)
**Trade-offs**: one voter until grown; static quorums need `kraft.version=1` first.

## Zero-Invariant Alerting + Latency Decomposition
**When to use**: baseline monitoring and latency regressions.
**How**: alert on non-zero URP, UnderMinIsr, Offline*, UncleanLeaderElections, ActiveControllerCount sum ≠ 1, idle % < 0.3; split TotalTimeMs into queue/local/remote/response; watch client `records-lag-max` (ch14)
**Trade-offs**: rolling restarts trip ISR alerts; lag lives on clients only.

## Hot-Local / Cold-Remote Tiering
**When to use**: long retention, tail-read topics, small broker disks.
**How**: broker `remote.log.storage.system.enable=true` + RSM plugin + RLMM listener; topic `remote.storage.enable=true`, small `local.retention.*`, long `retention.*` (ch16)
**Trade-offs**: third-party RSM; slow backfill reads; no compacted topics; broken RSM still fills disks.

## Local Cluster per DC + MirrorMaker 2
**When to use**: pipelines spanning DCs/regions.
**How**: apps use the local cluster; MM2 flows `A->B.enabled=true` (both directions in one shared config for active/active); run MM2 near the target with `--clusters` (consume remote, produce local) (ch11)
**Trade-offs**: async, renamed `{source}.{topic}` copies; never stretch one cluster over a WAN.

## Tenant Namespace with Prefixed ACLs and Quotas
**When to use**: shared multi-tenant clusters.
**How**: `<org>.<team>.<dataset>.<event>` names; one prefixed ACL per tenant; `auto.create.topics.enable=false` + `CreateTopicPolicy`; per-principal `request_percentage` first, then byte rates and `controller_mutation_rate` (ch12, ch20)
**Trade-offs**: topics cannot be renamed; quotas are per broker; shared credentials defeat it.

**Security**

## Phased Security Rollout (SSL/SASL, Mechanism Migration)
**When to use**: securing or switching mechanisms on a live cluster.
**How**: bounce 1 adds secured listener/mechanism; move clients; bounce 2 switches inter-broker protocol/mechanism; bounce 3 removes the old one; wait for ISR between brokers (ch18, ch19)
**Trade-offs**: three rolling restarts; old port/mechanism stays open until the end.

## Default-Deny ACL Rollout
**When to use**: enabling authorization on KRaft.
**How**: `StandardAuthorizer` on brokers AND controllers; admin + inter-broker principals in `super.users`; prefixed `--producer`/`--consumer --group` ACLs; debug with `--resource-pattern-type match` (ch20)
**Trade-offs**: a missing ACL is an outage; `allow.everyone.if.no.acl.found=true` bridge must be removed.

**Connect**

## Connect Error Ladder (Retry → Tolerate → Log → DLQ)
**When to use**: production connectors where a bad record must not halt the pipeline.
**How**: `errors.retry.timeout`, `errors.tolerance=all`, `errors.log.enable=true`, `errors.deadletterqueue.topic.name` + context headers; sinks use `ErrantRecordReporter` (ch21, ch22)
**Trade-offs**: tolerance without log/DLQ silently drops data; logged messages may leak payloads.

**Streams**

## Co-Partition Before Joining
**When to use**: equi-joins whose inputs differ in partition count/partitioner.
**How**: `repartition(Repartitioned.numberOfPartitions(n))` the smaller side (the stream in stream-table joins); or use GlobalKTable / FK join (ch25)
**Trade-offs**: extra topic and hop; Streams checks counts, not partitioners.

## Streams Production Baseline
**When to use**: every production Streams app.
**How**: `acks=all`, internal RF=3, min ISR 2, `num.standby.replicas=1`, persistent `state.dir`, name all stateful ops (`Grouped`/`Materialized`/`Joined`/`StreamJoined`) + `ensure.explicit.internal.resource.naming=true`; `exactly_once_v2` if needed (ch27, ch29)
**Trade-offs**: 3x broker storage, extra instances; retrofitting names forces a reset.

## Full Application Reset
**When to use**: reprocess after a bug fix or breaking topology change.
**How**: stop all instances; `--dry-run` then run `kafka-streams-application-reset.sh` (classic) or `kafka-streams-groups.sh --reset-offsets` (streams); `cleanUp()` on every host (ch29)
**Trade-offs**: irreversible; output topics not reset, downstream sees duplicates.
