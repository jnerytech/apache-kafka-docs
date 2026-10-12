# Chapter 30: Upgrade Notes (Kafka 4.x and Kafka Streams)

## Core Idea
Upgrading to Kafka 4.x is a two-phase operation: roll the new binaries broker-by-broker, verify, then *finalize* with `kafka-features.sh upgrade --release-version 4.x` — and finalization (metadata.version bump) is the point of no return whenever the target MetadataVersion carries metadata changes. Before any of that, the cluster must already be KRaft on ≥3.3.x and every client must be ≥2.1, because 4.0 removed ZooKeeper, old protocol versions and every API deprecated up to 3.x.

## Frameworks Introduced
- **Two-phase rolling upgrade (binaries, then feature finalization)**: software version and cluster `metadata.version` move independently.
  - When to use: every broker/controller upgrade 3.3.x → 4.0/4.1/4.2/4.3 (and 4.x → 4.y).
  - How:
    1. Pre-check: cluster is in KRaft mode; software *and* metadata version ≥ 3.3.x. KRaft < 3.3 → go to 3.9.x first. ZooKeeper cluster → complete ZK→KRaft migration (ch15) on 3.x first.
    2. Bounce nodes one at a time: shut down, swap binaries, restart. Old metadata.version keeps running — this is your soak window.
    3. Verify behavior and performance on the new code.
    4. Finalize: `bin/kafka-features.sh --bootstrap-server <host:port> upgrade --release-version 4.3`.
    5. Finalization auto-enables version-gated features (e.g. KIP-848 consumer protocol and KIP-890 transactions at 4.0).
  - Why it works / failure mode: until step 4 you can still roll binaries back. Each `MetadataVersion` enum entry carries a boolean "has metadata changes" (e.g. `IBP_4_3_IV0(30, "4.3", "IV0", true)`, `IBP_4_0_IV1(23, ..., true)`, `IBP_4_2_IV1(29, ..., false)`). Metadata downgrade is possible only if *no* version between current and target has metadata changes — so finalizing 4.0 or 4.3 is irreversible; 4.2 alone is downgradable.
- **Client-first compatibility floor (KIP-896 / KIP-1124)**: 4.0 dropped old protocol API versions.
  - When to use: before upgrading either side to 4.0+.
  - How: brokers must be ≥2.1 before upgrading Java clients (incl. Streams, Connect) to 4.0; clients must be ≥2.1 before upgrading brokers to 4.0. Audit non-Apache clients (librdkafka, Go, etc.) against KIP-896. Roll clients one at a time (stop, update, restart).
- **Feature-version gating via `kafka-features.sh`**: newer protocols are switched on by finalized feature levels rather than broker configs.
  - When to use: enabling `share.version`, `streams.version`, `group.version`, or checking state after an upgrade.
  - How: `kafka-features.sh --bootstrap-server ... describe` → read `FinalizedVersionLevel`; then `upgrade --feature streams.version=1` (or `share.version=1`). Optional `--node-id` (4.2+) to describe a specific node. `group.coordinator.rebalance.protocols` is deprecated (4.3) — in 5.0 protocols are governed only by `group.version`/`streams.version`/`share.version`.
- **Kafka Streams double-bounce with `upgrade.from`** (KIP-904): handles the FK-join and repartition-topic serialization format changes.
  - When to use: upgrading a Streams app from ≤3.4 to 4.3.
  - How: bounce 1 with `upgrade.from="<old version>"` (valid `"2.4"`–`"3.4"`); bounce 2 with it removed. Alternative: offline upgrade (stop all instances, swap jars, start) works from as old as 0.11.0.x.
  - Failure mode: skipping bounce 1 lets new instances write formats the still-running old instances cannot read.

## Key Concepts
- **`metadata.version` / release-version**: the finalized cluster metadata level, raised explicitly after binaries are rolled; gates on-disk/metadata formats and new protocols.
- **MetadataVersion "metadata changes" flag**: boolean on each `IBP_x_y_IVn` that decides whether a later downgrade is allowed.
- **KRaft-only**: 4.0+ has no ZooKeeper mode; `config/kraft` merged into `config/`; ZK tools (`ZkSecurityMigrator`, `kafka-acls --authorizer`/`--zk-tls-config-file`) removed.
- **New group coordinator (4.0)**: rewritten coordinator implementing the same APIs, tunable via `group.coordinator.*` configs.
- **KIP-848 consumer protocol**: GA in 4.0, auto-enabled on finalization; once used, the cluster can downgrade only to ≥3.4.1.
- **KIP-890 Transactions Server-Side Defense**: enabled on 4.0 finalization; 4.0 producers bump epoch every transaction; protocol downgrade is safe. Treat `TimeoutException` and `TransactionAbortableException` as "abort the transaction".
- **ELR (KIP-966 part 1)**: controller tracks out-of-ISR replicas safe to elect; default-on for new clusters from 4.1, which drops any broker-level `min.insync.replicas` — set it at cluster level.
- **Share groups (KIP-932)**: preview in 4.1 (`share.version=1`), production-ready in 4.2; internal topic `__share_group_state` defaults to RF 3.
- **Streams Rebalance Protocol (KIP-1071)**: `group.protocol=streams`; early access 4.1, GA core in 4.2; classic→streams migration is safe only from 4.2.1.
- **KIP-1035 store-managed offsets (Streams 4.3)**: changelog offsets live inside each store (RocksDB `offsets` column family), replacing the per-task `.checkpoint` file.

## Mental Models
- Think of an upgrade as **binary roll = reversible, finalize = commit**. Soak as long as you need between the two; do the finalize command only when you would be fine never going back.
- Think of 4.0 as **"the great removal"**: anything deprecated in 3.x (API, config, tool flag, eos-v1, MM1, Log4j1, Scala 2.12, Java 8) is gone. Grep your code/configs for each removed name before upgrading, not after.
- Use **x.y.1 rather than x.y.0** for Streams: 4.1.0 (KAFKA-19748 memory leak), 4.2.0 (KAFKA-20254 migration bug), 4.3.0 (KAFKA-20616 RocksDB native leak) all shipped critical Streams bugs fixed in the .1.
- Think of **Streams downgrades as "wipe local state, restore from changelog"**: RocksDB format (4.0) and offsets column family (4.3) make in-flight downgrade impossible.

## Anti-patterns
- **Upgrading a ZooKeeper cluster straight to 4.x**: not supported; migrate to KRaft on 3.x (ideally 3.9.x) first.
- **Finalizing immediately after the last bounce**: forfeits the only rollback window when the target MV has metadata changes.
- **Leaving pre-2.1 clients/brokers in the fleet**: old protocol versions were removed; those connections break.
- **Relying on removed defaults/fallbacks**: e.g. `enable.idempotence` no longer silently falls back when `max.in.flight.requests.per.connection` > 5 — it errors.
- **Space-separated `--bootstrap-server` lists**: 4.0 accepts only comma-separated `host1:port1,host2:port2`.
- **Using share groups on <3 brokers without lowering** `share.coordinator.state.topic.replication.factor` and `share.coordinator.state.topic.min.isr` to 1.
- **Migrating classic→streams groups on 4.2.0**: broker bug KAFKA-20254; use 4.2.1+. Reusing a classic `application.id`/`group.id` for `group.protocol=streams` on 4.1 is not supported either.
- **In-place Streams downgrade from 4.3 to ≤4.2**: older versions don't declare the `offsets` column family and crash on startup.

## Code Examples
```bash
# Phase 2 of a broker upgrade: finalize (irreversible if MV has metadata changes)
bin/kafka-features.sh --bootstrap-server localhost:9092 describe
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --release-version 4.3

# Enable a single gated feature (e.g. streams groups on 4.1 test clusters)
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
```
- **What it demonstrates**: `describe` before/after; release-version finalization vs per-feature upgrade.

```properties
# Kafka Streams app upgrading from 3.3 -> 4.3, first rolling bounce
upgrade.from=3.3
# second rolling bounce: remove upgrade.from entirely
```
- **What it demonstrates**: KIP-904 double bounce for FK-join and repartition-topic format changes.

```bash
# 4.1+ streams groups: group-level configs are set server-side, client-side values ignored
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups \
  --entity-name wordcount --add-config streams.num.standby.replicas=1
```
- **What it demonstrates**: `streams.session.timeout.ms`, `streams.heartbeat.interval.ms`, `streams.num.standby.replicas` become group configs under KIP-1071.

## Reference Tables

**Upgrade prerequisites (4.x)**

| Component | Requirement |
|---|---|
| Brokers/controllers | KRaft mode, software + metadata ≥ 3.3.x (KRaft <3.3 → 3.9.x first) |
| Java clients, Streams, Connect | ≥ 2.1 before broker → 4.0; brokers ≥ 2.1 before clients → 4.0 |
| Java runtime | Clients & Streams: Java 11+; brokers, Connect, tools: Java 17+ (Java 23 supported) |
| Streams EOS | eos-v1 removed; `exactly_once_v2` needs brokers ≥ 2.5 |
| Scala | 2.12 dropped; `kafka-streams-scala` deprecated 4.3, removed 5.0 |
| Logging | Log4j → Log4j2 (`log4j2.yaml`; convert with log4j-transform-cli); `KafkaLog4jAppender` removed |

**Version-by-version notable breaking/behavioral changes**

| Version | Change | Action |
|---|---|---|
| 4.0 | ZooKeeper removed; KRaft only | Migrate on 3.x first |
| 4.0 | MetadataVersion has metadata changes | No metadata downgrade after finalize |
| 4.0 | KIP-848 GA + KIP-890 enabled on finalize | Post-848 usage: downgrade only to ≥3.4.1 |
| 4.0 | `linger.ms` default 0 → 5 | Re-check latency SLOs |
| 4.0 | `message.timestamp.after.max.ms` default Long.MAX → 1 h | Future-dated CreateTime records rejected |
| 4.0 | `num.recovery.threads.per.data.dir` 1 → 2 | Faster recovery, more IO |
| 4.0 | Min `segment.bytes`/`log.segment.bytes` 14 B → 1 MB | Fix tiny-segment configs |
| 4.0 | `remote.log.manager.copier/expiration.thread.pool.size` −1 → 10 (−1 invalid); `remote.log.manager.thread.pool.size` 10 → 2 | Review tiered-storage pools |
| 4.0 | Removed `log.message.format.version`, `message.format.version`, `offsets.commit.required.acks`, `delegation.token.master.key` (→ `delegation.token.secret.key`), `log.message.timestamp.difference.max.ms` (→ `...before.max.ms`/`...after.max.ms`) | Delete/rename in server.properties |
| 4.0 | `metrics.jmx.blacklist/whitelist` → `exclude/include`; `auto.include.jmx.reporter` removed | Update metrics configs |
| 4.0 | MirrorMaker 1 removed; MM2 `*.blacklist` → `*.exclude` | Move to MM2 |
| 4.0 | Consumer `poll(long)` removed → `poll(Duration)` (no blocking past timeout for assignment); `committed(TopicPartition)` removed | Code change |
| 4.0 | Admin `alterConfigs` removed → `incrementalAlterConfigs`; `describeConsumerGroups` throws `GroupIdNotFoundException` instead of DEAD | Code change |
| 4.0 | `DefaultPartitioner`, `UniformStickyPartitioner`, `Partitioner.onNewBatch` removed | Use built-in default |
| 4.0 | `org.apache.kafka.sasl.oauthbearer.allowed.urls` system property, default empty | Allow-list token/JWKS URLs |
| 4.0 Streams | APIs deprecated ≤3.6 removed (old Processor API, `through()`, transformers, `branch`, `setUncaughtExceptionHandler`); RocksDB 7.9.2 → 9.7.3 | Migrate `transformValues` → `processValues` (≥4.0.1 fix) |
| 4.1 | ELR default-on for new clusters; broker-level `min.insync.replicas` removed | Set cluster-level |
| 4.1 | `log.cleaner.enable` deprecated; `ConfigCommand --force` removed; producer `flush()` in callback prohibited | — |
| 4.2 | Share groups production-ready; KIP-1071 GA core | Use 4.2.1 for streams migrations |
| 4.2 | LIST-type config validation stricter (no null, dedupe, no empty where harmful); empty `cleanup.policy` = infinite retention | Validate configs |
| 4.2 | KIP-1147 CLI renames: `--command-property`, `--command-config`, `--formatter-property`, `--reader-property`, `--num-records` | Old flags removed in 5.0 |
| 4.2 | `controller.quorum.auto.join.enable` (default false) | Shut controller down before removing from voters if true |
| 4.3 | MetadataVersion has metadata changes | No metadata downgrade after finalize |
| 4.3 | `group.consumer/share/streams.assignment.interval.ms` default 1 s (was effectively 0) | Expect batched assignment updates |
| 4.3 | `group.coordinator.rebalance.protocols` deprecated | Use feature versions |
| 4.3 Streams | KIP-1035 store-managed offsets; downgrade requires wiping `state.dir` | Upgrade straight to 4.3.1 |

**Streams ↔ broker compatibility**: Streams 2.4–4.3 works with brokers 2.4–4.3; EOS v2 requires brokers ≥2.5; Streams 4.x requires brokers ≥2.1.

## Worked Example
Upgrade a 3.7 KRaft cluster plus a Streams 3.4 app to 4.3:
1. Audit: confirm KRaft (no ZK), MV ≥ 3.3. Inventory clients — all ≥2.1? Grep configs for removed keys (`log.message.format.version`, `offsets.commit.required.acks`, `metrics.jmx.whitelist`, space-separated bootstrap lists). Ensure brokers will run Java 17, clients Java 11. Convert log4j.properties to `log4j2.yaml`.
2. Roll brokers/controllers to 4.3 binaries one node at a time; MV stays at 3.7. Watch URP, request latency, controller health.
3. Soak. If problems appear, roll binaries back — still possible because MV wasn't raised.
4. Finalize: `kafka-features.sh --bootstrap-server b1:9092 upgrade --release-version 4.3`. This enables KIP-848, KIP-890 and later features, and is irreversible (MVs 4.0 and 4.3 carry metadata changes).
5. Streams app (3.4 → 4.3.1): first rolling bounce with `upgrade.from=3.4`, second with it removed. Before compiling, replace removed APIs (`transformValues` → `processValues` per the migration guide, `KStream#through()`, old `branch`, old Processor API), switch `exactly_once` to `exactly_once_v2`.
6. Optionally opt consumers into `group.protocol=consumer` (KIP-848) now that it's finalized.

## Key Takeaways
1. Pre-flight for 4.x: KRaft on ≥3.3 (3.9.x recommended stepping stone), all clients/brokers ≥2.1, Java 17 server / Java 11 clients.
2. Separate "roll binaries" from "finalize release-version"; finalization is your commit point and, for 4.0/4.3, permanently blocks metadata downgrade.
3. Grep for removed configs/APIs before upgrading — 4.0 removes rather than deprecates, and changes defaults (`linger.ms=5`, 1 h future-timestamp limit, 1 MB min segment).
4. New protocols (consumer, streams, share groups) are gated by feature versions; manage them with `kafka-features.sh describe/upgrade`.
5. For Kafka Streams, prefer the x.y.1 patch release, use the `upgrade.from` double bounce from ≤3.4, and plan downgrades as "wipe state.dir + restore from changelog".
6. On small (<3 broker) clusters, lower internal-topic RF/min-ISR for `__share_group_state` before using share groups; under ELR set `min.insync.replicas` at cluster level.

## Connects To
- **ch02**: client/broker compatibility matrix and quickstart config layout (`config/` after `config/kraft` merge).
- **ch15**: KRaft operations, ZK→KRaft migration (mandatory precursor), `kafka-features.sh`, controller quorum.
- **ch17**: KIP-848 consumer protocol, KIP-890 transactions, ELR — all switched on by 4.0 finalization.
- **ch06**: share groups and transaction semantics (`TransactionAbortableException` handling).
- **ch16**: tiered-storage thread-pool default changes and `follower.fetch.last.tiered.offset.enable`.
- **ch24 / ch27 / ch29**: Streams state stores (KIP-1035, headers-aware stores), configs (`upgrade.from`, `dsl.store.format`), and the Streams rebalance protocol (KIP-1071).
- **ch11**: MirrorMaker 1 removal → MM2.
