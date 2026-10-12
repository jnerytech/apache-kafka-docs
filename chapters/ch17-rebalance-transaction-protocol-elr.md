# Chapter 17: Consumer Rebalance Protocol, Transaction Protocol and Eligible Leader Replicas

## Core Idea
Kafka 4.x ships three protocol upgrades, each gated by a KRaft **feature flag**: the server-driven, incremental consumer rebalance protocol (KIP-848, `group.version`), server-side transaction defense (KIP-890, `transaction.version`), and Eligible Leader Replicas (KIP-966, `eligible.leader.replicas.version`). Each one takes coordination work away from the clients and gives it to the brokers and controller. Each can be upgraded and downgraded safely, but each has client-side or config-side catches you need to plan for. The source material is short (about 10 KB), so this chapter is too.

## Frameworks Introduced

- **Next-gen Consumer Rebalance Protocol (KIP-848)**: the group coordinator on the broker owns heartbeat timing, session timeout and partition assignment. Rebalances are fully incremental, so there is no global synchronization barrier where every member stops for a join/sync round.
  - When to use: large or frequently scaling consumer groups, groups that suffer from long stop-the-world rebalances, and any new consumer app on Kafka 4.0+.
  - How:
    1. Server side: it is enabled automatically since 4.0 and controlled by the `group.version` feature flag.
    2. Tune liveness on the broker with `group.consumer.heartbeat.interval.ms` and `group.consumer.session.timeout.ms`, not on the client.
    3. Choose assignors on the broker with `group.consumer.assignors`. `uniform` and `range` are built in, and the first entry (`uniform`) is the default. A client can pick one from that list with `group.remote.assignor`.
    4. Client side: set `group.protocol=consumer`. It is still opt-in in 4.x.
    5. Remove `heartbeat.interval.ms`, `session.timeout.ms` and `partition.assignment.strategy` from the client config, and stop calling `enforceRebalance()`. None of these can be used under the new protocol.
  - Why it works / failure mode: no member has to stop and wait for the whole group, so each change only moves the affected partitions, and rebalance time no longer grows with group size. The catch is that **client-side assignors are not supported**, and rack-aware assignment is still incomplete (KAFKA-19387). A custom `ConsumerPartitionAssignor` therefore blocks migration until you rewrite it as a server-side `ConsumerGroupPartitionAssignor`.

- **Classic ↔ Consumer group conversion (online/offline migration)**: the coordinator converts a group's type in place.
  - When to use: migrating existing groups to `group.protocol=consumer`.
  - How:
    - *Offline*: stop all consumers. The group becomes empty and converts automatically. Restart them with `group.protocol=consumer`. This needs downtime.
    - *Online*: roll consumers one at a time with `group.protocol=consumer`. The group converts from Classic to Consumer when the **first** new-protocol member joins, and the coordinator handles classic members alongside new ones until the roll finishes. This only works if the classic assignor **does not embed custom metadata**.
    - *Downgrade*: do the reverse. The group converts back to Classic when the **last** new-protocol member leaves.
  - Why it works / failure mode: the conversion is driven by membership, so a rolling deploy is enough. A group that uses a metadata-embedding custom assignor has to take the offline path.

- **Transactions Server-Side Defense (KIP-890)**: with `transaction.version=2` and producer clients 4.0+, the producer epoch is **bumped on every transaction**. A late or zombie write from transaction N therefore cannot land inside transaction N+1. Adding partitions also takes a single server-side call, where it used to take a client add plus a server verify.
  - When to use: always, on 4.0+ clusters with transactional or exactly-once producers. It is on by default since 4.0.
  - How:
    1. Set the flag when you format a new cluster (storage tool) or on a live cluster (features tool): `transaction.version=2`.
    2. You don't need to restart producers. They switch over when they next connect or reconnect, at the **start of their next transaction**, never mid-transaction.
    3. Downgrading is safe. Clients go back to the old protocol on the first transaction after they see the downgrade.

- **Eligible Leader Replicas (KIP-966 Part 1)**: under the "strict min ISR" rule, the high watermark cannot advance while the ISR is smaller than `min.insync.replicas`. That makes some out-of-ISR replicas safe to lead, because they hold every committed record. The controller records them in the `PartitionRecord` field `Eligible Leader Replicas` and elects leaders in this order:
  1. a member of the ISR, if the ISR is not empty;
  2. otherwise an **unfenced** member of the ELR;
  3. otherwise the last known leader, if it is unfenced. This matches pre-4.0 behavior when all replicas are offline.
  - When to use: to keep partitions available after cascading ISR shrink without risking data loss (no unclean election). It is on by default on new 4.1+ clusters and opt-in on 4.0.
  - How: set `eligible.leader.replicas.version=1` and the controller starts tracking ELR. Downgrade by setting `eligible.leader.replicas.version=0`.

## Key Concepts
- **`group.version`**: the KRaft feature flag that turns the KIP-848 consumer protocol on or off on the server.
- **`group.protocol`**: the client config, `classic` or `consumer`, that picks which rebalance protocol a consumer speaks. The default is `classic` until Kafka 5.0.
- **Server-side assignor**: a `ConsumerGroupPartitionAssignor` implementation listed in `group.consumer.assignors`. The built-ins are `uniform` (the default) and `range`.
- **`subscribe(SubscriptionPattern)`**: a regex subscription for the new protocol. It uses **RE2J** syntax and is evaluated **on the server**.
- **`transaction.version`**: the feature flag for KIP-890. A value of `2` enables the per-transaction epoch bump and the server-side add-partitions call.
- **`CONCURRENT_TRANSACTIONS`**: the error returned when a producer adds partitions to transaction N+1 before transaction N's markers have been written. `endTransaction` is asynchronous, so this can happen.
- **ELR (Eligible Leader Replicas)**: replicas outside the ISR that the controller knows still hold all data up to the high watermark, which makes them safe election candidates.
- **`eligible.leader.replicas.version`**: the feature flag for ELR. `1` turns it on and `0` turns it off.
- **Cluster-level `min.insync.replicas`**: when ELR is on, this is the only non-topic scope allowed for the setting. Broker-level values are removed and cannot be changed.

## Mental Models
- **Think of KIP-848 as moving the group "brain" from the leader consumer to the broker.** Timeouts, assignment and regex evaluation now live in broker config, so client configs for those things are dead weight.
- **Use online migration when the classic assignor is a stock one** (Range, Sticky, CooperativeSticky, RoundRobin). Use offline migration when a custom assignor embeds metadata.
- **Think of KIP-890's latency shift as accounting, not slowdown.** Retry time for `CONCURRENT_TRANSACTIONS` moves from client-side backoff into the reported produce latency. End-to-end transaction latency does not increase.
- **Think of ELR as the safe middle ground between "partition offline" and `unclean.leader.election`.** It only works because strict min-ISR guarantees that ELR members are not behind the high watermark.

## Anti-patterns
- **Leaving `session.timeout.ms` / `heartbeat.interval.ms` / `partition.assignment.strategy` in a `group.protocol=consumer` client**: these settings no longer apply under the new protocol. Tune `group.consumer.*` on the broker instead.
- **Calling `enforceRebalance()` with the new protocol**: this API is not usable under KIP-848.
- **Counting on a custom client-side assignor after migration**: client-side assignors are not supported. Port the logic to a server-side `ConsumerGroupPartitionAssignor` or stay on classic, and send feedback via KAFKA-18327.
- **Expecting full rack-aware assignment under KIP-848 today**: it is still in progress (KAFKA-19387).
- **Alarming on higher produce latency after enabling `transaction.version=2`**: the server-side retry loop accounts for it. Check end-to-end transaction latency before tuning.
- **Setting or changing `min.insync.replicas` at broker level with ELR on**: this is not allowed, and existing broker-level values are removed. Also, *any* update to the cluster-level value, even to the same value, clears **all** ELR state, and a topic-level update clears that topic's ELR state. Avoid gratuitous re-sets.

## Code Examples

```properties
# Consumer client (Kafka 4.x) opting into KIP-848
group.id=orders-processor
group.protocol=consumer
group.remote.assignor=range        # optional; must be listed in broker group.consumer.assignors
# REMOVE: session.timeout.ms, heartbeat.interval.ms, partition.assignment.strategy
```
- **What it demonstrates**: the client opts in with `group.protocol=consumer`. Liveness and assignment move to the broker, and the client can only *choose* among the assignors the server offers.

```java
consumer.subscribe(new SubscriptionPattern("orders-.*"),   // RE2J, evaluated server-side
                   rebalanceListener);
```
- **What it demonstrates**: regex subscription under the new protocol. Pattern matching runs on the coordinator, so new matching topics are picked up without client-side metadata polling.

```bash
# Turn protocols on/off on an existing cluster (features tool)
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature transaction.version=2
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature eligible.leader.replicas.version=1
bin/kafka-features.sh --bootstrap-server localhost:9092 downgrade --feature eligible.leader.replicas.version=0
```
- **What it demonstrates**: all three features are switched with KRaft feature flags at runtime. For a brand-new cluster, set them when you format the storage instead.

## Reference Tables

**Client → server assignor mapping**

| Client-side assignor | Server-side assignor |
|---|---|
| `RangeAssignor` | `range` |
| `CooperativeStickyAssignor` | `uniform` |
| `StickyAssignor` | `uniform` |
| `RoundRobinAssignor` | `uniform` |

**Feature flags**

| Flag | Enable | Default state | Clients need restart? | Downgrade |
|---|---|---|---|---|
| `group.version` | on server | enabled since 4.0 | consumers must set `group.protocol=consumer` | auto when last new-protocol member leaves |
| `transaction.version` | `=2` | enabled since 4.0 | no; switches at next transaction after reconnect | safe; old protocol on next transaction |
| `eligible.leader.replicas.version` | `=1` | off in 4.0; on for new clusters from 4.1 | n/a | safe, `=0` |

**Configs you actually tune**

| Config | Scope | Meaning | When to change |
|---|---|---|---|
| `group.consumer.heartbeat.interval.ms` | broker | heartbeat cadence for new-protocol members | faster failure detection vs. coordinator load |
| `group.consumer.session.timeout.ms` | broker | member liveness deadline | long GC pauses / flaky networks |
| `group.consumer.assignors` | broker | available server assignors; first = default (`uniform`) | add `range` first or a custom `ConsumerGroupPartitionAssignor` class |
| `group.remote.assignor` | consumer | pick one of the broker's assignors | need range-style co-partitioning |
| `add.partitions.to.txn.retry.backoff.ms` | broker | server-side backoff on `CONCURRENT_TRANSACTIONS` | tune produce-latency vs. retry pressure |
| `add.partitions.to.txn.retry.backoff.max.ms` | broker | cap on total server retry time | before returning the error to the client |

**Rebalance protocol timeline (KIP-1274)**

| Version | Status |
|---|---|
| 3.7 | Early Access |
| 4.0 | GA, production-ready |
| 5.0 | `KafkaConsumer` defaults to `consumer`; `classic` still supported |
| 6.0 | `KafkaConsumer` supports only `consumer`; brokers still accept `classic` |

## Worked Example
**Zero-downtime migration of a 12-instance consumer group to KIP-848.**
1. Check the cluster is on 4.0+ with `group.version` enabled (the 4.0 default). On the brokers, confirm `group.consumer.assignors` lists what you need. The app used `CooperativeStickyAssignor`, which maps to `uniform`, the default, so the client doesn't need `group.remote.assignor`.
2. Confirm the classic assignor embeds no custom metadata. Stock assignors don't, so online migration is possible.
3. Change the client config: add `group.protocol=consumer` and delete `session.timeout.ms`, `heartbeat.interval.ms` and `partition.assignment.strategy`. Move any timeout tuning to `group.consumer.session.timeout.ms` / `group.consumer.heartbeat.interval.ms` on the brokers. Replace any `enforceRebalance()` calls, and if the app subscribes by regex, switch to `subscribe(SubscriptionPattern)` with an RE2J pattern.
4. Roll instance 1. As it joins, the coordinator converts the group from **Classic to Consumer** and handles the 11 classic members alongside it.
5. Roll the other instances. Each one moves only the partitions that need to move, with no group-wide barrier.
6. Rollback plan: roll back to `group.protocol=classic`. When the last new-protocol member leaves, the group converts back to **Classic**.

## Key Takeaways
1. KIP-848 is GA in 4.0 but **opt-in on the client** (`group.protocol=consumer`) until 5.0, and it becomes the only `KafkaConsumer` protocol in 6.0. Plan the migration now.
2. Move rebalance tuning to the broker (`group.consumer.*`) and remove the client-side timeout and assignor configs.
3. Inventory custom assignors before migrating. Client-side assignors are unsupported, rack-awareness is incomplete, and metadata-embedding assignors force an offline migration.
4. Keep `transaction.version=2` (the 4.0 default) for exactly-once producers. The per-transaction epoch bump prevents duplicates leaking into the next transaction, and no client restart is needed.
5. After enabling KIP-890, expect higher *produce* latency on add-partition requests but the same end-to-end transaction latency. Tune `add.partitions.to.txn.retry.backoff(.max).ms` only if it matters.
6. Enable ELR (`eligible.leader.replicas.version=1`) on 4.0 clusters for safer availability. Afterwards, manage `min.insync.replicas` **only at cluster or topic level**, and remember that every update clears ELR state.
7. Inspect ELR via the `DescribeTopicPartitions` API or by describing topics with the Admin client.

## Connects To
- **ch05**: consumer group design and the classic rebalance model that KIP-848 replaces.
- **ch06**: transactions and exactly-once semantics, which KIP-890 hardens.
- **ch07**: replication, ISR, `min.insync.replicas` and unclean leader election, which ELR builds on.
- **ch03**: the `KafkaConsumer`/`Producer`/`Admin` APIs (`subscribe(SubscriptionPattern)`, describe topics).
- **ch15**: KRaft feature flags, the storage tool and the features tool used to toggle all three protocols.
- **ch14**: new consumer metrics for the KIP-848 threading model, and produce-latency monitoring.
- **ch29**: the Streams rebalance protocol, the Streams counterpart to KIP-848.
- **ch30**: upgrade notes that list these defaults per release.
