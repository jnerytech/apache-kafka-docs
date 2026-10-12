# Chapter 10: Basic Kafka Operations

## Core Idea
Day-2 Kafka administration is a small set of `bin/` tools (`kafka-topics.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh`, `kafka-share-groups.sh`, `kafka-reassign-partitions.sh`, `kafka-leader-election.sh`, `kafka-cluster.sh`). Kafka never moves data for you: a new broker sits idle and a broker you remove keeps its data until you run a reassignment. So every cluster change follows the same loop: plan (and save the rollback JSON), execute with a throttle, verify, and run verify again so the throttle gets removed.

## Frameworks Introduced
- **Partition reassignment loop (`kafka-reassign-partitions.sh`)**: three mutually exclusive modes: `--generate`, `--execute`, `--verify`.
  - When to use: cluster expansion, broker or log-dir decommissioning, increasing replication factor, moving specific partitions.
  - How: (1) For whole-topic moves, write `{"topics":[{"topic":"foo1"}],"version":1}` and run `--generate --topics-to-move-json-file ... --broker-list "5,6"`. For targeted moves, hand-craft the JSON and skip `--generate`. (2) Save the printed *Current partition replica assignment*. It is your rollback file. (3) Run `--execute --reassignment-json-file plan.json` with `--throttle`. (4) Run `--verify` with the **same** JSON until every partition shows completed.
  - Why it works: the move is replicate-then-delete. The new broker joins as a follower, fully catches up and enters the ISR, and only then does an old replica delete its copy. Replication factor stays the same throughout.
  - Failure mode: the tool does not look at load. `--generate` spreads partitions evenly over the target brokers but ignores partition size, and it cannot build plans for decommissioning. You have to decide what moves where.
- **Throttled replication**: put an upper bound on reassignment bandwidth, both between brokers (`--throttle` B/s) and between disks on one broker (`--replica-alter-log-dirs-throttle` B/s).
  - When to use: every data-heavy move (rebalance, adding or removing brokers or disks) on a cluster serving live traffic.
  - How: set the throttle on `--execute`. To change it mid-flight, re-run with `--additional --execute` and the same JSON plus a new `--throttle`. Watch `kafka.server:type=FetcherLagMetrics,name=ConsumerLag,...`, which should keep falling. When the move is done, run `--verify`, which clears the broker-level and topic-level throttles.
  - Why it works / failure mode: if `max(BytesInPerSec) > throttle`, replication never catches up and the reassignment stalls. If you forget `--verify` after completion, the throttle stays in place and limits normal replication for those replicas.
- **Graceful (controlled) shutdown**: a clean stop (1) syncs logs to disk, so there is no log recovery or tail checksum validation on restart, and (2) moves leadership to other replicas before stopping (`controlled.shutdown.enable=true`), so each partition is unavailable for only a few ms.
  - When to use: every planned restart. Never hard-kill a broker for maintenance.
  - How: controlled shutdown succeeds only if every hosted partition has RF > 1 **and** at least one other live replica.
- **Preferred-replica leadership balancing**: the first replica in the replica list (e.g. 1 in `1,5,9`) is the preferred leader. A restarted broker comes back as a follower for all its partitions. `auto.leader.rebalance.enable=true` (the default) moves leadership back to preferred replicas. If you set it to false, run `kafka-leader-election.sh --election-type preferred --all-topic-partitions` yourself.
- **Rack-aware placement**: set `broker.rack=<id>` (works for racks or EC2 AZs) and each partition spreads across `min(#racks, RF)` racks. Leader count per broker stays constant. Racks with fewer brokers end up holding more replicas, so keep the same number of brokers per rack.
- **Cordon → drain → remove (decommissioning)**: cordon with the `cordoned.log.dirs` broker config, reassign everything off, shut down, then unregister the broker or remove the directory from config.

## Key Concepts
- **Replication factor**: number of servers that hold each message. With RF=3 you can lose 2 servers and still read the data. Use RF 2–3 so you can bounce machines without disrupting consumers.
- **Partition count**: the shard count of a topic. A partition must fit on one server, and the partition count caps consumer parallelism and how many servers can share the load.
- **Topic name limit**: 249 characters, because each partition folder is `<topic>-<partition>`, folder names are limited to 255 characters, and partition IDs are assumed to stay below 100,000 (5 digits).
- **Preferred replica**: the first broker in a partition's replica list. Leadership rebalancing moves leadership back to it.
- **Cordoned log dirs (`cordoned.log.dirs`)**: dynamic broker config that marks log directories (`"*"` = all) as being drained. It is the first step of decommissioning.
- **Lag**: `LOG-END-OFFSET − CURRENT-OFFSET` per partition, as shown by `kafka-consumer-groups.sh --describe`.
- **Share group start offset**: the earliest offset of in-flight records still being considered for delivery. Some records after it may already be fully delivered.
- **Throttled replicas lists**: per-topic `leader.replication.throttled.replicas` (all replicas that existed before the move) and `follower.replication.throttled.replicas` (move destinations only), written as `partition:broker`.
- **Quota entities**: `(user, client-id)`, `user` or `client-id`. Each can have a named override or an `--entity-default`. Clients get unlimited quota by default.

## Mental Models
- Think of adding a broker as adding **capacity that stays idle**. Until you reassign partitions or create topics, the new broker does nothing.
- Think of partition count as **append-only and changing key placement**. You can grow it but never shrink it, and growing it changes `hash(key) % N`, so existing keys move to new partitions.
- Use the **"execute with throttle, verify twice"** rule. `--verify` both checks the move and cleans up after it.
- Use `kafka-configs.sh` for anything dynamic: topic overrides, broker throttles, cordoning, quotas. Use `kafka-topics.sh` only for create, delete, partition count and describe.

## Anti-patterns
- **Increasing partitions on keyed topics without planning**: same-key messages land on different partitions, which breaks per-key ordering. Kafka does not redistribute existing data. Consumers with `auto.offset.reset=latest` can also miss records written to new partitions before they discover them (discovery waits on `metadata.max.age.ms`).
- **Adding partitions to internal topics** (`__consumer_offsets`, `__transaction_state`, `__share_group_state`, `__cluster_metadata`): this breaks coordinator mapping and can corrupt state.
- **Leaving throttles on after reassignment**: the throttle keeps limiting normal replication for those replicas.
- **Setting the throttle below the produce rate**: if `max(BytesInPerSec) > throttle`, the move never finishes.
- **Resetting offsets while consumers are running**: stop the group first. Deleting a consumer or share group also fails while it has active members.
- **Uneven brokers per rack**: racks with fewer brokers get more replicas, so they use more disk and spend more on replication.
- **Running `--execute` without saving the current assignment**: you lose your rollback plan.

## Code Examples
```bash
# Create, grow and delete topics; change topic overrides
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my_topic_name \
  --partitions 20 --replication-factor 3 --config x=y
bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic my_topic_name --partitions 40
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my_topic_name \
  --alter --add-config x=y          # or --delete-config x
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my_topic_name
```
- **What it demonstrates**: topic lifecycle. Partition count and topic overrides are set with different tools, and you can only increase partitions.

```bash
# Reset a stopped consumer group (dry-run by default; --execute applies; --export writes CSV)
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets \
  --group my-group --topic topic1 --to-datetime 2026-01-01T00:00:00.000 --execute
# Share groups: --dry-run | --execute; only --to-datetime / --to-earliest / --to-latest
bin/kafka-share-groups.sh --bootstrap-server localhost:9092 --reset-offsets \
  --group my-share-group --topic topic1 --to-latest --execute
```
- **What it demonstrates**: consumer-group resets (KIP-122) need a scope (`--all-topics` or `--topic`, unless you use `--from-file`) and work on one group at a time. Scenarios: `--to-datetime`, `--to-earliest`, `--to-latest`, `--shift-by ±n`, `--from-file`, `--to-current`, `--by-duration PnDTnHnMnS`, `--to-offset`. Out-of-range targets are clamped to the nearest available offset. Share-group resets accept fewer scenarios.

```bash
# Quotas: named override, then default for any client-id of user1
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' \
  --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' \
  --entity-type users --entity-name user1 --entity-type clients --entity-default
```
- **What it demonstrates**: quotas are dynamic configs on `users` and `clients` entities. Use `--entity-default` in place of `--entity-name` to set defaults. `--describe` without a name lists every entity of that type.

## Reference Tables

| Config | Default / scope | Meaning | When to change |
|---|---|---|---|
| `controlled.shutdown.enable` | `true` (broker) | Move leadership off the broker before it stops | Leave it on |
| `auto.leader.rebalance.enable` | `true` (broker) | Move leadership back to preferred replicas automatically | Turn off only if you run `kafka-leader-election.sh` yourself |
| `broker.rack` | unset (broker) | Rack or AZ ID used for rack-aware replica placement | Multi-rack or multi-AZ deployments |
| `cordoned.log.dirs` | unset (dynamic broker) | `"*"` or a list of paths to drain | First step of decommissioning |
| `leader.replication.throttled.rate` / `follower.replication.throttled.rate` | dynamic broker, B/s | Inter-broker reassignment bandwidth cap | Set by `--throttle` |
| `replica.alter.log.dirs.io.max.bytes.per.second` | dynamic broker, B/s | Disk-to-disk move cap | Set by `--replica-alter-log-dirs-throttle` |
| `leader/follower.replication.throttled.replicas` | per topic | Which `partition:broker` replicas are throttled | Set automatically. Edit by hand only for unusual cases |
| `producer_byte_rate`, `consumer_byte_rate`, `request_percentage` | unlimited (users/clients) | Client quotas | Multi-tenant clusters |

| Tool | Main uses |
|---|---|
| `kafka-groups.sh --list` | List all groups with TYPE (Consumer/Share/Streams) and PROTOCOL |
| `kafka-consumer-groups.sh` | `--list`, `--describe` (`--offsets` default, `--members [--verbose]`, `--state`), `--delete`, `--reset-offsets`. With the `consumer` protocol, describe needs DESCRIBE on all subscribed topics |
| `kafka-share-groups.sh` | `--describe` (START-OFFSET, LAG, `--members`, `--state`), `--reset-offsets`, `--delete-offsets --topic`, `--delete` |
| `kafka-cluster.sh unregister --id N` | Remove a decommissioned broker from the cluster |

## Worked Example
**Expand the cluster to brokers 5,6 and move topics foo1 and foo2 onto them, with a throttle.**
1. Start brokers 5 and 6 with unique IDs. They hold no partitions yet.
2. Write `topics-to-move.json` (`{"topics":[{"topic":"foo1"},{"topic":"foo2"}],"version":1}`).
3. Run `kafka-reassign-partitions.sh --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate`. It prints the current assignment (e.g. foo1-0 on `[2,1]`) and a proposal (foo1-0 on `[6,5]`, ...). Save the current assignment as `rollback.json` and the proposal as `expand-cluster-reassignment.json`.
4. Run `--execute --reassignment-json-file expand-cluster-reassignment.json --throttle 50000000` (50 MB/s). The output confirms "The inter-broker throttle limit was set to 50000000 B/s". Leader throttles go on the old replicas and follower throttles on brokers 5 and 6.
5. Watch FetcherLagMetrics ConsumerLag. If it stops falling, or BytesInPerSec is above 50 MB/s, re-run with `--additional --execute ... --throttle 700000000`.
6. Re-run `--verify` with the same JSON until every partition shows completed. The final verify prints "Clearing broker-level throttles..." and "Clearing topic-level throttles...". foo1 and foo2 now exist **only** on brokers 5 and 6.

## Key Takeaways
1. Save the "Current partition replica assignment" output before every `--execute`. It is your only rollback.
2. Always throttle reassignments, keep the throttle above peak `BytesInPerSec`, and run `--verify` after completion to clear it.
3. Plan partition counts up front for keyed topics. Growing them remaps keys, can lose data for `latest` consumers, and partitions can never be removed.
4. Decommission in order: cordon (`cordoned.log.dirs`), reassign away, shut down, then `kafka-cluster.sh unregister` (broker) or uncordon via `--bootstrap-controller` and edit `log.dirs` (log dir).
5. Keep `controlled.shutdown.enable` and `auto.leader.rebalance.enable` on, and use RF ≥ 2 so rolling restarts are close to invisible to clients.
6. Use `broker.rack` with the same number of brokers per rack so you survive the loss of a whole rack or AZ.
7. Stop group members before resetting offsets or deleting a group. Preview with the default dry-run (share groups: `--dry-run`) before `--execute`.

## Connects To
- **ch04**: dynamic broker/topic configs that `kafka-configs.sh` changes (throttles, cordoning, overrides).
- **ch07**: ISR and replication semantics behind reassignment, and the quota model behind `producer_byte_rate`.
- **ch06 / ch17**: share groups and the `consumer` rebalance protocol (KIP-848) managed by the group tools.
- **ch11**: mirroring and geo-replication across clusters (MirrorMaker 2).
- **ch12**: quotas and per-tenant isolation.
- **ch14**: `BytesInPerSec`, FetcherLagMetrics and consumer-lag monitoring.
- **ch15**: KRaft controllers (`--bootstrap-controller`, `kafka-cluster.sh unregister`, `__cluster_metadata`).
