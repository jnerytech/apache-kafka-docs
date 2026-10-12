# Chapter 11: Datacenters and Geo-Replication (MirrorMaker 2)

## Core Idea
Run one independent Kafka cluster per datacenter, keep applications talking only to their local cluster, and move data between clusters with MirrorMaker 2 (MM2): directional `{source}->{target}` replication flows on Kafka Connect. Place MM2 next to the cluster it produces to ("consume from remote, produce to local"). The source material is fairly short and centers on configuration. It does not cover failover runbooks or offset translation in depth.

## Frameworks Introduced
- **Local-cluster-per-DC topology**: each datacenter has its own cluster, local apps use only that cluster, and inter-DC traffic goes only through mirroring.
  - When to use: any pipeline that spans datacenters or regions.
  - How: (1) deploy a cluster in each DC; (2) point every app instance at the cluster in its own DC; (3) mirror between clusters with MM2; (4) for consumers that need a global view, build an *aggregate* cluster that receives mirrored data from the local clusters in all DCs and serve those reads from it.
  - Why it works / failure mode: each DC keeps running when the WAN link drops. Mirroring falls behind and catches up once the link comes back. The alternative is a *single cluster stretched across a high-latency link*. That adds WAN latency to every replicated write (acks wait on cross-DC followers), and the cluster cannot stay available in every location during a network partition.

- **Replication flows (`{source}->{target}`)**: MM2 consumes from a source cluster and produces to a target cluster. Each direction is its own flow and is configured separately.
  - When to use: to express any topology. Active/Passive `A->B`; Active/Active `A->B, B->A`; Aggregation `A->K, B->K, C->K`; Fan-out `K->A, K->B, K->C`; Forwarding `A->B, B->C, C->D`.
  - How: (1) declare aliases in `clusters = a, b`; (2) set `{alias}.bootstrap.servers` for each alias; (3) enable each direction you need with `a->b.enabled = true` (the default is `false`; bidirectional replication needs two flows); (4) narrow the scope with top-level `topics`/`groups` defaults, and override them per flow as `a->b.topics = ...`.
  - Why it works / failure mode: a flow replicates topic data, topic configs, consumer groups and their offsets, and ACLs. It keeps the partitioning and picks up new topics and partitions automatically. The most common failure is that nothing replicates because no flow was enabled.

- **Remote-topic naming via ReplicationPolicy**: `DefaultReplicationPolicy` renames a replicated topic to `{source}{separator}{topic}`, for example `us-west.foo-topic`. Records from different origins therefore never land in the same partition.
  - When to use: always active. Customize it only if downstream naming requires it.
  - How: change the separator with `replication.policy.separator` (default `.`). For full control, implement `ReplicationPolicy` and set `replication.policy.class`.
  - Why it matters: the prefix is how MM2 avoids replication loops in Active/Active setups. When both flows are defined in the same config file, you need no extra `topics.exclude`.

- **Consume from remote, produce to local**: run each MM2 process in the target's datacenter and pass `--clusters <local aliases>`.
  - When to use: whenever source and target are in different locations.
  - How: start the process with `bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters secondary`. This tells MM2 which clusters are nearby, so it produces data and writes configuration only to those clusters.
  - Rationale: producers handle high latency and unreliable links worse than consumers do. Producing across the WAN causes "producer lag" and extra data-transfer cost. `--clusters` is not required, but leaving it out costs throughput and money.

- **Two-phase exactly-once enablement (since 3.5.0, dedicated mode)**: per target, set `{target}.exactly.once.source.support`.
  - How: for a new MM2 cluster, set `enabled`. For an existing one, set `preparing` on all nodes and do a rolling restart, then set `enabled` on all nodes and do a second rolling restart. In both cases, also set `dedicated.mode.enable.internal.rest = true` plus `listeners` (KIP-710 intra-cluster REST), secure that REST endpoint, and set `{source}.consumer.isolation.level = read_committed` so records from aborted transactions are filtered out.

## Key Concepts
- **MirrorMaker 2 (MM2)**: a streaming tool for replication *between* clusters, built on Kafka Connect source connectors (KIP-382). It is separate from intra-cluster partition replication.
- **Cluster alias**: a logical name declared in `clusters`, used as a prefix for every per-cluster setting (`us-west.bootstrap.servers`) and in flow names.
- **Remote topic**: the copy of a source topic in the target cluster, named `{source}.{topic}` by default.
- **Aggregate cluster**: a cluster that receives mirrored data from every DC's local cluster and serves global-view readers.
- **MirrorSourceConnector**: the connector that replicates records. It emits per-partition metrics such as `replication-latency-ms` and `record-age-ms`.
- **MirrorCheckpointConnector**: the connector that replicates consumer-group offsets. It emits `checkpoint-latency-ms`.
- **Heartbeats**: per-flow liveness records, which you can turn off with `{flow}.emit.heartbeats = false`.
- **Shared config via target cluster**: MM2 processes that write to the same target elect a leader and share configuration through that target cluster.
- **Bandwidth-delay product**: the basis for sizing `socket.send.buffer.bytes`/`socket.receive.buffer.bytes` on producers, consumers and brokers so batches fill high-latency links.

## Mental Models
- Think of a flow as a **one-way pipe**: `A->B` never implies `B->A`, and a pipe stays closed until `.enabled = true`.
- Think of MM2 configuration as **layers**: Connect/worker defaults, then top-level MM2 defaults (`topics`, `groups`), then per-cluster (`us-west.*`), then per-client (`us-west.consumer.*`, `us-east.producer.*`, `{alias}.admin.*`), then per-flow (`us-west->us-east.*`), with the most specific layer winning.
- Use **more MM2 processes** to scale throughput. Processes configured for the same clusters find each other and balance work the way a Connect cluster does. Set `tasks.max` ≥ 2 (preferably higher) so work can actually spread.
- Think of the **cluster-name prefix** as both provenance and loop breaker: the topic name records where data came from, and MM2 uses it to avoid re-replicating it.

## Anti-patterns
- **One cluster stretched across a WAN**: every write pays the cross-DC replication latency, and a link failure takes the cluster down in some locations.
- **Divergent configs for the same target**: process 1 has `A->B.topics = foo` and process 2 has `A->B.topics = bar`. They race through cluster B, and only the elected leader's setting wins, so you get `foo` *or* `bar`, never both. Keep one shared config file, or generate configs with automation.
- **Running MM2 in the source DC (produce over the WAN)**: causes producer lag and data-transfer cost. Co-locate MM2 with the target cluster.
- **Leaving `tasks.max` at its default of 1**: the work cannot spread across MM2 processes.
- **Testing group replication with `kafka-console-consumer.sh`**: its `console-consumer-.*` groups are excluded by default, so it looks like groups are not replicating. If you change `groups.exclude` for the test, revert it afterwards.
- **Exactly-once without `read_committed`, or with an unsecured internal REST**: aborted transactional records leak into the target, and the inter-node REST port stays open.
- **Flipping `exactly.once.source.support=enabled` directly on a running MM2 cluster**: you must go through `preparing` first.
- **Expecting hot reload**: configuration changes take effect only after you restart the MM2 processes.

## Code Examples
```properties
# connect-mirror-maker.properties — Active/Active with client overrides
clusters = us-west, us-east
us-west.bootstrap.servers = broker1-west:9092,broker2-west:9092
us-east.bootstrap.servers = broker3-east:9092,broker4-east:9092
us-west->us-east.enabled = true
us-east->us-west.enabled = true          # two flows = bidirectional; prefixes prevent loops
tasks.max = 5                            # Connect setting, no prefix needed
us-west.consumer.isolation.level = read_committed
us-east.producer.compression.type = gzip
us-west->us-east.topics = foo.*          # per-flow override of top-level `topics = .*`
```
- **What it demonstrates**: aliases, bidirectional flows, unprefixed Connect settings, the `{alias}.consumer.`/`{alias}.producer.` client override formats, and per-flow overrides.

```properties
# Exactly-once for writes into us-east (dedicated MM2 >= 3.5.0)
us-east.exactly.once.source.support = enabled   # existing cluster: 'preparing' first, then 'enabled'
dedicated.mode.enable.internal.rest = true      # KIP-710
listeners = http://localhost:8080               # secure this in production
us-west.consumer.isolation.level = read_committed
```
- **What it demonstrates**: the complete set of settings EOS needs: the per-target flag, internal REST between MM2 nodes, and filtering of aborted transactions.

```bash
bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters secondary   # start, local target only
kill <MirrorMaker pid>                                                             # graceful stop (SIGTERM)
```
- **What it demonstrates**: the start/stop lifecycle. `--clusters` takes a space-separated list of aliases that must already be defined in the file.

## Reference Tables

| Setting | Default | Meaning | When to change |
|---|---|---|---|
| `clusters` | — (required) | Comma-separated cluster aliases | Always set |
| `{alias}.bootstrap.servers` | — (required) | Brokers for that alias | Always set |
| `{src}->{tgt}.enabled` | `false` | Turns a flow on | For every direction you want |
| `topics` | `.*` | Topics to replicate (list or regex) | Limit scope, globally or per flow |
| `topics.exclude` | `.*[\-\.]internal, .*\.replica, __.*` | Removed after `topics` matches | Excluding extra topics |
| `groups` | `.*` | Consumer groups to replicate | Limit offset sync |
| `groups.exclude` | `console-consumer-.*, connect-.*, __.*` | Removed after `groups` matches | CLI testing (then revert) |
| `replication.policy.separator` | `.` | Separator in `{source}.{topic}` | Downstream naming rules |
| `replication.policy.class` | `DefaultReplicationPolicy` | Naming strategy for remote topics | Fully custom naming |
| `tasks.max` | 1 (Connect) | Parallel tasks | Set ≥ 2, sized to partition count and hardware |
| `{flow}.emit.heartbeats` | `true` | Heartbeat emission | Turn off per flow if not wanted |
| `{tgt}.exactly.once.source.support` | disabled | EOS writes to that target | `preparing` → `enabled` on an existing cluster |
| `dedicated.mode.enable.internal.rest` | `false` | Inter-node REST (KIP-710) | Required for EOS |
| `socket.send/receive.buffer.bytes` | — | TCP buffers | High-latency WAN links (size to the bandwidth-delay product) |

| Client override format | Applies to |
|---|---|
| `{source}.consumer.<cfg>` | Consumer reading from the source |
| `{target}.producer.<cfg>` | Producer writing to the target |
| `{alias}.admin.<cfg>` | Admin client for that cluster |
| `{alias}.<connect cfg>` | Worker-level Connect setting for that cluster (e.g. `offset.storage.topic`) |
| `{alias}.security.protocol`, `{alias}.ssl.*` | TLS to that cluster (same settings as Connect) |

| MM2 metric (group `kafka.connect.mirror`) | MBean type | Watch for |
|---|---|---|
| `replication-latency-ms` (min/max/avg) | MirrorSourceConnector | End-to-end source→target delay |
| `record-age-ms` (min/max/avg) | MirrorSourceConnector | How stale records are when replicated |
| `record-rate`, `byte-rate`, `record-count`, `byte-count` | MirrorSourceConnector | Throughput and flow liveness |
| `checkpoint-latency-ms` (min/max/avg) | MirrorCheckpointConnector | Offset-sync delay |

Metrics are tagged with `target`, `topic` (the remote name, e.g. `primary.topic1`) and `partition`; checkpoint metrics also carry `source` and `group`. Connect metrics such as `source-record-poll-rate` still apply. Timestamps do not distinguish created-at from log-append time.

## Worked Example
**Three DCs, two clusters each, Active/Active inside each DC plus cross-DC replication (XDCR).**
1. One shared `connect-mirror-maker.properties`: `clusters = west-1, west-2, east-1, east-2, north-1, north-2`, with a `bootstrap.servers` entry for each alias.
2. Active/Active inside each DC: `west-1->west-2.enabled = true` and `west-2->west-1.enabled = true`, and the same pair for east and north.
3. XDCR through one gateway cluster per DC: a full mesh among `west-1`, `east-1` and `north-1` (six flows, e.g. `west-1->east-1`, `east-1->west-1`, `north-1->west-1`, ...).
4. Start MM2 in each DC, scoped to its local clusters: West runs `--clusters west-1 west-2`, East runs `--clusters east-1 east-2`, North runs `--clusters north-1 north-2`.
5. Result: a record produced to any cluster reaches its DC partner and the other DCs. Each MM2 process produces only to nearby clusters, so there is no cross-DC producer lag and no extra transfer cost. The cluster prefixes on remote topics prevent loops.
6. To add throughput, start more MM2 processes in a DC with the same file. They join the existing distributed group and rebalance tasks. To change a config, edit the shared file and restart.

## Key Takeaways
1. Give each datacenter its own cluster and connect DCs with MM2. Do not stretch one cluster over a high-latency link.
2. Flows are directional and off by default. Enable every `src->tgt` you need, and define both directions in one file for Active/Active.
3. Run MM2 in the target's DC with `--clusters <local aliases>`, because producers suffer more from the WAN than consumers.
4. Use one consistent config for all MM2 processes that write to the same target. Divergent configs race, and only the leader's version wins.
5. Set `tasks.max` ≥ 2, and scale by adding MM2 processes.
6. For EOS: set `exactly.once.source.support` (`preparing` before `enabled` on an existing cluster), enable `dedicated.mode.enable.internal.rest` and secure it, and use `read_committed` on the source consumer.
7. Monitor `replication-latency-ms`, `record-age-ms` and `checkpoint-latency-ms` for each flow. Restart MM2 after any config change.

## Connects To
- **ch21**: MM2 is a set of Kafka Connect connectors. Worker settings, the REST server, its security, and exactly-once source support come from there.
- **ch07**: intra-cluster replication (ISR, acks), compared with MM2's inter-cluster replication. Explains why a stretched cluster has to pay WAN latency on writes.
- **ch06**: transactions and `isolation.level=read_committed`, which MM2 exactly-once relies on.
- **ch13**: OS/network tuning, including socket buffer sizing for high-latency links.
- **ch14**: Connect and MM2 JMX metrics, and building alerts on replication lag.
- **ch18 / ch19**: SSL and SASL settings applied per alias (`{alias}.security.protocol`, `{alias}.ssl.*`).
- **ch20**: MM2 also replicates ACLs.
- **ch10**: basic operations around MM2, such as topic management and checking consumer-group offsets.
