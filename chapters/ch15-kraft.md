# Chapter 15: KRaft (and Differences from ZooKeeper Mode)

## Core Idea
In Kafka 4.x, cluster metadata lives in a Raft-replicated `__cluster_metadata` partition owned by a dedicated controller quorum. Run it as isolated controllers (3 or 5, sized `2N+1`), format every node explicitly with a shared cluster ID, and prefer a KIP-853 **dynamic** quorum (`kraft.version>=1`, `controller.quorum.bootstrap.servers`) so you can add and remove controllers online. Many ZooKeeper-era configs, metrics and behaviours are gone or have moved to the controller.

## Frameworks Introduced
- **Process roles (`process.roles`)**: each server is `broker`, `controller`, or `broker,controller` (a "combined" server).
  - When to use: use dedicated `controller` / `broker` roles in production. Combined mode is only for dev or small setups.
  - How: set `process.roles`, a unique `node.id`, `controller.listener.names` (e.g. `CONTROLLER`), a `listeners` entry for that name on controllers, and `controller.quorum.bootstrap.servers` on **every** broker and controller.
  - Failure mode: in combined mode the controller isn't isolated, so you can't roll or scale controllers separately from brokers.
- **Quorum sizing (2N+1)**: a majority of controllers must be alive. To survive N concurrent controller failures you need `2N+1` controllers.
  - When to use: when deciding how many controllers to deploy.
  - How: 3 controllers tolerate 1 failure and 5 tolerate 2. Pick based on cost vs the number of concurrent failures you must absorb without losing availability. Each controller is either the active controller or a hot standby.
  - Why it works: Raft commits only with majority acknowledgement. Losing the majority stops metadata writes (topic creation, leader election, ISR changes) until the quorum recovers.
- **Static vs dynamic quorum (KIP-853)**:
  - Static: `controller.quorum.voters` lists `id@host:port` for every controller, in every node's config. Membership is fixed. Shows up as `kraft.version` 0 or absent.
  - Dynamic: `controller.quorum.voters` is **unset** and `controller.quorum.bootstrap.servers` is set. That list works like a client's `bootstrap.servers`: list as many controllers as possible, but not necessarily all. Requires `kraft.version>=1` (release 4.1). Controllers can be added and removed with `kafka-metadata-quorum.sh`.
  - How it is decided: **at format time**. You get a dynamic quorum if `controller.quorum.voters` is absent AND one of `--standalone`, `--initial-controllers` or `--no-initial-controllers` is passed to `kafka-storage.sh format`.
- **Explicit formatting / provisioning**: Kafka no longer auto-formats blank dirs or auto-generates cluster IDs.
  - How: run `kafka-storage.sh random-uuid` once to get the cluster ID. Then run `kafka-storage.sh format --cluster-id <ID>` on every node, choosing one of three modes:
    - `--standalone`: first controller only, single voter.
    - `--initial-controllers`: all initial voters, with an identical value on every controller.
    - `--no-initial-controllers`: brokers and later controllers.
  - Why it works: auto-formatting could hide errors. If a majority of controllers started with empty metadata logs, a leader could be elected that is missing committed data.
- **Static → dynamic upgrade (Kafka 4.1+)**:
  1. Run `kafka-features.sh describe` and check `kraft.version`.
  2. Run `upgrade --feature kraft.version=1` (or `--release-version 4.1`).
  3. Once the finalized level is `>=1`, remove `controller.quorum.voters` from all nodes and add `controller.quorum.bootstrap.servers`.

## Key Concepts
- **Controller quorum / quorum controller**: the controllers that replicate the metadata log with Raft. One is active and the rest are hot standbys.
- **Voters vs observers**: voters (`CurrentVoters`) are the controllers that take part in elections and commits. Brokers are observers (`CurrentObservers`): they replicate the metadata log but don't vote.
- **Directory ID**: a random UUID stored in `meta.properties` in `metadata.log.dir`. It identifies a replica's log directory, and you need it (`--controller-directory-id`) to remove a controller.
- **Replica descriptor**: written as `id@host:port:directoryId`, e.g. `0@controller-0:1234:3Db5QLSqSZieL3rJBUUegA`. It is the format used by `--initial-controllers`.
- **Bootstrap checkpoint**: the file `00000000000000000000-0000000000.checkpoint` written at format time. It holds `KRaftVersionRecord` and `VotersRecord` and no cluster metadata, so it can't be used with the metadata shell.
- **`kraft.version`**: the feature level that controls the quorum type. 0 = static, 1 = dynamic (KIP-853).
- **`metadata.version`**: the cluster feature level managed with `kafka-features.sh`. It replaces `inter.broker.protocol.version`.
- **`--bootstrap-controller`**: the admin flag for talking directly to controller endpoints (port 9093 in the examples). Use `--bootstrap-server` to go through brokers.
- **Combined server**: a node with `process.roles=broker,controller`. It has a single set of log levels shared by both roles.

## Mental Models
- Think of `controller.quorum.bootstrap.servers` as `bootstrap.servers` for the metadata quorum. It's a discovery seed, not the authoritative membership list. In a dynamic quorum the membership lives in the `VotersRecord` inside the log.
- Think of brokers as Raft **observers** of the metadata log. They catch up on the log rather than receiving pushed RPCs, which is why the ZK-era controller channel metrics are gone.
- Use the controller (not the broker) as the home for anything that validates metadata changes: `CreateTopicPolicy`, `AlterConfigPolicy`, and the principal serde used when brokers forward requests.
- Treat quorum membership like a reassignment: first let the new node catch up (`describe --replication`), then change membership (`add-controller`). Remove a controller from the quorum *before* shutting it down.

## Anti-patterns
- **Combined mode in production**: you can't roll or scale controllers independently, and broker load can affect the controller.
- **An even number of controllers, or fewer than 3**: a majority must be alive, so 4 controllers still tolerate only 1 failure, the same as 3. 1 controller has no redundancy.
- **Mixing `controller.quorum.voters` with a dynamic quorum**: if `voters` is present at format time, you get a static quorum and can't add or remove controllers.
- **Different `--initial-controllers` values across controllers**: every controller sharing a cluster ID must get the identical string.
- **Shutting down a controller before running `remove-controller`**: the quorum still counts it as a voter, which shrinks your effective failure tolerance.
- **Expecting `advertised.listeners` to be dynamically updatable**: in KRaft, changing it requires a restart.
- **Leaving policy JARs only on brokers after migrating**: policies now run on the controller, so the JARs and the `create.topic.policy.class.name` / `alter.config.policy.class.name` settings must be there.
- **A custom `KafkaPrincipalBuilder` without `KafkaPrincipalSerde`**: brokers can't forward requests to the controller.
- **Using `append` to grow a config value past `Short.MAX_VALUE`**: KRaft rejects config values that large.

## Code Examples
```properties
# controller1 (dedicated controller, dynamic quorum)
process.roles=controller
node.id=1
listeners=CONTROLLER://controller1.example.com:9093
controller.listener.names=CONTROLLER
controller.quorum.bootstrap.servers=controller1.example.com:9093,controller2.example.com:9093,controller3.example.com:9093
```
- **What it demonstrates**: the minimal settings for a KRaft controller. Brokers use the same `controller.quorum.bootstrap.servers` and `controller.listener.names` with `process.roles=broker`. There is no `controller.quorum.voters`, so the quorum can be dynamic.

```bash
CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
C0="$(bin/kafka-storage.sh random-uuid)"; C1="$(bin/kafka-storage.sh random-uuid)"; C2="$(bin/kafka-storage.sh random-uuid)"
# on each controller, with the identical --initial-controllers value:
bin/kafka-storage.sh format --cluster-id ${CLUSTER_ID} \
  --initial-controllers "0@controller-0:1234:${C0},1@controller-1:1234:${C1},2@controller-2:1234:${C2}" \
  --config config/controller.properties
# on brokers / later controllers:
bin/kafka-storage.sh format --cluster-id ${CLUSTER_ID} --config config/server.properties --no-initial-controllers
```
- **What it demonstrates**: bootstrapping a quorum with several voters at once (an alternative to `--standalone` followed by `add-controller`), and how to format nodes that join an existing cluster.

```bash
# Dynamic log level on a controller: must use --bootstrap-controller, entity-type is still broker-loggers
bin/kafka-configs.sh --bootstrap-controller localhost:9093 --entity-type broker-loggers \
  --entity-name 1 --alter --add-config org.apache.kafka.raft.KafkaNetworkChannel=TRACE
# Cluster-wide dynamic config: applies to all brokers AND controllers
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default \
  --alter --add-config max.connections=10000
```
- **What it demonstrates**: dynamic configuration in KRaft. Controllers apply cluster-level defaults. Before 4.3, dynamic config updates were supported only with a static quorum.

## Reference Tables

| Config | Value / default | Meaning | Notes |
|---|---|---|---|
| `process.roles` | `broker` / `controller` / `broker,controller` | Role of the server | Use one role per server in production |
| `node.id` | required | Server identity (replaces `broker.id` generation) | Unique across brokers and controllers |
| `controller.quorum.bootstrap.servers` | `host:port,...` | Seed list for finding the quorum (dynamic) | Set on every node; list as many controllers as possible |
| `controller.quorum.voters` | `id@host:port,...` | Fixed voter set (static) | Deprecated by `kraft.version=1`; must be absent for a dynamic quorum |
| `controller.listener.names` | e.g. `CONTROLLER` | Listener(s) used for controller traffic | Replaces `control.plane.listener.name`, together with `listeners` and `listener.security.protocol.map` |
| `kraft.version` | 0 (static) / 1 (dynamic) | Quorum feature level | Upgrade with `kafka-features.sh upgrade --feature kraft.version=1` |
| Controller sizing | 5 GB RAM + 5 GB metadata disk | Typical guideline | Controllers keep all metadata in memory and on disk |

| ZooKeeper-era item (removed) | KRaft replacement |
|---|---|
| `zookeeper.*` (connect, session/connection timeouts, `zookeeper.ssl.*`, ...) | Not applicable |
| `password.encoder.*` (secret, old.secret, cipher/keyfactory algorithm, key.length, iterations) | None. Sensitive data is stored in records and **not encrypted** by Kafka |
| `control.plane.listener.name` | `controller.listener.names` + `listeners` + `listener.security.protocol.map` |
| `controlled.shutdown.max.retries`, `controlled.shutdown.retry.backoff.ms` | The controller manages shutdown and leader transfer |
| `broker.id.generation.enable`, `reserved.broker.max.id` | `node.id` |
| `inter.broker.protocol.version` | `metadata.version` via `kafka-features.sh` |
| `leader.imbalance.per.broker.percentage` | None |
| Dynamic `advertised.listeners` | Restart required |
| `ControlPlaneNetworkProcessorAvgIdlePercent`, `ControlPlaneExpiredConnectionsKilledCount` | `NetworkProcessorAvgIdlePercent`, `ExpiredConnectionsKilledCount` |
| `kafka.controller:type=KafkaController,*` / `ControllerStats,*` / `ControllerChannelManager,*`, `SessionExpireListener`, `ZooKeeperClientMetrics` | Removed (see ch14 for KRaft controller metrics) |

| Debug tool | Command | Use for |
|---|---|---|
| Quorum status | `kafka-metadata-quorum.sh --bootstrap-server ... describe --status` (or `--replication`) | LeaderId, LeaderEpoch, HighWatermark, MaxFollowerLag, voters/observers |
| Dump log | `kafka-dump-log.sh --cluster-metadata-decoder --files .../__cluster_metadata-0/<seg>.log` (or a `.checkpoint`) | Decoding metadata records and snapshots |
| Metadata shell | `kafka-metadata-shell.sh --snapshot .../<offset>-<epoch>.checkpoint`, then `ls /topics`, `cat /topics/foo/0/data` | Browsing metadata state (replicas, ISR, leader, epochs) |

## Worked Example
**Bootstrapping a dynamic 3-controller quorum and later replacing a controller**
1. Run `kafka-storage.sh random-uuid` to get `CLUSTER_ID`.
2. On controller A, run `kafka-storage.sh format --cluster-id $CLUSTER_ID --standalone --config config/controller.properties`. This writes `meta.properties` with a new `directory.id`, plus a bootstrap checkpoint whose `VotersRecord` makes A the only voter. Start A.
3. On controllers B and C, run `format ... --no-initial-controllers` and start them. They join as observers and replicate the metadata log.
4. Watch `kafka-metadata-quorum.sh --bootstrap-controller A:9093 describe --replication` until B has caught up with the leader. Then run `kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-controller A:9093 add-controller` (run with B's config). Repeat for C. Put any AdminClient auth settings in `controller.properties`.
5. Check with `describe --status`: `CurrentVoters` should list 3 entries with their directory IDs, and the brokers should appear under `CurrentObservers`. The quorum now tolerates 1 failure.
6. To retire controller C, run `kafka-metadata-quorum.sh --bootstrap-controller A:9093 remove-controller --controller-id <C id> --controller-directory-id <C dirId>` **first**, then shut C down. Add its replacement the same way as in steps 3–4.
7. If an existing cluster was formatted static (`kraft.version` 0), first run `kafka-features.sh --bootstrap-server ... upgrade --feature kraft.version=1`. Then swap `controller.quorum.voters` for `controller.quorum.bootstrap.servers` on every node before doing the steps above.

## Key Takeaways
1. Run dedicated controllers (3 or 5, i.e. `2N+1`) and keep combined mode for dev only. Budget about 5 GB RAM and 5 GB metadata disk per controller.
2. Format every node explicitly with one shared cluster ID. Use `--standalone`/`--initial-controllers` for the first voters and `--no-initial-controllers` for everything else, so you get a dynamic quorum.
3. Use `controller.quorum.bootstrap.servers` and never `controller.quorum.voters` on new clusters. Upgrade old clusters to `kraft.version=1` (Kafka 4.1+) before changing membership.
4. Changing membership is ordered: let the new controller catch up, then `add-controller`. Run `remove-controller` before shutdown.
5. Going from ZooKeeper to KRaft requires a bridge release (last one: **3.9**). Kafka 4.x has no ZooKeeper mode.
6. When migrating, move policy JARs and settings to the controllers, implement `KafkaPrincipalSerde` in custom principal builders, plan restarts for `advertised.listeners` changes, and remember that `password.encoder.*` encryption no longer exists.
7. To debug metadata, use `kafka-metadata-quorum.sh`, `kafka-dump-log.sh --cluster-metadata-decoder` and `kafka-metadata-shell.sh`. Point the metadata shell at a real snapshot, not the `0000...-0000.checkpoint` bootstrap file.

## Connects To
- **ch02**: the quickstart's KRaft format step (`random-uuid`, `format --standalone`).
- **ch04**: dynamic broker configs and the `--entity-default` cluster-level updates that controllers also apply.
- **ch07**: replication/ISR concepts; leader election and ISR changes are metadata operations committed by the quorum.
- **ch10**: basic operations (rolling restarts, controlled shutdown) now driven by the quorum controller.
- **ch14**: KRaft controller/quorum metrics that replace the removed ZK-era `kafka.controller` MBeans.
- **ch17**: ELR and newer protocols that depend on `metadata.version` feature levels.
- **ch18/ch19/ch20**: the controller listener's security protocol, SASL for `--command-config`, `KafkaPrincipalBuilder`/`KafkaPrincipalSerde` and authorizers.
- **ch30**: upgrade notes, `metadata.version` bumps, and the ZooKeeper→KRaft bridge release (3.9).
