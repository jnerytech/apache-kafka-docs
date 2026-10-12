# Chapter 02: Quickstart, Docker and Compatibility

## Core Idea
A single-node KRaft broker needs three steps (generate a cluster ID, format storage with `--standalone`, start the server), and from Kafka 4.0 onward compatibility is shaped by hard cuts: Java 17+ for broker and Connect, no ZooKeeper (`--bootstrap-server` only), no client protocols older than 0.10 (KIP-896), and no static-to-dynamic KRaft voter upgrade. The source material is thin (a tutorial plus three matrices), so this chapter is shorter than the target.

## Frameworks Introduced
- **KRaft single-node bootstrap (generate ID → format → start)**: `kafka-storage.sh random-uuid` → `kafka-storage.sh format --standalone -t <id> -c config/server.properties` → `kafka-server-start.sh config/server.properties`.
  - When to use: any local or dev broker started from the tarball (Java 17+ required).
  - How: (1) put the cluster ID in a shell variable so formatting and later nodes share it; (2) format the log directories once per node, before the first start. `--standalone` bootstraps a single-voter controller quorum (combined broker+controller); (3) start the server. Data lands in `/tmp/kraft-combined-logs` (and `/tmp/kafka-logs`). Delete those to reset.
  - Why it works / failure mode: KRaft keeps cluster metadata in a local log that has to be stamped with a cluster ID before the node will start. If you skip `format`, startup fails. If you re-format with a different ID over existing data, the node no longer matches its own metadata.
- **Three ways to run a broker**: tarball scripts, the JVM image `apache/kafka:<ver>` (available since 3.7.0), or the GraalVM native image `apache/kafka-native:<ver>` (since 3.8.0).
  - When to use: use the tarball to learn the scripts and configs. Use `apache/kafka` for containerized dev, CI and production-like setups. Use `apache/kafka-native` only for fast-starting, lightweight local dev and tests. It is experimental and not meant for production.
  - How: `docker run -p 9092:9092 apache/kafka:4.3.1` starts a broker with default configs on 9092. Pin a version tag rather than `:latest`.
- **Compatibility gates for Kafka 4.x upgrades**: before upgrading, check four things: JDK version per module, KRaft cluster version and voter mode, client version band, and tooling flags.
  - When to use: planning any move to 4.x, or connecting old clients to a 4.x cluster.
  - How: (1) server and Connect need Java 17 or 23, while clients and Streams still run on 11. Java 8 is gone. (2) The cluster must already be KRaft: 3.3–3.8 with static voters, or 3.9+ for dynamic voters. (3) Clients 0.x–2.0 are incompatible, 2.1–2.8 are partially compatible, and 3.x are fully compatible. (4) Scripts that pass `--zookeeper` have to switch to `--bootstrap-server`.
  - Why it works / failure mode: each gate is a removal, not a deprecation, so nothing degrades gracefully. A pre-0.10 protocol client or a Java 11 broker simply fails to work.

## Key Concepts
- **Cluster ID**: a random UUID from `kafka-storage.sh random-uuid`. Every node's storage is formatted with it.
- **`--standalone` format**: formats a node as the sole controller voter of a new KRaft quorum. This is the quickstart and dev path.
- **Static vs dynamic voters**: static voters come from a fixed `controller.quorum.voters` list. Dynamic voters (3.9+) let the quorum membership change. A cluster cannot be upgraded from static to dynamic (KAFKA-16538).
- **Event / record / message**: synonyms for the unit Kafka stores. Events are organized into topics (roughly "folder = topic, files = events").
- **Console producer/consumer**: CLI clients. Each stdin line becomes one event, and `--from-beginning` replays a topic from offset 0.
- **Connect standalone mode**: a single local process running connectors. Its first argument is the worker config, and every argument after that is a connector config.
- **`plugin.path`**: the Connect worker setting that lists connector jars and directories. Use absolute paths in production.
- **KIP-896**: removed the client protocol versions older than 0.10 in Kafka 4.0, which makes 0.x–2.0 clients incompatible.
- **`apache/kafka-native`**: the GraalVM native-image broker. It is experimental and for dev/test only.

## Mental Models
- Think of the quickstart as the whole platform in miniature: broker (storage), producer and consumer (clients), Connect (integration), and Streams (processing). Each later chapter goes deeper on one of these pieces.
- Use the client version band as the first triage question when an app breaks against a 4.x broker: below 2.1 it needs an upgrade, 2.1–2.8 needs the 4.0 upgrade-notes check, and 3.x should just work.
- Think of the JDK split as "server side 17+, client side 11+". You can upgrade brokers without forcing every application team onto Java 17 right away.
- Treat KRaft voter mode as a one-way choice made when the cluster is created. If you want dynamic quorum membership, create the cluster on 3.9+ with dynamic voters from the start.

## Anti-patterns
- **Using `--zookeeper` in admin scripts on 4.x**: the option has been removed. Use `--bootstrap-server host:port`.
- **Running `apache/kafka-native` in production**: it is explicitly experimental and limited to dev/test.
- **Relative `plugin.path` in production Connect workers**: it only works when you launch from the install directory. Use absolute paths.
- **Expecting to convert a static-voter KRaft cluster to dynamic voters**: this is not supported (KAFKA-16538). You need a new cluster.
- **Running a 4.x broker or Connect worker on Java 11 (or Java 8 anywhere)**: it is unsupported. Java 8 was removed in 4.0.
- **Pulling `:latest` images for reproducible environments**: the version drifts without notice. Pin `apache/kafka:4.3.1`.

## Code Examples
```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```
- **What it demonstrates**: the three-step KRaft bootstrap for one combined broker+controller node. No ZooKeeper is involved.

```bash
bin/kafka-topics.sh --create   --topic quickstart-events --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
# Topic: quickstart-events TopicId: ... PartitionCount: 1 ReplicationFactor: 1
# Topic: quickstart-events Partition: 0 Leader: 0 Replicas: 0 Isr: 0
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```
- **What it demonstrates**: topic lifecycle and a durable re-read. Running the consumer again with `--from-beginning` returns all events again, because a consumer read does not delete anything. Run a tool with no arguments to see its usage.

```java
KStream<String, String> textLines = builder.stream("quickstart-events");
KTable<String, Long> wordCounts = textLines
    .flatMapValues(line -> Arrays.asList(line.toLowerCase().split(" ")))
    .groupBy((keyIgnored, word) -> word)
    .count();
wordCounts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
```
- **What it demonstrates**: the canonical Streams WordCount. The stream becomes a re-keyed grouping, then a `KTable` count, and the table is emitted back to a topic as a changelog stream with explicit serdes.

## Reference Tables

**JDK compatibility (Kafka 4.0+)**

| Module | Java 11 | Java 17 | Java 23 |
|---|---|---|---|
| Clients | yes | yes | yes |
| Streams | yes | yes | yes |
| Connect | no | yes | yes |
| Server | no | yes | yes |

Java 8 was removed in 4.0.

**KRaft cluster version vs 4.0 server**

| Existing KRaft cluster | 4.0 dynamic voter | 4.0 static voter |
|---|---|---|
| before 3.2.x | no | no |
| 3.3.x – 3.8.x | no | yes |
| 3.9.x, 4.0.x | yes | yes |

**Client/Streams/Connect version vs 4.0 broker**

| Client version | Client | Streams | Connect | Notes |
|---|---|---|---|---|
| 0.x, 1.x, 2.0 | not compatible | not compatible | not compatible | pre-0.10 protocols removed (KIP-896) |
| 2.1 – 2.8 | partially | limited | limited | see 4.0 upgrade notes for consumer, producer, admin, Streams and Connect |
| 3.x | full | full | full | — |

**Docker images**

| Image | Since | Status | Use |
|---|---|---|---|
| `apache/kafka:<ver>` | 3.7.0 | supported | containerized broker, default configs, port 9092 |
| `apache/kafka-native:<ver>` | 3.8.0 | experimental | local dev/test only, fast startup |

## Worked Example
**File → Kafka → file pipeline with Connect standalone (on a running quickstart broker):**
1. Register the bundled file connectors: `echo "plugin.path=libs/connect-file-4.3.1.jar" >> config/connect-standalone.properties` (the relative path only works when run from the install directory).
2. Create seed data: `echo -e "foo\nbar" > test.txt`.
3. Launch: `bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties`. The first file is the worker config (bootstrap servers, converters). Each of the other files defines one connector (unique `name`, `connector.class`, and its settings).
4. The source connector tails `test.txt` and produces each line to topic `connect-test`. The sink connector consumes `connect-test` and appends to `test.sink.txt`. Check with `more test.sink.txt`, which shows `foo` and `bar`.
5. Inspect the topic: `kafka-console-consumer.sh --topic connect-test --from-beginning` shows JSON envelopes such as `{"schema":{"type":"string","optional":false},"payload":"foo"}`, because the worker's JSON converter embeds schemas.
6. Run `echo "Another line" >> test.txt`. The line flows through to the consumer and the sink file, which shows the pipeline is continuous rather than a batch job.
7. Tear down: Ctrl-C the clients and the broker, then `rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs`.

## Key Takeaways
1. Bootstrap KRaft nodes with `random-uuid` → `format` → `start`. Format once per node, and reuse the same cluster ID across nodes.
2. Before upgrading to 4.x, confirm that brokers and Connect run on Java 17+ and that the cluster is already KRaft on 3.3+ (3.9+ if you want dynamic voters).
3. Choose static or dynamic voters when you create the cluster. You cannot migrate from static to dynamic later.
4. Inventory client versions. Upgrade anything at 2.0 or older first, and review the 4.0 upgrade notes for 2.1–2.8 clients.
5. Replace every `--zookeeper` flag in scripts and runbooks with `--bootstrap-server`.
6. Use pinned `apache/kafka` images for containers, and keep `apache/kafka-native` out of production.
7. Use absolute `plugin.path` entries for production Connect workers.

## Connects To
- **ch01**: the concepts (events, topics, partitions) the quickstart exercises.
- **ch03**: the producer, consumer, admin, Connect and Streams APIs behind the CLI tools.
- **ch10**: `kafka-topics.sh` and other day-to-day operations in depth.
- **ch15**: KRaft internals, `kafka-storage.sh`, static vs dynamic quorums, controller voters.
- **ch13**: Java version and OS guidance for production brokers.
- **ch21**: Connect workers, `plugin.path`, standalone vs distributed mode.
- **ch23**: the full Streams quickstart and WordCount tutorial.
- **ch30**: the 4.0 upgrade notes linked from the 2.1–2.8 compatibility rows.
