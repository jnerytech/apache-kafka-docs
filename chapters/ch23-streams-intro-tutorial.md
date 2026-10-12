# Chapter 23: Kafka Streams — Introduction, Quickstart and Tutorial

## Core Idea
Kafka Streams is a plain client library (no separate processing cluster) in which you describe a **topology** of processor nodes with `StreamsBuilder`, pair it with a `Properties` config (at minimum `application.id` + `bootstrap.servers`), and run it with `KafkaStreams.start()`. Stateful results such as counts are a `KTable` whose updates leave as a **changelog stream**, so you write them to a compacted topic. The source is mostly an introductory walkthrough (WordCount, Pipe → LineSplit → WordCount), so this chapter is shorter than the target. Deeper mechanics live in ch24–ch29.

## Frameworks Introduced
- **Streams client = `StreamsConfig` + `Topology`**: every Kafka Streams app is `new KafkaStreams(topology, props)` and nothing more.
  - When to use: as the skeleton of any Streams app, from a pipe to a stateful aggregation.
  - How: (1) build `Properties` with `StreamsConfig.APPLICATION_ID_CONFIG` (a unique ID that sets the consumer group, internal-topic prefix and state directory) and `StreamsConfig.BOOTSTRAP_SERVERS_CONFIG`. Optionally set `DEFAULT_KEY_SERDE_CLASS_CONFIG` and `DEFAULT_VALUE_SERDE_CLASS_CONFIG`. (2) Create a `StreamsBuilder`, declare sources with `builder.stream("topic")`, chain operators, and finish with `.to("topic")`. (3) `Topology t = builder.build()`. (4) `new KafkaStreams(t, props)`, then `start()`. The app keeps running until `close()`.
  - Why it works / failure mode: the processing logic is ordinary Java/Scala packaged as an ordinary app, so you scale it by running more instances, and it uses Kafka's security and fault tolerance. Failure mode: if you never call `close()` (no shutdown hook), state stores and group membership are not released cleanly.

- **Topology inspection with `topology.describe()`**: print the graph of sub-topologies, sources, processors, sinks and state stores before you run the app.
  - When to use: while you write the app ("try and taste"), and whenever you need to know whether an operator added a repartition topic or a state store.
  - How: call `System.out.println(builder.build().describe())`. Read `-->` as children and `<--` as parents. `(stores: [])` means a stateless node. A sink to `*-repartition` followed by a new `Sub-topology` means a data shuffle. `Global Stores: none` means there are no global tables.
  - Why it works / failure mode: auto-generated node names (`KSTREAM-SOURCE-0000000000`, `KSTREAM-AGGREGATE-0000000003`…) show exactly where Kafka Streams inserted hidden key-select, filter and repartition steps. Missing these means hidden network or storage cost.

- **Stream–table duality**: a `KTable` (the current state) and its changelog `KStream` (every update) can each be rebuilt from the other. If you replay the whole changelog from the beginning, you get the table back.
  - When to use: to understand aggregation output, and to decide the cleanup policy of an output topic.
  - How: `count()`/aggregations produce a `KTable<K,V>`. Call `.toStream()` to emit each update as a record. Each later record with the same key replaces the earlier one, so create the output topic with `cleanup.policy=compact`.

- **Stateless → stateful progression (Pipe → LineSplit → WordCount)**: add operators one at a time and check the topology after each step.
  - How: `to()` (pure copy) → `flatMapValues` (stateless, one record in, N out, key unchanged) → `groupBy` + `count(Materialized.as("counts-store"))` (stateful; adds a repartition topic and a state store).

## Key Concepts
- **`application.id`**: the unique ID of the Streams app on a cluster. It separates the app from other apps and sets the names of its group and internal topics.
- **`StreamsBuilder`**: the DSL builder that turns `stream()`/operator/`to()` calls into a `Topology` of processor nodes.
- **`KStream`**: an unbounded record stream in which each record is an independent event.
- **`KTable`**: a continuously updated table in which each record for a key overwrites the previous value (an update stream).
- **Serde**: a serializer/deserializer pair (`Serdes.String()`, `Serdes.Long()`). Default serdes come from config, and per-operation serdes come from `Consumed.with(...)` and `Produced.with(...)`.
- **State store / `Materialized`**: local storage attached to a stateful processor. Naming it (`"counts-store"`) makes it queryable through interactive queries (ch28). Its innermost type is always `KeyValueStore<Bytes, byte[]>`.
- **Sub-topology**: a connected part of the topology. Parts are separated by repartition topics, and each part is scheduled independently.
- **Repartition topic**: an internal topic (`counts-store-repartition`, prefixed with the app ID) that re-keys records by the new grouping key so all records for one key reach the same task.
- **Unbounded WordCount**: the count never "finishes", so it emits its current state continuously instead of producing one final result.
- **`group.protocol=streams` (KIP-1071)**: an optional rebalance protocol that moves task assignment from the client to the broker. It shortens rebalances and removes stop-the-world rebalances (ch29).

## Mental Models
- Think of a Streams app as **a consumer + producer + local database packaged as a library**. You deploy it like any JVM service (containers, VMs, bare metal, cloud) and scale it by adding instances. There is no cluster to manage.
- Think of an aggregation's output topic as **a table's change log, not an event log**. Downstream consumers should keep the latest value per key, not add up the records.
- Use `topology.describe()` **as an explain plan**: every `groupBy`/re-key that changes the key costs one repartition topic and one sub-topology split.
- Use the default serdes for the common key/value type. **Override them per operation** whenever an operator changes the value type (String → Long after `count()`).

## Anti-patterns
- **Writing a `KTable` changelog to a non-compacted topic**: the topic grows forever with obsolete counts. Use `cleanup.policy=compact`.
- **Relying on default String serdes after `count()`**: the value type is now `Long`, and the default value serde throws at runtime. Pass `Produced.with(Serdes.String(), Serdes.Long())`.
- **Reading a Long-valued output topic with the default console deserializer**: you see garbage bytes. Set `value.deserializer=org.apache.kafka.common.serialization.LongDeserializer`.
- **Reusing the same `application.id` for different apps** (for example, copying Pipe.java to LineSplit.java without renaming it): the apps share a group and internal topics, and their state collides. Give each app its own ID (`streams-pipe`, `streams-linesplit`, `streams-wordcount`).
- **No shutdown hook**: on Ctrl-C the client is never `close()`d. Use a `CountDownLatch` plus `Runtime.addShutdownHook`.
- **Expecting a single final answer from a streaming aggregate**: on unbounded input, results are emitted continuously as updates.

## Code Examples
```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

StreamsBuilder builder = new StreamsBuilder();
builder.<String, String>stream("streams-plaintext-input")
       .flatMapValues(v -> Arrays.asList(v.toLowerCase(Locale.getDefault()).split("\\W+")))
       .groupBy((key, word) -> word)
       .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
       .toStream()
       .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
```
- **What it demonstrates**: the complete stateful DSL chain: a stateless split, a re-key with `groupBy`, a materialized and queryable count, and a changelog emitted with explicit serdes for the `Long` value.

```java
final KafkaStreams streams = new KafkaStreams(builder.build(), props);
final CountDownLatch latch = new CountDownLatch(1);
Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
    @Override public void run() { streams.close(); latch.countDown(); }
});
try { streams.start(); latch.await(); } catch (Throwable e) { System.exit(1); }
System.exit(0);
```
- **What it demonstrates**: the standard lifecycle. `start()` runs until `close()`, and the shutdown hook makes Ctrl-C trigger a clean close.

```bash
mvn archetype:generate -DarchetypeGroupId=org.apache.kafka \
  -DarchetypeArtifactId=streams-quickstart-java -DarchetypeVersion=4.3.1 \
  -DgroupId=streams.examples -DartifactId=streams-quickstart -Dversion=0.1 -Dpackage=myapps
mvn clean package && mvn exec:java -Dexec.mainClass=myapps.WordCount
```
- **What it demonstrates**: how to scaffold a project. The generated `pom.xml` already includes the Streams dependency and targets Java 11. It contains the example classes `Pipe`, `LineSplit` and `WordCount`.

## Reference Tables

| Setting | Default | Meaning | When to change |
|---|---|---|---|
| `application.id` (`APPLICATION_ID_CONFIG`) | none (required) | Unique app ID; namespaces the group and internal topics | Always set it, unique per logical app |
| `bootstrap.servers` (`BOOTSTRAP_SERVERS_CONFIG`) | none (required) | Initial host:port list | Always set it |
| `default.key.serde` / `default.value.serde` | — (set in the tutorial to `Serdes.String()`) | Serde used when an operator has none | Set it to the dominant type; override per operation with `Consumed`/`Produced` |
| `group.protocol` | classic | `streams` enables the KIP-1071 broker-side assignment | Use it for faster rebalances without stop-the-world pauses |
| Output topic `cleanup.policy` | `delete` | `compact` keeps the latest value per key | Use `compact` for any `KTable.toStream()` changelog output |

| Operator | Stateful? | Topology effect |
|---|---|---|
| `stream()` / `to()` | No | Source / sink nodes |
| `flatMapValues` | No | One processor, `(stores: [])`; the key is kept, so no repartition |
| `groupBy` (re-key) | No (by itself) | KEY-SELECT + FILTER (drops records with an empty key) + sink to `*-repartition` |
| `count(Materialized.as(...))` | Yes | AGGREGATE node with `stores: [counts-store]` in a new sub-topology |
| `toStream()` | No | KTABLE-TOSTREAM: turns table updates into a record stream |

## Worked Example
**The WordCount quickstart, end to end (single broker, KRaft):**
1. Untar and format storage. Run `tar -xzf kafka_2.13-4.3.1.tgz`, then `KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"`, then `bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties`, then `bin/kafka-server-start.sh config/server.properties`.
2. Create the topics. The input is `kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input`. The output is the same command with `--topic streams-wordcount-output --config cleanup.policy=compact`, because the output is a changelog. Check both with `--describe --exclude-internal`.
3. Run the app with `bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo`. To use the new protocol, run `echo "group.protocol=streams" > streams.properties` and pass that file as an argument. The app prints nothing to STDOUT; its results go to Kafka.
4. Produce lines with `kafka-console-producer.sh --topic streams-plaintext-input`. Each line has a null key and the text as its value.
5. Consume with `kafka-console-consumer.sh --topic streams-wordcount-output --from-beginning --formatter-property print.key=true --formatter-property print.value=true --formatter-property key.deserializer=...StringDeserializer --formatter-property value.deserializer=...LongDeserializer`.
6. Send "all streams lead to kafka", then "hello kafka streams", then "join kafka summit". The output grows as `all 1, streams 1, lead 1, to 1, kafka 1`, then `hello 1, kafka 2, streams 2`, then `join 1, kafka 3, summit 1`. Each new line for a key is an *update* of that key's count. Replaying the topic from the beginning rebuilds the KTable (stream–table duality).
7. Internally, `describe()` shows Sub-topology 0 (source → FLATMAPVALUES → KEY-SELECT → FILTER → sink to `counts-store-repartition`) and Sub-topology 1 (source from the repartition topic → AGGREGATE with `counts-store` → TOSTREAM → sink to `streams-wordcount-output`). For each record, the aggregate reads the current count from the store, adds 1, writes it back and forwards the new count downstream.
8. To tear down, press Ctrl-C to stop the consumer, the producer, the app and the broker, in that order.

## Key Takeaways
1. Start every Streams app with a unique `application.id` and `bootstrap.servers`. Everything else is optional at first.
2. Run `topology.describe()` before deploying. Each re-key (`groupBy`) adds a repartition topic and a new sub-topology, and each `Materialized` adds a state store.
3. Create aggregation output topics with `cleanup.policy=compact`, because they carry table updates, not events.
4. Pass explicit serdes (`Produced.with(...)`) whenever a value type differs from the defaults. Otherwise the job fails at runtime.
5. Name your state stores (`Materialized.as("counts-store")`) so they are queryable and their internal topics have stable names.
6. Register a shutdown hook that calls `streams.close()`. Consider `group.protocol=streams` (KIP-1071) for broker-driven rebalancing.

## Connects To
- **ch02**: the same KRaft single-node start (`kafka-storage.sh format --standalone`) used in the quickstart.
- **ch07**: log compaction, the reason changelog output topics use `cleanup.policy=compact`.
- **ch24**: core concepts behind the tutorial: topologies, tasks, stream–table duality, state and fault tolerance.
- **ch25**: the full DSL (`flatMapValues`, `groupBy`, `count`, `Materialized`, `Produced`, joins, windows).
- **ch26**: the Processor API and the serde/data-type catalog behind `Serdes.String()`/`Serdes.Long()`.
- **ch27**: the full `StreamsConfig` reference (`application.id`, default serdes) and how to run and scale apps.
- **ch28**: querying `counts-store` with interactive queries, and testing topologies.
- **ch29**: the streams rebalance protocol (`group.protocol=streams`, KIP-1071) and the application reset tool.
