# Chapter 26: Kafka Streams — Processor API, Data Types and Writing Apps

## Core Idea
Drop to the Processor API when the DSL can't express your logic. You implement `Processor<KIn,VIn,KOut,VOut>` (`init`/`process`/`close`), attach `StoreBuilder`-defined state stores, and wire source → processor → sink nodes on a `Topology` by name. Every key and value still needs a Serde, either a config default or passed explicitly per operation.

## Frameworks Introduced
- **Processor lifecycle (`init` → `process` → `close`)**: Streams calls `init(ProcessorContext)` during task construction, `process(Record)` once per record, and `close()` on teardown. The same object may be re-`init`ed after `close()`.
  - When to use: any custom stateless or stateful step, especially periodic emission, custom state access, or per-record metadata the DSL hides.
  - How: (1) in `init`, keep the context, fetch stores with `context.getStateStore("name")`, and register punctuators with `context.schedule(...)`; (2) in `process`, read and update stores and emit with `context.forward(record.withValue(...))`; (3) in `close`, release your own resources only, never the stores, because the library manages them. Set the output generics to `Void` if you never forward, or to `Object` if you forward types that share no common superclass.
  - Why it works / failure mode: Kafka Streams owns instantiation through `ProcessorSupplier#get()`, which must return a **new instance every call**. Returning a singleton breaks the supplier contract and fails at runtime. A store name that doesn't match, or a store not connected to this processor, also throws at runtime from `init()`.
- **Punctuation (`ProcessorContext#schedule(interval, PunctuationType, Punctuator)`)**: a periodic callback driven by either stream-time or wall-clock time.
  - When to use: emitting aggregates periodically, flushing buffers, expiring state.
  - How: pick `PunctuationType.STREAM_TIME` if the cadence should follow event timestamps (deterministic and reproducible on replay). Pick `PunctuationType.WALL_CLOCK_TIME` if it must fire on real elapsed time even when no input arrives. You can register several punctuators of different types in one `init()`.
  - Why it works / failure mode: stream-time advances **only when records are processed**. With no input, or while waiting under `max.task.idle.ms`, a STREAM_TIME punctuator never fires. Using `WallclockTimestampExtractor` does **not** change that.
- **StoreBuilder + `Stores` factory**: you never create stores directly. You declare a `StoreBuilder` (supplier + key serde + value serde + logging options), and Streams instantiates the store per task where it's needed.
  - How: `Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("name"), keySerde, valueSerde)`. Then add `.withLoggingDisabled()` or `.withLoggingEnabled(Map)`. Attach it with `Topology#addStateStore(builder, "ProcessorName")`, or return it from `ConnectedStoreProvider#stores()` on the `ProcessorSupplier`.
- **Changelog-backed fault tolerance**: each store is mirrored to a compacted changelog topic, and a failed or migrated task restores from it. Logging is on by default.
- **Serde resolution**: Streams uses the default serdes (`default.key.serde`, `default.value.serde`, both null by default) unless an operation passes explicit serdes (`Consumed`, `Produced`, `Materialized`, ...), and the explicit ones win. Operations that need serdes include `stream()`, `table()`, `to()`, `repartition()`, `groupByKey()` and `groupBy()`.

## Key Concepts
- **`Record<K,V>`**: an immutable data class holding key, value, timestamp and headers. `withKey`/`withValue`/`withTimestamp`/`withHeaders` return a **shallow** copy, so deep-copy any mutable payloads yourself.
- **`ProcessorContext`**: gives access to `schedule()`, `forward()`, `commit()` (a commit *request*), app metadata (`applicationId`, `taskId`, `stateDir`) and `RecordMetadata` (`topic`, `partition`, `offset`).
- **Persistent KV store (RocksDB)**: the recommended default. It is disk-backed, so state can exceed the heap but must fit on local disk, and you can tune it through the RocksDB config setter.
- **In-memory KV store**: state must fit in the heap. Use it when local disk is missing or wiped between restarts. There is no in-memory versioned store.
- **Timestamped store**: stores value plus timestamp. KTables always use these. Handles out-of-order data and exposes the last-update timestamp to Interactive Queries.
- **Versioned KV store** (since 3.5): keeps multiple versions per key and supports "as of timestamp T" lookups. Its *history retention* doubles as its *grace period*.
- **Header-aware stores** (KIP-1271): `persistent*WithHeaders` suppliers, available only for timestamped KV, timestamped window and session stores. Reads return `ValueTimestampHeaders`; session stores hold `AggregationWithHeaders`.
- **Read-only state store**: materialized from its input topic, which also serves as its changelog, so that topic must be compacted. The associated state-update processor is its only writer.
- **Window changelog**: uses `cleanup.policy` compact+delete, because the keys combine the record key with the window timestamp. Retention is `Windows#maintainMs()` + 1 day.
- **`Bytes`**: a wrapper around `byte[]` with proper equality and ordering. Prefer it over a raw `byte[]`.

## Mental Models
- Think of a `Topology` as a **named DAG**. Nodes refer to each other by string name (`addSource("Source")`, `addProcessor("Process", ..., "Source")`), and store names are the contract between the wiring code and `getStateStore()`.
- Use **STREAM_TIME punctuation for correctness** (replayable, tied to the data) and **WALL_CLOCK_TIME for liveness** (fires even when input stalls).
- Think of a versioned store's history retention as **both a lookback window and a late-write cutoff**. Stream-time is tracked per partition, not per key, so size retention to cover lateness *across* keys.
- Use **`ConnectedStoreProvider#stores()`** when a processor should "own" its store. Several processors can share one store this way if they return the **same `StoreBuilder` instance**.

## Anti-patterns
- **Returning a singleton from `ProcessorSupplier#get()`**: violates the supplier pattern and causes runtime exceptions, because every task needs its own instance.
- **Closing state stores in `Processor#close()`**: the library manages store lifecycle.
- **Leaving a `KeyValueIterator` open**: leaks native resources and can cause an OOM. Always use try-with-resources.
- **Expecting STREAM_TIME punctuation on an idle partition**: it won't fire. Switch to wall-clock time or make sure input keeps flowing.
- **Disabling the changelog for convenience**: the store loses fault tolerance and **can't have standby replicas**.
- **Mutating shared objects after `withValue()`**: the copy is shallow, so the mutation leaks into the other record.
- **A generic custom Serde class in `default.*.serde` config**: a config-registered serde must be a concrete class with no generic parameters. Pass generic serdes or `Serdes.serdeFrom(...)` serdes only through method arguments.
- **Using a read-only store for lookups without co-partitioning**: the lookup sees the wrong partition's data.

## Code Examples
```java
public class WordCountProcessor implements Processor<String, String, String, String> {
  private KeyValueStore<String, Integer> kvStore;
  @Override public void init(final ProcessorContext<String, String> context) {
    context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, ts -> {
      try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
        while (iter.hasNext()) {
          final KeyValue<String, Integer> e = iter.next();
          context.forward(new Record<>(e.key, e.value.toString(), ts));
        }
      }
    });
    kvStore = context.getStateStore("Counts");
  }
  @Override public void process(final Record<String, String> record) {
    for (final String w : record.value().toLowerCase(Locale.getDefault()).split("\\W+")) {
      final Integer old = kvStore.get(w);
      kvStore.put(w, old == null ? 1 : old + 1);
    }
  }
  @Override public void close() { /* don't close stores */ }
}
```
- **What it demonstrates**: a stateful processor that updates a store per record and emits a periodic snapshot from a STREAM_TIME punctuator, reading the store with try-with-resources.

```java
Map<String, String> changelogConfig = new HashMap<>();
changelogConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder =
    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("Counts"),
                                Serdes.String(), Serdes.Long())
          .withLoggingEnabled(changelogConfig);   // or .withLoggingDisabled()
```
- **What it demonstrates**: declaring a RocksDB store through a builder and overriding changelog topic configs. Streams accepts any log config and ignores unrecognized keys.

```java
settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
userCountByRegion.to("RegionCountsTopic", Produced.valueSerde(Serdes.Long())); // override value only
Serde<Windowed<String>> tw = WindowedSerdes.timeWindowedSerdeFrom(String.class, 500L);
```
- **What it demonstrates**: default serdes set in config, a selective per-operation override, and a windowed serde that needs the window size.

## Reference Tables

| Store supplier (`Stores.*`) | Engine | Use when |
|---|---|---|
| `persistentKeyValueStore` | RocksDB | Default choice. Plain KV with no embedded timestamp |
| `persistentTimestampedKeyValueStore` | RocksDB | You need value+timestamp, range queries, out-of-order handling |
| `persistentVersionedKeyValueStore(name, historyRetention)` | RocksDB | You need "as of T" lookups (e.g. temporal joins). No caching, no IQ |
| `persistentWindowStore` / `persistentTimestampedWindowStore` | RocksDB | Windowed aggregates |
| `persistentSessionStore` | RocksDB | Session windows |
| `persistent{TimestampedKeyValue,TimestampedWindow,Session}StoreWithHeaders` + matching `*WithHeadersBuilder` | RocksDB | Headers must survive into state and output |
| `inMemoryKeyValueStore` (+ window/session) | heap | No usable local disk, or state is small |

| Setting / API | Default | Meaning | When to change |
|---|---|---|---|
| `default.key.serde` / `default.value.serde` | null | App-wide serdes | Set them, or pass serdes on every op |
| `withLoggingEnabled/Disabled` | enabled | Store changelog backup | Disable only for rebuildable/ephemeral state (you lose standbys) |
| `WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG` | 1 day | Extra retention on window changelogs beyond `maintainMs` | Lengthen if restores need older windows |
| Versioned changelog `min.compaction.lag.ms` | broker default | Keeps old versions from being compacted away | Set ≥ history retention (recommended: + 1 day) |
| `TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG` | — | Window size for deserializing time-windowed keys | Replaces deprecated `StreamsConfig.WINDOW_SIZE_MS_CONFIG` |
| `TimeWindowed{Serializer,Deserializer}.WINDOWED_INNER_*_CLASS` | — | Inner key serde class | Replaces deprecated `StreamsConfig.WINDOWED_INNER_CLASS_SERDE` |

Built-in serdes (`kafka-clients`): `ByteArray`, `Bytes`, `ByteBuffer`, `Double`, `Integer`, `Long`, `String`, `UUID`, `Void`, `ListSerde`, `Boolean`. Windowed serdes (`kafka-streams`): `WindowedSerdes.TimeWindowedSerde`, `SessionWindowedSerde`.

## Worked Example
**Wire and run a word-count topology with the Processor API, then migrate it to a versioned store.**
1. Dependencies: `org.apache.kafka:kafka-streams:4.3.1` and `kafka-clients:4.3.1`. For Scala, add `kafka-streams-scala_2.13`.
2. Build the topology:
   ```java
   Topology t = new Topology();
   t.addSource("Source", "source-topic")
    .addProcessor("Process", WordCountProcessor::new, "Source")   // new instance per get()
    .addStateStore(countStoreBuilder, "Process")                    // store "Counts"
    .addSink("Sink", "sink-topic", "Process");
   ```
   Records flow Source → `process()` → store update. Every 1s of *stream-time*, the punctuator forwards the counts through "Sink" to `sink-topic`. 60 records stamped 1..60 s trigger 6 punctuations no matter how long processing takes. Under WALL_CLOCK_TIME with 10s scheduling, the same 60 records processed in 20 s trigger 2, and processed in 5 s trigger 0.
3. Run it: `KafkaStreams streams = new KafkaStreams(t, props)`. Then call `streams.setUncaughtExceptionHandler(...)`, then `streams.start()`, and add `Runtime.getRuntime().addShutdownHook(new Thread(streams::close))` so SIGTERM shuts down cleanly. Starting or stopping instances causes Streams to move tasks between them.
4. Later, switch "Counts" to a versioned store (plain and timestamped KV share the same changelog format, so the upgrade is allowed). (a) Stop **all** instances and clear the local state dirs for that store. (b) Change the code to `Stores.persistentVersionedKeyValueStore("Counts", historyRetention)`. (c) Set `min.compaction.lag.ms` on the changelog to history retention + 1 day. (d) Restart and let the store rebuild from the changelog. Expect a bigger RocksDB footprint and slower performance, so benchmark it.

## Key Takeaways
1. Start with the DSL. Use the Processor API (or mix it in via `process()`) when you need punctuation, direct store access, or record metadata.
2. `ProcessorSupplier#get()` must return a fresh `Processor` each time, and store names must match exactly between `Topology` wiring and `getStateStore()`.
3. STREAM_TIME punctuators fire only when data flows, even with `WallclockTimestampExtractor`. Use WALL_CLOCK_TIME for timer-like behavior.
4. Default to persistent RocksDB KV stores and keep changelogs enabled. Disabling a changelog costs you both fault tolerance and standby replicas.
5. Always close store iterators with try-with-resources to avoid OOM.
6. Set `default.key.serde`/`default.value.serde` (null by default) or pass serdes on every operation. Config-registered custom serdes must be non-generic classes.
7. Versioned stores are opt-in. History retention is both the lookback horizon and the late-write cutoff. They support no caching, no IQ, no windowed variants and no global tables.

## Connects To
- **ch24**: processor topology, stream-time and tasks, which this chapter builds on.
- **ch25**: the DSL alternative and `process()` integration with the Processor API.
- **ch27**: RocksDB/memory config, standby replicas, `max.task.idle.ms`, deserialization exception handlers, running and scaling apps.
- **ch28**: interactive queries over these stores and `test-utils` unit testing of processors.
- **ch29**: application reset tool (clearing local state for store migration).
- **ch07**: log compaction, which backs changelog and read-only store topics.
- **ch30**: upgrade notes for KIP-1271 headers-aware stores and timestamped-store upgrades.
