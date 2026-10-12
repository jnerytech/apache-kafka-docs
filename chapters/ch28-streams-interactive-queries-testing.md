# Chapter 28: Kafka Streams — Interactive Queries and Testing

## Core Idea
A Streams app's state is sharded across instances (one store per stream partition), so Interactive Queries (IQ) give you read-only local lookups for free, plus discovery metadata via `application.server`. You must build the RPC layer yourself to serve the *whole* app's state. Test topologies without a broker using `TopologyTestDriver` (whole topology, mocked wall clock) and `MockProcessorContext` (a single `Processor` in isolation).

## Frameworks Introduced
- **Local vs. remote interactive queries**: local = `KafkaStreams#store(...)` on this instance's stores. Remote = local query + instance discovery + an RPC layer you build.
  - When to use: local only when the caller is co-located with the shard it needs. Use remote whenever an external client (UI, mobile app, another service) needs any key.
  - How: (1) Name the store (`Materialized.as("name")` in the DSL, or a `StoreBuilder` in the PAPI). (2) Get it with a `QueryableStoreType` from `QueryableStoreTypes`. (3) For full-app queries, embed an RPC server (REST, Thrift, custom) in each instance. (4) Set a unique `application.server=host:port` per instance. (5) Route each query with the metadata methods, then call the owning instance over RPC.
  - Why it works / failure mode: Kafka Streams tracks each instance's endpoint, stores and assigned partitions in `StreamsMetadata`, so a key can be routed to its owner without scanning. If `application.server` is unset, the metadata methods find **nothing** for that instance and it can't be discovered.
- **Native IQ support matrix**: Kafka Streams gives you local store queries, discoverability and discovery of all instances for both scopes. RPC across instances is **not supported natively** — you configure and build it.
- **Queryable custom store contract** (PAPI only): to make a custom store queryable, implement:
  - the store itself (`StateStore`)
  - a read-write operations interface that extends a **read-only** interface
  - a `StoreBuilder`
  - a `QueryableStoreType<ReadOnlyIface>` with `accepts(StateStore)` and `create(StateStoreProvider, storeName)`
  - a **wrapper** that uses `StateStoreProvider#stores(storeName, type)` to fan out over all local store instances (one per task/partition), so callers see one logical store.
  - Why it works: an instance runs many tasks, so one store name maps to N physical stores. The wrapper hides the sharding, and the read-only interface stops out-of-band mutation.
- **TopologyTestDriver loop**: build the `Topology` → `new TopologyTestDriver(topology, props)` → `createInputTopic` / `createOutputTopic` with serdes → optionally pre-populate stores via `getKeyValueStore(...)` → `pipeInput` → assert with `readKeyValue()` / `isEmpty()` / store reads → `advanceWallClockTime(...)` for wall-clock punctuators → `close()`.
- **MockProcessorContext unit test**: `init(context)` → `process(...)` → assert on `context.forwarded()` (or `forwarded("child")`), `committed()`, and `scheduledPunctuators()`. Fire punctuators by hand: `getPunctuator().punctuate(ts)`.

## Key Concepts
- **`KafkaStreams#store(...)`**: looks up a local state store by *name* and *type*. The result is read-only and spans all local partitions of that store.
- **`QueryableStoreTypes`**: built-in store-type helpers:
  - `keyValueStore()` and `timestampedKeyValueStore()`
  - `windowStore()` and `timestampedWindowStore()`
  - `sessionStore()`
  - header-aware variants: `timestampedKeyValueStoreWithHeaders()`, `timestampedWindowStoreWithHeaders()`, `sessionStoreWithHeaders()`
- **`ValueAndTimestamp<V>`**: the value type returned by the `timestamped*` query types. DSL stores keep timestamps either way; `keyValueStore()` and `windowStore()` return just the value.
- **`application.server`** (`StreamsConfig.APPLICATION_SERVER_CONFIG`): this instance's unique `host:port` RPC endpoint. It's what makes the instance discoverable.
- **`StreamsMetadata`**: per-instance info: host/port, local stores, and assigned partitions.
- **`StateStoreProvider`**: hands a custom-store wrapper every local physical instance of a named store.
- **`TopologyTestDriver`**: a broker-less simulation of the Streams runtime. It pipes records through the topology synchronously, captures output, exposes stores and mocks wall-clock time.
- **`TestInputTopic` / `TestOutputTopic`**: typed helpers for piping in key/values, values, lists or timestamped records, and for reading back key/values, values or record lists.
- **`MockProcessorContext`**: captures what a processor forwards, commits and schedules, and lets you set record metadata. It does not manage changelogs or state directories.
- **Versioned stores**: interactive queries do **not** support versioned key-value stores (at this time).

## Mental Models
- **Think of a store name as a logical table split by partition.** `store()` merges the local shards. `metadataForKey` tells you which instance has the shard for a key.
- **Use `metadataForKey` routing, not broadcast.** It hits exactly one instance via the default (or a custom) `StreamPartitioner`. Querying every instance in `allMetadataForStore` until one answers works, but costs O(instances) per lookup.
- **Use `TopologyTestDriver` for topology behavior, and `MockProcessorContext` for processor logic.** If a test needs scheduled punctuators to fire on their own, move it up to `TopologyTestDriver`.
- **Treat IQ as strictly read-only.** Only the topology, driven by input data, may mutate a store.

## Anti-patterns
- **Forgetting `application.server` on some instances**: those instances don't show up in `allMetadata*` or `metadataForKey`, so their keys can't be routed.
- **Hardcoding the same `application.server` everywhere**: the value must be unique per instance (derive it from the host at runtime), or queries reach the wrong node.
- **Exposing a writable custom-store interface through IQ**: callers could mutate state outside the topology and corrupt processing. Always expose a read-only interface.
- **Querying an unmaterialized `KTable`**: stateless operators like `filter` only make a store if you pass `Materialized.as("name")`. Otherwise there is nothing to query.
- **Expecting IQ on versioned stores**: not supported.
- **Pre-populating a store with logging on in `TopologyTestDriver`**, or registering a logged store with `MockProcessorContext`: use `.withLoggingDisabled()`. The mock doesn't support changelogs, and the driver example disables logging so the store can be pre-populated.
- **Not calling `testDriver.close()`**: leaks resources between tests. Close it in `@After`.
- **Expecting `MockProcessorContext` to fire punctuators**: it only captures them. Call `punctuate()` yourself.

## Code Examples
```java
// Local KV query on a DSL count store
groupedByWord.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsKeyValueStore"));
ReadOnlyKeyValueStore<String, Long> kv =
    streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());
kv.get("hello"); kv.range("all", "streams"); kv.all();

// Window store: all windows for a key
ReadOnlyWindowStore<String, Long> ws = streams.store("CountsWindowStore", QueryableStoreTypes.windowStore());
WindowStoreIterator<Long> it = ws.fetch("world", Instant.ofEpochMilli(0), Instant.now()); // key = window start ts
```
- **What it demonstrates**: name the store with `Materialized.as`, then query it by name and type. KV stores support `get`, `range` and `all`. Window stores return one value per window for a key, keyed by window start. Note: in current releases `store` takes a `StoreQueryParameters.fromNameAndType(name, type)` argument; the two-argument form shown is the docs' example. Always close iterators.

```java
props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "host1:4460");   // unique per instance
StreamsMetadata m = streams.metadataForKey("word-count", "alice", Serdes.String().serializer());
Long count = http.getLong("http://" + m.host() + ":" + m.port() + "/word-count/alice");
```
- **What it demonstrates**: discovery plus key-based routing to the single instance that owns the key. The RPC layer (here a hypothetical HTTP client and endpoint) is your code, started on the same `host:port` as `application.server`. (Newer releases expose `queryMetadataForKey` / `KeyQueryMetadata` and `metadataForAllStreamsClients` for the same purpose.)

```java
MockProcessorContext<String, Long> ctx = new MockProcessorContext<>(props);
processor.init(ctx);
processor.process(...);
assertEquals(ctx.forwarded().iterator().next().record(), new Record<>(...));
ctx.resetForwards(); assertTrue(ctx.committed()); ctx.resetCommit();
ctx.setRecordMetadata("topicName", 0, 0L);           // sticky until reset
MockProcessorContext.CapturedPunctuator p = ctx.scheduledPunctuators().get(0);
p.getIntervalMs(); p.getType(); p.cancelled(); p.getPunctuator().punctuate(0L);
```
- **What it demonstrates**: a processor unit test with no topology. Assert on captured forwards (or `forwarded("childName")` for one child), on commits and on scheduled punctuators. Set record metadata when the logic depends on topic, partition or offset.

## Reference Tables

| Need | API |
|---|---|
| All instances of the app | `KafkaStreams#allMetadata()` |
| Instances hosting store X | `KafkaStreams#allMetadataForStore(storeName)` |
| Instance owning key K (default partitioning) | `metadataForKey(storeName, key, keySerializer)` |
| Instance owning key K (custom partitioner) | `metadataForKey(storeName, key, StreamPartitioner)` |

| Store kind | Query type | Returned view |
|---|---|---|
| KV (values only) | `keyValueStore()` | `ReadOnlyKeyValueStore<K,V>` |
| KV with timestamps | `timestampedKeyValueStore()` | `ReadOnlyKeyValueStore<K,ValueAndTimestamp<V>>` |
| Window | `windowStore()` / `timestampedWindowStore()` | `ReadOnlyWindowStore<K,V>` / `<K,ValueAndTimestamp<V>>` |
| Session | `sessionStore()` | read-only session store |
| Header-aware | `*WithHeaders()` variants | results include record headers |
| Custom (PAPI) | your `QueryableStoreType` | your read-only interface |

| Test tool | Scope | Punctuators | State |
|---|---|---|---|
| `TopologyTestDriver` | whole `Topology` (DSL or PAPI) | stream-time ones fire automatically from record timestamps; wall-clock ones fire via `advanceWallClockTime` | real stores, `getKeyValueStore(...)` before or after |
| `MockProcessorContext` | single `Processor` | captured only, fire them by hand | you register in-memory stores, logging disabled |

The test dependency is `org.apache.kafka:kafka-streams-test-utils` (test scope, matching your Kafka version, e.g. `4.3.1`).

## Worked Example
**Testing a max-per-key aggregator with punctuation (from the docs):**
1. **Topology**:
   - `addSource("sourceProcessor","input-topic")` → `addProcessor("aggregator", CustomMaxAggregatorSupplier, ...)`.
   - `addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("aggStore"), String, Long).withLoggingDisabled(), "aggregator")`.
   - `addSink("sinkProcessor","result-topic","aggregator")`.
2. **Processor**:
   - `init` schedules `flushStore()` twice: every 60 s `WALL_CLOCK_TIME` and every 10 s `STREAM_TIME`.
   - `process` stores the value only if it's larger than the current one.
   - `flushStore` forwards every entry in the store.
   - Nothing is forwarded during `process`.
3. **Setup**: create the driver with default String/Long serdes, then the input and output topics. Pre-populate the store with `store.put("a", 21L)`.
4. **Assertions**:
   - `pipeInput("a",1L)`: the first record starts stream time, and output is `("a",21)`, then empty (the store keeps 21).
   - `pipeInput("a",42L)`: the store now holds 42, and output is `("a",42)`.
   - New key `"b"`: output is `("a",21)` then `("b",21)`.
   - Event-time punctuation: two records at time t give one flush. A third at t+10 s gives another flush.
   - Wall-clock punctuation: `advanceWallClockTime(Duration.ofSeconds(60))` with no input still emits `("a",21)`.
5. **Teardown**: `testDriver.close()` in `@After`.

## Key Takeaways
1. Name every store you want to query (`Materialized.as(...)`). Unnamed or unmaterialized results can't be queried.
2. Set a unique `application.server` on every instance, or discovery silently returns nothing for it.
3. Build the RPC layer yourself, embedded in each instance. Route with `metadataForKey` rather than broadcasting.
4. Use `timestamped*` query types when callers need timestamps, and `*WithHeaders()` when they need headers. Versioned stores can't be queried.
5. Custom stores need a `QueryableStoreType` plus a wrapper that fans out over `StateStoreProvider`. Expose only read operations.
6. Test topologies with `TopologyTestDriver` (no broker, controllable wall clock). Unit-test processors with `MockProcessorContext`, and fire punctuators by hand there.
7. Disable logging on stores you pre-populate or register in mocks, and always close the driver.

## Connects To
- **ch24**: state stores, tasks, and how partitions map to tasks and stores, which explains why state is sharded.
- **ch25**: DSL `count`, `windowedBy` and `Materialized`, which create the stores you query.
- **ch26**: Processor API custom stores, versioned and header-aware stores, and punctuators.
- **ch27**: `application.server` and other Streams configs; running and scaling instances.
- **ch29**: the rebalance protocol, which decides which instance owns which partition and therefore which store shard.
- **ch23**: the WordCount topology these examples extend.
