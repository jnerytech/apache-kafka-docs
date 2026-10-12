# Chapter 25: Kafka Streams — DSL API

## Core Idea
The DSL lets you build a processor topology from three abstractions: `KStream` (inserts), `KTable` (upserts per key, partitioned) and `GlobalKTable` (upserts per key, fully replicated). Each operator's cost comes down to three questions: does it change the key (and so trigger a repartition), does it need a state store, and which join or window semantics apply. Learn those and you can predict how any topology will behave.

## Frameworks Introduced
- **Stream–table duality (KStream vs KTable vs GlobalKTable)**: a KStream record is an INSERT into an append-only ledger. A KTable record is an UPSERT of the last value for its key, and a `null` value is a DELETE (tombstone). A GlobalKTable has the same upsert semantics but every instance loads **all** partitions.
  - When to use: use KStream for facts and events (transactions, page views). Use KTable for the current state per key (profiles, CDC rows). Use GlobalKTable for small dimension data, foreign-key lookups, star joins, broadcast data, or skewed keys.
  - How: `builder.stream(topic, Consumed.with(k,v))`, `builder.table(topic, Materialized.as("store"))`, `builder.globalTable(topic, Materialized.as("store"))`. Name the store, or the table is not queryable through interactive queries.
  - Why it works / failure mode: summing `("alice",1) → ("alice",3)` gives 4 on a KStream and 3 on a KTable. Never enable log compaction on a KStream's topic: compaction purges older records and silently changes the results. Compaction is fine for a KTable topic.
- **Repartition marking**: key-changing operators (`map`, `flatMap`, `selectKey`, `groupBy`) **mark** the stream. The next grouping or join then inserts an internal repartition topic.
  - When to use: decide this every time you change a key before a `groupByKey` or a join.
  - How: prefer `mapValues`/`flatMapValues` over `map`/`flatMap`. Prefer `groupByKey` (repartitions only if the stream is marked) over `groupBy` (always repartitions). Use `repartition(Repartitioned.numberOfPartitions(n))` to force a repartition explicitly. It is required before `process()`, which does not auto-repartition after a key change, and it is also a way to rescale downstream sub-topologies.
  - Why it works / failure mode: repartition topics add a network hop, broker load and latency. Changing a key you didn't need to change costs a full shuffle.
- **Co-partitioning contract for equi-joins**: both sides need the **same partition count** and the **same partitioning strategy** (the same `partitioner.class` or `StreamPartitioner`). Kafka Streams checks the partition count at assignment time (`TopologyBuilderException`) but **cannot** verify the partitioner.
  - How to fix: find the side with fewer partitions ("SMALLER"). Repartition it up to the larger count with the same partitioner: `KStream#repartition(...)`, or for a KTable `toStream().repartition(...).toTable()`. Then join. For stream-table joins, repartition the stream (repartitioning a KTable can create a second store). For table-table joins, repartition the smaller table.
  - Exempt: KStream-GlobalKTable joins and KTable-KTable foreign-key joins (Streams co-partitions FK joins internally).
- **Aggregation adder/subtractor model**: a `KGroupedStream` needs an initializer and an adder. A `KGroupedTable` also needs a **subtractor**, because an update first retracts the old value and then adds the new one. A tombstone calls only the subtractor. If the subtractor returns `null`, the key is removed, and the next record for that key calls the initializer again.
- **Windowing + grace + suppression**: windows sub-group the records of each key. The grace period sets how late a record can be and still be accepted. `suppress(Suppressed.untilWindowCloses(...))` emits exactly one final result per window (KIP-328).
- **Versioned tables (timestamp semantics, 3.5+)**: materialize a table with a versioned store to switch from latest-by-offset to latest-by-timestamp semantics for joins and aggregations.

## Key Concepts
- **Branch (`split`)**: predicates are evaluated in order, and each record goes to the first match only. Records that match nothing go to `defaultBranch`, or are dropped if there is none. Multicast is broadcast plus filters.
- **`peek` vs `foreach`**: both run side effects. `peek` continues the stream; `foreach` is terminal. Neither side effect is covered by Kafka's processing guarantees.
- **`merge`**: no ordering guarantee across the inputs; order is kept within each input.
- **Cogroup**: aggregates several grouped streams that share a key type (their value types can differ) in one operation. The aggregators go in each `cogroup()` call, so `aggregate()` needs only an initializer. It never repartitions.
- **Windowed KTable**: windowed aggregations return `KTable<Windowed<K>, V>`. Read the parts with `Windowed#window()` and `Windowed#key()`.
- **Window retention**: windows are kept for at least the retention period (default **1 day**). Change it with `Materialized#withRetention()`.
- **Store types**: non-windowed tables and aggregations use `TimestampedKeyValueStore` (or `VersionedKeyValueStore`). Time windows and stream-stream joins use `TimestampedWindowStore`. Session windows use `SessionStore`.
- **`dsl.store.format=HEADERS` (KIP-1285)**: switches the DSL to headers-aware stores. Aggregations and table joins still write empty headers. `suppress()` and the left/outer stream-stream join buffers are not headers-aware, so headers are lost there.
- **Record cache**: enabled by default. It compacts consecutive updates per key, so you see fewer intermediate KTable updates. Disable it only for testing or debugging.
- **GlobalKTable bootstrap**: on every (re)start the GlobalKTable is fully loaded before processing begins. It has no notion of time, so a stream-global join is not temporal.

## Mental Models
- Think of every DSL operator as one of three things: a **stateless relabel** (cheap), a **key change** (it marks the stream, and the next grouping or join shuffles), or a **stateful operator** (it needs a store and a changelog).
- Use KStream-KTable joins for enrichment from a partitioned table. Only the stream side triggers output; table updates just refresh the lookup state. Use KStream-GlobalKTable when the lookup key comes from the value, or when you chain several lookups.
- Think of `suppress` as a **buffer in front of a side effect**. Use `untilWindowCloses` for "final answer only" alerts, and `untilTimeLimit` to rate-limit calls to external systems.
- Use a versioned table when out-of-order data per key is real and the join must use the table value that was valid at the stream record's timestamp.

## Anti-patterns
- **Using `groupBy`/`map` when the key doesn't change**: it adds a repartition topic for nothing. Use `groupByKey`/`mapValues`.
- **Assuming equal partition counts mean co-partitioning**: if different producers use different partitioners, the join is silently wrong and Streams cannot detect it.
- **Using `print()` in production**: it flushes on every record.
- **Writing to external systems from `foreach`/a custom sink**: delivery semantics become your job. Use Kafka Connect instead.
- **Using the deprecated `JoinWindows.of(...).grace(...)` API**: left/outer stream-stream joins emit spurious results eagerly. Use `JoinWindows.ofTimeDifferenceAndGrace` or `ofTimeDifferenceWithNoGrace`.
- **Applying `suppress` to a versioned table**: not allowed, because it collapses the version history.
- **Materializing aggregate or table-table join results with versioned stores**: these operators don't produce a complete history, so the results are unpredictable.
- **Returning a `null` foreign key from `foreignKeyExtractor`**: the record is ignored. Use a sentinel value (`"NULL"`, `-1`) if null FKs must join.

## Code Examples
```java
// Final-result alert: hourly count per user, emit only when the window closes
grouped
  .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10)))
  .count()
  .suppress(Suppressed.untilWindowCloses(unbounded()))
  .filter((windowedUserId, count) -> count < 3)
  .toStream()
  .foreach((wk, count) -> sendAlert(wk.window(), wk.key(), count));
```
- **What it demonstrates**: the 09:00–10:00 window accepts late records until 10:10, then emits exactly one value. Without `suppress`, every user would match `< 3` early on and trigger a false alert. Production code should cap the buffer instead of using `unbounded()`.

```java
// Rate-limit updates to an external DB: at most once per key every 5 min
groupedTable.count()
  .suppress(untilTimeLimit(ofMinutes(5), maxBytes(1_000_000L).emitEarlyWhenFull()))
  .toStream().foreach((key, count) -> updateCountsDatabase(key, count));
```
- **What it demonstrates**: `emitEarlyWhenFull()` flushes the oldest entries when the 1 MB buffer is full. The alternative, shutting the app down when full, enforces the time limit strictly. Most apps should emit early.

```java
// Stream-stream windowed join (always windowed) + FK table join
KStream<String,String> j = left.join(right,
    (l, r) -> "left=" + l + ", right=" + r,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
    Joined.with(Serdes.String(), Serdes.Long(), Serdes.Double()));
KTable<String,String> fk = leftTable.join(rightTable, v -> v /* foreignKeyExtractor */,
    (l, r) -> "left=" + l + ", right=" + r);
```
- **What it demonstrates**: the stream-stream join returns `KStream<K,…>`, not a windowed key, and one input record can match several records in the window. The FK join resolves `foreignKeyExtractor(left.value) == right.key`, so one right-side update fans out to every left row that has that FK.

## Reference Tables
| Join | Windowed? | Inner | Left | Outer | Co-partition needed |
|---|---|---|---|---|---|
| KStream-KStream | yes | ✓ | ✓ | ✓ | yes |
| KTable-KTable (equi) | no | ✓ | ✓ | ✓ | yes |
| KTable-KTable foreign-key | no | ✓ | ✓ | ✗ | no |
| KStream-KTable | no | ✓ | ✓ | ✗ | yes |
| KStream-GlobalKTable | no | ✓ | ✓ | ✗ | no |
| KTable-GlobalKTable | — | ✗ | ✗ | ✗ | — |

| Window | Factory | Shape | Alignment / bounds |
|---|---|---|---|
| Hopping | `TimeWindows.ofSizeWithNoGrace(size).advanceBy(hop)` | fixed size, overlapping | epoch-aligned, `[start;end)` |
| Tumbling | `TimeWindows.ofSizeAndGrace(size, grace)` | fixed size, no gaps, no overlap | epoch-aligned, `[start;end)` |
| Sliding | `SlidingWindows.ofTimeDifferenceAndGrace(diff, grace)` / `JoinWindows` | fixed size, based on timestamp difference | record-aligned, both bounds inclusive |
| Session | `SessionWindows.ofInactivityGapWithNoGrace(gap)` | variable size, per key | merged when within the gap; needs a session merger for `aggregate` |

| Operator | Repartitions? |
|---|---|
| `map`, `flatMap`, `selectKey` | marks the stream (the next grouping or join repartitions) |
| `mapValues`, `flatMapValues`, `filter`, `peek` | no |
| `groupBy` | always |
| `groupByKey`, joins, `cogroup` | only if the stream is already marked |
| `repartition()` | always (managed internal topic) |
| `to()` | if partition counts differ, the stream is marked, a custom `StreamPartitioner` is set, or the key is `null` |

| Null handling | Behavior |
|---|---|
| `null` key into any aggregation | ignored |
| `count` on KGroupedStream | `null` key **or** value ignored |
| `null` value on a KTable input | tombstone (DELETE); calls the subtractor only |
| Stream-table join | only stream records trigger; table tombstones never trigger |
| Table-table left join | right tombstones trigger; left tombstones forward a tombstone only if the key exists in the result |

## Worked Example
**KGroupedTable aggregation: users regrouped by region (sum of a numeric value).** The input is a `userProfiles` KTable, regrouped with `groupBy` into `(region, value)`, and the aggregation uses initializer 0, an adder and a subtractor.
1. `(alice,E)` INSERT → grouped as `(E,5)`; initializer and adder give `E=5`.
2. `(bob,A)` → `(A,3)` gives `A=3`. `(charlie,A)` → `(A,7)`; adder gives `A=10`.
3. `(alice,A)` UPDATE: the subtractor runs on the old group, `E = 5-5 = 0`, then the adder runs on the new group, `A = 10+5 = 15`. When the old and new grouping keys are equal, the subtractor is guaranteed to run before the adder. That depends on a correct `equals()` on the key type.
4. `(charlie,null)` DELETE: only the subtractor runs, `A = 15-7 = 8`.
5. `(null,E)`: the key is null, so the record is ignored.
6. `(bob,E)` UPDATE: `A = 8-3 = 5`, `E = 0+3 = 3`.
If the subtractor had returned `null`, the key would have been removed from the result, and the initializer would run again on the next record. With record caches enabled (the default), you may see fewer of these intermediate updates.

## Key Takeaways
1. Keep keys stable. Use `mapValues`/`flatMapValues`/`groupByKey`, and change a key only where the grouping or join really needs it.
2. Before any equi-join, check partition counts **and** the partitioner on both input topics. Repartition the smaller side up to the larger count.
3. Use a GlobalKTable to avoid co-partitioning, for joins on a value field, or for skewed keys. In exchange you pay full-topic storage and read load on every instance.
4. Always set a grace period with the non-deprecated window factories. Pair a window with `suppress(untilWindowCloses)` only when downstream needs final results.
5. As of Kafka 4.0, `transform`, `flatTransform`, `transformValues`, `flatTransformValues` and the old `process` are removed. Migrate to `process` (`Processor`) or `processValues` (`FixedKeyProcessor`). If you use the "merge repartition topics" optimization, use 4.0.1/4.1.1+ and set `TopologyConfig.InternalConfig.ENABLE_PROCESS_PROCESSVALUE_FIX=true` (KAFKA-19668). Compare `Topology.describe()` before and after.
6. Pass explicit Serdes (`Consumed`, `Grouped`, `Joined`, `Produced`) whenever the types differ from the default Serdes.
7. The Scala DSL (`kafka-streams-scala`) is deprecated in 4.3 and will be removed in 5.0. Migrate to the Java API.

## Connects To
- **ch24**: duality, time, windowing and state concepts that the DSL implements.
- **ch26**: the Processor API, versioned state stores and Serdes behind `process`/`processValues`.
- **ch27**: `dsl.store.format`, default Serdes, record cache and memory settings.
- **ch28**: interactive queries over the named stores; `TopologyTestDriver` testing.
- **ch07**: log compaction semantics, and why KTable topics can be compacted.
- **ch21**: Kafka Connect as the right way to sink to external systems.
- **ch30**: Streams upgrade notes (4.0 transformer removal, Scala deprecation).
