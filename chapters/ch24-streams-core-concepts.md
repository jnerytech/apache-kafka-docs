# Chapter 24: Kafka Streams — Core Concepts and Architecture

## Core Idea
Kafka Streams is a plain Java library (no cluster, no resource manager) whose parallelism, fault tolerance and state durability all come from Kafka itself: input partitions define a fixed set of tasks, local state stores are backed by compacted changelog topics, and exactly-once is achieved by committing offsets, state updates and outputs atomically in one Kafka transaction. Master the time model (event/ingestion/processing time, stream time) and the stream-table duality, and the rest of the API follows.

## Frameworks Introduced
- **Processor topology**: a logical DAG of stream processors (nodes) connected by streams (edges), bounded by **source processors** (read from one or more topics, no upstream) and **sink processors** (write to a topic, no downstream).
  - When to use: every Streams app is one or more topologies; build it with the DSL (`map`, `filter`, `join`, aggregations) or the Processor API (custom processors + direct state store access).
  - How: (1) define sources from input topics; (2) chain processors, each receiving one record at a time and forwarding zero or more records downstream; (3) end in sinks (or call external systems from a processor); (4) at runtime the logical topology is instantiated once per task.

- **Partitions → tasks → threads → instances (parallelism model)**: the number of tasks is fixed by the input partitions; each task owns a fixed list of partitions and its own copy of the topology plus local state; tasks are distributed over all stream threads across all instances.
  - When to use: sizing and scaling any Streams application.
  - How: (1) max parallelism ≈ max partition count of the input topics; (2) scale out by starting more instances or more threads (`num.stream.threads`, or add/remove threads at runtime since 2.8); (3) instances beyond the task count start but stay idle as hot spares that pick up work on failure; (4) the assignor is always `StreamsPartitionAssignor` — any other configured assignor is ignored.
  - Why it works: the partition→task mapping never changes, threads share no state, so no inter-thread coordination is needed and a failed task simply restarts elsewhere on the same partitions. Failure mode: under-partitioned input topics cap throughput no matter how many instances you deploy.

- **Changelog-backed state + standby replicas (fault tolerance)**: every state store has a replicated, partitioned, **log-compacted** changelog topic; one changelog partition per store instance/task. On failover the task is restarted elsewhere and its store is rebuilt by replaying the changelog before processing resumes.
  - When to use: any stateful app (joins, aggregations, windowing).
  - How: (1) accept the default changelogging; (2) set `num.standby.replicas` ≥ 1 to keep fully replicated, continuously updated copies on other instances; (3) since 2.6 a task is only assigned to an instance with a caught-up copy if one exists; (4) spread standbys across racks with `rack.aware.assignment.tags`, and reduce cross-rack traffic with `client.rack` + broker `broker.rack` + `rack.aware.assignment.strategy`.
  - Why it works: restoration cost dominates task (re)initialization; standbys turn a full replay into a near-instant handover. Failure mode: with no standbys, a large store means long recovery downtime after every failure or migration.

- **Exactly-once v2 (EOS)**: set `processing.guarantee=exactly_once_v2` so input-offset commits, state-store updates and output writes complete atomically (KIP-129 original, KIP-447 v2).
  - When to use: pipelines that tolerate neither loss nor duplicates (instead of bolting on a Lambda-architecture batch layer).
  - How: requires brokers ≥ 2.5; v1 is deprecated since 3.0. Guarantee covers Kafka outputs and state stores — not side effects in external systems.

## Key Concepts
- **Stream**: unbounded, ordered, replayable, fault-tolerant sequence of immutable key-value records.
- **Event time / ingestion time / processing time**: when the event happened at the source / when the broker appended it / when the Streams app processes it.
- **`TimestampExtractor`**: interface assigning each record its timestamp; the default returns the timestamp embedded in the Kafka message as-is (so topic/broker timestamp config decides event vs ingestion semantics).
- **Stream time**: data-driven time that advances only when new records arrive (max observed timestamp), distinct from **wall-clock time**.
- **Stream-table duality**: a stream is a table's changelog; a table is a point-in-time snapshot of the latest value per key — modeled as `KStream`, `KTable`, `GlobalKTable`.
- **Aggregation**: KStream or KTable in → always a KTable out, so late records can emit an updated value that overwrites the old one per key.
- **Grace period**: how long a window keeps accepting out-of-order records; a record is dropped if stream time > window end + grace.
- **State store**: named per-task local store (persistent key-value, in-memory, etc.), auto-created by DSL stateful operators, queryable read-only via **Interactive Queries**.
- **Standby replica**: a fully replicated shadow copy of a task's local state used for fast failover.
- **Versioned state store**: store keeping value history by timestamp, enabling timestamp-based lookups in joins to handle out-of-order data.

## Mental Models
- Think of a **task** as the unit of work and a **partition** as its permanent address: threads and instances are just containers you move tasks between.
- Think of a **KTable/state store** as a materialized view of a compacted topic — the same mechanism as CDC database replication.
- Use **event-time + grace periods** when correctness on late data matters; out-of-order is meaningless under processing time because nothing can be "late" relative to wall-clock processing.
- Think of out-of-order handling as a three-way trade-off between **latency, cost (buffered state), and correctness** — longer waits buy correctness.

## Anti-patterns
- **Running more instances than input partitions to "scale"**: extras sit idle; repartition the input instead (idle instances are only useful as failover spares).
- **Plugging in a custom partition assignor**: silently ignored; Streams always uses `StreamsPartitionAssignor`.
- **Relying on default stream-table / table-table joins with out-of-order input**: they process strictly in offset order and may give unpredictable results; use versioned stores (table-side out-of-order in stream-table joins is still not handled).
- **Using `exactly_once` (v1)**: deprecated since 3.0; migrate to `exactly_once_v2`.
- **No standbys for large stateful apps**: every failover replays the full changelog before processing resumes.
- **Assuming the extractor gives event time**: the default extractor returns whatever the topic stores — with log-append timestamps you actually have ingestion-time semantics.

## Code Examples
```properties
application.id=pageview-counter
processing.guarantee=exactly_once_v2     # default at_least_once; brokers >= 2.5
num.stream.threads=4                     # threads per instance
num.standby.replicas=1                   # warm copy of each store elsewhere
rack.aware.assignment.tags=zone          # place standbys in a different zone
client.tag.zone=eu-west-1a
```
- **What it demonstrates**: the core resiliency knobs — EOS v2, intra-instance parallelism, standby replicas and rack-aware standby placement.

```java
// Processor API: output timestamp defaults to the input's; override explicitly
context.forward(record.withTimestamp(eventTsFromPayload));
// In a Punctuator, forwarded records get the task's current internal time (context.timestamp())
```
- **What it demonstrates**: how output timestamps are assigned and how to override them.

## Reference Tables

**Output timestamp rules**

| Operation | Output record timestamp |
|---|---|
| Stateless (`map`, `filter`, `flatMap`…) | input record's timestamp (all `flatMap` outputs inherit it) |
| `context.forward()` in `process()` | inherited from input record (overridable) |
| `Punctuator#punctuate()` | task's current internal time (`context.timestamp()`) |
| Aggregation | max timestamp of contributing records, per key (globally or per window) |
| Stream-stream / table-table join | `max(left.ts, right.ts)` |
| Stream-table join | the stream record's timestamp |

**Out-of-order handling in joins**

| Join | Default (non-versioned) | With versioned stores |
|---|---|---|
| Stream-stream (inner/left/outer) | handled correctly | — |
| Stream-table | not handled, offset order, unpredictable | stream-side handled via timestamp lookup; table-side still not handled |
| Table-table | not handled, but result changelog is eventually consistent | switches to timestamp-based semantics, handled |

**Configs touched in this chapter**

| Config | Default | Meaning | When to change |
|---|---|---|---|
| `processing.guarantee` | `at_least_once` | delivery semantics | `exactly_once_v2` when duplicates are unacceptable |
| `num.stream.threads` | 1 | stream threads per instance | more cores, more tasks per instance |
| `num.standby.replicas` | 0 | shadow copies of each store | large state / tight recovery SLO |
| `rack.aware.assignment.tags` | none | spread standbys across tagged racks | multi-AZ; takes precedence over `client.rack` for standby placement |
| `rack.aware.assignment.strategy` | none | rack-aware active task placement (needs `client.rack` + `broker.rack`) | reduce cross-rack/AZ traffic cost |

## Worked Example
**Pageview counts, failover with a standby.** Input topic `pageviews` has 5 partitions, keyed by user. The topology `stream("pageviews").groupByKey().count()` creates 5 tasks (0_0…0_4), each with its own local count store and a dedicated partition of the compacted changelog topic. Deploy 3 instances × 2 threads: 6 threads, 5 tasks → one thread idle. With `num.standby.replicas=1`, each task also has a standby on a different instance that continuously tails its changelog partition. The table evolves as a changelog: alice=1, bob=1, alice=2 — replaying that changelog rebuilds the same table. Instance B crashes holding tasks 0_1 and 0_3. The `StreamsPartitionAssignor` reassigns them, preferring instances whose standby is caught up (guaranteed since 2.6 when one exists); those instances promote the standby, replay only the small tail of the changelog, and resume consuming the same partitions from the committed offsets. With `exactly_once_v2`, any count update/output written by B's uncommitted transaction is aborted, so no pageview is counted twice. Adding a 6th instance later would yield no extra throughput — only another spare — because there are only 5 tasks.

## Key Takeaways
1. Size input topic partitions for peak parallelism up front — tasks = partitions, and tasks cap scaling.
2. Scale by adding instances or threads (runtime thread add/remove since 2.8); never by swapping assignors.
3. Set `num.standby.replicas` ≥ 1 for any meaningful state, plus rack tags in multi-AZ deployments.
4. Use `processing.guarantee=exactly_once_v2` (brokers ≥ 2.5) for loss/duplicate-intolerant pipelines.
5. Decide your time semantics deliberately: the topic's timestamp type plus your `TimestampExtractor` define stream time.
6. Set window grace periods to trade latency for correctness on late data; use versioned stores for joins with out-of-order input.
7. Aggregations always produce KTables — downstream consumers must treat updates as upserts per key.

## Connects To
- **ch23**: Streams intro/quickstart where the first topology is built.
- **ch25**: DSL details — KStream/KTable/GlobalKTable, windows, grace, versioned stores, joins.
- **ch26**: Processor API, `forward()`, punctuators, timestamp extractors and state store types.
- **ch27**: full `StreamsConfig` reference (`num.stream.threads`, standbys, rack awareness, task idling).
- **ch28**: Interactive Queries over the state stores described here.
- **ch29**: Streams rebalance protocol and task assignment operations.
- **ch06**: transactions and idempotent producer underpinning EOS v2.
- **ch07**: log compaction used by changelog topics.
- **ch04**: topic-level timestamp config that decides event vs ingestion time.
