# Chapter 01: Introduction and Use Cases

## Core Idea
Kafka is an event streaming platform that unifies three capabilities — publish/subscribe, durable storage, and stream processing — on top of a partitioned, replicated, append-only log, so producers and consumers stay fully decoupled and events can be re-read for as long as you retain them. The source material is a conceptual primer (thin on configs and commands); exact configs and mechanics live in later chapters.

## Frameworks Introduced
- **The three capabilities of an event streaming platform**: (1) **publish/subscribe** streams of events, including continuous import/export from other systems; (2) **store** streams durably for as long as you want; (3) **process** streams as they occur or retrospectively.
  - When to use: to decide whether Kafka fits a problem — if you need only one of the three (e.g., a simple task queue with no replay), a traditional broker may suffice; if you need two or three together (replayable feeds + processing + integration), Kafka is the natural fit.
  - How: map each requirement to a capability — integration with databases/other systems → Kafka Connect; durable replay → topic retention; real-time transformation/joins/aggregations → Kafka Streams; plain read/write → Producer/Consumer APIs.
  - Why it works: because storage is the log itself, the same data serves real-time consumers, late-joining consumers, and batch reprocessing without copies. Failure mode: treating Kafka as a transient queue (short retention, single consumer) forfeits replay and fan-out, the main reasons to pick it.

- **Partitioned, replicated topic model**: a topic is split into partitions spread across brokers; each event is appended to exactly one partition; events with the same key go to the same partition; each partition is replicated to multiple brokers.
  - When to use: as the core reasoning model for scalability, ordering and fault tolerance.
  - How: (a) choose a key that matches your ordering unit (customer ID, vehicle ID) — ordering is guaranteed only per partition, so same-key events stay ordered; (b) choose partition count for parallelism — clients read/write many brokers at once; (c) choose replication factor (commonly **3** in production) — replication is per topic-partition and can span datacenters/regions.
  - Why it works: partitions give horizontal parallelism; key-to-partition affinity gives per-entity ordering; replication lets surviving brokers take over without data loss. Failure mode: expecting global ordering across a topic — Kafka only orders within a partition.

- **Producer/consumer decoupling**: producers and consumers are fully agnostic of each other; producers never wait for consumers.
  - When to use: when designing services that must scale independently or be added/removed without coordination.
  - How: producers write to topics; any number (zero to many) of consumers subscribe independently; consumption does not delete events, so new consumers can start later and read history.

- **Five core Java/Scala APIs** (plus CLI tools for admin): Admin, Producer, Consumer, Kafka Streams, Kafka Connect.
  - When to use: pick the lowest-effort API that covers the need — reuse a community connector before writing a Connect plugin; use Streams before hand-rolling stateful consumer logic.

## Key Concepts
- **Event (record/message)**: a fact that "something happened", with a key, value, timestamp, and optional metadata headers.
- **Producer**: a client application that publishes (writes) events to topics.
- **Consumer**: a client application that subscribes to (reads and processes) events from topics.
- **Topic**: a named, durable, multi-producer/multi-subscriber stream of events (analogy: a folder whose files are events); events are not deleted on consumption.
- **Retention**: per-topic setting for how long events are kept before old ones are discarded; long retention is fine because performance is effectively constant with respect to data size.
- **Partition**: a "bucket" of a topic hosted on a broker; the unit of append, ordering, parallelism and replication.
- **Replication factor**: number of copies of each topic-partition across brokers (production norm: 3).
- **Broker**: a server forming Kafka's storage layer; a cluster has one or more and can span datacenters or cloud regions.
- **Kafka Connect**: servers/framework running reusable connectors that import/export event streams to external systems (e.g., capture every change from PostgreSQL tables).
- **Kafka Streams**: Java library (since 0.10.0.0) for stream processing — transformations, aggregations, joins, windowing, event-time processing — reading from and writing to topics.

## Mental Models
- **Think of Kafka as the central nervous system** of a software-defined business: capture events from databases, sensors, devices, cloud services and apps; store them; react in real time and retrospectively; route them to destinations.
- **Think of a topic as a log, not a queue**: reading is non-destructive, so "who consumed it" is the consumer's state, not the broker's. Use this when reasoning about replay, new consumers, and reprocessing.
- **Use the event key as your ordering and locality unit**: whatever must be processed in order (all events for one customer/vehicle) must share a key.
- **Think of processing pipelines as graphs of topics**: each stage consumes topics and produces new topics (raw → cleansed → enriched → recommended).

## Anti-patterns
- **Assuming consumption deletes data**: events stay until retention expires; capacity planning must be driven by retention, not consumer lag alone.
- **Expecting topic-wide ordering**: only per-partition order is guaranteed; unkeyed or wrongly keyed events lose per-entity order.
- **Running production topics with a single replica**: a broker failure or maintenance makes data unavailable or lost; replicate (commonly RF=3).
- **Writing a custom connector first**: hundreds of community connectors already exist; build your own only when none fits.
- **Coupling producers to consumers** (e.g., producers waiting on consumer acknowledgment of business processing): defeats the decoupling that gives Kafka its scalability.

## Code Examples
```bash
# Create a "payments" topic: 4 partitions for parallelism, 3 replicas for fault tolerance
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic payments --partitions 4 --replication-factor 3
```
- **What it demonstrates**: the partitioned + replicated topic model in one command (see ch02/ch10 for full tooling).

```java
// Keyed event: all of Alice's payments land in the same partition, so they stay ordered
producer.send(new ProducerRecord<>("payments", "Alice", "Made a payment of $200 to Bob"));
```
- **What it demonstrates**: the key → partition affinity that gives per-entity ordering (Producer API details in ch03).

## Reference Tables

**Kafka core APIs**

| API | Purpose | Use when |
|---|---|---|
| Admin | Manage/inspect topics, brokers, other objects | Automating topic/cluster management from code |
| Producer | Publish events to one or more topics | Writing events from an application |
| Consumer | Subscribe to and process events from topics | Reading events with your own processing logic |
| Kafka Streams | Higher-level stream processing (stateful ops, joins, windows, event-time) | Topic-in → topic-out transformations and microservices |
| Kafka Connect | Reusable import/export connectors to external systems | Integrating databases, other clusters, SaaS — prefer existing connectors |

**Use cases**

| Use case | Profile | Kafka's edge / comparable systems |
|---|---|---|
| Messaging | Often low-throughput, needs low end-to-end latency and strong durability | Higher throughput, built-in partitioning, replication; vs ActiveMQ, RabbitMQ |
| Website activity tracking | Very high volume; original Kafka use case; one topic per activity type | Real-time pub-sub feeds feeding processing, monitoring, Hadoop/warehouse |
| Metrics | Aggregating operational stats into centralized feeds | Central operational-data pipeline |
| Log aggregation | Replace collecting log files to file server/HDFS | Stream abstraction, lower latency, replication; vs Scribe, Flume |
| Stream processing | Multi-stage pipelines topic → transform → topic | Kafka Streams (since 0.10.0.0); alternatives Apache Storm, Apache Samza |
| Event sourcing | State changes logged as time-ordered records | Supports very large stored log data |
| Commit log | External commit log for a distributed system; re-sync failed nodes | Log compaction; similar to Apache BookKeeper |

## Worked Example
**Payments event flow.** A bank publishes payment events to a `payments` topic with 4 partitions (P1–P4) and replication factor 3.
1. Two independent producer services write events over the network. Event `{key: "Alice", value: "Made a payment of $200 to Bob", timestamp: Jun 25 2020 14:06}` is hashed by key to, say, P2; every later "Alice" event also goes to P2. Both producers may write into the same partition when keys collide.
2. Each partition has 3 copies on different brokers (possibly in different datacenters); if one broker fails or is taken down for maintenance, another replica serves the data with no loss.
3. A fraud-detection consumer reads P1–P4 in parallel; within P2 it sees Alice's payments in exactly the order written.
4. A week later, a new analytics service subscribes and reads the same history from the beginning — nothing was deleted when the fraud service consumed it; events disappear only when the per-topic retention expires.
5. A Kafka Streams app aggregates payments per customer into a new topic, and a Kafka Connect sink exports that topic to a data warehouse — publish, store, process and integrate on one platform.

## Key Takeaways
1. Evaluate Kafka by the three capabilities — publish/subscribe, durable storage, processing; its value peaks when you need all three.
2. Pick event keys deliberately: they determine partition placement and therefore the only ordering guarantee you get.
3. Size partitions for parallelism and use replication factor 3 as the production default.
4. Set retention per topic based on replay needs; long retention is cheap in performance terms.
5. Reach for existing connectors and Kafka Streams before writing custom integration or stateful consumer code.
6. Use log compaction (ch07) when Kafka serves as a commit log or state store backend.

## Connects To
- **ch02**: hands-on quickstart to create topics and produce/consume the events introduced here.
- **ch03**: Producer, Consumer, Admin, Streams and Connect APIs in detail.
- **ch04**: per-topic configs such as retention.
- **ch05**: why performance is constant with data size (persistence and efficiency design).
- **ch06**: delivery guarantees and exactly-once processing mentioned here.
- **ch07**: replication mechanics and log compaction for the commit-log use case.
- **ch11**: replicating across datacenters/regions (MirrorMaker 2).
- **ch21**: Kafka Connect for import/export of data.
- **ch23–ch24**: Kafka Streams for the stream-processing use case.
