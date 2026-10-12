# Chapter 03: Client APIs Overview

## Core Idea
Kafka exposes six core APIs (Producer, Consumer, Share Consumer, Streams, Connect, Admin) over one language-independent wire protocol, but only the Java clients ship with Apache Kafka. Pick the API by the *shape of the job* and the matching Maven artifact (`kafka-clients` vs `kafka-streams`). The source is a thin index page that points to javadocs, so this chapter is shorter than the target. The usage details are in the linked chapters.

## Frameworks Introduced
- **The six-API map**: every Kafka integration fits one of these roles:
  1. **Producer API** (`KafkaProducer`): writes streams of records to topics.
  2. **Consumer API** (`KafkaConsumer`): reads streams of records from topics, as a consumer group with partition-exclusive assignment.
  3. **Share Consumer API** (`KafkaShareConsumer`): members of a *share group* consume and process records from the same topics cooperatively, like a queue.
  4. **Streams API** (`KafkaStreams`): transforms input topics into output topics, as a library embedded in your app.
  5. **Connect API** (`org.apache.kafka.connect`): connectors that continuously pull from a source system into Kafka or push from Kafka into a sink system.
  6. **Admin API** (`Admin`): manages and inspects topics, brokers, ACLs and other Kafka objects.
  - When to use: before you write any Kafka code, so you choose the highest-level abstraction that fits.
  - How: (a) moving data between Kafka and an external system: Connect first, and check for a pre-built connector before writing code; (b) topic-to-topic transformation, joins or aggregations: Streams; (c) the app *is* the source of events: Producer; (d) the app reacts to events: Consumer when you need per-partition ordering and offset-based progress, Share Consumer when you need queue-style cooperative work distribution; (e) provisioning, inspection or ACL automation: Admin.
  - Why it works / failure mode: the higher-level APIs (Streams, Connect) already handle offset management, fault tolerance and scaling that you would otherwise rebuild by hand on Producer/Consumer. The usual failure is a hand-written consume-transform-produce loop or an ad-hoc DB poller. Both would be better built on Streams or a Connect source connector.
- **Artifact selection rule**: Producer, Consumer, Share Consumer and Admin all come from **one** artifact, `org.apache.kafka:kafka-clients`. Streams is a separate artifact, `org.apache.kafka:kafka-streams`. Scala users can optionally add `kafka-streams-scala_2.13`, which is deprecated (see below).
  - When to use: when setting up dependencies.
  - How: pin every Kafka artifact to the same version (4.3.1 in these docs). Add `kafka-streams` only if you use the Streams API. Never add separate "admin" or "consumer" jars, because they don't exist.
  - Why it works / failure mode: if `kafka-clients` and `kafka-streams` are on different versions, you can get classpath conflicts. `kafka-streams` depends on `kafka-clients`.
- **Protocol-first, Java-maintained**: all functionality goes over the Kafka wire protocol (ch08). Only the Java clients are maintained in the main project. Non-Java clients (librdkafka-based, Go, Python, etc.) are independent open-source projects listed on the Kafka wiki.
  - When to use: when choosing a non-JVM language.
  - How: check that the third-party client supports the features you need, such as new protocols like share groups or the KIP-848 consumer protocol. Feature parity with Java is not guaranteed.

## Key Concepts
- **`kafka-clients`**: the single Maven artifact containing `KafkaProducer`, `KafkaConsumer`, `KafkaShareConsumer` and `Admin`.
- **`kafka-streams`**: the separate artifact for the Kafka Streams library (`KafkaStreams`), which turns input topics into output topics.
- **Share group**: a group whose members cooperatively consume the same topic-partitions, with records distributed queue-style. Its client is `KafkaShareConsumer` (semantics in ch06).
- **Consumer group**: the classic `KafkaConsumer` model, where each partition goes to exactly one member of the group (rebalance protocol in ch17).
- **Connector (source/sink)**: a Connect plugin that continuously pulls external data into Kafka (source) or pushes Kafka data out (sink). Most users configure pre-built connectors and write no code.
- **Admin API**: the programmatic interface for managing and inspecting topics, brokers, ACLs and other cluster objects (the CLI tools are built on the same capabilities).
- **Language-independent protocol**: the binary wire protocol through which Kafka exposes *all* functionality. Any language can implement a client against it.
- **`kafka-streams-scala`**: an optional Scala DSL wrapper for Streams. It is deprecated as of 4.3 and will be removed in 5.0.

## Mental Models
- Think of the six APIs as a ladder of abstraction. Producer/Consumer/Share Consumer are the primitives. Streams and Connect are frameworks built on them. Admin is the control plane. Start at the top of the ladder and drop down only when the framework gets in your way.
- Use Consumer when the *order within a partition* and the *offset position* are what matter. Use Share Consumer when *throughput of independent work items* matters more than order, and you want more consumers than partitions.
- Use Connect when the other end is a system (DB, object store, search index). Use Producer/Consumer when the other end is your own application logic.
- Think of non-Java clients as "protocol implementations of varying completeness", not as official SDKs.

## Anti-patterns
- **Writing a custom connector when a pre-built one exists**: Connect users mostly don't need the API. Custom code adds maintenance and loses community-tested offset and fault handling.
- **Hand-rolling consume→transform→produce loops**: you end up re-implementing what Streams provides (state, fault tolerance, exactly-once wiring).
- **Starting new Scala code on `kafka-streams-scala`**: it is deprecated in 4.3 and will be removed in 5.0. Call the Java Streams API directly from Scala.
- **Mixed Kafka artifact versions**: `kafka-streams` 4.3.1 next to an older `kafka-clients` leads to classpath and behavior mismatches.
- **Assuming a third-party client supports every feature**: only Java is maintained in-project, so new protocol features may arrive late or not at all.
- **Managing topics/ACLs by shelling out to CLI scripts from apps**: use the Admin API from the same `kafka-clients` dependency instead.

## Code Examples
```xml
<!-- Producer, Consumer, Share Consumer, Admin: one artifact -->
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>4.3.1</version>
</dependency>
```
- **What it demonstrates**: all four "plain client" APIs share one dependency. There's nothing extra to add for Admin or share groups.

```xml
<!-- Kafka Streams (pulls kafka-clients transitively) -->
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams</artifactId>
  <version>4.3.1</version>
</dependency>
<!-- Scala 2.13 DSL: DEPRECATED in 4.3, removed in 5.0 -->
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams-scala_2.13</artifactId>
  <version>4.3.1</version>
</dependency>
```
- **What it demonstrates**: Streams is a separate library. The Scala wrapper is optional, deprecated, and should be avoided in new code.

## Reference Tables

| API | Entry class / package | Maven artifact | Use when |
|---|---|---|---|
| Producer | `org.apache.kafka.clients.producer.KafkaProducer` | `kafka-clients` | App emits events into topics |
| Consumer | `org.apache.kafka.clients.consumer.KafkaConsumer` | `kafka-clients` | Ordered, partition-exclusive consumption in a consumer group |
| Share Consumer | `org.apache.kafka.clients.consumer.KafkaShareConsumer` | `kafka-clients` | Queue-like cooperative processing in a share group |
| Streams | `org.apache.kafka.streams.KafkaStreams` | `kafka-streams` (+ optional deprecated `kafka-streams-scala_2.13`) | Topic→topic transforms, joins, aggregations |
| Connect | `org.apache.kafka.connect` | (connector plugins; runtime ships with Kafka) | Moving data between Kafka and external systems |
| Admin | `org.apache.kafka.clients.admin.Admin` | `kafka-clients` | Manage/inspect topics, brokers, ACLs, other objects |

| Decision | Choose | Not |
|---|---|---|
| Need more parallel workers than partitions | Share Consumer | Consumer |
| Need per-key ordering | Consumer | Share Consumer |
| DB → Kafka ingestion | Connect source connector (pre-built) | Producer polling loop |
| Scala Streams app (new) | Java Streams API from Scala | `kafka-streams-scala` |
| Non-JVM language | Third-party client (check feature support) | Assuming parity with Java |

## Worked Example
**Designing an order-processing service on Kafka 4.3.1.**
1. *Ingest orders from Postgres*: this is external system → Kafka, so use **Connect** with a pre-built source connector. You write no Connect API code.
2. *Enrich orders with customer data and compute per-customer totals*: this is topic→topic with state, so use the **Streams API**. Add the `kafka-streams:4.3.1` dependency, which brings `kafka-clients` with it. The team writes Scala, so call the Java `KafkaStreams` API directly rather than adding the deprecated `kafka-streams-scala_2.13`.
3. *Send confirmation emails*: each order is an independent work item, ordering doesn't matter, and the team wants to scale to more workers than partitions. Use **`KafkaShareConsumer`** in a share group (from `kafka-clients`).
4. *Fraud check needing strict per-account ordering*: use a classic **`KafkaConsumer`** in a consumer group, keyed by account.
5. *Bootstrap topics and ACLs at deploy time*: use the **Admin API** (`Admin`) from the same `kafka-clients` dependency, rather than CLI scripts.
6. *A Go reporting service*: use a third-party Go client from the non-Java client list on the wiki. Verify that it supports the protocol features you rely on.

## Key Takeaways
1. Map each job to one of the six APIs before coding. Prefer Connect or Streams over hand-written producer/consumer loops.
2. `kafka-clients` covers Producer, Consumer, Share Consumer and Admin. Add `kafka-streams` only for Streams, on the same version.
3. Choose Share Consumer for queue-style parallelism beyond the partition count, and Consumer for ordered, offset-tracked consumption.
4. Most Connect users never touch the Connect API. Check for a pre-built connector first.
5. `kafka-streams-scala` is deprecated in 4.3 and will be removed in 5.0, so migrate to the Java API from Scala now.
6. Only the Java clients are maintained in-project. Vet non-Java clients for the features you need.

## Connects To
- **ch02**: quickstart exercises these clients end-to-end and covers client/broker compatibility.
- **ch04**: producer, consumer, admin, streams and connect configuration reference.
- **ch05**: producer/consumer design (batching, pull model, offsets) behind these APIs.
- **ch06**: delivery semantics, transactions, and share-consumer semantics.
- **ch08**: the language-independent wire protocol all clients implement.
- **ch17**: KIP-848 consumer rebalance protocol used by `KafkaConsumer` groups.
- **ch20**: ACLs that the Admin API manages.
- **ch21–22**: Connect usage and the Connect API for custom connector development.
- **ch23–29**: the Streams API in depth.
