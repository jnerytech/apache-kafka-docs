# Chapter 06: Design — Delivery Semantics, Transactions and Share Consumer

## Core Idea
Delivery guarantees are two separate problems: producer durability (committed-to-ISR, made duplicate-free by idempotence) and consumer position (when you save the offset relative to processing). Kafka gives exactly-once for read-process-write *within Kafka* by having the transactional producer commit the consumer's offsets atomically with its output, and the share consumer adds queue-style, per-record acknowledged consumption beyond the partition count. The source is short (~16 KB), so this chapter is compact.

## Frameworks Introduced
- **The three delivery semantics, decomposed**: *at most once* (may lose, never redeliver), *at least once* (never lose, may redeliver), *exactly once* (processed once and only once). Each is the combination of a publish-side guarantee and a consume-side guarantee.
  - When to use: choosing a guarantee per pipeline; checking vendor "exactly-once" claims (do they still hold when producers or consumers fail, with multiple consumer processes, or when disk writes are lost?).
  - How:
    1. Publish side: a message is *committed* when every ISR replica has applied it. Once committed, it survives while at least one replica broker stays alive.
    2. A producer network error is ambiguous: the write may or may not have been committed. Plain retry gives at-least-once. Idempotent producer (since 0.11.0.0) makes retries duplicate-free.
    3. Consume side: *save position → process* = at-most-once. *Process → save position* = at-least-once.
    4. Exactly-once = put the offset in the same atomic write as the output: a Kafka transaction for Kafka sinks, or offsets stored alongside the data for external sinks.
  - Kafka defaults to **at-least-once**. Get at-most-once by disabling producer retries and committing offsets *before* processing a batch.
  - Why it works / failure mode: the crash window between "save position" and "save output" decides which side you err on. Exactly-once closes that window by making them one write. Note that at-least-once is often fine when records carry a primary key, because a redelivery just overwrites the same row (idempotent updates).

- **Idempotent producer**: the broker gives each producer a producer ID and deduplicates using a per-message sequence number the producer sends.
  - When to use: always when you retry (it is what turns at-least-once publishing into exactly-once publishing to the log).
  - How: the producer attaches its ID and a sequence number to each batch. The broker drops resends whose sequence it already wrote.

- **Exactly-once read-process-write (the Kafka Streams recipe with plain clients)**: only the *producer* is transactional. It transactionally updates the consumer's committed offset (stored as a message in an internal topic) together with the output records.
  - When to use: consume from topic A → transform → produce to topic B without Streams. Prefer Kafka Streams when possible because it is the simplest path to exactly-once.
  - How (3 pillars + config):
    1. Partition assignment makes each consumer the sole processor of its partitions in the group.
    2. The producer wraps output records **and** offset updates in one transaction.
    3. Use **one producer instance per consumer instance** so transactions behave correctly across rebalances. Fancier sharing schemes exist but cost complexity.
    4. Consumer: `isolation.level=read_committed`, `enable.auto.commit=false`. Producer: `transactional.id=<name>`. Only the producer has `transactional.id`. It enables transactional delivery and makes a restarted instance abort the previous instance's in-flight transaction (fencing).
  - Why it works / failure mode: commit/abort writes **transaction marker records**, and read_committed consumers filter out aborted records using those markers. Failure mode: after an abort the offset in Kafka reverts, but the in-memory consumer position does **not** rewind. If you don't reset it explicitly, you skip the aborted batch.

- **Transactional error taxonomy (standardized)**: classify every producer exception and act by class.
  - When to use: writing the catch blocks of a transactional loop. See the `TransactionalClientDemo.java` template in the Kafka repo.
  - How: see the Reference Table below. Rule of thumb: *Abortable* → abort + rewind. *ApplicationRecoverable* → recover and **restart the producer**. *Retriable/RefreshRetriable* → the client handles them and you never see them.

- **Abort recovery strategies**:
  - *Simple*: discard and recreate the producer and consumer. The rebalance re-fetches the last committed offset, which rewinds you implicitly.
  - *Sophisticated* (message-copier style): keep the objects. Call `KafkaConsumer.committed(...)` to fetch the committed offsets, then `KafkaConsumer.seek(...)` to rewind.

- **Share groups / share consumer (queue semantics)**: a separate group type alongside consumer groups. Consumers cooperatively consume. A partition may be served by many consumers. Records are acquired under time-limited locks and acknowledged individually.
  - When to use: traditional work-queue workloads. Use it when you need more consumers than partitions, per-record ack/retry, or automated handling of poison records. Use it when you don't need per-partition ordering.
  - How:
    1. A fetch acquires available records from any subscribed partition under an **acquisition lock**. The lock lasts `share.record.lock.duration.ms` (default 30 s, a group config).
    2. While locked, the record is invisible to the other consumers in that share group.
    3. The holder then does one of these: **acknowledge** (success), **release** (make it available for another attempt), **reject** (unprocessable, no more attempts), **renew** (extend the lock because it is still processing), or nothing (the lock expires and the record is released).
    4. Delivery attempts are **counted**, which lets the broker handle unprocessable records automatically.
    5. Back-pressure: the broker caps acquired records per partition per share group (`group.share.partition.max.record.locks`). At the cap, fetches return nothing until locks clear.

## Key Concepts
- **Committed message**: a record applied by all replicas in the partition's ISR. It is durable while at least one replica broker is alive.
- **Producer ID + sequence number**: the broker-side deduplication key behind the idempotent producer.
- **Transactional producer**: an atomic write across multiple partitions (all or none). Available since 0.11.0.0.
- **`transactional.id`**: a stable producer identity. It enables transactions and lets a restarted instance abort its predecessor's open transaction.
- **Committed offset**: the consumer position stored as a message in an internal topic. Because it is stored there, a producer can update it inside a transaction.
- **Transaction markers**: commit/abort control records written to every partition a transaction touched. read_committed filtering relies on them.
- **`isolation.level`**: `read_uncommitted` (default) returns records from aborted and still-open transactions. `read_committed` returns only committed transactional records plus non-transactional ones.
- **Share group / share consumer**: a group type with cooperative, per-record consumption. Consumers may outnumber partitions. Each share group consumes a topic independently of other groups.
- **Acquisition lock**: a time-limited exclusive claim on a record within a share group. It is released automatically on expiry.
- **Delivery count**: the number of delivery attempts per record in a share group. It drives automated handling of unprocessable records.

## Mental Models
- Think of exactly-once as **"offset and output in one atomic write"**. Inside Kafka that write is a transaction. Toward an external store, write the offset *into the same store* as the data, e.g. a Connect HDFS sink that stores offsets with the files. This avoids two-phase commit, which most sinks don't support anyway.
- Think of a network error on produce like **an insert with an autogenerated key**: you can't tell whether it landed. Idempotence is what makes retrying safe.
- Use **consumer groups when you need partition-ordered, scalable streams**. Use **share groups when you need a work queue**: consumers > partitions, per-record ack, and retry/poison handling, with ordering given up.
- Think of the **share lock duration as the visibility timeout** of a queue. If you process longer than it, either renew the lock or accept redelivery to someone else.

## Anti-patterns
- **Trusting "exactly-once" marketing without failure fine print**: claims often fail once you add producer/consumer crashes, multiple consumers, or lost disk writes.
- **Committing offsets before processing while expecting no loss**: that is at-most-once by construction.
- **Transactional pipeline with `read_uncommitted` consumers downstream**: they see aborted and open-transaction records, so the guarantee is lost at the reader.
- **Leaving `enable.auto.commit=true` in an EOS loop**: auto-commit moves offsets outside the transaction.
- **Not rewinding after `abortTransaction()`**: the consumer's in-memory position doesn't revert, so the aborted batch is silently skipped.
- **Sharing one producer across many consumers naively**: transactions get tangled with rebalances. Use one producer per consumer unless you've designed for it.
- **Two-phase commit to external sinks**: most sinks don't support 2PC. Co-locate the offsets with the output instead.
- **Holding share-acquired records past the lock without renewing**: the record is redelivered to another consumer, so the work is duplicated.

## Code Examples
```properties
# consumer (EOS read side)
isolation.level=read_committed
enable.auto.commit=false
# producer (EOS write side)
transactional.id=copier-instance-1   # stable per consumer instance
```
- **What it demonstrates**: the minimum config contract for exactly-once with plain clients. Only the producer carries `transactional.id`.

```java
producer.initTransactions();
while (running) {
  ConsumerRecords<K,V> recs = consumer.poll(Duration.ofMillis(200));
  try {
    producer.beginTransaction();
    for (var r : recs) producer.send(new ProducerRecord<>("out", r.key(), transform(r.value())));
    producer.sendOffsetsToTransaction(nextOffsets(recs), consumer.groupMetadata());
    producer.commitTransaction();
  } catch (AbortableException e) {           // must abort + rewind
    producer.abortTransaction();
    var committed = consumer.committed(consumer.assignment());
    committed.forEach((tp, om) -> consumer.seek(tp, om == null ? 0 : om.offset()));
  } catch (ApplicationRecoverableException e) {
    // recovery MUST include closing and recreating the producer
  }
}
```
- **What it demonstrates**: the transactional-copier loop. Offsets go to the transaction with the consumer's group metadata, and an abort is followed by an explicit `committed()` + `seek()` rewind. A simpler alternative is to recreate both clients. The exception class names follow the source's taxonomy. Use `TransactionalClientDemo.java` for the canonical handling.

## Reference Tables

| Config | Scope | Default | Meaning | When to change |
|---|---|---|---|---|
| `isolation.level` | consumer | `read_uncommitted` | Whether aborted/open transactional records are returned | Set `read_committed` for any EOS reader |
| `enable.auto.commit` | consumer | `true` | Background offset commits | `false` in transactional loops |
| `transactional.id` | producer | unset | Enables transactions and fences old instances | Set a stable per-instance ID for EOS |
| producer retries | producer | on | Resend on ambiguous errors | Disable only for deliberate at-most-once |
| `share.record.lock.duration.ms` | share group config | 30000 | Acquisition lock lifetime | Raise for slow processing (or renew per record) |
| `group.share.partition.max.record.locks` | broker | (see ch04) | Max acquired records per partition per share group | Raise to boost in-flight parallelism; lower for back-pressure |

| Semantics | Producer side | Consumer side |
|---|---|---|
| At most once | retries disabled | commit offset → then process |
| At least once (default) | retries on | process → then commit offset |
| Exactly once (Kafka→Kafka) | `transactional.id` + offsets in txn | `read_committed`, no auto-commit |
| Exactly once (external sink) | — | store offset atomically with output in the sink |

| Exception class | Bubbles up? | Required action |
|---|---|---|
| `RetriableException` | No | Client retries automatically |
| `RefreshRetriableException` | No | Client refreshes metadata, then retries |
| `AbortableException` | Yes | Abort transaction, reset consumer position, reprocess |
| `ApplicationRecoverableException` | Yes | App-defined recovery that **must restart the producer** |
| `InvalidConfigurationException` | Yes | Fix config. Producer restart is optional |
| `KafkaException` (other) | Yes | General application handling |

| | Consumer group | Share group |
|---|---|---|
| Partition ownership | exclusive per member | shared by many members |
| Max useful consumers | = partitions | can exceed partitions |
| Ack granularity | offset (position) | per record (accept/release/reject/renew) |
| Retry/poison handling | app-built | delivery counting, reject |

## Worked Example
**Exactly-once topic copier without Streams** (the `TransactionalMessageCopier` tool, run with `--use-group-metadata`):
1. Start N instances in one consumer group. Each instance pairs one `KafkaConsumer` (`read_committed`, auto-commit off) with one `KafkaProducer` that has a unique, stable `transactional.id`.
2. `initTransactions()`. If a previous incarnation with the same ID crashed mid-transaction, that transaction is aborted now.
3. Loop: poll → `beginTransaction()` → send the transformed records to the output topic → `sendOffsetsToTransaction(offsets, groupMetadata)` → `commitTransaction()`. The broker writes commit markers, and downstream `read_committed` readers now see the batch.
4. A broker error surfaces as `AbortableException`: `abortTransaction()`. Abort markers hide the partial output. The stored committed offset is unchanged, but the in-memory position is ahead, so call `consumer.committed()` and `seek()` back, then reprocess. Result: no loss and no duplicates.
5. On rebalance, partition ownership moves together with the paired producer, so no two instances process the same partition.

## Key Takeaways
1. Kafka is at-least-once by default. Get at-most-once by disabling retries and committing before processing. Get exactly-once with transactions plus `read_committed`.
2. For Kafka→Kafka exactly-once, prefer Kafka Streams. With plain clients, follow the three pillars: sole-owner assignment, a transactional producer committing offsets, and one producer per consumer.
3. Always pair `transactional.id` on the producer with `isolation.level=read_committed` and `enable.auto.commit=false` on the consumer.
4. After any abort, explicitly rewind the consumer: either recreate the clients or use `committed()` + `seek()`.
5. For external sinks, store offsets alongside the output in the sink rather than attempting two-phase commit.
6. Reach for share groups when you need queue semantics: more consumers than partitions, per-record ack/release/reject/renew, and delivery counting. Size `share.record.lock.duration.ms` to your processing time.

## Connects To
- **ch05**: producer/consumer design (batching, pull model, offset tracking) that these semantics build on.
- **ch07**: the precise definitions of ISR, "committed" and "alive", plus `acks`/`min.insync.replicas` durability.
- **ch09**: record batch layout, producer ID/sequence fields and control (marker) records.
- **ch17**: the transaction protocol hardening (KIP-890) and the new consumer rebalance protocol (KIP-848).
- **ch04**: where `share.record.lock.duration.ms` (group) and `group.share.*` (broker) configs live.
- **ch21/ch22**: Connect sinks that store offsets with output for end-to-end exactly-once.
- **ch24/ch27**: Kafka Streams exactly-once (`processing.guarantee`) as the turnkey path.
- **ch03**: the KafkaProducer, KafkaConsumer and share consumer client APIs.
