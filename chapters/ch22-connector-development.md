# Chapter 22: Kafka Connect — Connector Development Guide

## Core Idea
A connector never copies data itself: the `Connector` splits a job into task configs, and each `Task` maps its slice of the external system onto streams of schema'd key-value records carrying a **source partition + source offset**, so the framework can commit progress and resume. Implement correctly by keeping lifecycle methods trivial, making offsets seekable, and letting the framework own offset commits, transactions and validation.

## Frameworks Introduced
- **Connector / Task split**: `Connector` describes *what* to copy and returns `List<Map<String,String>> taskConfigs(int maxTasks)`; `Task` (`SourceTask` / `SinkTask`) does the copying on a worker.
  - When to use: every connector, source or sink.
  - How: (1) extend `SourceConnector`/`SinkConnector`; (2) register the class in a ServiceLoader manifest `META-INF/services/org.apache.kafka.connect.source.SourceConnector` (FQCN on one line); (3) implement `taskClass()`, `start(props)`, `stop()`, `config()`; (4) in `taskConfigs(maxTasks)` discover the input units (tables, files), divide them into ≤ `maxTasks` groups (helpers in `ConnectorUtils`), and emit one config map per task — task configs may differ from or extend the connector config.
  - Why it works / failure mode: the framework can redistribute task configs across workers without knowing the external system. Returning more task configs than `maxTasks`, or putting heavy work in `start()`, breaks this contract.
- **Pull source vs push sink**: `SourceTask.poll()` *pulls* and returns `List<SourceRecord>`; `SinkTask.put(Collection<SinkRecord>)` gets records *pushed* to it, and `flush(Map<TopicPartition, OffsetAndMetadata>)` marks the commit point.
  - When to use: pick the side that matches your data direction; the interfaces are deliberately different.
  - How (source): each `poll()` reads up to a batch size and builds `new SourceRecord(sourcePartition, sourceOffset, topic, valueSchema, value)` (other overloads add output partition, key, headers). Return `null`/empty when no data. Blocking or `Thread.sleep` is fine because each task gets a **dedicated thread**; make `stop()` `synchronized` because another Worker thread calls it to interrupt a blocked task.
  - How (sink): `put()` may buffer and return before data is durable; `flush()` must push everything outstanding and **block until acknowledged**. Use the `currentOffsets` argument only when storing offsets in the destination for exactly-once (e.g. HDFS atomic move of data+offsets).
  - Why it works / failure mode: offset commits are only safe if `flush()` truly waits; a non-blocking `flush()` lets the framework commit offsets for data that can still be lost.
- **Offset resume via `OffsetStorageReader`**: framework commits source offsets automatically; only the task knows how to seek.
  - How: in `initialize()`/`start()`, call `context.offsetStorageReader().offset(sourcePartitionMap)`, read your field (e.g. `"position"`), and seek. For many partitions use the bulk `offsets(...)` read, then seek each stream.
- **Exactly-once source (KIP-618, since 3.3.0)**: framework wraps each `poll()` batch in a Kafka transaction by default; with `transaction.boundary=connector` the task controls boundaries via `context.transactionContext()` (`commitTransaction()`, `commitTransaction(record)`, abort variants).
  - Prerequisite: meaningful per-record source offsets and exact resume at any of them with no drops/duplicates.
  - Preflight: implement `exactlyOnceSupport(props)` → `ExactlyOnceSupport.SUPPORTED`/`UNSUPPORTED` (consulted when user sets `exactly.once.support=required`); implement `canDefineTransactionBoundaries(props)` → `ConnectorTransactionBoundaries.SUPPORTED` only if the connector can sometimes define boundaries.
- **Dynamic reconfiguration**: the `Connector` monitors the external system (on its own thread if needed) and calls `context.requestTaskReconfiguration()`; the framework re-calls `taskConfigs()`, lets tasks commit, and restarts them.
- **Config validation via `ConfigDef`**: expose `config()` returning a `ConfigDef` (name, type, default, `Validator`, importance, doc, group, order, width, display name, dependents, `Recommender`); `Connector.validate()` returns errors + recommended values and can be overridden for cross-field checks.

## Key Concepts
- **SourceConnector / SinkConnector**: plugin classes that import into / export from Kafka and generate task configs; they never move data.
- **Source partition**: a `Map<String,?>` identifying an input stream (e.g. `{"filename": ...}`, a table); the key under which offsets are stored.
- **Source offset**: a `Map<String,?>` position within the source partition (e.g. `{"position": byteOffset}`, last-seen timestamp column).
- **`SourceTask.commit()` / `commitRecord()`**: optional hooks to acknowledge messages in source systems that have acks — `commit()` blocks and acks up to offsets returned by `poll`; `commitRecord()` acks each record after it is written to Kafka. Implement at most one; not needed for offset tracking.
- **`ErrantRecordReporter`**: obtained via `context.errantRecordReporter()` in a sink task; `report(record, e)` routes a bad record to the DLQ/error reporting; `null` when error reporting is disabled.
- **`TransactionContext`**: available from `SourceTaskContext` only when `transaction.boundary=connector`; otherwise `context.transactionContext()` returns `null`.
- **`Schema` / `Struct` / `SchemaBuilder`**: the Connect `data` API for structured, serialization-agnostic records; converters handle the wire format.
- **`ConnectorUtils`**: helpers for the common "split N units into M groups" pattern in `taskConfigs()`.
- **`Recommender`**: `ConfigDef` hook that computes valid values and visibility of a config from other config values.

## Mental Models
- Think of a connector as a **planner** and tasks as **workers**: anything about "which tables/files exist" lives in the `Connector`; anything about "read/write these rows" lives in the `Task`.
- Think of every external system as **streams of schema-consistent records with an offset**. If you cannot name the stream ID and offset (file + byte position, table + timestamp column), you have not finished the design.
- Use framework-managed transactions (default per-`poll()` batch) unless one logical unit (a file/object) spans several records that must land atomically, or records can't have unique offsets — then use `transaction.boundary=connector`.
- Think of `flush()` as the **durability barrier** of a sink: everything before it is a buffer, everything after it is committed.

## Anti-patterns
- **Doing work in `start()`/`stop()`**: they should only allocate/free resources; long-running logic blocks lifecycle transitions.
- **Non-synchronized source `stop()`**: the task thread may be blocked in `poll()`; stop is called from another thread and must safely close the stream.
- **Recomputing schemas per record**: build fixed schemas once, statically, and reuse the instance; only rebuild when the source schema actually changes (e.g. `ALTER TABLE`).
- **Sink silently accepting mismatched schemas**: throw an exception so the system surfaces bad upstream data instead of writing garbage.
- **Assuming the reporter exists**: `errantRecordReporter()` can be `null` (DLQ off) or throw `NoSuchMethodException`/`NoClassDefFoundError` on runtimes older than 2.6 — guard both and fall back to `throw new ConnectException(...)`.
- **Racing sink tasks creating the same downstream resource**: when a new topic appears (e.g. regex subscription), several `SinkTask`s may try to create the same table at once — make creation idempotent.
- **Claiming exactly-once without exact seek**: if the source cannot resume at precisely any emitted offset, return `ExactlyOnceSupport.UNSUPPORTED`.

## Code Examples
```java
// SourceTask.poll(): build records with partition + offset
Map<String, Object> sourcePartition = Collections.singletonMap("filename", filename);
Map<String, Object> sourceOffset   = Collections.singletonMap("position", streamOffset);
records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
if (records.size() >= batchSize) return records;

// resume on start
Map<String, Object> offset = context.offsetStorageReader()
    .offset(Collections.singletonMap("filename", filename));
if (offset != null && offset.get("position") != null) seekToOffset(stream, (Long) offset.get("position"));
```
- **What it demonstrates**: the partition/offset maps you emit are exactly the keys you read back to resume; the framework stores them, you seek.

```java
// SinkTask with guarded DLQ reporting
public void start(Map<String, String> props) {
    try { reporter = context.errantRecordReporter(); }          // null if DLQ disabled
    catch (NoSuchMethodException | NoClassDefFoundError e) { reporter = null; } // < 2.6
}
public void put(Collection<SinkRecord> records) {
    for (SinkRecord r : records) {
        try { process(r); }
        catch (Exception e) {
            if (reporter != null) reporter.report(r, e);
            else throw new ConnectException("Failed on record", e);
        }
    }
}
```
- **What it demonstrates**: per-record error routing that degrades gracefully across runtime versions and configs.

```java
// Connector-defined transaction boundary: commit exactly every 10th record
for (SourceRecord record : records)
    if (++recordsSent % 10 == 0)
        context.transactionContext().commitTransaction(record);

static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to");
```
- **What it demonstrates**: `commitTransaction(record)` commits up to and including that record (vs `commitTransaction()` committing after the whole batch); `ConfigDef` with a required, validated field powers REST preflight validation.

## Reference Tables

| API / config | Side | Contract | When to implement / set |
|---|---|---|---|
| `taskConfigs(int maxTasks)` | Connector | ≤ `maxTasks` maps, one per task | Always |
| `poll()` | SourceTask | Return batch of `SourceRecord`, may block | Always (source) |
| `put(Collection<SinkRecord>)` | SinkTask | May buffer; need not be durable on return | Always (sink) |
| `flush(currentOffsets)` | SinkTask | Write outstanding data, block until acked | When buffering; use offsets for EOS-in-destination |
| `commit()` / `commitRecord()` | SourceTask | Ack in source system (bulk / per record) | Only for sources with ack mechanisms; pick one |
| `config()` / `validate()` | Connector | Expose `ConfigDef`; return errors + recommendations | Always `config()`; override `validate()` for cross-field rules |
| `exactlyOnceSupport(props)` | SourceConnector | `SUPPORTED` / `UNSUPPORTED` | Always for sources (even to say unsupported) |
| `canDefineTransactionBoundaries(props)` | SourceConnector | `ConnectorTransactionBoundaries.SUPPORTED` | Only if it can sometimes define boundaries |
| `transaction.boundary` | connector config | default: per `poll()` batch; `connector` = task-defined | Set `connector` for atomic multi-record units |
| `exactly.once.support` | connector config | `required` makes framework call `exactlyOnceSupport` | Users who must have EOS |

## Worked Example
**FileStream-style source connector, end to end.**
1. Package `com.example.FileStreamSourceConnector extends SourceConnector`; add `META-INF/services/org.apache.kafka.connect.source.SourceConnector` containing `com.example.FileStreamSourceConnector`.
2. Define `CONFIG_DEF`: `file` (STRING, default `null` → stdin), `topic` (STRING, `NO_DEFAULT_VALUE`, `NonEmptyString`, HIGH), batch size (INT, default, LOW); return it from `config()` so `validate()` gives preflight feedback.
3. `start(props)`: store props, parse with `new AbstractConfig(CONFIG_DEF, props)`, log the filename. `stop()`: nothing (no background monitor).
4. `taskClass()` returns `FileStreamSourceTask.class`; `taskConfigs(maxTasks)` returns a single-element list with `props` because one file = one stream, regardless of `maxTasks`.
5. Task `start()`: read file/topic/batch size, open the stream, then read `offsetStorageReader().offset({"filename": f})` and seek to `"position"`. `stop()` is `synchronized` and closes the stream.
6. `poll()`: loop reading lines; for each, emit `SourceRecord({"filename": f}, {"position": off}, topic, STRING_SCHEMA, line)`; return when batch size reached; `sleep(1)` when no data; on `IOException` (stream closed by `stop`) return `null`.
7. Framework commits offsets periodically; after a crash or restart, step 5 resumes from the last committed byte position, minimizing duplicates. To make it exactly-once, return `ExactlyOnceSupport.SUPPORTED` (byte offsets are exact) and let workers run with EOS enabled.

## Key Takeaways
1. Design the (source partition, source offset) pair first — resume, exactly-once and dedup all depend on it.
2. Keep `start()`/`stop()` to resource allocation; make source `stop()` thread-safe.
3. In sinks, buffer in `put()` but make `flush()` a blocking durability barrier.
4. Always implement `exactlyOnceSupport()`; only switch to `transaction.boundary=connector` for multi-record atomic units or non-unique offsets.
5. Put change detection in the `Connector` and call `requestTaskReconfiguration()`; still make tasks handle a vanished input (dropped table) by catching the error.
6. Expose a rich `ConfigDef` (validators, recommenders) so misconfiguration fails at validation, not at runtime.
7. Cache schemas; regenerate only on real source schema change; sinks should throw on unexpected schemas.

## Connects To
- **ch21**: deploying, configuring and operating connectors (REST API, workers, error-tolerance/DLQ settings, exactly-once worker settings).
- **ch06**: transactions and delivery semantics underlying KIP-618 exactly-once source support.
- **ch03**: Connect API is one of Kafka's client APIs; producer/consumer clients used under the hood.
- **ch04**: config providers and `ConfigDef`-style configuration conventions.
- **ch09**: offsets and record format that source/sink offsets map onto.
