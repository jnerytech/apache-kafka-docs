# Chapter 21: Kafka Connect — Overview, User Guide and Administration

## Core Idea
Kafka Connect is a config-driven framework that moves data between Kafka and external systems: workers (standalone or a distributed group) run connectors split into tasks, the framework owns offsets, serialization (converters), lightweight per-record transforms (SMTs), error handling and rebalancing, and everything is operated through a REST API. Run it distributed with hand-created internal topics, lock down the REST API and client-override policy, and choose error tolerance and exactly-once deliberately per connector.

## Frameworks Introduced
- **Standalone vs distributed execution**: two worker modes with the same connector model.
  - When to use: standalone for single-worker jobs (e.g. tailing a local log file), dev and tests; distributed for anything needing fault tolerance, scaling, or exactly-once source support.
  - How: standalone = `bin/connect-standalone.sh worker.properties [c1.properties c2.json ...]`, offsets in a local file (`offset.storage.file.filename`), connectors on the command line or via REST. Distributed = `bin/connect-distributed.sh worker.properties`; workers sharing a `group.id` form a cluster via Kafka's group protocol and store configs, offsets and statuses in three compacted topics; connectors are created only via REST.
  - Why it works / failure mode: state lives in Kafka, so any worker can take over any task. If you let Connect auto-create the internal topics, they get broker-default partitions/RF, which is usually wrong (e.g. RF 1, or a config topic with >1 partition).
- **Three client config scopes**: worker-level clients (internal topics) vs source-task producers vs sink-task consumers.
  - When to use: whenever you secure the cluster or tune task clients.
  - How: unprefixed worker configs apply only to the internal-topic clients; `producer.*` applies to all source tasks; `consumer.*` to all sink tasks; `producer.override.*` / `consumer.override.*` in a connector config override per connector (gated by `connector.client.config.override.policy`). Only `bootstrap.servers` is inherited without a prefix, so security settings must be repeated up to three times.
- **Transformation chain + predicates (SMTs)**: ordered, message-at-a-time modifications.
  - When to use: light data massaging and routing (wrap, add/drop fields, mask, reroute topics, filter). Not for joins/aggregations — use Kafka Streams.
  - How: `transforms=A,B` (order = execution order), `transforms.A.type=<class>`, `transforms.A.<cfg>=...`. Conditional application: define `predicates=P`, `predicates.P.type=<class>`, then set `transforms.A.predicate=P` and optionally `transforms.A.negate=true`. Combine `Filter` + predicate to drop records.
- **Error handling ladder**: fail-fast by default; escalate to retry → tolerate → log → DLQ.
  - When to use: any production connector where a poison record should not stop the pipeline.
  - How: `errors.retry.timeout` + `errors.retry.delay.max.ms` for transient failures; `errors.tolerance=all` to skip; `errors.log.enable=true` (+ `errors.log.include.messages=true` only if logs may hold payloads); `errors.deadletterqueue.topic.name` (+ `errors.deadletterqueue.context.headers.enable=true`) to park bad records (DLQ applies to sink connectors). Covers converter, transform, and sink-put errors.
  - Why it works / failure mode: `errors.tolerance=all` without logging or a DLQ silently drops data.
- **Incremental cooperative rebalancing** (default since 2.3.0, `connect.protocol=compatible`): only new, removed, or moving connectors/tasks are stopped/started; a departed worker's tasks wait `scheduled.rebalance.max.delay.ms` (default 300000 = 5 min) for it to return before reassignment. `connect.protocol=eager` restores the old stop-the-world rebalance.
- **Plugin discovery modes** (`plugin.discovery`, 3.6+): `only_scan` (legacy, slow, always compatible) → `hybrid_warn` (default; scans and warns about ServiceLoader-incompatible plugins) → `hybrid_fail` (fails startup if any incompatible) → `service_load` (fastest; incompatible plugins may be unusable).
  - How to migrate: verify with `hybrid_warn` logs or a test worker on `hybrid_fail`; fix by upgrading the plugin, asking the vendor, or running `bin/connect-plugin-path.sh` (`list` → `sync-manifests --dry-run` → `sync-manifests`, after backup); developers add `META-INF/services/<plugin-superclass>` manifests.

## Key Concepts
- **Worker**: a Connect JVM process that hosts connectors and tasks; in distributed mode it joins the cluster identified by `group.id`.
- **Connector / task**: a connector holds configuration and splits work into up to `tasks.max` tasks, which do the actual copying; it may create fewer if parallelism is limited.
- **Converter**: `key.converter` / `value.converter` translate between Connect's internal data format and the bytes in Kafka (JSON, Avro...), independent of the connector; overridable per connector.
- **Internal topics**: `config.storage.topic` (1 partition, replicated, compacted), `offset.storage.topic` (many partitions, replicated, compacted), `status.storage.topic` (multiple partitions, replicated, compacted).
- **Predicate**: a condition (`TopicNameMatches`, `HasHeaderKey`, `RecordIsTombstone`) that gates a transform; reversed via the transform's `negate`.
- **Dead letter queue (DLQ)**: a Kafka topic receiving records that failed conversion/transform/sink processing, optionally with error context in headers.
- **Pause vs stop**: pause keeps tasks idle with resources allocated (fast resume, state persists across restarts); stop (3.5.0+) shuts tasks down and frees resources and is required before editing offsets.
- **Offsets management (KIP-875)**: `GET/PATCH/DELETE /connectors/{name}/offsets`; alter/reset only while the connector is STOPPED. Sink offset format is common; source format is connector-specific.
- **Topic tracking**: since 2.5.0 the status topic records which topics each connector uses (`GET /connectors/{name}/topics`, reset via `PUT .../topics/reset`).
- **Connector states**: UNASSIGNED, RUNNING, PAUSED, STOPPED (connector only), FAILED, RESTARTING.

## Mental Models
- Think of a distributed Connect cluster as a **consumer group whose members are workers and whose "partitions" are tasks**; all durable state is in three compacted Kafka topics, so workers are disposable.
- Think of the REST API as **remote code execution by design**: whoever can POST a connector can load any installed plugin and, with permissive overrides, any class (e.g. via `sasl.jaas.config`). Secure it like an admin shell.
- Use SMTs when the change touches one record at a time; once you need state, lookups or multiple records, move the logic to Kafka Streams.
- Use **stop** for maintenance windows and offset surgery; use **pause** for brief interruptions where fast resume matters.

## Anti-patterns
- **Letting Connect auto-create internal topics**: they get default partitions/RF; a config topic with multiple partitions or RF 1 risks ordering issues or data loss.
- **Reusing a consumer group ID as the Connect `group.id`**: it must not conflict with consumer group IDs.
- **Setting security only in unprefixed worker configs**: source/sink tasks then fail to authenticate; repeat under `producer.` and `consumer.`.
- **Exposing the REST API unsecured / leaving client overrides unrestricted**: gives arbitrary code execution on workers. Use `connector.client.config.override.policy=Allowlist` (recommended since 4.2.0, default in 5.0.0) and allow only the keys you need.
- **`errors.tolerance=all` with no log/DLQ**: bad records disappear without trace.
- **Enabling `errors.log.include.messages=true` casually**: writes keys/values/headers (possibly sensitive) to application logs.
- **Expecting automatic restart of failed tasks**: Connect does not restart failed tasks; use the restart endpoints (a 409 during a rebalance means retry later).
- **Flipping `exactly.once.source.support=enabled` on a live cluster in one step**: existing clusters need two rolling upgrades (`preparing`, then `enabled`).
- **Switching straight to `plugin.discovery=service_load`** without verification: incompatible plugins become unusable.

## Code Examples
```properties
# Distributed worker essentials
bootstrap.servers=broker1:9092
group.id=connect-cluster-a
config.storage.topic=connect-configs      # pre-create: 1 partition, compacted, RF>=3
offset.storage.topic=connect-offsets      # pre-create: many partitions, compacted
status.storage.topic=connect-status       # pre-create: several partitions, compacted
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
plugin.path=/opt/connect/plugins
listeners=https://0.0.0.0:8443
connector.client.config.override.policy=Allowlist
exactly.once.source.support=enabled       # new clusters only
```
- **What it demonstrates**: the configs that must be right before first start of a distributed cluster, plus the security and EOS switches.

```properties
transforms=Filter,Extract
transforms.Filter.type=org.apache.kafka.connect.transforms.Filter
transforms.Filter.predicate=IsFoo
transforms.Extract.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.Extract.field=other_field
transforms.Extract.predicate=IsBar
transforms.Extract.negate=true
predicates=IsFoo,IsBar
predicates.IsFoo.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsFoo.pattern=foo
predicates.IsBar.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsBar.pattern=bar
```
- **What it demonstrates**: drop every record for topic `foo`; apply `ExtractField` to all topics except `bar` using `negate`.

```properties
errors.retry.timeout=600000          # retry up to 10 min
errors.retry.delay.max.ms=30000      # max 30 s between attempts
errors.tolerance=all
errors.log.enable=true
errors.log.include.messages=false
errors.deadletterqueue.topic.name=my-connector-errors
errors.deadletterqueue.context.headers.enable=true
```
- **What it demonstrates**: turning fail-fast into retry + tolerate + log + DLQ.

## Reference Tables

| Config | Default | Meaning | When to change |
|---|---|---|---|
| `plugin.path` | null | Dirs holding connector/converter/SMT plugins | Always set; even FileStream connectors (`connect-file-4.3.1.jar`) are not on it by default |
| `group.id` | — | Connect cluster identity | Unique per cluster; never equal to a consumer group |
| `tasks.max` | — | Upper bound of tasks for a connector | Raise for parallelism; connector may create fewer |
| `topics` / `topics.regex` | — | Sink input (one is required) | Regex for dynamic topic sets |
| `listeners` | `http://:8083` | REST listeners (`http`/`https`) | HTTPS in prod; `listeners.https.*` to use SSL settings distinct from broker `ssl.*` |
| `admin.listeners` | regular listeners | Where `/admin/loggers` is served | Isolate admin endpoints |
| `rest.advertised.host.name` / `.port` / `.listener` | — | URI followers use to reach the leader | Behind NAT/containers or with mixed HTTP/HTTPS |
| `errors.tolerance` | `none` | Fail on first error vs `all` skip | Production pipelines with DLQ |
| `errors.retry.timeout` | `0` | Total retry time for failed operations | Transient downstream errors |
| `connect.protocol` | `compatible` | Incremental cooperative rebalancing | `eager` only for legacy behavior |
| `scheduled.rebalance.max.delay.ms` | `300000` | Wait for a departed worker before reassigning | Lower for faster failover, higher for long upgrades |
| `exactly.once.source.support` | disabled | Framework EOS for source connectors | `preparing` then `enabled` on existing clusters |
| `consumer.isolation.level` | read_uncommitted | Sink consumer isolation | `read_committed` for EOS sinks |
| `plugin.discovery` | `hybrid_warn` | Plugin loading strategy | `service_load` after verification for fast startup |
| `topic.tracking.enable` / `topic.tracking.allow.reset` | true / true | Active-topic tracking and its reset | Disable reset in locked-down clusters |
| `connector.client.config.override.policy` | (permissive; `Allowlist` default in 5.0) | Which client overrides connectors may set | Set `Allowlist` now |

| REST endpoint | Purpose |
|---|---|
| `POST /connectors` | Create (`name`, `config`, optional `initial_state` = RUNNING/PAUSED/STOPPED) |
| `PUT` / `PATCH /connectors/{name}/config` | Replace / partially update config (`null` removes a key) |
| `GET /connectors/{name}/status` | Connector + task states and worker IDs |
| `PUT /connectors/{name}/pause` / `stop` / `resume` | Lifecycle control |
| `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true` | Restart only failed instances (both flags default false) |
| `POST /connectors/{name}/tasks/{id}/restart` | Restart one task |
| `GET/PATCH/DELETE /connectors/{name}/offsets` | Read / alter / reset offsets (connector must be STOPPED for writes) |
| `PUT /connector-plugins/{type}/config/validate` | Validate a config before submitting |
| `GET/PUT /admin/loggers/{name}` | Runtime log levels (KIP-495) |

| ACL principal | Required |
|---|---|
| Worker | Read Group `group.id`; Read+Write (+Create if absent) on config/offset/status topics; Write+Describe TransactionalId `connect-cluster-${groupId}` when EOS enabled/preparing |
| Source connector | Write (+Create if `topic.creation.enable`) on destination topics; with EOS: Describe offsets topic, Write+Describe TransactionalId `${groupId}-${connector}-${taskId}` (prefix `${groupId}-${connector}*` acceptable) |
| Sink connector | Read Group `connect-${connector}` (or overridden group id); Read on sink topics; Write on DLQ topic if set |

## Worked Example
Resetting a sink connector to reprocess part of a topic (KIP-875 flow):
1. `GET /connectors/my-sink/status` — confirm the connector and which workers run its tasks.
2. `PUT /connectors/my-sink/stop` — tasks shut down and resources are freed; status shows STOPPED (tasks disappear from status). Pause would not suffice — offsets can be modified only when stopped.
3. `GET /connectors/my-sink/offsets` — inspect current committed positions.
4. `PATCH /connectors/my-sink/offsets` with:
   ```json
   {"offsets":[
     {"partition":{"kafka_topic":"test","kafka_partition":0},"offset":{"kafka_offset":5}},
     {"partition":{"kafka_topic":"test","kafka_partition":1},"offset":null}]}
   ```
   Partition 0 rewinds to offset 5; `null` resets partition 1. (For a source connector the partition/offset keys are connector-defined, e.g. FileStreamSource uses `{"filename":"test.txt"}` → `{"position":30}`.) To wipe everything instead, `DELETE /connectors/my-sink/offsets`.
5. `PUT /connectors/my-sink/resume` — tasks are reassigned (UNASSIGNED → RUNNING) and consumption restarts from the new offsets. Poll status, since the status topic is consumed asynchronously and state changes appear with a short delay.

## Key Takeaways
1. Pre-create the three internal topics with correct partitions/RF/compaction before starting a distributed cluster, and give each cluster a unique `group.id`.
2. Repeat security client settings under unprefixed, `producer.`, and `consumer.` scopes; per-connector tweaks go in `producer.override.*` / `consumer.override.*`.
3. Treat the REST API as privileged: HTTPS, restricted network access, and `connector.client.config.override.policy=Allowlist`.
4. Default is fail-fast; for production pair `errors.tolerance=all` with logging and a DLQ, never alone.
5. Failed tasks are not auto-restarted — monitor `/status` and use `restart?includeTasks=true&onlyFailed=true`.
6. Exactly-once: sinks need `read_committed` plus a capable connector; sources need distributed mode, `exactly.once.source.support` (two-step rollout on existing clusters) and transactional-ID ACLs.
7. Verify plugins with `hybrid_warn`/`hybrid_fail` before switching to `plugin.discovery=service_load` for faster startup.

## Connects To
- **ch02**: quickstart runs the standalone FileStream example this chapter builds on.
- **ch04**: worker/connector config reference and config providers for externalizing secrets.
- **ch06**: transactions and `read_committed` underpin Connect's exactly-once support.
- **ch07**: internal topics rely on log compaction.
- **ch11**: MirrorMaker 2 is built on Kafka Connect.
- **ch14**: Connect worker/task metrics for monitoring statuses and errors.
- **ch18 / ch19 / ch20**: SSL, SASL and the ACLs each worker and connector principal needs.
- **ch22**: writing connectors, tasks, SMTs and ServiceLoader manifests.
- **ch25**: Kafka Streams DSL for transformations too heavy for SMTs.
