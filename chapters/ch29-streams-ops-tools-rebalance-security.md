# Chapter 29: Kafka Streams — Reset Tool, Groups CLI, Streams Rebalance Protocol, Topology Naming and Security

## Core Idea
Operating a Streams app safely means controlling three things you cannot see in the code: where it restarts from (reset tooling, which differs by `group.protocol`), who assigns its tasks (the broker-side Streams Rebalance Protocol of KIP-1071, `group.protocol=streams`, default-enabled on new AK 4.2+ clusters), and whether its stateful internal resources keep stable names across topology changes (explicit naming via `Materialized`/`Grouped`/`StreamJoined`/`Joined`). Security is ordinary client security plus ACLs scoped by the `application.id` prefix.

## Frameworks Introduced
- **Two-step application reset (classic protocol)**: `kafka-streams-application-reset.sh` + local state wipe.
  - When to use: reprocess from scratch after a bug fix, in dev/test, or after a breaking topology rename — on groups using `group.protocol=classic`.
  - How: (1) stop **all** instances; verify the consumer group `application.id` is empty via `kafka-consumer-groups`; (2) run the tool with `--dry-run`, then for real; it resets input-topic offsets (default `--to-earliest`) and **deletes** internal topics (which drops their committed offsets); (3) on every machine that ever ran an instance, call `KafkaStreams#cleanUp()` or delete `${java.io.tmpdir}/kafka-streams/<application.id>` before restarting; (4) separately handle downstream consumers of output topics — the tool never touches output topics.
  - Why it works / failure mode: offsets and changelogs are the app's memory on the broker, the state dir is its memory on disk; reset only one and the app restores stale state over rewound input (double counting). A typo in `--application-id` or wrong `--input-topics` can damage other groups — the tool is irreversible.
- **Streams Rebalance Protocol (KIP-1071)**: broker-driven task assignment for Streams, following the KIP-848 model for consumers.
  - When to use: AK 4.2+ brokers and clients, no static membership, no regex subscriptions, no HA/rack-aware assignor needed.
  - How: broker feature `streams.version=1` (default on new 4.2 clusters; `kafka-features.sh upgrade` on upgraded ones) and `"streams"` in `group.coordinator.rebalance.protocols`; client sets `group.protocol=streams`. Members send `StreamsGroupHeartbeat` carrying topology metadata (subtopologies); the group coordinator validates topics, computes assignments continuously with a sticky task assignor, persists group/topology/member records in `__consumer_offsets`.
  - Why it works / failure mode: removes the global client-side sync barrier → faster, more stable rebalances and one authoritative assigner (no split-brain). Failure mode: missing source/internal topics or broken copartitioning put the group in `NOT_READY` and every member gets an **empty assignment** — heartbeats succeed, the app silently processes nothing.
- **Topology validation ("topology configuration")**: on heartbeat the coordinator (1) checks all source topics exist, (2) checks copartition groups really are copartitioned, (3) derives internal-topic partition counts from source topics, (4) checks internal topics exist with the right config. Any gap → `NOT_READY`, with the problem reported in the heartbeat status.
- **Centralized assignment configuration**: `num.standby.replicas`, `session.timeout.ms`, `heartbeat.interval.ms` become group-level configs set on the broker (`group.streams.*` defaults, `streams.*` per-group overrides via `kafka-configs.sh --entity-type groups`); client-side values are ignored. Last-used config is stored in group metadata, so a dynamic change triggers reassignment immediately without redeploy.
- **Explicit internal-resource naming**: name every stateful DSL element so adding/removing operators doesn't shift auto-generated names (and thus changelog/repartition topics). Enforce with `ensure.explicit.internal.resource.naming=true` — the app refuses to start if any internal topic/store/changelog uses a generated name.

## Key Concepts
- **Streams group**: a broker-coordinated group type (alongside consumer and share groups) fixed by the first heartbeat for that GroupId; extra state `NOT_READY`, no classic legacy states.
- **Streams group states**: Empty, Not Ready, Assigning, Reconciling, Stable, Dead.
- **`kafka-streams-groups.sh`**: the CLI for streams groups (list/describe/reset-offsets/delete-offsets/delete); replaces `kafka-streams-application-reset.sh` for groups on the streams protocol.
- **Generated name pattern**: `KSTREAM|KTABLE-<OPERATOR>-<10-digit zero-padded global counter>`; position-dependent, so inserting an operator upstream renames everything after it.
- **Stateful vs stateless names**: processor names are stateless (in-memory, safe to shift); state-store, changelog and repartition-topic names are stateful (shifting them orphans data).
- **Internal topic naming**: `<application.id>-<operatorName>-<repartition|changelog>` — not public API, may change.
- **Offline migration**: classic↔streams conversion only with all members gone; only committed offsets survive; internal topics remain as plain topics.
- **`StreamsGroupDescribe` RPC / `Admin#describeStreamsGroups`**: Streams-specific describe, separate from consumer groups; no `removeMembersFromConsumerGroup` equivalent (no LeaveGroup in KIP-848-style groups).

## Mental Models
- Think of a reset as **three stores of memory**: input offsets (rewind), internal topics (delete), local state dir (wipe). Miss any one and you get a hybrid of old and new state.
- Use the streams protocol when you want the broker to be the source of truth for assignment and to retune standbys/timeouts **without redeploying**; stay on classic if you need static membership, warmup tasks, rack-aware assignment, regex sources, or a custom main consumer via `KafkaClientSupplier`.
- Think of `NOT_READY` as "healthy heartbeats, zero work" — alert on group state, not on client liveness.
- Think of DSL auto-names as **array indices**: fine until you insert an element. `Materialized.as(...)`/`Grouped.as(...)` turn them into map keys.

## Anti-patterns
- **Resetting while any instance runs**: invalid state, crashes or wrong results; with long session timeouts, leftover members block the reset — use `--force` only once everything is truly shut down.
- **Skipping `--dry-run`**: both tools make irreversible changes; deleting internal topics destroys state that must be rebuilt from input.
- **Forgetting `cleanUp()` / state dir**: the instance restores old local state against rewound offsets.
- **Setting `num.standby.replicas`, `session.timeout.ms`, `heartbeat.interval.ms`, `task.assignor.class`, `acceptable.recovery.lag`, `max.warmup.replicas`, `probing.rebalance.interval.ms`, `rack.aware.assignment.*` on the client under `group.protocol=streams`**: silently ignored.
- **Setting a static `instance.id` with the streams protocol**: rejected.
- **Significant topology changes on an existing streams group** (new source topics, different subtopology count): not supported — create a new streams group.
- **Migrating classic→streams on AK 4.2.0**: KAFKA-20254 broker bug; use 4.2.1+.
- **Relying on generated names for stores/repartition topics in production**: an upstream `filter` renames the changelog; no rolling redeploy is possible and the new changelog starts empty.
- **Pre-creating internal topics wrong**: wrong partition count → startup failure; changelogs without compaction → data loss.

## Code Examples
```bash
# Classic protocol: preview, then reset input to a time; delete internal topics
bin/kafka-streams-application-reset.sh --bootstrap-server broker:9092 \
  --application-id my-app --input-topics orders,payments \
  --to-datetime 2025-01-31T23:57:00.000 --command-config client.properties --dry-run

# Streams protocol: same intent via the groups CLI
kafka-streams-groups.sh --bootstrap-server localhost:9092 --group my-streams-app \
  --reset-offsets --all-input-topics --to-datetime 2025-01-31T23:57:00.000 --dry-run   # then --execute
kafka-streams-groups.sh --bootstrap-server localhost:9092 --describe --group my-streams-app --members --verbose
kafka-streams-groups.sh --bootstrap-server localhost:9092 --delete --group my-streams-app \
  --internal-topics my-app-repartition-0,my-app-changelog
```
- **What it demonstrates**: reset scenarios are mutually exclusive (`--by-duration PnDTnHnMnS`, `--from-file`, `--shift-by ±n`, `--to-datetime`, `--to-earliest` (default), `--to-latest`, `--to-offset`; groups CLI adds `--to-current`); the groups CLI requires `--dry-run` or `--execute` for mutations. Omitting `--input-topics` in the classic tool = restart from empty state without reprocessing old data.

```bash
bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1   # downgrade ... =0 to disable
# client: group.protocol=streams
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups \
  --entity-name wordcount --add-config streams.num.standby.replicas=1
```
- **What it demonstrates**: enabling KIP-1071 end to end and tuning standbys per group at runtime.

```java
stream.filter((k, v) -> v != null && v.length() >= 6, Named.as("drop_short_values"))
      .groupByKey(Grouped.as("purchases_by_key"))          // repartition topic name
      .count(Materialized.as("Purchase_count_store"))     // store + changelog name
      .toStream().to("output", Produced.as("purchase_counts_sink"));
props.put(StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG, true);
```
- **What it demonstrates**: stable names for every stateful resource, plus the guardrail that fails startup on any auto-generated internal name.

## Reference Tables
| Config | Scope | Meaning / when to change |
|---|---|---|
| `group.protocol` | Streams client | `classic` (default) or `streams` (KIP-1071) |
| `streams.version` | broker feature | `1` enables streams groups (default on new 4.2 clusters) |
| `group.coordinator.rebalance.protocols` | broker | must include `streams` |
| `group.streams.session.timeout.ms` (+`min`/`max`) | broker default | failure-detection timeout for streams groups |
| `group.streams.heartbeat.interval.ms` (+`min`/`max`) | broker default | heartbeat interval handed to members |
| `group.streams.num.standby.replicas` / `group.streams.max.standby.replicas` | broker | default standbys per task / cap on dynamic overrides |
| `group.streams.max.size` | broker | max clients in one streams group |
| `group.streams.initial.rebalance.delay.ms` | broker | delay first rebalance of an empty group so more members join |
| `streams.session.timeout.ms`, `streams.heartbeat.interval.ms`, `streams.num.standby.replicas`, `streams.initial.rebalance.delay.ms` | per GROUP (`kafka-configs.sh`) | per-app overrides, no redeploy |
| `ensure.explicit.internal.resource.naming` | Streams client | `true` = refuse to start on generated internal names |

| What to name | Naming class |
|---|---|
| Aggregation repartition topics | `Grouped` |
| KStream-KStream join repartition topics and state stores | `StreamJoined` |
| KStream-KTable join repartition topic | `Joined` |
| State stores (aggregations, KTable-KTable joins) | `Materialized` |
| Stateless operators / sources / sinks | `Named` / `Consumed.as` / `Produced.as` |

| ACL (with `group.protocol=streams`) | Operation | Resource | Note |
|---|---|---|---|
| STREAMS_GROUP_HEARTBEAT | Read | Group | the app's streams group |
| STREAMS_GROUP_HEARTBEAT | Create | Cluster, or Topic (all changelog + repartition-source topics) | only if auto-creating internal topics |
| STREAMS_GROUP_HEARTBEAT | Describe | Topic | all topology topics, on first join |
| STREAMS_GROUP_DESCRIBE | Describe | Group, Topic | group + all topology topics |

Broker metrics: `group-count,protocol={consumer|classic|streams}`, `streams-group-count,state={empty|not_ready|assigning|reconciling|stable|dead}`, `streams-group-rebalance-rate`, `streams-group-rebalance-count` (all under `kafka.server:type=group-coordinator-metrics`).

## Worked Example
A `groupByKey().count()` app relies on generated names: the store is `KSTREAM-AGGREGATE-STATE-STORE-0000000001`. A developer adds a `filter` before `groupByKey`; every downstream counter shifts by one, so the store becomes `...-STATE-STORE-0000000002` and its changelog topic is new and empty. A rolling redeploy would silently restart all counts from zero. Fix, once: (1) stop all instances; (2) change code to `count(Materialized.as("Purchase_count_store"))` and `groupByKey(Grouped.as(...))`, set `ensure.explicit.internal.resource.naming=true`; (3) on the classic protocol run `kafka-streams-application-reset.sh --application-id <id> --input-topics input --dry-run`, inspect the internal topics marked for deletion, rerun without `--dry-run` (on the streams protocol: `kafka-streams-groups.sh --reset-offsets --all-input-topics --to-earliest --dry-run` then `--execute`, and `--delete ... --internal-topics` for obsolete ones); (4) `KafkaStreams#cleanUp()` on each host; (5) restart — aggregations are recomputed into `<application.id>-Purchase_count_store-changelog`, and future upstream edits no longer rename it. Later, migrating to the streams protocol is offline: stop all instances, wait out `session.timeout.ms` (or force leave), set `group.protocol=streams` (broker ≥ 4.2.1), restart; only committed offsets carry over.

## Key Takeaways
1. Pick the reset tool by protocol: `kafka-streams-application-reset.sh` for classic, `kafka-streams-groups.sh --reset-offsets` for streams groups; always `--dry-run` first, all instances stopped, then wipe local state.
2. Under `group.protocol=streams`, standbys and timeouts live on the broker (`group.streams.*`, per-group `streams.*`); client values are ignored, and HA/warmup/rack-aware/static membership/regex are unavailable.
3. Monitor `streams-group-count,state=not_ready`: missing or mis-partitioned topics give every member an empty assignment while heartbeats look healthy.
4. Name every stateful DSL resource from day one and enforce it with `ensure.explicit.internal.resource.naming=true`; retrofitting names is a breaking change requiring a reset.
5. Grant ACLs with a prefixed pattern on `application.id` (`--resource-pattern-type prefixed --topic <app.id> --operation All`, KIP-277/KIP-290), or pre-create internal topics to avoid granting Create — with max-input partition count, compaction on changelogs, and `delete,compact` + store retention + delta (default +24h) on windowed changelogs.
6. Streams security is just client security (SSL/SASL settings in the Streams `Properties`); misconfigurations fail fast at startup (e.g. "Keystore was tampered with, or password was incorrect").
7. `kafka-streams-scala` is deprecated in 4.3 (removal in 5.0, KIP-1244): use the Java API from Scala with explicit Serdes (`DEFAULT_KEY/VALUE_SERDE_CLASS_CONFIG`, `Produced.with(...)`).

## Connects To
- **ch17**: KIP-848 consumer rebalance protocol, the model KIP-1071 extends to Streams.
- **ch24**: tasks, subtopologies, standby replicas and state stores being assigned and reset here.
- **ch25**: DSL operators and the `Grouped`/`Joined`/`StreamJoined`/`Materialized` config objects used for naming.
- **ch27**: `application.id`, `state.dir`, `group.protocol`, standby/HA assignor configs that the streams protocol overrides or ignores.
- **ch10**: `kafka-consumer-groups.sh` and offset-reset semantics mirrored by both reset tools.
- **ch14**: group-coordinator metrics for streams groups.
- **ch18 / ch19 / ch20**: SSL, SASL and prefixed ACLs that secure Streams apps.
- **ch30**: upgrade notes for 4.2 (streams protocol default) and the 4.2.1 migration fix.
