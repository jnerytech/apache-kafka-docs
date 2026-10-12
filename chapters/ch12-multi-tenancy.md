# Chapter 12: Multi-Tenancy

## Core Idea
A shared Kafka cluster stays healthy only when you layer four controls on purpose: a hierarchical topic-naming scheme (the "namespace"), prefixed ACLs and create/alter policies that enforce it, per-tenant topic configs (retention), and client/server quotas that stop noisy neighbors, with monitoring to meter it all. The source section is short (~11 KB) and mostly points to other chapters, so this chapter is below the usual target. Its value is in tying together naming, ACLs and quotas.

## Frameworks Introduced
- **Hierarchical topic naming as user space (namespace)**: a tenant's "user space" is the set of topics under one dotted prefix managed by one entity.
  - When to use: always on a multi-tenant cluster. Topics cannot be renamed. Renaming means creating a new topic, copying the data and deleting the old one, so you must get the structure right before the first topic exists.
  - How: (1) pick the main grouping. Use team/org unit when teams own the infrastructure: `<organization>.<team>.<dataset>.<event-name>` (e.g. `acme.infosec.telemetry.logins`). Use project/product when one team runs several projects with separate credentials: `<project>.<product>.<event-name>` (e.g. `mobility.payments.suspicious`). (2) Keep volatile or derivable data out of names: no intended-consumer name, no partition count or other config. (3) Tie security and quotas to the prefix.
  - Why it works: a single prefix lets one prefixed ACL cover every current and future topic of a tenant, so admin work grows with the number of tenants rather than the number of topics. Failure mode: putting a consumer's name or an "unstructured" scheme in topic names means you will eventually need a rename, and Kafka has no rename.
- **Naming enforcement ladder** (lightest to strongest control):
  1. Prefixed ACLs (KIP-290). Grant Create only on `payments.teamA.`, so team A can only create topics with that prefix.
  2. Custom `CreateTopicPolicy` (KIP-108, broker setting `create.topic.policy.class.name`). Arbitrary regex or business rules. This is the most flexible option.
  3. Deny topic creation to normal users with an ACL, and create topics through an external process (scripts, GitOps or IaC tooling).
  4. `auto.create.topics.enable=false` on brokers. This is necessary but **never sufficient on its own**, because explicit CreateTopics calls still succeed.
  - When to use: use 1 for most clusters, 2 when the rules go beyond a prefix (allowed suffixes, partition or RF limits), and 3 when you need a central review or approval step.
- **Tenant isolation via quotas**: client quotas apply per user principal, whatever topics the client touches. That makes them the natural unit for allocating resources to a tenant.
  - How: set request-rate quotas (broker CPU time spent on the request path) first. Add produce/fetch byte-rate quotas next. Add `controller_mutation_rate` (KIP-599) to cap topic create/delete/alter bursts. On the broker side, cap connection creation rate, connections per broker and connections per IP.
  - Why it works: excessive request processing burns broker CPU and lowers the bandwidth the broker can actually serve, so request-rate quotas often isolate tenants better than bandwidth quotas alone. Failure mode: bandwidth-only quotas let a chatty client with tiny requests (for example tight `fetch.max.wait.ms` polling or many small produces) still starve the request handler threads.
- **Security triad for tenants**: encryption (client-broker, broker-broker, broker-tools), authentication (clients and inter-broker), and authorization (topic CRUD, read/write, ACL management), plus policies (`CreateTopicPolicy`, `AlterConfigPolicy` via `alter.config.policy.class.name`). Authorization through ACLs is the day-to-day multi-tenant task.

## Key Concepts
- **User space / namespace**: a group of topics under one managing entity, expressed as a shared topic-name prefix.
- **Prefixed ACL**: an ACL with `--resource-pattern-type prefixed` that matches every resource whose name starts with the given string (KIP-290).
- **`CreateTopicPolicy`**: a pluggable broker-side validator, run on every topic creation, that can reject names or configs (KIP-108).
- **`AlterConfigPolicy`**: a pluggable validator for config changes. For example, it can stop tenants from raising `retention.ms` beyond a limit.
- **Noisy neighbor**: a tenant whose load (bytes, request rate, connections, topic churn) degrades other tenants' SLAs.
- **Request rate quota**: caps the share of broker request-handling time a principal may use. Throttling starts when the share is exceeded.
- **`controller_mutation_rate`**: a quota type (KIP-599) that limits how fast a principal can create, delete or alter topic partitions.
- **Server (broker) quotas**: limits on new-connection rate, total connections per broker, and connections per IP address.
- **Schema registry / data contract**: an external (non-Apache-Kafka) service that maps schemas to topics, so producers write valid events and consumers can parse them. Many registries also handle evolution and compatibility.

## Mental Models
- Think of the topic prefix as the tenant's home directory. ACLs, quotas, alerts and retention policies should all hang off it.
- Use broad prefixed ACLs when developer convenience matters, and narrow per-topic ACLs when data sensitivity matters. You choose where to sit between "fewer, broader ACLs" and "more, narrower ACLs".
- Think of the user principal as the unit of billing and isolation. Quotas follow principals, not topics, so give each tenant (or each tenant's project) its own credentials.
- Use retention (`retention.ms`, `retention.bytes`) as both a storage budget and a compliance tool, for example GDPR deletion windows.

## Anti-patterns
- **Relying only on `auto.create.topics.enable=false`**: it blocks implicit creation only. Any principal with Create permission can still make arbitrarily named topics.
- **Encoding consumers or config in topic names** (`orders-for-billing`, `clicks-12p`): consumers and partition counts change, and you cannot rename a topic.
- **Bandwidth-only quotas**: they leave broker CPU unprotected, and CPU starvation reduces effective bandwidth for everyone.
- **Shared credentials across tenants or projects**: quotas, ACLs and metering all key on the principal, so sharing it removes isolation and attribution.
- **No topic-operation quota**: automation that creates or deletes many topics at once can overwhelm the controller without `controller_mutation_rate`.
- **No schema contract between teams**: one producer team can write malformed events that break every downstream tenant.

## Code Examples
```bash
# Grant Alice (ACME InfoSec) produce rights on every topic under acme.infosec.
bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:Alice \
  --producer \
  --resource-pattern-type prefixed --topic acme.infosec.
```
- **What it demonstrates**: one prefixed ACL covers `acme.infosec.telemetry.logins`, `acme.infosec.syslogs.events` and future topics. `--producer` is the shortcut that grants Write and Describe on the topic and Create on the cluster.

```bash
# Per-tenant client quotas (see ch07/ch10 for the full syntax)
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=10485760,consumer_byte_rate=20971520,request_percentage=50,controller_mutation_rate=10' \
  --entity-type users --entity-name Alice
```
- **What it demonstrates**: byte-rate, request-rate and topic-mutation quotas attached to the tenant's principal. They apply across all topics that principal touches.

```properties
# broker: enforce naming / config rules server-side
auto.create.topics.enable=false
create.topic.policy.class.name=com.acme.kafka.TenantPrefixPolicy
alter.config.policy.class.name=com.acme.kafka.RetentionCapPolicy
```
- **What it demonstrates**: defense in depth. Implicit creation is off, and pluggable policies validate explicit creates and config changes.

## Reference Tables

| Control | Setting / tool | Purpose in multi-tenancy | When to use |
|---|---|---|---|
| Naming via ACL | `kafka-acls.sh --resource-pattern-type prefixed` (KIP-290) | Limit creates and access to the tenant prefix | Default for every tenant |
| Naming via policy | `create.topic.policy.class.name` (KIP-108) | Arbitrary name and config rules | Rules beyond a simple prefix |
| Config guardrails | `alter.config.policy.class.name` | Stop tenants from raising retention and similar settings | Tenants may alter their own topic configs |
| Implicit creation | `auto.create.topics.enable=false` | Prevent typo or accidental topics | Always on shared clusters, never alone |
| Storage budget | `retention.ms`, `retention.bytes` (topic) | Bound disk use, compliance | Per tenant SLA or legal requirement |
| CPU isolation | client quota `request_percentage` | Cap broker request-handling time | First quota to set |
| Bandwidth isolation | `producer_byte_rate`, `consumer_byte_rate` | Cap network throughput | High-volume tenants |
| Topic churn | `controller_mutation_rate` (KIP-599) | Throttle create/delete/alter partitions | Self-service or automated topic creation |
| Connection limits | broker connection-rate, max connections, per-IP connection quotas | Protect against connection storms | Many or untrusted clients |
| Storage metering | JMX `kafka.log.Log.Size.<TOPIC-NAME>` (partition size) | Alert when a tenant nears its storage budget | Every shared cluster |

| Grouping | Pattern | Example | Best when |
|---|---|---|---|
| Team / org unit | `<organization>.<team>.<dataset>.<event-name>` | `acme.infosec.telemetry.logins` | Teams are the primary Kafka users |
| Project / product | `<project>.<product>.<event-name>` | `mobility.payments.suspicious` | One team runs several projects with separate credentials |

## Worked Example
Onboarding ACME's InfoSec team onto a shared cluster:
1. **Namespace**: choose team grouping and reserve the prefix `acme.infosec.`. Topics will look like `acme.infosec.telemetry.logins` and `acme.infosec.syslogs.events`. Do not put the consuming SIEM's name in topic names.
2. **Enforce**: brokers already run with `auto.create.topics.enable=false` and a `CreateTopicPolicy` that rejects names outside a tenant's registered prefix. Alternatively, deny Create to tenants and create topics through the platform's automation.
3. **Authorize**: new member Alice gets `kafka-acls.sh --add --allow-principal User:Alice --producer --resource-pattern-type prefixed --topic acme.infosec.`. She can write to every InfoSec topic, including future ones, with no per-topic ACL work. Consumers get a matching prefixed `--consumer` grant.
4. **Retention**: set `retention.ms` (for example a 30-day compliance window) and `retention.bytes` on the topics. An `AlterConfigPolicy` stops the team from raising them on its own.
5. **Isolate**: attach quotas to InfoSec's principals: `request_percentage` first (CPU), then produce/consume byte rates, and `controller_mutation_rate` so their topic automation cannot flood the controller.
6. **Meter**: track partition size via `kafka.log.Log.Size.<TOPIC-NAME>` summed per prefix, plus quota-throttle metrics and failed-authentication rate. Alert when the tenant nears its storage budget.
7. **Share**: if another region's cluster needs InfoSec data, mirror the `acme.infosec.*` topics with MirrorMaker 2 (ch11) instead of granting cross-tenant access.
8. **Contracts**: register event schemas in a third-party schema registry so downstream tenants can always parse the events.

## Key Takeaways
1. Design the topic hierarchy before the first topic exists. Kafka has no rename, and a fix costs a create, copy and delete.
2. Build every isolation control on the prefix. Prefixed ACLs make security cost scale with the number of tenants, not the number of topics.
3. Enforce naming in layers: prefixed ACLs and/or `CreateTopicPolicy`, plus `auto.create.topics.enable=false`. Never rely on the last one alone.
4. Give each tenant (or project) its own principal. Quotas, ACLs and metering all key on it.
5. Set request-rate quotas, not just byte-rate quotas, because CPU starvation reduces everyone's effective bandwidth. Add `controller_mutation_rate` and connection limits too.
6. Meter storage per tenant with partition-size metrics, and alert before retention or disk limits are hit.
7. Add schema-based data contracts (external registry) when tenants exchange data.

## Connects To
- **ch04**: per-topic configs (`retention.ms`, `retention.bytes`) and dynamic broker configs used for tenant guardrails.
- **ch07**: quota design (byte-rate, request-rate, client groups) that underpins tenant isolation.
- **ch10**: operational how-to for setting quotas and managing topics with `kafka-configs.sh` and `kafka-topics.sh`.
- **ch11**: geo-replication and MirrorMaker 2 for inter-cluster data sharing between tenants.
- **ch14**: metrics for metering, including partition size, throttle time and failed authentications.
- **ch18 / ch19**: encryption and authentication (SSL, SASL) that give each tenant a principal.
- **ch20**: ACLs and prefixed resource patterns in depth.
