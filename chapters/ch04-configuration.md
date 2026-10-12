# Chapter 04: Configuration

## Core Idea
Kafka configuration is layered: a value resolves from dynamic per-broker → dynamic cluster-wide default → static `server.properties` → built-in default (and for topics, per-topic override → broker `log.*` default), and many broker settings (SSL stores, listeners, thread pools, topic defaults, cleaner) can be changed live with `kafka-configs.sh` instead of a restart. Secrets should never sit in plain properties: pull them in at runtime through `ConfigProvider`s, locked down with `allowed.paths` / `allowlist.pattern` and the `org.apache.kafka.automatic.config.providers` system property. (The source's generated per-config reference tables are not in this material, so it focuses on mechanisms rather than the full list.)

## Frameworks Introduced
- **Dynamic Update Modes**: every broker config has one of three modes: `read-only` (needs a restart), `per-broker` (dynamic, one broker at a time), `cluster-wide` (dynamic cluster default, which can also be set per broker for testing).
  - When to use: before changing any broker setting, check its mode to decide between `kafka-configs.sh --alter` and a rolling restart.
  - How: (1) look up the mode in the broker config reference; (2) for `cluster-wide`, use `--entity-type brokers --entity-default`; (3) for `per-broker`, use `--entity-name <id>`; (4) for `read-only`, edit `server.properties` and do a rolling restart; (5) check with `--describe`, and roll back with `--delete-config`.
  - Why it works / failure mode: dynamic values are stored in the **metadata log** (KRaft), so every broker picks them up without a restart. The trap is that a forgotten per-broker override silently wins over the cluster default you just set. Run `--describe` on each broker.
- **Config Precedence Ladder (broker)**: dynamic per-broker (metadata log) > dynamic cluster-wide default (metadata log) > static `server.properties` > Kafka default.
  - When to use: when you are debugging "why is broker N using value X?".
  - How: describe the per-broker entity, then `--entity-default`, then read `server.properties`, then the documented default. The first one that is set wins.
- **Topic Override Model**: each topic config has a server default (a broker `log.*` property, the "Server Default Property") plus an optional per-topic override. The default applies only when no override exists.
  - When to use: when one topic needs different retention, size or flush behavior.
  - How: set the override at creation with `kafka-topics.sh --create ... --config k=v` (repeatable). Change it later with `kafka-configs.sh --entity-type topics --entity-name T --alter --add-config k=v`, inspect it with `--describe`, and remove it with `--delete-config k`.
- **Listener-prefixed dynamic security updates**: to rotate SSL keystores or truststores, or to add listeners, use configs prefixed `listener.name.{listenerName}.` so the change touches only that listener.
  - When to use: for short-lived certificates (rotate without a restart), when adding or removing CAs, or when adding a new listener.
  - How: send all related keys (`ssl.keystore.type/location/password`, `ssl.key.password`, or `ssl.truststore.*`) in **one** per-broker alter request.
  - Why it works / failure mode: on the **inter-broker listener**, the broker validates trust. A new keystore must be trusted by the listener's truststore, and a new truststore must trust the existing keystore. Other listeners get **no** validation, so a keystore signed by a different CA, or a truststore missing the client-signing CA, breaks client authentication.
- **ConfigProvider indirection**: declare providers with `config.providers=<aliases>` and `config.providers.<alias>.class=<FQCN>`, pass parameters with `config.providers.<alias>.param.<name>=<value>`, then reference values as `${alias:...}` placeholders that are resolved at runtime.
  - When to use: for passwords, API keys, JAAS and certificates in brokers, clients and Connect (especially connector credentials and container secrets).
  - How: use a built-in provider (`FileConfigProvider`, `DirectoryConfigProvider`, `EnvVarConfigProvider`), or implement `ConfigProvider`, put the JAR on the classpath and reference the class.

## Key Concepts
- **Essential broker configs**: `node.id`, `log.dirs`, `process.roles`, `controller.quorum.bootstrap.servers`, `controller.listener.names` (KRaft-only world). Full definitions live in `kafka.server.KafkaConfig`.
- **Cluster-wide default (`--entity-default`)**: a dynamic broker config that every broker processes. Use it to keep values consistent across the cluster.
- **Dynamic topic defaults**: the broker `log.*` defaults (retention, segment, roll, flush, cleaner lag/ratio, `min.insync.replicas`, `unclean.leader.election.enable`, `compression.type`, `max.message.bytes`, `log.message.timestamp.type`...). You can change them at cluster-default level and they apply only to topics without an override.
- **Thread-pool resize bound**: a dynamic resize of a broker thread pool must stay within `currentSize / 2` to `currentSize * 2`.
- **Log cleaner dynamic configs**: `log.cleaner.threads`, `log.cleaner.io.max.bytes.per.second`, `log.cleaner.dedupe.buffer.size`, `log.cleaner.io.buffer.size`, `log.cleaner.io.buffer.load.factor`, `log.cleaner.backoff.ms`. They take effect on the next cleaning iteration.
- **ConnectionQuota configs**: `max.connections.per.ip` and `max.connections.per.ip.overrides`. Changes apply to new connections and count the connections that already exist.
- **Broker logger level**: change it live with `kafka-configs.sh --broker-logger <id> --alter --add-config <logger>=DEBUG`.
- **Inter-broker listener**: set statically via `inter.broker.listener.name` or `security.inter.broker.protocol`. Switching to a new listener means adding it dynamically on all brokers, then doing a rolling restart.
- **Kafka system properties (`-D`)**: JVM-level security switches: `org.apache.kafka.sasl.oauthbearer.allowed.files` (4.1), `org.apache.kafka.sasl.oauthbearer.allowed.urls` (4.0), `org.apache.kafka.allowed.login.modules` (4.2), the deprecated `org.apache.kafka.disallowed.login.modules`, and `org.apache.kafka.automatic.config.providers` (3.8).
- **RLMM client prefixes**: `TopicBasedRemoteLogMetadataManager` (the default `RemoteLogMetadataManager`) creates admin, producer and consumer clients for `__remote_log_metadata`. Tune them with `<rlmm.prefix>.remote.log.metadata.{common.client|admin|producer|consumer}.<kafka.property>`. The prefix comes from `remote.log.metadata.manager.impl.prefix`, for example `rlmm.config.`.

## Mental Models
- Think of dynamic configs as **rows in the metadata log layered on top of `server.properties`**: deleting the dynamic row reveals the static value underneath. It does not reset to the Kafka default unless nothing is set statically.
- Use **cluster default for policy and per-broker for experiments**: anything `cluster-wide` should be set with `--entity-default`. Keep per-broker overrides temporary (canarying a value), then delete them.
- Think of broker `log.*` properties as **the template for topics**: change the template to affect every topic without an override, and use per-topic overrides for exceptions.
- Use **allowlists by default for anything that reads files, URLs or env vars**: empty OAUTHBEARER allowlists, `allowed.paths`, `allowlist.pattern`, and `automatic.config.providers=none` on untrusted-config hosts (CVE-2024-31141).

## Anti-patterns
- **Plaintext secrets in `server.properties` or connector JSON**: these leak through config dumps and REST. Reference them through `FileConfigProvider`, `DirectoryConfigProvider` or `EnvVarConfigProvider` instead.
- **Providers with no restriction**: if `allowed.paths` is unset, all paths are readable, and without `allowlist.pattern`, all env vars are readable. Untrusted config input can then read arbitrary files or env vars.
- **Rotating a keystore to a cert from a different CA on a non-inter-broker listener**: the broker does not validate it, so clients fail to authenticate.
- **Dropping the client-signing CA from a new truststore**: new client connections fail authentication.
- **Resizing thread pools in big jumps** (for example 8 → 32 `num.io.threads`): this is rejected. Step within ×0.5–×2 per update.
- **Re-enabling `JndiLoginModule` or `LdapLoginModule`** (clearing `org.apache.kafka.disallowed.login.modules`) without vetting JNDI config: this reopens CVE-2023-25194. Prefer the allowlist `org.apache.kafka.allowed.login.modules` (it wins if both are set).
- **Expecting a dynamic change to the inter-broker listener to take effect**: `inter.broker.listener.name` is static, so a rolling restart is still required.

## Code Examples
```bash
# Cluster-wide dynamic default, then per-broker override, then revert
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config log.cleaner.threads=2
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --add-config log.cleaner.threads=3
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --delete-config log.cleaner.threads
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --describe
# Live log level
bin/kafka-configs.sh --bootstrap-server localhost:9092 --broker-logger 0 --alter \
  --add-config 'org.apache.kafka.server.quota.ClientQuotaManager$ThrottledChannelReaper=DEBUG'
```
- **What it demonstrates**: the precedence ladder in practice. Broker 0 runs 3 threads while the others run 2. After `--delete-config`, broker 0 falls back to the cluster default, not to the Kafka default.

```bash
# Topic override lifecycle
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 1 \
  --replication-factor 1 --config max.message.bytes=64000 --config flush.messages=1
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic --alter --add-config max.message.bytes=128000
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my-topic --alter --delete-config max.message.bytes
```
- **What it demonstrates**: you can set a per-topic override at creation, change it later, and delete it. Deleting it makes the topic fall back to the broker default (`message.max.bytes` / `log.*`).

```properties
config.providers=envVarProvider,dirProvider
config.providers.envVarProvider.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
config.providers.envVarProvider.param.allowlist.pattern=^KAFKA_SECRET_.*
config.providers.dirProvider.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
config.providers.dirProvider.param.allowed.paths=/run/secrets
ssl.keystore.password=${envVarProvider:KAFKA_SECRET_KS_PASS}
ssl.key.password=${dirProvider:/run/secrets:key_password}
```
- **What it demonstrates**: two restricted providers and both placeholder shapes. `${alias:VAR}` is for env vars. `${alias:<dir>:<file>}` is for a directory where each file name is a key and its content is the value.

## Reference Tables

**Dynamic update categories (what you can change without restart)**

| Category | Level | Keys | Notes |
|---|---|---|---|
| SSL keystore | per-broker, `listener.name.{L}.` prefix | `ssl.keystore.type/location/password`, `ssl.key.password` | one alter request; same CA as old cert |
| SSL truststore | per-broker, listener prefix | `ssl.truststore.type/location/password` | used for new connections only |
| Topic defaults | cluster-default | `log.segment.bytes`, `log.roll.*`, `log.retention.*`, `log.flush.*`, `log.cleaner.*.lag.ms`, `log.cleanup.policy`, `min.insync.replicas`, `unclean.leader.election.enable`, `compression.type`, `max.message.bytes`, `log.preallocate`, `log.message.timestamp.type` | only topics without override |
| Log cleaner | cluster-default | `log.cleaner.threads`, `.io.max.bytes.per.second`, `.dedupe.buffer.size`, `.io.buffer.size`, `.io.buffer.load.factor`, `.backoff.ms` | next cleaning iteration |
| Thread pools | cluster-default | `num.network.threads`, `num.io.threads`, `num.replica.fetchers`, `num.recovery.threads.per.data.dir`, `log.cleaner.threads`, `background.threads`, `remote.log.reader.threads`, `remote.log.manager.{copier,expiration,follower}.thread.pool.size` | range `size/2`..`size*2` per update |
| Connection quota | cluster-default | `max.connections.per.ip`, `max.connections.per.ip.overrides` | existing conns counted |
| Listeners | per-broker | `listeners`, `advertised.listeners`, `listener.security.protocol.map` (+ prefixed security, `sasl.jaas.config` with listener+mechanism prefix) | inter-broker listener stays static |

**Built-in ConfigProviders**

| Provider | Source | Restriction param | Placeholder |
|---|---|---|---|
| `FileConfigProvider` | one properties file | `allowed.paths` (unset = all) | `${fp:<path/file>:<key>}` |
| `DirectoryConfigProvider` | dir; file name = key, content = value | `allowed.paths` (unset = all) | `${dp:<dir>:<file>}` |
| `EnvVarConfigProvider` | environment variables | `allowlist.pattern` (regex) | `${ev:<VAR>}` |

**Security system properties**

| Property | Since | Default | Use |
|---|---|---|---|
| `org.apache.kafka.automatic.config.providers` | 3.8.0 | all built-ins enabled | `none` or FQCN list on hardened hosts (CVE-2024-31141) |
| `org.apache.kafka.sasl.oauthbearer.allowed.urls` | 4.0.0 | empty | allowlist token/JWKS endpoints |
| `org.apache.kafka.sasl.oauthbearer.allowed.files` | 4.1.0 | empty | allowlist token/key files |
| `org.apache.kafka.allowed.login.modules` | 4.2.0 | — | JAAS login-module allowlist; overrides disallowed |
| `org.apache.kafka.disallowed.login.modules` | 3.4.0 (deprecated 4.2.0) | `JndiLoginModule,LdapLoginModule` | legacy denylist (CVE-2023-25194) |

## Worked Example
**Rotating a short-lived broker certificate on listener `EXTERNAL`, plus raising cleaner capacity, with zero restarts.**
1. Put the new keystore at `/etc/kafka/ssl/broker0-v2.jks`, signed by the **same CA** as the current cert. EXTERNAL is not the inter-broker listener, so the broker will not check trust for you.
2. Apply all four keys in one per-broker request:
   `kafka-configs.sh --bootstrap-server b0:9092 --entity-type brokers --entity-name 0 --alter --add-config listener.name.external.ssl.keystore.location=/etc/kafka/ssl/broker0-v2.jks,listener.name.external.ssl.keystore.password=...,listener.name.external.ssl.key.password=...,listener.name.external.ssl.keystore.type=JKS`
   Better still, keep the passwords out of the command and the metadata by using provider placeholders.
3. Repeat for each broker id. Verify with `--entity-name <id> --describe` and a client TLS handshake.
4. The cleaner is lagging, so raise it cluster-wide: `--entity-type brokers --entity-default --alter --add-config log.cleaner.threads=2`. From 1, this is within the ×2 bound. It takes effect on the next cleaning iteration.
5. If one broker earlier got a test override (`--entity-name 3 --add-config log.cleaner.threads=4`), remove it with `--delete-config log.cleaner.threads` so the cluster default applies, because per-broker beats cluster default.

## Key Takeaways
1. Before changing a broker config, check its update mode (`read-only` / `per-broker` / `cluster-wide`). Only `read-only` needs a rolling restart.
2. Remember the precedence: per-broker dynamic > cluster-default dynamic > `server.properties` > Kafka default. Always `--describe` both the broker entity and `--entity-default`.
3. Treat broker `log.*` settings as topic templates, and use `--config` / `kafka-configs.sh --entity-type topics` for per-topic exceptions.
4. Rotate SSL material live with `listener.name.{L}.`-prefixed keys in one request, and keep the same CA. Only the inter-broker listener gets trust validation.
5. Resize thread pools gradually (×0.5–×2 per step).
6. Move secrets to ConfigProviders, and always restrict them with `allowed.paths` / `allowlist.pattern`. On hosts where config input is untrusted, set `-Dorg.apache.kafka.automatic.config.providers=none` or an explicit list.
7. Lock down JVM-level auth surfaces: keep the OAUTHBEARER file/URL allowlists minimal, and use `org.apache.kafka.allowed.login.modules`.

## Connects To
- **ch07**: log compaction and quotas. These explain what the cleaner and `min.compaction.lag`/ratio configs control, and how connection and client quotas behave.
- **ch10**: basic operations. Day-to-day topic create/alter and rolling restarts use the same `kafka-topics.sh` / `kafka-configs.sh` tools.
- **ch14**: monitoring. Use it to confirm the effect of thread-pool and cleaner tuning.
- **ch15**: KRaft. The essential configs (`process.roles`, `controller.quorum.bootstrap.servers`) live here, and so does the metadata log that stores dynamic configs.
- **ch16**: tiered storage. Remote log configs, RLMM, and the `remote.log.*` thread pools.
- **ch18–ch19**: listeners, SSL and SASL/JAAS. The background for listener-prefixed dynamic updates and the login-module and OAUTHBEARER system properties.
- **ch21**: Kafka Connect. `FileConfigProvider` is used to externalize connector credentials.
