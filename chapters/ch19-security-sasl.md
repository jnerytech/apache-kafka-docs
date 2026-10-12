# Chapter 19: Security — SASL Authentication

## Core Idea
Kafka authenticates clients and brokers through pluggable SASL mechanisms (GSSAPI/Kerberos, PLAIN, SCRAM-SHA-256/512, OAUTHBEARER), all wired through JAAS login modules. Pick the mechanism that matches your identity infrastructure, and always run it over `SASL_SSL` in production. Every default credential store or token implementation is either plaintext-on-disk (PLAIN), only as safe as your KRaft controllers (SCRAM, delegation tokens), or explicitly non-production (unsecured-JWT OAUTHBEARER).

## Frameworks Introduced
- **JAAS configuration precedence (broker)**: a broker resolves its SASL login config from the most specific source first:
  1. `listener.name.{listenerName}.{saslMechanism}.sasl.jaas.config` (broker property, lower-case listener and mechanism, one login module per value)
  2. `{listenerName}.KafkaServer` section of the static JAAS file (e.g. `sasl_ssl.KafkaServer`)
  3. `KafkaServer` section of the static JAAS file
  - When to use: per-listener properties when several listeners or mechanisms need different credentials. The static file only suits single-mechanism brokers.
  - How: when a listener enables several mechanisms, give one `sasl.jaas.config` per mechanism prefix. On clients, `sasl.jaas.config` overrides `-Djava.security.auth.login.config` (section `KafkaClient`).
  - Why it works / failure mode: the property form lets one JVM run several identities. The static `KafkaClient` file allows only **one user for all client connections in the JVM**, so it breaks multi-tenant apps and Connect/Streams workers that need per-client credentials.

- **Per-mechanism setup recipe** (the same shape for every mechanism):
  1. Broker JAAS: a login module in `KafkaServer`. Its `username`/`password` (or principal or token claims) is the broker's own identity for **inter-broker** connections.
  2. Broker `server.properties`: `listeners=SASL_SSL://...`, `security.inter.broker.protocol=SASL_SSL`, `sasl.mechanism.inter.broker.protocol=<MECH>`, `sasl.enabled.mechanisms=<MECH,...>`.
  3. Client: `security.protocol=SASL_SSL`, `sasl.mechanism=<MECH>`, `sasl.jaas.config=<LoginModule> required ...;`.
  - When to use: every SASL deployment. Only the login module and the credential source change.

- **Rolling SASL mechanism migration** (no downtime):
  1. Add the new mechanism to `sasl.enabled.mechanisms` and add its login module to JAAS, then roll the brokers.
  2. Restart the clients on the new mechanism.
  3. Optional: switch `sasl.mechanism.inter.broker.protocol`, then roll again.
  4. Optional: remove the old mechanism from `sasl.enabled.mechanisms` and JAAS, then roll again.
  - Why it works / failure mode: the brokers accept both mechanisms during the transition. If you skip step 1 on any broker, clients that already switched fail to authenticate against it.

- **OAUTHBEARER JWT retriever selection** (production clients): `sasl.oauthbearer.jwt.retriever.class` defaults to `DefaultJwtRetriever`. It delegates to `ClientCredentialsJwtRetriever` for http(s) token endpoints and to `FileJwtRetriever` for `file://` URLs. `ClientCredentialsJwtRetriever` picks its credential at config time in this order: (1) file assertion `sasl.oauthbearer.assertion.file`, (2) locally signed assertion (`sasl.oauthbearer.assertion.claim.iss` + private key), (3) `client.credentials.client.secret`. The choice is fixed for the client's lifetime, and a runtime failure does **not** fall back to the next option. The `jwt-bearer` grant needs `JwtBearerJwtRetriever` set explicitly.

- **Delegation token lifecycle (KIP-48)**: authenticate over SASL or SSL, then create the token (Admin API or `kafka-delegation-tokens.sh`), hand it to the workers, and renew or expire it. Clients authenticate with it through SCRAM (`tokenauth="true"`).

## Key Concepts
- **SASL_PLAINTEXT / SASL_SSL**: security protocols that run SASL over a cleartext or a TLS transport. `SASL_SSL` also needs the SSL config from ch18.
- **`KafkaServer` / `KafkaClient`**: the JAAS section names for the broker (which covers its outbound inter-broker logins too) and for clients.
- **`sasl.kerberos.service.name`**: must match the primary part of the broker's Kerberos principal (`kafka` in `kafka/host@REALM`). Set it on both brokers and clients.
- **`user_<name>="<pwd>"`**: PLAIN JAAS entries on the broker that define which users may connect, inter-broker user included.
- **SCRAM credential**: salt, iterations, StoredKey and ServerKey, stored in the **KRaft metadata log**. Iterations default to 4096 and 4096 is also the minimum.
- **Unsecured JWT**: the default OAUTHBEARER token format (`unsecuredLoginStringClaim_sub=...`). For dev and test only.
- **`OAuthBearerValidatorCallbackHandler`**: the built-in broker-side validator. It checks JWTs against `sasl.oauthbearer.jwks.endpoint.url`.
- **Client assertion (RFC 7523, KIP-1258)**: the client authenticates to the IdP with a signed JWT instead of a client secret, so the private key never leaves the client.
- **Delegation token**: a shared secret (token ID + HMAC) signed with `delegation.token.secret.key`. Its owner becomes the authenticated principal.
- **Principal mapping**: under the default `principal.builder.class`, the principal is the username (PLAIN/SCRAM), the OAuth `principalName`/`sub` (OAUTHBEARER), or the token owner (delegation token). ACLs (ch20) are written against it.

## Mental Models
- **Think of the mechanism choice as "where do identities already live?"**: Kerberos or Active Directory → GSSAPI. A Kafka-managed user list → SCRAM. An OAuth IdP (Okta, Entra, Keycloak) → OAUTHBEARER. An external password service → PLAIN with a custom `sasl.server.callback.handler.class`.
- **Prefer SCRAM over default PLAIN**: PLAIN's default store keeps cleartext passwords in the broker JAAS file and needs a restart to add users. SCRAM stores salted hashes in the metadata log, and you can add users dynamically with `kafka-configs.sh`.
- **Treat the KRaft controllers as the credential vault**: the security of both SCRAM credentials and delegation tokens reduces to "controllers are secure and on a private network". If they are not, plug in custom callback handlers.
- **Use delegation tokens when a framework fans work out to many workers**: it avoids shipping keytabs, TGTs or keystores to each one.

## Anti-patterns
- **PLAIN, SCRAM or OAUTHBEARER over `SASL_PLAINTEXT` in production**: passwords, SCRAM exchanges or bearer tokens can be sniffed. The docs require TLS for all three.
- **Default unsecured OAUTHBEARER in production**: anyone can mint arbitrary tokens. Replace it with the JWKS validator and a JWT retriever, or with custom handlers.
- **Static JAAS file on multi-identity clients**: it forces one user per JVM. Use `sasl.jaas.config` per client.
- **Short hostnames or IPs in `bootstrap.servers` / `advertised.listeners`**: they cause slow SASL handshakes through JRE reverse DNS lookups, and they break Kerberos, which requires resolvable FQDNs.
- **Creating the inter-broker SCRAM user after startup**: brokers cannot authenticate to each other. Seed the user with `kafka-storage.sh format --add-scram`.
- **Requesting delegation tokens over a token-authenticated connection**: this is refused. Token create, renew, expire and describe requests must come over a SASL or SSL authenticated channel.
- **Leaving `delegation.token.secret.key` unset or different across nodes**: token authentication and the token APIs fail. Set the same key on every broker and on the controllers.

## Code Examples
```properties
# Broker: two mechanisms on one listener, per-listener JAAS (highest precedence)
listeners=SASL_SSL://broker1.example.com:9093
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
sasl.enabled.mechanisms=SCRAM-SHA-256,PLAIN
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" password="admin-secret";
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="admin" password="admin-secret" user_admin="admin-secret" user_alice="alice-secret";
```
- **What it demonstrates**: the `listener.name.<listener>.<mechanism>.` prefix with one login module per mechanism, and inter-broker traffic pinned to SCRAM.

```properties
# Production OAUTHBEARER client: client_credentials with a locally signed assertion (no shared secret)
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;
sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/v1/token
sasl.oauthbearer.assertion.private.key.file=/path/to/private-key.pem
sasl.oauthbearer.assertion.algorithm=RS256
sasl.oauthbearer.assertion.claim.iss=my-kafka-client
sasl.oauthbearer.assertion.claim.sub=my-service-account
sasl.oauthbearer.assertion.claim.aud=https://example.com
sasl.oauthbearer.assertion.claim.exp.seconds=300
sasl.oauthbearer.assertion.claim.jti.include=true
sasl.oauthbearer.scope=my-application-scope
# Broker side:
# listener.name.<l>.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
# listener.name.<l>.oauthbearer.sasl.oauthbearer.jwks.endpoint.url=https://example.com/oauth2/v1/keys
```
- **What it demonstrates**: the RFC 7523 client assertion with a short expiry and a JTI claim to block replay, and the matching JWKS validation on the broker. The default implementation also needs `jackson-databind` added as a dependency.

```bash
# Delegation token: create (renewer user1), renew, expire
bin/kafka-delegation-tokens.sh --bootstrap-server localhost:9092 --create --max-life-time-period -1 \
  --command-config client.properties --renewer-principal User:user1 [--owner-principal User:owner1]
bin/kafka-delegation-tokens.sh --bootstrap-server localhost:9092 --renew  --renew-time-period -1  --command-config client.properties --hmac ABCDEFGHIJK
bin/kafka-delegation-tokens.sh --bootstrap-server localhost:9092 --expire --expiry-time-period -1 --command-config client.properties --hmac ABCDEFGHIJK
# Worker client config:
# sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<tokenID>" password="<HMAC>" tokenauth="true";
```
- **What it demonstrates**: the token lifecycle, and how token authentication rides on SCRAM, which must be enabled on the cluster. Describing another owner's tokens needs the `DESCRIBE_TOKEN` permission on that owner's User resource.

## Reference Tables

**Mechanism comparison**

| Mechanism | Login module | Credential store (default) | Production-ready by default? | Notes |
|---|---|---|---|---|
| GSSAPI | `com.sun.security.auth.module.Krb5LoginModule` | Kerberos KDC / AD | Yes | Needs FQDNs, keytab readable by the process user, `sasl.kerberos.service.name`, optional `-Djava.security.krb5.conf` |
| PLAIN | `...security.plain.PlainLoginModule` | Cleartext `user_<name>` in broker JAAS | No: override via `sasl.server.callback.handler.class` / `sasl.client.callback.handler.class` (2.0+) | TLS only |
| SCRAM-SHA-256/512 | `...security.scram.ScramLoginModule` | KRaft metadata log (salted) | Yes, if controllers are secure and private | Dynamic users; min 4096 iterations; TLS only |
| OAUTHBEARER | `...security.oauthbearer.OAuthBearerLoginModule` | Unsecured JWTs | No: use JWKS validator + JWT retriever | TLS only; needs `jackson-databind` |
| Delegation token | SCRAM module + `tokenauth="true"` | Controllers (metadata) | If controllers are private or traffic is encrypted | Secret is plaintext in server.properties |

**Configs you actually tune**

| Config | Default | Meaning | When to change |
|---|---|---|---|
| `sasl.enabled.mechanisms` | — | Mechanisms the broker accepts | Add or remove during a mechanism migration |
| `sasl.mechanism.inter.broker.protocol` | — | Mechanism brokers use with each other | Switch after all brokers accept the new one |
| SCRAM `iterations` | 4096 (minimum) | PBKDF iteration count per credential | Raise (e.g. 8192) to resist brute force |
| `delegation.token.expiry.time.ms` | 24 h | Renewal interval | Shorten for tighter exposure |
| `delegation.token.max.lifetime.ms` | 7 days | Max renewable life | Match the job duration |
| `unsecuredLoginLifetimeSeconds` | 3600 | Dev token lifetime | Test refresh behavior |
| `unsecuredValidatorAllowableClockSkewMs` | 0 | Dev validator clock skew | Clocks drift in test environments |
| `sasl.login.refresh.window.factor` / `.jitter` / `.min.period.seconds` / `.min.buffer.seconds` | sensible defaults | OAUTHBEARER token refresh timing | Rarely. Only if tokens expire before they are refreshed |
| `sasl.oauthbearer.expected.audience` / `.expected.issuer` / `.clock.skew.seconds` | — | Broker JWT validation constraints | Always set `audience` and `issuer` in production |

## Worked Example
**Stand up SCRAM-SHA-256 on a new KRaft cluster and onboard user alice.**
1. Seed the inter-broker credential while formatting storage, before any broker starts:
   `bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties --add-scram 'SCRAM-SHA-256=[name="admin",password="admin-secret"]'`
2. Broker JAAS (`kafka_server_jaas.conf`): `KafkaServer { org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret"; };`. Pass it with `-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf`, or use the `listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config` property instead.
3. `server.properties`: `listeners=SASL_SSL://host:9093`, `security.inter.broker.protocol=SASL_SSL`, `sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256`, `sasl.enabled.mechanisms=SCRAM-SHA-256`, plus the SSL keystore and truststore (ch18). Start the brokers.
4. Add alice dynamically, with no restart:
   `bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=alice-secret]' --entity-type users --entity-name alice --command-config client.properties`
   The salt is random when you omit it. Verify with `--describe --entity-type users --entity-name alice`.
5. Alice's client: `security.protocol=SASL_SSL`, `sasl.mechanism=SCRAM-SHA-256`, `sasl.jaas.config=...ScramLoginModule required username="alice" password="alice-secret";`. Her principal is `User:alice`. Grant ACLs as in ch20.
6. Rotate or revoke: run `--alter --add-config` again with a new password, which applies to **new connections**. Use `--alter --delete-config 'SCRAM-SHA-256'` to remove the credential.

## Key Takeaways
1. Use `SASL_SSL` for every mechanism in production. Only Kerberos is safe without TLS, and even then TLS adds encryption.
2. Prefer per-client `sasl.jaas.config` and per-listener `listener.name.<l>.<mech>.sasl.jaas.config` over static JAAS files. You get multiple identities per JVM and the highest precedence.
3. Choose SCRAM for Kafka-managed users. Seed the inter-broker user with `kafka-storage.sh format --add-scram`, manage everyone else with `kafka-configs.sh`, and use iterations of 4096 or more.
4. Never ship default OAUTHBEARER (unsecured JWT) to production. Configure `OAuthBearerValidatorCallbackHandler` with a JWKS URL on brokers, and a JWT retriever on clients, preferably client assertion with a short `exp` and `jti`.
5. Change mechanisms with the 4-step rolling sequence: enable on brokers, move clients, switch inter-broker, then remove the old mechanism.
6. Delegation tokens need SCRAM enabled and the same `delegation.token.secret.key` on all brokers and controllers. Rotating the secret means expiring all tokens, rolling the cluster, and reissuing tokens.
7. Use FQDNs in `bootstrap.servers` and `advertised.listeners` to avoid slow handshakes and Kerberos failures.

## Connects To
- **ch18**: SSL/TLS and listener setup. `SASL_SSL` depends on it.
- **ch20**: the SASL principal (username, `sub` or token owner) is what ACLs authorize.
- **ch15**: KRaft controllers store the SCRAM credentials and the delegation token metadata, and `kafka-storage.sh format` seeds them.
- **ch04**: dynamic broker configs and the per-listener config prefixes.
- **ch21 / ch29**: Connect workers and Streams apps set the same client `sasl.*` and `security.protocol` properties.
- **ch10**: operational CLI tools such as `kafka-configs.sh` and rolling restarts.
