# Chapter 18: Security — Overview, Listeners and SSL

## Core Idea
Kafka security is configured per listener: each named listener maps to one security protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`). TLS on a listener only works if the PKI underneath it is correct (SAN entries, both EKU usages, full chain, a truststore on both sides). To secure a running cluster, open the new secured port first, then switch traffic to it over several rolling bounces, and close `PLAINTEXT` last.

## Frameworks Introduced
- **Listener → protocol mapping**: `listeners={NAME}://{host}:{port},...` declares the endpoints, and `listener.security.protocol.map=NAME:PROTOCOL,...` assigns each one a protocol. Every server needs at least one listener.
  - When to use: on every broker or controller. Prefer descriptive names (`CLIENT`, `BROKER`, `CONTROLLER`) over protocol-named listeners (`SSL://...`) because the name shows what the port is for.
  - How: (1) give each traffic class its own name; (2) map each name to a protocol; (3) set `inter.broker.listener.name` for replication traffic (if you don't, Kafka picks the listener from `security.inter.broker.protocol`, default `PLAINTEXT`); (4) set `controller.listener.names` for KRaft controller traffic. It must differ from the inter-broker listener.
  - Why it works / failure mode: each traffic class (clients, replication, controller quorum) gets its own port, so you can isolate them at the network level and give each its own protocol. Common failure: forgetting that broker-only nodes must still define the controller listener's protocol in the map, because they connect *to* controllers.

- **KRaft role-dependent listener layout**:
  - Broker-only (`process.roles=broker`): put the controller listener in `controller.listener.names` and in `listener.security.protocol.map`, but **not** in `listeners`, because the broker does not expose it. The controller endpoints come from `controller.quorum.bootstrap.servers` (the text also mentions `controller.quorum.voters`).
  - Combined (`process.roles=broker,controller`): the controller listener **must** also appear in `listeners`.
  - Rule: the host:port in `controller.quorum.bootstrap.servers` must route to an exposed controller listener.
  - Multiple controller listeners are allowed. The controller accepts on all of them, and outbound requests use the **first** one in the list. Use this to move the controller to a new port or protocol with two rolls: one to add the new listener, one to remove the old.

- **Build-your-own PKI (keytool + OpenSSL `ca`)**: a keypair per broker, a CSR with SAN, a self-run CA, signing, then importing the CA and the signed cert into the keystore, and the CA into every truststore.
  - When to use: sandbox, dev and test. In production, send CSRs to the corporate CA.
  - How: (1) `keytool -genkey -keyalg RSA -storetype pkcs12` on the broker itself, so the private key never leaves it; (2) create the CSR with `-ext SAN=DNS:{FQDN},IP:{IP}`; (3) build the CA with `openssl req -x509` and an `openssl-ca.cnf` that contains `copy_extensions = copy`; (4) sign with `openssl ca -policy signing_policy -extensions signing_req`; (5) import `CARoot`, then the signed cert under the same alias as the key; (6) import the CA into `client.truststore.jks`, and into `server.truststore.jks` too if `ssl.client.auth` is `requested` or `required`.
  - Why it works / failure mode: the chain of trust means one shared truststore holding only the CA lets every node authenticate every other node. Use the OpenSSL `ca` module rather than `x509`, because `x509` does not copy SAN extensions from the CSR (an OpenSSL bug), so hostname verification would fail.

- **Phased security rollout ("incremental bounces")**: (1) bounce to open the secured port(s); (2) restart clients against the secured port; (3) bounce to enable broker-to-broker security; (4) a final bounce to close `PLAINTEXT`.
  - Rules: client-side and inter-broker changes go in **separate** bounces. Keep a `PLAINTEXT` port open until the last step. Stop brokers with SIGTERM, and wait for restarted replicas to rejoin the ISR before moving to the next node.

## Key Concepts
- **Security protocol**: one of `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` (case-insensitive). It sets whether a listener encrypts (SSL), authenticates (SASL or mTLS), or both.
- **SASL mechanisms supported**: GSSAPI/Kerberos (0.9.0.0), PLAIN (0.10.0.0), SCRAM-SHA-256/512 (0.10.2.0), OAUTHBEARER (2.0).
- **Mixed-security cluster**: security is optional. One cluster can serve authenticated, unauthenticated, encrypted and plaintext clients at the same time on different listeners.
- **Hostname verification**: the client checks the server's FQDN or IP against the certificate's SAN (or the deprecated CN) to block man-in-the-middle attacks. It has been on by default since Kafka 2.0.0, for both clients and inter-broker connections, through `ssl.endpoint.identification.algorithm`.
- **Keystore vs truststore**: the keystore holds a node's own private key and its signed cert chain, and must be kept secret. The truststore holds the CA certs it trusts. It is not sensitive, and you can share one across all nodes.
- **Key validity vs cert validity**: `-validity` on the key is separate from the certificate lifetime the CA sets. One 10-year key can back ten 1-year certificates.
- **PEM stores (2.7.0+)**: you can put key and trust material inline in the config with `ssl.keystore.key`, `ssl.keystore.certificate.chain` and `ssl.truststore.certificates`. Store passwords are ignored for PEM; only `ssl.key.password` applies, and only if the key is encrypted.
- **Extended Key Usage (EKU)**: a broker cert needs both `serverAuth` and `clientAuth`, because in intra-cluster traffic every broker acts as both client and server.
- **Intermediate CA chain**: if an intermediate CA signed the cert, import the full chain up to the root (cat the certs into one file, then run keytool).
- **SSL performance cost**: enabling SSL slows things down. How much depends on the CPU and the JVM implementation.

## Mental Models
- Think of a **listener as a door with its own lock**. `listeners` decides which doors exist, the protocol map decides which lock each door has, and `inter.broker.listener.name` and `controller.listener.names` decide which door staff use.
- Think of the **CA as a passport-issuing government**. Trusting the CA (putting it in the truststore) means trusting every certificate it signed. That is why one CA and one shared truststore scale to a large cluster.
- Use **SAN, not CN**, for hostname verification. CN matching has been deprecated since 2000, and SAN takes several DNS and IP entries. With SAN handling hostnames, CN is free to carry a meaningful identity for authorization.
- Get **hostname verification right at deployment time**. Disabling it to "just get it to work" is cheap at first, and turning it back on once the cluster is live is hard.

## Anti-patterns
- **Disabling hostname verification** (`ssl.endpoint.identification.algorithm=`): removes MITM protection. It is almost always a temporary hack that never gets reverted.
- **`ssl.client.auth=requested`**: gives a false sense of security, because clients without certificates still connect. Use `required` or `none`.
- **Using a webserver signing profile from the corporate CA**: those certs carry only `serverAuth`, so the inter-broker TLS handshake fails.
- **Trusting the CA to copy your SANs**: CA operators often drop requested extensions. Check every signed cert with `openssl x509 -in certificate.crt -text -noout`.
- **Signing with `openssl x509`**: does not copy the CSR's SAN extensions into the cert, so hostname verification breaks.
- **Omitting `ssl.truststore.password`**: the truststore still loads, but its integrity check is disabled.
- **Enabling client and inter-broker security in one bounce, or closing `PLAINTEXT` early**: nodes or clients lose a common protocol in the middle of the roll.
- **Reusing the inter-broker listener as the controller listener**: not allowed in KRaft. The controller listener must be separate and kept away from clients.
- **Accepting old protocols**: SSL is deprecated in favor of TLS. Don't allow SSL protocols in production.

## Code Examples
```properties
# Broker-only KRaft node: controller listener mapped but not exposed
process.roles=broker
listeners=BROKER://localhost:9092
inter.broker.listener.name=BROKER
controller.quorum.bootstrap.servers=localhost:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=BROKER:SASL_SSL,CONTROLLER:SASL_SSL
# Combined node: add CONTROLLER://localhost:9093 to listeners
```
- **What it demonstrates**: a broker must still declare the controller listener's name and protocol to reach the quorum, even though it doesn't listen on that port.

```bash
# Broker key + CSR with SAN, then sign with own CA and import
keytool -keystore server.keystore.jks -alias localhost -validity {validity} -genkey \
  -keyalg RSA -destkeystoretype pkcs12 -ext SAN=DNS:{FQDN},IP:{IPADDRESS1}
echo 01 > serial.txt && touch index.txt
openssl req -x509 -config openssl-ca.cnf -newkey rsa:4096 -sha256 -nodes -out cacert.pem -outform PEM
openssl ca -config openssl-ca.cnf -policy signing_policy -extensions signing_req \
  -out {server certificate} -infiles {csr}
keytool -keystore {keystore} -alias CARoot -import -file {CA certificate}
keytool -keystore {keystore} -alias localhost -import -file cert-signed
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
```
- **What it demonstrates**: the full PKI pipeline. The `copy_extensions = copy` line in `openssl-ca.cnf`, together with the `ca` module, carries the SAN into the cert. Both the CA and the signed cert go into the keystore.

```properties
# Broker SSL
ssl.keystore.location=/var/private/ssl/server.keystore.jks
ssl.keystore.password=test1234
ssl.key.password=test1234
ssl.truststore.location=/var/private/ssl/server.truststore.jks
ssl.truststore.password=test1234
ssl.client.auth=required
security.inter.broker.protocol=SSL
# Client (client-ssl.properties); keystore lines only if broker requires client auth
security.protocol=SSL
ssl.truststore.location=/var/private/ssl/client.truststore.jks
ssl.truststore.password=test1234
```
- **What it demonstrates**: the minimal mTLS setup. Pass the client file to the CLI tools with `--command-config client-ssl.properties`. Verify the broker with `openssl s_client -debug -connect localhost:9093 -tls1`; the broker's certificate should appear in the output.

## Reference Tables

| Config | Default | Meaning | When to change |
|---|---|---|---|
| `listeners` | — (≥1 required) | `NAME://host:port` list of endpoints | Add a secured port before migrating traffic to it |
| `listener.security.protocol.map` | — | Maps listener name → protocol | Always set when you use custom listener names |
| `inter.broker.listener.name` | unset | Listener used for replication | Set explicitly; prefer it over `security.inter.broker.protocol` |
| `security.inter.broker.protocol` | `PLAINTEXT` | Fallback inter-broker protocol | Set to `SSL`/`SASL_SSL` in its own bounce |
| `controller.listener.names` | — | KRaft controller listener(s); first is used outbound | Must differ from the inter-broker listener |
| `ssl.endpoint.identification.algorithm` | hostname verification on (since 2.0) | Empty string disables it | Practically never |
| `ssl.client.auth` | `none` | `required` = mTLS; `requested` = optional cert | Use `required` for mTLS; avoid `requested` |
| `ssl.enabled.protocols` | — | Accepted TLS versions | Client must share at least one with the broker; drop old SSL/TLS versions |
| `ssl.cipher.suites` | empty list | Allowed cipher suites | Restrict for compliance |
| `ssl.keystore.type` / `ssl.truststore.type` | `JKS` | Store format (also PKCS12, PEM) | Prefer PKCS12; JKS is deprecated |
| `ssl.secure.random.implementation` | JVM default | PRNG for TLS | Set `SHA1PRNG` if `NativePRNG`'s global lock slows things down (shown to handle 50 MB/s plus replication per broker) |
| `ssl.provider` | JVM default provider | Security provider | Custom crypto providers |
| `ssl.keystore.key` / `ssl.keystore.certificate.chain` / `ssl.truststore.certificates` | — | Inline PEM material (2.7.0+) | No store files on disk; externalize or encrypt the values |

| Symptom | Cause | Fix |
|---|---|---|
| Inter-broker handshake fails with a corporate cert | EKU has only `serverAuth` | Re-issue with both `serverAuth` and `clientAuth` |
| Hostname verification fails | SAN dropped (OpenSSL `x509` or CA policy) | Use `openssl ca` with `copy_extensions = copy`; inspect with `openssl x509 -text` |
| Chain validation fails | Intermediate CA missing | Concatenate the full chain to the root, then import |
| Need AES-256 on an older Oracle JRE | Export-restricted crypto | Install JCE Unlimited Strength policy files |

## Worked Example
**Goal: encrypt client and inter-broker traffic with SSL and add SASL authentication for clients, on a live cluster.**
1. **Bounce 1 (open the ports)**: on each broker, set `listeners=PLAINTEXT://broker1:9091,SSL://broker1:9092,SASL_SSL://broker1:9093`. Stop each broker with SIGTERM, restart it, and wait for its replicas to rejoin the ISR before moving to the next broker.
2. **Move the clients**: restart them with `bootstrap.servers=broker1:9093,...` and `security.protocol=SASL_SSL`, plus their SASL settings.
3. **Bounce 2 (inter-broker)**: keep the same `listeners` and add `security.inter.broker.protocol=SSL`, so replication moves to port 9092. `PLAINTEXT` stays open so that brokers not yet restarted can still replicate.
4. **Bounce 3 (close `PLAINTEXT`)**: set `listeners=SSL://broker1:9092,SASL_SSL://broker1:9093` and `security.inter.broker.protocol=SSL`.

For the simpler SSL-only version, use the same steps with only `SSL://broker1:9092` added. The end state is `listeners=SSL://broker1:9092` with `security.inter.broker.protocol=SSL`.

## Key Takeaways
1. Give listeners descriptive names, map each name to a protocol, and set `inter.broker.listener.name` explicitly. Without it, replication falls back to `security.inter.broker.protocol=PLAINTEXT`.
2. In KRaft, keep the controller listener separate from the inter-broker listener and away from clients. Broker-only nodes still map its protocol but don't list it in `listeners`.
3. Put hostnames in the SAN when you create the CSR, sign with OpenSSL `ca` (`copy_extensions = copy`), and confirm every issued cert with `openssl x509 -text`.
4. Broker certs need both the `serverAuth` and `clientAuth` EKUs and the full intermediate chain. Webserver signing profiles break inter-broker TLS.
5. Use `ssl.client.auth=required` for mTLS, never `requested`, and always set `ssl.truststore.password`.
6. Roll security out in separate bounces: open the port, move clients, switch inter-broker traffic, close `PLAINTEXT`. Use SIGTERM and wait for the ISR between nodes.
7. If TLS slows things down, try `ssl.secure.random.implementation=SHA1PRNG` before blaming Kafka.

## Connects To
- **ch19**: SASL mechanisms (GSSAPI, PLAIN, SCRAM, OAUTHBEARER) for the `SASL_PLAINTEXT` and `SASL_SSL` listeners defined here.
- **ch20**: authorization and ACLs act on the principal established by SSL or SASL authentication; the cert's CN/DN becomes the principal.
- **ch15**: KRaft `process.roles`, controller quorum and `controller.quorum.bootstrap.servers`, which the controller listener rules depend on.
- **ch04**: dynamic broker configs (per-listener `listener.name.<name>.ssl.*` via `kafka-configs.sh`) and config providers for keeping secrets out of the files.
- **ch10**: rolling restarts and controlled shutdown, which the phased security rollout relies on.
- **ch12**: multi-tenancy uses listeners, authentication and quotas to separate tenants.
- **ch13**: JVM and crypto provider choices that affect the cost of TLS.
