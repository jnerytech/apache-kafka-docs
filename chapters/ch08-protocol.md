# Chapter 08: Kafka Wire Protocol Guide

## Core Idea
Kafka speaks a size-delimited binary request/response protocol over persistent TCP connections. The client owns partitioning, routing (via cached metadata) and version negotiation (via `ApiVersionsRequest`), so a correct client is "bootstrap → metadata → route to leader → refresh on error", using the highest API version both sides support. (The source is thin here. The generated tables for primitive types, error codes, API keys and per-message BNF are not in this extract, so this chapter is shorter than the target and covers the protocol's rules and sequences, not byte layouts.)

## Frameworks Introduced
- **Bootstrap → Metadata → Leader-routing loop**: the client keeps a cached cluster map and refreshes it only when an error tells it the map is stale.
  - When to use: in every client implementation, and when you are debugging "wrong broker" or stale-routing problems.
  - How: (1) cycle through a list of 2–3 bootstrap URLs until one connects, then fetch cluster metadata (topics, partitions, leader per partition, broker host/port); (2) send each produce/fetch to the current **leader** of that partition; (3) on a socket error or a "broker no longer hosts this partition" error code (`NotLeaderForPartition`), refresh metadata and retry.
  - Why it works / failure mode: metadata is dynamic, so a static mapping file goes stale. Polling wastes work, so errors act as the invalidation signal. The broker enforces leadership, which means a misrouted request fails loudly instead of being silently accepted. If you configure only one bootstrap host, you have a single point of failure at startup.
- **API versioning with bidirectional compatibility**: every request carries a 16-bit `api_key` and a 16-bit `api_version`, and together they identify the exact schema.
  - When to use: whenever a client and broker may run different releases. The policy is that new clients work with old brokers and old clients work with new brokers.
  - How: support a range of versions per API, learn the broker's range per connection, pick `max(common versions)`, and report an error if there is no overlap. The server rejects versions it does not support and always answers in the format of the version it was sent (except for the ApiVersions fallback below). Roll out features on the server first, and clients adopt them as they upgrade.
  - Why it works / failure mode: you can upgrade clients or brokers independently without downtime. The failure mode is caching versions across reconnects: version info is valid **only for the connection it was obtained on**, because the broker may have been upgraded or downgraded since.
- **ApiVersions handshake (KIP-35, broker ≥ 0.10.0.0; fallback via KIP-511, broker ≥ 2.4.0)**:
  - How: (1) after TCP connect (and after the TLS handshake if SSL is used), send `ApiVersionsRequest`; (2) the broker returns all supported ApiKeys and version ranges *regardless of auth state*, so this works even before SASL; (3) if the client's `ApiVersionsRequest` version is too new, a broker ≥ 2.4.0 replies with a **v0** `ApiVersionsResponse` with `UNSUPPORTED_VERSION` and `api_versions` filled with its supported ApiVersions range, and the client then retries at the highest common version; (4) brokers older than 0.10.0.0 ignore the request or close the connection.
- **SASL authentication sequence**: `ApiVersionsRequest` (optional) → `SaslHandshakeRequest` (names the mechanism) → token exchange → Kafka requests.
  - How: if the mechanism is not enabled, the broker returns its enabled mechanisms and **closes** the connection. With `SaslHandshake` v0, SASL tokens travel as raw opaque packets with no Kafka header. With v1, the tokens are wrapped in `SaslAuthenticate` request/response messages. The final broker message's error code tells you whether authentication succeeded. On failure the broker closes the connection.
  - Legacy: for 0.9.0.x compatibility, a first packet that is not a valid Kafka request is treated as a SASL/GSSAPI token, which skips the handshake.
- **KIP-482 tagged fields**: optional fields that can be added to a message without bumping its version.
  - When to use: for rarely set fields, because an unset tagged field takes 0 bytes.
  - How / caveat: receivers that don't know the tag **silently ignore** it. If the sender needs the receiver to understand the field, bump the version instead.

## Key Concepts
- **Size-delimited framing**: every message is `Size (int32) + payload`. A reader reads 4 bytes to get N, then reads N bytes.
- **Per-connection ordering**: on one TCP connection, the broker processes requests in send order and returns responses in the same order. It handles only one in-flight request per connection to guarantee this.
- **Pipelining**: clients may (and should) send more requests while earlier ones are outstanding, using non-blocking IO. The pending requests buffer in the OS socket buffer.
- **Max request size**: a broker-side limit. A request over it causes the broker to **disconnect the socket**; you don't get a polite error.
- **Client-controlled partitioning**: the broker enforces no key→partition semantics. Producers address a specific partition, so two clients must use the same hash to agree on placement.
- **Semantic partitioning**: hashing a message key (for example, a user id) so all related records land in one partition. This keeps per-key ordering and lets one consumer hold that key's local state.
- **Batching (multi-partition)**: produce and fetch APIs always take sequences of messages, and a single request can span many topics and partitions.
- **Message structure**: `Message => RequestOrResponseHeader Body`. Header versions vary with the API version.
- **Error codes / ApiKeys**: numeric constants that the client translates into language-level exceptions or request types.

## Mental Models
- Think of the broker as a **dumb, strict router**: it accepts data only for partitions it leads, and it never picks a partition for you. All placement intelligence lives in the client.
- Think of **errors as cache invalidation**: don't poll metadata; refresh it when a socket error or a not-leader error tells you to.
- Use **one connection per broker** per client instance, not a pool. Get throughput from pipelining and batching on that connection.
- Use a **tagged field** when absence is harmless. Use a **version bump** when the receiver must understand the field.

## Anti-patterns
- **Connection pooling to a single broker**: this isn't needed. Ordering is per connection, and pipelining gives you throughput.
- **Opening a new TCP connection per request**: you pay the handshake every time. Keep connections persistent.
- **Bootstrapping from a single host**: if that host is down, the client can't discover the cluster. List 2–3 hosts or put a load balancer in front.
- **Static topic→broker mapping files**: leadership changes over time, so the mapping goes stale.
- **Caching `ApiVersions` across reconnects**: the broker may have been upgraded or downgraded in between.
- **Inconsistent key hashing across producers**: the same key lands in different partitions, which breaks ordering and co-location.
- **Sending one message per request**: you give up the protocol's single biggest performance win, which is batching.
- **Relying on a tagged field the receiver must honor**: an older receiver drops it silently.

## Code Examples
```
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
Message => RequestOrResponseHeader Body
```
- **What it demonstrates**: the framing every client parser starts with. Read an int32 length, read that many bytes, then decode a versioned header followed by an API-specific body.

```
connect → [TLS handshake] → ApiVersionsRequest
  ├─ ok: pick max common version per ApiKey (per connection)
  └─ UNSUPPORTED_VERSION (v0 response, broker ≥2.4) → retry ApiVersions at highest common version
→ SaslHandshakeRequest(mechanism)          # SASL listeners only
  ├─ mechanism not enabled → list returned, connection closed
  └─ v1: SaslAuthenticate tokens… → final error_code → Kafka API requests
```
- **What it demonstrates**: the order of the connection-setup handshakes, and where a connection gets closed instead of returning an error.

## Reference Tables
| Item | Rule / value | Implication |
|---|---|---|
| `api_key`, `api_version` | two 16-bit ints sent with every request | together they select the exact schema |
| Version choice | highest version supported by both sides | if there is no overlap, surface an error to the user |
| ApiVersions availability | broker ≥ 0.10.0.0 (KIP-35) | older brokers ignore the request or drop the connection |
| ApiVersions fallback | broker ≥ 2.4.0 (KIP-511) | the v0 response carries `UNSUPPORTED_VERSION` and the supported ApiVersions range |
| ApiVersions vs. auth | answered before SASL | to hide the broker version, use SSL client auth (happens before any Kafka request) |
| Version deprecation | marked as deprecated in the protocol docs | plan client upgrades ahead of removal |
| Oversized request | socket disconnected | size batches below the broker's request limit |
| In-flight per connection (broker side) | 1 processed at a time | ordering is guaranteed, and the client pipelines anyway |

| Partitioning goal | Strategy | Trade-off |
|---|---|---|
| Load balance only | round-robin across brokers/partitions | spreads load but loses key ordering |
| Load balance, many producers ≫ brokers | each producer picks one random partition | far fewer TCP connections |
| Ordering/locality per key | hash(key) mod P | all producers must use the same hash, and hot keys skew load |

## Worked Example
**A client that survives a leader failover.** The client is configured with `b1:9092,b2:9092,b3:9092`.
1. `b1` is down, so the client moves on and connects to `b2`. Over TLS, it sends `ApiVersionsRequest` after the handshake and records the version ranges *for this connection*.
2. The listener is SASL, so the client sends `SaslHandshakeRequest` v1 with its mechanism, then exchanges `SaslAuthenticate` tokens until the final response's error code is success.
3. The client sends a Metadata request: topic `clicks` has 6 partitions, and the leader of partition 3 is `b3`.
4. The producer hashes `user_id` to partition 3. It opens one connection to `b3` (repeating ApiVersions and SASL there) and pipelines batched produce requests that also carry data for other partitions led by `b3`.
5. Leadership of partition 3 moves to `b1`. The next produce to `b3` returns `NotLeaderForPartition`.
6. The client refreshes metadata, sees that `b1` now leads partition 3, connects to `b1` (new ApiVersions, because the info is per connection), and retries. Key→partition placement is unchanged, so per-user ordering still holds.

## Key Takeaways
1. Keep one persistent connection per broker. Get throughput from pipelining and multi-partition batching, not from pools.
2. Always send data to the partition leader. Treat socket errors and not-leader errors as the signal to refresh metadata, and don't poll.
3. Bootstrap from 2–3 hosts or a load balancer. Any single broker can give you the whole cluster map.
4. Negotiate `ApiVersions` on every new connection and use the highest common version. Handle the v0 `UNSUPPORTED_VERSION` fallback.
5. Partitioning belongs to the client. If you need per-key order or state co-location, every producer must hash keys identically.
6. Use tagged fields (KIP-482) for optional, ignorable additions. Bump the version when the receiver must understand the change.
7. On SASL listeners, an unknown mechanism or a failed authentication closes the connection. Clients must treat a close at that point as an authentication failure, not a network error.

## Connects To
- **ch03**: the Java producer/consumer/admin clients implement this protocol, including metadata refresh and version negotiation.
- **ch05**: the producer's batching and partitioner and the consumer's fetch design are the client-side use of these APIs.
- **ch09**: the record batch format, which is the produce/fetch payload referenced by this chapter.
- **ch07**: replication and leadership explain why only the leader accepts produce/fetch requests.
- **ch18 / ch19**: SSL and SASL setup that feeds the handshake sequence above.
- **ch30**: upgrade notes, where bidirectional compatibility and deprecated protocol versions matter in practice.
