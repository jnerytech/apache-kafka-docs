# Chapter 07: Design — Replication, Log Compaction and Quotas

## Core Idea
Kafka keeps data safe with a leader-based replicated log whose commit point is "all current ISR members have it", not a majority vote. That makes `f+1` replicas enough to survive `f` failures, but you only get real durability when you pair `acks=all` with `min.insync.replicas` and leave unclean election off. Compaction applies the same log to keyed state by keeping at least the last value per key. Quotas protect shared brokers by delaying and muting clients on each broker.

## Frameworks Introduced
- **ISR-based replication (Kafka's quorum model)**: each partition has one leader and N−1 followers. Followers *pull* from the leader like consumers do, which batches the writes naturally. A write is committed only when **all** in-sync replicas have it, and only ISR members can be elected leader. The ISR is persisted in cluster metadata every time it changes.
  - When to use: always. A topic with replication factor 1 is still a replicated topic, with RF=1.
  - How a replica stays "in sync": (1) it keeps an active session with the controller. In KRaft that means heartbeats within `broker.session.timeout.ms`. (2) As a follower it reaches the leader's log end within `replica.lag.time.max.ms`. If it fails (1), the controller drops it. If it fails (2), the leader drops it.
  - Why it works: with `f+1` replicas Kafka survives `f` failures. A majority quorum needs `2f+1` for the same tolerance, so the ISR model costs less disk and gives more throughput. Any ISR member is a valid leader, which also keeps leadership balanced across many partitions.
  - Failure mode: the ISR can shrink to just the leader. `acks=all` then means "acked by 1", and losing that one broker loses the write. `min.insync.replicas` exists to close this gap.
- **Commit and visibility rule**: consumers only see a message after (1) all ISR members have replicated it and (2) the ISR size is at least `min.insync.replicas`. This holds whatever `acks` the producer used. `acks` only decides when the *producer* gets its ack: `0` means no wait (replication is async), `1` means the leader has it, `all`/`-1` means the full current ISR has it.
- **Unclean leader election (availability vs consistency)**: if every ISR replica dies, Kafka can do one of two things. (a) Wait for an ISR member to come back. This is consistent, but the partition stays down as long as they do, and forever if their disks are gone. (b) Elect the first replica that returns. That restores availability, but its log becomes the truth and committed messages can be lost. Since 0.11.0.0 the default is (a). Set `unclean.leader.election.enable=true` to choose (b).
- **Log compaction (per-key retention)**: the log has a *head* that is uncompacted, with dense offsets, and a *tail* that is compacted. The cleaner removes a record whenever a later record has the same key. Offsets never change and order is never changed.
  - When to use: keyed, mutable data. Examples are DB change subscription (CDC into cache, search, or Hadoop), event sourcing, and journaling local state for HA (Streams and Samza changelogs).
  - How the cleaner works: (1) pick the log with the highest head-to-tail ratio. (2) Build a compact hash map of the last offset for each key in the head, at 24 bytes per entry. (3) Recopy the segments from start to end, dropping keys that appear again later. (4) Swap the clean segments in right away, so the extra disk needed is only one segment.
  - Failure mode: a consumer that lags more than `delete.retention.ms` behind the head can miss tombstones, so its rebuilt state keeps records that should have been deleted.
- **Quota enforcement by delay + mute**: when a client goes over its quota, the broker works out the delay that brings it back under. It replies immediately with that throttle time (a fetch reply carries no data) and mutes the client's channel until the delay ends. Newer clients also stop sending for that time. Old clients that ignore the delay still get back-pressure because the socket is muted.

## Key Concepts
- **Replication factor**: the total number of replicas of a partition, leader included. It is set per topic.
- **ISR (in-sync replicas)**: the replicas that are alive and caught up. Only they can become leader, and all of them must have a write before it is committed.
- **Controller**: in KRaft, the node that tracks broker registration and sessions. It removes dead brokers from ISRs and, when a broker fails, elects new leaders from the ISR in batches.
- **Committed message**: a message that is on every ISR member while the ISR size is at least `min.insync.replicas`. Kafka guarantees it is not lost as long as at least one ISR replica stays alive.
- **Fail/recover model**: Kafka handles crash-and-restart failures, not Byzantine ones. It also does not assume stable storage. A replica that restarts must fully re-sync before it rejoins the ISR, so per-write fsync is not needed.
- **Tombstone**: a record with a key and a null value. It deletes earlier values for that key and is itself removed after `delete.retention.ms`.
- **Compacted-away offset**: an offset that is still a valid position after its record is removed. Reading from it returns the next surviving offset, so reads from 36, 37, or 38 all start at 38.
- **Quota group**: the (user, client-id), user, or client-id entity that a quota applies to. All connections in the group share it, and it is enforced **per broker**.
- **Request rate quota**: `n%` means n% of one thread. Total capacity is `(num.io.threads + num.network.threads) * 100%`, so in practice this caps CPU use.

## Mental Models
- **Think of `acks=all` as "all of the current ISR", not "all replicas".** Its strength depends on how large the ISR is right now. `min.insync.replicas` is the floor that turns it into a real durability guarantee.
- **Use the ISR model when you want majority-quorum safety at a lower replication factor.** To survive 1 failure, a majority quorum needs 3 replicas and the ISR model needs 2, with one ack in both. What you give up is that a commit waits on the slowest ISR member.
- **Think of a compacted topic as a table snapshot plus a recent changelog in one place.** Live consumers read every update from the head. A consumer that rebuilds from offset 0 gets at least the final state of every key.
- **Use per-broker quotas, not cluster-wide ones.** Enforcing per broker needs no shared usage accounting. Remember that a client's effective cluster-wide limit is roughly the quota times the number of brokers it talks to.

## Anti-patterns
- **`acks=all` with `min.insync.replicas=1`**: when the ISR shrinks to the leader, writes are still acked, and the leader's next failure loses them.
- **Enabling unclean leader election on topics that must not lose data**: a stale replica becomes leader and committed messages silently disappear. Turn it on only where uptime matters more than consistency.
- **Majority-vote thinking for capacity planning**: 5 copies to survive 2 failures means 5x disk and 1/5 throughput. Kafka's ISR model needs RF=3 for the same tolerance.
- **Treating compaction as immediate dedup**: the active segment is never compacted, and timing depends on the dirty ratio, the lag settings, and whether a cleaner thread is free. Consumers must tolerate duplicate keys.
- **Consumers that bootstrap from a compacted topic slower than `delete.retention.ms`**: they miss tombstones and keep deleted keys.
- **Large quota windows** (e.g. 10 × 30 s): they allow big bursts and then long stalls. Keep windows small, like the default many-small-windows design (e.g. 30 × 1 s).

## Code Examples
```properties
# Durable topic: survive 1 broker loss without losing acked writes
replication.factor=3            # set at topic creation
min.insync.replicas=2           # topic config
unclean.leader.election.enable=false
# producer
acks=all
```
- **What it demonstrates**: the standard durability combination. Writes fail with NotEnoughReplicas once the ISR drops below 2, so the partition gives up write availability rather than risk loss.

```properties
# Compacted changelog topic (topic-level names; broker defaults are log.cleanup.policy / log.cleaner.*)
cleanup.policy=compact
min.compaction.lag.ms=60000      # keep at least 1 min of uncompacted head
max.compaction.lag.ms=86400000   # force eligibility within 1 day for low-traffic topics
delete.retention.ms=86400000     # tombstones kept 24h (default)
```
- **What it demonstrates**: a lower bound on how long records stay in the head, an upper bound on when a record becomes eligible for compaction, and how long tombstones stay visible to consumers rebuilding state.

```bash
# Quota for user1 + clientA (written to the metadata log, effective immediately, no restart)
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152,request_percentage=200' \
  --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
# Default quota for all users
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=1048576' --entity-type users --entity-default
```
- **What it demonstrates**: dynamic quota overrides and defaults set at the (user, client-id) and user levels. The full CLI is in ch10 and ch12.

## Reference Tables
| Config | Default | Meaning | When to change |
|---|---|---|---|
| `min.insync.replicas` (topic/broker) | 1 | Minimum ISR size for `acks=all` writes to succeed | Set to 2 with RF=3 for durability |
| `unclean.leader.election.enable` | false (since 0.11.0.0) | Allow a non-ISR replica to become leader | Only for topics where uptime beats consistency |
| `replica.lag.time.max.ms` | 30000 | A follower not caught up within this time leaves the ISR | Raise on slow or bursty links |
| `broker.session.timeout.ms` | 9000 | Broker counts as offline if the controller gets no heartbeat in this time | Rarely |
| `cleanup.policy` / `log.cleanup.policy` | delete | Choose `compact` (or `compact,delete`) per topic | Keyed changelog or state topics |
| `min.compaction.lag.ms` | 0 | Minimum age before a record can be compacted | Guarantee consumers a window of full history |
| `max.compaction.lag.ms` | Long.MAX | Maximum delay before a record becomes eligible for compaction | Low-throughput topics that never reach the dirty ratio |
| `min.cleanable.dirty.ratio` | 0.5 | Dirty fraction needed before a log is cleaned | Lower it to compact sooner (more I/O) |
| `delete.retention.ms` | 86400000 (24 h) | How long tombstones are kept | Raise above the slowest bootstrap time |

Quota precedence, from most to least specific: (1) user + client-id; (2) user + default client-id; (3) user; (4) default user + client-id; (5) default user + default client-id; (6) default user; (7) client-id; (8) default client-id.

| Quota type | Since | Unit | Scope |
|---|---|---|---|
| Network bandwidth (`producer_byte_rate`, `consumer_byte_rate`) | 0.9 | bytes/sec | per broker, per group |
| Request rate (`request_percentage`) | 0.11 | % of one thread over I/O + network threads | per broker, per group |

Defaults not stated in this source come from the broker/topic config reference (ch04).

## Worked Example
**Scenario: RF=2 topic, one broker dies.** The topic has `replication.factor=2` and `min.insync.replicas=1`, and the producer uses `acks=all`. Broker B (follower) misses heartbeats for longer than `broker.session.timeout.ms`. The controller drops B from the ISR, which is now {A}. `acks=all` writes keep succeeding because A *is* the whole ISR, and they become committed and visible on A alone. Then A's disk fails. No ISR member is left, so with `unclean.leader.election.enable=false` the partition stays offline until A comes back. If A's data is gone, the partition is down permanently. If unclean election is enabled, B comes back and becomes leader, and every write acked while B was out of the ISR is lost.

**The fix:** use RF=3 with `min.insync.replicas=2`. After one broker fails, the ISR is 2 and writes continue. After a second failure the ISR is 1, so producers get NotEnoughReplicas errors instead of false acks. Every acked write is on at least 2 replicas, which means it survives any single further failure.

## Key Takeaways
1. To survive `f` broker failures without losing acked data, use RF ≥ f+1, `acks=all`, and `min.insync.replicas` ≥ 2. The usual setup is RF=3 with minISR=2.
2. Leave `unclean.leader.election.enable=false` unless losing data is explicitly acceptable. With it off, losing every ISR replica means unavailability, not silent loss.
3. Watch ISR shrink and expand events. A follower is dropped for a lost controller session or for lagging more than `replica.lag.time.max.ms`.
4. Use compaction for keyed state and set `delete.retention.ms` longer than your slowest full-replay consumer takes, so it does not miss tombstones.
5. Set `max.compaction.lag.ms` on low-traffic compacted topics. Monitor `uncleanable-partitions-count`, `max-clean-time-secs`, and `max-compaction-delay-secs`.
6. Size the cleaner's dedupe buffer at 24 bytes per key. 8 GB covers about 366 GB of log head with 1 KB messages.
7. Quotas are per broker and dynamic. Set defaults plus per-(user, client-id) overrides, and expect throttled clients to see delays, with empty fetch responses during throttling.

## Connects To
- **ch04**: topic and broker config references for `min.insync.replicas`, `log.cleaner.*`, and dynamic quota configs.
- **ch05**: the producer `acks` and consumer fetch path that replication builds on.
- **ch06**: delivery semantics and idempotence, which depend on the commit guarantees defined here.
- **ch09**: segment and offset layout that the log cleaner rewrites.
- **ch10**: `kafka-configs.sh` quota commands, partition reassignment, and leadership balancing.
- **ch12**: multi-tenancy, which uses quotas to isolate tenants.
- **ch14**: ISR, under-replicated, cleaner, and throttle metrics.
- **ch15**: the KRaft controller, broker heartbeats, and session timeouts.
- **ch17**: Eligible Leader Replicas (ELR), which refines the ISR and unclean-election trade-off.
- **ch24**: Streams changelog topics, which rely on compaction.
