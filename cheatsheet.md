# Cheatsheet — Apache Kafka 4.3 Documentation

## Which API / group type
- Kafka ↔ external system → Connect with a pre-built connector (ch03)
- Topic → topic transforms/joins/aggregations → Streams (ch03)
- Need per-key order → KafkaConsumer group (ch03)
  - 4.x → `group.protocol=consumer` (classic stays default until 5.0) (ch17)
  - Assignor embeds custom metadata → stay classic or migrate offline (ch17)
- More workers than partitions, order irrelevant → share group (ch03, ch06)
- Streams on ≥ 4.2.1 without static membership/regex/warmup/rack-aware needs → `group.protocol=streams`, else classic (ch29, ch30)

## Delivery semantics
| Goal | Do |
|---|---|
| At-most-once | retries off, commit before processing (ch06) |
| At-least-once (default) | process → commit; make writes idempotent upserts (ch06) |
| Exactly-once Kafka→Kafka | Streams `exactly_once_v2`, else txn producer per consumer + `read_committed` (ch06, ch24) |

## Durability vs availability
| Setting | Effect |
|---|---|
| RF=3 + `min.insync.replicas=2` + `acks=all` | acked writes survive 1 broker loss; ISR < 2 → NotEnoughReplicas (ch07) |
| `acks=all` without minISR ≥ 2 | ISR may shrink to leader alone → weak (ch07) |
| `unclean.leader.election.enable=false` (default) | no committed loss; partition offline if all ISR die (ch07) |
| …`=true` | stays up; stale replica becomes truth (ch07) |
| ELR on | `min.insync.replicas` cluster/topic scope only (ch17) |

## SASL mechanism
- Kerberos/AD → GSSAPI; Kafka-managed users → SCRAM over PLAIN; OAuth IdP → OAUTHBEARER + JWKS validator; never unsecured JWT (ch19)
- Many distributed workers → delegation tokens (ch19)
- PLAIN/SCRAM/OAUTHBEARER → always SASL_SSL; `ssl.client.auth=required`, never `requested` (ch18, ch19)

## Streams join choice
- Same key, co-partitioned → KStream-KStream (windowed) or KStream-KTable (ch25)
- Key from value, skewed, or chained lookups → KStream-GlobalKTable (ch25)
- KTable-KTable on a foreign key → FK join (inner/left) (ch25)
- Out-of-order input in stream-table/table-table → versioned table (ch24, ch25)
- Only values change → `mapValues`/`groupByKey`; key changes force repartition (ch25)

## Defaults & thresholds
| Item | Value |
|---|---|
| `linger.ms` / `batch.size` | 5 (was 0 before 4.0) / 16384 (ch05, ch30) |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 / 500 (ch05) |
| `replica.lag.time.max.ms` | 30000 (ch07) |
| `delete.retention.ms` / dirty ratio | 24 h / 0.5 (ch07) |
| `share.record.lock.duration.ms` | 30000 (ch06) |
| Controllers | 2N+1; ~5 GB RAM + 5 GB disk each (ch15) |
| Broker heap / pagecache | ~6 GB G1 / ≈ write throughput × 30 (ch13) |
| `vm.max_map_count` / FDs | default ~65535, 2 per segment / ≥ 100000 (ch13) |
| Handler/network idle | < 0.3 = saturated (ch14) |
| Streams `commit.interval.ms` | 30000; 100 under EOS (ch27) |
| `statestore.cache.max.bytes` | 10 MB (ch27) |
| `num.standby.replicas` / `max.warmup.replicas` | 0 / 2 (ch27) |
| Connect `scheduled.rebalance.max.delay.ms` | 300000 (ch21) |
| SCRAM iterations | 4096 (min) (ch19) |
| `message.timestamp.after.max.ms` | 1 h since 4.0 (ch30) |

## Decision rules
- Never add partitions to keyed topics casually: keys remap; counts can't shrink (ch10)
- Production KRaft → separate `broker`/`controller` roles (ch15)
- Finalize `--release-version` only after soak: 4.0/4.3 MVs can't downgrade (ch30)
- < 3 brokers → lower `__share_group_state` and tiered RLMM RF/minISR to 1 (ch16, ch30)
- Shared cluster → request-rate quotas before byte quotas (ch12)
- Streams `state.dir` → persistent disk, never default tmp (ch27)
- `topology.optimization` → also pass props to `StreamsBuilder.build(props)` (ch27)

## Tells & smells
- Config ignored → per-broker dynamic override wins (ch04)
- `AtMinIsrPartitionCount` > 0 → one more failure blocks `acks=all` (ch14)
- High RequestQueueTimeMs → I/O threads; LocalTimeMs → disk; RemoteTimeMs → followers (ch14)
- ISR flapping on slow links → raise `replica.lag.time.max.ms` (ch07)
- Reassignment never finishes → throttle < max BytesInPerSec (ch10)
- `OutOfMemoryError (Map failed)` → `vm.max_map_count` (ch13)
- Consumer sees aborted records → still `read_uncommitted` (ch06)
- Share-group duplicates → lock expired before processing ended (ch06)
- Socket closed on produce → request over broker max size (ch08)
- Slow SASL handshake → short hostnames; use FQDNs (ch19)
- Compacted topic shows duplicate keys → normal; active segment never cleaned (ch07)
- Data outlives `retention.ms` → active segment; lower `segment.ms` (ch09)
- Streams full re-restore on 4.3 → unclean close (KIP-1035) (ch27)
- Punctuator silent → STREAM_TIME with idle input; use WALL_CLOCK_TIME (ch26)
- Streams group NOT_READY → missing topics or co-partitioning failure (ch29)
- Connect tasks fail auth → security configs not repeated under `producer.`/`consumer.` (ch21)
