# Chapter 20: Security — Authorization and ACLs

## Core Idea
Kafka authorization is a pluggable `Authorizer` (in KRaft: `StandardAuthorizer`, ACLs stored in the metadata log) that evaluates rules of the form "Principal P is Allowed/Denied Operation O from Host H on Resources matching ResourcePattern RP". Once it's enabled, access is **default-deny**: a resource with no matching ACL is reachable only by `super.users`. Every API call maps to specific (Operation, Resource) pairs, so least-privilege ACLs come down to knowing that mapping.

## Frameworks Introduced
- **ACL tuple (KIP-11 + KIP-290 resource patterns)**: `Principal {P} is [Allow|Deny] Operation {O} From Host {H} on Resource {R} matching ResourcePattern {RP}`.
  - When to use: every authorization decision. Design ACLs by filling each slot on purpose.
  - How: (1) Principal = `PrincipalType:name` (e.g. `User:Bob`; `User` is case-sensitive). (2) Permission = Allow or Deny. (3) Operation = one of Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, CreateTokens, DescribeTokens, All. (4) Host = an IP address or `*` (hostnames are not supported). (5) Resource type = Topic, Group, Cluster, TransactionalId, DelegationToken or User. (6) Pattern type = `literal` (the exact name, or `*` for every name) or `prefixed` (every name that starts with the given string).
  - Why it works / failure mode: Deny ACLs carve exceptions out of broad Allow ACLs (allow `User:*`, then deny `User:BadBob` from one IP). If no ACL matches at all, the request is refused, and producers and consumers break silently after you enable the authorizer unless the ACLs already exist.
- **Default-deny with escape hatches**: with no matching ACL, only super users get access.
  - When to use: decide this before you turn on `authorizer.class.name`.
  - How: list operators and inter-broker/controller principals in `super.users=User:Bob;User:Alice` (semicolon-separated, because SSL DNs contain commas). Use `allow.everyone.if.no.acl.found=true` only as a migration crutch: it opens resources that have *zero* ACLs, and once a resource has any ACL, normal enforcement applies to it.
- **Principal forwarding in KRaft (Envelope)**: clients send admin requests such as `CreateTopics` and `DeleteTopics` to a broker. The broker forwards them to the active controller, through the first listener in `controller.listener.names`, wrapped in an `Envelope` request.
  - How: the controller first authorizes the `Envelope` against the broker's principal (which needs `ClusterAction` on Cluster), then authorizes the inner request against the forwarded client principal.
  - Failure mode: a custom `principal.builder.class` must also implement `org.apache.kafka.common.security.auth.KafkaPrincipalSerde`, or forwarding can't serialize the principal. `DefaultKafkaPrincipalBuilder` already does (format: `DefaultPrincipalData.json`).
- **Principal name mapping**: turns an authenticated identity into the short name that ACLs match.
  - SSL: `ssl.principal.mapping.rules`. Rules are ordered and the first match wins. Syntax `RULE:pattern/replacement/` with an optional `/L` or `/U` suffix to force case; end the list with `DEFAULT`, which keeps the full X.500 DN.
  - SASL/Kerberos: `sasl.kerberos.principal.to.local.rules`, using krb5 `auth_to_local` syntax `RULE:[n:string](regexp)s/pattern/replacement/[g][/L|/U]`. The default keeps the primary part of the Kerberos principal.
  - Advanced: replace the whole mapping with `principal.builder.class=<YourBuilder>`.

## Key Concepts
- **`authorizer.class.name`**: the server property that turns authorization on. Set it on **all** KRaft nodes (brokers and controllers) to `org.apache.kafka.metadata.authorizer.StandardAuthorizer`.
- **`super.users`**: principals that bypass all ACL checks. The list is semicolon-delimited.
- **`allow.everyone.if.no.acl.found`**: when `true`, resources with no ACLs are open to everyone instead of super users only.
- **Literal vs prefixed pattern**: a literal pattern matches the exact name (or every name, when the name is `*`). A prefixed pattern matches every name that begins with the string. `--resource-pattern-type` defaults to `literal`.
- **`match` / `any` filters**: used only with `--list`/`--remove`. `any` means any pattern type but the exact name. `match` returns every literal, wildcard and prefixed ACL that *affects* the resource.
- **ClusterAction**: the operation for inter-node traffic: follower FETCH, ENVELOPE, VOTE, BROKER_HEARTBEAT, share-group state RPCs, and so on. Grant it only to broker and controller principals.
- **IdempotentWrite (Cluster)**: needed for idempotent produce and INIT_PRODUCER_ID when there is no `transactional.id`. Write on a TransactionalId implies idempotence.
- **Authorization error codes**: TOPIC_AUTHORIZATION_FAILED (29), GROUP_AUTHORIZATION_FAILED (30), CLUSTER_AUTHORIZATION_FAILED (31), TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53).
- **Unauthorizable APIs**: API_VERSIONS, SASL_HANDSHAKE and SASL_AUTHENTICATE run before or during authentication. GET_TELEMETRY_SUBSCRIPTIONS and PUSH_TELEMETRY perform no check.

## Mental Models
- Think of the authorizer as a **firewall with an implicit final DENY**: you write only Allow rules plus a few Deny exceptions, and anything unmatched is dropped (except for super users).
- Think of **Cluster-level vs Topic-level fallbacks** as "cluster grant, else per-object check": CREATE_TOPICS, METADATA auto-create, LIST_GROUPS and OFFSET_FOR_LEADER_EPOCH try Cluster first and quietly fall back to Topic or Group, without returning CLUSTER_AUTHORIZATION_FAILED.
- Use **prefixed ACLs when topic and group names follow a team or app namespace** (`orders-`, `app1.`). One ACL then covers future topics, and the ACL count stays bounded.
- Use **`--list ... --resource-pattern-type match` when debugging** "why can or can't X access topic T". A plain `--list --topic T` shows only the literal ACLs on T and hides wildcard and prefixed grants.

## Anti-patterns
- **Enabling the authorizer without super users or pre-created ACLs**: under default-deny, brokers, controllers and every client lose access at once. Set `super.users` (including the inter-broker principal) and create client ACLs first.
- **Leaving `allow.everyone.if.no.acl.found=true` in production**: any new topic or group without ACLs is world-accessible.
- **Using hostnames in `--allow-host` / `--deny-host`**: only IPs are supported, so the rule never matches as intended.
- **`--remove --resource-pattern-type match`**: removes every ACL that affects the resource, including wildcard and prefixed ACLs shared with other resources. Use it with great care.
- **Comma-delimited `super.users`**: the delimiter is `;`. Commas are valid inside SSL DNs, so a comma-separated list breaks parsing.
- **Writing ACLs for the raw SSL DN when mapping rules are in place, or the reverse**: ACLs must match the principal *after* `ssl.principal.mapping.rules` or `sasl.kerberos.principal.to.local.rules` have been applied.
- **Custom principal builder without `KafkaPrincipalSerde`**: breaks KRaft request forwarding for admin APIs.

## Code Examples
```properties
# server.properties on every broker AND controller (KRaft)
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:broker
# allow.everyone.if.no.acl.found=false   (default; keep it)
ssl.principal.mapping.rules=RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/,\
  RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L,\
  DEFAULT
sasl.kerberos.principal.to.local.rules=RULE:[1:$1@$0](.*@MYDOMAIN.COM)s/@.*//,DEFAULT
```
- **What it demonstrates**: a default-deny setup with semicolon-separated super users. The rules map `CN=serviceuser,OU=ServiceUsers,...` to `serviceuser` and `CN=adminUser,OU=Admin,...` to `adminuser@admin` (forced to lowercase), and strip the Kerberos realm so `user@MYDOMAIN.COM` becomes `user`.

```bash
# Producer role on a namespace (Write+Describe+Create on matching topics)
bin/kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal User:Jane --producer --topic Test- --resource-pattern-type prefixed
# Consumer role (Read+Describe on topic, Read on group) — the group is required
bin/kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal User:Alice --consumer --topic Test-topic --group Group-1
# Allow everyone to read, except one principal from one IP (Deny wins)
bin/kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal User:'*' --allow-host '*' \
  --deny-principal User:BadBob --deny-host 198.51.100.3 --operation Read --topic Test-topic
```
- **What it demonstrates**: the `--producer` / `--consumer` convenience roles, prefixed patterns, and an allow-all with a Deny exception. Add `--idempotent` together with `--producer` to also grant IdempotentWrite on Cluster.

```bash
bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic Test-topic              # literal only
bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic '*'                     # wildcard ACLs
bin/kafka-acls.sh --bootstrap-server localhost:9092 --list --topic Test-topic --resource-pattern-type match  # everything that applies
```
- **What it demonstrates**: the three levels of ACL visibility. Only `match` gives the effective ACLs for the resource.

## Reference Tables

**Server configs**
| Config | Default | Meaning | When to change |
|---|---|---|---|
| `authorizer.class.name` | empty (no authorization) | Authorizer implementation | Set to `StandardAuthorizer` on all KRaft nodes to enforce ACLs |
| `super.users` | none | `;`-separated principals that bypass ACLs | Always include admin and inter-node principals |
| `allow.everyone.if.no.acl.found` | false | Open resources that have no ACLs | Only temporarily, while rolling ACLs out |
| `ssl.principal.mapping.rules` | `DEFAULT` (full DN) | DN-to-short-name mapping, first match wins | When ACLs should use CN-style names |
| `sasl.kerberos.principal.to.local.rules` | `DEFAULT` (primary) | Kerberos-principal-to-short-name mapping | Multi-realm setups, or to strip `@REALM` |
| `principal.builder.class` | `DefaultKafkaPrincipalBuilder` | Fully custom principal | Advanced cases; must implement `KafkaPrincipalSerde` |

**`kafka-acls.sh` key options**
| Option | Default | Notes |
|---|---|---|
| `--add` / `--remove` / `--list` | — | The action |
| `--bootstrap-server` \| `--bootstrap-controller` | — | Exactly one. `--command-config` (admin client properties) works only with `--bootstrap-server` |
| `--topic` `--group` `--cluster` `--transactional-id` `--delegation-token` `--user-principal` | — | Resource; `*` = all of that type |
| `--resource-pattern-type` | `literal` | `literal`/`prefixed` for add; `any`/`match` filters for list/remove |
| `--allow-host` / `--deny-host` | `*` when a principal is given | IPs only |
| `--operation` | `All` | Repeatable |
| `--producer` / `--consumer` / `--idempotent` / `--force` | — | Role shortcuts; `--force` skips prompts |

**Minimum ACLs per client role (from the protocol table)**
| Role | Required (Operation → Resource) |
|---|---|
| Plain producer | Write → Topic, Describe → Topic (metadata); IdempotentWrite → Cluster if idempotent |
| Transactional producer | Write + Describe → TransactionalId; Write → Topic; Read → Group for sendOffsetsToTransaction (ADD_OFFSETS_TO_TXN, TXN_OFFSET_COMMIT) |
| Classic consumer | Read → Topic (FETCH, OFFSET_COMMIT); Read → Group (JOIN/SYNC/HEARTBEAT/LEAVE, OFFSET_COMMIT); Describe → Group+Topic (OFFSET_FETCH, FIND_COORDINATOR) |
| KIP-848 consumer | Read → Group (CONSUMER_GROUP_HEARTBEAT, CONSUMER_GROUP_DESCRIBE) plus the topic grants above |
| Share consumer | Read → Group + Read → Topic (SHARE_FETCH, SHARE_ACKNOWLEDGE, SHARE_GROUP_HEARTBEAT) |
| Streams (streams group) | Read → Group; Describe → all source and internal topics; Create → internal topics, unless they already exist |
| Topic admin | Create → Cluster *or* Topic; Delete → Topic; Alter → Topic (CREATE_PARTITIONS); AlterConfigs/DescribeConfigs → Topic |
| ACL admin | Describe → Cluster (DESCRIBE_ACLS); Alter → Cluster (CREATE_ACLS/DELETE_ACLS) |
| Ops admin | Alter → Cluster (reassignments, UPDATE_FEATURES, SCRAM creds, log dirs); AlterConfigs → Cluster (broker configs, client quotas) |
| Broker/controller | ClusterAction → Cluster (replication FETCH, ENVELOPE, VOTE, BROKER_REGISTRATION/HEARTBEAT, share-state RPCs) |

**Evaluation order quirks**: OFFSET_COMMIT, OFFSET_FETCH and the share-group offset APIs check Group before Topic. ADD_PARTITIONS_TO_TXN checks TransactionalId, then Topic. LIST_GROUPS and DESCRIBE_LOG_DIRS return an *empty* response instead of an error when unauthorized.

## Worked Example
**Locking down a KRaft cluster for one app (`orders`) with mTLS.**
1. Map certificates to short names: `ssl.principal.mapping.rules=RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/,DEFAULT`. A client certificate `CN=orders-svc,OU=ServiceUsers,...` then becomes `User:orders-svc`.
2. On every broker and controller, set `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` and `super.users=User:broker;User:admin`. The broker principal needs ClusterAction for replication and Envelope forwarding.
3. Grant the producer on the namespace, with idempotence: `kafka-acls.sh --bootstrap-server b:9093 --command-config admin.properties --add --allow-principal User:orders-svc --producer --idempotent --topic orders- --resource-pattern-type prefixed`.
4. Grant the consumer: `--add --allow-principal User:orders-reader --consumer --topic orders- --group orders-reader- --resource-pattern-type prefixed`.
5. Verify the effective rules: `--list --topic orders-events --resource-pattern-type match` shows the prefixed ACLs, which a plain literal `--list` would not.
6. Troubleshoot: TOPIC_AUTHORIZATION_FAILED (29) means a missing Topic grant. GROUP_AUTHORIZATION_FAILED (30) means a missing Group Read. CLUSTER_AUTHORIZATION_FAILED (31) on produce usually means a missing IdempotentWrite.
7. Revoke: run the same command with `--remove` instead of `--add`.

## Key Takeaways
1. Set `StandardAuthorizer` on **every** KRaft node, and define `super.users` (`;`-separated) before you enable it, because the default is deny.
2. Use `--producer`, `--consumer` (always with `--group`) and `--idempotent` for the common cases. Add TransactionalId Write/Describe for transactional producers.
3. Prefer `prefixed` patterns over many literal ACLs when names are namespaced, and use `--resource-pattern-type match` to see what actually applies.
4. Write ACLs against the *mapped* principal name produced by the SSL or Kerberos mapping rules, and check the mapping first when authorization unexpectedly fails.
5. Use Deny ACLs only as narrow exceptions (principal plus IP); hosts must be IPs.
6. Read the error code to find the missing grant (29 Topic, 30 Group, 31 Cluster, 53 TransactionalId), and remember that some APIs return empty results instead of errors.

## Connects To
- **ch18**: listeners and SSL produce the authenticated principal (DN) that ACLs match.
- **ch19**: SASL mechanisms, Kerberos principals and delegation tokens (the DelegationToken and User resources).
- **ch15**: KRaft controllers, `controller.listener.names` and Envelope forwarding.
- **ch06**: transactions and idempotence explain the TransactionalId and IdempotentWrite requirements; share consumers need the SHARE_* grants.
- **ch17**: KIP-848 consumer-group heartbeats are authorized as Group Read.
- **ch12**: in multi-tenancy, prefixed ACLs per tenant namespace.
- **ch29**: Streams security, which covers internal-topic Create/Describe ACLs.
- **ch08**: wire protocol API keys and the error codes.
