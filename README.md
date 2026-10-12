# apache-kafka-docs

Agent skill generated from the official *Apache Kafka 4.3 Documentation* (Apache Software Foundation) with [book-to-skill](https://github.com/virgiliojr94/book-to-skill). It covers Kafka design (replication/ISR, delivery semantics, transactions, log compaction, quotas), KRaft, operations, monitoring, tiered storage, geo-replication, the new group and transaction protocols (KIP-848, share groups, KIP-890, ELR), security (listeners, SSL, SASL, ACLs), Kafka Connect, Kafka Streams and 4.x upgrades. It is organized as rules, chapter summaries, a glossary, patterns and a cheatsheet.

## Install

```
npx skills add https://github.com/jnerytech/apache-kafka-docs --skill apache-kafka-docs
```

## Files

| File | Contents |
|---|---|
| `SKILL.md` | Core rules and mental models, chapter index, topic index |
| `chapters/ch01…ch30` | 30 chapter summaries with config/default tables, code/CLI examples and a worked example |
| `glossary.md` | Key terms with chapter references |
| `patterns.md` | Techniques (When / How / Trade-offs) |
| `cheatsheet.md` | Decision rules, thresholds, warning signs |

## Source and license

The content is synthesized summaries, not a copy of the documentation text. The source is the Apache Kafka documentation (kafka.apache.org, AK 4.3.x), © The Apache Software Foundation, licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Apache Kafka, Kafka and the Kafka logo are trademarks of The Apache Software Foundation. This project is not affiliated with or endorsed by the ASF.

The auto-generated reference tables (full config lists, metric catalogs, protocol schemas) are not included. See https://kafka.apache.org/documentation for the full reference.
