<img width="64" src="https://raw.githubusercontent.com/ydb-platform/ydb/main/ydb/docs/_assets/logo.svg" /><br/>

[![License](https://img.shields.io/badge/License-EPL%2D%2D2.0-blue.svg)](https://github.com/ydb-platform/jepsen.ydb/blob/main/LICENSE)

# jepsen.ydb

A command line utility for testing YDB with Jepsen.

## Usage

1. Install `gnuplot-nox` and `graphviz` packages at the control node.
2. Install JDK. Default one in ydb repository  is too old. JDK 21 is recomended, for example [adoptium](https://adoptium.net/temurin).
3. Set `JAVA_CMD` env:
```bash
export JAVA_CMD=$HOME/jdk-21.0.8+9/bin/java
```
4. Download [lein](https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein) to `/usr/local/bin/`
5. Create the file `~/ydb-nodes.txt` containing a list of YDB cluster nodes that will be excluded from the nemesis process (i.e., will not be targeted for termination).
6. To test column shard tables, you need to enable data query in YDB config by setting:
```yaml
table_service_config:
    allow_olap_data_query: true
```
7. To test topics through the Kafka API (`--workload-name kafka-topic`), enable the Kafka proxy
   in YDB config:
```yaml
kafka_proxy_config:
    enable_kafka_proxy: true
    listening_port: 9092
```
   `--kafka-txn` (the default) needs the `EnableKafkaTransactions` feature flag, which is a top-level
   `feature_flags` entry, not part of `kafka_proxy_config`, and defaults to `true` -- most clusters
   need no extra config for it. Only set it explicitly if your cluster has it turned off:
```yaml
feature_flags:
    enable_kafka_transactions: true
```
8. Runing jepsen tests

Please pay attention that some parameters are incompatible.

- The `--with-opindex` option is only compatible with `--model ydb-serializable`.
- The `kafka-topic` workload needs `--kafka-partition-count` >= `--key-count`, and ignores `--max-txn-length`
  (transactions are 4 operations long with `--kafka-txn` and 1 otherwise).
- The `kafka-topic` workload authenticates via `SASL_PLAINTEXT/PLAIN` by default (`--kafka-sasl`), even when
  the YDB cluster has anonymous auth enabled: YDB is multi-tenant but the Kafka protocol has no notion of
  database, so the target `--db-name` is conveyed as `user@database` in the SASL username (required by YDB
  only for the `PLAIN` mechanism). Without this, the proxy resolves topics against some other database and
  produces fail with `UNKNOWN_TOPIC_OR_PARTITION`. Use `--no-kafka-sasl` only if your cluster doesn't need
  this.
- The `kafka-topic` workload's end-of-test catch-up read (which re-reads every key from the beginning to
  check nothing was lost) is only bounded by `--kafka-final-time-limit` (default 300s) -- too low a value
  for the configured `--key-count`/`--max-writes-per-key`/`--concurrency` cuts it off before it's done
  reading, which shows up as spurious `:unseen` failures on an otherwise-correct run. Scale it up for
  larger workloads.
- The `topic-table` workload mixes table and topic operations inside single YDB transactions (native SDK,
  `TxMode.SERIALIZABLE_RW` only -- YQL alone can't do this), to catch atomicity violations between the two
  APIs. It requires `--model ydb-serializable`. Table keys work exactly like the `append` workload -- any
  number of reads/writes, freely mixed -- **as long as the transaction doesn't read a topic key at all**. A
  transaction that reads a topic key collapses down to just that one lone read: (1) the read is dropped if
  that same key was already appended to earlier in the same transaction (topics don't make a transaction's
  own writes visible to reads within that same transaction, so keeping such a read would look like an
  internal-consistency violation), and (2) if a topic read survives rule 1, *everything else* in the
  transaction is dropped -- other reads (table included) and all writes (table and topic). This is
  stricter than "just don't read the same key twice": `execute-topic-read!` is never attached to the
  transaction (a topic replay read can't be snapshot-pinned regardless), so it always runs strictly
  *before* the transaction's actual commit -- a different, earlier moment than when anything else in that
  transaction (including its own writes) takes effect. Mixing it with anything else risks a torn view no
  real atomic transaction could produce, which Elle would (correctly) flag as an anomaly for the wrong
  reason. A transaction with no topic read at all has none of this risk -- all its writes commit atomically
  together and its table reads are properly snapshot-consistent -- so it stays fully unrestricted. This is
  intentional, not a limitation of the checker setup: earlier, less restrictive versions of this workload
  hit exactly these false-positive anomalies, in an earlier topics-only proof of concept, in a real cluster
  run, and in code review. Topic reads/write-your-own-writes consistency is intentionally out of scope here
  and is
  covered separately by `kafka-topic`. Use
  `--table-key-count`/`--topic-key-count` to size the two key spaces (their sum becomes the workload's
  effective `--key-count`) and `--topic-partition-count`/`--topic-name` to configure the topic. Its
  end-of-test read sweep (every topic key touched during the run, read back once) is governed by the same
  `--kafka-final-time-limit` as `kafka-topic`, despite the flag's name -- it's shared final-generator
  plumbing, not Kafka-specific.


 Example command for running the test:
```bash
lein run test \
    --nodes-file ~/ydb-nodes.txt \
    --db-name /your/db/name \
    --no-ssh \
    --concurrency 10n \
    --key-count 15 \
    --max-writes-per-key 1000 \
    --max-txn-length 4 \
    --batch-ops-probability 0.85 \
    --batch-commit-probability 0.5 \
    --ballast-size 1024 \
    --store-type row
```
 Example command for running the Kafka API topic workload:
```bash
lein run test \
    --nodes-file ~/ydb-nodes.txt \
    --db-name /your/db/name \
    --no-ssh \
    --concurrency 10n \
    --workload-name kafka-topic \
    --kafka-port 9092 \
    --kafka-topic-name jepsen_kafka_topic \
    --kafka-partition-count 64 \
    --key-count 15 \
    --max-writes-per-key 1000 \
    --kafka-txn \
    --kafka-isolation-level read_committed
```
 Example command for running the mixed table+topic workload:
```bash
lein run test \
    --nodes-file ~/ydb-nodes.txt \
    --db-name /your/db/name \
    --no-ssh \
    --concurrency 10n \
    --workload-name topic-table \
    --model ydb-serializable \
    --topic-name jepsen_test_topic \
    --topic-partition-count 30 \
    --table-key-count 10 \
    --topic-key-count 10 \
    --max-writes-per-key 1000 \
    --store-type row
```
9. Run http server for observe results:
```bash
lein run serve -p 9000
```

## License

Copyright © 2024 YANDEX LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
