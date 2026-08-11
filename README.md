# Kinesis Connector for Spark Structured Streaming

A Kinesis source and sink for Spark Structured Streaming. [SPARK-18165](https://issues.apache.org/jira/browse/SPARK-18165)
describes the need for such an implementation; the original design is written up in this
[Qubole blog post](https://www.qubole.com/blog/kinesis-connector-for-structured-streaming/).

**Maintenance.** Qubole wrote this connector and Ron Cemer ported it to Spark 3.2, after which
[upstream](https://github.com/roncemer/spark-sql-kinesis) was declared unmaintained and its use
"strongly discouraged". It is maintained here instead, because the connector is in production use
and the alternatives change the read model (see [Design notes](#design-notes)). Releases are
published to Maven Central under `io.github.technobasant`.

## Compatibility

| Connector version | Spark | Scala | Java | AWS SDK |
| --- | --- | --- | --- | --- |
| `1.3.0_spark-4.2.0` | 4.2.0 | 2.13 | 17 | v2 |
| `1.2.6_spark-3.4.1` | 3.4.1 | 2.13 | 11 | v1 *(end of life)* |
| `1.2.x_spark-3.2` | 3.2.x | 2.12 / 2.13 | 8 / 11 | v1 *(end of life)* |

The connector links against Spark's streaming internals, which are relocated between minor
versions, so **a given jar only works on the Spark minor it was built for**. That is why the
target Spark version is part of the artifact version rather than buried in release notes.

AWS SDK v1 reached end of life on 2025-12-31 and KCL 1.x on 2026-01-30, so versions below
`1.3.0` receive no further updates.

## Using the connector

Available from Maven Central — no custom resolver needed.

**sbt**

```scala
libraryDependencies += "io.github.technobasant" % "spark-sql-kinesis_2.13" % "1.3.0_spark-4.2.0"
```

**Maven**

```xml
<dependency>
  <groupId>io.github.technobasant</groupId>
  <artifactId>spark-sql-kinesis_2.13</artifactId>
  <version>1.3.0_spark-4.2.0</version>
</dependency>
```

**spark-submit / spark-shell**

```sh
--packages io.github.technobasant:spark-sql-kinesis_2.13:1.3.0_spark-4.2.0
```

The published jar is shaded: AWS SDK v2, Netty, protobuf and Apache HttpClient are bundled and
relocated under `org.apache.spark.sql.kinesis.shaded`, so the connector cannot collide with
whatever versions Spark or `hadoop-aws` put on the classpath. Spark itself is `provided`.

## Quick start

### Set up a stream

See the [AWS CLI reference](https://docs.aws.amazon.com/cli/latest/reference/kinesis/create-stream.html)
for more options.

```sh
aws kinesis create-stream --stream-name test --shard-count 2

aws kinesis put-record --stream-name test --partition-key 1 --data 'Kinesis'
aws kinesis put-record --stream-name test --partition-key 1 --data 'Connector'
aws kinesis put-record --stream-name test --partition-key 1 --data 'for'
aws kinesis put-record --stream-name test --partition-key 1 --data 'Apache'
aws kinesis put-record --stream-name test --partition-key 1 --data 'Spark'
```

### Read from Kinesis

```sh
$SPARK_HOME/bin/spark-shell --packages io.github.technobasant:spark-sql-kinesis_2.13:1.3.0_spark-4.2.0
```

```scala
val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", "test")
  .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
  .option("startingposition", "TRIM_HORIZON")
  .load
```

Credentials resolve in this order: explicit `awsAccessKeyId`/`awsSecretKey`, then
`awsSTSRoleARN`, then the instance profile / IRSA / default provider chain. On EKS with IRSA,
pass no keys and leave `awsUseInstanceProfile` at its default.

### Schema

```
root
 |-- data: binary (nullable = true)
 |-- streamName: string (nullable = true)
 |-- partitionKey: string (nullable = true)
 |-- sequenceNumber: string (nullable = true)
 |-- approximateArrivalTimestamp: timestamp (nullable = true)
```

### Word count

```scala
kinesis
  .selectExpr("CAST(data AS STRING)").as[String]
  .groupBy("data").count()
  .writeStream
  .format("console")
  .outputMode("complete")
  .start()
  .awaitTermination()
```

```
+------------+-----+
|        data|count|
+------------+-----+
|         for|    1|
|      Apache|    1|
|       Spark|    1|
|     Kinesis|    1|
|   Connector|    1|
+------------+-----+
```

### Write to Kinesis

```scala
kinesis
  .selectExpr("CAST(rand() AS STRING) as partitionKey", "CAST(data AS STRING)").as[(String, String)]
  .writeStream
  .format("kinesis")
  .outputMode("update")
  .option("streamName", "spark-sink-example")
  .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
  .start()
  .awaitTermination()
```

## Kinesis source configuration

| Option | Default | Description |
| --- | :---: | --- |
| `streamName` | – | Name of the Kinesis stream to read from |
| `endpointUrl` | `https://kinesis.us-east-1.amazonaws.com` | Endpoint URL for the stream. The region is derived from this hostname |
| `awsAccessKeyId` | – | AWS credentials for describe and read operations |
| `awsSecretKey` | – | AWS credentials for describe and read operations |
| `awsSTSRoleARN` | – | STS role ARN to assume for describe and read operations |
| `awsSTSSessionName` | – | STS session name |
| `awsUseInstanceProfile` | `true` | Use the default provider chain (instance profile, IRSA, environment) when no credentials are given |
| `startingPosition` | `LATEST` | Where to start: `latest`, `trim_horizon`, `earliest` (alias for `trim_horizon`), or a JSON map of `shardId -> KinesisPosition` |
| `failondataloss` | `true` | Fail the query if an active shard is missing or expired |
| `kinesis.executor.maxFetchTimeInMs` | `1000` | Maximum time an executor spends fetching from one shard per batch |
| `kinesis.executor.maxFetchRecordsPerShard` | `100000` | Maximum records to fetch per shard per batch |
| `kinesis.executor.maxRecordPerRead` | `10000` | Maximum records per `getRecords` call |
| `kinesis.executor.addIdleTimeBetweenReads` | `false` | Add a delay between consecutive `getRecords` calls |
| `kinesis.executor.idleTimeBetweenReadsInMs` | `1000` | Minimum delay between consecutive `getRecords` calls |
| `kinesis.client.describeShardInterval` | `1s` | Minimum interval between `ListShards` calls when checking for resharding |
| `kinesis.client.numRetries` | `3` | Maximum retries for Kinesis API requests |
| `kinesis.client.retryIntervalMs` | `1000` | Cool-off before retrying a Kinesis API call |
| `kinesis.client.maxRetryIntervalMs` | `10000` | Maximum cool-off between retries |
| `kinesis.client.avoidEmptyBatches` | `true` | Check for unread data before starting a batch, avoiding empty micro-batches |

Option names are case-insensitive.

## Kinesis sink configuration

| Option | Default | Description |
| --- | :---: | --- |
| `streamName` | – | Name of the Kinesis stream to write to |
| `endpointUrl` | `https://kinesis.us-east-1.amazonaws.com` | Endpoint URL for the stream |
| `awsAccessKeyId` | – | AWS credentials for write operations |
| `awsSecretKey` | – | AWS credentials for write operations |
| `awsSTSRoleARN` | – | STS role ARN to assume |
| `awsSTSSessionName` | – | STS session name |
| `awsUseInstanceProfile` | `true` | Use the default provider chain when no credentials are given |
| `kinesis.executor.recordMaxBufferedTime` | `1000` (ms) | Maximum time a record stays buffered in the KPL |
| `kinesis.executor.maxConnections` | `1` | Maximum KPL connections to Kinesis |
| `kinesis.executor.aggregationEnabled` | `true` | Aggregate records before sending (KPL aggregation) |
| `kinesis.executor.flushwaittimemillis` | `100` | Wait time when flushing records on task end |

## Design notes

Worth knowing before comparing against the
[awslabs connector](https://github.com/awslabs/spark-sql-kinesis-connector), which registers
`aws-kinesis` and can be installed alongside this one.

**Synchronous fetch, no idle floor.** This is a DataSource V1 source that calls `getRecords`
inline in the task and stops as soon as `millisBehindLatest` reaches 0. An idle shard therefore
costs roughly one API round trip per batch, rather than the per-shard queue-drain floor a
background-prefetch design imposes. On many shards with modest traffic that difference dominates
batch latency, which is why the read model was kept rather than modernised.

**Deaggregation without KCL.** `AggregatorUtil` decodes the KPL aggregation wire format using the
protobuf classes the KPL already ships, so KCL — and its DynamoDB, CloudWatch and Netty
transitives — is not a dependency. Sub-records inherit the **parent** record's sequence number,
matching KCL 1.x's `UserRecord`, because this connector checkpoints on `Record.sequenceNumber()`.

**Checkpointing.** Shard progress lives in the query's own checkpoint directory under
`sources/<n>/shard-commit/`, alongside Spark's `offsets/` and `commits/`. There is no external
store to provision, and no DynamoDB lease table. Offsets advance only to records actually emitted
downstream, so the guarantee is at-least-once: a replay can duplicate, never skip.

**Upgrades are in place.** No option name, checkpoint format or on-disk layout has changed across
versions, so a running query can be moved to a new connector build — including across a Spark
major version — against its existing checkpoint.

## Building from source

Requires **Java 17** and Maven 3.9+.

```sh
git clone git@github.com:technobasant/spark-sql-kinesis.git
cd spark-sql-kinesis
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS
mvn clean package
```

This produces `target/spark-sql-kinesis_2.13-1.3.0_spark-4.2.0.jar`, containing the connector and
its shaded dependencies.

The unit suites run without AWS access. The integration suites create real Kinesis streams and are
skipped unless explicitly enabled:

```sh
export ENABLE_KINESIS_SQL_TESTS=1
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
mvn test
```

## Releasing

See [PUBLISHING.md](PUBLISHING.md) for Maven Central publishing — namespace verification, GPG
setup, and the release commands.

## Roadmap

* **DataSource V2** — deliberately not adopted yet. A DSv2 rewrite is only worth doing if it
  preserves the synchronous-fetch behaviour described above; the obvious ports introduce a
  per-shard idle floor and regress latency on idle-heavy streams.
* **Shard handling at the edges** — new and child shards created mid-query start from the
  configured `startingPosition` rather than the parent's committed position, and with
  `failondataloss=false` a deleted shard is dropped with a warning. Both are safe in steady state
  but are the known gaps around resharding.
* Optional external shard-commit state, for deployments that would rather not keep it on the
  checkpoint filesystem.

## Acknowledgement

This connector would not exist without the reference implementations it was modelled on: the
[Kafka connector](https://github.com/apache/spark/tree/branch-2.2/external/kafka-0-10-sql) for
Structured Streaming, the [Kinesis connector](https://github.com/apache/spark/tree/branch-2.2/external/kinesis-asl)
for legacy Streaming, and the [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client),
whose `UserRecord` deaggregation semantics `AggregatorUtil` reproduces. Parts of the structure are
influenced by the work of many Apache Spark contributors.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
