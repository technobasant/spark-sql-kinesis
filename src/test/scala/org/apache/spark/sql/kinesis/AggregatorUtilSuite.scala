/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kinesis

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.kinesis.producer.protobuf.Messages

import org.apache.spark.SparkFunSuite

/**
 * Covers the deaggregation that replaced KCL 1.x's UserRecord.deaggregate when this
 * connector moved to AWS SDK v2. Nothing else exercises it without a live Kinesis stream.
 */
class AggregatorUtilSuite extends SparkFunSuite {

  private val parentSeqNum = "49590338271490256608559692538361571095921575989136588898"
  private val arrival = Instant.ofEpochMilli(1700000000000L)

  private def md5(data: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("MD5").digest(data)

  private def plainRecord(data: String, partitionKey: String): Record = {
    Record.builder()
      .data(SdkBytes.fromByteArray(data.getBytes(StandardCharsets.UTF_8)))
      .partitionKey(partitionKey)
      .sequenceNumber(parentSeqNum)
      .approximateArrivalTimestamp(arrival)
      .build()
  }

  /** Builds a record in the KPL aggregation wire format: magic + protobuf + md5 tail. */
  private def aggregatedRecord(payloads: Seq[(String, String)]): Record = {
    val partitionKeys = payloads.map(_._2).distinct
    val builder = Messages.AggregatedRecord.newBuilder()
    partitionKeys.foreach(builder.addPartitionKeyTable)
    payloads.foreach { case (data, partitionKey) =>
      builder.addRecords(
        Messages.Record.newBuilder()
          .setPartitionKeyIndex(partitionKeys.indexOf(partitionKey).toLong)
          .setData(ByteString.copyFrom(data.getBytes(StandardCharsets.UTF_8)))
          .build())
    }
    val protoBytes = builder.build().toByteArray
    val payload =
      AggregatorUtil.AGGREGATED_RECORD_MAGIC ++ protoBytes ++ md5(protoBytes)

    Record.builder()
      .data(SdkBytes.fromByteArray(payload))
      .partitionKey(partitionKeys.head)
      .sequenceNumber(parentSeqNum)
      .approximateArrivalTimestamp(arrival)
      .build()
  }

  test("passes non-aggregated records through untouched") {
    val records = Seq(plainRecord("hello", "pk-1"), plainRecord("world", "pk-2"))
    val result = new AggregatorUtil().deaggregate(records.asJava).asScala

    assert(result.size === 2)
    assert(result.map(r => new String(r.data.asByteArray, StandardCharsets.UTF_8))
      === Seq("hello", "world"))
    assert(result.map(_.partitionKey) === Seq("pk-1", "pk-2"))
  }

  test("splits an aggregated record into its user records") {
    val payloads = Seq(("first", "pk-a"), ("second", "pk-b"), ("third", "pk-a"))
    val result = new AggregatorUtil().deaggregate(Seq(aggregatedRecord(payloads)).asJava).asScala

    assert(result.size === 3)
    assert(result.map(r => new String(r.data.asByteArray, StandardCharsets.UTF_8))
      === Seq("first", "second", "third"))
    assert(result.map(_.partitionKey) === Seq("pk-a", "pk-b", "pk-a"))
  }

  test("sub-records inherit the parent sequence number and arrival timestamp") {
    // KCL 1.x's UserRecord reported the parent sequence number too, and this connector
    // checkpoints on it, so changing this would silently invalidate existing checkpoints.
    val payloads = Seq(("a", "pk-a"), ("b", "pk-a"))
    val result = new AggregatorUtil().deaggregate(Seq(aggregatedRecord(payloads)).asJava).asScala

    assert(result.size === 2)
    assert(result.forall(_.sequenceNumber === parentSeqNum))
    assert(result.forall(_.approximateArrivalTimestamp === arrival))
  }

  test("treats a corrupt aggregate as a plain record rather than dropping it") {
    val good = aggregatedRecord(Seq(("payload", "pk-a")))
    // Corrupt the md5 tail so the checksum no longer matches the protobuf body.
    val bytes = good.data.asByteArray
    bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 0xFF).toByte
    val corrupted = good.toBuilder.data(SdkBytes.fromByteArray(bytes)).build()

    val result = new AggregatorUtil().deaggregate(Seq(corrupted).asJava).asScala

    assert(result.size === 1)
    assert(result.head.data.asByteArray === bytes)
  }

  test("mixes aggregated and plain records in one batch") {
    val records = Seq(
      plainRecord("plain", "pk-x"),
      aggregatedRecord(Seq(("agg1", "pk-y"), ("agg2", "pk-y"))))
    val result = new AggregatorUtil().deaggregate(records.asJava).asScala

    assert(result.size === 3)
    assert(result.map(r => new String(r.data.asByteArray, StandardCharsets.UTF_8))
      === Seq("plain", "agg1", "agg2"))
  }

  test("drops sub-records whose hash key falls outside the shard range") {
    // An explicit range that excludes everything: every effective hash key is >= 0, so an
    // ending key of 0 leaves only keys that hash to exactly 0, i.e. none of these.
    val payloads = Seq(("a", "pk-a"), ("b", "pk-b"))
    val result = new AggregatorUtil()
      .deaggregate(Seq(aggregatedRecord(payloads)).asJava, "0", "0").asScala

    assert(result.isEmpty)
  }
}
