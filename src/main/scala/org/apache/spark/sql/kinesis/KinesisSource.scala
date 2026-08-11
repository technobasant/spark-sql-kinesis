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

import java.io._
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import software.amazon.awssdk.services.kinesis.model.Record
import org.apache.hadoop.conf.Configuration
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source, _}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}

 /*
  * A [[Source]] that reads data from Kinesis using the following design.
  *
  *  - The [[KinesisSourceOffset]] is the custom [[Offset]] defined for this source
  *
  *  - The [[KinesisSource]] written to do the following.
  *
  *   - `getOffset()` uses the [[KinesisSourceOffset]] to query the latest
  *      available offsets, which are returned as a [[KinesisSourceOffset]].
  *
  *   - `getBatch()` returns a DF
  *   - The DF returned is based on [[KinesisSourceRDD]]
  */

private[kinesis] class KinesisSource(
    sqlContext: SQLContext,
    sourceOptions: Map[String, String],
    metadataPath: String,
    streamName: String,
    initialPosition: InitialKinesisPosition,
    endPointURL: String,
    kinesisCredsProvider: SparkAWSCredentials,
    failOnDataLoss: Boolean = true
    )
  extends Source with Serializable with Logging {

  import KinesisSource._

  private def sc: SparkContext = {
    sqlContext.sparkContext
  }

  /*
   * Spark 4 split SparkSession/SQLContext into an api-level type and a `classic`
   * implementation, and internalCreateDataFrame now lives only on the latter.
   * StreamSourceProvider still hands us the api-level SQLContext, but a DSv1 streaming
   * source only ever runs on Spark Classic, so narrowing here is safe.
   */
  private def classicSqlContext: org.apache.spark.sql.classic.SQLContext =
    sqlContext.asInstanceOf[org.apache.spark.sql.classic.SQLContext]

  private def kinesisReader: KinesisReader = {
    new KinesisReader(sourceOptions, streamName, kinesisCredsProvider, endPointURL)
  }

  private var currentShardOffsets: Option[ShardOffsets] = None

  private val minBatchesToRetain = sqlContext.sparkSession.sessionState.conf.minBatchesToRetain
  require(minBatchesToRetain > 0, "minBatchesToRetain has to be positive")

  private val describeShardInterval: Long = {
    Utils.timeStringAsMs(sourceOptions.getOrElse(KinesisSourceProvider.DESCRIBE_SHARD_INTERVAL,
      "1s"))
  }

  require(describeShardInterval >= 0, "describeShardInterval cannot be less than 0 sec")

  private var latestDescribeShardTimestamp: Long = -1L

  private def metadataCommitter: MetadataCommitter[ShardInfo] = {
    metaDataCommitterType.toLowerCase(Locale.ROOT) match {
      case "hdfs" =>
        new HDFSMetadataCommitter[ ShardInfo ](metaDataCommitterPath,
          hadoopConf(sqlContext), sourceOptions)
      case _ => throw new IllegalArgumentException("only HDFS is supported")
    }
  }

  private def metaDataCommitterType: String = {
    sourceOptions.getOrElse("executor.metadata.committer", "hdfs").toString
  }

  private def metaDataCommitterPath: String = {
    sourceOptions.getOrElse("executor.metadata.path", metadataPath).toString
  }

  private val avoidEmptyBatches =
    sourceOptions.getOrElse("client.avoidEmptyBatches".
      toLowerCase(Locale.ROOT), "true").toBoolean

  private val maxParallelThreads =
    sourceOptions.getOrElse("client.maxParallelThreads".
      toLowerCase(Locale.ROOT), "8").toInt

  def options: Map[String, String] = {
    // This function is used for testing
    sourceOptions
  }

  def getFailOnDataLoss(): Boolean = {
    // This function is used for testing
    failOnDataLoss
  }

  /*
   * `kinesisReader` is a def, so every reference builds a new KinesisReader — and each one
   * starts a thread in its constructor and an AWS client on first call. Referencing it twice
   * here therefore created two of each per shard per batch, and neither was ever closed
   * (stop() closes a third, freshly built one). Bind it once and close it.
   */
  private def withReader[T](body: KinesisReader => T): T = {
    val reader = kinesisReader
    try {
      body(reader)
    } finally {
      reader.close()
    }
  }

  /** Makes an API call to get one record for a shard. Return true if the call is successful  */
  def hasNewData(shardInfo: ShardInfo): Boolean = withReader { reader =>
    val shardIterator = reader.getShardIterator(
      shardInfo.shardId,
      shardInfo.iteratorType,
      shardInfo.iteratorPosition)
    val records = reader.getKinesisRecords(shardIterator, 1)
    // Return true if we can get back a record. Or if we have not reached the end of the stream
    (records.records.size() > 0 || records.millisBehindLatest.longValue() > 0)
  }

  def canCreateNewBatch(shardsInfo: Array[ShardInfo]): Boolean = {
    var shardsInfoToCheck = shardsInfo.par
    val threadPoolSize = Math.min(maxParallelThreads, shardsInfoToCheck.size)
    val evalPool = ThreadUtils.newForkJoinPool("checkCreateNewBatch", threadPoolSize)
    shardsInfoToCheck.tasksupport = new ForkJoinTaskSupport(evalPool)
    val hasRecords = new AtomicBoolean(false)
    try {
      shardsInfoToCheck.foreach { s =>
        if (!hasRecords.get() && hasNewData(s)) {
          hasRecords.set(true)
        }
      }
    } finally {
      evalPool.shutdown()
    }
    logDebug(s"Can create new batch = ${hasRecords.get()}")
    hasRecords.get()
  }

  def hasShardEndAsOffset(shardInfo: Seq[ShardInfo]): Boolean = {
    shardInfo.exists {
      s: (ShardInfo) => (s.iteratorType.contains(new ShardEnd().iteratorType))
    }
  }

  /** Returns the shards position to start reading data from */
  override def getOffset: Option[Offset] = synchronized {
    val defaultOffset = new ShardOffsets(-1L, streamName)
    val prevBatchId = currentShardOffsets.getOrElse(defaultOffset).batchId
    val prevShardsInfo = prevBatchShardInfo(prevBatchId)

    val latestShardInfo: Array[ShardInfo] = {
      if (prevBatchId < 0
        || latestDescribeShardTimestamp == -1
        || ((latestDescribeShardTimestamp + describeShardInterval) < System.currentTimeMillis())) {
        val latestShards = withReader(_.getShards())
        latestDescribeShardTimestamp = System.currentTimeMillis()
        ShardSyncer.getLatestShardInfo(latestShards, prevShardsInfo,
          initialPosition, failOnDataLoss)
      } else {
        prevShardsInfo
      }
    }.toArray

    if (!avoidEmptyBatches
        || prevBatchId < 0
        || hasShardEndAsOffset(latestShardInfo)
        || ShardSyncer.hasNewShards(prevShardsInfo, latestShardInfo)
        || ShardSyncer.hasDeletedShards(prevShardsInfo, latestShardInfo)
        || canCreateNewBatch(latestShardInfo)) {
      currentShardOffsets = Some(new ShardOffsets(prevBatchId + 1, streamName, latestShardInfo))
    } else {
      log.info("Offsets are unchanged since `kinesis.client.avoidEmptyBatches` is enabled")
    }

    currentShardOffsets match {
      case None => None
      case Some(cso) => Some(KinesisSourceOffset(cso))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"End Offset is ${end.toString}")
    val currBatchShardOffset = KinesisSourceOffset.getShardOffsets(end)
    val currBatchId = currBatchShardOffset.batchId
    var prevBatchId: Long = start match {
      case Some(prevBatchStartOffset) =>
        KinesisSourceOffset.getShardOffsets(prevBatchStartOffset).batchId
      case None => -1.toLong
    }
    assert(prevBatchId <= currBatchId)

    val shardInfos = {
      // filter out those shardInfos for which ShardIterator is shard_end
      currBatchShardOffset.shardInfoMap.values.toSeq.filter {
        s: (ShardInfo) => !(s.iteratorType.contains(new ShardEnd().iteratorType))
      }.sortBy(_.shardId.toString)
    }
    logInfo(s"Processing ${shardInfos.length} shards from ${shardInfos}")

    // Create an RDD that reads from Kinesis
    val kinesisSourceRDD = new KinesisSourceRDD(
      sc,
      sourceOptions,
      streamName,
      currBatchId,
      shardInfos,
      kinesisCredsProvider,
      endPointURL,
      hadoopConf(sqlContext),
      metadataPath,
      failOnDataLoss)

    val rdd = kinesisSourceRDD.map { r: Record =>
      InternalRow(
        // asByteArrayUnsafe avoids the defensive copy asByteArray would make. Safe here:
        // the array is handed straight to the InternalRow and the Record is discarded, so
        // nothing mutates it afterwards. This keeps the v1 zero-copy behaviour of
        // getData.array() without v1's risk of exposing backing-buffer slack.
        r.data.asByteArrayUnsafe(),
        UTF8String.fromString(streamName),
        UTF8String.fromString(r.partitionKey),
        UTF8String.fromString(r.sequenceNumber),
        // v2 returns an Instant rather than a java.util.Date.
        DateTimeUtils.instantToMicros(r.approximateArrivalTimestamp)
      )
    }

    // On recovery, getBatch will get called before getOffset
    if (currentShardOffsets.isEmpty) {
      currentShardOffsets = Some(currBatchShardOffset)
    }

    logInfo("GetBatch generating RDD of offset range: " +
      shardInfos.mkString(", "))

    classicSqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)

  }

  override def schema: StructType = KinesisReader.kinesisSchema

  /**
   * Stop this source and free any resources it has allocated.
   *
   * Readers are created and closed per operation (see withReader), so there is nothing
   * long-lived left to release here; this remains a no-op close for symmetry.
   */
  override def stop(): Unit = synchronized {
    withReader(_ => ())
  }

  override def commit(end: Offset): Unit = {
    val defaultOffset = new ShardOffsets(-1L, streamName)
    val currBatchId = currentShardOffsets.getOrElse(defaultOffset).batchId
    val thresholdBatchId = currBatchId - minBatchesToRetain
    if (thresholdBatchId >= 0) {
      logInfo(s"Purging Committed Entries. ThresholdBatchId = ${thresholdBatchId}")
      metadataCommitter.purge(thresholdBatchId)
    }
  }

  override def toString(): String = s"KinesisSource[$streamName]"

  private def prevBatchShardInfo(batchId: Long): Seq[ShardInfo] = {
    val shardInfo = if (batchId < 0) {
      logInfo(s"This is the first batch. Returning Empty sequence")
      Seq.empty[ShardInfo]
    } else {
      logDebug(s"BatchId of previously executed batch is $batchId")
      val prevShardinfo = metadataCommitter.get(batchId)
      if (prevShardinfo.isEmpty) {
        throw new IllegalStateException(s"Unable to fetch " +
          s"committed metadata from previous batch. Some data may have been missed")
      }
      prevShardinfo
    }
    logDebug(s"Shard Info is ${shardInfo.mkString(", ")}")
    shardInfo
  }

}

object KinesisSource {

  val VERSION = 1

  private var _hadoopConf: SerializableConfiguration = null

  def hadoopConf(sqlContext: SQLContext): SerializableConfiguration = {
    if (_hadoopConf == null) {
      val conf: Configuration = sqlContext.sparkSession.sessionState.newHadoopConf()
      _hadoopConf = new SerializableConfiguration(conf)
    }
    _hadoopConf
  }

}
