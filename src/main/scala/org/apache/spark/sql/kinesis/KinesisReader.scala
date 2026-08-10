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

import java.math.BigInteger
import java.net.URI
import java.time.Instant
import java.util
import java.util.{ArrayList, Locale}
import java.util.concurrent.{Executors, ThreadFactory}

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import software.amazon.awssdk.core.exception.AbortedException
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.{GetRecordsRequest, GetRecordsResponse, GetShardIteratorRequest, GetShardIteratorResponse, KinesisException, LimitExceededException, ListShardsRequest, ListShardsResponse, ProvisionedThroughputExceededException, Record, ResourceNotFoundException, Shard, ShardIteratorType}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}


// This class uses Kinesis API to read data offsets from Kinesis

private[kinesis] case class KinesisReader(
    readerOptions: Map[String, String],
    streamName: String,
    kinesisCredsProvider: SparkAWSCredentials,
    endpointUrl: String
) extends Serializable with Logging {

  /*
   * Used to ensure execute fetch operations execute in an UninterruptibleThread
   */
  val kinesisReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("Kinesis Reader") {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    }
  })

  val execContext = ExecutionContext.fromExecutorService(kinesisReaderThread)

  private val maxOffsetFetchAttempts =
    readerOptions.getOrElse("client.numRetries".toLowerCase(Locale.ROOT), "3").toInt

  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("client.retryIntervalMs".toLowerCase(Locale.ROOT), "1000").toLong

  private val maxRetryIntervalMs: Long = {
    readerOptions.getOrElse("client.maxRetryIntervalMs".toLowerCase(Locale.ROOT), "10000").toLong
  }

  private val maxSupportedShardsPerStream = 10000;

  private val aggregatorUtil = new AggregatorUtil

  private var _amazonClient: KinesisClient = null

  /*
   * SDK v1 inferred the region from setEndpoint; v2 requires it explicitly alongside any
   * endpoint override, so it is derived from the endpoint URL here.
   */
  private def getAmazonClient(): KinesisClient = {
    if (_amazonClient == null) {
      _amazonClient = KinesisClient.builder()
        .credentialsProvider(kinesisCredsProvider.provider)
        .region(KinesisReader.regionFromEndpointUrl(endpointUrl))
        .endpointOverride(URI.create(endpointUrl))
        // Pinned rather than left to SPI discovery. The KPL pulls in url-connection-client
        // via the Glue schema registry, so two sync HTTP implementations are registered and
        // ServiceLoader ordering would decide which one is used. Apache is the pooling
        // client, matching what the SDK v1 client used before this migration.
        .httpClientBuilder(ApacheHttpClient.builder())
        .build()
    }
    _amazonClient
  }

  def getShards(): Seq[Shard] = {
    val shards = listShards
    logInfo(s"List shards in Kinesis Stream:  ${shards}")
    shards
  }

  def close(): Unit = {
    runUninterruptibly {
      if (_amazonClient != null) {
        _amazonClient.close()
        _amazonClient = null
      }
    }
    kinesisReaderThread.shutdown()
  }

  def getShardIterator(shardId: String,
                       iteratorType: String,
                       iteratorPosition: String,
                       failOnDataLoss: Boolean = true): String = {

    val requestBuilder = GetShardIteratorRequest.builder()
      .shardId(shardId)
      .streamName(streamName)
      .shardIteratorType(ShardIteratorType.fromValue(iteratorType))

    if (iteratorType == "AFTER_SEQUENCE_NUMBER" || iteratorType == "AT_SEQUENCE_NUMBER") {
      requestBuilder.startingSequenceNumber(iteratorPosition)
    }

    if (iteratorType == "AT_TIMESTAMP") {
      logDebug(s"TimeStamp while getting shard iterator ${
        Instant.ofEpochMilli(iteratorPosition.toLong).toString}")
      requestBuilder.timestamp(Instant.ofEpochMilli(iteratorPosition.toLong))
    }

    val getShardIteratorRequest = requestBuilder.build()

    runUninterruptibly {
      retryOrTimeout[GetShardIteratorResponse](
        s"Fetching Shard Iterator") {
        try {
          getAmazonClient.getShardIterator(getShardIteratorRequest)
        } catch {
          case r: ResourceNotFoundException =>
            if (!failOnDataLoss) {
              GetShardIteratorResponse.builder().build()
            }
            else {
              throw r
            }
        }
      }
    }.shardIterator
  }


  def getKinesisRecords(shardIterator: String, limit: Int): GetRecordsResponse = {
    val getRecordsRequest = GetRecordsRequest.builder()
      .shardIterator(shardIterator)
      .limit(limit)
      .build()
    val getRecordsResult: GetRecordsResponse = runUninterruptibly {
      retryOrTimeout[ GetRecordsResponse ](s"get Records for a shard ") {
        getAmazonClient.getRecords(getRecordsRequest)
      }
    }
    getRecordsResult
  }


  /*
   * Splits KPL-aggregated records. Replaces KCL 1.x's UserRecord.deaggregate; the SDK v1
   * subclass check it relied on is gone because v2's Record is final, so aggregation is now
   * detected from the record payload's magic bytes instead (see AggregatorUtil).
   */
  def deaggregateRecords(records: util.List[ Record ], shard: Shard): util.List[ Record] = {
    if (records.isEmpty) {
      records
    } else if (shard != null) {
      aggregatorUtil.deaggregate(
        records,
        new BigInteger(shard.hashKeyRange.startingHashKey),
        new BigInteger(shard.hashKeyRange.endingHashKey))
    } else {
      aggregatorUtil.deaggregate(records)
    }
  }

  private def listShards(): Seq[Shard] = {
    var nextToken: String = null
    val shards = new ArrayList[Shard]()

    do {
      // A ListShards request carries either a stream name or a next-token, never both.
      val listShardsRequest = if (nextToken == null) {
        ListShardsRequest.builder()
          .streamName(streamName)
          .maxResults(maxSupportedShardsPerStream)
          .build()
      } else {
        ListShardsRequest.builder()
          .nextToken(nextToken)
          .maxResults(maxSupportedShardsPerStream)
          .build()
      }

      val listShardsResult: ListShardsResponse = runUninterruptibly {
        retryOrTimeout[ListShardsResponse]( s"List shards") {
            getAmazonClient.listShards(listShardsRequest)
        }
      }
      shards.addAll(listShardsResult.shards)
      nextToken = listShardsResult.nextToken()
    } while (nextToken != null && !nextToken.isEmpty)

    shards.asScala.toSeq
  }

  /*
   * This method ensures that the closure is called in an [[UninterruptibleThread]].
   * This is required when communicating with the AWS. In the case
   */
  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  /** Helper method to retry Kinesis API request with exponential backoff and timeouts */
  private def retryOrTimeout[T](message: String)(body: => T): T = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    val startTimeMs = System.currentTimeMillis()
    var retryCount = 0
    var result: Option[T] = None
    var lastError: Throwable = null
    var waitTimeInterval = offsetFetchAttemptIntervalMs

    def isMaxRetryDone = retryCount >= maxOffsetFetchAttempts

    while (result.isEmpty && !isMaxRetryDone) {
      if ( retryCount > 0 ) { // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval = scala.math.min(waitTimeInterval * 2, maxRetryIntervalMs)
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) =>
          lastError = t
          t match {
            case ptee: ProvisionedThroughputExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ptee)
            case lee: LimitExceededException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", lee)
            case ae: AbortedException =>
              logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ae)
            case ake: KinesisException =>
              if (ake.statusCode() >= 500) {
                logWarning(s"Error while $message [attempt = ${retryCount + 1}]", ake)
              } else {
                throw new IllegalStateException(s"Error while $message", ake)
              }
            case e: Throwable =>
              throw new IllegalStateException(s"Error while $message", e)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      throw new IllegalStateException(
        s"Gave up after $retryCount retries while $message, last exception: ", lastError)
    }
  }

}


private [kinesis]  object KinesisReader {

  /*
   * SDK v1's setEndpoint derived the region from the endpoint hostname; v2 has no equivalent,
   * so parse it. Handles the standard `kinesis.<region>.amazonaws.com` form (and the China
   * `.amazonaws.com.cn` variant), and falls back to the ambient default region for custom or
   * local endpoints such as kinesalite.
   */
  private[kinesis] def regionFromEndpointUrl(endpointUrl: String): Region = {
    val host = Option(URI.create(endpointUrl).getHost).getOrElse("")
    val fromHost = host.split('.') match {
      case parts if parts.length >= 3 && parts(0).startsWith("kinesis") => Some(parts(1))
      case _ => None
    }
    fromHost
      .filter(id => Region.regions().asScala.exists(_.id == id))
      .map(Region.of)
      .getOrElse {
        new DefaultAwsRegionProviderChain().getRegion
      }
  }

  val kinesisSchema: StructType =
      StructType(Seq(
        StructField("data", BinaryType),
        StructField("streamName", StringType),
        StructField("partitionKey", StringType),
        StructField("sequenceNumber", StringType),
        StructField("approximateArrivalTimestamp", TimestampType))
      )
}
