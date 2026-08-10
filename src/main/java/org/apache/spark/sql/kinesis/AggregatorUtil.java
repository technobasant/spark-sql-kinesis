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

package org.apache.spark.sql.kinesis;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.producer.protobuf.Messages;

/**
 * Splits KPL-aggregated Kinesis records back into their constituent user records.
 *
 * <p>This replaces KCL 1.x's {@code UserRecord.deaggregate}, which could not be kept because
 * KCL 1.x is built on the end-of-life AWS SDK v1. The aggregation wire format is documented at
 * <a href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md">
 * aggregation-format.md</a>; the protobuf classes come from the KPL artifact, so no KCL
 * dependency is needed. Derived from the equivalent class in KCL 2.x and in the awslabs
 * spark-sql-kinesis-connector, both Apache-2.0.
 *
 * <p>Deaggregated sub-records carry the <em>parent</em> record's sequence number, which is what
 * KCL 1.x's {@code UserRecord} reported too. That matters: this connector checkpoints on
 * {@code Record.sequenceNumber()}, so preserving it keeps existing checkpoints readable.
 */
public class AggregatorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatorUtil.class);

  public static final byte[] AGGREGATED_RECORD_MAGIC = new byte[]{-13, -119, -102, -62};
  private static final int DIGEST_SIZE = 16;
  private static final BigInteger STARTING_HASH_KEY = BigInteger.ZERO;
  /** Largest hash key: 2^128-1. */
  private static final BigInteger ENDING_HASH_KEY = new BigInteger("FF".repeat(16), 16);

  /** Deaggregate without discarding anything on hash-key grounds. */
  public List<Record> deaggregate(List<Record> records) {
    return deaggregate(records, STARTING_HASH_KEY, ENDING_HASH_KEY);
  }

  public List<Record> deaggregate(List<Record> records, String startingHashKey,
                                  String endingHashKey) {
    return deaggregate(records, new BigInteger(startingHashKey), new BigInteger(endingHashKey));
  }

  /**
   * Deaggregates the given records. Sub-records whose effective hash key falls outside
   * [startingHashKey, endingHashKey] are dropped, along with the earlier sub-records of the
   * same aggregate — matching KCL's behaviour for a record that does not belong to this shard.
   * Records that are not KPL aggregates are passed through untouched.
   */
  public List<Record> deaggregate(List<Record> records,
                                  BigInteger startingHashKey,
                                  BigInteger endingHashKey) {
    List<Record> result = new ArrayList<>();
    final int magicLen = AGGREGATED_RECORD_MAGIC.length;

    for (Record r : records) {
      /*
       * Read straight out of the payload array instead of copying the body into a
       * scratch buffer: on a high-volume aggregated stream that copy is per record and
       * shows up as GC pressure. asByteArrayUnsafe is safe because nothing here mutates
       * the array, and the md5 and protobuf parse both take (array, offset, length).
       */
      byte[] data = r.data().asByteArrayUnsafe();
      final int bodyOffset = magicLen;
      final int bodyLength = data.length - magicLen - DIGEST_SIZE;

      boolean isAggregated = data.length > magicLen + DIGEST_SIZE
          && Arrays.equals(AGGREGATED_RECORD_MAGIC, 0, magicLen, data, 0, magicLen);

      if (isAggregated) {
        byte[] expectedDigest = md5(data, bodyOffset, bodyLength);
        boolean digestMatches = Arrays.equals(
            expectedDigest, 0, DIGEST_SIZE,
            data, data.length - DIGEST_SIZE, data.length);

        if (!digestMatches) {
          // Tail checksum mismatch: treat as a plain record rather than risk mis-parsing.
          isAggregated = false;
        } else {
          try {
            Messages.AggregatedRecord ar = Messages.AggregatedRecord.parser()
                .parseFrom(data, bodyOffset, bodyLength);
            List<String> pks = ar.getPartitionKeyTableList();
            List<String> ehks = ar.getExplicitHashKeyTableList();
            int recordsInCurrRecord = 0;

            for (Messages.Record mr : ar.getRecordsList()) {
              String explicitHashKey = null;
              String partitionKey = pks.get((int) mr.getPartitionKeyIndex());
              if (mr.hasExplicitHashKeyIndex()) {
                explicitHashKey = ehks.get((int) mr.getExplicitHashKeyIndex());
              }

              BigInteger effectiveHashKey = effectiveHashKey(partitionKey, explicitHashKey);
              if (effectiveHashKey.compareTo(startingHashKey) < 0
                  || effectiveHashKey.compareTo(endingHashKey) > 0) {
                LOG.warn("effectiveHashKey {} not in range ({}, {}); dropping {} sub-record(s)",
                    effectiveHashKey, startingHashKey, endingHashKey, recordsInCurrRecord);
                for (int toRemove = 0; toRemove < recordsInCurrRecord; ++toRemove) {
                  result.remove(result.size() - 1);
                }
                break;
              }

              ++recordsInCurrRecord;
              result.add(Record.builder()
                  // toByteArray already produced a fresh array, so fromByteArrayUnsafe
                  // avoids copying it a second time.
                  .data(SdkBytes.fromByteArrayUnsafe(mr.getData().toByteArray()))
                  .partitionKey(partitionKey)
                  .sequenceNumber(r.sequenceNumber())
                  .approximateArrivalTimestamp(r.approximateArrivalTimestamp())
                  .encryptionType(r.encryptionType())
                  .build());
            }
            continue;
          } catch (InvalidProtocolBufferException e) {
            isAggregated = false;
          } catch (RuntimeException e) {
            LOG.error("Unexpected exception during deaggregation of record with sequence number {}",
                r.sequenceNumber(), e);
            isAggregated = false;
          }
        }
      }

      if (!isAggregated) {
        result.add(r);
      }
    }
    return result;
  }

  private BigInteger effectiveHashKey(String partitionKey, String explicitHashKey) {
    if (explicitHashKey == null) {
      return new BigInteger(1, md5(partitionKey.getBytes(StandardCharsets.UTF_8)));
    }
    return new BigInteger(explicitHashKey);
  }

  private byte[] md5(byte[] data) {
    return md5(data, 0, data.length);
  }

  private byte[] md5(byte[] data, int offset, int length) {
    try {
      MessageDigest d = MessageDigest.getInstance("MD5");
      d.update(data, offset, length);
      return d.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
