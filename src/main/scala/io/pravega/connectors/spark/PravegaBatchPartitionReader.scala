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

package io.pravega.connectors.spark

import io.pravega.client.batch.SegmentRange
import io.pravega.client.stream.TruncatedDataException
import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.{BatchClientFactory, ClientConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/** An [[InputPartition]] for reading a Pravega stream. */
case class PravegaBatchInputPartition(
                                 segmentRange: SegmentRange,
                                 clientConfig: ClientConfig) extends InputPartition

object PravegaBatchReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[PravegaBatchInputPartition]
    PravegaBatchPartitionReader(p.segmentRange, p.clientConfig)
  }
}

/** An [[PartitionReader]] for reading a Pravega stream. */
case class PravegaBatchPartitionReader(
                                              segmentRange: SegmentRange,
                                              clientConfig: ClientConfig) extends PartitionReader[InternalRow] with Logging {

  private val batchClientFactory = BatchClientFactory.withScope(segmentRange.getScope, clientConfig)
  private val iterator = batchClientFactory.readSegment(segmentRange, new ByteBufferSerializer)

  private val converter = new PravegaRecordToUnsafeRowConverter
  private var nextRow: UnsafeRow = _

  override def next(): Boolean = {
    if (iterator.hasNext) {
      val offset = iterator.getOffset

      try
      {
        val event = iterator.next()
        nextRow = converter.toUnsafeRow(
          event.array,
          segmentRange.getScope,
          segmentRange.getStreamName,
          segmentRange.getSegmentId,
          offset)
      }
      catch
      {
        case e: TruncatedDataException =>
          log.warn("next: TruncatedDataException while reading data. Data was deleted, skipping over missing entries.", e)
          return next()
      }
      true
    } else false
  }

  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    log.debug("close: BEGIN")
    iterator.close()
    // Make sure your Pravega Client use the changes from this PR: https://github.com/pravega/pravega/pull/5415
    batchClientFactory.close()
    log.debug("close: END")
  }
}
