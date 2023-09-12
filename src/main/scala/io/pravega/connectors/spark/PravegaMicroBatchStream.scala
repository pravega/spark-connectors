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

import io.pravega.client.admin.StreamManager
import io.pravega.client.segment.impl.SegmentTruncatedException
import io.pravega.client.stream.{Stream, StreamCut}
import io.pravega.client.{BatchClientFactory, ClientConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadAllAvailable, ReadLimit, ReadMaxRows, SupportsAdmissionControl}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import resource.managed

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * A [[MicroBatchStream]] for Pravega. It uses the Pravega batch API to read micro batches
 * from a Pravega stream.
 *
 * @param startStreamCut   Not used when starting from a checkpoint.
 * @param endStreamCut     Not used when starting from a checkpoint.
 */
class PravegaMicroBatchStream(
                               scopeName: String,
                               streamName: String,
                               clientConfig: ClientConfig,
                               options: CaseInsensitiveMap[String],
                               startStreamCut: PravegaStreamCut,
                               endStreamCut: PravegaStreamCut
                             )
  extends MicroBatchStream with SupportsAdmissionControl with Logging {

  protected var batchStartStreamCut: StreamCut = _
  protected var batchEndStreamCut: StreamCut = _
  log.info(s"Initializing micro-batch stream: ${this}")

  private val batchClientFactory = BatchClientFactory.withScope(scopeName, clientConfig)

  private val streamManager = StreamManager.create(clientConfig)

  // Resolve start and end stream cuts now.
  // We must ensure that getStartOffset and getEndOffset return specific stream cuts, even
  // if the caller passes "earliest" or "latest".
  private lazy val initialStreamInfo = PravegaUtils.getStreamInfo(streamManager, scopeName, streamName)

  private val resolvedStartStreamCut = startStreamCut match {
    case EarliestStreamCut | UnboundedStreamCut => initialStreamInfo.getHeadStreamCut
    case LatestStreamCut => initialStreamInfo.getTailStreamCut
    case SpecificStreamCut(sc) => sc
  }
  private val resolvedEndStreamCut = endStreamCut match {
    case LatestStreamCut => Some(initialStreamInfo.getTailStreamCut)
    case SpecificStreamCut(sc) => Some(sc)
    case UnboundedStreamCut => None
    case _ => throw new IllegalArgumentException()
  }
  log.info(s"resolvedStartStreamCut=${resolvedStartStreamCut}, resolvedEndStreamCut=${resolvedEndStreamCut}")


  override def initialOffset(): Offset = {
    PravegaSourceOffset(resolvedStartStreamCut)
  }

  override def latestOffset(): Offset = {
    PravegaSourceOffset(PravegaUtils.getStreamInfo(streamManager, scopeName, streamName).getTailStreamCut)
  }

  /**
   * Returns a list of {@link InputPartition}s. Each {@link InputPartition} is responsible for
   * creating a data reader to output data of one RDD partition. The number of input partitions
   * returned here is the same as the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    log.info(s"planInputPartitions(${start},${end})")
    lazy val streamInfo = PravegaUtils.getStreamInfo(streamManager, scopeName, streamName)
    batchStartStreamCut = Option(start)
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedStartStreamCut)
    batchEndStreamCut = Option(end)
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedEndStreamCut.getOrElse(streamInfo.getTailStreamCut))
    (for (batchClientFactory <- managed(BatchClientFactory.withScope(scopeName, clientConfig))) yield {
      batchClientFactory
        .getSegments(Stream.of(scopeName, streamName), batchStartStreamCut, batchEndStreamCut)
        .getIterator
        .asScala
        .toList
        .map(PravegaBatchInputPartition(_, clientConfig): InputPartition)
        .toArray
    }).acquireAndGet(identity)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    PravegaBatchReaderFactory
  }

  override def deserializeOffset(json: String): Offset = {
    PravegaSourceOffset(JsonUtils.streamCut(json))
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    streamManager.close()
  }

  override def toString(): String = {
    s"PravegaMicroBatchStream{clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}" +
      s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}}"
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startOffset = Option(start)
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut).get
    val limits: Seq[ReadLimit] = readLimit match {
      case rows => Seq(rows)
    }
    val nextStreamCut = if (limits.exists(_.isInstanceOf[ReadAllAvailable])) {
      PravegaSourceOffset(PravegaUtils.getStreamInfo(streamManager, scopeName, streamName).getTailStreamCut)
    } else {
      val upperLimit = limits.find(_.isInstanceOf[ReadMaxRows]).map(_.asInstanceOf[ReadMaxRows])
      PravegaSourceOffset(batchClientFactory.getNextStreamCut(startOffset, upperLimit.get.maxRows()))
      try {
        PravegaSourceOffset(batchClientFactory.getNextStreamCut(startOffset, upperLimit.get.maxRows()))
      }
      catch
      {
        case e: SegmentTruncatedException => PravegaSourceOffset(PravegaUtils.getStreamInfo(streamManager, scopeName, streamName).getTailStreamCut)
      }
    }
    log.info(s"nextStreamCut = ${nextStreamCut.streamCut}")
    nextStreamCut
  }

  override def getDefaultReadLimit: ReadLimit = {
    val approxBytesPerTrigger = Option(options.get(
          PravegaSourceProvider.APPROX_BYTES_PER_TRIGGER)).get.map(_.toLong)
    if ( approxBytesPerTrigger.isDefined) {
        ReadLimit.maxRows(approxBytesPerTrigger.get)
    } else {
      super.getDefaultReadLimit
    }
  }
}
