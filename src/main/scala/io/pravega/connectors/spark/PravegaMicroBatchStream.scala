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

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Executors;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.{StreamManager, ReaderGroupManager}
import io.pravega.client.stream.{Stream, StreamCut, ReaderGroupConfig, ReaderConfig, ReaderGroup, EventStreamReader, EventRead}
import io.pravega.client.stream.impl.{ByteBufferSerializer, PositionImpl, StreamCutImpl}
import io.pravega.client.{BatchClientFactory, ClientConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, SupportsAdmissionControl, ReadLimit, ReadAllAvailable, ReadMaxRows}
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
  extends SupportsAdmissionControl with MicroBatchStream with Logging {

  protected var batchStartStreamCut: StreamCut = _
  protected var batchEndStreamCut: StreamCut = _
  log.info(s"Initializing micro-batch stream: ${this}")

  private val streamManager = StreamManager.create(clientConfig)
  private val maxEventPerTrigger = options.get(PravegaSourceProvider.MAX_EVENTS_PER_TRIGGER).map(_.toLong)
  private val readerGroupName = UUID.randomUUID().toString().replace("-", "");
  private var readerGroupManager: ReaderGroupManager = _
  private var readerGroup: ReaderGroup = _
  private var clientFactory: EventStreamClientFactory = _
  private var eventReader: EventStreamReader[ByteBuffer] = _
  private val executor = Executors.newSingleThreadScheduledExecutor();

  // Resolve start and end stream cuts now.
  // We must ensure that getStartOffset and getEndOffset return specific stream cuts, even
  // if the caller passes "earliest" or "latest".
  private lazy val initialStreamInfo = streamManager.getStreamInfo(scopeName, streamName)

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

  if (maxEventPerTrigger.isDefined) {
      readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig.getControllerURI())
      readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName), resolvedStartStreamCut).build())
      readerGroup = readerGroupManager.getReaderGroup(readerGroupName)
      clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig)
      eventReader = clientFactory.createReader("reader", readerGroupName, new ByteBufferSerializer, ReaderConfig.builder().build())
  }

  override def getDefaultReadLimit: ReadLimit = {
    if (maxEventPerTrigger.isDefined) {
      ReadLimit.maxRows(maxEventPerTrigger.get)
    } else {
      super.getDefaultReadLimit
    }
  }

  override def initialOffset(): Offset = {
    PravegaSourceOffset(resolvedStartStreamCut)
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    if(readLimit.isInstanceOf[ReadAllAvailable]) {
      PravegaSourceOffset(initialStreamInfo.getTailStreamCut)
    } else {
      val maxRows = readLimit.asInstanceOf[ReadMaxRows].maxRows()
      var event: EventRead[ByteBuffer] = null
      var count: Long = 0
      do {
        event = eventReader.readNextEvent(100)
        count += 1
      } while (event.getEvent() != null && count < maxRows)
      log.info(s"check streamcut")
      val position = event.getPosition().asInstanceOf[PositionImpl].getOwnedSegmentsWithOffsets()
      val streamcut = new StreamCutImpl(Stream.of(scopeName, streamName), position)
      PravegaSourceOffset(streamcut)
    }
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
    lazy val streamInfo = streamManager.getStreamInfo(scopeName, streamName)
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
    if (readerGroup != null) {
      readerGroup.close()
    }
    if (readerGroupManager != null) {
      readerGroupManager.close()
    }
    if (eventReader != null) {
      eventReader.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }

  override def toString(): String = {
    s"PravegaMicroBatchStream{clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}" +
      s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}}"
  }
}
